const PROJECT_CHAT_ENRICHMENT_FIELDS = Object.freeze([
  'tags',
  'markers',
  'roles',
  'skills',
  'risks',
  'priority',
  'status',
  'tasks',
  'metrics',
  'owners',
  'participants',
  'resources',
  'outcomes',
  'industry',
  'departments',
  'stage',
  'date',
  'location',
  'phones',
  'links',
  'importance',
  'ignoredNoise',
]);

function createProjectEnrichmentPrompts(deps) {
  const {
    toTrimmedString,
    toProfile,
  } = deps;

  function buildCompactProjectContextGroups(groups) {
    return (Array.isArray(groups) ? groups : [])
      .slice(0, 80)
      .map((group) => {
        const row = toProfile(group);
        const id = toTrimmedString(row.id, 80);
        const nodeIds = (Array.isArray(row.nodeIds) ? row.nodeIds : [])
          .map((nodeId) => toTrimmedString(nodeId, 80))
          .filter(Boolean);
        const members = (Array.isArray(row.members) ? row.members : [])
          .map((member) => toTrimmedString(member, 160))
          .filter(Boolean)
          .slice(0, 24);
        if (!id || (nodeIds.length < 2 && members.length < 2)) return null;
        return {
          id,
          name: toTrimmedString(row.name, 120),
          ...(nodeIds.length ? { nodeIds } : {}),
          ...(members.length ? { members } : {}),
        };
      })
      .filter(Boolean);
  }

  function deriveIsolatedEntityIds(entities, connections) {
    const connected = new Set();
    for (const connection of Array.isArray(connections) ? connections : []) {
      const from = toTrimmedString(connection?.from, 80);
      const to = toTrimmedString(connection?.to, 80);
      if (from) connected.add(from);
      if (to) connected.add(to);
    }

    return (Array.isArray(entities) ? entities : [])
      .map((entity) => toTrimmedString(entity?.id, 80))
      .filter((entityId) => entityId && !connected.has(entityId))
      .slice(0, 80);
  }

  function deriveAuthorNeighborEntityIds(authorEntityId, connections) {
    const authorId = toTrimmedString(authorEntityId, 80);
    if (!authorId) return [];

    const neighbors = new Set();
    for (const connection of Array.isArray(connections) ? connections : []) {
      const from = toTrimmedString(connection?.from, 80);
      const to = toTrimmedString(connection?.to, 80);
      if (!from || !to) continue;
      if (from === authorId && to !== authorId) neighbors.add(to);
      if (to === authorId && from !== authorId) neighbors.add(from);
    }

    return Array.from(neighbors).slice(0, 40);
  }

  function buildProjectContextBuildPayload({
    contextData,
  }) {
    const scope = toProfile(contextData?.scope);
    const compactEntities = (Array.isArray(contextData?.entities) ? contextData.entities : [])
      .slice(0, 180)
      .map((entity) => {
        const row = toProfile(entity);
        return {
          id: toTrimmedString(row.id || row._id, 80),
          type: toTrimmedString(row.type, 24),
          name: toTrimmedString(row.name, 120),
          description: toTrimmedString(row.description || toProfile(row.ai_metadata).description, 2400),
          isAuthor: row.isAuthor === true || row.is_me === true || row.is_mine === true,
          is_me: row.is_me === true,
          is_mine: row.is_mine === true,
        };
      })
      .filter((entity) => entity.id && (entity.name || entity.description));

    const compactConnections = (Array.isArray(contextData?.connections) ? contextData.connections : [])
      .slice(0, 240)
      .map((connection) => {
        const row = toProfile(connection);
        return {
          from: toTrimmedString(row.from || row.source, 80),
          to: toTrimmedString(row.to || row.target, 80),
          label: toTrimmedString(row.label, 160),
          description: toTrimmedString(
            row.description || row.meaning || row.semanticMeaning || row.summary || row.label,
            600,
          ),
          relationMode: toTrimmedString(row.relationMode, 32),
          direction: toTrimmedString(row.direction, 64),
          directedFrom: toTrimmedString(row.directedFrom, 80),
          directedTo: toTrimmedString(row.directedTo, 80),
        };
      })
      .filter((connection) => connection.from && connection.to);
    const compactGroups = buildCompactProjectContextGroups(contextData?.groups);

    const authorEntityId = compactEntities.find((entity) => entity.is_me === true)?.id
      || compactEntities.find((entity) => entity.is_mine === true)?.id
      || '';
    const isolatedEntityIds = deriveIsolatedEntityIds(compactEntities, compactConnections);
    const authorNeighborEntityIds = deriveAuthorNeighborEntityIds(authorEntityId, compactConnections);

    return {
      project_name: toTrimmedString(scope.projectName || scope.name, 160),
      author_entity_id: authorEntityId,
      isolated_entity_ids: isolatedEntityIds,
      author_neighbor_entity_ids: authorNeighborEntityIds,
      graph: {
        entities: compactEntities,
        connections: compactConnections,
        groups: compactGroups,
      },
    };
  }

  function buildProjectEnrichmentSystemPrompt() {
    return [
      'Ты Synapse12 Project Context Extractor.',
      'Работай только по входному JSON-контексту без внешних фактов.',
      'Задача: извлечь структурированные поля проекта из диалога пользователя и ответа ассистента.',
      'Сохраняй только релевантные факты для проекта и связанных сущностей.',
      'Удаляй дубликаты (регистронезависимо), обрезай шум, не раздувай списки.',
      'Возвращай только короткие содержательные элементы (не отдельные слова без смысла).',
      `Разрешенные keys в fields: ${PROJECT_CHAT_ENRICHMENT_FIELDS.join(', ')}.`,
      'links: только валидные URL.',
      'importance: только [Низкая, Средняя, Высокая], массив из 0..1 элементов.',
      'tasks: конкретные действия/шаги, а не абстракции.',
      'ignoredNoise: факты, не относящиеся к текущему проекту.',
      'Верни СТРОГО JSON без markdown.',
      'Формат:',
      '{',
      '  "status": "ready | need_clarification",',
      '  "summary": "string",',
      '  "changeReason": "string",',
      '  "fields": {',
      '    "tags": [],',
      '    "markers": [],',
      '    "roles": [],',
      '    "skills": [],',
      '    "risks": [],',
      '    "priority": [],',
      '    "status": [],',
      '    "tasks": [],',
      '    "metrics": [],',
      '    "owners": [],',
      '    "participants": [],',
      '    "resources": [],',
      '    "outcomes": [],',
      '    "industry": [],',
      '    "departments": [],',
      '    "stage": [],',
      '    "date": [],',
      '    "location": [],',
      '    "phones": [],',
      '    "links": [],',
      '    "importance": [],',
      '    "ignoredNoise": []',
      '  },',
      '  "clarifyingQuestions": []',
      '}',
    ].join('\n');
  }

  function buildProjectEnrichmentUserPrompt({
    contextData,
    message,
    assistantReply,
    history,
    currentProjectFields,
    aggregatedEntityFields,
  }) {
    const compactEntities = (Array.isArray(contextData?.entities) ? contextData.entities : [])
      .slice(0, 140)
      .map((entity) => {
        const row = toProfile(entity);
        return {
          id: toTrimmedString(row.id || row._id, 80),
          type: toTrimmedString(row.type, 24),
          name: toTrimmedString(row.name, 120),
          description: toTrimmedString(row.description || toProfile(row.ai_metadata).description, 2400),
        };
      });

    const payload = {
      scope: toProfile(contextData?.scope),
      dialogue: {
        userMessage: toTrimmedString(message, 2400),
        assistantReply: toTrimmedString(assistantReply, 4000),
      },
      history: Array.isArray(history) ? history : [],
      currentProjectFields: toProfile(currentProjectFields),
      aggregatedEntityFields: toProfile(aggregatedEntityFields),
      entityHints: compactEntities,
      connections: Array.isArray(contextData?.connections) ? contextData.connections : [],
    };

    return ['Контекст обогащения проекта (JSON):', JSON.stringify(payload, null, 2)].join('\n');
  }

  function buildProjectContextBuildSystemPrompt() {
    return [
      'Ты Synapse12 Project Context Builder.',
      'Твоя задача: собрать для другой LLM широкий связный контекст проекта.',
      'На входе JSON граф проекта.',
      'Используй только данные из этого JSON.',
      'Вход содержит: project_name, author_entity_id, isolated_entity_ids, author_neighbor_entity_ids, entities, connections, groups.',
      'У каждой сущности учитывай только: id, type, name, description, is_me, is_mine, isAuthor.',
      'У каждой связи учитывай только: from, to, label, description, relationMode, direction, directedFrom, directedTo.',
      'Описание сущности — это общий профиль карточки. Роль сущности именно в проекте определяй в первую очередь по графу связей, их названиям и направлению стрелок.',
      'Название связи и направление обязательны для интерпретации смысла. Не считай связь симметричной, если это не подтверждено relationMode/direction.',
      'Не присваивай автору проекта цели, ресурсы, финансовые метрики, ограничения или мотивы других сущностей без явного основания в графе.',
      'Жёстко различай: личный контур автора, цели отдельных компаний, цели отдельных персон, цели специальных goal/result/task сущностей.',
      'Если сущность попала в isolated_entity_ids или не имеет рабочих связей, описывай её как отдельный узел на канве, а не как встроенный элемент активного контура.',
      'Верни compiled_context: развернутый, но компактный проектный бриф для другой LLM.',
      'Этот бриф должен логически связывать сущности между собой и двигаться по проекту связно, а не списком.',
      'Пиши normal prose с пустой строкой между абзацами.',
      'Количество абзацев не ограничивай искусственно: сохрани столько деталей, сколько нужно для полной картины проекта.',
      'Двигайся по логике графа: авторский контур (если есть) -> напрямую связанные с ним сущности -> другие рабочие кластеры -> изолированные узлы.',
      'Не давай советы и не решай за автора, что важно или неважно. Не пиши next steps, bottleneck, leverage или рекомендации.',
      'Не выдумывай факты вне графа, но можно осторожно связать факты между собой, если связь следует из описаний и ребер.',
      'Не сжимай агрессивно: лучше сохранить больше существенных фактов, чем потерять смысл проекта.',
      'Собери настолько полный контекст, насколько нужно для сохранения картины проекта для второй LLM.',
      'Не добавляй поля вне схемы.',
      'Верни только валидный JSON без markdown.',
      'JSON schema:',
      '{',
      '  "compiled_context": "string"',
      '}',
    ].join('\n');
  }

  function buildProjectContextBuildUserPrompt({
    contextData,
  }) {
    const payload = buildProjectContextBuildPayload({
      contextData,
    });

    return ['Project graph (JSON):', JSON.stringify(payload, null, 2)].join('\n');
  }

  return {
    buildProjectEnrichmentSystemPrompt,
    buildProjectEnrichmentUserPrompt,
    buildProjectContextBuildSystemPrompt,
    buildProjectContextBuildPayload,
    buildProjectContextBuildUserPrompt,
  };
}

module.exports = {
  createProjectEnrichmentPrompts,
};
