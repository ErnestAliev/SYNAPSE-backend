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
        };
      })
      .filter((connection) => connection.from && connection.to);

    const compactGroups = (Array.isArray(contextData?.groups) ? contextData.groups : [])
      .slice(0, 80)
      .map((group) => {
        const row = toProfile(group);
        const id = toTrimmedString(row.id, 80);
        const nodeIds = (Array.isArray(row.nodeIds) ? row.nodeIds : [])
          .map((nodeId) => toTrimmedString(nodeId, 80))
          .filter(Boolean);
        if (!id || nodeIds.length < 2) return null;
        return { id, nodeIds };
      })
      .filter(Boolean);

    const authorEntityId = compactEntities.find((entity) => entity.is_me === true)?.id
      || compactEntities.find((entity) => entity.is_mine === true)?.id
      || '';

    return {
      project_name: toTrimmedString(scope.projectName || scope.name, 160),
      author_entity_id: authorEntityId,
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
      'Твоя задача: собрать для другой LLM короткий связный контекст проекта и параллельно вернуть оценки сущностей и связей.',
      'На входе JSON граф проекта.',
      'Используй только данные из этого JSON.',
      'Вход содержит: project_name, author_entity_id, entities, connections, groups.',
      'У каждой сущности учитывай только: id, type, name, description, is_me, is_mine, isAuthor.',
      'У каждой связи учитывай только: from, to, label, description.',
      'Сначала собери compiled_context: короткий осмысленный пересказ проекта.',
      'Этот пересказ должен логически связывать сущности между собой и двигаться по проекту связно, а не списком.',
      'Не давай советы и не решай за автора, что важно или неважно. Не пиши next steps, bottleneck, leverage или рекомендации.',
      'Не выдумывай факты вне графа, но можно осторожно связать факты между собой, если связь следует из описаний и ребер.',
      'Собери настолько полный краткий контекст, насколько нужно для сохранения смысла проекта.',
      'Старайся не превышать 7000 символов и не раздувать текст без необходимости.',
      'После compiled_context верни project_analysis_map.',
      'Для каждой сущности верни только: entity_id, goal_relevance, confidence.',
      'Для каждой связи верни только: from, to, label, meaning, polarity, connection_relevance, connection_strength, confidence.',
      'Все числовые оценки в диапазоне 0..100.',
      'Если данных мало или описание слабое, понижай confidence.',
      'Если сущность или связь слабо относится к цели проекта, relevance должен быть низким или нулевым.',
      'Не добавляй поля вне схемы.',
      'Верни только валидный JSON без markdown.',
      'JSON schema:',
      '{',
      '  "compiled_context": "string",',
      '  "analysisMap": {',
      '    "project_name": "string",',
      '    "author_entity_id": "string",',
      '    "entities": [',
      '      {',
      '        "entity_id": "string",',
      '        "goal_relevance": 0,',
      '        "confidence": 0',
      '      }',
      '    ],',
      '    "connections": [',
      '      {',
      '        "from": "string",',
      '        "to": "string",',
      '        "label": "string",',
      '        "meaning": "string",',
      '        "polarity": "positive | neutral | negative",',
      '        "connection_relevance": 0,',
      '        "connection_strength": 0,',
      '        "confidence": 0',
      '      }',
      '    ]',
      '  }',
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
