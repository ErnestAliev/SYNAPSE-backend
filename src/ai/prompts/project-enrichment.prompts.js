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

const PROJECT_CONTEXT_ENTITY_TYPE_SEMANTICS = Object.freeze([
  'person: конкретный человек. Это не роль и не группа. Его описание раскрывает качества, статус, мотивацию, ограничения, отношения и влияние человека.',
  'company: юридическое лицо, бизнес-единица или организация. Это не просто группа людей. У компании могут быть активы, обязательства, риски, процессы, контракты, штрафы, показатели, контрагенты и операционная роль.',
  'event: событие, эпизод, происшествие, изменение или проверка, которое оказывает прямое или косвенное влияние на другие сущности. Детали внутри event описывают, что произошло, к чему это ведет, кто затронут и какие последствия возникают.',
  'resource: ресурс, актив, инструмент, канал, имущество, деньги, доступ или иное средство, которое можно использовать в проекте.',
  'goal: целевое состояние или намерение, к которому стремится конкретная сущность или контур. Goal не принадлежит автору автоматически; принадлежность цели нужно выводить из графа и формулировок.',
  'result: уже полученный итог, эффект, артефакт или достигнутое состояние. Это не план и не намерение, а то, что уже произошло или было получено.',
  'task: конкретное действие, поручение или работа, которую нужно выполнить. Task обычно обслуживает goal, entity или project-контур, но не равен цели.',
  'project: контейнерный или рамочный узел проекта. Его смысл в описании общего контура, а не в подмене остальных сущностей.',
  'shape: универсальный свободный вспомогательный блок. Он может хранить контекст, мысль, пояснение, дополнительное описание, расшифровку, комментарий к сущности, общий смысловой блок или расширение связи/ситуации. Его нельзя автоматически трактовать как полноценную бизнес-сущность.',
]);

const PROJECT_CONTEXT_GRAPH_INTERPRETATION_RULES = Object.freeze([
  'Связь и направление стрелки определяют проектную функцию сущности сильнее, чем ее общая карточка.',
  'Если связь направлена от A к B, не делай автоматически вывод о симметричности смысла. Проверяй directedFrom, directedTo, direction и relationMode.',
  'Лейбл связи задает характер отношения: конкурент, поставщик, партнер, отец, риск, владелец, клиент, проверка, влияние и т.д.',
  'Сущность может влиять на другую не только напрямую, но и через детали внутри своего описания. Например event может быть связан с person, а последствия внутри event могут создавать угрозу company.',
  'Если event связан с сущностью, интерпретируй это как факт влияния, изменения, давления, риска, повода или контекста для этой сущности, если описание event это подтверждает.',
  'Если внутри описания сущности явно названы другие сущности или последствия для них, учитывай это как косвенный смысловой контур, но не выдумывай отсутствующие ребра.',
  'Если узел не имеет связей, не встраивай его в активный рабочий контур без отдельного основания. Это отдельная сущность на канве, а не доказанная часть действующей системы отношений.',
  'Цели, ресурсы, риски и результаты должны привязываться к тем сущностям, для которых это подтверждено связями, направлением и текстом описаний.',
]);

function createProjectEnrichmentPrompts(deps) {
  const {
    toTrimmedString,
    toProfile,
  } = deps;

  function buildCompactProjectContextGroups(groups) {
    return (Array.isArray(groups) ? groups : [])
      .map((group) => {
        const row = toProfile(group);
        const id = toTrimmedString(row.id, 80);
        const nodeIds = (Array.isArray(row.nodeIds) ? row.nodeIds : [])
          .map((nodeId) => toTrimmedString(nodeId, 80))
          .filter(Boolean);
        const memberEntityIds = (Array.isArray(row.memberEntityIds) ? row.memberEntityIds : [])
          .map((memberId) => toTrimmedString(memberId, 80))
          .filter(Boolean);
        const members = (Array.isArray(row.members) ? row.members : [])
          .concat(Array.isArray(row.memberTitles) ? row.memberTitles : [])
          .map((member) => toTrimmedString(member, 160))
          .filter(Boolean)
          .filter((value, index, source) => source.indexOf(value) === index);
        const memberEntities = (Array.isArray(row.memberEntities) ? row.memberEntities : [])
          .map((entity) => {
            const entityRow = toProfile(entity);
            const entityId = toTrimmedString(entityRow.id, 80);
            const entityName = toTrimmedString(entityRow.name, 160);
            if (!entityId && !entityName) return null;
            return {
              id: entityId,
              type: toTrimmedString(entityRow.type, 40),
              name: entityName,
              description: toTrimmedString(entityRow.description, 2400),
              nodeIds: (Array.isArray(entityRow.nodeIds) ? entityRow.nodeIds : [])
                .map((nodeId) => toTrimmedString(nodeId, 80))
                .filter(Boolean),
            };
          })
          .filter(Boolean);
        const directConnections = (Array.isArray(row.directConnections) ? row.directConnections : [])
          .map((connection) => {
            const connectionRow = toProfile(connection);
            return {
              id: toTrimmedString(connectionRow.id, 80),
              sourceAnchorId: toTrimmedString(connectionRow.sourceAnchorId, 80),
              targetAnchorId: toTrimmedString(connectionRow.targetAnchorId, 80),
              sourceTitle: toTrimmedString(connectionRow.sourceTitle, 160),
              targetTitle: toTrimmedString(connectionRow.targetTitle, 160),
              sourceKind: toTrimmedString(connectionRow.sourceKind, 32),
              targetKind: toTrimmedString(connectionRow.targetKind, 32),
              label: toTrimmedString(connectionRow.label, 160),
              description: toTrimmedString(connectionRow.description, 1200),
              relationType: toTrimmedString(connectionRow.relationType, 64),
              relationMode: toTrimmedString(connectionRow.relationMode, 32),
              direction: toTrimmedString(connectionRow.direction, 64),
              directedFrom: toTrimmedString(connectionRow.directedFrom, 160),
              directedTo: toTrimmedString(connectionRow.directedTo, 160),
            };
          })
          .filter((connection) => connection.sourceTitle && connection.targetTitle);
        const effectiveConnections = (Array.isArray(row.effectiveConnections) ? row.effectiveConnections : [])
          .map((connection) => {
            const connectionRow = toProfile(connection);
            return {
              id: toTrimmedString(connectionRow.id, 80),
              rawConnectionId: toTrimmedString(connectionRow.rawConnectionId, 80),
              sourceEntityId: toTrimmedString(connectionRow.sourceEntityId, 80),
              targetEntityId: toTrimmedString(connectionRow.targetEntityId, 80),
              sourceTitle: toTrimmedString(connectionRow.sourceTitle, 160),
              targetTitle: toTrimmedString(connectionRow.targetTitle, 160),
              label: toTrimmedString(connectionRow.label, 160),
              description: toTrimmedString(connectionRow.description, 1200),
              relationType: toTrimmedString(connectionRow.relationType, 64),
              relationMode: toTrimmedString(connectionRow.relationMode, 32),
              direction: toTrimmedString(connectionRow.direction, 64),
              directedFrom: toTrimmedString(connectionRow.directedFrom, 160),
              directedTo: toTrimmedString(connectionRow.directedTo, 160),
              isInheritedFromGroup: connectionRow.isInheritedFromGroup === true,
              inheritanceMode: toTrimmedString(connectionRow.inheritanceMode, 40),
              inheritedViaSourceGroupId: toTrimmedString(connectionRow.inheritedViaSourceGroupId, 80),
              inheritedViaSourceGroupTitle: toTrimmedString(connectionRow.inheritedViaSourceGroupTitle, 160),
              inheritedViaTargetGroupId: toTrimmedString(connectionRow.inheritedViaTargetGroupId, 80),
              inheritedViaTargetGroupTitle: toTrimmedString(connectionRow.inheritedViaTargetGroupTitle, 160),
            };
          })
          .filter((connection) => connection.sourceTitle && connection.targetTitle);
        if (!id || (nodeIds.length < 2 && members.length < 2)) return null;
        return {
          id,
          name: toTrimmedString(row.name, 120),
          ...(nodeIds.length ? { nodeIds } : {}),
          ...(memberEntityIds.length ? { memberEntityIds } : {}),
          ...(members.length ? { members } : {}),
          ...(memberEntities.length ? { memberEntities } : {}),
          ...(directConnections.length ? { directConnections } : {}),
          ...(effectiveConnections.length ? { effectiveConnections } : {}),
        };
      })
      .filter(Boolean);
  }

  function deriveIsolatedEntityIds(entities, connections) {
    const connected = new Set();
    for (const connection of Array.isArray(connections) ? connections : []) {
      const from = toTrimmedString(connection?.sourceEntityId || connection?.from, 80);
      const to = toTrimmedString(connection?.targetEntityId || connection?.to, 80);
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
      const from = toTrimmedString(connection?.sourceEntityId || connection?.from, 80);
      const to = toTrimmedString(connection?.targetEntityId || connection?.to, 80);
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
      .map((entity) => {
        const row = toProfile(entity);
        return {
          id: toTrimmedString(row.id || row._id, 80),
          type: toTrimmedString(row.type, 24),
          name: toTrimmedString(row.name, 120),
          description: toTrimmedString(row.description || toProfile(row.ai_metadata).description, 6000),
          isAuthor: row.isAuthor === true || row.is_me === true || row.is_mine === true,
          is_me: row.is_me === true,
          is_mine: row.is_mine === true,
          nodeIds: (Array.isArray(row.nodeIds) ? row.nodeIds : [])
            .map((nodeId) => toTrimmedString(nodeId, 80))
            .filter(Boolean),
        };
      })
      .filter((entity) => entity.id && (entity.name || entity.description));

    const compactConnections = (Array.isArray(contextData?.connections) ? contextData.connections : [])
      .map((connection) => {
        const row = toProfile(connection);
        return {
          id: toTrimmedString(row.id, 80),
          sourceNodeId: toTrimmedString(row.sourceNodeId || row.sourceAnchorId || row.source, 80),
          targetNodeId: toTrimmedString(row.targetNodeId || row.targetAnchorId || row.target, 80),
          sourceEntityId: toTrimmedString(row.sourceEntityId || row.from, 80),
          targetEntityId: toTrimmedString(row.targetEntityId || row.to, 80),
          from: toTrimmedString(row.sourceEntityId || row.from, 80),
          to: toTrimmedString(row.targetEntityId || row.to, 80),
          sourceKind: toTrimmedString(row.sourceKind, 32),
          targetKind: toTrimmedString(row.targetKind, 32),
          sourceType: toTrimmedString(row.sourceType, 40),
          targetType: toTrimmedString(row.targetType, 40),
          sourceTitle: toTrimmedString(row.sourceTitle || row.fromTitle || row.from || row.source, 160),
          targetTitle: toTrimmedString(row.targetTitle || row.toTitle || row.to || row.target, 160),
          label: toTrimmedString(row.label, 160),
          description: toTrimmedString(
            row.description || row.meaning || row.semanticMeaning || row.summary || row.label,
            1200,
          ),
          relationType: toTrimmedString(row.relationType || row.type, 64),
          relationMode: toTrimmedString(row.relationMode, 32),
          direction: toTrimmedString(row.direction, 64),
          directedFrom: toTrimmedString(row.directedFrom, 160),
          directedTo: toTrimmedString(row.directedTo, 160),
        };
      })
      .filter((connection) => connection.sourceTitle && connection.targetTitle);
    const compactRawConnections = (Array.isArray(contextData?.rawConnections) ? contextData.rawConnections : [])
      .map((connection) => {
        const row = toProfile(connection);
        return {
          id: toTrimmedString(row.id, 80),
          sourceAnchorId: toTrimmedString(row.sourceAnchorId, 80),
          targetAnchorId: toTrimmedString(row.targetAnchorId, 80),
          sourceEntityId: toTrimmedString(row.sourceEntityId || row.from, 80),
          targetEntityId: toTrimmedString(row.targetEntityId || row.to, 80),
          sourceKind: toTrimmedString(row.sourceKind, 32),
          targetKind: toTrimmedString(row.targetKind, 32),
          sourceType: toTrimmedString(row.sourceType, 40),
          targetType: toTrimmedString(row.targetType, 40),
          sourceTitle: toTrimmedString(row.sourceTitle || row.fromTitle, 160),
          targetTitle: toTrimmedString(row.targetTitle || row.toTitle, 160),
          label: toTrimmedString(row.label, 160),
          description: toTrimmedString(row.description, 1200),
          relationType: toTrimmedString(row.relationType || row.type, 64),
          relationMode: toTrimmedString(row.relationMode, 32),
          direction: toTrimmedString(row.direction, 64),
          directedFrom: toTrimmedString(row.directedFrom, 160),
          directedTo: toTrimmedString(row.directedTo, 160),
        };
      })
      .filter((connection) => connection.sourceTitle && connection.targetTitle);
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
      graph_stats: {
        entities: compactEntities.length,
        raw_connections: compactRawConnections.length,
        effective_connections: compactConnections.length,
        groups: compactGroups.length,
      },
      graph: {
        entities: compactEntities,
        connections: compactConnections,
        raw_connections: compactRawConnections,
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
    const typeSemanticsText = PROJECT_CONTEXT_ENTITY_TYPE_SEMANTICS.map((line) => `- ${line}`).join('\n');
    const graphRulesText = PROJECT_CONTEXT_GRAPH_INTERPRETATION_RULES.map((line) => `- ${line}`).join('\n');

    return [
      'Ты Synapse12 Project Context Builder.',
      'Твоя задача: собрать для другой LLM широкий связный контекст проекта.',
      'На входе JSON граф проекта.',
      'Используй только данные из этого JSON.',
      'Вход содержит: project_name, author_entity_id, isolated_entity_ids, author_neighbor_entity_ids, graph_stats, entities, connections, raw_connections, groups.',
      'У каждой сущности учитывай только: id, type, name, description, is_me, is_mine, isAuthor.',
      'graph.connections — это effective graph для LLM: в нем group-level связи уже развернуты на участников групп.',
      'graph.raw_connections — это физические ребра канвы в том виде, как они реально нарисованы.',
      'У каждой effective связи учитывай: sourceNodeId, targetNodeId, sourceEntityId, targetEntityId, sourceTitle, targetTitle, sourceKind, targetKind, sourceType, targetType, label, description, relationType, relationMode, direction, directedFrom, directedTo.',
      'Если isInheritedFromGroup=true, значит связь не рисовалась напрямую между двумя сущностями, а была унаследована от group-level связи.',
      'Если у группы есть связи, сначала интерпретируй группу как единый кластер, а затем наследуй этот смысл на всех members группы.',
      'Группа — это не декоративная рамка, а родительский контур сущностей. Связи группы распространяются на ее members, если для конкретного member нет более точной прямой связи, которая меняет интерпретацию.',
      'Если есть и прямая member-level связь, и inherited group-level связь, считай прямую связь более точной.',
      'sourceTitle/targetTitle показывают, кто физически соединен на канве или в effective graph. sourceEntityId/targetEntityId показывают, какие entity-карточки стоят за узлами.',
      'Описание сущности — это общий профиль карточки. Роль сущности именно в проекте определяй в первую очередь по графу связей, их названиям и направлению стрелок.',
      'Название связи, relationType и направление обязательны для интерпретации смысла. Не считай связь симметричной, если это не подтверждено relationMode/direction.',
      'Сначала прочитай groups и raw_connections, чтобы понять контуры и кластеры. Затем используй effective graph (connections), чтобы понять, как group-level смысл распространяется на отдельных участников.',
      'Не присваивай автору проекта цели, ресурсы, финансовые метрики, ограничения или мотивы других сущностей без явного основания в графе.',
      'Жёстко различай: личный контур автора, цели отдельных компаний, цели отдельных персон, цели специальных goal/result/task сущностей.',
      'Если сущность попала в isolated_entity_ids или не имеет рабочих связей, описывай её как отдельный узел на канве, а не как встроенный элемент активного контура.',
      'Семантика типов сущностей:',
      typeSemanticsText,
      'Правила чтения графа и связей:',
      graphRulesText,
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
