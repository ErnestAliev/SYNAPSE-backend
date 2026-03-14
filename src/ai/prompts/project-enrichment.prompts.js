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
    author,
    narrativeRings,
    sourceHash,
  }) {
    const compactEntities = (Array.isArray(contextData?.entities) ? contextData.entities : [])
      .slice(0, 180)
      .map((entity) => {
        const row = toProfile(entity);
        return {
          id: toTrimmedString(row.id || row._id, 80),
          name: toTrimmedString(row.name, 120),
          description: toTrimmedString(row.description || toProfile(row.ai_metadata).description, 2400),
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

    return {
      sourceHash: toTrimmedString(sourceHash, 120),
      author: toProfile(author),
      narrativeRings: toProfile(narrativeRings),
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
      'Работай только по входному JSON-контексту без внешних фактов и догадок.',
      'Задача: собрать project_analysis_map по dashboard snapshot.',
      'Источник истины для этой задачи: только description сущностей, связи/группы графа и author flags.',
      'Нельзя использовать структурированные поля сущностей, историю чатов сущностей, text_input, voice_input, documents, description_history и любые сырые диалоги.',
      'Нужно вернуть аналитическую карту проекта, а не набор project fields.',
      'Не выдумывай факты, роли, метрики и связи.',
      'Учитывай группы как агрегированные узлы, если они есть во входных данных.',
      'Сначала оцени каждую сущность как рабочий узел проекта: роль, сильные стороны, слабые стороны, возможности, риски, why_now.',
      'Если во входе указан author, используй его как опорную систему координат: кто это, какова его роль в проекте, из какого личного контура задаются цели и вопросы.',
      'Важно: author не равен единственному центру анализа. После фиксации авторского контура обязательно проверь внешний слой проекта на скрытые возможности, недооцененные активы, bottlenecks и ограничения.',
      'Двигайся по спирали: author contour -> ближайшие сущности -> рабочие связи -> проектный синтез.',
      'Связывай тезисы причинно-следственно: кто управляет чем, что уже дает результат, что мешает росту, где скрытое leverage.',
      'importance в entities[] возвращай числом от 0 до 100.',
      'strength в connections[] возвращай числом от 0 до 100.',
      'confidence в project_synthesis возвращай числом от 0 до 100.',
      'evidence храни короткими цитатами или указателями на факты, не длинными абзацами.',
      'Если данных не хватает, лучше верни пустую строку/пустой массив, чем выдумывай.',
      'Верни СТРОГО JSON без markdown.',
      'Формат:',
      '{',
      '  "status": "ready | need_clarification",',
      '  "summary": "string",',
      '  "changeReason": "string",',
      '  "missing": [],',
      '  "analysisMap": {',
      '    "project_name": "string",',
      '    "author_context": {',
      '      "entity_id": "string",',
      '      "name": "string",',
      '      "role_in_project": "string",',
      '      "why_matters": "string"',
      '    },',
      '    "entities": [',
      '      {',
      '        "entity_id": "string",',
      '        "name": "string",',
      '        "type": "string",',
      '        "role_in_project": "string",',
      '        "summary": "string",',
      '        "strengths": [],',
      '        "weaknesses": [],',
      '        "opportunities": [],',
      '        "risks": [],',
      '        "importance": 0,',
      '        "why_now": "string",',
      '        "relation_to_author": "string",',
      '        "relation_to_goal": "string",',
      '        "stage": "string",',
      '        "evidence": []',
      '      }',
      '    ],',
      '    "connections": [',
      '      {',
      '        "from": "string",',
      '        "to": "string",',
      '        "label": "string",',
      '        "meaning": "string",',
      '        "impact": "positive | negative | neutral",',
      '        "strength": 0',
      '      }',
      '    ],',
      '    "project_synthesis": {',
      '      "main_goal": "string",',
      '      "current_engine": "string",',
      '      "main_bottleneck": "string",',
      '      "hidden_leverage": "string",',
      '      "critical_constraint": "string",',
      '      "next_focus": "string",',
      '      "confidence": 0',
      '    }',
      '  }',
      '}',
    ].join('\n');
  }

  function buildProjectContextBuildUserPrompt({
    contextData,
    author,
    narrativeRings,
    sourceHash,
  }) {
    const payload = buildProjectContextBuildPayload({
      contextData,
      author,
      narrativeRings,
      sourceHash,
    });

    return ['Контекст сборки проекта (JSON):', JSON.stringify(payload, null, 2)].join('\n');
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
