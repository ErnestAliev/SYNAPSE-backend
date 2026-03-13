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
      'Задача: собрать краткий рабочий контекст проекта по dashboard snapshot.',
      'Источник истины для этой задачи: только description сущностей, разрешенные структурированные поля сущностей и связи/группы графа.',
      'Нельзя использовать историю чатов сущностей, text_input, voice_input, documents, description_history и любые сырые диалоги.',
      'Нужно вернуть короткое описание проекта и заполнить структурированные поля проекта.',
      'Если данных по полю нет, верни пустой массив.',
      'Не выдумывай факты, роли, метрики и ссылки.',
      'Учитывай группы как агрегированные узлы, если они есть во входных данных.',
      'Строй описание как тезисное повествование, а не как набор слов или перечисление тегов.',
      'Если во входе указан author, начинай с него как с центра контекста: кто это, чем он занимается в проекте, что находится в его ближайшем рабочем круге, какие объекты/люди/контуры вокруг него, к каким целям это ведет и какие ограничения уже видны.',
      'Двигайся по спирали: автор -> ближайшие сущности и связи -> проектный контур -> цели/метрики -> риски/статусы.',
      'Связывай тезисы причинно-следственно: кто управляет чем, зачем это делается, что уже происходит, что ограничивает движение.',
      'Описание должно быть содержательным: что это за проект, какие ключевые объекты/контуры в него входят, кто отвечает, какие цели/риски/статусы уже видны. До 900 символов.',
      `Разрешенные keys в fields: ${PROJECT_CHAT_ENRICHMENT_FIELDS.join(', ')}.`,
      'links: только валидные URL.',
      'importance: только [Низкая, Средняя, Высокая], массив из 0..1 элементов.',
      'tasks: только конкретные действия/обязательства, если они явно следуют из контекста.',
      'ignoredNoise: только то, что точно не относится к проекту.',
      'Верни СТРОГО JSON без markdown.',
      'Формат:',
      '{',
      '  "status": "ready | need_clarification",',
      '  "description": "string",',
      '  "summary": "string",',
      '  "changeReason": "string",',
      '  "missing": [],',
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
      '  }',
      '}',
    ].join('\n');
  }

  function buildProjectContextBuildUserPrompt({
    contextData,
    aggregatedEntityFields,
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
          type: toTrimmedString(row.type, 24),
          name: toTrimmedString(row.name, 120),
          description: toTrimmedString(row.description || toProfile(row.ai_metadata).description, 2400),
        };
      });

    const payload = {
      scope: toProfile(contextData?.scope),
      sourceHash: toTrimmedString(sourceHash, 120),
      author: toProfile(author),
      narrativeRings: toProfile(narrativeRings),
      graph: {
        entities: compactEntities,
        connections: Array.isArray(contextData?.connections) ? contextData.connections : [],
        groups: Array.isArray(contextData?.groups) ? contextData.groups : [],
      },
      aggregatedEntityFields: toProfile(aggregatedEntityFields),
    };

    return ['Контекст сборки проекта (JSON):', JSON.stringify(payload, null, 2)].join('\n');
  }

  return {
    buildProjectEnrichmentSystemPrompt,
    buildProjectEnrichmentUserPrompt,
    buildProjectContextBuildSystemPrompt,
    buildProjectContextBuildUserPrompt,
  };
}

module.exports = {
  createProjectEnrichmentPrompts,
};
