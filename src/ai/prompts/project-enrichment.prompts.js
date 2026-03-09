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

  return {
    buildProjectEnrichmentSystemPrompt,
    buildProjectEnrichmentUserPrompt,
  };
}

module.exports = {
  createProjectEnrichmentPrompts,
};
