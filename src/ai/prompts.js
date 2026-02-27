function createAiPrompts(deps) {
  const {
    AI_CONTEXT_ENTITY_LIMIT,
    toTrimmedString,
    toProfile,
    getEntityAnalyzerFields,
    normalizeDescriptionHistory,
    normalizeImportanceHistory,
  } = deps;

  function buildAgentSystemPrompt(scopeContext) {
    const scopeDescription =
      scopeContext.scopeType === 'project'
        ? `Текущий контекст: проект "${scopeContext.projectName}" (${scopeContext.totalEntities} сущностей).`
        : `Текущий контекст: вкладка "${scopeContext.entityType}" (${scopeContext.totalEntities} сущностей).`;

    return [
      'Ты LLM-аналитик системы Synapse12.',
      scopeDescription,
      'Жесткое правило: используй ТОЛЬКО данные из переданного контекста.',
      'Нельзя подтягивать данные из других вкладок, проектов или внешних источников.',
      'Если пользователь просит "повторить анализ" или "обновить вывод", анализируй текущий контекст как есть и историю диалога.',
      'Не отвечай "данные не предоставлены", если в контексте уже есть описание/теги/поля сущностей.',
      'Фразу "Недостаточно данных в текущем контексте" используй только когда в контексте реально нет фактов для вывода.',
      'Отвечай по-русски, структурно и кратко.',
      'Формат ответа:',
      '1) Краткий вывод',
      '2) Наблюдения',
      '3) Возможности и риски',
      '4) Следующие шаги',
    ].join('\n');
  }

  function buildAgentUserPrompt({ scopeContext, message, history, attachments }) {
    const contextPayload = {
      scope: {
        type: scopeContext.scopeType,
        name: scopeContext.scopeName,
        entityType: scopeContext.entityType,
        projectId: scopeContext.projectId,
        projectName: scopeContext.projectName,
        totalEntities: scopeContext.totalEntities,
        contextLimit: AI_CONTEXT_ENTITY_LIMIT,
      },
      entities: scopeContext.entities,
      connections: scopeContext.connections,
      attachments,
      history,
    };

    return [
      'Контекст Synapse12 (JSON):',
      JSON.stringify(contextPayload, null, 2),
      '',
      'Текущий запрос пользователя:',
      message,
    ].join('\n');
  }

  function buildEntityAnalyzerSystemPrompt(entityType) {
    const allowedFields = getEntityAnalyzerFields(entityType);

    return [
      'Ты Synapse12 Entity Analyst.',
      `Текущий тип сущности: ${entityType}.`,
      'Работай только на данных из входного JSON.',
      'Твоя задача: интерпретировать сырые пользовательские данные и вернуть структурированный JSON.',
      'Нельзя превращать весь текст в теги. Добавляй только осмысленные признаки.',
      `Разрешенные поля для fields: ${allowedFields.join(', ')}.`,
      'importance: только одно из [Низкая, Средняя, Высокая], вернуть как массив из 0..1 элементов.',
      'links: только валидные URL.',
      'description: 3-6 предложений, емко и без воды.',
      'changeType: одно из [initial, addition, update] относительно текущего описания.',
      'changeReason: кратко (1-2 фразы), почему это initial/addition/update.',
      'importanceSignal: одно из [increase, decrease, neutral] на основе новых фактов и истории.',
      'importanceReason: кратко, почему важность нужно повысить/понизить/оставить.',
      'Если данных мало, status=need_clarification и до 3 уточняющих вопросов.',
      'Если данных хватает, status=ready.',
      'Верни СТРОГО JSON без markdown.',
      'Формат:',
      '{',
      '  "status": "ready | need_clarification",',
      '  "description": "string",',
      '  "changeType": "initial | addition | update",',
      '  "changeReason": "string",',
      '  "fields": { "tags": [], "roles": [], ... },',
      '  "importanceSignal": "increase | decrease | neutral",',
      '  "importanceReason": "string",',
      '  "clarifyingQuestions": [],',
      '  "confidence": {},',
      '  "ignoredNoise": []',
      '}',
    ].join('\n');
  }

  function buildEntityAnalyzerUserPrompt({
    entity,
    message,
    history,
    attachments,
    currentFields,
    voiceInput,
    documents,
  }) {
    const aiMetadata = toProfile(entity.ai_metadata);

    const contextPayload = {
      entity: {
        id: String(entity._id),
        type: entity.type,
        name: toTrimmedString(entity.name, 120),
      },
      descriptionContext: {
        currentDescription: toTrimmedString(aiMetadata.description, 2200),
        recentDescriptionHistory: normalizeDescriptionHistory(aiMetadata.description_history)
          .slice(-5)
          .map((row) => ({
            at: row.at,
            changeType: row.changeType,
            reason: row.reason,
          })),
        recentImportanceHistory: normalizeImportanceHistory(aiMetadata.importance_history)
          .slice(-5)
          .map((row) => ({
            at: row.at,
            before: row.before,
            after: row.after,
            signal: row.signal,
            reason: row.reason,
          })),
      },
      currentFields,
      message,
      voiceInput,
      history,
      attachments,
      documents,
    };

    return ['Контекст сущности (JSON):', JSON.stringify(contextPayload, null, 2)].join('\n');
  }

  function buildEntityAnalysisReplyText(analysis) {
    if (analysis.status === 'need_clarification') {
      if (analysis.clarifyingQuestions.length) {
        return ['Нужны уточнения перед заполнением профиля:', ...analysis.clarifyingQuestions.map((q) => `- ${q}`)].join(
          '\n',
        );
      }
      return 'Нужны уточнения перед заполнением профиля.';
    }

    if (analysis.description) {
      const changeLabels = {
        initial: 'Первичное описание',
        addition: 'Описание дополнено',
        update: 'Описание обновлено',
      };
      const changeLabel = changeLabels[analysis.changeType] || 'Описание обновлено';
      return `Готово. ${changeLabel}.\n\n${analysis.description}`;
    }

    return 'Готово. Поля профиля обновлены.';
  }

  return {
    buildAgentSystemPrompt,
    buildAgentUserPrompt,
    buildEntityAnalyzerSystemPrompt,
    buildEntityAnalyzerUserPrompt,
    buildEntityAnalysisReplyText,
  };
}

module.exports = {
  createAiPrompts,
};
