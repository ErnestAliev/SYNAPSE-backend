const { buildEntityAnalyzerSystemPrompt: buildEntityAnalyzerSystemPromptText } = require('./entity.prompt-builder');

const ENTITY_ANALYZER_PROMPT_LIMITS = Object.freeze({
  totalTextBudget: 26_000,
  messageMaxLength: 2400,
  voiceInputMaxLength: 2400,
  historyMaxItems: 10,
  historyItemTextMaxLength: 900,
  attachmentsMaxItems: 4,
  attachmentTextMaxLength: 2400,
  documentsMaxItems: 4,
  documentTextMaxLength: 3200,
});

function createEntityProtectedPrompts(deps) {
  const {
    toTrimmedString,
    toProfile,
    getEntityAnalyzerFields,
    normalizeDescriptionHistory,
    normalizeImportanceHistory,
  } = deps;
  function buildEntityAnalyzerSystemPrompt(entityType) {
    const allowedFields = getEntityAnalyzerFields(entityType);
    return buildEntityAnalyzerSystemPromptText({ entityType, allowedFields });
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
    const textBudgetState = { remaining: ENTITY_ANALYZER_PROMPT_LIMITS.totalTextBudget };

    function takeBudgetedText(value, maxLength) {
      if (textBudgetState.remaining <= 0) return '';
      const normalized = toTrimmedString(value, maxLength);
      if (!normalized) return '';
      if (normalized.length <= textBudgetState.remaining) {
        textBudgetState.remaining -= normalized.length;
        return normalized;
      }
      const clipped = toTrimmedString(normalized, textBudgetState.remaining);
      textBudgetState.remaining = 0;
      return clipped;
    }

    function normalizeCompactHistory(rawHistory) {
      if (!Array.isArray(rawHistory)) return [];
      return rawHistory
        .slice(-ENTITY_ANALYZER_PROMPT_LIMITS.historyMaxItems)
        .map((item) => {
          const row = toProfile(item);
          const role = row.role === 'assistant' ? 'assistant' : row.role === 'user' ? 'user' : '';
          const text = takeBudgetedText(row.text, ENTITY_ANALYZER_PROMPT_LIMITS.historyItemTextMaxLength);
          if (!role || !text) return null;
          return { role, text };
        })
        .filter(Boolean);
    }

    function normalizeCompactFiles(rawFiles, { maxItems, textMaxLength }) {
      if (!Array.isArray(rawFiles)) return [];
      return rawFiles
        .slice(0, maxItems)
        .map((item) => {
          const file = toProfile(item);
          const name = toTrimmedString(file.name, 120);
          const mime = toTrimmedString(file.mime, 120);
          const contentCategory = toTrimmedString(file.contentCategory, 24);
          const size = Number.isFinite(Number(file.size)) ? Math.max(0, Math.floor(Number(file.size))) : 0;
          const text = takeBudgetedText(file.text, textMaxLength);
          if (!name && !text) return null;
          return {
            name: name || 'Файл',
            mime,
            size,
            contentCategory,
            text,
            hasInlineData: file.hasInlineData === true,
          };
        })
        .filter(Boolean);
    }

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
      message: takeBudgetedText(message, ENTITY_ANALYZER_PROMPT_LIMITS.messageMaxLength),
      voiceInput: takeBudgetedText(voiceInput, ENTITY_ANALYZER_PROMPT_LIMITS.voiceInputMaxLength),
      history: normalizeCompactHistory(history),
      attachments: normalizeCompactFiles(attachments, {
        maxItems: ENTITY_ANALYZER_PROMPT_LIMITS.attachmentsMaxItems,
        textMaxLength: ENTITY_ANALYZER_PROMPT_LIMITS.attachmentTextMaxLength,
      }),
      documents: normalizeCompactFiles(documents, {
        maxItems: ENTITY_ANALYZER_PROMPT_LIMITS.documentsMaxItems,
        textMaxLength: ENTITY_ANALYZER_PROMPT_LIMITS.documentTextMaxLength,
      }),
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
    buildEntityAnalyzerSystemPrompt,
    buildEntityAnalyzerUserPrompt,
    buildEntityAnalysisReplyText,
  };
}

module.exports = {
  createEntityProtectedPrompts,
};
