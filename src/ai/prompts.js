const SYSTEM_CONTEXT_KEYS_TO_DROP = new Set(['__v', 'createdAt', 'updatedAt']);
const EXPERT_PROFILES = Object.freeze({
  investor:
    'Ты Жесткий Инвест-аналитик. Фокус: юнит-экономика, кассовые разрывы, ROI, риски договоров.',
  hr: 'Ты HR-профайлер. Фокус: совместимость команды, стили управления, психотипы, риск конфликтов.',
  strategist: 'Ты Бизнес-стратег. Фокус: поиск неочевидных рычагов, транзитных связей, точек роста.',
  default: 'Ты Аналитик Synapse12. Фокус: базовая оценка связей.',
});

const STRICT_FORMATTING_RULES = `
ПРАВИЛА ВЫДАЧИ (КРИТИЧЕСКИ ВАЖНО):
1. ПИШИ ТОЛЬКО ЧИСТЫМ ТЕКСТОМ.
2. КАТЕГОРИЧЕСКИ ЗАПРЕЩЕНО использовать Markdown (звездочки **, решетки #, списки -, _, жирный шрифт, курсив).
3. ЗАПРЕЩЕНО использовать эмодзи.
4. НИКАКОЙ ВОДЫ. Отвечай строго в 4 абзаца:
Факт: [сухой факт из данных]
Связь: [твой инсайт]
Вывод: [оценка риска/вероятности]
Вопрос: [один хирургический вопрос пользователю для следующего шага]
`.trim();

const ALLOWED_ROUTER_ROLES = new Set(['investor', 'hr', 'strategist', 'default']);

function isPathInsideDocuments(path) {
  return Array.isArray(path) && path.includes('documents');
}

function cleanContextValue(value, path) {
  if (value instanceof Date) {
    return new Date(value.getTime());
  }

  if (Array.isArray(value)) {
    return value.map((item, index) => cleanContextValue(item, path.concat(String(index))));
  }

  if (!value || typeof value !== 'object') {
    return value;
  }

  const source = value;
  const output = {};

  for (const [key, nestedValue] of Object.entries(source)) {
    if (SYSTEM_CONTEXT_KEYS_TO_DROP.has(key)) {
      continue;
    }

    if (key === 'attachments') {
      continue;
    }

    if (key === 'chat_history') {
      continue;
    }

    if (key === 'description_history') {
      if (Array.isArray(nestedValue)) {
        if (!nestedValue.length) {
          output[key] = [];
        } else {
          const lastItem = nestedValue[nestedValue.length - 1];
          output[key] = [cleanContextValue(lastItem, path.concat(key, '0'))];
        }
      } else {
        output[key] = cleanContextValue(nestedValue, path.concat(key));
      }
      continue;
    }

    if (key === 'data' && isPathInsideDocuments(path)) {
      continue;
    }

    if (key === 'image' && path[path.length - 1] === 'profile') {
      continue;
    }

    output[key] = cleanContextValue(nestedValue, path.concat(key));
  }

  return output;
}

function cleanContextData(entities) {
  return cleanContextValue(entities, []);
}

function collectEntitySemanticSignals(entity) {
  if (!entity || typeof entity !== 'object') return [];

  const metadata = entity.ai_metadata && typeof entity.ai_metadata === 'object' ? entity.ai_metadata : {};
  const directTags = Array.isArray(entity.tags) ? entity.tags : [];
  const directRoles = Array.isArray(entity.roles) ? entity.roles : [];
  const metaTags = Array.isArray(metadata.tags) ? metadata.tags : [];
  const metaRoles = Array.isArray(metadata.roles) ? metadata.roles : [];

  return [...directTags, ...directRoles, ...metaTags, ...metaRoles]
    .map((item) => (typeof item === 'string' ? item.trim() : ''))
    .filter(Boolean);
}

function normalizeDetectedRole(rawRole) {
  const normalized = typeof rawRole === 'string' ? rawRole.trim().toLowerCase() : '';
  if (!normalized) return 'default';

  // Router can return extra text; take first semantic token only.
  const firstToken = normalized
    .split(/[\s,.;:!?\n\r\t]+/g)
    .map((item) => item.trim())
    .find(Boolean);

  if (firstToken && ALLOWED_ROUTER_ROLES.has(firstToken)) {
    return firstToken;
  }

  if (normalized.includes('investor')) return 'investor';
  if (normalized.includes('strategist')) return 'strategist';
  if (normalized === 'hr' || normalized.includes(' hr')) return 'hr';
  return 'default';
}

function createAiPrompts(deps) {
  const {
    AI_CONTEXT_ENTITY_LIMIT,
    toTrimmedString,
    toProfile,
    getEntityAnalyzerFields,
    normalizeDescriptionHistory,
    normalizeImportanceHistory,
  } = deps;

  function buildAgentContextData({ scopeContext, history, attachments }) {
    const cleanedEntities = cleanContextData(scopeContext.entities);

    return {
      scope: {
        type: scopeContext.scopeType,
        name: scopeContext.scopeName,
        entityType: scopeContext.entityType,
        projectId: scopeContext.projectId,
        projectName: scopeContext.projectName,
        totalEntities: scopeContext.totalEntities,
        contextLimit: AI_CONTEXT_ENTITY_LIMIT,
      },
      entities: cleanedEntities,
      connections: scopeContext.connections,
      attachments,
      history,
    };
  }

  function buildRouterPrompt(contextData, userMessage) {
    const entities = Array.isArray(contextData?.entities) ? contextData.entities : [];
    const tagsAndRoles = entities
      .map((entity) => collectEntitySemanticSignals(entity).join(' '))
      .filter(Boolean)
      .join(' ')
      .slice(0, 6000);

    const query = toTrimmedString(userMessage, 2400);

    return [
      `Проанализируй запрос "${query}" и теги: "${tagsAndRoles}".`,
      'Определи, какой эксперт нужен. Верни СТРОГО ОДНО СЛОВО из списка: investor, hr, strategist, default.',
      'Не пиши больше ничего.',
    ].join('\n');
  }

  function buildAgentSystemPrompt(contextData, detectedRole = 'default') {
    const normalizedRole = normalizeDetectedRole(detectedRole);
    const expertText = EXPERT_PROFILES[normalizedRole] || EXPERT_PROFILES.default;
    const scopeSource =
      contextData && typeof contextData === 'object'
        ? contextData.scope && typeof contextData.scope === 'object'
          ? contextData.scope
          : contextData
        : {};
    const scopeType = toTrimmedString(scopeSource.scopeType || scopeSource.type, 24);
    const projectName = toTrimmedString(scopeSource.projectName, 140);
    const entityType = toTrimmedString(scopeSource.entityType, 64);
    const totalEntities = Number(scopeSource.totalEntities) || 0;
    const scopeDescription =
      scopeType === 'project'
        ? `Текущий контекст: проект "${projectName}" (${totalEntities} сущностей).`
        : `Текущий контекст: вкладка "${entityType}" (${totalEntities} сущностей).`;

    return [
      expertText,
      scopeDescription,
      'Жесткое правило: используй ТОЛЬКО данные из переданного контекста.',
      STRICT_FORMATTING_RULES,
    ].join('\n');
  }

  function buildAgentUserPrompt({ contextData, scopeContext, message, history, attachments }) {
    const payloadContext =
      contextData && typeof contextData === 'object'
        ? contextData
        : buildAgentContextData({
            scopeContext,
            history,
            attachments,
          });

    return [
      'Контекст Synapse12 (JSON):',
      JSON.stringify(payloadContext, null, 2),
      '',
      'Текущий запрос пользователя:',
      toTrimmedString(message, 2400),
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
    buildAgentContextData,
    buildRouterPrompt,
    normalizeDetectedRole,
    buildAgentSystemPrompt,
    buildAgentUserPrompt,
    buildEntityAnalyzerSystemPrompt,
    buildEntityAnalyzerUserPrompt,
    buildEntityAnalysisReplyText,
  };
}

module.exports = {
  createAiPrompts,
  cleanContextData,
};
