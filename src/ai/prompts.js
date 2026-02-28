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
    const projectExtractionHint =
      scopeType === 'project'
        ? 'Важно: для проектного чата выделяй факты, риски, задачи и метрики максимально конкретно.'
        : '';

    return [
      expertText,
      scopeDescription,
      'Жесткое правило: используй ТОЛЬКО данные из переданного контекста.',
      projectExtractionHint,
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
        const metadata = toProfile(row.ai_metadata);
        return {
          id: toTrimmedString(row.id || row._id, 80),
          type: toTrimmedString(row.type, 24),
          name: toTrimmedString(row.name, 120),
          tags: Array.isArray(metadata.tags) ? metadata.tags.slice(0, 8) : [],
          markers: Array.isArray(metadata.markers) ? metadata.markers.slice(0, 6) : [],
          roles: Array.isArray(metadata.roles) ? metadata.roles.slice(0, 6) : [],
          risks: Array.isArray(metadata.risks) ? metadata.risks.slice(0, 6) : [],
          tasks: Array.isArray(metadata.tasks) ? metadata.tasks.slice(0, 8) : [],
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

  function buildEntityQuizSystemPrompt({ entityType, level = 'level1', forceStopCheck = false } = {}) {
    const normalizedType = toTrimmedString(entityType, 24) || 'shape';
    const normalizedLevel = toTrimmedString(level, 24) || 'level1';
    const mustStopCheck = forceStopCheck === true;

    return [
      'Ты Quiz Master для заполнения сущностей Synapse12.',
      `Текущий тип сущности: ${normalizedType}.`,
      `Текущий уровень квиза: ${normalizedLevel}.`,
      'Твоя задача: короткими вопросами собрать факты для описания и полей сущности.',
      'Используй только входной контекст и текущее состояние квиза.',
      'Время не упоминай и не используй (никаких "сейчас/потом/в будущем").',
      'Не лей воду, не делай длинных объяснений.',
      'Один вопрос = один смысл.',
      'Всегда возвращай варианты: 3 смысловых + "Свой вариант".',
      'Если пользователь пишет "Ответ 2", трактуй это как выбор option id=2.',
      'Если пользователь пишет текст, трактуй это как "Свой вариант".',
      mustStopCheck
        ? 'Критично: на этом шаге обязательно верни mode="quiz_stop_check".'
        : 'Обычно возвращай mode="quiz_step".',
      'Возвращай строго JSON, без markdown и без текста вокруг.',
      'Формат quiz_step:',
      '{',
      '  "mode": "quiz_step",',
      '  "entityType": "person | company | project | event | resource | goal | result | task | shape",',
      '  "questionId": "string",',
      '  "questionText": "string",',
      '  "options": [',
      '    { "id": "1", "text": "..." },',
      '    { "id": "2", "text": "..." },',
      '    { "id": "3", "text": "..." },',
      '    { "id": "4", "text": "Свой вариант" }',
      '  ],',
      '  "expects": { "type": "choice_or_text" },',
      '  "state": {',
      '    "facts": {},',
      '    "missing": [],',
      '    "confidence": 0.0',
      '  },',
      '  "draftUpdate": {',
      '    "description": "string",',
      '    "fieldsPatch": { "tagsAdd": [], "rolesAdd": [], "linksAdd": [] }',
      '  },',
      '  "stopCheck": null',
      '}',
      'Формат quiz_stop_check:',
      '{',
      '  "mode": "quiz_stop_check",',
      '  "questionId": "stop_check",',
      '  "questionText": "Данных достаточно или углубляемся?",',
      '  "options": [',
      '    { "id": "1", "text": "Достаточно — завершить" },',
      '    { "id": "2", "text": "Углубить" },',
      '    { "id": "3", "text": "Пауза" },',
      '    { "id": "4", "text": "Свой вариант" }',
      '  ],',
      '  "summary": {',
      '    "keyFacts": [],',
      '    "risks": [],',
      '    "nextSuggestedStep": "string"',
      '  },',
      '  "state": {',
      '    "facts": {},',
      '    "missing": [],',
      '    "confidence": 0.0',
      '  },',
      '  "draftUpdate": {',
      '    "description": "string",',
      '    "fieldsPatch": { "tagsAdd": [], "rolesAdd": [], "linksAdd": [] }',
      '  }',
      '}',
    ].join('\n');
  }

  function buildEntityQuizUserPrompt({
    entityType,
    name,
    currentDescription,
    currentFields,
    quizState,
    lastQuestion,
    answer,
    forceStopCheck,
    level,
  }) {
    const quizStateSource = toProfile(quizState);
    const normalizedLevel =
      Number(quizStateSource.level) >= 2 || toTrimmedString(level, 24).toLowerCase() === 'level2' ? 2 : 1;

    const payload = {
      entity: {
        entityType: toTrimmedString(entityType, 24),
        name: toTrimmedString(name, 120),
        currentDescription: toTrimmedString(currentDescription, 2600),
        currentFields: toProfile(currentFields),
      },
      quiz: {
        level: normalizedLevel,
        forceStopCheck: forceStopCheck === true,
        state: {
          ...quizStateSource,
          level: normalizedLevel,
        },
        lastQuestion: toProfile(lastQuestion),
        answer: toProfile(answer),
      },
      instruction: {
        askShort: true,
        returnStrictJsonOnly: true,
      },
    };

    return ['Контекст шага квиза (JSON):', JSON.stringify(payload, null, 2)].join('\n');
  }

  function buildEntityAnalyzerSystemPrompt(entityType) {
    const allowedFields = getEntityAnalyzerFields(entityType);

    return [
      'Ты Synapse12 Entity Analyst.',
      `Текущий тип сущности: ${entityType}.`,
      'Работай только на данных из входного JSON.',
      'Твоя задача: интерпретировать сырые пользовательские данные и вернуть структурированный JSON.',
      `Разрешенные поля для fields: ${allowedFields.join(', ')}. Заполняй ТОЛЬКО ИХ.`,
      '',
      'ПРАВИЛА КЛАССИФИКАЦИИ ПОЛЕЙ (ПОЛНАЯ ОНТОЛОГИЯ СИСТЕМЫ):',
      'ПРАВИЛО ГРАНИЦ: Анализируй ТОЛЬКО текущую сущность. Если в тексте есть факты, описывающие ДРУГИЕ сущности (других людей, здания, компании, чужие проблемы) — КАТЕГОРИЧЕСКИ запрещено добавлять их в поля (теги, маркеры и т.д.) текущей сущности. Отправляй чужие факты в массив ignoredNoise.',
      '- markers (Маркеры): Внешняя среда, локация, или историческое событие (например: "Офис в Дубае", "Пандемия"). Это независимые условия. НИКОГДА не пиши сюда внутренние свойства или проблемы самой сущности.',
      '- tags (Теги): Внутренняя суть, свойства, характеристики (например: "B2B", "финтех", "выгорание").',
      '- roles (Роли): Социальная или бизнес-функция (например: "инвестор", "CEO", "арендатор").',
      '- risks (Риски): Угрозы и уязвимости (например: "кассовый разрыв", "текучка кадров", "поломка").',
      '- skills (Навыки): Прикладные компетенции (например: "продажи", "управление", "разработка").',
      '- status (Статус): Текущее состояние (например: "сдан в аренду", "требует ремонта", "в процессе").',
      '- outcomes (Результаты): Итоги события, решения, договоренности (например: "сделан взаимозачет", "договор расторгнут").',
      '- participants (Участники): Кто присутствовал или физически вовлечен в событие.',
      '- owners (Ответственные/Владельцы): Кто владеет активом или несет прямую ответственность за задачу/цель.',
      '- metrics (Метрики): Оцифрованные показатели, KPI, суммы, сроки (например: "LTV > $100", "2 млн тенге/мес").',
      '- priority (Приоритет): Степень важности задачи или цели.',
      '- resources (Ресурсы): Физические, цифровые или финансовые активы (например: "сервер", "бюджет 10к", "здание").',
      '- stage (Стадия): Этап жизненного цикла проекта или бизнеса (например: "инициация", "MVP", "масштабирование").',
      '- industry (Индустрия): Сфера деятельности (например: "коммерческая недвижимость", "образование").',
      '- departments (Отделы): Внутренняя организационная структура.',
      '- date / location: Конкретная дата/время и физическое/виртуальное местоположение.',
      '',
      'Нельзя превращать весь текст в теги. Каждая мысль должна лежать в правильном смысловом слое.',
      '',
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
      '  "fields": { "tags": [], "roles": [], "markers": [], "...": [] },',
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
    buildProjectEnrichmentSystemPrompt,
    buildProjectEnrichmentUserPrompt,
    buildEntityQuizSystemPrompt,
    buildEntityQuizUserPrompt,
    buildEntityAnalyzerSystemPrompt,
    buildEntityAnalyzerUserPrompt,
    buildEntityAnalysisReplyText,
  };
}

module.exports = {
  createAiPrompts,
  cleanContextData,
};
