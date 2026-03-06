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
      '- ВАЖНО: priority НЕ равен importance. Значения [Низкая, Средняя, Высокая] относятся к importance, а не к priority.',
      '- ВАЖНО: status/priority/metrics НИКОГДА не дублируй в markers.',
      '- resources (Ресурсы): Физические, цифровые или финансовые активы (например: "сервер", "бюджет 10к", "здание").',
      '- stage (Стадия): Этап жизненного цикла проекта или бизнеса (например: "инициация", "MVP", "масштабирование").',
      '- industry (Индустрия): Сфера деятельности (например: "коммерческая недвижимость", "образование").',
      '- departments (Отделы): Внутренняя организационная структура.',
      '- date / location: Конкретная дата/время и физическое/виртуальное местоположение.',
      '- Если внешних условий нет, markers должен быть пустым массивом [].',
      '',
      'ПРАВИЛА ОБРАБОТКИ ДОКУМЕНТОВ И ВЛОЖЕНИЙ:',
      'Если в contextPayload есть attachments или documents — определи их тип по полю contentCategory и действуй по правилам:',
      '',
      'contentCategory="table" (CSV, таблица, расчёты, числовые данные, XLS):',
      '  * КАТЕГОРИЧЕСКИ ЗАПРЕЩЕНО переносить сырые числа, строки таблицы, ячейки в теги, маркеры, roles и другие поля — это мусор.',
      '  * Твоя задача: проанализировать данные и сделать ВЫВОДЫ (тренды, аномалии, риски, ключевые показатели).',
      '  * Обнови description — 3-6 предложений с аналитикой по данным документа.',
      '  * Для table по умолчанию markers = []. Заполняй markers только если явно есть внешний контекст (рынок, регуляторика, локация, историческое событие).',
      '  * Нельзя переносить metrics/status/priority в markers.',
      '  * В поля (fields) записывай ТОЛЬКО значимые выводы:',
      '    - metrics: ["LTV $120", "CAC растёт 15% МоМ"] — только если цифра несёт смысловой вывод',
      '    - risks: ["кассовый разрыв Q3"] — если данные указывают на угрозу',
      '    - status, stage, priority — если из данных явно следует состояние сущности.',
      '  * Если вывода нет — оставь поле пустым. Лучше description с анализом, чем мусор в полях.',
      '',
      'contentCategory="text" (plain text, markdown, .txt, .log, .md):',
      '  * Обрабатывай как стандартное сообщение пользователя по правилам онтологии.',
      '  * Извлекай теги, роли, маркеры, риски и другие поля в обычном режиме.',
      '',
      'contentCategory="document" (DOCX, PDF, Word):',
      '  * Обрабатывай как развёрнутый текстовый документ.',
      '  * Если документ — описание сущности: заполняй description и поля по онтологии.',
      '  * Если документ — отчёт с числами: применяй правила для "table".',
      '',
      'contentCategory="structured" (JSON, XML, YAML):',
      '  * Интерпретируй структуру как факты о сущности.',
      '  * Заполняй соответствующие поля если ключи/значения значимы.',
      '  * Числовые значения — только в metrics если они являются KPI.',
      '',
      'Если contentCategory отсутствует или неизвестен — применяй правила для "text".',
      '',
      'Нельзя превращать весь текст в теги. Каждая мысль должна лежать в правильном смысловом слое.',
      '',
      'ПРАВИЛА ЗАПОЛНЕНИЯ ПОЛЕЙ:',
      `Разрешённые поля для данного типа сущности (${entityType}): ${allowedFields.join(', ')}.`,
      'Все поля внутри fields ОБЯЗАТЕЛЬНО присутствуют в ответе (схема фиксирована).',
      'Незаполненные или не разрешённые для этого типа поля — возвращай как пустой массив [].',
      'НЕ дублируй значения между полями (одно значение — в одном поле).',
      'LLM сама определяет правильное поле — сервер не переносит значения между полями.',
      '',
      'ПРАВИЛА ДЛЯ КОНКРЕТНЫХ ПОЛЕЙ:',
      'importance: массив из 0..1 элементов. Допустимые значения: "Низкая", "Средняя", "Высокая". Пусто — [].',
      'links: только валидные URL. Некорректные ссылки не включай.',
      'description: 3–6 предложений, ёмко и по существу.',
      'changeType: одно из [initial, addition, update] относительно текущего описания.',
      'changeReason: кратко (1-2 фразы), почему это initial/addition/update.',
      'importanceSignal: одно из [increase, decrease, neutral] на основе новых фактов и истории.',
      'importanceReason: кратко, почему важность нужно повысить/понизить/оставить.',
      'status (поле ответа): "ready" если данных достаточно, "need_clarification" если нужны уточнения.',
      'clarifyingQuestions: максимум 3 вопроса, только если status=need_clarification.',
      'ignoredNoise (верхний уровень): факты из текста, которые относятся к ДРУГИМ сущностям и были проигнорированы.',
      ...(['goal', 'event', 'result', 'task'].includes(entityType) ? [
        `suggestedName: полное готовое название сущности, 2-5 слов, отражающее суть. Это финальное имя — сервер использует его напрямую без изменений. Пример для цели: "Снизить арендную плату на 20%". Если данных недостаточно для осмысленного названия — верни null.`,
      ] : [
        'suggestedName: верни null (для данного типа сущности название не генерируется).',
      ]),
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
    buildEntityAnalyzerSystemPrompt,
    buildEntityAnalyzerUserPrompt,
    buildEntityAnalysisReplyText,
  };
}

module.exports = {
  createAiPrompts,
  cleanContextData,
};
