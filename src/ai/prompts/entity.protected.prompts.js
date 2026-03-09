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
      '- tags (Теги): Короткие внутренние ярлыки и темы, 1-3 слова (например: "B2B", "финтех", "автоматизация"). Не пиши сюда длинные фразы и не дублируй skills/roles.',
      '- roles (Роли): Только социальная, профессиональная или бизнес-роль (например: "предприниматель", "CEO", "арендатор"). Не записывай сюда навыки, личные качества, стиль мышления или функции вроде "аналитик по деньгам и рискам".',
      '- risks (Риски): Угрозы и уязвимости (например: "кассовый разрыв", "текучка кадров", "поломка").',
      '- skills (Навыки): Только прикладные компетенции в короткой форме 1-3 слова (например: "продажи", "переговоры", "разработка"). Без скобок, без пояснений, без длинных формулировок.',
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
      'ПРАВИЛА ДЛЯ КОРОТКИХ ПОЛЕЙ:',
      '- Для tags, skills, roles, markers: один элемент = 1-3 слова.',
      '- Без скобок, без пояснений, без двоеточий, без обрывков предложений.',
      '- Не пиши элементы вроде "маркетинг (понимание поведения клиентов)", "быстрое принятие решений", "итеративное улучшение проектов".',
      '- Если мысль длинная — сократи до короткого ярлыка или перенеси смысл в description.',
      '- Если не можешь выразить элемент коротко и чисто — лучше не включай его в поле.',
      '- tags = короткие темы и ярлыки.',
      '- skills = только практические умения.',
      '- roles = только кем сущность является.',
      '- markers = только внешний контекст (страны, города, внешняя среда).',
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
      'importance: "Высокая", если факты напрямую влияют на личный доход, доступные ресурсы, обязательства или критичные цели пользователя.',
      'importance: "Низкая" только для второстепенных фактов без прямого влияния на цели/доход/ресурсы.',
      'links: только валидные URL. Некорректные ссылки не включай.',
      'description: 3–6 предложений, ёмко и по существу.',
      'changeType: одно из [initial, addition, update] относительно текущего описания.',
      'changeReason: кратко (1-2 фразы), почему это initial/addition/update.',
      'importanceSignal: одно из [increase, decrease, neutral] на основе новых фактов и истории.',
      'importanceReason: кратко, почему важность нужно повысить/понизить/оставить.',
      'status (поле ответа): "ready" если данных достаточно, "need_clarification" если нужны уточнения.',
      'clarifyingQuestions: максимум 3 вопроса, только если status=need_clarification.',
      'ignoredNoise (верхний уровень): факты из текста, которые относятся к ДРУГИМ сущностям и были проигнорированы.',
      'Пример хорошего заполнения compact fields:',
      'tags: ["недвижимость", "IT", "автоматизация"]',
      'skills: ["переговоры", "аналитика", "маркетинг"]',
      'roles: ["предприниматель", "продуктовый менеджер"]',
      'markers: ["Казахстан", "Алматы"]',
      'Пример плохого заполнения compact fields:',
      'tags: ["быстрое принятие решений"]',
      'skills: ["маркетинг (понимание поведения клиентов)"]',
      'roles: ["аналитик по деньгам и рискам"]',
      'ФОРМАТ ОТВЕТА: верни ТОЛЬКО валидный JSON без markdown, без пояснений, без дополнительного текста вне объекта.',
      ...(entityType === 'person' ? [
        'ДОПОЛНИТЕЛЬНЫЕ ПРАВИЛА ДЛЯ PERSON:',
        '- Не превращай сильные стороны, стиль мышления и личные качества в roles.',
        '- roles: только социальные или профессиональные роли человека.',
        '- skills: только прикладные компетенции, без пояснений в скобках.',
        '- tags: короткие тематические ярлыки, не дублирующие skills.',
        '- Если пользователь сам говорит, что какая-то область НЕ является его сильнейшей стороной, не делай её центральной компетенцией.',
        '- Приоритет отдавай прямому самоописанию человека, а не красивому обобщению.',
      ] : []),
      ...(!['person', 'company'].includes(entityType) ? [
        'suggestedName: полное готовое название сущности, 1-3 слова, максимум 64 символа, отражающее суть. Это финальное имя — сервер использует его напрямую без изменений. Пример для цели: "Снизить аренду на 20%". Если данных недостаточно для осмысленного названия — верни null.',
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
