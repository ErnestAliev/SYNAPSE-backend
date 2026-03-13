const { PERSON_ENTITY_RUBRIC } = require('../rubrics/person.rubric');
const { COMPANY_ENTITY_RUBRIC } = require('../rubrics/company.rubric');
const { GOAL_ENTITY_RUBRIC } = require('../rubrics/goal.rubric');
const { RESULT_ENTITY_RUBRIC } = require('../rubrics/result.rubric');
const { RESOURCE_ENTITY_RUBRIC } = require('../rubrics/resource.rubric');
const { TASK_ENTITY_RUBRIC } = require('../rubrics/task.rubric');
const { EVENT_ENTITY_RUBRIC } = require('../rubrics/event.rubric');
const { CONNECTION_ENTITY_RUBRIC } = require('../rubrics/connection.rubric');
const { PROJECT_ENTITY_RUBRIC } = require('../rubrics/project.rubric');
const { SHAPE_ENTITY_RUBRIC } = require('../rubrics/shape.rubric');
const { PERSON_GOLD_EXAMPLES } = require('../examples/person.examples');
const { COMPANY_GOLD_EXAMPLES } = require('../examples/company.examples');
const { GOAL_GOLD_EXAMPLES } = require('../examples/goal.examples');
const { RESULT_GOLD_EXAMPLES } = require('../examples/result.examples');
const { RESOURCE_GOLD_EXAMPLES } = require('../examples/resource.examples');
const { TASK_GOLD_EXAMPLES } = require('../examples/task.examples');
const { EVENT_GOLD_EXAMPLES } = require('../examples/event.examples');
const { CONNECTION_GOLD_EXAMPLES } = require('../examples/connection.examples');
const { PROJECT_GOLD_EXAMPLES } = require('../examples/project.examples');
const { SHAPE_GOLD_EXAMPLES } = require('../examples/shape.examples');

const ENTITY_PROMPT_CONFIGS = Object.freeze({
  person: Object.freeze({
    rubric: PERSON_ENTITY_RUBRIC,
    examples: PERSON_GOLD_EXAMPLES,
  }),
  company: Object.freeze({
    rubric: COMPANY_ENTITY_RUBRIC,
    examples: COMPANY_GOLD_EXAMPLES,
  }),
  goal: Object.freeze({
    rubric: GOAL_ENTITY_RUBRIC,
    examples: GOAL_GOLD_EXAMPLES,
  }),
  result: Object.freeze({
    rubric: RESULT_ENTITY_RUBRIC,
    examples: RESULT_GOLD_EXAMPLES,
  }),
  resource: Object.freeze({
    rubric: RESOURCE_ENTITY_RUBRIC,
    examples: RESOURCE_GOLD_EXAMPLES,
  }),
  task: Object.freeze({
    rubric: TASK_ENTITY_RUBRIC,
    examples: TASK_GOLD_EXAMPLES,
  }),
  event: Object.freeze({
    rubric: EVENT_ENTITY_RUBRIC,
    examples: EVENT_GOLD_EXAMPLES,
  }),
  connection: Object.freeze({
    rubric: CONNECTION_ENTITY_RUBRIC,
    examples: CONNECTION_GOLD_EXAMPLES,
  }),
  project: Object.freeze({
    rubric: PROJECT_ENTITY_RUBRIC,
    examples: PROJECT_GOLD_EXAMPLES,
  }),
  shape: Object.freeze({
    rubric: SHAPE_ENTITY_RUBRIC,
    examples: SHAPE_GOLD_EXAMPLES,
  }),
});

function buildEntityAnalyzerBasePrompt({ entityType, allowedFields }) {
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
  ];
}

function formatBulletSection(title, items) {
  if (!Array.isArray(items) || !items.length) return [];
  return [title, ...items.map((item) => `- ${item}`), ''];
}

function buildPersonSections(config) {
  const rubric = config?.rubric;
  const examples = Array.isArray(config?.examples) ? config.examples : [];
  const importanceRules = rubric?.importanceModel?.rules || [];
  const importanceSignals = rubric?.importanceModel?.hardSignalsHigh || [];
  const descriptionStrategy = rubric?.descriptionStrategy || [];
  const fieldRules = rubric?.fieldRules || [];
  const hardMistakes = rubric?.hardMistakes || [];

  const lines = [
    'ДОПОЛНИТЕЛЬНЫЕ ПРАВИЛА ДЛЯ PERSON:',
    'Анализируй не только биографию, а реальную ценность человека для системы.',
    '',
    ...formatBulletSection('КАК ПИСАТЬ DESCRIPTION:', descriptionStrategy),
    ...formatBulletSection('КАК ОПРЕДЕЛЯТЬ IMPORTANCE ДЛЯ PERSON:', importanceRules),
    ...formatBulletSection('СИГНАЛЫ, ПРИ КОТОРЫХ IMPORTANCE НЕ ДОЛЖНА БЫТЬ НИЖЕ "ВЫСОКАЯ":', importanceSignals),
    ...formatBulletSection('ПРАВИЛА ДЛЯ ПОЛЕЙ PERSON:', fieldRules),
    ...formatBulletSection('КРИТИЧЕСКИЕ ОШИБКИ ДЛЯ PERSON:', hardMistakes),
  ];

  if (examples.length) {
    lines.push('ЭТАЛОННЫЕ МИНИ-ПРИМЕРЫ ДЛЯ PERSON:');
    for (const example of examples) {
      if (!example || typeof example !== 'object') continue;
      lines.push(`- Вход: ${example.input}`);
      lines.push(`  Выход: description="${example.output.description}"`);
      lines.push(`  importance="${example.output.importance}"`);
      lines.push(`  roles=${JSON.stringify(example.output.roles || [])}`);
      lines.push(`  skills=${JSON.stringify(example.output.skills || [])}`);
      lines.push(`  tags=${JSON.stringify(example.output.tags || [])}`);
    }
    lines.push('');
  }

  return lines;
}

function buildEntitySpecificSections(entityType, config) {
  const rubric = config?.rubric;
  const examples = Array.isArray(config?.examples) ? config.examples : [];
  const title = String(entityType || '').toUpperCase();
  const importanceRules = rubric?.importanceModel?.rules || [];
  const importanceSignals = rubric?.importanceModel?.hardSignalsHigh || [];
  const descriptionStrategy = rubric?.descriptionStrategy || [];
  const fieldRules = rubric?.fieldRules || [];
  const hardMistakes = rubric?.hardMistakes || [];

  const lines = [
    `ДОПОЛНИТЕЛЬНЫЕ ПРАВИЛА ДЛЯ ${title}:`,
    `Анализируй ${entityType} не формально, а через его реальную ценность для системы.`,
    '',
    ...formatBulletSection('КАК ПИСАТЬ DESCRIPTION:', descriptionStrategy),
    ...formatBulletSection(`КАК ОПРЕДЕЛЯТЬ IMPORTANCE ДЛЯ ${title}:`, importanceRules),
    ...formatBulletSection('СИГНАЛЫ, ПРИ КОТОРЫХ IMPORTANCE НЕ ДОЛЖНА БЫТЬ НИЖЕ "ВЫСОКАЯ":', importanceSignals),
    ...formatBulletSection(`ПРАВИЛА ДЛЯ ПОЛЕЙ ${title}:`, fieldRules),
    ...formatBulletSection(`КРИТИЧЕСКИЕ ОШИБКИ ДЛЯ ${title}:`, hardMistakes),
  ];

  if (examples.length) {
    lines.push(`ЭТАЛОННЫЕ МИНИ-ПРИМЕРЫ ДЛЯ ${title}:`);
    for (const example of examples) {
      if (!example || typeof example !== 'object') continue;
      lines.push(`- Вход: ${example.input}`);
      lines.push(`  Выход: description="${example.output.description}"`);
      lines.push(`  importance="${example.output.importance}"`);
      const exampleFields = Object.entries(example.output)
        .filter(([key, value]) => !['description', 'importance'].includes(key) && Array.isArray(value));
      for (const [key, value] of exampleFields) {
        lines.push(`  ${key}=${JSON.stringify(value)}`);
      }
    }
    lines.push('');
  }

  return lines;
}

function buildEntityTypeSpecificSections(entityType) {
  const config = ENTITY_PROMPT_CONFIGS[entityType];
  if (!config) return [];

  if (entityType === 'person') {
    return buildPersonSections(config);
  }

  return buildEntitySpecificSections(entityType, config);
}

function buildSuggestedNameSection(entityType) {
  if (!['person', 'company'].includes(entityType)) {
    return [
      'suggestedName: полное готовое название сущности, 1-3 слова, максимум 64 символа, отражающее суть. Это финальное имя — сервер использует его напрямую без изменений. Пример для цели: "Снизить аренду на 20%". Если данных недостаточно для осмысленного названия — верни null.',
    ];
  }

  return ['suggestedName: верни null (для данного типа сущности название не генерируется).'];
}

function buildEntityAnalyzerSystemPrompt({ entityType, allowedFields }) {
  return [
    ...buildEntityAnalyzerBasePrompt({ entityType, allowedFields }),
    ...buildEntityTypeSpecificSections(entityType),
    ...buildSuggestedNameSection(entityType),
    'ФОРМАТ ОТВЕТА: верни ТОЛЬКО валидный JSON без markdown, без пояснений, без дополнительного текста вне объекта.',
  ].join('\n');
}

module.exports = {
  buildEntityAnalyzerSystemPrompt,
};
