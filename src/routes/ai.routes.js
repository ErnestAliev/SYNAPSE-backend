const express = require('express');

function createAiRouter(deps) {
  const {
    requireAuth,
    requireOwnerId,
    toTrimmedString,
    toProfile,
    AI_DEBUG_ECHO,
    OPENAI_MODEL,
    OPENAI_PROJECT_MODEL,
    OPENAI_ROUTER_MODEL,
    OPENAI_DEEP_MODEL,
    OPENAI_QUIZ_FAST_MODEL,
    OPENAI_QUIZ_SMART_MODEL,
    Entity,
    resolveAgentScopeContext,
    buildEntityAnalyzerCurrentFields,
    extractJsonObjectFromText,
    normalizeEntityAnalysisOutput,
    buildEntityMetadataPatch,
    upsertEntityVector,
    broadcastEntityEvent,
    AgentChatHistory,
    entityTypes,
    aiPrompts,
    aiAttachments,
    aiProvider,
  } = deps;

  const router = express.Router();
  const AGENT_CHAT_HISTORY_MESSAGE_LIMIT = Math.max(20, Number(deps.AGENT_CHAT_HISTORY_MESSAGE_LIMIT) || 140);
  const AGENT_CHAT_HISTORY_ATTACHMENT_LIMIT = Math.max(0, Number(deps.AGENT_CHAT_HISTORY_ATTACHMENT_LIMIT) || 6);
  const AGENT_CHAT_HISTORY_ATTACHMENT_DATA_MAX_LENGTH = Math.max(
    2000,
    Number(deps.AGENT_CHAT_HISTORY_ATTACHMENT_DATA_MAX_LENGTH) || 320000,
  );
  const AGENT_CHAT_HISTORY_TEXT_MAX_LENGTH = Math.max(
    400,
    Number(deps.AGENT_CHAT_HISTORY_TEXT_MAX_LENGTH) || 12000,
  );
  const AGENT_CHAT_SCOPE_TYPES = new Set(['collection', 'project']);
  const AGENT_CHAT_ENTITY_TYPES = new Set(Array.isArray(entityTypes) ? entityTypes : []);
  const PROJECT_CHAT_FIELD_CONFIGS = Object.freeze({
    tags: { maxItems: 40, itemMaxLength: 64 },
    markers: { maxItems: 40, itemMaxLength: 64 },
    roles: { maxItems: 36, itemMaxLength: 64 },
    skills: { maxItems: 36, itemMaxLength: 64 },
    risks: { maxItems: 36, itemMaxLength: 96 },
    priority: { maxItems: 18, itemMaxLength: 64 },
    status: { maxItems: 24, itemMaxLength: 64 },
    tasks: { maxItems: 36, itemMaxLength: 120 },
    metrics: { maxItems: 28, itemMaxLength: 96 },
    owners: { maxItems: 28, itemMaxLength: 64 },
    participants: { maxItems: 36, itemMaxLength: 64 },
    resources: { maxItems: 36, itemMaxLength: 96 },
    outcomes: { maxItems: 36, itemMaxLength: 96 },
    industry: { maxItems: 24, itemMaxLength: 64 },
    departments: { maxItems: 24, itemMaxLength: 64 },
    stage: { maxItems: 18, itemMaxLength: 64 },
    date: { maxItems: 24, itemMaxLength: 64 },
    location: { maxItems: 24, itemMaxLength: 64 },
    phones: { maxItems: 28, itemMaxLength: 40 },
    links: { maxItems: 24, itemMaxLength: 240 },
    importance: { maxItems: 1, itemMaxLength: 24 },
    ignoredNoise: { maxItems: 40, itemMaxLength: 120 },
  });
  const PROJECT_CHAT_FIELD_KEYS = Object.freeze(Object.keys(PROJECT_CHAT_FIELD_CONFIGS));
  const QUIZ_ALLOWED_MODES = new Set(['quiz_step', 'quiz_stop_check']);
  const QUIZ_MIN_LEVEL1_QUESTIONS = 7;
  const QUIZ_MAX_LEVEL1_QUESTIONS = 10;
  const QUIZ_MAX_LEVEL2_QUESTIONS = 12;
  const QUIZ_HISTORY_LIMIT = 60;
  const QUIZ_FIELDS_PATCH_ALLOWED = new Set([
    'tagsAdd',
    'markersAdd',
    'rolesAdd',
    'skillsAdd',
    'risksAdd',
    'priorityAdd',
    'statusAdd',
    'tasksAdd',
    'metricsAdd',
    'ownersAdd',
    'participantsAdd',
    'resourcesAdd',
    'outcomesAdd',
    'industryAdd',
    'departmentsAdd',
    'stageAdd',
    'dateAdd',
    'locationAdd',
    'phonesAdd',
    'linksAdd',
    'importanceAdd',
    'ignoredNoiseAdd',
  ]);
  const QUIZ_FIRST_QUESTION_BY_TYPE = Object.freeze({
    person: {
      questionText: '{name} для тебя — это…',
      options: [
        { id: '1', text: 'Дело (клиент/партнёр/конкурент)' },
        { id: '2', text: 'Личное (семья/друг/отношения)' },
        { id: '3', text: 'Контакт (знакомый/нетворк)' },
        { id: '4', text: 'Свой вариант' },
      ],
    },
    company: {
      questionText: '{name} для тебя — это…',
      options: [
        { id: '1', text: 'Мы с ними работаем (есть/были сделки, деньги)' },
        { id: '2', text: 'Я хочу с ними работать (потенциальный клиент/партнёр/поставщик/работодатель)' },
        { id: '3', text: 'Они мне мешают (конкурент/конфликт/проблема)' },
        { id: '4', text: 'Свой вариант' },
      ],
    },
    project: {
      questionText: 'Проект “{name}” тебе нужен, чтобы…',
      options: [
        { id: '1', text: 'Заработать' },
        { id: '2', text: 'Сделать продукт/сервис' },
        { id: '3', text: 'Навести порядок/систему' },
        { id: '4', text: 'Свой вариант' },
      ],
    },
    event: {
      questionText: '{name} для тебя — это…',
      options: [
        { id: '1', text: 'Встреча/разговор' },
        { id: '2', text: 'Публичная штука (ивент/выступление/медиа)' },
        { id: '3', text: 'Личное (семья/друзья)' },
        { id: '4', text: 'Свой вариант' },
      ],
    },
    resource: {
      questionText: '{name} для тебя — это…',
      options: [
        { id: '1', text: 'Деньги (счёт, наличка, бюджет)' },
        { id: '2', text: 'Канал/аудитория (соцсеть, база, медиа)' },
        { id: '3', text: 'Объект/имущество (помещение, недвижимость, техника, авто)' },
        { id: '4', text: 'Свой вариант' },
      ],
    },
    goal: {
      questionText: '{name} для тебя — это…',
      options: [
        { id: '1', text: 'Деньги' },
        { id: '2', text: 'Рост' },
        { id: '3', text: 'Порядок/контроль' },
        { id: '4', text: 'Свой вариант' },
      ],
    },
    result: {
      questionText: '{name} для тебя — это…',
      options: [
        { id: '1', text: 'Цифра/деньги' },
        { id: '2', text: 'Факт/достижение' },
        { id: '3', text: 'Артефакт (документ/файл/продукт)' },
        { id: '4', text: 'Свой вариант' },
      ],
    },
    task: {
      questionText: '{name} для тебя — это…',
      options: [
        { id: '1', text: 'Рутина' },
        { id: '2', text: 'Проектная штука (сделать один раз)' },
        { id: '3', text: 'Решение (выбор/согласование)' },
        { id: '4', text: 'Свой вариант' },
      ],
    },
    shape: {
      questionText: '{name} — ты это оставляешь на доске как…',
      options: [
        { id: '1', text: 'Мысль/набросок (ещё не оформлено)' },
        { id: '2', text: 'Вопрос/неясность (надо разобраться)' },
        { id: '3', text: 'Напоминание/якорь (держать в фокусе)' },
        { id: '4', text: 'Свой вариант' },
      ],
    },
    connection: {
      questionText: '{name} для тебя — это…',
      options: [
        { id: '1', text: 'Контакт по делу' },
        { id: '2', text: 'Контакт по личным вопросам' },
        { id: '3', text: 'Контакт для нетворка' },
        { id: '4', text: 'Свой вариант' },
      ],
    },
  });
  const QUIZ_LEVEL1_BANK_BY_TYPE = Object.freeze({
    person: [
      {
        questionId: 'P2',
        questionText: '{name} для тебя полезен?',
        options: ['Да', 'Нет', 'Не уверен', 'Свой вариант'],
        updatesHints: ['statusAdd', 'importanceAdd'],
      },
      {
        questionId: 'P3',
        questionText: 'Главная ценность от {name} — это…',
        options: ['Деньги/сделки', 'Доступ/знакомства', 'Экспертиза/помощь', 'Свой вариант'],
        updatesHints: ['tagsAdd', 'rolesAdd'],
      },
      {
        questionId: 'P4',
        questionText: 'Как вы связаны?',
        options: ['Прямое общение', 'Через людей/интро', 'Почти не общаемся', 'Свой вариант'],
        updatesHints: ['markersAdd', 'statusAdd'],
      },
      {
        questionId: 'P5',
        questionText: 'Уровень доверия к {name} — это…',
        options: ['Высокий', 'Средний', 'Низкий', 'Свой вариант'],
        updatesHints: ['importanceAdd', 'risksAdd'],
      },
      {
        questionId: 'P6',
        questionText: 'Есть ‘красный флаг’?',
        options: ['Нет', 'Есть', 'Не знаю', 'Свой вариант'],
        updatesHints: ['risksAdd', 'markersAdd'],
      },
      {
        questionId: 'P7',
        questionText: 'Что ты хочешь от {name}?',
        options: ['Укрепить связь', 'Получить пользу/сделку', 'Дистанция', 'Свой вариант'],
        updatesHints: ['tasksAdd', 'priorityAdd'],
      },
      {
        questionId: 'P8',
        questionText: 'Лучший следующий шаг — это…',
        options: ['Сообщение/созвон', 'Встреча/разговор', 'Ничего', 'Свой вариант'],
        updatesHints: ['tasksAdd', 'statusAdd'],
      },
    ],
    company: [
      {
        questionId: 'C2',
        questionText: 'Это про…',
        options: ['Они платят мне', 'Я плачу им', 'Мы делаем вместе', 'Свой вариант'],
        updatesHints: ['rolesAdd', 'tagsAdd'],
      },
      {
        questionId: 'C3',
        questionText: 'С ними легко работать?',
        options: ['Да', 'Средне', 'Нет', 'Свой вариант'],
        updatesHints: ['statusAdd', 'risksAdd'],
      },
      {
        questionId: 'C4',
        questionText: 'Главная польза от {name} — это…',
        options: ['Деньги/контракты', 'Доступ к рынку/людям', 'Ресурс/инфраструктура', 'Свой вариант'],
        updatesHints: ['tagsAdd', 'resourcesAdd'],
      },
      {
        questionId: 'C5',
        questionText: 'Главный риск с {name} — это…',
        options: ['Юридика/договоры', 'Деньги/платежи', 'Репутация/конфликты', 'Свой вариант'],
        updatesHints: ['risksAdd'],
      },
      {
        questionId: 'C6',
        questionText: 'Уровень доверия к компании — это…',
        options: ['Высокий', 'Средний', 'Низкий', 'Свой вариант'],
        updatesHints: ['importanceAdd', 'statusAdd'],
      },
      {
        questionId: 'C7',
        questionText: 'Что ты хочешь сделать с {name}?',
        options: ['Закрепить', 'Начать/попробовать', 'Избежать', 'Свой вариант'],
        updatesHints: ['tasksAdd', 'priorityAdd'],
      },
      {
        questionId: 'C8',
        questionText: 'Следующий шаг — это…',
        options: ['КП/письмо', 'Встреча/созвон', 'Пауза', 'Свой вариант'],
        updatesHints: ['tasksAdd', 'statusAdd'],
      },
    ],
    project: [
      {
        questionId: 'PR2',
        questionText: 'В чём успех проекта выражается?',
        options: ['Деньги/прибыль', 'Запуск/результат', 'Система/процесс', 'Свой вариант'],
        updatesHints: ['metricsAdd', 'outcomesAdd'],
      },
      {
        questionId: 'PR3',
        questionText: 'Проект реально важный?',
        options: ['Да', 'Средне', 'Нет', 'Свой вариант'],
        updatesHints: ['importanceAdd', 'priorityAdd'],
      },
      {
        questionId: 'PR4',
        questionText: 'Главный тормоз проекта — это…',
        options: ['Нет ясности', 'Нет ресурса/людей', 'Нет решения', 'Свой вариант'],
        updatesHints: ['risksAdd', 'resourcesAdd'],
      },
      {
        questionId: 'PR5',
        questionText: 'Главный риск проекта — это…',
        options: ['Деньги/перерасход', 'Сроки/провал', 'Люди/исполнение', 'Свой вариант'],
        updatesHints: ['risksAdd'],
      },
      {
        questionId: 'PR6',
        questionText: 'Что нужно, чтобы проект ожил?',
        options: ['1 решение', '1 человек/ресурс', '1 шаг', 'Свой вариант'],
        updatesHints: ['tasksAdd', 'resourcesAdd'],
      },
      {
        questionId: 'PR7',
        questionText: 'Следующий шаг по проекту — это…',
        options: ['План/задачи', 'Найти исполнителя/партнёра', 'Заморозить', 'Свой вариант'],
        updatesHints: ['tasksAdd', 'statusAdd'],
      },
    ],
    event: [
      {
        questionId: 'E2',
        questionText: 'Это событие полезное?',
        options: ['Да', 'Нет', 'Не уверен', 'Свой вариант'],
        updatesHints: ['importanceAdd', 'statusAdd'],
      },
      {
        questionId: 'E3',
        questionText: 'Главная цель события — это…',
        options: ['Договориться/решить', 'Нетворк', 'Имидж/контент', 'Свой вариант'],
        updatesHints: ['tagsAdd', 'outcomesAdd'],
      },
      {
        questionId: 'E4',
        questionText: 'Главный результат события — это…',
        options: ['Решение/договорённость', 'Контакт/интро', 'Материал/контент', 'Свой вариант'],
        updatesHints: ['outcomesAdd', 'participantsAdd'],
      },
      {
        questionId: 'E5',
        questionText: 'Есть риск?',
        options: ['Нет', 'Есть', 'Не знаю', 'Свой вариант'],
        updatesHints: ['risksAdd'],
      },
      {
        questionId: 'E6',
        questionText: 'Следующий шаг — это…',
        options: ['Подготовка тем/вопросов', 'Организация', 'Ничего', 'Свой вариант'],
        updatesHints: ['tasksAdd'],
      },
    ],
    resource: [
      {
        questionId: 'R2',
        questionText: 'Этот ресурс приносит пользу?',
        options: ['Да', 'Нет', 'Не уверен', 'Свой вариант'],
        updatesHints: ['importanceAdd', 'statusAdd'],
      },
      {
        questionId: 'R3',
        questionText: 'Польза ресурса — это…',
        options: ['Доход/экономия', 'Ускорение работы', 'Доступ/возможности', 'Свой вариант'],
        updatesHints: ['tagsAdd', 'metricsAdd'],
      },
      {
        questionId: 'R4',
        questionText: 'Основной риск ресурса — это…',
        options: ['Потери денег', 'Неиспользование', 'Юридика/ограничения', 'Свой вариант'],
        updatesHints: ['risksAdd'],
      },
      {
        questionId: 'R5',
        questionText: 'Этот ресурс нужно…',
        options: ['Увеличить', 'Сохранить', 'Убрать/продать/закрыть', 'Свой вариант'],
        updatesHints: ['statusAdd', 'tasksAdd'],
      },
      {
        questionId: 'R6',
        questionText: 'Следующий шаг — это…',
        options: ['Оценить цифрами', 'Проверить условия', 'Ничего', 'Свой вариант'],
        updatesHints: ['tasksAdd', 'metricsAdd'],
      },
    ],
    goal: [
      {
        questionId: 'G2',
        questionText: 'Эта цель реально важна?',
        options: ['Да', 'Средне', 'Нет', 'Свой вариант'],
        updatesHints: ['priorityAdd', 'importanceAdd'],
      },
      {
        questionId: 'G3',
        questionText: 'Как поймёшь, что цель достигнута?',
        options: ['Цифра/сумма', 'Факт', 'Стабильная система', 'Свой вариант'],
        updatesHints: ['metricsAdd', 'outcomesAdd'],
      },
      {
        questionId: 'G4',
        questionText: 'Главный барьер — это…',
        options: ['Нет плана', 'Нет ресурса', 'Нет дисциплины', 'Свой вариант'],
        updatesHints: ['risksAdd', 'resourcesAdd'],
      },
      {
        questionId: 'G5',
        questionText: 'Следующий шаг — это…',
        options: ['1 метрика', '1 действие', 'Отложить', 'Свой вариант'],
        updatesHints: ['tasksAdd'],
      },
    ],
    result: [
      {
        questionId: 'RS2',
        questionText: 'Это результат хороший?',
        options: ['Да', 'Нет', 'Не уверен', 'Свой вариант'],
        updatesHints: ['importanceAdd', 'statusAdd'],
      },
      {
        questionId: 'RS3',
        questionText: 'Что этот результат меняет?',
        options: ['Деньги', 'Доступ/возможности', 'Порядок/процесс', 'Свой вариант'],
        updatesHints: ['outcomesAdd', 'metricsAdd'],
      },
      {
        questionId: 'RS4',
        questionText: 'Следующий шаг после результата — это…',
        options: ['Зафиксировать', 'Повторить/масштабировать', 'Закрыть тему', 'Свой вариант'],
        updatesHints: ['tasksAdd', 'statusAdd'],
      },
    ],
    task: [
      {
        questionId: 'T2',
        questionText: 'Эта задача приносит пользу?',
        options: ['Да', 'Нет', 'Не уверен', 'Свой вариант'],
        updatesHints: ['importanceAdd', 'statusAdd'],
      },
      {
        questionId: 'T3',
        questionText: 'Цена ошибки здесь — это…',
        options: ['Деньги', 'Репутация/отношения', 'Время/фокус', 'Свой вариант'],
        updatesHints: ['risksAdd', 'metricsAdd'],
      },
      {
        questionId: 'T4',
        questionText: 'Кто должен сделать?',
        options: ['Я', 'Кто-то другой', 'Вместе', 'Свой вариант'],
        updatesHints: ['ownersAdd', 'participantsAdd'],
      },
      {
        questionId: 'T5',
        questionText: 'Что мешает закрыть?',
        options: ['Нет ясности', 'Нет ресурса', 'Нет решения', 'Свой вариант'],
        updatesHints: ['risksAdd'],
      },
      {
        questionId: 'T6',
        questionText: 'Следующий шаг — это…',
        options: ['1 действие', 'Поставить человеку', 'Удалить/не делать', 'Свой вариант'],
        updatesHints: ['tasksAdd', 'statusAdd'],
      },
    ],
    shape: [
      {
        questionId: 'S2',
        questionText: 'Это важно?',
        options: ['Да', 'Средне', 'Нет', 'Свой вариант'],
        updatesHints: ['importanceAdd', 'priorityAdd'],
      },
      {
        questionId: 'S3',
        questionText: 'Что ты хочешь получить на выходе?',
        options: ['Понять/решить', 'Сформулировать', 'Запомнить', 'Свой вариант'],
        updatesHints: ['tagsAdd', 'outcomesAdd'],
      },
      {
        questionId: 'S4',
        questionText: 'Следующий шаг — это…',
        options: ['Уточнить 1 факт', 'Свести в одну фразу', 'Оставить как есть', 'Свой вариант'],
        updatesHints: ['tasksAdd', 'statusAdd'],
      },
    ],
    connection: [
      {
        questionId: 'P2',
        questionText: '{name} для тебя полезен?',
        options: ['Да', 'Нет', 'Не уверен', 'Свой вариант'],
        updatesHints: ['statusAdd', 'importanceAdd'],
      },
      {
        questionId: 'P3',
        questionText: 'Главная ценность от {name} — это…',
        options: ['Деньги/сделки', 'Доступ/знакомства', 'Экспертиза/помощь', 'Свой вариант'],
        updatesHints: ['tagsAdd', 'rolesAdd'],
      },
      {
        questionId: 'P4',
        questionText: 'Как вы связаны?',
        options: ['Прямое общение', 'Через людей/интро', 'Почти не общаемся', 'Свой вариант'],
        updatesHints: ['markersAdd', 'statusAdd'],
      },
      {
        questionId: 'P5',
        questionText: 'Уровень доверия к {name} — это…',
        options: ['Высокий', 'Средний', 'Низкий', 'Свой вариант'],
        updatesHints: ['importanceAdd', 'risksAdd'],
      },
      {
        questionId: 'P6',
        questionText: 'Есть ‘красный флаг’?',
        options: ['Нет', 'Есть', 'Не знаю', 'Свой вариант'],
        updatesHints: ['risksAdd', 'markersAdd'],
      },
      {
        questionId: 'P7',
        questionText: 'Что ты хочешь от {name}?',
        options: ['Укрепить связь', 'Получить пользу/сделку', 'Дистанция', 'Свой вариант'],
        updatesHints: ['tasksAdd', 'priorityAdd'],
      },
      {
        questionId: 'P8',
        questionText: 'Лучший следующий шаг — это…',
        options: ['Сообщение/созвон', 'Встреча/разговор', 'Ничего', 'Свой вариант'],
        updatesHints: ['tasksAdd', 'statusAdd'],
      },
    ],
  });

  function normalizeScope(rawScope) {
    const scope = toProfile(rawScope);
    const scopeType = toTrimmedString(scope.type, 24).toLowerCase();
    if (!AGENT_CHAT_SCOPE_TYPES.has(scopeType)) {
      throw Object.assign(new Error('Invalid chat scope type'), { status: 400 });
    }

    if (scopeType === 'collection') {
      const entityType = toTrimmedString(scope.entityType, 24).toLowerCase();
      if (!entityType || !AGENT_CHAT_ENTITY_TYPES.has(entityType)) {
        throw Object.assign(new Error('Invalid collection entity type'), { status: 400 });
      }

      return {
        type: 'collection',
        entityType,
        projectId: '',
        scopeKey: `collection:${entityType}`,
      };
    }

    const projectId = toTrimmedString(scope.projectId, 80);
    if (!projectId) {
      throw Object.assign(new Error('projectId is required for project scope'), { status: 400 });
    }

    return {
      type: 'project',
      entityType: '',
      projectId,
      scopeKey: `project-canvas:${projectId}`,
    };
  }

  function normalizeAttachment(rawAttachment) {
    const attachment = toProfile(rawAttachment);
    const id = toTrimmedString(attachment.id, 120) || `att_${Date.now()}_${Math.floor(Math.random() * 100000)}`;
    const name = toTrimmedString(attachment.name, 240) || 'Файл';
    const mime = toTrimmedString(attachment.mime, 180);
    const sizeRaw = Number(attachment.size);
    const size = Number.isFinite(sizeRaw) ? Math.max(0, Math.floor(sizeRaw)) : 0;
    const data = toTrimmedString(attachment.data, AGENT_CHAT_HISTORY_ATTACHMENT_DATA_MAX_LENGTH);

    if (!data && !name) return null;

    return {
      id,
      name,
      mime,
      size,
      data,
    };
  }

  function normalizeMessage(rawMessage) {
    const message = toProfile(rawMessage);
    const id = toTrimmedString(message.id, 120) || `msg_${Date.now()}_${Math.floor(Math.random() * 100000)}`;
    const role = toTrimmedString(message.role, 24) === 'assistant' ? 'assistant' : 'user';
    const text = toTrimmedString(message.text, AGENT_CHAT_HISTORY_TEXT_MAX_LENGTH);
    const createdAtRaw = toTrimmedString(message.createdAt, 80);
    const parsedCreatedAt = Date.parse(createdAtRaw);
    const createdAt = Number.isFinite(parsedCreatedAt) ? new Date(parsedCreatedAt) : new Date();
    const rawAttachments = Array.isArray(message.attachments) ? message.attachments : [];
    const attachments = rawAttachments
      .slice(0, AGENT_CHAT_HISTORY_ATTACHMENT_LIMIT)
      .map((item) => normalizeAttachment(item))
      .filter((item) => Boolean(item));

    if (!text && !attachments.length) {
      return null;
    }

    return {
      id,
      role,
      text,
      createdAt,
      attachments,
    };
  }

  function normalizeMessages(rawMessages) {
    const source = Array.isArray(rawMessages) ? rawMessages : [];
    const dedup = new Set();
    const normalized = [];

    for (const item of source) {
      const nextMessage = normalizeMessage(item);
      if (!nextMessage) continue;
      if (dedup.has(nextMessage.id)) continue;
      dedup.add(nextMessage.id);
      normalized.push(nextMessage);
    }

    normalized.sort((a, b) => a.createdAt.getTime() - b.createdAt.getTime());
    return normalized.slice(-AGENT_CHAT_HISTORY_MESSAGE_LIMIT);
  }

  function normalizeMarkerList(rawValues, maxItems = 6) {
    const source = Array.isArray(rawValues) ? rawValues : [];
    const dedup = new Set();
    const result = [];

    for (const item of source) {
      const value = toTrimmedString(item, 64);
      if (!value) continue;
      const key = value.toLowerCase();
      if (dedup.has(key)) continue;
      dedup.add(key);
      result.push(value);
      if (result.length >= maxItems) break;
    }

    return result;
  }

  function ensureAnalysisMarkers(analysis) {
    if (!analysis || typeof analysis !== 'object') return analysis;
    if (analysis.status !== 'ready') return analysis;

    const fields = toProfile(analysis.fields);
    const currentMarkers = normalizeMarkerList(fields.markers);
    if (currentMarkers.length) {
      return {
        ...analysis,
        fields: {
          ...fields,
          markers: currentMarkers,
        },
      };
    }

    const fallbackKeys = [
      'risks',
      'status',
      'stage',
      'priority',
      'outcomes',
      'owners',
      'industry',
      'departments',
      'resources',
      'metrics',
      'participants',
      'location',
      'date',
      'roles',
      'skills',
      'phones',
      'tags',
    ];

    const fallback = [];
    for (const key of fallbackKeys) {
      const values = normalizeMarkerList(fields[key], 6);
      for (const value of values) {
        if (fallback.includes(value)) continue;
        fallback.push(value);
        if (fallback.length >= 6) break;
      }
      if (fallback.length >= 6) break;
    }

    if (!fallback.length) {
      fallback.push('Контекст требует уточнения');
    }

    return {
      ...analysis,
      fields: {
        ...fields,
        markers: fallback,
      },
    };
  }

  function normalizeProjectImportanceValues(rawValue) {
    const source = Array.isArray(rawValue) ? rawValue : [rawValue];
    const normalized = source
      .map((item) => toTrimmedString(item, 32).toLowerCase())
      .filter(Boolean)
      .map((item) => {
        if (item === 'низкая' || item === 'low' || item === 'l') return 'Низкая';
        if (item === 'средняя' || item === 'medium' || item === 'med' || item === 'm') return 'Средняя';
        if (item === 'высокая' || item === 'high' || item === 'h' || item === 'critical' || item === 'критично') {
          return 'Высокая';
        }
        return '';
      })
      .filter(Boolean);

    return normalized.length ? [normalized[0]] : [];
  }

  function normalizeProjectLinkValue(rawValue) {
    const value = toTrimmedString(rawValue, 240);
    if (!value) return '';

    const withProtocol = /^https?:\/\//i.test(value) ? value : `https://${value}`;
    try {
      const url = new URL(withProtocol);
      if (!url.hostname || !url.protocol.startsWith('http')) return '';
      return url.toString();
    } catch {
      return '';
    }
  }

  function normalizeProjectFieldArray(fieldKey, rawValue) {
    if (!PROJECT_CHAT_FIELD_CONFIGS[fieldKey]) return [];

    if (fieldKey === 'importance') {
      return normalizeProjectImportanceValues(rawValue);
    }

    const { maxItems, itemMaxLength } = PROJECT_CHAT_FIELD_CONFIGS[fieldKey];
    const source = Array.isArray(rawValue) ? rawValue : [rawValue];
    const dedup = new Set();
    const normalized = [];

    for (const item of source) {
      const value =
        fieldKey === 'links'
          ? normalizeProjectLinkValue(item)
          : toTrimmedString(item, itemMaxLength);
      if (!value) continue;
      const key = value.toLowerCase();
      if (dedup.has(key)) continue;
      dedup.add(key);
      normalized.push(value);
      if (normalized.length >= maxItems) break;
    }

    return normalized;
  }

  function createEmptyProjectFieldMap() {
    const map = {};
    for (const fieldKey of PROJECT_CHAT_FIELD_KEYS) {
      map[fieldKey] = [];
    }
    return map;
  }

  function mergeProjectFieldValues(fieldKey, ...lists) {
    const dedup = new Set();
    const merged = [];
    const maxItems = PROJECT_CHAT_FIELD_CONFIGS[fieldKey]?.maxItems || 24;

    for (const list of lists) {
      const normalized = normalizeProjectFieldArray(fieldKey, list);
      for (const value of normalized) {
        const key = value.toLowerCase();
        if (dedup.has(key)) continue;
        dedup.add(key);
        merged.push(value);
        if (merged.length >= maxItems) return merged;
      }
    }

    return merged;
  }

  function buildProjectFieldMapFromMetadata(aiMetadata) {
    const metadata = toProfile(aiMetadata);
    const fieldMap = createEmptyProjectFieldMap();
    for (const fieldKey of PROJECT_CHAT_FIELD_KEYS) {
      fieldMap[fieldKey] = normalizeProjectFieldArray(fieldKey, metadata[fieldKey]);
    }
    return fieldMap;
  }

  function buildProjectEntityAggregatedFields(entities) {
    const fieldMap = createEmptyProjectFieldMap();
    const source = Array.isArray(entities) ? entities : [];

    for (const entity of source) {
      const metadata = toProfile(entity?.ai_metadata);
      for (const fieldKey of PROJECT_CHAT_FIELD_KEYS) {
        fieldMap[fieldKey] = mergeProjectFieldValues(fieldKey, fieldMap[fieldKey], metadata[fieldKey]);
      }
    }

    return fieldMap;
  }

  function mergeProjectFieldMaps(...maps) {
    const merged = createEmptyProjectFieldMap();
    for (const fieldKey of PROJECT_CHAT_FIELD_KEYS) {
      const lists = maps.map((map) => (map && typeof map === 'object' ? map[fieldKey] : []));
      merged[fieldKey] = mergeProjectFieldValues(fieldKey, ...lists);
    }
    return merged;
  }

  function normalizeProjectEnrichmentOutput(rawResponse) {
    const parsed = toProfile(rawResponse);
    const status = toTrimmedString(parsed.status, 32) === 'need_clarification' ? 'need_clarification' : 'ready';
    const summary = toTrimmedString(parsed.summary || parsed.description, 2200);
    const changeReason = toTrimmedString(parsed.changeReason, 240);
    const fieldsSource = toProfile(parsed.fields);
    const fields = createEmptyProjectFieldMap();

    for (const fieldKey of PROJECT_CHAT_FIELD_KEYS) {
      fields[fieldKey] = normalizeProjectFieldArray(fieldKey, fieldsSource[fieldKey]);
    }

    const ignoredNoiseFallback = normalizeProjectFieldArray('ignoredNoise', parsed.ignoredNoise);
    if (ignoredNoiseFallback.length) {
      fields.ignoredNoise = mergeProjectFieldValues('ignoredNoise', fields.ignoredNoise, ignoredNoiseFallback);
    }

    const clarifyingQuestions = (Array.isArray(parsed.clarifyingQuestions) ? parsed.clarifyingQuestions : [])
      .map((item) => toTrimmedString(item, 220))
      .filter(Boolean)
      .slice(0, 3);

    return {
      status,
      summary,
      changeReason,
      fields,
      clarifyingQuestions,
    };
  }

  async function runProjectChatAutoEnrichment({
    ownerId,
    scopeContext,
    contextData,
    message,
    history,
    assistantReply,
    includeDebug,
  }) {
    if (!scopeContext || scopeContext.scopeType !== 'project') {
      return null;
    }

    const projectId = toTrimmedString(scopeContext.projectId, 80);
    if (!projectId) {
      return null;
    }

    const projectEntity = await Entity.findOne({
      _id: projectId,
      owner_id: ownerId,
      type: 'project',
    });
    if (!projectEntity) {
      return null;
    }

    const currentProjectFields = buildProjectFieldMapFromMetadata(projectEntity.ai_metadata);
    const aggregatedEntityFields = buildProjectEntityAggregatedFields(scopeContext.entities);
    const systemPrompt = aiPrompts.buildProjectEnrichmentSystemPrompt();
    const userPrompt = aiPrompts.buildProjectEnrichmentUserPrompt({
      contextData,
      message,
      assistantReply,
      history,
      currentProjectFields,
      aggregatedEntityFields,
    });

    const enrichmentResponse = await aiProvider.requestOpenAiAgentReply({
      systemPrompt,
      userPrompt,
      includeRawPayload: includeDebug,
      model: OPENAI_MODEL,
      temperature: 0.2,
      maxOutputTokens: 2200,
    });

    const parsed = extractJsonObjectFromText(enrichmentResponse.reply);
    const enrichment = normalizeProjectEnrichmentOutput(parsed);
    const mergedFields = mergeProjectFieldMaps(currentProjectFields, aggregatedEntityFields, enrichment.fields);
    const existingMetadata = toProfile(projectEntity.ai_metadata);
    const existingDescription = toTrimmedString(existingMetadata.description, 2200);
    const nextDescription = enrichment.summary || existingDescription;

    const analysisForPatch = {
      status: 'ready',
      description: nextDescription,
      changeType: enrichment.summary ? 'addition' : '',
      changeReason: enrichment.changeReason || 'project_chat_auto_enrichment',
      fields: mergedFields,
      importanceSignal: '',
      importanceReason: '',
      clarifyingQuestions: enrichment.clarifyingQuestions,
      ignoredNoise: mergedFields.ignoredNoise || [],
      confidence: {},
    };

    const nextMetadata = buildEntityMetadataPatch('project', projectEntity.ai_metadata, analysisForPatch);
    nextMetadata.project_chat_enrichment = {
      updatedAt: new Date().toISOString(),
      model: toTrimmedString(enrichmentResponse?.debug?.response?.model, 120) || OPENAI_MODEL,
      source: 'agent_chat',
      status: enrichment.status,
      changeReason: analysisForPatch.changeReason,
    };

    projectEntity.ai_metadata = nextMetadata;
    await projectEntity.save();
    broadcastEntityEvent(ownerId, 'entity.updated', {
      entity: projectEntity.toObject(),
    });

    return {
      status: enrichment.status,
      model: nextMetadata.project_chat_enrichment.model,
      updatedAt: nextMetadata.project_chat_enrichment.updatedAt,
      mergedFieldCounts: Object.fromEntries(
        PROJECT_CHAT_FIELD_KEYS.map((fieldKey) => [fieldKey, Array.isArray(mergedFields[fieldKey]) ? mergedFields[fieldKey].length : 0]),
      ),
    };
  }

  function normalizeQuizOptions(rawOptions) {
    const source = Array.isArray(rawOptions) ? rawOptions : [];
    const result = [];

    for (const item of source) {
      if (result.length >= 4) break;
      const row = toProfile(item);
      const id = toTrimmedString(row.id, 8);
      const text = toTrimmedString(row.text, 220);
      if (!id || !text) continue;
      result.push({ id, text });
    }

    const fallback = [
      { id: '1', text: 'Да' },
      { id: '2', text: 'Нет' },
      { id: '3', text: 'Не знаю' },
      { id: '4', text: 'Свой вариант' },
    ];
    if (!result.length) return fallback;

    const normalized = result
      .slice(0, 4)
      .map((item, index) => ({
        id: String(index + 1),
        text: item.text,
      }));
    while (normalized.length < 4) {
      const nextIndex = normalized.length + 1;
      normalized.push({
        id: String(nextIndex),
        text: nextIndex === 4 ? 'Свой вариант' : fallback[nextIndex - 1].text,
      });
    }
    normalized[3] = { id: '4', text: 'Свой вариант' };
    return normalized;
  }

  function getQuizFirstQuestion(entityType, entityName) {
    const config = QUIZ_FIRST_QUESTION_BY_TYPE[entityType] || QUIZ_FIRST_QUESTION_BY_TYPE.shape;
    const name = toTrimmedString(entityName, 120) || 'Эта сущность';

    return {
      mode: 'quiz_step',
      entityType,
      questionId: 'q1',
      questionText: config.questionText.replace('{name}', name),
      options: normalizeQuizOptions(config.options),
      expects: { type: 'choice_or_text' },
      state: {
        facts: {},
        missing: ['value', 'risk_signal', 'next_step'],
        confidence: 0,
      },
      draftUpdate: {
        description: '',
        fieldsPatch: {},
      },
      stopCheck: null,
    };
  }

  function getQuizLevel1TemplateQuestion(entityType, entityName, level1AnswersCount) {
    const bank = Array.isArray(QUIZ_LEVEL1_BANK_BY_TYPE[entityType])
      ? QUIZ_LEVEL1_BANK_BY_TYPE[entityType]
      : Array.isArray(QUIZ_LEVEL1_BANK_BY_TYPE.shape)
        ? QUIZ_LEVEL1_BANK_BY_TYPE.shape
        : [];
    const index = Math.max(0, Number(level1AnswersCount || 0) - 1);
    const row = bank[index];
    if (!row) return null;

    const name = toTrimmedString(entityName, 120) || 'Эта сущность';
    return {
      questionId: toTrimmedString(row.questionId, 40) || `q_${index + 2}`,
      questionText: toTrimmedString(row.questionText, 320).replace('{name}', name),
      options: normalizeQuizOptions(
        (Array.isArray(row.options) ? row.options : []).map((text, optionIndex) => ({
          id: String(optionIndex + 1),
          text: toTrimmedString(text, 220),
        })),
      ),
      updatesHints: Array.isArray(row.updatesHints)
        ? row.updatesHints.map((item) => toTrimmedString(item, 48)).filter(Boolean).slice(0, 8)
        : [],
    };
  }

  function createInitialQuizState(entityType, entityName, firstQuestion) {
    const nowIso = new Date().toISOString();
    return {
      version: 1,
      active: true,
      entityType,
      entityName: toTrimmedString(entityName, 120),
      level: 'level1',
      stepCount: 0,
      level1Answers: 0,
      level2Answers: 0,
      facts: {},
      missing: Array.isArray(firstQuestion?.state?.missing) ? firstQuestion.state.missing : [],
      confidence: 0,
      history: [],
      lastQuestion: {
        mode: firstQuestion.mode,
        questionId: firstQuestion.questionId,
        questionText: firstQuestion.questionText,
        options: normalizeQuizOptions(firstQuestion.options),
      },
      startedAt: nowIso,
      updatedAt: nowIso,
    };
  }

  function normalizeStoredQuizState(rawState, entityType, entityName) {
    const state = toProfile(rawState);
    const fallbackQuestion = getQuizFirstQuestion(entityType, entityName);
    const lastQuestion = toProfile(state.lastQuestion);
    const mode = toTrimmedString(lastQuestion.mode, 24);
    const questionId = toTrimmedString(lastQuestion.questionId, 80);
    const questionText = toTrimmedString(lastQuestion.questionText, 320);
    const options = normalizeQuizOptions(lastQuestion.options);
    const historyRaw = Array.isArray(state.history) ? state.history : [];
    const history = historyRaw
      .slice(-QUIZ_HISTORY_LIMIT)
      .map((item) => {
        const row = toProfile(item);
        return {
          questionId: toTrimmedString(row.questionId, 80),
          questionText: toTrimmedString(row.questionText, 320),
          answerText: toTrimmedString(row.answerText, 260),
          optionId: toTrimmedString(row.optionId, 8),
          mode: toTrimmedString(row.mode, 32),
          at: toTrimmedString(row.at, 80) || new Date().toISOString(),
        };
      })
      .filter((item) => item.questionId && item.answerText);

    const normalized = {
      version: Number.isFinite(Number(state.version)) ? Math.max(1, Math.floor(Number(state.version))) : 1,
      active: state.active === true,
      entityType: toTrimmedString(state.entityType, 24) || entityType,
      entityName: toTrimmedString(state.entityName, 120) || toTrimmedString(entityName, 120),
      level: toTrimmedString(state.level, 24) || 'level1',
      stepCount: Number.isFinite(Number(state.stepCount)) ? Math.max(0, Math.floor(Number(state.stepCount))) : 0,
      level1Answers:
        Number.isFinite(Number(state.level1Answers)) ? Math.max(0, Math.floor(Number(state.level1Answers))) : 0,
      level2Answers:
        Number.isFinite(Number(state.level2Answers)) ? Math.max(0, Math.floor(Number(state.level2Answers))) : 0,
      facts: toProfile(state.facts),
      missing: (Array.isArray(state.missing) ? state.missing : [])
        .map((item) => toTrimmedString(item, 64))
        .filter(Boolean)
        .slice(0, 20),
      confidence: (() => {
        const value = Number(state.confidence);
        if (!Number.isFinite(value)) return 0;
        return Math.min(1, Math.max(0, value));
      })(),
      history,
      lastQuestion:
        mode && questionId && questionText
          ? {
              mode: QUIZ_ALLOWED_MODES.has(mode) ? mode : 'quiz_step',
              questionId,
              questionText,
              options,
            }
          : {
              mode: fallbackQuestion.mode,
              questionId: fallbackQuestion.questionId,
              questionText: fallbackQuestion.questionText,
              options: normalizeQuizOptions(fallbackQuestion.options),
            },
      startedAt: toTrimmedString(state.startedAt, 80) || new Date().toISOString(),
      updatedAt: toTrimmedString(state.updatedAt, 80) || new Date().toISOString(),
      completedAt: toTrimmedString(state.completedAt, 80),
      stopSummary: toProfile(state.stopSummary),
    };

    return normalized;
  }

  function parseQuizAnswer(rawAnswerText, rawOptionId, lastQuestion) {
    const answerText = toTrimmedString(rawAnswerText, 1200);
    const optionIdRaw = toTrimmedString(rawOptionId, 8);
    const options = normalizeQuizOptions(lastQuestion?.options);
    const optionMap = new Map(options.map((item) => [item.id, item.text]));
    const fromOptionId = optionMap.get(optionIdRaw) ? optionIdRaw : '';

    if (fromOptionId) {
      return {
        optionId: fromOptionId,
        answerText: optionMap.get(fromOptionId) || answerText || '',
        isCustom: fromOptionId === '4',
      };
    }

    const numericMatch = answerText.match(/^\s*(?:ответ\s*)?([1-4])\s*$/i);
    if (numericMatch?.[1] && optionMap.get(numericMatch[1])) {
      const optionId = numericMatch[1];
      return {
        optionId,
        answerText: optionMap.get(optionId) || '',
        isCustom: optionId === '4',
      };
    }

    if (!answerText) {
      return {
        optionId: '',
        answerText: '',
        isCustom: false,
      };
    }

    return {
      optionId: '4',
      answerText,
      isCustom: true,
    };
  }

  function normalizeQuizStatePayload(rawState, fallbackState) {
    const state = toProfile(rawState);
    const fallback = toProfile(fallbackState);
    const confidenceValue = Number(state.confidence);

    return {
      facts: toProfile(state.facts || fallback.facts),
      missing: (Array.isArray(state.missing) ? state.missing : Array.isArray(fallback.missing) ? fallback.missing : [])
        .map((item) => toTrimmedString(item, 64))
        .filter(Boolean)
        .slice(0, 20),
      confidence: Number.isFinite(confidenceValue)
        ? Math.min(1, Math.max(0, confidenceValue))
        : Number.isFinite(Number(fallback.confidence))
          ? Math.min(1, Math.max(0, Number(fallback.confidence)))
          : 0,
    };
  }

  function normalizeQuizDraftUpdate(rawDraftUpdate) {
    const update = toProfile(rawDraftUpdate);
    const fieldsPatchRaw = toProfile(update.fieldsPatch);
    const fieldsPatch = {};

    for (const [key, value] of Object.entries(fieldsPatchRaw)) {
      if (!QUIZ_FIELDS_PATCH_ALLOWED.has(key)) continue;
      const nextValues = (Array.isArray(value) ? value : [value])
        .map((item) => toTrimmedString(item, key === 'linksAdd' ? 240 : 96))
        .filter(Boolean)
        .slice(0, 18);
      if (!nextValues.length) continue;
      fieldsPatch[key] = nextValues;
    }

    return {
      description: toTrimmedString(update.description, 2200),
      fieldsPatch,
    };
  }

  function normalizeQuizModelResponse(rawResponse, { entityType, fallbackQuestion, fallbackState }) {
    const parsed = toProfile(rawResponse);
    const modeRaw = toTrimmedString(parsed.mode, 24);
    const mode = QUIZ_ALLOWED_MODES.has(modeRaw) ? modeRaw : 'quiz_step';
    const questionId = toTrimmedString(parsed.questionId, 80) || `q_${Date.now()}`;
    const questionText = toTrimmedString(parsed.questionText, 320) || fallbackQuestion.questionText;
    const options = normalizeQuizOptions(parsed.options);
    const state = normalizeQuizStatePayload(parsed.state, fallbackState);
    const draftUpdate = normalizeQuizDraftUpdate(parsed.draftUpdate);
    const stopCheck = mode === 'quiz_stop_check' ? toProfile(parsed.summary || parsed.stopCheck) : null;

    return {
      mode,
      entityType,
      questionId,
      questionText,
      options,
      expects: { type: 'choice_or_text' },
      state,
      draftUpdate,
      stopCheck,
    };
  }

  function getAllowedQuizFieldSet(entityType) {
    const base = buildEntityAnalyzerCurrentFields(entityType, {});
    const fields = base && typeof base === 'object' ? Object.keys(base) : [];
    return new Set(fields);
  }

  function normalizeQuizLinkValue(rawValue) {
    const value = toTrimmedString(rawValue, 240);
    if (!value) return '';
    const withProtocol = /^https?:\/\//i.test(value) ? value : `https://${value}`;
    try {
      const url = new URL(withProtocol);
      if (!url.hostname || !url.protocol.startsWith('http')) return '';
      return url.toString();
    } catch {
      return '';
    }
  }

  function normalizeQuizImportanceValue(rawValue) {
    const value = toTrimmedString(rawValue, 32).toLowerCase();
    if (!value) return '';
    if (value === 'низкая' || value === 'low' || value === 'l') return 'Низкая';
    if (value === 'средняя' || value === 'medium' || value === 'med' || value === 'm') return 'Средняя';
    if (value === 'высокая' || value === 'high' || value === 'h' || value === 'critical' || value === 'критично') {
      return 'Высокая';
    }
    return '';
  }

  function mergeQuizFieldValues(existingValues, addedValues, fieldKey) {
    const sourceExisting = Array.isArray(existingValues) ? existingValues : [];
    const sourceAdded = Array.isArray(addedValues) ? addedValues : [];
    const dedup = new Set();
    const result = [];
    const maxItems = fieldKey === 'importance' ? 1 : fieldKey === 'links' ? 24 : 18;
    const maxLength = fieldKey === 'links' ? 240 : 96;

    const pushValue = (rawValue) => {
      const normalized =
        fieldKey === 'links'
          ? normalizeQuizLinkValue(rawValue)
          : fieldKey === 'importance'
            ? normalizeQuizImportanceValue(rawValue)
            : toTrimmedString(rawValue, maxLength);
      if (!normalized) return;
      const key = normalized.toLowerCase();
      if (dedup.has(key)) return;
      dedup.add(key);
      result.push(normalized);
    };

    for (const item of sourceExisting) {
      pushValue(item);
      if (result.length >= maxItems) return result;
    }
    for (const item of sourceAdded) {
      pushValue(item);
      if (result.length >= maxItems) return result;
    }

    return result;
  }

  function applyQuizDraftUpdateToMetadata(entityType, existingMetadata, draftUpdate) {
    const metadata = toProfile(existingMetadata);
    const nextMetadata = {
      ...metadata,
    };
    const allowedFields = getAllowedQuizFieldSet(entityType);
    const currentFields = buildEntityAnalyzerCurrentFields(entityType, metadata);
    const patch = toProfile(draftUpdate?.fieldsPatch);

    for (const fieldName of allowedFields) {
      const patchKey = `${fieldName}Add`;
      const patchValues = Array.isArray(patch[patchKey]) ? patch[patchKey] : [];
      currentFields[fieldName] = mergeQuizFieldValues(currentFields[fieldName], patchValues, fieldName);
    }

    for (const fieldName of allowedFields) {
      nextMetadata[fieldName] = currentFields[fieldName];
    }

    const nextDescription = toTrimmedString(draftUpdate?.description, 2200);
    if (nextDescription) {
      nextMetadata.description = nextDescription;
    }

    return nextMetadata;
  }

  function buildQuizCompletedPayload(entityType, message, state, draftUpdate) {
    return {
      mode: 'quiz_completed',
      entityType,
      questionId: 'quiz_completed',
      questionText: toTrimmedString(message, 320) || 'Квиз завершён.',
      options: [],
      expects: { type: 'none' },
      state: normalizeQuizStatePayload(state, state),
      draftUpdate: normalizeQuizDraftUpdate(draftUpdate),
      stopCheck: null,
    };
  }

  function mapHistoryMessagesToResponse(messages) {
    return (Array.isArray(messages) ? messages : []).map((message) => ({
      id: toTrimmedString(message.id, 120),
      role: toTrimmedString(message.role, 24) === 'assistant' ? 'assistant' : 'user',
      text: toTrimmedString(message.text, AGENT_CHAT_HISTORY_TEXT_MAX_LENGTH),
      createdAt: (() => {
        const raw = message.createdAt;
        if (raw instanceof Date) return raw.toISOString();
        const asString = toTrimmedString(raw, 80);
        const parsed = Date.parse(asString);
        if (Number.isFinite(parsed)) return new Date(parsed).toISOString();
        return new Date().toISOString();
      })(),
      attachments: (Array.isArray(message.attachments) ? message.attachments : [])
        .slice(0, AGENT_CHAT_HISTORY_ATTACHMENT_LIMIT)
        .map((item) => ({
          id: toTrimmedString(item.id, 120) || `att_${Date.now()}`,
          name: toTrimmedString(item.name, 240) || 'Файл',
          mime: toTrimmedString(item.mime, 180),
          size: Number.isFinite(Number(item.size)) ? Math.max(0, Math.floor(Number(item.size))) : 0,
          data: toTrimmedString(item.data, AGENT_CHAT_HISTORY_ATTACHMENT_DATA_MAX_LENGTH),
        }))
        .filter((item) => item.data || item.name),
    }));
  }

  router.get('/chat-history', requireAuth, async (req, res, next) => {
    try {
      const ownerId = requireOwnerId(req);
      const scope = normalizeScope({
        type: req.query.scopeType,
        entityType: req.query.entityType,
        projectId: req.query.projectId,
      });

      const doc = await AgentChatHistory.findOne({
        owner_id: ownerId,
        scope_key: scope.scopeKey,
      })
        .select({ messages: 1, updatedAt: 1 })
        .lean();

      return res.status(200).json({
        scopeKey: scope.scopeKey,
        scope: {
          type: scope.type,
          entityType: scope.entityType,
          projectId: scope.projectId,
        },
        updatedAt: doc?.updatedAt || null,
        messages: mapHistoryMessagesToResponse(doc?.messages || []),
      });
    } catch (error) {
      return next(error);
    }
  });

  router.put('/chat-history', requireAuth, async (req, res, next) => {
    try {
      const ownerId = requireOwnerId(req);
      const scope = normalizeScope(req.body?.scope);
      const normalizedMessages = normalizeMessages(req.body?.messages);

      if (!normalizedMessages.length) {
        await AgentChatHistory.deleteOne({
          owner_id: ownerId,
          scope_key: scope.scopeKey,
        });

        broadcastEntityEvent(ownerId, 'agent-chat.history.deleted', {
          scopeKey: scope.scopeKey,
        });

        return res.status(200).json({
          scopeKey: scope.scopeKey,
          messageCount: 0,
          updatedAt: new Date().toISOString(),
        });
      }

      const savedDoc = await AgentChatHistory.findOneAndUpdate(
        {
          owner_id: ownerId,
          scope_key: scope.scopeKey,
        },
        {
          $set: {
            owner_id: ownerId,
            scope_key: scope.scopeKey,
            scope_type: scope.type,
            entity_type: scope.entityType,
            project_id: scope.projectId,
            messages: normalizedMessages,
          },
        },
        {
          upsert: true,
          new: true,
          setDefaultsOnInsert: true,
          runValidators: true,
        },
      ).lean();

      broadcastEntityEvent(ownerId, 'agent-chat.history.updated', {
        scopeKey: scope.scopeKey,
        updatedAt: savedDoc?.updatedAt || new Date().toISOString(),
        messageCount: normalizedMessages.length,
      });

      return res.status(200).json({
        scopeKey: scope.scopeKey,
        updatedAt: savedDoc?.updatedAt || new Date().toISOString(),
        messageCount: normalizedMessages.length,
      });
    } catch (error) {
      return next(error);
    }
  });

  router.delete('/chat-history', requireAuth, async (req, res, next) => {
    try {
      const ownerId = requireOwnerId(req);
      const clearAll = req.body?.all === true || String(req.query?.all || '').toLowerCase() === 'true';

      if (clearAll) {
        const result = await AgentChatHistory.deleteMany({
          owner_id: ownerId,
        });

        broadcastEntityEvent(ownerId, 'agent-chat.history.cleared', {
          scopeKey: '*',
          deletedCount: Number(result?.deletedCount || 0),
        });

        return res.status(200).json({
          deletedCount: Number(result?.deletedCount || 0),
        });
      }

      const scope = normalizeScope({
        type: req.body?.scope?.type || req.query.scopeType,
        entityType: req.body?.scope?.entityType || req.query.entityType,
        projectId: req.body?.scope?.projectId || req.query.projectId,
      });

      const result = await AgentChatHistory.deleteOne({
        owner_id: ownerId,
        scope_key: scope.scopeKey,
      });

      broadcastEntityEvent(ownerId, 'agent-chat.history.deleted', {
        scopeKey: scope.scopeKey,
        deletedCount: Number(result?.deletedCount || 0),
      });

      return res.status(200).json({
        scopeKey: scope.scopeKey,
        deletedCount: Number(result?.deletedCount || 0),
      });
    } catch (error) {
      return next(error);
    }
  });

  router.post('/agent-chat', requireAuth, async (req, res, next) => {
    try {
      const ownerId = requireOwnerId(req);
      const message = toTrimmedString(req.body?.message, 2400);
      const includeDebug = AI_DEBUG_ECHO || req.body?.debug === true;

      if (!message) {
        return res.status(400).json({ message: 'message is required' });
      }

      const history = aiAttachments.normalizeAgentHistory(req.body?.history);
      const attachments = await aiAttachments.prepareAgentAttachments(req.body?.attachments);
      const scopeContext = await resolveAgentScopeContext(ownerId, req.body?.scope);
      const contextData = aiPrompts.buildAgentContextData({
        scopeContext,
        history,
        attachments,
      });

      const routerPrompt = aiPrompts.buildRouterPrompt(contextData, message);
      const routerSystemPrompt =
        'Ты Semantic Router Synapse12. Верни строго одно слово из списка: investor, hr, strategist, default.';
      const routerModel = toTrimmedString(OPENAI_ROUTER_MODEL, 120) || 'gpt-5.2-pro';

      const routerResponse = await aiProvider.requestOpenAiAgentReply({
        systemPrompt: routerSystemPrompt,
        userPrompt: routerPrompt,
        includeRawPayload: includeDebug,
        model: routerModel,
        temperature: 0,
        maxOutputTokens: 5,
        allowEmptyResponse: true,
        emptyResponseFallback: 'default',
      });
      const detectedRoleRaw = toTrimmedString(routerResponse.reply, 60);
      const detectedRole = aiPrompts.normalizeDetectedRole(detectedRoleRaw);
      const deepModel =
        toTrimmedString(OPENAI_DEEP_MODEL, 120) ||
        toTrimmedString(OPENAI_PROJECT_MODEL, 120) ||
        'gpt-5-mini';

      const systemPrompt = aiPrompts.buildAgentSystemPrompt(contextData, detectedRole);
      const userPrompt = aiPrompts.buildAgentUserPrompt({
        contextData,
        message,
      });

      const aiResponse = await aiProvider.requestOpenAiAgentReply({
        systemPrompt,
        userPrompt,
        includeRawPayload: includeDebug,
        model: deepModel,
        temperature: 0.25,
        maxOutputTokens: 4000,
        allowEmptyResponse: true,
        emptyResponseFallback: 'Пустой ответ от модели. Уточните запрос или повторите через несколько секунд.',
        timeoutMs: 130_000,
      });
      const usedModel = toTrimmedString(aiResponse?.debug?.response?.model, 120) || deepModel;
      const usedRouterModel = toTrimmedString(routerResponse?.debug?.response?.model, 120) || routerModel;

      if (scopeContext.scopeType === 'project') {
        void runProjectChatAutoEnrichment({
          ownerId,
          scopeContext,
          contextData,
          message,
          history,
          assistantReply: aiResponse.reply,
          includeDebug,
        }).catch(() => {
          // Background enrichment must never break the main reply.
        });
      }

      const debugPayload = includeDebug
        ? {
            timestamp: new Date().toISOString(),
            scope: {
              type: scopeContext.scopeType,
              entityType: scopeContext.entityType,
              projectId: scopeContext.projectId,
              totalEntities: scopeContext.totalEntities,
            },
            input: {
              message,
              history,
              attachments,
            },
            semanticRouter: {
              model: usedRouterModel,
              prompt: {
                system: routerSystemPrompt,
                user: routerPrompt,
              },
              detectedRoleRaw,
              detectedRole,
              usage: routerResponse.usage,
              provider: routerResponse.debug || {},
            },
            prompts: {
              systemPrompt,
              userPrompt,
            },
            response: {
              reply: aiResponse.reply,
              usage: aiResponse.usage,
              model: usedModel,
            },
            projectAutoEnrichment: scopeContext.scopeType === 'project'
              ? {
                  queued: true,
                  projectId: scopeContext.projectId,
                }
              : null,
            provider: aiResponse.debug || {},
          }
        : undefined;

      return res.status(200).json({
        reply: aiResponse.reply,
        usage: aiResponse.usage,
        model: usedModel,
        detectedRole,
        context: {
          scopeType: scopeContext.scopeType,
          entityType: scopeContext.entityType,
          projectId: scopeContext.projectId,
          totalEntities: scopeContext.totalEntities,
        },
        ...(debugPayload ? { debug: debugPayload } : {}),
      });
    } catch (error) {
      return next(error);
    }
  });

  router.post('/entity-analyze', requireAuth, async (req, res, next) => {
    try {
      const ownerId = requireOwnerId(req);
      const entityId = toTrimmedString(req.body?.entityId, 80);
      if (!entityId) {
        return res.status(400).json({ message: 'entityId is required' });
      }

      const entity = await Entity.findOne({
        _id: entityId,
        owner_id: ownerId,
      }).lean();

      if (!entity) {
        return res.status(404).json({ message: 'Entity not found' });
      }

      const message = toTrimmedString(req.body?.message, 4000);
      const voiceInput = toTrimmedString(req.body?.voiceInput, 4000);
      const history = aiAttachments.normalizeAgentHistory(req.body?.history);
      const attachments = await aiAttachments.prepareAgentAttachments(req.body?.attachments);
      const documents = await aiAttachments.prepareAgentAttachments(req.body?.documents);

      if (!message && !voiceInput && !history.length && !attachments.length && !documents.length) {
        return res
          .status(400)
          .json({ message: 'message or at least one context item (history/attachments/documents) is required' });
      }

      const aiMetadata = toProfile(entity.ai_metadata);
      const currentFields = buildEntityAnalyzerCurrentFields(entity.type, aiMetadata);
      const systemPrompt = aiPrompts.buildEntityAnalyzerSystemPrompt(entity.type);
      const userPrompt = aiPrompts.buildEntityAnalyzerUserPrompt({
        entity,
        message,
        history,
        attachments,
        currentFields,
        voiceInput,
        documents,
      });
      const includeDebug = AI_DEBUG_ECHO || req.body?.debug === true;

      const aiResponse = await aiProvider.requestOpenAiAgentReply({
        systemPrompt,
        userPrompt,
        includeRawPayload: includeDebug,
        model: OPENAI_MODEL,
        temperature: 0.3,
        maxOutputTokens: 4000,
        timeoutMs: 130_000,
      });
      const usedModel = toTrimmedString(aiResponse?.debug?.response?.model, 120) || OPENAI_MODEL;

      const parsedResponse = extractJsonObjectFromText(aiResponse.reply);
      const analysis = ensureAnalysisMarkers(normalizeEntityAnalysisOutput(entity.type, parsedResponse));
      const reply = aiPrompts.buildEntityAnalysisReplyText(analysis);

      let vector = null;
      let vectorWarning = '';
      if (analysis.status === 'ready') {
        try {
          const vectorDoc = await upsertEntityVector(ownerId, entity, analysis);
          if (vectorDoc) {
            vector = {
              id: String(vectorDoc._id),
              model: vectorDoc.model,
              dimensions: Array.isArray(vectorDoc.vector) ? vectorDoc.vector.length : 0,
              updatedAt: vectorDoc.updatedAt,
            };
          }
        } catch (error) {
          vectorWarning = toTrimmedString(error?.message, 220) || 'Vector build failed';
        }
      }

      const debugPayload = includeDebug
        ? {
            entity: {
              id: String(entity._id),
              type: entity.type,
              name: entity.name || '',
            },
            input: {
              message,
              voiceInput,
              history,
              attachments,
              documents,
              currentFields,
            },
            prompts: {
              systemPrompt,
              userPrompt,
            },
            response: {
              raw: aiResponse.reply,
              parsed: parsedResponse,
              normalized: analysis,
              reply,
              usage: aiResponse.usage,
              model: usedModel,
            },
            provider: aiResponse.debug || {},
            vector: vector || null,
            vectorWarning: vectorWarning || '',
          }
        : undefined;

      return res.status(200).json({
        reply,
        suggestion: analysis,
        usage: aiResponse.usage,
        model: usedModel,
        vector,
        ...(vectorWarning ? { vectorWarning } : {}),
        ...(debugPayload ? { debug: debugPayload } : {}),
      });
    } catch (error) {
      return next(error);
    }
  });

  router.post('/entity-quiz-step', requireAuth, async (req, res, next) => {
    try {
      const ownerId = requireOwnerId(req);
      const entityId = toTrimmedString(req.body?.entityId, 80);
      if (!entityId) {
        return res.status(400).json({ message: 'entityId is required' });
      }

      const includeDebug = AI_DEBUG_ECHO || req.body?.debug === true;
      const entity = await Entity.findOne({
        _id: entityId,
        owner_id: ownerId,
      });

      if (!entity) {
        return res.status(404).json({ message: 'Entity not found' });
      }

      const aiMetadata = toProfile(entity.ai_metadata);
      const entityType = toTrimmedString(entity.type, 24);
      const entityName = toTrimmedString(entity.name, 120) || 'Эта сущность';
      const requestedAction = toTrimmedString(req.body?.action, 24).toLowerCase();
      const action =
        requestedAction === 'answer' || requestedAction === 'start'
          ? requestedAction
          : toTrimmedString(req.body?.answerText || req.body?.message, 1200)
            ? 'answer'
            : 'start';
      const storedState = normalizeStoredQuizState(aiMetadata.quiz_state, entityType, entityName);

      if (action === 'start') {
        if ((storedState.active || storedState.level === 'paused') && storedState.lastQuestion?.questionId) {
          if (!storedState.active) {
            storedState.active = true;
            storedState.level = storedState.lastQuestion.mode === 'quiz_stop_check' ? 'stop_check' : 'level2';
            storedState.updatedAt = new Date().toISOString();
            entity.ai_metadata = {
              ...aiMetadata,
              quiz_state: storedState,
            };
            await entity.save();
            broadcastEntityEvent(ownerId, 'entity.updated', {
              entity: entity.toObject(),
            });
          }

          return res.status(200).json({
            mode: storedState.lastQuestion.mode,
            entityType,
            questionId: storedState.lastQuestion.questionId,
            questionText: storedState.lastQuestion.questionText,
            options: normalizeQuizOptions(storedState.lastQuestion.options),
            expects: { type: 'choice_or_text' },
            state: {
              facts: toProfile(storedState.facts),
              missing: storedState.missing,
              confidence: storedState.confidence,
            },
            draftUpdate: {
              description: toTrimmedString(aiMetadata.description, 2200),
              fieldsPatch: {},
            },
            stopCheck: storedState.lastQuestion.mode === 'quiz_stop_check' ? toProfile(storedState.stopSummary) : null,
            resumed: true,
          });
        }

        const firstQuestion = getQuizFirstQuestion(entityType, entityName);
        const nextState = createInitialQuizState(entityType, entityName, firstQuestion);
        entity.ai_metadata = {
          ...aiMetadata,
          quiz_state: nextState,
        };
        await entity.save();
        broadcastEntityEvent(ownerId, 'entity.updated', {
          entity: entity.toObject(),
        });

        return res.status(200).json({
          ...firstQuestion,
          resumed: false,
        });
      }

      if (!storedState.active || !storedState.lastQuestion?.questionId) {
        const firstQuestion = getQuizFirstQuestion(entityType, entityName);
        const nextState = createInitialQuizState(entityType, entityName, firstQuestion);
        entity.ai_metadata = {
          ...aiMetadata,
          quiz_state: nextState,
        };
        await entity.save();
        broadcastEntityEvent(ownerId, 'entity.updated', {
          entity: entity.toObject(),
        });
        return res.status(200).json({
          ...firstQuestion,
          resumed: false,
        });
      }

      const answer = parseQuizAnswer(
        req.body?.answerText || req.body?.message,
        req.body?.optionId,
        storedState.lastQuestion,
      );
      if (!answer.answerText) {
        return res.status(400).json({ message: 'answerText or optionId is required' });
      }

      const nowIso = new Date().toISOString();
      storedState.history = [
        ...storedState.history,
        {
          questionId: storedState.lastQuestion.questionId,
          questionText: storedState.lastQuestion.questionText,
          answerText: answer.answerText,
          optionId: answer.optionId || '',
          mode: storedState.lastQuestion.mode,
          at: nowIso,
        },
      ].slice(-QUIZ_HISTORY_LIMIT);
      storedState.stepCount += 1;

      if (storedState.level === 'level1') {
        storedState.level1Answers += 1;
      } else if (storedState.level === 'level2') {
        storedState.level2Answers += 1;
      }

      if (storedState.lastQuestion.mode === 'quiz_stop_check') {
        const answerLower = answer.answerText.toLowerCase();
        const chooseDeep =
          answer.optionId === '2' ||
          answerLower.includes('углуб') ||
          answerLower.includes('deep');
        const choosePause =
          answer.optionId === '3' ||
          answerLower.includes('пауза');

        if (chooseDeep) {
          storedState.level = 'level2';
          storedState.active = true;
        } else if (choosePause) {
          storedState.level = 'paused';
          storedState.active = false;
          storedState.completedAt = nowIso;
          storedState.updatedAt = nowIso;

          entity.ai_metadata = {
            ...aiMetadata,
            quiz_state: storedState,
          };
          await entity.save();
          broadcastEntityEvent(ownerId, 'entity.updated', {
            entity: entity.toObject(),
          });

          return res.status(200).json(
            buildQuizCompletedPayload(
              entityType,
              'Квиз поставлен на паузу. Можно продолжить позже.',
              storedState,
              {
                description: '',
                fieldsPatch: {},
              },
            ),
          );
        } else {
          storedState.level = 'done';
          storedState.active = false;
          storedState.completedAt = nowIso;
          storedState.updatedAt = nowIso;

          entity.ai_metadata = {
            ...aiMetadata,
            quiz_state: storedState,
          };
          await entity.save();
          broadcastEntityEvent(ownerId, 'entity.updated', {
            entity: entity.toObject(),
          });

          return res.status(200).json(
            buildQuizCompletedPayload(
              entityType,
              'Квиз завершён. Данные сохранены.',
              storedState,
              {
                description: '',
                fieldsPatch: {},
              },
            ),
          );
        }
      }

      const hasNextLevel1Template =
        storedState.level === 'level1' &&
        Boolean(getQuizLevel1TemplateQuestion(entityType, entityName, storedState.level1Answers));
      const forceStopCheck =
        (storedState.level === 'level1' && storedState.level1Answers >= QUIZ_MAX_LEVEL1_QUESTIONS) ||
        (storedState.level === 'level1' &&
          storedState.level1Answers >= QUIZ_MIN_LEVEL1_QUESTIONS &&
          !hasNextLevel1Template) ||
        (storedState.level === 'level2' && storedState.level2Answers >= QUIZ_MAX_LEVEL2_QUESTIONS);
      const currentFields = buildEntityAnalyzerCurrentFields(entity.type, aiMetadata);

      const systemPrompt = aiPrompts.buildEntityQuizSystemPrompt({
        entityType,
        level: storedState.level,
        forceStopCheck,
      });
      const userPrompt = aiPrompts.buildEntityQuizUserPrompt({
        entityType,
        name: entityName,
        currentDescription: toTrimmedString(aiMetadata.description, 2200),
        currentFields,
        quizState: {
          level: storedState.level,
          stepCount: storedState.stepCount,
          level1Answers: storedState.level1Answers,
          level2Answers: storedState.level2Answers,
          facts: storedState.facts,
          missing: storedState.missing,
          confidence: storedState.confidence,
          history: storedState.history.slice(-18),
        },
        lastQuestion: storedState.lastQuestion,
        answer,
        forceStopCheck,
        level: storedState.level,
      });

      const model =
        forceStopCheck
          ? toTrimmedString(OPENAI_QUIZ_SMART_MODEL, 120) || toTrimmedString(OPENAI_MODEL, 120)
          : toTrimmedString(OPENAI_QUIZ_FAST_MODEL, 120) || toTrimmedString(OPENAI_MODEL, 120);
      const aiResponse = await aiProvider.requestOpenAiAgentReply({
        systemPrompt,
        userPrompt,
        includeRawPayload: includeDebug,
        model,
        temperature: 0.2,
        maxOutputTokens: forceStopCheck ? 2200 : 1200,
        timeoutMs: forceStopCheck ? 130_000 : 90_000,
      });
      const usedModel = toTrimmedString(aiResponse?.debug?.response?.model, 120) || model;

      const parsedResponse = extractJsonObjectFromText(aiResponse.reply);
      const fallbackQuestion = {
        questionId: `q_${storedState.stepCount + 1}`,
        questionText: 'Уточни это в одном коротком ответе.',
      };
      let normalizedResponse = normalizeQuizModelResponse(parsedResponse, {
        entityType,
        fallbackQuestion,
        fallbackState: storedState,
      });

      const level1TemplateQuestion =
        storedState.level === 'level1' && !forceStopCheck
          ? getQuizLevel1TemplateQuestion(entityType, entityName, storedState.level1Answers)
          : null;
      if (level1TemplateQuestion) {
        normalizedResponse = {
          ...normalizedResponse,
          mode: 'quiz_step',
          questionId: level1TemplateQuestion.questionId,
          questionText: level1TemplateQuestion.questionText,
          options: level1TemplateQuestion.options,
          stopCheck: null,
        };
      }

      if (forceStopCheck && normalizedResponse.mode !== 'quiz_stop_check') {
        normalizedResponse = {
          ...normalizedResponse,
          mode: 'quiz_stop_check',
          questionId: 'stop_check',
          questionText: 'Данных достаточно или углубляемся?',
          options: normalizeQuizOptions([
            { id: '1', text: 'Достаточно — завершить' },
            { id: '2', text: 'Углубить' },
            { id: '3', text: 'Пауза' },
            { id: '4', text: 'Свой вариант' },
          ]),
          stopCheck: toProfile(parsedResponse?.summary),
        };
      }

      storedState.facts = toProfile(normalizedResponse.state.facts);
      storedState.missing = normalizedResponse.state.missing;
      storedState.confidence = normalizedResponse.state.confidence;
      storedState.updatedAt = nowIso;
      storedState.lastQuestion = {
        mode: normalizedResponse.mode,
        questionId: normalizedResponse.questionId,
        questionText: normalizedResponse.questionText,
        options: normalizeQuizOptions(normalizedResponse.options),
      };
      if (normalizedResponse.mode === 'quiz_stop_check') {
        storedState.level = 'stop_check';
        storedState.stopSummary = toProfile(normalizedResponse.stopCheck);
      }

      const nextMetadata = applyQuizDraftUpdateToMetadata(entity.type, aiMetadata, normalizedResponse.draftUpdate);
      nextMetadata.quiz_state = storedState;

      entity.ai_metadata = nextMetadata;
      await entity.save();
      broadcastEntityEvent(ownerId, 'entity.updated', {
        entity: entity.toObject(),
      });

      const debugPayload = includeDebug
        ? {
            entity: {
              id: String(entity._id),
              type: entity.type,
              name: entity.name || '',
            },
            input: {
              action,
              answer,
              storedStateBefore: {
                level: storedState.level,
                stepCount: storedState.stepCount,
                level1Answers: storedState.level1Answers,
                level2Answers: storedState.level2Answers,
              },
            },
            prompts: {
              systemPrompt,
              userPrompt,
            },
            response: {
              raw: aiResponse.reply,
              parsed: parsedResponse,
              normalized: normalizedResponse,
              usage: aiResponse.usage,
              model: usedModel,
            },
            provider: aiResponse.debug || {},
          }
        : undefined;

      return res.status(200).json({
        ...normalizedResponse,
        model: usedModel,
        usage: aiResponse.usage,
        ...(debugPayload ? { debug: debugPayload } : {}),
      });
    } catch (error) {
      return next(error);
    }
  });

  router.post('/entity-apply', requireAuth, async (req, res, next) => {
    try {
      const ownerId = requireOwnerId(req);
      const entityId = toTrimmedString(req.body?.entityId, 80);
      if (!entityId) {
        return res.status(400).json({ message: 'entityId is required' });
      }

      const entity = await Entity.findOne({
        _id: entityId,
        owner_id: ownerId,
      });

      if (!entity) {
        return res.status(404).json({ message: 'Entity not found' });
      }

      const analysis = ensureAnalysisMarkers(normalizeEntityAnalysisOutput(entity.type, req.body?.suggestion));
      const nextMetadata = buildEntityMetadataPatch(entity.type, entity.ai_metadata, analysis);
      entity.ai_metadata = nextMetadata;
      await entity.save();
      broadcastEntityEvent(ownerId, 'entity.updated', {
        entity: entity.toObject(),
      });

      let vector = null;
      let vectorWarning = '';
      if (analysis.status === 'ready') {
        try {
          const vectorDoc = await upsertEntityVector(ownerId, entity, analysis);
          if (vectorDoc) {
            vector = {
              id: String(vectorDoc._id),
              model: vectorDoc.model,
              dimensions: Array.isArray(vectorDoc.vector) ? vectorDoc.vector.length : 0,
              updatedAt: vectorDoc.updatedAt,
            };
          }
        } catch (error) {
          vectorWarning = toTrimmedString(error?.message, 220) || 'Vector build failed';
        }
      }

      return res.status(200).json({
        entity,
        suggestion: analysis,
        vector,
        ...(vectorWarning ? { vectorWarning } : {}),
      });
    } catch (error) {
      return next(error);
    }
  });

  return router;
}

module.exports = {
  createAiRouter,
};
