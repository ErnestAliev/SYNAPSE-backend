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
  const QUIZ_PROFILE_SUMMARY_QUESTION_ID = 'P9_PROFILE_SUMMARY';
  const QUIZ_MODE_STANDARD = 'standard';
  const QUIZ_MODE_MY = 'my';
  const QUIZ_MIN_LEVEL1_QUESTIONS = 7;
  const QUIZ_STOP_CHECK_MIN_STEPS = 8;
  const QUIZ_STOP_CHECK_MAX_STEPS = 10;
  const QUIZ_START_DEDUP_WINDOW_MS = 2_000;
  const QUIZ_MAX_LEVEL2_QUESTIONS = 12;
  const QUIZ_HISTORY_LIMIT = 60;
  const QUIZ_PROCESSED_EVENT_LIMIT = 80;
  const QUIZ_DEFAULT_MISSING_BY_TYPE = Object.freeze({
    person: ['relation_type', 'usefulness', 'value_type', 'connection_mode', 'trust_level', 'risk_signal', 'desired_outcome', 'next_step'],
    connection: [
      'relation_type',
      'usefulness',
      'value_type',
      'connection_mode',
      'trust_level',
      'risk_signal',
      'desired_outcome',
      'next_step',
    ],
    company: ['company_relation_type', 'deal_direction', 'usefulness', 'main_risk', 'trust_level', 'desired_action', 'next_step'],
    project: ['project_goal_type', 'success_metric_type', 'priority_level', 'main_blocker', 'main_risk', 'next_step'],
    event: ['event_type', 'usefulness', 'event_goal', 'needed_outcome', 'risk_signal', 'next_step'],
    resource: ['resource_type', 'usefulness', 'benefit_type', 'main_risk', 'action_needed', 'next_step'],
    goal: ['goal_type', 'priority_level', 'success_definition', 'main_barrier', 'next_step'],
    result: ['result_type', 'result_quality', 'impact_type', 'next_step'],
    task: ['task_type', 'usefulness', 'cost_of_error', 'owner', 'main_blocker', 'next_step'],
    shape: ['shape_type', 'priority_level', 'desired_output', 'next_step'],
  });
  const QUIZ_REQUIRED_FACT_KEYS_BY_TYPE = Object.freeze({
    person: ['risk_signal', 'next_step'],
    connection: ['risk_signal', 'next_step'],
    company: ['main_risk', 'next_step'],
    project: ['main_risk', 'next_step'],
    event: ['risk_signal', 'next_step'],
    resource: ['main_risk', 'next_step'],
    goal: ['main_barrier', 'next_step'],
    result: ['result_quality', 'next_step'],
    task: ['main_blocker', 'next_step'],
    shape: ['next_step'],
  });
  const QUIZ_Q1_KEY_BY_TYPE = Object.freeze({
    person: 'relation_type',
    connection: 'relation_type',
    company: 'company_relation_type',
    project: 'project_goal_type',
    event: 'event_type',
    resource: 'resource_type',
    goal: 'goal_type',
    result: 'result_type',
    task: 'task_type',
    shape: 'shape_type',
  });
  const QUIZ_LEVEL1_QUESTION_KEYS_BY_TYPE = Object.freeze({
    person: {
      P2: 'usefulness',
      P3: 'value_type',
      P4: 'connection_mode',
      P5: 'trust_level',
      P6: 'risk_signal',
      P7: 'desired_outcome',
      P8: 'next_step',
    },
    connection: {
      P2: 'usefulness',
      P3: 'value_type',
      P4: 'connection_mode',
      P5: 'trust_level',
      P6: 'risk_signal',
      P7: 'desired_outcome',
      P8: 'next_step',
    },
    company: {
      C2: 'deal_direction',
      C3: 'usefulness',
      C4: 'usefulness',
      C5: 'main_risk',
      C6: 'trust_level',
      C7: 'desired_action',
      C8: 'next_step',
    },
    project: {
      PR2: 'success_metric_type',
      PR3: 'priority_level',
      PR4: 'main_blocker',
      PR5: 'main_risk',
      PR6: 'main_blocker',
      PR7: 'next_step',
    },
    event: {
      E2: 'usefulness',
      E3: 'event_goal',
      E4: 'needed_outcome',
      E5: 'risk_signal',
      E6: 'next_step',
    },
    resource: {
      R2: 'usefulness',
      R3: 'benefit_type',
      R4: 'main_risk',
      R5: 'action_needed',
      R6: 'next_step',
    },
    goal: {
      G2: 'priority_level',
      G3: 'success_definition',
      G4: 'main_barrier',
      G5: 'next_step',
    },
    result: {
      RS2: 'result_quality',
      RS3: 'impact_type',
      RS4: 'next_step',
    },
    task: {
      T2: 'usefulness',
      T3: 'cost_of_error',
      T4: 'owner',
      T5: 'main_blocker',
      T6: 'next_step',
    },
    shape: {
      S2: 'priority_level',
      S3: 'desired_output',
      S4: 'next_step',
    },
  });
  const QUIZ_RISK_SIGNAL_QUESTION_IDS = new Set(['P6', 'C5', 'PR5', 'E5', 'R4', 'G4', 'T3', 'T5']);
  const QUIZ_NEXT_STEP_QUESTION_IDS = new Set(['P8', 'C8', 'PR7', 'E6', 'R6', 'G5', 'RS4', 'T6', 'S4']);
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
  const MY_QUIZ_SCENARIOS = Object.freeze({
    PERSON: 'MY_PERSON',
    COMPANY: 'MY_COMPANY',
    GENERIC: 'MY_GENERIC',
  });
  const MY_QUIZ_REENTRY_QUESTION_ID = 'MY_REENTRY';
  const MY_QUIZ_REENTRY_QUESTION = Object.freeze({
    questionId: MY_QUIZ_REENTRY_QUESTION_ID,
    questionKey: 'reentry_action',
    questionText: 'Квиз уже проходили. Что сделать?',
    options: [
      { id: '1', text: 'Освежить (2 вопроса)' },
      { id: '2', text: 'Пройти заново' },
    ],
    expectsType: 'choice_or_text',
  });
  const MY_QUIZ_REFRESH_BANK = Object.freeze([
    {
      questionId: 'R1',
      questionKey: 'refresh_status',
      questionText: 'Статус',
      options: ['В работе', 'На паузе', 'Не актуально', 'Свой вариант'],
      expectsType: 'choice_or_text',
    },
    {
      questionId: 'R2',
      questionKey: 'refresh_next_step',
      questionText: 'Следующий шаг',
      options: ['Ничего', 'Маленький шаг', 'Активный шаг', 'Свой вариант'],
      expectsType: 'choice_or_text',
    },
  ]);
  const MY_QUIZ_GENERIC_M1_TEXT_BY_TYPE = Object.freeze({
    project: 'Проект {name} — это…',
    goal: 'Цель {name} — это…',
    task: 'Задача {name} — это…',
    resource: 'Ресурс {name} — это…',
    event: 'Событие {name} — это…',
    result: 'Результат {name} — это…',
    shape: 'Заметка {name} — это…',
    connection: 'Контакт {name} — это…',
  });
  const MY_QUIZ_BANK_BY_SCENARIO = Object.freeze({
    [MY_QUIZ_SCENARIOS.PERSON]: [
      {
        questionId: 'P1',
        questionKey: 'role_current',
        questionText: 'Твоя основная роль сейчас',
        options: ['Владелец/предприниматель', 'Руководитель/оператор', 'Эксперт/специалист', 'Свой вариант'],
      },
      {
        questionId: 'P2',
        questionKey: 'focus_main',
        questionText: 'Твой главный фокус',
        options: ['Деньги/рост', 'Порядок/контроль', 'Продукт/проекты', 'Свой вариант'],
      },
      {
        questionId: 'P3',
        questionKey: 'priority_main',
        questionText: 'Что тебе сейчас важнее всего',
        options: ['Увеличить доход/прибыль', 'Снизить хаос/навести систему', 'Найти возможности/партнёров', 'Свой вариант'],
      },
      {
        questionId: 'P4',
        questionKey: 'red_line',
        questionText: 'Твоя главная “красная линия” (ограничение)',
        options: ['Деньги/ресурсы (лимиты, бюджет, касса)', 'Закон/штрафы/риски', 'Репутация/конфликт', 'Время/выгорание'],
      },
      {
        questionId: 'P5',
        questionKey: 'support_style',
        questionText: 'Какой стиль помощи тебе нужен',
        options: ['Коротко и по шагам', 'Варианты + риски + вероятности', 'Жёстко: “делай/не делай”', 'Свой вариант'],
      },
      {
        questionId: 'P6',
        questionKey: 'strength_competency',
        questionText: 'Твоя сильная компетенция',
        options: ['Деньги/учёт/управление', 'Продажи/маркетинг/переговоры', 'Продукт/IT/системы', 'Свой вариант'],
      },
      {
        questionId: 'P7',
        questionKey: 'completion_decision',
        questionText: 'Данных достаточно?',
        options: ['Достаточно — завершить', 'Углубить', 'Пауза', 'Свой вариант'],
      },
      {
        questionId: 'P8',
        questionKey: 'profile_summary',
        questionText: 'Скажи 3–7 предложений: чем занимаешься, что умеешь, какой софт/инструменты используешь чаще всего.',
        options: [],
        expectsType: 'text',
      },
    ],
    [MY_QUIZ_SCENARIOS.COMPANY]: [
      {
        questionId: 'C1',
        questionKey: 'company_kind',
        questionText: 'Компания {name} — это',
        options: ['Основной бизнес', 'Дочка/проект', 'Инвестиция/доля', 'Свой вариант'],
      },
      {
        questionId: 'C2',
        questionKey: 'company_audience',
        questionText: 'Кому компания продаёт',
        options: ['B2B', 'B2C', 'Смешанно', 'Свой вариант'],
      },
      {
        questionId: 'C3',
        questionKey: 'company_focus',
        questionText: 'Что сейчас важнее всего для компании',
        options: ['Продажи/клиенты', 'Маржа/прибыль', 'Порядок/операционка', 'Свой вариант'],
      },
      {
        questionId: 'C4',
        questionKey: 'company_main_risk',
        questionText: 'Главный риск/ограничение',
        options: ['Деньги/касса', 'Закон/проверки', 'Люди/качество', 'Свой вариант'],
      },
      {
        questionId: 'C5',
        questionKey: 'company_strength',
        questionText: 'Сильная сторона компании',
        options: ['Продажи/маркетинг', 'Продукт/экспертиза', 'Операционка/финансы', 'Свой вариант'],
      },
      {
        questionId: 'C6',
        questionKey: 'completion_decision',
        questionText: 'Данных достаточно?',
        options: ['Достаточно — завершить', 'Углубить', 'Пауза', 'Свой вариант'],
      },
      {
        questionId: 'C7',
        questionKey: 'profile_summary',
        questionText: '3–7 предложений: что делает компания, на чём зарабатывает, кто клиент, в чём сила.',
        options: [],
        expectsType: 'text',
      },
    ],
    [MY_QUIZ_SCENARIOS.GENERIC]: [
      {
        questionId: 'M1',
        questionKey: 'generic_type',
        questionText: '{generic_m1}',
        options: ['Про деньги/выгоду', 'Про результат/ускорение дел', 'Про порядок/контроль', 'Свой вариант'],
      },
      {
        questionId: 'M2',
        questionKey: 'generic_goal',
        questionText: 'Зачем это тебе',
        options: ['Прибыль/эффект', 'Ускорить дела', 'Зафиксировать/не забыть', 'Свой вариант'],
      },
      {
        questionId: 'M3',
        questionKey: 'generic_risk',
        questionText: 'Главный риск/минус',
        options: ['Потеря времени', 'Потеря денег/ресурсов', 'Хаос/распыление', 'Свой вариант'],
      },
      {
        questionId: 'M4',
        questionKey: 'generic_status',
        questionText: 'Статус',
        options: ['В работе', 'На паузе', 'Не актуально', 'Свой вариант'],
      },
      {
        questionId: 'M5',
        questionKey: 'generic_next_step',
        questionText: 'Следующий шаг',
        options: ['Ничего', 'Маленький шаг', 'Активный шаг', 'Свой вариант'],
      },
      {
        questionId: 'M6',
        questionKey: 'completion_decision',
        questionText: 'Данных достаточно?',
        options: ['Достаточно — завершить', 'Углубить', 'Пауза', 'Свой вариант'],
      },
      {
        questionId: 'M7',
        questionKey: 'profile_summary',
        questionText: '1–3 предложения: что это и что хочешь получить на выходе.',
        options: [],
        expectsType: 'text',
      },
    ],
  });
  const MY_QUIZ_SUFFICIENCY_IDS = new Set(['P7', 'C6', 'M6']);
  const MY_QUIZ_FINAL_FREEFORM_IDS = new Set(['P8', 'C7', 'M7']);
  const MY_QUIZ_REFRESH_FINAL_ID = 'R2';

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

  function normalizeProcessedQuizEvents(rawEvents) {
    const source = Array.isArray(rawEvents) ? rawEvents : [];
    return source
      .slice(-QUIZ_PROCESSED_EVENT_LIMIT)
      .map((item) => {
        const row = toProfile(item);
        const id = toTrimmedString(row.id, 120);
        if (!id) return null;
        const response = toProfile(row.response);
        const at = toTrimmedString(row.at, 80) || new Date().toISOString();
        return {
          id,
          response,
          at,
        };
      })
      .filter(Boolean);
  }

  function findProcessedQuizEvent(processedEvents, eventId) {
    const normalizedEventId = toTrimmedString(eventId, 120);
    if (!normalizedEventId) return null;
    const events = normalizeProcessedQuizEvents(processedEvents);
    return events.find((item) => item.id === normalizedEventId) || null;
  }

  function rememberProcessedQuizEvent(processedEvents, eventId, responsePayload) {
    const normalizedEventId = toTrimmedString(eventId, 120);
    if (!normalizedEventId) return normalizeProcessedQuizEvents(processedEvents);
    const events = normalizeProcessedQuizEvents(processedEvents).filter((item) => item.id !== normalizedEventId);
    events.push({
      id: normalizedEventId,
      response: toProfile(responsePayload),
      at: new Date().toISOString(),
    });
    return events.slice(-QUIZ_PROCESSED_EVENT_LIMIT);
  }

  function buildQuizOrchestratorPayload(state, isMyQuiz = false) {
    const normalizedState = toProfile(state);
    return {
      activeQuestionId: toTrimmedString(normalizedState.activeQuestionId, 80),
      answeredQuestionIds: Array.isArray(normalizedState.answeredQuestionIds)
        ? normalizedState.answeredQuestionIds
            .map((item) => toTrimmedString(item, 80))
            .filter(Boolean)
        : [],
      answers: isMyQuiz ? toProfile(normalizedState.answers) : toProfile(normalizedState.facts),
      stepIndex: Number.isFinite(Number(normalizedState.stepIndex)) ? Math.max(0, Math.floor(Number(normalizedState.stepIndex))) : 0,
    };
  }

  function enforceQuizStateInvariants(state, isMyQuiz = false) {
    if (!state || typeof state !== 'object') return state;
    const activeQuestionId = toTrimmedString(state.activeQuestionId, 80);
    const activeQuestionIdUpper = activeQuestionId.toUpperCase();
    const answered = Array.isArray(state.answeredQuestionIds) ? state.answeredQuestionIds : [];
    state.answeredQuestionIds = Array.from(
      new Set(
        answered
          .map((item) => toTrimmedString(item, 80))
          .filter(Boolean)
          .filter((item) => item.toUpperCase() !== activeQuestionIdUpper),
      ),
    ).slice(0, isMyQuiz ? 120 : 80);
    return state;
  }

  function getQuizDefaultMissing(entityType) {
    const source = Array.isArray(QUIZ_DEFAULT_MISSING_BY_TYPE[entityType])
      ? QUIZ_DEFAULT_MISSING_BY_TYPE[entityType]
      : Array.isArray(QUIZ_DEFAULT_MISSING_BY_TYPE.shape)
        ? QUIZ_DEFAULT_MISSING_BY_TYPE.shape
        : [];
    return source.map((item) => toTrimmedString(item, 64)).filter(Boolean);
  }

  function getQuizRequiredKeys(entityType) {
    const source = Array.isArray(QUIZ_REQUIRED_FACT_KEYS_BY_TYPE[entityType])
      ? QUIZ_REQUIRED_FACT_KEYS_BY_TYPE[entityType]
      : Array.isArray(QUIZ_REQUIRED_FACT_KEYS_BY_TYPE.shape)
        ? QUIZ_REQUIRED_FACT_KEYS_BY_TYPE.shape
        : [];
    return source.map((item) => toTrimmedString(item, 64)).filter(Boolean);
  }

  function getQuizQ1Key(entityType) {
    const key = toTrimmedString(QUIZ_Q1_KEY_BY_TYPE[entityType], 64);
    if (key) return key;
    const defaults = getQuizDefaultMissing(entityType);
    return defaults[0] || 'relation_type';
  }

  function getQuizQuestionKey(entityType, questionId, fallbackQuestionKey = '') {
    const normalizedQuestionId = toTrimmedString(questionId, 64).toUpperCase();
    if (!normalizedQuestionId) {
      return toTrimmedString(fallbackQuestionKey, 64) || '';
    }
    if (normalizedQuestionId === 'Q1') {
      return getQuizQ1Key(entityType);
    }
    const mapForType = toProfile(QUIZ_LEVEL1_QUESTION_KEYS_BY_TYPE[entityType]);
    const fromMap = toTrimmedString(mapForType[normalizedQuestionId], 64);
    if (fromMap) return fromMap;
    return toTrimmedString(fallbackQuestionKey, 64) || '';
  }

  function getQuizFirstQuestion(entityType, entityName) {
    const config = QUIZ_FIRST_QUESTION_BY_TYPE[entityType] || QUIZ_FIRST_QUESTION_BY_TYPE.shape;
    const name = toTrimmedString(entityName, 120) || 'Эта сущность';
    const missingDefaults = getQuizDefaultMissing(entityType);
    const questionKey = getQuizQ1Key(entityType);

    return {
      mode: 'quiz_step',
      entityType,
      questionId: 'Q1',
      questionKey,
      questionText: config.questionText.replace('{name}', name),
      options: normalizeQuizOptions(config.options),
      expects: { type: 'choice_or_text' },
      state: {
        facts: {},
        missing: missingDefaults,
        confidence: 0,
      },
      draftUpdate: {
        description: '',
        fieldsPatch: {},
      },
      stopCheck: null,
    };
  }

  function isQuizProfileSummaryQuestionId(questionId) {
    return toTrimmedString(questionId, 80).toUpperCase() === QUIZ_PROFILE_SUMMARY_QUESTION_ID;
  }

  function buildQuizProfileSummaryQuestion(entityName) {
    const name = toTrimmedString(entityName, 120) || 'эта сущность';
    return {
      mode: 'quiz_step',
      entityType: 'person',
      questionId: QUIZ_PROFILE_SUMMARY_QUESTION_ID,
      questionKey: 'profile_summary',
      questionText: `В двух словах: чем занимается ${name} и чем может быть полезен?`,
      options: [],
      expectsType: 'text',
      expects: { type: 'text' },
      state: {
        facts: {},
        missing: [],
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
    const questionId = toTrimmedString(row.questionId, 40) || `Q${index + 2}`;
    const questionKey = getQuizQuestionKey(entityType, questionId, row.questionKey);
    return {
      questionId,
      questionKey,
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

  function getQuizLevel1Bank(entityType) {
    return Array.isArray(QUIZ_LEVEL1_BANK_BY_TYPE[entityType])
      ? QUIZ_LEVEL1_BANK_BY_TYPE[entityType]
      : Array.isArray(QUIZ_LEVEL1_BANK_BY_TYPE.shape)
        ? QUIZ_LEVEL1_BANK_BY_TYPE.shape
        : [];
  }

  function buildQuizQuestionBank(entityType, entityName) {
    const questions = [];
    const firstQuestion = getQuizFirstQuestion(entityType, entityName);
    questions.push({
      questionId: toTrimmedString(firstQuestion.questionId, 80) || 'Q1',
      questionKey: toTrimmedString(firstQuestion.questionKey, 64) || getQuizQ1Key(entityType),
      questionText: toTrimmedString(firstQuestion.questionText, 320),
      options: normalizeQuizOptions(firstQuestion.options),
    });

    const name = toTrimmedString(entityName, 120) || 'Эта сущность';
    const bank = getQuizLevel1Bank(entityType);
    for (const row of bank) {
      const questionId = toTrimmedString(row.questionId, 80);
      const questionText = toTrimmedString(row.questionText, 320).replace('{name}', name);
      if (!questionId || !questionText) continue;
      questions.push({
        questionId,
        questionKey: getQuizQuestionKey(entityType, questionId, row.questionKey),
        questionText,
        options: normalizeQuizOptions(
          (Array.isArray(row.options) ? row.options : []).map((text, optionIndex) => ({
            id: String(optionIndex + 1),
            text: toTrimmedString(text, 220),
          })),
        ),
      });
    }

    const dedup = new Set();
    return questions.filter((question) => {
      const key = toTrimmedString(question.questionId, 80).toUpperCase();
      if (!key || dedup.has(key)) return false;
      dedup.add(key);
      return true;
    });
  }

  function findQuizQuestionById(entityType, entityName, questionId) {
    const normalizedQuestionId = toTrimmedString(questionId, 80).toUpperCase();
    if (!normalizedQuestionId) return null;
    const bank = buildQuizQuestionBank(entityType, entityName);
    return (
      bank.find((question) => toTrimmedString(question.questionId, 80).toUpperCase() === normalizedQuestionId) || null
    );
  }

  function chooseNextQuizQuestion(state, entityType, entityName, options = {}) {
    const bank = buildQuizQuestionBank(entityType, entityName);
    const answeredSet = new Set(
      (Array.isArray(state?.answeredQuestionIds) ? state.answeredQuestionIds : [])
        .map((item) => toTrimmedString(item, 80).toUpperCase())
        .filter(Boolean),
    );
    const excludedQuestionId = toTrimmedString(options.excludeQuestionId, 80).toUpperCase();
    if (excludedQuestionId) {
      answeredSet.add(excludedQuestionId);
    }
    const missingSet = new Set(
      (Array.isArray(state?.missing) ? state.missing : [])
        .map((item) => toTrimmedString(item, 64))
        .filter(Boolean),
    );

    for (const question of bank) {
      const questionId = toTrimmedString(question.questionId, 80).toUpperCase();
      if (!questionId || answeredSet.has(questionId)) continue;
      if (question.questionKey && missingSet.has(question.questionKey)) {
        return question;
      }
    }

    for (const question of bank) {
      const questionId = toTrimmedString(question.questionId, 80).toUpperCase();
      if (!questionId || answeredSet.has(questionId)) continue;
      return question;
    }

    return null;
  }

  function buildQuizStopSummary(state, entityType) {
    const facts = toProfile(state?.facts);
    const keyFacts = Object.entries(facts)
      .filter(([, value]) => hasQuizFactValue(value))
      .slice(0, 12)
      .map(([key, value]) => `${key}: ${toTrimmedString(value, 220)}`);
    const risks = Object.entries(facts)
      .filter(([key, value]) => key.toLowerCase().includes('risk') && hasQuizFactValue(value))
      .slice(0, 6)
      .map(([, value]) => toTrimmedString(value, 220))
      .filter(Boolean);
    const nextSuggestedStep = toTrimmedString(facts.next_step, 220) || 'Уточнить следующий шаг.';
    return {
      entityType,
      keyFacts,
      risks,
      nextSuggestedStep,
      missing: (Array.isArray(state?.missing) ? state.missing : [])
        .map((item) => toTrimmedString(item, 64))
        .filter(Boolean),
    };
  }

  function createInitialQuizState(entityType, entityName, firstQuestion) {
    const nowIso = new Date().toISOString();
    const defaultsMissing = getQuizDefaultMissing(entityType);
    return {
      version: 1,
      isActive: true,
      active: true,
      activeQuestionId: toTrimmedString(firstQuestion?.questionId, 80) || 'Q1',
      answeredQuestionIds: [],
      processedEvents: [],
      entityType,
      entityName: toTrimmedString(entityName, 120),
      level: 1,
      stepCount: 0,
      stepIndex: 1,
      level1Answers: 0,
      level2Answers: 0,
      facts: {},
      missing: Array.isArray(firstQuestion?.state?.missing) ? firstQuestion.state.missing : defaultsMissing,
      confidence: 0,
      history: [],
      lastQuestion: {
        mode: firstQuestion.mode,
        questionId: firstQuestion.questionId,
        questionKey: toTrimmedString(firstQuestion.questionKey, 64),
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
    const fallbackMissing = getQuizDefaultMissing(entityType);
    const lastQuestion = toProfile(state.lastQuestion);
    const mode = toTrimmedString(lastQuestion.mode, 24);
    const questionId = toTrimmedString(lastQuestion.questionId, 80);
    const questionKey = toTrimmedString(lastQuestion.questionKey, 64);
    const questionText = toTrimmedString(lastQuestion.questionText, 320);
    const options = normalizeQuizOptions(lastQuestion.options);
    const historyRaw = Array.isArray(state.history) ? state.history : [];
    const history = historyRaw
      .slice(-QUIZ_HISTORY_LIMIT)
      .map((item) => {
        const row = toProfile(item);
        return {
          questionId: toTrimmedString(row.questionId, 80),
          questionKey: toTrimmedString(row.questionKey, 64),
          questionText: toTrimmedString(row.questionText, 320),
          answerText: toTrimmedString(row.answerText, 260),
          optionId: toTrimmedString(row.optionId, 8),
          mode: toTrimmedString(row.mode, 32),
          at: toTrimmedString(row.at, 80) || new Date().toISOString(),
        };
      })
      .filter((item) => item.questionId && item.answerText);
    const answeredFromState = Array.isArray(state.answeredQuestionIds) ? state.answeredQuestionIds : [];
    const answeredFromHistory = history.map((item) => item.questionId);
    const answeredQuestionIdsRaw = Array.from(
      new Set(
        [...answeredFromState, ...answeredFromHistory]
          .map((item) => toTrimmedString(item, 80))
          .filter(Boolean),
      ),
    ).slice(0, 80);
    const normalizedLevelRaw = Number(state.level);
    const normalizedLegacyLevel = toTrimmedString(state.level, 24).toLowerCase();
    const level =
      Number.isFinite(normalizedLevelRaw) && normalizedLevelRaw >= 2
        ? 2
        : normalizedLegacyLevel === 'level2' || normalizedLegacyLevel === '2'
          ? 2
          : 1;
    const stepCount = Number.isFinite(Number(state.stepCount)) ? Math.max(0, Math.floor(Number(state.stepCount))) : 0;
    const level1Answers = Number.isFinite(Number(state.level1Answers))
      ? Math.max(0, Math.floor(Number(state.level1Answers)))
      : history.length;
    const stepIndex = Number.isFinite(Number(state.stepIndex))
      ? Math.max(1, Math.floor(Number(state.stepIndex)))
      : Math.max(1, level1Answers || stepCount || history.length || 1);
    const activeQuestionId =
      toTrimmedString(state.activeQuestionId, 80) ||
      (mode && questionId ? questionId : toTrimmedString(fallbackQuestion.questionId, 80));
    const activeQuestionIdUpper = toTrimmedString(activeQuestionId, 80).toUpperCase();
    const answeredQuestionIds = answeredQuestionIdsRaw.filter(
      (item) => toTrimmedString(item, 80).toUpperCase() !== activeQuestionIdUpper,
    );
    const isActive = state.isActive === true || state.active === true;

    const normalized = {
      version: Number.isFinite(Number(state.version)) ? Math.max(1, Math.floor(Number(state.version))) : 1,
      isActive,
      active: isActive,
      activeQuestionId,
      answeredQuestionIds,
      processedEvents: normalizeProcessedQuizEvents(state.processedEvents),
      entityType: toTrimmedString(state.entityType, 24) || entityType,
      entityName: toTrimmedString(state.entityName, 120) || toTrimmedString(entityName, 120),
      level,
      stepCount,
      stepIndex,
      level1Answers,
      level2Answers:
        Number.isFinite(Number(state.level2Answers)) ? Math.max(0, Math.floor(Number(state.level2Answers))) : 0,
      facts: toProfile(state.facts),
      missing: (Array.isArray(state.missing) ? state.missing : fallbackMissing)
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
              questionKey: questionKey || getQuizQuestionKey(entityType, questionId),
              questionText,
              options,
            }
          : {
              mode: fallbackQuestion.mode,
              questionId: fallbackQuestion.questionId,
              questionKey: toTrimmedString(fallbackQuestion.questionKey, 64),
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

  function hasQuizFactValue(rawValue) {
    if (Array.isArray(rawValue)) {
      return rawValue.some((item) => Boolean(toTrimmedString(item, 320)));
    }
    if (rawValue && typeof rawValue === 'object') {
      return Object.keys(rawValue).length > 0;
    }
    return Boolean(toTrimmedString(rawValue, 320));
  }

  function normalizeQuizLevel(rawLevel) {
    const numericLevel = Number(rawLevel);
    if (Number.isFinite(numericLevel) && numericLevel >= 2) return 2;
    const normalized = toTrimmedString(rawLevel, 24).toLowerCase();
    if (normalized === 'level2' || normalized === '2') return 2;
    return 1;
  }

  function getQuizRequiredStateKeys(entityType, rawLevel) {
    const level = normalizeQuizLevel(rawLevel);
    if (level >= 2) {
      // Для Level-2 пока сохраняем тот же обязательный набор; при расширении добавляем отдельный маппинг.
      return getQuizDefaultMissing(entityType);
    }
    return getQuizDefaultMissing(entityType);
  }

  function computeQuizConfidenceFromFacts(rawFacts, requiredKeys, fallbackConfidence = 0) {
    const facts = toProfile(rawFacts);
    const required = Array.isArray(requiredKeys) ? requiredKeys : [];
    const requiredCount = required.length || 1;
    let filled = 0;
    for (const key of required) {
      if (hasQuizFactValue(facts[key])) filled += 1;
    }
    const byFacts = Math.min(1, Math.max(0, filled / requiredCount));
    const fallback = Number.isFinite(Number(fallbackConfidence))
      ? Math.min(1, Math.max(0, Number(fallbackConfidence)))
      : 0;
    return Math.max(byFacts, fallback);
  }

  function normalizeQuizFactsAndMissing(state, entityType) {
    const facts = toProfile(state?.facts);
    const requiredKeys = getQuizRequiredStateKeys(entityType, state?.level);
    const missing = requiredKeys.filter((key) => !hasQuizFactValue(facts[key]));
    return {
      facts,
      missing: missing.slice(0, 24),
      confidence: computeQuizConfidenceFromFacts(facts, requiredKeys, state?.confidence),
    };
  }

  function updateQuizFactsFromAnswer(state, lastQuestion, answer, entityType) {
    const facts = toProfile(state?.facts);
    const questionId = toTrimmedString(lastQuestion?.questionId, 80);
    const questionIdUpper = questionId.toUpperCase();
    const questionKey = getQuizQuestionKey(entityType, questionId, lastQuestion?.questionKey);
    const questionText = toTrimmedString(lastQuestion?.questionText, 320).toLowerCase();
    const answerText = toTrimmedString(answer?.answerText, 320);

    if (!questionId || !answerText) {
      return normalizeQuizFactsAndMissing(
        {
          ...toProfile(state),
          facts,
        },
        entityType,
      );
    }

    if (questionKey) {
      facts[questionKey] = answerText;
    }

    const isRiskSignalQuestion =
      QUIZ_RISK_SIGNAL_QUESTION_IDS.has(questionIdUpper) ||
      questionIdUpper === 'RISK_SIGNAL' ||
      questionText.includes('риск') ||
      questionText.includes('красный флаг');
    if (isRiskSignalQuestion && !questionKey) {
      facts.risk_signal = answerText;
    }

    const isNextStepQuestion =
      QUIZ_NEXT_STEP_QUESTION_IDS.has(questionIdUpper) ||
      questionIdUpper === 'NEXT_STEP' ||
      questionText.includes('следующий шаг') ||
      questionText.includes('next step');
    if (isNextStepQuestion && !questionKey) {
      facts.next_step = answerText;
    }

    return normalizeQuizFactsAndMissing(
      {
        ...toProfile(state),
        facts,
      },
      entityType,
    );
  }

  function hasQuizRequiredStopFacts(state, entityType) {
    const facts = toProfile(state?.facts);
    const requiredKeys = getQuizRequiredKeys(entityType);
    if (!requiredKeys.length) return false;
    return requiredKeys.every((key) => hasQuizFactValue(facts[key]));
  }

  function hasValidModelQuizStepQuestion(rawResponse) {
    const parsed = toProfile(rawResponse);
    const mode = toTrimmedString(parsed.mode, 24).toLowerCase();
    if (mode !== 'quiz_step') return false;
    const questionId = toTrimmedString(parsed.questionId, 80);
    const questionText = toTrimmedString(parsed.questionText, 320);
    if (!questionId || !questionText) return false;
    const rawOptions = Array.isArray(parsed.options) ? parsed.options : [];
    if (rawOptions.length !== 4) return false;
    for (let index = 0; index < 4; index += 1) {
      const row = toProfile(rawOptions[index]);
      const id = toTrimmedString(row.id, 8);
      const text = toTrimmedString(row.text, 220);
      if (id !== String(index + 1) || !text) return false;
    }
    return true;
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

  function hasQuizPatchValues(rawPatch) {
    const patch = toProfile(rawPatch);
    for (const [key, value] of Object.entries(patch)) {
      if (!QUIZ_FIELDS_PATCH_ALLOWED.has(key)) continue;
      if (!Array.isArray(value)) continue;
      if (value.some((item) => Boolean(toTrimmedString(item, key === 'linksAdd' ? 240 : 96)))) {
        return true;
      }
    }
    return false;
  }

  function mergeQuizDraftUpdates(baseUpdate, extraUpdate) {
    const normalizedBase = normalizeQuizDraftUpdate(baseUpdate);
    const normalizedExtra = normalizeQuizDraftUpdate(extraUpdate);
    const mergedFieldsPatch = {};

    for (const key of QUIZ_FIELDS_PATCH_ALLOWED) {
      const existing = Array.isArray(normalizedBase.fieldsPatch[key]) ? normalizedBase.fieldsPatch[key] : [];
      const incoming = Array.isArray(normalizedExtra.fieldsPatch[key]) ? normalizedExtra.fieldsPatch[key] : [];
      if (!existing.length && !incoming.length) continue;

      const dedup = new Set();
      const merged = [];
      const maxLength = key === 'linksAdd' ? 240 : 96;
      for (const item of [...existing, ...incoming]) {
        const value = toTrimmedString(item, maxLength);
        if (!value) continue;
        const dedupKey = value.toLowerCase();
        if (dedup.has(dedupKey)) continue;
        dedup.add(dedupKey);
        merged.push(value);
        if (merged.length >= 18) break;
      }
      if (merged.length) {
        mergedFieldsPatch[key] = merged;
      }
    }

    return normalizeQuizDraftUpdate({
      description: toTrimmedString(normalizedExtra.description, 2200) || normalizedBase.description,
      fieldsPatch: mergedFieldsPatch,
    });
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

  function resolveMyQuizScenario(entityType, entity) {
    const normalizedType = toTrimmedString(entityType, 24).toLowerCase();
    if (!normalizedType) return '';
    const isMe = normalizedType === 'person' && entity?.is_me === true;
    if (isMe) return MY_QUIZ_SCENARIOS.PERSON;
    if (normalizedType === 'person') return '';
    const isMine = entity?.is_mine === true;
    if (!isMine) return '';
    if (normalizedType === 'company') return MY_QUIZ_SCENARIOS.COMPANY;
    return MY_QUIZ_SCENARIOS.GENERIC;
  }

  function normalizeMyQuizOptions(rawOptions) {
    const source = Array.isArray(rawOptions) ? rawOptions : [];
    const normalized = source
      .slice(0, 4)
      .map((item, index) => {
        if (typeof item === 'string') {
          const text = toTrimmedString(item, 220);
          if (!text) return null;
          return {
            id: String(index + 1),
            text,
          };
        }
        const row = toProfile(item);
        const text = toTrimmedString(row.text, 220);
        if (!text) return null;
        const id = toTrimmedString(row.id, 8) || String(index + 1);
        return { id, text };
      })
      .filter(Boolean);

    return normalized;
  }

  function buildMyQuizBankForScenario(scenario, entityType, entityName, mode = 'full') {
    const normalizedMode = toTrimmedString(mode, 24).toLowerCase();
    if (normalizedMode === 'refresh') {
      return MY_QUIZ_REFRESH_BANK.map((row) => ({
        questionId: toTrimmedString(row.questionId, 80),
        questionKey: toTrimmedString(row.questionKey, 64),
        questionText: toTrimmedString(row.questionText, 320),
        options: normalizeMyQuizOptions(row.options),
        expectsType: toTrimmedString(row.expectsType, 24) === 'text' ? 'text' : 'choice_or_text',
      }));
    }

    const source = Array.isArray(MY_QUIZ_BANK_BY_SCENARIO[scenario]) ? MY_QUIZ_BANK_BY_SCENARIO[scenario] : [];
    const normalizedType = toTrimmedString(entityType, 24).toLowerCase();
    const name = toTrimmedString(entityName, 120) || 'Эта сущность';
    const genericM1Template = toTrimmedString(MY_QUIZ_GENERIC_M1_TEXT_BY_TYPE[normalizedType], 220) || 'Сущность {name} — это…';

    return source
      .map((row) => {
        const questionId = toTrimmedString(row.questionId, 80);
        if (!questionId) return null;
        let questionText = toTrimmedString(row.questionText, 320);
        questionText = questionText.replace('{name}', name).replace('{generic_m1}', genericM1Template.replace('{name}', name));
        return {
          questionId,
          questionKey: toTrimmedString(row.questionKey, 64),
          questionText,
          options: normalizeMyQuizOptions(row.options),
          expectsType: toTrimmedString(row.expectsType, 24) === 'text' ? 'text' : 'choice_or_text',
        };
      })
      .filter(Boolean);
  }

  function createInitialMyQuizState({ scenario, entityType, entityName, mode = 'full' }) {
    const nowIso = new Date().toISOString();
    return {
      version: 1,
      scenario,
      entityType: toTrimmedString(entityType, 24),
      entityName: toTrimmedString(entityName, 120),
      mode: toTrimmedString(mode, 24) || 'full',
      isActive: true,
      activeQuestionId: '',
      answeredQuestionIds: [],
      processedEvents: [],
      answers: {},
      history: [],
      stepIndex: 1,
      completed: false,
      startedAt: nowIso,
      started_at: nowIso,
      updatedAt: nowIso,
      updated_at: nowIso,
      completedAt: '',
      completed_at: '',
    };
  }

  function normalizeStoredMyQuizState(rawState, { scenario, entityType, entityName }) {
    const baseState = createInitialMyQuizState({
      scenario,
      entityType,
      entityName,
      mode: 'full',
    });
    baseState.isActive = false;
    baseState.activeQuestionId = '';
    baseState.stepIndex = 0;
    baseState.startedAt = '';
    baseState.updatedAt = '';

    const state = toProfile(rawState);
    const storedScenario = toTrimmedString(state.scenario, 40);
    if (!storedScenario || storedScenario !== scenario) {
      return baseState;
    }

    const answeredQuestionIdsRaw = Array.from(
      new Set(
        (Array.isArray(state.answeredQuestionIds) ? state.answeredQuestionIds : [])
          .map((item) => toTrimmedString(item, 80))
          .filter(Boolean),
      ),
    ).slice(0, 120);
    const history = (Array.isArray(state.history) ? state.history : [])
      .slice(-QUIZ_HISTORY_LIMIT)
      .map((row) => {
        const item = toProfile(row);
        return {
          questionId: toTrimmedString(item.questionId, 80),
          questionKey: toTrimmedString(item.questionKey, 64),
          answerText: toTrimmedString(item.answerText, 320),
          optionId: toTrimmedString(item.optionId, 8),
          at: toTrimmedString(item.at, 80) || new Date().toISOString(),
        };
      })
      .filter((item) => item.questionId && item.answerText);

    const activeQuestionId = toTrimmedString(state.activeQuestionId, 80);
    const activeQuestionIdUpper = activeQuestionId.toUpperCase();
    const answeredQuestionIds = answeredQuestionIdsRaw.filter(
      (item) => toTrimmedString(item, 80).toUpperCase() !== activeQuestionIdUpper,
    );

    return {
      ...baseState,
      mode: (() => {
        const mode = toTrimmedString(state.mode, 24).toLowerCase();
        if (mode === 'chooser' || mode === 'refresh') return mode;
        return 'full';
      })(),
      isActive: state.isActive === true,
      activeQuestionId,
      answeredQuestionIds,
      processedEvents: normalizeProcessedQuizEvents(state.processedEvents),
      answers: toProfile(state.answers),
      history,
      stepIndex: Number.isFinite(Number(state.stepIndex)) ? Math.max(0, Math.floor(Number(state.stepIndex))) : answeredQuestionIds.length,
      completed: state.completed === true,
      startedAt: toTrimmedString(state.startedAt || state.started_at, 80),
      started_at: toTrimmedString(state.started_at || state.startedAt, 80),
      updatedAt: toTrimmedString(state.updatedAt || state.updated_at, 80),
      updated_at: toTrimmedString(state.updated_at || state.updatedAt, 80),
      completedAt: toTrimmedString(state.completedAt || state.completed_at, 80),
      completed_at: toTrimmedString(state.completed_at || state.completedAt, 80),
    };
  }

  function findMyQuizQuestionById(questionBank, questionId) {
    const normalizedQuestionId = toTrimmedString(questionId, 80).toUpperCase();
    if (!normalizedQuestionId) return null;
    return (
      (Array.isArray(questionBank) ? questionBank : []).find(
        (item) => toTrimmedString(item?.questionId, 80).toUpperCase() === normalizedQuestionId,
      ) || null
    );
  }

  function pickNextMyQuizQuestion(questionBank, answeredQuestionIds, excludedQuestionId = '') {
    const answeredSet = new Set(
      (Array.isArray(answeredQuestionIds) ? answeredQuestionIds : [])
        .map((item) => toTrimmedString(item, 80).toUpperCase())
        .filter(Boolean),
    );
    const excluded = toTrimmedString(excludedQuestionId, 80).toUpperCase();
    if (excluded) answeredSet.add(excluded);

    for (const question of Array.isArray(questionBank) ? questionBank : []) {
      const questionId = toTrimmedString(question?.questionId, 80).toUpperCase();
      if (!questionId || answeredSet.has(questionId)) continue;
      return question;
    }
    return null;
  }

  function buildMyQuizStatePayload(state, questionBank) {
    const facts = toProfile(state?.answers);
    const expectedKeys = (Array.isArray(questionBank) ? questionBank : [])
      .map((item) => toTrimmedString(item?.questionKey, 64))
      .filter(Boolean);
    const missing = expectedKeys.filter((key) => !hasQuizFactValue(facts[key]));
    const confidence = expectedKeys.length ? Math.min(1, Math.max(0, 1 - missing.length / expectedKeys.length)) : 0;
    return normalizeQuizStatePayload(
      {
        facts,
        missing,
        confidence,
      },
      {
        facts: {},
        missing: [],
        confidence: 0,
      },
    );
  }

  function buildMyQuizDescriptionFromText(text, fallback = '') {
    const normalizedText = toTrimmedString(text, 2200);
    if (!normalizedText) return toTrimmedString(fallback, 2200);
    const sentences = normalizedText
      .split(/(?<=[.!?])\s+/g)
      .map((item) => toTrimmedString(item, 420))
      .filter(Boolean);
    if (!sentences.length) return normalizedText;
    return toTrimmedString(sentences.slice(0, 3).join(' '), 2200);
  }

  function buildMyQuizDraftUpdate(entityType, entityName, answers, existingDescription = '') {
    const facts = toProfile(answers);
    const role = toTrimmedString(facts.role_current, 120);
    const focus = toTrimmedString(facts.focus_main || facts.company_focus || facts.generic_goal, 120);
    const risk = toTrimmedString(facts.red_line || facts.company_main_risk || facts.generic_risk, 120);
    const fragments = [role, focus, risk].filter(Boolean);
    const description = fragments.length
      ? `${toTrimmedString(entityName, 120) || 'Сущность'}: ${fragments.join('. ')}.`
      : '';

    const fieldsPatch = {};
    if (focus) {
      fieldsPatch.tagsAdd = [focus];
    }
    if (risk) {
      fieldsPatch.risksAdd = [risk];
    }
    if (toTrimmedString(facts.generic_next_step || facts.refresh_next_step, 120)) {
      fieldsPatch.tasksAdd = [toTrimmedString(facts.generic_next_step || facts.refresh_next_step, 120)];
    }
    if (entityType === 'person' && role) {
      fieldsPatch.rolesAdd = [role];
    }

    return normalizeQuizDraftUpdate({
      description: buildMyQuizDescriptionFromText(description, existingDescription),
      fieldsPatch,
    });
  }

  function buildMyQuizRefreshDraftUpdate(answers) {
    const facts = toProfile(answers);
    return normalizeQuizDraftUpdate({
      description: '',
      fieldsPatch: {
        statusAdd: [toTrimmedString(facts.refresh_status, 120)].filter(Boolean),
        tasksAdd: [toTrimmedString(facts.refresh_next_step, 120)].filter(Boolean),
      },
    });
  }

  function normalizeMyQuizStringList(rawValue, maxItems = 24, maxLength = 160) {
    const source = Array.isArray(rawValue) ? rawValue : [rawValue];
    const dedup = new Set();
    const result = [];

    for (const item of source) {
      const value = toTrimmedString(item, maxLength);
      if (!value) continue;
      const key = value.toLowerCase();
      if (dedup.has(key)) continue;
      dedup.add(key);
      result.push(value);
      if (result.length >= maxItems) break;
    }

    return result;
  }

  function mapMyQuizModelFieldsToPatch(entityType, rawFields) {
    const allowedFields = getAllowedQuizFieldSet(entityType);
    const source = toProfile(rawFields);
    const patch = {};

    for (const [key, value] of Object.entries(source)) {
      const normalizedKey = toTrimmedString(key, 64);
      if (!normalizedKey) continue;

      let patchKey = '';
      if (QUIZ_FIELDS_PATCH_ALLOWED.has(normalizedKey)) {
        const fieldName = normalizedKey.endsWith('Add') ? normalizedKey.slice(0, -3) : '';
        if (fieldName && allowedFields.has(fieldName)) {
          patchKey = normalizedKey;
        }
      } else if (allowedFields.has(normalizedKey)) {
        patchKey = `${normalizedKey}Add`;
      }

      if (!patchKey) continue;
      const values = Array.isArray(value) ? value : [value];
      const maxLength = patchKey === 'linksAdd' ? 240 : 96;
      const normalizedValues = values
        .map((item) => toTrimmedString(item, maxLength))
        .filter(Boolean)
        .slice(0, 18);
      if (!normalizedValues.length) continue;
      patch[patchKey] = normalizedValues;
    }

    return patch;
  }

  function isLikelyRawMyQuizDescription(description, finalNotes) {
    const desc = toTrimmedString(description, 2200);
    const notes = toTrimmedString(finalNotes, 2200);
    if (!desc || !notes) return false;
    const normalize = (value) => value.replace(/\s+/g, ' ').trim().toLowerCase();
    const normalizedDesc = normalize(desc);
    const normalizedNotes = normalize(notes);
    if (!normalizedDesc || !normalizedNotes) return false;
    if (normalizedDesc === normalizedNotes) return true;
    if (normalizedNotes.length >= 300 && normalizedDesc.length >= Math.floor(normalizedNotes.length * 0.85)) {
      return true;
    }
    return false;
  }

  async function runMyQuizSmartFinalization({
    entity,
    entityType,
    entityName,
    aiMetadata,
    quizAnswers,
    finalNotes,
    includeDebug,
  }) {
    const currentDescription = toTrimmedString(aiMetadata.description, 2200);
    const currentFields = buildEntityAnalyzerCurrentFields(entity.type, aiMetadata);
    const baseSnapshot = {
      base_description: currentDescription,
      base_fields: currentFields,
      quiz_answers: toProfile(quizAnswers),
      new_notes: toTrimmedString(finalNotes, 2200),
    };

    const systemPrompt = [
      'Ты Synapse12 MY-Quiz Finalizer.',
      `Текущий тип сущности: ${entityType}.`,
      'Работай только с переданным контекстом.',
      'Задача: обновить профиль сущности через MERGE+ENRICH, а не REPLACE.',
      'Критично: НЕ копируй new_notes в description. Description должен быть сжатым резюме (1-3 предложения), а не стенограмма.',
      'Сохрани существующие факты: добавляй новые теги/роли/поля, не удаляй старые.',
      'При противоречиях не затирай данные молча: верни conflicts и needs_review=true.',
      'Верни СТРОГО JSON без markdown.',
      'Формат:',
      '{',
      '  "updated_description": "string",',
      '  "updated_fields": {',
      '    "tags": [], "markers": [], "roles": [], "skills": [], "links": [], "importance": [],',
      '    "industry": [], "departments": [], "stage": [], "risks": [], "date": [], "location": [],',
      '    "participants": [], "outcomes": [], "resources": [], "priority": [], "status": [], "owners": [], "metrics": []',
      '  },',
      '  "changed_fields": [],',
      '  "confidence": {},',
      '  "conflicts": [],',
      '  "needs_review": false',
      '}',
    ].join('\n');

    const userPrompt = [
      'Контекст MY-quiz finalization (JSON):',
      JSON.stringify(
        {
          entity: {
            id: String(entity._id),
            type: entityType,
            name: entityName,
          },
          ...baseSnapshot,
        },
        null,
        2,
      ),
    ].join('\n');

    const model = toTrimmedString(OPENAI_QUIZ_SMART_MODEL, 120) || toTrimmedString(OPENAI_MODEL, 120);
    let llmError = '';
    let aiRawReply = '';
    let aiParsedResponse = {};
    let aiUsage = null;
    let usedModel = '';
    let providerDebug = {};
    let draftUpdate = normalizeQuizDraftUpdate({
      description: '',
      fieldsPatch: {},
    });
    let changedFields = [];
    let confidence = {};
    let conflicts = [];
    let needsReview = false;

    try {
      const aiResponse = await aiProvider.requestOpenAiAgentReply({
        systemPrompt,
        userPrompt,
        includeRawPayload: includeDebug,
        model,
        temperature: 0.2,
        maxOutputTokens: 2600,
        timeoutMs: 90_000,
      });
      usedModel = toTrimmedString(aiResponse?.debug?.response?.model, 120) || model;
      aiUsage = aiResponse.usage;
      aiRawReply = aiResponse.reply || '';
      providerDebug = aiResponse.debug || {};
      aiParsedResponse = extractJsonObjectFromText(aiResponse.reply);
      const parsed = toProfile(aiParsedResponse);

      const hasAnalyzerShape =
        typeof parsed.status === 'string' ||
        (parsed.fields && typeof parsed.fields === 'object' && !parsed.updated_fields);
      if (hasAnalyzerShape) {
        const normalizedAnalysis = normalizeEntityAnalysisOutput(entity.type, parsed);
        const patch = mapMyQuizModelFieldsToPatch(entityType, normalizedAnalysis.fields);
        if (Array.isArray(normalizedAnalysis.ignoredNoise) && normalizedAnalysis.ignoredNoise.length) {
          patch.ignoredNoiseAdd = normalizeMyQuizStringList(normalizedAnalysis.ignoredNoise, 18, 120);
        }
        draftUpdate = normalizeQuizDraftUpdate({
          description: toTrimmedString(normalizedAnalysis.description, 2200),
          fieldsPatch: patch,
        });
        changedFields = Object.keys(patch)
          .map((key) => (key.endsWith('Add') ? key.slice(0, -3) : key))
          .filter(Boolean);
        confidence = toProfile(normalizedAnalysis.confidence);
        conflicts = normalizeMyQuizStringList(parsed.conflicts || normalizedAnalysis.ignoredNoise, 12, 180);
        needsReview = conflicts.length > 0;
      } else {
        const mappedPatch = mapMyQuizModelFieldsToPatch(
          entityType,
          parsed.updated_fields || parsed.fieldsPatch || parsed.fields || {},
        );
        if (Array.isArray(parsed.ignoredNoise) && parsed.ignoredNoise.length) {
          mappedPatch.ignoredNoiseAdd = normalizeMyQuizStringList(parsed.ignoredNoise, 18, 120);
        }
        draftUpdate = normalizeQuizDraftUpdate({
          description: toTrimmedString(parsed.updated_description || parsed.description, 2200),
          fieldsPatch: mappedPatch,
        });
        changedFields = normalizeMyQuizStringList(
          Array.isArray(parsed.changed_fields)
            ? parsed.changed_fields
            : Object.keys(mappedPatch).map((key) => (key.endsWith('Add') ? key.slice(0, -3) : key)),
          32,
          64,
        );
        confidence = toProfile(parsed.confidence);
        conflicts = normalizeMyQuizStringList(parsed.conflicts, 12, 180);
        needsReview = parsed.needs_review === true || conflicts.length > 0;
      }
    } catch (error) {
      llmError = toTrimmedString(error?.message, 220) || 'my_quiz_smart_finalization_failed';
    }

    if (isLikelyRawMyQuizDescription(draftUpdate.description, finalNotes)) {
      draftUpdate = normalizeQuizDraftUpdate({
        ...draftUpdate,
        description: '',
      });
    }

    if (!toTrimmedString(draftUpdate.description, 2200) && !hasQuizPatchValues(draftUpdate.fieldsPatch)) {
      const fallbackDraftUpdate = buildMyQuizDraftUpdate(entityType, entityName, quizAnswers, currentDescription);
      draftUpdate = mergeQuizDraftUpdates(draftUpdate, fallbackDraftUpdate);
    }

    return {
      draftUpdate: normalizeQuizDraftUpdate(draftUpdate),
      summary: {
        changed_fields: normalizeMyQuizStringList(changedFields, 32, 64),
        confidence: toProfile(confidence),
        conflicts,
        needs_review: needsReview,
        base_description: currentDescription,
        new_notes: toTrimmedString(finalNotes, 2200),
        updated_description: toTrimmedString(draftUpdate.description, 2200),
        updated_at: new Date().toISOString(),
      },
      llm: {
        model: usedModel || model,
        usage: aiUsage,
        llmError,
        aiRawReply,
        aiParsedResponse,
        provider: providerDebug,
        prompts: includeDebug
          ? {
              systemPrompt,
              userPrompt,
            }
          : undefined,
      },
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
      const clientEventId = toTrimmedString(req.body?.client_event_id, 120);
      const nowIso = new Date().toISOString();
      const myScenario = resolveMyQuizScenario(entityType, entity);

      if (myScenario) {
        const requestedQuestionId = toTrimmedString(
          req.body?.input?.activeQuestion?.questionId || req.body?.questionId,
          80,
        ).toUpperCase();
        let myFinalizationDebug = null;
        let myState = normalizeStoredMyQuizState(aiMetadata.quiz_my, {
          scenario: myScenario,
          entityType,
          entityName,
        });
        myState = enforceQuizStateInvariants(myState, true);

        const getQuestionBankByMode = (modeValue) => {
          const mode = toTrimmedString(modeValue, 24).toLowerCase();
          if (mode === 'chooser') return [MY_QUIZ_REENTRY_QUESTION];
          return buildMyQuizBankForScenario(myScenario, entityType, entityName, mode === 'refresh' ? 'refresh' : 'full');
        };

        const getCurrentQuestionBank = () => getQuestionBankByMode(myState.mode);

        const toDebugState = (state) => ({
          scenario: toTrimmedString(state?.scenario, 40),
          mode: toTrimmedString(state?.mode, 24),
          isActive: state?.isActive === true,
          completed: state?.completed === true,
          activeQuestionId: toTrimmedString(state?.activeQuestionId, 80),
          answeredQuestionIds: Array.isArray(state?.answeredQuestionIds) ? state.answeredQuestionIds : [],
          answers: toProfile(state?.answers),
          stepIndex: Number.isFinite(Number(state?.stepIndex)) ? Number(state.stepIndex) : 0,
          updatedAt: toTrimmedString(state?.updatedAt, 80),
        });

        const buildMyStepPayload = (question, extras = {}) => {
          const expectsType = toTrimmedString(question?.expectsType, 24).toLowerCase() === 'text' ? 'text' : 'choice_or_text';
          return {
            mode: 'quiz_step',
            entityType,
            questionId: toTrimmedString(question?.questionId, 80),
            questionText: toTrimmedString(question?.questionText, 320),
            options: expectsType === 'text' ? [] : normalizeMyQuizOptions(question?.options),
            expects: { type: expectsType },
            stopCheck: null,
            ...extras,
          };
        };

        const persistMyQuizStateAndRespond = async ({
          responsePayload,
          draftUpdate = { description: '', fieldsPatch: {} },
          debugExtra = {},
          statusCode = 200,
          eventIdToRemember = '',
        }) => {
          myState.updatedAt = nowIso;
          myState.updated_at = nowIso;
          const normalizedDraftUpdate = normalizeQuizDraftUpdate(draftUpdate);
          const questionBank = getCurrentQuestionBank();
          const nextMetadata = applyQuizDraftUpdateToMetadata(entity.type, aiMetadata, normalizedDraftUpdate);
          const payload = {
            ...responsePayload,
            quizMode: QUIZ_MODE_MY,
            myScenario,
            state: buildMyQuizStatePayload(myState, questionBank),
            draftUpdate: normalizedDraftUpdate,
            orchestrator: buildQuizOrchestratorPayload(myState, true),
          };

          if (eventIdToRemember) {
            myState.processedEvents = rememberProcessedQuizEvent(myState.processedEvents, eventIdToRemember, payload);
          }

          myState = enforceQuizStateInvariants(myState, true);
          nextMetadata.quiz_my = myState;
          entity.ai_metadata = nextMetadata;
          await entity.save();
          broadcastEntityEvent(ownerId, 'entity.updated', {
            entity: entity.toObject(),
          });

          if (includeDebug) {
            payload.debug = {
              quizMode: QUIZ_MODE_MY,
              scenario: myScenario,
              action,
              requestedQuestionId,
              state: toDebugState(myState),
              finalization: myFinalizationDebug,
              ...toProfile(debugExtra),
            };
          }

          return res.status(Number.isFinite(Number(statusCode)) ? Number(statusCode) : 200).json(payload);
        };

        const startMyQuizFull = (keepAnswers = false) => {
          const preservedAnswers = keepAnswers ? toProfile(myState.answers) : {};
          const previousStartedAt = keepAnswers ? toTrimmedString(myState.startedAt, 80) : '';
          myState = createInitialMyQuizState({
            scenario: myScenario,
            entityType,
            entityName,
            mode: 'full',
          });
          myState.answers = preservedAnswers;
          if (previousStartedAt) {
            myState.startedAt = previousStartedAt;
          }
          const questionBank = getCurrentQuestionBank();
          const firstQuestion = questionBank[0] || null;
          if (!firstQuestion) return null;
          myState.activeQuestionId = firstQuestion.questionId;
          return firstQuestion;
        };

        const startMyQuizRefresh = () => {
          myState.mode = 'refresh';
          myState.isActive = true;
          myState.completed = false;
          myState.completedAt = '';
          myState.completed_at = '';
          myState.answeredQuestionIds = [];
          myState.history = [];
          myState.stepIndex = 1;
          const questionBank = getCurrentQuestionBank();
          const firstQuestion = questionBank[0] || null;
          if (!firstQuestion) return null;
          myState.activeQuestionId = firstQuestion.questionId;
          return firstQuestion;
        };

        const openMyQuizChooser = () => {
          myState.mode = 'chooser';
          myState.isActive = true;
          myState.activeQuestionId = MY_QUIZ_REENTRY_QUESTION_ID;
          myState.stepIndex = Math.max(1, Number(myState.stepIndex) || 1);
          return MY_QUIZ_REENTRY_QUESTION;
        };

        const getActiveMyQuestion = () => {
          const questionBank = getCurrentQuestionBank();
          return findMyQuizQuestionById(questionBank, myState.activeQuestionId);
        };

        const buildMyCurrentQuestionResponse = (question, extras = {}) =>
          ({
            ...buildMyStepPayload(question, extras),
            quizMode: QUIZ_MODE_MY,
            myScenario,
            state: buildMyQuizStatePayload(myState, getCurrentQuestionBank()),
            draftUpdate: normalizeQuizDraftUpdate({
              description: '',
              fieldsPatch: {},
            }),
            orchestrator: buildQuizOrchestratorPayload(myState, true),
          });

        if (action === 'answer' && clientEventId) {
          const cachedEvent = findProcessedQuizEvent(myState.processedEvents, clientEventId);
          if (cachedEvent && Object.keys(toProfile(cachedEvent.response)).length) {
            const cachedResponse = toProfile(cachedEvent.response);
            const replayPayload = {
              ...cachedResponse,
              replayed: true,
              quizMode: toTrimmedString(cachedResponse.quizMode, 24) || QUIZ_MODE_MY,
              myScenario: toTrimmedString(cachedResponse.myScenario, 40) || myScenario,
            };
            return res.status(200).json(replayPayload);
          }
        }

        if (action === 'start') {
          if (myState.completed) {
            const chooserQuestion = openMyQuizChooser();
            return persistMyQuizStateAndRespond({
              responsePayload: buildMyStepPayload(chooserQuestion, { resumed: true }),
            });
          }

          const activeQuestion = getActiveMyQuestion();
          if (myState.isActive && activeQuestion) {
            return persistMyQuizStateAndRespond({
              responsePayload: buildMyStepPayload(activeQuestion, { resumed: true }),
            });
          }

          if (Array.isArray(myState.answeredQuestionIds) && myState.answeredQuestionIds.length) {
            myState.isActive = true;
            const nextQuestion = pickNextMyQuizQuestion(getCurrentQuestionBank(), myState.answeredQuestionIds);
            if (nextQuestion) {
              myState.activeQuestionId = nextQuestion.questionId;
              return persistMyQuizStateAndRespond({
                responsePayload: buildMyStepPayload(nextQuestion, { resumed: true }),
              });
            }
          }

          const firstQuestion = startMyQuizFull(false);
          if (!firstQuestion) {
            return persistMyQuizStateAndRespond({
              responsePayload: buildQuizCompletedPayload(entityType, 'Квиз завершён. Данные сохранены.', myState, {
                description: '',
                fieldsPatch: {},
              }),
            });
          }
          return persistMyQuizStateAndRespond({
            responsePayload: buildMyStepPayload(firstQuestion, { resumed: false }),
          });
        }

        if (!myState.isActive) {
          if (myState.completed) {
            const chooserQuestion = openMyQuizChooser();
            return persistMyQuizStateAndRespond({
              responsePayload: buildMyStepPayload(chooserQuestion, { resumed: true }),
            });
          }
          const firstQuestion = startMyQuizFull(true);
          if (!firstQuestion) {
            return persistMyQuizStateAndRespond({
              responsePayload: buildQuizCompletedPayload(entityType, 'Квиз завершён. Данные сохранены.', myState, {
                description: '',
                fieldsPatch: {},
              }),
            });
          }
          return persistMyQuizStateAndRespond({
            responsePayload: buildMyStepPayload(firstQuestion, { resumed: false }),
          });
        }

        const activeQuestion = getActiveMyQuestion();
        if (!activeQuestion) {
          const firstQuestion = startMyQuizFull(false);
          if (!firstQuestion) {
            return persistMyQuizStateAndRespond({
              responsePayload: buildQuizCompletedPayload(entityType, 'Квиз завершён. Данные сохранены.', myState, {
                description: '',
                fieldsPatch: {},
              }),
            });
          }
          return persistMyQuizStateAndRespond({
            responsePayload: buildMyStepPayload(firstQuestion, { resumed: false }),
          });
        }

        const activeQuestionIdUpper = toTrimmedString(activeQuestion.questionId, 80).toUpperCase();
        if (!requestedQuestionId) {
          return res.status(409).json({
            ...buildMyCurrentQuestionResponse(activeQuestion, {
              resumed: true,
              syncError: true,
            }),
            message: 'quiz_state_out_of_sync',
            expectedQuestionId: toTrimmedString(activeQuestion.questionId, 80),
            activeQuestionId: toTrimmedString(activeQuestion.questionId, 80),
            answeredQuestionIds: Array.isArray(myState.answeredQuestionIds) ? myState.answeredQuestionIds : [],
          });
        }
        if (requestedQuestionId && requestedQuestionId !== activeQuestionIdUpper) {
          const answeredSet = new Set(
            (Array.isArray(myState.answeredQuestionIds) ? myState.answeredQuestionIds : [])
              .map((item) => toTrimmedString(item, 80).toUpperCase())
              .filter(Boolean),
          );

          if (answeredSet.has(requestedQuestionId)) {
            return res.status(200).json(
              buildMyCurrentQuestionResponse(activeQuestion, {
                resumed: true,
                duplicate: true,
              }),
            );
          }

          return res.status(409).json({
            ...buildMyCurrentQuestionResponse(activeQuestion, {
              resumed: true,
              syncError: true,
            }),
            message: 'quiz_state_out_of_sync',
            expectedQuestionId: toTrimmedString(activeQuestion.questionId, 80),
            activeQuestionId: toTrimmedString(activeQuestion.questionId, 80),
            answeredQuestionIds: Array.isArray(myState.answeredQuestionIds) ? myState.answeredQuestionIds : [],
          });
        }

        const answer = parseQuizAnswer(
          req.body?.answerText || req.body?.message,
          req.body?.optionId,
          {
            options: normalizeMyQuizOptions(activeQuestion.options),
          },
        );
        if (!answer.answerText) {
          return res.status(400).json({ message: 'answerText or optionId is required' });
        }
        const answeredSet = new Set(
          (Array.isArray(myState.answeredQuestionIds) ? myState.answeredQuestionIds : [])
            .map((item) => toTrimmedString(item, 80))
            .filter(Boolean),
        );
        answeredSet.add(activeQuestion.questionId);
        myState.answeredQuestionIds = Array.from(answeredSet).slice(0, 120);
        if (toTrimmedString(activeQuestion.questionKey, 64)) {
          myState.answers = {
            ...toProfile(myState.answers),
            [activeQuestion.questionKey]: answer.answerText,
          };
        }
        myState.history = [
          ...(Array.isArray(myState.history) ? myState.history : []),
          {
            questionId: activeQuestion.questionId,
            questionKey: toTrimmedString(activeQuestion.questionKey, 64),
            answerText: answer.answerText,
            optionId: answer.optionId || '',
            at: nowIso,
          },
        ].slice(-QUIZ_HISTORY_LIMIT);
        myState.stepIndex = Math.max(1, Number(myState.stepIndex) || 1) + 1;

        if (activeQuestionIdUpper === MY_QUIZ_REENTRY_QUESTION_ID) {
          const answerLower = answer.answerText.toLowerCase();
          const chooseRefresh = answer.optionId === '1' || answerLower.includes('освеж');
          const chooseRestart = answer.optionId === '2' || answerLower.includes('занов') || answerLower.includes('сначала');
          if (chooseRefresh) {
            const firstQuestion = startMyQuizRefresh();
            if (!firstQuestion) {
              myState.isActive = false;
              myState.completed = true;
              myState.completedAt = nowIso;
              myState.completed_at = nowIso;
              myState.activeQuestionId = '';
              myState.mode = 'full';
              return persistMyQuizStateAndRespond({
                responsePayload: buildQuizCompletedPayload(entityType, 'Квиз завершён. Данные сохранены.', myState, {
                  description: '',
                  fieldsPatch: {},
                }),
                eventIdToRemember: clientEventId,
              });
            }
            return persistMyQuizStateAndRespond({
              responsePayload: buildMyStepPayload(firstQuestion, { resumed: false }),
              eventIdToRemember: clientEventId,
            });
          }

          if (chooseRestart) {
            const firstQuestion = startMyQuizFull(false);
            if (!firstQuestion) {
              myState.isActive = false;
              myState.completed = true;
              myState.completedAt = nowIso;
              myState.completed_at = nowIso;
              myState.activeQuestionId = '';
              return persistMyQuizStateAndRespond({
                responsePayload: buildQuizCompletedPayload(entityType, 'Квиз завершён. Данные сохранены.', myState, {
                  description: '',
                  fieldsPatch: {},
                }),
                eventIdToRemember: clientEventId,
              });
            }
            return persistMyQuizStateAndRespond({
              responsePayload: buildMyStepPayload(firstQuestion, { resumed: false }),
              eventIdToRemember: clientEventId,
            });
          }

          myState.isActive = false;
          myState.activeQuestionId = '';
          myState.mode = 'full';
          return persistMyQuizStateAndRespond({
            responsePayload: buildQuizCompletedPayload(entityType, 'Пауза. Данные сохранены.', myState, {
              description: '',
              fieldsPatch: {},
            }),
            eventIdToRemember: clientEventId,
          });
        }

        if (MY_QUIZ_SUFFICIENCY_IDS.has(activeQuestionIdUpper)) {
          const answerLower = answer.answerText.toLowerCase();
          const chooseDeep = answer.optionId === '2' || answerLower.includes('углуб');
          const choosePause = answer.optionId === '3' || answerLower.includes('пауза');
          if (choosePause) {
            myState.isActive = false;
            myState.activeQuestionId = '';
            return persistMyQuizStateAndRespond({
              responsePayload: buildQuizCompletedPayload(entityType, 'Пауза. Данные сохранены.', myState, {
                description: '',
                fieldsPatch: {},
              }),
              eventIdToRemember: clientEventId,
            });
          }

          if (chooseDeep) {
            myState.isActive = false;
            myState.activeQuestionId = '';
            myState.completed = true;
            myState.completedAt = nowIso;
            myState.completed_at = nowIso;
            myState.mode = 'full';
            return persistMyQuizStateAndRespond({
              responsePayload: buildQuizCompletedPayload(entityType, 'Ок, углубление добавим позже.', myState, {
                description: '',
                fieldsPatch: {},
              }),
              eventIdToRemember: clientEventId,
            });
          }

          const questionBank = getCurrentQuestionBank();
          const finalQuestion =
            questionBank.find((item) => MY_QUIZ_FINAL_FREEFORM_IDS.has(toTrimmedString(item?.questionId, 80).toUpperCase())) ||
            null;
          if (!finalQuestion) {
            myState.isActive = false;
            myState.activeQuestionId = '';
            myState.completed = true;
            myState.completedAt = nowIso;
            myState.completed_at = nowIso;
            myState.mode = 'full';
            return persistMyQuizStateAndRespond({
              responsePayload: buildQuizCompletedPayload(entityType, 'Квиз завершён. Данные сохранены.', myState, {
                description: '',
                fieldsPatch: {},
              }),
              eventIdToRemember: clientEventId,
            });
          }
          myState.isActive = true;
          myState.activeQuestionId = finalQuestion.questionId;
          return persistMyQuizStateAndRespond({
            responsePayload: buildMyStepPayload(finalQuestion, { resumed: false }),
            eventIdToRemember: clientEventId,
          });
        }

        if (activeQuestionIdUpper === MY_QUIZ_REFRESH_FINAL_ID && toTrimmedString(myState.mode, 24).toLowerCase() === 'refresh') {
          const refreshDraftUpdate = buildMyQuizRefreshDraftUpdate(myState.answers);
          myState.isActive = false;
          myState.activeQuestionId = '';
          myState.completed = true;
          myState.completedAt = nowIso;
          myState.completed_at = nowIso;
          myState.mode = 'full';
          return persistMyQuizStateAndRespond({
            responsePayload: buildQuizCompletedPayload(entityType, 'Квиз завершён. Данные сохранены.', myState, refreshDraftUpdate),
            draftUpdate: refreshDraftUpdate,
            eventIdToRemember: clientEventId,
          });
        }

        if (MY_QUIZ_FINAL_FREEFORM_IDS.has(activeQuestionIdUpper)) {
          const finalizationResult = await runMyQuizSmartFinalization({
            entity,
            entityType,
            entityName,
            aiMetadata,
            quizAnswers: myState.answers,
            finalNotes: answer.answerText,
            includeDebug,
          });
          myFinalizationDebug = includeDebug ? finalizationResult.llm : null;
          myState.summary = {
            ...toProfile(finalizationResult.summary),
            completed: true,
          };
          myState.isActive = false;
          myState.activeQuestionId = '';
          myState.completed = true;
          myState.completedAt = nowIso;
          myState.completed_at = nowIso;
          myState.mode = 'full';
          return persistMyQuizStateAndRespond({
            responsePayload: buildQuizCompletedPayload(
              entityType,
              'Квиз завершён. Данные сохранены.',
              myState,
              finalizationResult.draftUpdate,
            ),
            draftUpdate: finalizationResult.draftUpdate,
            eventIdToRemember: clientEventId,
          });
        }

        const nextQuestion = pickNextMyQuizQuestion(getCurrentQuestionBank(), myState.answeredQuestionIds, activeQuestion.questionId);
        if (!nextQuestion) {
          myState.isActive = false;
          myState.activeQuestionId = '';
          myState.completed = true;
          myState.completedAt = nowIso;
          myState.completed_at = nowIso;
          myState.mode = 'full';
          return persistMyQuizStateAndRespond({
            responsePayload: buildQuizCompletedPayload(entityType, 'Квиз завершён. Данные сохранены.', myState, {
              description: '',
              fieldsPatch: {},
            }),
            eventIdToRemember: clientEventId,
          });
        }

        myState.isActive = true;
        myState.activeQuestionId = nextQuestion.questionId;
        return persistMyQuizStateAndRespond({
          responsePayload: buildMyStepPayload(nextQuestion, { resumed: false }),
          eventIdToRemember: clientEventId,
        });
      }

      const storedState = normalizeStoredQuizState(aiMetadata.quiz_state, entityType, entityName);
      const normalizedStoredState = normalizeQuizFactsAndMissing(storedState, entityType);
      storedState.level = normalizeQuizLevel(storedState.level);
      storedState.facts = normalizedStoredState.facts;
      storedState.missing = normalizedStoredState.missing;
      storedState.confidence = normalizedStoredState.confidence;
      enforceQuizStateInvariants(storedState, false);
      const requestedQuestionId = toTrimmedString(
        req.body?.input?.activeQuestion?.questionId || req.body?.questionId,
        80,
      ).toUpperCase();

      if (action === 'answer' && clientEventId) {
        const cachedEvent = findProcessedQuizEvent(storedState.processedEvents, clientEventId);
        if (cachedEvent && Object.keys(toProfile(cachedEvent.response)).length) {
          const cachedResponse = toProfile(cachedEvent.response);
          return res.status(200).json({
            ...cachedResponse,
            replayed: true,
            quizMode: toTrimmedString(cachedResponse.quizMode, 24) || QUIZ_MODE_STANDARD,
          });
        }
      }

      const persistQuizState = async (nextState, metadataForPatch = aiMetadata) => {
        enforceQuizStateInvariants(nextState, false);
        const nextMetadata = {
          ...toProfile(metadataForPatch),
          quiz_state: nextState,
        };
        entity.ai_metadata = nextMetadata;
        await entity.save();
        broadcastEntityEvent(ownerId, 'entity.updated', {
          entity: entity.toObject(),
        });
      };

      const buildStepResponseFromQuestion = (question, state, draftUpdate, extras = {}) => {
        const expectsType =
          toTrimmedString(question?.expectsType, 24).toLowerCase() === 'text' ? 'text' : 'choice_or_text';
        return {
          mode: 'quiz_step',
          quizMode: QUIZ_MODE_STANDARD,
          entityType,
          questionId: question.questionId,
          questionText: question.questionText,
          options: expectsType === 'text' ? [] : normalizeQuizOptions(question.options),
          expects: { type: expectsType },
          state: normalizeQuizStatePayload(state, state),
          draftUpdate: normalizeQuizDraftUpdate(draftUpdate),
          stopCheck: null,
          orchestrator: buildQuizOrchestratorPayload(state, false),
          ...extras,
        };
      };

      const stopCheckQuestion = {
        questionId: 'stop_check',
        questionText: 'Данных достаточно или углубляемся?',
        options: normalizeQuizOptions([
          { id: '1', text: 'Достаточно — завершить' },
          { id: '2', text: 'Углубить' },
          { id: '3', text: 'Пауза' },
          { id: '4', text: 'Свой вариант' },
        ]),
      };
      const profileSummaryQuestion = buildQuizProfileSummaryQuestion(entityName);

      if (action === 'start') {
        if (storedState.isActive && toTrimmedString(storedState.lastQuestion?.mode, 24) === 'quiz_stop_check') {
          storedState.active = true;
          storedState.isActive = true;
          storedState.activeQuestionId = stopCheckQuestion.questionId;
          storedState.lastQuestion = {
            mode: 'quiz_stop_check',
            questionId: stopCheckQuestion.questionId,
            questionKey: '',
            questionText: stopCheckQuestion.questionText,
            options: normalizeQuizOptions(stopCheckQuestion.options),
          };
          storedState.updatedAt = nowIso;
          if (!storedState.stopSummary || !Object.keys(toProfile(storedState.stopSummary)).length) {
            storedState.stopSummary = buildQuizStopSummary(storedState, entityType);
          }
          await persistQuizState(storedState);

          return res.status(200).json({
            mode: 'quiz_stop_check',
            entityType,
            questionId: stopCheckQuestion.questionId,
            questionText: stopCheckQuestion.questionText,
            options: normalizeQuizOptions(stopCheckQuestion.options),
            expects: { type: 'choice_or_text' },
            state: normalizeQuizStatePayload(storedState, storedState),
            draftUpdate: {
              description: toTrimmedString(aiMetadata.description, 2200),
              fieldsPatch: {},
            },
            stopCheck: toProfile(storedState.stopSummary),
            resumed: true,
          });
        }

        const hasProfileSummaryActiveQuestion =
          isQuizProfileSummaryQuestionId(storedState.activeQuestionId) ||
          isQuizProfileSummaryQuestionId(storedState.lastQuestion?.questionId);
        const activeQuestion = hasProfileSummaryActiveQuestion
          ? profileSummaryQuestion
          : findQuizQuestionById(entityType, entityName, storedState.activeQuestionId) ||
            findQuizQuestionById(entityType, entityName, storedState.lastQuestion?.questionId);
        const activeQuestionId = toTrimmedString(activeQuestion?.questionId, 80).toUpperCase();
        const answeredSet = new Set(
          (Array.isArray(storedState.answeredQuestionIds) ? storedState.answeredQuestionIds : [])
            .map((item) => toTrimmedString(item, 80).toUpperCase())
            .filter(Boolean),
        );
        const activeQuestionAlreadyAnswered = activeQuestionId ? answeredSet.has(activeQuestionId) : false;
        const updatedAtMs = Date.parse(toTrimmedString(storedState.updatedAt, 80));
        const recentlyTouched = Number.isFinite(updatedAtMs) && Date.now() - updatedAtMs <= QUIZ_START_DEDUP_WINDOW_MS;
        if (
          storedState.isActive &&
          activeQuestion &&
          !activeQuestionAlreadyAnswered &&
          (storedState.activeQuestionId || recentlyTouched)
        ) {
          storedState.active = true;
          storedState.isActive = true;
          storedState.activeQuestionId = activeQuestion.questionId;
          storedState.lastQuestion = {
            mode: 'quiz_step',
            questionId: activeQuestion.questionId,
            questionKey: toTrimmedString(activeQuestion.questionKey, 64),
            questionText: activeQuestion.questionText,
            options: normalizeQuizOptions(activeQuestion.options),
          };
          storedState.updatedAt = nowIso;
          await persistQuizState(storedState);

          return res.status(200).json(
            buildStepResponseFromQuestion(
              activeQuestion,
              storedState,
              {
                description: toTrimmedString(aiMetadata.description, 2200),
                fieldsPatch: {},
              },
              { resumed: true },
            ),
          );
        }

        if (storedState.isActive && activeQuestionAlreadyAnswered) {
          const nextQuestion = chooseNextQuizQuestion(storedState, entityType, entityName, {
            excludeQuestionId: storedState.activeQuestionId,
          });
          if (nextQuestion) {
            storedState.active = true;
            storedState.isActive = true;
            storedState.activeQuestionId = nextQuestion.questionId;
            storedState.lastQuestion = {
              mode: 'quiz_step',
              questionId: nextQuestion.questionId,
              questionKey: toTrimmedString(nextQuestion.questionKey, 64),
              questionText: nextQuestion.questionText,
              options: normalizeQuizOptions(nextQuestion.options),
            };
            storedState.updatedAt = nowIso;
            await persistQuizState(storedState);
            return res.status(200).json(
              buildStepResponseFromQuestion(
                nextQuestion,
                storedState,
                {
                  description: toTrimmedString(aiMetadata.description, 2200),
                  fieldsPatch: {},
                },
                { resumed: true },
              ),
            );
          }
        }

        const firstQuestion = getQuizFirstQuestion(entityType, entityName);
        const nextState = createInitialQuizState(entityType, entityName, firstQuestion);
        nextState.updatedAt = nowIso;
        await persistQuizState(nextState);

        return res.status(200).json({
          ...firstQuestion,
          state: normalizeQuizStatePayload(nextState, nextState),
          resumed: false,
        });
      }

      if (!storedState.isActive || !storedState.activeQuestionId) {
        const firstQuestion = getQuizFirstQuestion(entityType, entityName);
        const nextState = createInitialQuizState(entityType, entityName, firstQuestion);
        nextState.updatedAt = nowIso;
        await persistQuizState(nextState);
        return res.status(200).json({
          ...firstQuestion,
          state: normalizeQuizStatePayload(nextState, nextState),
          resumed: false,
        });
      }

      const activeQuestionMode = toTrimmedString(storedState.lastQuestion?.mode, 24);
      const hasProfileSummaryActiveQuestion =
        isQuizProfileSummaryQuestionId(storedState.activeQuestionId) ||
        isQuizProfileSummaryQuestionId(storedState.lastQuestion?.questionId);
      let activeQuestion =
        activeQuestionMode === 'quiz_stop_check'
          ? stopCheckQuestion
          : hasProfileSummaryActiveQuestion
            ? profileSummaryQuestion
            : findQuizQuestionById(entityType, entityName, storedState.activeQuestionId) ||
              findQuizQuestionById(entityType, entityName, storedState.lastQuestion?.questionId);
      if (!activeQuestion) {
        const firstQuestion = getQuizFirstQuestion(entityType, entityName);
        const nextState = createInitialQuizState(entityType, entityName, firstQuestion);
        nextState.updatedAt = nowIso;
        await persistQuizState(nextState);
        return res.status(200).json({
          ...firstQuestion,
          state: normalizeQuizStatePayload(nextState, nextState),
          resumed: false,
        });
      }

      let quizDesyncDetected = false;
      const quizDesyncFixed = false;
      const activeQuestionIdUpper = toTrimmedString(activeQuestion.questionId, 80).toUpperCase();
      if (action === 'answer' && !requestedQuestionId) {
        return res.status(409).json(
          buildStepResponseFromQuestion(
            activeQuestion,
            storedState,
            {
              description: toTrimmedString(aiMetadata.description, 2200),
              fieldsPatch: {},
            },
            {
              resumed: true,
              syncError: true,
              message: 'quiz_state_out_of_sync',
              expectedQuestionId: toTrimmedString(activeQuestion.questionId, 80),
              activeQuestionId: toTrimmedString(activeQuestion.questionId, 80),
              answeredQuestionIds: Array.isArray(storedState.answeredQuestionIds) ? storedState.answeredQuestionIds : [],
            },
          ),
        );
      }
      if (action === 'answer' && requestedQuestionId && activeQuestionIdUpper && requestedQuestionId !== activeQuestionIdUpper) {
        quizDesyncDetected = true;
        const answeredSet = new Set(
          (Array.isArray(storedState.answeredQuestionIds) ? storedState.answeredQuestionIds : [])
            .map((item) => toTrimmedString(item, 80).toUpperCase())
            .filter(Boolean),
        );
        if (answeredSet.has(requestedQuestionId)) {
          if (toTrimmedString(activeQuestion.questionId, 32).toLowerCase() === 'stop_check') {
            return res.status(200).json({
              mode: 'quiz_stop_check',
              quizMode: QUIZ_MODE_STANDARD,
              entityType,
              questionId: stopCheckQuestion.questionId,
              questionText: stopCheckQuestion.questionText,
              options: normalizeQuizOptions(stopCheckQuestion.options),
              expects: { type: 'choice_or_text' },
              state: normalizeQuizStatePayload(storedState, storedState),
              draftUpdate: {
                description: toTrimmedString(aiMetadata.description, 2200),
                fieldsPatch: {},
              },
              stopCheck: toProfile(storedState.stopSummary || buildQuizStopSummary(storedState, entityType)),
              resumed: true,
              duplicate: true,
              orchestrator: buildQuizOrchestratorPayload(storedState, false),
            });
          }

          return res.status(200).json(
            buildStepResponseFromQuestion(
              activeQuestion,
              storedState,
              {
                description: toTrimmedString(aiMetadata.description, 2200),
                fieldsPatch: {},
              },
              {
                resumed: true,
                duplicate: true,
              },
            ),
          );
        }

        if (toTrimmedString(activeQuestion.questionId, 32).toLowerCase() === 'stop_check') {
          return res.status(409).json({
            mode: 'quiz_stop_check',
            quizMode: QUIZ_MODE_STANDARD,
            entityType,
            questionId: stopCheckQuestion.questionId,
            questionText: stopCheckQuestion.questionText,
            options: normalizeQuizOptions(stopCheckQuestion.options),
            expects: { type: 'choice_or_text' },
            state: normalizeQuizStatePayload(storedState, storedState),
            draftUpdate: {
              description: toTrimmedString(aiMetadata.description, 2200),
              fieldsPatch: {},
            },
            stopCheck: toProfile(storedState.stopSummary || buildQuizStopSummary(storedState, entityType)),
            resumed: true,
            syncError: true,
            message: 'quiz_state_out_of_sync',
            expectedQuestionId: toTrimmedString(activeQuestion.questionId, 80),
            activeQuestionId: toTrimmedString(activeQuestion.questionId, 80),
            answeredQuestionIds: Array.isArray(storedState.answeredQuestionIds) ? storedState.answeredQuestionIds : [],
            orchestrator: buildQuizOrchestratorPayload(storedState, false),
          });
        }

        return res.status(409).json(
          buildStepResponseFromQuestion(
            activeQuestion,
            storedState,
            {
              description: toTrimmedString(aiMetadata.description, 2200),
              fieldsPatch: {},
            },
            {
              resumed: true,
              syncError: true,
              message: 'quiz_state_out_of_sync',
              expectedQuestionId: toTrimmedString(activeQuestion.questionId, 80),
              activeQuestionId: toTrimmedString(activeQuestion.questionId, 80),
              answeredQuestionIds: Array.isArray(storedState.answeredQuestionIds) ? storedState.answeredQuestionIds : [],
            },
          ),
        );
      }

      const answer = parseQuizAnswer(
        req.body?.answerText || req.body?.message,
        req.body?.optionId,
        {
          options: activeQuestion.options,
        },
      );
      if (!answer.answerText) {
        return res.status(400).json({ message: 'answerText or optionId is required' });
      }

      if (toTrimmedString(activeQuestion.questionId, 32).toLowerCase() === 'stop_check') {
        const answerLower = answer.answerText.toLowerCase();
        const chooseDeep = answer.optionId === '2' || answerLower.includes('углуб') || answerLower.includes('deep');
        const choosePause = answer.optionId === '3' || answerLower.includes('пауза');

        if (choosePause) {
          storedState.level = 1;
          storedState.active = false;
          storedState.isActive = false;
          storedState.activeQuestionId = '';
          storedState.completedAt = nowIso;
          storedState.updatedAt = nowIso;
          storedState.lastQuestion = {
            mode: 'quiz_stop_check',
            questionId: stopCheckQuestion.questionId,
            questionKey: '',
            questionText: stopCheckQuestion.questionText,
            options: normalizeQuizOptions(stopCheckQuestion.options),
          };
          await persistQuizState(storedState);

          return res.status(200).json(
            buildQuizCompletedPayload(entityType, 'Квиз поставлен на паузу. Можно продолжить позже.', storedState, {
              description: '',
              fieldsPatch: {},
            }),
          );
        }

        if (!chooseDeep) {
          storedState.level = 1;
          storedState.active = true;
          storedState.isActive = true;
          storedState.activeQuestionId = profileSummaryQuestion.questionId;
          storedState.updatedAt = nowIso;
          storedState.lastQuestion = {
            mode: 'quiz_step',
            questionId: profileSummaryQuestion.questionId,
            questionKey: toTrimmedString(profileSummaryQuestion.questionKey, 64),
            questionText: profileSummaryQuestion.questionText,
            options: [],
          };
          await persistQuizState(storedState);

          return res.status(200).json(
            buildStepResponseFromQuestion(profileSummaryQuestion, storedState, {
              description: '',
              fieldsPatch: {},
            }),
          );
        }

        storedState.level = 2;
        storedState.active = true;
        storedState.isActive = true;
        const nextQuestionAfterDeep = chooseNextQuizQuestion(storedState, entityType, entityName, {
          excludeQuestionId: storedState.activeQuestionId,
        });
        if (!nextQuestionAfterDeep) {
          storedState.active = false;
          storedState.isActive = false;
          storedState.activeQuestionId = '';
          storedState.completedAt = nowIso;
          storedState.updatedAt = nowIso;
          await persistQuizState(storedState);
          return res.status(200).json(
            buildQuizCompletedPayload(entityType, 'Квиз завершён. Данные сохранены.', storedState, {
              description: '',
              fieldsPatch: {},
            }),
          );
        }

        storedState.activeQuestionId = nextQuestionAfterDeep.questionId;
        storedState.updatedAt = nowIso;
        storedState.lastQuestion = {
          mode: 'quiz_step',
          questionId: nextQuestionAfterDeep.questionId,
          questionKey: toTrimmedString(nextQuestionAfterDeep.questionKey, 64),
          questionText: nextQuestionAfterDeep.questionText,
          options: normalizeQuizOptions(nextQuestionAfterDeep.options),
        };
        await persistQuizState(storedState);

        return res.status(200).json(
          buildStepResponseFromQuestion(nextQuestionAfterDeep, storedState, {
            description: '',
            fieldsPatch: {},
          }),
        );
      }

      const activeQuestionId = toTrimmedString(activeQuestion.questionId, 80).toUpperCase();
      const answeredSet = new Set(
        (Array.isArray(storedState.answeredQuestionIds) ? storedState.answeredQuestionIds : [])
          .map((item) => toTrimmedString(item, 80).toUpperCase())
          .filter(Boolean),
      );
      if (activeQuestionId) {
        answeredSet.add(activeQuestionId);
      }
      storedState.answeredQuestionIds = Array.from(answeredSet).slice(0, 80);
      storedState.history = [
        ...storedState.history,
        {
          questionId: activeQuestion.questionId,
          questionKey: toTrimmedString(activeQuestion.questionKey, 64),
          questionText: activeQuestion.questionText,
          answerText: answer.answerText,
          optionId: answer.optionId || '',
          mode: 'quiz_step',
          at: nowIso,
        },
      ].slice(-QUIZ_HISTORY_LIMIT);
      storedState.stepCount = Math.max(0, Number(storedState.stepCount) || 0) + 1;
      storedState.stepIndex = Math.max(1, Number(storedState.stepIndex) || 1) + 1;
      if (Number(storedState.level) === 2) {
        storedState.level2Answers = Math.max(0, Number(storedState.level2Answers) || 0) + 1;
      } else {
        storedState.level1Answers = Math.max(0, Number(storedState.level1Answers) || 0) + 1;
      }

      const updatedStateFromAnswer = updateQuizFactsFromAnswer(
        storedState,
        {
          questionId: activeQuestion.questionId,
          questionKey: toTrimmedString(activeQuestion.questionKey, 64),
          questionText: activeQuestion.questionText,
        },
        answer,
        entityType,
      );
      storedState.facts = updatedStateFromAnswer.facts;
      storedState.missing = updatedStateFromAnswer.missing;
      storedState.confidence = updatedStateFromAnswer.confidence;

      const isProfileSummaryAnswer = isQuizProfileSummaryQuestionId(activeQuestion.questionId);
      if (isProfileSummaryAnswer && answer.answerText) {
        storedState.facts = {
          ...toProfile(storedState.facts),
          profile_summary: answer.answerText,
        };
      }
      const quizStepLimit = Number(storedState.level) >= 2 ? QUIZ_STOP_CHECK_MAX_STEPS + QUIZ_MAX_LEVEL2_QUESTIONS : QUIZ_STOP_CHECK_MAX_STEPS;
      const shouldStopCheck = !isProfileSummaryAnswer && (storedState.missing.length === 0 || Number(storedState.stepIndex) >= quizStepLimit);

      const currentFields = buildEntityAnalyzerCurrentFields(entity.type, aiMetadata);
      const forceStopCheck = shouldStopCheck;
      const normalizedPromptLevel = normalizeQuizLevel(storedState.level);
      const quizPromptState = {
        level: normalizedPromptLevel,
        stepCount: Math.max(0, Number(storedState.stepCount) || 0),
        level1Answers: Math.max(0, Number(storedState.level1Answers) || 0),
        level2Answers: Math.max(0, Number(storedState.level2Answers) || 0),
        facts: toProfile(storedState.facts),
        missing: Array.isArray(storedState.missing) ? storedState.missing : [],
        confidence: Number.isFinite(Number(storedState.confidence))
          ? Math.min(1, Math.max(0, Number(storedState.confidence)))
          : 0,
        history: Array.isArray(storedState.history) ? storedState.history.slice(-18) : [],
      };
      const systemPrompt = isProfileSummaryAnswer
        ? [
            'Ты Synapse12 Quiz Finalizer.',
            `Текущий тип сущности: ${entityType}.`,
            'Нужно финализировать профиль сущности после квиза.',
            'Используй ТОЛЬКО данные из входного JSON.',
            'Не обнуляй ранее собранные факты квиза: они остаются валидными и должны быть учтены.',
            'Если в финальном тексте есть противоречия к ранее собранным фактам, не стирай факты; добавь риск в risksAdd и помести спорный фрагмент в ignoredNoiseAdd.',
            'Верни строго JSON без markdown.',
            'Формат:',
            '{',
            '  "description": "string",',
            '  "fieldsPatch": {',
            '    "tagsAdd": [], "markersAdd": [], "rolesAdd": [], "skillsAdd": [], "risksAdd": [],',
            '    "statusAdd": [], "tasksAdd": [], "metricsAdd": [], "ownersAdd": [],',
            '    "participantsAdd": [], "resourcesAdd": [], "outcomesAdd": [], "industryAdd": [],',
            '    "departmentsAdd": [], "stageAdd": [], "dateAdd": [], "locationAdd": [],',
            '    "phonesAdd": [], "linksAdd": [], "importanceAdd": [], "ignoredNoiseAdd": []',
            '  }',
            '}',
          ].join('\n')
        : aiPrompts.buildEntityQuizSystemPrompt({
            entityType,
            level: normalizedPromptLevel,
            forceStopCheck,
          });
      const userPrompt = isProfileSummaryAnswer
        ? [
            'Контекст финализации квиза (JSON):',
            JSON.stringify(
              {
                entity: {
                  id: String(entity._id),
                  type: entityType,
                  name: entityName,
                  currentDescription: toTrimmedString(aiMetadata.description, 2200),
                  currentFields,
                },
                quiz: {
                  facts: toProfile(storedState.facts),
                  missing: Array.isArray(storedState.missing) ? storedState.missing : [],
                  history: Array.isArray(storedState.history) ? storedState.history.slice(-24) : [],
                  profileSummary: answer.answerText,
                },
              },
              null,
              2,
            ),
          ].join('\n')
        : aiPrompts.buildEntityQuizUserPrompt({
            entityType,
            name: entityName,
            currentDescription: toTrimmedString(aiMetadata.description, 2200),
            currentFields,
            quizState: quizPromptState,
            lastQuestion: {
              questionId: activeQuestion.questionId,
              questionKey: toTrimmedString(activeQuestion.questionKey, 64),
              questionText: activeQuestion.questionText,
              options: normalizeQuizOptions(activeQuestion.options),
              mode: 'quiz_step',
            },
            answer,
            forceStopCheck,
            level: normalizedPromptLevel,
          });

      const model = isProfileSummaryAnswer
        ? toTrimmedString(OPENAI_QUIZ_SMART_MODEL, 120) || toTrimmedString(OPENAI_MODEL, 120)
        : forceStopCheck
          ? toTrimmedString(OPENAI_QUIZ_SMART_MODEL, 120) || toTrimmedString(OPENAI_MODEL, 120)
          : toTrimmedString(OPENAI_QUIZ_FAST_MODEL, 120) || toTrimmedString(OPENAI_MODEL, 120);
      let aiUsage = null;
      let usedModel = '';
      let aiRawReply = '';
      let aiParsedResponse = {};
      let aiProviderDebug = {};
      let profileSummaryFallbackDebug = null;
      let draftUpdate = {
        description: '',
        fieldsPatch: {},
      };
      let llmError = '';
      try {
        const aiResponse = await aiProvider.requestOpenAiAgentReply({
          systemPrompt,
          userPrompt,
          includeRawPayload: includeDebug,
          model,
          temperature: 0.2,
          maxOutputTokens: isProfileSummaryAnswer ? 2400 : forceStopCheck ? 1800 : 900,
          timeoutMs: isProfileSummaryAnswer ? 90_000 : forceStopCheck ? 90_000 : 60_000,
        });
        usedModel = toTrimmedString(aiResponse?.debug?.response?.model, 120) || model;
        aiUsage = aiResponse.usage;
        aiRawReply = aiResponse.reply || '';
        aiProviderDebug = aiResponse.debug || {};
        aiParsedResponse = extractJsonObjectFromText(aiResponse.reply);
        if (isProfileSummaryAnswer) {
          const parsedResponse = toProfile(aiParsedResponse);
          const hasAnalyzerShape =
            typeof parsedResponse.status === 'string' ||
            (parsedResponse.fields && typeof parsedResponse.fields === 'object');
          if (hasAnalyzerShape) {
            const normalizedAnalysis = normalizeEntityAnalysisOutput(entity.type, parsedResponse);
            const fieldsPatchFromAnalysis = {};
            for (const [fieldKey, rawValues] of Object.entries(toProfile(normalizedAnalysis.fields))) {
              if (!Array.isArray(rawValues) || !rawValues.length) continue;
              fieldsPatchFromAnalysis[`${fieldKey}Add`] = rawValues;
            }
            if (Array.isArray(normalizedAnalysis.ignoredNoise) && normalizedAnalysis.ignoredNoise.length) {
              fieldsPatchFromAnalysis.ignoredNoiseAdd = normalizedAnalysis.ignoredNoise;
            }
            draftUpdate = normalizeQuizDraftUpdate({
              description: toTrimmedString(normalizedAnalysis.description, 2200),
              fieldsPatch: fieldsPatchFromAnalysis,
            });
          } else {
            draftUpdate = normalizeQuizDraftUpdate(parsedResponse.draftUpdate || parsedResponse);
          }

          const shouldRunFinalizerFallback = !hasQuizPatchValues(draftUpdate.fieldsPatch);
          if (shouldRunFinalizerFallback) {
            const fallbackSystemPrompt = aiPrompts.buildEntityAnalyzerSystemPrompt(entity.type);
            const fallbackQuizHistory = (Array.isArray(storedState.history) ? storedState.history : [])
              .slice(-18)
              .map((item) => {
                const row = toProfile(item);
                const questionText = toTrimmedString(row.questionText, 240);
                const answerText = toTrimmedString(row.answerText, 240);
                if (!questionText && !answerText) return '';
                return `${questionText || 'Вопрос'} -> ${answerText || 'Ответ отсутствует'}`;
              })
              .filter(Boolean);

            const fallbackMessage = [
              'Финальный ответ пользователя после квиза:',
              answer.answerText,
              '',
              'Факты квиза (JSON):',
              JSON.stringify(toProfile(storedState.facts), null, 2),
              '',
              'История последних шагов квиза:',
              fallbackQuizHistory.length ? fallbackQuizHistory.join('\n') : 'Нет данных',
              '',
              'Сформируй итоговое описание и заполни поля сущности без потери фактов.',
            ].join('\n');

            const fallbackUserPrompt = aiPrompts.buildEntityAnalyzerUserPrompt({
              entity,
              message: fallbackMessage,
              history: [],
              attachments: [],
              currentFields,
              voiceInput: '',
              documents: [],
            });

            try {
              const fallbackResponse = await aiProvider.requestOpenAiAgentReply({
                systemPrompt: fallbackSystemPrompt,
                userPrompt: fallbackUserPrompt,
                includeRawPayload: includeDebug,
                model: toTrimmedString(OPENAI_QUIZ_SMART_MODEL, 120) || toTrimmedString(OPENAI_MODEL, 120),
                temperature: 0.2,
                maxOutputTokens: 2400,
                timeoutMs: 90_000,
              });
              const fallbackParsed = extractJsonObjectFromText(fallbackResponse.reply);
              const normalizedFallback = normalizeEntityAnalysisOutput(entity.type, fallbackParsed);
              const fallbackPatchFromAnalysis = {};
              for (const [fieldKey, rawValues] of Object.entries(toProfile(normalizedFallback.fields))) {
                if (!Array.isArray(rawValues) || !rawValues.length) continue;
                fallbackPatchFromAnalysis[`${fieldKey}Add`] = rawValues;
              }
              if (Array.isArray(normalizedFallback.ignoredNoise) && normalizedFallback.ignoredNoise.length) {
                fallbackPatchFromAnalysis.ignoredNoiseAdd = normalizedFallback.ignoredNoise;
              }
              const fallbackDraftUpdate = normalizeQuizDraftUpdate({
                description: toTrimmedString(normalizedFallback.description, 2200),
                fieldsPatch: fallbackPatchFromAnalysis,
              });
              draftUpdate = mergeQuizDraftUpdates(draftUpdate, fallbackDraftUpdate);
              profileSummaryFallbackDebug = {
                model: toTrimmedString(fallbackResponse?.debug?.response?.model, 120),
                usage: fallbackResponse.usage || null,
                aiRawReply: fallbackResponse.reply || '',
                aiParsedResponse: fallbackParsed,
                llmError: '',
              };
            } catch (fallbackError) {
              const fallbackErrorMessage =
                toTrimmedString(fallbackError?.message, 220) || 'profile_summary_fallback_failed';
              profileSummaryFallbackDebug = {
                model: '',
                usage: null,
                aiRawReply: '',
                aiParsedResponse: {},
                llmError: fallbackErrorMessage,
              };
              if (!llmError) {
                llmError = fallbackErrorMessage;
              }
            }
          }

          if (!toTrimmedString(draftUpdate.description, 2200)) {
            draftUpdate = normalizeQuizDraftUpdate({
              ...draftUpdate,
              description: answer.answerText,
            });
          }
        } else {
          draftUpdate = normalizeQuizDraftUpdate(toProfile(aiParsedResponse).draftUpdate || aiParsedResponse);
        }
      } catch (error) {
        llmError = toTrimmedString(error?.message, 220) || 'quiz_draft_update_failed';
      }

      let responsePayload;
      if (isProfileSummaryAnswer) {
        storedState.level = 1;
        storedState.active = false;
        storedState.isActive = false;
        storedState.activeQuestionId = '';
        storedState.completedAt = nowIso;
        storedState.updatedAt = nowIso;
        storedState.lastQuestion = {
          mode: 'quiz_step',
          questionId: profileSummaryQuestion.questionId,
          questionKey: toTrimmedString(profileSummaryQuestion.questionKey, 64),
          questionText: profileSummaryQuestion.questionText,
          options: [],
        };
        responsePayload = buildQuizCompletedPayload(entityType, 'Квиз завершён. Данные сохранены.', storedState, draftUpdate);
      } else if (shouldStopCheck) {
        storedState.active = true;
        storedState.isActive = true;
        storedState.activeQuestionId = stopCheckQuestion.questionId;
        storedState.stopSummary = buildQuizStopSummary(storedState, entityType);
        storedState.lastQuestion = {
          mode: 'quiz_stop_check',
          questionId: stopCheckQuestion.questionId,
          questionKey: '',
          questionText: stopCheckQuestion.questionText,
          options: normalizeQuizOptions(stopCheckQuestion.options),
        };
        storedState.updatedAt = nowIso;
        responsePayload = {
          mode: 'quiz_stop_check',
          entityType,
          questionId: stopCheckQuestion.questionId,
          questionText: stopCheckQuestion.questionText,
          options: normalizeQuizOptions(stopCheckQuestion.options),
          expects: { type: 'choice_or_text' },
          state: normalizeQuizStatePayload(storedState, storedState),
          draftUpdate: normalizeQuizDraftUpdate(draftUpdate),
          stopCheck: toProfile(storedState.stopSummary),
        };
      } else {
        const nextQuestion = chooseNextQuizQuestion(storedState, entityType, entityName, {
          excludeQuestionId: activeQuestion.questionId,
        });
        if (!nextQuestion) {
          storedState.active = true;
          storedState.isActive = true;
          storedState.activeQuestionId = stopCheckQuestion.questionId;
          storedState.stopSummary = buildQuizStopSummary(storedState, entityType);
          storedState.lastQuestion = {
            mode: 'quiz_stop_check',
            questionId: stopCheckQuestion.questionId,
            questionKey: '',
            questionText: stopCheckQuestion.questionText,
            options: normalizeQuizOptions(stopCheckQuestion.options),
          };
          storedState.updatedAt = nowIso;
          responsePayload = {
            mode: 'quiz_stop_check',
            entityType,
            questionId: stopCheckQuestion.questionId,
            questionText: stopCheckQuestion.questionText,
            options: normalizeQuizOptions(stopCheckQuestion.options),
            expects: { type: 'choice_or_text' },
            state: normalizeQuizStatePayload(storedState, storedState),
            draftUpdate: normalizeQuizDraftUpdate(draftUpdate),
            stopCheck: toProfile(storedState.stopSummary),
          };
        } else {
          storedState.active = true;
          storedState.isActive = true;
          storedState.activeQuestionId = nextQuestion.questionId;
          storedState.lastQuestion = {
            mode: 'quiz_step',
            questionId: nextQuestion.questionId,
            questionKey: toTrimmedString(nextQuestion.questionKey, 64),
            questionText: nextQuestion.questionText,
            options: normalizeQuizOptions(nextQuestion.options),
          };
          storedState.updatedAt = nowIso;
          responsePayload = buildStepResponseFromQuestion(nextQuestion, storedState, draftUpdate);
        }
      }

      const responseEnvelope = {
        ...responsePayload,
        quizMode: toTrimmedString(responsePayload?.quizMode, 24) || QUIZ_MODE_STANDARD,
        orchestrator: buildQuizOrchestratorPayload(storedState, false),
      };
      if (action === 'answer' && clientEventId) {
        storedState.processedEvents = rememberProcessedQuizEvent(
          storedState.processedEvents,
          clientEventId,
          responseEnvelope,
        );
      }

      const nextMetadata = applyQuizDraftUpdateToMetadata(entity.type, aiMetadata, responsePayload.draftUpdate);
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
              requestedQuestionId,
              answer,
              activeQuestion: {
                questionId: activeQuestion.questionId,
                questionKey: toTrimmedString(activeQuestion.questionKey, 64),
                questionText: activeQuestion.questionText,
              },
              quizSync: {
                mismatchDetected: quizDesyncDetected,
                mismatchFixed: quizDesyncFixed,
              },
              storedState: {
                isActive: storedState.isActive,
                activeQuestionId: storedState.activeQuestionId,
                answeredQuestionIds: storedState.answeredQuestionIds,
                stepIndex: storedState.stepIndex,
                level: storedState.level,
                missing: storedState.missing,
              },
            },
            prompts: {
              systemPrompt,
              userPrompt,
            },
            response: {
              mode: responsePayload.mode,
              questionId: responsePayload.questionId,
              questionText: responsePayload.questionText,
              draftUpdate: responsePayload.draftUpdate,
              stopCheck: responsePayload.stopCheck,
              model: usedModel,
              usage: aiUsage,
              llmError,
              aiRawReply,
              aiParsedResponse,
              profileSummaryFallback: profileSummaryFallbackDebug,
            },
            provider: aiProviderDebug,
          }
        : undefined;

      return res.status(200).json({
        ...responseEnvelope,
        ...(usedModel ? { model: usedModel } : {}),
        ...(aiUsage ? { usage: aiUsage } : {}),
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
