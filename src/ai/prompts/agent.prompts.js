const SYSTEM_CONTEXT_KEYS_TO_DROP = new Set(['__v', 'createdAt', 'updatedAt']);
const BASE_AGENT_PROFILE = 'Ты аналитик Synapse12. Твоя цель: быстрый практичный анализ и следующий осмысленный шаг.';

const STRICT_FORMATTING_RULES = `
ПРАВИЛА ВЫДАЧИ (КРИТИЧЕСКИ ВАЖНО):
1. ПИШИ ТОЛЬКО ЧИСТЫМ ТЕКСТОМ.
2. КАТЕГОРИЧЕСКИ ЗАПРЕЩЕНО использовать Markdown (звездочки **, решетки #, списки -, _, жирный шрифт, курсив).
3. ЗАПРЕЩЕНО использовать эмодзи.
4. По умолчанию дай ОДИН короткий абзац: прямой ответ на вопрос пользователя + минимум необходимой аргументации из контекста.
5. Не показывай внутренний анализ, цепочку рассуждений, промежуточные шаги и служебные метки.
6. Не пересказывай весь контекст: используй только релевантные сигналы, которые реально влияют на вывод.
7. Развернутое объяснение давай только если пользователь явно просит разбор/обоснование.
`.trim();

const ALLOWED_ROUTER_ROLES = new Set(['investor', 'hr', 'strategist', 'default']);
const ROLE_ON_DEMAND_SELECTION_LIMIT = 3;
const ROLE_ON_DEMAND_MIN_SELECTION = 1;
const ROLE_ON_DEMAND_CATALOG = Object.freeze([
  {
    key: 'structure_mapper',
    name: 'Картограф структуры',
    playbook: 'Разложи задачу по сущностям, связям, узким местам и зависимостям.',
    keywords: ['структур', 'карта', 'связ', 'граф', 'узел', 'система', 'контекст', 'entity', 'topology'],
  },
  {
    key: 'meaning_editor',
    name: 'Смысловой редактор',
    playbook: 'Убери размытость формулировок и оставь проверяемые, операционные тезисы.',
    keywords: ['переформулир', 'смысл', 'ясно', 'четко', 'формулиров', 'позиционир', 'коммуникац'],
  },
  {
    key: 'financial_analyst',
    name: 'Финансовый аналитик',
    playbook: 'Фокус на деньгах: экономика, денежный поток, маржа, окупаемость.',
    keywords: [
      'бюджет',
      'деньги',
      'финанс',
      'roi',
      'cac',
      'ltv',
      'mrr',
      'arr',
      'доход',
      'профит',
      'дефицит',
      'расход',
      'выручк',
      'маржа',
      'cash',
      'окуп',
      'экономик',
    ],
  },
  {
    key: 'operations_analyst',
    name: 'Операционный аналитик',
    playbook: 'Проверь процессы, ресурсы, загрузку команды, bottlenecks и SLA.',
    keywords: [
      'операц',
      'процесс',
      'регламент',
      'поставка',
      'логист',
      'срок',
      'дедлайн',
      'команда',
      'ресурс',
      'исполнен',
      'sla',
    ],
  },
  {
    key: 'risk_analyst',
    name: 'Риск-аналитик',
    playbook: 'Ищи вероятность срыва, цену ошибки и защитные меры.',
    keywords: ['риск', 'угроза', 'срыв', 'потер', 'штраф', 'регулятор', 'sensitivity', 'неопредел'],
  },
  {
    key: 'strategist',
    name: 'Стратег',
    playbook: 'Сфокусируйся на рычагах роста, позиционировании и выборе направления.',
    keywords: ['стратег', 'направлен', 'позиционир', 'рынок', 'экспанс', 'growth', 'масштаб', 'долгосроч'],
  },
  {
    key: 'tactician_7_30',
    name: 'Тактик (7–30 дней)',
    playbook: 'Преобразуй идею в короткий исполнимый план на 7-30 дней.',
    keywords: ['7', '14', '30', 'недел', 'месяц', 'спринт', 'быстро', 'завтра', 'план', 'roadmap'],
  },
  {
    key: 'marketing_decoder',
    name: 'Маркетинговый дешифровщик',
    playbook: 'Разбери аудиторию, оффер, канал и воронку спроса.',
    keywords: [
      'маркет',
      'бренд',
      'аудитор',
      'сегмент',
      'воронк',
      'лид',
      'трафик',
      'контент',
      'креатив',
      'позиционир',
      'utm',
    ],
  },
  {
    key: 'sales_negotiator',
    name: 'Продажник/переговорщик',
    playbook: 'Определи тактику сделки, аргументы, уступки и закрытие.',
    keywords: ['продаж', 'сделк', 'переговор', 'клиент', 'возражен', 'чек', 'лид', 'закрыт', 'договор'],
  },
  {
    key: 'negotiator',
    name: 'Переговорщик',
    playbook: 'Подготовь и проведи сложные переговоры: BATNA, цели, рамки уступок, якорь, сценарии и фиксация договоренностей.',
    keywords: [
      'переговор',
      'сложн',
      'конфликт',
      'эскалац',
      'оппонент',
      'давление',
      'batna',
      'уступк',
      'якор',
      'аргумент',
      'позици',
      'договорен',
      'подготовк',
    ],
  },
  {
    key: 'product_analyst',
    name: 'Продуктовый аналитик',
    playbook: 'Свяжи пользовательскую проблему, ценность и продуктовые метрики.',
    keywords: ['продукт', 'фича', 'mvp', 'retention', 'конверс', 'onboarding', 'ux', 'jtbd', 'гипотез'],
  },
  {
    key: 'contradiction_detector',
    name: 'Детектор противоречий',
    playbook: 'Найди несостыковки между фактами, целями и ограничениями.',
    keywords: ['противореч', 'несостык', 'конфликт', 'взаимоисключ', 'не сходится', 'не совпад', 'расхожд'],
  },
  {
    key: 'omission_detector',
    name: 'Детектор упущений',
    playbook: 'Покажи, каких данных/шагов не хватает для качественного решения.',
    keywords: ['упустил', 'не хватает', 'пробел', 'слепая зона', 'чего нет', 'missing'],
  },
  {
    key: 'prioritizer',
    name: 'Приоритизатор',
    playbook: 'Отранжируй действия по эффекту, срочности и стоимости.',
    keywords: ['приоритет', 'сначала', 'первым', 'очеред', 'самое важное', 'focus', 'rank', 'конкретно', 'пошаг'],
  },
  {
    key: 'hidden_potential_hunter',
    name: 'Охотник за скрытым потенциалом',
    playbook: 'Найди недоиспользованные активы, связи и короткие рычаги роста.',
    keywords: ['рычаг', 'скрыт', 'резерв', 'потенциал', 'неочевид', 'быстрый рост', 'leverage'],
  },
  {
    key: 'illusion_breaker',
    name: 'Разрушитель иллюзий',
    playbook: 'Отдели гипотезы от фактов, вскрой ложные допущения.',
    keywords: ['иллюз', 'самообман', 'допущен', 'провер', 'реальн', 'факт', 'wishful'],
  },
  {
    key: 'change_archivist',
    name: 'Архивариус изменений',
    playbook: 'Сопоставь новый запрос с прошлым контекстом и зафиксируй сдвиг.',
    keywords: ['изменен', 'раньше', 'теперь', 'обнови', 'динамик', 'история', 'эволюц'],
  },
]);
const ROLE_ON_DEMAND_BY_KEY = Object.freeze(
  ROLE_ON_DEMAND_CATALOG.reduce((acc, role) => {
    acc[role.key] = role;
    return acc;
  }, {}),
);
const LEGACY_ROLE_HINT_TO_ON_DEMAND = Object.freeze({
  investor: ['financial_analyst', 'risk_analyst'],
  strategist: ['strategist', 'hidden_potential_hunter'],
  hr: ['operations_analyst', 'contradiction_detector'],
  default: ['structure_mapper'],
});
const ROLE_SELECTION_GROUPS = Object.freeze({
  structure_mapper: 'structure',
  meaning_editor: 'structure',
  contradiction_detector: 'structure',
  omission_detector: 'structure',
  change_archivist: 'structure',
  strategist: 'strategy',
  tactician_7_30: 'strategy',
  prioritizer: 'strategy',
  hidden_potential_hunter: 'strategy',
  financial_analyst: 'finance',
  risk_analyst: 'finance',
  operations_analyst: 'operations',
  marketing_decoder: 'growth',
  product_analyst: 'growth',
  sales_negotiator: 'negotiation',
  negotiator: 'negotiation',
  illusion_breaker: 'reality',
});
const ROLE_SELECTION_NEGOTIATION_KEYS = new Set(['sales_negotiator', 'negotiator']);
const ROLE_SELECTION_DIVERSITY_DUPLICATE_MIN_SCORE = 8;
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
const AGENT_LLM_HISTORY_MAX_ITEMS = 6;
const AGENT_LLM_HISTORY_ITEM_MAX_LENGTH = 1200;
const AGENT_LLM_ATTACHMENT_MAX_ITEMS = 4;
const AGENT_LLM_ATTACHMENT_TEXT_MAX_LENGTH = 1600;
const AGENT_STATE_SIGNAL_MAX_ITEMS = 6;
const AGENT_STATE_SIGNAL_MAX_LENGTH = 220;

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

function normalizeEntityId(value, maxLength = 120) {
  if (typeof value === 'string') {
    return value.trim().slice(0, maxLength);
  }
  if (typeof value === 'number' && Number.isFinite(value)) {
    return String(value).slice(0, maxLength);
  }
  if (value && typeof value === 'object' && typeof value.toString === 'function') {
    const asString = value.toString();
    if (typeof asString === 'string' && asString !== '[object Object]') {
      return asString.trim().slice(0, maxLength);
    }
  }
  return '';
}

function collectEntitySemanticSignals(entity) {
  if (!entity || typeof entity !== 'object') return [];

  const metadata = entity.ai_metadata && typeof entity.ai_metadata === 'object' ? entity.ai_metadata : {};
  const directTags = Array.isArray(entity.tags) ? entity.tags : [];
  const directRoles = Array.isArray(entity.roles) ? entity.roles : [];
  const metaTags = Array.isArray(metadata.tags) ? metadata.tags : [];
  const metaRoles = Array.isArray(metadata.roles) ? metadata.roles : [];
  const directName = typeof entity.name === 'string' ? entity.name : '';
  const directType = typeof entity.type === 'string' ? entity.type : '';

  return [...directTags, ...directRoles, ...metaTags, ...metaRoles, directName, directType]
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


function createAgentPrompts(deps) {
  const {
    AI_CONTEXT_ENTITY_LIMIT,
    toTrimmedString,
    toProfile,
    getEntityAnalyzerFields,
    normalizeDescriptionHistory,
    normalizeImportanceHistory,
  } = deps;

  function normalizeSignalText(value, maxLength = 24_000) {
    return toTrimmedString(value, maxLength).toLowerCase();
  }

  function countKeywordHits(text, keywords) {
    const source = typeof text === 'string' ? text : '';
    if (!source) return 0;
    const list = Array.isArray(keywords) ? keywords : [];
    return list.reduce((count, keyword) => {
      const normalized = toTrimmedString(keyword, 64).toLowerCase();
      if (!normalized) return count;
      return source.includes(normalized) ? count + 1 : count;
    }, 0);
  }

  function countWordLikeTokens(value) {
    const normalized = toTrimmedString(value, 2000);
    if (!normalized) return 0;
    return normalized.split(/[\s,.;:!?()\[\]{}"']+/g).map((item) => item.trim()).filter(Boolean).length;
  }

  function isVagueFollowUpMessage(value) {
    const normalized = toTrimmedString(value, 2400).toLowerCase();
    if (!normalized) return true;
    const wordCount = countWordLikeTokens(normalized);
    const vaguePattern = /^(ок|окей|ясно|понял|понятно|и что|что дальше|так что|так что конкретно|конкретно|ну и|дальше)\??$/i;
    return wordCount <= 4 || vaguePattern.test(normalized);
  }

  function sumWeightedHits(weightedSignals, keywords) {
    const source = Array.isArray(weightedSignals) ? weightedSignals : [];
    let total = 0;
    for (const signal of source) {
      const weight = Number(signal?.weight);
      if (!Number.isFinite(weight) || weight <= 0) continue;
      const text = normalizeSignalText(signal?.text || '', 24_000);
      if (!text) continue;
      const hits = countKeywordHits(text, keywords);
      if (!hits) continue;
      total += hits * weight;
    }
    return total;
  }

  function buildRoleSelectionSignals({
    payloadContext,
    stateSnapshot,
    message,
    entities,
    connections,
    attachments,
    stage,
  }) {
    const currentMessage = toTrimmedString(message, 2400);
    const currentRequest = toTrimmedString(stateSnapshot?.currentUserRequest, 1200);
    const latestUserSignals = (Array.isArray(stateSnapshot?.latestUserSignals) ? stateSnapshot.latestUserSignals : [])
      .map((item) => toTrimmedString(item, 280))
      .filter(Boolean);
    const recentUserTurns = (Array.isArray(stateSnapshot?.recentUserTurns) ? stateSnapshot.recentUserTurns : [])
      .map((item) => toTrimmedString(item, 420))
      .filter(Boolean);
    const goalsText = (Array.isArray(stateSnapshot?.goals) ? stateSnapshot.goals : [])
      .map((goal) => {
        const row = toProfile(goal);
        return [toTrimmedString(row.name, 160), toTrimmedString(row.description, 320)].filter(Boolean).join(' ');
      })
      .filter(Boolean)
      .join('\n');
    const entityFactsText = entities
      .slice(0, 120)
      .map((entity) => {
        const row = toProfile(entity);
        return [toTrimmedString(row.type, 40), toTrimmedString(row.name, 180)].filter(Boolean).join(' ');
      })
      .filter(Boolean)
      .join('\n');
    const entityDescriptionsText = entities
      .slice(0, 80)
      .map((entity) => {
        const row = toProfile(entity);
        return toTrimmedString(row.description, 260);
      })
      .filter(Boolean)
      .join('\n');
    const connectionLabelsText = connections
      .slice(0, 140)
      .map((edge) => {
        const row = toProfile(edge);
        return [toTrimmedString(row.type, 48), toTrimmedString(row.label, 180)].filter(Boolean).join(' ');
      })
      .filter(Boolean)
      .join('\n');
    const attachmentsText = attachments
      .slice(0, 8)
      .map((attachment) => {
        const row = toProfile(attachment);
        return [toTrimmedString(row.name, 80), toTrimmedString(row.contentCategory, 24), toTrimmedString(row.text, 420)]
          .filter(Boolean)
          .join(' ');
      })
      .filter(Boolean)
      .join('\n');
    const scopeSignals = [
      toTrimmedString(payloadContext?.scope?.name, 120),
      toTrimmedString(payloadContext?.scope?.projectName, 120),
      toTrimmedString(payloadContext?.scope?.entityType, 120),
    ]
      .filter(Boolean)
      .join('\n');
    const userIntentText = [
      currentMessage,
      currentRequest,
      ...latestUserSignals,
      ...recentUserTurns.slice(-2),
      goalsText,
    ]
      .filter(Boolean)
      .join('\n');
    const followUpVague = stage === 'follow_up' && isVagueFollowUpMessage(currentMessage);
    const weightedSignals = [
      { id: 'current_message', text: currentMessage, weight: followUpVague ? 1.2 : 3.4 },
      { id: 'state_current_request', text: currentRequest, weight: 2.6 },
      { id: 'latest_user_signals', text: latestUserSignals.join('\n'), weight: 2.4 },
      { id: 'recent_user_turns', text: recentUserTurns.join('\n'), weight: followUpVague ? 3.6 : 2.8 },
      { id: 'goals', text: goalsText, weight: 2.6 },
      { id: 'entity_facts', text: entityFactsText, weight: 1.1 },
      { id: 'entity_descriptions', text: entityDescriptionsText, weight: 0.55 },
      { id: 'connections', text: connectionLabelsText, weight: 0.8 },
      { id: 'attachments', text: attachmentsText, weight: 1.3 },
      { id: 'scope', text: scopeSignals, weight: 0.8 },
    ];

    return {
      currentMessage,
      currentRequest,
      userIntentText,
      followUpVague,
      weightedSignals,
      combinedSignalText: normalizeSignalText(
        weightedSignals.map((item) => `${item.id}: ${item.text}`).join('\n'),
        36_000,
      ),
      contextHeavyText: normalizeSignalText([entityFactsText, entityDescriptionsText, connectionLabelsText].join('\n'), 24_000),
    };
  }

  function applyRoleSelectionCoverage({
    selected,
    sorted,
    userIntentText,
    followUpVague,
    scopeType,
  }) {
    const nextSelected = Array.isArray(selected) ? [...selected] : [];
    const selectedKeys = new Set(nextSelected.map((item) => item.key));
    const selectedGroups = new Set(nextSelected.map((item) => ROLE_SELECTION_GROUPS[item.key] || 'general'));
    const intent = normalizeSignalText(userIntentText, 8000);

    function upsertRoleIfNeeded(roleKeys, reason) {
      if (roleKeys.some((key) => selectedKeys.has(key))) return;
      const candidate = sorted.find((item) => roleKeys.includes(item.key));
      const fallbackKey = roleKeys.find((key) => ROLE_ON_DEMAND_BY_KEY[key]);
      if (!candidate && !fallbackKey) return;
      const withReason = candidate
        ? {
          ...candidate,
          reasons: [...candidate.reasons, reason],
        }
        : {
          ...ROLE_ON_DEMAND_BY_KEY[fallbackKey],
          score: 1.2,
          reasons: [`${reason} (fallback injection)`],
        };
      if (nextSelected.length < ROLE_ON_DEMAND_SELECTION_LIMIT) {
        nextSelected.push(withReason);
        selectedKeys.add(withReason.key);
        selectedGroups.add(ROLE_SELECTION_GROUPS[withReason.key] || 'general');
        return;
      }
      const replacementIndex = nextSelected
        .map((item, index) => ({ index, score: item.score }))
        .sort((left, right) => left.score - right.score)[0]?.index;
      if (Number.isFinite(replacementIndex)) {
        selectedKeys.delete(nextSelected[replacementIndex].key);
        selectedGroups.delete(ROLE_SELECTION_GROUPS[nextSelected[replacementIndex].key] || 'general');
        nextSelected[replacementIndex] = withReason;
        selectedKeys.add(withReason.key);
        selectedGroups.add(ROLE_SELECTION_GROUPS[withReason.key] || 'general');
      }
    }

    const needsFinancial = /доход|деньг|финанс|дефицит|расход|маржа|выруч|cash|roi|kpi|прибыл/i.test(intent);
    const needsPriority = /конкретно|что делать|следующ|план|по шагам|приоритет|дорожн|сначала/i.test(intent);
    const needsHidden = /скрыт|потенциал|рычаг|возможност|неочевид/i.test(intent);
    const needsNegotiation = /переговор|оппонент|уступк|batna|договорен|эскалац/i.test(intent);

    if (needsFinancial) {
      upsertRoleIfNeeded(['financial_analyst', 'risk_analyst'], 'coverage: финансовый контур');
    }
    if (needsPriority) {
      upsertRoleIfNeeded(['prioritizer', 'tactician_7_30'], 'coverage: нужен конкретный следующий шаг');
    }
    if (needsHidden) {
      upsertRoleIfNeeded(['hidden_potential_hunter'], 'coverage: запрос на скрытые резервы');
    }
    if (needsNegotiation) {
      upsertRoleIfNeeded(['negotiator', 'sales_negotiator'], 'coverage: переговорный контур');
    }

    if (scopeType === 'project' && followUpVague) {
      upsertRoleIfNeeded(['change_archivist', 'structure_mapper'], 'coverage: follow-up требует фиксации сдвига');
    }

    return nextSelected
      .sort((left, right) => right.score - left.score || left.name.localeCompare(right.name))
      .slice(0, ROLE_ON_DEMAND_SELECTION_LIMIT);
  }

  function mapLegacyRoleHintToRoleKeys(rawRoleHint) {
    const normalizedHint = normalizeDetectedRole(rawRoleHint);
    return Array.isArray(LEGACY_ROLE_HINT_TO_ON_DEMAND[normalizedHint])
      ? LEGACY_ROLE_HINT_TO_ON_DEMAND[normalizedHint]
      : [];
  }

  function selectAgentRolesOnDemand({
    contextData,
    message,
    roleHint = '',
  }) {
    const payloadContext = toProfile(contextData);
    const stateSnapshot = toProfile(payloadContext?.stateSnapshot);
    const entities = Array.isArray(payloadContext?.entities) ? payloadContext.entities : [];
    const connections = Array.isArray(payloadContext?.connections) ? payloadContext.connections : [];
    const attachments = Array.isArray(payloadContext?.attachments) ? payloadContext.attachments : [];
    const scopeType = toTrimmedString(payloadContext?.scope?.type, 24).toLowerCase();
    const stage = toTrimmedString(stateSnapshot?.stage, 24).toLowerCase();
    const roleSignals = buildRoleSelectionSignals({
      payloadContext,
      stateSnapshot,
      message,
      entities,
      connections,
      attachments,
      stage,
    });
    const signalText = roleSignals.combinedSignalText;
    const userIntentText = roleSignals.userIntentText;
    const contextHeavyText = roleSignals.contextHeavyText;
    const legacyHintRoleKeys = mapLegacyRoleHintToRoleKeys(roleHint);
    const scored = [];
    const decisionIntent = /как|что делать|план|приоритет|выбрать|стоит ли|запуск|масштаб|переговор|бюджет|риск|стратег|тактик|по шагам|конкретно/i
      .test(normalizeSignalText(userIntentText, 8000));
    const explicitNegotiationIntent = /переговор|оппонент|уступк|batna|эскалац|договорен|жестк/i
      .test(normalizeSignalText(userIntentText, 8000));
    const explicitHiddenPotentialIntent = /скрыт|потенциал|рычаг|неочевид|возможност/i
      .test(normalizeSignalText(userIntentText, 8000));
    const explicitFinancialIntent = /доход|деньг|финанс|дефицит|расход|маржа|выруч|cash|roi|kpi|прибыл|монет/i
      .test(normalizeSignalText(userIntentText, 8000));
    const explicitConcretenessIntent = /конкретно|по шагам|что делать|следующ|сначала|приоритет|чеклист/i
      .test(normalizeSignalText(userIntentText, 8000));
    const followUpVague = roleSignals.followUpVague;

    for (const role of ROLE_ON_DEMAND_CATALOG) {
      let score = 0;
      const reasons = [];

      const keywordScore = sumWeightedHits(roleSignals.weightedSignals, role.keywords);
      if (keywordScore > 0) {
        score += keywordScore;
        reasons.push(`взвешенные совпадения: ${keywordScore.toFixed(1)}`);
      }

      if (legacyHintRoleKeys.includes(role.key)) {
        score += 4;
        reasons.push('усилен legacy role hint');
      }

      if (scopeType === 'project' && ['structure_mapper', 'strategist', 'tactician_7_30'].includes(role.key)) {
        score += 1;
        reasons.push('релевантно project scope');
      }

      if (stage === 'follow_up' && ['change_archivist', 'contradiction_detector'].includes(role.key)) {
        score += 2;
        reasons.push('релевантно follow-up стадии');
      }

      if (decisionIntent && ['prioritizer', 'tactician_7_30', 'strategist'].includes(role.key)) {
        score += 2;
        reasons.push('обнаружен decision intent');
      }

      if (explicitConcretenessIntent && ['prioritizer', 'tactician_7_30'].includes(role.key)) {
        score += 3;
        reasons.push('запрос на конкретный план действий');
      }

      if (
        /противореч|несостык|взаимоисключ|конфликт|не сходится|расхожд/i.test(signalText)
        && role.key === 'contradiction_detector'
      ) {
        score += 3;
        reasons.push('найден конфликт в формулировках');
      }

      if (
        /деньг|финанс|cash|roi|маржа|выруч|затрат|бюджет|окуп/i.test(signalText)
        && role.key === 'financial_analyst'
      ) {
        score += 3;
        reasons.push('финансовый контур задачи');
      }

      if (/риск|угроз|штраф|неопредел|потер/i.test(signalText) && role.key === 'risk_analyst') {
        score += 3;
        reasons.push('повышенная риск-нагруженность');
      }

      if (/воронк|лид|трафик|аудитор|бренд|креатив/i.test(signalText) && role.key === 'marketing_decoder') {
        score += 3;
        reasons.push('маркетинговый контур задачи');
      }

      if (/продаж|сделк|переговор|возражен|договор|чек/i.test(signalText) && role.key === 'sales_negotiator') {
        score += 3;
        reasons.push('контур продаж/переговоров');
      }

      if (explicitNegotiationIntent && role.key === 'negotiator') {
        score += 5;
        reasons.push('явный запрос на подготовку/ведение переговоров');
      }

      if (
        /сложн[а-я]*\s*переговор|переговор|оппонент|эскалац|конфликт|batna|якор|уступк|позици|подготов/i
          .test(signalText)
        && role.key === 'negotiator'
      ) {
        score += 4;
        reasons.push('контур сложных переговоров/подготовки');
      }

      if (explicitHiddenPotentialIntent && role.key === 'hidden_potential_hunter') {
        score += 5;
        reasons.push('явный запрос на скрытые рычаги и резервы');
      }

      if (explicitFinancialIntent && role.key === 'financial_analyst') {
        score += 4;
        reasons.push('явный финансовый запрос');
      }

      if (/продукт|mvp|retention|onboarding|конверс|фича|гипотез/i.test(signalText) && role.key === 'product_analyst') {
        score += 3;
        reasons.push('продуктовый контур');
      }

      if (/слепая зона|упуст|не хватает|пробел|чего нет|missing/i.test(signalText) && role.key === 'omission_detector') {
        score += 3;
        reasons.push('явный запрос на поиск упущений');
      }

      if (/иллюз|самообман|розовы|wishful|нереал/i.test(signalText) && role.key === 'illusion_breaker') {
        score += 3;
        reasons.push('проверка гипотез на реализм');
      }

      if (ROLE_SELECTION_NEGOTIATION_KEYS.has(role.key) && !explicitNegotiationIntent) {
        const contextNegotiationNoise = countKeywordHits(contextHeavyText, role.keywords);
        if (contextNegotiationNoise > 0) {
          score -= Math.min(3, contextNegotiationNoise);
          reasons.push('штраф: переговорная лексика только из фонового контекста');
        }
      }

      if (followUpVague && ROLE_SELECTION_NEGOTIATION_KEYS.has(role.key) && !explicitNegotiationIntent) {
        score -= 2;
        reasons.push('штраф: короткий follow-up без переговорного интента');
      }

      if (score > 0.2) {
        scored.push({
          ...role,
          score: Number(score.toFixed(2)),
          reasons,
        });
      }
    }

    const sorted = scored.sort((left, right) => right.score - left.score || left.name.localeCompare(right.name));
    const selected = [];
    const selectedGroups = new Set();
    for (const candidate of sorted) {
      if (selected.length >= ROLE_ON_DEMAND_SELECTION_LIMIT) break;
      const group = ROLE_SELECTION_GROUPS[candidate.key] || 'general';
      if (
        selectedGroups.has(group)
        && candidate.score < ROLE_SELECTION_DIVERSITY_DUPLICATE_MIN_SCORE
      ) {
        continue;
      }
      selected.push(candidate);
      selectedGroups.add(group);
    }

    const selectedKeysSeed = new Set(selected.map((item) => item.key));
    if (selected.length < ROLE_ON_DEMAND_MIN_SELECTION) {
      for (const candidate of sorted) {
        if (selected.length >= ROLE_ON_DEMAND_MIN_SELECTION) break;
        if (selectedKeysSeed.has(candidate.key)) continue;
        selected.push(candidate);
        selectedKeysSeed.add(candidate.key);
      }
    }

    if (!selected.length) {
      const fallback = ROLE_ON_DEMAND_BY_KEY.structure_mapper || ROLE_ON_DEMAND_CATALOG[0];
      selected.push({
        ...fallback,
        score: 1,
        reasons: ['fallback: базовый структурный разбор'],
      });
    }

    const coveredSelected = applyRoleSelectionCoverage({
      selected,
      sorted,
      userIntentText,
      followUpVague,
      scopeType,
    });

    const selectedKeySet = new Set(coveredSelected.map((item) => item.key));
    const dropped = ROLE_ON_DEMAND_CATALOG
      .filter((role) => !selectedKeySet.has(role.key))
      .map((role) => {
        const scoredRow = sorted.find((item) => item.key === role.key);
        const score = scoredRow?.score || 0;
        return {
          key: role.key,
          name: role.name,
          score,
          reason: score > 0
            ? score >= ROLE_SELECTION_DIVERSITY_DUPLICATE_MIN_SCORE
              ? 'уступил coverage/diversity выбору'
              : 'ниже top-3 после взвешивания'
            : 'нет релевантных сигналов',
        };
      });

    return {
      selectedRoles: coveredSelected.map((item) => ({
        key: item.key,
        name: item.name,
        playbook: item.playbook,
        score: item.score,
      })),
      whySelected: coveredSelected.map((item) => ({
        key: item.key,
        name: item.name,
        reasons: item.reasons,
      })),
      droppedRoles: dropped,
      roleHint: normalizeDetectedRole(roleHint),
    };
  }

  function resolveQuestionFocus(missingSignals) {
    const source = Array.isArray(missingSignals) ? missingSignals : [];
    if (source.includes('цель')) return 'цель следующего шага';
    if (source.includes('ограничения')) return 'ограничения (бюджет/команда/дедлайн)';
    if (source.includes('срок')) return 'срок реализации';
    if (source.includes('метрика')) return 'метрика успеха';
    return 'ключевой операционный блокер';
  }

  function evaluateAgentQuestionGate({
    contextData,
    message,
    selectedRoles,
  }) {
    const payloadContext = toProfile(contextData);
    const normalizedMessage = toTrimmedString(message, 2400);
    const entitiesCount = Array.isArray(payloadContext?.entities) ? payloadContext.entities.length : 0;
    const stage = toTrimmedString(payloadContext?.stateSnapshot?.stage, 24).toLowerCase();
    const stateSnapshot = toProfile(payloadContext?.stateSnapshot);
    const recentUserTurns = (Array.isArray(stateSnapshot?.recentUserTurns) ? stateSnapshot.recentUserTurns : [])
      .map((item) => toTrimmedString(item, 420))
      .filter(Boolean);
    const latestUserSignals = (Array.isArray(stateSnapshot?.latestUserSignals) ? stateSnapshot.latestUserSignals : [])
      .map((item) => toTrimmedString(item, 220))
      .filter(Boolean);
    const goalsText = (Array.isArray(stateSnapshot?.goals) ? stateSnapshot.goals : [])
      .map((goal) => {
        const row = toProfile(goal);
        return [toTrimmedString(row.name, 120), toTrimmedString(row.description, 220)].filter(Boolean).join(' ');
      })
      .filter(Boolean)
      .join('\n');
    const entitiesText = (Array.isArray(payloadContext?.entities) ? payloadContext.entities : [])
      .slice(0, 90)
      .map((entity) => {
        const row = toProfile(entity);
        return [toTrimmedString(row.name, 120), toTrimmedString(row.description, 220)].filter(Boolean).join(' ');
      })
      .filter(Boolean)
      .join('\n');
    const contextIntentText = normalizeSignalText([
      normalizedMessage,
      toTrimmedString(stateSnapshot?.currentUserRequest, 420),
      latestUserSignals.join('\n'),
      recentUserTurns.join('\n'),
      goalsText,
      entitiesText,
    ].join('\n'), 24_000);
    const followUpVague = stage === 'follow_up' && isVagueFollowUpMessage(normalizedMessage);
    const selectedRoleKeys = (Array.isArray(selectedRoles) ? selectedRoles : [])
      .map((item) => toTrimmedString(item?.key, 64))
      .filter(Boolean);

    let decisionIntent = /как|что делать|план|приоритет|выбрать|стоит ли|нужно ли|сценар|дорожн|шаг|конкретно|по шагам/i
      .test(contextIntentText);
    if (!decisionIntent && followUpVague) {
      decisionIntent = /цель|доход|план|приоритет|конкрет|вариант|рычаг|рост/i.test(contextIntentText);
    }
    const hasGoal = /цель|хочу|нужно|надо|добиться|увелич|сниз|запустить|сделать/i.test(contextIntentText)
      || (Array.isArray(stateSnapshot?.goals) && stateSnapshot.goals.length > 0);
    const hasConstraints = /бюджет|лимит|ресурс|команда|дедлайн|срок|огранич|расход|кредит|обязательств|дефицит/i
      .test(contextIntentText);
    const hasTimeframe = /дн|недел|месяц|квартал|год|до\s+\d{1,2}[./-]\d{1,2}|q[1-4]|202\d|\d+\s*(дней|недель|месяцев)/i
      .test(contextIntentText);
    const hasMetric = /(\d+[%$₸€₽])|kpi|roi|ltv|cac|mrr|arr|конверс|маржа|выручк|x[2-9]/i.test(contextIntentText);

    const missingSignals = [];
    if (!hasGoal) missingSignals.push('цель');
    if (!hasConstraints) missingSignals.push('ограничения');
    if (!hasTimeframe) missingSignals.push('срок');
    if (!hasMetric) missingSignals.push('метрика');

    const baseCanProgressWithoutQuestion = !decisionIntent || missingSignals.length <= 1 || followUpVague;
    const contextIsThin = entitiesCount < 2 && stage === 'initial';
    const roleNeedsClarification = selectedRoleKeys.some((key) =>
      ['financial_analyst', 'prioritizer', 'tactician_7_30', 'sales_negotiator', 'negotiator'].includes(key),
    );
    const allowQuestion =
      decisionIntent
      && roleNeedsClarification
      && (missingSignals.length >= 2 || contextIsThin);

    const allowReason = allowQuestion
      ? `без уточнения страдают решение и план: не хватает ${missingSignals.slice(0, 2).join(', ')}`
      : baseCanProgressWithoutQuestion
        ? followUpVague
          ? 'follow-up короткий, но контекст уже позволяет дать конкретный шаг без уточнений'
          : 'данных достаточно для следующего шага без уточнений'
        : 'можно продвинуть решение по текущему контексту без дополнительного вопроса';

    return {
      allowQuestion,
      allowReason,
      decisionIntent,
      missingSignals,
      questionFocus: resolveQuestionFocus(missingSignals),
      entitiesInContext: entitiesCount,
      stage: stage || 'unknown',
      followUpVague,
      contextIntentSignals: {
        hasGoal,
        hasConstraints,
        hasTimeframe,
        hasMetric,
      },
      policy: allowQuestion
        ? 'question_allowed_if_it_changes_plan'
        : 'question_blocked_unless_plan_changes',
    };
  }

  function extractQuestionFromReply(replyText) {
    const text = toTrimmedString(replyText, 6000);
    if (!text) return '';
    const labeledMatch = text.match(/(?:^|\n)\s*Вопрос:\s*(.+)$/im);
    if (labeledMatch && labeledMatch[1]) {
      return toTrimmedString(labeledMatch[1], 320);
    }
    const chunks = text
      .split(/(?<=[?!])\s+/g)
      .map((item) => toTrimmedString(item, 320))
      .filter(Boolean);
    for (let index = chunks.length - 1; index >= 0; index -= 1) {
      if (chunks[index].includes('?')) return chunks[index];
    }
    return '';
  }

  function inspectAgentReplyQuestionGate({
    reply,
    questionGate,
  }) {
    const gate = toProfile(questionGate);
    const extractedQuestion = extractQuestionFromReply(reply);
    const asked = Boolean(extractedQuestion);
    let reason = '';
    if (asked && gate.allowQuestion) reason = 'вопрос добавлен и gate разрешил';
    if (asked && !gate.allowQuestion) reason = 'вопрос добавлен несмотря на запрет gate';
    if (!asked && gate.allowQuestion) reason = 'вопрос не добавлен: можно двигаться без уточнений';
    if (!asked && !gate.allowQuestion) reason = 'вопрос пропущен: gate запретил';

    return {
      asked,
      reason,
      extractedQuestion,
      allowQuestion: gate.allowQuestion === true,
      allowReason: toTrimmedString(gate.allowReason, 240),
    };
  }

  function buildAgentContextData({ scopeContext, history, attachments }) {
    const cleanedEntities = cleanContextData(scopeContext.entities);
    const projectMetadata = toProfile(scopeContext.projectMetadata);
    const isProjectScope = scopeContext.scopeType === 'project';
    const projectContext =
      isProjectScope
        ? {
            description: toTrimmedString(
              projectMetadata.project_context_compiled_description || projectMetadata.description,
              18000,
            ),
            contextStatus: toTrimmedString(projectMetadata.project_context_status, 32),
            builtAt: toTrimmedString(projectMetadata.project_context_built_at, 80),
          }
        : null;

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
      ...(projectContext ? { projectContext } : {}),
      entities: isProjectScope ? [] : cleanedEntities,
      connections: isProjectScope ? [] : scopeContext.connections,
      attachments,
      history,
    };
  }

  function serializeEntityForLlm(entity) {
    const row = toProfile(entity);
    const metadata = toProfile(row.ai_metadata);
    const id = normalizeEntityId(row.id || row._id, 120);
    if (!id) return null;

    return {
      id,
      type: toTrimmedString(row.type, 24) || 'shape',
      name: toTrimmedString(row.name, 160) || '(без названия)',
      description: toTrimmedString(metadata.description || row.description, 2400),
      is_me: row.is_me === true,
      is_mine: row.is_mine === true,
    };
  }

  function buildAuthorHint(llmNodes, projectContext = null) {
    const nodes = Array.isArray(llmNodes) ? llmNodes : [];
    const authorNode = nodes.find((node) => node?.is_me === true)
      || nodes.find((node) => node?.is_mine === true)
      || null;
    if (!authorNode) return null;

    return {
      id: toTrimmedString(authorNode.id, 120),
      type: toTrimmedString(authorNode.type, 24),
      name: toTrimmedString(authorNode.name, 160),
      description: toTrimmedString(authorNode.description, 360),
      is_me: authorNode.is_me === true,
      is_mine: authorNode.is_mine === true,
    };
  }

  function buildProjectContextAuthorHint() {
    return null;
  }

  function serializeSourceNode(node) {
    const row = toProfile(node);
    const id = toTrimmedString(row.id, 120);
    const entityId = toTrimmedString(row.entityId, 120);
    if (!id || !entityId) return null;
    return {
      id,
      entityId,
    };
  }

  function serializeSourceEdge(edge) {
    const row = toProfile(edge);
    const id = toTrimmedString(row.id, 120);
    const source = toTrimmedString(row.source, 120);
    const target = toTrimmedString(row.target, 120);
    if (!source || !target) return null;
    return {
      ...(id ? { id } : {}),
      source,
      target,
      type: toTrimmedString(row.type, 40),
      label: toTrimmedString(row.label, 120),
      arrowLeft: row.arrowLeft === true,
      arrowRight: row.arrowRight === true,
    };
  }

  function resolveEdgeTypeForLlm(edge) {
    const directType = toTrimmedString(edge.type, 40).toLowerCase();
    if (directType) return directType;
    if (edge.arrowLeft && edge.arrowRight) return 'bidirectional';
    if (edge.arrowRight) return 'directed';
    if (edge.arrowLeft) return 'directed_reverse';
    return 'undirected';
  }

  function resolveEdgeDirectionForLlm(edge, from, to) {
    const normalizedFrom = toTrimmedString(from, 120);
    const normalizedTo = toTrimmedString(to, 120);

    if (edge.arrowLeft && !edge.arrowRight) {
      return {
        relationMode: 'directed',
        direction: 'target_to_source',
        directedFrom: normalizedTo,
        directedTo: normalizedFrom,
      };
    }

    if (!edge.arrowLeft && edge.arrowRight) {
      return {
        relationMode: 'directed',
        direction: 'source_to_target',
        directedFrom: normalizedFrom,
        directedTo: normalizedTo,
      };
    }

    return {
      relationMode: 'equivalent',
      direction: edge.arrowLeft && edge.arrowRight ? 'bidirectional' : 'equivalent',
      directedFrom: '',
      directedTo: '',
    };
  }

  function normalizeAgentHistoryForLlm(history) {
    const source = Array.isArray(history) ? history : [];
    const normalized = source
      .map((item) => {
        const row = toProfile(item);
        const role = row.role === 'assistant' ? 'assistant' : row.role === 'user' ? 'user' : '';
        const text = toTrimmedString(row.text, AGENT_LLM_HISTORY_ITEM_MAX_LENGTH);
        if (!role || !text) return null;
        return { role, text };
      })
      .filter(Boolean);

    const userOnly = normalized.filter((item) => item.role === 'user');
    const llmHistory = userOnly.slice(-AGENT_LLM_HISTORY_MAX_ITEMS);

    return {
      sourceHistory: normalized,
      llmHistory,
      stats: {
        sourceTotal: normalized.length,
        sourceUserTotal: userOnly.length,
        sourceAssistantTotal: Math.max(0, normalized.length - userOnly.length),
        keptUserTotal: llmHistory.length,
        droppedAssistantTotal: Math.max(0, normalized.length - userOnly.length),
        droppedUserTailTotal: Math.max(0, userOnly.length - llmHistory.length),
        mode: 'user_only_tail',
        maxItems: AGENT_LLM_HISTORY_MAX_ITEMS,
      },
    };
  }

  function normalizeAgentAttachmentsForLlm(attachments) {
    const source = Array.isArray(attachments) ? attachments : [];
    const normalized = source
      .map((item) => {
        const row = toProfile(item);
        const name = toTrimmedString(row.name, 120);
        const mime = toTrimmedString(row.mime, 120);
        const contentCategory = toTrimmedString(row.contentCategory, 40);
        const size = Number.isFinite(Number(row.size)) ? Math.max(0, Math.floor(Number(row.size))) : 0;
        const text = toTrimmedString(row.text, AGENT_LLM_ATTACHMENT_TEXT_MAX_LENGTH);
        if (!name && !text) return null;
        return {
          name: name || 'Файл',
          mime,
          size,
          contentCategory,
          text,
        };
      })
      .filter(Boolean);
    const llmAttachments = normalized.slice(0, AGENT_LLM_ATTACHMENT_MAX_ITEMS);

    return {
      sourceAttachments: normalized,
      llmAttachments,
      stats: {
        sourceTotal: normalized.length,
        keptTotal: llmAttachments.length,
        droppedTailTotal: Math.max(0, normalized.length - llmAttachments.length),
        maxItems: AGENT_LLM_ATTACHMENT_MAX_ITEMS,
      },
    };
  }

  function extractLatestAssistantQuestion(sourceHistory) {
    const history = Array.isArray(sourceHistory) ? sourceHistory : [];
    for (let index = history.length - 1; index >= 0; index -= 1) {
      const item = history[index];
      if (item?.role !== 'assistant') continue;
      const text = toTrimmedString(item?.text, AGENT_LLM_HISTORY_ITEM_MAX_LENGTH);
      if (!text) continue;

      const labeledMatch = text.match(/(?:^|\n)\s*Вопрос:\s*(.+)$/im);
      if (labeledMatch && labeledMatch[1]) {
        return toTrimmedString(labeledMatch[1], 360);
      }

      const chunks = text
        .split(/(?<=[?!])\s+/g)
        .map((chunk) => toTrimmedString(chunk, 360))
        .filter(Boolean);
      for (let chunkIndex = chunks.length - 1; chunkIndex >= 0; chunkIndex -= 1) {
        if (chunks[chunkIndex].includes('?')) {
          return chunks[chunkIndex];
        }
      }
    }
    return '';
  }

  function extractUserSignals(message) {
    const normalized = toTrimmedString(message, 2400);
    if (!normalized) return [];

    const directChunks = normalized
      .split(/\n+/g)
      .map((chunk) => toTrimmedString(chunk, AGENT_STATE_SIGNAL_MAX_LENGTH))
      .filter(Boolean);
    const source = directChunks.length > 1
      ? directChunks
      : normalized
        .split(/(?<=[.!?])\s+/g)
        .map((chunk) => toTrimmedString(chunk, AGENT_STATE_SIGNAL_MAX_LENGTH))
        .filter(Boolean);

    const dedup = new Set();
    const signals = [];
    for (const chunk of source) {
      const key = chunk.toLowerCase();
      if (dedup.has(key)) continue;
      dedup.add(key);
      signals.push(chunk);
      if (signals.length >= AGENT_STATE_SIGNAL_MAX_ITEMS) break;
    }
    return signals;
  }

  function buildAgentStateSnapshot({
    llmNodes,
    llmEdges,
    sourceHistory,
    llmHistory,
    message,
    projectContext,
  }) {
    const latestUserRequestFull =
      toTrimmedString(message, 2400) || toTrimmedString(llmHistory[llmHistory.length - 1]?.text, 2400);
    const goalHints = (Array.isArray(llmNodes) ? llmNodes : [])
      .filter((node) => ['goal', 'result', 'task'].includes(toTrimmedString(node.type, 24).toLowerCase()))
      .slice(0, 4)
      .map((node) => ({
        id: node.id,
        type: node.type,
        name: node.name,
        description: toTrimmedString(node.description, 280),
      }));
    const authorHint = buildAuthorHint(llmNodes) || buildProjectContextAuthorHint(projectContext);

    return {
      stage: sourceHistory.length > 1 ? 'follow_up' : 'initial',
      currentUserRequest: toTrimmedString(latestUserRequestFull, 420),
      latestUserSignals: extractUserSignals(latestUserRequestFull),
      recentUserTurns: llmHistory.slice(-3).map((item) => item.text),
      latestAssistantQuestion: extractLatestAssistantQuestion(sourceHistory),
      author: authorHint,
      goals: goalHints,
      graphStats: {
        entities: Array.isArray(llmNodes) ? llmNodes.length : 0,
        connections: Array.isArray(llmEdges) ? llmEdges.length : 0,
      },
      memoryPolicy: {
        historyMode: 'user_only_tail',
        historyMaxItems: AGENT_LLM_HISTORY_MAX_ITEMS,
        latestUserFactPriority: true,
      },
    };
  }

  function buildAgentLlmContextData({ scopeContext, history, attachments, message }) {
    const scope = toProfile(scopeContext);
    const projectMetadata = toProfile(scope.projectMetadata);
    const isProjectScope = scope.scopeType === 'project';
    const rawEntities = isProjectScope
      ? Array.isArray(scope.entities)
        ? scope.entities
        : Array.isArray(scope.sourceEntities)
          ? scope.sourceEntities
          : []
      : Array.isArray(scope.sourceEntities)
        ? scope.sourceEntities
        : Array.isArray(scope.entities)
          ? scope.entities
          : [];
    const sourceNodes = (Array.isArray(scope.sourceNodes) ? scope.sourceNodes : [])
      .map((node) => serializeSourceNode(node))
      .filter(Boolean);
    const sourceEdges = (Array.isArray(scope.sourceEdges) ? scope.sourceEdges : [])
      .map((edge) => serializeSourceEdge(edge))
      .filter(Boolean);

    const llmNodes = [];
    const llmNodeIdSet = new Set();
    for (const rawEntity of rawEntities) {
      const serialized = serializeEntityForLlm(rawEntity);
      if (!serialized) continue;
      if (llmNodeIdSet.has(serialized.id)) continue;
      llmNodeIdSet.add(serialized.id);
      llmNodes.push(serialized);
    }

    const nodeEntityByNodeId = new Map();
    for (const node of sourceNodes) {
      nodeEntityByNodeId.set(node.id, node.entityId);
    }

    const droppedEdges = [];
    const llmEdges = [];
    const llmEdgeDedup = new Set();
    for (const edge of sourceEdges) {
      const sourceNodeId = toTrimmedString(edge.source, 120);
      const targetNodeId = toTrimmedString(edge.target, 120);
      if (!sourceNodeId || !targetNodeId) {
        droppedEdges.push({
          edge,
          reason: 'invalid_edge_endpoint',
        });
        continue;
      }

      const from =
        nodeEntityByNodeId.get(sourceNodeId) || (llmNodeIdSet.has(sourceNodeId) ? sourceNodeId : '');
      const to = nodeEntityByNodeId.get(targetNodeId) || (llmNodeIdSet.has(targetNodeId) ? targetNodeId : '');

      if (!from || !to) {
        droppedEdges.push({
          edge,
          reason: !from && !to ? 'missing_source_and_target_node_mapping' : !from ? 'missing_source_node_mapping' : 'missing_target_node_mapping',
        });
        continue;
      }

      if (!llmNodeIdSet.has(from) || !llmNodeIdSet.has(to)) {
        droppedEdges.push({
          edge,
          reason: !llmNodeIdSet.has(from) && !llmNodeIdSet.has(to)
            ? 'source_and_target_entity_filtered_out'
            : !llmNodeIdSet.has(from)
              ? 'source_entity_filtered_out'
              : 'target_entity_filtered_out',
          from,
          to,
        });
        continue;
      }

      const relation = {
        from,
        to,
        type: resolveEdgeTypeForLlm(edge),
        label: toTrimmedString(edge.label, 120),
        ...resolveEdgeDirectionForLlm(edge, from, to),
      };
      const dedupKey = `${relation.from}|${relation.to}|${relation.type}|${relation.label}`;
      if (llmEdgeDedup.has(dedupKey)) {
        droppedEdges.push({
          edge,
          reason: 'duplicate_relation',
          from,
          to,
        });
        continue;
      }
      llmEdgeDedup.add(dedupKey);
      llmEdges.push(relation);
    }

    const historyContext = normalizeAgentHistoryForLlm(history);
    const attachmentContext = normalizeAgentAttachmentsForLlm(attachments);
    const projectContext = isProjectScope
      ? {
          description: toTrimmedString(
            projectMetadata.project_context_compiled_description || projectMetadata.description,
            18000,
          ),
          contextStatus: toTrimmedString(projectMetadata.project_context_status, 32),
          builtAt: toTrimmedString(projectMetadata.project_context_built_at, 80),
        }
      : null;

    const stateSnapshot = buildAgentStateSnapshot({
      llmNodes,
      llmEdges,
      sourceHistory: historyContext.sourceHistory,
      llmHistory: historyContext.llmHistory,
      message,
      projectContext,
    });

    const contextData = {
      scope: {
        type: scope.scopeType,
        name: scope.scopeName,
        entityType: scope.entityType,
        projectId: scope.projectId,
        projectName: scope.projectName,
        totalEntities: isProjectScope ? Number(scope.totalEntities) || llmNodes.length : llmNodes.length,
        contextLimit: AI_CONTEXT_ENTITY_LIMIT,
      },
      ...(projectContext ? { projectContext } : {}),
      entities: llmNodes,
      connections: llmEdges,
      groups: Array.isArray(scope.groups) ? scope.groups : [],
      attachments: attachmentContext.llmAttachments,
      history: historyContext.llmHistory,
      stateSnapshot,
    };

    const contextJson = JSON.stringify(contextData);
    const trace = {
      sourceNodes,
      sourceEdges,
      sourceHistory: historyContext.sourceHistory,
      sourceAttachments: attachmentContext.sourceAttachments,
      llmNodes,
      llmEdges,
      llmHistory: historyContext.llmHistory,
      llmAttachments: attachmentContext.llmAttachments,
      historyPolicy: historyContext.stats,
      attachmentsPolicy: attachmentContext.stats,
      droppedEdges,
      stateSnapshot,
      payloadSize: {
        chars: contextJson.length,
        bytes: Buffer.byteLength(contextJson, 'utf8'),
      },
      preview: {
        entities: llmNodes.slice(0, 8),
        relations: llmEdges.slice(0, 12),
        groups: (Array.isArray(scope.groups) ? scope.groups : []).slice(0, 8),
        history: historyContext.llmHistory.slice(-3),
        stateSnapshot,
      },
    };

    return {
      contextData,
      trace,
    };
  }

  function buildRouterPrompt(contextData, userMessage) {
    const entities = Array.isArray(contextData?.entities) ? contextData.entities : [];
    const projectContextDescription = toTrimmedString(toProfile(contextData?.projectContext).description, 18000);
    const semanticSignals = (
      entities.length
        ? entities
          .map((entity) => collectEntitySemanticSignals(entity).join(' '))
          .filter(Boolean)
          .join(' ')
        : projectContextDescription
    ).slice(0, 12000);

    const query = toTrimmedString(userMessage, 2400);

    return [
      `Проанализируй запрос "${query}" и сигналы контекста: "${semanticSignals}".`,
      'Определи, какой эксперт нужен. Верни СТРОГО ОДНО СЛОВО из списка: investor, hr, strategist, default.',
      'Не пиши больше ничего.',
    ].join('\n');
  }

  function buildAgentSystemPrompt(contextData) {
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
        ? 'Важно: для project chat главным источником истины является собранный projectContext.description; если ответа нет в этом контексте, скажи это прямо. Отвечай коротко, ёмко и в формате живого диалога, а не длинного эссе.'
        : '';

    return [
      BASE_AGENT_PROFILE,
      scopeDescription,
      'Жесткое правило: используй ТОЛЬКО данные из переданного контекста.',
      projectExtractionHint,
      'Если в contextData.entities есть флаги is_me / is_mine или в stateSnapshot есть author, считай это личным контуром пользователя.',
      'Важно: личный контур автора — это не единственный центр анализа, а система координат для понимания, от чьего имени ставятся цели, задаются вопросы и принимаются решения.',
      'Используй автора как опорную точку для интерпретации целей, ресурсов, ограничений и личной роли в проекте, но не зацикливайся только на нём.',
      'Обязательно проверяй, нет ли вне прямого авторского контура скрытых недооцененных активов, узлов, возможностей или ограничений, которые сильнее влияют на результат проекта.',
      'Критично: при конфликте между старым контекстом и новыми фактами пользователя приоритет у САМЫХ СВЕЖИХ user-сообщений.',
      'Не повторяй дословно предыдущие ответы ассистента: каждый ход должен обновлять оценку по новым данным.',
      STRICT_FORMATTING_RULES,
    ].join('\n');
  }

  function buildAgentUserPrompt({ contextData, scopeContext, message, history, attachments }) {
    const payloadContext =
      contextData && typeof contextData === 'object'
        ? contextData
        : buildAgentLlmContextData({
            scopeContext,
            history,
            attachments,
            message,
          }).contextData;
    const stateSnapshot = toProfile(payloadContext?.stateSnapshot);
    const graphContext = {
      scope: toProfile(payloadContext?.scope),
      projectContext: toProfile(payloadContext?.projectContext),
      attachments: Array.isArray(payloadContext?.attachments) ? payloadContext.attachments : [],
    };
    if (toTrimmedString(toProfile(payloadContext?.scope).type, 24) !== 'project') {
      graphContext.entities = Array.isArray(payloadContext?.entities) ? payloadContext.entities : [];
      graphContext.connections = Array.isArray(payloadContext?.connections) ? payloadContext.connections : [];
    }
    const dialogueMemory = {
      history: Array.isArray(payloadContext?.history) ? payloadContext.history : [],
      latestAssistantQuestion: toTrimmedString(stateSnapshot?.latestAssistantQuestion, 360),
    };
    const currentRequest = toTrimmedString(message, 2400);

    return [
      'State Snapshot (JSON):',
      JSON.stringify(stateSnapshot, null, 2),
      '',
      'Relevant Graph Context (JSON):',
      JSON.stringify(graphContext, null, 2),
      '',
      'Dialogue Memory (JSON):',
      JSON.stringify(dialogueMemory, null, 2),
      '',
      'Current User Turn:',
      currentRequest,
      '',
      'Response Contract:',
      '- Сначала молча разберись в смысле вопроса, проверь релевантные факты и ограничения в переданном контексте.',
      '- Для project chat опирайся только на projectContext.description.',
      '- Не подменяй ответ общим управленческим консалтингом, если в projectContext уже есть конкретные факты, цели, роли, активы и ограничения.',
      '- Если вопрос можно ответить из projectContext.description, отвечай прямо по нему.',
      '- Если в projectContext недостаточно данных, скажи коротко чего именно не хватает.',
      '- Если для сильного ответа нужна недостающая информация, задай один короткий уточняющий вопрос.',
      '- Если видишь, что контекст проекта устарел или в нём не хватает важного факта, можешь коротко предложить обновить контекст или добавить конкретную информацию на дашборд.',
      '- Если в stateSnapshot.author есть author, используй это как ориентир: вопрос задан из личного контура автора проекта.',
      '- Но не своди анализ только к автору: проверь внешний контур проекта на скрытые возможности, bottlenecks и недооцененные узлы.',
      '- При конфликте с устаревшими данными используй приоритет новых фактов пользователя.',
      '- По умолчанию верни короткий ёмкий ответ: 1-3 коротких абзаца без показа внутреннего анализа.',
      '- Не пиши длинные портянки. Лучше коротко ответить, затем при необходимости задать вопрос или предложить следующий точечный шаг.',
      '- Развернутое объяснение давай только если пользователь явно запросил обоснование.',
    ].join('\n');
  }

  return {
    buildAgentContextData,
    buildAgentLlmContextData,
    buildRouterPrompt,
    normalizeDetectedRole,
    selectAgentRolesOnDemand,
    evaluateAgentQuestionGate,
    inspectAgentReplyQuestionGate,
    buildAgentSystemPrompt,
    buildAgentUserPrompt,
  };
}

module.exports = {
  createAgentPrompts,
  cleanContextData,
  normalizeDetectedRole,
};
