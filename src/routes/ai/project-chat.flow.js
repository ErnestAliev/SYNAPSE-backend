const PROJECT_DEEP_REASONING_OUTPUT_SCHEMA = Object.freeze({
  type: 'json_schema',
  name: 'ProjectDeepReasoningReply',
  strict: true,
  schema: {
    type: 'object',
    required: ['final_answer', 'reasoning_state', 'next_best_question', 'answer_pattern'],
    additionalProperties: false,
    properties: {
      final_answer: { type: 'string', maxLength: 3200 },
      next_best_question: { anyOf: [{ type: 'string' }, { type: 'null' }] },
      answer_pattern: { type: 'string', maxLength: 80 },
      reasoning_state: {
        type: 'object',
        required: [
          'actor',
          'intent',
          'current_point',
          'target_point',
          'constraints',
          'fast_levers',
          'excluded_paths',
          'next_best_question',
          'relevant_entities',
          'confirmed_facts',
          'uncertain_facts',
          'conflicts',
          'core_conclusion',
          'why_not_enough',
          'next_growth_contour',
        ],
        additionalProperties: false,
        properties: {
          actor: { type: 'string', maxLength: 240 },
          intent: { type: 'string', maxLength: 320 },
          current_point: { type: 'string', maxLength: 900 },
          target_point: { type: 'string', maxLength: 900 },
          constraints: { type: 'array', maxItems: 5, items: { type: 'string', maxLength: 180 } },
          fast_levers: { type: 'array', maxItems: 4, items: { type: 'string', maxLength: 180 } },
          excluded_paths: { type: 'array', maxItems: 4, items: { type: 'string', maxLength: 180 } },
          next_best_question: { anyOf: [{ type: 'string' }, { type: 'null' }] },
          relevant_entities: {
            type: 'array',
            maxItems: 6,
            items: {
              type: 'object',
              required: ['id', 'name', 'type', 'why_relevant'],
              additionalProperties: false,
              properties: {
                id: { type: 'string', maxLength: 120 },
                name: { type: 'string', maxLength: 120 },
                type: { type: 'string', maxLength: 48 },
                why_relevant: { type: 'string', maxLength: 220 },
              },
            },
          },
          confirmed_facts: { type: 'array', maxItems: 6, items: { type: 'string', maxLength: 220 } },
          uncertain_facts: { type: 'array', maxItems: 4, items: { type: 'string', maxLength: 220 } },
          conflicts: { type: 'array', maxItems: 4, items: { type: 'string', maxLength: 220 } },
          core_conclusion: { type: 'string', maxLength: 1000 },
          why_not_enough: { type: 'string', maxLength: 1000 },
          next_growth_contour: { type: 'string', maxLength: 1000 },
        },
      },
    },
  },
});

const PROJECT_REASONING_CONTRACT = Object.freeze([
  '1. Определи actor (кто субъект запроса).',
  '2. Зафиксируй intent (что именно хочет получить сейчас).',
  '3. Определи current_point (где система находится сейчас по фактам контекста).',
  '4. Определи target_point (к какому результату нужно прийти).',
  '5. Выдели constraints (реальные ограничения: ресурс, срок, риски, зависимости).',
  '6. Найди fast_levers (быстрые рычаги наибольшего эффекта).',
  '7. Отсеки excluded_paths (что сейчас делать не стоит и почему).',
  '8. Сформулируй core_conclusion (главный вывод, а не пересказ фактов).',
  '9. Зафиксируй why_not_enough (почему найденные шаги пока не решают главную цель).',
  '10. Зафиксируй next_growth_contour (куда копать дальше для следующего слоя роста).',
  '11. При необходимости задай один next_best_question, только если он открывает новый слой анализа.',
]);

const PROJECT_ANSWER_PATTERNS = Object.freeze([
  'Pattern A: Факт-ориентированный: что происходит -> главный узел -> быстрый шаг -> что исключаем -> (опционально) следующий вопрос.',
  'Pattern B: Ограничения-first: ограничения -> реалистичный рычаг -> короткий план действия -> что не сработает -> (опционально) следующий вопрос.',
  'Pattern C: Конфликты/пробелы: конфликт данных -> рабочая гипотеза -> проверяемый следующий шаг -> что не делать -> (опционально) следующий вопрос.',
]);

const PROJECT_REQUIRED_REASONING_BLOCKS = Object.freeze([
  'actor',
  'intent',
  'current_point',
  'target_point',
  'constraints',
  'fast_levers',
  'excluded_paths',
  'core_conclusion',
  'why_not_enough',
  'next_growth_contour',
]);
const PROJECT_DEEP_REASONING_MAX_CALLS = 2;
const PROJECT_DEEP_REASONING_MIN_OUTPUT_TOKENS = 3200;
const PROJECT_DEEP_REASONING_MIN_TIMEOUT_MS = 130_000;

function createProjectChatFlow({ deps, helpers }) {
  const {
    toTrimmedString,
    toProfile,
    OPENAI_PROJECT_MODEL,
    OPENAI_DEEP_MODEL,
    aiPrompts,
    aiProvider,
  } = deps;

  const {
    AGENT_CHAT_MAIN_REQUEST_CONFIG,
    dedupeHistoryTailByCurrentMessage,
    resolveCompatibleDetectedRole,
    buildRequestBodySize,
    summarizePreviewEntities,
    withAiTrace,
    runProjectChatAutoEnrichment,
    buildAgentLlmContext,
  } = helpers;

  function resolveProjectDeepReasoningRequestConfig() {
    const configuredTokens = Number(AGENT_CHAT_MAIN_REQUEST_CONFIG.maxOutputTokens);
    const configuredTimeout = Number(AGENT_CHAT_MAIN_REQUEST_CONFIG.timeoutMs);
    const configuredVerbosity = toTrimmedString(AGENT_CHAT_MAIN_REQUEST_CONFIG.verbosity, 12).toLowerCase();

    return {
      temperature: AGENT_CHAT_MAIN_REQUEST_CONFIG.temperature,
      maxOutputTokens: Number.isFinite(configuredTokens)
        ? Math.max(PROJECT_DEEP_REASONING_MIN_OUTPUT_TOKENS, Math.floor(configuredTokens))
        : PROJECT_DEEP_REASONING_MIN_OUTPUT_TOKENS,
      timeoutMs: Number.isFinite(configuredTimeout)
        ? Math.max(PROJECT_DEEP_REASONING_MIN_TIMEOUT_MS, Math.floor(configuredTimeout))
        : PROJECT_DEEP_REASONING_MIN_TIMEOUT_MS,
      reasoningEffort: AGENT_CHAT_MAIN_REQUEST_CONFIG.reasoningEffort,
      verbosity: configuredVerbosity === 'low' ? 'medium' : configuredVerbosity || 'medium',
    };
  }

  function normalizeStringList(values, {
    maxItems = 12,
    itemMaxLength = 240,
  } = {}) {
    const source = Array.isArray(values)
      ? values
      : typeof values === 'string'
        ? [values]
        : [];
    const dedup = new Set();
    const result = [];
    for (const item of source) {
      const normalized = toTrimmedString(item, itemMaxLength);
      if (!normalized) continue;
      const dedupKey = normalized.toLowerCase();
      if (dedup.has(dedupKey)) continue;
      dedup.add(dedupKey);
      result.push(normalized);
      if (result.length >= maxItems) break;
    }
    return result;
  }

  function normalizeOptionalQuestion(value) {
    const question = toTrimmedString(value, 360);
    return question || '';
  }

  function normalizeRelevantEntities(values) {
    const source = Array.isArray(values) ? values : [];
    const dedup = new Set();
    const normalized = [];
    for (const item of source) {
      const row = toProfile(item);
      const id = toTrimmedString(row.id || row.entity_id, 120);
      const name = toTrimmedString(row.name, 140);
      const type = toTrimmedString(row.type, 48);
      const whyRelevant = toTrimmedString(row.why_relevant || row.reason, 320);
      const dedupKey = `${id}|${name}|${type}`.toLowerCase();
      if (!name && !id) continue;
      if (dedup.has(dedupKey)) continue;
      dedup.add(dedupKey);
      normalized.push({
        id,
        name,
        type,
        why_relevant: whyRelevant,
      });
      if (normalized.length >= 12) break;
    }
    return normalized;
  }

  function normalizeReasoningState(rawState, topLevelQuestion = '') {
    const state = toProfile(rawState);
    const stateQuestion = normalizeOptionalQuestion(state.next_best_question);
    const nextBestQuestion = topLevelQuestion || stateQuestion;

    return {
      actor: toTrimmedString(state.actor, 320),
      intent: toTrimmedString(state.intent, 420),
      current_point: toTrimmedString(state.current_point, 1200),
      target_point: toTrimmedString(state.target_point, 1200),
      constraints: normalizeStringList(state.constraints, { maxItems: 8, itemMaxLength: 240 }),
      fast_levers: normalizeStringList(state.fast_levers, { maxItems: 8, itemMaxLength: 240 }),
      excluded_paths: normalizeStringList(state.excluded_paths, { maxItems: 8, itemMaxLength: 240 }),
      next_best_question: nextBestQuestion || null,
      relevant_entities: normalizeRelevantEntities(state.relevant_entities),
      confirmed_facts: normalizeStringList(state.confirmed_facts, { maxItems: 14, itemMaxLength: 360 }),
      uncertain_facts: normalizeStringList(state.uncertain_facts, { maxItems: 10, itemMaxLength: 360 }),
      conflicts: normalizeStringList(state.conflicts, { maxItems: 8, itemMaxLength: 360 }),
      core_conclusion: toTrimmedString(state.core_conclusion || state.conclusion, 1400),
      why_not_enough: toTrimmedString(state.why_not_enough || state.insufficiency_reason || state.why_insufficient, 1400),
      next_growth_contour: toTrimmedString(state.next_growth_contour, 1400),
    };
  }

  function normalizeDeepReasoningPayload(rawPayload, rawReplyText) {
    const payload = toProfile(rawPayload);
    const topLevelQuestion = normalizeOptionalQuestion(payload.next_best_question);
    const reasoningState = normalizeReasoningState(payload.reasoning_state, topLevelQuestion);

    return {
      final_answer: toTrimmedString(payload.final_answer, 8000) || toTrimmedString(rawReplyText, 8000),
      next_best_question: topLevelQuestion || reasoningState.next_best_question || null,
      answer_pattern: toTrimmedString(payload.answer_pattern, 120),
      reasoning_state: {
        ...reasoningState,
        next_best_question: topLevelQuestion || reasoningState.next_best_question || null,
      },
    };
  }

  function tryParseDeepReasoningJson(rawText) {
    const text = toTrimmedString(rawText, 200_000);
    if (!text) {
      return {
        parsed: null,
        parseError: 'empty_reply',
      };
    }

    try {
      return {
        parsed: JSON.parse(text),
        parseError: '',
      };
    } catch {
      // Fallback: try to parse JSON object from text envelope.
    }

    const firstBrace = text.indexOf('{');
    const lastBrace = text.lastIndexOf('}');
    if (firstBrace >= 0 && lastBrace > firstBrace) {
      const candidate = text.slice(firstBrace, lastBrace + 1);
      try {
        return {
          parsed: JSON.parse(candidate),
          parseError: '',
        };
      } catch {
        return {
          parsed: null,
          parseError: 'json_parse_failed_after_brace_extract',
        };
      }
    }

    return {
      parsed: null,
      parseError: 'json_braces_not_found',
    };
  }

  function hasNonEmptyString(value) {
    return typeof value === 'string' && value.trim().length > 0;
  }

  function hasNonEmptyArray(value) {
    return Array.isArray(value) && value.length > 0;
  }

  function normalizeComparableText(value) {
    return toTrimmedString(value, 2400)
      .toLowerCase()
      .replace(/[^\p{L}\p{N}\s]/gu, ' ')
      .replace(/\s+/g, ' ')
      .trim();
  }

  function areSemanticallyClose(left, right) {
    const a = normalizeComparableText(left);
    const b = normalizeComparableText(right);
    if (!a || !b) return false;
    if (a === b) return true;
    if (a.length >= 24 && b.includes(a)) return true;
    if (b.length >= 24 && a.includes(b)) return true;
    return false;
  }

  function countMeaningfulParagraphs(value) {
    const text = toTrimmedString(value, 8000);
    if (!text) return 0;
    return text
      .split(/\n\s*\n/g)
      .map((part) => part.trim())
      .filter((part) => part.length >= 12)
      .length;
  }

  function evaluateNextBestQuestionQuality(question, contextData) {
    const normalized = normalizeOptionalQuestion(question);
    if (!normalized) {
      return {
        present: false,
        accepted: true,
        reason: 'optional_omitted',
        value: '',
      };
    }

    const low = normalized.toLowerCase();
    const wordCount = low
      .split(/[\s,.;:!?()\[\]{}"']+/g)
      .map((item) => item.trim())
      .filter(Boolean)
      .length;
    const tooGeneric = /(что дальше\??|как думаешь\??|можно подробнее\??|что еще\??|продолжим\??)$/i.test(low);
    const hasQuestionMark = normalized.includes('?');
    const hasEnoughDepth = wordCount >= 6;

    const entityNames = (Array.isArray(contextData?.entities) ? contextData.entities : [])
      .map((entity) => toTrimmedString(entity?.name, 160).toLowerCase())
      .filter((name) => name.length >= 3)
      .slice(0, 80);
    const mentionsEntity = entityNames.some((name) => low.includes(name));

    const accepted = hasQuestionMark && hasEnoughDepth && !tooGeneric && (mentionsEntity || wordCount >= 8);

    return {
      present: true,
      accepted,
      reason: accepted
        ? 'question_opens_new_analysis_layer'
        : !hasQuestionMark
          ? 'question_mark_missing'
          : tooGeneric
            ? 'question_too_generic'
            : !hasEnoughDepth
              ? 'question_too_short'
              : 'question_not_contextualized',
      value: normalized,
    };
  }

  function evaluateProjectReasoningQuality(candidate, contextData) {
    const reasoningState = toProfile(candidate?.reasoning_state);

    const blockChecks = {
      actor: hasNonEmptyString(reasoningState.actor),
      intent: hasNonEmptyString(reasoningState.intent),
      current_point: hasNonEmptyString(reasoningState.current_point),
      target_point: hasNonEmptyString(reasoningState.target_point),
      constraints: hasNonEmptyArray(reasoningState.constraints),
      fast_levers: hasNonEmptyArray(reasoningState.fast_levers),
      excluded_paths: hasNonEmptyArray(reasoningState.excluded_paths),
      core_conclusion: hasNonEmptyString(reasoningState.core_conclusion),
      why_not_enough: hasNonEmptyString(reasoningState.why_not_enough),
      next_growth_contour: hasNonEmptyString(reasoningState.next_growth_contour),
    };

    const missingMandatoryBlocks = PROJECT_REQUIRED_REASONING_BLOCKS.filter((key) => !blockChecks[key]);
    const finalAnswerPresent = hasNonEmptyString(candidate?.final_answer);
    const finalAnswerStructured = countMeaningfulParagraphs(candidate?.final_answer) >= 5;

    const finalAnswerLower = toTrimmedString(candidate?.final_answer, 9000).toLowerCase();
    const entityNames = (Array.isArray(contextData?.entities) ? contextData.entities : [])
      .map((entity) => toTrimmedString(entity?.name, 160).toLowerCase())
      .filter((name) => name.length >= 3)
      .slice(0, 120);
    const mentionsKnownEntity = entityNames.some((name) => finalAnswerLower.includes(name));
    const hasConfirmedFacts = hasNonEmptyArray(reasoningState.confirmed_facts);
    const hasRelevantEntities = hasNonEmptyArray(reasoningState.relevant_entities);
    const factAnchored = entityNames.length === 0
      ? finalAnswerPresent
      : hasConfirmedFacts || hasRelevantEntities || mentionsKnownEntity;
    const coreConclusion = toTrimmedString(reasoningState.core_conclusion, 1400);
    const whyNotEnough = toTrimmedString(reasoningState.why_not_enough, 1400);
    const nextGrowthContour = toTrimmedString(reasoningState.next_growth_contour, 1400);
    const coreConclusionTooShort = coreConclusion.length < 40;
    const whyNotEnoughTooShort = whyNotEnough.length < 40;
    const nextGrowthContourTooShort = nextGrowthContour.length < 30;
    const factualPool = [
      ...(Array.isArray(reasoningState.confirmed_facts) ? reasoningState.confirmed_facts : []),
      ...(Array.isArray(reasoningState.uncertain_facts) ? reasoningState.uncertain_facts : []),
      ...(Array.isArray(reasoningState.conflicts) ? reasoningState.conflicts : []),
    ];
    const coreConclusionEchoesFacts = factualPool.some((item) => areSemanticallyClose(coreConclusion, item));
    const factRetellingDetected = coreConclusionTooShort || coreConclusionEchoesFacts;

    const nextQuestionQuality = evaluateNextBestQuestionQuality(
      candidate?.next_best_question || reasoningState?.next_best_question,
      contextData,
    );

    let score = 0;
    score += Object.values(blockChecks).filter(Boolean).length * 12;
    if (finalAnswerPresent) score += 10;
    if (finalAnswerStructured) score += 8;
    if (factAnchored) score += 10;
    if (!factRetellingDetected) score += 8;
    if (!whyNotEnoughTooShort) score += 6;
    if (!nextGrowthContourTooShort) score += 6;
    if (nextQuestionQuality.present && nextQuestionQuality.accepted) score += 4;
    if (nextQuestionQuality.present && !nextQuestionQuality.accepted) score -= 2;

    const passed =
      missingMandatoryBlocks.length === 0
      && finalAnswerPresent
      && finalAnswerStructured
      && factAnchored
      && !factRetellingDetected
      && !whyNotEnoughTooShort
      && !nextGrowthContourTooShort;

    return {
      passed,
      score,
      blocks: blockChecks,
      missingMandatoryBlocks,
      finalAnswerPresent,
      finalAnswerStructured,
      factAnchored,
      factRetellingDetected,
      coreConclusionTooShort,
      coreConclusionEchoesFacts,
      whyNotEnoughTooShort,
      nextGrowthContourTooShort,
      nextQuestionQuality,
    };
  }

  function buildRegenerationFeedback(gateResult) {
    const failedBlocks = Array.isArray(gateResult?.missingMandatoryBlocks)
      ? gateResult.missingMandatoryBlocks
      : [];
    const feedbackLines = [
      'QUALITY_GATE_FEEDBACK: нужно исправить ответ.',
      failedBlocks.length
        ? `Не покрыты обязательные блоки: ${failedBlocks.join(', ')}.`
        : 'Обязательные блоки покрыты, но есть проблемы качества.',
      gateResult?.factAnchored ? '' : 'Усиль привязку к фактам и сущностям из контекста.',
      gateResult?.finalAnswerStructured ? '' : 'Сделай final_answer в 5 смысловых абзацах по заданной структуре.',
      gateResult?.factRetellingDetected ? 'Избегай пересказа фактов: дай явный синтетический главный вывод.' : '',
      gateResult?.whyNotEnoughTooShort ? 'Подробно объясни, почему найденные шаги пока недостаточны для главной цели.' : '',
      gateResult?.nextGrowthContourTooShort ? 'Определи следующий контур роста (next_growth_contour), а не общий совет.' : '',
      gateResult?.nextQuestionQuality?.present && !gateResult?.nextQuestionQuality?.accepted
        ? `next_best_question некорректен: ${gateResult.nextQuestionQuality.reason}.`
        : '',
      'Сохрани single-call структуру ответа и верни строгий JSON по схеме.',
    ].filter(Boolean);

    return feedbackLines.join('\n');
  }

  function buildProjectReasoningPrompts({
    contextData,
    message,
    regenerationFeedback = '',
    previousAttempt = null,
  }) {
    const payloadContext = toProfile(contextData);
    const compactEntities = (Array.isArray(payloadContext.entities) ? payloadContext.entities : [])
      .slice(0, 140)
      .map((entity) => {
        const row = toProfile(entity);
        return {
          id: toTrimmedString(row.id || row._id, 120),
          type: toTrimmedString(row.type, 48),
          name: toTrimmedString(row.name, 160),
          description: toTrimmedString(row.description || toProfile(row.ai_metadata).description, 1800),
        };
      });
    const compactConnections = (Array.isArray(payloadContext.connections) ? payloadContext.connections : [])
      .slice(0, 220)
      .map((edge) => {
        const row = toProfile(edge);
        return {
          source: toTrimmedString(row.source, 120),
          target: toTrimmedString(row.target, 120),
          type: toTrimmedString(row.type, 64),
          label: toTrimmedString(row.label, 160),
        };
      });
    const compactHistory = (Array.isArray(payloadContext.history) ? payloadContext.history : [])
      .slice(-10)
      .map((historyItem) => {
        const row = toProfile(historyItem);
        return {
          role: row.role === 'assistant' ? 'assistant' : 'user',
          text: toTrimmedString(row.text, 1200),
        };
      });
    const compactAttachments = (Array.isArray(payloadContext.attachments) ? payloadContext.attachments : [])
      .slice(0, 8)
      .map((attachment) => {
        const row = toProfile(attachment);
        return {
          name: toTrimmedString(row.name, 160),
          contentCategory: toTrimmedString(row.contentCategory, 40),
          text: toTrimmedString(row.text, 1400),
        };
      });
    const reasoningPayload = {
      scope: toProfile(payloadContext.scope),
      stateSnapshot: toProfile(payloadContext.stateSnapshot),
      entities: compactEntities,
      connections: compactConnections,
      history: compactHistory,
      attachments: compactAttachments,
      currentUserMessage: toTrimmedString(message, 2400),
    };

    const contractText = PROJECT_REASONING_CONTRACT.map((line) => `- ${line}`).join('\n');
    const patternsText = PROJECT_ANSWER_PATTERNS.map((line) => `- ${line}`).join('\n');

    const systemPrompt = [
      'PROJECT DEEP REASONING MODE (single-call):',
      'Ты Synapse12 Project Deep Reasoning Analyst.',
      'Работай только по данным из входного JSON-контекста.',
      'Сначала выполни внутренний анализ по reasoning_contract, затем верни строго JSON по схеме.',
      'Не выдумывай факты вне переданного контекста.',
      'Не добавляй markdown и не добавляй текст вне JSON-объекта.',
      'final_answer не должен быть пересказом фактов: сначала дай главный вывод, затем действие/ограничения/следующий контур.',
      'final_answer должен быть из 5 отдельных абзацев (с пустой строкой между абзацами).',
      'next_best_question добавляй только если он реально открывает новый слой анализа.',
      'Если вопрос не нужен — верни next_best_question = null.',
    ].join('\n');

    const userPrompt = [
      'PROJECT_CONTEXT_JSON:',
      JSON.stringify(reasoningPayload, null, 2),
      '',
      'REASONING_CONTRACT (обязательный порядок):',
      contractText,
      '',
      'ANSWER_PATTERNS (ориентир структуры final_answer, не копировать текст):',
      patternsText,
      '',
      'OUTPUT CONTRACT (строго JSON):',
      '{',
      '  "final_answer": "string",',
      '  "next_best_question": "string | null",',
      '  "answer_pattern": "string",',
      '  "reasoning_state": {',
      '    "actor": "string",',
      '    "intent": "string",',
      '    "current_point": "string",',
      '    "target_point": "string",',
      '    "constraints": ["string"],',
      '    "fast_levers": ["string"],',
      '    "excluded_paths": ["string"],',
      '    "next_best_question": "string | null",',
      '    "relevant_entities": [{"id":"string","name":"string","type":"string","why_relevant":"string"}],',
      '    "confirmed_facts": ["string"],',
      '    "uncertain_facts": ["string"],',
      '    "conflicts": ["string"],',
      '    "core_conclusion": "string",',
      '    "why_not_enough": "string",',
      '    "next_growth_contour": "string"',
      '  }',
      '}',
      '',
      'ФОРМАТ final_answer (обязателен, 5 абзацев, между абзацами пустая строка):',
      '1. Главный вывод.',
      '2. Что делать прямо сейчас.',
      '3. Что не делать сейчас.',
      '4. Почему этого недостаточно для главной цели.',
      '5. Куда копать дальше / следующий вопрос.',
      '',
      'КРИТИЧЕСКОЕ ТРЕБОВАНИЕ: обязательные блоки reasoning_state должны быть заполнены содержательно:',
      '- actor, intent, current_point, target_point, constraints, fast_levers, excluded_paths, core_conclusion, why_not_enough, next_growth_contour.',
      '- next_best_question — условный блок, только при реальной пользе.',
      regenerationFeedback ? '' : '',
      regenerationFeedback ? regenerationFeedback : '',
      previousAttempt
        ? ['PREVIOUS_DRAFT_JSON (исправь недочеты, не копируй вслепую):', JSON.stringify(previousAttempt, null, 2)].join('\n')
        : '',
    ]
      .filter(Boolean)
      .join('\n');

    return {
      systemPrompt,
      userPrompt,
    };
  }

  function joinNumberedList(values, fallbackText) {
    const items = normalizeStringList(values, { maxItems: 3, itemMaxLength: 260 });
    if (!items.length) return fallbackText;
    return items.map((item, index) => `${index + 1}) ${item}`).join('\n');
  }

  function buildStructuredFinalAnswer(candidate, nextQuestionDecision) {
    const payload = toProfile(candidate);
    const state = toProfile(payload.reasoning_state);
    const rawFinalAnswer = toTrimmedString(payload.final_answer, 8000);
    const coreConclusion = toTrimmedString(state.core_conclusion, 1400);
    const whyNotEnough = toTrimmedString(state.why_not_enough, 1400);
    const nextGrowthContour = toTrimmedString(state.next_growth_contour, 1400);
    const doNow = joinNumberedList(
      state.fast_levers,
      'Критические быстрые действия не выделены; нужно уточнить контекст исполнения.',
    );
    const dontDo = joinNumberedList(
      state.excluded_paths,
      'Неподходящие шаги не выделены; высок риск распыления ресурсов.',
    );
    const nextQuestion = nextQuestionDecision?.present && nextQuestionDecision?.accepted
      ? toTrimmedString(nextQuestionDecision.value, 360)
      : '';

    const blocks = [
      `1. Главный вывод.\n${coreConclusion || 'Главный вывод не зафиксирован.'}`,
      `2. Что делать прямо сейчас.\n${doNow}`,
      `3. Что не делать сейчас.\n${dontDo}`,
      `4. Почему этого недостаточно для главной цели.\n${whyNotEnough || 'Причина недостаточности не раскрыта.'}`,
      `5. Куда копать дальше / следующий вопрос.\n${[
        nextGrowthContour || 'Следующий контур роста не определен.',
        nextQuestion ? `Уточняющий вопрос: ${nextQuestion}` : '',
      ].filter(Boolean).join('\n')}`,
    ];
    const structured = blocks.join('\n\n');

    // Keep fallback for emergency cases when reasoning-state synthesis is empty.
    if (!coreConclusion && !whyNotEnough && !nextGrowthContour && rawFinalAnswer) {
      return rawFinalAnswer;
    }
    return structured;
  }

  async function requestProjectReasoningAttempt({
    attemptIndex,
    ownerId,
    deepModel,
    includeDebug,
    scopeContext,
    message,
    history,
    attachments,
    roleSelection,
    questionGate,
    systemPrompt,
    userPrompt,
    contextData,
    llmSerializationTrace,
  }) {
    const requestConfig = resolveProjectDeepReasoningRequestConfig();
    const requestPreview = typeof aiProvider.previewOpenAiAgentRequest === 'function'
      ? aiProvider.previewOpenAiAgentRequest({
        model: deepModel,
        systemPrompt,
        userPrompt,
        temperature: requestConfig.temperature,
        maxOutputTokens: requestConfig.maxOutputTokens,
        timeoutMs: requestConfig.timeoutMs,
        reasoningEffort: requestConfig.reasoningEffort,
        verbosity: requestConfig.verbosity,
        jsonSchema: PROJECT_DEEP_REASONING_OUTPUT_SCHEMA,
      })
      : null;

    const payloadSize = buildRequestBodySize(toProfile(requestPreview?.requestBody));

    const traceMeta = {
      label: `agent-chat.project.deep-reasoning.pass${attemptIndex}`,
      ownerId,
      model: deepModel,
      scope: {
        type: scopeContext.scopeType,
        entityType: scopeContext.entityType,
        projectId: scopeContext.projectId,
      },
      promptLengths: {
        system: systemPrompt.length,
        user: userPrompt.length,
      },
      messageLength: message.length,
      historyLength: history.length,
      attachmentsCount: attachments.length,
      llmPayload: llmSerializationTrace?.payloadSize || { chars: 0, bytes: 0 },
      includeDebug,
      selectedRoles: roleSelection.selectedRoles.map((role) => role.name),
      questionGate,
      requestBodySize: payloadSize,
    };

    const aiResponse = await withAiTrace(traceMeta, () => aiProvider.requestOpenAiAgentReply({
      systemPrompt,
      userPrompt,
      includeRawPayload: includeDebug,
      model: deepModel,
      temperature: requestConfig.temperature,
      maxOutputTokens: requestConfig.maxOutputTokens,
      allowEmptyResponse: true,
      emptyResponseFallback: 'Пустой ответ от модели. Уточните запрос или повторите через несколько секунд.',
      timeoutMs: requestConfig.timeoutMs,
      reasoningEffort: requestConfig.reasoningEffort,
      verbosity: requestConfig.verbosity,
      jsonSchema: PROJECT_DEEP_REASONING_OUTPUT_SCHEMA,
      singleRequest: true,
    }));

    const parsedResult = tryParseDeepReasoningJson(aiResponse.reply);
    const normalizedCandidate = normalizeDeepReasoningPayload(parsedResult.parsed, aiResponse.reply);
    const qualityGate = evaluateProjectReasoningQuality(normalizedCandidate, contextData);

    return {
      attemptIndex,
      aiResponse,
      requestPreview,
      payloadSize,
      parsedResult,
      normalizedCandidate,
      qualityGate,
      systemPrompt,
      userPrompt,
    };
  }

  function pickBestAttempt(attempts) {
    const source = Array.isArray(attempts) ? attempts.filter(Boolean) : [];
    if (!source.length) return null;

    return source
      .slice()
      .sort((left, right) => {
        const leftScore = Number(left?.qualityGate?.score) || 0;
        const rightScore = Number(right?.qualityGate?.score) || 0;
        if (rightScore !== leftScore) return rightScore - leftScore;
        return (right?.attemptIndex || 0) - (left?.attemptIndex || 0);
      })[0];
  }

  async function buildPreview({
    scope,
    scopeContext,
    history,
    attachments,
    requestedMessage,
    roleHint,
  }) {
    const latestUserMessage = [...history]
      .reverse()
      .find((historyItem) => historyItem.role === 'user' && historyItem.text)?.text || '';
    const message = toTrimmedString(requestedMessage, 2400) || latestUserMessage || '';
    const hasQuestion = Boolean(message);
    const historyBeforeDedup = history.length;
    const historyDedup = dedupeHistoryTailByCurrentMessage(history, message);
    const dedupedHistory = historyDedup.history;

    const llmContextResult = buildAgentLlmContext({
      scopeContext,
      history: dedupedHistory,
      attachments,
      message,
    });
    const contextData = llmContextResult.contextData;
    const llmSerializationTrace = llmContextResult.trace;
    const normalizedRoleHint = toTrimmedString(roleHint, 24) || 'default';
    const roleSelection = hasQuestion
      ? aiPrompts.selectAgentRolesOnDemand({
        contextData,
        message,
        roleHint: normalizedRoleHint,
      })
      : {
        selectedRoles: [],
        whySelected: [],
        droppedRoles: [],
        roleHint: aiPrompts.normalizeDetectedRole(normalizedRoleHint),
      };
    const questionGate = hasQuestion
      ? aiPrompts.evaluateAgentQuestionGate({
        contextData,
        message,
        selectedRoles: roleSelection.selectedRoles,
      })
      : {
        allowQuestion: false,
        allowReason: 'empty user message',
        decisionIntent: false,
        missingSignals: [],
        questionFocus: '',
        entitiesInContext: Array.isArray(contextData?.entities) ? contextData.entities.length : 0,
        stage: toTrimmedString(contextData?.stateSnapshot?.stage, 24) || 'unknown',
        policy: 'question_blocked_unless_plan_changes',
      };
    const detectedRole = resolveCompatibleDetectedRole(roleSelection, normalizedRoleHint);

    const deepModel =
      toTrimmedString(OPENAI_DEEP_MODEL, 120) ||
      toTrimmedString(OPENAI_PROJECT_MODEL, 120) ||
      '';

    const promptPack = hasQuestion
      ? buildProjectReasoningPrompts({
        contextData,
        message,
      })
      : { systemPrompt: '', userPrompt: '' };

    const systemPromptWithoutRoleInjection = promptPack.systemPrompt;
    const systemPrompt = promptPack.systemPrompt;
    const userPrompt = promptPack.userPrompt;
    const requestConfig = resolveProjectDeepReasoningRequestConfig();

    const requestPreviewBeforeRoleInjection = hasQuestion && typeof aiProvider.previewOpenAiAgentRequest === 'function'
      ? aiProvider.previewOpenAiAgentRequest({
        model: deepModel,
        systemPrompt: systemPromptWithoutRoleInjection,
        userPrompt,
        temperature: requestConfig.temperature,
        maxOutputTokens: requestConfig.maxOutputTokens,
        timeoutMs: requestConfig.timeoutMs,
        reasoningEffort: requestConfig.reasoningEffort,
        verbosity: requestConfig.verbosity,
        jsonSchema: PROJECT_DEEP_REASONING_OUTPUT_SCHEMA,
      })
      : null;
    const mainReplyRequestPreview = hasQuestion && typeof aiProvider.previewOpenAiAgentRequest === 'function'
      ? aiProvider.previewOpenAiAgentRequest({
        model: deepModel,
        systemPrompt,
        userPrompt,
        temperature: requestConfig.temperature,
        maxOutputTokens: requestConfig.maxOutputTokens,
        timeoutMs: requestConfig.timeoutMs,
        reasoningEffort: requestConfig.reasoningEffort,
        verbosity: requestConfig.verbosity,
        jsonSchema: PROJECT_DEEP_REASONING_OUTPUT_SCHEMA,
      })
      : null;

    const payloadSizeBeforeRoleInjection = buildRequestBodySize(toProfile(requestPreviewBeforeRoleInjection?.requestBody));
    const payloadSizeAfterRoleInjection = buildRequestBodySize(toProfile(mainReplyRequestPreview?.requestBody));

    const contextJson = JSON.stringify(contextData, null, 2);
    const llmPromptText = hasQuestion ? `${systemPrompt}\n${userPrompt}` : '';
    const entities = Array.isArray(contextData?.entities) ? contextData.entities : [];
    const connections = Array.isArray(contextData?.connections) ? contextData.connections : [];
    const sourceNodes = Array.isArray(llmSerializationTrace?.sourceNodes) ? llmSerializationTrace.sourceNodes : [];
    const sourceEdges = Array.isArray(llmSerializationTrace?.sourceEdges) ? llmSerializationTrace.sourceEdges : [];

    const preview = {
      timestamp: new Date().toISOString(),
      scope: {
        type: scope.type,
        entityType: scope.entityType,
        projectId: scope.projectId,
        scopeKey: scope.scopeKey,
        totalEntities: scopeContext.totalEntities,
        entitiesInContext: entities.length,
        connectionsInContext: connections.length,
      },
      input: {
        message,
        hasQuestion,
        history: dedupedHistory,
        attachments,
        historyDedup: {
          before: historyBeforeDedup,
          after: dedupedHistory.length,
          droppedTailDuplicates: historyDedup.droppedCount,
        },
      },
      semanticRouter: {
        mode: 'role-on-demand',
        roleHint: roleSelection.roleHint,
        selectedRoles: roleSelection.selectedRoles,
        whySelected: roleSelection.whySelected,
        droppedRoles: roleSelection.droppedRoles,
        questionGate: {
          asked: false,
          reason: 'preview-mode-no-assistant-reply',
          allowed: questionGate.allowQuestion === true,
          allowReason: questionGate.allowReason,
          questionFocus: questionGate.questionFocus,
        },
        requestBody: null,
      },
      prompts: {
        detectedRole,
        selectedRoles: roleSelection.selectedRoles,
        model: deepModel,
        systemPromptWithoutRoleInjection,
        systemPrompt,
        userPrompt,
        payloadSizeBeforeRoleInjection,
        payloadSizeAfterRoleInjection,
        requestBodyBeforeRoleInjection: hasQuestion
          ? toProfile(requestPreviewBeforeRoleInjection?.requestBody)
          : null,
        requestBody: hasQuestion
          ? toProfile(mainReplyRequestPreview?.requestBody)
          : null,
      },
      contextData,
      llmSerialization: llmSerializationTrace,
      entitiesSummary: summarizePreviewEntities(entities),
    };

    const previewJson = JSON.stringify(preview);

    return {
      stats: {
        totalEntitiesInProject: scopeContext.totalEntities,
        entitiesInContext: entities.length,
        connectionsInContext: connections.length,
        sourceNodesInScope: sourceNodes.length,
        sourceEdgesInScope: sourceEdges.length,
        historyMessages: dedupedHistory.length,
        historyTextChars: dedupedHistory.reduce((sum, item) => sum + String(item.text || '').length, 0),
        attachmentsCount: attachments.length,
        contextChars: contextJson.length,
        contextBytes: Buffer.byteLength(contextJson, 'utf8'),
        llmPayloadChars: Number(llmSerializationTrace?.payloadSize?.chars) || 0,
        llmPayloadBytes: Number(llmSerializationTrace?.payloadSize?.bytes) || 0,
        routerPromptChars: 0,
        routerPromptBytes: 0,
        requestBodyBeforeRoleInjectionChars: payloadSizeBeforeRoleInjection.chars,
        requestBodyBeforeRoleInjectionBytes: payloadSizeBeforeRoleInjection.bytes,
        requestBodyAfterRoleInjectionChars: payloadSizeAfterRoleInjection.chars,
        requestBodyAfterRoleInjectionBytes: payloadSizeAfterRoleInjection.bytes,
        llmPromptChars: hasQuestion ? llmPromptText.length : 0,
        llmPromptBytes: hasQuestion ? Buffer.byteLength(llmPromptText, 'utf8') : 0,
        previewJsonBytes: Buffer.byteLength(previewJson, 'utf8'),
      },
      preview,
    };
  }

  async function buildReply({
    ownerId,
    scopeContext,
    message,
    rawHistory,
    attachments,
    includeDebug,
    roleHint,
    monitorMode = false,
  }) {
    const historyDedup = dedupeHistoryTailByCurrentMessage(rawHistory, message);
    const history = historyDedup.history;
    const llmContextResult = buildAgentLlmContext({
      scopeContext,
      history,
      attachments,
      message,
    });
    const contextData = llmContextResult.contextData;
    const llmSerializationTrace = llmContextResult.trace;
    const normalizedRoleHint = toTrimmedString(roleHint, 24) || 'default';
    const roleSelection = aiPrompts.selectAgentRolesOnDemand({
      contextData,
      message,
      roleHint: normalizedRoleHint,
    });
    const questionGate = aiPrompts.evaluateAgentQuestionGate({
      contextData,
      message,
      selectedRoles: roleSelection.selectedRoles,
    });
    const detectedRole = resolveCompatibleDetectedRole(roleSelection, normalizedRoleHint);

    const deepModel =
      toTrimmedString(OPENAI_DEEP_MODEL, 120) ||
      toTrimmedString(OPENAI_PROJECT_MODEL, 120) ||
      '';

    const passOnePromptPack = buildProjectReasoningPrompts({
      contextData,
      message,
    });
    const systemPromptWithoutRoleInjection = passOnePromptPack.systemPrompt;
    const systemPrompt = passOnePromptPack.systemPrompt;
    const userPrompt = passOnePromptPack.userPrompt;
    const requestConfig = resolveProjectDeepReasoningRequestConfig();

    const requestPreviewBeforeRoleInjection = typeof aiProvider.previewOpenAiAgentRequest === 'function'
      ? aiProvider.previewOpenAiAgentRequest({
        model: deepModel,
        systemPrompt: systemPromptWithoutRoleInjection,
        userPrompt,
        temperature: requestConfig.temperature,
        maxOutputTokens: requestConfig.maxOutputTokens,
        timeoutMs: requestConfig.timeoutMs,
        reasoningEffort: requestConfig.reasoningEffort,
        verbosity: requestConfig.verbosity,
        jsonSchema: PROJECT_DEEP_REASONING_OUTPUT_SCHEMA,
      })
      : null;
    const mainReplyRequestPreview = typeof aiProvider.previewOpenAiAgentRequest === 'function'
      ? aiProvider.previewOpenAiAgentRequest({
        model: deepModel,
        systemPrompt,
        userPrompt,
        temperature: requestConfig.temperature,
        maxOutputTokens: requestConfig.maxOutputTokens,
        timeoutMs: requestConfig.timeoutMs,
        reasoningEffort: requestConfig.reasoningEffort,
        verbosity: requestConfig.verbosity,
        jsonSchema: PROJECT_DEEP_REASONING_OUTPUT_SCHEMA,
      })
      : null;

    const payloadSizeBeforeRoleInjection = buildRequestBodySize(toProfile(requestPreviewBeforeRoleInjection?.requestBody));
    const payloadSizeAfterRoleInjection = buildRequestBodySize(toProfile(mainReplyRequestPreview?.requestBody));

    const attempts = [];

    const passOne = await requestProjectReasoningAttempt({
      attemptIndex: 1,
      ownerId,
      deepModel,
      includeDebug,
      scopeContext,
      message,
      history,
      attachments,
      roleSelection,
      questionGate,
      systemPrompt: passOnePromptPack.systemPrompt,
      userPrompt: passOnePromptPack.userPrompt,
      contextData,
      llmSerializationTrace,
    });
    attempts.push(passOne);

    if (!passOne.qualityGate.passed && PROJECT_DEEP_REASONING_MAX_CALLS > 1) {
      const passTwoPromptPack = buildProjectReasoningPrompts({
        contextData,
        message,
        regenerationFeedback: buildRegenerationFeedback(passOne.qualityGate),
        previousAttempt: passOne.normalizedCandidate,
      });

      const passTwo = await requestProjectReasoningAttempt({
        attemptIndex: 2,
        ownerId,
        deepModel,
        includeDebug,
        scopeContext,
        message,
        history,
        attachments,
        roleSelection,
        questionGate,
        systemPrompt: passTwoPromptPack.systemPrompt,
        userPrompt: passTwoPromptPack.userPrompt,
        contextData,
        llmSerializationTrace,
      });
      attempts.push(passTwo);
    }

    const selectedAttempt = pickBestAttempt(attempts) || passOne;
    const selectedCandidate = selectedAttempt.normalizedCandidate;
    const nextQuestionDecision = evaluateNextBestQuestionQuality(
      selectedCandidate?.next_best_question || selectedCandidate?.reasoning_state?.next_best_question,
      contextData,
    );

    const finalReply = buildStructuredFinalAnswer(selectedCandidate, nextQuestionDecision);
    const questionGateResult = aiPrompts.inspectAgentReplyQuestionGate({
      reply: finalReply,
      questionGate,
    });

    const canRunProjectAutoEnrichment = typeof runProjectChatAutoEnrichment === 'function';
    const shouldQueueProjectAutoEnrichment = (
      scopeContext.scopeType === 'project'
      && canRunProjectAutoEnrichment
      && monitorMode !== true
    );
    if (shouldQueueProjectAutoEnrichment) {
      void runProjectChatAutoEnrichment({
        ownerId,
        scopeContext,
        sourceEntities: Array.isArray(scopeContext.sourceEntities) ? scopeContext.sourceEntities : scopeContext.entities,
        contextData,
        message,
        history,
        assistantReply: finalReply,
        includeDebug,
      }).catch(() => {
        // Background enrichment must never break the main reply.
      });
    } else if (scopeContext.scopeType === 'project') {
      console.warn('[agent-chat] project auto enrichment is unavailable');
    }

    const selectedModel =
      toTrimmedString(selectedAttempt?.aiResponse?.debug?.response?.model, 120)
      || toTrimmedString(selectedAttempt?.aiResponse?.debug?.request?.model, 120)
      || deepModel
      || 'unknown';

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
          historyDedup: {
            before: rawHistory.length,
            after: history.length,
            droppedTailDuplicates: historyDedup.droppedCount,
          },
        },
        llmSerialization: llmSerializationTrace,
        semanticRouter: {
          mode: 'role-on-demand',
          roleHint: roleSelection.roleHint,
          selectedRoles: roleSelection.selectedRoles,
          whySelected: roleSelection.whySelected,
          droppedRoles: roleSelection.droppedRoles,
          questionGate: {
            asked: questionGateResult.asked,
            reason: questionGateResult.reason,
            allowed: questionGate.allowQuestion === true,
            allowReason: questionGate.allowReason,
            questionFocus: questionGate.questionFocus,
            extractedQuestion: questionGateResult.extractedQuestion,
          },
          detectedRole,
        },
        prompts: {
          systemPromptWithoutRoleInjection,
          systemPrompt,
          userPrompt,
          payloadSizeBeforeRoleInjection,
          payloadSizeAfterRoleInjection,
          requestBodyBeforeRoleInjection: toProfile(requestPreviewBeforeRoleInjection?.requestBody),
          requestBody: toProfile(mainReplyRequestPreview?.requestBody),
        },
        deepReasoning: {
          mode: PROJECT_DEEP_REASONING_MAX_CALLS > 1
            ? 'single_call_with_one_regeneration_max'
            : 'single_call_only',
          requestConfig,
          reasoningContract: PROJECT_REASONING_CONTRACT,
          answerPatterns: PROJECT_ANSWER_PATTERNS,
          maxCalls: PROJECT_DEEP_REASONING_MAX_CALLS,
          regenerationUsed: attempts.length > 1,
          selectedAttempt: selectedAttempt?.attemptIndex || 1,
          qualityGate: selectedAttempt?.qualityGate || null,
          reasoningState: selectedCandidate?.reasoning_state || null,
          answerPattern: selectedCandidate?.answer_pattern || '',
          nextBestQuestion: nextQuestionDecision,
          finalAnswerAssembly: 'server_structured_from_reasoning_state',
          attempts: attempts.map((attempt) => ({
            attemptIndex: attempt.attemptIndex,
            qualityGate: attempt.qualityGate,
            parseError: attempt.parsedResult?.parseError || '',
            model: toTrimmedString(attempt?.aiResponse?.debug?.response?.model, 120) || deepModel,
            usage: attempt?.aiResponse?.usage || null,
            payloadSize: attempt?.payloadSize || { chars: 0, bytes: 0 },
          })),
        },
        response: {
          reply: finalReply,
          usage: selectedAttempt?.aiResponse?.usage,
          model: selectedModel,
        },
        projectAutoEnrichment: scopeContext.scopeType === 'project'
          ? {
            queued: shouldQueueProjectAutoEnrichment,
            projectId: scopeContext.projectId,
            monitorMode: monitorMode === true,
          }
          : null,
        provider: selectedAttempt?.aiResponse?.debug || {},
      }
      : undefined;

    return {
      reply: finalReply,
      usage: selectedAttempt?.aiResponse?.usage,
      model: selectedModel,
      detectedRole,
      selectedRoles: roleSelection.selectedRoles,
      context: {
        scopeType: scopeContext.scopeType,
        entityType: scopeContext.entityType,
        projectId: scopeContext.projectId,
        totalEntities: scopeContext.totalEntities,
      },
      ...(debugPayload ? { debug: debugPayload } : {}),
    };
  }

  return {
    buildPreview,
    buildReply,
  };
}

module.exports = {
  createProjectChatFlow,
};
