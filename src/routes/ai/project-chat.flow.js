const PROJECT_DEEP_REASONING_OUTPUT_SCHEMA = Object.freeze({
  type: 'json_schema',
  name: 'ProjectDeepReasoningReply',
  strict: true,
  schema: {
    type: 'object',
    required: ['final_answer', 'reasoning_state', 'next_best_question'],
    additionalProperties: false,
    properties: {
      final_answer: { type: 'string' },
      next_best_question: { anyOf: [{ type: 'string' }, { type: 'null' }] },
      answer_pattern: { type: 'string' },
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
        ],
        additionalProperties: false,
        properties: {
          actor: { type: 'string' },
          intent: { type: 'string' },
          current_point: { type: 'string' },
          target_point: { type: 'string' },
          constraints: { type: 'array', items: { type: 'string' } },
          fast_levers: { type: 'array', items: { type: 'string' } },
          excluded_paths: { type: 'array', items: { type: 'string' } },
          next_best_question: { anyOf: [{ type: 'string' }, { type: 'null' }] },
          relevant_entities: {
            type: 'array',
            items: {
              type: 'object',
              required: ['id', 'name', 'type', 'why_relevant'],
              additionalProperties: false,
              properties: {
                id: { type: 'string' },
                name: { type: 'string' },
                type: { type: 'string' },
                why_relevant: { type: 'string' },
              },
            },
          },
          confirmed_facts: { type: 'array', items: { type: 'string' } },
          uncertain_facts: { type: 'array', items: { type: 'string' } },
          conflicts: { type: 'array', items: { type: 'string' } },
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
  '8. При необходимости задай один next_best_question, только если он открывает новый слой анализа.',
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
]);

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
    };

    const missingMandatoryBlocks = PROJECT_REQUIRED_REASONING_BLOCKS.filter((key) => !blockChecks[key]);
    const finalAnswerPresent = hasNonEmptyString(candidate?.final_answer);

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

    const nextQuestionQuality = evaluateNextBestQuestionQuality(
      candidate?.next_best_question || reasoningState?.next_best_question,
      contextData,
    );

    let score = 0;
    score += Object.values(blockChecks).filter(Boolean).length * 12;
    if (finalAnswerPresent) score += 10;
    if (factAnchored) score += 10;
    if (nextQuestionQuality.present && nextQuestionQuality.accepted) score += 4;
    if (nextQuestionQuality.present && !nextQuestionQuality.accepted) score -= 2;

    const passed =
      missingMandatoryBlocks.length === 0
      && finalAnswerPresent
      && factAnchored;

    return {
      passed,
      score,
      blocks: blockChecks,
      missingMandatoryBlocks,
      finalAnswerPresent,
      factAnchored,
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
    const baseSystemPrompt = aiPrompts.buildAgentSystemPrompt(contextData);
    const baseUserPrompt = aiPrompts.buildAgentUserPrompt({
      contextData,
      message,
    });

    const contractText = PROJECT_REASONING_CONTRACT.map((line) => `- ${line}`).join('\n');
    const patternsText = PROJECT_ANSWER_PATTERNS.map((line) => `- ${line}`).join('\n');

    const systemPrompt = [
      baseSystemPrompt,
      '',
      'PROJECT DEEP REASONING MODE (single-call):',
      'Сначала выполни внутренний анализ по reasoning_contract, затем верни только JSON по схеме.',
      'Не выдумывай факты вне переданного контекста.',
      'next_best_question добавляй только если он реально открывает новый слой анализа.',
      'Если вопрос не нужен — верни next_best_question = null.',
    ].join('\n');

    const userPrompt = [
      baseUserPrompt,
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
      '    "conflicts": ["string"]',
      '  }',
      '}',
      '',
      'КРИТИЧЕСКОЕ ТРЕБОВАНИЕ: обязательные блоки reasoning_state должны быть заполнены содержательно:',
      '- actor, intent, current_point, target_point, constraints, fast_levers, excluded_paths.',
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

  function composeFinalReply(finalAnswer, nextQuestionDecision) {
    const answer = toTrimmedString(finalAnswer, 9000);
    if (!answer) {
      return 'Пустой ответ от модели. Уточните запрос или повторите через несколько секунд.';
    }

    if (!nextQuestionDecision?.present || !nextQuestionDecision?.accepted || !nextQuestionDecision?.value) {
      return answer;
    }

    return `${answer}\n\nВопрос: ${nextQuestionDecision.value}`;
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
    const requestPreview = typeof aiProvider.previewOpenAiAgentRequest === 'function'
      ? aiProvider.previewOpenAiAgentRequest({
        model: deepModel,
        systemPrompt,
        userPrompt,
        temperature: AGENT_CHAT_MAIN_REQUEST_CONFIG.temperature,
        maxOutputTokens: AGENT_CHAT_MAIN_REQUEST_CONFIG.maxOutputTokens,
        timeoutMs: AGENT_CHAT_MAIN_REQUEST_CONFIG.timeoutMs,
        reasoningEffort: AGENT_CHAT_MAIN_REQUEST_CONFIG.reasoningEffort,
        verbosity: AGENT_CHAT_MAIN_REQUEST_CONFIG.verbosity,
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
      temperature: AGENT_CHAT_MAIN_REQUEST_CONFIG.temperature,
      maxOutputTokens: AGENT_CHAT_MAIN_REQUEST_CONFIG.maxOutputTokens,
      allowEmptyResponse: true,
      emptyResponseFallback: 'Пустой ответ от модели. Уточните запрос или повторите через несколько секунд.',
      timeoutMs: AGENT_CHAT_MAIN_REQUEST_CONFIG.timeoutMs,
      reasoningEffort: AGENT_CHAT_MAIN_REQUEST_CONFIG.reasoningEffort,
      verbosity: AGENT_CHAT_MAIN_REQUEST_CONFIG.verbosity,
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
      'gpt-5';

    const promptPack = hasQuestion
      ? buildProjectReasoningPrompts({
        contextData,
        message,
      })
      : { systemPrompt: '', userPrompt: '' };

    const systemPromptWithoutRoleInjection = promptPack.systemPrompt;
    const systemPrompt = promptPack.systemPrompt;
    const userPrompt = promptPack.userPrompt;

    const requestPreviewBeforeRoleInjection = hasQuestion && typeof aiProvider.previewOpenAiAgentRequest === 'function'
      ? aiProvider.previewOpenAiAgentRequest({
        model: deepModel,
        systemPrompt: systemPromptWithoutRoleInjection,
        userPrompt,
        temperature: AGENT_CHAT_MAIN_REQUEST_CONFIG.temperature,
        maxOutputTokens: AGENT_CHAT_MAIN_REQUEST_CONFIG.maxOutputTokens,
        timeoutMs: AGENT_CHAT_MAIN_REQUEST_CONFIG.timeoutMs,
        reasoningEffort: AGENT_CHAT_MAIN_REQUEST_CONFIG.reasoningEffort,
        verbosity: AGENT_CHAT_MAIN_REQUEST_CONFIG.verbosity,
        jsonSchema: PROJECT_DEEP_REASONING_OUTPUT_SCHEMA,
      })
      : null;
    const mainReplyRequestPreview = hasQuestion && typeof aiProvider.previewOpenAiAgentRequest === 'function'
      ? aiProvider.previewOpenAiAgentRequest({
        model: deepModel,
        systemPrompt,
        userPrompt,
        temperature: AGENT_CHAT_MAIN_REQUEST_CONFIG.temperature,
        maxOutputTokens: AGENT_CHAT_MAIN_REQUEST_CONFIG.maxOutputTokens,
        timeoutMs: AGENT_CHAT_MAIN_REQUEST_CONFIG.timeoutMs,
        reasoningEffort: AGENT_CHAT_MAIN_REQUEST_CONFIG.reasoningEffort,
        verbosity: AGENT_CHAT_MAIN_REQUEST_CONFIG.verbosity,
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
      'gpt-5';

    const passOnePromptPack = buildProjectReasoningPrompts({
      contextData,
      message,
    });
    const systemPromptWithoutRoleInjection = passOnePromptPack.systemPrompt;
    const systemPrompt = passOnePromptPack.systemPrompt;
    const userPrompt = passOnePromptPack.userPrompt;

    const requestPreviewBeforeRoleInjection = typeof aiProvider.previewOpenAiAgentRequest === 'function'
      ? aiProvider.previewOpenAiAgentRequest({
        model: deepModel,
        systemPrompt: systemPromptWithoutRoleInjection,
        userPrompt,
        temperature: AGENT_CHAT_MAIN_REQUEST_CONFIG.temperature,
        maxOutputTokens: AGENT_CHAT_MAIN_REQUEST_CONFIG.maxOutputTokens,
        timeoutMs: AGENT_CHAT_MAIN_REQUEST_CONFIG.timeoutMs,
        reasoningEffort: AGENT_CHAT_MAIN_REQUEST_CONFIG.reasoningEffort,
        verbosity: AGENT_CHAT_MAIN_REQUEST_CONFIG.verbosity,
        jsonSchema: PROJECT_DEEP_REASONING_OUTPUT_SCHEMA,
      })
      : null;
    const mainReplyRequestPreview = typeof aiProvider.previewOpenAiAgentRequest === 'function'
      ? aiProvider.previewOpenAiAgentRequest({
        model: deepModel,
        systemPrompt,
        userPrompt,
        temperature: AGENT_CHAT_MAIN_REQUEST_CONFIG.temperature,
        maxOutputTokens: AGENT_CHAT_MAIN_REQUEST_CONFIG.maxOutputTokens,
        timeoutMs: AGENT_CHAT_MAIN_REQUEST_CONFIG.timeoutMs,
        reasoningEffort: AGENT_CHAT_MAIN_REQUEST_CONFIG.reasoningEffort,
        verbosity: AGENT_CHAT_MAIN_REQUEST_CONFIG.verbosity,
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

    if (!passOne.qualityGate.passed) {
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

    const finalReply = composeFinalReply(selectedCandidate?.final_answer, nextQuestionDecision);
    const questionGateResult = aiPrompts.inspectAgentReplyQuestionGate({
      reply: finalReply,
      questionGate,
    });

    const canRunProjectAutoEnrichment = typeof runProjectChatAutoEnrichment === 'function';
    const shouldQueueProjectAutoEnrichment = scopeContext.scopeType === 'project' && canRunProjectAutoEnrichment;
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
      || deepModel;

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
          mode: 'single_call_with_one_regeneration_max',
          reasoningContract: PROJECT_REASONING_CONTRACT,
          answerPatterns: PROJECT_ANSWER_PATTERNS,
          maxCalls: 2,
          regenerationUsed: attempts.length > 1,
          selectedAttempt: selectedAttempt?.attemptIndex || 1,
          qualityGate: selectedAttempt?.qualityGate || null,
          reasoningState: selectedCandidate?.reasoning_state || null,
          nextBestQuestion: nextQuestionDecision,
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
