function createCollectionChatFlow({ deps, helpers }) {
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
    buildAgentLlmContext,
  } = helpers;

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
      .find((message) => message.role === 'user' && message.text)?.text || '';
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
    const systemPromptWithoutRoleInjection = hasQuestion
      ? aiPrompts.buildAgentSystemPrompt(contextData)
      : '';
    const systemPrompt = hasQuestion
      ? aiPrompts.buildAgentSystemPrompt(contextData)
      : '';
    const userPrompt = hasQuestion
      ? aiPrompts.buildAgentUserPrompt({
        contextData,
        message,
      })
      : '';

    const deepModel =
      toTrimmedString(OPENAI_PROJECT_MODEL, 120) ||
      toTrimmedString(OPENAI_MODEL, 120) ||
      'gpt-5';
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
      toTrimmedString(OPENAI_PROJECT_MODEL, 120) ||
      toTrimmedString(OPENAI_MODEL, 120) ||
      'gpt-5';

    const systemPrompt = aiPrompts.buildAgentSystemPrompt(contextData);
    const systemPromptWithoutRoleInjection = systemPrompt;
    const userPrompt = aiPrompts.buildAgentUserPrompt({
      contextData,
      message,
    });
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
      })
      : null;
    const payloadSizeBeforeRoleInjection = buildRequestBodySize(toProfile(requestPreviewBeforeRoleInjection?.requestBody));
    const payloadSizeAfterRoleInjection = buildRequestBodySize(toProfile(mainReplyRequestPreview?.requestBody));

    const chatTraceMeta = {
      label: 'agent-chat.reply',
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
      detectedRole,
      selectedRoles: roleSelection.selectedRoles.map((role) => role.name),
      questionGate,
      payloadSizeBeforeRoleInjection,
      payloadSizeAfterRoleInjection,
    };
    const aiResponse = await withAiTrace(chatTraceMeta, () => aiProvider.requestOpenAiAgentReply({
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
      singleRequest: true,
    }));
    const usedModel = toTrimmedString(aiResponse?.debug?.response?.model, 120) || deepModel;
    const questionGateResult = aiPrompts.inspectAgentReplyQuestionGate({
      reply: aiResponse.reply,
      questionGate,
    });

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
        response: {
          reply: aiResponse.reply,
          usage: aiResponse.usage,
          model: usedModel,
        },
        projectAutoEnrichment: scopeContext.scopeType === 'project'
          ? {
            queued: false,
            projectId: scopeContext.projectId,
          }
          : null,
        provider: aiResponse.debug || {},
      }
      : undefined;

    return {
      reply: aiResponse.reply,
      usage: aiResponse.usage,
      model: usedModel,
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
  createCollectionChatFlow,
};
