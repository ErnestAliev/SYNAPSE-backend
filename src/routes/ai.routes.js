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

  async function setEntityAnalysisPending(entity, pending, errorMessage = '') {
    if (!entity) return null;
    const metadata = toProfile(entity.ai_metadata);
    metadata.analysis_pending = Boolean(pending);
    if (pending) {
      metadata.analysis_started_at = new Date().toISOString();
    } else {
      metadata.analysis_completed_at = new Date().toISOString();
      if (errorMessage) {
        metadata.analysis_error = errorMessage;
      } else {
        delete metadata.analysis_error;
      }
    }
    entity.ai_metadata = metadata;
    await entity.save();
    return entity;
  }
  function buildAiTraceId(label) {
    const safeLabel = toTrimmedString(label, 80) || 'ai-call';
    return `${safeLabel}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  }

  function logAiCallStart(payload) {
    console.log('[AI TRACE] start', payload);
  }

  function logAiCallEnd(payload) {
    console.log('[AI TRACE] end', payload);
  }

  async function withAiTrace(meta, requestFn) {
    const traceId = buildAiTraceId(meta.label);
    const startedAt = Date.now();
    logAiCallStart({
      traceId,
      at: new Date().toISOString(),
      ...meta,
    });
    try {
      const result = await requestFn();
      const responseModel = toTrimmedString(result?.debug?.response?.model, 120) || meta.model;
      logAiCallEnd({
        traceId,
        at: new Date().toISOString(),
        label: meta.label,
        model: responseModel,
        durationMs: Date.now() - startedAt,
        replyLength: String(result?.reply || '').length,
        usage: result?.usage || null,
      });
      return result;
    } catch (error) {
      logAiCallEnd({
        traceId,
        at: new Date().toISOString(),
        label: meta.label,
        model: meta.model || null,
        durationMs: Date.now() - startedAt,
        error: toTrimmedString(error?.message, 300) || 'unknown',
      });
      throw error;
    }
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

  function normalizeScope(rawScope) {
    const scope = toProfile(rawScope);
    const scopeType = toTrimmedString(scope.type, 24).toLowerCase();

    if (!AGENT_CHAT_SCOPE_TYPES.has(scopeType)) {
      throw Object.assign(new Error('Invalid scope type'), { status: 400 });
    }

    if (scopeType === 'collection') {
      const entityType = toTrimmedString(scope.entityType, 24).toLowerCase();
      if (!AGENT_CHAT_ENTITY_TYPES.has(entityType)) {
        throw Object.assign(new Error('Invalid collection scope type'), { status: 400 });
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
      // Must match frontend scope key format in AgentChatDock.
      scopeKey: `project-canvas:${projectId}`,
    };
  }

  function normalizeHistoryAttachment(rawAttachment, index) {
    const attachment = toProfile(rawAttachment);
    const data = toTrimmedString(attachment.data, AGENT_CHAT_HISTORY_ATTACHMENT_DATA_MAX_LENGTH);
    const name = toTrimmedString(attachment.name, 240) || 'Файл';
    if (!data && !name) return null;

    return {
      id: toTrimmedString(attachment.id, 120) || `att_${Date.now()}_${index}`,
      name,
      mime: toTrimmedString(attachment.mime, 180),
      size: Number.isFinite(Number(attachment.size)) ? Math.max(0, Math.floor(Number(attachment.size))) : 0,
      data,
    };
  }

  function normalizeHistoryMessage(rawMessage, index) {
    const message = toProfile(rawMessage);
    const id = toTrimmedString(message.id, 120) || `msg_${Date.now()}_${index}`;
    const role = toTrimmedString(message.role, 24) === 'assistant' ? 'assistant' : 'user';
    const text = toTrimmedString(message.text, AGENT_CHAT_HISTORY_TEXT_MAX_LENGTH);
    const createdAtRaw = toTrimmedString(message.createdAt, 80);
    const parsedCreatedAt = Date.parse(createdAtRaw);
    const createdAt = Number.isFinite(parsedCreatedAt) ? new Date(parsedCreatedAt) : new Date();
    const attachments = (Array.isArray(message.attachments) ? message.attachments : [])
      .slice(0, AGENT_CHAT_HISTORY_ATTACHMENT_LIMIT)
      .map((item, attachmentIndex) => normalizeHistoryAttachment(item, attachmentIndex))
      .filter(Boolean);

    if (!text && attachments.length === 0) {
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
    if (!Array.isArray(rawMessages)) return [];

    const dedup = new Set();
    const normalized = rawMessages
      .map((message, index) => normalizeHistoryMessage(message, index))
      .filter(Boolean)
      .filter((message) => {
        if (dedup.has(message.id)) return false;
        dedup.add(message.id);
        return true;
      })
      .sort((left, right) => left.createdAt.getTime() - right.createdAt.getTime());

    return normalized.slice(-AGENT_CHAT_HISTORY_MESSAGE_LIMIT);
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
          returnDocument: 'after',
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
      const routerModel = toTrimmedString(OPENAI_ROUTER_MODEL, 120) || 'gpt-5';

      const routerTraceMeta = {
        label: 'agent-chat.router',
        ownerId,
        model: routerModel,
        scope: {
          type: scopeContext.scopeType,
          entityType: scopeContext.entityType,
          projectId: scopeContext.projectId,
        },
        promptLengths: {
          system: routerSystemPrompt.length,
          user: routerPrompt.length,
        },
        messageLength: message.length,
        historyLength: history.length,
        attachmentsCount: attachments.length,
        includeDebug,
      };
      const routerResponse = await withAiTrace(routerTraceMeta, () => aiProvider.requestOpenAiAgentReply({
        systemPrompt: routerSystemPrompt,
        userPrompt: routerPrompt,
        includeRawPayload: includeDebug,
        model: routerModel,
        temperature: 0,
        maxOutputTokens: 5,
        allowEmptyResponse: true,
        emptyResponseFallback: 'default',
      }));
      const detectedRoleRaw = toTrimmedString(routerResponse.reply, 60);
      const detectedRole = aiPrompts.normalizeDetectedRole(detectedRoleRaw);
      const deepModel =
        toTrimmedString(OPENAI_DEEP_MODEL, 120) ||
        toTrimmedString(OPENAI_PROJECT_MODEL, 120) ||
        'gpt-5';

      const systemPrompt = aiPrompts.buildAgentSystemPrompt(contextData, detectedRole);
      const userPrompt = aiPrompts.buildAgentUserPrompt({
        contextData,
        message,
      });

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
        includeDebug,
        detectedRole,
      };
      const aiResponse = await withAiTrace(chatTraceMeta, () => aiProvider.requestOpenAiAgentReply({
        systemPrompt,
        userPrompt,
        includeRawPayload: includeDebug,
        model: deepModel,
        temperature: 0.25,
        maxOutputTokens: 4000,
        allowEmptyResponse: true,
        emptyResponseFallback: 'Пустой ответ от модели. Уточните запрос или повторите через несколько секунд.',
        timeoutMs: 130_000,
      }));
      const usedModel = toTrimmedString(aiResponse?.debug?.response?.model, 120) || deepModel;
      const usedRouterModel = toTrimmedString(routerResponse?.debug?.response?.model, 120) || routerModel;

      const canRunProjectAutoEnrichment = typeof runProjectChatAutoEnrichment === 'function';
      const shouldQueueProjectAutoEnrichment = scopeContext.scopeType === 'project' && canRunProjectAutoEnrichment;
      if (shouldQueueProjectAutoEnrichment) {
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
      } else if (scopeContext.scopeType === 'project') {
        console.warn('[agent-chat] project auto enrichment is unavailable');
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
              queued: shouldQueueProjectAutoEnrichment,
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
      });

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

      const includeDebug = AI_DEBUG_ECHO || req.body?.debug === true;
      await setEntityAnalysisPending(entity, true);
      broadcastEntityEvent(ownerId, 'entity.updated', {
        entity: entity.toObject(),
      });

      const entityIdValue = String(entity._id);
      void (async () => {
        try {
          const freshEntity = await Entity.findOne({ _id: entityIdValue, owner_id: ownerId });
          if (!freshEntity) return;

          const aiMetadata = toProfile(freshEntity.ai_metadata);
          const currentFields = buildEntityAnalyzerCurrentFields(freshEntity.type, aiMetadata);
          const systemPrompt = aiPrompts.buildEntityAnalyzerSystemPrompt(freshEntity.type);
          const userPrompt = aiPrompts.buildEntityAnalyzerUserPrompt({
            entity: freshEntity,
            message,
            history,
            attachments,
            currentFields,
            voiceInput,
            documents,
          });

          const analyzeTraceMeta = {
            label: 'entity-analyze.reply',
            ownerId,
            entityId: String(freshEntity._id),
            entityType: freshEntity.type,
            model: OPENAI_MODEL,
            promptLengths: {
              system: systemPrompt.length,
              user: userPrompt.length,
            },
            messageLength: message.length,
            voiceInputLength: voiceInput.length,
            historyLength: history.length,
            attachmentsCount: attachments.length,
            documentsCount: documents.length,
            includeDebug,
          };
          const aiResponse = await withAiTrace(analyzeTraceMeta, () => aiProvider.requestOpenAiAgentReply({
            systemPrompt,
            userPrompt,
            includeRawPayload: includeDebug,
            model: OPENAI_MODEL,
            temperature: 0.3,
            maxOutputTokens: 4000,
            timeoutMs: 130_000,
          }));

          const parsedResponse = extractJsonObjectFromText(aiResponse.reply);
          const analysis = ensureAnalysisMarkers(normalizeEntityAnalysisOutput(freshEntity.type, parsedResponse));
          const latestEntity = await Entity.findOne({ _id: entityIdValue, owner_id: ownerId });
          if (!latestEntity) return;
          const nextMetadata = buildEntityMetadataPatch(latestEntity.type, latestEntity.ai_metadata, analysis);
          nextMetadata.analysis_pending = false;
          nextMetadata.analysis_completed_at = new Date().toISOString();
          delete nextMetadata.analysis_error;

          latestEntity.ai_metadata = nextMetadata;
          await latestEntity.save();
          broadcastEntityEvent(ownerId, 'entity.updated', {
            entity: latestEntity.toObject(),
          });

          if (analysis.status === 'ready') {
            try {
              await upsertEntityVector(ownerId, latestEntity, analysis);
            } catch (error) {
              console.error('Entity analyze vector error:', error);
            }
          }
        } catch (error) {
          const safeMessage = toTrimmedString(error?.message, 240) || 'Entity analyze failed';
          console.error('Entity analyze background error:', error);
          const fallbackEntity = await Entity.findOne({ _id: entityIdValue, owner_id: ownerId });
          if (!fallbackEntity) return;
          await setEntityAnalysisPending(fallbackEntity, false, safeMessage);
          broadcastEntityEvent(ownerId, 'entity.updated', {
            entity: fallbackEntity.toObject(),
          });
        }
      })();

      return res.status(202).json({
        status: 'processing',
        message: 'Анализ запущен в фоне',
        reply: 'Анализ запущен. Результат придет автоматически.',
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
