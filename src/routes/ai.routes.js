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
