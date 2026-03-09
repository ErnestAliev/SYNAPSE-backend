const { createProjectChatFlow } = require('./project-chat.flow');
const { createCollectionChatFlow } = require('./collection-chat.flow');

function registerAgentRoutes({ router, deps, helpers }) {
  const {
    requireAuth,
    requireOwnerId,
    toTrimmedString,
    AI_DEBUG_ECHO,
    OPENAI_PROJECT_MODEL,
    OPENAI_DEEP_MODEL,
    resolveAgentScopeContext,
    broadcastEntityEvent,
    AgentChatHistory,
    aiAttachments,
    aiPrompts,
    aiProvider,
  } = deps;

  const {
    AGENT_CHAT_MAIN_REQUEST_CONFIG,
    scopeContextService,
    historyService,
    llmContextTools,
    withAiTrace,
    runProjectChatAutoEnrichment,
  } = helpers;

  const {
    normalizeScope,
    buildScopeKeyCandidates,
  } = scopeContextService;

  const {
    normalizeMessages,
    mapHistoryDocMessages,
    mapHistoryMessagesToResponse,
    loadStoredAgentHistory,
  } = historyService;

  const flowDeps = {
    toTrimmedString,
    toProfile: deps.toProfile,
    OPENAI_PROJECT_MODEL,
    OPENAI_DEEP_MODEL,
    aiPrompts,
    aiProvider,
  };

  const flowHelpersBase = {
    AGENT_CHAT_MAIN_REQUEST_CONFIG,
    dedupeHistoryTailByCurrentMessage: historyService.dedupeHistoryTailByCurrentMessage,
    resolveCompatibleDetectedRole: llmContextTools.resolveCompatibleDetectedRole,
    buildRequestBodySize: llmContextTools.buildRequestBodySize,
    summarizePreviewEntities: llmContextTools.summarizePreviewEntities,
    withAiTrace,
    buildAgentLlmContext: llmContextTools.buildAgentLlmContext,
  };

  const projectChatFlow = createProjectChatFlow({
    deps: flowDeps,
    helpers: {
      ...flowHelpersBase,
      runProjectChatAutoEnrichment,
    },
  });

  const collectionChatFlow = createCollectionChatFlow({
    deps: flowDeps,
    helpers: flowHelpersBase,
  });

  router.post('/agent-chat-preview', requireAuth, async (req, res, next) => {
    try {
      const ownerId = requireOwnerId(req);
      const scope = normalizeScope(req.body?.scope);
      const includeStoredHistory = req.body?.includeStoredHistory !== false;
      const requestedHistory = aiAttachments.normalizeAgentHistory(req.body?.history);
      const attachments = await aiAttachments.prepareAgentAttachments(req.body?.attachments);

      let history = requestedHistory;
      if (!history.length && includeStoredHistory) {
        const scopeKeys = buildScopeKeyCandidates(scope);
        history = await loadStoredAgentHistory({
          AgentChatHistory,
          ownerId,
          scopeKeys,
        });
      }

      const scopeContext = await resolveAgentScopeContext(ownerId, {
        type: scope.type,
        entityType: scope.entityType,
        projectId: scope.projectId,
      });

      const selectedFlow = scope.type === 'project' ? projectChatFlow : collectionChatFlow;
      const result = await selectedFlow.buildPreview({
        scope,
        scopeContext,
        history,
        attachments,
        requestedMessage: req.body?.message,
        roleHint: req.body?.detectedRole,
      });

      return res.status(200).json(result);
    } catch (error) {
      return next(error);
    }
  });

  router.get('/chat-history', requireAuth, async (req, res, next) => {
    try {
      const ownerId = requireOwnerId(req);
      const scope = normalizeScope({
        type: req.query.scopeType,
        entityType: req.query.entityType,
        projectId: req.query.projectId,
      });
      const scopeKeys = buildScopeKeyCandidates(scope);

      const docs = await AgentChatHistory.find({
        owner_id: ownerId,
        scope_key: { $in: scopeKeys },
      })
        .select({ messages: 1, updatedAt: 1, scope_key: 1 })
        .sort({ updatedAt: -1, _id: -1 })
        .lean();

      const mergedMessages = normalizeMessages(
        docs.flatMap((doc) => mapHistoryDocMessages(doc)),
      );
      const newestUpdatedAt = docs[0]?.updatedAt || null;

      // Best-effort migration: when legacy key exists, rewrite into canonical key.
      if (scope.type === 'project' && docs.length) {
        const hasLegacyKeys = docs.some((doc) => toTrimmedString(doc.scope_key, 120) !== scope.scopeKey);
        if (hasLegacyKeys) {
          try {
            await AgentChatHistory.findOneAndUpdate(
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
                  messages: mergedMessages,
                },
              },
              {
                upsert: true,
                returnDocument: 'after',
                setDefaultsOnInsert: true,
                runValidators: true,
              },
            );

            const legacyKeys = scopeKeys.filter((key) => key !== scope.scopeKey);
            if (legacyKeys.length) {
              await AgentChatHistory.deleteMany({
                owner_id: ownerId,
                scope_key: { $in: legacyKeys },
              });
            }
          } catch (migrationError) {
            console.error('[agent-chat] failed to migrate legacy project history key', migrationError);
          }
        }
      }

      return res.status(200).json({
        scopeKey: scope.scopeKey,
        scope: {
          type: scope.type,
          entityType: scope.entityType,
          projectId: scope.projectId,
        },
        updatedAt: newestUpdatedAt,
        messages: mapHistoryMessagesToResponse(mergedMessages),
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
      const scopeKeys = buildScopeKeyCandidates(scope);

      if (!normalizedMessages.length) {
        await AgentChatHistory.deleteMany({
          owner_id: ownerId,
          scope_key: { $in: scopeKeys },
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

      if (scope.type === 'project') {
        const legacyKeys = scopeKeys.filter((key) => key !== scope.scopeKey);
        if (legacyKeys.length) {
          await AgentChatHistory.deleteMany({
            owner_id: ownerId,
            scope_key: { $in: legacyKeys },
          });
        }
      }

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
      const scopeKeys = buildScopeKeyCandidates(scope);

      const result = await AgentChatHistory.deleteMany({
        owner_id: ownerId,
        scope_key: { $in: scopeKeys },
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
      const monitorMode = req.body?.monitorMode === true;
      const includeDebug = AI_DEBUG_ECHO || req.body?.debug === true || monitorMode;

      if (!message) {
        return res.status(400).json({ message: 'message is required' });
      }

      const rawHistory = aiAttachments.normalizeAgentHistory(req.body?.history);
      const attachments = await aiAttachments.prepareAgentAttachments(req.body?.attachments);
      const scopeContext = await resolveAgentScopeContext(ownerId, req.body?.scope);
      const selectedFlow = scopeContext.scopeType === 'project' ? projectChatFlow : collectionChatFlow;

      const result = await selectedFlow.buildReply({
        ownerId,
        scopeContext,
        message,
        rawHistory,
        attachments,
        includeDebug,
        roleHint: req.body?.detectedRole,
        monitorMode,
      });

      return res.status(200).json(result);
    } catch (error) {
      return next(error);
    }
  });
}

module.exports = {
  registerAgentRoutes,
};
