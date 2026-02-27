const express = require('express');

function createAiRouter(deps) {
  const {
    requireAuth,
    requireOwnerId,
    toTrimmedString,
    toProfile,
    AI_DEBUG_ECHO,
    OPENAI_MODEL,
    Entity,
    resolveAgentScopeContext,
    buildEntityAnalyzerCurrentFields,
    extractJsonObjectFromText,
    normalizeEntityAnalysisOutput,
    buildEntityMetadataPatch,
    upsertEntityVector,
    broadcastEntityEvent,
    aiPrompts,
    aiAttachments,
    aiProvider,
  } = deps;

  const router = express.Router();

  router.post('/agent-chat', requireAuth, async (req, res, next) => {
    try {
      const ownerId = requireOwnerId(req);
      const message = toTrimmedString(req.body?.message, 2400);

      if (!message) {
        return res.status(400).json({ message: 'message is required' });
      }

      const history = aiAttachments.normalizeAgentHistory(req.body?.history);
      const attachments = await aiAttachments.prepareAgentAttachments(req.body?.attachments);
      const scopeContext = await resolveAgentScopeContext(ownerId, req.body?.scope);

      const systemPrompt = aiPrompts.buildAgentSystemPrompt(scopeContext);
      const userPrompt = aiPrompts.buildAgentUserPrompt({
        scopeContext,
        message,
        history,
        attachments,
      });

      const aiResponse = await aiProvider.requestOpenAiAgentReply({
        systemPrompt,
        userPrompt,
      });

      const includeDebug = AI_DEBUG_ECHO || req.body?.debug === true;
      const debugPayload = includeDebug
        ? {
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
            prompts: {
              systemPrompt,
              userPrompt,
            },
            response: {
              reply: aiResponse.reply,
              usage: aiResponse.usage,
              model: OPENAI_MODEL,
            },
          }
        : undefined;

      return res.status(200).json({
        reply: aiResponse.reply,
        usage: aiResponse.usage,
        model: OPENAI_MODEL,
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

      const aiResponse = await aiProvider.requestOpenAiAgentReply({
        systemPrompt,
        userPrompt,
      });

      const parsedResponse = extractJsonObjectFromText(aiResponse.reply);
      const analysis = normalizeEntityAnalysisOutput(entity.type, parsedResponse);
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

      const includeDebug = AI_DEBUG_ECHO || req.body?.debug === true;
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
              model: OPENAI_MODEL,
            },
            vector: vector || null,
            vectorWarning: vectorWarning || '',
          }
        : undefined;

      return res.status(200).json({
        reply,
        suggestion: analysis,
        usage: aiResponse.usage,
        model: OPENAI_MODEL,
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

      const analysis = normalizeEntityAnalysisOutput(entity.type, req.body?.suggestion);
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
