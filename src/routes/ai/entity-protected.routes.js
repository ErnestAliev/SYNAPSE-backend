// PROTECTED: Entity chat/analyze branch. Keep behavior unchanged.
function registerEntityProtectedRoutes({ router, deps, helpers }) {
  const {
    requireAuth,
    requireOwnerId,
    toTrimmedString,
    toProfile,
    AI_DEBUG_ECHO,
    OPENAI_MODEL,
    Entity,
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

  const {
    setEntityAnalysisPending,
    withAiTrace,
    filterToAllowedFields,
    resolveCurrentEntityNameMode,
    isSystemDefaultEntityName,
    AUTO_NAME_TYPES,
    AUTO_NAME_MAX_LENGTH,
    ENTITY_ANALYSIS_OUTPUT_SCHEMA,
    postValidateEntityAnalysis,
  } = helpers;

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
      const requestedAt = new Date().toISOString();
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
            timeoutMs: 180_000,
            jsonSchema: ENTITY_ANALYSIS_OUTPUT_SCHEMA,
          }));

          // Parse the response.
          // extractJsonObjectFromText is used as the primary parser — it handles:
          //   • Strict JSON (what Structured Outputs guarantees when schema is accepted)
          //   • Markdown-fenced JSON (model fallback when schema format is not honored)
          //   • Raw JSON substring (defensive extraction)
          // If the model returned a refusal or free-form non-JSON text this will
          // throw, which is caught by the background job's outer try-catch.
          const parsedResponse = extractJsonObjectFromText(aiResponse.reply);

          // normalizeEntityAnalysisOutput handles length limits and type coercion.
          // filterToAllowedFields enforces the entity-type field whitelist.
          // No semantic remapping — the LLM owns field placement.
          const normalized = normalizeEntityAnalysisOutput(freshEntity.type, parsedResponse);
          const analysis = {
            ...normalized,
            fields: filterToAllowedFields(freshEntity.type, parsedResponse.fields),
            confidence: {},
          };
          const latestEntity = await Entity.findOne({ _id: entityIdValue, owner_id: ownerId });
          if (!latestEntity) return;
          const nextMetadata = buildEntityMetadataPatch(latestEntity.type, latestEntity.ai_metadata, analysis);
          nextMetadata.analysis_pending = false;
          nextMetadata.analysis_completed_at = new Date().toISOString();
          delete nextMetadata.analysis_error;

          // Auto-assign the LLM-generated name when the current name is still a
          // system/default template (even if legacy metadata has manual mode).
          // Never overwrite names that were already assigned by LLM.
          let renameStatusNote = '';
          const autoSuggestedName = toTrimmedString(analysis.suggestedName, AUTO_NAME_MAX_LENGTH);
          if (AUTO_NAME_TYPES.has(latestEntity.type) && analysis.status === 'ready' && autoSuggestedName) {
            const currentName = toTrimmedString(latestEntity.name, 120);
            const currentNameMode = resolveCurrentEntityNameMode(
              latestEntity.type,
              currentName,
              latestEntity.ai_metadata,
            );
            nextMetadata.name_mode = currentNameMode;
            const hasDefaultSystemName = isSystemDefaultEntityName(latestEntity.type, currentName);
            const canAutoRename =
              currentNameMode === 'system' ||
              (currentNameMode === 'manual' && hasDefaultSystemName);
            if (canAutoRename) {
              latestEntity.name = autoSuggestedName.slice(0, AUTO_NAME_MAX_LENGTH);
              nextMetadata.name_mode = 'llm';
              nextMetadata.name_auto = true;
              renameStatusNote = `Название обновлено: ${latestEntity.name}.`;
            } else if (toProfile(latestEntity.ai_metadata).name_auto) {
              nextMetadata.name_auto = false;
              renameStatusNote = `Предложенное название: ${autoSuggestedName}. Не применено: имя уже зафиксировано пользователем или LLM.`;
            } else {
              renameStatusNote = `Предложенное название: ${autoSuggestedName}. Не применено: имя уже зафиксировано пользователем или LLM.`;
            }
          } else {
            // Clear the auto-name flag if this analysis didn't produce a name
            // (e.g. need_clarification or type doesn't support suggestedName).
            if (toProfile(latestEntity.ai_metadata).name_auto) {
              nextMetadata.name_auto = false;
            }
          }

          const analysisReplyTextBase = aiPrompts.buildEntityAnalysisReplyText(analysis);
          const analysisReplyText = renameStatusNote
            ? `${analysisReplyTextBase}\n\n${renameStatusNote}`
            : analysisReplyTextBase;
          if (analysisReplyText) {
            const existingChatHistory = Array.isArray(nextMetadata.chat_history) ? nextMetadata.chat_history : [];
            // Build user entry: use message text, or fall back to attachment file names.
            // If the same user message is already the latest chat_history item
            // (saved by frontend autosave before background job completion), skip
            // appending it again to avoid user-message duplication in modal chat.
            const userMessageText = message
              || attachments.map((a) => toTrimmedString(a.name, 120) || 'Файл').join(', ');
            const lastHistoryItem =
              existingChatHistory.length > 0 ? toProfile(existingChatHistory[existingChatHistory.length - 1]) : null;
            const lastHistoryRole = lastHistoryItem?.role === 'assistant' ? 'assistant' : 'user';
            const lastHistoryText = toTrimmedString(lastHistoryItem?.text, 4000);
            const shouldAppendUserMessage =
              Boolean(userMessageText)
              && !(lastHistoryRole === 'user' && lastHistoryText && lastHistoryText === userMessageText);
            const userChatMessage = shouldAppendUserMessage
              ? {
                id: `msg_${Date.now()}_u_${Math.random().toString(36).slice(2, 6)}`,
                role: 'user',
                text: userMessageText,
                createdAt: requestedAt,
                attachments: [],
              }
              : null;
            const assistantChatMessage = {
              id: `msg_${Date.now()}_a_${Math.random().toString(36).slice(2, 6)}`,
              role: 'assistant',
              text: analysisReplyText,
              createdAt: new Date().toISOString(),
              attachments: [],
            };
            const newMessages = [userChatMessage, assistantChatMessage].filter(Boolean);
            nextMetadata.chat_history = [...existingChatHistory, ...newMessages].slice(-40);
          }

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
          const fallbackMeta = toProfile(fallbackEntity.ai_metadata);
          const existingChatHistory = Array.isArray(fallbackMeta.chat_history) ? fallbackMeta.chat_history : [];
          const assistantErrorMessage = {
            id: `msg_${Date.now()}_a_${Math.random().toString(36).slice(2, 6)}`,
            role: 'assistant',
            text: `Не удалось завершить анализ. ${safeMessage}`,
            createdAt: new Date().toISOString(),
            attachments: [],
          };
          fallbackEntity.ai_metadata = {
            ...fallbackMeta,
            chat_history: [...existingChatHistory, assistantErrorMessage].slice(-40),
          };
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

      const analysis = postValidateEntityAnalysis(
        entity.type,
        normalizeEntityAnalysisOutput(entity.type, req.body?.suggestion),
      );
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
}

module.exports = {
  registerEntityProtectedRoutes,
};
