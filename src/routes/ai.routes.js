const express = require('express');
const { registerAgentRoutes } = require('./ai/agent.routes');
const { registerEntityProtectedRoutes } = require('./ai/entity-protected.routes');
const { createScopeContextService } = require('./ai/scope-context');
const { createHistoryService } = require('./ai/history-service');
const { createBuildLlmContext } = require('./ai/build-llm-context');

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
    getEntityAnalyzerFields,
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
  const AGENT_CHAT_MAIN_REQUEST_CONFIG = Object.freeze({
    temperature: 0.25,
    maxOutputTokens: 2200,
    timeoutMs: 95_000,
    reasoningEffort: 'low',
    verbosity: 'low',
  });
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
  const ENTITY_ANALYSIS_FIELD_CONFIGS = Object.freeze({
    tags: { maxItems: 12, itemMaxLength: 64 },
    markers: { maxItems: 12, itemMaxLength: 64 },
    roles: { maxItems: 12, itemMaxLength: 64 },
    skills: { maxItems: 12, itemMaxLength: 64 },
    links: { maxItems: 12, itemMaxLength: 240 },
    phones: { maxItems: 12, itemMaxLength: 40 },
    status: { maxItems: 12, itemMaxLength: 64 },
    priority: { maxItems: 12, itemMaxLength: 64 },
    metrics: { maxItems: 12, itemMaxLength: 120 },
    owners: { maxItems: 12, itemMaxLength: 64 },
    participants: { maxItems: 12, itemMaxLength: 64 },
    outcomes: { maxItems: 12, itemMaxLength: 96 },
    resources: { maxItems: 12, itemMaxLength: 96 },
    industry: { maxItems: 12, itemMaxLength: 64 },
    departments: { maxItems: 12, itemMaxLength: 64 },
    stage: { maxItems: 12, itemMaxLength: 64 },
    date: { maxItems: 12, itemMaxLength: 64 },
    location: { maxItems: 12, itemMaxLength: 64 },
    risks: { maxItems: 12, itemMaxLength: 96 },
    importance: { maxItems: 1, itemMaxLength: 24 },
    ignoredNoise: { maxItems: 20, itemMaxLength: 120 },
  });
  // JSON Schema for OpenAI Structured Outputs (Responses API text.format).
  //
  // IMPORTANT — Responses API uses a FLAT format, NOT the nested Chat-Completions
  // format ({ json_schema: { name, strict, schema } }). Here name/strict/schema
  // live directly inside the format object alongside `type`.
  //
  // Universal schema (all entity types share it):
  //   - fields contains ALL possible field keys; the prompt instructs the LLM to
  //     fill only the allowed ones and return [] for everything else.
  //   - The server then filters to the entity-type-specific allow-list.
  //   - confidence is omitted (dynamic keys are incompatible with strict mode);
  //     the server always sets it to {} after normalization.
  const ENTITY_ANALYSIS_OUTPUT_SCHEMA = Object.freeze({
    type: 'json_schema',
    name: 'EntityAnalysis',
    strict: true,
    schema: {
      type: 'object',
      required: [
        'status',
        'description',
        'changeType',
        'changeReason',
        'suggestedName',
        'importanceSignal',
        'importanceReason',
        'clarifyingQuestions',
        'ignoredNoise',
        'fields',
      ],
      additionalProperties: false,
      properties: {
        status: { type: 'string', enum: ['ready', 'need_clarification'] },
        description: { type: 'string' },
        changeType: { type: 'string', enum: ['initial', 'addition', 'update'] },
        changeReason: { type: 'string' },
        suggestedName: { anyOf: [{ type: 'string' }, { type: 'null' }] },
        importanceSignal: { type: 'string', enum: ['increase', 'decrease', 'neutral'] },
        importanceReason: { type: 'string' },
        clarifyingQuestions: { type: 'array', items: { type: 'string' } },
        ignoredNoise: { type: 'array', items: { type: 'string' } },
        fields: {
          type: 'object',
          required: [
            'tags',
            'markers',
            'roles',
            'skills',
            'links',
            'phones',
            'status',
            'priority',
            'metrics',
            'owners',
            'participants',
            'outcomes',
            'resources',
            'industry',
            'departments',
            'stage',
            'date',
            'location',
            'risks',
            'importance',
            'tasks',
            'ignoredNoise',
          ],
          additionalProperties: false,
          properties: {
            tags: { type: 'array', items: { type: 'string' } },
            markers: { type: 'array', items: { type: 'string' } },
            roles: { type: 'array', items: { type: 'string' } },
            skills: { type: 'array', items: { type: 'string' } },
            links: { type: 'array', items: { type: 'string' } },
            phones: { type: 'array', items: { type: 'string' } },
            status: { type: 'array', items: { type: 'string' } },
            priority: { type: 'array', items: { type: 'string' } },
            metrics: { type: 'array', items: { type: 'string' } },
            owners: { type: 'array', items: { type: 'string' } },
            participants: { type: 'array', items: { type: 'string' } },
            outcomes: { type: 'array', items: { type: 'string' } },
            resources: { type: 'array', items: { type: 'string' } },
            industry: { type: 'array', items: { type: 'string' } },
            departments: { type: 'array', items: { type: 'string' } },
            stage: { type: 'array', items: { type: 'string' } },
            date: { type: 'array', items: { type: 'string' } },
            location: { type: 'array', items: { type: 'string' } },
            risks: { type: 'array', items: { type: 'string' } },
            importance: { type: 'array', items: { type: 'string' } },
            tasks: { type: 'array', items: { type: 'string' } },
            ignoredNoise: { type: 'array', items: { type: 'string' } },
          },
        },
      },
    },
  });

  const IMPORTANCE_VALUE_MAP = Object.freeze({
    низкая: 'Низкая',
    low: 'Низкая',
    l: 'Низкая',
    низкий: 'Низкая',
    средняя: 'Средняя',
    medium: 'Средняя',
    med: 'Средняя',
    m: 'Средняя',
    средний: 'Средняя',
    высокая: 'Высокая',
    high: 'Высокая',
    h: 'Высокая',
    высокий: 'Высокая',
    критично: 'Высокая',
    critical: 'Высокая',
  });
  const ENTITY_NAME_MODE_VALUES = new Set(['system', 'manual', 'llm']);
  const AUTO_NAME_TYPES = new Set(
    (Array.isArray(entityTypes) ? entityTypes : [])
      .filter((type) => !['person', 'company'].includes(type)),
  );
  const AUTO_NAME_MAX_LENGTH = 64;
  const SYSTEM_DEFAULT_NAME_LABELS = Object.freeze({
    project: ['Проект', 'Новый проект'],
    connection: ['Подключение', 'Новое подключение'],
    person: ['Персона', 'Новая персона'],
    company: ['Компания', 'Новая компания'],
    event: ['Событие', 'Новое событие'],
    resource: ['Ресурс', 'Новый ресурс'],
    goal: ['Цель', 'Новая цель'],
    result: ['Результат', 'Новый результат'],
    task: ['Задача', 'Новая задача'],
    shape: ['Элемент', 'Новый элемент'],
  });

  function escapeRegExp(value) {
    return String(value).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  }

  function normalizeEntityNameMode(value) {
    const mode = toTrimmedString(value, 16).toLowerCase();
    return ENTITY_NAME_MODE_VALUES.has(mode) ? mode : '';
  }

  function isSystemDefaultEntityName(entityType, rawName) {
    const name = toTrimmedString(rawName, 120);
    if (!name) return false;

    const labels = SYSTEM_DEFAULT_NAME_LABELS[entityType];
    if (!Array.isArray(labels) || !labels.length) return false;

    return labels.some((label) => {
      const prefix = escapeRegExp(toTrimmedString(label, 80));
      if (!prefix) return false;
      // Accept both exact defaults and indexed defaults:
      // "Результат", "Результат - 2", "Новый результат 2".
      const pattern = new RegExp(`^${prefix}(?:\\s*(?:-\\s*)?\\d+)?$`, 'i');
      return pattern.test(name);
    });
  }

  function resolveCurrentEntityNameMode(entityType, entityName, aiMetadata) {
    const metadata = toProfile(aiMetadata);
    const explicitMode = normalizeEntityNameMode(metadata.name_mode || metadata.nameMode);
    if (explicitMode) return explicitMode;
    if (metadata.name_auto === true) return 'llm';
    return isSystemDefaultEntityName(entityType, entityName) ? 'system' : 'manual';
  }

  function normalizeEntityFieldList(rawValues, config = {}) {
    const {
      maxItems = 12,
      itemMaxLength = 64,
    } = config;
    const source = Array.isArray(rawValues)
      ? rawValues
      : typeof rawValues === 'string'
        ? [rawValues]
        : [];
    const dedup = new Set();
    const result = [];

    for (const item of source) {
      const value = toTrimmedString(item, itemMaxLength);
      if (!value) continue;
      const key = value.toLowerCase();
      if (dedup.has(key)) continue;
      dedup.add(key);
      result.push(value);
      if (result.length >= maxItems) break;
    }

    return result;
  }

  function normalizeImportanceValue(value) {
    const normalized = toTrimmedString(value, 24).toLowerCase();
    if (!normalized) return '';
    return IMPORTANCE_VALUE_MAP[normalized] || '';
  }

  function normalizeImportanceList(rawValues) {
    const list = normalizeEntityFieldList(rawValues, ENTITY_ANALYSIS_FIELD_CONFIGS.importance);
    const normalized = list
      .map((item) => normalizeImportanceValue(item))
      .find(Boolean);
    return normalized ? [normalized] : [];
  }

  // filterToAllowedFields — keeps only entity-type-allowed fields from the LLM output.
  // The LLM (via Structured Outputs) decides which field each value belongs to;
  // the server only enforces the per-type field permission whitelist and
  // applies length/count limits. No semantic remapping between fields.
  function filterToAllowedFields(entityType, rawFields) {
    const allowedFieldsSource =
      typeof getEntityAnalyzerFields === 'function' ? getEntityAnalyzerFields(entityType) : [];
    const allowedFields = new Set(Array.isArray(allowedFieldsSource) ? allowedFieldsSource : []);
    const sourceFields = toProfile(rawFields);
    const fields = {};

    for (const field of allowedFields) {
      if (field === 'importance') {
        fields.importance = normalizeImportanceList(sourceFields.importance);
        continue;
      }
      const config = ENTITY_ANALYSIS_FIELD_CONFIGS[field] || ENTITY_ANALYSIS_FIELD_CONFIGS.tags;
      fields[field] = normalizeEntityFieldList(sourceFields[field], config);
    }

    return fields;
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
  function normalizeProjectEnrichmentFieldValue(fieldKey, rawValue, itemMaxLength) {
    const str = typeof rawValue === 'string' ? rawValue.trim() : '';
    if (!str) return '';

    if (fieldKey === 'importance') {
      const lower = str.toLowerCase();
      if (lower === 'низкая' || lower === 'low' || lower === 'l') return 'Низкая';
      if (lower === 'средняя' || lower === 'medium' || lower === 'med' || lower === 'm') return 'Средняя';
      if (lower === 'высокая' || lower === 'high' || lower === 'h' || lower === 'critical' || lower === 'критично') return 'Высокая';
      return '';
    }

    if (fieldKey === 'links') {
      const withProtocol = /^https?:\/\//i.test(str) ? str : `https://${str}`;
      try {
        const url = new URL(withProtocol);
        if (!url.hostname || !url.protocol.startsWith('http')) return '';
        return url.toString().slice(0, itemMaxLength);
      } catch {
        return '';
      }
    }

    return str.slice(0, itemMaxLength);
  }

  async function runProjectChatAutoEnrichment({
    ownerId,
    scopeContext,
    sourceEntities,
    contextData,
    message,
    history,
    assistantReply,
    includeDebug,
  }) {
    const projectId = toTrimmedString(scopeContext?.projectId, 80);
    if (!projectId) return;

    const project = await Entity.findOne({ _id: projectId, owner_id: ownerId });
    if (!project || project.type !== 'project') return;

    const projectMeta = toProfile(project.ai_metadata);
    const currentProjectFields = {};
    for (const fieldKey of PROJECT_CHAT_FIELD_KEYS) {
      currentProjectFields[fieldKey] = Array.isArray(projectMeta[fieldKey]) ? projectMeta[fieldKey] : [];
    }

    const aggregatedEntityFields = {};
    const scopeEntities = Array.isArray(sourceEntities) ? sourceEntities : [];
    for (const fieldKey of PROJECT_CHAT_FIELD_KEYS) {
      const config = PROJECT_CHAT_FIELD_CONFIGS[fieldKey];
      const dedup = new Set();
      const values = [];
      for (const entity of scopeEntities) {
        const meta = toProfile(entity.ai_metadata);
        const fieldValues = Array.isArray(meta[fieldKey]) ? meta[fieldKey] : [];
        for (const val of fieldValues) {
          const normalized = normalizeProjectEnrichmentFieldValue(fieldKey, val, config.itemMaxLength);
          if (!normalized) continue;
          const key = normalized.toLowerCase();
          if (dedup.has(key)) continue;
          dedup.add(key);
          values.push(normalized);
        }
      }
      aggregatedEntityFields[fieldKey] = values;
    }

    const systemPrompt = aiPrompts.buildProjectEnrichmentSystemPrompt();
    const userPrompt = aiPrompts.buildProjectEnrichmentUserPrompt({
      contextData,
      message,
      assistantReply,
      history,
      currentProjectFields,
      aggregatedEntityFields,
    });

    const enrichmentModel = toTrimmedString(OPENAI_PROJECT_MODEL, 120) || 'gpt-5';
    const aiResponse = await withAiTrace({
      label: 'project-chat.enrichment',
      ownerId,
      model: enrichmentModel,
      projectId,
      promptLengths: { system: systemPrompt.length, user: userPrompt.length },
      includeDebug,
    }, () => aiProvider.requestOpenAiAgentReply({
      systemPrompt,
      userPrompt,
      includeRawPayload: includeDebug,
      model: enrichmentModel,
      temperature: 0.1,
      maxOutputTokens: 2000,
      allowEmptyResponse: true,
      emptyResponseFallback: '',
      timeoutMs: 60_000,
    }));

    if (!aiResponse.reply) return;

    const parsed = extractJsonObjectFromText(aiResponse.reply);
    if (!parsed || parsed.status !== 'ready') return;

    const enrichedFields = toProfile(parsed.fields);
    const freshProject = await Entity.findOne({ _id: projectId, owner_id: ownerId });
    if (!freshProject) return;

    const freshMeta = toProfile(freshProject.ai_metadata);
    const patch = {};
    let hasChanges = false;

    for (const fieldKey of PROJECT_CHAT_FIELD_KEYS) {
      const config = PROJECT_CHAT_FIELD_CONFIGS[fieldKey];
      const newValues = Array.isArray(enrichedFields[fieldKey]) ? enrichedFields[fieldKey] : [];
      if (!newValues.length) continue;

      const existingValues = Array.isArray(freshMeta[fieldKey]) ? freshMeta[fieldKey] : [];
      const existingDedup = new Set(
        existingValues.map((v) => (typeof v === 'string' ? v.toLowerCase() : '')).filter(Boolean),
      );
      const merged = [...existingValues];

      for (const rawVal of newValues) {
        const normalized = normalizeProjectEnrichmentFieldValue(fieldKey, rawVal, config.itemMaxLength);
        if (!normalized) continue;
        const key = normalized.toLowerCase();
        if (existingDedup.has(key)) continue;
        existingDedup.add(key);
        merged.push(normalized);
        if (merged.length >= config.maxItems) break;
      }

      if (merged.length > existingValues.length) {
        patch[fieldKey] = merged;
        hasChanges = true;
      }
    }

    if (!hasChanges) return;

    freshProject.ai_metadata = { ...freshMeta, ...patch };
    await freshProject.save();
    broadcastEntityEvent(ownerId, 'entity.updated', {
      entity: freshProject.toObject(),
    });
  }

  const scopeContextService = createScopeContextService({
    toTrimmedString,
    toProfile,
    entityTypes,
  });
  const historyService = createHistoryService({
    toTrimmedString,
    toProfile,
    AGENT_CHAT_HISTORY_MESSAGE_LIMIT,
    AGENT_CHAT_HISTORY_ATTACHMENT_LIMIT,
    AGENT_CHAT_HISTORY_ATTACHMENT_DATA_MAX_LENGTH,
    AGENT_CHAT_HISTORY_TEXT_MAX_LENGTH,
  });
  const llmContextTools = createBuildLlmContext({
    toTrimmedString,
    toProfile,
    aiPrompts,
  });

  registerAgentRoutes({
    router,
    deps: {
      requireAuth,
      requireOwnerId,
      toTrimmedString,
      toProfile,
      AI_DEBUG_ECHO,
      OPENAI_PROJECT_MODEL,
      OPENAI_DEEP_MODEL,
      resolveAgentScopeContext,
      broadcastEntityEvent,
      AgentChatHistory,
      aiPrompts,
      aiAttachments,
      aiProvider,
    },
    helpers: {
      AGENT_CHAT_MAIN_REQUEST_CONFIG,
      scopeContextService,
      historyService,
      llmContextTools,
      withAiTrace,
      runProjectChatAutoEnrichment,
    },
  });

  // PROTECTED: Entity chat/analyze branch.
  registerEntityProtectedRoutes({
    router,
    deps: {
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
    },
    helpers: {
      setEntityAnalysisPending,
      withAiTrace,
      filterToAllowedFields,
      resolveCurrentEntityNameMode,
      isSystemDefaultEntityName,
      AUTO_NAME_TYPES,
      AUTO_NAME_MAX_LENGTH,
      ENTITY_ANALYSIS_OUTPUT_SCHEMA,
      postValidateEntityAnalysis: (...args) => postValidateEntityAnalysis(...args),
    },
  });

  return router;
}

module.exports = {
  createAiRouter,
};
