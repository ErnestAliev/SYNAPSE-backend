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
  const AGENT_CHAT_SCOPE_TYPES = new Set(['collection', 'project']);
  const AGENT_CHAT_ENTITY_TYPES = new Set(Array.isArray(entityTypes) ? entityTypes : []);
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
  const AUTO_NAME_TYPES = new Set(['goal', 'event', 'result', 'task']);
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
      // Accept both canvas and list defaults:
      // "Результат - 2" and "Новый результат 2".
      const pattern = new RegExp(`^${prefix}\\s*(?:-\\s*)?\\d+$`, 'i');
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

  function buildScopeKeyCandidates(scope) {
    if (!scope || typeof scope !== 'object') return [];
    if (scope.type === 'project') {
      const projectId = toTrimmedString(scope.projectId, 80);
      if (!projectId) return [];
      return Array.from(new Set([
        scope.scopeKey,
        `project:${projectId}`,
      ]));
    }
    return scope.scopeKey ? [scope.scopeKey] : [];
  }

  function mapHistoryDocMessages(doc) {
    if (!doc || typeof doc !== 'object') return [];
    return Array.isArray(doc.messages) ? doc.messages : [];
  }

  function mapNormalizedMessagesToAgentHistory(messages) {
    return (Array.isArray(messages) ? messages : [])
      .map((message) => ({
        role: message?.role === 'assistant' ? 'assistant' : 'user',
        text: toTrimmedString(message?.text, 1800),
      }))
      .filter((message) => message.text);
  }

  function dedupeHistoryTailByCurrentMessage(history, currentMessage) {
    const normalizedMessage = toTrimmedString(currentMessage, 1800);
    const safeHistory = Array.isArray(history) ? history : [];
    if (!normalizedMessage || !safeHistory.length) {
      return {
        history: safeHistory,
        droppedCount: 0,
      };
    }

    const nextHistory = safeHistory.slice();
    let droppedCount = 0;
    while (nextHistory.length > 0) {
      const last = nextHistory[nextHistory.length - 1];
      if (last?.role !== 'user') break;
      if (toTrimmedString(last?.text, 1800) !== normalizedMessage) break;
      nextHistory.pop();
      droppedCount += 1;
    }

    return {
      history: nextHistory,
      droppedCount,
    };
  }

  function summarizePreviewEntities(entities) {
    return (Array.isArray(entities) ? entities : []).map((item) => {
      const entity = toProfile(item);
      const metadata = toProfile(entity.ai_metadata);
      const description = toTrimmedString(metadata.description || entity.description, 2400);
      const fieldCounts = {};
      let fieldsItemsTotal = 0;

      for (const [key, rawValue] of Object.entries(metadata)) {
        if (!Array.isArray(rawValue)) continue;
        const count = rawValue
          .map((value) => toTrimmedString(value, 240))
          .filter(Boolean)
          .length;
        if (!count) continue;
        fieldCounts[key] = count;
        fieldsItemsTotal += count;
      }

      return {
        id: toTrimmedString(entity.id || entity._id, 120),
        type: toTrimmedString(entity.type, 40),
        name: toTrimmedString(entity.name, 160) || '(без названия)',
        description,
        descriptionLength: description.length,
        fieldsItemsTotal,
        fieldCounts,
        updatedAt: toTrimmedString(entity.updatedAt, 80),
      };
    });
  }

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
        const docs = await AgentChatHistory.find({
          owner_id: ownerId,
          scope_key: { $in: scopeKeys },
        })
          .select({ messages: 1, updatedAt: 1 })
          .sort({ updatedAt: -1, _id: -1 })
          .lean();
        const mergedMessages = normalizeMessages(docs.flatMap((doc) => mapHistoryDocMessages(doc)));
        history = mapNormalizedMessagesToAgentHistory(mergedMessages);
      }

      const requestedMessage = toTrimmedString(req.body?.message, 2400);
      const latestUserMessage = [...history]
        .reverse()
        .find((message) => message.role === 'user' && message.text)?.text || '';
      const message = requestedMessage || latestUserMessage || '';
      const hasQuestion = Boolean(message);
      const historyBeforeDedup = history.length;
      const historyDedup = dedupeHistoryTailByCurrentMessage(history, message);
      history = historyDedup.history;

      const scopeContext = await resolveAgentScopeContext(ownerId, {
        type: scope.type,
        entityType: scope.entityType,
        projectId: scope.projectId,
      });

      const llmContextResult = aiPrompts.buildAgentLlmContextData({
        scopeContext,
        history,
        attachments,
      });
      const contextData = llmContextResult.contextData;
      const llmSerializationTrace = llmContextResult.trace;
      const selectedRole = aiPrompts.normalizeDetectedRole(toTrimmedString(req.body?.detectedRole, 24) || 'default');
      const systemPrompt = hasQuestion ? aiPrompts.buildAgentSystemPrompt(contextData, selectedRole) : '';
      const userPrompt = hasQuestion
        ? aiPrompts.buildAgentUserPrompt({
          contextData,
          message,
        })
        : '';

      const deepModel =
        toTrimmedString(OPENAI_DEEP_MODEL, 120) ||
        toTrimmedString(OPENAI_PROJECT_MODEL, 120) ||
        'gpt-5';
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
          history,
          attachments,
          historyDedup: {
            before: historyBeforeDedup,
            after: history.length,
            droppedTailDuplicates: historyDedup.droppedCount,
          },
        },
        semanticRouter: {
          mode: 'disabled',
          reason: 'single-request-mode',
          requestBody: null,
        },
        prompts: {
          detectedRole: selectedRole,
          model: deepModel,
          systemPrompt,
          userPrompt,
          requestBody: hasQuestion
            ? toProfile(mainReplyRequestPreview?.requestBody)
            : null,
        },
        contextData,
        llmSerialization: llmSerializationTrace,
        entitiesSummary: summarizePreviewEntities(entities),
      };

      const previewJson = JSON.stringify(preview);

      return res.status(200).json({
        stats: {
          totalEntitiesInProject: scopeContext.totalEntities,
          entitiesInContext: entities.length,
          connectionsInContext: connections.length,
          sourceNodesInScope: sourceNodes.length,
          sourceEdgesInScope: sourceEdges.length,
          historyMessages: history.length,
          historyTextChars: history.reduce((sum, item) => sum + String(item.text || '').length, 0),
          attachmentsCount: attachments.length,
          contextChars: contextJson.length,
          contextBytes: Buffer.byteLength(contextJson, 'utf8'),
          llmPayloadChars: Number(llmSerializationTrace?.payloadSize?.chars) || 0,
          llmPayloadBytes: Number(llmSerializationTrace?.payloadSize?.bytes) || 0,
          routerPromptChars: 0,
          routerPromptBytes: 0,
          llmPromptChars: hasQuestion ? llmPromptText.length : 0,
          llmPromptBytes: hasQuestion ? Buffer.byteLength(llmPromptText, 'utf8') : 0,
          previewJsonBytes: Buffer.byteLength(previewJson, 'utf8'),
        },
        preview,
      });
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
      const includeDebug = AI_DEBUG_ECHO || req.body?.debug === true;

      if (!message) {
        return res.status(400).json({ message: 'message is required' });
      }

      const rawHistory = aiAttachments.normalizeAgentHistory(req.body?.history);
      const historyDedup = dedupeHistoryTailByCurrentMessage(rawHistory, message);
      const history = historyDedup.history;
      const attachments = await aiAttachments.prepareAgentAttachments(req.body?.attachments);
      const scopeContext = await resolveAgentScopeContext(ownerId, req.body?.scope);
      const llmContextResult = aiPrompts.buildAgentLlmContextData({
        scopeContext,
        history,
        attachments,
      });
      const contextData = llmContextResult.contextData;
      const llmSerializationTrace = llmContextResult.trace;
      const detectedRole = aiPrompts.normalizeDetectedRole(toTrimmedString(req.body?.detectedRole, 24) || 'default');
      const deepModel =
        toTrimmedString(OPENAI_DEEP_MODEL, 120) ||
        toTrimmedString(OPENAI_PROJECT_MODEL, 120) ||
        'gpt-5';

      const systemPrompt = aiPrompts.buildAgentSystemPrompt(contextData, detectedRole);
      const userPrompt = aiPrompts.buildAgentUserPrompt({
        contextData,
        message,
      });
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
          historyDedup: {
            before: rawHistory.length,
            after: history.length,
            droppedTailDuplicates: historyDedup.droppedCount,
          },
        },
          llmSerialization: llmSerializationTrace,
          semanticRouter: {
            mode: 'disabled',
            reason: 'single-request-mode',
            detectedRole,
          },
          prompts: {
            systemPrompt,
            userPrompt,
            requestBody: toProfile(mainReplyRequestPreview?.requestBody),
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

          // Auto-assign the LLM-generated name only when name_mode is "system".
          // If name_mode is "manual" or "llm", never overwrite.
          const autoSuggestedName = toTrimmedString(analysis.suggestedName, AUTO_NAME_MAX_LENGTH);
          if (AUTO_NAME_TYPES.has(latestEntity.type) && analysis.status === 'ready' && autoSuggestedName) {
            const currentName = toTrimmedString(latestEntity.name, 120);
            const currentNameMode = resolveCurrentEntityNameMode(
              latestEntity.type,
              currentName,
              latestEntity.ai_metadata,
            );
            nextMetadata.name_mode = currentNameMode;
            const canAutoRename = currentNameMode === 'system';
            if (canAutoRename) {
              latestEntity.name = autoSuggestedName.slice(0, AUTO_NAME_MAX_LENGTH);
              nextMetadata.name_mode = 'llm';
              nextMetadata.name_auto = true;
            } else if (toProfile(latestEntity.ai_metadata).name_auto) {
              nextMetadata.name_auto = false;
            }
          } else {
            // Clear the auto-name flag if this analysis didn't produce a name
            // (e.g. need_clarification or type doesn't support suggestedName).
            if (toProfile(latestEntity.ai_metadata).name_auto) {
              nextMetadata.name_auto = false;
            }
          }

          const analysisReplyText = aiPrompts.buildEntityAnalysisReplyText(analysis);
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

  return router;
}

module.exports = {
  createAiRouter,
};
