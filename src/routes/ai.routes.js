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
  const PROJECT_CONTEXT_BUILD_OUTPUT_SCHEMA = Object.freeze({
    type: 'json_schema',
    name: 'ProjectContextBuild',
    strict: true,
    schema: {
      type: 'object',
      required: ['status', 'description', 'summary', 'changeReason', 'missing', 'fields'],
      additionalProperties: false,
      properties: {
        status: { type: 'string', enum: ['ready', 'need_clarification'] },
        description: { type: 'string' },
        summary: { type: 'string' },
        changeReason: { type: 'string' },
        missing: { type: 'array', items: { type: 'string' } },
        fields: {
          type: 'object',
          required: PROJECT_CHAT_FIELD_KEYS,
          additionalProperties: false,
          properties: Object.fromEntries(
            PROJECT_CHAT_FIELD_KEYS.map((fieldKey) => [fieldKey, { type: 'array', items: { type: 'string' } }]),
          ),
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

  function normalizeProjectContextDescription(rawValue, fallbackValue = '') {
    const primary = toTrimmedString(rawValue, 3000);
    if (primary) return primary;
    return toTrimmedString(fallbackValue, 3000);
  }

  function collectProjectAggregatedEntityFields(sourceEntities) {
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
          if (values.length >= config.maxItems) break;
        }
        if (values.length >= config.maxItems) break;
      }
      aggregatedEntityFields[fieldKey] = values;
    }

    return aggregatedEntityFields;
  }

  function normalizeProjectContextFields(rawFields) {
    const fields = toProfile(rawFields);
    const normalized = {};
    for (const fieldKey of PROJECT_CHAT_FIELD_KEYS) {
      const config = PROJECT_CHAT_FIELD_CONFIGS[fieldKey];
      const source = Array.isArray(fields[fieldKey]) ? fields[fieldKey] : [];
      const dedup = new Set();
      const values = [];
      for (const rawValue of source) {
        const value = normalizeProjectEnrichmentFieldValue(fieldKey, rawValue, config.itemMaxLength);
        if (!value) continue;
        const key = value.toLowerCase();
        if (dedup.has(key)) continue;
        dedup.add(key);
        values.push(value);
        if (values.length >= config.maxItems) break;
      }
      normalized[fieldKey] = values;
    }
    return normalized;
  }

  function normalizeProjectContextMissing(rawValues) {
    const source = Array.isArray(rawValues) ? rawValues : [];
    const dedup = new Set();
    const values = [];
    for (const item of source) {
      const value = toTrimmedString(item, 180);
      if (!value) continue;
      const key = value.toLowerCase();
      if (dedup.has(key)) continue;
      dedup.add(key);
      values.push(value);
      if (values.length >= 8) break;
    }
    return values;
  }

  function mergeProjectContextFieldLists(fieldKey, ...lists) {
    const config = PROJECT_CHAT_FIELD_CONFIGS[fieldKey];
    const dedup = new Set();
    const values = [];
    for (const list of lists) {
      const source = Array.isArray(list) ? list : [];
      for (const item of source) {
        const value = normalizeProjectEnrichmentFieldValue(fieldKey, item, config.itemMaxLength);
        if (!value) continue;
        const key = value.toLowerCase();
        if (dedup.has(key)) continue;
        dedup.add(key);
        values.push(value);
        if (values.length >= config.maxItems) {
          return values;
        }
      }
    }
    return values;
  }

  function buildProjectContextFallbackDescription({
    scopeContext,
    aggregatedEntityFields,
    currentDescription,
  }) {
    const existingDescription = normalizeProjectContextDescription(currentDescription);
    if (existingDescription) return existingDescription;

    const projectName = toTrimmedString(scopeContext?.projectName, 160) || 'Проект';
    const groups = Array.isArray(scopeContext?.groups) ? scopeContext.groups : [];
    const entities = Array.isArray(scopeContext?.entities) ? scopeContext.entities : [];
    const locations = Array.isArray(aggregatedEntityFields?.location) ? aggregatedEntityFields.location.slice(0, 3) : [];
    const owners = Array.isArray(aggregatedEntityFields?.owners) ? aggregatedEntityFields.owners.slice(0, 3) : [];
    const members = entities
      .slice(0, 4)
      .map((entity) => toTrimmedString(entity?.name, 80))
      .filter(Boolean);

    const parts = [
      `${projectName} включает ${entities.length} сущн.`,
      groups.length ? `Групп: ${groups.length}.` : '',
      members.length ? `Ключевые объекты: ${members.join(', ')}.` : '',
      owners.length ? `Ответственные: ${owners.join(', ')}.` : '',
      locations.length ? `Локации: ${locations.join(', ')}.` : '',
    ].filter(Boolean);

    return toTrimmedString(parts.join(' '), 900);
  }

  function buildProjectContextFallbackResult({
    scopeContext,
    currentProjectFields,
    aggregatedEntityFields,
    currentDescription,
  }) {
    const fields = {};
    for (const fieldKey of PROJECT_CHAT_FIELD_KEYS) {
      fields[fieldKey] = mergeProjectContextFieldLists(
        fieldKey,
        currentProjectFields[fieldKey],
        aggregatedEntityFields[fieldKey],
      );
    }

    return {
      description: buildProjectContextFallbackDescription({
        scopeContext,
        aggregatedEntityFields,
        currentDescription,
      }),
      summary: 'Контекст собран в упрощенном режиме без полного LLM-анализа.',
      changeReason: 'fallback_after_timeout',
      missing: [],
      fields,
    };
  }

  function buildProjectContextCanvasSignature(canvasData) {
    const canvas = toProfile(canvasData);
    const nodes = (Array.isArray(canvas.nodes) ? canvas.nodes : [])
      .map((node) => {
        const row = toProfile(node);
        const id = toTrimmedString(row.id, 120);
        const entityId = toTrimmedString(row.entityId, 120);
        if (!id || !entityId) return null;
        return {
          id,
          entityId,
          x: Number.isFinite(Number(row.x)) ? Number(row.x) : 0,
          y: Number.isFinite(Number(row.y)) ? Number(row.y) : 0,
          scale: Number.isFinite(Number(row.scale)) ? Number(row.scale) : 1,
        };
      })
      .filter(Boolean)
      .sort((left, right) => String(left.id).localeCompare(String(right.id)));
    const edges = (Array.isArray(canvas.edges) ? canvas.edges : [])
      .map((edge) => {
        const row = toProfile(edge);
        const source = toTrimmedString(row.source, 120);
        const target = toTrimmedString(row.target, 120);
        if (!source || !target) return null;
        return {
          id: toTrimmedString(row.id, 120),
          source,
          target,
          label: toTrimmedString(row.label, 120),
          color: toTrimmedString(row.color, 32),
          arrowLeft: row.arrowLeft === true,
          arrowRight: row.arrowRight === true,
        };
      })
      .filter(Boolean)
      .sort((left, right) => {
        const leftKey = `${left.id}|${left.source}|${left.target}|${left.label}`;
        const rightKey = `${right.id}|${right.source}|${right.target}|${right.label}`;
        return leftKey.localeCompare(rightKey);
      });
    const groups = (Array.isArray(canvas.groups) ? canvas.groups : [])
      .map((group) => {
        const row = toProfile(group);
        const id = toTrimmedString(row.id, 120);
        if (!id) return null;
        const nodeIds = (Array.isArray(row.nodeIds) ? row.nodeIds : [])
          .map((nodeId) => toTrimmedString(nodeId, 120))
          .filter(Boolean)
          .sort((left, right) => left.localeCompare(right));
        if (nodeIds.length < 2) return null;
        return {
          id,
          nodeIds,
        };
      })
      .filter(Boolean)
      .sort((left, right) => String(left.id).localeCompare(String(right.id)));

    return { nodes, edges, groups };
  }

  function hashProjectContextCanvasSignature(signature) {
    const text = JSON.stringify(signature);
    let hash = 2166136261;
    for (let index = 0; index < text.length; index += 1) {
      hash ^= text.charCodeAt(index);
      hash = Math.imul(hash, 16777619);
    }
    return `ctx-${(hash >>> 0).toString(16).padStart(8, '0')}`;
  }

  function buildProjectContextSourceHash(canvasData) {
    return hashProjectContextCanvasSignature(buildProjectContextCanvasSignature(canvasData));
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

  router.post('/project-context/build', requireAuth, async (req, res, next) => {
    try {
      const ownerId = requireOwnerId(req);
      const projectId = toTrimmedString(req.body?.projectId, 80);
      if (!projectId) {
        return res.status(400).json({ message: 'projectId is required' });
      }

      const project = await Entity.findOne({ _id: projectId, owner_id: ownerId });
      if (!project || project.type !== 'project') {
        return res.status(404).json({ message: 'Project not found' });
      }

      const initialMeta = toProfile(project.ai_metadata);
      const sourceHash = buildProjectContextSourceHash(project.canvas_data);
      const buildingMeta = {
        ...initialMeta,
        project_context_status: 'building',
        project_context_error: '',
      };
      project.ai_metadata = buildingMeta;
      await project.save();
      broadcastEntityEvent(ownerId, 'entity.updated', {
        entity: project.toObject(),
      });

      const scopeContext = await resolveAgentScopeContext(ownerId, {
        type: 'project',
        projectId,
      });
      const llmContextResult = llmContextTools.buildAgentLlmContext({
        scopeContext,
        history: [],
        attachments: [],
        message: 'Собери контекст проекта по текущему dashboard snapshot.',
      });
      const contextData = llmContextResult.contextData;
      const reducedContextData = {
        ...toProfile(contextData),
        entities: (Array.isArray(contextData?.entities) ? contextData.entities : []).slice(0, 80),
        connections: (Array.isArray(contextData?.connections) ? contextData.connections : []).slice(0, 120),
        groups: (Array.isArray(contextData?.groups) ? contextData.groups : []).slice(0, 40),
      };
      const sourceEntities = Array.isArray(scopeContext.sourceEntities) ? scopeContext.sourceEntities : scopeContext.entities;
      const currentProjectFields = {};
      for (const fieldKey of PROJECT_CHAT_FIELD_KEYS) {
        currentProjectFields[fieldKey] = Array.isArray(initialMeta[fieldKey]) ? initialMeta[fieldKey] : [];
      }
      const aggregatedEntityFields = collectProjectAggregatedEntityFields(sourceEntities);

      const systemPrompt = aiPrompts.buildProjectContextBuildSystemPrompt();
      const userPrompt = aiPrompts.buildProjectContextBuildUserPrompt({
        contextData: reducedContextData,
        currentProjectDescription: initialMeta.description,
        currentProjectFields,
        aggregatedEntityFields,
        sourceHash,
      });

      const buildModel = toTrimmedString(OPENAI_MODEL, 120) || toTrimmedString(OPENAI_PROJECT_MODEL, 120) || 'gpt-5';
      let payload = null;
      try {
        const aiResponse = await withAiTrace({
          label: 'project-context.build',
          ownerId,
          model: buildModel,
          projectId,
          promptLengths: { system: systemPrompt.length, user: userPrompt.length },
          includeDebug: false,
        }, () => aiProvider.requestOpenAiAgentReply({
          systemPrompt,
          userPrompt,
          model: buildModel,
          temperature: 0.1,
          maxOutputTokens: 1400,
          allowEmptyResponse: false,
          timeoutMs: 65_000,
          reasoningEffort: 'low',
          verbosity: 'low',
          jsonSchema: PROJECT_CONTEXT_BUILD_OUTPUT_SCHEMA,
          singleRequest: true,
        }));

        const parsed = extractJsonObjectFromText(aiResponse.reply);
        if (!parsed || parsed.status !== 'ready') {
          throw Object.assign(new Error('Failed to parse project context build result'), { status: 502 });
        }
        payload = toProfile(parsed);
      } catch (error) {
        payload = buildProjectContextFallbackResult({
          scopeContext,
          currentProjectFields,
          aggregatedEntityFields,
          currentDescription: initialMeta.description,
        });
        payload._fallbackError = toTrimmedString(error?.message, 240);
      }

      const normalizedFields = normalizeProjectContextFields(payload.fields);
      const nextDescription = normalizeProjectContextDescription(payload.description, payload.summary);
      const missing = normalizeProjectContextMissing(payload.missing);

      const freshProject = await Entity.findOne({ _id: projectId, owner_id: ownerId });
      if (!freshProject || freshProject.type !== 'project') {
        return res.status(404).json({ message: 'Project not found' });
      }

      const freshMeta = toProfile(freshProject.ai_metadata);
      const nextVersion = Math.max(0, Number(freshMeta.project_context_version) || 0) + 1;
      freshProject.ai_metadata = {
        ...freshMeta,
        ...normalizedFields,
        description: nextDescription,
        project_context_status: 'fresh',
        project_context_source_hash: sourceHash,
        project_context_built_at: new Date().toISOString(),
        project_context_version: nextVersion,
        project_context_error: '',
        project_context_summary: toTrimmedString(payload.summary, 600),
        project_context_change_reason: toTrimmedString(payload.changeReason, 400),
        project_context_missing: missing,
        project_context_build_mode: payload._fallbackError ? 'fallback' : 'llm',
        project_context_last_llm_error: toTrimmedString(payload._fallbackError, 240),
        project_context_entity_count: Array.isArray(contextData?.entities) ? contextData.entities.length : 0,
        project_context_connection_count: Array.isArray(contextData?.connections) ? contextData.connections.length : 0,
        project_context_group_count: Array.isArray(contextData?.groups) ? contextData.groups.length : 0,
      };
      await freshProject.save();
      broadcastEntityEvent(ownerId, 'entity.updated', {
        entity: freshProject.toObject(),
      });

      return res.status(200).json({
        status: 'ready',
        sourceHash,
        entity: freshProject.toObject(),
      });
    } catch (error) {
      const ownerId = (() => {
        try {
          return requireOwnerId(req);
        } catch {
          return '';
        }
      })();
      const projectId = toTrimmedString(req.body?.projectId, 80);
      if (ownerId && projectId) {
        try {
          const failedProject = await Entity.findOne({ _id: projectId, owner_id: ownerId });
          if (failedProject && failedProject.type === 'project') {
            const failedMeta = toProfile(failedProject.ai_metadata);
            failedProject.ai_metadata = {
              ...failedMeta,
              project_context_status: 'failed',
              project_context_error: toTrimmedString(error?.message, 240) || 'build_failed',
            };
            await failedProject.save();
            broadcastEntityEvent(ownerId, 'entity.updated', {
              entity: failedProject.toObject(),
            });
          }
        } catch {
          // Ignore failure-status persistence errors.
        }
      }

      return next(error);
    }
  });

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
