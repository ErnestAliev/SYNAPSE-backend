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
      required: ['status', 'summary', 'changeReason', 'missing', 'analysisMap'],
      additionalProperties: false,
      properties: {
        status: { type: 'string', enum: ['ready', 'need_clarification'] },
        summary: { type: 'string' },
        changeReason: { type: 'string' },
        missing: { type: 'array', items: { type: 'string' } },
        analysisMap: {
          type: 'object',
          required: ['project_name', 'author_context', 'entities', 'connections', 'project_synthesis'],
          additionalProperties: false,
          properties: {
            project_name: { type: 'string' },
            author_context: {
              type: 'object',
              required: ['entity_id', 'name', 'role_in_project', 'why_matters'],
              additionalProperties: false,
              properties: {
                entity_id: { type: 'string' },
                name: { type: 'string' },
                role_in_project: { type: 'string' },
                why_matters: { type: 'string' },
              },
            },
            entities: {
              type: 'array',
              items: {
                type: 'object',
                required: [
                  'entity_id',
                  'name',
                  'type',
                  'role_in_project',
                  'summary',
                  'strengths',
                  'weaknesses',
                  'opportunities',
                  'risks',
                  'importance',
                  'why_now',
                  'relation_to_author',
                  'relation_to_goal',
                  'stage',
                  'evidence',
                ],
                additionalProperties: false,
                properties: {
                  entity_id: { type: 'string' },
                  name: { type: 'string' },
                  type: { type: 'string' },
                  role_in_project: { type: 'string' },
                  summary: { type: 'string' },
                  strengths: { type: 'array', items: { type: 'string' } },
                  weaknesses: { type: 'array', items: { type: 'string' } },
                  opportunities: { type: 'array', items: { type: 'string' } },
                  risks: { type: 'array', items: { type: 'string' } },
                  importance: { type: 'integer', minimum: 0, maximum: 100 },
                  why_now: { type: 'string' },
                  relation_to_author: { type: 'string' },
                  relation_to_goal: { type: 'string' },
                  stage: { type: 'string' },
                  evidence: { type: 'array', items: { type: 'string' } },
                },
              },
            },
            connections: {
              type: 'array',
              items: {
                type: 'object',
                required: ['from', 'to', 'label', 'meaning', 'impact', 'strength'],
                additionalProperties: false,
                properties: {
                  from: { type: 'string' },
                  to: { type: 'string' },
                  label: { type: 'string' },
                  meaning: { type: 'string' },
                  impact: { type: 'string', enum: ['positive', 'negative', 'neutral'] },
                  strength: { type: 'integer', minimum: 0, maximum: 100 },
                },
              },
            },
            project_synthesis: {
              type: 'object',
              required: [
                'main_goal',
                'current_engine',
                'main_bottleneck',
                'hidden_leverage',
                'critical_constraint',
                'next_focus',
                'confidence',
              ],
              additionalProperties: false,
              properties: {
                main_goal: { type: 'string' },
                current_engine: { type: 'string' },
                main_bottleneck: { type: 'string' },
                hidden_leverage: { type: 'string' },
                critical_constraint: { type: 'string' },
                next_focus: { type: 'string' },
                confidence: { type: 'integer', minimum: 0, maximum: 100 },
              },
            },
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

  function normalizeProjectEntityId(rawValue, maxLength = 120) {
    if (typeof rawValue === 'string') {
      return toTrimmedString(rawValue, maxLength);
    }
    if (typeof rawValue === 'number' && Number.isFinite(rawValue)) {
      return toTrimmedString(String(rawValue), maxLength);
    }
    if (rawValue && typeof rawValue === 'object' && typeof rawValue.toString === 'function') {
      const serialized = rawValue.toString();
      if (serialized && serialized !== '[object Object]') {
        return toTrimmedString(serialized, maxLength);
      }
    }
    return '';
  }

  function clampProjectScore(rawValue, fallbackValue = 0) {
    const numeric = Number(rawValue);
    if (!Number.isFinite(numeric)) {
      return Math.max(0, Math.min(100, Math.round(Number(fallbackValue) || 0)));
    }
    return Math.max(0, Math.min(100, Math.round(numeric)));
  }

  function normalizeProjectAnalysisList(rawValues, { maxItems = 4, maxLength = 120 } = {}) {
    const source = Array.isArray(rawValues)
      ? rawValues
      : typeof rawValues === 'string'
        ? [rawValues]
        : [];
    const dedup = new Set();
    const values = [];
    for (const item of source) {
      const value = toTrimmedString(item, maxLength)
        .replace(/\s+/g, ' ')
        .trim();
      if (!value) continue;
      const key = value.toLowerCase();
      if (dedup.has(key)) continue;
      dedup.add(key);
      values.push(value);
      if (values.length >= maxItems) break;
    }
    return values;
  }

  function normalizeProjectAnalysisEvidence(rawValues) {
    return normalizeProjectAnalysisList(rawValues, { maxItems: 5, maxLength: 140 });
  }

  function firstProjectAnalysisValue(rawValue, maxLength = 220) {
    return normalizeProjectAnalysisList(rawValue, { maxItems: 1, maxLength })[0] || '';
  }

  function getProjectEntityFieldList(entity, fieldKey, maxItems = 4, maxLength = 120) {
    const meta = toProfile(entity?.ai_metadata);
    return normalizeProjectAnalysisList(meta[fieldKey], { maxItems, maxLength });
  }

  function buildEntityDescriptionSentences(entity, maxItems = 3, maxLength = 180) {
    const description = toTrimmedString(toProfile(entity?.ai_metadata).description, 2200)
      .replace(/\s+/g, ' ')
      .trim();
    if (!description) return [];
    return description
      .split(/(?<=[.!?])\s+|;\s+/)
      .map((part) => toTrimmedString(part, maxLength))
      .filter(Boolean)
      .slice(0, maxItems);
  }

  function hasProjectNegativeSignal(value) {
    return /проблем|риск|убыт|не готов|требует|низк|долг|просроч|ручн|налог|юрид|канализ|ремонт|не вед/.test(
      toTrimmedString(value, 180).toLowerCase(),
    );
  }

  function hasProjectOpportunitySignal(value) {
    return /рост|масштаб|цель|план|увелич|сдан|аренд|прибыл|поток|автомат|возмож/.test(
      toTrimmedString(value, 180).toLowerCase(),
    );
  }

  function pickProjectEntityEvidence(entity) {
    const descriptionSentences = buildEntityDescriptionSentences(entity, 2, 140)
      .map((item) => `desc: ${item}`);
    const status = getProjectEntityFieldList(entity, 'status', 2, 96).map((item) => `status: ${item}`);
    const metrics = getProjectEntityFieldList(entity, 'metrics', 2, 120).map((item) => `metric: ${item}`);
    const risks = getProjectEntityFieldList(entity, 'risks', 2, 120).map((item) => `risk: ${item}`);
    const roles = getProjectEntityFieldList(entity, 'roles', 2, 96).map((item) => `role: ${item}`);
    return normalizeProjectAnalysisEvidence([
      ...descriptionSentences,
      ...status,
      ...metrics,
      ...risks,
      ...roles,
    ]);
  }

  function deriveProjectEntityRole(entity, authorEntity, relationLabels = []) {
    const row = toProfile(entity);
    const meta = toProfile(row.ai_metadata);
    const roles = getProjectEntityFieldList(entity, 'roles', 2, 96);
    const name = toTrimmedString(row.name, 120);
    if (row.is_me === true || row.is_mine === true) {
      return 'Авторский управленческий контур проекта';
    }
    if (roles.length) return roles.join(', ');
    const relationHint = normalizeProjectAnalysisList(relationLabels, { maxItems: 1, maxLength: 96 })[0];
    if (relationHint) {
      return `${name || 'Сущность'} участвует в проекте как ${relationHint}`;
    }
    const type = toTrimmedString(row.type, 24);
    if (type === 'goal' || type === 'result') return 'Целевой ориентир проекта';
    if (type === 'task') return 'Операционный шаг проекта';
    if (type === 'person') return authorEntity ? 'Человек в рабочем контуре автора' : 'Человек в рабочем контуре проекта';
    if (type === 'company') return 'Организационный или юридический контур проекта';
    if (type === 'resource') return 'Ресурсная база проекта';
    return toTrimmedString(meta.description, 96) ? 'Рабочий узел проекта' : 'Сущность проекта';
  }

  function deriveProjectEntityStrengths(entity) {
    const metrics = getProjectEntityFieldList(entity, 'metrics', 2, 120);
    const outcomes = getProjectEntityFieldList(entity, 'outcomes', 2, 120);
    const resources = getProjectEntityFieldList(entity, 'resources', 2, 120);
    const statuses = getProjectEntityFieldList(entity, 'status', 2, 96).filter((item) => !hasProjectNegativeSignal(item));
    const description = buildEntityDescriptionSentences(entity, 2, 140).filter((item) => hasProjectOpportunitySignal(item));
    const values = [
      ...metrics,
      ...outcomes,
      ...resources,
      ...statuses,
      ...description,
    ];
    return normalizeProjectAnalysisList(values, { maxItems: 4, maxLength: 120 });
  }

  function deriveProjectEntityWeaknesses(entity) {
    const statuses = getProjectEntityFieldList(entity, 'status', 4, 120).filter((item) => hasProjectNegativeSignal(item));
    const description = buildEntityDescriptionSentences(entity, 2, 140).filter((item) => hasProjectNegativeSignal(item));
    return normalizeProjectAnalysisList([...statuses, ...description], { maxItems: 4, maxLength: 120 });
  }

  function deriveProjectEntityOpportunities(entity) {
    const outcomes = getProjectEntityFieldList(entity, 'outcomes', 2, 120);
    const metrics = getProjectEntityFieldList(entity, 'metrics', 2, 120);
    const description = buildEntityDescriptionSentences(entity, 2, 140).filter((item) => hasProjectOpportunitySignal(item));
    return normalizeProjectAnalysisList([...outcomes, ...metrics, ...description], { maxItems: 4, maxLength: 120 });
  }

  function deriveProjectEntityRisks(entity) {
    const risks = getProjectEntityFieldList(entity, 'risks', 4, 120);
    const weaknesses = deriveProjectEntityWeaknesses(entity);
    return normalizeProjectAnalysisList([...risks, ...weaknesses], { maxItems: 4, maxLength: 120 });
  }

  function deriveProjectEntityStage(entity) {
    return (
      firstProjectAnalysisValue(toProfile(entity?.ai_metadata).stage, 96)
      || firstProjectAnalysisValue(toProfile(entity?.ai_metadata).status, 96)
      || ''
    );
  }

  function deriveProjectEntitySummary(entity, fallbackRole = '') {
    const descriptionSentence = buildEntityDescriptionSentences(entity, 1, 180)[0];
    if (descriptionSentence) return descriptionSentence;
    const name = toTrimmedString(entity?.name, 120);
    const stage = deriveProjectEntityStage(entity);
    if (fallbackRole && stage) return `${name} — ${fallbackRole}; стадия: ${stage}`;
    if (fallbackRole) return `${name} — ${fallbackRole}`;
    if (stage) return `${name} — стадия: ${stage}`;
    return name;
  }

  function deriveProjectEntityWhyNow(entity, { opportunities = [], weaknesses = [], risks = [] } = {}) {
    if (weaknesses.length) {
      return `Требует внимания сейчас, потому что ${weaknesses[0].replace(/[.]+$/g, '')}.`;
    }
    if (risks.length) {
      return `Актуальна сейчас из-за риска: ${risks[0].replace(/[.]+$/g, '')}.`;
    }
    if (opportunities.length) {
      return `Важна сейчас как ближайшая точка роста: ${opportunities[0].replace(/[.]+$/g, '')}.`;
    }
    const type = toTrimmedString(entity?.type, 24);
    if (type === 'goal' || type === 'result') {
      return 'Задает целевой горизонт и критерий результата для всего проекта.';
    }
    return 'Поддерживает текущий рабочий контур проекта и влияет на следующие решения.';
  }

  function buildProjectConnectionIndex(connections) {
    const list = Array.isArray(connections) ? connections : [];
    const byEntityId = new Map();
    const pairLabels = new Map();
    for (const rawConnection of list) {
      const connection = toProfile(rawConnection);
      const from = toTrimmedString(connection.from, 120);
      const to = toTrimmedString(connection.to, 120);
      const label = toTrimmedString(connection.label, 120) || toTrimmedString(connection.type, 80);
      if (!from || !to) continue;
      byEntityId.set(from, (byEntityId.get(from) || 0) + 1);
      byEntityId.set(to, (byEntityId.get(to) || 0) + 1);
      const leftKey = `${from}|${to}`;
      const rightKey = `${to}|${from}`;
      if (label) {
        pairLabels.set(leftKey, [...(pairLabels.get(leftKey) || []), label]);
        pairLabels.set(rightKey, [...(pairLabels.get(rightKey) || []), label]);
      }
    }
    return { byEntityId, pairLabels };
  }

  function buildProjectEntityImportance(entity, connectionCount = 0) {
    const row = toProfile(entity);
    let score = 35;
    if (row.is_me === true) score += 35;
    else if (row.is_mine === true) score += 20;
    const type = toTrimmedString(row.type, 24);
    if (type === 'goal' || type === 'result') score += 20;
    if (type === 'project') score += 12;
    if (type === 'company' || type === 'person') score += 10;
    score += Math.min(20, connectionCount * 5);
    if (deriveProjectEntityWeaknesses(entity).length) score += 8;
    if (deriveProjectEntityOpportunities(entity).length) score += 6;
    return clampProjectScore(score, 50);
  }

  function deriveProjectEntityGoalRelation(entity, goalEntityIds) {
    const type = toTrimmedString(entity?.type, 24);
    if (type === 'goal' || type === 'result') return 'Формирует цель проекта';
    if (goalEntityIds.has(normalizeProjectEntityId(entity?._id || entity?.id, 120))) return 'Напрямую связан с целевым контуром';
    const metrics = getProjectEntityFieldList(entity, 'metrics', 1, 120);
    if (metrics.length) return `Поддерживает цель через метрику: ${metrics[0]}`;
    const outcomes = getProjectEntityFieldList(entity, 'outcomes', 1, 120);
    if (outcomes.length) return `Поддерживает цель через результат: ${outcomes[0]}`;
    return 'Косвенно влияет на достижение цели проекта';
  }

  function deriveProjectEntityAuthorRelation(entity, authorEntity, pairLabels) {
    const row = toProfile(entity);
    if (!authorEntity) return '';
    if (
      normalizeProjectEntityId(row._id || row.id, 120) === normalizeProjectEntityId(authorEntity._id || authorEntity.id, 120)
      || row.is_me === true
      || row.is_mine === true
    ) {
      return 'Это и есть личный контур автора';
    }
    const entityId = normalizeProjectEntityId(row._id || row.id, 120);
    const authorId = normalizeProjectEntityId(authorEntity._id || authorEntity.id, 120);
    const relationLabels = normalizeProjectAnalysisList(pairLabels.get(`${authorId}|${entityId}`) || [], {
      maxItems: 2,
      maxLength: 96,
    });
    if (relationLabels.length) {
      return `Прямая связь с автором через ${relationLabels.join(', ')}`;
    }
    return 'Относится к внешнему рабочему слою проекта';
  }

  function deriveProjectConnectionImpact(connection) {
    const label = `${toTrimmedString(connection?.label, 120)} ${toTrimmedString(connection?.type, 80)}`.toLowerCase();
    if (/риск|проблем|блок|долг|конфликт|зависим/.test(label)) return 'negative';
    if (/админ|управ|владел|аренд|цель|метрик|ресурс|контур|актив/.test(label)) return 'positive';
    return 'neutral';
  }

  function buildProjectConnectionStrength(connection, entitiesById) {
    const from = toTrimmedString(connection?.from, 120);
    const to = toTrimmedString(connection?.to, 120);
    const label = toTrimmedString(connection?.label, 120);
    const leftImportance = clampProjectScore(entitiesById.get(from)?.importance, 40);
    const rightImportance = clampProjectScore(entitiesById.get(to)?.importance, 40);
    let score = 40 + Math.round((leftImportance + rightImportance) / 10);
    if (label) score += 10;
    return clampProjectScore(score, 55);
  }

  function buildProjectAnalysisMapFallback({
    scopeContext,
    sourceEntities,
    connections,
  }) {
    const entities = Array.isArray(sourceEntities) ? sourceEntities : [];
    const authorEntity = pickProjectAuthorEntity(entities);
    const { byEntityId, pairLabels } = buildProjectConnectionIndex(connections);
    const goalEntityIds = new Set(
      entities
        .filter((entity) => ['goal', 'result'].includes(toTrimmedString(entity?.type, 24)))
        .map((entity) => normalizeProjectEntityId(entity?._id || entity?.id, 120))
        .filter(Boolean),
    );

    const entityMap = entities
      .map((entity) => {
        const row = toProfile(entity);
        const entityId = normalizeProjectEntityId(row._id || row.id, 120);
        const name = toTrimmedString(row.name, 120);
        if (!entityId || !name) return null;
        const relationLabels = normalizeProjectAnalysisList(pairLabels.get(`${entityId}|${normalizeProjectEntityId(authorEntity?._id || authorEntity?.id, 120)}`) || [], {
          maxItems: 2,
          maxLength: 96,
        });
        const roleInProject = deriveProjectEntityRole(entity, authorEntity, relationLabels);
        const strengths = deriveProjectEntityStrengths(entity);
        const weaknesses = deriveProjectEntityWeaknesses(entity);
        const opportunities = deriveProjectEntityOpportunities(entity);
        const risks = deriveProjectEntityRisks(entity);
        const stage = deriveProjectEntityStage(entity);
        const importance = buildProjectEntityImportance(entity, byEntityId.get(entityId) || 0);
        return {
          entity_id: entityId,
          name,
          type: toTrimmedString(row.type, 24),
          role_in_project: roleInProject,
          summary: deriveProjectEntitySummary(entity, roleInProject),
          strengths,
          weaknesses,
          opportunities,
          risks,
          importance,
          why_now: deriveProjectEntityWhyNow(entity, { opportunities, weaknesses, risks }),
          relation_to_author: deriveProjectEntityAuthorRelation(entity, authorEntity, pairLabels),
          relation_to_goal: deriveProjectEntityGoalRelation(entity, goalEntityIds),
          stage,
          evidence: pickProjectEntityEvidence(entity),
        };
      })
      .filter(Boolean)
      .sort((left, right) => right.importance - left.importance);

    const entitiesById = new Map(entityMap.map((item) => [item.entity_id, item]));
    const normalizedConnections = (Array.isArray(connections) ? connections : [])
      .map((rawConnection) => {
        const connection = toProfile(rawConnection);
        const from = toTrimmedString(connection.from, 120);
        const to = toTrimmedString(connection.to, 120);
        if (!from || !to || !entitiesById.has(from) || !entitiesById.has(to)) return null;
        const label = toTrimmedString(connection.label, 120) || toTrimmedString(connection.type, 80);
        const fromName = entitiesById.get(from)?.name || from;
        const toName = entitiesById.get(to)?.name || to;
        return {
          from,
          to,
          label,
          meaning: label
            ? `${fromName} связан с ${toName} через "${label}".`
            : `${fromName} связан с ${toName}.`,
          impact: deriveProjectConnectionImpact(connection),
          strength: buildProjectConnectionStrength(connection, entitiesById),
        };
      })
      .filter(Boolean)
      .slice(0, 160);

    const topEntities = entityMap.slice(0, 4);
    const topPositiveEntity = entityMap.find((item) => item.strengths.length || item.opportunities.length) || entityMap[0] || null;
    const topRiskEntity = entityMap.find((item) => item.weaknesses.length || item.risks.length) || entityMap[0] || null;
    const hiddenLeverageEntity = [...entityMap]
      .sort((left, right) => {
        const leftSignal = left.opportunities.length * 20 - left.weaknesses.length * 5 - left.risks.length * 5;
        const rightSignal = right.opportunities.length * 20 - right.weaknesses.length * 5 - right.risks.length * 5;
        return rightSignal - leftSignal;
      })
      .find((item) => item && item.entity_id !== topPositiveEntity?.entity_id)
      || topPositiveEntity
      || null;

    const authorContext = authorEntity
      ? {
          entity_id: normalizeProjectEntityId(authorEntity._id || authorEntity.id, 120),
          name: toTrimmedString(authorEntity.name, 120),
          role_in_project: deriveProjectEntityRole(authorEntity, authorEntity),
          why_matters: deriveProjectEntityWhyNow(authorEntity, {
            opportunities: deriveProjectEntityOpportunities(authorEntity),
            weaknesses: deriveProjectEntityWeaknesses(authorEntity),
            risks: deriveProjectEntityRisks(authorEntity),
          }),
        }
      : {
          entity_id: '',
          name: '',
          role_in_project: '',
          why_matters: '',
        };

    const goalEntity = entityMap.find((item) => item.type === 'goal' || item.type === 'result') || null;
    const evidenceCount = entityMap.reduce((sum, item) => sum + item.evidence.length, 0);

    return {
      project_name: toTrimmedString(scopeContext?.projectName, 160) || 'Проект',
      author_context: authorContext,
      entities: entityMap,
      connections: normalizedConnections,
      project_synthesis: {
        main_goal:
          goalEntity?.summary
          || firstProjectAnalysisValue(toProfile(topPositiveEntity).relation_to_goal, 220)
          || 'Цель проекта требует уточнения.',
        current_engine: topEntities.length
          ? `Текущий двигатель проекта держится на ${topEntities.slice(0, 3).map((item) => item.name).join(', ')}.`
          : 'Текущий двигатель проекта не выявлен.',
        main_bottleneck:
          topRiskEntity?.weaknesses[0]
          || topRiskEntity?.risks[0]
          || 'Главное ограничение пока не выделено.',
        hidden_leverage:
          hiddenLeverageEntity?.opportunities[0]
          || hiddenLeverageEntity?.strengths[0]
          || 'Скрытое leverage пока не выделено.',
        critical_constraint:
          topRiskEntity?.risks[0]
          || topRiskEntity?.weaknesses[0]
          || 'Критическое ограничение пока не выделено.',
        next_focus:
          topRiskEntity
            ? `Сфокусироваться на ${topRiskEntity.name}: ${topRiskEntity.why_now}`
            : 'Собрать более полную карту проекта и уточнить главный bottleneck.',
        confidence: clampProjectScore(35 + entityMap.length * 3 + normalizedConnections.length * 2 + evidenceCount * 2, 62),
      },
    };
  }

  function normalizeProjectAnalysisEntity(rawValue, fallbackValue = {}) {
    const raw = toProfile(rawValue);
    const fallback = toProfile(fallbackValue);
    return {
      entity_id: toTrimmedString(raw.entity_id, 120) || toTrimmedString(fallback.entity_id, 120),
      name: toTrimmedString(raw.name, 120) || toTrimmedString(fallback.name, 120),
      type: toTrimmedString(raw.type, 24) || toTrimmedString(fallback.type, 24) || 'shape',
      role_in_project: toTrimmedString(raw.role_in_project, 180) || toTrimmedString(fallback.role_in_project, 180),
      summary: toTrimmedString(raw.summary, 260) || toTrimmedString(fallback.summary, 260),
      strengths: normalizeProjectAnalysisList(raw.strengths, { maxItems: 4, maxLength: 120 }).length
        ? normalizeProjectAnalysisList(raw.strengths, { maxItems: 4, maxLength: 120 })
        : normalizeProjectAnalysisList(fallback.strengths, { maxItems: 4, maxLength: 120 }),
      weaknesses: normalizeProjectAnalysisList(raw.weaknesses, { maxItems: 4, maxLength: 120 }).length
        ? normalizeProjectAnalysisList(raw.weaknesses, { maxItems: 4, maxLength: 120 })
        : normalizeProjectAnalysisList(fallback.weaknesses, { maxItems: 4, maxLength: 120 }),
      opportunities: normalizeProjectAnalysisList(raw.opportunities, { maxItems: 4, maxLength: 120 }).length
        ? normalizeProjectAnalysisList(raw.opportunities, { maxItems: 4, maxLength: 120 })
        : normalizeProjectAnalysisList(fallback.opportunities, { maxItems: 4, maxLength: 120 }),
      risks: normalizeProjectAnalysisList(raw.risks, { maxItems: 4, maxLength: 120 }).length
        ? normalizeProjectAnalysisList(raw.risks, { maxItems: 4, maxLength: 120 })
        : normalizeProjectAnalysisList(fallback.risks, { maxItems: 4, maxLength: 120 }),
      importance: clampProjectScore(raw.importance, fallback.importance),
      why_now: toTrimmedString(raw.why_now, 240) || toTrimmedString(fallback.why_now, 240),
      relation_to_author: toTrimmedString(raw.relation_to_author, 220) || toTrimmedString(fallback.relation_to_author, 220),
      relation_to_goal: toTrimmedString(raw.relation_to_goal, 220) || toTrimmedString(fallback.relation_to_goal, 220),
      stage: toTrimmedString(raw.stage, 96) || toTrimmedString(fallback.stage, 96),
      evidence: normalizeProjectAnalysisEvidence(raw.evidence).length
        ? normalizeProjectAnalysisEvidence(raw.evidence)
        : normalizeProjectAnalysisEvidence(fallback.evidence),
    };
  }

  function normalizeProjectAnalysisConnection(rawValue, fallbackValue = {}) {
    const raw = toProfile(rawValue);
    const fallback = toProfile(fallbackValue);
    const impact = toTrimmedString(raw.impact, 24);
    const normalizedImpact = ['positive', 'negative', 'neutral'].includes(impact) ? impact : toTrimmedString(fallback.impact, 24) || 'neutral';
    return {
      from: toTrimmedString(raw.from, 120) || toTrimmedString(fallback.from, 120),
      to: toTrimmedString(raw.to, 120) || toTrimmedString(fallback.to, 120),
      label: toTrimmedString(raw.label, 120) || toTrimmedString(fallback.label, 120),
      meaning: toTrimmedString(raw.meaning, 240) || toTrimmedString(fallback.meaning, 240),
      impact: normalizedImpact,
      strength: clampProjectScore(raw.strength, fallback.strength),
    };
  }

  function normalizeProjectAnalysisMap(rawValue, fallbackValue = {}) {
    const raw = toProfile(rawValue);
    const fallback = toProfile(fallbackValue);
    const fallbackEntities = Array.isArray(fallback.entities) ? fallback.entities : [];
    const fallbackConnections = Array.isArray(fallback.connections) ? fallback.connections : [];
    const rawEntityById = new Map(
      (Array.isArray(raw.entities) ? raw.entities : [])
        .map((item) => [toTrimmedString(toProfile(item).entity_id, 120), item])
        .filter(([entityId]) => Boolean(entityId)),
    );
    const rawConnectionByKey = new Map(
      (Array.isArray(raw.connections) ? raw.connections : [])
        .map((item) => {
          const row = toProfile(item);
          const from = toTrimmedString(row.from, 120);
          const to = toTrimmedString(row.to, 120);
          const label = toTrimmedString(row.label, 120);
          return [`${from}|${to}|${label}`, item];
        })
        .filter(([key]) => !key.startsWith('||')),
    );

    const normalizedEntities = fallbackEntities
      .map((fallbackEntity) => {
        const fallbackProfile = toProfile(fallbackEntity);
        return normalizeProjectAnalysisEntity(
          rawEntityById.get(normalizeProjectEntityId(fallbackProfile.entity_id, 120)),
          fallbackEntity,
        );
      })
      .filter((item) => item.entity_id && item.name);

    const normalizedConnections = fallbackConnections
      .map((fallbackConnection) => {
        const fallbackProfile = toProfile(fallbackConnection);
        const key = [
          toTrimmedString(fallbackProfile.from, 120),
          toTrimmedString(fallbackProfile.to, 120),
          toTrimmedString(fallbackProfile.label, 120),
        ].join('|');
        return normalizeProjectAnalysisConnection(rawConnectionByKey.get(key), fallbackConnection);
      })
      .filter((item) => item.from && item.to);

    const rawSynthesis = toProfile(raw.project_synthesis);
    const fallbackSynthesis = toProfile(fallback.project_synthesis);

    return {
      project_name: toTrimmedString(raw.project_name, 160) || toTrimmedString(fallback.project_name, 160) || 'Проект',
      author_context: {
        entity_id: normalizeProjectEntityId(toProfile(raw.author_context).entity_id, 120) || normalizeProjectEntityId(toProfile(fallback.author_context).entity_id, 120),
        name: toTrimmedString(toProfile(raw.author_context).name, 120) || toTrimmedString(toProfile(fallback.author_context).name, 120),
        role_in_project: toTrimmedString(toProfile(raw.author_context).role_in_project, 180) || toTrimmedString(toProfile(fallback.author_context).role_in_project, 180),
        why_matters: toTrimmedString(toProfile(raw.author_context).why_matters, 240) || toTrimmedString(toProfile(fallback.author_context).why_matters, 240),
      },
      entities: normalizedEntities,
      connections: normalizedConnections,
      project_synthesis: {
        main_goal: toTrimmedString(rawSynthesis.main_goal, 240) || toTrimmedString(fallbackSynthesis.main_goal, 240),
        current_engine: toTrimmedString(rawSynthesis.current_engine, 240) || toTrimmedString(fallbackSynthesis.current_engine, 240),
        main_bottleneck: toTrimmedString(rawSynthesis.main_bottleneck, 240) || toTrimmedString(fallbackSynthesis.main_bottleneck, 240),
        hidden_leverage: toTrimmedString(rawSynthesis.hidden_leverage, 240) || toTrimmedString(fallbackSynthesis.hidden_leverage, 240),
        critical_constraint: toTrimmedString(rawSynthesis.critical_constraint, 240) || toTrimmedString(fallbackSynthesis.critical_constraint, 240),
        next_focus: toTrimmedString(rawSynthesis.next_focus, 240) || toTrimmedString(fallbackSynthesis.next_focus, 240),
        confidence: clampProjectScore(rawSynthesis.confidence, fallbackSynthesis.confidence),
      },
    };
  }

  function compileProjectDescriptionFromAnalysisMap(analysisMap) {
    const map = toProfile(analysisMap);
    const author = toProfile(map.author_context);
    const entities = Array.isArray(map.entities) ? map.entities : [];
    const synthesis = toProfile(map.project_synthesis);
    const topEntities = entities.slice(0, 4).map((entity) => toProfile(entity));
    const topBase = topEntities
      .map((entity) => {
        const name = toTrimmedString(entity.name, 120);
        const summary = toTrimmedString(entity.summary, 180);
        if (!name) return '';
        return summary ? `${name}: ${summary}` : name;
      })
      .filter(Boolean)
      .join('; ');
    const topStrengths = topEntities
      .flatMap((entity) => normalizeProjectAnalysisList(entity.strengths, { maxItems: 2, maxLength: 110 }))
      .slice(0, 4)
      .join('; ');

    const sections = [
      ['Название проекта', toTrimmedString(map.project_name, 180) || 'Проект'],
      ['Авторский контур', [toTrimmedString(author.name, 120), toTrimmedString(author.role_in_project, 180), toTrimmedString(author.why_matters, 240)].filter(Boolean).join('. ')],
      ['Главная цель', toTrimmedString(synthesis.main_goal, 240)],
      ['Текущая база проекта', topBase],
      ['Сильные стороны контура', topStrengths],
      ['Ограничения и bottlenecks', [toTrimmedString(synthesis.main_bottleneck, 240), toTrimmedString(synthesis.critical_constraint, 240)].filter(Boolean).join('. ')],
      ['Скрытая возможность', toTrimmedString(synthesis.hidden_leverage, 240)],
      ['Ближайший фокус', toTrimmedString(synthesis.next_focus, 240)],
    ].filter(([, value]) => Boolean(toTrimmedString(value, 2000)));

    return toTrimmedString(
      sections
        .map(([label, value]) => `${label}:\n${value}`)
        .join('\n\n'),
      3000,
    );
  }

  function pickProjectAuthorEntity(sourceEntities) {
    const entities = Array.isArray(sourceEntities) ? sourceEntities : [];
    const authorByMe = entities.find((entity) => entity?.is_me === true);
    if (authorByMe) return authorByMe;
    const authorByMine = entities.find((entity) => entity?.is_mine === true);
    if (authorByMine) return authorByMine;
    return entities.find((entity) => entity?.type === 'person') || entities[0] || null;
  }

  function buildProjectEntityNarrativeCard(entity) {
    const row = toProfile(entity);
    const meta = toProfile(row.ai_metadata);
    const name = toTrimmedString(row.name, 120);
    if (!name) return null;

    return {
      id: normalizeProjectEntityId(row._id, 120),
      name,
      type: toTrimmedString(row.type, 24),
      isAuthor: row.is_me === true || row.is_mine === true,
      is_me: row.is_me === true,
      is_mine: row.is_mine === true,
      description: toTrimmedString(meta.description, 320),
    };
  }

  function splitNarrativeSentences(rawValue, maxItems = 3, maxLength = 220) {
    const text = toTrimmedString(rawValue, 2000)
      .replace(/\s+/g, ' ')
      .trim();
    if (!text) return [];

    return text
      .split(/(?<=[.!?])\s+|;\s+/)
      .map((part) => toTrimmedString(part, maxLength))
      .filter(Boolean)
      .slice(0, maxItems);
  }

  function trimSentenceEnding(value) {
    return toTrimmedString(value, 240).replace(/[.!\s]+$/g, '').trim();
  }

  function buildEntityFocusSnippet(card) {
    const row = toProfile(card);
    const name = toTrimmedString(row.name, 120);
    if (!name) return '';

    const roles = Array.isArray(row.roles) ? row.roles.map((item) => toTrimmedString(item, 72)).filter(Boolean) : [];
    const statuses = Array.isArray(row.status) ? row.status.map((item) => toTrimmedString(item, 72)).filter(Boolean) : [];
    const metrics = Array.isArray(row.metrics) ? row.metrics.map((item) => toTrimmedString(item, 96)).filter(Boolean) : [];
    const descriptionSentences = splitNarrativeSentences(row.description, 2, 200);
    const focus = descriptionSentences[0] || '';

    if (focus) {
      return `${name} — ${trimSentenceEnding(focus)}.`;
    }

    const fragments = [];
    if (roles.length) fragments.push(roles.slice(0, 2).join(', '));
    if (statuses.length) fragments.push(`статус: ${statuses.slice(0, 2).join(', ')}`);
    if (metrics.length) fragments.push(`метрика: ${metrics[0]}`);
    if (!fragments.length) return name;
    return `${name} — ${fragments.join('; ')}.`;
  }

  function buildAuthorOpening(author) {
    const row = toProfile(author);
    const name = toTrimmedString(row.name, 120);
    const roles = Array.isArray(row.roles) ? row.roles.map((item) => toTrimmedString(item, 72)).filter(Boolean) : [];
    const sentences = splitNarrativeSentences(row.description, 2, 220);
    const roleText = roles.length ? `, ${roles.slice(0, 2).join(', ')}` : '';
    const opening = name
      ? `В центре проекта находится ${name}${roleText}.`
      : 'В центре проекта находится автор и его операционный контур.';

    if (!sentences.length) return opening;
    return `${opening} ${trimSentenceEnding(sentences[0])}.`;
  }

  function buildNarrativeRingSentence(label, cards) {
    const items = (Array.isArray(cards) ? cards : [])
      .map((card) => buildEntityFocusSnippet(card))
      .filter(Boolean)
      .slice(0, 3);
    if (!items.length) return '';
    return `${label}: ${items.join(' ')}`;
  }

  function buildMetricsAndStateSentence({ aggregatedEntityFields }) {
    const metrics = Array.isArray(aggregatedEntityFields?.metrics) ? aggregatedEntityFields.metrics.slice(0, 2) : [];
    const statuses = Array.isArray(aggregatedEntityFields?.status) ? aggregatedEntityFields.status.slice(0, 3) : [];
    const tasks = Array.isArray(aggregatedEntityFields?.tasks) ? aggregatedEntityFields.tasks.slice(0, 2) : [];
    const fragments = [];
    if (metrics.length) fragments.push(`Целевой контур: ${metrics.join(', ')}`);
    if (statuses.length) fragments.push(`текущие статусы: ${statuses.join(', ')}`);
    if (tasks.length) fragments.push(`рабочие задачи: ${tasks.join(', ')}`);
    if (!fragments.length) return '';
    return `${fragments.join('; ')}.`;
  }

  function buildConstraintsSentence({ aggregatedEntityFields, owners, locations }) {
    const risks = Array.isArray(aggregatedEntityFields?.risks) ? aggregatedEntityFields.risks.slice(0, 3) : [];
    const fragments = [];
    if (owners.length) fragments.push(`Ответственность закреплена за ${owners.join(', ')}`);
    if (locations.length) fragments.push(`география проекта: ${locations.join(', ')}`);
    if (risks.length) fragments.push(`ключевые ограничения: ${risks.join(', ')}`);
    if (!fragments.length) return '';
    return `${fragments.join('; ')}.`;
  }

  function buildGoalSentence({ sourceEntities, aggregatedEntityFields }) {
    const goalCards = (Array.isArray(sourceEntities) ? sourceEntities : [])
      .filter((entity) => entity?.type === 'goal' || entity?.type === 'result')
      .map((entity) => buildProjectEntityNarrativeCard(entity))
      .filter(Boolean)
      .slice(0, 2);
    const goalSnippets = goalCards
      .map((card) => trimSentenceEnding(toProfile(card).description))
      .filter(Boolean);
    if (goalSnippets.length) {
      return `Цель проекта: ${goalSnippets.join(' ')}.`;
    }

    const metrics = Array.isArray(aggregatedEntityFields?.metrics) ? aggregatedEntityFields.metrics.slice(0, 2) : [];
    const outcomes = Array.isArray(aggregatedEntityFields?.outcomes) ? aggregatedEntityFields.outcomes.slice(0, 2) : [];
    if (metrics.length || outcomes.length) {
      const fragments = [];
      if (metrics.length) fragments.push(`метрики ${metrics.join(', ')}`);
      if (outcomes.length) fragments.push(`ожидаемые результаты ${outcomes.join(', ')}`);
      return `Цель проекта задают ${fragments.join(' и ')}.`;
    }

    return '';
  }

  function deriveProjectBottleneckThemes({ sourceEntities, aggregatedEntityFields }) {
    const sourceTexts = [
      ...(Array.isArray(aggregatedEntityFields?.risks) ? aggregatedEntityFields.risks : []),
      ...(Array.isArray(aggregatedEntityFields?.status) ? aggregatedEntityFields.status : []),
      ...(Array.isArray(aggregatedEntityFields?.tasks) ? aggregatedEntityFields.tasks : []),
      ...(Array.isArray(sourceEntities) ? sourceEntities.map((entity) => toProfile(entity?.ai_metadata).description) : []),
    ]
      .map((value) => toTrimmedString(value, 240).toLowerCase())
      .filter(Boolean);

    const themes = [];
    const hasAny = (patterns) => patterns.some((pattern) => sourceTexts.some((text) => pattern.test(text)));

    if (hasAny([/ручн/, /вовлечен/, /процесс/, /систем/, /автомат/])) {
      themes.push('операционная воспроизводимость');
    }
    if (hasAny([/ремонт/, /цоколь/, /канализ/, /отоплен/, /ктп/, /генератор/, /инженер/])) {
      themes.push('инженерный контур и доведение проблемных объектов');
    }
    if (hasAny([/документ/, /правопреем/, /юрид/, /договор/])) {
      themes.push('документы и юридическая чистота');
    }
    if (hasAny([/налог/, /коэффициент/, /нагрузк/])) {
      themes.push('налоговая устойчивость');
    }

    return themes.slice(0, 4);
  }

  function buildProjectPhaseSentence({ sourceEntities, aggregatedEntityFields }) {
    const themes = deriveProjectBottleneckThemes({ sourceEntities, aggregatedEntityFields });
    if (themes.length) {
      return `По смыслу проект сейчас находится не в фазе чистого масштабирования, а в фазе перехода от ручного управления текущими активами к системе, где ключевой bottleneck — ${themes.join(', ')}.`;
    }

    const statuses = Array.isArray(aggregatedEntityFields?.status) ? aggregatedEntityFields.status.slice(0, 3) : [];
    if (statuses.length) {
      return `Сейчас проект находится в фазе стабилизации текущих объектов и управленческого контура; это видно по статусам: ${statuses.join(', ')}.`;
    }

    return '';
  }

  function buildAssetBaseSentence({ narrativeRings, scopeContext }) {
    const innerRing = Array.isArray(toProfile(narrativeRings).inner) ? toProfile(narrativeRings).inner : [];
    const outerRing = Array.isArray(toProfile(narrativeRings).outer) ? toProfile(narrativeRings).outer : [];
    const cards = [...innerRing, ...outerRing]
      .map((card) => toProfile(card))
      .filter((card) => !card.isAuthor)
      .slice(0, 4);
    const snippets = cards
      .map((card) => buildEntityFocusSnippet(card))
      .filter(Boolean);
    if (snippets.length) {
      return `Текущая база проекта выглядит так: ${snippets.join(' ')}`;
    }

    const totalEntities = Math.max(0, Number(scopeContext?.totalEntities) || 0);
    if (totalEntities > 0) {
      return `Сейчас проект опирается на ${totalEntities} сущностей, связанных в единый операционный контур.`;
    }

    return '';
  }

  function synthesizeProjectContextNarrative({
    scopeContext,
    sourceEntities,
    aggregatedEntityFields,
    author,
    narrativeRings,
  }) {
    const projectName = toTrimmedString(scopeContext?.projectName, 160) || 'Проект';
    const authorSentence = buildAuthorOpening(author);
    const goalSentence = buildGoalSentence({ sourceEntities, aggregatedEntityFields });
    const assetSentence = buildAssetBaseSentence({ narrativeRings, scopeContext });
    const phaseSentence = buildProjectPhaseSentence({ sourceEntities, aggregatedEntityFields });
    const owners = Array.isArray(aggregatedEntityFields?.owners) ? aggregatedEntityFields.owners.slice(0, 3) : [];
    const locations = Array.isArray(aggregatedEntityFields?.location) ? aggregatedEntityFields.location.slice(0, 3) : [];
    const constraintsSentence = buildConstraintsSentence({ aggregatedEntityFields, owners, locations });

    const sentences = [
      authorSentence || `Проект «${projectName}» собран вокруг центрального управленческого контура.`,
      goalSentence,
      assetSentence,
      phaseSentence,
      constraintsSentence,
    ].filter(Boolean);

    return toTrimmedString(sentences.join(' '), 900);
  }

  function buildProjectNarrativeContext({ sourceEntities, connections }) {
    const entities = Array.isArray(sourceEntities) ? sourceEntities : [];
    const authorEntity = pickProjectAuthorEntity(entities);
    const cards = entities
      .map((entity) => ({ entity, card: buildProjectEntityNarrativeCard(entity) }))
      .filter((item) => Boolean(item.card));

    const authorCard = authorEntity ? buildProjectEntityNarrativeCard(authorEntity) : null;
    if (!authorCard) {
      return {
        author: null,
        narrativeRings: {
          inner: cards.slice(0, 4).map((item) => item.card),
          outer: cards.slice(4, 8).map((item) => item.card),
        },
      };
    }

    const authorId = toTrimmedString(authorCard.id, 120);
    const connectionNames = new Set();
    for (const rawConnection of Array.isArray(connections) ? connections : []) {
      const connection = toProfile(rawConnection);
      const from = toTrimmedString(connection.from, 120);
      const to = toTrimmedString(connection.to, 120);
      if (!from || !to) continue;
      if (from === authorId) connectionNames.add(to);
      if (to === authorId) connectionNames.add(from);
    }

    const inner = [];
    const outer = [];
    for (const item of cards) {
      if (!item.card || toTrimmedString(item.card.id, 120) === authorId) continue;
      if (connectionNames.has(toTrimmedString(item.card.id, 120))) {
        inner.push(item.card);
      } else {
        outer.push(item.card);
      }
    }

    return {
      author: authorCard,
      narrativeRings: {
        inner: inner.slice(0, 5),
        outer: outer.slice(0, 6),
      },
    };
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
    sourceEntities,
    author,
    narrativeRings,
  }) {
    return synthesizeProjectContextNarrative({
      scopeContext,
      sourceEntities,
      aggregatedEntityFields,
      author,
      narrativeRings,
    });
  }

  function buildProjectContextFallbackResult({
    scopeContext,
    aggregatedEntityFields,
    sourceEntities,
    connections,
  }) {
    const analysisMap = buildProjectAnalysisMapFallback({
      scopeContext,
      sourceEntities,
      connections,
      aggregatedEntityFields,
    });

    return {
      analysisMap,
      summary: 'Контекст собран в упрощенном режиме без полного LLM-анализа.',
      changeReason: 'fallback_after_timeout',
      missing: [],
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

  function buildProjectContextBuildPreview({
    project,
    scopeContext,
    sourceHash,
    reducedContextData,
    narrativeContext,
    contextData,
    sourceEntities,
  }) {
    const systemPrompt = aiPrompts.buildProjectContextBuildSystemPrompt();
    const userPayload = aiPrompts.buildProjectContextBuildPayload({
      contextData: reducedContextData,
      author: narrativeContext.author,
      narrativeRings: narrativeContext.narrativeRings,
      sourceHash,
    });
    const userPrompt = aiPrompts.buildProjectContextBuildUserPrompt({
      contextData: reducedContextData,
      author: narrativeContext.author,
      narrativeRings: narrativeContext.narrativeRings,
      sourceHash,
    });
    const buildModel = toTrimmedString(OPENAI_MODEL, 120) || toTrimmedString(OPENAI_PROJECT_MODEL, 120) || 'gpt-5';
    return {
      exportedAt: new Date().toISOString(),
      source: 'project-context.preview',
      llm_input: {
        model: buildModel,
        systemPrompt,
        userPayload,
      },
      llm_output: {
        mode: 'preview_only',
        rawReply: '',
        parsedPayload: null,
        fallbackUsed: false,
        error: '',
      },
    };
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

  router.post('/project-context/preview', requireAuth, async (req, res, next) => {
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

      const sourceHash = buildProjectContextSourceHash(project.canvas_data);
      const scopeContext = await resolveAgentScopeContext(ownerId, {
        type: 'project',
        projectId,
      });
      const sourceEntities = Array.isArray(scopeContext.sourceEntities) ? scopeContext.sourceEntities : scopeContext.entities;
      const llmContextResult = llmContextTools.buildAgentLlmContext({
        scopeContext,
        history: [],
        attachments: [],
        message: 'Собери контекст проекта по текущему dashboard snapshot.',
      });
      const contextData = llmContextResult.contextData;
      const reducedContextData = {
        ...toProfile(contextData),
        entities: (Array.isArray(sourceEntities) ? sourceEntities : [])
          .slice(0, 80)
          .map((entity) => {
            const row = toProfile(entity);
            const meta = toProfile(row.ai_metadata);
            return {
              id: normalizeProjectEntityId(row._id || row.id, 120),
              type: toTrimmedString(row.type, 24),
              name: toTrimmedString(row.name, 120),
              description: toTrimmedString(meta.description, 2400),
              is_me: row.is_me === true,
              is_mine: row.is_mine === true,
            };
          }),
        connections: (Array.isArray(contextData?.connections) ? contextData.connections : []).slice(0, 120),
        groups: (Array.isArray(contextData?.groups) ? contextData.groups : []).slice(0, 40),
      };
      const narrativeContext = buildProjectNarrativeContext({
        sourceEntities,
        connections: reducedContextData.connections,
      });

      return res.status(200).json(
        buildProjectContextBuildPreview({
          project,
          scopeContext,
          sourceHash,
          reducedContextData,
          narrativeContext,
          contextData,
          sourceEntities,
        }),
      );
    } catch (error) {
      return next(error);
    }
  });

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
      const sourceEntities = Array.isArray(scopeContext.sourceEntities) ? scopeContext.sourceEntities : scopeContext.entities;
      const llmContextResult = llmContextTools.buildAgentLlmContext({
        scopeContext,
        history: [],
        attachments: [],
        message: 'Собери контекст проекта по текущему dashboard snapshot.',
      });
      const contextData = llmContextResult.contextData;
      const reducedContextData = {
        ...toProfile(contextData),
        entities: (Array.isArray(sourceEntities) ? sourceEntities : [])
          .slice(0, 80)
          .map((entity) => {
            const row = toProfile(entity);
            const meta = toProfile(row.ai_metadata);
            return {
              id: normalizeProjectEntityId(row._id || row.id, 120),
              type: toTrimmedString(row.type, 24),
              name: toTrimmedString(row.name, 120),
              description: toTrimmedString(meta.description, 2400),
              is_me: row.is_me === true,
              is_mine: row.is_mine === true,
            };
          }),
        connections: (Array.isArray(contextData?.connections) ? contextData.connections : []).slice(0, 120),
        groups: (Array.isArray(contextData?.groups) ? contextData.groups : []).slice(0, 40),
      };
      const narrativeContext = buildProjectNarrativeContext({
        sourceEntities,
        connections: reducedContextData.connections,
      });

      const systemPrompt = aiPrompts.buildProjectContextBuildSystemPrompt();
      const userPayload = aiPrompts.buildProjectContextBuildPayload({
        contextData: reducedContextData,
        author: narrativeContext.author,
        narrativeRings: narrativeContext.narrativeRings,
        sourceHash,
      });
      const userPrompt = aiPrompts.buildProjectContextBuildUserPrompt({
        contextData: reducedContextData,
        author: narrativeContext.author,
        narrativeRings: narrativeContext.narrativeRings,
        sourceHash,
      });

      const buildModel = toTrimmedString(OPENAI_MODEL, 120) || toTrimmedString(OPENAI_PROJECT_MODEL, 120) || 'gpt-5';
      let payload = null;
      let buildAiResponse = null;
      try {
        buildAiResponse = await withAiTrace({
          label: 'project-context.build',
          ownerId,
          model: buildModel,
          projectId,
          promptLengths: { system: systemPrompt.length, user: userPrompt.length },
          includeDebug: true,
        }, () => aiProvider.requestOpenAiAgentReply({
          systemPrompt,
          userPrompt,
          includeRawPayload: true,
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

        const parsed = extractJsonObjectFromText(buildAiResponse.reply);
        if (!parsed || parsed.status !== 'ready') {
          throw Object.assign(new Error('Failed to parse project context build result'), { status: 502 });
        }
        payload = toProfile(parsed);
      } catch (error) {
        payload = buildProjectContextFallbackResult({
          scopeContext,
          sourceEntities,
          connections: contextData?.connections,
        });
        payload._fallbackError = toTrimmedString(error?.message, 240);
      }

      const fallbackAnalysisMap = buildProjectAnalysisMapFallback({
        scopeContext,
        sourceEntities,
        connections: contextData?.connections,
      });
      const normalizedAnalysisMap = normalizeProjectAnalysisMap(payload.analysisMap, fallbackAnalysisMap);
      const nextDescription =
        compileProjectDescriptionFromAnalysisMap(normalizedAnalysisMap)
        || normalizeProjectContextDescription(payload.description, payload.summary);
      const missing = normalizeProjectContextMissing(payload.missing);

      const freshProject = await Entity.findOne({ _id: projectId, owner_id: ownerId });
      if (!freshProject || freshProject.type !== 'project') {
        return res.status(404).json({ message: 'Project not found' });
      }

      const freshMeta = toProfile(freshProject.ai_metadata);
      const nextVersion = Math.max(0, Number(freshMeta.project_context_version) || 0) + 1;
      const buildLog = {
        exportedAt: new Date().toISOString(),
        source: 'project-context.build',
        llm_input: {
          model: buildModel,
          systemPrompt,
          userPayload,
        },
        llm_output: payload._fallbackError
          ? {
            mode: 'fallback',
            rawReply: '',
            parsedPayload: null,
            fallbackUsed: true,
            error: toTrimmedString(payload._fallbackError, 240),
          }
          : {
            mode: 'llm',
            rawReply: toTrimmedString(buildAiResponse?.reply, 20000),
            parsedPayload: payload,
            fallbackUsed: false,
            error: '',
          },
      };
      freshProject.ai_metadata = {
        ...freshMeta,
        description: nextDescription,
        project_context_compiled_description: nextDescription,
        project_analysis_map: normalizedAnalysisMap,
        project_context_last_build_log: buildLog,
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
