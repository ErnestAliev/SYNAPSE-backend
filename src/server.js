const express = require('express');
const cors = require('cors');
const dotenv = require('dotenv');
const cookieParser = require('cookie-parser');
const jwt = require('jsonwebtoken');
const crypto = require('crypto');
const { OAuth2Client } = require('google-auth-library');

const connectDB = require('./config/db');
const Entity = require('./models/Entity');
const User = require('./models/User');
const EntityVector = require('./models/EntityVector');

let whatsappWeb = null;
let QRCode = null;
let sharp = null;

try {
  whatsappWeb = require('whatsapp-web.js');
} catch {
  whatsappWeb = null;
}

try {
  QRCode = require('qrcode');
} catch {
  QRCode = null;
}

try {
  sharp = require('sharp');
} catch {
  sharp = null;
}

dotenv.config();

const app = express();
const PORT = Number(process.env.PORT) || 3001;
const LEGACY_SHAPE_NAME_PATTERN = /^Пуст(?:ой|ая|ые)(?:\s*-\s*(\d+))?$/i;
const SESSION_COOKIE_NAME = 'synapse12_session';
const SESSION_TTL_SECONDS = Number(process.env.SESSION_TTL_SECONDS) || 60 * 60 * 24 * 7;
const GOOGLE_CLIENT_ID = String(process.env.GOOGLE_CLIENT_ID || '').trim();
const SESSION_SECRET = String(process.env.SESSION_SECRET || process.env.GOOGLE_CLIENT_SECRET || '').trim();
const IS_PRODUCTION = process.env.NODE_ENV === 'production';
const AUTH_REQUIRED = String(process.env.AUTH_REQUIRED || 'true').toLowerCase() !== 'false';
const DEV_AUTH_ENABLED =
  !IS_PRODUCTION && String(process.env.DEV_AUTH_ENABLED || 'true').toLowerCase() !== 'false';
const DEFAULT_ALLOWED_ORIGINS = ['http://localhost:5173', 'http://localhost:3000'];
const OPENAI_API_KEY = String(process.env.OPENAI_API_KEY || '').trim();
const OPENAI_MODEL = String(process.env.OPENAI_MODEL || 'gpt-4.1-mini').trim();
const OPENAI_EMBEDDING_MODEL = String(process.env.OPENAI_EMBEDDING_MODEL || 'text-embedding-3-small').trim();
const AI_CONTEXT_ENTITY_LIMIT = Math.max(1, Number(process.env.AI_CONTEXT_ENTITY_LIMIT) || 120);
const AI_HISTORY_MESSAGE_LIMIT = Math.max(1, Number(process.env.AI_HISTORY_MESSAGE_LIMIT) || 12);
const AI_ATTACHMENT_LIMIT = Math.max(1, Number(process.env.AI_ATTACHMENT_LIMIT) || 6);
const AI_DEBUG_ECHO = String(process.env.AI_DEBUG_ECHO || '').toLowerCase() === 'true';
const WHATSAPP_CONTACT_IMPORT_LIMIT = Math.max(1, Number(process.env.WHATSAPP_CONTACT_IMPORT_LIMIT) || 2500);
const WHATSAPP_IMPORT_CONCURRENCY = Math.max(1, Number(process.env.WHATSAPP_IMPORT_CONCURRENCY) || 4);
const WHATSAPP_IMAGE_MAX_BYTES = Math.max(40_000, Number(process.env.WHATSAPP_IMAGE_MAX_BYTES) || 260_000);
const WHATSAPP_MEDIA_TIMEOUT_MS = Math.max(5_000, Number(process.env.WHATSAPP_MEDIA_TIMEOUT_MS) || 15_000);
const ENTITY_TYPES = new Set([
  'project',
  'connection',
  'person',
  'company',
  'event',
  'resource',
  'goal',
  'result',
  'task',
  'shape',
]);
const ENTITY_ANALYZER_FIELDS = Object.freeze({
  connection: ['tags', 'markers', 'roles', 'links', 'status', 'importance'],
  person: ['tags', 'markers', 'roles', 'skills', 'links', 'importance'],
  company: ['tags', 'industry', 'departments', 'stage', 'risks', 'links', 'importance'],
  event: ['tags', 'date', 'location', 'participants', 'outcomes', 'links', 'importance'],
  resource: ['tags', 'resources', 'status', 'owners', 'links', 'importance'],
  goal: ['tags', 'priority', 'metrics', 'owners', 'status', 'links', 'importance'],
  result: ['tags', 'outcomes', 'metrics', 'owners', 'links', 'importance'],
  task: ['tags', 'priority', 'status', 'owners', 'date', 'links', 'importance'],
  project: ['tags', 'stage', 'priority', 'risks', 'owners', 'links', 'importance'],
  shape: ['tags', 'markers', 'status', 'links', 'importance'],
});
const ENTITY_IMPORTANCE_VALUES = ['low', 'medium', 'high', 'critical'];
const ENTITY_VECTOR_WEIGHTS = Object.freeze({
  description: 0.45,
  roles: 0.15,
  skills: 0.15,
  tags: 0.1,
  markers: 0.05,
  links: 0.05,
  nameType: 0.05,
});
const whatsappSessionsByOwner = new Map();

function parseAllowedOrigins() {
  const raw = [
    process.env.FRONTEND_ORIGIN,
    process.env.FRONTEND_ORIGINS,
    process.env.CORS_ORIGIN,
    process.env.CORS_ORIGINS,
  ]
    .filter((value) => typeof value === 'string' && value.trim().length > 0)
    .join(',');

  const normalized = raw
    .split(',')
    .map((origin) => origin.trim())
    .filter(Boolean);

  if (normalized.length) {
    return new Set(normalized);
  }

  return new Set(DEFAULT_ALLOWED_ORIGINS);
}

const allowedOrigins = parseAllowedOrigins();
const googleClient = GOOGLE_CLIENT_ID ? new OAuth2Client(GOOGLE_CLIENT_ID) : null;

app.use(
  cors({
    origin(origin, callback) {
      if (!origin) {
        callback(null, true);
        return;
      }

      if (!allowedOrigins.size || allowedOrigins.has(origin)) {
        callback(null, true);
        return;
      }

      callback(Object.assign(new Error(`CORS blocked for origin: ${origin}`), { status: 403 }));
    },
    credentials: true,
  }),
);
app.use(cookieParser());
app.use(express.json({ limit: '10mb' }));

function normalizeShapeName(name) {
  if (typeof name !== 'string') return name;

  const trimmed = name.trim();
  const match = trimmed.match(LEGACY_SHAPE_NAME_PATTERN);
  if (!match) return trimmed;

  const serial = match[1];
  return serial ? `Элемент - ${serial}` : 'Элемент';
}

function normalizeProjectCanvasData(canvasData) {
  const raw = canvasData && typeof canvasData === 'object' ? canvasData : {};
  const rawNodes = Array.isArray(raw.nodes) ? raw.nodes : [];
  const rawEdges = Array.isArray(raw.edges) ? raw.edges : [];
  const rawViewport = raw.viewport && typeof raw.viewport === 'object' ? raw.viewport : null;
  const rawBackground = typeof raw.background === 'string' ? raw.background.trim() : '';

  const nodes = rawNodes.flatMap((node) => {
    if (!node || typeof node !== 'object') return [];

    const id = typeof node.id === 'string' ? node.id : '';
    const entityId = typeof node.entityId === 'string' ? node.entityId : '';
    const x = typeof node.x === 'number' && Number.isFinite(node.x) ? node.x : null;
    const y = typeof node.y === 'number' && Number.isFinite(node.y) ? node.y : null;
    const scaleRaw = typeof node.scale === 'number' && Number.isFinite(node.scale) ? node.scale : undefined;
    const scale = typeof scaleRaw === 'number' ? Math.min(1.2, Math.max(0.8, scaleRaw)) : undefined;

    if (!id || !entityId || x === null || y === null) {
      return [];
    }

    return [{ id, entityId, x, y, ...(typeof scale === 'number' ? { scale } : {}) }];
  });

  const edges = rawEdges.flatMap((edge) => {
    if (!edge || typeof edge !== 'object') return [];

    const id = typeof edge.id === 'string' ? edge.id : '';
    const source = typeof edge.source === 'string' ? edge.source : '';
    const target = typeof edge.target === 'string' ? edge.target : '';
    const label = typeof edge.label === 'string' ? edge.label : undefined;
    const color = typeof edge.color === 'string' && edge.color.trim() ? edge.color : undefined;
    const arrowLeft = typeof edge.arrowLeft === 'boolean' ? edge.arrowLeft : undefined;
    const arrowRight = typeof edge.arrowRight === 'boolean' ? edge.arrowRight : undefined;

    if (!id || !source || !target) {
      return [];
    }

    return [{ id, source, target, label, color, arrowLeft, arrowRight }];
  });

  const viewport =
    rawViewport &&
    typeof rawViewport.x === 'number' &&
    Number.isFinite(rawViewport.x) &&
    typeof rawViewport.y === 'number' &&
    Number.isFinite(rawViewport.y) &&
    typeof rawViewport.zoom === 'number' &&
    Number.isFinite(rawViewport.zoom) &&
    rawViewport.zoom > 0 &&
    typeof rawViewport.width === 'number' &&
    Number.isFinite(rawViewport.width) &&
    rawViewport.width > 0 &&
    typeof rawViewport.height === 'number' &&
    Number.isFinite(rawViewport.height) &&
    rawViewport.height > 0
      ? {
          x: rawViewport.x,
          y: rawViewport.y,
          zoom: rawViewport.zoom,
          width: rawViewport.width,
          height: rawViewport.height,
        }
      : undefined;

  return {
    nodes,
    edges,
    ...(viewport ? { viewport } : {}),
    ...(rawBackground ? { background: rawBackground } : {}),
  };
}

function toProfile(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    return {};
  }
  return value;
}

function toTrimmedString(value, maxLength = 240) {
  if (typeof value !== 'string') return '';
  const trimmed = value.trim();
  if (!trimmed) return '';
  return trimmed.slice(0, maxLength);
}

function toStringArray(value, maxItems = 8, itemMaxLength = 80) {
  if (!Array.isArray(value)) return [];
  return value
    .map((item) => toTrimmedString(item, itemMaxLength))
    .filter(Boolean)
    .slice(0, maxItems);
}

function compactObject(value) {
  if (Array.isArray(value)) {
    const nextArray = value
      .map((item) => compactObject(item))
      .filter((item) => item !== undefined);
    return nextArray.length ? nextArray : undefined;
  }

  if (value && typeof value === 'object') {
    const nextObject = Object.entries(value).reduce((acc, [key, item]) => {
      const compacted = compactObject(item);
      if (compacted === undefined) return acc;
      acc[key] = compacted;
      return acc;
    }, {});
    return Object.keys(nextObject).length ? nextObject : undefined;
  }

  if (typeof value === 'string') {
    return value.trim() ? value : undefined;
  }

  if (value === null || value === undefined) {
    return undefined;
  }

  return value;
}

function getEntityAnalyzerFields(entityType) {
  const fields = ENTITY_ANALYZER_FIELDS[entityType];
  if (!Array.isArray(fields)) return ENTITY_ANALYZER_FIELDS.shape;
  return fields;
}

function normalizeEntityFieldArray(value, options = {}) {
  const maxItems = Number.isFinite(options.maxItems) ? Math.max(1, Math.floor(options.maxItems)) : 12;
  const itemMaxLength = Number.isFinite(options.itemMaxLength)
    ? Math.max(1, Math.floor(options.itemMaxLength))
    : 64;

  if (!Array.isArray(value)) {
    if (typeof value === 'string') {
      const trimmed = toTrimmedString(value, itemMaxLength);
      return trimmed ? [trimmed] : [];
    }
    return [];
  }

  const dedup = new Set();
  const result = [];
  for (const item of value) {
    const trimmed = toTrimmedString(item, itemMaxLength);
    if (!trimmed) continue;
    const key = trimmed.toLowerCase();
    if (dedup.has(key)) continue;
    dedup.add(key);
    result.push(trimmed);
    if (result.length >= maxItems) break;
  }

  return result;
}

function normalizeImportanceValue(value) {
  const direct = toTrimmedString(value, 24).toLowerCase();
  if (ENTITY_IMPORTANCE_VALUES.includes(direct)) return direct;

  const map = {
    низкая: 'low',
    low: 'low',
    medium: 'medium',
    med: 'medium',
    средняя: 'medium',
    высокая: 'high',
    high: 'high',
    критично: 'critical',
    критическая: 'critical',
    критическаяя: 'critical',
    critical: 'critical',
  };

  return map[direct] || '';
}

function normalizeEntityAnalysisFields(entityType, rawFields) {
  const source = toProfile(rawFields);
  const allowed = new Set(getEntityAnalyzerFields(entityType));
  const normalized = {};

  for (const field of allowed) {
    if (field === 'importance') {
      const direct = normalizeImportanceValue(source.importance);
      const fromArray = normalizeEntityFieldArray(source.importance, { maxItems: 1, itemMaxLength: 24 })
        .map((item) => normalizeImportanceValue(item))
        .find(Boolean);
      const importance = direct || fromArray || '';
      normalized.importance = importance ? [importance] : [];
      continue;
    }

    if (field === 'links') {
      const rawLinks = Array.isArray(source.links) ? source.links : [];
      const links = rawLinks
        .map((item) => {
          if (typeof item === 'string') return toTrimmedString(item, 240);
          const row = toProfile(item);
          return toTrimmedString(row.url || row.link || row.href || row.value, 240);
        })
        .filter(Boolean)
        .slice(0, 12);
      normalized.links = Array.from(new Set(links));
      continue;
    }

    normalized[field] = normalizeEntityFieldArray(source[field], { maxItems: 12, itemMaxLength: 64 });
  }

  return normalized;
}

function normalizeConfidence(rawConfidence) {
  const source = toProfile(rawConfidence);
  const confidence = {};
  const keys = ['description', ...ENTITY_IMPORTANCE_VALUES, ...Object.values(ENTITY_ANALYZER_FIELDS).flat()];
  for (const key of keys) {
    const value = source[key];
    if (typeof value !== 'number' || !Number.isFinite(value)) continue;
    confidence[key] = Math.min(1, Math.max(0, value));
  }
  return confidence;
}

function extractJsonObjectFromText(text) {
  const trimmed = toTrimmedString(text, 80_000);
  if (!trimmed) {
    throw Object.assign(new Error('AI response is empty'), { status: 502 });
  }

  try {
    return JSON.parse(trimmed);
  } catch {
    // continue
  }

  const fencedMatch = trimmed.match(/```(?:json)?\s*([\s\S]*?)\s*```/i);
  if (fencedMatch?.[1]) {
    try {
      return JSON.parse(fencedMatch[1].trim());
    } catch {
      // continue
    }
  }

  const firstBrace = trimmed.indexOf('{');
  const lastBrace = trimmed.lastIndexOf('}');
  if (firstBrace >= 0 && lastBrace > firstBrace) {
    const candidate = trimmed.slice(firstBrace, lastBrace + 1);
    try {
      return JSON.parse(candidate);
    } catch {
      // continue
    }
  }

  throw Object.assign(new Error('AI response is not valid JSON'), { status: 502 });
}

function normalizeEntityAnalysisOutput(entityType, rawResponse) {
  const parsed = toProfile(rawResponse);
  const status = toTrimmedString(parsed.status, 32) === 'need_clarification' ? 'need_clarification' : 'ready';
  const description = toTrimmedString(parsed.description, 2200);
  const fields = normalizeEntityAnalysisFields(entityType, parsed.fields);
  const clarifyingQuestions = normalizeEntityFieldArray(parsed.clarifyingQuestions, {
    maxItems: 3,
    itemMaxLength: 220,
  });
  const ignoredNoise = normalizeEntityFieldArray(parsed.ignoredNoise, {
    maxItems: 20,
    itemMaxLength: 120,
  });
  const confidence = normalizeConfidence(parsed.confidence);

  return {
    status,
    description,
    fields,
    clarifyingQuestions,
    ignoredNoise,
    confidence,
  };
}

function buildEntityAnalyzerCurrentFields(entityType, aiMetadata) {
  const allowed = getEntityAnalyzerFields(entityType);
  const current = {};
  for (const field of allowed) {
    if (field === 'importance') {
      current.importance = normalizeEntityAnalysisFields(entityType, { importance: aiMetadata.importance }).importance;
      continue;
    }
    current[field] = normalizeEntityFieldArray(aiMetadata[field], {
      maxItems: field === 'links' ? 12 : 8,
      itemMaxLength: field === 'links' ? 240 : 64,
    });
  }

  return current;
}

function buildEntityAnalyzerSystemPrompt(entityType) {
  const allowedFields = getEntityAnalyzerFields(entityType);

  return [
    'Ты Synapse12 Entity Analyst.',
    `Текущий тип сущности: ${entityType}.`,
    'Работай только на данных из входного JSON.',
    'Твоя задача: интерпретировать сырые пользовательские данные и вернуть структурированный JSON.',
    'Нельзя превращать весь текст в теги. Добавляй только осмысленные признаки.',
    `Разрешенные поля для fields: ${allowedFields.join(', ')}.`,
    'importance: только одно из [low, medium, high, critical], вернуть как массив из 0..1 элементов.',
    'links: только валидные URL.',
    'description: 3-6 предложений, емко и без воды.',
    'Если данных мало, status=need_clarification и до 3 уточняющих вопросов.',
    'Если данных хватает, status=ready.',
    'Верни СТРОГО JSON без markdown.',
    'Формат:',
    '{',
    '  "status": "ready | need_clarification",',
    '  "description": "string",',
    '  "fields": { "tags": [], "roles": [], ... },',
    '  "clarifyingQuestions": [],',
    '  "confidence": {},',
    '  "ignoredNoise": []',
    '}',
  ].join('\n');
}

function buildEntityAnalyzerUserPrompt({
  entity,
  message,
  history,
  attachments,
  currentFields,
  voiceInput,
  documents,
}) {
  const contextPayload = {
    entity: {
      id: String(entity._id),
      type: entity.type,
      name: toTrimmedString(entity.name, 120),
    },
    currentFields,
    message,
    voiceInput,
    history,
    attachments,
    documents,
  };

  return ['Контекст сущности (JSON):', JSON.stringify(contextPayload, null, 2)].join('\n');
}

function buildEntityAnalysisReplyText(analysis) {
  if (analysis.status === 'need_clarification') {
    if (analysis.clarifyingQuestions.length) {
      return ['Нужны уточнения перед заполнением профиля:', ...analysis.clarifyingQuestions.map((q) => `- ${q}`)].join(
        '\n',
      );
    }
    return 'Нужны уточнения перед заполнением профиля.';
  }

  if (analysis.description) {
    return `Готово. Обновил описание и поля.\n\n${analysis.description}`;
  }

  return 'Готово. Поля профиля обновлены.';
}

function buildEntityVectorContent(entity, analysis) {
  const fields = analysis.fields || {};
  const asText = (value) => (Array.isArray(value) ? value.filter(Boolean).join(', ') : '');
  const chunks = [
    toTrimmedString(entity.name, 160),
    entity.type,
    toTrimmedString(analysis.description, 4000),
    asText(fields.roles),
    asText(fields.skills),
    asText(fields.tags),
    asText(fields.markers),
    asText(fields.links),
  ].filter(Boolean);
  return chunks.join('\n');
}

async function requestOpenAiEmbedding(text) {
  if (!OPENAI_API_KEY) {
    throw Object.assign(new Error('OPENAI_API_KEY is not configured'), { status: 503 });
  }

  const input = toTrimmedString(text, 12_000);
  if (!input) return null;

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 30_000);
  let response;
  let payload;

  try {
    response = await fetch('https://api.openai.com/v1/embeddings', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${OPENAI_API_KEY}`,
      },
      body: JSON.stringify({
        model: OPENAI_EMBEDDING_MODEL,
        input,
      }),
      signal: controller.signal,
    });
    payload = await response.json();
  } catch (error) {
    if (error && error.name === 'AbortError') {
      throw Object.assign(new Error('Embedding request timeout'), { status: 504 });
    }
    throw Object.assign(new Error('Failed to call embedding provider'), { status: 502 });
  } finally {
    clearTimeout(timeout);
  }

  if (!response.ok) {
    const providerMessage =
      toTrimmedString(payload?.error?.message, 300) || 'Embedding provider error';
    throw Object.assign(new Error(providerMessage), { status: 502 });
  }

  const vector = Array.isArray(payload?.data?.[0]?.embedding) ? payload.data[0].embedding : [];
  if (!vector.length) {
    throw Object.assign(new Error('Embedding response is empty'), { status: 502 });
  }

  return vector;
}

async function upsertEntityVector(ownerId, entity, analysis) {
  const content = buildEntityVectorContent(entity, analysis);
  if (!content) {
    return null;
  }

  const vector = await requestOpenAiEmbedding(content);
  if (!vector) {
    return null;
  }

  const saved = await EntityVector.findOneAndUpdate(
    {
      owner_id: ownerId,
      entity_id: String(entity._id),
    },
    {
      $set: {
        owner_id: ownerId,
        entity_id: String(entity._id),
        entity_type: entity.type,
        model: OPENAI_EMBEDDING_MODEL,
        vector,
        weights: ENTITY_VECTOR_WEIGHTS,
        content: {
          description: analysis.description,
          fields: analysis.fields,
        },
      },
    },
    {
      upsert: true,
      new: true,
      setDefaultsOnInsert: true,
    },
  ).lean();

  return saved;
}

function buildEntityMetadataPatch(entityType, existingMetadata, analysis) {
  const nextMetadata = {
    ...toProfile(existingMetadata),
  };

  if (typeof analysis.description === 'string') {
    nextMetadata.description = analysis.description;
  }

  const allowedFields = getEntityAnalyzerFields(entityType);
  const normalizedFields = normalizeEntityAnalysisFields(entityType, analysis.fields);
  for (const field of allowedFields) {
    nextMetadata[field] = normalizedFields[field] || [];
  }

  nextMetadata.ai_last_analysis = {
    status: analysis.status,
    confidence: toProfile(analysis.confidence),
    clarifyingQuestions: normalizeEntityFieldArray(analysis.clarifyingQuestions, {
      maxItems: 3,
      itemMaxLength: 220,
    }),
    ignoredNoise: normalizeEntityFieldArray(analysis.ignoredNoise, {
      maxItems: 20,
      itemMaxLength: 120,
    }),
    updatedAt: new Date().toISOString(),
  };

  return nextMetadata;
}

function normalizeAgentHistory(rawHistory) {
  if (!Array.isArray(rawHistory)) return [];
  return rawHistory
    .map((item) => {
      const row = toProfile(item);
      const role = row.role === 'assistant' ? 'assistant' : row.role === 'user' ? 'user' : '';
      const text = toTrimmedString(row.text, 1800);
      if (!role || !text) return null;
      return { role, text };
    })
    .filter(Boolean)
    .slice(-AI_HISTORY_MESSAGE_LIMIT);
}

function normalizeAgentAttachments(rawAttachments) {
  if (!Array.isArray(rawAttachments)) return [];
  return rawAttachments
    .map((item) => {
      const attachment = toProfile(item);
      const name = toTrimmedString(attachment.name, 120);
      if (!name) return null;
      const mime = toTrimmedString(attachment.mime, 120);
      const size =
        typeof attachment.size === 'number' && Number.isFinite(attachment.size)
          ? Math.max(0, Math.floor(attachment.size))
          : 0;
      return { name, mime, size };
    })
    .filter(Boolean)
    .slice(0, AI_ATTACHMENT_LIMIT);
}

function summarizeEntityForAgent(entity) {
  const profile = toProfile(entity.profile);
  const aiMetadata = toProfile(entity.ai_metadata);
  const logo = toProfile(profile.logo);

  return compactObject({
    id: String(entity._id),
    type: entity.type,
    name: toTrimmedString(entity.name, 120) || '(без названия)',
    description: toTrimmedString(aiMetadata.description, 260),
    tags: toStringArray(aiMetadata.tags, 8),
    markers: toStringArray(aiMetadata.markers, 6),
    skills: toStringArray(aiMetadata.skills, 8),
    roles: toStringArray(aiMetadata.roles, 8),
    importance: toStringArray(aiMetadata.importance, 4),
    status: toStringArray(aiMetadata.status, 4),
    stage: toStringArray(aiMetadata.stage, 4),
    owners: toStringArray(aiMetadata.owners, 6),
    industry: toStringArray(aiMetadata.industry, 5),
    location: toStringArray(aiMetadata.location, 5),
    date: toStringArray(aiMetadata.date, 5),
    links: toStringArray(aiMetadata.links, 6, 140),
    visual: {
      color: toTrimmedString(profile.color, 24),
      emoji: toTrimmedString(profile.emoji, 8),
      logo: toTrimmedString(logo.name || logo.id, 64),
      hasImage: typeof profile.image === 'string' && profile.image.trim().length > 0,
    },
  });
}

function extractOpenAiResponseText(payload) {
  if (payload && typeof payload.output_text === 'string' && payload.output_text.trim()) {
    return payload.output_text.trim();
  }

  const chunks = [];
  const outputs = Array.isArray(payload?.output) ? payload.output : [];
  for (const item of outputs) {
    const contentItems = Array.isArray(item?.content) ? item.content : [];
    for (const content of contentItems) {
      if (typeof content?.text === 'string' && content.text.trim()) {
        chunks.push(content.text.trim());
      }
    }
  }

  if (!chunks.length) return '';
  return chunks.join('\n').trim();
}

function buildProjectConnections(canvasData, entitiesById) {
  const nodeEntityByNodeId = new Map();
  for (const node of canvasData.nodes) {
    if (!node.id || !node.entityId) continue;
    nodeEntityByNodeId.set(node.id, node.entityId);
  }

  const entityNameById = new Map();
  for (const entity of entitiesById) {
    entityNameById.set(String(entity._id), toTrimmedString(entity.name, 120) || '(без названия)');
  }

  return canvasData.edges
    .map((edge) => {
      const sourceEntityId = nodeEntityByNodeId.get(edge.source);
      const targetEntityId = nodeEntityByNodeId.get(edge.target);
      if (!sourceEntityId || !targetEntityId) return null;

      return compactObject({
        from: entityNameById.get(sourceEntityId) || sourceEntityId,
        to: entityNameById.get(targetEntityId) || targetEntityId,
        label: toTrimmedString(edge.label, 80),
        color: toTrimmedString(edge.color, 32),
        arrows: {
          left: Boolean(edge.arrowLeft),
          right: Boolean(edge.arrowRight),
        },
      });
    })
    .filter(Boolean)
    .slice(0, 180);
}

async function resolveAgentScopeContext(ownerId, rawScope) {
  const scope = toProfile(rawScope);
  const scopeType = toTrimmedString(scope.type, 24).toLowerCase();

  if (scopeType === 'collection') {
    const entityType = toTrimmedString(scope.entityType, 24);
    if (!ENTITY_TYPES.has(entityType)) {
      throw Object.assign(new Error('Invalid collection scope type'), { status: 400 });
    }

    const [entities, totalEntities] = await Promise.all([
      Entity.find({ owner_id: ownerId, type: entityType })
        .sort({ updatedAt: -1, _id: -1 })
        .limit(AI_CONTEXT_ENTITY_LIMIT)
        .lean(),
      Entity.countDocuments({ owner_id: ownerId, type: entityType }),
    ]);

    return {
      scopeType: 'collection',
      entityType,
      scopeName: entityType,
      projectId: '',
      projectName: '',
      totalEntities,
      entities,
      connections: [],
    };
  }

  if (scopeType === 'project') {
    const projectId = toTrimmedString(scope.projectId, 80);
    if (!projectId) {
      throw Object.assign(new Error('projectId is required for project scope'), { status: 400 });
    }

    const project = await Entity.findOne({
      _id: projectId,
      owner_id: ownerId,
      type: 'project',
    })
      .select({ _id: 1, name: 1, canvas_data: 1 })
      .lean();

    if (!project) {
      throw Object.assign(new Error('Project not found'), { status: 404 });
    }

    const canvasData = normalizeProjectCanvasData(project.canvas_data);
    const uniqueEntityIds = Array.from(
      new Set(
        canvasData.nodes
          .map((node) => toTrimmedString(node.entityId, 80))
          .filter(Boolean),
      ),
    );

    const limitedEntityIds = uniqueEntityIds.slice(0, AI_CONTEXT_ENTITY_LIMIT);
    const entities = limitedEntityIds.length
      ? await Entity.find({
          owner_id: ownerId,
          _id: { $in: limitedEntityIds },
        }).lean()
      : [];

    const entityById = new Map(entities.map((entity) => [String(entity._id), entity]));
    const orderedEntities = limitedEntityIds
      .map((id) => entityById.get(id))
      .filter(Boolean);

    const connections = buildProjectConnections(canvasData, orderedEntities);

    return {
      scopeType: 'project',
      entityType: '',
      scopeName: toTrimmedString(project.name, 140) || 'Без названия',
      projectId: String(project._id),
      projectName: toTrimmedString(project.name, 140) || 'Без названия',
      totalEntities: uniqueEntityIds.length,
      entities: orderedEntities,
      connections,
    };
  }

  throw Object.assign(new Error('Invalid scope type'), { status: 400 });
}

function buildAgentSystemPrompt(scopeContext) {
  const scopeDescription =
    scopeContext.scopeType === 'project'
      ? `Текущий контекст: проект "${scopeContext.projectName}" (${scopeContext.totalEntities} сущностей).`
      : `Текущий контекст: вкладка "${scopeContext.entityType}" (${scopeContext.totalEntities} сущностей).`;

  return [
    'Ты LLM-аналитик системы Synapse12.',
    scopeDescription,
    'Жесткое правило: используй ТОЛЬКО данные из переданного контекста.',
    'Нельзя подтягивать данные из других вкладок, проектов или внешних источников.',
    'Если данных в контексте недостаточно, прямо напиши: "Недостаточно данных в текущем контексте".',
    'Отвечай по-русски, структурно и кратко.',
    'Формат ответа:',
    '1) Краткий вывод',
    '2) Наблюдения',
    '3) Возможности и риски',
    '4) Следующие шаги',
  ].join('\n');
}

function buildAgentUserPrompt({ scopeContext, message, history, attachments }) {
  const contextPayload = {
    scope: compactObject({
      type: scopeContext.scopeType,
      name: scopeContext.scopeName,
      entityType: scopeContext.entityType,
      projectId: scopeContext.projectId,
      projectName: scopeContext.projectName,
      totalEntities: scopeContext.totalEntities,
      contextLimit: AI_CONTEXT_ENTITY_LIMIT,
    }),
    entities: scopeContext.entities.map((entity) => summarizeEntityForAgent(entity)),
    connections: scopeContext.connections,
    attachments,
    history,
  };

  return [
    'Контекст Synapse12 (JSON):',
    JSON.stringify(contextPayload, null, 2),
    '',
    'Текущий запрос пользователя:',
    message,
  ].join('\n');
}

async function requestOpenAiAgentReply({ systemPrompt, userPrompt }) {
  if (!OPENAI_API_KEY) {
    throw Object.assign(new Error('OPENAI_API_KEY is not configured'), { status: 503 });
  }

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 45_000);

  let response;
  let payload;
  try {
    response = await fetch('https://api.openai.com/v1/responses', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${OPENAI_API_KEY}`,
      },
      body: JSON.stringify({
        model: OPENAI_MODEL,
        input: [
          {
            role: 'system',
            content: [{ type: 'input_text', text: systemPrompt }],
          },
          {
            role: 'user',
            content: [{ type: 'input_text', text: userPrompt }],
          },
        ],
        temperature: 0.25,
        max_output_tokens: 900,
      }),
      signal: controller.signal,
    });

    payload = await response.json();
  } catch (error) {
    if (error && error.name === 'AbortError') {
      throw Object.assign(new Error('AI request timeout'), { status: 504 });
    }
    throw Object.assign(new Error('Failed to call AI provider'), { status: 502 });
  } finally {
    clearTimeout(timeout);
  }

  if (!response.ok) {
    const providerMessage = toTrimmedString(payload?.error?.message, 300) || 'AI provider error';
    throw Object.assign(new Error(providerMessage), { status: 502 });
  }

  const reply = extractOpenAiResponseText(payload);
  if (!reply) {
    throw Object.assign(new Error('AI response is empty'), { status: 502 });
  }

  return {
    reply,
    usage: payload?.usage || null,
  };
}

function getSessionCookieOptions() {
  return {
    httpOnly: true,
    secure: IS_PRODUCTION,
    sameSite: IS_PRODUCTION ? 'none' : 'lax',
    maxAge: SESSION_TTL_SECONDS * 1000,
    path: '/',
  };
}

function toPublicUser(user) {
  const id =
    (user && (user._id || user.id || user.uid || user.sub) && String(user._id || user.id || user.uid || user.sub)) ||
    '';
  const email = (user && typeof user.email === 'string' && user.email.trim()) || '';
  const name = (user && typeof user.name === 'string' && user.name.trim()) || email;
  const picture = (user && typeof user.picture === 'string' && user.picture.trim()) || '';
  const givenName = (user && typeof user.givenName === 'string' && user.givenName.trim()) || '';
  const familyName = (user && typeof user.familyName === 'string' && user.familyName.trim()) || '';
  const provider = (user && typeof user.provider === 'string' && user.provider.trim()) || '';
  const settings =
    user && user.settings && typeof user.settings === 'object' && !Array.isArray(user.settings)
      ? user.settings
      : {};

  return {
    id,
    email,
    name,
    picture,
    givenName,
    familyName,
    provider,
    settings,
  };
}

function normalizeSettingsUpdate(rawSettings) {
  if (!rawSettings || typeof rawSettings !== 'object' || Array.isArray(rawSettings)) {
    return {};
  }

  return Object.entries(rawSettings).reduce((acc, [key, value]) => {
    const normalizedKey = String(key || '').trim();
    if (!normalizedKey) return acc;
    if (value === undefined) return acc;
    acc[normalizedKey] = value;
    return acc;
  }, {});
}

function getSessionTokenFromRequest(req) {
  const authHeader = req.headers.authorization;
  if (typeof authHeader === 'string' && authHeader.startsWith('Bearer ')) {
    const token = authHeader.slice('Bearer '.length).trim();
    if (token) return token;
  }

  const cookieToken = req.cookies?.[SESSION_COOKIE_NAME];
  if (typeof cookieToken === 'string' && cookieToken.trim()) {
    return cookieToken.trim();
  }

  return '';
}

function createSessionToken(user) {
  if (!SESSION_SECRET) {
    throw Object.assign(new Error('SESSION_SECRET is not configured'), { status: 503 });
  }

  const userId = String(user?._id || user?.id || user?.uid || '').trim();
  if (!userId) {
    throw Object.assign(new Error('User id is missing for session token'), { status: 500 });
  }

  return jwt.sign(
    {
      sub: userId,
      uid: userId,
      email: user.email,
      name: user.name,
      picture: user.picture || '',
      givenName: user.givenName || '',
      familyName: user.familyName || '',
    },
    SESSION_SECRET,
    {
      algorithm: 'HS256',
      expiresIn: SESSION_TTL_SECONDS,
    },
  );
}

function verifySessionToken(sessionToken) {
  if (!SESSION_SECRET) {
    throw Object.assign(new Error('SESSION_SECRET is not configured'), { status: 503 });
  }

  return jwt.verify(sessionToken, SESSION_SECRET, {
    algorithms: ['HS256'],
  });
}

function setSessionCookie(res, sessionToken) {
  res.cookie(SESSION_COOKIE_NAME, sessionToken, getSessionCookieOptions());
}

function clearSessionCookie(res) {
  res.clearCookie(SESSION_COOKIE_NAME, {
    ...getSessionCookieOptions(),
    maxAge: undefined,
  });
}

async function verifyGoogleCredential(credential) {
  if (!GOOGLE_CLIENT_ID || !googleClient) {
    throw Object.assign(new Error('Google OAuth is not configured'), { status: 503 });
  }

  let ticket;
  try {
    ticket = await googleClient.verifyIdToken({
      idToken: credential,
      audience: GOOGLE_CLIENT_ID,
    });
  } catch (error) {
    throw Object.assign(
      new Error('Invalid Google credential. Check OAuth origin and client configuration.'),
      {
        status: 401,
      },
    );
  }

  const payload = ticket.getPayload();
  if (!payload || !payload.sub) {
    throw Object.assign(new Error('Invalid Google token payload'), { status: 401 });
  }

  if (!payload.email || payload.email_verified !== true) {
    throw Object.assign(new Error('Google account email is not verified'), { status: 401 });
  }

  return {
    provider: 'google',
    providerId: payload.sub,
    email: payload.email,
    name: payload.name || payload.email,
    picture: payload.picture || '',
    givenName: payload.given_name || '',
    familyName: payload.family_name || '',
  };
}

async function upsertUserFromIdentity(identity) {
  const provider = String(identity?.provider || '').trim();
  const providerId = String(identity?.providerId || '').trim();
  const email = String(identity?.email || '')
    .trim()
    .toLowerCase();
  const name = String(identity?.name || email).trim() || email;

  if (!provider || !providerId || !email) {
    throw Object.assign(new Error('Invalid user identity payload'), { status: 400 });
  }

  const update = {
    provider,
    providerId,
    email,
    name,
    picture: String(identity?.picture || '').trim(),
    givenName: String(identity?.givenName || '').trim(),
    familyName: String(identity?.familyName || '').trim(),
    lastLoginAt: new Date(),
  };

  const user = await User.findOneAndUpdate(
    { provider, providerId },
    {
      $set: update,
      $setOnInsert: {
        settings: {
          locale: 'ru',
          onboardingCompleted: false,
        },
      },
    },
    {
      upsert: true,
      new: true,
      setDefaultsOnInsert: true,
    },
  );

  return user;
}

async function resolveAuthUserFromSessionToken(sessionToken) {
  const sessionPayload = verifySessionToken(sessionToken);
  const userId = String(sessionPayload?.uid || sessionPayload?.sub || '').trim();
  if (!userId) {
    throw Object.assign(new Error('Unauthorized'), { status: 401 });
  }

  const user = await User.findById(userId).lean();
  if (!user) {
    throw Object.assign(new Error('Unauthorized'), { status: 401 });
  }

  return user;
}

async function requireAuth(req, res, next) {
  try {
    const sessionToken = getSessionTokenFromRequest(req);
    if (!sessionToken) {
      return res.status(401).json({ message: 'Unauthorized' });
    }

    const user = await resolveAuthUserFromSessionToken(sessionToken);
    req.authUser = toPublicUser(user);
    return next();
  } catch (error) {
    return res.status(401).json({ message: 'Unauthorized' });
  }
}

function getOwnerIdFromRequest(req) {
  return String(req?.authUser?.id || '').trim();
}

function requireOwnerId(req) {
  const ownerId = getOwnerIdFromRequest(req);
  if (!ownerId) {
    throw Object.assign(new Error('Unauthorized'), { status: 401 });
  }
  return ownerId;
}

async function removeEntityFromProjectCanvases(entityId, ownerId) {
  return removeEntitiesFromProjectCanvases([entityId], ownerId);
}

async function removeEntitiesFromProjectCanvases(entityIds, ownerId) {
  const normalizedOwnerId = String(ownerId || '').trim();
  if (!normalizedOwnerId) {
    throw Object.assign(new Error('Unauthorized'), { status: 401 });
  }

  const normalizedIds = Array.from(
    new Set(
      (Array.isArray(entityIds) ? entityIds : [entityIds])
        .map((id) => String(id || '').trim())
        .filter(Boolean),
    ),
  );

  if (!normalizedIds.length) {
    return;
  }

  const projects = await Entity.find(
    {
      type: 'project',
      owner_id: normalizedOwnerId,
      'canvas_data.nodes.entityId': { $in: normalizedIds },
    },
    { _id: 1, canvas_data: 1 },
  ).lean();

  if (!projects.length) {
    return;
  }

  const operations = [];

  for (const project of projects) {
    const canvasData = normalizeProjectCanvasData(project.canvas_data);
    const removeSet = new Set(normalizedIds);
    const removedNodeIds = new Set(
      canvasData.nodes
        .filter((node) => removeSet.has(node.entityId))
        .map((node) => node.id),
    );

    if (!removedNodeIds.size) {
      continue;
    }

    const nextNodes = canvasData.nodes.filter((node) => !removeSet.has(node.entityId));
    const nextEdges = canvasData.edges.filter(
      (edge) => !removedNodeIds.has(edge.source) && !removedNodeIds.has(edge.target),
    );

    operations.push({
      updateOne: {
        filter: { _id: project._id, owner_id: normalizedOwnerId },
        update: {
          $set: {
            canvas_data: {
              nodes: nextNodes,
              edges: nextEdges,
              ...(canvasData.viewport ? { viewport: canvasData.viewport } : {}),
              ...(canvasData.background ? { background: canvasData.background } : {}),
            },
          },
        },
      },
    });
  }

  if (!operations.length) {
    return;
  }

  await Entity.bulkWrite(operations, { ordered: false });
}

function normalizePhone(value) {
  if (typeof value !== 'string') return '';
  const trimmed = value.trim();
  if (!trimmed) return '';

  const hasPlus = trimmed.startsWith('+');
  const digits = trimmed.replace(/[^\d]/g, '');
  if (!digits) return '';
  return hasPlus ? `+${digits}` : digits;
}

function sanitizeOwnerSessionKey(ownerId) {
  return toTrimmedString(String(ownerId || '').replace(/[^a-zA-Z0-9_-]/g, '_'), 64) || 'owner';
}

function createWhatsappSessionId() {
  if (typeof crypto.randomUUID === 'function') {
    return crypto.randomUUID();
  }
  return `wa-${Date.now()}-${Math.floor(Math.random() * 100_000)}`;
}

function isWhatsappIntegrationAvailable() {
  return Boolean(whatsappWeb && QRCode);
}

function toWhatsappSessionStatus(session) {
  return {
    sessionId: session.id,
    status: session.status,
    qrCodeDataUrl: session.qrCodeDataUrl || '',
    error: session.error || '',
    createdAt: session.createdAt,
    updatedAt: session.updatedAt,
    lastImportedAt: session.lastImportedAt || '',
  };
}

function touchWhatsappSession(session) {
  session.updatedAt = new Date().toISOString();
}

function getOwnerWhatsappSession(ownerId) {
  return whatsappSessionsByOwner.get(ownerId) || null;
}

async function stopOwnerWhatsappSession(ownerId, reason = '') {
  const session = getOwnerWhatsappSession(ownerId);
  if (!session) return;

  whatsappSessionsByOwner.delete(ownerId);

  if (session.client) {
    try {
      await session.client.destroy();
    } catch {
      // Ignore client destroy errors.
    }
  }

  session.status = 'disconnected';
  session.error = reason || session.error || 'Session stopped';
  touchWhatsappSession(session);
}

async function ensureOwnerWhatsappSession(ownerId) {
  if (!isWhatsappIntegrationAvailable()) {
    throw Object.assign(
      new Error('WhatsApp integration is unavailable. Install whatsapp-web.js and qrcode on backend.'),
      { status: 503 },
    );
  }

  const existing = getOwnerWhatsappSession(ownerId);
  if (existing && ['initializing', 'qr', 'ready', 'importing'].includes(existing.status)) {
    return existing;
  }

  if (existing) {
    await stopOwnerWhatsappSession(ownerId, 'Restarting session');
  }

  const { Client, LocalAuth } = whatsappWeb;
  const ownerSessionKey = sanitizeOwnerSessionKey(ownerId);
  const session = {
    id: createWhatsappSessionId(),
    ownerId,
    status: 'initializing',
    qrCodeDataUrl: '',
    error: '',
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    lastImportedAt: '',
    client: null,
  };

  const client = new Client({
    authStrategy: new LocalAuth({
      clientId: `synapse12_${ownerSessionKey}`,
    }),
    puppeteer: {
      headless: true,
      args: ['--no-sandbox', '--disable-setuid-sandbox'],
    },
  });

  session.client = client;
  whatsappSessionsByOwner.set(ownerId, session);

  client.on('qr', async (qr) => {
    try {
      session.qrCodeDataUrl = await QRCode.toDataURL(qr, {
        width: 300,
        margin: 1,
      });
      session.status = 'qr';
      session.error = '';
      touchWhatsappSession(session);
    } catch (error) {
      session.status = 'error';
      session.error = toTrimmedString(error?.message, 260) || 'Failed to render QR code';
      touchWhatsappSession(session);
    }
  });

  client.on('ready', () => {
    session.status = 'ready';
    session.qrCodeDataUrl = '';
    session.error = '';
    touchWhatsappSession(session);
  });

  client.on('auth_failure', (message) => {
    session.status = 'error';
    session.error = toTrimmedString(String(message || 'Authentication failed'), 260);
    touchWhatsappSession(session);
  });

  client.on('disconnected', (reason) => {
    session.status = 'disconnected';
    session.error = toTrimmedString(String(reason || 'Disconnected'), 260);
    touchWhatsappSession(session);
  });

  client
    .initialize()
    .catch((error) => {
      session.status = 'error';
      session.error = toTrimmedString(error?.message, 260) || 'Failed to initialize WhatsApp client';
      touchWhatsappSession(session);
    });

  return session;
}

async function mapWithConcurrency(items, limit, iterator) {
  const maxWorkers = Math.max(1, Math.min(limit, items.length || 1));
  const results = new Array(items.length);
  let cursor = 0;

  async function worker() {
    while (true) {
      const index = cursor;
      cursor += 1;
      if (index >= items.length) break;
      try {
        results[index] = await iterator(items[index], index);
      } catch {
        results[index] = null;
      }
    }
  }

  await Promise.all(Array.from({ length: maxWorkers }, () => worker()));
  return results;
}

async function fetchWhatsappImageDataUrl(url) {
  const sourceUrl = toTrimmedString(url, 2048);
  if (!sourceUrl) return '';

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), WHATSAPP_MEDIA_TIMEOUT_MS);
  let response;

  try {
    response = await fetch(sourceUrl, {
      signal: controller.signal,
    });
  } catch {
    clearTimeout(timeout);
    return '';
  } finally {
    clearTimeout(timeout);
  }

  if (!response || !response.ok) return '';

  const contentType = toTrimmedString(response.headers.get('content-type') || '', 80).toLowerCase();
  if (!contentType.startsWith('image/')) return '';

  const rawBuffer = Buffer.from(await response.arrayBuffer());
  if (!rawBuffer.length) return '';

  if (sharp) {
    try {
      let compressed = await sharp(rawBuffer)
        .resize(320, 320, { fit: 'cover' })
        .jpeg({ quality: 72, mozjpeg: true })
        .toBuffer();

      if (compressed.length > WHATSAPP_IMAGE_MAX_BYTES) {
        compressed = await sharp(rawBuffer)
          .resize(256, 256, { fit: 'cover' })
          .jpeg({ quality: 58, mozjpeg: true })
          .toBuffer();
      }

      if (compressed.length <= WHATSAPP_IMAGE_MAX_BYTES) {
        return `data:image/jpeg;base64,${compressed.toString('base64')}`;
      }
    } catch {
      // Fallback to raw image if possible.
    }
  }

  if (rawBuffer.length > WHATSAPP_IMAGE_MAX_BYTES) {
    return '';
  }

  return `data:${contentType};base64,${rawBuffer.toString('base64')}`;
}

async function readWhatsappContactAbout(contact) {
  if (!contact || typeof contact !== 'object') return '';

  if (typeof contact.getAbout === 'function') {
    try {
      const about = await contact.getAbout();
      if (typeof about === 'string') return toTrimmedString(about, 1200);
      if (about && typeof about === 'object') {
        return toTrimmedString(about.status || about.about || about.text, 1200);
      }
    } catch {
      // Ignore unavailable about.
    }
  }

  if (typeof contact.about === 'string') {
    return toTrimmedString(contact.about, 1200);
  }

  if (typeof contact.status === 'string') {
    return toTrimmedString(contact.status, 1200);
  }

  return '';
}

function normalizeWhatsappLinks(rawLinks) {
  if (!Array.isArray(rawLinks)) return [];
  const links = [];
  for (const row of rawLinks) {
    if (typeof row === 'string') {
      const value = toTrimmedString(row, 240);
      if (value) links.push(value);
      continue;
    }
    if (row && typeof row === 'object') {
      const value = toTrimmedString(row.url || row.link || row.website || row.href || row.value, 240);
      if (value) links.push(value);
    }
  }
  return Array.from(new Set(links)).slice(0, 12);
}

function normalizeWhatsappContact(rawContact, index) {
  const row = toProfile(rawContact);
  const nameCandidates = [
    row.name,
    row.displayName,
    row.fullName,
    row.pushName,
    row.shortName,
    row.title,
  ];
  const name =
    nameCandidates
      .map((value) => toTrimmedString(value, 120))
      .find(Boolean) || '';

  const phone = normalizePhone(
    toTrimmedString(
      row.phone || row.number || row.waId || row.user || row.id || row.contact || row.mobile,
      60,
    ),
  );

  const description = toTrimmedString(
    row.description || row.about || row.status || row.bio || row.note,
    1200,
  );
  const tags = normalizeEntityFieldArray(row.tags, { maxItems: 12, itemMaxLength: 64 });
  const markers = normalizeEntityFieldArray(row.markers, { maxItems: 12, itemMaxLength: 64 });
  const roles = normalizeEntityFieldArray(row.roles, { maxItems: 8, itemMaxLength: 64 });
  const links = normalizeWhatsappLinks(Array.isArray(row.links) ? row.links : []);
  const status = normalizeEntityFieldArray(
    Array.isArray(row.statuses) ? row.statuses : [row.status].filter(Boolean),
    { maxItems: 4, itemMaxLength: 64 },
  );
  const image = toTrimmedString(row.image, 10_000_000);

  if (!name && !phone && !description) {
    return null;
  }

  const fallbackName = phone ? `Контакт ${phone}` : `Контакт ${index + 1}`;
  const normalizedName = name || fallbackName;
  const importKeySource = phone || normalizedName.toLowerCase().replace(/\s+/g, '-');
  const importKey = toTrimmedString(`whatsapp:${importKeySource}`, 180);

  if (!importKey) {
    return null;
  }

  return {
    importKey,
    name: normalizedName,
    phone,
    description,
    tags,
    markers,
    roles,
    links,
    status,
    image,
  };
}

async function migrateLegacyShapeNames() {
  const legacyShapeEntities = await Entity.find(
    {
      type: 'shape',
      name: { $regex: LEGACY_SHAPE_NAME_PATTERN },
    },
    { _id: 1, name: 1 },
  ).lean();

  if (!legacyShapeEntities.length) {
    return;
  }

  const operations = legacyShapeEntities.map((entity) => ({
    updateOne: {
      filter: { _id: entity._id },
      update: {
        $set: {
          name: normalizeShapeName(entity.name),
        },
      },
    },
  }));

  await Entity.bulkWrite(operations, { ordered: false });
  console.log(`[migration] shape names renamed to "Элемент": ${operations.length}`);
}

app.post('/api/integrations/whatsapp/session/start', requireAuth, async (req, res, next) => {
  try {
    const ownerId = requireOwnerId(req);
    const session = await ensureOwnerWhatsappSession(ownerId);
    return res.status(200).json({
      integration: 'whatsapp',
      session: toWhatsappSessionStatus(session),
    });
  } catch (error) {
    return next(error);
  }
});

app.get('/api/integrations/whatsapp/session/:sessionId', requireAuth, async (req, res) => {
  const ownerId = requireOwnerId(req);
  const sessionId = toTrimmedString(req.params.sessionId, 120);
  const session = getOwnerWhatsappSession(ownerId);

  if (!session || session.id !== sessionId) {
    return res.status(404).json({ message: 'WhatsApp session not found' });
  }

  return res.status(200).json({
    integration: 'whatsapp',
    session: toWhatsappSessionStatus(session),
  });
});

app.delete('/api/integrations/whatsapp/session/:sessionId', requireAuth, async (req, res, next) => {
  try {
    const ownerId = requireOwnerId(req);
    const sessionId = toTrimmedString(req.params.sessionId, 120);
    const session = getOwnerWhatsappSession(ownerId);
    if (!session || session.id !== sessionId) {
      return res.status(404).json({ message: 'WhatsApp session not found' });
    }
    await stopOwnerWhatsappSession(ownerId, 'Closed by user');
    return res.status(204).send();
  } catch (error) {
    return next(error);
  }
});

app.post('/api/integrations/whatsapp/import', requireAuth, async (req, res, next) => {
  try {
    const ownerId = requireOwnerId(req);
    const sessionId = toTrimmedString(req.body?.sessionId, 120);
    const session = getOwnerWhatsappSession(ownerId);

    if (!session || (sessionId && session.id !== sessionId)) {
      return res.status(404).json({ message: 'WhatsApp session not found. Start a session first.' });
    }

    if (session.status !== 'ready') {
      return res.status(409).json({
        message: 'WhatsApp session is not ready. Scan QR and wait for connection.',
        session: toWhatsappSessionStatus(session),
      });
    }

    if (!session.client) {
      return res.status(500).json({ message: 'WhatsApp client is unavailable for this session.' });
    }

    session.status = 'importing';
    session.error = '';
    touchWhatsappSession(session);

    const allContacts = await session.client.getContacts();
    const importCandidates = allContacts
      .filter((contact) => {
        if (!contact || typeof contact !== 'object') return false;
        if (contact.isGroup || contact.isBroadcast || contact.isMe) return false;
        const number = toTrimmedString(contact.number || contact.id?.user, 60);
        if (!number) return false;
        return true;
      })
      .slice(0, WHATSAPP_CONTACT_IMPORT_LIMIT);

    const normalizedContacts = (
      await mapWithConcurrency(importCandidates, WHATSAPP_IMPORT_CONCURRENCY, async (contact, index) => {
        const about = await readWhatsappContactAbout(contact);

        let image = '';
        if (typeof contact.getProfilePicUrl === 'function') {
          try {
            const photoUrl = await contact.getProfilePicUrl();
            image = await fetchWhatsappImageDataUrl(photoUrl);
          } catch {
            image = '';
          }
        }

        const businessProfile = toProfile(contact.businessProfile);
        const websites = Array.isArray(businessProfile.websites)
          ? businessProfile.websites
          : [businessProfile.websites].filter(Boolean);

        return normalizeWhatsappContact(
          {
            name: contact.name,
            displayName: contact.pushname,
            fullName: contact.shortName,
            phone: contact.number || contact.id?.user,
            id: contact.id?._serialized,
            description: about || businessProfile.description || '',
            links: websites,
            tags: ['WhatsApp'],
            markers: [contact.isBusiness ? 'Бизнес' : '', contact.isMyContact ? 'Мой контакт' : ''],
            roles: contact.isBusiness ? ['Компания'] : ['Контакт'],
            statuses: [contact.isBlocked ? 'blocked' : '', contact.isBusiness ? 'business' : ''],
            image,
          },
          index,
        );
      })
    )
      .filter(Boolean)
      .reduce((map, item) => {
        map.set(item.importKey, item);
        return map;
      }, new Map());

    const uniqueContacts = Array.from(normalizedContacts.values());

    if (!uniqueContacts.length) {
      session.status = 'ready';
      session.error = '';
      touchWhatsappSession(session);
      return res.status(200).json({
        source: 'whatsapp',
        imported: 0,
        skipped: 0,
        total: 0,
        entities: [],
      });
    }

    const importKeys = uniqueContacts.map((item) => item.importKey);
    const existingConnections = await Entity.find(
      {
        owner_id: ownerId,
        type: 'connection',
        'profile.import_key': { $in: importKeys },
      },
      { _id: 1, profile: 1 },
    ).lean();

    const existingKeySet = new Set(
      existingConnections
        .map((entity) => toTrimmedString(toProfile(entity.profile).import_key, 180))
        .filter(Boolean),
    );

    const toCreate = uniqueContacts.filter((item) => !existingKeySet.has(item.importKey));

    let createdEntities = [];
    if (toCreate.length) {
      createdEntities = await Entity.insertMany(
        toCreate.map((item) => ({
          owner_id: ownerId,
          type: 'connection',
          name: item.name,
          profile: {
            color: '#1058ff',
            source: 'whatsapp',
            import_key: item.importKey,
            phone: item.phone,
            image: item.image || '',
            categoryLocked: false,
            imported_at: new Date().toISOString(),
          },
          ai_metadata: {
            description: item.description,
            tags: item.tags,
            markers: item.markers,
            roles: item.roles,
            links: item.links,
            status: item.status,
          },
        })),
        { ordered: false },
      );
    }

    session.status = 'ready';
    session.error = '';
    session.lastImportedAt = new Date().toISOString();
    touchWhatsappSession(session);

    return res.status(200).json({
      source: 'whatsapp',
      imported: createdEntities.length,
      skipped: uniqueContacts.length - createdEntities.length,
      total: uniqueContacts.length,
      entities: createdEntities,
      session: toWhatsappSessionStatus(session),
    });
  } catch (error) {
    const ownerId = getOwnerIdFromRequest(req);
    const session = getOwnerWhatsappSession(ownerId);
    if (session) {
      session.status = 'error';
      session.error = toTrimmedString(error?.message, 260) || 'Import failed';
      touchWhatsappSession(session);
    }
    return next(error);
  }
});

app.post('/api/auth/google', async (req, res, next) => {
  try {
    const credential = typeof req.body?.credential === 'string' ? req.body.credential.trim() : '';
    if (!credential) {
      return res.status(400).json({ message: 'Google credential is required' });
    }

    const verifiedIdentity = await verifyGoogleCredential(credential);
    const user = await upsertUserFromIdentity(verifiedIdentity);
    const sessionToken = createSessionToken(user);
    setSessionCookie(res, sessionToken);

    return res.status(200).json({
      user: toPublicUser(user),
      sessionToken,
      expiresIn: SESSION_TTL_SECONDS,
      mode: 'google',
    });
  } catch (error) {
    return next(error);
  }
});

app.get('/api/auth/config', async (req, res) => {
  return res.status(200).json({
    googleClientId: GOOGLE_CLIENT_ID,
    googleEnabled: Boolean(GOOGLE_CLIENT_ID),
    devAuthEnabled: DEV_AUTH_ENABLED,
    authRequired: AUTH_REQUIRED,
  });
});

app.post('/api/auth/dev-login', async (req, res, next) => {
  try {
    if (!DEV_AUTH_ENABLED) {
      return res.status(404).json({ message: 'Not found' });
    }

    const rawEmail =
      typeof req.body?.email === 'string' && req.body.email.trim()
        ? req.body.email.trim().toLowerCase()
        : 'local.dev@synapse12.local';
    const rawName =
      typeof req.body?.name === 'string' && req.body.name.trim()
        ? req.body.name.trim()
        : 'Local Developer';

    const devIdentity = {
      provider: 'dev',
      providerId: rawEmail,
      email: rawEmail,
      name: rawName,
      picture: '',
      givenName: rawName,
      familyName: '',
    };

    const user = await upsertUserFromIdentity(devIdentity);
    const sessionToken = createSessionToken(user);
    setSessionCookie(res, sessionToken);

    return res.status(200).json({
      user: toPublicUser(user),
      sessionToken,
      expiresIn: SESSION_TTL_SECONDS,
      mode: 'dev',
    });
  } catch (error) {
    return next(error);
  }
});

app.get('/api/auth/me', requireAuth, async (req, res) => {
  return res.status(200).json({
    user: req.authUser,
  });
});

app.put('/api/auth/settings', requireAuth, async (req, res, next) => {
  try {
    const settingsPatch = normalizeSettingsUpdate(req.body?.settings);
    if (!Object.keys(settingsPatch).length) {
      return res.status(400).json({ message: 'settings payload is required' });
    }

    const currentSettings =
      req.authUser && req.authUser.settings && typeof req.authUser.settings === 'object'
        ? req.authUser.settings
        : {};
    const nextSettings = {
      ...currentSettings,
      ...settingsPatch,
    };

    const user = await User.findByIdAndUpdate(
      req.authUser.id,
      {
        $set: {
          settings: nextSettings,
        },
      },
      {
        new: true,
        runValidators: true,
      },
    ).lean();

    if (!user) {
      return res.status(401).json({ message: 'Unauthorized' });
    }

    req.authUser = toPublicUser(user);
    return res.status(200).json({
      user: req.authUser,
    });
  } catch (error) {
    return next(error);
  }
});

app.post('/api/auth/logout', async (req, res) => {
  clearSessionCookie(res);
  return res.status(204).send();
});

app.post('/api/ai/agent-chat', requireAuth, async (req, res, next) => {
  try {
    const ownerId = requireOwnerId(req);
    const message = toTrimmedString(req.body?.message, 2400);

    if (!message) {
      return res.status(400).json({ message: 'message is required' });
    }

    const history = normalizeAgentHistory(req.body?.history);
    const attachments = normalizeAgentAttachments(req.body?.attachments);
    const scopeContext = await resolveAgentScopeContext(ownerId, req.body?.scope);

    const systemPrompt = buildAgentSystemPrompt(scopeContext);
    const userPrompt = buildAgentUserPrompt({
      scopeContext,
      message,
      history,
      attachments,
    });

    const aiResponse = await requestOpenAiAgentReply({
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
        limitedTo: AI_CONTEXT_ENTITY_LIMIT,
      },
      ...(debugPayload ? { debug: debugPayload } : {}),
    });
  } catch (error) {
    return next(error);
  }
});

app.post('/api/ai/entity-analyze', requireAuth, async (req, res, next) => {
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
    const history = normalizeAgentHistory(req.body?.history);
    const attachments = normalizeAgentAttachments(req.body?.attachments);
    const documents = normalizeAgentAttachments(req.body?.documents);

    if (!message && !voiceInput && !history.length && !attachments.length && !documents.length) {
      return res
        .status(400)
        .json({ message: 'message or at least one context item (history/attachments/documents) is required' });
    }

    const aiMetadata = toProfile(entity.ai_metadata);
    const currentFields = buildEntityAnalyzerCurrentFields(entity.type, aiMetadata);
    const systemPrompt = buildEntityAnalyzerSystemPrompt(entity.type);
    const userPrompt = buildEntityAnalyzerUserPrompt({
      entity,
      message,
      history,
      attachments,
      currentFields,
      voiceInput,
      documents,
    });

    const aiResponse = await requestOpenAiAgentReply({
      systemPrompt,
      userPrompt,
    });

    const parsedResponse = extractJsonObjectFromText(aiResponse.reply);
    const analysis = normalizeEntityAnalysisOutput(entity.type, parsedResponse);
    const reply = buildEntityAnalysisReplyText(analysis);

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

app.post('/api/ai/entity-apply', requireAuth, async (req, res, next) => {
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

app.use('/api/entities', requireAuth);

app.get('/api/entities', async (req, res, next) => {
  try {
    const filter = {};
    const ownerId = requireOwnerId(req);
    filter.owner_id = ownerId;

    if (req.query.type) {
      filter.type = req.query.type;
    }

    const entities = await Entity.find(filter).sort({ createdAt: -1, _id: -1 });
    res.json(entities);
  } catch (error) {
    next(error);
  }
});

app.post('/api/entities', async (req, res, next) => {
  try {
    const ownerId = requireOwnerId(req);
    const payload = req.body && typeof req.body === 'object' ? { ...req.body } : {};
    payload.owner_id = ownerId;

    const entity = await Entity.create(payload);
    res.status(201).json(entity);
  } catch (error) {
    next(error);
  }
});

app.put('/api/entities/:id', async (req, res, next) => {
  try {
    const ownerId = requireOwnerId(req);
    const payload = req.body && typeof req.body === 'object' ? { ...req.body } : {};
    payload.owner_id = ownerId;

    const updatedEntity = await Entity.findOneAndUpdate(
      {
        _id: req.params.id,
        owner_id: ownerId,
      },
      payload,
      {
      new: true,
      runValidators: true,
      },
    );

    if (!updatedEntity) {
      return res.status(404).json({ message: 'Entity not found' });
    }

    return res.json(updatedEntity);
  } catch (error) {
    return next(error);
  }
});

app.delete('/api/entities/:id', async (req, res, next) => {
  try {
    const ownerId = requireOwnerId(req);
    const entityToDelete = await Entity.findOne(
      {
        _id: req.params.id,
        owner_id: ownerId,
      },
      { _id: 1, type: 1, canvas_data: 1 },
    ).lean();
    if (!entityToDelete) {
      return res.status(404).json({ message: 'Entity not found' });
    }

    const entityId = String(entityToDelete._id);

    if (entityToDelete.type === 'project') {
      const projectCanvas = normalizeProjectCanvasData(entityToDelete.canvas_data);
      const nodeEntityIds = Array.from(
        new Set(
          projectCanvas.nodes
            .map((node) => node.entityId)
            .filter((id) => id && id !== entityId),
        ),
      );

      await removeEntitiesFromProjectCanvases([entityId, ...nodeEntityIds], ownerId);
      if (nodeEntityIds.length) {
        await Entity.deleteMany({
          _id: { $in: nodeEntityIds },
          owner_id: ownerId,
        });
      }
    } else {
      await removeEntityFromProjectCanvases(entityId, ownerId);
    }

    await Entity.deleteOne({
      _id: entityToDelete._id,
      owner_id: ownerId,
    });

    return res.status(204).send();
  } catch (error) {
    return next(error);
  }
});

app.use((err, req, res, next) => {
  console.error(err);

  if (typeof err?.status === 'number' && err.status >= 400 && err.status < 600) {
    return res.status(err.status).json({ message: err.message || 'Request failed' });
  }

  if (err.name === 'ValidationError') {
    return res.status(400).json({ message: err.message });
  }

  if (err.name === 'CastError') {
    return res.status(400).json({ message: 'Invalid entity id' });
  }

  return res.status(500).json({ message: 'Internal server error' });
});

async function startServer() {
  await connectDB();
  await migrateLegacyShapeNames();

  if (!GOOGLE_CLIENT_ID) {
    console.warn('[auth] GOOGLE_CLIENT_ID is not set. /api/auth/google will be unavailable.');
  }
  if (!SESSION_SECRET) {
    console.warn('[auth] SESSION_SECRET is not set. Session endpoints will be unavailable.');
  }
  if (!OPENAI_API_KEY) {
    console.warn('[ai] OPENAI_API_KEY is not set. /api/ai/* endpoints will be unavailable.');
  } else {
    console.warn(
      `[ai] Enabled models: chat=${OPENAI_MODEL}, embedding=${OPENAI_EMBEDDING_MODEL}, debugEcho=${AI_DEBUG_ECHO}`,
    );
  }
  if (!isWhatsappIntegrationAvailable()) {
    console.warn(
      '[integrations] WhatsApp integration is disabled. Install backend deps: whatsapp-web.js and qrcode.',
    );
  }
  if (!sharp) {
    console.warn(
      '[integrations] sharp is not installed. WhatsApp avatars will be imported without compression fallback.',
    );
  }
  if (DEV_AUTH_ENABLED) {
    console.warn('[auth] DEV_AUTH_ENABLED=true. /api/auth/dev-login is available (development only).');
  }
  if (!AUTH_REQUIRED) {
    console.warn('[auth] AUTH_REQUIRED=false is ignored. /api/entities is always protected.');
  }
  if (!GOOGLE_CLIENT_ID || !SESSION_SECRET) {
    console.warn(
      '[auth] Auth config is incomplete. Check GOOGLE_CLIENT_ID and SESSION_SECRET.',
    );
  }

  app.listen(PORT, () => {
    console.log(`Backend server started on port ${PORT}`);
  });
}

startServer();
