const express = require('express');
const cors = require('cors');
const dotenv = require('dotenv');
const cookieParser = require('cookie-parser');
const jwt = require('jsonwebtoken');
const crypto = require('crypto');
const path = require('path');
const fs = require('fs');
const { OAuth2Client } = require('google-auth-library');

const connectDB = require('./config/db');
const Entity = require('./models/Entity');
const EntityWebSearch = require('./models/EntityWebSearch');
const User = require('./models/User');
const EntityVector = require('./models/EntityVector');
const AgentChatHistory = require('./models/AgentChatHistory');
const { createAiPrompts } = require('./ai/prompts');
const { createAiAttachmentTools } = require('./ai/attachments');
const { createAiProvider } = require('./ai/provider');
const { createAiRouter } = require('./routes/ai.routes');
const { createTranscribeRouter } = require('./routes/transcribe.routes');

let whatsappWeb = null;
let whatsappBaileys = null;
let QRCode = null;
let sharp = null;
let mammoth = null;

if (!process.env.PUPPETEER_CACHE_DIR) {
  process.env.PUPPETEER_CACHE_DIR = path.resolve(__dirname, '..', '.cache', 'puppeteer');
}

try {
  whatsappWeb = require('whatsapp-web.js');
} catch {
  whatsappWeb = null;
}

try {
  whatsappBaileys = require('@whiskeysockets/baileys');
} catch {
  whatsappBaileys = null;
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

try {
  mammoth = require('mammoth');
} catch {
  mammoth = null;
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
const DEFAULT_ALLOWED_ORIGIN_PATTERNS = [/^https:\/\/synapse-frontend[-\w]*\.vercel\.app$/];
const OPENAI_API_KEY = String(process.env.OPENAI_API_KEY || '').trim();
const OPENAI_MODEL = String(process.env.OPENAI_MODEL || 'gpt-5').trim();
const OPENAI_PROJECT_MODEL = String(process.env.OPENAI_PROJECT_MODEL || 'gpt-5').trim();
const OPENAI_ROUTER_MODEL = String(process.env.OPENAI_ROUTER_MODEL || 'gpt-5').trim();
const OPENAI_DEEP_MODEL = String(process.env.OPENAI_DEEP_MODEL || OPENAI_PROJECT_MODEL || 'gpt-5').trim();
const OPENAI_WEB_SEARCH_MODEL = String(process.env.OPENAI_WEB_SEARCH_MODEL || OPENAI_MODEL || 'gpt-5').trim();
const OPENAI_TRANSCRIBE_MODEL = String(process.env.OPENAI_TRANSCRIBE_MODEL || 'gpt-4o-transcribe').trim();
const OPENAI_TRANSCRIBE_MAX_AUDIO_BYTES = Math.max(
  512_000,
  Number(process.env.OPENAI_TRANSCRIBE_MAX_AUDIO_BYTES) || 25 * 1024 * 1024,
);
const OPENAI_REQUEST_TIMEOUT_MS = Number(process.env.OPENAI_REQUEST_TIMEOUT_MS) || 0;
const OPENAI_EMBEDDING_MODEL = String(process.env.OPENAI_EMBEDDING_MODEL || 'text-embedding-3-small').trim();
const AI_CONTEXT_ENTITY_LIMIT = Math.max(1, Number(process.env.AI_CONTEXT_ENTITY_LIMIT) || 120);
const AI_HISTORY_MESSAGE_LIMIT = Math.max(1, Number(process.env.AI_HISTORY_MESSAGE_LIMIT) || 12);
const AI_ATTACHMENT_LIMIT = Math.max(1, Number(process.env.AI_ATTACHMENT_LIMIT) || 6);
const AI_ATTACHMENT_TEXT_MAX_LENGTH = Math.max(400, Number(process.env.AI_ATTACHMENT_TEXT_MAX_LENGTH) || 12_000);
const AI_ATTACHMENT_DATA_URL_MAX_LENGTH = Math.max(
  2_000,
  Number(process.env.AI_ATTACHMENT_DATA_URL_MAX_LENGTH) || 3_000_000,
);
const AI_ATTACHMENT_BINARY_MAX_BYTES = Math.max(
  64_000,
  Number(process.env.AI_ATTACHMENT_BINARY_MAX_BYTES) || 2_000_000,
);
const AI_DEBUG_ECHO = String(process.env.AI_DEBUG_ECHO || '').toLowerCase() === 'true';
const AGENT_CHAT_HISTORY_MESSAGE_LIMIT = Math.max(
  20,
  Number(process.env.AGENT_CHAT_HISTORY_MESSAGE_LIMIT) || 140,
);
const AGENT_CHAT_HISTORY_ATTACHMENT_LIMIT = Math.max(
  0,
  Number(process.env.AGENT_CHAT_HISTORY_ATTACHMENT_LIMIT) || 6,
);
const AGENT_CHAT_HISTORY_ATTACHMENT_DATA_MAX_LENGTH = Math.max(
  2000,
  Number(process.env.AGENT_CHAT_HISTORY_ATTACHMENT_DATA_MAX_LENGTH) || 320000,
);
const AGENT_CHAT_HISTORY_TEXT_MAX_LENGTH = Math.max(
  400,
  Number(process.env.AGENT_CHAT_HISTORY_TEXT_MAX_LENGTH) || 12000,
);
const WHATSAPP_CONTACT_IMPORT_LIMIT = Math.max(1, Number(process.env.WHATSAPP_CONTACT_IMPORT_LIMIT) || 2500);
const WHATSAPP_IMPORT_CONCURRENCY = Math.max(1, Number(process.env.WHATSAPP_IMPORT_CONCURRENCY) || 4);
const WHATSAPP_IMPORT_BATCH_SIZE = Math.max(1, Number(process.env.WHATSAPP_IMPORT_BATCH_SIZE) || 80);
const WHATSAPP_IMAGE_MAX_BYTES = Math.max(40_000, Number(process.env.WHATSAPP_IMAGE_MAX_BYTES) || 260_000);
const WHATSAPP_MEDIA_TIMEOUT_MS = Math.max(5_000, Number(process.env.WHATSAPP_MEDIA_TIMEOUT_MS) || 15_000);
const WHATSAPP_SESSION_IDLE_TIMEOUT_RAW = Number(process.env.WHATSAPP_SESSION_IDLE_TIMEOUT_MS);
const WHATSAPP_SESSION_IDLE_TIMEOUT_MS = Number.isFinite(WHATSAPP_SESSION_IDLE_TIMEOUT_RAW)
  ? WHATSAPP_SESSION_IDLE_TIMEOUT_RAW <= 0
    ? 0
    : Math.max(60_000, WHATSAPP_SESSION_IDLE_TIMEOUT_RAW)
  : 0;
const WHATSAPP_IMAGE_FETCH_ENABLED =
  String(process.env.WHATSAPP_IMAGE_FETCH_ENABLED || 'true').toLowerCase() !== 'false';
const WHATSAPP_IMAGE_IMPORT_MAX_COUNT = Math.max(
  0,
  Number(process.env.WHATSAPP_IMAGE_IMPORT_MAX_COUNT) || WHATSAPP_CONTACT_IMPORT_LIMIT,
);
const WHATSAPP_IMAGE_IMPORT_CONCURRENCY = Math.max(
  1,
  Number(process.env.WHATSAPP_IMAGE_IMPORT_CONCURRENCY) || 1,
);
const WHATSAPP_PHOTOS_BACKFILL_BATCH_LIMIT = Math.max(
  20,
  Number(process.env.WHATSAPP_PHOTOS_BACKFILL_BATCH_LIMIT) || 80,
);
const WHATSAPP_PHOTOS_BACKFILL_MAX_LIMIT = Math.max(
  WHATSAPP_PHOTOS_BACKFILL_BATCH_LIMIT,
  Number(process.env.WHATSAPP_PHOTOS_BACKFILL_MAX_LIMIT) || 300,
);
const WHATSAPP_PHOTO_LOOKUP_TIMEOUT_MS = Math.max(
  1000,
  Number(process.env.WHATSAPP_PHOTO_LOOKUP_TIMEOUT_MS) || 4000,
);
const WHATSAPP_PHOTO_RETRY_AFTER_MS = Math.max(
  60_000,
  Number(process.env.WHATSAPP_PHOTO_RETRY_AFTER_MS) || 5 * 60 * 1000,
);
const WHATSAPP_INIT_TIMEOUT_MS = Math.max(
  20_000,
  Number(process.env.WHATSAPP_INIT_TIMEOUT_MS) || 60_000,
);
const WHATSAPP_MAX_CONCURRENT_SESSIONS = Math.max(
  1,
  Number(process.env.WHATSAPP_MAX_CONCURRENT_SESSIONS) || 1,
);
const WHATSAPP_DEBUG_LOG_LIMIT = Math.max(
  50,
  Number(process.env.WHATSAPP_DEBUG_LOG_LIMIT) || 400,
);
const WHATSAPP_BACKGROUND_IMPORT_POLL_MS = Math.max(
  600,
  Number(process.env.WHATSAPP_BACKGROUND_IMPORT_POLL_MS) || 1200,
);
const PUPPETEER_BROWSER_WS_ENDPOINT = String(process.env.PUPPETEER_BROWSER_WS_ENDPOINT || '').trim().slice(0, 2048);
const WHATSAPP_ALLOW_LOCAL_CHROME =
  String(process.env.WHATSAPP_ALLOW_LOCAL_CHROME || (!IS_PRODUCTION ? 'true' : 'false')).toLowerCase() === 'true';
const WHATSAPP_CONNECTOR = String(process.env.WHATSAPP_CONNECTOR || 'baileys')
  .trim()
  .toLowerCase();
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
  connection: ['tags', 'markers', 'roles', 'links', 'phones', 'status', 'importance'],
  person: ['tags', 'markers', 'roles', 'skills', 'links', 'phones', 'importance', 'risks', 'ignoredNoise'],
  company: ['tags', 'markers', 'industry', 'departments', 'stage', 'risks', 'links', 'phones', 'importance'],
  event: ['tags', 'markers', 'date', 'location', 'participants', 'outcomes', 'links', 'phones', 'importance'],
  resource: ['tags', 'markers', 'resources', 'status', 'owners', 'links', 'phones', 'importance'],
  goal: ['tags', 'markers', 'priority', 'metrics', 'owners', 'status', 'links', 'phones', 'importance'],
  result: ['tags', 'markers', 'outcomes', 'metrics', 'owners', 'links', 'phones', 'importance'],
  task: ['tags', 'markers', 'priority', 'status', 'owners', 'date', 'links', 'phones', 'importance'],
  project: [
    'tags',
    'markers',
    'roles',
    'skills',
    'risks',
    'priority',
    'status',
    'tasks',
    'metrics',
    'owners',
    'participants',
    'resources',
    'outcomes',
    'industry',
    'departments',
    'stage',
    'date',
    'location',
    'phones',
    'links',
    'importance',
    'ignoredNoise',
  ],
  shape: ['tags', 'markers', 'status', 'links', 'phones', 'importance'],
});
const ENTITY_IMPORTANCE_VALUES = ['Низкая', 'Средняя', 'Высокая'];
const DESCRIPTION_CHANGE_TYPES = new Set(['initial', 'addition', 'update']);
const IMPORTANCE_SIGNAL_TYPES = new Set(['increase', 'decrease', 'neutral']);
const DESCRIPTION_HISTORY_LIMIT = Math.max(10, Number(process.env.DESCRIPTION_HISTORY_LIMIT) || 40);
const IMPORTANCE_HISTORY_LIMIT = Math.max(20, Number(process.env.IMPORTANCE_HISTORY_LIMIT) || 80);
const IMPORTANCE_SCORE_BY_LABEL = Object.freeze({
  Низкая: 0,
  Средняя: 1,
  Высокая: 2,
});
const IMPORTANCE_LABEL_BY_SCORE = ['Низкая', 'Средняя', 'Высокая'];
const ENTITY_NAME_MODE_VALUES = new Set(['system', 'manual', 'llm']);
const ENTITY_SYSTEM_NAME_LABELS = Object.freeze({
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
const IMPORTANCE_AUTO_WEIGHTS = Object.freeze({
  resources: 2.4,
  skills: 2.2,
  knowledge: 2.1,
  experience: 2.1,
  contacts: 1.9,
  roles: 1.8,
  owners: 1.8,
  phones: 1.7,
  participants: 1.6,
  industry: 1.5,
  departments: 1.4,
  outcomes: 1.4,
  metrics: 1.4,
  tags: 1.2,
  markers: 1.1,
  links: 1.0,
  status: 1.0,
  stage: 1.0,
  risks: 0.9,
  priority: 0.9,
  location: 0.8,
  date: 0.7,
});
const IMPORTANCE_AUTO_FIELD_CAP = 3;
const ENTITY_VECTOR_WEIGHTS = Object.freeze({
  description: 0.45,
  roles: 0.15,
  skills: 0.15,
  tags: 0.1,
  markers: 0.05,
  links: 0.05,
  nameType: 0.05,
});
const PERSON_SKILL_LEVEL_MIN = 1;
const PERSON_SKILL_LEVEL_MAX = 10;
const PERSON_SKILL_DEFAULT_LEVEL = 5;
const PERSON_SKILL_DEFAULT_GROUP = 'Пользовательские';
const PERSON_ROLE_DEFAULT_GROUP = 'Пользовательские';
const whatsappSessionsByOwner = new Map();
const entityEventStreamsByOwner = new Map();
let entityEventStreamSeq = 0;

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

  const labels = ENTITY_SYSTEM_NAME_LABELS[entityType];
  if (!Array.isArray(labels) || !labels.length) return false;

  return labels.some((label) => {
    const prefix = escapeRegExp(toTrimmedString(label, 80));
    if (!prefix) return false;
    const pattern = new RegExp(`^${prefix}(?:\\s*(?:-\\s*)?\\d+)?$`, 'i');
    return pattern.test(name);
  });
}

function sanitizeSsePayload(payload) {
  try {
    return JSON.stringify(payload ?? {});
  } catch {
    return JSON.stringify({ message: 'serialization_failed' });
  }
}

function writeSseEvent(res, event, payload) {
  if (!res || res.writableEnded || res.destroyed) return;
  const eventName = String(event || '').trim();
  if (!eventName) return;

  res.write(`event: ${eventName}\n`);
  res.write(`data: ${sanitizeSsePayload(payload)}\n\n`);
}

function broadcastEntityEvent(ownerId, event, payload) {
  const normalizedOwnerId = String(ownerId || '').trim();
  if (!normalizedOwnerId) return;

  const ownerStreams = entityEventStreamsByOwner.get(normalizedOwnerId);
  if (!ownerStreams || !ownerStreams.size) return;

  for (const stream of ownerStreams.values()) {
    writeSseEvent(stream.res, event, payload);
  }
}

function registerEntityEventStream(ownerId, req, res) {
  const normalizedOwnerId = String(ownerId || '').trim();
  if (!normalizedOwnerId) return () => {};

  const streamId = `stream_${Date.now()}_${entityEventStreamSeq++}`;
  const keepAliveTimer = setInterval(() => {
    if (!res.writableEnded && !res.destroyed) {
      res.write(': ping\n\n');
    }
  }, 25000);

  const ownerStreams = entityEventStreamsByOwner.get(normalizedOwnerId) || new Map();
  ownerStreams.set(streamId, {
    res,
    keepAliveTimer,
  });
  entityEventStreamsByOwner.set(normalizedOwnerId, ownerStreams);

  const cleanup = () => {
    clearInterval(keepAliveTimer);
    const streams = entityEventStreamsByOwner.get(normalizedOwnerId);
    if (!streams) return;
    streams.delete(streamId);
    if (!streams.size) {
      entityEventStreamsByOwner.delete(normalizedOwnerId);
    }
  };

  req.on('close', cleanup);
  req.on('aborted', cleanup);
  res.on('close', cleanup);
  res.on('error', cleanup);

  writeSseEvent(res, 'connected', {
    ownerId: normalizedOwnerId,
    connectedAt: new Date().toISOString(),
  });

  return cleanup;
}

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

function parseAllowedOriginPatterns() {
  const raw = [
    process.env.FRONTEND_ORIGIN_PATTERNS,
    process.env.CORS_ORIGIN_PATTERNS,
  ]
    .filter((value) => typeof value === 'string' && value.trim().length > 0)
    .join(',');

  const normalized = raw
    .split(',')
    .map((pattern) => pattern.trim())
    .filter(Boolean)
    .map((pattern) => {
      if (pattern.startsWith('/') && pattern.endsWith('/')) {
        try {
          return new RegExp(pattern.slice(1, -1));
        } catch {
          return null;
        }
      }

      const escaped = pattern.replace(/[.*+?^${}()|[\]\\]/g, '\\$&').replace(/\\\*/g, '.*');
      return new RegExp(`^${escaped}$`);
    })
    .filter((pattern) => Boolean(pattern));

  if (normalized.length) {
    return normalized;
  }

  return DEFAULT_ALLOWED_ORIGIN_PATTERNS;
}

const allowedOrigins = parseAllowedOrigins();
const allowedOriginPatterns = parseAllowedOriginPatterns();
const googleClient = GOOGLE_CLIENT_ID ? new OAuth2Client(GOOGLE_CLIENT_ID) : null;

app.use(
  cors({
    origin(origin, callback) {
      if (!origin) {
        callback(null, true);
        return;
      }

      if (
        !allowedOrigins.size ||
        allowedOrigins.has(origin) ||
        allowedOriginPatterns.some((pattern) => pattern.test(origin))
      ) {
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
  const rawGroups = Array.isArray(raw.groups) ? raw.groups : [];
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
    const massRaw = typeof node.mass === 'number' && Number.isFinite(node.mass) ? node.mass : undefined;
    const mass = typeof massRaw === 'number' ? Math.max(0, Math.min(10, Math.round(massRaw))) : undefined;

    if (!id || !entityId || x === null || y === null) {
      return [];
    }

    return [{
      id,
      entityId,
      x,
      y,
      ...(typeof scale === 'number' ? { scale } : {}),
      ...(typeof mass === 'number' ? { mass } : {}),
    }];
  });

  const edges = rawEdges.flatMap((edge) => {
    if (!edge || typeof edge !== 'object') return [];

    const id = typeof edge.id === 'string' ? edge.id : '';
    const source = typeof edge.source === 'string' ? edge.source : '';
    const target = typeof edge.target === 'string' ? edge.target : '';
    const label = typeof edge.label === 'string' ? edge.label : undefined;
    const type = typeof edge.type === 'string' ? edge.type : undefined;
    const relationType = typeof edge.relationType === 'string' ? edge.relationType : undefined;
    const description = typeof edge.description === 'string' ? edge.description : undefined;
    const meaning = typeof edge.meaning === 'string' ? edge.meaning : undefined;
    const semanticMeaning = typeof edge.semanticMeaning === 'string' ? edge.semanticMeaning : undefined;
    const summary = typeof edge.summary === 'string' ? edge.summary : undefined;
    const color = typeof edge.color === 'string' && edge.color.trim() ? edge.color : undefined;
    const arrowLeft = typeof edge.arrowLeft === 'boolean' ? edge.arrowLeft : undefined;
    const arrowRight = typeof edge.arrowRight === 'boolean' ? edge.arrowRight : undefined;

    if (!id || !source || !target) {
      return [];
    }

    return [{
      id,
      source,
      target,
      label,
      type,
      relationType,
      description,
      meaning,
      semanticMeaning,
      summary,
      color,
      arrowLeft,
      arrowRight,
    }];
  });

  const nodeIdSet = new Set(nodes.map((node) => node.id));
  const groups = rawGroups.flatMap((group) => {
    if (!group || typeof group !== 'object') return [];

    const id = typeof group.id === 'string' ? group.id : '';
    const name = typeof group.name === 'string' ? group.name.trim().slice(0, 120) : '';
    const color = typeof group.color === 'string' && group.color.trim() ? group.color.trim().slice(0, 24) : undefined;
    const nodeIds = Array.isArray(group.nodeIds)
      ? Array.from(
        new Set(
          group.nodeIds
            .map((nodeId) => (typeof nodeId === 'string' ? nodeId : ''))
            .filter((nodeId) => nodeId && nodeIdSet.has(nodeId)),
        ),
      )
      : [];

    if (!id || nodeIds.length < 2) {
      return [];
    }

    return [{
      id,
      name: name || 'Группа',
      nodeIds,
      ...(color ? { color } : {}),
    }];
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
    groups,
    ...(viewport ? { viewport } : {}),
    ...(rawBackground ? { background: rawBackground } : {}),
  };
}

function buildProjectCanvasContentVersion(canvasData) {
  const normalized = normalizeProjectCanvasData(canvasData);
  let fingerprint = 2166136261;
  const hashChunk = (value) => {
    const chunk = typeof value === 'string' ? value : String(value ?? '');
    for (let index = 0; index < chunk.length; index += 1) {
      fingerprint ^= chunk.charCodeAt(index);
      fingerprint = Math.imul(fingerprint, 16777619);
    }
  };

  for (const node of normalized.nodes) {
    const scale = typeof node.scale === 'number' ? node.scale : 1;
    const mass = typeof node.mass === 'number' ? node.mass : 5;
    hashChunk(
      `${node.id}|${node.entityId}|${Math.round(node.x * 100)}|${Math.round(node.y * 100)}|${Math.round(
        scale * 1000,
      )}|${Math.round(mass * 1000)};`,
    );
  }
  for (const edge of normalized.edges) {
    hashChunk(
      `${edge.id}|${edge.source}|${edge.target}|${edge.label || ''}|${edge.color || ''}|${
        edge.arrowLeft ? 1 : 0
      }|${edge.arrowRight ? 1 : 0};`,
    );
  }
  for (const group of normalized.groups) {
    hashChunk(`${group.id}|${group.name}|${group.color || ''}|${group.nodeIds.join(',')};`);
  }

  return [
    normalized.groups.length,
    normalized.nodes.length,
    normalized.edges.length,
    normalized.background || 'default',
    String(fingerprint >>> 0),
  ].join('|');
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

function toTrimmedTailString(value, maxLength = 240) {
  if (typeof value !== 'string') return '';
  const trimmed = value.trim();
  if (!trimmed) return '';
  if (trimmed.length <= maxLength) return trimmed;
  return trimmed.slice(trimmed.length - maxLength);
}

function toBooleanFlag(value, fallback = false) {
  if (typeof value === 'boolean') {
    return value;
  }

  if (typeof value === 'string') {
    const normalized = value.trim().toLowerCase();
    if (normalized === 'true') return true;
    if (normalized === 'false') return false;
  }

  return fallback;
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

function normalizePersonSkillName(value) {
  return toTrimmedString(value, 64);
}

function normalizePersonSkillLevel(value) {
  const numeric = Math.round(Number(value));
  if (!Number.isFinite(numeric)) return PERSON_SKILL_DEFAULT_LEVEL;
  return Math.max(PERSON_SKILL_LEVEL_MIN, Math.min(PERSON_SKILL_LEVEL_MAX, numeric));
}

function normalizePersonSkillCategory(value) {
  return toTrimmedString(value, 16).toLowerCase() === 'soft' ? 'soft' : 'hard';
}

function normalizePersonSkillGroup(value) {
  return toTrimmedString(value, 48) || PERSON_SKILL_DEFAULT_GROUP;
}

function normalizePersonRoleName(value) {
  return toTrimmedString(value, 64);
}

function normalizePersonRoleCategory(value) {
  return toTrimmedString(value, 16).toLowerCase() === 'personal' ? 'personal' : 'professional';
}

function normalizePersonRoleGroup(value) {
  return toTrimmedString(value, 48) || PERSON_ROLE_DEFAULT_GROUP;
}

function normalizeManualPersonRoles(value) {
  if (!Array.isArray(value)) return [];

  const dedupe = new Set();
  const result = [];

  for (const item of value) {
    if (typeof item === 'string') {
      const normalizedName = normalizePersonRoleName(item);
      if (!normalizedName) continue;
      const key = normalizedName.toLowerCase();
      if (dedupe.has(key)) continue;
      dedupe.add(key);
      result.push({
        name: normalizedName,
        category: 'professional',
        group: PERSON_ROLE_DEFAULT_GROUP,
      });
      continue;
    }

    const row = toProfile(item);
    const normalizedName = normalizePersonRoleName(row.name);
    if (!normalizedName) continue;
    const key = normalizedName.toLowerCase();
    if (dedupe.has(key)) continue;
    dedupe.add(key);
    result.push({
      name: normalizedName,
      category: normalizePersonRoleCategory(row.category),
      group: normalizePersonRoleGroup(row.group),
    });
  }

  return result;
}

function mergeEntityRoleValues(aiRoles, manualRoles, maxItems = 8) {
  const dedupe = new Set();
  const result = [];
  const normalizedManualRoles = normalizeManualPersonRoles(manualRoles).map((role) => role.name);
  const normalizedAiRoles = toStringArray(aiRoles, maxItems, 64);

  for (const item of [...normalizedManualRoles, ...normalizedAiRoles]) {
    const normalized = normalizePersonRoleName(item);
    if (!normalized) continue;
    const key = normalized.toLowerCase();
    if (dedupe.has(key)) continue;
    dedupe.add(key);
    result.push(normalized);
    if (result.length >= maxItems) break;
  }

  return result;
}

function normalizeManualPersonSkills(value) {
  if (!Array.isArray(value)) return [];

  const dedupe = new Set();
  const result = [];

  for (const item of value) {
    if (typeof item === 'string') {
      const normalizedName = normalizePersonSkillName(item);
      if (!normalizedName) continue;
      const key = normalizedName.toLowerCase();
      if (dedupe.has(key)) continue;
      dedupe.add(key);
      result.push({
        name: normalizedName,
        level: PERSON_SKILL_DEFAULT_LEVEL,
        category: 'hard',
        group: PERSON_SKILL_DEFAULT_GROUP,
      });
      continue;
    }

    const row = toProfile(item);
    const normalizedName = normalizePersonSkillName(row.name);
    if (!normalizedName) continue;
    const key = normalizedName.toLowerCase();
    if (dedupe.has(key)) continue;
    dedupe.add(key);
    result.push({
      name: normalizedName,
      level: normalizePersonSkillLevel(row.level),
      category: normalizePersonSkillCategory(row.category),
      group: normalizePersonSkillGroup(row.group),
    });
  }

  return result;
}

function normalizeImportanceValue(value) {
  const directRaw = toTrimmedString(value, 24);
  if (ENTITY_IMPORTANCE_VALUES.includes(directRaw)) return directRaw;
  const direct = directRaw.toLowerCase();

  const map = {
    низкая: 'Низкая',
    low: 'Низкая',
    l: 'Низкая',
    средняя: 'Средняя',
    medium: 'Средняя',
    med: 'Средняя',
    m: 'Средняя',
    высокая: 'Высокая',
    high: 'Высокая',
    h: 'Высокая',
    критично: 'Высокая',
    критическая: 'Высокая',
    критическаяя: 'Высокая',
    critical: 'Высокая',
  };

  return map[direct] || '';
}

function normalizeImportanceArray(value) {
  const normalized = normalizeEntityFieldArray(value, { maxItems: 1, itemMaxLength: 24 })
    .map((item) => normalizeImportanceValue(item))
    .find(Boolean);
  return normalized ? [normalized] : [];
}

function normalizeImportanceSource(value) {
  const source = toTrimmedString(value, 16).toLowerCase();
  // 'manual' = user explicitly set it; 'llm' = AI analysis explicitly set it.
  // Both are treated as authoritative and bypass computeAutomaticImportance.
  if (source === 'manual') return 'manual';
  if (source === 'llm') return 'llm';
  return 'auto';
}

function normalizeHistorySource(value) {
  const source = toTrimmedString(value, 16).toLowerCase();
  if (source === 'llm' || source === 'manual') return source;
  return 'system';
}

function normalizeDescriptionChangeType(value) {
  const changeType = toTrimmedString(value, 24).toLowerCase();
  return DESCRIPTION_CHANGE_TYPES.has(changeType) ? changeType : '';
}

function normalizeImportanceSignal(value) {
  const signal = toTrimmedString(value, 24).toLowerCase();
  return IMPORTANCE_SIGNAL_TYPES.has(signal) ? signal : '';
}

function normalizeIsoTimestamp(value) {
  const raw = toTrimmedString(value, 64);
  if (!raw) return '';
  const parsed = Date.parse(raw);
  if (!Number.isFinite(parsed)) return '';
  return new Date(parsed).toISOString();
}

function normalizeDescriptionHistory(rawHistory) {
  if (!Array.isArray(rawHistory)) return [];

  return rawHistory
    .map((item) => {
      const row = toProfile(item);
      const at = normalizeIsoTimestamp(row.at || row.updatedAt || row.createdAt);
      const changeType = normalizeDescriptionChangeType(row.changeType || row.type);
      const source = normalizeHistorySource(row.source);
      const previousDescription = toTrimmedString(
        row.previousDescription || row.previous || row.before,
        2200,
      );
      const nextDescription = toTrimmedString(row.nextDescription || row.next || row.after, 2200);
      const reason = toTrimmedString(row.reason || row.note, 240);
      const versionRaw = Number(row.version);
      const version = Number.isFinite(versionRaw) ? Math.max(1, Math.floor(versionRaw)) : 0;

      if (!at || !changeType) return null;
      if (!previousDescription && !nextDescription) return null;

      return compactObject({
        at,
        version: version || undefined,
        changeType,
        source,
        previousDescription,
        nextDescription,
        reason,
      });
    })
    .filter(Boolean)
    .slice(-DESCRIPTION_HISTORY_LIMIT);
}

function normalizeImportanceHistory(rawHistory) {
  if (!Array.isArray(rawHistory)) return [];

  return rawHistory
    .map((item) => {
      const row = toProfile(item);
      const at = normalizeIsoTimestamp(row.at || row.updatedAt || row.createdAt);
      const before = normalizeImportanceValue(row.before || row.previous || row.previousImportance);
      const after = normalizeImportanceValue(row.after || row.next || row.nextImportance);
      const signal = normalizeImportanceSignal(row.signal);
      const source = normalizeHistorySource(row.source);
      const reason = toTrimmedString(row.reason || row.note, 240);
      if (!at) return null;
      if (!before && !after && !signal && !reason) return null;

      return compactObject({
        at,
        before,
        after,
        signal,
        source,
        reason,
      });
    })
    .filter(Boolean)
    .slice(-IMPORTANCE_HISTORY_LIMIT);
}

function resolveDescriptionChangeType(previousDescription, nextDescription, requestedChangeType) {
  const explicitType = normalizeDescriptionChangeType(requestedChangeType);
  if (explicitType) return explicitType;

  if (!previousDescription && nextDescription) return 'initial';
  if (previousDescription && !nextDescription) return 'update';
  if (!previousDescription && !nextDescription) return '';

  const normalizedPrev = previousDescription.replace(/\s+/g, ' ').trim();
  const normalizedNext = nextDescription.replace(/\s+/g, ' ').trim();
  if (!normalizedPrev || !normalizedNext || normalizedPrev === normalizedNext) return '';

  const growth = normalizedNext.length - normalizedPrev.length;
  const prevPrefix = normalizedPrev.slice(0, Math.min(120, normalizedPrev.length));
  const includesPrevious = normalizedNext.includes(normalizedPrev) || normalizedNext.startsWith(prevPrefix);
  if (growth >= Math.max(40, Math.floor(normalizedPrev.length * 0.15)) && includesPrevious) {
    return 'addition';
  }

  return 'update';
}

function importanceLabelToScore(label) {
  const normalized = normalizeImportanceValue(label);
  if (!normalized) return -1;
  return IMPORTANCE_SCORE_BY_LABEL[normalized];
}

function deriveImportanceSignal(beforeLabel, afterLabel, requestedSignal) {
  const explicit = normalizeImportanceSignal(requestedSignal);
  if (explicit) return explicit;

  const beforeScore = importanceLabelToScore(beforeLabel);
  const afterScore = importanceLabelToScore(afterLabel);
  if (beforeScore >= 0 && afterScore >= 0) {
    if (afterScore > beforeScore) return 'increase';
    if (afterScore < beforeScore) return 'decrease';
    return 'neutral';
  }
  return '';
}

function computeImportanceTrendStep(history, pendingSignal = '') {
  const normalizedHistory = normalizeImportanceHistory(history);
  if (!normalizedHistory.length && !normalizeImportanceSignal(pendingSignal)) return 0;

  let weightedSignal = 0;
  let weightedTotal = 0;
  const reversedHistory = normalizedHistory.slice(-14).reverse();
  for (let index = 0; index < reversedHistory.length; index += 1) {
    const row = reversedHistory[index];
    const weight = Math.pow(0.86, index);
    const signal =
      row.signal ||
      deriveImportanceSignal(row.before, row.after, '');

    let signalValue = 0;
    if (signal === 'increase') signalValue = 1;
    if (signal === 'decrease') signalValue = -1;
    if (!signalValue) continue;

    weightedSignal += signalValue * weight;
    weightedTotal += weight;
  }

  const pending = normalizeImportanceSignal(pendingSignal);
  if (pending === 'increase' || pending === 'decrease') {
    const pendingValue = pending === 'increase' ? 1 : -1;
    weightedSignal += pendingValue * 1.2;
    weightedTotal += 1.2;
  }

  if (!weightedTotal) return 0;

  const trend = weightedSignal / weightedTotal;
  if (trend >= 0.34) return 1;
  if (trend <= -0.34) return -1;
  return 0;
}

function countSignalItems(value, cap = IMPORTANCE_AUTO_FIELD_CAP) {
  const maxItems = Math.max(1, Math.floor(cap));

  if (Array.isArray(value)) {
    const uniq = new Set(
      value
        .map((item) => toTrimmedString(item, 120).toLowerCase())
        .filter(Boolean),
    );
    return Math.min(maxItems, uniq.size);
  }

  const asString = toTrimmedString(value, 240);
  if (!asString) return 0;

  const chunks = asString
    .split(/[,;\n|]+/g)
    .map((item) => item.trim())
    .filter(Boolean);

  if (!chunks.length) return 1;
  return Math.min(maxItems, chunks.length);
}

function computeAutomaticImportance(aiMetadata, options = {}) {
  const metadata = toProfile(aiMetadata);
  let score = 0;
  let maxScore = 0;

  for (const [field, weight] of Object.entries(IMPORTANCE_AUTO_WEIGHTS)) {
    maxScore += weight * IMPORTANCE_AUTO_FIELD_CAP;
    score += countSignalItems(metadata[field], IMPORTANCE_AUTO_FIELD_CAP) * weight;
  }

  if (!maxScore || score <= 0) {
    const emptyBase = 0;
    const trendStep = computeImportanceTrendStep(metadata.importance_history, options.pendingSignal);
    const nextScore = Math.max(0, Math.min(2, emptyBase + trendStep));
    return [IMPORTANCE_LABEL_BY_SCORE[nextScore]];
  }

  const ratio = score / maxScore;
  let baseScore = 0;
  if (ratio >= 0.4) baseScore = 2;
  else if (ratio >= 0.17) baseScore = 1;

  const trendStep = computeImportanceTrendStep(metadata.importance_history, options.pendingSignal);
  const adjustedScore = Math.max(0, Math.min(2, baseScore + trendStep));
  return [IMPORTANCE_LABEL_BY_SCORE[adjustedScore]];
}

function applyImportancePolicy(rawMetadata, options = {}) {
  const metadata = {
    ...toProfile(rawMetadata),
  };
  const previousMetadata = toProfile(options.previousMetadata);
  const nowIso = toTrimmedString(options.nowIso, 64) || new Date().toISOString();
  const source = normalizeHistorySource(options.source);
  const importanceReason = toTrimmedString(options.importanceReason, 240);
  const requestedSignal = normalizeImportanceSignal(options.importanceSignal);

  const previousImportance = normalizeImportanceArray(previousMetadata.importance)[0] || '';
  const historySource = previousMetadata.importance_history || metadata.importance_history;
  const importanceHistory = normalizeImportanceHistory(historySource);

  const hasDescription = toTrimmedString(metadata.description, 2200).length > 0;
  if (!hasDescription) {
    metadata.importance = [];
    metadata.importance_source = 'auto';
  } else {
    const sourceMode = normalizeImportanceSource(metadata.importance_source);
    const explicitImportance = normalizeImportanceArray(metadata.importance);
    // 'manual' and 'llm' are both authoritative explicit values — do not override
    // with computeAutomaticImportance. Only 'auto' (no explicit source) is computed.
    if ((sourceMode === 'manual' || sourceMode === 'llm') && explicitImportance.length) {
      metadata.importance = explicitImportance;
      metadata.importance_source = sourceMode;
    } else {
      metadata.importance = computeAutomaticImportance(
        {
          ...metadata,
          importance_history: importanceHistory,
        },
        { pendingSignal: requestedSignal },
      );
      metadata.importance_source = 'auto';
    }
  }

  const nextImportance = normalizeImportanceArray(metadata.importance)[0] || '';
  const resolvedSignal = deriveImportanceSignal(previousImportance, nextImportance, requestedSignal);
  const importanceChanged = previousImportance !== nextImportance;
  if (resolvedSignal && (importanceChanged || requestedSignal || importanceReason)) {
    importanceHistory.push(
      compactObject({
        at: nowIso,
        before: previousImportance,
        after: nextImportance,
        signal: resolvedSignal,
        source,
        reason: importanceReason,
      }),
    );
  }

  metadata.importance_history = importanceHistory.slice(-IMPORTANCE_HISTORY_LIMIT);
  return metadata;
}

function enrichEntityMetadata(existingMetadata, incomingMetadata, options = {}) {
  const previousMetadata = toProfile(existingMetadata);
  const currentMetadata = toProfile(incomingMetadata);
  const nextMetadata = {
    ...previousMetadata,
    ...currentMetadata,
  };
  const nowIso = new Date().toISOString();
  const source = normalizeHistorySource(options.source);
  const descriptionReason = toTrimmedString(options.descriptionReason, 240);
  const requestedDescriptionChangeType = normalizeDescriptionChangeType(options.descriptionChangeType);

  const previousDescription = toTrimmedString(previousMetadata.description, 2200);
  const nextDescription = toTrimmedString(nextMetadata.description, 2200);
  nextMetadata.description = nextDescription;

  const descriptionHistory = normalizeDescriptionHistory(previousMetadata.description_history);
  const previousDescriptionMeta = toProfile(previousMetadata.description_meta);
  const previousVersionRaw = Number(previousDescriptionMeta.version);
  let version = Number.isFinite(previousVersionRaw)
    ? Math.max(0, Math.floor(previousVersionRaw))
    : descriptionHistory.length;
  let lastChangeType = normalizeDescriptionChangeType(
    previousDescriptionMeta.lastChangeType || previousDescriptionMeta.changeType,
  );
  let lastSource = normalizeHistorySource(previousDescriptionMeta.lastSource || previousDescriptionMeta.source);
  let lastUpdatedAt = normalizeIsoTimestamp(previousDescriptionMeta.lastUpdatedAt || previousDescriptionMeta.updatedAt);
  let lastReason = toTrimmedString(previousDescriptionMeta.lastReason || previousDescriptionMeta.reason, 240);

  if (nextDescription !== previousDescription) {
    const resolvedChangeType =
      resolveDescriptionChangeType(
      previousDescription,
      nextDescription,
      requestedDescriptionChangeType,
      ) || 'update';
    const nowMs = Date.parse(nowIso);
    const lastEntry = descriptionHistory[descriptionHistory.length - 1] || null;
    const lastEntryAtMs = Date.parse(toTrimmedString(lastEntry?.at, 64));
    const mergeWithLastManualEntry =
      source === 'manual' &&
      lastEntry &&
      normalizeHistorySource(lastEntry.source) === 'manual' &&
      Number.isFinite(lastEntryAtMs) &&
      Number.isFinite(nowMs) &&
      nowMs - lastEntryAtMs <= 90_000;

    if (mergeWithLastManualEntry) {
      lastEntry.at = nowIso;
      lastEntry.changeType = resolvedChangeType;
      lastEntry.nextDescription = nextDescription;
      lastEntry.reason = descriptionReason;
      version = Number.isFinite(Number(lastEntry.version)) ? Math.max(1, Math.floor(Number(lastEntry.version))) : version;
    } else {
      version += 1;
      descriptionHistory.push(
        compactObject({
          at: nowIso,
          version,
          changeType: resolvedChangeType,
          source: lastSource || source,
          previousDescription,
          nextDescription,
          reason: descriptionReason,
        }),
      );
    }

    lastChangeType = resolvedChangeType;
    lastSource = source;
    lastUpdatedAt = nowIso;
    lastReason = descriptionReason;
  }

  nextMetadata.description_history = descriptionHistory.slice(-DESCRIPTION_HISTORY_LIMIT);
  nextMetadata.description_meta = compactObject({
    version,
    lastChangeType,
    lastSource,
    lastUpdatedAt,
    lastReason,
  });

  const autoImportanceSignal =
    normalizeImportanceSignal(options.importanceSignal) ||
    normalizeImportanceSignal(currentMetadata.importance_signal) ||
    normalizeImportanceSignal(toProfile(currentMetadata.ai_last_analysis).importanceSignal);
  const autoImportanceReason =
    toTrimmedString(options.importanceReason, 240) ||
    toTrimmedString(currentMetadata.importance_reason, 240) ||
    toTrimmedString(toProfile(currentMetadata.ai_last_analysis).importanceReason, 240);

  const withImportance = applyImportancePolicy(nextMetadata, {
    previousMetadata,
    source,
    importanceSignal: autoImportanceSignal,
    importanceReason: autoImportanceReason,
    nowIso,
  });

  delete withImportance.description_change_type;
  delete withImportance.description_change_reason;
  delete withImportance.importance_signal;
  delete withImportance.importance_reason;
  return withImportance;
}

function normalizeIncomingEntityPayload(rawPayload, options = {}) {
  const payload = rawPayload && typeof rawPayload === 'object' ? { ...rawPayload } : {};
  const hasAiMetadataPayload =
    payload.ai_metadata && typeof payload.ai_metadata === 'object' && !Array.isArray(payload.ai_metadata);
  const metadata = hasAiMetadataPayload
    ? {
      ...toProfile(payload.ai_metadata),
    }
    : {};

  if (Object.prototype.hasOwnProperty.call(metadata, 'name_mode')) {
    const normalizedNameMode = normalizeEntityNameMode(metadata.name_mode);
    if (normalizedNameMode) metadata.name_mode = normalizedNameMode;
    else delete metadata.name_mode;
  }

  if (normalizeHistorySource(options.source) === 'manual' && Object.prototype.hasOwnProperty.call(payload, 'name')) {
    const previousName = toTrimmedString(options.existingName, 120);
    const nextName = toTrimmedString(payload.name, 120);
    if (nextName && previousName !== nextName) {
      // User changed the name manually: lock this custom name.
      metadata.name_auto = false;
      metadata.name_mode = 'manual';
    }
  }

  const resolvedEntityType = toTrimmedString(options.entityType || payload.type, 32);
  if (Object.prototype.hasOwnProperty.call(metadata, 'manual_roles')) {
    if (resolvedEntityType === 'person') {
      metadata.manual_roles = normalizeManualPersonRoles(metadata.manual_roles);
    } else {
      delete metadata.manual_roles;
    }
  }
  if (Object.prototype.hasOwnProperty.call(metadata, 'manual_skills')) {
    if (resolvedEntityType === 'person') {
      metadata.manual_skills = normalizeManualPersonSkills(metadata.manual_skills);
    } else {
      delete metadata.manual_skills;
    }
  }

  if (!Object.keys(metadata).length) {
    return payload;
  }

  if (Object.prototype.hasOwnProperty.call(metadata, 'importance') && !metadata.importance_source) {
    metadata.importance_source = 'manual';
  }

  payload.ai_metadata = enrichEntityMetadata(options.existingMetadata, metadata, {
    source: options.source,
  });
  return payload;
}

function stripEntityUpdateControlFields(rawPayload) {
  if (!rawPayload || typeof rawPayload !== 'object') {
    return {};
  }

  const payload = { ...rawPayload };
  delete payload.expectedUpdatedAt;
  delete payload.expectedCanvasVersion;
  return payload;
}

function readExpectedEntityUpdatedAt(rawPayload) {
  if (!rawPayload || typeof rawPayload !== 'object') {
    return '';
  }

  const normalized = toTrimmedString(rawPayload.expectedUpdatedAt, 80);
  if (!normalized) return '';

  const parsed = Date.parse(normalized);
  if (!Number.isFinite(parsed)) {
    return '';
  }

  return new Date(parsed).toISOString();
}

function readExpectedCanvasVersion(rawPayload) {
  if (!rawPayload || typeof rawPayload !== 'object') {
    return '';
  }

  return toTrimmedString(rawPayload.expectedCanvasVersion, 240);
}

function normalizeMineFlagsInPayload(payload, entityType, options = {}) {
  const mode = options.mode === 'update' ? 'update' : 'create';
  if (!payload || typeof payload !== 'object') {
    return payload;
  }

  const hasIsMine = Object.prototype.hasOwnProperty.call(payload, 'is_mine');
  const hasIsMe = Object.prototype.hasOwnProperty.call(payload, 'is_me');

  if (entityType === 'person') {
    if (mode === 'create') {
      const nextIsMe = toBooleanFlag(payload.is_me, false);
      const nextIsMine = toBooleanFlag(payload.is_mine, false);
      payload.is_me = nextIsMe;
      payload.is_mine = nextIsMe ? true : nextIsMine;
      return payload;
    }

    if (hasIsMe) {
      const nextIsMe = toBooleanFlag(payload.is_me, false);
      payload.is_me = nextIsMe;
      if (nextIsMe) {
        payload.is_mine = true;
      }
    }

    if (hasIsMine) {
      payload.is_mine = toBooleanFlag(payload.is_mine, false);
    }

    if (hasIsMe && payload.is_me === true) {
      payload.is_mine = true;
    }

    return payload;
  }

  payload.is_me = false;

  if (mode === 'create' || hasIsMine) {
    payload.is_mine = toBooleanFlag(payload.is_mine, false);
  }

  return payload;
}

function normalizeEntityAnalysisFields(entityType, rawFields) {
  const source = toProfile(rawFields);
  const allowed = new Set(getEntityAnalyzerFields(entityType));
  const normalized = {};

  for (const field of allowed) {
    if (field === 'importance') {
      normalized.importance = normalizeImportanceArray(source.importance);
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

  // Fallback: scan balanced JSON objects and return the first parsable one.
  const candidates = [];
  for (let start = 0; start < trimmed.length; start += 1) {
    if (trimmed[start] !== '{') continue;

    let depth = 0;
    let inString = false;
    let escaped = false;

    for (let index = start; index < trimmed.length; index += 1) {
      const ch = trimmed[index];
      if (inString) {
        if (escaped) {
          escaped = false;
        } else if (ch === '\\') {
          escaped = true;
        } else if (ch === '"') {
          inString = false;
        }
        continue;
      }

      if (ch === '"') {
        inString = true;
        continue;
      }

      if (ch === '{') {
        depth += 1;
        continue;
      }

      if (ch === '}') {
        depth -= 1;
        if (depth === 0) {
          const candidate = trimmed.slice(start, index + 1).trim();
          if (candidate) candidates.push(candidate);
          break;
        }
      }
    }

    if (candidates.length >= 24) break;
  }

  if (candidates.length) {
    const byLengthDesc = candidates.sort((a, b) => b.length - a.length);
    for (const candidate of byLengthDesc) {
      try {
        const parsed = JSON.parse(candidate);
        if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
          return parsed;
        }
      } catch {
        // continue
      }
    }
  }

  throw Object.assign(new Error('AI response is not valid JSON'), { status: 502 });
}

function normalizeEntityAnalysisOutput(entityType, rawResponse) {
  const parsed = toProfile(rawResponse);
  const status = toTrimmedString(parsed.status, 32) === 'need_clarification' ? 'need_clarification' : 'ready';
  const description = toTrimmedString(parsed.description, 2200);
  const changeType = normalizeDescriptionChangeType(parsed.changeType);
  const changeReason = toTrimmedString(parsed.changeReason, 240);
  const fields = normalizeEntityAnalysisFields(entityType, parsed.fields);
  const importanceSignal = normalizeImportanceSignal(parsed.importanceSignal);
  const importanceReason = toTrimmedString(parsed.importanceReason, 240);
  const clarifyingQuestions = normalizeEntityFieldArray(parsed.clarifyingQuestions, {
    maxItems: 3,
    itemMaxLength: 220,
  });
  const ignoredNoise = normalizeEntityFieldArray(parsed.ignoredNoise, {
    maxItems: 20,
    itemMaxLength: 120,
  });
  const confidence = normalizeConfidence(parsed.confidence);
  const suggestedName = toTrimmedString(parsed.suggestedName, 64);

  return {
    status,
    description,
    changeType,
    changeReason,
    fields,
    importanceSignal,
    importanceReason,
    clarifyingQuestions,
    ignoredNoise,
    confidence,
    suggestedName,
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
      returnDocument: 'after',
      setDefaultsOnInsert: true,
    },
  ).lean();

  return saved;
}

function buildEntityMetadataPatch(entityType, existingMetadata, analysis) {
  const nextMetadata = {
    ...toProfile(existingMetadata),
  };
  const manualImportanceWasSet = normalizeImportanceSource(nextMetadata.importance_source) === 'manual';

  if (typeof analysis.description === 'string') {
    nextMetadata.description = analysis.description;
  }

  const allowedFields = getEntityAnalyzerFields(entityType);
  const normalizedFields = normalizeEntityAnalysisFields(entityType, analysis.fields);
  for (const field of allowedFields) {
    if (field === 'importance' && manualImportanceWasSet) {
      // Preserve the user's explicit manual importance — do not overwrite with LLM value.
      continue;
    }
    nextMetadata[field] = normalizedFields[field] || [];
  }

  // When LLM explicitly returned a non-empty importance value, mark it as 'llm' source
  // so applyImportancePolicy treats it as authoritative and does NOT override it with
  // computeAutomaticImportance. Only empty-importance responses fall through to auto-compute.
  if (allowedFields.includes('importance') && !manualImportanceWasSet) {
    const llmImportance = normalizedFields.importance;
    if (Array.isArray(llmImportance) && llmImportance.length) {
      nextMetadata.importance_source = 'llm';
    }
  }

  nextMetadata.ai_last_analysis = {
    status: analysis.status,
    changeType: normalizeDescriptionChangeType(analysis.changeType),
    changeReason: toTrimmedString(analysis.changeReason, 240),
    importanceSignal: normalizeImportanceSignal(analysis.importanceSignal),
    importanceReason: toTrimmedString(analysis.importanceReason, 240),
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

  return enrichEntityMetadata(existingMetadata, nextMetadata, {
    source: 'llm',
    descriptionChangeType: analysis.changeType,
    descriptionReason: analysis.changeReason,
    importanceSignal: analysis.importanceSignal,
    importanceReason: analysis.importanceReason,
  });
}

function summarizeEntityForAgent(entity) {
  const profile = toProfile(entity.profile);
  const aiMetadata = toProfile(entity.ai_metadata);
  const logo = toProfile(profile.logo);
  const fullDescription = toTrimmedString(aiMetadata.description, 2400);
  const descriptionHead = toTrimmedString(fullDescription, 900);
  const descriptionTail =
    fullDescription.length > 900 ? toTrimmedTailString(fullDescription, 520) : '';

  return compactObject({
    id: String(entity._id),
    type: entity.type,
    name: toTrimmedString(entity.name, 120) || '(без названия)',
    description: descriptionHead,
    descriptionTail,
    descriptionLength: fullDescription.length || undefined,
    tags: toStringArray(aiMetadata.tags, 8),
    markers: toStringArray(aiMetadata.markers, 6),
    skills: toStringArray(aiMetadata.skills, 8),
    roles: mergeEntityRoleValues(aiMetadata.roles, aiMetadata.manual_roles, 8),
    importance: toStringArray(aiMetadata.importance, 1, 24),
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
    updatedAt: entity.updatedAt || '',
  });
}

function buildProjectConnections(canvasData, entitiesById) {
  function resolveConnectionDirection(edge, from, to) {
    if (edge.arrowLeft && !edge.arrowRight) {
      return {
        relationMode: 'directed',
        direction: 'target_to_source',
        directedFrom: to,
        directedTo: from,
      };
    }

    if (!edge.arrowLeft && edge.arrowRight) {
      return {
        relationMode: 'directed',
        direction: 'source_to_target',
        directedFrom: from,
        directedTo: to,
      };
    }

    return {
      relationMode: 'equivalent',
      direction: edge.arrowLeft && edge.arrowRight ? 'bidirectional' : 'equivalent',
      directedFrom: '',
      directedTo: '',
    };
  }

  const nodeEntityByNodeId = new Map();
  for (const node of canvasData.nodes) {
    if (!node.id || !node.entityId) continue;
    nodeEntityByNodeId.set(node.id, node.entityId);
  }

  const anchorNameById = new Map();
  for (const entity of entitiesById) {
    anchorNameById.set(String(entity._id), toTrimmedString(entity.name, 120) || '(без названия)');
  }

  for (const node of canvasData.nodes) {
    const entityId = nodeEntityByNodeId.get(node.id);
    const name = entityId ? anchorNameById.get(entityId) : '';
    if (!name) continue;
    anchorNameById.set(node.id, name);
  }

  for (const group of Array.isArray(canvasData.groups) ? canvasData.groups : []) {
    if (!group?.id) continue;
    anchorNameById.set(group.id, toTrimmedString(group.name, 120) || 'Группа');
  }

  return canvasData.edges
    .map((edge) => {
      const from = anchorNameById.get(edge.source);
      const to = anchorNameById.get(edge.target);
      if (!from || !to) return null;

      return compactObject({
        from,
        to,
        label: toTrimmedString(edge.label, 80),
        color: toTrimmedString(edge.color, 32),
        ...resolveConnectionDirection(edge, from, to),
        arrows: {
          source: Boolean(edge.arrowLeft),
          target: Boolean(edge.arrowRight),
        },
      });
    })
    .filter(Boolean)
    .slice(0, 180);
}

function buildProjectGroups(canvasData, entitiesById) {
  const entityById = new Map(
    (Array.isArray(entitiesById) ? entitiesById : []).map((entity) => [String(entity?._id), entity]),
  );
  const entityNameByNodeId = new Map();

  for (const node of canvasData.nodes) {
    if (!node?.id || !node?.entityId) continue;
    const entity = entityById.get(node.entityId);
    if (!entity) continue;
    entityNameByNodeId.set(node.id, toTrimmedString(entity.name, 120) || '(без названия)');
  }

  return (Array.isArray(canvasData.groups) ? canvasData.groups : [])
    .map((group) => {
      const members = group.nodeIds
        .map((nodeId) => entityNameByNodeId.get(nodeId))
        .filter(Boolean)
        .slice(0, 24);

      if (members.length < 2) return null;

      return compactObject({
        id: toTrimmedString(group.id, 120),
        name: toTrimmedString(group.name, 120) || 'Группа',
        color: toTrimmedString(group.color, 24),
        members,
      });
    })
    .filter(Boolean)
    .slice(0, 80);
}

async function resolveAgentScopeContext(ownerId, rawScope) {
  const scope = toProfile(rawScope);
  const scopeType = toTrimmedString(scope.type, 24).toLowerCase();
  const preserveFullGraph = scope.preserveFullGraph === true;

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
      sourceEntities: entities,
      sourceNodes: [],
      sourceEdges: [],
      sourceGroups: [],
      connections: [],
      groups: [],
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
      .select({ _id: 1, name: 1, canvas_data: 1, ai_metadata: 1 })
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

    const scopedEntityIds = preserveFullGraph
      ? uniqueEntityIds
      : uniqueEntityIds.slice(0, AI_CONTEXT_ENTITY_LIMIT);
    const entities = scopedEntityIds.length
      ? await Entity.find({
          owner_id: ownerId,
          _id: { $in: scopedEntityIds },
        }).lean()
      : [];

    const entityById = new Map(entities.map((entity) => [String(entity._id), entity]));
    const scopedEntityIdSet = new Set(scopedEntityIds);
    const sharedEntityIds = new Set();

    if (!preserveFullGraph && scopedEntityIds.length) {
      const siblingProjects = await Entity.find(
        {
          owner_id: ownerId,
          type: 'project',
          _id: { $ne: project._id },
          'canvas_data.nodes.entityId': { $in: scopedEntityIds },
        },
        { _id: 1, canvas_data: 1 },
      ).lean();

      for (const siblingProject of siblingProjects) {
        const siblingCanvas = normalizeProjectCanvasData(siblingProject.canvas_data);
        for (const node of siblingCanvas.nodes) {
          const entityId = toTrimmedString(node.entityId, 80);
          if (!entityId) continue;
          if (!scopedEntityIdSet.has(entityId)) continue;
          sharedEntityIds.add(entityId);
        }
      }
    }

    const currentProjectId = String(project._id);
    const sourceEntities = scopedEntityIds
      .map((id) => entityById.get(id))
      .filter(Boolean);
    const orderedEntities = preserveFullGraph
      ? sourceEntities
      : scopedEntityIds
        .map((id) => entityById.get(id))
        .filter(Boolean)
        .filter((entity) => {
          const entityId = String(entity._id);
          if (entity.type === 'project' && entityId !== currentProjectId) {
            return false;
          }
          if (sharedEntityIds.has(entityId) && entityId !== currentProjectId) {
            return false;
          }
          return true;
        });

    const connections = buildProjectConnections(canvasData, orderedEntities);
    const groups = buildProjectGroups(canvasData, orderedEntities);

    return {
      scopeType: 'project',
      entityType: '',
      scopeName: toTrimmedString(project.name, 140) || 'Без названия',
      projectId: String(project._id),
      projectName: toTrimmedString(project.name, 140) || 'Без названия',
      totalEntities: sourceEntities.length,
      projectMetadata: toProfile(project.ai_metadata),
      entities: orderedEntities,
      sourceEntities,
      sourceNodes: canvasData.nodes,
      sourceEdges: canvasData.edges,
      sourceGroups: canvasData.groups,
      connections,
      groups,
    };
  }

  throw Object.assign(new Error('Invalid scope type'), { status: 400 });
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

function getSessionTokenFromRequest(req, options = {}) {
  const allowQueryToken = options?.allowQueryToken === true;
  const authHeader = req.headers.authorization;
  if (typeof authHeader === 'string' && authHeader.startsWith('Bearer ')) {
    const token = authHeader.slice('Bearer '.length).trim();
    if (token) return token;
  }

  const cookieToken = req.cookies?.[SESSION_COOKIE_NAME];
  if (typeof cookieToken === 'string' && cookieToken.trim()) {
    return cookieToken.trim();
  }

  if (allowQueryToken) {
    const queryValue = Array.isArray(req.query?.sessionToken)
      ? req.query.sessionToken[0]
      : req.query?.sessionToken;
    if (typeof queryValue === 'string' && queryValue.trim()) {
      return queryValue.trim().slice(0, 4096);
    }
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
      returnDocument: 'after',
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

function normalizeWhatsappJidToPhone(jid) {
  const raw = toTrimmedString(jid, 200);
  if (!raw || !raw.includes('@')) return '';

  const userPart = raw.split('@')[0] || '';
  const cleanUser = userPart.split(':')[0] || '';
  return normalizePhone(cleanUser);
}

function buildBaileysImportContacts(session) {
  const collected = new Map();

  function pushContact(rawJid, rawName = '', rawStatus = '', rawPhone = '') {
    const jid = toTrimmedString(rawJid, 200);
    if (!jid) return;
    if (jid.endsWith('@g.us') || jid.endsWith('@broadcast') || jid.endsWith('@newsletter')) return;

    const phone = normalizePhone(toTrimmedString(rawPhone, 80)) || normalizeWhatsappJidToPhone(jid);
    const key = phone ? `${phone}@s.whatsapp.net` : jid;

    const existing = collected.get(key);
    const nextName = toTrimmedString(rawName, 120);
    const nextStatus = toTrimmedString(rawStatus, 1200);

    if (existing) {
      if (!existing.name && nextName) existing.name = nextName;
      if (!existing.status && nextStatus) existing.status = nextStatus;
      return;
    }

    collected.set(key, {
      jid,
      phone,
      name: nextName,
      status: nextStatus,
    });
  }

  const contactsSource = session?.store?.contacts;
  if (contactsSource && typeof contactsSource === 'object') {
    const contactEntries =
      contactsSource instanceof Map
        ? Array.from(contactsSource.entries())
        : Array.isArray(contactsSource)
          ? contactsSource.map((item, index) => [toTrimmedString(item?.id || item?.jid, 200) || `${index}`, item])
          : Object.entries(contactsSource);

    for (const [jid, contact] of contactEntries) {
      const row = toProfile(contact);
      pushContact(
        jid || row.id || row.jid,
        row.name || row.notify || row.verifiedName || row.shortName || row.pushname,
        row.status || row.description || row.about,
        row.phone || row.phoneNumber || row.number || row.waId || row.user || row.id?.user,
      );
    }
  }

  const chatsSource = session?.store?.chats;
  if (chatsSource && typeof chatsSource === 'object') {
    const chatEntries =
      chatsSource instanceof Map
        ? Array.from(chatsSource.entries())
        : Array.isArray(chatsSource)
          ? chatsSource.map((item, index) => [toTrimmedString(item?.id || item?.jid, 200) || `${index}`, item])
          : Object.entries(chatsSource);

    for (const [jid, chat] of chatEntries) {
      const row = toProfile(chat);
      pushContact(
        jid || row.id || row.jid,
        row.name || row.notify || row.subject || row.pushname,
        '',
        row.phone || row.phoneNumber || row.number || row.waId || row.user || row.id?.user,
      );
    }
  }

  if (session?.contactsMirror instanceof Map) {
    for (const mirror of session.contactsMirror.values()) {
      const row = toProfile(mirror);
      pushContact(row.jid || row.id, row.name || row.notify || row.verifiedName, row.status, row.phone || row.phoneNumber);
    }
  }

  if (session?.chatsMirror instanceof Map) {
    for (const mirror of session.chatsMirror.values()) {
      const row = toProfile(mirror);
      pushContact(row.jid || row.id, row.name || row.notify || row.subject, '', row.phone || row.phoneNumber);
    }
  }

  return Array.from(collected.values());
}

function getBaileysCollectionSize(collection) {
  if (!collection) return 0;
  if (collection instanceof Map) return collection.size;
  if (Array.isArray(collection)) return collection.length;
  if (typeof collection === 'object') return Object.keys(collection).length;
  return 0;
}

function mergeBaileysContactMirror(session, rawContact) {
  if (!(session?.contactsMirror instanceof Map)) return;
  const row = toProfile(rawContact);
  const jid = toTrimmedString(row.id || row.jid || row.lid || row.phoneNumber, 220);
  if (!jid) return;

  const existing = toProfile(session.contactsMirror.get(jid));
  session.contactsMirror.set(jid, {
    id: jid,
    jid,
    phoneNumber: toTrimmedString(row.phoneNumber || existing.phoneNumber, 80),
    phone: toTrimmedString(row.phone || row.phoneNumber || existing.phone, 80),
    name: toTrimmedString(row.name || existing.name, 120),
    notify: toTrimmedString(row.notify || existing.notify, 120),
    verifiedName: toTrimmedString(row.verifiedName || existing.verifiedName, 120),
    status: toTrimmedString(row.status || existing.status, 1200),
  });
}

function mergeBaileysChatMirror(session, rawChat) {
  if (!(session?.chatsMirror instanceof Map)) return;
  const row = toProfile(rawChat);
  const jid = toTrimmedString(row.id || row.jid, 220);
  if (!jid) return;

  const existing = toProfile(session.chatsMirror.get(jid));
  session.chatsMirror.set(jid, {
    id: jid,
    jid,
    phone: toTrimmedString(row.phone || existing.phone, 80),
    name: toTrimmedString(row.name || row.notify || row.subject || existing.name, 120),
  });
}

async function waitForBaileysPendingNotifications(session, timeoutMs = 15_000, pollMs = 500) {
  if (!session) return false;
  if (session.receivedPendingNotifications) return true;

  const startedAt = Date.now();
  while (Date.now() - startedAt < timeoutMs) {
    if (session.receivedPendingNotifications) {
      return true;
    }
    await delay(pollMs);
  }
  return false;
}

async function syncBaileysContactsWithRetry(session, options = {}) {
  const timeoutMs = Math.max(3_000, Number(options.timeoutMs) || 18_000);
  const pollMs = Math.max(250, Number(options.pollMs) || 900);
  const minContacts = Math.max(1, Number(options.minContacts) || 1);

  const socket = session?.client;
  if (!socket) {
    appendWhatsappSessionLog(session, 'import.sync.skip', { reason: 'socket_missing' });
    return [];
  }

  appendWhatsappSessionLog(session, 'import.sync.start', {
    timeoutMs,
    pollMs,
    minContacts,
    storeContacts: getBaileysCollectionSize(session?.store?.contacts),
    storeChats: getBaileysCollectionSize(session?.store?.chats),
    mirroredContacts: session?.contactsMirror instanceof Map ? session.contactsMirror.size : 0,
    mirroredChats: session?.chatsMirror instanceof Map ? session.chatsMirror.size : 0,
  });

  if (typeof socket.resyncAppState === 'function') {
    try {
      await socket.resyncAppState(
        ['critical_block', 'critical_unblock_low', 'regular_high', 'regular_low', 'regular'],
        false,
      );
      appendWhatsappSessionLog(session, 'import.sync.resync', { result: 'ok' });
    } catch {
      appendWhatsappSessionLog(session, 'import.sync.resync', { result: 'failed' });
      // Ignore sync errors and fallback to store polling.
    }
  }

  const startedAt = Date.now();
  let latest = buildBaileysImportContacts(session);
  let polls = 0;
  while (Date.now() - startedAt < timeoutMs) {
    if (latest.length >= minContacts) {
      appendWhatsappSessionLog(session, 'import.sync.done', { contacts: latest.length, polls });
      return latest;
    }
    await delay(pollMs);
    latest = buildBaileysImportContacts(session);
    polls += 1;
    if (polls % 3 === 0) {
      appendWhatsappSessionLog(session, 'import.sync.poll', {
        polls,
        contacts: latest.length,
        storeContacts: getBaileysCollectionSize(session?.store?.contacts),
        storeChats: getBaileysCollectionSize(session?.store?.chats),
        mirroredContacts: session?.contactsMirror instanceof Map ? session.contactsMirror.size : 0,
        mirroredChats: session?.chatsMirror instanceof Map ? session.chatsMirror.size : 0,
      });
    }
  }

  appendWhatsappSessionLog(session, 'import.sync.timeout', {
    contacts: latest.length,
    polls,
    storeContacts: getBaileysCollectionSize(session?.store?.contacts),
    storeChats: getBaileysCollectionSize(session?.store?.chats),
    mirroredContacts: session?.contactsMirror instanceof Map ? session.contactsMirror.size : 0,
    mirroredChats: session?.chatsMirror instanceof Map ? session.chatsMirror.size : 0,
  });
  return latest;
}

function extractNormalizedPhonesFromProfile(rawProfile) {
  const profile = toProfile(rawProfile);
  const phones = new Set();

  const directPhone = normalizePhone(toTrimmedString(profile.phone, 80));
  if (directPhone) {
    phones.add(directPhone);
  }

  const rawPhoneList = Array.isArray(profile.phones) ? profile.phones : [];
  for (const value of rawPhoneList) {
    const normalized = normalizePhone(toTrimmedString(value, 80));
    if (normalized) {
      phones.add(normalized);
    }
  }

  return Array.from(phones);
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

function isWhatsappWebJsAvailable() {
  return Boolean(whatsappWeb && QRCode);
}

function isWhatsappBaileysAvailable() {
  return Boolean(whatsappBaileys && QRCode);
}

function resolveWhatsappConnector() {
  if (WHATSAPP_CONNECTOR === 'baileys') {
    if (isWhatsappBaileysAvailable()) return 'baileys';
    if (isWhatsappWebJsAvailable()) return 'webjs';
    return '';
  }

  if (WHATSAPP_CONNECTOR === 'webjs') {
    if (isWhatsappWebJsAvailable()) return 'webjs';
    if (isWhatsappBaileysAvailable()) return 'baileys';
    return '';
  }

  if (isWhatsappBaileysAvailable()) return 'baileys';
  if (isWhatsappWebJsAvailable()) return 'webjs';
  return '';
}

function isWhatsappIntegrationAvailable() {
  return Boolean(resolveWhatsappConnector());
}

function sanitizeWhatsappLogValue(value, depth = 0) {
  if (value === null || value === undefined) return value;
  if (typeof value === 'number' || typeof value === 'boolean') return value;
  if (typeof value === 'string') return value.length > 320 ? `${value.slice(0, 320)}...` : value;
  if (depth >= 3) return '[nested]';
  if (Array.isArray(value)) {
    return value.slice(0, 12).map((item) => sanitizeWhatsappLogValue(item, depth + 1));
  }
  if (typeof value === 'object') {
    const next = {};
    const entries = Object.entries(value).slice(0, 24);
    for (const [key, entryValue] of entries) {
      next[key] = sanitizeWhatsappLogValue(entryValue, depth + 1);
    }
    return next;
  }
  return String(value);
}

function appendWhatsappSessionLog(session, step, payload = {}) {
  if (!session || typeof session !== 'object') return;
  if (!Array.isArray(session.debugLog)) {
    session.debugLog = [];
  }

  session.debugLog.push({
    ts: new Date().toISOString(),
    step: toTrimmedString(step, 80) || 'event',
    data: sanitizeWhatsappLogValue(payload),
  });

  if (session.debugLog.length > WHATSAPP_DEBUG_LOG_LIMIT) {
    session.debugLog = session.debugLog.slice(-WHATSAPP_DEBUG_LOG_LIMIT);
  }
}

function delay(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, Math.max(0, Number(ms) || 0));
  });
}

function toWhatsappSessionStatus(session) {
  const importProgress =
    session?.importProgress && typeof session.importProgress === 'object'
      ? {
          stage: toTrimmedString(session.importProgress.stage, 32),
          note: toTrimmedString(session.importProgress.note, 220),
          total: Number(session.importProgress.total) || 0,
          processed: Number(session.importProgress.processed) || 0,
          percent: Math.max(0, Math.min(100, Number(session.importProgress.percent) || 0)),
        }
      : null;
  const backgroundImport =
    session?.backgroundImport && typeof session.backgroundImport === 'object'
      ? {
          state: toTrimmedString(session.backgroundImport.state, 24) || 'idle',
          includeImages: session.backgroundImport.includeImages !== false,
          overwriteNames: session.backgroundImport.overwriteNames === true,
          cursor: Math.max(0, Number(session.backgroundImport.cursor) || 0),
          total: Math.max(0, Number(session.backgroundImport.total) || 0),
          imported: Math.max(0, Number(session.backgroundImport.imported) || 0),
          matched: Math.max(0, Number(session.backgroundImport.matched) || 0),
          newWithName: Math.max(0, Number(session.backgroundImport.newWithName) || 0),
          newWithoutName: Math.max(0, Number(session.backgroundImport.newWithoutName) || 0),
          importedWithImage: Math.max(0, Number(session.backgroundImport.importedWithImage) || 0),
          updatedNames: Math.max(0, Number(session.backgroundImport.updatedNames) || 0),
          updatedImages: Math.max(0, Number(session.backgroundImport.updatedImages) || 0),
          batchSize: Math.max(1, Number(session.backgroundImport.batchSize) || WHATSAPP_IMPORT_BATCH_SIZE),
          startedAt: toTrimmedString(session.backgroundImport.startedAt, 80),
          updatedAt: toTrimmedString(session.backgroundImport.updatedAt, 80),
          endedAt: toTrimmedString(session.backgroundImport.endedAt, 80),
          error: toTrimmedString(session.backgroundImport.error, 260),
        }
      : null;

  return {
    sessionId: session.id,
    status: session.status,
    connector: session.connector || '',
    receivedPendingNotifications: Boolean(session.receivedPendingNotifications),
    qrCodeDataUrl: session.qrCodeDataUrl || '',
    error: session.error || '',
    createdAt: session.createdAt,
    updatedAt: session.updatedAt,
    lastImportedAt: session.lastImportedAt || '',
    mirroredContacts: session?.contactsMirror instanceof Map ? session.contactsMirror.size : 0,
    mirroredChats: session?.chatsMirror instanceof Map ? session.chatsMirror.size : 0,
    debugLogCount: Array.isArray(session.debugLog) ? session.debugLog.length : 0,
    ...(Array.isArray(session.debugLog) && session.debugLog.length ? { lastLog: session.debugLog[session.debugLog.length - 1] } : {}),
    ...(importProgress ? { importProgress } : {}),
    ...(backgroundImport ? { backgroundImport } : {}),
  };
}

function clearWhatsappSessionInitTimer(session) {
  if (session?.initTimer) {
    clearTimeout(session.initTimer);
    session.initTimer = null;
  }
}

function clearWhatsappSessionIdleTimer(session) {
  if (session?.idleTimer) {
    clearTimeout(session.idleTimer);
    session.idleTimer = null;
  }
}

function clearWhatsappSessionReconnectTimer(session) {
  if (session?.reconnectTimer) {
    clearTimeout(session.reconnectTimer);
    session.reconnectTimer = null;
  }
}

function scheduleWhatsappSessionIdleTimer(ownerId, session) {
  clearWhatsappSessionIdleTimer(session);
  if (!ownerId || !session) return;
  if (!WHATSAPP_SESSION_IDLE_TIMEOUT_MS) return;

  session.idleTimer = setTimeout(() => {
    stopOwnerWhatsappSession(ownerId, 'Session timed out due to inactivity').catch(() => {
      // Ignore cleanup errors for background timeout task.
    });
  }, WHATSAPP_SESSION_IDLE_TIMEOUT_MS);

  if (typeof session.idleTimer?.unref === 'function') {
    session.idleTimer.unref();
  }
}

function touchWhatsappSession(session, ownerId = '') {
  session.updatedAt = new Date().toISOString();
  if (ownerId) {
    scheduleWhatsappSessionIdleTimer(ownerId, session);
  }
}

function getOwnerWhatsappSession(ownerId) {
  return whatsappSessionsByOwner.get(ownerId) || null;
}

function getActiveWhatsappSessionCount() {
  let count = 0;
  for (const session of whatsappSessionsByOwner.values()) {
    if (session && ['initializing', 'qr', 'ready', 'importing'].includes(session.status)) {
      count += 1;
    }
  }
  return count;
}

async function stopOwnerWhatsappSession(ownerId, reason = '') {
  const session = getOwnerWhatsappSession(ownerId);
  if (!session) return;
  const backgroundImport = ensureWhatsappBackgroundImportState(session);
  if (backgroundImport) {
    backgroundImport.stopRequested = true;
    backgroundImport.pauseRequested = false;
  }

  appendWhatsappSessionLog(session, 'session.stop', { reason: reason || 'manual' });
  clearWhatsappSessionInitTimer(session);
  clearWhatsappSessionIdleTimer(session);
  clearWhatsappSessionReconnectTimer(session);

  if (backgroundImport?.workerPromise) {
    try {
      await Promise.race([
        backgroundImport.workerPromise,
        delay(2_000),
      ]);
    } catch {
      // Ignore background import shutdown errors.
    }
  }

  whatsappSessionsByOwner.delete(ownerId);

  if (session.client) {
    try {
      if (session.connector === 'baileys') {
        if (session.client.ws && typeof session.client.ws.close === 'function') {
          session.client.ws.close();
        }
        if (typeof session.client.end === 'function') {
          session.client.end(new Error('Session stopped'));
        }
      } else if (typeof session.client.destroy === 'function') {
        await session.client.destroy();
      }
    } catch {
      // Ignore client destroy errors.
    }
  }

  if (session.connectionListener && session.client?.ev && typeof session.client.ev.off === 'function') {
    try {
      session.client.ev.off('connection.update', session.connectionListener);
    } catch {
      // Ignore listener cleanup errors.
    }
  }

  if (session.credsListener && session.client?.ev && typeof session.client.ev.off === 'function') {
    try {
      session.client.ev.off('creds.update', session.credsListener);
    } catch {
      // Ignore listener cleanup errors.
    }
  }

  if (session.historyListener && session.client?.ev && typeof session.client.ev.off === 'function') {
    try {
      session.client.ev.off('messaging-history.set', session.historyListener);
    } catch {
      // Ignore listener cleanup errors.
    }
  }

  if (session.contactsUpsertListener && session.client?.ev && typeof session.client.ev.off === 'function') {
    try {
      session.client.ev.off('contacts.upsert', session.contactsUpsertListener);
    } catch {
      // Ignore listener cleanup errors.
    }
  }

  if (session.contactsUpdateListener && session.client?.ev && typeof session.client.ev.off === 'function') {
    try {
      session.client.ev.off('contacts.update', session.contactsUpdateListener);
    } catch {
      // Ignore listener cleanup errors.
    }
  }

  if (session.chatsUpsertListener && session.client?.ev && typeof session.client.ev.off === 'function') {
    try {
      session.client.ev.off('chats.upsert', session.chatsUpsertListener);
    } catch {
      // Ignore listener cleanup errors.
    }
  }

  session.status = 'disconnected';
  session.error = reason || session.error || 'Session stopped';
  touchWhatsappSession(session);
}

function createBaseWhatsappSession(ownerId, connector) {
  return {
    id: createWhatsappSessionId(),
    ownerId,
    connector,
    status: 'initializing',
    qrCodeDataUrl: '',
    error: '',
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    lastImportedAt: '',
    idleTimer: null,
    initTimer: null,
    reconnectTimer: null,
    restartAttempts: 0,
    importProgress: null,
    debugLog: [],
    receivedPendingNotifications: false,
    contactsMirror: new Map(),
    chatsMirror: new Map(),
    client: null,
    store: null,
    authDir: '',
    authResetAttempted: false,
    authResetInProgress: false,
    connectionListener: null,
    credsListener: null,
    historyListener: null,
    contactsUpsertListener: null,
    contactsUpdateListener: null,
    chatsUpsertListener: null,
    backgroundImport: {
      state: 'idle',
      includeImages: true,
      overwriteNames: false,
      cursor: 0,
      total: 0,
      imported: 0,
      matched: 0,
      newWithName: 0,
      newWithoutName: 0,
      importedWithImage: 0,
      updatedNames: 0,
      updatedImages: 0,
      batchSize: WHATSAPP_IMPORT_BATCH_SIZE,
      startedAt: '',
      updatedAt: '',
      endedAt: '',
      error: '',
      stopRequested: false,
      pauseRequested: false,
      workerPromise: null,
    },
  };
}

function attachWhatsappInitTimeout(ownerId, session, timeoutMessage) {
  session.initTimer = setTimeout(() => {
    if (session.status !== 'initializing') {
      return;
    }

    session.status = 'error';
    session.error = timeoutMessage;
    touchWhatsappSession(session, ownerId);

    if (session.client) {
      if (session.connector === 'baileys') {
        try {
          if (session.client.ws && typeof session.client.ws.close === 'function') {
            session.client.ws.close();
          }
        } catch {
          // Ignore close errors.
        }
      } else if (typeof session.client.destroy === 'function') {
        session.client.destroy().catch(() => {
          // Ignore cleanup error.
        });
      }
    }
  }, WHATSAPP_INIT_TIMEOUT_MS);

  if (typeof session.initTimer?.unref === 'function') {
    session.initTimer.unref();
  }
}

async function ensureOwnerWhatsappSessionWebJs(ownerId) {
  if (IS_PRODUCTION && !PUPPETEER_BROWSER_WS_ENDPOINT && !WHATSAPP_ALLOW_LOCAL_CHROME) {
    throw Object.assign(
      new Error(
        'WhatsApp connector is disabled on this backend instance. Configure PUPPETEER_BROWSER_WS_ENDPOINT (remote browser) or set WHATSAPP_ALLOW_LOCAL_CHROME=true on a high-memory instance.',
      ),
      { status: 503 },
    );
  }

  const { Client, LocalAuth } = whatsappWeb;
  const ownerSessionKey = sanitizeOwnerSessionKey(ownerId);
  const puppeteerOptions = {
    headless: true,
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--disable-gpu',
      '--disable-extensions',
      '--disable-background-networking',
      '--renderer-process-limit=1',
      '--no-zygote',
    ],
  };
  if (PUPPETEER_BROWSER_WS_ENDPOINT) {
    puppeteerOptions.browserWSEndpoint = PUPPETEER_BROWSER_WS_ENDPOINT;
    delete puppeteerOptions.executablePath;
    delete puppeteerOptions.args;
  }
  if (!PUPPETEER_BROWSER_WS_ENDPOINT && toTrimmedString(process.env.PUPPETEER_EXECUTABLE_PATH, 2048)) {
    puppeteerOptions.executablePath = process.env.PUPPETEER_EXECUTABLE_PATH;
  }

  const session = createBaseWhatsappSession(ownerId, 'webjs');
  const client = new Client({
    authStrategy: new LocalAuth({
      clientId: `synapse12_${ownerSessionKey}`,
    }),
    puppeteer: puppeteerOptions,
  });

  session.client = client;
  whatsappSessionsByOwner.set(ownerId, session);
  appendWhatsappSessionLog(session, 'session.start', {
    connector: 'webjs',
    hasRemoteBrowser: Boolean(PUPPETEER_BROWSER_WS_ENDPOINT),
  });
  attachWhatsappInitTimeout(ownerId, session, 'Initialization timed out. Chrome did not return QR in time.');

  client.on('qr', async (qr) => {
    try {
      clearWhatsappSessionInitTimer(session);
      session.qrCodeDataUrl = await QRCode.toDataURL(qr, {
        width: 300,
        margin: 1,
      });
      session.status = 'qr';
      session.error = '';
      appendWhatsappSessionLog(session, 'session.qr', { connector: 'webjs' });
      touchWhatsappSession(session, ownerId);
    } catch (error) {
      session.status = 'error';
      session.error = toTrimmedString(error?.message, 260) || 'Failed to render QR code';
      appendWhatsappSessionLog(session, 'session.qr.error', { message: session.error });
      touchWhatsappSession(session, ownerId);
    }
  });

  client.on('ready', () => {
    clearWhatsappSessionInitTimer(session);
    session.status = 'ready';
    session.qrCodeDataUrl = '';
    session.error = '';
    appendWhatsappSessionLog(session, 'session.ready', { connector: 'webjs' });
    touchWhatsappSession(session, ownerId);
  });

  client.on('auth_failure', (message) => {
    clearWhatsappSessionInitTimer(session);
    session.status = 'error';
    session.error = toTrimmedString(String(message || 'Authentication failed'), 260);
    appendWhatsappSessionLog(session, 'session.auth_failure', { message: session.error });
    touchWhatsappSession(session, ownerId);
  });

  client.on('disconnected', (reason) => {
    clearWhatsappSessionInitTimer(session);
    session.status = 'disconnected';
    session.error = toTrimmedString(String(reason || 'Disconnected'), 260);
    appendWhatsappSessionLog(session, 'session.disconnected', { reason: session.error });
    touchWhatsappSession(session, ownerId);
  });

  client
    .initialize()
    .catch((error) => {
      clearWhatsappSessionInitTimer(session);
      session.status = 'error';
      session.error = toTrimmedString(error?.message, 260) || 'Failed to initialize WhatsApp client';
      appendWhatsappSessionLog(session, 'session.initialize.error', { message: session.error });
      touchWhatsappSession(session, ownerId);
    });

  touchWhatsappSession(session, ownerId);
  return session;
}

async function ensureOwnerWhatsappSessionBaileys(ownerId) {
  const {
    default: makeWASocket,
    useMultiFileAuthState,
    makeInMemoryStore,
    fetchLatestBaileysVersion,
    DisconnectReason,
    Browsers,
  } = whatsappBaileys;

  const ownerSessionKey = sanitizeOwnerSessionKey(ownerId);
  const authBaseDir = path.resolve(__dirname, '..', '.wa-auth');
  const authDir = path.join(authBaseDir, ownerSessionKey);

  fs.mkdirSync(authBaseDir, { recursive: true });
  fs.mkdirSync(authDir, { recursive: true });

  let authState = await useMultiFileAuthState(authDir);
  const versionData =
    typeof fetchLatestBaileysVersion === 'function'
      ? await fetchLatestBaileysVersion().catch(() => ({ version: undefined }))
      : { version: undefined };

  const store = makeInMemoryStore ? makeInMemoryStore({}) : null;
  const session = createBaseWhatsappSession(ownerId, 'baileys');
  session.store = store;
  session.authDir = authDir;
  appendWhatsappSessionLog(session, 'session.start', { connector: 'baileys' });
  const retryableDisconnectCodes = new Set(
    [
      DisconnectReason?.connectionClosed,
      DisconnectReason?.connectionLost,
      DisconnectReason?.timedOut,
      DisconnectReason?.restartRequired,
    ]
      .map((code) => Number(code))
      .filter((code) => Number.isFinite(code) && code > 0),
  );

  function createSocket() {
    return makeWASocket({
      auth: authState.state,
      printQRInTerminal: false,
      markOnlineOnConnect: false,
      syncFullHistory: true,
      fireInitQueries: true,
      shouldSyncHistoryMessage: () => true,
      browser:
        Browsers && typeof Browsers.macOS === 'function'
          ? Browsers.macOS('Desktop')
          : ['Synapse12', 'Chrome', '1.0.0'],
      ...(Array.isArray(versionData?.version) ? { version: versionData.version } : {}),
    });
  }

  function detachSocketListeners(socket) {
    if (!socket?.ev || typeof socket.ev.off !== 'function') return;
    if (session.connectionListener) {
      try {
        socket.ev.off('connection.update', session.connectionListener);
      } catch {
        // Ignore detach error.
      }
    }
    if (session.credsListener) {
      try {
        socket.ev.off('creds.update', session.credsListener);
      } catch {
        // Ignore detach error.
      }
    }
    if (session.historyListener) {
      try {
        socket.ev.off('messaging-history.set', session.historyListener);
      } catch {
        // Ignore detach error.
      }
    }
    if (session.contactsUpsertListener) {
      try {
        socket.ev.off('contacts.upsert', session.contactsUpsertListener);
      } catch {
        // Ignore detach error.
      }
    }
    if (session.contactsUpdateListener) {
      try {
        socket.ev.off('contacts.update', session.contactsUpdateListener);
      } catch {
        // Ignore detach error.
      }
    }
    if (session.chatsUpsertListener) {
      try {
        socket.ev.off('chats.upsert', session.chatsUpsertListener);
      } catch {
        // Ignore detach error.
      }
    }
  }

  async function closeSocket(socket, reason = 'Session reconnect') {
    if (!socket) return;
    try {
      if (socket.ws && typeof socket.ws.close === 'function') {
        socket.ws.close();
      }
    } catch {
      // Ignore close errors.
    }
    try {
      if (typeof socket.end === 'function') {
        socket.end(new Error(reason));
      }
    } catch {
      // Ignore close errors.
    }
  }

  async function resetAuthAndRestart(reasonCode = 0) {
    if (session.authResetInProgress) return;
    session.authResetInProgress = true;
    appendWhatsappSessionLog(session, 'session.auth.reset', { statusCode: reasonCode });

    try {
      const previousSocket = session.client;
      detachSocketListeners(previousSocket);
      await closeSocket(previousSocket, 'Resetting auth state');

      try {
        fs.rmSync(authDir, { recursive: true, force: true });
      } catch {
        // Ignore stale auth cleanup failures.
      }
      fs.mkdirSync(authDir, { recursive: true });
      authState = await useMultiFileAuthState(authDir);

      clearWhatsappSessionInitTimer(session);
      clearWhatsappSessionReconnectTimer(session);
      session.restartAttempts = 0;
      session.status = 'initializing';
      session.qrCodeDataUrl = '';
      session.error = '';
      touchWhatsappSession(session, ownerId);

      const nextSocket = createSocket();
      bindSocket(nextSocket);
      attachWhatsappInitTimeout(ownerId, session, 'Initialization timed out. Socket did not return QR in time.');
      touchWhatsappSession(session, ownerId);
    } finally {
      session.authResetInProgress = false;
    }
  }

  async function restartSocket(delayMs = 800) {
    const activeSession = getOwnerWhatsappSession(ownerId);
    if (!activeSession || activeSession.id !== session.id) {
      return;
    }

    clearWhatsappSessionReconnectTimer(session);
    session.reconnectTimer = setTimeout(async () => {
      const latestSession = getOwnerWhatsappSession(ownerId);
      if (!latestSession || latestSession.id !== session.id) {
        return;
      }

      const previousSocket = session.client;
      appendWhatsappSessionLog(session, 'session.reconnect', {
        attempt: session.restartAttempts,
        delayMs,
      });
      detachSocketListeners(previousSocket);
      await closeSocket(previousSocket);

      const nextSocket = createSocket();
      bindSocket(nextSocket);
      clearWhatsappSessionInitTimer(session);
      attachWhatsappInitTimeout(ownerId, session, 'Initialization timed out. Socket did not return QR in time.');
      touchWhatsappSession(session, ownerId);
    }, Math.max(300, delayMs));

    if (typeof session.reconnectTimer?.unref === 'function') {
      session.reconnectTimer.unref();
    }
  }

  function bindSocket(socket) {
    session.client = socket;

    if (store && typeof store.bind === 'function') {
      store.bind(socket.ev);
    }

    session.credsListener = () => {
      authState.saveCreds().catch(() => {
        // Ignore auth state persistence errors.
      });
    };
    socket.ev.on('creds.update', session.credsListener);

    session.historyListener = (historyPayload) => {
      const payload = toProfile(historyPayload);
      const contacts = Array.isArray(payload.contacts) ? payload.contacts : [];
      const chats = Array.isArray(payload.chats) ? payload.chats : [];
      for (const contact of contacts) {
        mergeBaileysContactMirror(session, contact);
      }
      for (const chat of chats) {
        mergeBaileysChatMirror(session, chat);
      }
      appendWhatsappSessionLog(session, 'session.history', {
        contacts: contacts.length,
        chats: chats.length,
        isLatest: Boolean(payload.isLatest),
        progress: Number(payload.progress) || 0,
      });
    };
    socket.ev.on('messaging-history.set', session.historyListener);

    session.contactsUpsertListener = (contacts) => {
      const list = Array.isArray(contacts) ? contacts : [];
      for (const contact of list) {
        mergeBaileysContactMirror(session, contact);
      }
      appendWhatsappSessionLog(session, 'session.contacts.upsert', {
        count: list.length,
        mirroredContacts: session.contactsMirror.size,
      });
    };
    socket.ev.on('contacts.upsert', session.contactsUpsertListener);

    session.contactsUpdateListener = (contacts) => {
      const list = Array.isArray(contacts) ? contacts : [];
      for (const contact of list) {
        mergeBaileysContactMirror(session, contact);
      }
      appendWhatsappSessionLog(session, 'session.contacts.update', {
        count: list.length,
        mirroredContacts: session.contactsMirror.size,
      });
    };
    socket.ev.on('contacts.update', session.contactsUpdateListener);

    session.chatsUpsertListener = (chats) => {
      const list = Array.isArray(chats) ? chats : [];
      for (const chat of list) {
        mergeBaileysChatMirror(session, chat);
      }
      appendWhatsappSessionLog(session, 'session.chats.upsert', {
        count: list.length,
        mirroredChats: session.chatsMirror.size,
      });
    };
    socket.ev.on('chats.upsert', session.chatsUpsertListener);

    session.connectionListener = async (update) => {
      const { connection, qr, lastDisconnect } = update || {};
      if (Object.prototype.hasOwnProperty.call(update || {}, 'receivedPendingNotifications')) {
        session.receivedPendingNotifications = Boolean(update?.receivedPendingNotifications);
        appendWhatsappSessionLog(session, 'session.pending_notifications', {
          value: session.receivedPendingNotifications,
        });
      }

      if (qr) {
        try {
          clearWhatsappSessionInitTimer(session);
          session.qrCodeDataUrl = await QRCode.toDataURL(qr, {
            width: 300,
            margin: 1,
          });
          session.status = 'qr';
          session.error = '';
          appendWhatsappSessionLog(session, 'session.qr', { connector: 'baileys' });
          touchWhatsappSession(session, ownerId);
        } catch (error) {
          session.status = 'error';
          session.error = toTrimmedString(error?.message, 260) || 'Failed to render QR code';
          appendWhatsappSessionLog(session, 'session.qr.error', { message: session.error });
          touchWhatsappSession(session, ownerId);
        }
      }

      if (connection === 'open') {
        clearWhatsappSessionInitTimer(session);
        clearWhatsappSessionReconnectTimer(session);
        session.restartAttempts = 0;
        session.authResetAttempted = false;
        session.status = 'ready';
        session.qrCodeDataUrl = '';
        session.error = '';
        appendWhatsappSessionLog(session, 'session.ready', { connector: 'baileys' });
        touchWhatsappSession(session, ownerId);
        return;
      }

      if (connection === 'close') {
        clearWhatsappSessionInitTimer(session);
        const statusCode = Number(lastDisconnect?.error?.output?.statusCode) || 0;
        const isLoggedOut = statusCode === Number(DisconnectReason?.loggedOut || 0);
        const shouldRetry =
          !isLoggedOut && (statusCode === 0 || retryableDisconnectCodes.has(statusCode));
        appendWhatsappSessionLog(session, 'session.close', {
          statusCode,
          isLoggedOut,
          shouldRetry,
        });

        if (shouldRetry) {
          if (session.restartAttempts < 6) {
            session.restartAttempts += 1;
            session.status = 'initializing';
            session.qrCodeDataUrl = '';
            session.error = '';
            touchWhatsappSession(session, ownerId);
            await restartSocket(Math.min(5000, 800 * session.restartAttempts));
            return;
          }

          session.status = 'error';
          session.error = `WhatsApp socket reconnect failed (code ${statusCode || 'unknown'}). Retry QR.`;
          appendWhatsappSessionLog(session, 'session.reconnect.failed', { statusCode });
          touchWhatsappSession(session, ownerId);
          return;
        }

        if (isLoggedOut) {
          if (!session.authResetAttempted) {
            session.authResetAttempted = true;
            await resetAuthAndRestart(statusCode);
            return;
          }
          session.status = 'disconnected';
          session.error = 'Logged out from WhatsApp. Scan QR again.';
          appendWhatsappSessionLog(session, 'session.logged_out', { statusCode });
        } else {
          session.status = 'error';
          session.error = `WhatsApp socket disconnected (code ${statusCode || 'unknown'}). Please retry QR.`;
          appendWhatsappSessionLog(session, 'session.disconnected', { statusCode, message: session.error });
        }
        touchWhatsappSession(session, ownerId);
      }
    };

    socket.ev.on('connection.update', session.connectionListener);
  }

  bindSocket(createSocket());

  whatsappSessionsByOwner.set(ownerId, session);
  attachWhatsappInitTimeout(ownerId, session, 'Initialization timed out. Socket did not return QR in time.');
  touchWhatsappSession(session, ownerId);
  return session;
}

async function ensureOwnerWhatsappSession(ownerId) {
  const connector = resolveWhatsappConnector();
  if (!connector) {
    throw Object.assign(
      new Error('WhatsApp integration is unavailable. Install @whiskeysockets/baileys or whatsapp-web.js with qrcode.'),
      { status: 503 },
    );
  }

  const existing = getOwnerWhatsappSession(ownerId);
  if (existing && ['initializing', 'qr', 'ready', 'importing'].includes(existing.status)) {
    appendWhatsappSessionLog(existing, 'session.reuse', {
      status: existing.status,
    });
    return existing;
  }

  if (existing) {
    await stopOwnerWhatsappSession(ownerId, 'Restarting session');
  }

  const activeSessions = getActiveWhatsappSessionCount();
  if (activeSessions >= WHATSAPP_MAX_CONCURRENT_SESSIONS) {
    throw Object.assign(
      new Error('WhatsApp is busy right now. Try again in 1-2 minutes.'),
      { status: 429 },
    );
  }

  if (connector === 'baileys') {
    return ensureOwnerWhatsappSessionBaileys(ownerId);
  }

  return ensureOwnerWhatsappSessionWebJs(ownerId);
}

async function mapWithConcurrency(items, limit, iterator, onProgress = null) {
  const maxWorkers = Math.max(1, Math.min(limit, items.length || 1));
  const results = new Array(items.length);
  let cursor = 0;
  let completed = 0;

  async function worker() {
    while (true) {
      const index = cursor;
      cursor += 1;
      if (index >= items.length) break;
      try {
        results[index] = await iterator(items[index], index);
      } catch {
        results[index] = null;
      } finally {
        completed += 1;
        if (typeof onProgress === 'function') {
          try {
            onProgress(completed, items.length);
          } catch {
            // Ignore progress callback errors.
          }
        }
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

function buildWhatsappJidFromPhone(phone) {
  const normalized = normalizePhone(toTrimmedString(phone, 80));
  if (!normalized) return '';
  const digits = normalized.startsWith('+') ? normalized.slice(1) : normalized;
  if (!digits) return '';
  return `${digits}@s.whatsapp.net`;
}

function resolveWhatsappContactJid(contactLike) {
  const row = toProfile(contactLike);
  const direct = toTrimmedString(row.jid || row.id?._serialized || row.id || row.contactId, 220);
  if (direct) return direct;
  return buildWhatsappJidFromPhone(row.phone || row.number || row.phoneNumber || row.id?.user);
}

async function promiseWithTimeout(factory, timeoutMs) {
  let timer = null;
  try {
    return await Promise.race([
      Promise.resolve().then(factory),
      new Promise((_, reject) => {
        timer = setTimeout(() => {
          reject(Object.assign(new Error('Operation timeout'), { code: 'ETIMEOUT' }));
        }, timeoutMs);
      }),
    ]);
  } finally {
    if (timer) {
      clearTimeout(timer);
    }
  }
}

async function fetchWhatsappContactImage(session, contactLike) {
  if (!WHATSAPP_IMAGE_FETCH_ENABLED || !session?.client) return '';

  const jid = resolveWhatsappContactJid(contactLike);
  if (!jid) return '';

  if (session.connector === 'baileys') {
    if (typeof session.client.profilePictureUrl !== 'function') return '';
    let photoUrl = '';
    try {
      photoUrl = await promiseWithTimeout(
        () => session.client.profilePictureUrl(jid, 'image'),
        WHATSAPP_PHOTO_LOOKUP_TIMEOUT_MS,
      );
    } catch {
      photoUrl = '';
    }
    if (!photoUrl) {
      try {
        photoUrl = await promiseWithTimeout(
          () => session.client.profilePictureUrl(jid, 'preview'),
          WHATSAPP_PHOTO_LOOKUP_TIMEOUT_MS,
        );
      } catch {
        photoUrl = '';
      }
    }
    return fetchWhatsappImageDataUrl(photoUrl);
  }

  const contact = toProfile(contactLike)._contact || contactLike;
  if (contact && typeof contact.getProfilePicUrl === 'function') {
    try {
      const photoUrl = await promiseWithTimeout(
        () => contact.getProfilePicUrl(),
        WHATSAPP_PHOTO_LOOKUP_TIMEOUT_MS,
      );
      return fetchWhatsappImageDataUrl(photoUrl);
    } catch {
      // Ignore photo fetch errors.
    }
  }

  if (typeof session.client.getContactById === 'function') {
    try {
      const resolved = await promiseWithTimeout(
        () => session.client.getContactById(jid),
        WHATSAPP_PHOTO_LOOKUP_TIMEOUT_MS,
      );
      if (resolved && typeof resolved.getProfilePicUrl === 'function') {
        const photoUrl = await promiseWithTimeout(
          () => resolved.getProfilePicUrl(),
          WHATSAPP_PHOTO_LOOKUP_TIMEOUT_MS,
        );
        return fetchWhatsappImageDataUrl(photoUrl);
      }
    } catch {
      // Ignore photo fetch errors.
    }
  }

  return '';
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

function normalizeEntityNameForMatch(value) {
  if (typeof value !== 'string') return '';
  const lowered = value
    .trim()
    .toLowerCase()
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^\p{L}\p{N}]+/gu, ' ')
    .replace(/\s+/g, ' ')
    .trim();
  return toTrimmedString(lowered, 160);
}

function isGeneratedWhatsappContactName(value) {
  if (typeof value !== 'string') return false;
  const normalized = value.trim().toLowerCase();
  if (!normalized) return false;
  return /^(контакт|contact)(?:\s+[+\d][\d\s\-()]*)?$/i.test(normalized);
}

function normalizeWhatsappContact(rawContact, index) {
  const row = toProfile(rawContact);
  const sourceId = toTrimmedString(
    row.jid || row.id?._serialized || row.id || row.contactId || row.contact || row.chatId,
    220,
  );
  const nameCandidates = [
    row.name,
    row.notify,
    row.displayName,
    row.fullName,
    row.pushName,
    row.pushname,
    row.shortName,
    row.verifiedName,
    row.verified_name,
    row.vname,
    row.subject,
    row.businessName,
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

  if (!name && !phone && !description && !sourceId) {
    return null;
  }

  const fallbackName = phone ? `Контакт ${phone}` : `Контакт ${index + 1}`;
  const normalizedName = name || fallbackName;
  const importKeySource = phone || sourceId || normalizedName.toLowerCase().replace(/\s+/g, '-');
  const importKey = toTrimmedString(`whatsapp:${importKeySource}`, 180);

  if (!importKey) {
    return null;
  }

  return {
    importKey,
    id: sourceId,
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

function pickPreferredWhatsappName(...rawValues) {
  const values = rawValues
    .map((value) => toTrimmedString(value, 120))
    .filter(Boolean);
  if (!values.length) return '';

  const nonGenerated = values.find((value) => !isGeneratedWhatsappContactName(value));
  return nonGenerated || values[0];
}

function shouldReplaceExistingWhatsappName(existingName, nextName) {
  const current = toTrimmedString(existingName, 120);
  const incoming = toTrimmedString(nextName, 120);
  if (!incoming) return false;
  if (!current) return true;

  const currentGenerated = isGeneratedWhatsappContactName(current);
  const incomingGenerated = isGeneratedWhatsappContactName(incoming);
  if (currentGenerated && !incomingGenerated) return true;
  return false;
}

function shouldForceReplaceWhatsappName(existingName, nextName) {
  const current = toTrimmedString(existingName, 120);
  const incoming = toTrimmedString(nextName, 120);
  if (!incoming) return false;
  if (current === incoming) return false;
  const currentGenerated = isGeneratedWhatsappContactName(current);
  const incomingGenerated = isGeneratedWhatsappContactName(incoming);
  if (!currentGenerated && incomingGenerated) return false;
  return true;
}

function ensureWhatsappBackgroundImportState(session) {
  if (!session || typeof session !== 'object') return null;
  if (!session.backgroundImport || typeof session.backgroundImport !== 'object') {
    session.backgroundImport = {
      state: 'idle',
      includeImages: true,
      overwriteNames: false,
      cursor: 0,
      total: 0,
      imported: 0,
      matched: 0,
      newWithName: 0,
      newWithoutName: 0,
      importedWithImage: 0,
      updatedNames: 0,
      updatedImages: 0,
      batchSize: WHATSAPP_IMPORT_BATCH_SIZE,
      startedAt: '',
      updatedAt: '',
      endedAt: '',
      error: '',
      stopRequested: false,
      pauseRequested: false,
      workerPromise: null,
    };
  }
  if (typeof session.backgroundImport.overwriteNames !== 'boolean') {
    session.backgroundImport.overwriteNames = false;
  }
  return session.backgroundImport;
}

function touchWhatsappBackgroundImport(session) {
  const state = ensureWhatsappBackgroundImportState(session);
  if (!state) return;
  state.updatedAt = new Date().toISOString();
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

app.get('/api/integrations/whatsapp/session/current', requireAuth, async (req, res) => {
  const ownerId = requireOwnerId(req);
  const session = getOwnerWhatsappSession(ownerId);
  if (!session) {
    return res.status(200).json({
      integration: 'whatsapp',
      session: null,
    });
  }

  touchWhatsappSession(session, ownerId);
  return res.status(200).json({
    integration: 'whatsapp',
    session: toWhatsappSessionStatus(session),
  });
});

app.get('/api/integrations/whatsapp/session/:sessionId', requireAuth, async (req, res) => {
  const ownerId = requireOwnerId(req);
  const sessionId = toTrimmedString(req.params.sessionId, 120);
  const session = getOwnerWhatsappSession(ownerId);

  if (!session || session.id !== sessionId) {
    return res.status(404).json({ message: 'WhatsApp session not found' });
  }

  touchWhatsappSession(session, ownerId);

  return res.status(200).json({
    integration: 'whatsapp',
    session: toWhatsappSessionStatus(session),
  });
});

app.get('/api/integrations/whatsapp/session/:sessionId/logs', requireAuth, async (req, res) => {
  const ownerId = requireOwnerId(req);
  const sessionId = toTrimmedString(req.params.sessionId, 120);
  const session = getOwnerWhatsappSession(ownerId);

  if (!session || session.id !== sessionId) {
    return res.status(404).json({ message: 'WhatsApp session not found' });
  }

  touchWhatsappSession(session, ownerId);
  return res.status(200).json({
    integration: 'whatsapp',
    sessionId: session.id,
    status: session.status,
    connector: session.connector || '',
    logs: Array.isArray(session.debugLog) ? session.debugLog : [],
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

async function performWhatsappImportBatch({
  ownerId,
  session,
  includeImages = true,
  overwriteNames = false,
  requestedCursor = 0,
  batchSize = WHATSAPP_IMPORT_BATCH_SIZE,
  setSessionImporting = true,
  runSource = 'manual',
}) {
  if (!session) {
    throw Object.assign(new Error('WhatsApp session not found. Start a session first.'), { status: 404 });
  }
  if (!session.client) {
    throw Object.assign(new Error('WhatsApp client is unavailable for this session.'), { status: 500 });
  }

  const setImportProgress = (stage, percent, processed, total, note = '') => {
    session.importProgress = {
      stage: toTrimmedString(stage, 32) || 'import',
      percent: Math.max(0, Math.min(100, Number(percent) || 0)),
      processed: Math.max(0, Number(processed) || 0),
      total: Math.max(0, Number(total) || 0),
      note: toTrimmedString(note, 220),
    };
    touchWhatsappSession(session, ownerId);
  };

  if (setSessionImporting) {
    session.status = 'importing';
  }
  session.error = '';
  setImportProgress('prepare', 5, 0, 0, runSource === 'background' ? 'Фоновая подготовка импорта' : 'Подготовка импорта');
  appendWhatsappSessionLog(session, 'import.start', {
    connector: session.connector,
    sessionId: session.id,
    includeImages,
    overwriteNames,
    cursor: requestedCursor,
    batchSize,
    runSource,
    receivedPendingNotifications: session.receivedPendingNotifications,
    mirroredContacts: session?.contactsMirror instanceof Map ? session.contactsMirror.size : 0,
    mirroredChats: session?.chatsMirror instanceof Map ? session.chatsMirror.size : 0,
    imageFetchEnabled: WHATSAPP_IMAGE_FETCH_ENABLED,
    imageMaxCount: WHATSAPP_IMAGE_IMPORT_MAX_COUNT,
  });

  if (session.connector === 'baileys' && !session.receivedPendingNotifications) {
    setImportProgress('prepare', 8, 0, 0, 'Ожидание синхронизации уведомлений');
    const pendingReady = await waitForBaileysPendingNotifications(session, 15_000, 600);
    appendWhatsappSessionLog(session, 'import.pending_notifications.wait', {
      pendingReady,
      receivedPendingNotifications: session.receivedPendingNotifications,
    });
  }

  const allContacts =
    session.connector === 'baileys'
      ? await syncBaileysContactsWithRetry(session, { timeoutMs: 18_000, pollMs: 900, minContacts: 1 })
      : await session.client.getContacts();
  appendWhatsappSessionLog(session, 'import.raw_contacts', {
    count: Array.isArray(allContacts) ? allContacts.length : 0,
    connector: session.connector,
    storeContacts: getBaileysCollectionSize(session?.store?.contacts),
    storeChats: getBaileysCollectionSize(session?.store?.chats),
    mirroredContacts: session?.contactsMirror instanceof Map ? session.contactsMirror.size : 0,
    mirroredChats: session?.chatsMirror instanceof Map ? session.chatsMirror.size : 0,
    sample: (Array.isArray(allContacts) ? allContacts : [])
      .slice(0, 5)
      .map((row) => ({
        jid: toTrimmedString(row?.jid || row?.id?._serialized || row?.id, 160),
        number: toTrimmedString(row?.number || row?.phone || row?.id?.user, 60),
        name: toTrimmedString(row?.name || row?.notify || row?.pushname || row?.displayName, 120),
      })),
  });
  setImportProgress('scan', 20, 0, allContacts.length || 0, 'Сканирование контактов');

  const importCandidates = allContacts
    .filter((contact) => {
      if (!contact || typeof contact !== 'object') return false;
      if (contact.isGroup || contact.isBroadcast || contact.isMe) return false;
      const jid = toTrimmedString(contact.jid || contact.id?._serialized || contact.id, 200);
      if (jid.endsWith('@g.us') || jid.endsWith('@broadcast') || jid.endsWith('@newsletter')) return false;
      const number = toTrimmedString(
        contact.number || contact.phone || contact.id?.user || normalizeWhatsappJidToPhone(jid),
        60,
      );
      const name = toTrimmedString(
        pickPreferredWhatsappName(
          contact.notify,
          contact.verifiedName,
          contact.pushname,
          contact.shortName,
          contact.displayName,
          contact.name,
        ),
        120,
      );
      const hasIdentity = Boolean(number || jid || name);
      if (!hasIdentity) return false;
      return true;
    })
    .slice(0, WHATSAPP_CONTACT_IMPORT_LIMIT);
  appendWhatsappSessionLog(session, 'import.candidates', {
    count: importCandidates.length,
    limit: WHATSAPP_CONTACT_IMPORT_LIMIT,
  });

  const totalCandidates = importCandidates.length;
  const cursor = Math.min(requestedCursor, totalCandidates);
  const nextCursor = Math.min(totalCandidates, cursor + batchSize);
  const hasMore = nextCursor < totalCandidates;
  const batchCandidates = importCandidates.slice(cursor, nextCursor);
  appendWhatsappSessionLog(session, 'import.batch', {
    cursor,
    nextCursor,
    hasMore,
    batchSize,
    batchCount: batchCandidates.length,
    totalCandidates,
  });
  setImportProgress('scan', 30, cursor, totalCandidates, 'Контакты подготовлены');

  const preparedContactsMap = (
    await mapWithConcurrency(batchCandidates, WHATSAPP_IMPORT_CONCURRENCY, async (contact, index) => {
      const about =
        session.connector === 'baileys'
          ? toTrimmedString(contact.status, 1200)
          : await readWhatsappContactAbout(contact);
      const businessProfile = session.connector === 'baileys' ? {} : toProfile(contact.businessProfile);
      const websites = Array.isArray(businessProfile.websites)
        ? businessProfile.websites
        : [businessProfile.websites].filter(Boolean);

      const normalized = normalizeWhatsappContact(
        {
          name: pickPreferredWhatsappName(contact.notify, contact.verifiedName, contact.name),
          displayName: pickPreferredWhatsappName(contact.pushname, contact.shortName, contact.displayName),
          fullName: pickPreferredWhatsappName(contact.verifiedName, contact.shortName, contact.displayName),
          phone:
            contact.number ||
            contact.phone ||
            contact.id?.user ||
            normalizeWhatsappJidToPhone(contact.jid || contact.id?._serialized || contact.id),
          id: contact.id?._serialized || contact.jid || contact.id,
          description: about || businessProfile.description || '',
          links: websites,
          markers: [contact.isBusiness ? 'Бизнес' : '', contact.isMyContact ? 'Мой контакт' : ''].filter(Boolean),
          roles: [],
          statuses: [contact.isBlocked ? 'blocked' : '', contact.isBusiness ? 'business' : ''].filter(Boolean),
          image: '',
        },
        cursor + index,
      );

      if (!normalized) {
        return null;
      }

      return {
        ...normalized,
        _contact: contact,
      };
    }, (completed, total) => {
      setImportProgress(
        'normalize',
        30 + Math.round((Math.max(0, Math.min(1, total ? completed / total : 1))) * 30),
        cursor + completed,
        totalCandidates,
        'Нормализация контактов',
      );
    })
  )
    .filter(Boolean)
    .reduce((map, item) => {
      map.set(item.importKey, item);
      return map;
    }, new Map());

  const uniqueContacts = Array.from(preparedContactsMap.values());
  appendWhatsappSessionLog(session, 'import.normalized', {
    count: uniqueContacts.length,
  });
  setImportProgress('dedupe', 65, nextCursor, totalCandidates, 'Удаление дубликатов');

  if (!uniqueContacts.length) {
    if (setSessionImporting) {
      session.status = 'ready';
    }
    session.error = '';
    setImportProgress('done', 100, nextCursor, totalCandidates, 'Батч обработан');
    session.importProgress = null;
    touchWhatsappSession(session, ownerId);
    return {
      source: 'whatsapp',
      imported: 0,
      skipped: 0,
      total: totalCandidates,
      cursor,
      nextCursor,
      hasMore,
      batchSize,
      batchCount: batchCandidates.length,
      matched: 0,
      matchedByPhone: 0,
      matchedByImportKey: 0,
      matchedByJid: 0,
      matchedByName: 0,
      newAvailable: 0,
      newWithName: 0,
      newWithoutName: 0,
      importedWithImage: 0,
      updatedNames: 0,
      updatedImages: 0,
      entities: [],
      session: toWhatsappSessionStatus(session),
    };
  }

  const importKeys = uniqueContacts.map((item) => item.importKey);
  const importJids = Array.from(
    new Set(
      uniqueContacts
        .map((item) => resolveWhatsappContactJid({ jid: item.id, phone: item.phone }))
        .filter(Boolean),
    ),
  );
  const importNameKeys = Array.from(
    new Set(
      uniqueContacts
        .map((item) => ({
          normalizedName: normalizeEntityNameForMatch(item.name),
          generatedName: isGeneratedWhatsappContactName(item.name),
        }))
        .filter((item) => item.normalizedName && !item.generatedName && item.normalizedName.length >= 4)
        .map((item) => item.normalizedName),
    ),
  );
  const importPhones = Array.from(
    new Set(
      uniqueContacts
        .map((item) => normalizePhone(item.phone))
        .filter(Boolean),
    ),
  );
  const importPhoneSet = new Set(importPhones);
  const importPhoneVariants = Array.from(
    new Set(
      importPhones.flatMap((phone) => {
        if (!phone) return [];
        if (phone.startsWith('+')) {
          return [phone, phone.slice(1)].filter(Boolean);
        }
        return [phone, `+${phone}`];
      }),
    ),
  );

  const importIdentityFilters = [{ 'profile.import_key': { $in: importKeys } }];
  if (importJids.length) {
    importIdentityFilters.push({ 'profile.import_jid': { $in: importJids } });
  }

  const existingImportIdentityEntities = await Entity.find(
    {
      owner_id: ownerId,
      type: 'connection',
      'profile.source': 'whatsapp',
      $or: importIdentityFilters,
    },
    { _id: 1, profile: 1, name: 1 },
  ).lean();

  let existingPhoneEntities = [];
  if (importPhoneVariants.length) {
    existingPhoneEntities = await Entity.find(
      {
        owner_id: ownerId,
        type: 'connection',
        'profile.source': 'whatsapp',
        $or: [{ 'profile.phone': { $in: importPhoneVariants } }, { 'profile.phones': { $in: importPhoneVariants } }],
      },
      { _id: 1, profile: 1, type: 1, name: 1 },
    ).lean();
  }

  let existingNameEntities = [];
  if (importNameKeys.length) {
    existingNameEntities = await Entity.find(
      {
        owner_id: ownerId,
        type: 'connection',
        'profile.source': 'whatsapp',
        name: { $exists: true, $ne: '' },
      },
      { _id: 1, name: 1 },
    ).lean();
  }
  setImportProgress('match', 75, cursor, totalCandidates, 'Сопоставление с базой');

  const existingKeySet = new Set();
  const existingJidSet = new Set();
  const existingPhoneSet = new Set();
  const existingNameSet = new Set();
  const existingEntityByImportKey = new Map();
  const existingEntityByImportJid = new Map();
  const existingConnectionByPhone = new Map();

  for (const entity of existingImportIdentityEntities) {
    const profile = toProfile(entity.profile);
    const importKey = toTrimmedString(profile.import_key, 180);
    const importJid = toTrimmedString(profile.import_jid, 220);
    if (importKey) {
      existingKeySet.add(importKey);
      if (!existingEntityByImportKey.has(importKey)) {
        existingEntityByImportKey.set(importKey, entity);
      }
    }
    if (importJid) {
      existingJidSet.add(importJid);
      if (!existingEntityByImportJid.has(importJid)) {
        existingEntityByImportJid.set(importJid, entity);
      }
    }
  }

  for (const entity of existingPhoneEntities) {
    const profile = toProfile(entity.profile);
    const isWhatsappConnection =
      toTrimmedString(entity.type, 32) === 'connection' &&
      toTrimmedString(profile.source, 40).toLowerCase() === 'whatsapp';
    for (const phone of extractNormalizedPhonesFromProfile(profile)) {
      if (importPhoneSet.has(phone) || importPhoneSet.has(phone.startsWith('+') ? phone.slice(1) : `+${phone}`)) {
        existingPhoneSet.add(phone);
        if (isWhatsappConnection && !existingConnectionByPhone.has(phone)) {
          existingConnectionByPhone.set(phone, entity);
        }
      }
    }
  }

  for (const entity of existingNameEntities) {
    const normalizedName = normalizeEntityNameForMatch(entity.name);
    if (!normalizedName || normalizedName.length < 4) continue;
    if (isGeneratedWhatsappContactName(entity.name)) continue;
    existingNameSet.add(normalizedName);
  }

  let matchedByImportKey = 0;
  let matchedByJid = 0;
  let matchedByPhone = 0;
  let matchedByName = 0;
  let matchedTotal = 0;
  const toCreate = [];
  let newWithName = 0;
  let newWithoutName = 0;
  const matchedUpdateCandidates = new Map();

  for (const item of uniqueContacts) {
    const normalizedPhone = normalizePhone(item.phone);
    const normalizedJid = resolveWhatsappContactJid({ jid: item.id, phone: item.phone });
    const normalizedName = normalizeEntityNameForMatch(item.name);
    const hasGeneratedName = isGeneratedWhatsappContactName(item.name);
    const hasImportKeyMatch = existingKeySet.has(item.importKey);
    const hasJidMatch = normalizedJid ? existingJidSet.has(normalizedJid) : false;
    const hasPhoneMatch = normalizedPhone
      ? existingPhoneSet.has(normalizedPhone) ||
        (normalizedPhone.startsWith('+')
          ? existingPhoneSet.has(normalizedPhone.slice(1))
          : existingPhoneSet.has(`+${normalizedPhone}`))
      : false;
    const hasNameMatch =
      normalizedName && normalizedName.length >= 4 && !hasGeneratedName
        ? existingNameSet.has(normalizedName)
        : false;

    if (hasImportKeyMatch || hasJidMatch || hasPhoneMatch || hasNameMatch) {
      matchedTotal += 1;
      if (hasImportKeyMatch) {
        matchedByImportKey += 1;
      }
      if (hasJidMatch) {
        matchedByJid += 1;
      }
      if (hasPhoneMatch) {
        matchedByPhone += 1;
      }
      if (hasNameMatch) {
        matchedByName += 1;
      }

      const matchedEntity =
        existingEntityByImportKey.get(item.importKey) ||
        (normalizedJid ? existingEntityByImportJid.get(normalizedJid) : null) ||
        (normalizedPhone
          ? existingConnectionByPhone.get(normalizedPhone) ||
            existingConnectionByPhone.get(
              normalizedPhone.startsWith('+') ? normalizedPhone.slice(1) : `+${normalizedPhone}`,
            )
          : null);
      if (matchedEntity) {
        const existingProfile = toProfile(matchedEntity.profile);
        const shouldUpdateName = overwriteNames
          ? shouldForceReplaceWhatsappName(matchedEntity.name, item.name)
          : shouldReplaceExistingWhatsappName(matchedEntity.name, item.name);
        const hasImage = Boolean(toTrimmedString(existingProfile.image, 10_000_000));
        const shouldTryImage = includeImages && WHATSAPP_IMAGE_FETCH_ENABLED && !hasImage;
        if (shouldUpdateName || shouldTryImage) {
          matchedUpdateCandidates.set(String(matchedEntity._id), {
            id: matchedEntity._id,
            item,
            shouldUpdateName,
            shouldTryImage,
          });
        }
      }
      continue;
    }

    if (hasGeneratedName) {
      newWithoutName += 1;
    } else {
      newWithName += 1;
    }
    toCreate.push(item);
  }
  appendWhatsappSessionLog(session, 'import.matches', {
    matchedTotal,
    matchedByPhone,
    matchedByImportKey,
    matchedByJid,
    matchedByName,
    newWithName,
    newWithoutName,
    toCreate: toCreate.length,
    matchedUpdateCandidates: matchedUpdateCandidates.size,
  });
  setImportProgress('match', 82, nextCursor, totalCandidates, 'Сопоставление завершено');

  let updatedNames = 0;
  let updatedImages = 0;
  if (matchedUpdateCandidates.size) {
    const updates = await mapWithConcurrency(
      Array.from(matchedUpdateCandidates.values()),
      WHATSAPP_IMAGE_IMPORT_CONCURRENCY,
      async (row) => {
        const setFields = {};
        if (row.shouldUpdateName) {
          setFields.name = row.item.name;
        }

        let image = '';
        if (row.shouldTryImage) {
          image = await fetchWhatsappContactImage(session, row.item);
          if (image) {
            setFields['profile.image'] = image;
            setFields['profile.avatar_synced_at'] = new Date().toISOString();
          }
        }

        if (!Object.keys(setFields).length) {
          return null;
        }
        return {
          id: row.id,
          setFields,
          updatedName: Boolean(row.shouldUpdateName),
          updatedImage: Boolean(image),
        };
      },
    );

    const validUpdates = updates.filter(Boolean);
    if (validUpdates.length) {
      await Entity.bulkWrite(
        validUpdates.map((item) => ({
          updateOne: {
            filter: { _id: item.id, owner_id: ownerId, type: 'connection', 'profile.source': 'whatsapp' },
            update: {
              $set: item.setFields,
            },
          },
        })),
        { ordered: false },
      );
      updatedNames = validUpdates.reduce((total, item) => total + (item.updatedName ? 1 : 0), 0);
      updatedImages = validUpdates.reduce((total, item) => total + (item.updatedImage ? 1 : 0), 0);
    }
  }

  let createdEntities = [];
  let importedWithImage = 0;
  if (toCreate.length) {
    const contactsWithImages = await mapWithConcurrency(
      toCreate,
      WHATSAPP_IMAGE_IMPORT_CONCURRENCY,
      async (item, index) => {
        let image = '';
        if (includeImages && WHATSAPP_IMAGE_FETCH_ENABLED && index < WHATSAPP_IMAGE_IMPORT_MAX_COUNT) {
          image = await fetchWhatsappContactImage(session, item);
        }

        return {
          importKey: item.importKey,
          jid: resolveWhatsappContactJid(item),
          name: item.name,
          phone: item.phone,
          description: item.description,
          tags: item.tags,
          markers: item.markers,
          roles: item.roles,
          links: item.links,
          status: item.status,
          image,
        };
      },
      (completed, total) => {
        setImportProgress(
          'enrich',
          82 + Math.round((Math.max(0, Math.min(1, total ? completed / total : 1))) * 10),
          cursor + completed,
          totalCandidates,
          'Подготовка к записи',
        );
      },
    );
    importedWithImage = contactsWithImages.reduce((total, row) => total + (row.image ? 1 : 0), 0);

    setImportProgress('save', 95, nextCursor, totalCandidates, 'Сохранение в базу');
    createdEntities = await Entity.insertMany(
      contactsWithImages.map((item) => ({
        owner_id: ownerId,
        type: 'connection',
        name: item.name,
        profile: {
          color: '#1058ff',
          source: 'whatsapp',
          import_key: item.importKey,
          import_jid: item.jid || '',
          phone: item.phone,
          phones: item.phone ? [item.phone] : [],
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
          phones: item.phone ? [item.phone] : [],
          status: item.status,
        },
      })),
      { ordered: false },
    );
    appendWhatsappSessionLog(session, 'import.saved', {
      created: createdEntities.length,
    });
  }

  setImportProgress('done', 100, nextCursor, totalCandidates, 'Батч импортирован');
  if (setSessionImporting) {
    session.status = 'ready';
  }
  session.error = '';
  session.lastImportedAt = new Date().toISOString();
  session.importProgress = null;
  appendWhatsappSessionLog(session, 'import.result', {
    imported: createdEntities.length,
    matched: matchedTotal,
    total: totalCandidates,
    matchedByPhone,
    matchedByImportKey,
    matchedByJid,
    matchedByName,
    newAvailable: toCreate.length,
    newWithName,
    newWithoutName,
    importedWithImage,
    updatedNames,
    updatedImages,
    cursor,
    nextCursor,
    hasMore,
    batchSize,
    batchCount: batchCandidates.length,
  });
  touchWhatsappSession(session, ownerId);

  return {
    source: 'whatsapp',
    imported: createdEntities.length,
    skipped: matchedTotal,
    total: totalCandidates,
    cursor,
    nextCursor,
    hasMore,
    batchSize,
    batchCount: batchCandidates.length,
    matched: matchedTotal,
    matchedByPhone,
    matchedByImportKey,
    matchedByJid,
    matchedByName,
    newAvailable: toCreate.length,
    newWithName,
    newWithoutName,
    importedWithImage,
    updatedNames,
    updatedImages,
    entities: createdEntities,
    session: toWhatsappSessionStatus(session),
  };
}

app.post('/api/integrations/whatsapp/import', requireAuth, async (req, res, next) => {
  try {
    const ownerId = requireOwnerId(req);
    const sessionId = toTrimmedString(req.body?.sessionId, 120);
    const includeImages = req.body?.includeImages === true;
    const overwriteNames = req.body?.overwriteNames === true;
    const requestedCursor = Math.max(0, Number(req.body?.cursor) || 0);
    const requestedBatchSize = Math.max(1, Number(req.body?.batchSize) || WHATSAPP_IMPORT_BATCH_SIZE);
    const batchSize = Math.min(requestedBatchSize, WHATSAPP_CONTACT_IMPORT_LIMIT);
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
    const backgroundState = ensureWhatsappBackgroundImportState(session);
    if (backgroundState && ['running', 'paused'].includes(backgroundState.state)) {
      return res.status(409).json({
        message: 'Background import is active. Pause or stop it before starting manual import.',
        session: toWhatsappSessionStatus(session),
      });
    }
    const result = await performWhatsappImportBatch({
      ownerId,
      session,
      includeImages,
      overwriteNames,
      requestedCursor,
      batchSize,
      setSessionImporting: true,
      runSource: 'manual',
    });
    return res.status(200).json(result);
  } catch (error) {
    const ownerId = getOwnerIdFromRequest(req);
    const session = getOwnerWhatsappSession(ownerId);
    if (session) {
      session.status = 'error';
      session.error = toTrimmedString(error?.message, 260) || 'Import failed';
      session.importProgress = null;
      appendWhatsappSessionLog(session, 'import.error', {
        message: session.error,
      });
      touchWhatsappSession(session, ownerId);
    }
    return next(error);
  }
});

async function runWhatsappBackgroundImport(ownerId, session) {
  const state = ensureWhatsappBackgroundImportState(session);
  if (!state) return;

  while (true) {
    if (state.stopRequested) {
      state.state = 'stopped';
      state.endedAt = new Date().toISOString();
      touchWhatsappBackgroundImport(session);
      appendWhatsappSessionLog(session, 'import.background.stopped', {
        cursor: state.cursor,
        imported: state.imported,
      });
      break;
    }

    if (state.pauseRequested) {
      if (state.state !== 'paused') {
        state.state = 'paused';
        touchWhatsappBackgroundImport(session);
        appendWhatsappSessionLog(session, 'import.background.paused', {
          cursor: state.cursor,
        });
      }
      await delay(WHATSAPP_BACKGROUND_IMPORT_POLL_MS);
      continue;
    }

    state.state = 'running';
    touchWhatsappBackgroundImport(session);

    const result = await performWhatsappImportBatch({
      ownerId,
      session,
      includeImages: state.includeImages !== false,
      overwriteNames: state.overwriteNames === true,
      requestedCursor: Math.max(0, Number(state.cursor) || 0),
      batchSize: Math.max(1, Number(state.batchSize) || WHATSAPP_IMPORT_BATCH_SIZE),
      setSessionImporting: false,
      runSource: 'background',
    });

    state.total = Math.max(state.total, Number(result.total) || 0);
    state.cursor = Math.max(0, Number(result.nextCursor) || 0);
    state.imported += Math.max(0, Number(result.imported) || 0);
    state.matched += Math.max(0, Number(result.matched ?? result.skipped) || 0);
    state.newWithName += Math.max(0, Number(result.newWithName) || 0);
    state.newWithoutName += Math.max(0, Number(result.newWithoutName) || 0);
    state.importedWithImage += Math.max(0, Number(result.importedWithImage) || 0);
    state.updatedNames += Math.max(0, Number(result.updatedNames) || 0);
    state.updatedImages += Math.max(0, Number(result.updatedImages) || 0);
    touchWhatsappBackgroundImport(session);

    if (!result.hasMore || (state.total > 0 && state.cursor >= state.total)) {
      state.state = 'completed';
      state.endedAt = new Date().toISOString();
      touchWhatsappBackgroundImport(session);
      appendWhatsappSessionLog(session, 'import.background.completed', {
        total: state.total,
        imported: state.imported,
        matched: state.matched,
        updatedNames: state.updatedNames,
        updatedImages: state.updatedImages,
      });
      break;
    }
  }
}

app.post('/api/integrations/whatsapp/import/background/start', requireAuth, async (req, res, next) => {
  try {
    const ownerId = requireOwnerId(req);
    const sessionId = toTrimmedString(req.body?.sessionId, 120);
    const includeImages = req.body?.includeImages !== false;
    const overwriteNames = req.body?.overwriteNames === true;
    const requestedCursor = Math.max(0, Number(req.body?.cursor) || 0);
    const requestedBatchSize = Math.max(1, Number(req.body?.batchSize) || WHATSAPP_IMPORT_BATCH_SIZE);
    const batchSize = Math.min(requestedBatchSize, WHATSAPP_CONTACT_IMPORT_LIMIT);
    const session = getOwnerWhatsappSession(ownerId);

    if (!session || (sessionId && session.id !== sessionId)) {
      return res.status(404).json({ message: 'WhatsApp session not found. Start a session first.' });
    }
    if (!session.client) {
      return res.status(500).json({ message: 'WhatsApp client is unavailable for this session.' });
    }
    if (!['ready', 'importing'].includes(session.status)) {
      return res.status(409).json({
        message: 'WhatsApp session is not ready. Scan QR and wait for connection.',
        session: toWhatsappSessionStatus(session),
      });
    }

    const state = ensureWhatsappBackgroundImportState(session);
    if (!state) {
      return res.status(500).json({ message: 'Background import state is unavailable.' });
    }
    if (state.workerPromise && ['running', 'paused'].includes(state.state)) {
      return res.status(409).json({
        message: 'Background import is already running.',
        session: toWhatsappSessionStatus(session),
      });
    }

    state.state = 'running';
    state.includeImages = includeImages;
    state.overwriteNames = overwriteNames;
    state.cursor = requestedCursor;
    state.total = 0;
    state.imported = 0;
    state.matched = 0;
    state.newWithName = 0;
    state.newWithoutName = 0;
    state.importedWithImage = 0;
    state.updatedNames = 0;
    state.updatedImages = 0;
    state.batchSize = batchSize;
    state.startedAt = new Date().toISOString();
    state.updatedAt = state.startedAt;
    state.endedAt = '';
    state.error = '';
    state.stopRequested = false;
    state.pauseRequested = false;
    appendWhatsappSessionLog(session, 'import.background.start', {
      includeImages,
      overwriteNames,
      cursor: requestedCursor,
      batchSize,
    });

    state.workerPromise = (async () => {
      try {
        await runWhatsappBackgroundImport(ownerId, session);
      } catch (error) {
        state.state = 'error';
        state.error = toTrimmedString(error?.message, 260) || 'Background import failed';
        state.endedAt = new Date().toISOString();
        touchWhatsappBackgroundImport(session);
        appendWhatsappSessionLog(session, 'import.background.error', {
          message: state.error,
        });
      } finally {
        state.workerPromise = null;
        session.importProgress = null;
        if (session.status === 'importing') {
          session.status = 'ready';
        }
        touchWhatsappSession(session, ownerId);
      }
    })();

    touchWhatsappSession(session, ownerId);
    return res.status(200).json({
      started: true,
      session: toWhatsappSessionStatus(session),
    });
  } catch (error) {
    return next(error);
  }
});

app.post('/api/integrations/whatsapp/import/background/pause', requireAuth, async (req, res, next) => {
  try {
    const ownerId = requireOwnerId(req);
    const sessionId = toTrimmedString(req.body?.sessionId, 120);
    const session = getOwnerWhatsappSession(ownerId);
    if (!session || (sessionId && session.id !== sessionId)) {
      return res.status(404).json({ message: 'WhatsApp session not found. Start a session first.' });
    }
    const state = ensureWhatsappBackgroundImportState(session);
    if (!state || !['running', 'paused'].includes(state.state)) {
      return res.status(409).json({
        message: 'Background import is not running.',
        session: toWhatsappSessionStatus(session),
      });
    }

    state.pauseRequested = true;
    if (state.state !== 'paused') {
      // Confirm pause in API response immediately so UI does not flap between
      // running/paused while the current batch is finishing.
      state.state = 'paused';
    }
    touchWhatsappBackgroundImport(session);
    touchWhatsappSession(session, ownerId);
    appendWhatsappSessionLog(session, 'import.background.pause.request');
    return res.status(200).json({ paused: true, session: toWhatsappSessionStatus(session) });
  } catch (error) {
    return next(error);
  }
});

app.post('/api/integrations/whatsapp/import/background/resume', requireAuth, async (req, res, next) => {
  try {
    const ownerId = requireOwnerId(req);
    const sessionId = toTrimmedString(req.body?.sessionId, 120);
    const session = getOwnerWhatsappSession(ownerId);
    if (!session || (sessionId && session.id !== sessionId)) {
      return res.status(404).json({ message: 'WhatsApp session not found. Start a session first.' });
    }
    const state = ensureWhatsappBackgroundImportState(session);
    if (!state || !['paused', 'running'].includes(state.state)) {
      return res.status(409).json({
        message: 'Background import is not paused.',
        session: toWhatsappSessionStatus(session),
      });
    }

    state.pauseRequested = false;
    if (state.state === 'paused') {
      state.state = 'running';
    }
    touchWhatsappBackgroundImport(session);
    touchWhatsappSession(session, ownerId);
    appendWhatsappSessionLog(session, 'import.background.resume.request');
    return res.status(200).json({ resumed: true, session: toWhatsappSessionStatus(session) });
  } catch (error) {
    return next(error);
  }
});

app.post('/api/integrations/whatsapp/import/background/stop', requireAuth, async (req, res, next) => {
  try {
    const ownerId = requireOwnerId(req);
    const sessionId = toTrimmedString(req.body?.sessionId, 120);
    const session = getOwnerWhatsappSession(ownerId);
    if (!session || (sessionId && session.id !== sessionId)) {
      return res.status(404).json({ message: 'WhatsApp session not found. Start a session first.' });
    }
    const state = ensureWhatsappBackgroundImportState(session);
    if (!state || !['running', 'paused'].includes(state.state)) {
      return res.status(409).json({
        message: 'Background import is not active.',
        session: toWhatsappSessionStatus(session),
      });
    }

    state.stopRequested = true;
    state.pauseRequested = false;
    if (state.state === 'paused') {
      state.state = 'running';
    }
    touchWhatsappBackgroundImport(session);
    touchWhatsappSession(session, ownerId);
    appendWhatsappSessionLog(session, 'import.background.stop.request');
    return res.status(200).json({ stopped: true, session: toWhatsappSessionStatus(session) });
  } catch (error) {
    return next(error);
  }
});

app.post('/api/integrations/whatsapp/photos/backfill', requireAuth, async (req, res, next) => {
  try {
    const ownerId = requireOwnerId(req);
    const sessionId = toTrimmedString(req.body?.sessionId, 120);
    const onlyMissing = req.body?.onlyMissing !== false;
    const cleanupRoles = req.body?.cleanupRoles === true;
    const requestedLimit = Math.max(1, Number(req.body?.limit) || WHATSAPP_PHOTOS_BACKFILL_BATCH_LIMIT);
    const limit = Math.min(requestedLimit, WHATSAPP_PHOTOS_BACKFILL_MAX_LIMIT);
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

    if (!WHATSAPP_IMAGE_FETCH_ENABLED) {
      appendWhatsappSessionLog(session, 'photos.backfill.skip', {
        reason: 'image_fetch_disabled',
      });
      return res.status(409).json({
        message: 'WhatsApp image fetch is disabled on server (WHATSAPP_IMAGE_FETCH_ENABLED=false).',
        session: toWhatsappSessionStatus(session),
      });
    }

    if (cleanupRoles) {
      const cleanupResult = await Entity.updateMany(
        {
          owner_id: ownerId,
          type: 'connection',
          'profile.source': 'whatsapp',
        },
        {
          $pull: {
            'ai_metadata.roles': 'Контакт',
          },
        },
      );
      appendWhatsappSessionLog(session, 'photos.backfill.roles.cleanup', {
        modified: Number(cleanupResult?.modifiedCount) || 0,
      });
    }

    const retryBeforeIso = new Date(Date.now() - WHATSAPP_PHOTO_RETRY_AFTER_MS).toISOString();
    const missingImageFilter = [{ 'profile.image': { $exists: false } }, { 'profile.image': '' }];
    const retryWindowFilter = [
      { 'profile.avatar_sync_attempted_at': { $exists: false } },
      { 'profile.avatar_sync_attempted_at': '' },
      { 'profile.avatar_sync_attempted_at': { $lt: retryBeforeIso } },
    ];

    const query = {
      owner_id: ownerId,
      type: 'connection',
      'profile.source': 'whatsapp',
      ...(onlyMissing
        ? {
            $and: [{ $or: missingImageFilter }, { $or: retryWindowFilter }],
          }
        : {}),
    };

    const candidates = await Entity.find(query, { _id: 1, profile: 1 })
      .sort({ 'profile.avatar_sync_attempted_at': 1, _id: 1 })
      .limit(limit)
      .lean();

    let scanned = 0;
    let updated = 0;
    let skippedNoIdentity = 0;
    let failed = 0;

    function setProgress(percent, processed, total, note) {
      session.importProgress = {
        stage: 'photos',
        percent: Math.max(0, Math.min(100, Number(percent) || 0)),
        processed: Math.max(0, Number(processed) || 0),
        total: Math.max(0, Number(total) || 0),
        note: toTrimmedString(note, 220),
      };
      touchWhatsappSession(session, ownerId);
    }

    appendWhatsappSessionLog(session, 'photos.backfill.start', {
      onlyMissing,
      limit,
      candidates: candidates.length,
    });

    session.status = 'importing';
    session.error = '';
    setProgress(5, 0, candidates.length, 'Подготовка догрузки фото');

    await mapWithConcurrency(
      candidates,
      WHATSAPP_IMAGE_IMPORT_CONCURRENCY,
      async (entity) => {
        scanned += 1;
        const attemptAt = new Date().toISOString();
        const profile = toProfile(entity.profile);
        const contactLike = {
          jid: toTrimmedString(profile.import_jid, 220),
          phone: toTrimmedString(profile.phone, 80) || toTrimmedString((Array.isArray(profile.phones) ? profile.phones[0] : ''), 80),
        };

        if (!contactLike.jid && !contactLike.phone) {
          skippedNoIdentity += 1;
          try {
            await Entity.updateOne(
              {
                _id: entity._id,
                owner_id: ownerId,
              },
              {
                $set: {
                  'profile.avatar_sync_attempted_at': attemptAt,
                  'profile.avatar_sync_attempt_error': 'missing_identity',
                },
              },
            );
          } catch {
            // Ignore per-row write errors.
          }
          return null;
        }

        const image = await fetchWhatsappContactImage(session, contactLike);
        if (!image) {
          try {
            await Entity.updateOne(
              {
                _id: entity._id,
                owner_id: ownerId,
              },
              {
                $set: {
                  'profile.avatar_sync_attempted_at': attemptAt,
                  'profile.avatar_sync_attempt_error': 'no_image',
                },
              },
            );
          } catch {
            // Ignore per-row write errors.
          }
          return null;
        }

        try {
          await Entity.updateOne(
            {
              _id: entity._id,
              owner_id: ownerId,
            },
            {
              $set: {
                'profile.image': image,
                'profile.avatar_synced_at': new Date().toISOString(),
                'profile.avatar_sync_attempted_at': attemptAt,
              },
              $unset: {
                'profile.avatar_sync_attempt_error': '',
              },
              $pull: {
                'ai_metadata.roles': 'Контакт',
              },
            },
          );
          updated += 1;
        } catch (error) {
          failed += 1;
          try {
            await Entity.updateOne(
              {
                _id: entity._id,
                owner_id: ownerId,
              },
              {
                $set: {
                  'profile.avatar_sync_attempted_at': attemptAt,
                  'profile.avatar_sync_attempt_error':
                    toTrimmedString(error?.message, 180) || 'update_failed',
                },
              },
            );
          } catch {
            // Ignore per-row write errors.
          }
        }
        return null;
      },
      (processed, total) => {
        const percent = 5 + Math.round((Math.max(0, Math.min(1, total ? processed / total : 1))) * 93);
        setProgress(percent, processed, total, 'Догрузка фотографий');
      },
    );

    const remaining = onlyMissing ? await Entity.countDocuments(query) : 0;
    const hasMore = onlyMissing && remaining > 0;

    setProgress(100, scanned, candidates.length, 'Догрузка фото завершена');
    session.status = 'ready';
    session.error = '';
    session.importProgress = null;
    touchWhatsappSession(session, ownerId);
    appendWhatsappSessionLog(session, 'photos.backfill.result', {
      scanned,
      updated,
      skippedNoIdentity,
      failed,
      remaining,
      hasMore,
      limit,
    });

    return res.status(200).json({
      scanned,
      updated,
      skippedNoIdentity,
      failed,
      remaining,
      hasMore,
      limit,
      session: toWhatsappSessionStatus(session),
    });
  } catch (error) {
    const ownerId = getOwnerIdFromRequest(req);
    const session = getOwnerWhatsappSession(ownerId);
    if (session) {
      session.status = 'error';
      session.error = toTrimmedString(error?.message, 260) || 'Photo backfill failed';
      session.importProgress = null;
      appendWhatsappSessionLog(session, 'photos.backfill.error', { message: session.error });
      touchWhatsappSession(session, ownerId);
    }
    return next(error);
  }
});

app.delete('/api/integrations/whatsapp/imported', requireAuth, async (req, res, next) => {
  try {
    const ownerId = requireOwnerId(req);
    const importedEntities = await Entity.find(
      {
        owner_id: ownerId,
        type: 'connection',
        'profile.source': 'whatsapp',
      },
      { _id: 1 },
    ).lean();

    const ids = importedEntities.map((row) => String(row._id)).filter(Boolean);
    if (!ids.length) {
      return res.status(200).json({ deleted: 0 });
    }

    await removeEntitiesFromProjectCanvases(ids, ownerId);
    await Entity.deleteMany({
      owner_id: ownerId,
      _id: { $in: ids },
    });

    return res.status(200).json({ deleted: ids.length });
  } catch (error) {
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

app.get('/api/events', async (req, res, next) => {
  try {
    const sessionToken = getSessionTokenFromRequest(req, { allowQueryToken: true });
    if (!sessionToken) {
      return res.status(401).json({ message: 'Unauthorized' });
    }

    const user = await resolveAuthUserFromSessionToken(sessionToken);
    const ownerId = String(user?._id || user?.id || '').trim();
    if (!ownerId) {
      return res.status(401).json({ message: 'Unauthorized' });
    }

    res.setHeader('Content-Type', 'text/event-stream; charset=utf-8');
    res.setHeader('Cache-Control', 'no-cache, no-transform');
    res.setHeader('Connection', 'keep-alive');
    res.setHeader('X-Accel-Buffering', 'no');
    if (typeof res.flushHeaders === 'function') {
      res.flushHeaders();
    }

    if (req.socket) {
      req.socket.setTimeout(0);
      req.socket.setNoDelay(true);
      req.socket.setKeepAlive(true);
    }

    res.write('retry: 2500\n\n');
    registerEntityEventStream(ownerId, req, res);
    return undefined;
  } catch (error) {
    if (!res.headersSent) {
      return res.status(401).json({ message: 'Unauthorized' });
    }
    return next(error);
  }
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
        returnDocument: 'after',
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

const aiPrompts = createAiPrompts({
  AI_CONTEXT_ENTITY_LIMIT,
  toTrimmedString,
  toProfile,
  getEntityAnalyzerFields,
  normalizeDescriptionHistory,
  normalizeImportanceHistory,
});

const aiAttachments = createAiAttachmentTools({
  toProfile,
  toTrimmedString,
  compactObject,
  AI_HISTORY_MESSAGE_LIMIT,
  AI_ATTACHMENT_LIMIT,
  AI_ATTACHMENT_TEXT_MAX_LENGTH,
  AI_ATTACHMENT_DATA_URL_MAX_LENGTH,
  AI_ATTACHMENT_BINARY_MAX_BYTES,
  mammoth,
});

const aiProvider = createAiProvider({
  OPENAI_API_KEY,
  OPENAI_MODEL,
  OPENAI_REQUEST_TIMEOUT_MS,
  toTrimmedString,
});

const aiRouter = createAiRouter({
  requireAuth,
  requireOwnerId,
  toTrimmedString,
  toProfile,
  AI_DEBUG_ECHO,
  OPENAI_MODEL,
  OPENAI_PROJECT_MODEL,
  OPENAI_ROUTER_MODEL,
  OPENAI_DEEP_MODEL,
  OPENAI_WEB_SEARCH_MODEL,
  sharp,
  Entity,
  EntityWebSearch,
  resolveAgentScopeContext,
  buildEntityAnalyzerCurrentFields,
  getEntityAnalyzerFields,
  extractJsonObjectFromText,
  normalizeEntityAnalysisOutput,
  buildEntityMetadataPatch,
  upsertEntityVector,
  broadcastEntityEvent,
  AgentChatHistory,
  entityTypes: Array.from(ENTITY_TYPES),
  AGENT_CHAT_HISTORY_MESSAGE_LIMIT,
  AGENT_CHAT_HISTORY_ATTACHMENT_LIMIT,
  AGENT_CHAT_HISTORY_ATTACHMENT_DATA_MAX_LENGTH,
  AGENT_CHAT_HISTORY_TEXT_MAX_LENGTH,
  aiPrompts,
  aiAttachments,
  aiProvider,
});

app.use('/api/ai', aiRouter);

const transcribeRouter = createTranscribeRouter({
  requireAuth,
  toTrimmedString,
  aiProvider,
  OPENAI_TRANSCRIBE_MODEL,
  OPENAI_TRANSCRIBE_MAX_AUDIO_BYTES,
});

app.use('/api/transcribe', transcribeRouter);

app.use('/api/entities', requireAuth);

app.get('/api/entities', async (req, res, next) => {
  try {
    const filter = {};
    const ownerId = requireOwnerId(req);
    filter.owner_id = ownerId;

    const typeQueryRaw = Array.isArray(req.query.type) ? req.query.type[0] : req.query.type;
    const excludeTypeQueryRaw = Array.isArray(req.query.excludeType)
      ? req.query.excludeType[0]
      : req.query.excludeType;
    const requestedType =
      typeof typeQueryRaw === 'string' && ENTITY_TYPES.has(typeQueryRaw) ? typeQueryRaw : '';
    const excludedType =
      typeof excludeTypeQueryRaw === 'string' && ENTITY_TYPES.has(excludeTypeQueryRaw)
        ? excludeTypeQueryRaw
        : '';

    if (requestedType) {
      filter.type = requestedType;
    }

    if (excludedType) {
      if (requestedType && requestedType === excludedType) {
        return res.json([]);
      }

      if (!requestedType) {
        filter.type = { $ne: excludedType };
      }
    }

    if (requestedType === 'connection') {
      // Keep contact avatars in the list response so the Collection tab can render photos after a page reload.
      // Strip only known heavy nested debug payloads.
      const entities = await Entity.find(filter, {
      })
        .sort({ createdAt: -1, _id: -1 })
        .lean();
      return res.json(entities);
    }

    // Return plain JSON objects to avoid Mongoose document hydration cost on large collections.
    const entities = await Entity.find(filter).sort({ createdAt: -1, _id: -1 }).lean();
    res.json(entities);
  } catch (error) {
    next(error);
  }
});

app.get('/api/entities/:id', async (req, res, next) => {
  try {
    const ownerId = requireOwnerId(req);
    const entity = await Entity.findOne({
      _id: req.params.id,
      owner_id: ownerId,
    }).lean();

    if (!entity) {
      return res.status(404).json({ message: 'Entity not found' });
    }

    return res.json(entity);
  } catch (error) {
    return next(error);
  }
});

app.post('/api/entities', async (req, res, next) => {
  try {
    const ownerId = requireOwnerId(req);
    let payload = normalizeIncomingEntityPayload(req.body, {
      source: 'system',
      entityType: req.body?.type,
    });
    const payloadEntityType =
      typeof payload.type === 'string' && ENTITY_TYPES.has(payload.type) ? payload.type : 'shape';
    payload = normalizeMineFlagsInPayload(payload, payloadEntityType, { mode: 'create' });
    const nextMetadata = toProfile(payload.ai_metadata);
    const explicitNameMode = normalizeEntityNameMode(nextMetadata.name_mode);
    const inferredNameMode = isSystemDefaultEntityName(payloadEntityType, payload.name) ? 'system' : 'manual';
    nextMetadata.name_mode = explicitNameMode || inferredNameMode;
    if (nextMetadata.name_mode !== 'llm') {
      nextMetadata.name_auto = false;
    }
    payload.ai_metadata = nextMetadata;
    payload.owner_id = ownerId;

    const entity = await Entity.create(payload);
    broadcastEntityEvent(ownerId, 'entity.created', {
      entity: entity.toObject(),
    });
    res.status(201).json(entity);
  } catch (error) {
    next(error);
  }
});

app.put('/api/entities/:id', async (req, res, next) => {
  try {
    const ownerId = requireOwnerId(req);
    const expectedUpdatedAt = readExpectedEntityUpdatedAt(req.body);
    const expectedCanvasVersion = readExpectedCanvasVersion(req.body);
    const existingEntity = await Entity.findOne(
      {
        _id: req.params.id,
        owner_id: ownerId,
      },
      { ai_metadata: 1, type: 1, name: 1, updatedAt: 1, canvas_data: 1 },
    ).lean();
    if (!existingEntity) {
      return res.status(404).json({ message: 'Entity not found' });
    }

    let payload = normalizeIncomingEntityPayload(stripEntityUpdateControlFields(req.body), {
      existingMetadata: existingEntity.ai_metadata,
      existingName: existingEntity.name,
      source: 'manual',
      entityType:
        typeof req.body?.type === 'string' && req.body.type.trim()
          ? req.body.type
          : existingEntity.type,
    });
    const payloadEntityType =
      typeof payload.type === 'string' && ENTITY_TYPES.has(payload.type) ? payload.type : existingEntity.type;
    payload = normalizeMineFlagsInPayload(payload, payloadEntityType, { mode: 'update' });
    payload.owner_id = ownerId;

    if (expectedCanvasVersion && payloadEntityType === 'project' && Object.prototype.hasOwnProperty.call(payload, 'canvas_data')) {
      const currentCanvasVersion = buildProjectCanvasContentVersion(existingEntity.canvas_data);
      if (currentCanvasVersion !== expectedCanvasVersion) {
        const currentEntity = await Entity.findOne({
          _id: req.params.id,
          owner_id: ownerId,
        });
        if (currentEntity) {
          return res.status(409).json({
            message: 'Project canvas has a newer server version',
            code: 'entity_conflict',
            entity: currentEntity.toObject(),
          });
        }
      }
    }

    const updateFilter = {
      _id: req.params.id,
      owner_id: ownerId,
    };
    if (expectedUpdatedAt) {
      updateFilter.updatedAt = new Date(expectedUpdatedAt);
    }

    const updatedEntity = await Entity.findOneAndUpdate(
      updateFilter,
      payload,
      {
        returnDocument: 'after',
        runValidators: true,
      },
    );

    if (!updatedEntity) {
      if (expectedUpdatedAt) {
        const currentEntity = await Entity.findOne({
          _id: req.params.id,
          owner_id: ownerId,
        });
        if (currentEntity) {
          return res.status(409).json({
            message: 'Entity has a newer server version',
            code: 'entity_conflict',
            entity: currentEntity.toObject(),
          });
        }
      }
      return res.status(404).json({ message: 'Entity not found' });
    }

    broadcastEntityEvent(ownerId, 'entity.updated', {
      entity: updatedEntity.toObject(),
    });

    return res.json(updatedEntity);
  } catch (error) {
    return next(error);
  }
});

app.post('/api/entities/:id/set-me', async (req, res, next) => {
  try {
    const ownerId = requireOwnerId(req);
    const personId = toTrimmedString(req.params.id, 80);
    if (!personId) {
      return res.status(400).json({ message: 'Entity id is required' });
    }

    const person = await Entity.findOne({
      _id: personId,
      owner_id: ownerId,
      type: 'person',
    });
    if (!person) {
      return res.status(404).json({ message: 'Person not found' });
    }

    const existingMeEntities = await Entity.find(
      {
        owner_id: ownerId,
        type: 'person',
        _id: { $ne: person._id },
        is_me: true,
      },
      { _id: 1 },
    ).lean();
    const clearedPersonIds = existingMeEntities.map((item) => String(item._id)).filter(Boolean);

    if (clearedPersonIds.length) {
      await Entity.updateMany(
        {
          owner_id: ownerId,
          _id: { $in: clearedPersonIds },
        },
        {
          $set: { is_me: false },
        },
      );
    }

    const updatedPerson = await Entity.findOneAndUpdate(
      {
        _id: person._id,
        owner_id: ownerId,
        type: 'person',
      },
      {
        $set: {
          is_me: true,
          is_mine: true,
        },
      },
      {
        returnDocument: 'after',
        runValidators: true,
      },
    );

    if (!updatedPerson) {
      return res.status(404).json({ message: 'Person not found' });
    }

    const changedIds = Array.from(new Set([...clearedPersonIds, String(updatedPerson._id)]));
    const changedEntities = changedIds.length
      ? await Entity.find(
          {
            owner_id: ownerId,
            _id: { $in: changedIds },
          },
          {},
        ).lean()
      : [];
    const changedById = new Map(changedEntities.map((item) => [String(item._id), item]));
    const orderedChangedEntities = changedIds
      .map((id) => changedById.get(id))
      .filter((item) => Boolean(item));

    for (const changedEntity of orderedChangedEntities) {
      broadcastEntityEvent(ownerId, 'entity.updated', {
        entity: changedEntity,
      });
    }

    return res.status(200).json({
      entity: updatedPerson.toObject(),
      entities: orderedChangedEntities,
      clearedPersonIds,
    });
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
    const removedEntityIds = new Set([entityId]);

    await removeEntityFromProjectCanvases(entityId, ownerId);

    await Entity.deleteOne({
      _id: entityToDelete._id,
      owner_id: ownerId,
    });

    broadcastEntityEvent(ownerId, 'entity.deleted', {
      entityIds: Array.from(removedEntityIds),
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
      `[ai] Enabled models: entity_analyzer=${OPENAI_MODEL}, chat_router=${OPENAI_ROUTER_MODEL}, chat_deep=${OPENAI_DEEP_MODEL}, embedding=${OPENAI_EMBEDDING_MODEL}, debugEcho=${AI_DEBUG_ECHO}`,
    );
  }
  if (!isWhatsappIntegrationAvailable()) {
    console.warn(
      '[integrations] WhatsApp integration is disabled. Install backend deps: @whiskeysockets/baileys or whatsapp-web.js, and qrcode.',
    );
  } else {
    const resolvedConnector = resolveWhatsappConnector();
    console.warn(
      `[integrations] WhatsApp settings: connector=${resolvedConnector}, requestedConnector=${WHATSAPP_CONNECTOR}, contactsLimit=${WHATSAPP_CONTACT_IMPORT_LIMIT}, imageFetch=${WHATSAPP_IMAGE_FETCH_ENABLED}, imageMaxCount=${WHATSAPP_IMAGE_IMPORT_MAX_COUNT}, sessionIdleMs=${WHATSAPP_SESSION_IDLE_TIMEOUT_MS}, initTimeoutMs=${WHATSAPP_INIT_TIMEOUT_MS}, maxSessions=${WHATSAPP_MAX_CONCURRENT_SESSIONS}`,
    );
    if (resolvedConnector === 'webjs' && PUPPETEER_BROWSER_WS_ENDPOINT) {
      console.warn('[integrations] WhatsApp uses remote browser via PUPPETEER_BROWSER_WS_ENDPOINT.');
    } else if (resolvedConnector === 'webjs' && IS_PRODUCTION && !WHATSAPP_ALLOW_LOCAL_CHROME) {
      console.warn(
        '[integrations] Local Chromium is disabled in production. Set PUPPETEER_BROWSER_WS_ENDPOINT for stable WhatsApp QR.',
      );
    }
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
