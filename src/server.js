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
const User = require('./models/User');
const EntityVector = require('./models/EntityVector');

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
const OPENAI_API_KEY = String(process.env.OPENAI_API_KEY || '').trim();
const OPENAI_MODEL = String(process.env.OPENAI_MODEL || 'gpt-4.1-mini').trim();
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
const WHATSAPP_CONTACT_IMPORT_LIMIT = Math.max(1, Number(process.env.WHATSAPP_CONTACT_IMPORT_LIMIT) || 2500);
const WHATSAPP_IMPORT_CONCURRENCY = Math.max(1, Number(process.env.WHATSAPP_IMPORT_CONCURRENCY) || 4);
const WHATSAPP_IMPORT_BATCH_SIZE = Math.max(1, Number(process.env.WHATSAPP_IMPORT_BATCH_SIZE) || 80);
const WHATSAPP_IMAGE_MAX_BYTES = Math.max(40_000, Number(process.env.WHATSAPP_IMAGE_MAX_BYTES) || 260_000);
const WHATSAPP_MEDIA_TIMEOUT_MS = Math.max(5_000, Number(process.env.WHATSAPP_MEDIA_TIMEOUT_MS) || 15_000);
const WHATSAPP_SESSION_IDLE_TIMEOUT_MS = Math.max(
  60_000,
  Number(process.env.WHATSAPP_SESSION_IDLE_TIMEOUT_MS) || 5 * 60 * 1000,
);
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
  person: ['tags', 'markers', 'roles', 'skills', 'links', 'importance'],
  company: ['tags', 'industry', 'departments', 'stage', 'risks', 'links', 'phones', 'importance'],
  event: ['tags', 'date', 'location', 'participants', 'outcomes', 'links', 'importance'],
  resource: ['tags', 'resources', 'status', 'owners', 'links', 'importance'],
  goal: ['tags', 'priority', 'metrics', 'owners', 'status', 'links', 'importance'],
  result: ['tags', 'outcomes', 'metrics', 'owners', 'links', 'importance'],
  task: ['tags', 'priority', 'status', 'owners', 'date', 'links', 'importance'],
  project: ['tags', 'stage', 'priority', 'risks', 'owners', 'links', 'importance'],
  shape: ['tags', 'markers', 'status', 'links', 'importance'],
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
const whatsappSessionsByOwner = new Map();
const entityEventStreamsByOwner = new Map();
let entityEventStreamSeq = 0;

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

function toTrimmedTailString(value, maxLength = 240) {
  if (typeof value !== 'string') return '';
  const trimmed = value.trim();
  if (!trimmed) return '';
  if (trimmed.length <= maxLength) return trimmed;
  return trimmed.slice(trimmed.length - maxLength);
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
  return toTrimmedString(value, 16).toLowerCase() === 'manual' ? 'manual' : 'auto';
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
    const manualImportance = normalizeImportanceArray(metadata.importance);
    if (sourceMode === 'manual' && manualImportance.length) {
      metadata.importance = manualImportance;
      metadata.importance_source = 'manual';
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
  if (!payload.ai_metadata || typeof payload.ai_metadata !== 'object' || Array.isArray(payload.ai_metadata)) {
    return payload;
  }

  const metadata = {
    ...toProfile(payload.ai_metadata),
  };

  if (Object.prototype.hasOwnProperty.call(metadata, 'importance') && !metadata.importance_source) {
    metadata.importance_source = 'manual';
  }

  payload.ai_metadata = enrichEntityMetadata(options.existingMetadata, metadata, {
    source: options.source,
  });
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
    'importance: только одно из [Низкая, Средняя, Высокая], вернуть как массив из 0..1 элементов.',
    'links: только валидные URL.',
    'description: 3-6 предложений, емко и без воды.',
    'changeType: одно из [initial, addition, update] относительно текущего описания.',
    'changeReason: кратко (1-2 фразы), почему это initial/addition/update.',
    'importanceSignal: одно из [increase, decrease, neutral] на основе новых фактов и истории.',
    'importanceReason: кратко, почему важность нужно повысить/понизить/оставить.',
    'Если данных мало, status=need_clarification и до 3 уточняющих вопросов.',
    'Если данных хватает, status=ready.',
    'Верни СТРОГО JSON без markdown.',
    'Формат:',
    '{',
    '  "status": "ready | need_clarification",',
    '  "description": "string",',
    '  "changeType": "initial | addition | update",',
    '  "changeReason": "string",',
    '  "fields": { "tags": [], "roles": [], ... },',
    '  "importanceSignal": "increase | decrease | neutral",',
    '  "importanceReason": "string",',
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
    descriptionContext: {
      currentDescription: toTrimmedString(toProfile(entity.ai_metadata).description, 2200),
      recentDescriptionHistory: normalizeDescriptionHistory(toProfile(entity.ai_metadata).description_history)
        .slice(-5)
        .map((row) => ({
          at: row.at,
          changeType: row.changeType,
          reason: row.reason,
        })),
      recentImportanceHistory: normalizeImportanceHistory(toProfile(entity.ai_metadata).importance_history)
        .slice(-5)
        .map((row) => ({
          at: row.at,
          before: row.before,
          after: row.after,
          signal: row.signal,
          reason: row.reason,
        })),
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
    const changeLabels = {
      initial: 'Первичное описание',
      addition: 'Описание дополнено',
      update: 'Описание обновлено',
    };
    const changeLabel = changeLabels[analysis.changeType] || 'Описание обновлено';
    return `Готово. ${changeLabel}.\n\n${analysis.description}`;
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
  const manualImportanceWasSet = normalizeImportanceSource(nextMetadata.importance_source) === 'manual';

  if (typeof analysis.description === 'string') {
    nextMetadata.description = analysis.description;
  }

  const allowedFields = getEntityAnalyzerFields(entityType);
  const normalizedFields = normalizeEntityAnalysisFields(entityType, analysis.fields);
  for (const field of allowedFields) {
    if (field === 'importance' && manualImportanceWasSet) {
      continue;
    }
    nextMetadata[field] = normalizedFields[field] || [];
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
      const data = toTrimmedString(attachment.data, AI_ATTACHMENT_DATA_URL_MAX_LENGTH);
      const text = toTrimmedString(attachment.text, AI_ATTACHMENT_TEXT_MAX_LENGTH);
      return compactObject({ name, mime, size, data, text });
    })
    .filter(Boolean)
    .slice(0, AI_ATTACHMENT_LIMIT);
}

function parseDataUrl(value) {
  const raw = toTrimmedString(value, AI_ATTACHMENT_DATA_URL_MAX_LENGTH);
  if (!raw.startsWith('data:')) return null;

  const commaIndex = raw.indexOf(',');
  if (commaIndex <= 5) return null;

  const meta = raw.slice(5, commaIndex);
  const payload = raw.slice(commaIndex + 1);
  const metaParts = meta.split(';').map((part) => part.trim()).filter(Boolean);
  const mime = toTrimmedString(metaParts[0] || '', 160).toLowerCase();
  const isBase64 = metaParts.includes('base64');
  if (!payload) return null;

  return { mime, isBase64, payload };
}

function shouldTreatAsTextAttachment(name, mime) {
  const loweredMime = toTrimmedString(mime, 120).toLowerCase();
  const loweredName = toTrimmedString(name, 160).toLowerCase();
  if (loweredMime.startsWith('text/')) return true;
  if (loweredMime === 'application/json') return true;
  if (loweredMime === 'application/xml') return true;
  if (loweredMime === 'application/x-yaml') return true;
  return (
    loweredName.endsWith('.txt') ||
    loweredName.endsWith('.md') ||
    loweredName.endsWith('.json') ||
    loweredName.endsWith('.csv') ||
    loweredName.endsWith('.yaml') ||
    loweredName.endsWith('.yml') ||
    loweredName.endsWith('.xml') ||
    loweredName.endsWith('.log')
  );
}

function isDocxAttachment(name, mime) {
  const loweredMime = toTrimmedString(mime, 120).toLowerCase();
  const loweredName = toTrimmedString(name, 160).toLowerCase();
  return (
    loweredMime === 'application/vnd.openxmlformats-officedocument.wordprocessingml.document' ||
    loweredName.endsWith('.docx')
  );
}

function decodeAttachmentBuffer(attachment) {
  const parsed = parseDataUrl(attachment?.data);
  if (!parsed) return null;

  let buffer = null;
  try {
    buffer = parsed.isBase64
      ? Buffer.from(parsed.payload, 'base64')
      : Buffer.from(decodeURIComponent(parsed.payload), 'utf8');
  } catch {
    return null;
  }

  if (!buffer || !buffer.length) return null;
  if (buffer.length > AI_ATTACHMENT_BINARY_MAX_BYTES) return null;

  return {
    buffer,
    mime: parsed.mime || toTrimmedString(attachment?.mime, 120).toLowerCase(),
  };
}

async function extractAttachmentText(attachment) {
  const directText = toTrimmedString(attachment?.text, AI_ATTACHMENT_TEXT_MAX_LENGTH);
  if (directText) {
    return directText;
  }

  const decoded = decodeAttachmentBuffer(attachment);
  if (!decoded) return '';

  const name = toTrimmedString(attachment?.name, 120);
  const mime = toTrimmedString(attachment?.mime, 120).toLowerCase() || decoded.mime;

  if (shouldTreatAsTextAttachment(name, mime)) {
    return toTrimmedString(decoded.buffer.toString('utf8'), AI_ATTACHMENT_TEXT_MAX_LENGTH);
  }

  if (isDocxAttachment(name, mime) && mammoth) {
    try {
      const parsed = await mammoth.extractRawText({ buffer: decoded.buffer });
      return toTrimmedString(parsed?.value, AI_ATTACHMENT_TEXT_MAX_LENGTH);
    } catch {
      return '';
    }
  }

  return '';
}

async function prepareAgentAttachments(rawAttachments) {
  const normalized = normalizeAgentAttachments(rawAttachments);
  const prepared = [];

  for (const attachment of normalized) {
    const text = await extractAttachmentText(attachment);
    prepared.push(
      compactObject({
        name: attachment.name,
        mime: attachment.mime,
        size: attachment.size,
        text,
        hasInlineData: Boolean(attachment.data),
      }),
    );
  }

  return prepared;
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
    roles: toStringArray(aiMetadata.roles, 8),
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
    'Если пользователь просит "повторить анализ" или "обновить вывод", анализируй текущий контекст как есть и историю диалога.',
    'Не отвечай "данные не предоставлены", если в контексте уже есть описание/теги/поля сущностей.',
    'Фразу "Недостаточно данных в текущем контексте" используй только когда в контексте реально нет фактов для вывода.',
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

  appendWhatsappSessionLog(session, 'session.stop', { reason: reason || 'manual' });
  clearWhatsappSessionInitTimer(session);
  clearWhatsappSessionIdleTimer(session);
  clearWhatsappSessionReconnectTimer(session);
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
    connectionListener: null,
    credsListener: null,
    historyListener: null,
    contactsUpsertListener: null,
    contactsUpdateListener: null,
    chatsUpsertListener: null,
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

  const { state, saveCreds } = await useMultiFileAuthState(authDir);
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
      auth: state,
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
      saveCreds().catch(() => {
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

app.post('/api/integrations/whatsapp/import', requireAuth, async (req, res, next) => {
  try {
    const ownerId = requireOwnerId(req);
    const sessionId = toTrimmedString(req.body?.sessionId, 120);
    const includeImages = req.body?.includeImages === true;
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

    if (!session.client) {
      return res.status(500).json({ message: 'WhatsApp client is unavailable for this session.' });
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

    session.status = 'importing';
    session.error = '';
    setImportProgress('prepare', 5, 0, 0, 'Подготовка импорта');
    appendWhatsappSessionLog(session, 'import.start', {
      connector: session.connector,
      sessionId: session.id,
      includeImages,
      cursor: requestedCursor,
      batchSize,
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
          contact.name || contact.notify || contact.pushname || contact.shortName || contact.displayName,
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
        const businessProfile =
          session.connector === 'baileys' ? {} : toProfile(contact.businessProfile);
        const websites = Array.isArray(businessProfile.websites)
          ? businessProfile.websites
          : [businessProfile.websites].filter(Boolean);

        const normalized = normalizeWhatsappContact(
          {
            name: contact.name || contact.notify,
            displayName: contact.pushname || contact.verifiedName,
            fullName: contact.shortName || contact.displayName,
            phone:
              contact.number ||
              contact.phone ||
              contact.id?.user ||
              normalizeWhatsappJidToPhone(contact.jid || contact.id?._serialized || contact.id),
            id: contact.id?._serialized || contact.jid || contact.id,
            description: about || businessProfile.description || '',
            links: websites,
            tags: ['WhatsApp'],
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
      appendWhatsappSessionLog(session, 'import.result', {
        imported: 0,
        matched: 0,
        total: totalCandidates,
        cursor,
        nextCursor,
        hasMore,
        reason: 'normalized_contacts_empty_batch',
      });
      session.status = 'ready';
      session.error = '';
      setImportProgress('done', 100, nextCursor, totalCandidates, 'Батч обработан');
      session.importProgress = null;
      touchWhatsappSession(session, ownerId);
      return res.status(200).json({
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
        entities: [],
        session: toWhatsappSessionStatus(session),
      });
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
        $or: importIdentityFilters,
      },
      { _id: 1, profile: 1, name: 1 },
    ).lean();

    let existingPhoneEntities = [];
    if (importPhoneVariants.length) {
      existingPhoneEntities = await Entity.find(
        {
          owner_id: ownerId,
          type: { $in: ['connection', 'person', 'company'] },
          $or: [{ 'profile.phone': { $in: importPhoneVariants } }, { 'profile.phones': { $in: importPhoneVariants } }],
        },
        { _id: 1, profile: 1 },
      ).lean();
    }

    let existingNameEntities = [];
    if (importNameKeys.length) {
      existingNameEntities = await Entity.find(
        {
          owner_id: ownerId,
          type: { $in: ['connection', 'person', 'company'] },
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

    for (const entity of existingImportIdentityEntities) {
      const profile = toProfile(entity.profile);
      const importKey = toTrimmedString(profile.import_key, 180);
      const importJid = toTrimmedString(profile.import_jid, 220);
      if (importKey) {
        existingKeySet.add(importKey);
      }
      if (importJid) {
        existingJidSet.add(importJid);
      }
    }

    for (const entity of existingPhoneEntities) {
      const profile = toProfile(entity.profile);
      for (const phone of extractNormalizedPhonesFromProfile(profile)) {
        if (importPhoneSet.has(phone) || importPhoneSet.has(phone.startsWith('+') ? phone.slice(1) : `+${phone}`)) {
          existingPhoneSet.add(phone);
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
    });
    setImportProgress('match', 82, nextCursor, totalCandidates, 'Сопоставление завершено');

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
    session.status = 'ready';
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
      cursor,
      nextCursor,
      hasMore,
      batchSize,
      batchCount: batchCandidates.length,
    });
    touchWhatsappSession(session, ownerId);

    return res.status(200).json({
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
      entities: createdEntities,
      session: toWhatsappSessionStatus(session),
    });
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
    const attachments = await prepareAgentAttachments(req.body?.attachments);
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
    const attachments = await prepareAgentAttachments(req.body?.attachments);
    const documents = await prepareAgentAttachments(req.body?.documents);

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

    const entities = await Entity.find(filter).sort({ createdAt: -1, _id: -1 });
    res.json(entities);
  } catch (error) {
    next(error);
  }
});

app.post('/api/entities', async (req, res, next) => {
  try {
    const ownerId = requireOwnerId(req);
    const payload = normalizeIncomingEntityPayload(req.body, { source: 'system' });
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
    const existingEntity = await Entity.findOne(
      {
        _id: req.params.id,
        owner_id: ownerId,
      },
      { ai_metadata: 1 },
    ).lean();
    if (!existingEntity) {
      return res.status(404).json({ message: 'Entity not found' });
    }

    const payload = normalizeIncomingEntityPayload(req.body, {
      existingMetadata: existingEntity.ai_metadata,
      source: 'manual',
    });
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

    broadcastEntityEvent(ownerId, 'entity.updated', {
      entity: updatedEntity.toObject(),
    });

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
    const removedEntityIds = new Set([entityId]);

    if (entityToDelete.type === 'project') {
      const projectCanvas = normalizeProjectCanvasData(entityToDelete.canvas_data);
      const nodeEntityIds = Array.from(
        new Set(
          projectCanvas.nodes
            .map((node) => node.entityId)
            .filter((id) => id && id !== entityId),
        ),
      );
      for (const nodeEntityId of nodeEntityIds) {
        removedEntityIds.add(nodeEntityId);
      }

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
      `[ai] Enabled models: chat=${OPENAI_MODEL}, embedding=${OPENAI_EMBEDDING_MODEL}, debugEcho=${AI_DEBUG_ECHO}`,
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
