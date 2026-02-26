const express = require('express');
const cors = require('cors');
const dotenv = require('dotenv');
const cookieParser = require('cookie-parser');
const jwt = require('jsonwebtoken');
const { OAuth2Client } = require('google-auth-library');

const connectDB = require('./config/db');
const Entity = require('./models/Entity');
const User = require('./models/User');

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
const AI_CONTEXT_ENTITY_LIMIT = Math.max(1, Number(process.env.AI_CONTEXT_ENTITY_LIMIT) || 120);
const AI_HISTORY_MESSAGE_LIMIT = Math.max(1, Number(process.env.AI_HISTORY_MESSAGE_LIMIT) || 12);
const AI_ATTACHMENT_LIMIT = Math.max(1, Number(process.env.AI_ATTACHMENT_LIMIT) || 6);
const ENTITY_TYPES = new Set([
  'project',
  'person',
  'company',
  'event',
  'resource',
  'goal',
  'result',
  'task',
  'shape',
]);

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
    console.warn('[ai] OPENAI_API_KEY is not set. /api/ai/agent-chat will be unavailable.');
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
