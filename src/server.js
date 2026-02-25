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
const AUTH_REQUIRED = String(process.env.AUTH_REQUIRED || '').toLowerCase() === 'true';
const DEV_AUTH_ENABLED =
  !IS_PRODUCTION && String(process.env.DEV_AUTH_ENABLED || 'true').toLowerCase() !== 'false';
const DEFAULT_ALLOWED_ORIGINS = ['http://localhost:5173', 'http://localhost:3000'];

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

async function removeEntityFromProjectCanvases(entityId, ownerId) {
  return removeEntitiesFromProjectCanvases([entityId], ownerId);
}

async function removeEntitiesFromProjectCanvases(entityIds, ownerId) {
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
      ...(ownerId ? { owner_id: ownerId } : {}),
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
        filter: { _id: project._id, ...(ownerId ? { owner_id: ownerId } : {}) },
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

if (AUTH_REQUIRED) {
  app.use('/api/entities', requireAuth);
}

app.get('/api/entities', async (req, res, next) => {
  try {
    const filter = {};
    const ownerId = getOwnerIdFromRequest(req);

    if (ownerId) {
      filter.owner_id = ownerId;
    }

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
    const ownerId = getOwnerIdFromRequest(req);
    const payload = req.body && typeof req.body === 'object' ? { ...req.body } : {};

    if (ownerId) {
      payload.owner_id = ownerId;
    }

    const entity = await Entity.create(payload);
    res.status(201).json(entity);
  } catch (error) {
    next(error);
  }
});

app.put('/api/entities/:id', async (req, res, next) => {
  try {
    const ownerId = getOwnerIdFromRequest(req);
    const payload = req.body && typeof req.body === 'object' ? { ...req.body } : {};
    if (ownerId) {
      payload.owner_id = ownerId;
    }

    const updatedEntity = await Entity.findOneAndUpdate(
      {
        _id: req.params.id,
        ...(ownerId ? { owner_id: ownerId } : {}),
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
    const ownerId = getOwnerIdFromRequest(req);
    const entityToDelete = await Entity.findOne(
      {
        _id: req.params.id,
        ...(ownerId ? { owner_id: ownerId } : {}),
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
          ...(ownerId ? { owner_id: ownerId } : {}),
        });
      }
    } else {
      await removeEntityFromProjectCanvases(entityId, ownerId);
    }

    await Entity.deleteOne({
      _id: entityToDelete._id,
      ...(ownerId ? { owner_id: ownerId } : {}),
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
  if (DEV_AUTH_ENABLED) {
    console.warn('[auth] DEV_AUTH_ENABLED=true. /api/auth/dev-login is available (development only).');
  }
  if (AUTH_REQUIRED && (!GOOGLE_CLIENT_ID || !SESSION_SECRET)) {
    console.warn(
      '[auth] AUTH_REQUIRED=true but auth config is incomplete. Check GOOGLE_CLIENT_ID and SESSION_SECRET.',
    );
  }

  app.listen(PORT, () => {
    console.log(`Backend server started on port ${PORT}`);
  });
}

startServer();
