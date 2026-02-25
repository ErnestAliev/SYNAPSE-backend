const express = require('express');
const cors = require('cors');
const dotenv = require('dotenv');

const connectDB = require('./config/db');
const Entity = require('./models/Entity');

dotenv.config();

const app = express();
const PORT = Number(process.env.PORT) || 3001;
const LEGACY_SHAPE_NAME_PATTERN = /^Пуст(?:ой|ая|ые)(?:\s*-\s*(\d+))?$/i;

app.use(cors());
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

async function removeEntityFromProjectCanvases(entityId) {
  return removeEntitiesFromProjectCanvases([entityId]);
}

async function removeEntitiesFromProjectCanvases(entityIds) {
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
    { type: 'project', 'canvas_data.nodes.entityId': { $in: normalizedIds } },
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
        filter: { _id: project._id },
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

app.get('/api/entities', async (req, res, next) => {
  try {
    const filter = {};

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
    const entity = await Entity.create(req.body);
    res.status(201).json(entity);
  } catch (error) {
    next(error);
  }
});

app.put('/api/entities/:id', async (req, res, next) => {
  try {
    const updatedEntity = await Entity.findByIdAndUpdate(req.params.id, req.body, {
      new: true,
      runValidators: true,
    });

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
    const entityToDelete = await Entity.findById(req.params.id, { _id: 1, type: 1, canvas_data: 1 }).lean();
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

      await removeEntitiesFromProjectCanvases([entityId, ...nodeEntityIds]);
      if (nodeEntityIds.length) {
        await Entity.deleteMany({ _id: { $in: nodeEntityIds } });
      }
    } else {
      await removeEntityFromProjectCanvases(entityId);
    }

    await Entity.deleteOne({ _id: entityToDelete._id });

    return res.status(204).send();
  } catch (error) {
    return next(error);
  }
});

app.use((err, req, res, next) => {
  console.error(err);

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

  app.listen(PORT, () => {
    console.log(`Backend server started on port ${PORT}`);
  });
}

startServer();
