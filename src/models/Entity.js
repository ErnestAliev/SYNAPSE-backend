const mongoose = require('mongoose');

const ALLOWED_TYPES = [
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
];

const LEGACY_SHAPE_NAME_PATTERN = /^Пуст(?:ой|ая|ые)(?:\s*-\s*(\d+))?$/i;

function normalizeShapeName(name) {
  if (typeof name !== 'string') return name;

  const trimmed = name.trim();
  const match = trimmed.match(LEGACY_SHAPE_NAME_PATTERN);
  if (!match) return trimmed;

  const serial = match[1];
  return serial ? `Элемент - ${serial}` : 'Элемент';
}

function normalizeShapeNameForUpdate(update) {
  if (!update || typeof update !== 'object') return update;

  const hasSet = !!update.$set && typeof update.$set === 'object';
  const container = hasSet ? update.$set : update;
  const nextType = container.type ?? update.type;

  if (nextType !== 'shape') {
    return update;
  }

  if (typeof container.name === 'string') {
    container.name = normalizeShapeName(container.name);
  }

  if (hasSet) {
    update.$set = container;
  }

  return update;
}

function toBoolean(value) {
  if (typeof value === 'boolean') return value;
  if (typeof value === 'string') {
    const normalized = value.trim().toLowerCase();
    if (normalized === 'true') return true;
    if (normalized === 'false') return false;
  }
  return false;
}

function normalizeMineFlagsForUpdate(update) {
  if (!update || typeof update !== 'object' || Array.isArray(update)) return update;

  const hasSet = !!update.$set && typeof update.$set === 'object' && !Array.isArray(update.$set);
  const container = hasSet ? update.$set : update;
  const nextType = container.type ?? update.type;

  if (typeof nextType !== 'string') {
    if (Object.prototype.hasOwnProperty.call(container, 'is_me')) {
      const nextIsMe = toBoolean(container.is_me);
      container.is_me = nextIsMe;
      if (nextIsMe) {
        container.is_mine = true;
      }
    }

    if (Object.prototype.hasOwnProperty.call(container, 'is_mine')) {
      container.is_mine = toBoolean(container.is_mine);
    }

    if (hasSet) {
      update.$set = container;
    }
    return update;
  }

  if (nextType === 'person') {
    const nextIsMe = toBoolean(container.is_me);
    const nextIsMine = toBoolean(container.is_mine);
    container.is_me = nextIsMe;
    container.is_mine = nextIsMe ? true : nextIsMine;
  } else {
    container.is_me = false;
    container.is_mine = toBoolean(container.is_mine);
  }

  if (hasSet) {
    update.$set = container;
  }

  return update;
}

function isEmptyCanvasData(value) {
  return value === undefined || value === null || (typeof value === 'object' && Object.keys(value).length === 0);
}

function isProjectCanvasData(value) {
  if (!value || typeof value !== 'object') {
    return false;
  }

  return Array.isArray(value.nodes) && Array.isArray(value.edges);
}

function resolveTypeFromValidationContext(ctx) {
  if (!ctx) return null;

  if (typeof ctx.type === 'string') {
    return ctx.type;
  }

  if (typeof ctx.getUpdate === 'function') {
    const update = ctx.getUpdate() || {};
    const updateSet = update.$set || {};
    const type = updateSet.type || update.type;
    if (typeof type === 'string') {
      return type;
    }
  }

  return null;
}

const entitySchema = new mongoose.Schema(
  {
    owner_id: {
      type: String,
      trim: true,
      index: true,
    },
    type: {
      type: String,
      required: true,
      enum: ALLOWED_TYPES,
    },
    name: {
      type: String,
      trim: true,
      default: '',
    },
    profile: {
      type: mongoose.Schema.Types.Mixed,
      default: {},
    },
    is_mine: {
      type: Boolean,
      default: false,
    },
    is_me: {
      type: Boolean,
      default: false,
    },
    ai_metadata: {
      type: mongoose.Schema.Types.Mixed,
      default: {},
    },
    canvas_data: {
      type: mongoose.Schema.Types.Mixed,
      default: undefined,
      validate: {
        validator(value) {
          const resolvedType = resolveTypeFromValidationContext(this);

          if (resolvedType === 'project') {
            return isProjectCanvasData(value);
          }

          if (resolvedType && resolvedType !== 'project') {
            return isEmptyCanvasData(value);
          }

          // Query validators may not always have `type` available (e.g. update only canvas_data).
          // In this case accept both valid project-shape and empty payloads.
          return isEmptyCanvasData(value) || isProjectCanvasData(value);
        },
        message:
          "canvas_data must be empty for non-project entities and contain { nodes: [], edges: [] } for 'project' entities",
      },
    },
  },
  {
    timestamps: true,
    collection: 'entities',
  },
);

entitySchema.pre('validate', function preValidate() {
  if (this.type === 'shape' && typeof this.name === 'string') {
    this.name = normalizeShapeName(this.name);
  }

  if (this.type === 'person') {
    const nextIsMe = toBoolean(this.is_me);
    const nextIsMine = toBoolean(this.is_mine);
    this.is_me = nextIsMe;
    this.is_mine = nextIsMe ? true : nextIsMine;
  } else {
    this.is_me = false;
    this.is_mine = toBoolean(this.is_mine);
  }

  if (this.type === 'project' && !this.canvas_data) {
    this.canvas_data = {
      nodes: [],
      edges: [],
    };
  }

  if (this.type !== 'project' && isEmptyCanvasData(this.canvas_data)) {
    this.canvas_data = undefined;
  }
});

entitySchema.pre('findOneAndUpdate', function preFindOneAndUpdate() {
  const update = this.getUpdate() || {};
  const normalized = normalizeMineFlagsForUpdate(normalizeShapeNameForUpdate(update));
  this.setUpdate(normalized);
});

module.exports = mongoose.model('Entity', entitySchema);
