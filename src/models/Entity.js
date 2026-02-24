const mongoose = require('mongoose');

const ALLOWED_TYPES = [
  'project',
  'person',
  'company',
  'event',
  'resource',
  'goal',
  'result',
  'task',
  'shape',
];

const entitySchema = new mongoose.Schema(
  {
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
    ai_metadata: {
      type: mongoose.Schema.Types.Mixed,
      default: {},
    },
    canvas_data: {
      type: mongoose.Schema.Types.Mixed,
      default: undefined,
      validate: {
        validator(value) {
          if (this.type !== 'project') {
            return value === undefined || value === null || Object.keys(value).length === 0;
          }

          if (!value || typeof value !== 'object') {
            return false;
          }

          return Array.isArray(value.nodes) && Array.isArray(value.edges);
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
  if (this.type === 'project' && !this.canvas_data) {
    this.canvas_data = {
      nodes: [],
      edges: [],
    };
  }

  if (this.type !== 'project' && this.canvas_data && Object.keys(this.canvas_data).length === 0) {
    this.canvas_data = undefined;
  }
});

module.exports = mongoose.model('Entity', entitySchema);
