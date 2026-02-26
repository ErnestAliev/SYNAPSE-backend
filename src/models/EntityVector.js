const mongoose = require('mongoose');

const entityVectorSchema = new mongoose.Schema(
  {
    owner_id: {
      type: String,
      required: true,
      trim: true,
      index: true,
    },
    entity_id: {
      type: String,
      required: true,
      trim: true,
      index: true,
    },
    entity_type: {
      type: String,
      required: true,
      trim: true,
    },
    model: {
      type: String,
      required: true,
      trim: true,
    },
    vector: {
      type: [Number],
      default: [],
    },
    weights: {
      type: mongoose.Schema.Types.Mixed,
      default: {},
    },
    content: {
      type: mongoose.Schema.Types.Mixed,
      default: {},
    },
  },
  {
    timestamps: true,
    collection: 'entity_vectors',
  },
);

entityVectorSchema.index({ owner_id: 1, entity_id: 1 }, { unique: true });

module.exports = mongoose.model('EntityVector', entityVectorSchema);
