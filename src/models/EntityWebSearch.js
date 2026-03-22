const mongoose = require('mongoose');

const webSearchCitationSchema = new mongoose.Schema(
  {
    id: { type: String, default: '', trim: true },
    sourceIndex: { type: Number, default: 0 },
    title: { type: String, default: '', trim: true },
    url: { type: String, default: '', trim: true },
    domain: { type: String, default: '', trim: true },
    startIndex: { type: Number, default: 0 },
    endIndex: { type: Number, default: 0 },
  },
  { _id: false },
);

const webSearchImageSchema = new mongoose.Schema(
  {
    id: { type: String, default: '', trim: true },
    imageUrl: { type: String, default: '', trim: true },
    thumbnailUrl: { type: String, default: '', trim: true },
    title: { type: String, default: '', trim: true },
    domain: { type: String, default: '', trim: true },
    sourcePageUrl: { type: String, default: '', trim: true },
    width: { type: Number, default: 0 },
    height: { type: Number, default: 0 },
  },
  { _id: false },
);

const webSearchEntrySchema = new mongoose.Schema(
  {
    status: {
      type: String,
      enum: ['idle', 'searching', 'ready', 'failed'],
      default: 'idle',
    },
    query: { type: String, default: '', trim: true },
    summary: { type: String, default: '', trim: true },
    citations: { type: [webSearchCitationSchema], default: [] },
    images: { type: [webSearchImageSchema], default: [] },
    errorMessage: { type: String, default: '', trim: true },
    startedAt: { type: String, default: '', trim: true },
    completedAt: { type: String, default: '', trim: true },
    updatedAt: { type: String, default: '', trim: true },
    model: { type: String, default: '', trim: true },
    sourceCount: { type: Number, default: 0 },
    searchQueries: { type: [String], default: [] },
    fieldSuggestion: { type: mongoose.Schema.Types.Mixed, default: () => ({}) },
  },
  { _id: false },
);

const entityWebSearchSchema = new mongoose.Schema(
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
    },
    project_id: {
      type: String,
      default: '',
      trim: true,
    },
    current: {
      type: webSearchEntrySchema,
      default: () => ({}),
    },
    history: {
      type: [webSearchEntrySchema],
      default: [],
    },
  },
  {
    timestamps: true,
    collection: 'entity_web_searches',
  },
);

entityWebSearchSchema.index({ owner_id: 1, entity_id: 1 }, { unique: true });
entityWebSearchSchema.index({ owner_id: 1, project_id: 1, updatedAt: -1 });

module.exports = mongoose.model('EntityWebSearch', entityWebSearchSchema);
