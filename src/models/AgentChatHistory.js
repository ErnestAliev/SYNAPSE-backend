const mongoose = require('mongoose');

const chatAttachmentSchema = new mongoose.Schema(
  {
    id: {
      type: String,
      required: true,
      trim: true,
    },
    name: {
      type: String,
      default: 'Файл',
      trim: true,
    },
    mime: {
      type: String,
      default: '',
      trim: true,
    },
    size: {
      type: Number,
      default: 0,
    },
    data: {
      type: String,
      default: '',
    },
  },
  {
    _id: false,
  },
);

const chatMessageSchema = new mongoose.Schema(
  {
    id: {
      type: String,
      required: true,
      trim: true,
    },
    role: {
      type: String,
      required: true,
      enum: ['user', 'assistant'],
    },
    text: {
      type: String,
      default: '',
      trim: true,
    },
    createdAt: {
      type: Date,
      required: true,
    },
    attachments: {
      type: [chatAttachmentSchema],
      default: [],
    },
  },
  {
    _id: false,
  },
);

const agentChatHistorySchema = new mongoose.Schema(
  {
    owner_id: {
      type: String,
      required: true,
      trim: true,
      index: true,
    },
    scope_key: {
      type: String,
      required: true,
      trim: true,
    },
    scope_type: {
      type: String,
      required: true,
      enum: ['collection', 'project'],
    },
    entity_type: {
      type: String,
      default: '',
      trim: true,
    },
    project_id: {
      type: String,
      default: '',
      trim: true,
    },
    messages: {
      type: [chatMessageSchema],
      default: [],
    },
  },
  {
    timestamps: true,
    collection: 'agent_chat_histories',
  },
);

agentChatHistorySchema.index({ owner_id: 1, scope_key: 1 }, { unique: true });
agentChatHistorySchema.index({ owner_id: 1, updatedAt: -1 });

module.exports = mongoose.model('AgentChatHistory', agentChatHistorySchema);
