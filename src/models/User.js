const mongoose = require('mongoose');

const userSchema = new mongoose.Schema(
  {
    provider: {
      type: String,
      required: true,
      enum: ['google', 'dev'],
      trim: true,
    },
    providerId: {
      type: String,
      required: true,
      trim: true,
    },
    email: {
      type: String,
      required: true,
      trim: true,
      lowercase: true,
    },
    name: {
      type: String,
      required: true,
      trim: true,
    },
    picture: {
      type: String,
      default: '',
      trim: true,
    },
    givenName: {
      type: String,
      default: '',
      trim: true,
    },
    familyName: {
      type: String,
      default: '',
      trim: true,
    },
    settings: {
      type: mongoose.Schema.Types.Mixed,
      default: {
        locale: 'ru',
        onboardingCompleted: false,
      },
    },
    lastLoginAt: {
      type: Date,
      default: Date.now,
    },
  },
  {
    timestamps: true,
    collection: 'users',
  },
);

userSchema.index({ provider: 1, providerId: 1 }, { unique: true });
userSchema.index({ email: 1 });

module.exports = mongoose.model('User', userSchema);
