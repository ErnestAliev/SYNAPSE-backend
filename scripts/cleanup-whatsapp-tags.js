#!/usr/bin/env node
'use strict';

require('dotenv').config();
const mongoose = require('mongoose');
const connectDB = require('../src/config/db');
const Entity = require('../src/models/Entity');

function parseArgs(argv) {
  const flags = new Set();
  const values = new Map();
  for (const token of argv) {
    if (!token.startsWith('--')) continue;
    const eqIndex = token.indexOf('=');
    if (eqIndex === -1) {
      flags.add(token.slice(2));
      continue;
    }
    const key = token.slice(2, eqIndex);
    const value = token.slice(eqIndex + 1);
    values.set(key, value);
  }
  return { flags, values };
}

function normalizeTag(value) {
  if (typeof value !== 'string') return '';
  return value.trim().toLowerCase().replace(/[\s_.-]+/g, '');
}

function isWhatsappTag(value) {
  const normalized = normalizeTag(value);
  if (!normalized) return false;
  return new Set([
    'whatsapp',
    'ватсап',
    'ватсапп',
    'ватцап',
    'вацап',
    'вотсап',
  ]).has(normalized);
}

function sanitizeTags(value) {
  if (!Array.isArray(value)) return { changed: false, next: value };
  const next = value.filter((item) => !isWhatsappTag(item));
  if (next.length === value.length) {
    return { changed: false, next: value };
  }
  return { changed: true, next };
}

async function main() {
  const { flags, values } = parseArgs(process.argv.slice(2));
  const apply = flags.has('apply');
  const ownerId = values.get('owner') ? String(values.get('owner')).trim() : '';

  await connectDB();

  const query = {};
  if (ownerId) {
    query.owner_id = ownerId;
  }

  const cursor = Entity.find(query, { _id: 1, owner_id: 1, profile: 1, ai_metadata: 1 }).lean().cursor();
  let scanned = 0;
  let matched = 0;
  let profileChanged = 0;
  let aiChanged = 0;
  const ops = [];

  for await (const doc of cursor) {
    scanned += 1;
    const profile = doc && doc.profile && typeof doc.profile === 'object' && !Array.isArray(doc.profile) ? doc.profile : {};
    const ai = doc && doc.ai_metadata && typeof doc.ai_metadata === 'object' && !Array.isArray(doc.ai_metadata) ? doc.ai_metadata : {};

    const profileTags = sanitizeTags(profile.tags);
    const aiTags = sanitizeTags(ai.tags);
    if (!profileTags.changed && !aiTags.changed) {
      continue;
    }

    matched += 1;
    if (profileTags.changed) profileChanged += 1;
    if (aiTags.changed) aiChanged += 1;

    if (!apply) continue;

    const setFields = {};
    if (profileTags.changed) {
      setFields['profile.tags'] = profileTags.next;
    }
    if (aiTags.changed) {
      setFields['ai_metadata.tags'] = aiTags.next;
    }
    ops.push({
      updateOne: {
        filter: { _id: doc._id },
        update: { $set: setFields },
      },
    });
  }

  if (apply && ops.length) {
    await Entity.bulkWrite(ops, { ordered: false });
  }

  console.log('[cleanup-whatsapp-tags] done');
  console.log(`[cleanup-whatsapp-tags] mode=${apply ? 'apply' : 'dry-run'}`);
  console.log(`[cleanup-whatsapp-tags] scanned=${scanned}`);
  console.log(`[cleanup-whatsapp-tags] matched=${matched}`);
  console.log(`[cleanup-whatsapp-tags] profile.tags changed=${profileChanged}`);
  console.log(`[cleanup-whatsapp-tags] ai_metadata.tags changed=${aiChanged}`);
  if (!apply) {
    console.log('[cleanup-whatsapp-tags] no writes performed. Re-run with --apply to update database.');
  }
}

main()
  .catch((error) => {
    console.error('[cleanup-whatsapp-tags] failed:', error?.message || error);
    process.exitCode = 1;
  })
  .finally(async () => {
    try {
      await mongoose.disconnect();
    } catch {
      // Ignore disconnect errors.
    }
  });
