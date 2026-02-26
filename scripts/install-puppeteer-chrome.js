const { spawnSync } = require('child_process');
const fs = require('fs');
const path = require('path');

const skip =
  String(process.env.PUPPETEER_SKIP_CHROME_DOWNLOAD || '')
    .trim()
    .toLowerCase() === 'true';

if (skip) {
  console.log('[postinstall] Skip Chrome download: PUPPETEER_SKIP_CHROME_DOWNLOAD=true');
  process.exit(0);
}

const cacheDir = process.env.PUPPETEER_CACHE_DIR || path.resolve(process.cwd(), '.cache', 'puppeteer');
process.env.PUPPETEER_CACHE_DIR = cacheDir;

try {
  fs.mkdirSync(cacheDir, { recursive: true });
} catch (error) {
  console.warn('[postinstall] Failed to create Puppeteer cache dir:', cacheDir, error?.message || error);
}

console.log(`[postinstall] Installing Chrome for Puppeteer into: ${cacheDir}`);

const isWin = process.platform === 'win32';
const command = isWin ? 'npx.cmd' : 'npx';
const result = spawnSync(command, ['puppeteer', 'browsers', 'install', 'chrome'], {
  stdio: 'inherit',
  env: {
    ...process.env,
    PUPPETEER_CACHE_DIR: cacheDir,
  },
});

if (result.status !== 0) {
  console.error('[postinstall] Puppeteer Chrome install failed.');
  process.exit(result.status || 1);
}

console.log('[postinstall] Puppeteer Chrome install completed.');
