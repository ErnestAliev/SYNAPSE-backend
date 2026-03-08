const express = require('express');

function createTranscribeRouter(deps) {
  const {
    requireAuth,
    toTrimmedString,
    aiProvider,
    OPENAI_TRANSCRIBE_MODEL,
    OPENAI_TRANSCRIBE_MAX_AUDIO_BYTES,
  } = deps;

  const router = express.Router();
  const maxAudioBytes = Math.max(512_000, Number(OPENAI_TRANSCRIBE_MAX_AUDIO_BYTES) || 25 * 1024 * 1024);
  const parserLimitMb = Math.max(1, Math.ceil(maxAudioBytes / (1024 * 1024)));

  function sanitizeFileName(inputFileName, mimeType) {
    const raw = toTrimmedString(inputFileName, 140) || 'voice-input';
    const safe = raw.replace(/[^\w.\-]+/g, '_');
    if (safe.includes('.')) return safe;
    if (mimeType.includes('ogg')) return `${safe}.ogg`;
    if (mimeType.includes('mp4') || mimeType.includes('m4a')) return `${safe}.m4a`;
    if (mimeType.includes('mpeg')) return `${safe}.mp3`;
    if (mimeType.includes('wav')) return `${safe}.wav`;
    return `${safe}.webm`;
  }

  function extractMimeType(rawHeader) {
    const normalized = toTrimmedString(rawHeader, 120).toLowerCase();
    const value = normalized.split(';')[0].trim();
    if (!value) return '';
    if (value.startsWith('audio/')) return value;
    if (value === 'application/octet-stream') return value;
    return '';
  }

  router.post(
    '/file',
    requireAuth,
    express.raw({
      type: ['audio/*', 'application/octet-stream'],
      limit: `${parserLimitMb}mb`,
    }),
    async (req, res, next) => {
      try {
        const audioBuffer = Buffer.isBuffer(req.body) ? req.body : Buffer.alloc(0);
        if (!audioBuffer.length) {
          return res.status(400).json({ message: 'Audio payload is required' });
        }
        if (audioBuffer.length > maxAudioBytes) {
          return res.status(413).json({ message: `Audio file is too large. Max ${maxAudioBytes} bytes.` });
        }

        const mimeType = extractMimeType(req.headers['content-type']) || 'audio/webm';
        const language = toTrimmedString(req.headers['x-audio-language'], 16) || 'ru';
        const fileName = sanitizeFileName(req.headers['x-audio-filename'], mimeType);
        const model = toTrimmedString(OPENAI_TRANSCRIBE_MODEL, 120) || 'gpt-4o-transcribe';

        const transcription = await aiProvider.requestOpenAiAudioTranscription({
          audioBuffer,
          mimeType,
          fileName,
          model,
          language,
          timeoutMs: 120_000,
        });

        return res.status(200).json({
          text: transcription.text,
          model: transcription.model,
        });
      } catch (error) {
        console.error('[transcribe:file] failed', {
          message: error instanceof Error ? error.message : String(error),
          status: typeof error?.status === 'number' ? error.status : null,
          contentType: req.headers['content-type'] || '',
          bytes: Buffer.isBuffer(req.body) ? req.body.length : 0,
        });
        return next(error);
      }
    },
  );

  return router;
}

module.exports = {
  createTranscribeRouter,
};
