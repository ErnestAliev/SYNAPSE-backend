function createAiAttachmentTools(deps) {
  const {
    toProfile,
    toTrimmedString,
    compactObject,
    AI_HISTORY_MESSAGE_LIMIT,
    AI_ATTACHMENT_LIMIT,
    AI_ATTACHMENT_TEXT_MAX_LENGTH,
    AI_ATTACHMENT_DATA_URL_MAX_LENGTH,
    AI_ATTACHMENT_BINARY_MAX_BYTES,
    mammoth,
  } = deps;

  function normalizeAgentHistory(rawHistory) {
    if (!Array.isArray(rawHistory)) return [];
    return rawHistory
      .map((item) => {
        const row = toProfile(item);
        const role = row.role === 'assistant' ? 'assistant' : row.role === 'user' ? 'user' : '';
        const text = toTrimmedString(row.text, 1800);
        if (!role || !text) return null;
        return { role, text };
      })
      .filter(Boolean)
      .slice(-AI_HISTORY_MESSAGE_LIMIT);
  }

  function normalizeAgentAttachments(rawAttachments) {
    if (!Array.isArray(rawAttachments)) return [];
    return rawAttachments
      .map((item) => {
        const attachment = toProfile(item);
        const name = toTrimmedString(attachment.name, 120);
        if (!name) return null;
        const mime = toTrimmedString(attachment.mime, 120);
        const size =
          typeof attachment.size === 'number' && Number.isFinite(attachment.size)
            ? Math.max(0, Math.floor(attachment.size))
            : 0;
        const data = toTrimmedString(attachment.data, AI_ATTACHMENT_DATA_URL_MAX_LENGTH);
        const text = toTrimmedString(attachment.text, AI_ATTACHMENT_TEXT_MAX_LENGTH);
        return compactObject({ name, mime, size, data, text });
      })
      .filter(Boolean)
      .slice(0, AI_ATTACHMENT_LIMIT);
  }

  function parseDataUrl(value) {
    const raw = toTrimmedString(value, AI_ATTACHMENT_DATA_URL_MAX_LENGTH);
    if (!raw.startsWith('data:')) return null;

    const commaIndex = raw.indexOf(',');
    if (commaIndex <= 5) return null;

    const meta = raw.slice(5, commaIndex);
    const payload = raw.slice(commaIndex + 1);
    const metaParts = meta.split(';').map((part) => part.trim()).filter(Boolean);
    const mime = toTrimmedString(metaParts[0] || '', 160).toLowerCase();
    const isBase64 = metaParts.includes('base64');
    if (!payload) return null;

    return { mime, isBase64, payload };
  }

  function shouldTreatAsTextAttachment(name, mime) {
    const loweredMime = toTrimmedString(mime, 120).toLowerCase();
    const loweredName = toTrimmedString(name, 160).toLowerCase();
    if (loweredMime.startsWith('text/')) return true;
    if (loweredMime === 'application/json') return true;
    if (loweredMime === 'application/xml') return true;
    if (loweredMime === 'application/x-yaml') return true;
    return (
      loweredName.endsWith('.txt') ||
      loweredName.endsWith('.md') ||
      loweredName.endsWith('.json') ||
      loweredName.endsWith('.csv') ||
      loweredName.endsWith('.yaml') ||
      loweredName.endsWith('.yml') ||
      loweredName.endsWith('.xml') ||
      loweredName.endsWith('.log')
    );
  }

  function isDocxAttachment(name, mime) {
    const loweredMime = toTrimmedString(mime, 120).toLowerCase();
    const loweredName = toTrimmedString(name, 160).toLowerCase();
    return (
      loweredMime === 'application/vnd.openxmlformats-officedocument.wordprocessingml.document' ||
      loweredName.endsWith('.docx')
    );
  }

  function decodeAttachmentBuffer(attachment) {
    const parsed = parseDataUrl(attachment?.data);
    if (!parsed) return null;

    let buffer = null;
    try {
      buffer = parsed.isBase64
        ? Buffer.from(parsed.payload, 'base64')
        : Buffer.from(decodeURIComponent(parsed.payload), 'utf8');
    } catch {
      return null;
    }

    if (!buffer || !buffer.length) return null;
    if (buffer.length > AI_ATTACHMENT_BINARY_MAX_BYTES) return null;

    return {
      buffer,
      mime: parsed.mime || toTrimmedString(attachment?.mime, 120).toLowerCase(),
    };
  }

  async function extractAttachmentText(attachment) {
    const directText = toTrimmedString(attachment?.text, AI_ATTACHMENT_TEXT_MAX_LENGTH);
    if (directText) {
      return directText;
    }

    const decoded = decodeAttachmentBuffer(attachment);
    if (!decoded) return '';

    const name = toTrimmedString(attachment?.name, 120);
    const mime = toTrimmedString(attachment?.mime, 120).toLowerCase() || decoded.mime;

    if (shouldTreatAsTextAttachment(name, mime)) {
      return toTrimmedString(decoded.buffer.toString('utf8'), AI_ATTACHMENT_TEXT_MAX_LENGTH);
    }

    if (isDocxAttachment(name, mime) && mammoth) {
      try {
        const parsed = await mammoth.extractRawText({ buffer: decoded.buffer });
        return toTrimmedString(parsed?.value, AI_ATTACHMENT_TEXT_MAX_LENGTH);
      } catch {
        return '';
      }
    }

    return '';
  }

  function detectContentCategory(name, mime, extractedText) {
    const loweredMime = toTrimmedString(mime, 120).toLowerCase();
    const loweredName = toTrimmedString(name, 160).toLowerCase();

    // Structured data formats
    if (
      loweredMime === 'application/json' ||
      loweredName.endsWith('.json')
    ) return 'structured';

    if (
      loweredMime === 'application/xml' ||
      loweredMime === 'text/xml' ||
      loweredName.endsWith('.xml')
    ) return 'structured';

    if (
      loweredMime === 'application/x-yaml' ||
      loweredName.endsWith('.yaml') ||
      loweredName.endsWith('.yml')
    ) return 'structured';

    // Explicit spreadsheet/table MIME types
    if (
      loweredMime === 'text/csv' ||
      loweredMime === 'application/csv' ||
      loweredMime === 'application/vnd.ms-excel' ||
      loweredMime.includes('spreadsheet') ||
      loweredMime.includes('opendocument.spreadsheet') ||
      loweredName.endsWith('.csv') ||
      loweredName.endsWith('.tsv') ||
      loweredName.endsWith('.xls') ||
      loweredName.endsWith('.xlsx') ||
      loweredName.endsWith('.ods')
    ) return 'table';

    // Heuristic: detect CSV/TSV-like content in extracted text
    if (extractedText) {
      const lines = extractedText.split('\n').filter((l) => l.trim()).slice(0, 15);
      if (lines.length >= 3) {
        const commaRich = lines.filter((l) => (l.match(/,/g) || []).length >= 3).length;
        const tabRich = lines.filter((l) => (l.match(/\t/g) || []).length >= 2).length;
        const numberRich = lines.filter((l) => (l.match(/\b\d+(\.\d+)?\b/g) || []).length >= 3).length;
        if ((commaRich >= 3 || tabRich >= 3) && numberRich >= 3) return 'table';
      }
    }

    // Word documents
    if (isDocxAttachment(name, mime)) return 'document';

    // Log files and plain text
    if (loweredName.endsWith('.log')) return 'text';

    // Default: treat as text
    return 'text';
  }

  async function prepareAgentAttachments(rawAttachments) {
    const normalized = normalizeAgentAttachments(rawAttachments);
    const prepared = [];

    for (const attachment of normalized) {
      const text = await extractAttachmentText(attachment);
      const contentCategory = detectContentCategory(attachment.name, attachment.mime, text);
      prepared.push(
        compactObject({
          name: attachment.name,
          mime: attachment.mime,
          size: attachment.size,
          text,
          hasInlineData: Boolean(attachment.data),
          contentCategory,
        }),
      );
    }

    return prepared;
  }

  return {
    normalizeAgentHistory,
    normalizeAgentAttachments,
    prepareAgentAttachments,
  };
}

module.exports = {
  createAiAttachmentTools,
};
