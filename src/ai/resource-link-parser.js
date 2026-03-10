const RESOURCE_LINK_FETCH_TIMEOUT_MS = 12_000;
const RESOURCE_LINK_HTML_MAX_LENGTH = 400_000;
const RESOURCE_LINK_TEXT_MAX_LENGTH = 2_400;
const RESOURCE_LINK_LINE_MAX_LENGTH = 260;
const RESOURCE_LINK_MAX_LINES = 8;

const SOCIAL_HOST_LABELS = Object.freeze([
  { label: 'Instagram', hosts: ['instagram.com'] },
  { label: 'Facebook', hosts: ['facebook.com', 'fb.com'] },
  { label: 'LinkedIn', hosts: ['linkedin.com'] },
  { label: 'Telegram', hosts: ['t.me', 'telegram.me', 'telegram.org'] },
  { label: 'WhatsApp', hosts: ['wa.me', 'whatsapp.com', 'chat.whatsapp.com'] },
  { label: 'YouTube', hosts: ['youtube.com', 'youtu.be'] },
  { label: 'TikTok', hosts: ['tiktok.com'] },
  { label: 'X', hosts: ['x.com', 'twitter.com'] },
  { label: 'VK', hosts: ['vk.com', 'vkontakte.ru'] },
  { label: 'GitHub', hosts: ['github.com'] },
]);

const TEXT_NOISE_PATTERNS = [
  /cookie/i,
  /accept all/i,
  /privacy/i,
  /sign in/i,
  /log in/i,
  /зарегистр/i,
  /войти/i,
  /cookies/i,
  /подписывайтесь/i,
];

function normalizeResourceLinkUrl(rawValue) {
  const raw = String(rawValue || '').trim();
  if (!raw) return '';

  const withProtocol = /^https?:\/\//i.test(raw) ? raw : `https://${raw}`;
  try {
    const url = new URL(withProtocol);
    if (!url.hostname || !url.protocol.startsWith('http')) return '';

    const host = url.hostname.toLowerCase();
    if (!host.includes('.') || host === 'localhost') return '';
    if (/^\d{1,3}(?:\.\d{1,3}){3}$/.test(host)) return '';
    if (host.includes(':')) return '';

    url.hash = '';
    return url.toString().slice(0, 2048);
  } catch {
    return '';
  }
}

function decodeHtmlEntities(value) {
  return String(value || '')
    .replace(/&nbsp;/gi, ' ')
    .replace(/&amp;/gi, '&')
    .replace(/&quot;/gi, '"')
    .replace(/&#39;|&apos;/gi, "'")
    .replace(/&lt;/gi, '<')
    .replace(/&gt;/gi, '>')
    .replace(/&#(\d+);/g, (_, code) => String.fromCharCode(Number(code) || 0))
    .replace(/&#x([0-9a-f]+);/gi, (_, code) => String.fromCharCode(parseInt(code, 16) || 0));
}

function collapseWhitespace(value) {
  return decodeHtmlEntities(value).replace(/\s+/g, ' ').trim();
}

function stripTags(value) {
  return collapseWhitespace(String(value || '').replace(/<[^>]+>/g, ' '));
}

function extractTagInnerHtml(html, tagName) {
  const match = String(html || '').match(new RegExp(`<${tagName}\\b[^>]*>([\\s\\S]*?)<\\/${tagName}>`, 'i'));
  return match ? match[1] : '';
}

function parseTagAttributes(tag) {
  const attributes = {};
  const pattern = /([^\s=/>]+)(?:\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s"'=<>`]+)))?/g;
  let match = pattern.exec(tag);
  while (match) {
    const key = String(match[1] || '').toLowerCase();
    const value = match[2] || match[3] || match[4] || '';
    if (key) {
      attributes[key] = decodeHtmlEntities(value);
    }
    match = pattern.exec(tag);
  }
  return attributes;
}

function extractMetaContent(html, keys) {
  const wanted = new Set((Array.isArray(keys) ? keys : []).map((item) => String(item || '').toLowerCase()));
  const pattern = /<meta\b[^>]*>/gi;
  let match = pattern.exec(String(html || ''));
  while (match) {
    const attributes = parseTagAttributes(match[0]);
    const key = String(
      attributes.property || attributes.name || attributes['http-equiv'] || attributes.itemprop || '',
    ).toLowerCase();
    const content = collapseWhitespace(attributes.content || '');
    if (content && wanted.has(key)) {
      return content;
    }
    match = pattern.exec(String(html || ''));
  }
  return '';
}

function extractJsonLdObjects(html) {
  const objects = [];
  const pattern = /<script\b[^>]*type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi;
  let match = pattern.exec(String(html || ''));
  while (match) {
    const raw = String(match[1] || '').trim();
    if (!raw) {
      match = pattern.exec(String(html || ''));
      continue;
    }

    try {
      const parsed = JSON.parse(raw);
      if (Array.isArray(parsed)) {
        objects.push(...parsed);
      } else if (parsed && typeof parsed === 'object') {
        objects.push(parsed);
      }
    } catch {
      // Ignore malformed JSON-LD blocks.
    }
    match = pattern.exec(String(html || ''));
  }
  return objects;
}

function pickJsonLdValue(objects, keys) {
  const wanted = Array.isArray(keys) ? keys : [];
  for (const object of objects) {
    if (!object || typeof object !== 'object') continue;
    for (const key of wanted) {
      const value = object[key];
      if (typeof value === 'string' && collapseWhitespace(value)) {
        return collapseWhitespace(value);
      }
      if (value && typeof value === 'object' && typeof value.name === 'string' && collapseWhitespace(value.name)) {
        return collapseWhitespace(value.name);
      }
    }
  }
  return '';
}

function pickSiteLabel(hostname) {
  const host = String(hostname || '').toLowerCase();
  for (const entry of SOCIAL_HOST_LABELS) {
    if (entry.hosts.some((candidate) => host === candidate || host.endsWith(`.${candidate}`))) {
      return entry.label;
    }
  }
  return host.replace(/^www\./, '');
}

function pickSourceKind(hostname) {
  const host = String(hostname || '').toLowerCase();
  return SOCIAL_HOST_LABELS.some((entry) => entry.hosts.some((candidate) => host === candidate || host.endsWith(`.${candidate}`)))
    ? 'Соцсеть или медиаплатформа'
    : 'Сайт';
}

function extractVisibleText(html) {
  const sourceHtml = extractTagInnerHtml(html, 'body') || String(html || '');
  const stripped = sourceHtml
    .replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi, ' ')
    .replace(/<style\b[^>]*>[\s\S]*?<\/style>/gi, ' ')
    .replace(/<noscript\b[^>]*>[\s\S]*?<\/noscript>/gi, ' ')
    .replace(/<svg\b[^>]*>[\s\S]*?<\/svg>/gi, ' ')
    .replace(/<template\b[^>]*>[\s\S]*?<\/template>/gi, ' ')
    .replace(/<(br|hr)\b[^>]*\/?>/gi, '\n')
    .replace(/<\/(p|div|section|article|header|footer|main|aside|li|ul|ol|h1|h2|h3|h4|h5|h6)>/gi, '\n')
    .replace(/<li\b[^>]*>/gi, '\n- ')
    .replace(/<[^>]+>/g, ' ');

  const unique = new Set();
  const lines = [];
  for (const rawLine of decodeHtmlEntities(stripped).split('\n')) {
    const line = rawLine.replace(/\s+/g, ' ').trim();
    if (!line) continue;
    if (line.length < 24) continue;
    if (line.length > RESOURCE_LINK_LINE_MAX_LENGTH) continue;
    if (TEXT_NOISE_PATTERNS.some((pattern) => pattern.test(line))) continue;

    const key = line.toLowerCase();
    if (unique.has(key)) continue;
    unique.add(key);
    lines.push(line);
    if (lines.length >= RESOURCE_LINK_MAX_LINES) break;
  }

  return lines.join('\n');
}

function trimTextBlock(value, maxLength = RESOURCE_LINK_TEXT_MAX_LENGTH) {
  const text = String(value || '').trim();
  if (!text) return '';
  if (text.length <= maxLength) return text;

  const sliced = text.slice(0, maxLength);
  const lastBoundary = Math.max(sliced.lastIndexOf('\n'), sliced.lastIndexOf('. '), sliced.lastIndexOf(' '));
  return `${sliced.slice(0, lastBoundary > 160 ? lastBoundary : maxLength).trim()}…`;
}

function buildPreparedTextBlock(parsed) {
  const parts = [];

  if (parsed.siteLabel) {
    parts.push(`Площадка: ${parsed.siteLabel}`);
  }
  if (parsed.sourceKind) {
    parts.push(`Тип источника: ${parsed.sourceKind}`);
  }
  if (parsed.title) {
    parts.push(`Название: ${parsed.title}`);
  }
  if (parsed.description) {
    parts.push(`Краткое описание: ${parsed.description}`);
  }
  if (parsed.textSnippet) {
    parts.push(`Извлечённый текст:\n${parsed.textSnippet}`);
  }

  return trimTextBlock(parts.join('\n\n'));
}

async function fetchPage(url) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), RESOURCE_LINK_FETCH_TIMEOUT_MS);

  try {
    const response = await fetch(url, {
      method: 'GET',
      redirect: 'follow',
      signal: controller.signal,
      headers: {
        'accept-language': 'ru,en;q=0.9',
        accept: 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'user-agent': 'Mozilla/5.0 (compatible; Synapse12ResourceParser/1.0; +https://synapse.local)',
      },
    });

    if (!response.ok) {
      throw new Error(`Источник недоступен: HTTP ${response.status}`);
    }

    const contentType = String(response.headers.get('content-type') || '').toLowerCase();
    if (contentType && !contentType.includes('text/html') && !contentType.includes('application/xhtml+xml')) {
      throw new Error('Парсер поддерживает только HTML-страницы');
    }

    const html = (await response.text()).slice(0, RESOURCE_LINK_HTML_MAX_LENGTH);
    return {
      html,
      finalUrl: normalizeResourceLinkUrl(response.url) || url,
    };
  } finally {
    clearTimeout(timeout);
  }
}

async function parseResourceLink(rawUrl) {
  const sourceUrl = normalizeResourceLinkUrl(rawUrl);
  if (!sourceUrl) {
    throw new Error('Некорректная ссылка');
  }

  const { html, finalUrl } = await fetchPage(sourceUrl);
  const jsonLdObjects = extractJsonLdObjects(html);
  const hostname = new URL(finalUrl).hostname.toLowerCase();

  const title = trimTextBlock(
    extractMetaContent(html, ['og:title', 'twitter:title']) ||
      collapseWhitespace(extractTagInnerHtml(html, 'title')) ||
      pickJsonLdValue(jsonLdObjects, ['headline', 'name']),
    180,
  );
  const description = trimTextBlock(
    extractMetaContent(html, ['og:description', 'twitter:description', 'description']) ||
      pickJsonLdValue(jsonLdObjects, ['description']),
    420,
  );
  const textSnippet = trimTextBlock(extractVisibleText(html), 1_400);

  const preparedText = buildPreparedTextBlock({
    siteLabel: pickSiteLabel(hostname),
    sourceKind: pickSourceKind(hostname),
    title,
    description,
    textSnippet,
  });

  if (!preparedText) {
    throw new Error('Парсер не нашёл полезный текст на странице');
  }

  return {
    sourceUrl,
    finalUrl,
    hostname,
    siteLabel: pickSiteLabel(hostname),
    sourceKind: pickSourceKind(hostname),
    title,
    description,
    textSnippet,
    preparedText,
  };
}

module.exports = {
  normalizeResourceLinkUrl,
  parseResourceLink,
};
