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

const GENERIC_TITLE_PATTERNS = [
  /^instagram$/i,
  /^facebook$/i,
  /^linkedin$/i,
  /^telegram$/i,
  /^youtube$/i,
  /^tiktok$/i,
  /^x$/i,
  /^twitter$/i,
  /^vk(?:ontakte)?$/i,
  /^github$/i,
];

const GENERIC_DESCRIPTION_PATTERNS = [
  /создайте аккаунт или войдите/i,
  /create an account or log in/i,
  /share (?:photos|videos|moments)/i,
  /людьми, которые вас понимают/i,
  /sign up .* instagram/i,
  /from breaking news and entertainment/i,
  /join facebook/i,
  /connect with friends/i,
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

function parseUrlPathSegments(urlString) {
  try {
    const url = new URL(urlString);
    return url.pathname
      .split('/')
      .map((part) => decodeURIComponent(part || '').trim())
      .filter(Boolean);
  } catch {
    return [];
  }
}

function isReservedSocialSegment(segment) {
  const value = String(segment || '').toLowerCase();
  return new Set([
    'p',
    'reel',
    'reels',
    'tv',
    'explore',
    'stories',
    'accounts',
    'about',
    'legal',
    'directory',
    'developer',
    'developers',
    'privacy',
    'terms',
    'login',
    'signup',
    'share',
    'watch',
    'shorts',
    'video',
    'videos',
    'status',
    'channel',
    'c',
    'user',
    'company',
    'in',
    's',
  ]).has(value);
}

function isGenericTitle(title, siteLabel) {
  const value = String(title || '').trim();
  if (!value) return true;
  if (GENERIC_TITLE_PATTERNS.some((pattern) => pattern.test(value))) return true;
  return siteLabel ? value.toLowerCase() === String(siteLabel).trim().toLowerCase() : false;
}

function isGenericDescription(description) {
  const value = String(description || '').trim();
  if (!value) return true;
  return GENERIC_DESCRIPTION_PATTERNS.some((pattern) => pattern.test(value));
}

function unwrapQuotedText(value) {
  const text = String(value || '').trim();
  if (!text) return '';
  return text.replace(/^["“”'«]+/, '').replace(/["“”'»]+$/, '').trim();
}

function extractInstagramProfileBio(metaDescription) {
  const text = String(metaDescription || '').trim();
  if (!text) return '';

  const patterns = [
    /instagram:\s*["“]?([\s\S]+?)["”]?$/i,
    /on instagram:\s*["“]?([\s\S]+?)["”]?$/i,
  ];

  for (const pattern of patterns) {
    const match = text.match(pattern);
    const bio = unwrapQuotedText(match?.[1] || '');
    if (bio) return trimTextBlock(bio, 600);
  }

  return '';
}

function extractInstagramProfileStats(value) {
  const text = String(value || '').trim();
  if (!text) return '';

  const match = text.match(/^(.*?)(?:\s+[–-]\s+|\s+-\s+)/);
  return trimTextBlock(match?.[1] || '', 220);
}

function extractUrlHints(finalUrl, hostname) {
  const host = String(hostname || '').toLowerCase();
  const segments = parseUrlPathSegments(finalUrl);
  const hints = [];
  let fallbackTitle = '';

  if (host.includes('instagram.com')) {
    const [first, second] = segments;
    if (first && !isReservedSocialSegment(first)) {
      fallbackTitle = `Instagram @${first}`;
      hints.push(`Аккаунт: @${first}`);
    } else if (first === 'p' && second) {
      fallbackTitle = `Instagram пост ${second}`;
      hints.push(`Тип ссылки: пост Instagram`);
      hints.push(`Идентификатор поста: ${second}`);
    } else if ((first === 'reel' || first === 'reels' || first === 'tv') && second) {
      fallbackTitle = `Instagram ${first} ${second}`;
      hints.push(`Тип ссылки: ${first === 'tv' ? 'видео Instagram TV' : 'reel Instagram'}`);
      hints.push(`Идентификатор: ${second}`);
    }
  } else if (host.includes('tiktok.com')) {
    const [first, second, third] = segments;
    if (first && first.startsWith('@')) {
      fallbackTitle = `TikTok ${first}`;
      hints.push(`Аккаунт: ${first}`);
      if (second === 'video' && third) {
        hints.push(`Тип ссылки: TikTok видео`);
        hints.push(`Идентификатор видео: ${third}`);
      }
    }
  } else if (host === 't.me' || host.includes('telegram.')) {
    const [first, second] = segments;
    if (first === 's' && second) {
      fallbackTitle = `Telegram ${second}`;
      hints.push(`Канал или профиль: @${second}`);
    } else if (first) {
      fallbackTitle = `Telegram ${first}`;
      hints.push(`Канал или профиль: @${first}`);
    }
  } else if (host.includes('youtube.com') || host.includes('youtu.be')) {
    const url = new URL(finalUrl);
    const [first, second] = segments;
    if (host.includes('youtu.be') && first) {
      fallbackTitle = `YouTube видео ${first}`;
      hints.push(`Тип ссылки: YouTube видео`);
      hints.push(`Идентификатор видео: ${first}`);
    } else if (first === '@' || (first && first.startsWith('@'))) {
      const handle = first === '@' ? second : first;
      if (handle) {
        fallbackTitle = `YouTube ${handle}`;
        hints.push(`Канал: ${handle.startsWith('@') ? handle : `@${handle}`}`);
      }
    } else if (first === 'watch' && url.searchParams.get('v')) {
      fallbackTitle = `YouTube видео ${url.searchParams.get('v')}`;
      hints.push(`Тип ссылки: YouTube видео`);
      hints.push(`Идентификатор видео: ${url.searchParams.get('v')}`);
    } else if ((first === 'shorts' || first === 'channel' || first === 'c' || first === 'user') && second) {
      fallbackTitle = `YouTube ${second}`;
      hints.push(`Тип ссылки: ${first === 'shorts' ? 'YouTube Shorts' : 'YouTube канал'}`);
      hints.push(`Идентификатор: ${second}`);
    }
  } else if (host.includes('x.com') || host.includes('twitter.com')) {
    const [first, second, third] = segments;
    if (first && !isReservedSocialSegment(first)) {
      fallbackTitle = `X @${first}`;
      hints.push(`Аккаунт: @${first}`);
      if (second === 'status' && third) {
        hints.push(`Тип ссылки: пост X`);
        hints.push(`Идентификатор поста: ${third}`);
      }
    }
  } else if (host.includes('linkedin.com')) {
    const [first, second] = segments;
    if ((first === 'in' || first === 'company') && second) {
      fallbackTitle = `LinkedIn ${second}`;
      hints.push(`Тип ссылки: ${first === 'company' ? 'страница компании' : 'профиль'}`);
      hints.push(`Slug: ${second}`);
    }
  } else if (host.includes('github.com')) {
    const [first, second] = segments;
    if (first && second) {
      fallbackTitle = `GitHub ${first}/${second}`;
      hints.push(`Тип ссылки: репозиторий`);
      hints.push(`Репозиторий: ${first}/${second}`);
    } else if (first) {
      fallbackTitle = `GitHub ${first}`;
      hints.push(`Аккаунт: ${first}`);
    }
  } else if (host.includes('vk.com') || host.includes('vkontakte.ru')) {
    const [first] = segments;
    if (first) {
      fallbackTitle = `VK ${first}`;
      hints.push(`Идентификатор или slug: ${first}`);
    }
  } else if (host === 'wa.me' || host.includes('whatsapp.com')) {
    const [first] = segments;
    if (first) {
      fallbackTitle = `WhatsApp ${first}`;
      hints.push(`Номер или код ссылки: ${first}`);
    }
  } else if (segments[0]) {
    fallbackTitle = segments[0];
    hints.push(`Путь ссылки: /${segments.join('/')}`);
  }

  return {
    hints,
    fallbackTitle,
  };
}

function hasUsefulParsedPayload(parsed) {
  return Boolean(
    (parsed.title && parsed.title.trim()) ||
      (parsed.profileBio && parsed.profileBio.trim()) ||
      (parsed.profileStats && parsed.profileStats.trim()) ||
      (parsed.description && parsed.description.trim()) ||
      (parsed.textSnippet && parsed.textSnippet.trim()) ||
      (Array.isArray(parsed.urlHints) && parsed.urlHints.length) ||
      (parsed.accessNote && parsed.accessNote.trim()),
  );
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
  if (parsed.profileBio) {
    parts.push(`Описание профиля: ${parsed.profileBio}`);
  }
  if (parsed.profileStats) {
    parts.push(`Показатели профиля: ${parsed.profileStats}`);
  }
  if (parsed.description && parsed.description !== parsed.profileBio) {
    parts.push(`Краткое описание: ${parsed.description}`);
  }
  if (Array.isArray(parsed.urlHints) && parsed.urlHints.length) {
    parts.push(`Что удалось понять из ссылки:\n${parsed.urlHints.join('\n')}`);
  }
  if (parsed.textSnippet) {
    parts.push(`Извлечённый текст:\n${parsed.textSnippet}`);
  }
  if (parsed.accessNote) {
    parts.push(`Ограничение: ${parsed.accessNote}`);
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
  const { hints: urlHints, fallbackTitle } = extractUrlHints(finalUrl, hostname);
  const siteLabel = pickSiteLabel(hostname);
  const sourceKind = pickSourceKind(hostname);
  const metaDescription = trimTextBlock(extractMetaContent(html, ['description']), 800);
  const socialDescription = trimTextBlock(extractMetaContent(html, ['og:description', 'twitter:description']), 420);
  const profileBio = hostname.includes('instagram.com') ? extractInstagramProfileBio(metaDescription) : '';
  const profileStats = hostname.includes('instagram.com')
    ? extractInstagramProfileStats(socialDescription || metaDescription)
    : '';

  const rawTitle = trimTextBlock(
    extractMetaContent(html, ['og:title', 'twitter:title']) ||
      collapseWhitespace(extractTagInnerHtml(html, 'title')) ||
      pickJsonLdValue(jsonLdObjects, ['headline', 'name']),
    180,
  );
  const rawDescription = trimTextBlock(
    (hostname.includes('instagram.com')
      ? metaDescription || socialDescription
      : socialDescription || metaDescription) ||
      pickJsonLdValue(jsonLdObjects, ['description']),
    800,
  );
  const textSnippet = trimTextBlock(extractVisibleText(html), 1_400);
  const title = isGenericTitle(rawTitle, siteLabel) ? trimTextBlock(fallbackTitle, 180) : rawTitle;
  const description = profileBio || (isGenericDescription(rawDescription) ? '' : rawDescription);
  const hasRealPageText = Boolean(textSnippet);
  const accessNote =
    !hasRealPageText && urlHints.length
      ? 'Площадка не отдала содержимое страницы без авторизации или клиентского рендера. Ниже сохранены только признаки, которые удалось извлечь из самой ссылки.'
      : '';

  const preparedText = buildPreparedTextBlock({
    siteLabel,
    sourceKind,
    title,
    description,
    profileBio,
    profileStats,
    textSnippet,
    urlHints,
    accessNote,
  });

  if (!preparedText || !hasUsefulParsedPayload({
    title,
    profileBio,
    profileStats,
    description,
    textSnippet,
    urlHints,
    accessNote,
  })) {
    throw new Error('Парсер не нашёл полезный текст на странице');
  }

  return {
    sourceUrl,
    finalUrl,
    hostname,
    siteLabel,
    sourceKind,
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
