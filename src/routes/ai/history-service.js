function createHistoryService({
  toTrimmedString,
  toProfile,
  AGENT_CHAT_HISTORY_MESSAGE_LIMIT,
  AGENT_CHAT_HISTORY_ATTACHMENT_LIMIT,
  AGENT_CHAT_HISTORY_ATTACHMENT_DATA_MAX_LENGTH,
  AGENT_CHAT_HISTORY_TEXT_MAX_LENGTH,
}) {
  function normalizeHistoryAttachment(rawAttachment, index) {
    const attachment = toProfile(rawAttachment);
    const data = toTrimmedString(attachment.data, AGENT_CHAT_HISTORY_ATTACHMENT_DATA_MAX_LENGTH);
    const name = toTrimmedString(attachment.name, 240) || 'Файл';
    if (!data && !name) return null;

    return {
      id: toTrimmedString(attachment.id, 120) || `att_${Date.now()}_${index}`,
      name,
      mime: toTrimmedString(attachment.mime, 180),
      size: Number.isFinite(Number(attachment.size)) ? Math.max(0, Math.floor(Number(attachment.size))) : 0,
      data,
    };
  }

  function normalizeHistoryMessage(rawMessage, index) {
    const message = toProfile(rawMessage);
    const id = toTrimmedString(message.id, 120) || `msg_${Date.now()}_${index}`;
    const role = toTrimmedString(message.role, 24) === 'assistant' ? 'assistant' : 'user';
    const text = toTrimmedString(message.text, AGENT_CHAT_HISTORY_TEXT_MAX_LENGTH);
    const createdAtRaw = toTrimmedString(message.createdAt, 80);
    const parsedCreatedAt = Date.parse(createdAtRaw);
    const createdAt = Number.isFinite(parsedCreatedAt) ? new Date(parsedCreatedAt) : new Date();
    const attachments = (Array.isArray(message.attachments) ? message.attachments : [])
      .slice(0, AGENT_CHAT_HISTORY_ATTACHMENT_LIMIT)
      .map((item, attachmentIndex) => normalizeHistoryAttachment(item, attachmentIndex))
      .filter(Boolean);

    if (!text && attachments.length === 0) {
      return null;
    }

    return {
      id,
      role,
      text,
      createdAt,
      attachments,
    };
  }

  function normalizeMessages(rawMessages) {
    if (!Array.isArray(rawMessages)) return [];

    const dedup = new Set();
    const normalized = rawMessages
      .map((message, index) => normalizeHistoryMessage(message, index))
      .filter(Boolean)
      .filter((message) => {
        if (dedup.has(message.id)) return false;
        dedup.add(message.id);
        return true;
      })
      .sort((left, right) => left.createdAt.getTime() - right.createdAt.getTime());

    return normalized.slice(-AGENT_CHAT_HISTORY_MESSAGE_LIMIT);
  }

  function mapHistoryDocMessages(doc) {
    if (!doc || typeof doc !== 'object') return [];
    return Array.isArray(doc.messages) ? doc.messages : [];
  }

  function mapNormalizedMessagesToAgentHistory(messages) {
    return (Array.isArray(messages) ? messages : [])
      .map((message) => ({
        role: message?.role === 'assistant' ? 'assistant' : 'user',
        text: toTrimmedString(message?.text, 1800),
      }))
      .filter((message) => message.text);
  }

  function dedupeHistoryTailByCurrentMessage(history, currentMessage) {
    const normalizedMessage = toTrimmedString(currentMessage, 1800);
    const safeHistory = Array.isArray(history) ? history : [];
    if (!normalizedMessage || !safeHistory.length) {
      return {
        history: safeHistory,
        droppedCount: 0,
      };
    }

    const nextHistory = safeHistory.slice();
    let droppedCount = 0;
    while (nextHistory.length > 0) {
      const last = nextHistory[nextHistory.length - 1];
      if (last?.role !== 'user') break;
      if (toTrimmedString(last?.text, 1800) !== normalizedMessage) break;
      nextHistory.pop();
      droppedCount += 1;
    }

    return {
      history: nextHistory,
      droppedCount,
    };
  }

  function mapHistoryMessagesToResponse(messages) {
    return (Array.isArray(messages) ? messages : []).map((message) => ({
      id: toTrimmedString(message.id, 120),
      role: toTrimmedString(message.role, 24) === 'assistant' ? 'assistant' : 'user',
      text: toTrimmedString(message.text, AGENT_CHAT_HISTORY_TEXT_MAX_LENGTH),
      createdAt: (() => {
        const raw = message.createdAt;
        if (raw instanceof Date) return raw.toISOString();
        const asString = toTrimmedString(raw, 80);
        const parsed = Date.parse(asString);
        if (Number.isFinite(parsed)) return new Date(parsed).toISOString();
        return new Date().toISOString();
      })(),
      attachments: (Array.isArray(message.attachments) ? message.attachments : [])
        .slice(0, AGENT_CHAT_HISTORY_ATTACHMENT_LIMIT)
        .map((item) => ({
          id: toTrimmedString(item.id, 120) || `att_${Date.now()}`,
          name: toTrimmedString(item.name, 240) || 'Файл',
          mime: toTrimmedString(item.mime, 180),
          size: Number.isFinite(Number(item.size)) ? Math.max(0, Math.floor(Number(item.size))) : 0,
          data: toTrimmedString(item.data, AGENT_CHAT_HISTORY_ATTACHMENT_DATA_MAX_LENGTH),
        }))
        .filter((item) => item.data || item.name),
    }));
  }

  async function loadStoredAgentHistory({
    AgentChatHistory,
    ownerId,
    scopeKeys,
  }) {
    const docs = await AgentChatHistory.find({
      owner_id: ownerId,
      scope_key: { $in: scopeKeys },
    })
      .select({ messages: 1, updatedAt: 1 })
      .sort({ updatedAt: -1, _id: -1 })
      .lean();
    const mergedMessages = normalizeMessages(docs.flatMap((doc) => mapHistoryDocMessages(doc)));
    return mapNormalizedMessagesToAgentHistory(mergedMessages);
  }

  return {
    normalizeMessages,
    mapHistoryDocMessages,
    mapNormalizedMessagesToAgentHistory,
    dedupeHistoryTailByCurrentMessage,
    mapHistoryMessagesToResponse,
    loadStoredAgentHistory,
  };
}

module.exports = {
  createHistoryService,
};
