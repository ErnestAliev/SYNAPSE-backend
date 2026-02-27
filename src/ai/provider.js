function createAiProvider(deps) {
  const {
    OPENAI_API_KEY,
    OPENAI_MODEL,
    toTrimmedString,
  } = deps;
  const REQUEST_TIMEOUT_MS = 45_000;
  const DEFAULT_TEMPERATURE = 0.25;
  const DEFAULT_MAX_OUTPUT_TOKENS = 900;

  function extractOpenAiResponseText(payload) {
    if (payload && typeof payload.output_text === 'string' && payload.output_text.trim()) {
      return payload.output_text.trim();
    }

    const chunks = [];
    const outputs = Array.isArray(payload?.output) ? payload.output : [];
    for (const item of outputs) {
      const contentItems = Array.isArray(item?.content) ? item.content : [];
      for (const content of contentItems) {
        if (typeof content?.text === 'string' && content.text.trim()) {
          chunks.push(content.text.trim());
        }
      }
    }

    if (!chunks.length) return '';
    return chunks.join('\n').trim();
  }

  async function requestOpenAiAgentReply({ systemPrompt, userPrompt, includeRawPayload = false }) {
    if (!OPENAI_API_KEY) {
      throw Object.assign(new Error('OPENAI_API_KEY is not configured'), { status: 503 });
    }

    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
    const startedAt = Date.now();
    const requestConfig = {
      model: OPENAI_MODEL,
      temperature: DEFAULT_TEMPERATURE,
      max_output_tokens: DEFAULT_MAX_OUTPUT_TOKENS,
      timeout_ms: REQUEST_TIMEOUT_MS,
    };

    let response;
    let payload;
    try {
      response = await fetch('https://api.openai.com/v1/responses', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${OPENAI_API_KEY}`,
        },
        body: JSON.stringify({
          model: requestConfig.model,
          input: [
            {
              role: 'system',
              content: [{ type: 'input_text', text: systemPrompt }],
            },
            {
              role: 'user',
              content: [{ type: 'input_text', text: userPrompt }],
            },
          ],
          temperature: requestConfig.temperature,
          max_output_tokens: requestConfig.max_output_tokens,
        }),
        signal: controller.signal,
      });

      payload = await response.json();
    } catch (error) {
      if (error && error.name === 'AbortError') {
        throw Object.assign(new Error('AI request timeout'), { status: 504 });
      }
      throw Object.assign(new Error('Failed to call AI provider'), { status: 502 });
    } finally {
      clearTimeout(timeout);
    }

    if (!response.ok) {
      const providerMessage = toTrimmedString(payload?.error?.message, 300) || 'AI provider error';
      throw Object.assign(new Error(providerMessage), { status: 502 });
    }

    const reply = extractOpenAiResponseText(payload);
    if (!reply) {
      throw Object.assign(new Error('AI response is empty'), { status: 502 });
    }

    return {
      reply,
      usage: payload?.usage || null,
      debug: {
        request: requestConfig,
        response: {
          status: response.status,
          ok: response.ok,
          id: toTrimmedString(payload?.id, 120),
          created: toTrimmedString(payload?.created, 120),
          model: toTrimmedString(payload?.model, 120) || requestConfig.model,
          output_text_length: reply.length,
          completed_in_ms: Math.max(1, Date.now() - startedAt),
        },
        ...(includeRawPayload ? { raw_payload: payload } : {}),
      },
    };
  }

  return {
    extractOpenAiResponseText,
    requestOpenAiAgentReply,
  };
}

module.exports = {
  createAiProvider,
};
