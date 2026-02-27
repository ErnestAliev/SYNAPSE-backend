function createAiProvider(deps) {
  const {
    OPENAI_API_KEY,
    OPENAI_MODEL,
    toTrimmedString,
  } = deps;

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

  async function requestOpenAiAgentReply({ systemPrompt, userPrompt }) {
    if (!OPENAI_API_KEY) {
      throw Object.assign(new Error('OPENAI_API_KEY is not configured'), { status: 503 });
    }

    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 45_000);

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
          model: OPENAI_MODEL,
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
          temperature: 0.25,
          max_output_tokens: 900,
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
