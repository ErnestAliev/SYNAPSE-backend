function createAiProvider(deps) {
  const {
    OPENAI_API_KEY,
    OPENAI_MODEL,
    toTrimmedString,
  } = deps;
  const REQUEST_TIMEOUT_MS = 45_000;
  const DEFAULT_TEMPERATURE = 0.25;
  const DEFAULT_MAX_OUTPUT_TOKENS = 900;
  const MIN_MAX_OUTPUT_TOKENS = 16;

  function modelSupportsTemperature(modelName) {
    const normalized = toTrimmedString(modelName, 120).toLowerCase();
    if (!normalized) return true;

    // GPT-5 family ignores or rejects temperature in Responses API.
    if (normalized.startsWith('gpt-5')) {
      return false;
    }

    return true;
  }

  function buildResponsesRequestBody({
    model,
    systemPrompt,
    userPrompt,
    temperature,
    maxOutputTokens,
  }) {
    const body = {
      model,
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
      max_output_tokens: maxOutputTokens,
    };

    if (typeof temperature === 'number' && Number.isFinite(temperature)) {
      body.temperature = temperature;
    }

    return body;
  }

  async function callResponsesApi({ requestBody, signal }) {
    const response = await fetch('https://api.openai.com/v1/responses', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${OPENAI_API_KEY}`,
      },
      body: JSON.stringify(requestBody),
      signal,
    });

    const payload = await response.json();
    return { response, payload };
  }

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

  async function requestOpenAiAgentReply({
    systemPrompt,
    userPrompt,
    includeRawPayload = false,
    model = '',
    temperature = DEFAULT_TEMPERATURE,
    maxOutputTokens = DEFAULT_MAX_OUTPUT_TOKENS,
  }) {
    if (!OPENAI_API_KEY) {
      throw Object.assign(new Error('OPENAI_API_KEY is not configured'), { status: 503 });
    }

    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
    const startedAt = Date.now();
    const resolvedModel = toTrimmedString(model, 120) || OPENAI_MODEL;
    const numericTemperature = Number.isFinite(Number(temperature)) ? Number(temperature) : DEFAULT_TEMPERATURE;
    const numericMaxOutputTokens = Number.isFinite(Number(maxOutputTokens))
      ? Math.max(MIN_MAX_OUTPUT_TOKENS, Math.floor(Number(maxOutputTokens)))
      : DEFAULT_MAX_OUTPUT_TOKENS;
    const shouldUseTemperature = modelSupportsTemperature(resolvedModel);
    const requestConfig = {
      model: resolvedModel,
      temperature: shouldUseTemperature ? numericTemperature : null,
      max_output_tokens: numericMaxOutputTokens,
      timeout_ms: REQUEST_TIMEOUT_MS,
    };

    let response;
    let payload;
    try {
      const firstRequestBody = buildResponsesRequestBody({
        model: requestConfig.model,
        systemPrompt,
        userPrompt,
        temperature: requestConfig.temperature,
        maxOutputTokens: requestConfig.max_output_tokens,
      });
      const firstAttempt = await callResponsesApi({
        requestBody: firstRequestBody,
        signal: controller.signal,
      });
      response = firstAttempt.response;
      payload = firstAttempt.payload;

      const providerMessage = toTrimmedString(payload?.error?.message, 300);
      const canRetryWithoutTemperature =
        response &&
        !response.ok &&
        typeof requestConfig.temperature === 'number' &&
        providerMessage.toLowerCase().includes('unsupported parameter') &&
        providerMessage.toLowerCase().includes('temperature');

      if (canRetryWithoutTemperature) {
        requestConfig.temperature = null;
        const retryRequestBody = buildResponsesRequestBody({
          model: requestConfig.model,
          systemPrompt,
          userPrompt,
          temperature: null,
          maxOutputTokens: requestConfig.max_output_tokens,
        });
        const retryAttempt = await callResponsesApi({
          requestBody: retryRequestBody,
          signal: controller.signal,
        });
        response = retryAttempt.response;
        payload = retryAttempt.payload;
      }

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
