function createAiProvider(deps) {
  const {
    OPENAI_API_KEY,
    OPENAI_MODEL,
    OPENAI_REQUEST_TIMEOUT_MS,
    toTrimmedString,
  } = deps;
  const REQUEST_TIMEOUT_MS = Number.isFinite(Number(OPENAI_REQUEST_TIMEOUT_MS))
    ? Math.max(15_000, Math.min(300_000, Math.floor(Number(OPENAI_REQUEST_TIMEOUT_MS))))
    : 45_000;
  const DEFAULT_TEMPERATURE = 0.25;
  const DEFAULT_MAX_OUTPUT_TOKENS = 900;
  const MIN_MAX_OUTPUT_TOKENS = 16;

  function resolveTimeoutMs(modelName, timeoutMsOverride) {
    const override = Number(timeoutMsOverride);
    if (Number.isFinite(override)) {
      return Math.max(15_000, Math.min(300_000, Math.floor(override)));
    }

    const normalized = toTrimmedString(modelName, 120).toLowerCase();
    if (!normalized) return REQUEST_TIMEOUT_MS;

    if (normalized.startsWith('gpt-5.2-pro') || normalized.startsWith('gpt-5-pro')) {
      return Math.max(REQUEST_TIMEOUT_MS, 130_000);
    }

    if (normalized.startsWith('gpt-5')) {
      return Math.max(REQUEST_TIMEOUT_MS, 95_000);
    }

    if (normalized.startsWith('o1') || normalized.startsWith('o3') || normalized.startsWith('o4')) {
      return Math.max(REQUEST_TIMEOUT_MS, 95_000);
    }

    return REQUEST_TIMEOUT_MS;
  }

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
    jsonSchema,
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

    if (jsonSchema && typeof jsonSchema === 'object') {
      body.text = { format: jsonSchema };
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
          continue;
        }

        if (typeof content?.refusal === 'string' && content.refusal.trim()) {
          chunks.push(content.refusal.trim());
          continue;
        }

        if (Array.isArray(content?.summary)) {
          for (const summaryItem of content.summary) {
            if (typeof summaryItem?.text === 'string' && summaryItem.text.trim()) {
              chunks.push(summaryItem.text.trim());
            }
          }
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
    allowEmptyResponse = false,
    emptyResponseFallback = '',
    timeoutMs,
    jsonSchema,
  }) {
    if (!OPENAI_API_KEY) {
      throw Object.assign(new Error('OPENAI_API_KEY is not configured'), { status: 503 });
    }

    const resolvedModel = toTrimmedString(model, 120) || OPENAI_MODEL;
    const resolvedTimeoutMs = resolveTimeoutMs(resolvedModel, timeoutMs);
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), resolvedTimeoutMs);
    const startedAt = Date.now();
    const numericTemperature = Number.isFinite(Number(temperature)) ? Number(temperature) : DEFAULT_TEMPERATURE;
    const numericMaxOutputTokens = Number.isFinite(Number(maxOutputTokens))
      ? Math.max(MIN_MAX_OUTPUT_TOKENS, Math.floor(Number(maxOutputTokens)))
      : DEFAULT_MAX_OUTPUT_TOKENS;
    const shouldUseTemperature = modelSupportsTemperature(resolvedModel);
    const requestConfig = {
      model: resolvedModel,
      temperature: shouldUseTemperature ? numericTemperature : null,
      max_output_tokens: numericMaxOutputTokens,
      timeout_ms: resolvedTimeoutMs,
    };

    let response;
    let payload;
    let extractedReply = '';
    let emptyRetryCount = 0;
    try {
      const firstRequestBody = buildResponsesRequestBody({
        model: requestConfig.model,
        systemPrompt,
        userPrompt,
        temperature: requestConfig.temperature,
        maxOutputTokens: requestConfig.max_output_tokens,
        jsonSchema,
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
          jsonSchema,
        });
        const retryAttempt = await callResponsesApi({
          requestBody: retryRequestBody,
          signal: controller.signal,
        });
        response = retryAttempt.response;
        payload = retryAttempt.payload;
      }

      const maxEmptyResponseRetries = 1;
      while (response && response.ok) {
        extractedReply = extractOpenAiResponseText(payload);
        const fallbackReply = toTrimmedString(emptyResponseFallback, 1200);
        if (extractedReply || allowEmptyResponse || fallbackReply) {
          break;
        }
        if (emptyRetryCount >= maxEmptyResponseRetries) {
          break;
        }

        emptyRetryCount += 1;
        const retryRequestBody = buildResponsesRequestBody({
          model: requestConfig.model,
          systemPrompt,
          userPrompt,
          temperature: requestConfig.temperature,
          maxOutputTokens: requestConfig.max_output_tokens,
          jsonSchema,
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

    if (!extractedReply) {
      extractedReply = extractOpenAiResponseText(payload);
    }
    const fallbackReply = toTrimmedString(emptyResponseFallback, 1200);
    const reply = extractedReply || fallbackReply;

    if (!reply && !allowEmptyResponse) {
      throw Object.assign(new Error('AI response is empty'), { status: 502 });
    }

    return {
      reply: reply || 'Не удалось получить текстовый ответ. Попробуйте уточнить запрос и повторить.',
      usage: payload?.usage || null,
      debug: {
        request: requestConfig,
        response: {
          status: response.status,
          ok: response.ok,
          id: toTrimmedString(payload?.id, 120),
          created: toTrimmedString(payload?.created, 120),
          model: toTrimmedString(payload?.model, 120) || requestConfig.model,
          output_text_length: extractedReply.length,
          empty_retry_count: emptyRetryCount,
          used_empty_fallback: !extractedReply.length,
          completed_in_ms: Math.max(1, Date.now() - startedAt),
        },
        ...(includeRawPayload ? { raw_payload: payload } : {}),
      },
    };
  }

  async function requestOpenAiAudioTranscription({
    audioBuffer,
    mimeType = 'audio/webm',
    fileName = 'recording.webm',
    model = 'gpt-4o-transcribe',
    language = 'ru',
    timeoutMs,
  }) {
    if (!OPENAI_API_KEY) {
      throw Object.assign(new Error('OPENAI_API_KEY is not configured'), { status: 503 });
    }

    const resolvedModel = toTrimmedString(model, 120) || 'gpt-4o-transcribe';
    const resolvedLanguage = toTrimmedString(language, 16) || 'ru';
    const resolvedMimeType = toTrimmedString(mimeType, 80) || 'audio/webm';
    const resolvedFileName = toTrimmedString(fileName, 120) || 'recording.webm';
    const resolvedTimeoutMs = resolveTimeoutMs(resolvedModel, timeoutMs);
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), resolvedTimeoutMs);

    function shouldFallbackToWhisper(providerMessage, providerCode) {
      const message = toTrimmedString(providerMessage, 400).toLowerCase();
      const code = toTrimmedString(providerCode, 80).toLowerCase();
      if (code === 'model_not_found' || code === 'invalid_model') return true;
      if (!message) return false;
      return (
        message.includes('model') &&
        (
          message.includes('not found') ||
          message.includes('does not exist') ||
          message.includes('do not have access') ||
          message.includes('does not have access') ||
          message.includes('permission')
        )
      );
    }

    async function callTranscriptionApi(modelName, payload, mimeType, fileName, language) {
      const form = new FormData();
      form.append('model', modelName);
      form.append('language', language);
      form.append('file', new Blob([payload], { type: mimeType }), fileName);

      const response = await fetch('https://api.openai.com/v1/audio/transcriptions', {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${OPENAI_API_KEY}`,
        },
        body: form,
        signal: controller.signal,
      });
      const body = await response.json().catch(() => ({}));
      return { response, body };
    }

    try {
      const payload = Buffer.isBuffer(audioBuffer)
        ? audioBuffer
        : audioBuffer instanceof Uint8Array
          ? Buffer.from(audioBuffer)
          : null;
      if (!payload || payload.length === 0) {
        throw Object.assign(new Error('Audio file is empty'), { status: 400 });
      }

      let usedModel = resolvedModel;
      let { response, body } = await callTranscriptionApi(
        usedModel,
        payload,
        resolvedMimeType,
        resolvedFileName,
        resolvedLanguage,
      );

      const providerMessage = toTrimmedString(body?.error?.message, 300) || '';
      const providerCode = toTrimmedString(body?.error?.code, 80) || '';
      const canFallbackToWhisper =
        usedModel !== 'whisper-1' && shouldFallbackToWhisper(providerMessage, providerCode);
      if (!response.ok && canFallbackToWhisper) {
        usedModel = 'whisper-1';
        ({ response, body } = await callTranscriptionApi(
          usedModel,
          payload,
          resolvedMimeType,
          resolvedFileName,
          resolvedLanguage,
        ));
      }

      if (!response.ok) {
        const nextProviderMessage = toTrimmedString(body?.error?.message, 300) || 'AI provider error';
        throw Object.assign(new Error(nextProviderMessage), { status: 502 });
      }

      const text = toTrimmedString(body?.text, 20_000);
      if (!text) {
        throw Object.assign(new Error('AI transcription is empty'), { status: 502 });
      }

      return {
        text,
        model: usedModel,
      };
    } catch (error) {
      if (error && error.name === 'AbortError') {
        throw Object.assign(new Error('AI transcription timeout'), { status: 504 });
      }
      throw error;
    } finally {
      clearTimeout(timeout);
    }
  }

  return {
    extractOpenAiResponseText,
    requestOpenAiAgentReply,
    requestOpenAiAudioTranscription,
  };
}

module.exports = {
  createAiProvider,
};
