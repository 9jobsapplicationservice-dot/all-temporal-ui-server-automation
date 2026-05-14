type ApiErrorPayload = {
  error?: string;
  message?: string;
};

function summarizeText(value: string): string {
  const compact = value.replace(/\s+/g, ' ').trim();
  if (!compact) {
    return '';
  }
  return compact.length > 180 ? `${compact.slice(0, 177)}...` : compact;
}

function extractPayloadError(payload: unknown): string {
  if (!payload || typeof payload !== 'object') {
    return '';
  }

  const { error, message } = payload as ApiErrorPayload;
  if (typeof error === 'string' && error.trim()) {
    return error.trim();
  }
  if (typeof message === 'string' && message.trim()) {
    return message.trim();
  }
  return '';
}

export async function readApiJson<T>(response: Response, fallbackMessage: string): Promise<T> {
  const raw = await response.text();
  const contentType = response.headers.get('content-type')?.toLowerCase() ?? '';
  const trimmed = raw.trim();

  let payload: unknown = {};
  if (trimmed) {
    const looksJson = contentType.includes('application/json') || trimmed.startsWith('{') || trimmed.startsWith('[');
    if (looksJson) {
      try {
        payload = JSON.parse(trimmed);
      } catch {
        throw new Error(`${fallbackMessage} The server returned malformed JSON.`);
      }
    } else if (trimmed.startsWith('<')) {
      const statusLabel = response.status ? ` (${response.status} ${response.statusText})` : '';
      throw new Error(
        `${fallbackMessage} The server returned HTML instead of JSON${statusLabel}. If the dev server is running, restart it and reload the page.`,
      );
    } else {
      throw new Error(`${fallbackMessage} ${summarizeText(trimmed)}`.trim());
    }
  }

  if (!response.ok) {
    throw new Error(
      extractPayloadError(payload) || `${fallbackMessage} Request failed with status ${response.status}.`,
    );
  }

  return payload as T;
}
