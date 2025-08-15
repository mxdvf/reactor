export default async function fetchWithTimeout(
  url: string,
  timeoutMs: number
): Promise<{ ok: boolean; json?: any; latencyMs?: number; error?: string }> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);

  const start = performance.now();
  try {
    const response = await fetch(url, { signal: controller.signal });
    const latencyMs = Math.round(performance.now() - start);

    if (!response.ok) {
      return { ok: false, error: `HTTP ${response.status}`, latencyMs };
    }

    const json = await response.json();
    return { ok: true, json, latencyMs };
  } catch (err: any) {
    return { ok: false, error: err.name === "AbortError" ? "Timeout" : err.message };
  } finally {
    clearTimeout(timeout);
  }
}
