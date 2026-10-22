export type Labels = Readonly<Record<string, string>>;
export type Sample = { name: string; value: number; labels?: Labels; timestamp?: number };
export type Options = {
  endpoint: string; token: string; labels?: Labels; allowHttpLoopback?: boolean;
  maxSeries?: number; maxPending?: number; batchSize?: number; timeoutMs?: number; retries?: number;
};
export class MetricsError extends Error {
  constructor(public readonly code: string, public readonly status?: number) { super(`metrics233: ${code}${status ? ` (${status})` : ""}`); this.name = "MetricsError"; }
}
type Entry = { sample: Sample; kind: "counter" | "gauge" | "record"; version: number; dirty: boolean };
type Pending = { body: string; count: number; entries: [string, number][] };
type Core = {
  url: string; token: string; maxSeries: number; maxPending: number; batchSize: number; timeout: number; retries: number;
  series: Map<string, Entry>; kinds: Map<string, string>; records: Sample[]; pending: Pending[];
  state: "open" | "closing" | "closed"; inFlight?: Promise<void>; timer?: ReturnType<typeof setTimeout>;
  interval?: number; retryAt: number; lastError?: string; accepted: number;
};
const encoder = new TextEncoder();
const bytes = (value: string) => encoder.encode(value).length;
const labelName = /^[a-zA-Z_][a-zA-Z0-9_]{0,99}$/;
function labels(value: Labels): Record<string, string> {
  if (!value || typeof value !== "object" || Array.isArray(value) || Object.keys(value).length > 32) throw new MetricsError("invalid_labels");
  const out: Record<string, string> = Object.create(null);
  for (const key of Object.keys(value).sort()) { const v = value[key]; if (!labelName.test(key) || key === "__name__" || typeof v !== "string" || bytes(v) > 512) throw new MetricsError("invalid_labels"); out[key] = v; }
  return out;
}
function integer(value: number, min: number, max: number) { if (!Number.isInteger(value) || value < min || value > max) throw new MetricsError("invalid_limit"); return value; }
function endpoint(value: string, allow: boolean): string {
  let url: URL; try { url = new URL(value); } catch { throw new MetricsError("invalid_endpoint"); }
  const loopback = url.hostname === "localhost" || url.hostname === "[::1]" || /^127\.\d+\.\d+\.\d+$/.test(url.hostname);
  if (url.username || url.password || url.search || url.hash || (url.protocol !== "https:" && !(allow && loopback && url.protocol === "http:"))) throw new MetricsError("invalid_endpoint");
  url.pathname = url.pathname.replace(/\/+$/, "") + "/api/v1/ingest"; return url.toString();
}

/** No implicit I/O, environment reads or background tasks. Views share a bounded registry. */
export class Metrics233 {
  #core: Core;
  #labels: Record<string, string>;
  constructor(options: Options) {
    if (typeof options.token !== "string" || !/^[\x21-\x7e]{32,8192}$/.test(options.token)) throw new MetricsError("invalid_token");
    this.#labels = labels(options.labels || {});
    this.#core = { url: endpoint(options.endpoint, !!options.allowHttpLoopback), token: options.token,
      maxSeries: integer(options.maxSeries ?? 1000, 1, 100000), maxPending: integer(options.maxPending ?? 1000, 1, 100000), batchSize: integer(options.batchSize ?? 500, 1, 10000), timeout: integer(options.timeoutMs ?? 5000, 1, 300000), retries: integer(options.retries ?? 2, 0, 5),
      series: new Map(), kinds: new Map(), records: [], pending: [], state: "open", retryAt: 0, accepted: 0 };
  }
  withLabels(extra: Labels): Metrics233 {
    const view = new Metrics233({ endpoint: this.#core.url.replace(/\/api\/v1\/ingest$/, ""), token: this.#core.token, allowHttpLoopback: true });
    view.#labels = labels({ ...this.#labels, ...extra }); view.#core = this.#core; return view;
  }
  counter(name: string, delta = 1): this { return this.#put({ name, value: delta }, "counter"); }
  gauge(name: string, value: number): this { return this.#put({ name, value }, "gauge"); }
  record(sample: Sample): this { return this.#put(sample, "record"); }
  #put(input: Sample, kind: Entry["kind"]): this {
    const core = this.#core;
    if (core.state !== "open") throw new MetricsError("closed");
    if (!/^[a-zA-Z_:][a-zA-Z0-9_:]{0,199}$/.test(input.name) || !Number.isFinite(input.value) || (kind === "counter" && input.value < 0)) throw new MetricsError("invalid_sample");
    const ls = labels({ ...this.#labels, ...input.labels }), key = JSON.stringify([input.name, ls]);
    const before = core.series.get(key), registeredKind = core.kinds.get(input.name);
    if (registeredKind && registeredKind !== kind) throw new MetricsError("metric_kind_conflict");
    const timestamp = input.timestamp === undefined || input.timestamp === 0 ? Date.now() : input.timestamp;
    if (!Number.isSafeInteger(timestamp) || timestamp < 0 || timestamp > Date.now() + 300000) throw new MetricsError("invalid_timestamp");
    if (!before && core.series.size >= core.maxSeries) throw new MetricsError("series_limit");
    if (kind === "record" && core.records.length + core.pending.reduce((n, p) => n + p.count, 0) >= core.maxPending) throw new MetricsError("pending_limit");
    const value = kind === "counter" ? (before?.sample.value || 0) + input.value : input.value;
    if (!Number.isFinite(value)) throw new MetricsError("counter_overflow");
    const sample = { name: input.name, value, labels: ls, timestamp };
    core.kinds.set(input.name, kind);
    core.series.set(key, { sample, kind, version: (before?.version || 0) + 1, dirty: kind !== "record" });
    if (kind === "record") core.records.push(sample);
    return this;
  }
  stats() { const c = this.#core; return { state: c.state, series: c.series.size, pendingRecords: c.records.length, pendingBatches: c.pending.length, accepted: c.accepted, lastError: c.lastError }; }
  start(intervalMs = 15000): this {
    const c = this.#core; integer(intervalMs, 100, 86400000);
    if (c.state !== "open") throw new MetricsError("closed"); if (c.interval !== undefined) throw new MetricsError("already_running"); c.interval = intervalMs;
    const run = async () => { try { await this.flush(); } catch { /* available in stats */ } finally { if (c.interval !== undefined && c.state === "open") c.timer = setTimeout(run, c.interval); } };
    void run(); return this;
  }
  flush(): Promise<void> {
    const c = this.#core;
    if (c.state === "closed") return Promise.resolve();
    if (c.inFlight) return c.inFlight;
    c.inFlight = this.#send().catch(error => { const safe = error instanceof MetricsError ? error : new MetricsError("delivery_failed"); c.lastError = safe.code; throw safe; }).finally(() => { c.inFlight = undefined; });
    return c.inFlight;
  }
  async shutdown(): Promise<void> {
    const c = this.#core; if (c.state === "closed") return;
    c.state = "closing"; c.interval = undefined; if (c.timer) clearTimeout(c.timer);
    if (c.inFlight) await c.inFlight; await this.flush(); c.state = "closed";
  }
  #snapshot() {
    const c = this.#core; if (c.pending.length) return;
    const items: { sample: Sample; entry?: [string, number] }[] = [];
    for (const [key, entry] of c.series) if (entry.dirty) items.push({ sample: { ...entry.sample, timestamp: Date.now() }, entry: [key, entry.version] });
    items.push(...c.records.map(sample => ({ sample })));
    let encoded: string[] = [], entries: [string, number][] = [], size = 16;
    const finish = () => { if (encoded.length) c.pending.push({ body: `{"samples":[${encoded.join(",")}]}`, count: encoded.length, entries }); encoded = []; entries = []; size = 16; };
    for (const item of items) { const json = JSON.stringify(item.sample), length = bytes(json) + 1; if (encoded.length >= c.batchSize || size + length > 4 * 1024 * 1024) finish(); encoded.push(json); size += length; if (item.entry) entries.push(item.entry); }
    finish(); c.records = [];
  }
  async #send(): Promise<void> {
    const c = this.#core; if (Date.now() < c.retryAt) throw new MetricsError("retry_after");
    this.#snapshot(); if (!c.pending.length) { c.lastError = undefined; return; }
    const abort = new AbortController(), timer = setTimeout(() => abort.abort(), c.timeout);
    const pause = (ms: number) => new Promise<void>((resolve, reject) => { if (abort.signal.aborted) { reject(new MetricsError("deadline")); return; } const stop = () => { clearTimeout(wait); reject(new MetricsError("deadline")); }; const wait = setTimeout(() => { abort.signal.removeEventListener("abort", stop); resolve(); }, ms); abort.signal.addEventListener("abort", stop, { once: true }); });
    try {
      while (true) {
        if (!c.pending.length) this.#snapshot();
        if (!c.pending.length) break;
        const batch = c.pending[0];
        for (let attempt = 0; ; attempt++) {
          let response: Response;
          try { response = await fetch(c.url, { method: "POST", redirect: "manual", credentials: "omit", referrerPolicy: "no-referrer", headers: { "Authorization": `Bearer ${c.token}`, "Content-Type": "application/json" }, body: batch.body, signal: abort.signal }); }
          catch { if (abort.signal.aborted) throw new MetricsError("deadline"); if (attempt >= c.retries) throw new MetricsError("network"); await pause(100 * 2 ** attempt); continue; }
          if (response.status === 429 || response.status >= 500) {
            const retry = response.headers.get("retry-after"); let delay = retry && /^\d+$/.test(retry) ? Number(retry) * 1000 : retry ? Date.parse(retry) - Date.now() : 100 * 2 ** attempt;
            if (!Number.isFinite(delay) || delay < 0) delay = 100 * 2 ** attempt; delay = Math.min(delay, 7 * 86400000); c.retryAt = Date.now() + delay; await response.body?.cancel();
            if (attempt >= c.retries) throw new MetricsError("http_status", response.status); await pause(delay); continue;
          }
          if (!response.ok) { await response.body?.cancel(); throw new MetricsError("http_status", response.status); }
          const reader = response.body?.getReader(); if (!reader) throw new MetricsError("invalid_ack");
          let body = "", size = 0; const decoder = new TextDecoder();
          try { while (true) { const value = await reader.read(); if (value.done) break; size += value.value.length; if (size > 65536) { await reader.cancel(); throw new MetricsError("invalid_ack"); } body += decoder.decode(value.value, { stream: true }); } body += decoder.decode(); }
          catch (error) { if (error instanceof MetricsError) throw error; throw new MetricsError(abort.signal.aborted ? "deadline" : "invalid_ack"); }
          let ack: unknown; try { ack = JSON.parse(body); } catch { throw new MetricsError("invalid_ack"); }
          if (!ack || typeof ack !== "object" || (ack as { accepted?: unknown }).accepted !== batch.count) throw new MetricsError("partial_ack");
          c.pending.shift(); c.accepted += batch.count; c.retryAt = 0;
          for (const [key, version] of batch.entries) { const entry = c.series.get(key); if (entry?.version === version) entry.dirty = false; }
          break;
        }
      }
      c.lastError = undefined;
    } finally { clearTimeout(timer); }
  }
}
