// Span buffer for the workerd platform workers (ADR-167).
//
// workerd cannot run the OpenTelemetry SDK, so platform JS records lightweight
// span records and forwards them (best-effort) to cell-agent's
// /v1/internal/telemetry/spans, which exports them through the same OTLP
// pipeline as the Go services. Spans are only recorded when the current trace
// is sampled (W3C traceparent sampled bit), so an unsampled request pays
// nothing beyond the traceparent check.

const MAX_BUFFER = 256;
let buffer = [];

function randomHex(bytes) {
  const b = new Uint8Array(bytes);
  crypto.getRandomValues(b);
  let out = "";
  for (let i = 0; i < b.length; i++) out += b[i].toString(16).padStart(2, "0");
  return out;
}

// parseTraceparent returns the ids and sampled bit of a W3C traceparent.
function parseTraceparent(tp) {
  const empty = { traceId: "", spanId: "", sampled: false };
  if (!tp || typeof tp !== "string") return empty;
  const m = /^[0-9a-f]{2}-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})/.exec(tp.trim().toLowerCase());
  if (!m) return empty;
  return { traceId: m[1], spanId: m[2], sampled: (parseInt(m[3], 16) & 1) === 1 };
}

// traceparent builds a W3C traceparent with the sampled bit set from `sampled`.
export function traceparent(sampled) {
  return "00-" + randomHex(16) + "-" + randomHex(8) + "-" + (sampled ? "01" : "00");
}

// sampled reports whether the active trace should be recorded.
export function sampled() {
  const p = parseTraceparent(globalThis.__cellhiveTraceparent);
  return p.sampled;
}

// startSpan begins a span; returns null when the trace is not sampled.
export function startSpan(name, attrs = {}) {
  const p = parseTraceparent(globalThis.__cellhiveTraceparent);
  if (!p.sampled) return null;
  const span = {
    trace_id: p.traceId,
    span_id: randomHex(8),
    parent_span_id: p.spanId,
    name,
    kind: attrs.kind || "internal",
    start_unix_nano: Date.now() * 1e6,
    attributes: {},
    status_code: 0,
    status_message: "",
  };
  for (const [k, v] of Object.entries(attrs)) {
    if (k === "kind" || v === null || v === undefined) continue;
    span.attributes[k] = String(v);
  }
  return span;
}

// endSpan finishes a span and buffers it (dropped oldest-first when full).
export function endSpan(span, opts = {}) {
  if (!span) return;
  span.end_unix_nano = Date.now() * 1e6;
  span.status_code = opts.code || 0;
  span.status_message = opts.message || "";
  buffer.push(span);
  if (buffer.length > MAX_BUFFER) buffer.splice(0, buffer.length - MAX_BUFFER);
}

// flushSpans posts buffered spans to cell-agent (best-effort, never throws).
export async function flushSpans(env) {
  if (buffer.length === 0) return;
  const spans = buffer;
  buffer = [];
  const base = String(env.CELL_URL || "").replace(/\/$/, "");
  if (!base) return;
  try {
    await fetch(base + "/v1/internal/telemetry/spans", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-cellhive-internal-token": env.CELL_TOKEN || "",
      },
      body: JSON.stringify({ spans }),
    });
  } catch (e) {
    /* best-effort: spans are dropped, the request path is unaffected */
  }
}
