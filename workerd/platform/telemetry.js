// Span buffer for the workerd platform workers (ADR-167).
//
// workerd cannot run the OpenTelemetry SDK, so platform JS records lightweight
// span records and forwards them through the trusted local supervisor pipe.
// Embeddings without a file token fall back to cell-agent's internal span API. Spans are only recorded when the current trace
// is sampled (W3C traceparent sampled bit), so an unsampled request pays
// nothing beyond the traceparent check.

import { internalHeaders } from "jwt.js";

const MAX_BUFFER = 256;
let buffer = [];

// Only trusted host code receives this token. The parent strips it before
// persistence; tenant console text cannot become an attributed log record.
export function fileLog(env, attributes, message, level = "info", at = Date.now(), tp = "") {
  const token = env.FILE_LOG_TOKEN;
  if (!token) return false;
  const trace = parseTraceparent(tp);
  const record = JSON.stringify({kind:"log", timestamp:new Date(at).toISOString(),
    observed_timestamp:new Date().toISOString(), severity_text:level, message,
    trace_id:trace.traceId || "", span_id:trace.spanId || "", attributes});
  // Handled but dropped: never fall back to HTTP or allocate an unbounded pipe line.
  if (new TextEncoder().encode(record).byteLength <= 60000) console.log("@cellhive-log:" + token + " " + record);
  else console.log("@cellhive-log:" + token + ' {"kind":"drop"}');
  return true;
}

// Per-isolate best-effort export budget, shared by invocation logs and spans.
// Never queue callers behind a slow collector or retain unbounded response data.
let exportsInFlight = 0;
export async function postTelemetry(url, init) {
  if (exportsInFlight >= 16) return null;
  exportsInFlight++;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(new Error("telemetry timeout")), 3000);
  let reader;
  try {
    const response = await fetch(url, {...init, signal: controller.signal});
    if (response.body) {
      reader = response.body.getReader();
      let size = 0;
      for (;;) {
        const {done, value} = await reader.read();
        if (done) break;
        size += value.byteLength;
        if (size > 65536) throw new Error("telemetry response too large");
      }
    }
    return response;
  } catch (error) {
    controller.abort(error);
    // Cancellation must not hold export admission if a broken peer never ends.
    if (reader) void reader.cancel(error).catch(() => {});
    throw error;
  } finally {
    clearTimeout(timer);
    if (reader) reader.releaseLock();
    exportsInFlight--;
  }
}

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

// traceparentForSpan continues propagation from a platform span rather than
// from that span's parent.
export function traceparentForSpan(span) {
  if (!span) return "";
  return "00-" + span.trace_id + "-" + span.span_id + "-01";
}

// sampled reports whether the active trace should be recorded.
export function sampled() {
  const p = parseTraceparent(globalThis.__cellhiveTraceparent);
  return p.sampled;
}

// startSpan begins a span; returns null when the trace is not sampled. parent
// may be another platform span when work is nested in this isolate.
export function startSpan(name, attrs = {}, parent = null) {
  const p = parseTraceparent(globalThis.__cellhiveTraceparent);
  if (!p.sampled) return null;
  const span = {
    trace_id: parent ? parent.trace_id : p.traceId,
    span_id: randomHex(8),
    parent_span_id: parent ? parent.span_id : p.spanId,
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

// flushSpans emits local frames, or uses cell-agent for legacy embeddings.
export async function flushSpans(env) {
  if (buffer.length === 0) return;
  const spans = buffer;
  buffer = [];
  if (env.FILE_LOG_TOKEN) {
    for (const span of spans) {
      const record = JSON.stringify({kind:"span", span});
      if (new TextEncoder().encode(record).byteLength <= 60000) console.log("@cellhive-log:" + env.FILE_LOG_TOKEN + " " + record);
      else console.log("@cellhive-log:" + env.FILE_LOG_TOKEN + ' {"kind":"drop"}');
    }
    return;
  }
  const base = String(env.CELL_URL || "").replace(/\/$/, "");
  if (!base) return;
  try {
    await postTelemetry(base + "/v1/internal/telemetry/spans", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...await internalHeaders(env.CELL_TOKEN),
      },
      body: JSON.stringify({ spans }),
    });
  } catch (e) {
    /* best-effort: spans are dropped, the request path is unaffected */
  }
}
