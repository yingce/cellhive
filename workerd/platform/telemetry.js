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
export function sampled(traceparent = "") {
  const p = parseTraceparent(traceparent);
  return p.sampled;
}

// startSpan begins a span; returns null when the trace is not sampled. parent
// may be another platform span when work is nested in this isolate.
export function startSpan(name, attrs = {}, parent = null, traceparent = "") {
  const p = parseTraceparent(traceparent);
  if (!parent && !p.sampled) return null;
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

// --- Generic OTLP/HTTP log record encoding -----------------------------------
//
// Platform code cannot run the OpenTelemetry SDK, so requests that must reach a
// collector directly encode a minimal ExportLogsServiceRequest by hand. Only the
// fields CellHive actually queries are emitted.

function otlpVarint(value) {
  let v = BigInt(value);
  const out = [];
  while (v > 127n) { out.push(Number(v & 127n) | 128); v >>= 7n; }
  out.push(Number(v));
  return out;
}
function otlpTag(field, wire) { return otlpVarint((field << 3) | wire); }
function otlpBytes(field, bytes) { return [...otlpTag(field, 2), ...otlpVarint(bytes.length), ...bytes]; }
function otlpString(field, value) { return otlpBytes(field, new TextEncoder().encode(String(value))); }
function otlpFixed64(field, value) {
  const bytes = new Uint8Array(8);
  new DataView(bytes.buffer).setBigUint64(0, BigInt(value), true);
  return [...otlpTag(field, 1), ...bytes];
}
function otlpInt(field, value) { return [...otlpTag(field, 0), ...otlpVarint(value)]; }
function otlpAny(value) {
  if (typeof value === "boolean") return otlpInt(2, value ? 1 : 0);
  if (typeof value === "number") return otlpInt(3, value);
  return otlpString(1, value);
}
function otlpKeyValue(key, value) { return [...otlpString(1, key), ...otlpBytes(2, otlpAny(value))]; }
function otlpHex(id) {
  const out = new Uint8Array(id.length / 2);
  for (let i = 0; i < out.length; i++) out[i] = parseInt(id.slice(i * 2, i * 2 + 2), 16);
  return out;
}

// traceIds splits a W3C traceparent into its trace and span ids.
export function traceIds(tp) {
  const m = /^[0-9a-fA-F]{2}-([0-9a-fA-F]{32})-([0-9a-fA-F]{16})/.exec(String(tp || ""));
  return m ? [m[1].toLowerCase(), m[2].toLowerCase()] : ["", ""];
}

// otlpLogsProtobuf encodes one ExportLogsServiceRequest. Every record carries
// the supplied attributes; `message` becomes the log body.
export function otlpLogsProtobuf(scopeName, serviceName, records) {
  const scopeLogs = [
    ...otlpBytes(1, otlpString(1, scopeName)),
    ...records.flatMap((r) => otlpBytes(2, otlpLogRecord(r))),
  ];
  const resourceLogs = [
    ...otlpBytes(1, otlpBytes(1, otlpKeyValue("service.name", serviceName))),
    ...otlpBytes(2, scopeLogs),
  ];
  return new Uint8Array(otlpBytes(1, resourceLogs));
}

function otlpLogRecord(r) {
  const at = Number(r.atMs) || Date.now();
  const record = [
    ...otlpFixed64(1, BigInt(at) * 1000000n),
    ...otlpFixed64(11, BigInt(at) * 1000000n),
    ...otlpInt(2, r.severityNumber || 9),
    ...otlpString(3, r.severityText || "INFO"),
    ...otlpBytes(5, otlpAny(r.message === undefined ? "" : String(r.message))),
  ];
  for (const [k, v] of Object.entries(r.attributes || {})) {
    if (v === null || v === undefined || v === "") continue;
    record.push(...otlpBytes(6, otlpKeyValue(k, v)));
  }
  if (r.traceId && r.spanId && r.traceId.length === 32 && r.spanId.length === 16) {
    record.push(...otlpBytes(9, otlpHex(r.traceId)), ...otlpBytes(10, otlpHex(r.spanId)));
  }
  return record;
}

// exportLogRecords ships records to env.OTLP_LOGS_URL, falling back to the
// trusted local log pipe when no collector is configured. Best-effort: a
// failure never affects the caller's result.
export async function exportLogRecords(env, records) {
  if (!records || records.length === 0) return;
  if (env.FILE_LOG_TOKEN) {
    for (const r of records) {
      fileLog(env, r.attributes || {}, r.message || "", (r.severityText || "info").toLowerCase(), r.atMs, r.traceparent || "");
    }
    return;
  }
  const base = String(env.OTLP_LOGS_URL || "").trim();
  if (!base) return;
  let extra = {};
  try { extra = JSON.parse(env.OTLP_LOGS_HEADERS || "{}"); } catch (e) { extra = {}; }
  const headers = new Headers({ "content-type": "application/x-protobuf" });
  for (const [k, v] of Object.entries(extra)) headers.set(k, String(v));
  const url = base.replace(/\/$/, "").replace(/\/v1\/(traces|logs)$/, "") + "/v1/logs";
  try {
    await postTelemetry(url, {
      method: "POST",
      headers,
      body: otlpLogsProtobuf("cellhive.user-runtime", "cellhive-user-runtime", records),
    });
  } catch (e) {
    /* best-effort: the invocation itself already succeeded */
  }
}

// Failure boundaries are evidence, not ownership. Never inspect response bodies.
export function invocationFailureAttributes(stage, error, status, origin = "") {
  const allowed = ['worker_load','bundle_fetch','binding_setup','runtime_capacity','handler','handler_response','response_processing','dispatch'];
  const boundary = allowed.includes(stage) ? stage : 'unknown';
  let name='Error', message='', stack='';
  try { name=error?.name || 'Error'; message=error == null ? '' : String(error); stack=typeof error?.stack === 'string' ? error.stack : ''; } catch {}
  const safe = value => String(value).replace(/bearer\s+[^\s,;]+/gi,'Bearer [redacted]')
    .replace(/(token|password|secret|api[_-]?key|authorization)"?\s*[=:]\s*"?[^"\s,;]+/gi,'credential=[redacted]')
    .replace(/https?:\/\/[^\s]+/g,'[url]').slice(0,2048);
  let frame=null;
  for(const line of stack.slice(0,8192).split('\n')) {
    if(!line.trim().startsWith('at ')) continue;
    const m=/([^/\\\s():?#]+):([1-9][0-9]{0,5}):([1-9][0-9]{0,5})\)?$/.exec(line.trim());
    if(m && /^[A-Za-z0-9_.-]{1,80}$/.test(m[1])) { frame=m[1]+':'+m[2]+':'+m[3]; break; }
  }
  const functionResult=boundary==='handler_response'||boundary==='response_processing'?'returned':boundary==='handler'&&origin==='user_function'?'threw':boundary==='handler' && !origin && frame?.startsWith('tenant.js:')?'threw':['worker_load','bundle_fetch','binding_setup','runtime_capacity'].includes(boundary)?'not_started':'unknown';
  return {
    'cellhive.function_result':functionResult,
    'cellhive.status_kind':'http',
    'cellhive.failure_stage':boundary,
    'cellhive.error_origin':origin==='platform'||origin==='user_function'?origin:boundary==='response_processing'?'unknown':functionResult==='returned'||functionResult==='threw'?'user_function':boundary==='runtime_capacity'?'platform':'unknown',
    'cellhive.error_name':error && /^[A-Za-z][A-Za-z0-9_.-]{0,63}$/.test(name)?name:null,
    'cellhive.error_frame':frame,
    'cellhive.error':safe(message || (boundary==='handler_response'?'Handler returned HTTP '+status:'Invocation failed at '+boundary)),
  };
}
