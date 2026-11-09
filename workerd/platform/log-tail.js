import { WorkerEntrypoint } from "cloudflare:workers";
import { authorized, internalHeaders } from "jwt.js";

// A stuck control-plane read must not pin workerd's shared HTTP client's old
// resolved address indefinitely. The budget includes response body parsing.
export async function readLogCapture(env) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 2000);
  try {
    const response = await fetch(String(env.CELL_URL).replace(/\/$/, "") + "/v1/internal/log-capture", {
      headers: await internalHeaders(env.CELL_TOKEN),
      signal: controller.signal,
    });
    if (!response.ok) throw new Error("log capture configuration unavailable");
    return await response.json();
  } finally {
    controller.abort();
    clearTimeout(timeout);
  }
}

function formatValue(value) {
  if (typeof value === "string") return value;
  if (typeof value === "bigint") return `${value}n`;
  if (value === undefined) return "undefined";
  try {
    const encoded = JSON.stringify(value, (_key, item) =>
      typeof item === "bigint" ? `${item}n` : item,
    );
    return encoded === undefined ? String(value) : encoded;
  } catch (_err) {
    return String(value);
  }
}

function safeArgs(message) {
  const args = Array.isArray(message) ? message : [message];
  return args.map((arg) => {
    try {
      return JSON.parse(JSON.stringify(arg, (_key, value) =>
        typeof value === "bigint" ? `${value}n` : value));
    } catch (_err) {
      return formatValue(arg);
    }
  });
}

function dispatchMetadata(item, tailKey) {
  const method = item?.event?.rpcMethod;
  if (!tailKey || (method !== "handleQueue" && method !== "handleScheduled")) return null;
  for (const line of item.logs || []) {
    const args = line.message;
    if (!Array.isArray(args) || args[0] !== "__cellhive_dispatch_v1__" || args[1] !== tailKey) continue;
    const value = args[2];
    if (!value || typeof value !== "object" || Array.isArray(value)) continue;
    const startedMs = Number.isSafeInteger(value.started_ms) && value.started_ms > 0 ? value.started_ms : null;
    const requestId = typeof value.request_id === "string" ? value.request_id : "";
    if (method === "handleQueue" && typeof value.queue === "string" && value.queue &&
        Number.isSafeInteger(value.batchSize) && value.batchSize >= 0) {
      return { line, startedMs, requestId, event: { rpcMethod: method, queue: value.queue, batchSize: value.batchSize } };
    }
    if (method === "handleScheduled" && typeof value.cron === "string" && value.cron &&
        Number.isSafeInteger(value.scheduledTime) && value.scheduledTime > 0) {
      return { line, startedMs, requestId, event: { rpcMethod: method, cron: value.cron, scheduledTime: value.scheduledTime } };
    }
  }
  return null;
}

function eventInfo(info) {
  if (!info || typeof info !== "object") return null;
  if (info.request) {
    const request = info.request;
    const headers = {};
    for (const [name, value] of Object.entries(request.headers || {})) {
      if (["authorization", "cookie", "proxy-authorization", "x-api-key"].includes(name.toLowerCase())) continue;
      headers[name] = value;
    }
    return { request: { method: request.method, url: request.url, headers },
      ...(info.response ? { response: { status: info.response.status } } : {}) };
  }
  if (typeof info.cron === "string") return { cron: info.cron, ...(info.scheduledTime ? { scheduledTime: info.scheduledTime } : {}), ...(typeof info.request_id === "string" && info.request_id ? { request_id: info.request_id } : {}), ...(info.rpcMethod ? { rpcMethod: info.rpcMethod } : {}) };
  if (typeof info.queue === "string") return { queue: info.queue, batchSize: info.batchSize, ...(info.rpcMethod ? { rpcMethod: info.rpcMethod } : {}) };
  if (typeof info.rpcMethod === "string") return { rpcMethod: info.rpcMethod };
  return null;
}

function eventKind(info) {
  if (info?.request) return "fetch";
  if (typeof info?.cron === "string") return "scheduled";
  if (typeof info?.queue === "string") return "queue";
  if (info?.rpcMethod === "handleQueue") return "queue";
  if (info?.rpcMethod === "handleScheduled") return "scheduled";
  if (info?.rpcMethod === "handleRun") return "workflow";
  return "";
}

// Platform lifecycle RPCs (status/retire) are control-plane calls into the
// dynamic worker, not tenant invocations. workerd marks the aborting retire
// call as an exception, which would otherwise be exported as a failed
// function_request (CELLHIVE_WORKER_RETIRED) even though retirement only runs
// once the child reports idle and no tenant request was interrupted.
export function platformLifecycleEvent(item) {
  if (item?.entrypoint === "CellHiveLifecycle") return true;
  return !item?.entrypoint &&
    (item?.event?.rpcMethod === "status" || item?.event?.rpcMethod === "retire");
}

function eventRecords(events, props) {
  return (events || []).filter((item) => !platformLifecycleEvent(item)).map((item) => {
    const dispatch = dispatchMetadata(item, props.tailKey);
    const info = eventInfo(dispatch?.event || item.event);
    const headers = info?.request?.headers || {};
    const traceparent = headers.traceparent || headers.Traceparent || "";
    const requestId = dispatch?.requestId || info?.request_id || headers["x-request-id"] || headers["X-Request-Id"] || headers["cf-ray"] || "";
    const startedMs = dispatch?.startedMs ?? null;
    // For scheduled/queue the native eventTimestamp is the dispatch time, not
    // the handler finish. The tail record is stored when the invocation ends,
    // so use the observation time for the completion timestamp.
    const finishedMs = startedMs !== null ? Math.max(startedMs + 1, Date.now()) : (item.eventTimestamp || Date.now());
    const nativeVersion = item.scriptVersion;
    const version = nativeVersion
      ? { id: String(nativeVersion.id), name: props.worker, tags: nativeVersion.tag ? [nativeVersion.tag] : [] }
      : { id: String(props.version || 0), name: props.worker, tags: [] };
    return {
      scriptName: props.worker,
      scriptVersion: version,
      outcome: item.outcome || "unknown",
      exceptions: (item.exceptions || []).map((error) => ({
        name: error.name || "Error", message: error.message || "", timestamp: error.timestamp || item.eventTimestamp || Date.now(),
        ...(error.stack ? { stack: error.stack } : {}),
      })),
      logs: (item.logs || []).filter((line) => line !== dispatch?.line).map((line) => ({
        message: safeArgs(line.message), level: line.level || "log",
        timestamp: line.timestamp || item.eventTimestamp || Date.now(),
      })),
      eventTimestamp: item.eventTimestamp || Date.now(),
      ...(startedMs !== null ? { started_ms: startedMs } : {}),
      ...(startedMs !== null ? { finished_ms: finishedMs } : {}),
      ...(info ? { event: info } : {}),
      event_type: eventKind(dispatch?.event || item.event),
      ...(item.dispatchNamespace ? { dispatchNamespace: item.dispatchNamespace } : {}),
      ...(item.entrypoint ? { entrypoint: item.entrypoint } : {}),
      ...(item.durableObjectId ? { durableObjectId: item.durableObjectId } : {}),
      diagnosticsChannelEvents: item.diagnosticsChannelEvents || [],
      requestId,
      traceparent,
    };
  });
}

// Each host service has its own bounded ring. Tail writes only JS memory; the
// authenticated runtime endpoint lets cell-agent pull when a client subscribes.
const rings = new Map();
let globalItems = [];
let globalHead = 0;
let globalBytes = 0;
let ringSession;
const maxWorkers = 200;
const maxEvents = 1000;
const maxBytes = 1 << 20;
let sequence = 0;

function storeRecords(namespace, worker, records, props) {
  const key = JSON.stringify([namespace, worker]);
  let ring = rings.get(key);
  if (!ring) {
    ring = { items: [], bytes: 0 };
    rings.set(key, ring);
    if (rings.size > maxWorkers) rings.delete(rings.keys().next().value);
  }
  for (const event of records) {
    // Quiet invocation events are small. Avoid JSON serialization on the hot
    // path; allow for long request URLs/headers in the byte estimate.
    const request = event.event?.request;
    let quietSize = 512 + (request?.url?.length || 0) * 2;
    for (const [name, value] of Object.entries(request?.headers || {})) {
      quietSize += (name.length + String(value).length) * 2 + 32;
    }
    const size = event.logs?.length || event.exceptions?.length
      ? new TextEncoder().encode(JSON.stringify(event)).byteLength : quietSize;
    if (size > 512 * 1024) continue; // one record must fit a bounded query page
    const item = { seq: ++sequence, namespace, worker, raw: event, props, size };
    ring.items.push(item);
    ring.bytes += size;
    globalItems.push(item);
    globalBytes += size;
    while (globalItems.length - globalHead > 10000 || globalBytes > 8 * maxBytes) {
      globalBytes -= globalItems[globalHead++].size;
    }
    if (globalHead > 10000) { globalItems = globalItems.slice(globalHead); globalHead = 0; }
    while (ring.items.length > maxEvents || ring.bytes > maxBytes) {
      ring.bytes -= ring.items.shift().size;
    }
  }
}

export function readTailEvents(namespace, worker, since = 0, limit = 128) {
  if (!ringSession) ringSession = crypto.randomUUID();
  const ring = namespace && worker ? rings.get(JSON.stringify([namespace, worker])) : null;
  const items = ring ? ring.items : globalItems;
  const events = [];
  let bytes = 0;
  for (let i = ring ? 0 : globalHead; i < items.length; i++) {
    const item = items[i];
    if (item.seq <= since) continue;
    if (events.length >= Math.min(limit, 128) || bytes + item.size > 512 * 1024) break;
    bytes += item.size;
    events.push({ seq: item.seq, namespace: item.namespace, worker: item.worker, event: eventRecords([item.raw], item.props)[0] });
  }
  return { session: ringSession, events };
}

export async function tailQuery(req, env) {
  if (!await authorized(req, env.LOG_TOKEN, "log")) return new Response("unauthorized", { status: 401 });
  const url = new URL(req.url);
  const ns = url.searchParams.get("ns"), worker = url.searchParams.get("worker");
  const since = Number(url.searchParams.get("since") || "0");
  if (!!ns !== !!worker || !Number.isSafeInteger(since) || since < 0) return new Response("bad query", { status: 400 });
  return new Response(JSON.stringify(readTailEvents(ns, worker, since)), { headers: { "content-type": "application/json", "cache-control": "no-store" } });
}

function exportVarint(value) {
  let v = BigInt(value);
  const out = [];
  while (v > 127n) { out.push(Number(v & 127n) | 128); v >>= 7n; }
  out.push(Number(v));
  return out;
}
function exportTag(field, wire) { return exportVarint((field << 3) | wire); }
function exportBytes(field, bytes) { return [...exportTag(field, 2), ...exportVarint(bytes.length), ...bytes]; }
function exportString(field, value) { return exportBytes(field, new TextEncoder().encode(String(value))); }
function exportFixed64(field, value) {
  const bytes = new Uint8Array(8);
  new DataView(bytes.buffer).setBigUint64(0, BigInt(value), true);
  return [...exportTag(field, 1), ...bytes];
}
function exportInt(field, value) { return [...exportTag(field, 0), ...exportVarint(value)]; }
function exportAny(value) { return exportString(1, value); }
function exportKeyValue(key, value) { return [...exportString(1, key), ...exportBytes(2, exportAny(value))]; }
function exportHex(id) {
  const out = new Uint8Array(id.length / 2);
  for (let i = 0; i < out.length; i++) out[i] = parseInt(id.slice(i * 2, i * 2 + 2), 16);
  return out;
}
function exportSeverity(level) {
  switch (String(level || "").toLowerCase()) {
    case "error": return [17, "ERROR"];
    case "warn": case "warning": return [13, "WARN"];
    case "debug": return [5, "DEBUG"];
    default: return [9, "INFO"];
  }
}
function exportBody(message) {
  if (typeof message === "string") return message;
  const args = Array.isArray(message) ? message : [message];
  return args.map((arg) => {
    if (typeof arg === "string") return arg;
    try { return JSON.stringify(arg); } catch (_err) { return String(arg); }
  }).join(" ");
}
function exportLogRecords(props, records) {
  const out = [];
  for (const raw of records) {
    const event = eventRecords([raw], props)[0];
    if (!event) continue;
    const attrs = [
      ["cellhive.namespace", props.namespace || ""],
      ["cellhive.worker", props.worker || ""],
      ["cellhive.version", String(props.version || 0)],
      ["cellhive.event_type", event.event_type || ""],
      ["cellhive.outcome", event.outcome || ""],
      ["cellhive.request_id", event.requestId || ""],
    ];
    const m = /^[0-9a-fA-F]{2}-([0-9a-fA-F]{32})-([0-9a-fA-F]{16})-/.exec(event.traceparent || "");
    for (const line of event.logs || []) {
      const [severityNumber, severityText] = exportSeverity(line.level);
      const time = Number(line.timestamp || event.eventTimestamp || Date.now());
      const record = [
        ...exportFixed64(1, BigInt(time) * 1000000n),
        ...exportFixed64(11, BigInt(time) * 1000000n),
        ...exportInt(2, severityNumber),
        ...exportString(3, severityText),
        ...exportBytes(5, exportAny(exportBody(line.message))),
        ...attrs.flatMap(([k, v]) => exportBytes(6, exportKeyValue(k, v))),
      ];
      if (m) record.push(...exportBytes(9, exportHex(m[1])), ...exportBytes(10, exportHex(m[2])));
      out.push(record);
    }
    for (const error of event.exceptions || []) {
      const time = Number(error.timestamp || event.eventTimestamp || Date.now());
      out.push([
        ...exportFixed64(1, BigInt(time) * 1000000n),
        ...exportFixed64(11, BigInt(time) * 1000000n),
        ...exportInt(2, 17), ...exportString(3, "ERROR"),
        ...exportBytes(5, exportAny(String(error.message || error.name || "exception"))),
        ...attrs.flatMap(([k, v]) => exportBytes(6, exportKeyValue(k, v))),
      ]);
    }
  }
  return out;
}
async function exportTailRecords(props, records) {
  const base = String(props.otlpLogsURL || "").trim();
  if (!base || !records.length) return;
  const recordsOut = exportLogRecords(props, records);
  if (!recordsOut.length) return;
  let extra = {};
  try { extra = JSON.parse(props.otlpLogsHeaders || "{}"); } catch (_err) { extra = {}; }
  const headers = new Headers({ "content-type": "application/x-protobuf" });
  for (const [key, value] of Object.entries(extra)) headers.set(key, String(value));
  const resourceLogs = [
    ...exportBytes(1, exportKeyValue("service.name", props.otlpLogsService || "cellhive-runtime")),
    ...exportBytes(2, [
      ...exportBytes(1, exportString(1, "cellhive.tail")),
      ...recordsOut.flatMap((record) => exportBytes(2, record)),
    ]),
  ];
  const body = new Uint8Array(exportBytes(1, resourceLogs));
  const url = base.replace(/\/$/, "").replace(/\/v1\/(traces|logs)$/, "") + "/v1/logs";
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(new Error("tail log timeout")), 3000);
  try {
    const response = await fetch(url, { method: "POST", headers, body, signal: controller.signal });
    if (!response.ok) console.error(JSON.stringify({ event: "tail_log_otlp_failed", status: response.status }));
  } catch (err) {
    console.error(JSON.stringify({ event: "tail_log_otlp_failed", error: String(err) }));
  } finally {
    clearTimeout(timer);
  }
}

// TenantTail executes in the trusted host isolate. Its identity and transport
// come from host-owned props/env; neither is visible to the loaded tenant.
export class TenantTail extends WorkerEntrypoint {
  tail(events) {
    const props = this.ctx.props || {};
    if (!props.namespace || !props.worker) return;
    const kept = (events || []).filter((item) => !platformLifecycleEvent(item));
    if (!kept.length) return;
    storeRecords(props.namespace, props.worker, kept, props);
    const exported = exportTailRecords(props, kept);
    if (this.ctx && typeof this.ctx.waitUntil === "function") this.ctx.waitUntil(exported);
  }
}
