import { WorkerEntrypoint } from "cloudflare:workers";

// A stuck control-plane read must not pin workerd's shared HTTP client's old
// resolved address indefinitely. The budget includes response body parsing.
export async function readLogCapture(env) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 2000);
  try {
    const response = await fetch(String(env.CELL_URL).replace(/\/$/, "") + "/v1/internal/log-capture", {
      headers: { "x-cellhive-internal-token": env.CELL_TOKEN },
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
    if (method === "handleQueue" && typeof value.queue === "string" && value.queue &&
        Number.isSafeInteger(value.batchSize) && value.batchSize >= 0) {
      return { line, event: { rpcMethod: method, queue: value.queue, batchSize: value.batchSize } };
    }
    if (method === "handleScheduled" && typeof value.cron === "string" && value.cron &&
        Number.isSafeInteger(value.scheduledTime) && value.scheduledTime > 0) {
      return { line, event: { rpcMethod: method, cron: value.cron, scheduledTime: value.scheduledTime } };
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
  if (typeof info.cron === "string") return { cron: info.cron, ...(info.scheduledTime ? { scheduledTime: info.scheduledTime } : {}), ...(info.rpcMethod ? { rpcMethod: info.rpcMethod } : {}) };
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
    const requestId = headers["x-request-id"] || headers["X-Request-Id"] || headers["cf-ray"] || "";
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

export function tailQuery(req, env) {
  const expected = env.LOG_TOKEN || "";
  const token = req.headers.get("x-cellhive-internal-token") || "";
  if (!expected || token.length !== expected.length) return new Response("unauthorized", { status: 401 });
  let diff = 0;
  for (let i = 0; i < token.length; i++) diff |= token.charCodeAt(i) ^ expected.charCodeAt(i);
  if (diff) return new Response("unauthorized", { status: 401 });
  const url = new URL(req.url);
  const ns = url.searchParams.get("ns"), worker = url.searchParams.get("worker");
  const since = Number(url.searchParams.get("since") || "0");
  if (!!ns !== !!worker || !Number.isSafeInteger(since) || since < 0) return new Response("bad query", { status: 400 });
  return new Response(JSON.stringify(readTailEvents(ns, worker, since)), { headers: { "content-type": "application/json", "cache-control": "no-store" } });
}

// TenantTail executes in the trusted host isolate. Its identity and transport
// come from host-owned props/env; neither is visible to the loaded tenant.
export class TenantTail extends WorkerEntrypoint {
  tail(events) {
    const props = this.ctx.props || {};
    if (!props.namespace || !props.worker) return;
    const kept = (events || []).filter((item) => !platformLifecycleEvent(item));
    if (kept.length) storeRecords(props.namespace, props.worker, kept, props);
  }
}
