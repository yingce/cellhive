import { WorkerEntrypoint } from "cloudflare:workers";

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

function eventRecords(events, props) {
  return (events || []).map((item) => {
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

// TenantTail executes in the trusted host isolate. Its identity and transport
// come from host-owned props/env; neither is visible to the loaded tenant.
export class TenantTail extends WorkerEntrypoint {
  async tail(events) {
    const props = this.ctx.props || {};
    if (!props.namespace || !props.worker || !this.env.CELL_URL || !this.env.LOG_TOKEN) return;
    const records = eventRecords(events, props);
    if (records.length === 0) return;
    const url = new URL(this.env.CELL_URL.replace(/\/$/, "") + "/v1/internal/logs");
    url.searchParams.set("ns", props.namespace);
    url.searchParams.set("worker", props.worker);
    try {
      await this.env.PLATFORM.fetch(url, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "x-cellhive-internal-token": this.env.LOG_TOKEN,
        },
        body: JSON.stringify({ events: records }),
      });
    } catch (_err) {
      // Logging is best-effort and must never affect the producer invocation.
    }
  }
}
