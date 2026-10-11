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

function eventEntries(events) {
  const entries = [];
  for (const event of events || []) {
    const headers = event && event.event && event.event.request && event.event.request.headers || {};
    const traceparent = headers.traceparent || headers.Traceparent || "";
    const requestId = headers["x-request-id"] || headers["X-Request-Id"] || headers["cf-ray"] || "";
    for (const line of event.logs || []) {
      entries.push({
        level: line.level || "log",
        message: (line.message || []).map(formatValue).join(" "),
        request_id: requestId,
        traceparent,
        at_ms: line.timestamp || event.eventTimestamp || Date.now(),
      });
    }
    for (const exception of event.exceptions || []) {
      const name = exception.name ? `${exception.name}: ` : "";
      entries.push({
        level: "error",
        message: name + (exception.message || "Uncaught exception"),
        request_id: requestId,
        traceparent,
        at_ms: exception.timestamp || event.eventTimestamp || Date.now(),
      });
    }
  }
  return entries;
}

// TenantTail executes in the trusted host isolate. Its identity and transport
// come from host-owned props/env; neither is visible to the loaded tenant.
export class TenantTail extends WorkerEntrypoint {
  async tail(events) {
    const entries = eventEntries(events);
    if (entries.length === 0) return;
    const props = this.ctx.props || {};
    if (!props.namespace || !props.worker || !this.env.CELL_URL || !this.env.LOG_TOKEN) return;
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
        body: JSON.stringify(entries),
      });
    } catch (_err) {
      // Logging is best-effort and must never affect the producer invocation.
    }
  }
}
