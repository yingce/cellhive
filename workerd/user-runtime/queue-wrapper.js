// Platform wrapper for loaded tenant workers (user-runtime).
//
// workerLoader only exposes the default export's fetch over RPC; non-fetch
// handlers (queue/scheduled) are reached by wrapping the tenant in a
// WorkerEntrypoint class the platform can call. The tenant never sees platform
// values: it receives only binding facades built from a cloneable spec.
import { WorkerEntrypoint, env as __env } from "cloudflare:workers";
import * as tenantMod from "tenant.js";

// Trace context travels with the request/event (W3C traceparent): the loader
// adds it to inbound requests, cell-agent adds it to dispatch payloads, and the
// facades read it back to tag their own calls (ADR-146). A global keeps the
// wrapper and the facades (same isolate) in sync without cross-module wiring.
function traceOf(x) {
  if (!x) return undefined;
  if (x.headers && typeof x.headers.get === "function") return x.headers.get("traceparent");
  return x.traceparent;
}
function setTraceContext(tp) {
  if (tp) globalThis.__cellhiveTraceparent = tp;
  else delete globalThis.__cellhiveTraceparent;
}

const tenant = tenantMod.default;
import { buildBindings, wrapR2Metadata, makeDOFromStub } from "facades.js";

// Patch the importable env with local facades for bindings whose CF API cannot
// cross RPC (DO namespaces, Workflows) — same mechanism as the do-runtime
// bindings-wrapper (ADR-090). Migrated bindings are already entrypoint stubs.
try {

  // DO namespaces arrive as platform-side entrypoint stubs; rebuild the
  // CF-shaped facade on top of them (fetch(request)/rpcObject; ADR-184).
  if (__cellhivePlatform.doBindings.length > 0) {
    for (const name of __cellhivePlatform.doBindings) {
      if (__env[name]) {
        Object.defineProperty(__env, name, {
          value: makeDOFromStub(__env[name]),
          writable: true, configurable: true, enumerable: true,
        });
      }
    }
  }
  // R2ObjectBody.writeHttpMetadata must run in the tenant isolate to mutate the
  // caller's Headers (RPC serializes arguments by value): wrap each R2 binding.
  if (__cellhivePlatform.r2Bindings.length > 0) {
    for (const name of __cellhivePlatform.r2Bindings) {
      if (__env[name]) {
        Object.defineProperty(__env, name, { value: wrapR2Metadata(__env[name]), writable: true, configurable: true, enumerable: true });
      }
    }
  }
} catch (e) {
  /* leave unpatched */
}

export class CellHiveHost extends WorkerEntrypoint {
  #bindings() {
    // env is now complete: vars + entrypoint stubs + patched local facades.
    return this.env;
  }

  async fetch(req) {
    if (typeof tenant.fetch !== "function") return new Response("worker has no fetch handler", { status: 501 });
    setTraceContext(traceOf(req));
    try {
      return await tenant.fetch(req, this.#bindings(), this.ctx);
    } finally {
      setTraceContext(undefined);
    }
  }

  async handleFetch(req) {
    return this.fetch(req);
  }

  async handleQueue(payload) {
    if (typeof tenant.queue !== "function") throw new Error("worker has no queue() handler");
    // CF MessageBatch parity (ADR-154): the batch is array-like (length/index,
    // what the platform has always passed) and also exposes .messages/.queue/
    // ackAll()/retryAll(). Built here because RPC structured clone drops
    // non-index properties and functions.
    const batch = Array.isArray(payload) ? payload : (payload && payload.messages) || [];
    const queueName = (payload && payload.queue) || "";
    const traceparent = (payload && payload.traceparent) || undefined;
    // Per-message CF semantics (ADR-155): message.ack() / message.retry({delaySeconds})
    // and batch.ackAll() / batch.retryAll({delaySeconds}). Messages that are
    // neither acked nor retried are implicitly acked when the handler returns.
    const outcomes = { ack: [], retry: [] };
    const retryIdx = new Map();
    const markRetry = (id, delaySeconds) => {
      const spec = { id, delay_seconds: Math.max(0, delaySeconds | 0) };
      const i = retryIdx.get(id);
      if (i === undefined) {
        retryIdx.set(id, outcomes.retry.length);
        outcomes.retry.push(spec);
      } else {
        outcomes.retry[i] = spec;
      }
    };
    const markAck = (id) => {
      if (!retryIdx.has(id)) outcomes.ack.push(id);
    };
    for (const m of batch) {
      if (!m || typeof m !== "object") continue;
      m.ack = () => markAck(m.id);
      m.retry = (opts) => markRetry(m.id, opts && opts.delaySeconds);
    }
    Object.defineProperties(batch, {
      messages: { value: batch, enumerable: false },
      queue: { value: queueName, enumerable: false },
      traceparent: { value: traceparent, enumerable: false },
    });
    batch.ackAll = () => { for (const m of batch) if (m && m.id) markAck(m.id); };
    batch.retryAll = (opts) => { for (const m of batch) if (m && m.id) markRetry(m.id, opts && opts.delaySeconds); };
    setTraceContext(traceparent || traceOf(batch));
    try {
      const result = await tenant.queue(batch, this.#bindings(), this.ctx);
      return { result, ack: outcomes.ack, retry: outcomes.retry };
    } finally {
      setTraceContext(undefined);
    }
  }

  async handleScheduled(event) {
    if (typeof tenant.scheduled !== "function") throw new Error("worker has no scheduled() handler");
    setTraceContext(traceOf(event));
    try {
      return await tenant.scheduled(event, this.#bindings(), this.ctx);
    } finally {
      setTraceContext(undefined);
    }
  }

  // callMethod invokes a named tenant entrypoint method (service bindings, RPC).
  // A class entrypoint is constructed with (ctx, env); a plain object is used
  // as-is. Empty entrypoint targets the default export.
  async callMethod(entrypoint, method, args, traceparent) {
    const exp = entrypoint ? tenantMod[entrypoint] : tenantMod.default;
    if (exp === undefined) throw new Error("no entrypoint " + entrypoint);
    let inst = exp;
    if (typeof exp === "function") {
      inst = new exp(this.ctx, this.#bindings());
    }
    if (typeof inst[method] !== "function") throw new Error("entrypoint has no method " + method);
    setTraceContext(traceparent);
    try {
      return await inst[method](...(args || []));
    } finally {
      setTraceContext(undefined);
    }
  }
}

export default {
  async fetch() {
    return new Response("cellhive user-runtime wrapper");
  },
};
