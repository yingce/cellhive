// Platform wrapper for loaded tenant workers (user-runtime).
//
// workerLoader only exposes the default export's fetch over RPC; non-fetch
// handlers (queue/scheduled) are reached by wrapping the tenant in a
// WorkerEntrypoint class the platform can call. The tenant never sees platform
// values: it receives only binding facades built from a cloneable spec.
import "__cellhive-bindings.js";
import { WorkerEntrypoint, env as __env } from "cloudflare:workers";
import { loggedInvocation, dispatchDebug } from "__cellhive-invocation.js";
import * as tenantMod from "tenant.js";

// Trace context is asynchronous-call-local; binding stubs carry it over RPC.
function traceOf(x) {
  return x?.headers?.get ? x.headers.get("traceparent") : x?.traceparent;
}

// Non-fetch entrypoints (queue/scheduled/service RPC) do not go through
// CellHiveManagedFetch. Without this hold, the loader can retire the isolate
// while one of those handlers is still running and abort the invocation.
// Static-generation wrappers have no __cellhive-state module; these helpers
// are no-ops there.
function lifecycleHold() {
  const life = globalThis.__cellhiveLife;
  return life && typeof life.hold === "function" ? life.hold() : () => {};
}
function lifecycleContext(ctx) {
  const life = globalThis.__cellhiveLife;
  return life && typeof life.context === "function" ? life.context(ctx) : ctx;
}
function lifecycleResponse(res) {
  const life = globalThis.__cellhiveLife;
  // callMethod may return any structured-clone value (including null); only
  // fetch-shaped Responses have a body lifetime that needs tracking.
  return life && typeof life.response === "function" && res instanceof Response ? life.response(res) : res;
}

const tenant = tenantMod.default;
import { prepareBindingEnv, runWithTrace } from "facades.js";
import { runInvocation } from "__cellhive-quota.js";

// Patch the importable env with local facades for binding API portions that
// cannot cross RPC synchronously or must mutate caller-isolate state.
prepareBindingEnv(__env, __cellhivePlatform);

export class CellHiveHost extends WorkerEntrypoint {
  #bindings() {
    // env is now complete: vars + entrypoint stubs + patched local facades.
    return this.env;
  }

  async fetch(req) {
    if (typeof tenant.fetch !== "function") return new Response("worker has no fetch handler", { status: 501 });
    return runWithTrace({ traceparent: traceOf(req), requestId: req.headers.get("x-request-id") },
      () => runInvocation(() => tenant.fetch(req, withR2Metadata(this.#bindings()), this.ctx)));
  }

  async handleFetch(req) {
    const release=lifecycleHold();
    try {
      lifecycleContext(this.ctx);
      return lifecycleResponse(await this.fetch(req));
    } finally { release(); }
  }

  async handleQueue(payload) {
    const release=lifecycleHold();
    try {
      const ctx=lifecycleContext(this.ctx);
      // The native Trace for this RPC exposes only rpcMethod. Emit the trusted
      // dispatch payload before tenant code runs; TenantTail consumes this one
      // record from the same invocation and removes it from public logs.
      if (__cellhiveTailKey) dispatchDebug("__cellhive_dispatch_v1__", __cellhiveTailKey, {
        queue: typeof payload?.queue === "string" ? payload.queue : "",
        batchSize: Array.isArray(payload?.messages) ? payload.messages.length : 0,
        started_ms: Date.now(),
        request_id: typeof payload?.request_id === "string" ? payload.request_id : "",
      });
      return await loggedInvocation(() => this.handleQueueBody(payload, ctx), ctx);
    } finally { release(); }
  }

  async handleQueueBody(payload, ctx=this.ctx) {
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
    return runWithTrace({ traceparent: traceparent || traceOf(batch), requestId: payload?.request_id }, async () => {
      const result = await runInvocation(() => tenant.queue(batch, withR2Metadata(this.#bindings()), ctx));
      return { result, ack: outcomes.ack, retry: outcomes.retry };
    });
  }

  async handleScheduled(event) {
    const release=lifecycleHold();
    try {
      const ctx=lifecycleContext(this.ctx);
      if (__cellhiveTailKey) dispatchDebug("__cellhive_dispatch_v1__", __cellhiveTailKey, {
        cron: typeof event?.cron === "string" ? event.cron : "",
        scheduledTime: Number(event?.scheduledTime) || 0,
        started_ms: Date.now(),
        request_id: typeof event?.request_id === "string" ? event.request_id : "",
      });
      return await loggedInvocation(() => this.handleScheduledBody(event, ctx), ctx);
    } finally { release(); }
  }

  async handleScheduledBody(event, ctx=this.ctx) {
    if (typeof tenant.scheduled !== "function") throw new Error("worker has no scheduled() handler");
    let noRetry = false;
    const controller = { scheduledTime: event.scheduledTime, cron: event.cron, noRetry() { noRetry = true; } };
    return runWithTrace({ traceparent: traceOf(event), requestId: event.request_id }, async () => {
      try {
        const result = await runInvocation(() => tenant.scheduled(controller, withR2Metadata(this.#bindings()), ctx));
        return { ok: true, no_retry: noRetry, result: result === undefined ? null : result };
      } catch (e) {
        if (noRetry) return { ok: false, no_retry: true, error: String(e), error_stack: typeof e?.stack === "string" ? e.stack.slice(0,4096) : "" };
        throw e;
      }
    });
  }

  // callMethod invokes a named tenant entrypoint method (service bindings, RPC).
  // A class entrypoint is constructed with (ctx, env); a plain object is used
  // as-is. Empty entrypoint targets the default export.
  async callMethod(entrypoint, method, args, traceparent) {
    const release=lifecycleHold();
    try {
      const exp = entrypoint ? tenantMod[entrypoint] : tenantMod.default;
      if (exp === undefined) throw new Error("no entrypoint " + entrypoint);
      let inst = exp;
      if (typeof exp === "function") {
        inst = new exp(lifecycleContext(this.ctx), withR2Metadata(this.#bindings()));
      }
      if (typeof inst[method] !== "function") throw new Error("entrypoint has no method " + method);
      return await runWithTrace({ traceparent }, async () =>
        lifecycleResponse(await runInvocation(() => inst[method](...(args || [])))));
    } finally { release(); }
  }
}

export default {
  async fetch() {
    return new Response("cellhive user-runtime wrapper");
  },
};


// Static env objects can expose non-configurable binding properties, which makes
// the module-load defineProperty patches silently fail. Re-apply the R2 metadata
// facade through a per-call env Proxy so writeHttpMetadata stays tenant-local.
export function withR2Metadata(env) { return prepareBindingEnv(env, __cellhivePlatform); }

// Static-generation entry: same facade patching (done at module load) and the
// same tenant fetch path as CellHiveHost.fetch, without constructing a
// WorkerEntrypoint per request. Service-entrypoint addressing is dynamic-only;
// the static ingress always calls this plain fetch.
export async function handleFetch(req, env, ctx) {
  if (typeof tenant.fetch !== "function") return new Response("worker has no fetch handler", { status: 501 });
  return runWithTrace({ traceparent: traceOf(req), requestId: req.headers.get("x-request-id") },
    () => runInvocation(() => tenant.fetch(req, withR2Metadata(env), ctx)));
}
