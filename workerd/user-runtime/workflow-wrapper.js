// Platform wrapper that runs a tenant WorkflowEntrypoint (P2 workflows).
//
// workerLoader only exposes `fetch`; the platform imports all tenant exports and
// calls the named WorkflowEntrypoint class's run(event, step). `step` calls back
// to cell-agent to memoize step results and schedule sleeps, so a re-dispatched
// attempt resumes from the last durable step.
import { WorkerEntrypoint, env as __env } from "cloudflare:workers";
import * as tenant from "tenant.js";
import { NonRetryableError, WorkflowTimeoutError } from "cellhive-workflow.js";
import { buildBindings, makeKVFromStub, makeD1FromStub, makeDOFromStub, makeQueueFromStub, wrapR2Metadata } from "facades.js";
import { runInvocation } from "__cellhive-quota.js";

// Rebuild synchronous tenant-isolate APIs over the props-bound capabilities.

try {

  for (const name of __cellhivePlatform.kvBindings) {
    if (__env[name]) {
      Object.defineProperty(__env, name, { value: makeKVFromStub(__env[name]), writable: true, configurable: true, enumerable: true });
    }
  }
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
  for (const name of __cellhivePlatform.d1Bindings) {
    if (__env[name]) {
      Object.defineProperty(__env, name, { value: makeD1FromStub(__env[name]), writable: true, configurable: true, enumerable: true });
    }
  }
  for (const name of __cellhivePlatform.queueBindings) {
    if (__env[name]) {
      Object.defineProperty(__env, name, { value: makeQueueFromStub(__env[name]), writable: true, configurable: true, enumerable: true });
    }
  }
} catch (e) {
  /* leave unpatched */
}

class SleepSignal {}
class StopSignal {}

function b64encode(s) {
  return btoa(unescape(encodeURIComponent(s)));
}
function b64decode(s) {
  try {
    return decodeURIComponent(escape(atob(s)));
  } catch (e) {
    return "";
  }
}

// WorkflowEntrypoint receives env at construction time. Re-apply the R2
// metadata facade so this.env.BUCKET.writeHttpMetadata stays tenant-local.
const __r2EnvCache = new WeakMap();
function withR2Metadata(env) {
  if (!env || typeof env !== "object" || !__cellhivePlatform?.r2Bindings?.length) return env;
  let cached = __r2EnvCache.get(env);
  if (cached) return cached;
  const wrapped = new Map();
  const proxy = new Proxy(env, {
    get(target, prop, receiver) {
      const value = Reflect.get(target, prop, receiver);
      if (typeof prop !== "string" || value === undefined || value === null) return value;
      if (__cellhivePlatform.r2Bindings.includes(prop)) {
        if (!wrapped.has(prop)) wrapped.set(prop, wrapR2Metadata(value));
        return wrapped.get(prop);
      }
      return value;
    },
  });
  __r2EnvCache.set(env, proxy);
  return proxy;
}

export class CellHiveWorkflow extends WorkerEntrypoint {
  async handleRun(className, eventJson, workflowBridge) {
    const cls = tenant[className];
    if (typeof cls !== "function") {
      throw new Error("no WorkflowEntrypoint class " + className + " exported by the bundle");
    }
    const env = this.env;
    const bindings = withR2Metadata(env);
    const step = makeStep(workflowBridge, eventJson.instanceId);
    const inst = new cls(this.ctx, bindings);
    globalThis.__cellhiveTraceparent = eventJson.traceparent || "";
    globalThis.__cellhiveRequestId = eventJson.request_id || "";
    try {
      const out = await runInvocation(() => inst.run(eventJson.event, step));
      await finish(workflowBridge, eventJson.instanceId, out);
      return { status: "complete" };
    } catch (e) {
      if (e instanceof SleepSignal) return { status: "sleeping" };
      if (e instanceof StopSignal) return { status: "stopped" };
      await finishError(workflowBridge, eventJson.instanceId, String(e && e.message ? e.message : e));
      return { status: "errored", error: String(e && e.message ? e.message : e) };
    } finally {
      delete globalThis.__cellhiveTraceparent;
      delete globalThis.__cellhiveRequestId;
    }
  }
}

// call routes one workflow step callback to cell-agent through the
// trusted WorkflowBridgeTarget RpcTarget passed by the dispatcher. The target
// owns the :7001 transport and the internal token, and it accepts a fixed op name
// (never a raw path) with the namespace forced to this worker — so the tenant
// cannot turn it into a generic relay. Returns a Response-shaped view for the
// step helpers below.
function call(bridge, op, params, init) {
  if (!bridge || typeof bridge.workflowStep !== "function") {
    return Promise.reject(new Error("workflow: step transport unavailable"));
  }
  const body = init && init.body !== undefined ? init.body : null;
  return (async () => {
    const res = await bridge.workflowStep(op, params || {}, body);
    return {
      ok: res.status >= 200 && res.status < 300,
      status: res.status,
      text: async () => res.body,
      json: async () => JSON.parse(res.body),
    };
  })();
}

// retryDelayMs computes the backoff for a step.do retries config. CF's `delay`
// is in seconds; backoff is "constant" | "linear" | "exponential".
function retryDelayMs(retries, attempt) {
  const sec = typeof retries.delay === "number" ? retries.delay : 0;
  const base = sec * 1000;
  switch (retries.backoff) {
    case "linear": return base * (attempt + 1);
    case "exponential": return base * Math.pow(2, attempt);
    default: return base;
  }
}

function parseDurationMs(value) {
  if (typeof value === "number" && Number.isFinite(value)) return Math.max(0, value);
  if (value instanceof Date) return Math.max(0, value.getTime() - Date.now());
  if (typeof value !== "string") return 0;
  const m = value.trim().match(/^(-?\d+(?:\.\d+)?)\s*(ms|milliseconds?|s|seconds?|m|minutes?|h|hours?|d|days?)$/i);
  if (!m) return 0;
  const n = Number(m[1]); const unit = m[2].toLowerCase();
  const factor = unit.startsWith("ms") ? 1 : unit.startsWith("s") ? 1000 : unit.startsWith("m") ? 60000 : unit.startsWith("h") ? 3600000 : 86400000;
  return Math.max(0, n * factor);
}

function makeStep(bridge, id) {
  // Identity (ns/workflow/id/run) is bound into the dispatcher bridge's props.
  const base = () => ({});
  const named = (name) => ({ name });

  async function getAttempt(name) {
    const r = await call(bridge, "attempt.get", named(name));
    if (!r.ok) return 0;
    return (await r.json()).attempts || 0;
  }
  async function setAttempt(name, attempts, message) {
    await call(bridge, "attempt.put", Object.assign(named(name), { attempts, error: message || "" }));
  }
  async function clearAttempt(name) {
    await call(bridge, "attempt.delete", named(name));
  }

  // Cooperative pause/terminate: at each step boundary, a fenced token (409) or a
  // paused/terminated status stops the run instead of advancing further.
  async function checkState() {
    const r = await call(bridge, "state.get", base());
    if (r.status === 409) throw new StopSignal();
    if (!r.ok) return;
    const st = (await r.json()).status;
    if (st === "paused" || st === "terminated" || st === "complete") throw new StopSignal();
  }
  async function getStep(name) {
    const r = await call(bridge, "step.get", named(name));
    if (!r.ok) throw new Error("step get " + r.status);
    return await r.json();
  }
  async function putStep(name, value) {
    const r = await call(bridge, "step.put", named(name), {
      body: b64encode(JSON.stringify(value === undefined ? null : value)),
    });
    if (!r.ok) throw new Error("step put " + r.status);
  }

  return {
    async do(name, configOrFn, maybeFn) {
      const fn = typeof configOrFn === "function" ? configOrFn : maybeFn;
      const config = typeof configOrFn === "function" ? {} : (configOrFn || {});
      if (typeof fn !== "function") throw new Error("step.do(" + name + ") needs a function");
      await checkState();
      const got = await getStep(name);
      if (got.found) return got.result ? JSON.parse(b64decode(got.result)) : undefined;
      const retries = config.retries || {};
      const limit = typeof retries.limit === "number" ? retries.limit : 0;
      // Durable retries: the attempt count lives in the cell, so a crash or a
      // re-dispatch resumes the same attempt/backoff instead of restarting.
      const attempts = await getAttempt(name);
      try {
        const timeout = parseDurationMs(config.timeout);
        let result;
        if (timeout > 0) {
          let timer;
          try {
            result = await Promise.race([Promise.resolve().then(fn), new Promise((_, reject) => { timer = setTimeout(() => reject(new WorkflowTimeoutError("workflow step timed out")), timeout); })]);
          } finally { if (timer) clearTimeout(timer); }
        } else result = await fn();
        await putStep(name, result);
        await clearAttempt(name);
        return result;
      } catch (e) {
        const next = attempts + 1;
        await setAttempt(name, next, String(e && e.message ? e.message : e));
        if (e instanceof NonRetryableError) throw e;
        if (next > limit) throw e;
        // Park; a timer re-dispatches after the backoff delay.
        const wake = Date.now() + retryDelayMs(retries, attempts);
        await call(bridge, "sleep", Object.assign(named(name), { wake_at_ms: wake }));
        throw new SleepSignal();
      }
    },
    async sleep(name, duration) {
      await checkState();
      const got = await getStep(name);
      if (got.found) {
        const wake = JSON.parse(b64decode(got.result)).wake_at_ms;
        if (Date.now() < wake) throw new SleepSignal();
        return;
      }
      const wake = Date.now() + parseDurationMs(duration);
      await putStep(name, { wake_at_ms: wake });
      const r = await call(bridge, "sleep", Object.assign(named(name), { wake_at_ms: wake }));
      if (!r.ok) throw new Error("step sleep " + r.status);
      throw new SleepSignal();
    },
    // waitForEvent blocks until an event of the given type is delivered (via
    // sendEvent) or the timeout elapses (returns undefined on timeout).
    async waitForEvent(name, options = {}) {
      await checkState();
      const got = await getStep(name);
      if (got.found) return got.result ? JSON.parse(b64decode(got.result)) : undefined;
      const type = options.type || "";
      const consume = async () => {
        const r = await call(bridge, "event.consume", Object.assign(named(name), { type }));
        if (!r.ok) throw new Error("waitForEvent consume " + r.status);
        return await r.json();
      };
      const found = await consume();
      if (found.found) {
        const val = found.event ? JSON.parse(b64decode(found.event)) : undefined;
        await putStep(name, val);
        await call(bridge, "wait.delete", named(name));
        return val;
      }
      const timeoutMs = typeof options.timeout === "number" ? options.timeout * 1000 : parseDurationMs(options.timeout);
      const deadline = timeoutMs > 0 ? Date.now() + timeoutMs : 0;
      // On resume, honor an elapsed timeout.
      const wr = await call(bridge, "wait.get", named(name));
      if (wr.ok) {
        const wj = await wr.json();
        if (wj.found && wj.deadline_ms > 0 && Date.now() >= wj.deadline_ms) {
          await putStep(name, null);
          await call(bridge, "wait.delete", named(name));
          throw new WorkflowTimeoutError();
        }
      }
      const parked = await call(bridge, "wait.put", Object.assign(named(name), { deadline_ms: deadline, type }));
      if (!parked.ok) throw new Error("waitForEvent park " + parked.status);
      throw new SleepSignal();
    },
    async sleepUntil(name, timestamp) {
      const at = timestamp instanceof Date ? timestamp.getTime() : (typeof timestamp === "number" ? timestamp : Date.parse(timestamp));
      return this.sleep(name, Number.isFinite(at) ? Math.max(0, at - Date.now()) : 0);
    },
  };
}

async function finish(bridge, id, output) {
  await call(bridge, "finish", {}, {
    body: b64encode(JSON.stringify(output === undefined ? null : output)),
  });
}

async function finishError(bridge, id, message) {
  await call(bridge, "finish.error", { error: message });
}

export default {
  async fetch() {
    return new Response("cellhive workflow wrapper");
  },
};
