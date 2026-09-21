// Platform wrapper that runs a tenant WorkflowEntrypoint (P2 workflows).
//
// workerLoader only exposes `fetch`; the platform imports all tenant exports and
// calls the named WorkflowEntrypoint class's run(event, step). `step` calls back
// to cell-agent to memoize step results and schedule sleeps, so a re-dispatched
// attempt resumes from the last durable step.
import "log-tail.js";
import { WorkerEntrypoint, env as __env } from "cloudflare:workers";
import * as tenant from "tenant.js";
import { NonRetryableError } from "cellhive-workflow.js";
import { buildBindings, makeDOFromStub } from "facades.js";

try {
  if (__env.CH_FACADE_SPEC) {
    // __cellhivePlatform is injected as a module-scope const in this module's
    // source (internal.js platformConsts) — never in the tenant env.
    const facades = buildBindings(
      { url: __cellhivePlatform.cellUrl, token: __cellhivePlatform.cellToken, fetcher: __env.PLATFORM },
      JSON.parse(__env.CH_FACADE_SPEC),
    );
    for (const [name, value] of Object.entries(facades)) {
      Object.defineProperty(__env, name, { value, writable: true, configurable: true, enumerable: true });
    }
  }
  if (__env.CH_DO_BINDINGS && __env.CH_DO_CONNECT) {
    for (const name of JSON.parse(__env.CH_DO_BINDINGS)) {
      if (__env[name]) {
        Object.defineProperty(__env, name, {
          value: makeDOFromStub(__env[name], __env.CH_DO_CONNECT),
          writable: true, configurable: true, enumerable: true,
        });
      }
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

export class CellHiveWorkflow extends WorkerEntrypoint {
  async handleRun(className, eventJson) {
    const cls = tenant[className];
    if (typeof cls !== "function") {
      throw new Error("no WorkflowEntrypoint class " + className + " exported by the bundle");
    }
    const env = this.env;
    const bindings = env;
    const step = makeStep(env, eventJson.instanceId);
    const inst = new cls(this.ctx, bindings);
    try {
      const out = await inst.run(eventJson.event, step);
      await finish(env, eventJson.instanceId, out);
      return { status: "complete" };
    } catch (e) {
      if (e instanceof SleepSignal) return { status: "sleeping" };
      if (e instanceof StopSignal) return { status: "stopped" };
      await finishError(env, eventJson.instanceId, String(e && e.message ? e.message : e));
      return { status: "errored", error: String(e && e.message ? e.message : e) };
    }
  }
}

// call routes one workflow step callback to cell-agent through the
// platform-side WorkflowSteps entrypoint stub (env.CH_WF_STEPS). The stub owns
// the :7001 transport and the internal token, and it accepts a fixed op name
// (never a raw path) with the namespace forced to this worker — so the tenant
// cannot turn it into a generic relay. Returns a Response-shaped view for the
// step helpers below.
function call(env, op, params, init) {
  const steps = env.CH_WF_STEPS;
  if (!steps || typeof steps.call !== "function") {
    return Promise.reject(new Error("workflow: step transport unavailable"));
  }
  const body = init && init.body !== undefined ? init.body : null;
  return (async () => {
    const res = await steps.call(op, params || {}, body);
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

function makeStep(env, id) {
  // Identity (ns/workflow/id/run) is bound into the CH_WF_STEPS stub's props.
  const base = () => ({});
  const named = (name) => ({ name });

  async function getAttempt(name) {
    const r = await call(env, "attempt.get", named(name));
    if (!r.ok) return 0;
    return (await r.json()).attempts || 0;
  }
  async function setAttempt(name, attempts, message) {
    await call(env, "attempt.put", Object.assign(named(name), { attempts, error: message || "" }));
  }
  async function clearAttempt(name) {
    await call(env, "attempt.delete", named(name));
  }

  // Cooperative pause/terminate: at each step boundary, a fenced token (409) or a
  // paused/terminated status stops the run instead of advancing further.
  async function checkState() {
    const r = await call(env, "state.get", base());
    if (r.status === 409) throw new StopSignal();
    if (!r.ok) return;
    const st = (await r.json()).status;
    if (st === "paused" || st === "terminated" || st === "complete") throw new StopSignal();
  }
  async function getStep(name) {
    const r = await call(env, "step.get", named(name));
    if (!r.ok) throw new Error("step get " + r.status);
    return await r.json();
  }
  async function putStep(name, value) {
    const r = await call(env, "step.put", named(name), {
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
        const result = await fn();
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
        await call(env, "sleep", Object.assign(named(name), { wake_at_ms: wake }));
        throw new SleepSignal();
      }
    },
    async sleep(name, durationMs) {
      await checkState();
      const got = await getStep(name);
      if (got.found) {
        const wake = JSON.parse(b64decode(got.result)).wake_at_ms;
        if (Date.now() < wake) throw new SleepSignal();
        return;
      }
      const wake = Date.now() + (typeof durationMs === "number" ? durationMs : 0);
      await putStep(name, { wake_at_ms: wake });
      const r = await call(env, "sleep", Object.assign(named(name), { wake_at_ms: wake }));
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
        const r = await call(env, "event.consume", Object.assign(named(name), { type }));
        if (!r.ok) throw new Error("waitForEvent consume " + r.status);
        return await r.json();
      };
      const found = await consume();
      if (found.found) {
        const val = found.event ? JSON.parse(b64decode(found.event)) : undefined;
        await putStep(name, val);
        await call(env, "wait.delete", named(name));
        return val;
      }
      const timeoutMs = typeof options.timeout === "number" ? options.timeout * 1000 : 0;
      const deadline = timeoutMs > 0 ? Date.now() + timeoutMs : 0;
      // On resume, honor an elapsed timeout.
      const wr = await call(env, "wait.get", named(name));
      if (wr.ok) {
        const wj = await wr.json();
        if (wj.found && wj.deadline_ms > 0 && Date.now() >= wj.deadline_ms) {
          await putStep(name, null);
          await call(env, "wait.delete", named(name));
          return undefined;
        }
      }
      await call(env, "wait.put", Object.assign(named(name), { deadline_ms: deadline }));
      if (deadline > 0) {
        const sr = await call(env, "sleep", Object.assign(named(name), { wake_at_ms: deadline }));
        if (!sr.ok) throw new Error("waitForEvent sleep " + sr.status);
      }
      throw new SleepSignal();
    },
    async sleepUntil(name, timestampMs) {
      const d = typeof timestampMs === "number" ? timestampMs - Date.now() : 0;
      return this.sleep(name, d);
    },
  };
}

async function finish(env, id, output) {
  await call(env, "finish", {}, {
    body: b64encode(JSON.stringify(output === undefined ? null : output)),
  });
}

async function finishError(env, id, message) {
  await call(env, "finish.error", { error: message });
}

export default {
  async fetch() {
    return new Response("cellhive workflow wrapper");
  },
};
