// CellHive user-runtime — internal privileged dispatch service (:8088).
import { bindingStub, WorkflowBridgeTarget } from "bindings.js";
import { budgetErrorBody, checkedWorkerGet } from "budget.js";
export { KV, D1Database, R2Bucket, QueueProducer, ServiceBinding, AI, Hyperdrive , DurableObjectNamespace, WorkflowBinding, Vectorize } from "bindings.js";
//
// Runs tenant handlers that are not fetch: queue() and scheduled(). It loads the
// tenant's immutable bundle through workerLoader, wrapped by a platform module
// exposing WorkerEntrypoint RPC methods (workerLoader only exposes fetch).
//
//   POST /v1/queues/dispatch
//     { namespace, worker, bundle_sha, version, queue, messages:[{id, body(b64), content_type, attempts}], bindings?, vars? }
//   POST /v1/timers/dispatch
//     { namespace, worker, bundle_sha, version, kind, scheduled_time_ms, cron?, bindings?, vars? }
//
// When a dispatch body omits `bindings`, the worker's binding spec is fetched
// from cell-agent once per (worker, version) and injected into env, so queue()/
// scheduled()/workflow handlers see the same env as fetch() (ADR-128).

export default {
  async fetch(req, env, ctx) {
    const url = new URL(req.url);
    if (url.pathname === "/healthz") return new Response("ok");
    if (req.method === "POST" && url.pathname === "/v1/queues/dispatch") {
      if (!authorized(req, env)) return json({ error: "unauthorized" }, 401);
      return dispatchQueues(req, env, ctx);
    }
    if (req.method === "POST" && url.pathname === "/v1/timers/dispatch") {
      if (!authorized(req, env)) return json({ error: "unauthorized" }, 401);
      return dispatchTimers(req, env, ctx);
    }
    if (req.method === "POST" && url.pathname === "/v1/workflows/run") {
      if (!authorized(req, env)) return json({ error: "unauthorized" }, 401);
      return dispatchWorkflows(req, env, ctx);
    }
    if (req.method === "POST" && url.pathname === "/v1/services/fetch") {
      if (!authorized(req, env)) return json({ error: "unauthorized" }, 401);
      return dispatchServiceFetch(req, env, ctx);
    }
    if (req.method === "POST" && url.pathname === "/v1/services/run") {
      if (!authorized(req, env)) return json({ error: "unauthorized" }, 401);
      return dispatchServiceRun(req, env, ctx);
    }
    return new Response("not found", { status: 404 });
  },
};

// Dispatch endpoints are privileged: only the cell-agent may call them with the
// dispatch-role token (Tier-1, ADR-075). Constant-time compare.
function authorized(req, env) {
  const want = env.DISPATCH_TOKEN || "";
  if (!want) return false;
  const got = req.headers.get("x-cellhive-internal-token") || "";
  if (got.length !== want.length) return false;
  let diff = 0;
  for (let i = 0; i < got.length; i++) diff |= got.charCodeAt(i) ^ want.charCodeAt(i);
  return diff === 0;
}

async function dispatchQueues(req, env, ctx) {
  const body = await readJSON(req);
  if (body.error) return json({ error: "bad_json", message: body.error }, 400);
  const { namespace, worker, bundle_sha, queue, messages } = body;
  if (!namespace || !worker || !bundle_sha || !queue || !Array.isArray(messages)) {
    return json({ error: "bad_request", message: "namespace, worker, bundle_sha, queue, messages are required" }, 400);
  }
  let stub;
  try {
    stub = await loadWorker(env, ctx, body);
  } catch (e) {
    return workerLoadFailure(e);
  }
  const batch = messages.map((m) => ({
    id: m.id,
    // CF types message.body by content type (ADR-155): JSON is parsed, text is a
    // string, anything else is a byte array.
    body: decodeBody(m.body, m.content_type),
    contentType: m.content_type,
    attempts: m.attempts,
    timestamp: new Date(),
  }));
  // The batch crosses an isolate boundary over RPC, and structured clone drops
  // non-index array properties and functions — so send a plain payload and let
  // the tenant-side wrapper build the CF MessageBatch (ADR-154).
  const payload = { messages: batch, queue, traceparent: body.traceparent };
  try {
    const out = await stub.getEntrypoint("CellHiveHost").handleQueue(payload);
    // The wrapper returns {result, ack, retry}; older returns are the raw value.
    const result = out && typeof out === "object" && "result" in out ? out.result : out;
    return json({
      ok: true, queue, worker, handled: batch.length,
      result: result === undefined ? null : result,
      ack: (out && out.ack) || [],
      retry: (out && out.retry) || [],
    });
  } catch (e) {
    return json({ error: "handler_failed", message: String(e) }, 500);
  }
}

async function dispatchTimers(req, env, ctx) {
  const body = await readJSON(req);
  if (body.error) return json({ error: "bad_json", message: body.error }, 400);
  const { namespace, worker, bundle_sha, kind, scheduled_time_ms, cron } = body;
  if (!namespace || !worker || !bundle_sha) {
    return json({ error: "bad_request", message: "namespace, worker, bundle_sha are required" }, 400);
  }
  let stub;
  try {
    stub = await loadWorker(env, ctx, body);
  } catch (e) {
    return workerLoadFailure(e);
  }
  const event = { scheduledTime: scheduled_time_ms || Date.now(), cron: cron || undefined };
  if (body.traceparent) Object.defineProperty(event, "traceparent", { value: body.traceparent, enumerable: false });
  try {
    const result = await stub.getEntrypoint("CellHiveHost").handleScheduled(event);
    return json({ ok: true, kind: kind || "cron", worker, scheduled_time_ms: event.scheduledTime, result: result === undefined ? null : result });
  } catch (e) {
    return json({ error: "handler_failed", message: String(e) }, 500);
  }
}

async function dispatchServiceFetch(req, env, ctx) {
  const body = await readJSON(req);
  if (body.error) return json({ error: "bad_json", message: body.error }, 400);
  const { namespace, worker, bundle_sha, version, bindings, vars, method, url: targetURL, content_type, entrypoint } = body;
  if (!namespace || !worker || !bundle_sha) {
    return json({ error: "bad_request", message: "namespace, worker, bundle_sha are required" }, 400);
  }
  let stub;
  try {
    stub = await loadWorker(env, ctx, { namespace, worker, bundle_sha, version, bindings: bindings || {}, vars: vars || {} });
  } catch (e) {
    return workerLoadFailure(e);
  }
  const m = method || "GET";
  const init = { method: m };
  if (m !== "GET" && m !== "HEAD") {
    const raw = body.body ? atob(body.body) : "";
    const bytes = new Uint8Array(raw.length);
    for (let i = 0; i < raw.length; i++) bytes[i] = raw.charCodeAt(i);
    init.body = bytes;
    init.headers = { "content-type": content_type || "text/plain" };
  }
  if (body.traceparent) init.headers = Object.assign({}, init.headers, { traceparent: body.traceparent });
  try {
    const host = stub.getEntrypoint("CellHiveHost");
    const req = new Request(targetURL || "http://service/", init);
    const r = entrypoint ? await host.callMethod(entrypoint, "fetch", [req]) : await host.handleFetch(req);
    return new Response(await r.arrayBuffer(), {
      status: r.status,
      headers: { "content-type": r.headers.get("content-type") || "text/plain" },
    });
  } catch (e) {
    return json({ error: "service_failed", message: String(e) }, 500);
  }
}

async function dispatchServiceRun(req, env, ctx) {
  const body = await readJSON(req);
  if (body.error) return json({ error: "bad_json", message: body.error }, 400);
  const { namespace, worker, bundle_sha, version, entrypoint, method, args, bindings, vars } = body;
  if (!namespace || !worker || !bundle_sha || !method) {
    return json({ error: "bad_request", message: "namespace, worker, bundle_sha, method are required" }, 400);
  }
  let stub;
  try {
    stub = await loadWorker(env, ctx, { namespace, worker, bundle_sha, version, bindings: bindings || {}, vars: vars || {} });
  } catch (e) {
    return workerLoadFailure(e);
  }
  try {
    const host = stub.getEntrypoint("CellHiveHost");
    const result = await host.callMethod(entrypoint || "", method, args || [], body.traceparent);
    return json({ result: result === undefined ? null : result });
  } catch (e) {
    return json({ error: "service_failed", message: String(e) }, 500);
  }
}

async function dispatchWorkflows(req, env, ctx) {
  const body = await readJSON(req);
  if (body.error) return json({ error: "bad_json", message: body.error }, 400);
  const { namespace, worker, bundle_sha, workflow, class_name, id } = body;
  if (!namespace || !worker || !bundle_sha || !workflow || !class_name || !id) {
    return json({ error: "bad_request", message: "namespace, worker, bundle_sha, workflow, class_name, id are required" }, 400);
  }
  let stub;
  try {
    stub = await loadWorkflowWorker(env, ctx, body);
  } catch (e) {
    return workerLoadFailure(e);
  }
  try {
    const bridge = new WorkflowBridgeTarget(env, {
      ns: body.namespace,
      worker: body.worker,
      workflow: body.workflow,
      id: body.id,
      run: body.run_token || "",
    });
    const out = await stub.getEntrypoint("CellHiveWorkflow").handleRun(class_name, {
      instanceId: id,
      event: { payload: decodeBody(body.params, "application/json"), instanceId: id, timestamp: new Date() },
    }, bridge);
    return json({ ok: true, workflow, id, ...(out || {}) });
  } catch (e) {
    return json({ error: "workflow_failed", message: String(e) }, 500);
  }
}

// loadWorkflowWorker rewrites the tenant's `cloudflare:workers` import to the
// platform WorkflowEntrypoint base and loads the workflow wrapper (P2 workflows).
async function loadWorkflowWorker(env, ctx, body) {
  let source = await loadSource(env, body.bundle_sha);
  source = source
    .replaceAll('"cloudflare:workers"', '"cellhive-workflow.js"')
    .replaceAll("'cloudflare:workers'", "'cellhive-workflow.js'")
    .replaceAll('"cloudflare:workflows"', '"cellhive-workflow.js"')
    .replaceAll("'cloudflare:workflows'", "'cellhive-workflow.js'");
  // Include the version when given: a binding/vars-only redeploy keeps the sha
  // but must not reuse the old isolate/env (see loader.js workerStub).
  const wfVer = body.version != null ? `${body.version}-` : "";
  const spec = await specFor(env, body);
  const workerCode = {
    compatibilityDate: body.compat_date || spec.compat_date || "2026-06-15",
    compatibilityFlags: body.compat_flags || spec.compat_flags || [],
    mainModule: "workflow-wrapper.js",
    modules: {
      "workflow-wrapper.js": platformConsts(spec && spec.bindings) + env.WF_WRAPPER_SRC,
      "cellhive-workflow.js": env.WF_BASE_SRC,
      "tenant.js": source,
      "facades.js": env.FACADES_SRC,
      "rpc-codec.js": env.RPC_CODEC_SRC,
    },
    env: tenantEnv(env, ctx, spec.bindings, spec.vars),
    globalOutbound: env.OUTBOUND,
  };
  return checkedWorkerGet(
    env.LOADER,
    `${body.namespace}/${body.worker}@${wfVer}${body.bundle_sha}~wf`,
    workerCode,
    spec.vars,
    spec.bindings,
    "user",
  );
}

// platformConsts renders only non-secret binding-name metadata. Platform
// URLs/tokens remain in the trusted host and never enter dynamic WorkerCode.
function platformConsts(spec) {
  const names = (kind) => Object.entries(spec || {}).filter(([, b]) => b && b.kind === kind).map(([n]) => n);
  return `;const __cellhivePlatform = Object.freeze({ r2Bindings: ${JSON.stringify(names("r2"))}, doBindings: ${JSON.stringify(names("do"))} });\n`;
}

// tenantEnv builds the loaded env: vars + migrated entrypoint stubs. The
// Platform credentials remain in the trusted host and are neither placed here
// nor rendered into the module-scope __cellhivePlatform metadata (ADR-186).
function tenantEnv(env, ctx, spec, vars) {
  const out = Object.assign({}, vars || {});
  const unmigrated = {};
  for (const [name, b] of Object.entries(spec || {})) {
    const stub = bindingStub(ctx, b);
    if (stub !== undefined) out[name] = stub;
    else unmigrated[name] = b;
  }
  // All binding kinds are platform-side stubs now; a leftover entry means a
  // new kind was added without a stub, and the old facade fallback has no
  // transport any more — fail loudly instead of dropping the binding.
  if (Object.keys(unmigrated).length > 0) {
    console.error("cellhive: binding kinds without a platform stub:", Object.keys(unmigrated).join(","));
  }
  return out;
}

async function loadWorker(env, ctx, body) {
  const source = await loadSource(env, body.bundle_sha);
  const spec = await specFor(env, body);
  // Same id format as loader.js workerStub (ADR-126/127): a worker's fetch and
  // queue/scheduled/service handlers share one isolate per (worker, version).
  const ver = body.version != null ? `${body.version}-` : "";
  const workerCode = {
    compatibilityDate: body.compat_date || spec.compat_date || "2026-06-15",
    compatibilityFlags: body.compat_flags || spec.compat_flags || [],
    mainModule: "wrapper.js",
    modules: {
      "wrapper.js": platformConsts(spec && spec.bindings) + env.WRAPPER_SRC,
      "tenant.js": source,
      "facades.js": env.FACADES_SRC,
      "rpc-codec.js": env.RPC_CODEC_SRC,
    },
    env: tenantEnv(env, ctx, spec.bindings, spec.vars, body.namespace, body.worker),
    globalOutbound: env.OUTBOUND,
  };
  return checkedWorkerGet(
    env.LOADER,
    `${body.namespace}/${body.worker}@${ver}${body.bundle_sha}`,
    workerCode,
    spec.vars,
    spec.bindings,
    "user",
  );
}

// --- cold-path caches -------------------------------------------------------
// workerLoader caches the loaded worker by id and does not re-run the getter on
// a hit, so bundle/spec fetches are cached here; without this every queue poll
// (interval 1s) would re-download the bundle. Bounded FIFO maps.
const BUNDLE_CACHE_MAX = 256;
const SPEC_CACHE_MAX = 64;
const bundleCache = new Map(); // bundle sha -> bundle source text (pre-rewrite)
const bundleInflight = new Map(); // bundle sha -> Promise (single-flight)
const specCache = new Map();   // "ns/worker@version" -> { bindings, vars }

function cachePut(map, key, val, max) {
  if (map.size >= max) map.delete(map.keys().next().value);
  map.set(key, val);
}

async function loadSource(env, sha) {
  let src = bundleCache.get(sha);
  if (src !== undefined) return src;
  const running = bundleInflight.get(sha);
  if (running) return running; // single-flight: one fetch per sha
  const p = (async () => {
    try {
      const text = await fetchBundle(env, sha);
      cachePut(bundleCache, sha, text, BUNDLE_CACHE_MAX);
      return text;
    } finally {
      bundleInflight.delete(sha);
    }
  })();
  bundleInflight.set(sha, p);
  return p;
}

// specFor resolves the binding spec + vars for a dispatch body. An explicit
// body.bindings wins (service dispatch carries the cell-agent-resolved spec);
// otherwise the spec is fetched once per (worker, version) (ADR-128).
async function specFor(env, body) {
  if (body.bindings !== undefined) {
    return {
      bindings: body.bindings || {}, vars: body.vars || {},
      compat_date: body.compat_date, compat_flags: body.compat_flags,
    };
  }
  const key = `${body.namespace}/${body.worker}@${body.version !== undefined ? body.version : ""}`;
  let spec = specCache.get(key);
  if (spec === undefined) {
    spec = await fetchSpec(env, body.namespace, body.worker, body.version);
    // Only successful lookups are cached: a cell-agent blip must not pin an
    // empty spec for the worker's lifetime.
    if (spec.ok !== false) cachePut(specCache, key, spec, SPEC_CACHE_MAX);
  }
  return spec;
}

// fetchSpec fails open (worker loads without bindings) so a cell-agent hiccup
// cannot wedge dispatch; the next cold load retries.
async function fetchSpec(env, ns, worker, version) {
  let path = "/v1/internal/worker/bindings?ns=" + encodeURIComponent(ns) + "&worker=" + encodeURIComponent(worker);
  if (version !== undefined && version !== null) path += "&version=" + encodeURIComponent(version);
  try {
    const r = await fetch(env.CELL_URL + path, {
      headers: { "x-cellhive-internal-token": env.CELL_TOKEN },
    });
    if (!r.ok) return { ok: false, bindings: {}, vars: {} };
    const j = await r.json();
    return { ok: true, bindings: j.bindings || {}, vars: j.vars || {}, compat_date: j.compat_date, compat_flags: j.compat_flags };
  } catch (e) {
    return { ok: false, bindings: {}, vars: {} };
  }
}

async function readJSON(req) {
  try {
    return await req.json();
  } catch (e) {
    return { error: String(e) };
  }
}

async function fetchBundle(env, sha) {
  const q = "?sha=" + encodeURIComponent(sha);
  const headers = { "x-cellhive-internal-token": env.CELL_TOKEN };
  const signed = await fetch(env.CELL_URL + "/v1/internal/bundle-url" + q, { headers });
  if (signed.ok) {
    const { url } = await signed.json();
    if (url && !url.startsWith("file:")) {
      const direct = await fetch(url);
      if (!direct.ok) throw new Error("GET bundle " + sha + " -> " + direct.status);
      return await direct.text();
    }
  } else if (signed.status !== 404 && signed.status !== 501) {
    throw new Error("GET bundle URL " + sha + " -> " + signed.status);
  }
  const r = await fetch(env.CELL_URL + "/v1/internal/bundle" + q, { headers });
  if (!r.ok) throw new Error("GET bundle " + sha + " -> " + r.status);
  return await r.text();
}

function decodeBody(b64, contentType) {
  if (!b64) return "";
  let bytes;
  try {
    const raw = atob(b64);
    bytes = Uint8Array.from(raw, (ch) => ch.charCodeAt(0));
  } catch {
    return "";
  }
  const ct = String(contentType || "").toLowerCase().split(";")[0].trim();
  if (ct === "application/json" || ct.endsWith("+json")) {
    const text = new TextDecoder().decode(bytes);
    try {
      return JSON.parse(text);
    } catch {
      return text;
    }
  }
  if (ct === "" || ct.startsWith("text/")) return new TextDecoder().decode(bytes);
  return bytes;
}

function json(o, status) {
  return new Response(JSON.stringify(o), {
    status: status || 200,
    headers: { "content-type": "application/json" },
  });
}

function workerLoadFailure(error) {
  const body = budgetErrorBody(error);
  return body ? json(body, 500) : json({ error: "bundle_fetch_failed", message: String(error) }, 502);
}
