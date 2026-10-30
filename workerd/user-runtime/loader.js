// CellHive user-runtime — public loader (:8081).
//
// Gateway-facing entry (Traefik host routing -> here). It resolves the worker
// from the routing projection (pure pull, ADR-031), loads the worker's active
// immutable version through workerLoader, builds the tenant-visible env (vars +
// binding facades) and runs the fetch handler. Platform headers are stripped
// before tenant code runs.
//
// Static assets (ADR-069) with router config (ADR-071): when the active version
// has an assets token, the loader serves assets — directory index, content
// types, ETag/304, `_redirects`, `_headers` — honoring run_worker_first and
// not_found_handling, falling back to the worker on a miss.

import { bindingStub, setServiceLoader } from "bindings.js";
import { budgetErrorBody, checkWorkerBudgets, estimateWorkerCode, workerLimits } from "budget.js";
import { ByteLRU, Lifetime, RuntimeCapacity, textWeight, jsonWeight, managedModules, quotaSource, invocationSource } from "lifecycle.js";
export { KV, D1Database, R2Bucket, QueueProducer, ServiceBinding, AI, Hyperdrive , DurableObjectNamespace, WorkflowBinding, Vectorize, Assets } from "bindings.js";
export { TenantTail } from "log-tail.js";
import { tailQuery, readLogCapture } from "log-tail.js";

const SCOPE_TOKEN_TTL_S = 300;

// Per-host routing cache (ADR-115): the loader fetches a small pointer view per
// host (routes + worker versions), revalidates it with ETag, and fetches the
// worker's immutable per-version env (bindings/vars) separately. See the EOF
// "routing cache" section for the policies (TTL+jitter, single-flight, SWR,
// negative caching, failure backoff, bounded LRU).

// LRU with a size cap; insertion order doubles as recency.
class LRU {
  constructor(max) {
    this.max = max;
    this.m = new Map();
  }
  has(k) { return this.m.has(k); }
  get(k) {
    const v = this.m.get(k);
    if (v === undefined) return undefined;
    this.m.delete(k);
    this.m.set(k, v);
    return v;
  }
  set(k, v) {
    if (this.m.has(k)) this.m.delete(k);
    this.m.set(k, v);
    if (this.m.size > this.max) this.m.delete(this.m.keys().next().value);
    return v;
  }
  delete(k) { return this.m.delete(k); }
}

// hostCache: host -> { value(pointer view|null for known-404), etag, expiresAt, inflight }
const hostCache = new LRU(10000);
// envCache: disjoint active-pointer and immutable-incarnation query identities.
const envCache = new ByteLRU(2000, 16*1024*1024, jsonWeight);
const envInflight = new Map(); // "ns/worker@version" -> Promise
const bundleCache = new ByteLRU(2000, 32*1024*1024, textWeight); // sha -> source
const bundleInflight = new Map(); // sha -> Promise
const assetCache = new ByteLRU(4000, 32*1024*1024, v=>v?v.byteLength:0, 1024*1024+1024);
const metaCache = new ByteLRU(4000, 4*1024*1024, jsonWeight);
const preparedCache = new ByteLRU(2000, 64*1024*1024, jsonWeight); // conservative plan accounting
const preparedInflight = new Map();

const residents = new Map(); // plain descriptors only, never WorkerStub/Fetcher
// Warm-isolate bounds are deployment-configurable (CH_ISOLATE_MAX/BYTES); the
// Go supervisor derives defaults from the node/cgroup memory limit.
const DEFAULT_ISOLATE_MAX = 128;
const DEFAULT_ISOLATE_BYTES = 256*1024*1024;
let isolateCapacityState = null;
function isolateCapacity(env) {
  if (isolateCapacityState) return isolateCapacityState;
  const max = Math.floor(Number(env && env.CH_ISOLATE_MAX));
  const bytes = Math.floor(Number(env && env.CH_ISOLATE_BYTES));
  isolateCapacityState = new RuntimeCapacity(
    Number.isFinite(max) && max > 0 ? max : DEFAULT_ISOLATE_MAX,
    Number.isFinite(bytes) && bytes >= 1024*1024 ? bytes : DEFAULT_ISOLATE_BYTES,
  );
  return isolateCapacityState;
}
let coldLoads = 0;

function managedCode(entry) {
  const base=entry.plan.directFetch?entry.plan.directWorkerCode:entry.plan.workerCode;
  const modules=Object.assign({},base.modules);
  // Track importable waitUntil as well as ctx.waitUntil in the same child state.
  modules['tenant.js']=modules['tenant.js'].replaceAll('"cloudflare:workers"','"__cellhive-workers.js"').replaceAll("'cloudflare:workers'","'__cellhive-workers.js'");
  Object.assign(modules,managedModules(base,entry.plan.directFetch?'default':'CellHiveHost',entry.nonce,entry.plan.spec));
  const flags=base.limits?.subRequests?[...base.compatibilityFlags.filter(f=>f!=='no_nodejs_als'),'nodejs_als']:base.compatibilityFlags;
  return Object.assign({},base,{mainModule:'__cellhive-managed.js',modules,compatibilityFlags:[...new Set(flags)]});
}
function managedStub(env,ctx,entry) {
  const code=entry.code;
  return env.LOADER.get(entry.id,()=>Object.assign({},code,{
    env:entry.plan.directFetch?Object.assign({},entry.version.vars||{}):tenantEnv(env,ctx,entry.plan.spec,entry.version.vars),
    globalOutbound:env.OUTBOUND,
    ...(entry.tailOn?{tails:[ctx.exports.TenantTail({props:{namespace:entry.app.namespace,worker:entry.worker,version:entry.version.version||0,tailKey:entry.plan.tailKey}})]}:{}),
  }));
}
async function sweepResidents(env,ctx) {
  for(const [key,e] of residents){
    if(!e.retiring||e.cleaning||!e.life.idle()||e.pinned)continue;
    e.cleaning=(async()=>{
      try{
        const ctl=managedStub(env,ctx,e).getEntrypoint('CellHiveLifecycle');
        const status=await ctl.status();
        if(status.pinned){e.pinned=true;return;}
        // The host lease is taken in residentWorker(), not here. Re-check it
        // after the async status() round trip: a request may have acquired the
        // entry while we were waiting, and retiring it would abort that request.
        if(!e.retiring||status.nonce!==e.nonce||!status.idle||!e.life.idle())return;
        try{if(await ctl.retire(e.nonce)===false)return;}
        catch(err){if(!/internal error; reference =|execution.*abort|disconnected/i.test(String(err)))throw err;}
        if(residents.get(key)===e){residents.delete(key);isolateCapacity(env).release(key);}
        if(![...residents.values()].some(o=>o.versionKey===e.versionKey))preparedCache.delete(e.versionKey);
      }catch(err){console.error('worker retirement failed:',String(err));}
    })();
    await e.cleaning;
    e.cleaning=null;
  }
}
function scheduleSweep(env,ctx){
  if(![...residents.values()].some(e=>e.retiring&&!e.pinned))return;
  ctx.waitUntil((async()=>{
    // Bounded retry for waitUntil/stream teardown; later requests also sweep.
    for(let i=0;i<100;i++){
      await sweepResidents(env,ctx);
      if(![...residents.values()].some(e=>e.retiring&&!e.pinned))return;
      await new Promise(resolve=>setTimeout(resolve,50));
    }
  })());
}
async function residentWorker(env,ctx,app,worker,version,plan,tailOn){
  const capacity = isolateCapacity(env);
  const versionKey=workerVersionKey(app,worker,version),family=app.namespace+'/'+worker;
  const key=residentVariantKey(versionKey,tailOn);
  let retiring=false;
  for(const e of residents.values()){
    if(e.family===family&&Number(e.version.version)<Number(version.version))e.retiring=true;
    // Tail-on/off variants use different worker ids (same bundle). Do not
    // abort the old variant on this switch: abortIsolate makes workerd emit
    // "script terminated for unknown reasons" while the new variant is being
    // loaded. Keep both resident; the capacity sweep can reclaim them later.
    if(e.retiring)retiring=true;
  }
  // Avoid an async suspension on the steady-state fetch path. Existing
  // retirement candidates still get swept before a new resident is used.
  if(retiring)await sweepResidents(env,ctx);
  let e=residents.get(key);
  if(e?.cleaning){await e.cleaning;e=residents.get(key);}
  if(!e){
    // Reserve before any further await: concurrent misses cannot exceed admission.
    e={key,versionKey,tailOn,family,app,worker,version,plan,nonce:crypto.randomUUID(),life:new Lifetime(),retiring:false,cleaning:null,pinned:false};
    e.id='managed-fetch/'+key+'/'+e.nonce;
    e.code=managedCode(e);
    checkWorkerBudgets(e.code,version.vars,plan.spec,'user');
    const weight=3*estimateWorkerCode(e.code)+2*1024*1024+jsonWeight(version)+jsonWeight(plan.spec);
    if(!capacity.reserve(key,weight)){
      for(const old of residents.values())if(old.life.idle())old.retiring=true;
      await sweepResidents(env,ctx);
      if(!capacity.reserve(key,weight))throw new Error('runtime_capacity');
      e=residents.get(key)||e;
    }
    residents.set(key,e);
  }
  // Do not clear e.retiring here: a request for an older version must not
  // revive an isolate that another request already selected for retirement.
  // The sweep skips entries with an active lease and retires them when idle.
  // Moving the descriptor refreshes the eviction order.
  residents.delete(key);residents.set(key,e);
  const release=e.life.hold();
  return {entry:e,release};
}

export default {
  async fetch(req, env, ctx) {
    const url = new URL(req.url);
    if (req.method === "GET" && url.pathname === "/v1/internal/logs/events") return tailQuery(req, env);
    if (url.pathname === "/healthz") return new Response("ok");
    if (env.CH_POOL_MODE === "1" && url.pathname === "/drain") return text("not found", 404);
    // Rolling-update probes (ADR-156): /ready reports projection freshness and
    // draining so the edge can pull this instance out before it stops; /drain
    // (internal token) flips that flag.
    if (url.pathname === "/ready") return readyResponse(env);
    if (url.pathname === "/drain" && req.method === "POST") return drainRequest(req, env);
    if (env.CH_POOL_MODE === "1") {
      if (draining) return text("runtime draining", 503);
      if (!secureEqual(req.headers.get("x-cellhive-internal-token") || "", env.CELL_TOKEN || "")) return text("unauthorized", 401);
      const ns=req.headers.get("x-cellhive-pool-namespace"),worker=req.headers.get("x-cellhive-pool-worker");
      const number=Number(req.headers.get("x-cellhive-pool-version")),sha=req.headers.get("x-cellhive-pool-bundle");
      const storageID=req.headers.get("x-cellhive-pool-storage-id")||"";
      if(!ns||!worker||!Number.isSafeInteger(number)||number<=0||!sha)return text("invalid execution identity",400);
      const target=JSON.parse(env.CH_POOL_TARGET||"null");
      if(target&&(target.namespace!==ns||target.worker!==worker||target.version!==number||target.bundle_sha!==sha||(target.storage_id||"")!==storageID))return text("dedicated target mismatch",403);
      let version;
      try{version=await workerEnv(env,ns,worker,number,sha,storageID);}catch(e){return text("worker env unavailable",503);}
      if(!version||version.version!==number||version.bundle_sha!==sha)return text("execution version mismatch",409);
      // Go already stripped the route mount and pinned this immutable version.
      // Reuse the existing asset/worker routing contract below with no control
      // lookup of active version in the child.
      return fixedFetch(req,env,ctx,{namespace:ns},worker,version,url);
    }
    maybePollRoutes(env, ctx);
    maybePollLogCapture(env, ctx);
    const host = normalizeHost(req.headers.get("host") || url.host);
    // A missing/blank Host has no route to resolve. Treat it as a miss instead
    // of querying the control plane with an empty host (which returns 400 and
    // would surface as a 503).
    if (!host) return text("no route for host", 404);

    let view;
    try {
      view = await hostView(env, host, ctx);
    } catch (e) {
      return text("routing lookup unavailable", 503);
    }
    if (!view || !view.routes || view.routes.length === 0) {
      return text("no route for host " + host, 404);
    }
    const route = matchPath(view.routes, url.pathname);
    if (!route) return text("no route for host " + host, 404);
    // Mount semantics (ADR-131): the matched path prefix is always removed, so
    // the worker and its assets see paths relative to it.
    const effReq = stripRoutePrefix(req, route);
    const effURL = effReq === req ? url : new URL(effReq.url);
    const ptr = view.workers[route.ns + "/" + route.worker];
    if (!ptr || !ptr.bundle_sha) {
      return text("worker " + route.worker + " has no active version", 503);
    }
    let version;
    try {
      version = await workerEnv(env, route.ns, route.worker, ptr.version, ptr.bundle_sha, ptr.storage_id);
    } catch (e) {
      return text("worker env unavailable", 503);
    }
    if (!version || !version.bundle_sha) {
      return text("worker " + route.worker + " has no active version", 503);
    }
    const app = { namespace: route.ns };
    const isAssetMethod = req.method === "GET" || req.method === "HEAD";

    if (version.assets_sha && isAssetMethod) {
      const assetsCfg = version.assets || {};
      try {
        if (shouldRunWorkerFirst(assetsCfg, effURL.pathname)) {
          const wres = await runWorker(effReq, env, ctx, app, route.worker, version, version.class_storage || {}, version.deleted_classes || []);
          return wres;
        }
        const served = await serveAsset(effReq, env, app, route.worker, version, effURL);
        if (served) return served;
        // miss with assets-first: fall through to the worker.
      } catch (e) {
        console.error("asset error:", e); // never take down dynamic routes
      }
    }
    return runWorker(effReq, env, ctx, app, route.worker, version, version.class_storage || {}, version.deleted_classes || []);
  },
};

function secureEqual(a,b){if(!b||a.length!==b.length)return false;let diff=0;for(let i=0;i<a.length;i++)diff|=a.charCodeAt(i)^b.charCodeAt(i);return diff===0;}
async function fixedFetch(req,env,ctx,app,worker,version,url){
  if(version.assets_sha&&(req.method==='GET'||req.method==='HEAD')){
    const cfg=version.assets||{};
    if(shouldRunWorkerFirst(cfg,url.pathname)){
      return await runWorker(req,env,ctx,app,worker,version,version.class_storage||{},version.deleted_classes||[]);
    }
    const asset=await serveAsset(req,env,app,worker,version,url);if(asset)return asset;
  }
  return runWorker(req,env,ctx,app,worker,version,version.class_storage||{},version.deleted_classes||[]);
}

import { traceparent as newTraceparent, startSpan, endSpan, flushSpans } from "telemetry.js";

// --- worker ---------------------------------------------------------------

// prepareRequest propagates W3C trace context and strips host-only headers in
// one headers/request copy. The sampled bit lets the unsampled hot path skip
// all span allocation and flushing without weakening ADR-146 propagation.
function prepareRequest(req, env) {
  let tp = req.headers.get("traceparent");
  let sampled = false;
  if (tp) {
    const m = /^[0-9a-fA-F]{2}-[0-9a-fA-F]{32}-[0-9a-fA-F]{16}-([0-9a-fA-F]{2})/.exec(tp.trim());
    sampled = !!(m && (parseInt(m[1], 16) & 1));
  } else {
    const ratio = Number(env.CH_TRACE_RATIO);
    sampled = Number.isFinite(ratio) && Math.random() < ratio;
    tp = newTraceparent(sampled);
  }
  globalThis.__cellhiveTraceparent = tp;
  globalThis.__cellhiveTraceSampled = sampled;
  const headers = new Headers(req.headers);
  for (const k of [...headers.keys()]) {
    if (k.toLowerCase().startsWith("x-cellhive-")) headers.delete(k);
  }
  headers.set("traceparent", tp);
  if (!headers.get("x-request-id")) headers.set("x-request-id", crypto.randomUUID());
  return { request: new Request(req, { headers }), sampled };
}

async function runWorker(req, env, ctx, app, worker, version, classStorage, deletedClasses) {
  const cold=!preparedCache.has(workerVersionKey(app,worker,version));
  if(cold&&coldLoads>=16)return text('runtime cold-load capacity exhausted',503);
  if(cold)coldLoads++;
  let plan;
  try {
    plan = await preparedWorker(env, app, worker, version, classStorage, deletedClasses);
  } catch (e) {
    const body = budgetErrorBody(e);
    if (body) {
      return new Response(JSON.stringify(body), {
        status: 500,
        headers: { "content-type": "application/json" },
      });
    }
    if (e && e.stage === "bundle") return text("bundle fetch failed: " + e.cause, 502);
    if (e && e.stage === "binding") return text("binding setup failed: " + e.cause, 502);
    return text("worker preparation failed: " + e, 502);
  } finally {
    if(cold)coldLoads--;
  }
  const tailOn=tailEnabled(env,app.namespace,worker);
  let lease;
  try { lease=await residentWorker(env,ctx,app,worker,version,plan,tailOn); }
  catch(e){return text(String(e).includes('runtime_capacity')?'runtime load capacity exhausted':'worker load failed',503);}
  const finish=()=>{
    lease.release();
    scheduleSweep(env,ctx);
  };
  const traced = prepareRequest(req, env);
  if (!traced.sampled) {
    try {
      const res=await managedStub(env,ctx,lease.entry).getEntrypoint('CellHiveManagedFetch').fetch(traced.request);
      // The child Lifetime owns the response body through EOF/cancel/error.
      // Retirement also checks child status, so another host stream wrapper is
      // unnecessary once the fetch RPC has completed.
      finish();
      return res;
    } catch (e) {
      finish();
      return text("worker error: " + e, 500);
    }
  }
  const span = startSpan("http.server", {
    kind: "server",
    "http.method": req.method,
    "cellhive.namespace": app.namespace,
    "cellhive.worker": worker,
  });
  try {
    const res = await managedStub(env,ctx,lease.entry).getEntrypoint('CellHiveManagedFetch').fetch(traced.request);
    endSpan(span, { code: res.status >= 500 ? 2 : 0 });
    ctx.waitUntil(flushSpans(env));
    finish();
    return res;
  } catch (e) {
    finish();
    endSpan(span, { code: 2, message: String(e) });
    ctx.waitUntil(flushSpans(env));
    return text("worker error: " + e, 500);
  }
}

function workerVersionKey(app, worker, version) {
  return `${app.namespace}/${worker}@${version.storage_id || "legacy"}:${version.version}-${version.bundle_sha}`;
}

async function preparedWorker(env, app, worker, version, classStorage, deletedClasses) {
  const key = workerVersionKey(app, worker, version);
  const cached = preparedCache.get(key);
  if (cached) return cached;
  const running = preparedInflight.get(key);
  if (running) return running;
  const pending = (async () => {
    try {
      let source;
      try {
        source = await bundle(env, version.bundle_sha);
      } catch (e) {
        throw { stage: "bundle", cause: e };
      }
      let spec;
      try {
        spec = await bindingSpec(env, app, worker, version, classStorage, deletedClasses);
      } catch (e) {
        throw { stage: "binding", cause: e };
      }
      const plan = workerPlan(env, version, source, spec);
      checkWorkerBudgets(plan.directFetch ? plan.directWorkerCode : plan.workerCode, version.vars, spec, "user");
      preparedCache.set(key, plan);
      return plan;
    } finally {
      preparedInflight.delete(key);
    }
  })();
  preparedInflight.set(key, pending);
  return pending;
}

// workerPlan contains only immutable plain data and may cross request contexts.
// workerd capabilities cannot: loader.get(), OUTBOUND, entrypoints, binding
// stubs, and tails must all be acquired in the current request.
function workerPlan(env, version, source, spec) {
  const directFetch = Object.keys(spec).length === 0;
  const tailKey = env.CH_LOG_CAPTURE === "0" ? "" : crypto.randomUUID();
  const compatibilityFlags = version.limits?.subrequests
    ? [...new Set([...(version.compat_flags || []), "nodejs_als"])]
    : (version.compat_flags || []);
  return {
    spec,
    tailKey,
    directFetch,
    directWorkerCode: directFetch ? {
      compatibilityDate: version.compat_date || "2026-06-15",
      compatibilityFlags,
      limits: workerLimits(version.limits),
      mainModule: "tenant.js",
      modules: { "tenant.js": source },
    } : null,
    workerCode: {
      compatibilityDate: version.compat_date || "2026-06-15",
      compatibilityFlags: [...new Set([...compatibilityFlags.filter(f=>f!=="no_nodejs_als"), "nodejs_als"])],
      limits: workerLimits(version.limits),
      mainModule: "wrapper.js",
      modules: {
        "wrapper.js": platformConsts(spec, tailKey) + env.WRAPPER_SRC,
        "tenant.js": source,
        "facades.js": env.FACADES_SRC,
        "rpc-codec.js": env.RPC_CODEC_SRC,
        "__cellhive-quota.js": quotaSource(version.limits?.subrequests, spec),
        "__cellhive-invocation.js": invocationSource(),
      },
      // globalOutbound is request-scoped and attached by workerStub.
    },
  };
}

function fetchWorkerStub(env, ctx, app, worker, version, plan) {
  if (!plan.directFetch) return workerStub(env, ctx, app, worker, version, plan);
  const tailOn = tailEnabled(env, app.namespace, worker);
  const id = "fetch/" + residentVariantKey(workerVersionKey(app, worker, version), tailOn);
  return env.LOADER.get(id, () => Object.assign({}, plan.directWorkerCode, {
    globalOutbound: env.OUTBOUND,
    env: Object.assign({}, version.vars || {}),
    ...(tailOn ? { tails: [ctx.exports.TenantTail({
      props: { namespace: app.namespace, worker, version: version.version || 0, tailKey: plan.tailKey },
    })] } : {}),
  }));
}

function workerStub(env, ctx, app, worker, version, plan) {
  const tailOn = tailEnabled(env, app.namespace, worker);
  const id = residentVariantKey(workerVersionKey(app, worker, version), tailOn);
  return env.LOADER.get(id, () => Object.assign({}, plan.workerCode, {
    globalOutbound: env.OUTBOUND,
    env: tenantEnv(env, ctx, plan.spec, version.vars),
    ...(tailOn ? { tails: [ctx.exports.TenantTail({
      props: { namespace: app.namespace, worker, version: version.version || 0, tailKey: plan.tailKey },
    })] } : {}),
  }));
}

// The service loader is registered once: ServiceBinding (bindings.js) calls it
// to obtain the pinned target's stub, so env.SVC.<method>() is a native,
// same-instance JSRPC call instead of a cell-agent HTTP/JSON round trip.
setServiceLoader(async (ctx, env, target) => {
  let version = await workerEnv(env, target.ns, target.worker);
  if (!version || !version.bundle_sha) {
    throw new Error("service target " + target.ns + "/" + target.worker + " has no active version");
  }
  // ADR-104: the caller's deploy pinned the target bundle. Load that exact
  // version (the active env is wrong once the target redeploys).
  const pinned = target.version || "";
  if (pinned && pinned !== version.bundle_sha) {
    const pinnedEnv = await workerEnv(env, target.ns, target.worker, undefined, pinned);
    if (!pinnedEnv || pinnedEnv.bundle_sha !== pinned) {
      throw new Error("service target " + target.ns + "/" + target.worker + " pinned version " + pinned + " is gone");
    }
    version = pinnedEnv;
  }
  const app = { namespace: target.ns };
  const plan = await preparedWorker(
    env, app, target.worker, version,
    version.class_storage || {}, version.deleted_classes || [],
  );
  return workerStub(env, ctx, app, target.worker, version, plan);
});

// tenantEnv builds the loaded worker env: vars + entrypoint stubs for migrated
// bindings; every kind is a platform-side stub (ADR-184).
function tenantEnv(env, ctx, spec, vars) {
  const out = Object.assign({}, vars || {});
  const unmigrated = {};
  const r2names = [];
  for (const [name, b] of Object.entries(spec || {})) {
    const stub = bindingStub(ctx, b);
    if (stub !== undefined) {
      out[name] = stub;
      if (b && b.kind === "r2") r2names.push(name);
    } else unmigrated[name] = b;
  }
  // All binding kinds are platform-side stubs now; a leftover entry means a
  // new kind was added without a stub, and the old facade fallback has no
  // transport any more — fail loudly instead of dropping the binding.
  if (Object.keys(unmigrated).length > 0) {
    console.error("cellhive: binding kinds without a platform stub:", Object.keys(unmigrated).join(","));
  }
  return out;
}

// platformConsts renders binding names and a Tail-only correlation key into
// the trusted wrapper module. The key authorizes no platform API; platform
// URLs and role credentials stay in the host worker (ADR-186/230).
function platformConsts(spec, tailKey = "") {
  // PREPENDED to the wrapper source: the wrapper's top-level code runs before
  // any later statement, so the const must exist before the try block (a const
  // declared after use is in the temporal dead zone).
  const names = (kind) => Object.entries(spec || {}).filter(([, b]) => b && b.kind === kind).map(([n]) => n);
  return `;const __cellhivePlatform = Object.freeze({ kvBindings: ${JSON.stringify(names("kv"))}, d1Bindings: ${JSON.stringify(names("d1"))}, r2Bindings: ${JSON.stringify(names("r2"))}, queueBindings: ${JSON.stringify(names("queue"))}, doBindings: ${JSON.stringify(names("do"))} }); const __cellhiveTailKey = ${JSON.stringify(tailKey)};\n`;
}

// --- assets ---------------------------------------------------------------

function shouldRunWorkerFirst(cfg, pathname) {
  if (cfg.run_worker_first === true) return true;
  let matched = false;
  for (const p of cfg.run_worker_first_paths || []) {
    if (!p) continue;
    const negative = p.startsWith("!");
    const pattern = negative ? p.slice(1) : p;
    const hit = pattern.endsWith("*") ? pathname.startsWith(pattern.slice(0, -1)) : pathname === pattern || pathname.startsWith(pattern + "/");
    if (hit && negative) return false;
    if (hit) matched = true;
  }
  return matched;
}

async function serveAsset(req, env, app, worker, version, url) {
  const token = version.assets_sha;
  const pathname = decodeURIComponent(url.pathname);
	if (pathname === "/_headers" || pathname === "/_redirects") return null;
	const canonical = await htmlCanonicalRedirect(req, env, app, worker, token, pathname, (version.assets || {}).html_handling);
	if (canonical) return canonical;

  for (const rule of await redirectRules(env, app, worker, token)) {
    const dest = matchRedirect(rule, pathname);
    if (dest === null) continue;
    if (rule.status === 200) {
      const rewritten = await assetResponse(req, env, app, worker, token, dest, 200, pathname);
      if (rewritten) return rewritten;
      continue;
    }
    return new Response(null, { status: rule.status, headers: { location: dest } });
  }

  const found = await assetResponse(req, env, app, worker, token, pathname, 200);
  if (found) return found;

  const nf = (version.assets || {}).not_found_handling;
  if (nf === "single-page-application") return assetResponse(req, env, app, worker, token, "index.html", 200, pathname);
  if (nf === "404-page") return assetResponse(req, env, app, worker, token, "404.html", 404, pathname);
  return null; // "none"/unset -> caller falls back to the worker
}

async function htmlCanonicalRedirect(req, env, app, worker, token, pathname, mode) {
  if (!mode || mode === "none") return null;
  const redirect = (location) => new Response(null, { status: 307, headers: { location: location + new URL(req.url).search } });
  if (mode === "drop-trailing-slash" && pathname.length > 1 && pathname.endsWith("/")) return redirect(pathname.slice(0, -1));
  if ((mode === "auto-trailing-slash" || mode === "force-trailing-slash") && pathname.endsWith(".html")) {
    const base = pathname.slice(0, -5);
    return redirect(mode === "force-trailing-slash" ? base + "/" : base || "/");
  }
  if ((mode === "auto-trailing-slash" || mode === "force-trailing-slash") && !pathname.endsWith("/") && !pathname.match(/\.[^/]+$/)) {
    const base = pathname.replace(/^\/+/, "");
    const hit = await asset(env, app, worker, token, base + ".html");
    const index = hit || await asset(env, app, worker, token, base + "/index.html");
    if (index) return redirect(pathname + "/");
  }
  if (mode === "auto-trailing-slash" && pathname.length > 1 && pathname.endsWith("/")) {
    const base = pathname.slice(1, -1);
    const hit = await asset(env, app, worker, token, base + ".html");
    if (hit) return redirect(pathname.slice(0, -1));
  }
  return null;
}

// assetResponse serves one asset. `pathname` is the file to serve; `matchPath`
// (defaults to it) is the request path used for _headers rule matching, which by
// CF semantics follows the request, not the served file (SPA/200-rewrite case).
async function assetResponse(req, env, app, worker, token, pathname, status, matchPath) {
  for (const candidate of assetCandidates(pathname)) {
    const bytes = await asset(env, app, worker, token, candidate);
    if (!bytes) continue;
    if(bytes instanceof Response){
      const etag=bytes.headers.get('etag')||'"'+fnv1a(new TextEncoder().encode(token+'/'+candidate)).toString(16)+'"';
      const headers=new Headers({'content-type':contentType(candidate),etag});
      if(bytes.headers.has('content-length'))headers.set('content-length',bytes.headers.get('content-length'));
      applyHeaderRules(headers,await headerRules(env,app,worker,token),matchPath||pathname);
      if(req.headers.get('if-none-match')===etag){await bytes.body?.cancel();return new Response(null,{status:304,headers});}
      if(req.method==='HEAD'){await bytes.body?.cancel();return new Response(null,{status,headers});}
      return new Response(bytes.body,{status,headers});
    }
    const etag = '"' + fnv1a(bytes).toString(16) + "-" + bytes.length + '"';
    if (req.headers.get("if-none-match") === etag) return new Response(null, { status: 304, headers: { etag } });
    const headers = new Headers({
      "content-type": contentType(candidate),
      etag,
      "content-length": String(bytes.length),
    });
    applyHeaderRules(headers, await headerRules(env, app, worker, token), matchPath || pathname);
    return new Response(req.method === "HEAD" ? null : bytes, { status, headers });
  }
  return null;
}

function assetCandidates(pathname) {
  const p = pathname.replace(/^\/+/, ""); // asset keys have no leading slash
  if (pathname.endsWith("/") || p === "") return [(p ? p + "/" : "") + "index.html"];
  const out = [p];
  if (!/\.[a-zA-Z0-9]+$/.test(p)) {
    out.push(p + ".html");
    out.push(p + "/index.html");
  }
  return out;
}

async function asset(env, app, worker, token, path) {
  const key = `${app.namespace}/${worker}/${token}/${path}`;
  if (assetCache.has(key)) return assetCache.get(key);
  const q = new URLSearchParams({ ns: app.namespace, worker, token, path });
  const headers = { "x-cellhive-internal-token": env.CELL_TOKEN };
  const signed = await fetch(env.CELL_URL + "/v1/internal/asset-url?" + q, { headers });
  let r;
  if (signed.ok) {
    const { url } = await signed.json();
    if (url && !url.startsWith("file:")) r = await fetch(url);
  } else if (signed.status === 404) {
    assetCache.set(key, null);
    return null;
  } else if (signed.status !== 501) {
    throw new Error("GET asset URL " + path + " -> " + signed.status);
  }
  if (!r) r = await fetch(env.CELL_URL + "/v1/internal/asset?" + q, { headers });
  if (r.status === 404) {
    assetCache.set(key, null);
    return null;
  }
  if (!r.ok) throw new Error("GET asset " + path + " -> " + r.status);
  const metadata=path==='_headers'||path==='_redirects';
  const length=Number(r.headers.get('content-length')||NaN);
  if(!metadata&&(!Number.isFinite(length)||length>1024*1024))return r;
  if(metadata){
    const reader=r.body?.getReader();const chunks=[];let size=0;
    if(reader)for(;;){const part=await reader.read();if(part.done)break;size+=part.value.byteLength;if(size>64*1024){await reader.cancel();throw new Error('asset metadata too large');}chunks.push(part.value);}
    const bytes=new Uint8Array(size);let offset=0;for(const chunk of chunks){bytes.set(chunk,offset);offset+=chunk.byteLength;}
    assetCache.set(key,bytes);return bytes;
  }
  const bytes = new Uint8Array(await r.arrayBuffer());
  assetCache.set(key, bytes);
  return bytes;
}

async function redirectRules(env, app, worker, token) {
  const key = `${app.namespace}/${worker}/${token}/_redirects`;
  if (metaCache.has(key)) return metaCache.get(key);
  const bytes = await asset(env, app, worker, token, "_redirects");
  const rules = [];
  if (bytes) {
    for (const line of decode(bytes).split("\n")) {
      const t = line.trim();
      if (!t || t.startsWith("#")) continue;
      const parts = t.split(/\s+/);
      if (parts.length < 2) continue;
      const status = parts[2] ? parseInt(parts[2], 10) : 302;
      rules.push({ from: parts[0], to: parts[1], status: status || 302 });
    }
  }
  metaCache.set(key, rules);
  return rules;
}

function matchRedirect(rule, pathname) {
  if (rule.from.endsWith("*")) {
    const prefix = rule.from.slice(0, -1);
    if (!pathname.startsWith(prefix)) return null;
    return rule.to.replace("*", pathname.slice(prefix.length));
  }
  if (rule.from !== pathname) return null;
  return rule.to;
}

async function headerRules(env, app, worker, token) {
  const key = `${app.namespace}/${worker}/${token}/_headers`;
  if (metaCache.has(key)) return metaCache.get(key);
  const bytes = await asset(env, app, worker, token, "_headers");
  const rules = [];
  if (bytes) {
    let current = null;
    for (const raw of decode(bytes).split("\n")) {
      const line = raw.replace(/\r$/, "");
      if (!line.trim() || line.trim().startsWith("#")) continue;
      if (/^\s/.test(line)) {
        if (!current) continue;
        const idx = line.indexOf(":");
        if (idx > 0) current.headers.push([line.slice(0, idx).trim(), line.slice(idx + 1).trim()]);
      } else {
        current = { path: line.trim(), headers: [] };
        rules.push(current);
      }
    }
  }
  metaCache.set(key, rules);
  return rules;
}

function applyHeaderRules(headers, rules, pathname) {
  for (const rule of rules) {
    const p = rule.path;
    let match = false;
    if (p === "/*" || p === "*") match = true;
    else if (p.endsWith("/*")) match = pathname.startsWith(p.slice(0, -1));
    else match = p === pathname;
    if (!match) continue;
    for (const [k, v] of rule.headers) headers.set(k, v);
  }
}

function contentType(path) {
  const ext = (path.match(/\.([a-zA-Z0-9]+)$/) || [])[1]?.toLowerCase();
  const types = {
    html: "text/html; charset=utf-8",
    htm: "text/html; charset=utf-8",
    js: "text/javascript; charset=utf-8",
    mjs: "text/javascript; charset=utf-8",
    css: "text/css; charset=utf-8",
    json: "application/json; charset=utf-8",
    txt: "text/plain; charset=utf-8",
    xml: "application/xml",
    svg: "image/svg+xml",
    png: "image/png",
    jpg: "image/jpeg",
    jpeg: "image/jpeg",
    gif: "image/gif",
    webp: "image/webp",
    avif: "image/avif",
    ico: "image/x-icon",
    woff: "font/woff",
    woff2: "font/woff2",
    wasm: "application/wasm",
    map: "application/json",
    pdf: "application/pdf",
    mp4: "video/mp4",
    webm: "video/webm",
  };
  return types[ext] || "application/octet-stream";
}

function decode(bytes) {
  return new TextDecoder().decode(bytes);
}

function fnv1a(bytes) {
  let h = 0x811c9dc5;
  for (let i = 0; i < bytes.length; i++) {
    h ^= bytes[i];
    h = (h * 0x01000193) >>> 0;
  }
  return h >>> 0;
}

// --- projection / loading -------------------------------------------------

// --- routing cache (ADR-115) ----------------------------------------------

const HOST_TTL_MS = 10000;      // base per-host pointer TTL; +/- jitter
const HOST_TTL_JITTER = 0.2;    // 10s -> 8..12s, so entries do not expire together
const BACKOFF_MAX_MS = 4000;
const REV_POLL_MS = 2000;       // routing revision poll interval (ADR-116)
const HOST_LOOKUP_MAX_PER_SEC = 100; // unknown-host upstream lookups/sec (ADR-116)

let backoffMs = 0;      // current failure backoff (0 = healthy)
let backoffUntil = 0;   // no upstream attempt before this timestamp

function noteFailure() {
  backoffMs = backoffMs === 0 ? 1000 : Math.min(backoffMs * 2, BACKOFF_MAX_MS);
  backoffUntil = Date.now() + backoffMs;
}

function noteSuccess() {
  backoffMs = 0;
  backoffUntil = 0;
}

function hostTtlBase(env) {
  const n = Number(env.HOST_TTL_MS);
  return Number.isFinite(n) && n > 0 ? n : HOST_TTL_MS;
}

function hostTtlMs(env) {
  return hostTtlBase(env) * (1 + (Math.random() * 2 - 1) * HOST_TTL_JITTER);
}

function revPollMs(env) {
  const n = Number(env.REV_POLL_MS);
  return Number.isFinite(n) && n > 0 ? n : REV_POLL_MS;
}

function hostLookupLimit(env) {
  const n = Number(env.HOST_LOOKUP_MAX_PER_SEC);
  return Number.isFinite(n) && n > 0 ? n : HOST_LOOKUP_MAX_PER_SEC;
}

// maybePollRoutes checks the routing projection's ETag at most once per interval
// (via /v1/control/routes + If-None-Match -> 304). When it changes, every host
// entry is marked stale so the next request to an affected host revalidates
// immediately instead of waiting out the host TTL (ADR-116). The projection body
// is not needed (routing is read per host) and is dropped on the rare 200.
let revInflight = null;
let revPolledAt = 0;
let knownRev = "";
// revOKAt is the last successful projection poll; /ready reports its age so a
// stale-cell loader is pulled out of rotation (ADR-156).
let revOKAt = 0;
let revFailed = false;
// draining is set by POST /drain (internal token); /ready then fails until the
// process is replaced.
let draining = false;
const READY_STALE_MS = 30000;

function jsonStatus(obj, status) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: { "content-type": "application/json" },
  });
}

async function readyResponse(env) {
  if (draining) return jsonStatus({ ready: false, reason: "draining" }, 503);
  // Probes arrive before any tenant traffic, so refresh on demand instead of
  // waiting for the request-path poll (ADR-156).
  if (!revOKAt || Date.now() - revOKAt > READY_STALE_MS) await pollRoutesOnce(env);
  if (!revOKAt) {
    return jsonStatus({ ready: false, reason: revFailed ? "cell_unreachable" : "projection_missing" }, 503);
  }
  const age = Date.now() - revOKAt;
  if (age > READY_STALE_MS) return jsonStatus({ ready: false, reason: "cell_unreachable" }, 503);
  return jsonStatus({ ready: true, projection_age_ms: age, cell: "ok" }, 200);
}

function drainRequest(req, env) {
  const want = env.CELL_TOKEN || "";
  const got = req.headers.get("x-cellhive-internal-token") || "";
  let diff = got.length ^ want.length;
  for (let i = 0; i < Math.max(got.length, want.length); i++) {
    diff |= (got.charCodeAt(i) || 0) ^ (want.charCodeAt(i) || 0);
  }
  if (want === "" || diff !== 0) return new Response("unauthorized", { status: 401 });
  draining = true;
  return jsonStatus({ ok: true, draining: true }, 200);
}
// routeEpoch increments whenever a rev change invalidates cached routes. Fetches
// capture it so a response that started before the invalidation cannot write
// stale data back into the cache (ADR-134; the WDL "epoch guard").
let routeEpoch = 0;
let lookupWindowAt = 0;
let lookupCount = 0;

async function pollRoutesOnce(env) {
  try {
    const headers = { "x-cellhive-internal-token": env.CELL_TOKEN };
    if (knownRev) headers["if-none-match"] = knownRev;
    const r = await fetch(env.CELL_URL + "/v1/control/routes", { headers });
    if (r.status === 304) {
      revOKAt = Date.now();
      revFailed = false;
      return true;
    }
    if (!r.ok) {
      revFailed = true;
      return false;
    }
    revOKAt = Date.now();
    revFailed = false;
    const rev = r.headers.get("etag") || "";
    try {
      if (r.body && typeof r.body.cancel === "function") r.body.cancel();
    } catch (e) { /* body already consumed */ }
    if (knownRev && rev && rev !== knownRev) {
      routeEpoch += 1;
      for (const e of hostCache.m.values()) {
        if (e) e.expiresAt = 0;
      }
    }
    knownRev = rev;
    return true;
  } catch (e) {
    // ignore: revocation then falls back to the per-host TTL
    revFailed = true;
    return false;
  }
}

function maybePollRoutes(env, ctx) {
  if (revInflight) return;
  const now = Date.now();
  if (now - revPolledAt < revPollMs(env)) return;
  revPolledAt = now;
  const p = pollRoutesOnce(env).finally(() => { revInflight = null; });
  revInflight = p;
  if (ctx && typeof ctx.waitUntil === "function") ctx.waitUntil(p.catch(() => {}));
}

// Live log-capture demand (ADR-238): native Tail is attached only while the
// worker has an unexpired tail subscription (or OTLP export covers every
// worker). The loader polls the cell-agent, and the resident worker variant is
// chosen per request, so enabling/disabling capture never needs a redeploy.
const LOG_CAPTURE_POLL_MS = 1000;
let captureMode = "tail";
let captureWorkers = new Set();
let capturePolledAt = 0;
let captureInflight = null;
let captureOK = false; // false until the first successful poll => legacy behavior

async function pollLogCaptureOnce(env) {
  try {
    const body = await readLogCapture(env);
    captureMode = body && (body.mode === "all" || body.mode === "off") ? body.mode : "tail";
    captureWorkers = new Set(body && Array.isArray(body.workers) ? body.workers : []);
    captureOK = true;
    return true;
  } catch (e) {
    return false;
  }
}

function maybePollLogCapture(env, ctx) {
  if (captureInflight) return;
  const now = Date.now();
  if (now - capturePolledAt < LOG_CAPTURE_POLL_MS) return;
  capturePolledAt = now;
  const p = pollLogCaptureOnce(env).finally(() => { captureInflight = null; });
  captureInflight = p;
  if (ctx && typeof ctx.waitUntil === "function") ctx.waitUntil(p.catch(() => {}));
}

function tailEnabled(env, ns, worker) {
  if (env.CH_LOG_CAPTURE === "0") return false;
  if (captureMode === "all") return true;
  if (!captureOK) return true; // endpoint unavailable: preserve legacy behavior
  return captureWorkers.has(ns + "/" + worker);
}

function residentVariantKey(versionKey, tailOn) {
  return versionKey + (tailOn ? ":t" : ":n");
}

function normalizeHost(host) {
  let h = String(host || "").trim().toLowerCase();
  const i = h.lastIndexOf(":");
  if (i > 0 && !h.slice(i).includes("]")) h = h.slice(0, i);
  return h.replace(/\.$/, "");
}

// hostView returns the cached pointer view for a host (null = known 404).
// Fresh entries are served locally; expired entries are served stale while a
// single background revalidation runs (stale-while-revalidate).
async function hostView(env, host, ctx) {
  const now = Date.now();
  const e = hostCache.get(host);
  if (e) {
    if (e.value === undefined) {
      // Cold miss already being fetched by another request: share it instead of
      // returning "unknown" (request coalescing).
      if (e.inflight) await e.inflight.catch(() => {});
      if (e.value === undefined && Date.now() >= backoffUntil) {
        // The entry was created by a cold lookup that failed. Retry once the
        // backoff window has passed instead of returning 503 forever.
        await revalidateHost(env, host, e).catch(() => {});
      }
      if (e.value === undefined) throw new Error("host view unavailable");
      return e.value;
    }
    if (now < e.expiresAt) return e.value;
    if ((!e.inflight || now - (e.inflightAt || 0) >= 5000) && now >= backoffUntil) {
      // Stale-while-revalidate. The refresh must be tied to ctx.waitUntil,
      // otherwise workerd cancels it after the response and the entry would stay
      // marked in-flight forever.
      const p = revalidateHost(env, host, e);
      if (ctx && typeof ctx.waitUntil === "function") ctx.waitUntil(p.catch(() => {}));
    }
    return e.value;
  }
  // Unknown-host throttle: a Host-scanning client would otherwise turn every
  // request into one upstream lookup (each unique host misses the cache).
  const t = Date.now();
  if (t - lookupWindowAt >= 1000) {
    lookupWindowAt = t;
    lookupCount = 0;
  }
  if (lookupCount >= hostLookupLimit(env)) return null;
  lookupCount++;
  const fresh = { value: undefined, etag: "", expiresAt: 0, inflight: null };
  hostCache.set(host, fresh);
  try {
    await revalidateHost(env, host, fresh);
  } catch (err) {
    // Do not leave a poisoned cold entry behind: the next request should retry
    // (subject to the global backoff) rather than get 503 until LRU eviction.
    hostCache.delete(host);
    throw err;
  }
  if (fresh.value === undefined) {
    hostCache.delete(host);
    throw new Error("host view unavailable");
  }
  return fresh.value;
}

// revalidateHost refreshes one host entry with a single in-flight request per
// host (request coalescing) and ETag revalidation.
function revalidateHost(env, host, e) {
  const epoch = routeEpoch;
  if (e.inflight && Date.now() - (e.inflightAt || 0) < 5000) return e.inflight;
  // Supersede hung flights before aborting them; late success/error/finally may
  // not mutate the new generation. Epoch additionally guards route invalidation.
  const generation = (e.generation || 0) + 1;
  e.generation = generation;
  if (e.controller) e.controller.abort();
  const controller = new AbortController();
  e.controller = controller;
  e.inflightAt = Date.now();
  let timer;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => { controller.abort(); reject(new Error("host view timeout")); }, 5000);
  });
  const current = () => e.generation === generation;
  const p = (async () => {
    try {
      const headers = { "x-cellhive-internal-token": env.CELL_TOKEN };
      if (e.etag) headers["if-none-match"] = e.etag;
      const read = (async () => {
        const r = await fetch(env.CELL_URL + "/v1/control/host?host=" + encodeURIComponent(host), { headers, signal: controller.signal });
        if (r.status === 304) return { unchanged: true };
        if (r.status !== 200 && r.status !== 404) throw new Error("host view " + r.status);
        return { etag: r.headers.get("etag") || "", value: r.status === 200 ? await r.json() : null };
      })();
      const result = await Promise.race([read, timeout]);
      if (!current()) return;
      noteSuccess();
      if (epoch !== routeEpoch) { e.expiresAt = 0; return; }
      if (!result.unchanged) { e.value = result.value; e.etag = result.etag; }
      e.expiresAt = Date.now() + hostTtlMs(env);
    } catch (err) {
      if (current()) { noteFailure(); e.expiresAt = backoffUntil; }
      throw err;
    } finally {
      clearTimeout(timer);
      if (current()) { e.inflight = null; e.controller = null; }
    }
  })();
  e.inflight = p;
  return p;
}

// workerEnv returns the immutable per-version env for a worker. Public fetch
// supplies the version from the host pointer. Service bindings do not have a
// pointer, so active lookups must revalidate after a short TTL.
async function workerEnv(env, ns, worker, wantVersion, wantSHA, wantStorageID) {
  const pinned = wantVersion !== undefined || !!wantSHA;
  // An explicit version number/SHA alone is not an incarnation identity: purge
  // can reuse the number (and identical source can have a new environment).
  // Legacy pointers without storage_id must revalidate, never cache forever.
  const cacheable = !pinned || !!wantStorageID;
  const key = JSON.stringify([ns, worker, pinned ? "pinned" : "active",
    wantVersion ?? null, wantSHA || "", wantStorageID || ""]);
  const e = envCache.get(key);
  if (cacheable && e && (pinned || Date.now() < e.activeExpiresAt)) return e.value;
  const running = envInflight.get(key);
  if (running) return running;
  const p = (async () => {
    try {
      let q = "?ns=" + encodeURIComponent(ns) + "&worker=" + encodeURIComponent(worker);
      // Numeric version is authoritative when both version and SHA are supplied.
      if (wantVersion !== undefined) q += "&version=" + encodeURIComponent(wantVersion);
      else if (wantSHA) q += "&sha=" + encodeURIComponent(wantSHA);
      const r = await fetch(env.CELL_URL + "/v1/control/worker" + q, {
        headers: { "x-cellhive-internal-token": env.CELL_TOKEN },
      });
      if (r.status === 404) {
        noteSuccess();
        // Never poison a different identity (nor retain a pinned negative).
        if (!pinned) envCache.set(key, { value: null, activeExpiresAt: Date.now() + hostTtlMs(env) });
        return null;
      }
      if (!r.ok) throw new Error("worker env " + r.status);
      const value = await r.json();
      if (!value || (wantVersion !== undefined && value.version !== wantVersion) ||
          (wantSHA && value.bundle_sha !== wantSHA) ||
          (wantStorageID && value.storage_id !== wantStorageID)) {
        const err = new Error("worker execution identity mismatch");
        err.identityMismatch = true;
        throw err;
      }
      noteSuccess();
      if (cacheable) envCache.set(key, { value, activeExpiresAt: pinned ? 0 : Date.now() + hostTtlMs(env) });
      return value;
    } catch (err) {
      // A healthy response for a different identity must not postpone the host
      // refresh needed to resolve that mismatch on the next request.
      if (!err.identityMismatch) noteFailure();
      // A resolved immutable pointer may never silently execute another version.
      // Active lookups also fail closed after their TTL rather than hiding errors.
      throw err;
    } finally {
      envInflight.delete(key);
    }
  })();
  envInflight.set(key, p);
  return p;
}

// matchPath picks the longest path prefix among the host's routes.
// routePrefix normalizes a route path for matching: "" and "/" mean the whole
// host, trailing slashes are ignored.
function routePrefix(rt) {
  const raw = (rt && rt.path) || "";
  if (raw === "/" || raw === "") return "";
  return raw.replace(/\/+$/, "");
}

// matchPath returns the longest route whose path prefix matches on a segment
// boundary ("/api" matches "/api" and "/api/x" but not "/apix").
function matchPath(routes, pathname) {
  let best = null;
  let bestLen = -1;
  for (const rt of routes || []) {
    const p = routePrefix(rt);
    if (p && !(pathname === p || pathname.startsWith(p + "/"))) continue;
    if (p.length > bestLen) {
      best = rt;
      bestLen = p.length;
    }
  }
  return best;
}

// stripRoutePrefix removes the matched route's prefix from the request path
// (query/host untouched). Always applied: a route is a mount point.
function stripRoutePrefix(req, route) {
  if (!route) return req;
  const p = routePrefix(route);
  if (!p) return req;
  const url = new URL(req.url);
  let rest = null;
  if (url.pathname === p) rest = "/";
  else if (url.pathname.startsWith(p + "/")) rest = url.pathname.slice(p.length);
  if (rest === null) return req;
  url.pathname = rest;
  return new Request(url.toString(), req);
}

async function bundle(env, sha) {
  const cached = bundleCache.get(sha);
  if (cached !== undefined) return cached;
  const running = bundleInflight.get(sha);
  if (running) return running;
  const p = (async () => {
    try {
      const q = "?sha=" + encodeURIComponent(sha);
      const headers = { "x-cellhive-internal-token": env.CELL_TOKEN };
      const signed = await fetch(env.CELL_URL + "/v1/internal/bundle-url" + q, { headers });
      let r;
      if (signed.ok) {
        const { url } = await signed.json();
        if (url && !url.startsWith("file:")) r = await fetch(url);
      } else if (signed.status !== 404 && signed.status !== 501) {
        throw new Error("GET bundle URL " + sha + " -> " + signed.status);
      }
      if (!r) r = await fetch(env.CELL_URL + "/v1/internal/bundle" + q, { headers });
      if (!r.ok) throw new Error("GET bundle " + sha + " -> " + r.status);
      const src = await r.text();
      bundleCache.set(sha, src);
      return src;
    } finally {
      bundleInflight.delete(sha);
    }
  })();
  bundleInflight.set(sha, p);
  return p;
}

// hyperdriveConnString resolves a hyperdrive binding to its origin connection
// string: an inline URL wins (back-compat with --hyperdrive NAME=URL), otherwise
// the reference names a registered hyperdrive resource. Cached per ns/ref (cold
// path: bindingSpec runs once per loaded worker version). A missing resource
// omits the binding rather than injecting an empty credential.
const hyperdriveSecrets = new Map();
const HYPERDRIVE_CACHE_MAX = 64;

async function hyperdriveConnString(env, ns, ref, name) {
  ref = (ref || "").trim();
  if (!ref) return "";
  if (ref.includes("://")) return ref;
  const key = ns + "/" + ref;
  if (hyperdriveSecrets.has(key)) return hyperdriveSecrets.get(key);
  let cs = "";
  const candidates = ref === name ? [ref] : [ref, name];
  for (const c of candidates) {
    try {
      const r = await fetch(
        env.CELL_URL.replace(/\/$/, "") + "/v1/internal/hyperdrive?ns=" + encodeURIComponent(ns) + "&name=" + encodeURIComponent(c),
        { headers: { "x-cellhive-internal-token": env.CELL_TOKEN } },
      );
      if (r.ok) {
        const j = await r.json();
        cs = j.connection_string || "";
        if (cs) break;
      }
    } catch (e) {
      /* try the next candidate */
    }
  }
  if (!cs) {
    // Do not cache a failure: a transient cell-agent miss must be retried on the
    // next load rather than omitting the binding until cache eviction.
    console.error("hyperdrive: no origin connection string for " + ns + "/" + ref + "; binding omitted");
    return "";
  }
  if (hyperdriveSecrets.size >= HYPERDRIVE_CACHE_MAX) hyperdriveSecrets.delete(hyperdriveSecrets.keys().next().value);
  hyperdriveSecrets.set(key, cs);
  return cs;
}

async function bindingSpec(env, app, worker, version, classStorage, deletedClasses) {
  const spec = {};
  if (version.assets_sha && version.assets && version.assets.binding) {
    spec[version.assets.binding] = { kind: "assets", ns: app.namespace, worker,
      token: version.assets_sha, config: version.assets };
  }
  const deleted = new Set(deletedClasses || []);
  for (const b of version.bindings || []) {
    if (b.type === "do") {
      const cls = b.id || b.name;
      spec[b.name] = {
        kind: "do", ns: app.namespace, worker, bundle_sha: version.bundle_sha,
        version: version.version,
        direct: env.CH_DO_DIRECT !== "0",
        storage_id: version.storage_id || "", class: cls,
        storage_class: (classStorage && classStorage[cls]) || cls,
        deleted: deleted.has(cls),
        name: b.name,
        token: await scopedToken(env.SCOPE_SECRET, app.namespace, "do", b.name),
      };
      continue;
    }
    if (b.type === "ai") {
      spec[b.name] = { kind: "ai", ns: app.namespace, name: b.name };
      continue;
    }
    if (b.type === "hyperdrive") {
      // The binding id is either an inline origin URL or the name of a
      // registered hyperdrive resource (origin URL sealed in the control plane,
      // ADR-129). Resolution is a cold-path call, cached per binding ref.
      const cs = await hyperdriveConnString(env, app.namespace, b.id || b.name, b.name);
      if (cs) {
        spec[b.name] = { kind: "hyperdrive", ns: app.namespace, name: b.name, connectionString: cs };
      }
      continue;
    }
    if (b.type === "service") {
      // "worker" is same-namespace; "ns/worker" is a cross-namespace target
      // (ADR-144): props carry both so scoped-token stays caller-bound.
      const raw = b.id || b.name;
      let targetNS = app.namespace;
      let target = raw;
      const slash = raw.indexOf("/");
      if (slash > 0 && slash < raw.length - 1) {
        targetNS = raw.slice(0, slash);
        target = raw.slice(slash + 1);
      }
      let targetVersion = "";
      try {
        const tv = await workerEnv(env, targetNS, target);
        if (tv) targetVersion = tv.bundle_sha;
      } catch (e) {
        targetVersion = "";
      }
      spec[b.name] = {
        kind: "service", ns: targetNS, caller_ns: app.namespace, name: b.name, target,
        entrypoint: b.entrypoint || "",
        // Prefer the version pinned at deploy (ADR-104); fall back to the
        // target's active version resolved at render (ADR-102). Empty -> HTTP.
        version: b.version || targetVersion,
        token: await scopedToken(env.SCOPE_SECRET, app.namespace, "service", b.name),
      };
      continue;
    }
    if (b.type === "queue") {
      // The env key is arbitrary; the queue resource is the binding ID.
      const qname = b.id || b.name;
      spec[b.name] = {
        kind: "queue", ns: app.namespace, name: qname,
        token: await scopedToken(env.SCOPE_SECRET, app.namespace, "queue", qname),
      };
      continue;
    }
    if (b.type === "vectorize") {
      // The env key is the binding name; the resource (and scoped token) is the
      // index name (ADR-158).
      const index = b.id || b.name;
      spec[b.name] = {
        kind: "vectorize", ns: app.namespace, name: b.name, index,
        token: await scopedToken(env.SCOPE_SECRET, app.namespace, "vectorize", index),
      };
      continue;
    }
    if (b.type !== "kv" && b.type !== "d1" && b.type !== "r2" && b.type !== "queue" && b.type !== "workflow") continue;
    // D1 and R2 bindings have an independent env key (`name`) and resource id
    // (`id`: database_id / bucket_name). The runtime must address the resource,
    // not the env key: wrangler commonly uses binding="DB" or "BUCKET" with a
    // different database_id/bucket_name.
    const raw = (b.type === "d1" || b.type === "r2") ? (b.id || b.name) : b.name;
    // Native D1 deployments accepted a full cell scope as the id; the wire
    // protocol wants the cell id, not "ns/__d1__/DB".
    const resource = b.type === "d1" && raw.includes("/") ? raw.slice(raw.lastIndexOf("/") + 1) : raw;
    spec[b.name] = {
      kind: b.type,
      ns: app.namespace,
      // KV keeps the declared binding key in name for the control ACL;
      // resource is the resolved cell identifier used for data routing.
      name: b.type === "kv" ? b.name : resource,
      token: await scopedToken(
        env.SCOPE_SECRET,
        app.namespace,
        b.type,
        resource,
        b.type === "kv" && b.id ? {
          worker,
          version: version.version,
          resource: b.id,
        } : null,
      ),
    };
  }
  return spec;
}

// scopedToken computes the binding capability token locally from the shared
// secret (ADR-074): HMAC-SHA256 over canonical JSON {ns,kind,name[,resource]} (no expiry).
// Resource is the resolved cell ID used for routing; name remains the binding key.
// The platform loader holds the secret; only the computed token reaches a
// loaded worker. Byte-identical to Go scopedtoken.Mint (verified by tests).
async function scopedToken(secret, ns, kind, name, pinned = null) {
  const claims = {ns, kind, name};
  if (pinned) Object.assign(claims, pinned);
  const payload = JSON.stringify(claims);
  const key = await crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode(secret || ""),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  const sig = await crypto.subtle.sign("HMAC", key, new TextEncoder().encode(payload));
  return b64url(new TextEncoder().encode(payload)) + "." + b64url(new Uint8Array(sig));
}

function b64url(bytes) {
  let s = "";
  for (const b of bytes) s += String.fromCharCode(b);
  return btoa(s).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}


function text(msg, status) {
  return new Response(msg + "\n", { status, headers: { "content-type": "text/plain" } });
}
