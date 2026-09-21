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
export { KV, D1Database, R2Bucket, QueueProducer, ServiceBinding, AI, Hyperdrive , DurableObjectNamespace, WorkflowBinding, Vectorize, PlatformBridge } from "bindings.js";

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
// envCache: "ns/worker" -> { version, value(immutable view) }
const envCache = new LRU(2000);
const envInflight = new Map(); // "ns/worker@version" -> Promise
const bundleCache = new LRU(2000); // sha -> source
const bundleInflight = new Map(); // sha -> Promise
const assetCache = new LRU(4000); // "ns/worker/token/path" -> Uint8Array | null
const metaCache = new LRU(4000); // "ns/worker/token/_redirects|_headers" -> parsed

export default {
  async fetch(req, env, ctx) {
    const url = new URL(req.url);
    if (url.pathname === "/healthz") return new Response("ok");
    // Rolling-update probes (ADR-156): /ready reports projection freshness and
    // draining so the edge can pull this instance out before it stops; /drain
    // (internal token) flips that flag.
    if (url.pathname === "/ready") return readyResponse(env);
    if (url.pathname === "/drain" && req.method === "POST") return drainRequest(req, env);
    maybePollRoutes(env, ctx);

    let view;
    try {
      view = await hostView(env, normalizeHost(req.headers.get("host") || url.host), ctx);
    } catch (e) {
      return text("routing lookup unavailable", 503);
    }
    if (!view || !view.routes || view.routes.length === 0) {
      return text("no route for host " + normalizeHost(req.headers.get("host") || url.host), 404);
    }
    const route = matchPath(view.routes, url.pathname);
    if (!route) return text("no route for host " + normalizeHost(req.headers.get("host") || url.host), 404);
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
      version = await workerEnv(env, route.ns, route.worker, ptr.version);
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
          if (wres.status !== 404) return wres;
          const served = await serveAsset(effReq, env, app, route.worker, version, effURL);
          return served || wres;
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

import { traceparent as newTraceparent, startSpan, endSpan, flushSpans } from "telemetry.js";

// --- worker ---------------------------------------------------------------

// withTraceparent ensures the request handed to the tenant carries a W3C
// traceparent, generating one when the client did not (ADR-146).
function withTraceparent(req, env) {
  const existing = req.headers.get("traceparent");
  if (existing) {
    const m = /^[0-9a-fA-F]{2}-[0-9a-fA-F]{32}-[0-9a-fA-F]{16}-([0-9a-fA-F]{2})/.exec(existing.trim());
    globalThis.__cellhiveTraceparent = existing;
    globalThis.__cellhiveTraceSampled = !!(m && (parseInt(m[1], 16) & 1));
    return req;
  }
  const ratio = Number(env.CH_TRACE_RATIO);
  const sample = Number.isFinite(ratio) && Math.random() < ratio;
  const tp = newTraceparent(sample);
  globalThis.__cellhiveTraceparent = tp;
  globalThis.__cellhiveTraceSampled = sample;
  const r = new Request(req);
  r.headers.set("traceparent", tp);
  return r;
}

async function runWorker(req, env, ctx, app, worker, version, classStorage, deletedClasses) {
  let source;
  try {
    source = await bundle(env, version.bundle_sha);
  } catch (e) {
    return text("bundle fetch failed: " + e, 502);
  }
  let spec;
  try {
    spec = await bindingSpec(env, app, worker, version, classStorage, deletedClasses);
  } catch (e) {
    return text("binding setup failed: " + e, 502);
  }
  const stub = workerStub(env, ctx, app, worker, version, source, spec);
  const traced = cleanRequest(withTraceparent(req, env));
  const span = startSpan("http.server", {
    kind: "server",
    "http.method": req.method,
    "cellhive.namespace": app.namespace,
    "cellhive.worker": worker,
  });
  try {
    const res = await stub.getEntrypoint("CellHiveHost").handleFetch(traced);
    endSpan(span, { code: res.status >= 500 ? 2 : 0 });
    await flushSpans(env);
    return res;
  } catch (e) {
    endSpan(span, { code: 2, message: String(e) });
    await flushSpans(env);
    return text("worker error: " + e, 500);
  }
}

// workerStub loads a worker version through the shared workerLoader in this
// workerd instance and returns its stub. Used both to serve a request and, via
// the service loader below, to run a service binding target natively (ADR-102).
function workerStub(env, ctx, app, worker, version, source, spec) {
  // The loader id keys the cached loaded worker (and therefore its isolate,
  // module state and connection pools). It must include the VERSION NUMBER and
  // not just the bundle sha: a binding/vars-only deploy keeps the same sha but
  // is a new version, and reusing the old isolate would keep the old env.
  const id = `${app.namespace}/${worker}@${version.version}-${version.bundle_sha}`;
  return env.LOADER.get(id, () => ({
    compatibilityDate: version.compat_date || "2026-06-15",
    compatibilityFlags: version.compat_flags || [],
    mainModule: "wrapper.js",
    modules: {
      "wrapper.js": platformConsts(env, spec) + env.WRAPPER_SRC,
      "tenant.js": source,
      "facades.js": env.FACADES_SRC,
      "rpc-codec.js": env.RPC_CODEC_SRC,
    },
    // ADR-090: migrated bindings become props-bound entrypoint stubs in env;
    // all kinds are platform-side stubs (ADR-184), so the wrapper only wraps
    // R2 (local metadata) and DO (WebSocket) bindings.
    env: tenantEnv(env, ctx, spec, version.vars, app.namespace, worker),
    globalOutbound: env.OUTBOUND,
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
    const src = await bundle(env, pinned);
    version = pinnedEnv;
    const app0 = { namespace: target.ns };
    const spec0 = await bindingSpec(env, app0, target.worker, version, version.class_storage || {}, version.deleted_classes || []);
    return workerStub(env, ctx, app0, target.worker, version, src, spec0);
  }
  const app = { namespace: target.ns };
  const source = await bundle(env, version.bundle_sha);
  const spec = await bindingSpec(env, app, target.worker, version, version.class_storage || {}, version.deleted_classes || []);
  return workerStub(env, ctx, app, target.worker, version, source, spec);
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

// platformConsts renders the platform-only credentials as a module-scope const
// appended to a wrapper module's source. It is how platform JS (queue/workflow
// wrappers, do-runtime bindings-wrapper) gets the internal token WITHOUT it
// ever entering the tenant env object (ADR-074: the internal token must not be
// visible to tenant code). Module scope is isolated per module, so tenant.js
// cannot read a const defined in wrapper.js — unlike `env`, which is shared.
function platformConsts(env, spec) {
  // PREPENDED to the wrapper source: the wrapper's top-level code runs before
  // any later statement, so the const must exist before the try block (a const
  // declared after use is in the temporal dead zone).
  const names = (kind) => Object.entries(spec || {}).filter(([, b]) => b && b.kind === kind).map(([n]) => n);
  return `;const __cellhivePlatform = Object.freeze({ cellUrl: ${JSON.stringify(env.CELL_URL || "")}, cellToken: ${JSON.stringify(env.CELL_TOKEN || "")}, r2Bindings: ${JSON.stringify(names("r2"))}, doBindings: ${JSON.stringify(names("do"))} });\n`;
}

// --- assets ---------------------------------------------------------------

function shouldRunWorkerFirst(cfg, pathname) {
  if (cfg.run_worker_first) return true;
  for (const p of cfg.run_worker_first_paths || []) {
    if (!p) continue;
    if (p.endsWith("*") ? pathname.startsWith(p.slice(0, -1)) : pathname === p || pathname.startsWith(p + "/")) {
      return true;
    }
  }
  return false;
}

async function serveAsset(req, env, app, worker, version, url) {
  const token = version.assets_sha;
  const pathname = decodeURIComponent(url.pathname);

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

// assetResponse serves one asset. `pathname` is the file to serve; `matchPath`
// (defaults to it) is the request path used for _headers rule matching, which by
// CF semantics follows the request, not the served file (SPA/200-rewrite case).
async function assetResponse(req, env, app, worker, token, pathname, status, matchPath) {
  for (const candidate of assetCandidates(pathname)) {
    const bytes = await asset(env, app, worker, token, candidate);
    if (!bytes) continue;
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
  const r = await fetch(env.CELL_URL + "/v1/internal/asset?" + q, {
    headers: { "x-cellhive-internal-token": env.CELL_TOKEN },
  });
  if (r.status === 404) {
    assetCache.set(key, null);
    return null;
  }
  if (!r.ok) throw new Error("GET asset " + path + " -> " + r.status);
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
    if (!e.inflight && now >= backoffUntil) {
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
  if (e.inflight) {
    // A cancelled background refresh would otherwise leave the entry unusable.
    if (Date.now() - (e.inflightAt || 0) < 5000) return e.inflight;
    e.inflight = null;
  }
  e.inflightAt = Date.now();
  e.inflight = (async () => {
    try {
      const headers = { "x-cellhive-internal-token": env.CELL_TOKEN };
      if (e.etag) headers["if-none-match"] = e.etag;
      const r = await fetch(env.CELL_URL + "/v1/control/host?host=" + encodeURIComponent(host), { headers });
      if (r.status === 304) {
        noteSuccess();
        if (epoch === routeEpoch) e.expiresAt = Date.now() + hostTtlMs(env);
        return;
      }
      if (r.status !== 200 && r.status !== 404) throw new Error("host view " + r.status);
      const etag = r.headers.get("etag") || "";
      const value = r.status === 200 ? await r.json() : null; // 404 -> negative entry
      noteSuccess();
      if (epoch !== routeEpoch) {
        // A rev change landed while this fetch was in flight: the response may
        // predate it. Leave the entry expired so the next request refetches.
        e.expiresAt = 0;
        return;
      }
      e.value = value;
      e.etag = etag;
      e.expiresAt = Date.now() + hostTtlMs(env);
    } catch (err) {
      // stale-on-error: keep the last known value; back off before retrying so
      // an unavailable cell-agent is not hit once per request.
      noteFailure();
      e.expiresAt = backoffUntil;
      throw err;
    } finally {
      e.inflight = null;
    }
  })();
  return e.inflight;
}

// workerEnv returns the immutable per-version env for a worker. wantVersion is
// the version number from the host pointer; a mismatch (redeploy, including
// binding-only deploys) refetches. Entries carry no TTL: a version never
// changes, and the pointer tells us when it is superseded.
async function workerEnv(env, ns, worker, wantVersion, wantSHA) {
  const key = ns + "/" + worker;
  const e = envCache.get(key);
  const matches = (entry) => {
    if (!entry) return false;
    if (wantSHA) return entry.value && entry.value.bundle_sha === wantSHA;
    if (wantVersion !== undefined) return entry.version === wantVersion;
    return true;
  };
  if (matches(e)) return e.value;
  const inflightKey = key + "@" + (wantVersion === undefined ? "" : wantVersion) + (wantSHA ? "#" + wantSHA : "");
  const running = envInflight.get(inflightKey);
  if (running) return running;
  const p = (async () => {
    try {
      let q = "?ns=" + encodeURIComponent(ns) + "&worker=" + encodeURIComponent(worker);
      if (wantSHA) q += "&sha=" + encodeURIComponent(wantSHA);
      else if (wantVersion !== undefined) q += "&version=" + encodeURIComponent(wantVersion);
      const r = await fetch(env.CELL_URL + "/v1/control/worker" + q, {
        headers: { "x-cellhive-internal-token": env.CELL_TOKEN },
      });
      if (r.status === 404) {
        noteSuccess();
        envCache.set(key, { version: wantVersion || 0, value: null });
        return null;
      }
      if (!r.ok) throw new Error("worker env " + r.status);
      const value = await r.json();
      noteSuccess();
      const current = envCache.get(key);
      // Versions are monotonic: an older in-flight fetch must not overwrite a
      // newer version's env (ADR-134).
      if (!current || current.version === undefined || value.version >= current.version) {
        envCache.set(key, { version: value.version, value });
      }
      return value;
    } catch (err) {
      noteFailure();
      if (e) return e.value; // stale-on-error
      throw err;
    } finally {
      envInflight.delete(inflightKey);
    }
  })();
  envInflight.set(inflightKey, p);
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
      const r = await fetch(env.CELL_URL + "/v1/internal/bundle?sha=" + encodeURIComponent(sha), {
        headers: { "x-cellhive-internal-token": env.CELL_TOKEN },
      });
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
    spec[b.name] = {
      kind: b.type,
      ns: app.namespace,
      name: b.name,
      token: await scopedToken(env.SCOPE_SECRET, app.namespace, b.type, b.name),
    };
  }
  return spec;
}

// scopedToken computes the binding capability token locally from the shared
// secret (ADR-074): HMAC-SHA256 over canonical JSON {ns,kind,name} (no expiry).
// The platform loader holds the secret; only the computed token reaches a
// loaded worker. Byte-identical to Go scopedtoken.Mint (verified by tests).
async function scopedToken(secret, ns, kind, name) {
  const payload = `{"ns":${JSON.stringify(ns)},"kind":${JSON.stringify(kind)},"name":${JSON.stringify(name)}}`;
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

// Strip platform-only headers before tenant code runs.
function cleanRequest(req) {
  const headers = new Headers(req.headers);
  for (const k of [...headers.keys()]) {
    if (k.toLowerCase().startsWith("x-cellhive-")) headers.delete(k);
  }
  return new Request(req, { headers });
}

function text(msg, status) {
  return new Response(msg + "\n", { status, headers: { "content-type": "text/plain" } });
}
