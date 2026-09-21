// CellHive do-runtime — native Durable Object host with ownership fence (ADR-077/078).
//
// A host actor (class Host) runs tenant Durable Objects as facets: the tenant DO
// class is loaded from the immutable bundle via workerLoader
// (getDurableObjectClass lives on the loaded WorkerStub), and facets.get(...)
// gives a stub to the tenant object. Storage is workerd's local-disk SQLite.
//
// Ownership follows WDL's design (docs/modules/durable-objects): a class's
// objects are sharded across host actors; a single owner lease with a monotonic
// generation (epoch) fences stale owners. CellHive stores the lease in the
// object store via cell-agent (not Redis). A pre-dispatch guard renews when the
// lease is nearly expired and fails closed (owner_unavailable) otherwise.
//
// Internal endpoints (internal role token):
//   POST /v1/do/invoke   { namespace, worker, bundle_sha, class, id, request? }
//   POST /v1/do/renew    renew all held leases
//   POST /v1/do/drain    stop new work, wait in-flight, release leases

import { DurableObject } from "cloudflare:workers";
import { bindingStub } from "bindings.js";
import {
  decode, encode, encodedSize, MAX_RPC_BYTES,
  RPC_METHOD_RE, RPC_RESERVED_METHODS,
} from "rpc-codec.js";
import { startSpan, endSpan, flushSpans } from "telemetry.js";

export { KV, D1Database, R2Bucket, QueueProducer, ServiceBinding, AI , DurableObjectNamespace, WorkflowBinding, WorkflowSteps, Vectorize, LogSink } from "bindings.js";

const SHARD_COUNT = 16;
// Owner lease TTL. Longer TTLs reduce renew churn and tolerate a slow/partitioned
// cell-agent; shorter TTLs fail over faster. WDL uses 120s for DO ownership.
const LEASE_TTL_S = 30;
function leaseTtlS(env) {
  const n = Number(env && env.DO_LEASE_S);
  return Number.isFinite(n) && n > 0 ? n : LEASE_TTL_S;
}
const GUARD_MS = 3000;
// In-memory registry/bundle cache bounds: the durable object index and the
// bucket bundle are the authorities; these caches only avoid re-work.
const SEEN_MAX = 10000;
const BUNDLE_CACHE_MAX = 64;
const DRAIN_WAIT_MS = 8000;
const HINT_TTL_MS = 30000;

const claims = new Map(); // scopeKey -> { epoch, expiryMs, spec, shard }
const hints = new Map(); // scopeKey -> { address, expiry } (owner hint, ADR-080)
const boundRouters = new Set(); // host_id reported to the supervisor (per isolate)
const boundActors = new Set(); // host_hash reported to the supervisor (per isolate)
const seen = new Map(); // objectKey -> { namespace, worker, class, storage_class, storage_id, shard, id } (registry, ADR-082)
let inflight = 0;
let draining = false;

// do-runtime metrics (ADR-166), reported best-effort to the supervisor after
// each invocation. Counters are deltas since the last report; wsNet is
// opens - closes so the supervisor can keep a gauge.
const doStats = { alarmsOk: 0, alarmsError: 0, gateTimeouts: 0 };
let wsNet = 0;
const doStatsReported = { alarmsOk: 0, alarmsError: 0, gateTimeouts: 0, wsNet: 0 };

async function reportDoStats(env) {
  const base = env.GATE_URL || "";
  if (!base) return;
  const delta = {
    alarms_ok: doStats.alarmsOk - doStatsReported.alarmsOk,
    alarms_error: doStats.alarmsError - doStatsReported.alarmsError,
    gate_timeouts: doStats.gateTimeouts - doStatsReported.gateTimeouts,
    ws_sessions: wsNet - doStatsReported.wsNet,
  };
  doStatsReported.alarmsOk = doStats.alarmsOk;
  doStatsReported.alarmsError = doStats.alarmsError;
  doStatsReported.gateTimeouts = doStats.gateTimeouts;
  doStatsReported.wsNet = wsNet;
  if (!delta.alarms_ok && !delta.alarms_error && !delta.gate_timeouts && !delta.ws_sessions) return;
  try {
    await fetch(base.replace(/\/$/, "") + "/internal/do/stats", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(delta),
    });
  } catch (e) {
    /* best-effort; the next invocation re-reports */
  }
}

// GET /ready is the readiness/drain probe for rolling updates (ADR-156): 503
// while draining (POST /v1/do/drain) or while cell-agent is unreachable.
async function ready(env) {
  if (draining) return json({ ready: false, reason: "draining" }, 503);
  try {
    const r = await fetch(String(env.CELL_URL).replace(/\/$/, "") + "/readyz", { method: "GET" });
    if (!r.ok) return json({ ready: false, reason: "cell_unreachable" }, 503);
  } catch (e) {
    return json({ ready: false, reason: "cell_unreachable" }, 503);
  }
  return json({ ready: true, projection_age_ms: 0, cell: "ok" });
}

export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    if (url.pathname === "/healthz") return new Response("ok");
    if (url.pathname === "/ready") return ready(env);
    const auth = await authorize(req, env);
    if (!auth.ok) return json({ error: "unauthorized" }, 401);
    // An owner ticket (ADR-105) is a narrow capability: it authorizes
    // POST /v1/do/invoke and the GET /v1/do/connect upgrade for its own shard;
    // every operator/lifecycle route needs the internal token.
    const ticketRoute = req.method === "POST" && url.pathname === "/v1/do/invoke"
      || req.method === "GET" && url.pathname === "/v1/do/connect";
    if (auth.ticket && !ticketRoute) {
      return json({ error: "forbidden", scope: "invoke_only" }, 403);
    }
    if (req.method === "GET" && url.pathname === "/v1/do/connect") return connect(req, env, url, auth.ticket);
    if (req.method === "GET" && url.pathname === "/v1/do/objects") return listObjects();
    if (req.method !== "POST") return new Response("not found", { status: 404 });
    switch (url.pathname) {
      case "/v1/do/invoke":
        return invoke(req, env, auth.ticket);
      case "/v1/do/abort":
        return abortFacet(req, env);
      case "/v1/do/delete":
        return deleteFacet(req, env);
      case "/v1/do/restart":
        return restartObjects(req, env);
      case "/v1/do/renew":
        return renewAll(env);
      case "/v1/do/drain":
        return drain(env);
      default:
        return new Response("not found", { status: 404 });
    }
  },
};

// indexObject mirrors one object into the durable bucket index (ADR-108). Only
// active when DO_OBJECT_INDEX=1. Failures never break the caller: the index is a
// cold-start accelerator, the runtime registry stays authoritative while up.
async function indexObject(env, o, remove) {
  if (env.DO_OBJECT_INDEX !== "1" || !env.CELL_URL) return;
  const base = env.CELL_URL.replace(/\/$/, "") + "/v1/internal/do/objects";
  try {
    if (remove) {
      await fetch(base + "?ns=" + encodeURIComponent(o.namespace) + "&worker=" + encodeURIComponent(o.worker) +
        "&class=" + encodeURIComponent(o.class) + "&shard=" + o.shard + "&name=" + encodeURIComponent(o.id), {
        method: "DELETE", headers: { "x-cellhive-internal-token": env.CELL_TOKEN },
      });
    } else {
      await fetch(base, {
        method: "POST",
        headers: { "x-cellhive-internal-token": env.CELL_TOKEN, "content-type": "application/json" },
        body: JSON.stringify({
          namespace: o.namespace, worker: o.worker, class: o.storage_class || o.class,
          shard: o.shard, name: o.id,
        }),
      });
    }
  } catch (e) { /* best effort */ }
}

async function invoke(req, env, ticket) {
  if (draining) return json({ error: "draining" }, 503);
  let spec;
  try {
    spec = await req.json();
  } catch (e) {
    return json({ error: "bad_json", message: String(e) }, 400);
  }
  const { namespace, worker, class: cls, id, bundle_sha } = spec;
  if (!namespace || !worker || !cls || !id || !bundle_sha) {
    return json({ error: "bad_request", message: "namespace, worker, class, id, bundle_sha are required" }, 400);
  }
  const shard = fnv1a(`${namespace}/${worker}/${spec.storage_class || cls}/${id}`) % SHARD_COUNT;
  const indexKey = `${spec.storage_id || `${namespace}/${worker}`}/${spec.storage_class || cls}/${shard}/${id}`;
  const firstSight = !seen.has(indexKey);
  if (firstSight && seen.size >= SEEN_MAX) {
    // Bound the in-memory object registry; the durable index
    // (/v1/internal/do/objects) is the authority for cold enumeration.
    const oldest = seen.keys().next().value;
    seen.delete(oldest);
  }
  seen.set(indexKey, {
    namespace, worker, class: cls, storage_class: spec.storage_class || cls,
    storage_id: spec.storage_id || "", shard, id,
  });
  if (firstSight) {
    await indexObject(env, { namespace, worker, class: cls, storage_class: spec.storage_class || cls, shard, id }, false);
  }
  // Defense in depth: a ticket must match this exact shard/scope.
  if (ticket) {
    const sc = spec.storage_class || cls;
    if (ticket.ns !== namespace || ticket.worker !== worker || (ticket.storage_class || "") !== sc || ticket.shard !== shard) {
      return json({ error: "forbidden", scope: "ticket_mismatch" }, 403);
    }
  }
  // Lease/hint identity follows the storage identity (class alias), so an aliased
  // class cannot hold two leases for one storage (ADR-082/135).
  const storageClass = spec.storage_class || cls;
  const key = `${namespace}/${worker}/${storageClass}/shard${shard}`;
  const now = Date.now();

  // Owner hint (ADR-080): another task owns this shard; forward once (safe
  // pre-dispatch) instead of claiming.
  const hint = hints.get(key);
  if (hint && hint.expiry > now) return forward(env, hint.address, spec);
  if (hint) hints.delete(key);

  const ensured = await ensureClaim(env, spec, shard);
  if (ensured && ensured.forwardTo) {
    hints.set(key, { address: ensured.forwardTo, expiry: now + HINT_TTL_MS });
    return forward(env, ensured.forwardTo, spec);
  }
  if (ensured && ensured.resp) return ensured.resp; // fail closed (pre-dispatch)

  const hostIdName = `${spec.storage_id || `${namespace}/${worker}`}/shard${shard}`;
  await reportRouterBinding(env, hostIdName, {
    host_id: hostIdName,
    storage_id: spec.storage_id || "",
    class: spec.storage_class || cls,
    namespace, worker,
  });
  const hostId = env.HOST.idFromName(hostIdName);
  const host = env.HOST.get(hostId);
  inflight++;
  try {
    const res = await host.fetch("http://host/invoke", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ ...spec, shard }),
    });
    // Dispatch finished; if we lost ownership during execution the outcome is
    // unknown to the caller (do not blindly replay) — ADR-080.
    const c = claims.get(key);
    if (!c || c.expiryMs < Date.now()) {
      return json({ error: "result_unknown", reason: "ownership_lost_after_dispatch" }, 409);
    }
    const body = await res.text();
    const hdrs = { "content-type": res.headers.get("content-type") || "text/plain" };
    // Preserve the tenant-response marker through the sharding wrapper so the
    // caller facade can return 4xx/5xx Durable Object responses.
    if (res.headers.get("x-cellhive-do-app") === "1") hdrs["x-cellhive-do-app"] = "1";
    // Tell upstreams that this runtime is the owner, so they can cache a hint
    // and skip the sharding router next time (ADR-103).
    if (env.ADVERTISE) hdrs["x-cellhive-do-owner"] = String(env.ADVERTISE);
    return new Response(body, { status: res.status, headers: hdrs });
  } catch (e) {
    return json({ error: "invoke_failed", message: String(e) }, 500);
  } finally {
    inflight--;
  }
}

// forward sends the invoke to the owner task; a transport failure after the
// request may have been applied is reported as result_unknown (ADR-080).
async function forward(env, address, spec) {
  try {
    const r = await fetch(String(address).replace(/\/$/, "") + "/v1/do/invoke", {
      method: "POST",
      headers: { "x-cellhive-internal-token": env.CELL_TOKEN, "content-type": "application/json" },
      body: JSON.stringify(spec),
    });
    const hdrs = { "content-type": r.headers.get("content-type") || "text/plain" };
    if (r.headers.get("x-cellhive-do-app") === "1") hdrs["x-cellhive-do-app"] = "1";
    const owner = r.headers.get("x-cellhive-do-owner");
    if (owner) hdrs["x-cellhive-do-owner"] = owner; // propagate the owner hint
    return new Response(await r.text(), { status: r.status, headers: hdrs });
  } catch (e) {
    return json({ error: "result_unknown", reason: "forward_transport_failed" }, 409);
  }
}

// ensureClaim returns {forwardTo} when another task owns the shard (with an
// address to forward to), {resp} on other ownership failures, else null.
async function ensureClaim(env, spec, shard) {
  const storageClass = spec.storage_class || spec.class;
  const key = `${spec.namespace}/${spec.worker}/${storageClass}/shard${shard}`;
  const c = claims.get(key);
  const now = Date.now();
  if (!c) {
    const r = await post(env, "/v1/internal/do/claim", {
      namespace: spec.namespace, worker: spec.worker, class: storageClass, shard,
      node: env.NODE_ID, advertise: env.ADVERTISE, ttl_seconds: leaseTtlS(env),
    });
    if (r.status === 409) {
      let address = "";
      try {
        address = (await r.json()).address || "";
      } catch (e) {
        /* no address */
      }
      if (address) return { forwardTo: address };
      return { resp: json({ error: "owner_unavailable", reason: "owner_live" }, 409) };
    }
    if (!r.ok) return { resp: json({ error: "owner_unavailable", reason: "claim_" + r.status }, 503) };
    const o = await r.json();
    claims.set(key, { epoch: o.epoch, expiryMs: o.expiry_ms, spec: { namespace: spec.namespace, worker: spec.worker, class: storageClass }, shard });
    return null;
  }
  if (c.expiryMs - now < GUARD_MS) {
    const r = await post(env, "/v1/internal/do/renew", {
      namespace: c.spec.namespace, worker: c.spec.worker, class: c.spec.class, shard: c.shard,
      node: env.NODE_ID, epoch: c.epoch, ttl_seconds: leaseTtlS(env),
    });
    if (!r.ok) {
      claims.delete(key); // self-fence: stale owner must not dispatch
      return { resp: json({ error: "owner_unavailable", reason: "renew_" + r.status }, 503) };
    }
    const o = await r.json();
    c.epoch = o.epoch;
    c.expiryMs = o.expiry_ms;
  }
  return null;
}

// connect upgrades a WebSocket and hands it to the tenant DO as-is (ADR-080).
// Owner-elsewhere is not forwarded yet (documented); restart/abort closes 1012.
async function connect(req, env, url, ticket) {
  if (draining) return json({ error: "draining" }, 503);
  const q = url.searchParams;
  // Same identity inputs as /v1/do/invoke: a class alias (storage_class), the
  // stable storage_id and the version must resolve to the same facet/host actor
  // as an invoke, otherwise a WebSocket lands on a different facet (ADR-134
  // follow-up).
  const spec = {
    namespace: q.get("namespace"), worker: q.get("worker"), class: q.get("class"),
    id: q.get("id"), bundle_sha: q.get("bundle_sha"),
    storage_id: q.get("storage_id") || "",
    storage_class: q.get("storage_class") || "",
    version: q.get("version") ?? undefined,
  };
  if (!spec.namespace || !spec.worker || !spec.class || !spec.id || !spec.bundle_sha) {
    return json({ error: "bad_request", message: "namespace, worker, class, id, bundle_sha are required" }, 400);
  }
  const shard = fnv1a(`${spec.namespace}/${spec.worker}/${spec.storage_class || spec.class}/${spec.id}`) % SHARD_COUNT;
  if (ticket) {
    const sc = spec.storage_class || spec.class;
    if (ticket.ns !== spec.namespace || ticket.worker !== spec.worker ||
        (ticket.storage_class || "") !== sc || ticket.shard !== shard) {
      return json({ error: "forbidden", scope: "ticket_mismatch" }, 403);
    }
  }
  const indexKey = `${spec.storage_id || `${spec.namespace}/${spec.worker}`}/${spec.storage_class || spec.class}/${shard}/${spec.id}`;
  const firstSight = !seen.has(indexKey);
  seen.set(indexKey, {
    namespace: spec.namespace, worker: spec.worker, class: spec.class,
    storage_class: spec.storage_class || spec.class, storage_id: spec.storage_id || "", shard, id: spec.id,
  });
  if (firstSight) {
    await indexObject(env, { namespace: spec.namespace, worker: spec.worker, class: spec.class, storage_class: spec.storage_class || spec.class, shard, id: spec.id }, false);
  }
  const key = `${spec.namespace}/${spec.worker}/${spec.class}/shard${shard}`;
  const hint = hints.get(key);
  if (hint && hint.expiry > Date.now()) return proxyConnect(env, hint.address, req, url);
  const ensured = await ensureClaim(env, spec, shard);
  if (ensured && ensured.forwardTo) return proxyConnect(env, ensured.forwardTo, req, url);
  if (ensured && ensured.resp) return ensured.resp;
  await reportRouterBinding(env, `${spec.storage_id || `${spec.namespace}/${spec.worker}`}/shard${shard}`, {
    host_id: `${spec.storage_id || `${spec.namespace}/${spec.worker}`}/shard${shard}`,
    storage_id: spec.storage_id || "",
    class: spec.storage_class || spec.class,
    namespace: spec.namespace, worker: spec.worker,
  });
  wsNet += 1;
  const host = env.HOST.get(env.HOST.idFromName(`${spec.storage_id || `${spec.namespace}/${spec.worker}`}/shard${shard}`));
  return host.fetch(req); // preserve the Upgrade
}

// buildFacetEnv materializes the worker's bindings as props-bound entrypoint
// stubs (ADR-090) plus vars, so the tenant DO's env is complete natively.
// platformConsts renders the platform-only credentials as a module-scope const
// PREPENDED to a wrapper module's source (same mechanism as user-runtime's
// loader.js/internal.js; ADR-074): module scope is isolated per module, so the
// tenant DO class cannot read it — unlike `env`, which is shared.
function platformConsts(env) {
  return `;const __cellhivePlatform = Object.freeze({ cellUrl: ${JSON.stringify(env.CELL_URL || "")}, cellToken: ${JSON.stringify(env.CELL_TOKEN || "")}, logToken: ${JSON.stringify(env.LOG_TOKEN || "")} });\n`;
}

function buildFacetEnv(ctx, hostEnv, bs, spec) {
  const env = {};
  for (const [name, val] of Object.entries(bs.vars || {})) env[name] = val;
  const facades = {};
  const r2names = [];
  for (const [name, spec] of Object.entries(bs.bindings || {})) {
    const stub = bindingStub(ctx, spec);
    if (stub !== undefined) {
      env[name] = stub;
      if (spec && spec.kind === "r2") r2names.push(name);
    } else facades[name] = spec; // DO/Workflow: local facades via bindings-wrapper
  }
  for (const name of [...Object.keys(bs.vars || {}), ...Object.keys(bs.bindings || {})]) {
    if (name.startsWith("CH_") || name.startsWith("CELL_") || name.startsWith("__cellhive") || name === "PLATFORM") {
      console.error("cellhive: user env name collides with a platform key:", name);
    }
  }
  if (Object.keys(facades).length > 0) {
    console.error("cellhive: binding kinds without a platform stub:", Object.keys(facades).join(","));
  }
  env.CH_FACADE_SPEC = JSON.stringify(facades);
  env.CH_R2_BINDINGS = JSON.stringify(r2names);
  // DO namespaces and Workflows are platform-side entrypoint stubs (WDL
  // alignment): no PLATFORM/CELL_URL enters the facet env. The DO WebSocket
  // upgrade keeps the cluster-only WS binding; workflow steps get the
  // data-only steps stub.
  const doSpecs = {};
  let hasWorkflow = false;
  for (const [name, s] of Object.entries(bs.bindings || {})) {
    if (s && s.kind === "do") doSpecs[name] = s;
    if (s && s.kind === "workflow") hasWorkflow = true;
  }
  if (Object.keys(doSpecs).length > 0) {
    // Names only (ADR-184): the platform stub owns the identity/scoped token.
    env.CH_DO_BINDINGS = JSON.stringify(Object.keys(doSpecs));
    if (hostEnv.CH_DO_CONNECT) env.CH_DO_CONNECT = hostEnv.CH_DO_CONNECT;
  }
  const ex = ctx && ctx.exports;
  const facetNS = (spec && spec.namespace) || env.LOG_NS || "";
  const facetWorker = (spec && spec.worker) || env.LOG_WORKER || "";
  if (ex && ex.WorkflowSteps) env.CH_WF_STEPS = ex.WorkflowSteps({ props: { ns: facetNS, worker: facetWorker } });
  if (ex && ex.LogSink) env.CH_LOG_SINK = ex.LogSink({ props: { ns: facetNS, worker: facetWorker } });
  return env;
}

// proxyConnect forwards an upgraded WebSocket to the owning do-runtime and pipes
// frames both ways (ADR-084). No resume: a peer/migration close propagates its
// code (e.g. 1012) so the client reconnects per ADR-032/080.
async function proxyConnect(env, address, req, url) {
  let upstream;
  try {
    const target = new URL("/v1/do/connect", String(address).replace(/\/$/, ""));
    target.search = url.search;
    upstream = await fetch(target.toString(), {
      headers: {
        "x-cellhive-internal-token": env.CELL_TOKEN,
        Upgrade: req.headers.get("Upgrade") || "websocket",
        Connection: "Upgrade",
      },
    });
  } catch (e) {
    return json({ error: "owner_unavailable", reason: "forward_transport_failed" }, 502);
  }
  const peer = upstream.webSocket;
  if (!peer) return json({ error: "owner_unavailable", reason: "owner_no_socket_" + upstream.status }, 502);
  const pair = new WebSocketPair();
  const [client, server] = Object.values(pair);
  server.accept();
  peer.accept();
  wsNet += 1;
  server.addEventListener("close", () => { wsNet -= 1; });
  server.addEventListener("message", (e) => {
    try { peer.send(e.data); } catch (_) { /* peer gone */ }
  });
  peer.addEventListener("message", (e) => {
    try { server.send(e.data); } catch (_) { /* client gone */ }
  });
  const relayClose = (from, to) => (e) => {
    try { to.close(e.code, e.reason); } catch (_) { /* already closed */ }
  };
  server.addEventListener("close", relayClose(server, peer));
  peer.addEventListener("close", relayClose(peer, server));
  server.addEventListener("error", () => { try { peer.close(1011); } catch (_) {} });
  peer.addEventListener("error", () => { try { server.close(1011); } catch (_) {} });
  return new Response(null, { status: 101, webSocket: client });
}

// abortFacet stops a facet (closing its WebSocket with 1012); used on restart or
// ownership loss — never facets.delete(), so SQLite is preserved (ADR-078/080).
async function abortFacet(req, env) {
  let spec;
  try {
    spec = await req.json();
  } catch (e) {
    return json({ error: "bad_json" }, 400);
  }
  const shard = typeof spec.shard === "number"
    ? spec.shard
    : fnv1a(`${spec.namespace}/${spec.worker}/${spec.storage_class || spec.class}/${spec.id}`) % SHARD_COUNT;
  const host = env.HOST.get(env.HOST.idFromName(`${spec.storage_id || `${spec.namespace}/${spec.worker}`}/shard${shard}`));
  await host.fetch("http://host/v1/do/abort", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(spec), // include class so the host can find the facet
  });
  return json({ aborted: true });
}

// listObjects returns this runtime's seen objects (registry, ADR-082).
async function listObjects() {
  return json({ objects: [...seen.values()] });
}

// deleteFacet physically deletes an object's facet storage (ADR-082). This is
// the only path that uses facets.delete() (normal restart uses abort()).
async function deleteFacet(req, env) {
  let spec;
  try {
    spec = await req.json();
  } catch (e) {
    return json({ error: "bad_json" }, 400);
  }
  if (!spec.id || !(spec.class || spec.storage_class)) {
    return json({ error: "bad_request" }, 400);
  }
  const shard = typeof spec.shard === "number"
    ? spec.shard
    : fnv1a(`${spec.namespace}/${spec.worker}/${spec.storage_class || spec.class}/${spec.id}`) % SHARD_COUNT;
  const host = env.HOST.get(env.HOST.idFromName(`${spec.storage_id || `${spec.namespace}/${spec.worker}`}/shard${shard}`));
  const facetId = `${spec.storage_class || spec.class}/${spec.id}`;
  await host.fetch("http://host/v1/do/delete", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ facet_id: facetId }),
  });
  const key = `${spec.storage_id || `${spec.namespace}/${spec.worker}`}/${spec.storage_class || spec.class}/${shard}/${spec.id}`;
  seen.delete(key);
  await indexObject(env, { namespace: spec.namespace, worker: spec.worker, class: spec.class, storage_class: spec.storage_class || spec.class, shard, id: spec.id }, true);
  return json({ deleted: true });
}

// restartObjects eagerly aborts every seen facet for a storage id (ADR-082);
// the next dispatch rebuilds from the active bundle. Default is lazy restart.
async function restartObjects(req, env) {
  let spec;
  try {
    spec = await req.json();
  } catch (e) {
    return json({ error: "bad_json" }, 400);
  }
  let restarted = 0;
  for (const o of [...seen.values()]) {
    const key = o.storage_id || `${o.namespace}/${o.worker}`;
    if (spec.storage_id && key !== spec.storage_id) continue;
    const host = env.HOST.get(env.HOST.idFromName(`${key}/shard${o.shard}`));
    await host.fetch("http://host/v1/do/abort", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ class: o.storage_class, storage_class: o.storage_class, id: o.id }),
    });
    restarted++;
  }
  return json({ restarted });
}

async function renewAll(env) {
  let renewed = 0;
  let lost = 0;
  for (const [key, c] of [...claims.entries()]) {
    const r = await post(env, "/v1/internal/do/renew", {
      namespace: c.spec.namespace, worker: c.spec.worker, class: c.spec.class, shard: c.shard,
      node: env.NODE_ID, epoch: c.epoch, ttl_seconds: leaseTtlS(env),
    });
    if (!r.ok) {
      claims.delete(key);
      lost++;
      continue;
    }
    const o = await r.json();
    c.epoch = o.epoch;
    c.expiryMs = o.expiry_ms;
    renewed++;
  }
  return json({ renewed, lost });
}

async function drain(env) {
  draining = true;
  const deadline = Date.now() + DRAIN_WAIT_MS;
  while (inflight > 0 && Date.now() < deadline) {
    await new Promise((r) => setTimeout(r, 50));
  }
  let released = 0;
  for (const c of claims.values()) {
    const r = await post(env, "/v1/internal/do/release", {
      namespace: c.spec.namespace, worker: c.spec.worker, class: c.spec.class, shard: c.shard,
      node: env.NODE_ID, epoch: c.epoch,
    });
    if (r.ok) released++;
  }
  claims.clear();
  return json({ drained: true, released, inflight });
}

// RPC dispatch (ADR-162): the host receives a tagged JSON envelope
// {method, args} and calls the tenant method on the facet stub with native
// workerd JSRPC (the same mechanism as the platform __ch* methods). The result
// is tagged back onto the JSON wire.
function errMessage(e) {
  if (e && typeof e.message === "string" && e.message) return e.message;
  return String(e);
}

function rpcErrorResponse(status, code, message, err) {
  const body = { error: code, message };
  if (err && err.name) body.name = String(err.name);
  if (err && typeof err.stack === "string") body.stack = err.stack;
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

async function dispatchRPC(facet, rpc) {
  if (rpc === null || typeof rpc !== "object") {
    return rpcErrorResponse(400, "bad_request", "rpc must be an object with method and args");
  }
  const method = typeof rpc.method === "string" ? rpc.method : "";
  if (!method) return rpcErrorResponse(400, "bad_request", "rpc.method is required");
  if (!RPC_METHOD_RE.test(method) || method.startsWith("__ch")) {
    return rpcErrorResponse(400, "do_rpc_invalid_method", "rpc.method is not a valid method name");
  }
  if (RPC_RESERVED_METHODS.has(method)) {
    return rpcErrorResponse(400, "do_rpc_reserved_method", "rpc.method is reserved");
  }
  let args;
  try {
    args = decode(rpc.args);
  } catch (e) {
    return rpcErrorResponse(400, e && e.code ? e.code : "do_rpc_unsupported_value", errMessage(e));
  }
  if (!Array.isArray(args)) args = [args];
  let result;
  try {
    // Call via the stub so workerd's native JSRPC handles structured clone.
    // (Do not use Function.prototype.apply: it makes the facet stub appear as
    // an argument and fails with "Stubs ... are not serializable".)
    result = await facet[method](...args);
  } catch (e) {
    // A DO stub answers any property, so the missing-method error surfaces
    // here: map workerd's "does not implement the method" to 404.
    const msg = errMessage(e);
    if (/does not implement the method|is not a function|method not found/i.test(msg)) {
      return rpcErrorResponse(404, "do_rpc_method_not_found",
        "Durable Object RPC method " + method + " was not found");
    }
    return rpcErrorResponse(500, "do_rpc_error", msg, e);
  }
  let encoded;
  try {
    encoded = encode(result);
    if (encodedSize(encoded) > MAX_RPC_BYTES) {
      throw new Error("RPC result exceeds " + MAX_RPC_BYTES + " bytes");
    }
  } catch (e) {
    return rpcErrorResponse(500, e && e.code ? e.code : "do_rpc_result_invalid", errMessage(e));
  }
  return Response.json({ ok: true, result: encoded });
}

// renderFacetModule builds the loaded worker's entry module. It re-exports the
// tenant module unchanged and, for each bound DO class, exports either the class
// itself (already extends DurableObject) or a platform wrapper that delegates
// fetch/alarm/webSocket* to a legacy instance. Detection uses the platform
// __chAlarmState method, present on every class extending cellhive-do.js.
const WRAP_SRC = `function __wrap(Inner) {
  return class extends __Base {
    #inner;
    #get() {
      if (!this.#inner) this.#inner = new Inner(this.ctx, this.env);
      return this.#inner;
    }
    fetch(r) { const i = this.#get(); return typeof i.fetch === "function" ? i.fetch(r, this.env) : super.fetch(r); }
    alarm(...a) { const i = this.#get(); return typeof i.alarm === "function" ? i.alarm(...a) : super.alarm(...a); }
    webSocketMessage(...a) { const i = this.#get(); return typeof i.webSocketMessage === "function" ? i.webSocketMessage(...a) : super.webSocketMessage(...a); }
    webSocketClose(...a) { const i = this.#get(); return typeof i.webSocketClose === "function" ? i.webSocketClose(...a) : super.webSocketClose(...a); }
    webSocketError(...a) { const i = this.#get(); return typeof i.webSocketError === "function" ? i.webSocketError(...a) : super.webSocketError(...a); }
  };
}`;

function renderFacetModule(classes) {
  const names = (classes || []).filter((c) => /^[A-Za-z_$][A-Za-z0-9_$]*$/.test(c));
  if (!names.length) return "";
  const aliases = names.map((_, i) => `__i${i}`);
  const decl = `const [${aliases.join(", ")}] = [${names.map((n) => `__tenant[${JSON.stringify(n)}]`).join(", ")}];`;
  const picks = names
    .map((n, i) => `const __E${i} = __pick(__i${i});\nexport { __E${i} as ${n} };`)
    .join("\n");
  return [
    'import "bindings-wrapper.js";',
    'import { DurableObject as __Base } from "cellhive-do.js";',
    'import * as __tenant from "tenant.js";',
    'export * from "tenant.js";',
    decl,
    'function __pick(I) { return (I && typeof I.prototype.__chAlarmState === "function") ? I : __wrap(I); }',
    WRAP_SRC,
    picks,
  ].join("\n") + "\n";
}

export class Host extends DurableObject {
  async fetch(req) {
    await this.#bind();
    const url = new URL(req.url);
    if (url.pathname === "/v1/do/connect") {
      const q = url.searchParams;
      // Mirror /v1/do/invoke's identity inputs (class alias, stable storage id,
      // version) so the facet matches the one an invoke would use.
      const spec = {
        namespace: q.get("namespace"), worker: q.get("worker"), class: q.get("class"),
        id: q.get("id"), bundle_sha: q.get("bundle_sha"),
        storage_id: q.get("storage_id") || "",
        storage_class: q.get("storage_class") || "",
        version: q.get("version") ?? undefined,
      };
      const facet = await this.#facet(spec);
      return facet.fetch(req); // 101 + webSocket preserved
    }
    if (url.pathname === "/v1/do/abort") {
      const spec = await req.json();
      const facetId = `${spec.storage_class || spec.class}/${spec.id}`;
      // Close resident sockets gracefully (1012) using the existing facet stub;
      // no rebuild, so no bundle_sha is needed here.
      const facet = this.#facets.get(facetId);
      if (facet) {
        try {
          const closed = await facet.__chCloseAll(1012);
          wsNet -= Number(closed) || 0;
        } catch (e) {
          /* already gone */
        }
        await new Promise((r) => setTimeout(r, 50));
      }
      this.#facetVersion.delete(facetId);
      this.#facets.delete(facetId);
      try {
        this.ctx.facets.abort(facetId, new Error("owner changed"));
      } catch (e) {
        /* not resident */
      }
      return new Response("ok");
    }
    if (url.pathname === "/v1/do/delete") {
      const spec = await req.json();
      const facetId = spec.facet_id;
      this.#facetVersion.delete(facetId);
      this.#facets.delete(facetId);
      try {
        this.ctx.facets.delete(facetId);
      } catch (e) {
        /* not resident */
      }
      return new Response("ok");
    }
    const spec = await req.json();
    const facet = await this.#facet(spec);
    globalThis.__cellhiveTraceparent = spec.traceparent || "";
    const span = startSpan("do.invoke", {
      kind: "server",
      "cellhive.namespace": spec.namespace,
      "cellhive.worker": spec.worker,
      "cellhive.class": spec.class,
      "cellhive.do_kind": spec.kind || "fetch",
    });
    let res;
    if (spec.kind === "alarm") {
      try {
        const r = await facet.__chRunAlarm();
        doStats.alarmsOk++;
        res = new Response(typeof r === "string" ? r : "");
      } catch (e) {
        doStats.alarmsError++;
        res = json({ error: "alarm_failed", message: String((e && e.message) || e) }, 500);
      }
    } else if (spec.kind === "rpc") {
      res = await dispatchRPC(facet, spec.rpc);
    } else {
      const init = spec.request || {};
      // W3C trace context: carry the caller's traceparent into the tenant DO's
      // request (and therefore into its facade calls, which read the request
      // header via the wrapper) — ADR-146.
      const hdrs = Object.assign({}, init.headers || {});
      if (spec.traceparent && !hdrs["traceparent"]) hdrs["traceparent"] = spec.traceparent;
      const doReq = new Request("http://do" + (init.path || "/"), {
        method: init.method || "GET",
        headers: hdrs,
        body: init.body,
      });
      res = await facet.fetch(doReq);
    }
    // Report the (possibly changed) alarm so the platform scheduler can deliver.
    const state = await facet.__chAlarmState();
    await reportAlarm(this.env, spec, state.dueMs);
    await reportDoStats(this.env);
    // Output gate (ADR-083): do not ack until the supervisor has captured and
    // proven durability; unknown => result_unknown (do not silently ack).
    const gate = await gateSync(this.env);
    if (gate) {
      endSpan(span, { code: 2, message: "gate_failed" });
      await flushSpans(this.env);
      return gate;
    }
    endSpan(span, { code: res.status >= 500 ? 2 : 0 });
    await flushSpans(this.env);
    const out = new Response(await res.text(), {
      status: res.status,
      headers: { "content-type": res.headers.get("content-type") || "text/plain" },
    });
    // Mark a tenant-produced response so the caller facade returns its status
    // rather than treating every non-2xx as a transport failure: Cloudflare's
    // DurableObjectStub.fetch() resolves with the object's Response (4xx/5xx
    // included). Platform error envelopes do not carry this header.
    out.headers.set("x-cellhive-do-app", "1");
    return out;
  }

  async #facet(spec) {
    // Two identities: code is immutable per sha (bundles cache), while the
    // facet/isolate is keyed by (version, sha) so a binding/vars-only redeploy
    // refreshes the facet env instead of reusing the old one (ADR-127).
    const ver = spec.version !== undefined && spec.version !== null ? `${spec.version}-` : "";
    const buildKey = `${ver}${spec.bundle_sha}`;
    const loaderId = `${spec.namespace}/${spec.worker}@${buildKey}`;
    if (!this.#bundles.has(spec.bundle_sha)) {
      const raw = await fetchBundle(this.env, spec.bundle_sha);
      // Route the tenant's `cloudflare:workers` import to the platform base
      // class that installs the alarm shim (ADR-079).
      this.#bundles.set(
        spec.bundle_sha,
        raw.replaceAll('"cloudflare:workers"', '"cellhive-do.js"').replaceAll("'cloudflare:workers'", "'cellhive-do.js'"),
      );
    }
    const bundle = this.#bundles.get(spec.bundle_sha);
    this.#bundles.delete(spec.bundle_sha);
    this.#bundles.set(spec.bundle_sha, bundle); // refresh LRU position
    if (this.#bundles.size > BUNDLE_CACHE_MAX) {
      this.#bundles.delete(this.#bundles.keys().next().value);
    }
    // Facet id is class-scoped within the host so distinct classes never collide.
    const facetId = `${spec.storage_class || spec.class}/${spec.id}`;
    // Version-driven lazy restart (ADR-081): the facet holds the (version, sha)
    // it was built from, so a deploy that changes either must abort this facet
    // (only this one) and let the next get() construct it from the new bundle.
    // SQLite is preserved (abort, never delete).
    const built = this.#facetVersion.get(facetId);
    if (built !== undefined && built !== buildKey) {
      this.#facets.delete(facetId);
      try {
        this.ctx.facets.abort(facetId, new Error("cellhive: version changed"));
      } catch (e) {
        /* not resident */
      }
    }
    const facet = this.ctx.facets.get(facetId, async () => {
      // CF parity: a Durable Object's env must expose the worker's bindings.
      // Fetch the worker's binding spec once (cold path); the tenant's base class
      // (cellhive-do.js) builds the facades and merges them into this.env, since
      // workerd constructs the facet itself and its class cannot be subclassed.
      let bs = { bindings: {}, vars: {} };
      try {
        const r = await fetch(
          this.env.CELL_URL.replace(/\/$/, "") + "/v1/internal/do/bindings?ns=" +
            encodeURIComponent(spec.namespace) + "&worker=" + encodeURIComponent(spec.worker),
          { headers: { "x-cellhive-internal-token": this.env.CELL_TOKEN } },
        );
        if (r.ok) bs = await r.json();
      } catch (e) {
        /* no bindings: DO runs with the host env only */
      }
      // Legacy Durable Object classes (fetch/alarm/webSocket* but no
      // DurableObject base) are not RPC-capable, and facets must be. Cloudflare
      // accepts them, so generate a facet entry module that wraps every DO class
      // the worker binds in a platform subclass delegating to an inner instance.
      const doClasses = [];
      for (const b of Object.values(bs.bindings || {})) {
        if (b && b.kind === "do") {
          const c = b.class || b.storage_class;
          if (c && !doClasses.includes(c)) doClasses.push(c);
        }
      }
      const facetSrc = renderFacetModule(doClasses);
      const loaded = this.env.LOADER.get(loaderId, () => ({
        compatibilityDate: spec.compat_date || "2026-06-15",
        compatibilityFlags: spec.compat_flags || [],
        mainModule: facetSrc ? "cellhive-facet.js" : "bindings-wrapper.js",
        modules: Object.assign(
          facetSrc ? { "cellhive-facet.js": facetSrc } : {},
          {
            // The internal token reaches the wrapper via a module-scope const
            // appended to its source (ADR-074) — never via the tenant env.
            "bindings-wrapper.js": platformConsts(this.env) + this.env.BINDINGS_WRAPPER_SRC,
            "tenant.js": bundle,
            "cellhive-do.js": this.env.CELLHIVE_DO_SRC,
            "facades.js": this.env.FACADES_SRC,
            "log-tail.js": platformConsts(this.env) + this.env.LOG_TAIL_SRC,
            "rpc-codec.js": this.env.RPC_CODEC_SRC,
          },
        ),
        env: buildFacetEnv(this.ctx, this.env, bs, spec),
        globalOutbound: this.env.OUTBOUND, // tenant DO: public-only (I-09)
      }));
      const cls = loaded.getDurableObjectClass(spec.class, {
        props: { namespace: spec.namespace, worker: spec.worker, class: spec.class, shard: spec.shard },
      });
      return { class: cls, id: facetId };
    });
    this.#facetVersion.set(facetId, buildKey);
    this.#facets.set(facetId, facet);
    return facet;
  }

  // #bind reports this host actor's identity to the supervisor: name is the
  // hostId (contains storage_id) and toString() is the deterministic 64-hex
  // filename prefix that keys <hash>.sqlite / <hash>.<n>.sqlite (ADR-084).
  async #bind() {
    const base = this.env.GATE_URL || "";
    if (!base) return;
    const hash = this.ctx.id.toString();
    if (boundActors.has(hash)) return;
    boundActors.add(hash);
    await reportHostBinding(this.env, { host_id: this.ctx.id.name, host_hash: hash });
  }

  #bundles = new Map();
  #facetVersion = new Map();
  #facets = new Map();
}

async function fetchBundle(env, sha) {
  const r = await fetch(env.CELL_URL + "/v1/internal/bundle?sha=" + encodeURIComponent(sha), {
    headers: { "x-cellhive-internal-token": env.CELL_TOKEN },
  });
  if (!r.ok) throw new Error("GET bundle " + sha + " -> " + r.status);
  return await r.text();
}

function post(env, path, body) {
  return fetch(env.CELL_URL + path, {
    method: "POST",
    headers: { "x-cellhive-internal-token": env.CELL_TOKEN, "content-type": "application/json" },
    body: JSON.stringify(body),
  });
}

function fnv1a(s) {
  let h = 0x811c9dc5;
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 0x01000193) >>> 0;
  }
  return h >>> 0;
}

// reportRouterBinding tells the supervisor which storage_id/class a hostId maps
// to (the router knows the spec; the host actor knows the hash). Best-effort.
async function reportRouterBinding(env, hostIdName, body) {
  if (!env.GATE_URL) return;
  if (boundRouters.has(hostIdName)) return;
  boundRouters.add(hostIdName);
  await reportHostBinding(env, body);
}

async function reportHostBinding(env, body) {
  const base = env.GATE_URL || "";
  if (!base) return;
  try {
    await fetch(base.replace(/\/$/, "") + "/internal/do/bind", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
  } catch (e) {
    /* best-effort; capture falls back to relpath scopes */
  }
}

// gateSync asks the do-supervisor to capture+prove all SQLite files (ADR-083).
// Returns a Response to fail with, or null when the gate is disabled/ok.
async function gateSync(env) {
  const base = env.GATE_URL || "";
  if (!base) return null;
  const span = startSpan("do.gate", { kind: "internal" });
  const gateHeaders = {};
  if (globalThis.__cellhiveTraceparent) gateHeaders["traceparent"] = globalThis.__cellhiveTraceparent;
  try {
    const r = await fetch(base.replace(/\/$/, "") + "/sync-all", { method: "POST", headers: gateHeaders });
    if (!r.ok) {
      doStats.gateTimeouts++;
      endSpan(span, { code: 2, message: "gate_" + r.status });
      return json({ error: "result_unknown", reason: "gate_" + r.status }, 409);
    }
    endSpan(span, { code: 1 });
    return null;
  } catch (e) {
    doStats.gateTimeouts++;
    endSpan(span, { code: 2, message: "gate_transport" });
    return json({ error: "result_unknown", reason: "gate_transport" }, 409);
  }
}

// reportAlarm tells cell-agent this object's current alarm due time (0 = clear).
async function reportAlarm(env, spec, dueMs) {
  try {
    await fetch(env.CELL_URL + "/v1/internal/do/alarm/upsert", {
      method: "POST",
      headers: { "x-cellhive-internal-token": env.CELL_TOKEN, "content-type": "application/json" },
      body: JSON.stringify({
        namespace: spec.namespace, worker: spec.worker, class: spec.class, shard: spec.shard,
        id: spec.id, bundle_sha: spec.bundle_sha, storage_class: spec.storage_class || spec.class,
        storage_id: spec.storage_id || "", due_ms: typeof dueMs === "number" ? dueMs : 0,
      }),
    });
  } catch (e) {
    // Alarm reporting is best-effort; the next invocation re-reports.
  }
}

// authorize accepts the internal token (full access) or a signed owner ticket
// (invoke-only). It returns { ok, ticket } with the ticket claims, or null.
async function authorize(req, env) {
  const want = env.CELL_TOKEN || "";
  const got = req.headers.get("x-cellhive-internal-token") || "";
  if (want && got.length === want.length) {
    let diff = 0;
    for (let i = 0; i < got.length; i++) diff |= got.charCodeAt(i) ^ want.charCodeAt(i);
    if (diff === 0) return { ok: true, ticket: null };
  }
  const t = await verifyTicket(env, req.headers.get("x-cellhive-do-ticket"), Date.now());
  if (t) return { ok: true, ticket: t };
  return { ok: false, ticket: null };
}

let ticketKeyPromise = null;
function ticketKey(secret) {
  if (!ticketKeyPromise) {
    ticketKeyPromise = crypto.subtle.importKey(
      "raw", new TextEncoder().encode(secret || ""),
      { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
  }
  return ticketKeyPromise;
}

function b64urlToBytes(s) {
  let b = String(s).replace(/-/g, "+").replace(/_/g, "/");
  while (b.length % 4) b += "=";
  const bin = atob(b);
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
}

// verifyTicket checks the HMAC and expiry of a DO owner ticket (ADR-105).
async function verifyTicket(env, token, nowMs) {
  const secret = env.DO_TICKET_SECRET || "";
  if (!secret || !token) return null;
  const dot = token.lastIndexOf(".");
  if (dot <= 0 || dot === token.length - 1) return null;
  let payload, sig, key, got;
  try {
    payload = b64urlToBytes(token.slice(0, dot));
    sig = b64urlToBytes(token.slice(dot + 1));
    key = await ticketKey(secret);
    got = new Uint8Array(await crypto.subtle.sign("HMAC", key, payload));
  } catch (e) {
    return null;
  }
  if (got.length !== sig.length) return null;
  let diff = 0;
  for (let i = 0; i < got.length; i++) diff |= got[i] ^ sig[i];
  if (diff !== 0) return null;
  let c;
  try { c = JSON.parse(new TextDecoder().decode(payload)); } catch (e) { return null; }
  if (!c || typeof c.exp !== "number" || c.exp <= nowMs) return null;
  return c;
}

function json(o, status) {
  return new Response(JSON.stringify(o), {
    status: status || 200,
    headers: { "content-type": "application/json" },
  });
}
