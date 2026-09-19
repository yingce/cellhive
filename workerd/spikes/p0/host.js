// P0.7 host worker: mint per-binding scoped tokens, materialize each binding as
// a props-bound RPC entrypoint stub (ADR-090), and load the tenant with that env.
import { WorkerEntrypoint } from "cloudflare:workers";
import { bindingStub } from "bindings.js";

export { KV, D1Database, R2Bucket, QueueProducer } from "bindings.js";

const TENANT = `
export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    const key = url.searchParams.get("key") || "k";
    const path = url.pathname;
    try {
      if (path.startsWith("/d1")) {
        await env.DB.exec("CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, n TEXT)");
        await env.DB.prepare("INSERT INTO t (id,n) VALUES (?,?) ON CONFLICT(id) DO UPDATE SET n=excluded.n").bind(1, key).run();
        const row = await env.DB.prepare("SELECT n FROM t WHERE id=?").bind(1).first();
        return json({ d1: row ? row.n : null });
      }
      if (path.startsWith("/r2")) {
        if (req.method === "POST") { const o = await env.BUCKET.put(key, await req.text()); return json({ put: o.etag ? "ok" : "no" }); }
        const o = await env.BUCKET.get(key);
        return json({ r2: o ? await o.text() : null });
      }
      if (path.startsWith("/queue")) {
        const r = await env.QUEUE.send({ key, at: Date.now() });
        return json({ queued: r.id });
      }
      if (req.method === "POST") { await env.KV.put(key, await req.text()); return json({ kv: "put" }); }
      const v = await env.KV.get(key);
      return json({ kv: v });
    } catch (e) {
      return json({ error: String(e) }, 500);
    }
  },
};
function json(o, status) {
  return new Response(JSON.stringify(o), { status: status || 200, headers: { "content-type": "application/json" } });
}
`;

class Host extends WorkerEntrypoint {
  async fetch(req) {
    const specs = {
      KV: { kind: "kv", ns: "p0", name: "main", token: await mintScoped(this.env, "p0", "kv", "main") },
      DB: { kind: "d1", ns: "p0", name: "main", token: await mintScoped(this.env, "p0", "d1", "main") },
      BUCKET: { kind: "r2", ns: "p0", name: "files", token: await mintScoped(this.env, "p0", "r2", "files") },
      QUEUE: { kind: "queue", ns: "p0", name: "jobs", token: await mintScoped(this.env, "p0", "queue", "jobs") },
    };
    const bindings = {};
    for (const [name, spec] of Object.entries(specs)) {
      const stub = bindingStub(this.ctx, spec);
      if (stub !== undefined) bindings[name] = stub;
    }
    const worker = this.env.LOADER.get("t1", () => ({
      compatibilityDate: "2026-04-24",
      mainModule: "tenant.js",
      modules: { "tenant.js": TENANT },
      env: bindings,
      globalOutbound: this.env.OUTBOUND,
    }));
    return worker.getEntrypoint().fetch(req);
  }
}

export default Host;

// Local binding-token computation (ADR-074): no mint round trip.
async function mintScoped(env, ns, kind, name) {
  const payload = `{"ns":${JSON.stringify(ns)},"kind":${JSON.stringify(kind)},"name":${JSON.stringify(name)}}`;
  const key = await crypto.subtle.importKey("raw", new TextEncoder().encode(env.SCOPE_SECRET), { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
  const sig = await crypto.subtle.sign("HMAC", key, new TextEncoder().encode(payload));
  const b64 = (u8) => { let s = ""; for (const b of u8) s += String.fromCharCode(b); return btoa(s).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, ""); };
  return b64(new TextEncoder().encode(payload)) + "." + b64(new Uint8Array(sig));
}

export default {
  async fetch(req, env) {
    const specs = {
      KV: { kind: "kv", ns: "p0", name: "main", token: await mintScoped(env, "p0", "kv", "main") },
      DB: { kind: "d1", ns: "p0", name: "main", token: await mintScoped(env, "p0", "d1", "main") },
      BUCKET: { kind: "r2", ns: "p0", name: "files", token: await mintScoped(env, "p0", "r2", "files") },
      QUEUE: { kind: "queue", ns: "p0", name: "jobs", token: await mintScoped(env, "p0", "queue", "jobs") },
    };
    const worker = env.LOADER.get("t1", () => ({
      compatibilityDate: "2026-04-24",
      mainModule: "worker.js",
      modules: { "worker.js": WRAPPER, "tenant.js": TENANT, "facades.js": env.FACADES_SRC },
      env: { CELL_URL: env.CELL_URL, CELL_TOKEN: env.CELL_TOKEN, SCOPE_SPEC: JSON.stringify(specs) },
      globalOutbound: env.OUTBOUND,
    }));
    return worker.getEntrypoint().fetch(req);
  },
};
