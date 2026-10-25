// Platform-side binding facades (host adapter) mapping CF-shaped env bindings onto
// cell-agent binding endpoints (docs/bindings.md). Tenant code sees only these
// facades; it never sees the internal token, backend address, or a generic
// Fetcher. Each facade closes over its own scoped token (ADR-029).

import {
  decode, encode, encodedSize, MAX_RPC_BYTES,
  RPC_METHOD_RE, RPC_RESERVED_METHODS, DO_ID_HEADER,
} from "rpc-codec.js";

function headers(platform, scopeToken) {
  // Binding calls authenticate with the scoped token only (ADR-074); the broad
  // internal token is never sent from a tenant-adjacent worker.
  const h = {};
  if (scopeToken) h["x-cellhive-scope-token"] = scopeToken;
  return h;
}

// pfetch routes facade traffic through the platform's service binding when
// present, so binding calls reach cell-agent even though the tenant worker's
// globalOutbound is public-only (ADR-029/I-09). Falls back to global fetch
// (e.g. the p0 loader, which allows private outbound).
function pfetch(platform, url, opts) {
  // W3C trace context: the loaded-worker wrapper sets the inbound request's
  // traceparent on globalThis; every facade call carries it (ADR-146).
  const tp = globalThis.__cellhiveTraceparent;
  if (tp) {
    opts = Object.assign({}, opts || {});
    opts.headers = Object.assign({}, opts.headers || {});
    if (!opts.headers["traceparent"]) opts.headers["traceparent"] = tp;
  }
  const f = platform.fetcher;
  if (f && typeof f.fetch === "function") return f.fetch(url, opts);
  if (typeof f === "function") return f(url, opts);
  return fetch(url, opts);
}

function q(params) {
  return Object.entries(params)
    .filter(([, v]) => v !== undefined && v !== null)
    .map(([k, v]) => encodeURIComponent(k) + "=" + encodeURIComponent(v))
    .join("&");
}

const BINDING_ERROR_NAMES = { kv: "KVError", d1: "D1_ERROR", r2: "R2Error", queue: "QueueError", ai: "AIError" };
async function must(res, what) {
  if (res.ok) return res;
  const kind = String(what).split(".")[0];
  const text = await res.text();
  let code = "";
  let message = text;
  try {
    const j = JSON.parse(text);
    if (j && typeof j === "object") {
      if (typeof j.error === "string") code = j.error;
      if (typeof j.message === "string") message = j.message;
    }
  } catch (e) { /* non-JSON body */ }
  const err = new Error(code ? code + ": " + message : message);
  err.name = BINDING_ERROR_NAMES[kind] || "Error";
  err.status = res.status;
  if (code) err.code = code;
  throw err;
}

// KV helpers (ADR-098): TTL options and metadata encoding for the scoped-token
// KV binding.
function b64enc(s) {
  const bytes = new TextEncoder().encode(s);
  let bin = "";
  for (const b of bytes) bin += String.fromCharCode(b);
  return btoa(bin);
}
function b64decJson(h) {
  if (!h) return null;
  try {
    const bin = atob(h);
    const bytes = Uint8Array.from(bin, (c) => c.charCodeAt(0));
    return JSON.parse(new TextDecoder().decode(bytes));
  } catch {
    return null;
  }
}
function kvPutOpts(options = {}) {
  const o = {};
  if (options.expiration) o.expiration = options.expiration;
  if (options.expirationTtl) o.expiration_ttl = options.expirationTtl;
  if (options.metadata !== undefined) o.metadata = b64enc(JSON.stringify(options.metadata));
  return o;
}
function kvKeyOut(k) {
  if (typeof k === "string") return { name: k };
  const out = { name: k.name };
  if (k.expiration) out.expiration = k.expiration;
  if (k.metadata !== undefined) out.metadata = b64decJson(k.metadata);
  return out;
}

function b64bytes(s) {
  const bin = atob(s || "");
  return Uint8Array.from(bin, (c) => c.charCodeAt(0));
}

function kvValue(bytes, type) {
  if (type === "arrayBuffer") return bytes.buffer;
  const text = new TextDecoder().decode(bytes);
  return type === "json" ? JSON.parse(text) : text;
}

function kvReadType(options) {
  return options && typeof options === "object" ? options.type : options;
}

// makeKVFromStub keeps micro-batching in the tenant isolate. A WorkerEntrypoint
// may be instantiated once per JSRPC call, so batching inside the platform stub
// cannot observe sibling Promise.all() calls.
export function makeKVFromStub(stub) {
  let pending = null;
  const enqueue = (key, type, withMetadata) => {
    if (!pending) {
      pending = [];
      const batch = pending;
      queueMicrotask(() => flush(batch));
    }
    return new Promise((resolve, reject) => pending.push({ key, type, withMetadata, resolve, reject }));
  };
  const flush = async (batch) => {
    if (pending !== batch) return;
    pending = null;
    try {
      for (let start = 0; start < batch.length; start += 256) {
        const items = batch.slice(start, start + 256);
        let results = await stub.getBatch(items.map((item) => item.key));
        if (results === null) {
          results = await Promise.all(items.map(async (item) => {
            const got = item.withMetadata ? await stub.getWithMetadata(item.key, item.type) : await stub.get(item.key, item.type);
            if (item.withMetadata) return got.value === null ? { found: false } : { direct: got };
            return got === null ? { found: false } : { direct: { value: got, metadata: null } };
          }));
        }
        if (!Array.isArray(results) || results.length !== items.length) throw new Error("kv.get: invalid batch response");
        for (let i = 0; i < items.length; i++) {
          const item = items[i];
          const result = results[i];
          if (result.direct) {
            item.resolve(item.withMetadata ? result.direct : result.direct.value);
            continue;
          }
          if (!result || !result.found) {
            item.resolve(item.withMetadata ? { value: null, metadata: null } : null);
            continue;
          }
          try {
            const value = kvValue(b64bytes(result.value), item.type);
            const metadata = b64decJson(result.metadata);
            item.resolve(item.withMetadata ? { value, metadata } : value);
          } catch (e) {
            item.reject(e);
          }
        }
      }
    } catch (e) {
      for (const item of batch) item.reject(e);
    }
  };
  const database = {
    get: (key, options) => enqueue(key, kvReadType(options), false),
    getWithMetadata: (key, options) => enqueue(key, kvReadType(options), true),
    put: (...args) => stub.put(...args),
    delete: (...args) => stub.delete(...args),
    list: (...args) => stub.list(...args),
  };
  return database;
}

export function makeKV(platform, ns, scopeToken) {
  const base = platform.url + "/v1/kv/";
  return {
    async get(key, options) {
      const type = kvReadType(options);
      const r = await pfetch(platform, base + "get?" + q({ ns, key }), { headers: headers(platform, scopeToken) });
      if (r.status === 404) return null;
      await must(r, "kv.get");
      return type === "json" ? r.json() : type === "arrayBuffer" ? r.arrayBuffer() : r.text();
    },
    async put(key, value, options = {}) {
      const body = typeof value === "string" ? value : JSON.stringify(value);
      const r = await pfetch(platform, base + "put?" + q({ ns, key, ...kvPutOpts(options) }), { method: "POST", headers: headers(platform, scopeToken), body });
      await must(r, "kv.put");
    },
    async getWithMetadata(key, options) {
      const type = kvReadType(options);
      const r = await pfetch(platform, base + "get?" + q({ ns, key }), { headers: headers(platform, scopeToken) });
      if (r.status === 404) return { value: null, metadata: null };
      await must(r, "kv.getWithMetadata");
      const metadata = b64decJson(r.headers.get("x-cellhive-kv-metadata"));
      const value = type === "json" ? await r.json() : type === "arrayBuffer" ? await r.arrayBuffer() : await r.text();
      return { value, metadata };
    },
    async delete(key) {
      const r = await pfetch(platform, base + "delete?" + q({ ns, key }), { method: "DELETE", headers: headers(platform, scopeToken) });
      await must(r, "kv.delete");
    },
    async list(options = {}) {
      const r = await pfetch(platform, base + "list?" + q({ ns, prefix: options.prefix, limit: options.limit, cursor: options.cursor, with_metadata: options.includeMetadata ? 1 : undefined }), {
        headers: headers(platform, scopeToken),
      });
      await must(r, "kv.list");
      const out = await r.json();
      return { keys: (out.keys || []).map(kvKeyOut), list_complete: out.list_complete, cursor: out.cursor || undefined };
    },
  };
}

// Durable Object binding (ADR-080): env.DO.get(id).fetch(...) is placed by
// cell-agent onto a do-runtime by shard. HTTP bodies use a byte-safe envelope.
export function makeDO(platform, spec) {
  // Owner hint (ADR-105): after the router (cell-agent) answers once, cache the
  // owner address plus a short-lived ticket and call the owner directly next
  // time. The ticket authorizes only POST /v1/do/invoke for this shard.
  const directEnabled = spec.direct !== false;
  let hint = null; // { url, ticket, exp }

  function identity(id) {
    return {
      namespace: spec.ns, worker: spec.worker, bundle_sha: spec.bundle_sha, version: spec.version,
      limits: spec.limits,
      storage_id: spec.storage_id || "",
      class: spec.class, storage_class: spec.storage_class || spec.class, id: String(id),
    };
  }

  // invoke posts one /v1/do/invoke envelope, reusing the owner hint when
  // present. On a direct-owner 409/5xx the request may have executed, so it is
  // never replayed; such a response is returned when allowErrorResponse is set
  // (RPC wants to read the error envelope) and thrown otherwise (fetch).
  async function invoke(id, payload, allowErrorResponse) {
    if (spec.deleted) throw new Error("do: class " + spec.class + " was deleted");
    if (directEnabled && hint && hint.exp > Date.now() + 500) {
      let direct;
      try {
        direct = await pfetch(platform, hint.url + "/v1/do/invoke", {
          method: "POST",
          headers: { "content-type": "application/json", "x-cellhive-do-ticket": hint.ticket },
          body: payload, redirect: "manual",
        });
      } catch (e) {
        // Transport failure: the request may or may not have reached the owner.
        // A replay could double-execute, so surface it (ADR-080).
        throw new Error("do: direct invoke failed: " + (e && e.message ? e.message : e));
      }
      if (direct.ok || direct.headers.get("x-cellhive-do-app") === "1") return direct;
      if (direct.status === 409 || direct.status >= 500) {
        // 409 result_unknown and 5xx may have executed; never replay.
        if (allowErrorResponse) return direct;
        throw new Error("do: " + direct.status + " " + (await direct.text()));
      }
      hint = null; // provably pre-execution failure (e.g. 404 route refresh): fall back
    }

    const r = await pfetch(platform, platform.url + "/v1/do/invoke", {
      method: "POST",
      headers: { ...headers(platform, spec.token), "content-type": "application/json" },
      body: payload, redirect: "manual",
    });
    if (r.ok) {
      const owner = r.headers.get("x-cellhive-do-owner");
      const ticket = r.headers.get("x-cellhive-do-owner-ticket");
      const exp = Number(r.headers.get("x-cellhive-do-owner-exp") || 0);
      if (directEnabled && owner && ticket && exp > 0) {
        const base = owner.startsWith("http") ? owner : "http://" + owner;
        hint = { url: base.replace(/\/$/, ""), ticket, exp };
      }
    }
    return r;
  }

  // upgrade opens a WebSocket to the DO's owner. A live socket cannot cross the
  // JSON invoke envelope, so ask the sharding router for the owner + ticket
  // (scoped token), then hand the upgrade to the owner's /v1/do/connect.
  async function connectInfo(id) {
    if (spec.deleted) throw new Error("do: class " + spec.class + " was deleted");
    const qs = q(identity(id));
    const look = await pfetch(platform, platform.url + "/v1/do/connect?" + qs, { headers: headers(platform, spec.token) });
    if (!look.ok) throw new Error("do: connect lookup " + look.status + " " + (await look.text()));
    const info = await look.json();
    const base = String(info.owner || "").startsWith("http") ? info.owner : "http://" + info.owner;
    return { url: base.replace(/\/$/, "") + "/v1/do/connect?" + qs, ticket: info.ticket || "" };
  }

  async function upgrade(id, req) {
    const info = await connectInfo(id);
    // Preserve the tenant's headers (cookies/auth/WebSocket subprotocol) on the
    // direct-to-owner hop. Never forward a caller-supplied x-cellhive-* value:
    // the platform adds its own scope token/ticket below.
    const h = {};
    req.headers.forEach((v, k) => {
      const lk = k.toLowerCase();
      // Hop-by-hop headers are set explicitly below. Copying them first can
      // produce a duplicate ("websocket, websocket") after header
      // normalization, which the owner no longer recognizes as an upgrade.
      if (lk === "host" || lk === "upgrade" || lk === "connection" ||
          lk === "content-length" || lk.startsWith("x-cellhive-")) return;
      h[k] = v;
    });
    Object.assign(h, headers(platform, spec.token));
    h["Upgrade"] = req.headers.get("Upgrade") || "websocket";
    h["Connection"] = "Upgrade";
    if (info.ticket) h["x-cellhive-do-ticket"] = info.ticket;
    return await pfetch(platform, info.url, { headers: h });
  }

  async function call(id, input, init) {
    const req = input instanceof Request ? input : new Request(input, init);
    if (String(req.headers.get("Upgrade") || "").toLowerCase() === "websocket") {
      return upgrade(id, req);
    }
    const method = req.method || "GET";
    const url = new URL(req.url);
    let body_base64;
    if (method !== "GET" && method !== "HEAD" && req.body !== null) {
      body_base64 = base64Queue(await req.arrayBuffer());
    }
    const hdrs = {};
    req.headers.forEach((v, k) => {
      if (!k.toLowerCase().startsWith("x-cellhive-")) hdrs[k] = v;
    });
    const payload = JSON.stringify({
      ...identity(id),
      request: { method, path: url.pathname + url.search, headers: hdrs, body_base64 },
    });
    const r = await invoke(id, payload, false);
    // A DO fetch resolves with the object's Response, including 4xx/5xx; only a
    // platform error envelope (no x-cellhive-do-app marker) is thrown.
    if (!r.ok && r.headers.get("x-cellhive-do-app") !== "1") {
      throw new Error("do: " + r.status + " " + (await r.text()));
    }
    const responseHeaders = new Headers(r.headers);
    for (const key of [...responseHeaders.keys()]) {
      if (key.toLowerCase().startsWith("x-cellhive-") || key.toLowerCase() === "content-length") responseHeaders.delete(key);
    }
    const bytes = await r.arrayBuffer();
    return new Response(method === "HEAD" || [204, 205, 304].includes(r.status) ? null : bytes, {
      status: r.status, statusText: r.statusText, headers: responseHeaders,
    });
  }

  // rpc calls a tenant DO method over the tagged JSON transport (ADR-162). The
  // do-runtime host replays it as native JSRPC on the facet, so Map/Date/
  // ArrayBuffer/cycles survive the round trip (see rpc-codec.js).
  async function rpc(id, method, args) {
    if (typeof method !== "string" || !RPC_METHOD_RE.test(method) ||
        method.startsWith("__ch") || RPC_RESERVED_METHODS.has(method)) {
      throw new Error("do: rpc method " + method + " is not allowed");
    }
    const encodedArgs = encode(args);
    if (encodedSize(encodedArgs) > MAX_RPC_BYTES) {
      throw new Error("do: rpc args exceed " + MAX_RPC_BYTES + " bytes");
    }
    const payload = JSON.stringify({
      ...identity(id), kind: "rpc", rpc: { method, args: encodedArgs },
    });
    const r = await invoke(id, payload, true);
    const text = await r.text();
    let envl = null;
    try { envl = JSON.parse(text); } catch (e) { /* not JSON */ }
    if (r.status >= 200 && r.status < 300 && envl && envl.ok === true) {
      return decode(envl.result);
    }
    const message = envl && typeof envl.message === "string" && envl.message
      ? envl.message
      : "do: rpc failed with status " + r.status + (text ? " " + text : "");
    const err = new Error(message);
    if (envl && typeof envl.name === "string" && envl.name) err.name = envl.name;
    if (envl && typeof envl.error === "string" && envl.error) err.code = envl.error;
    if (envl && typeof envl.stack === "string" && envl.stack) err.stack = envl.stack;
    throw err;
  }

  function stub(id) {
    const real = { fetch: (input, init) => call(id, input, init) };
    return new Proxy(real, {
      get(target, prop, receiver) {
        if (typeof prop !== "string") return Reflect.get(target, prop, receiver);
        // Never look like a thenable or JSON-serializable RPC object.
        if (prop === "then" || prop === "toJSON") return undefined;
        // fetch is the only real method; everything else is RPC or nothing.
        // Checking the deny-list before the target's own properties also stops
        // Object.prototype members (constructor/toString/...) from leaking.
        if (prop === "fetch") return target.fetch;
        if (!RPC_METHOD_RE.test(prop) || prop.startsWith("__ch") || RPC_RESERVED_METHODS.has(prop)) {
          return undefined;
        }
        return (...args) => rpc(id, prop, args);
      },
    });
  }

  return {
    idFromName: (name) => String(name),
    newUniqueId: () => crypto.randomUUID(),
    idFromString: (s) => String(s),
    getByName: (name) => stub(String(name)),
    get: (id) => stub(id),
    // Internal transport hooks for the platform-side namespace entrypoint
    // (bindings.js DurableObjectNamespace, WDL alignment). Never tenant-visible.
    __call: (id, input, init) => call(id, input, init),
    __rpc: (id, method, args) => rpc(id, method, args),
  };
}

// makeDOTransport builds makeDO's :7001 transport for the platform-side
// namespace entrypoint: the DO transport (owner hint cache, scoped token,
// invoke/rpc envelopes) lives entirely in the trusted platform worker.
export function makeDOTransport(platform, spec) {
  return makeDO(platform, spec);
}

// makeDOFromStub rebuilds the CF-shaped namespace (env.NS.get(id).fetch(...)
// and env.NS.get(id).anyMethod(...)) on the TENANT side over the platform-side
// DurableObjectNamespace entrypoint (bindings.js). Both calls target direct
// methods of that entrypoint with the object id as an argument: a WebSocket
// 101 can cross workerLoader RPC only as the return value of an entrypoint
// method, never from an RpcTarget instance the tenant calls itself (verified on
// the pinned workerd; WDL's do-client uses the same fetchObject shape). All
// transport state — :7001, the scoped token, the owner-hint cache, the shard
// ticket — stays in the platform worker, so the tenant env needs none of it.
export function makeDOFromStub(stub) {
  function wrap(id) {
    return new Proxy({}, {
      get(_, prop) {
        if (typeof prop !== "string" || prop === "then" || prop === "toJSON") return undefined;
        if (prop === "fetch") {
          return async (input, init) => {
            const req = input instanceof Request ? input : new Request(input, init);
            // The id rides in the request (DO_ID_HEADER): the platform-side
            // namespace implements it as fetch(request) — the only RPC shape
            // workerd lets carry a WebSocket 101 back (ADR-184).
            const h = new Headers(req.headers);
            h.set(DO_ID_HEADER, String(id));
            return await stub.fetch(new Request(req, { headers: h }));
          };
        }
        // Arbitrary DO methods: forwarded as (method, args) data over RPC.
        if (!RPC_METHOD_RE.test(prop) || prop.startsWith("__ch") || RPC_RESERVED_METHODS.has(prop)) {
          return undefined;
        }
        return (...args) => stub.rpcObject(String(id), prop, args);
      },
    });
  }
  return {
    idFromName: (name) => String(name),
    newUniqueId: () => crypto.randomUUID(),
    idFromString: (s) => String(s),
    get: (id) => wrap(id),
    getByName: (name) => wrap(String(name)),
  };
}

function d1Param(value) {
  if (value instanceof ArrayBuffer) return Array.from(new Uint8Array(value));
  if (ArrayBuffer.isView(value)) return Array.from(new Uint8Array(value.buffer, value.byteOffset, value.byteLength));
  return value;
}

export function makeD1(platform, ns, db, scopeToken) {
  const base = platform.url + "/v1/d1/";
  async function call(path, body) {
    const r = await pfetch(platform, base + path + "?" + q({ ns, db }), {
      method: "POST",
      headers: { ...headers(platform, scopeToken), "content-type": "application/json" },
      body: JSON.stringify(Object.assign({}, body, { params: (body.params || []).map(d1Param) })),
    });
    await must(r, "d1." + path);
    return (await r.json()).results;
  }
  function prepare(sql) {
    let params = [];
    const stmt = {
      sql,
      params,
      bind(...p) { params = p; stmt.params = p; return stmt; },
      async all() { const [res] = await call("query", { sql, params }); return { results: rowsToObjects(res), success: true, meta: d1Meta(res, true) }; },
       async first(column) { const [res] = await call("query", { sql, params }); const objs = rowsToObjects(res); if (!objs.length) return null; if (column) { if (!Object.prototype.hasOwnProperty.call(objs[0], column)) throw new Error("D1_ERROR: column not found: " + column); return objs[0][column]; } return objs[0]; },
       async run() { const [res] = await call("exec", { sql, params }); return { results: [], success: true, meta: d1Meta(res, false) }; },
      async raw(options = {}) { const [res] = await call("query", { sql, params }); const rows = res.rows || []; return options.columnNames ? [res.columns || [], ...rows] : rows; },
    };
    return stmt;
  }
  const out = {
    prepare,
    async batch(stmts) {
      const statements = stmts.map((s) => { const spec = s.spec ? s.spec() : s; return { sql: spec.sql, params: (spec.params || []).map(d1Param) }; });
      const results = await call("batch", { statements });
       return results.map((res) => ({ success: true, results: rowsToObjects(res), meta: d1Meta(res, (res.columns || []).length > 0) }));
    },
    async exec(sql) {
      const [res] = await call("exec", { sql });
      return { count: res.statement_count || 1, duration: res.duration_ms };
    },
  };
  out.session = (bookmark) => makeD1Session(out, bookmark);
  out.withSession = (bookmark) => makeD1Session(out, bookmark);
  return out;
}

export function makeR2(platform, ns, bucket, scopeToken) {
  const base = platform.url + "/v1/r2/";

  function objFromJSON(o) {
    const etag = o.etag || "";
    return {
      key: o.key, size: o.size, etag,
      httpEtag: o.httpEtag || (etag ? '"' + etag + '"' : ""),
      version: o.version || "",
      uploaded: o.uploaded ? new Date(o.uploaded) : undefined,
      httpMetadata: o.httpMetadata || {}, customMetadata: o.customMetadata || {}, checksums: o.checksums || {},
    };
  }
  function objFromResponse(r, key) {
    const md = b64decJson(r.headers.get("x-cellhive-r2-meta")) || {};
    const etag = r.headers.get("etag") || md.etag || "";
    let size = Number(r.headers.get("content-length") || md.size || 0);
    const cr = r.headers.get("content-range");
    if (cr) {
      const total = cr.split("/")[1];
      if (total && total !== "*") size = Number(total);
    }
    const checksums = {};
    if (md.md5) checksums.md5 = md.md5;
    if (md.sha256) checksums.sha256 = md.sha256;
    return {
      key, size, etag, httpEtag: etag ? '"' + etag + '"' : "", version: "",
      uploaded: md.uploaded_ms ? new Date(md.uploaded_ms) : undefined,
      httpMetadata: md.http || {}, customMetadata: md.custom || {}, checksums,
    };
  }
  function body(buf, info, range) {
    const o = Object.assign({}, info);
    o.bodyUsed = false;
    if (range) o.range = { offset: range.offset, length: range.length === undefined ? buf.byteLength : range.length };
    // Non-enumerable like Cloudflare's prototype methods, so spread/JSON see
    // only the data fields.
    const methods = {
      arrayBuffer: async () => { o.bodyUsed = true; return buf; },
      text: async () => { o.bodyUsed = true; return new TextDecoder().decode(buf); },
      json: async () => { o.bodyUsed = true; return JSON.parse(new TextDecoder().decode(buf)); },
      blob: async () => { o.bodyUsed = true; return new Blob([buf]); },
    };
    for (const [name, fn] of Object.entries(methods)) {
      Object.defineProperty(o, name, { value: fn, enumerable: false, writable: true, configurable: true });
    }
    return o;
  }

  return {
    async put(key, value, options = {}) {
      const payload = typeof value === "string" ? value : value;
      const h = Object.assign({}, headers(platform, scopeToken));
      if (options.onlyIf !== undefined) h["x-cellhive-r2-only-if"] = b64enc(JSON.stringify(options.onlyIf));
      if (options.httpMetadata) h["x-cellhive-r2-http-metadata"] = b64enc(JSON.stringify(options.httpMetadata));
      if (options.customMetadata) h["x-cellhive-r2-custom-metadata"] = b64enc(JSON.stringify(options.customMetadata));
      if (options.md5) h["x-cellhive-r2-md5"] = options.md5;
      if (options.sha256) h["x-cellhive-r2-sha256"] = options.sha256;
      const r = await pfetch(platform, base + "object?" + q({ ns, bucket, key }), { method: "PUT", headers: h, body: payload });
      if (r.status === 412) return null;
      await must(r, "r2.put");
      return objFromJSON(await r.json());
    },
    async get(key, options = {}) {
      const h = headers(platform, scopeToken);
      if (options.onlyIf !== undefined) h["x-cellhive-r2-only-if"] = b64enc(JSON.stringify(options.onlyIf));
      if (options.range) {
        const end = options.range.length ? options.range.offset + options.range.length - 1 : "";
        h["range"] = "bytes=" + options.range.offset + "-" + end;
      }
      const r = await pfetch(platform, base + "object?" + q({ ns, bucket, key }), { headers: h });
      if (r.status === 404 || r.status === 304) return null;
      await must(r, "r2.get");
      return body(await r.arrayBuffer(), objFromResponse(r, key), options.range);
    },
    async head(key) {
      const r = await pfetch(platform, base + "object?" + q({ ns, bucket, key, head: 1 }), { headers: headers(platform, scopeToken) });
      if (r.status === 404) return null;
      await must(r, "r2.head");
      return objFromJSON(await r.json());
    },
    async delete(key) {
      const r = await pfetch(platform, base + "object?" + q({ ns, bucket, key }), { method: "DELETE", headers: headers(platform, scopeToken) });
      await must(r, "r2.delete");
    },
    async list(options = {}) {
      const r = await pfetch(platform, base + "list?" + q({
        ns, bucket, prefix: options.prefix, limit: options.limit,
        cursor: options.cursor, start_after: options.startAfter,
        delimiter: options.delimiter,
        include: Array.isArray(options.include) ? options.include.join(",") : options.include,
      }), { headers: headers(platform, scopeToken) });
      await must(r, "r2.list");
      const out = await r.json();
      const res = { objects: (out.objects || []).map(objFromJSON), truncated: !!out.truncated, cursor: out.cursor };
      if (options.delimiter !== undefined) res.delimitedPrefixes = out.delimitedPrefixes || [];
      return res;
    },
  };
}

export function makeQueue(platform, ns, queue, scopeToken) {
  return {
    async send(body, options = {}) {
      const encoded = encodeQueueBody(body, options.contentType);
      const r = await pfetch(platform, platform.url + "/v1/queue/send?" + q({ ns, queue, delay_seconds: options.delaySeconds, idempotency_key: options.idempotencyKey }), {
        method: "POST",
        headers: { ...headers(platform, scopeToken), "content-type": encoded.mime, "x-cellhive-queue-content-type": encoded.contentType },
        body: encoded.body,
      });
      await must(r, "queue.send");
      const out = await r.json();
      return { id: out.id };
    },
    async sendBatch(messages = []) {
      if (!Array.isArray(messages) || messages.length < 1 || messages.length > 100) throw new TypeError("sendBatch requires 1..100 messages");
      const payload = messages.map((item) => { if (!item || typeof item !== "object" || !("body" in item)) throw new TypeError("sendBatch message must contain body"); const e = encodeQueueBody(item.body, item.contentType); return { body: base64Queue(e.body), contentType: e.contentType, delaySeconds: item.delaySeconds, idempotencyKey: item.idempotencyKey, metadata: item.metadata }; });
      const r = await pfetch(platform, platform.url + "/v1/queue/send-batch?" + q({ ns, queue }), { method: "POST", headers: { ...headers(platform, scopeToken), "content-type": "application/json" }, body: JSON.stringify({ messages: payload }) });
      await must(r, "queue.sendBatch");
      return { ids: (await r.json()).ids || [] };
    },
  };
}

// Keep the public Queue API in the tenant isolate. The platform stub runs in
// another isolate, so it receives the current invocation context explicitly.
export function makeQueueFromStub(stub) {
  const context = () => ({
    traceparent: globalThis.__cellhiveTraceparent || "",
    requestId: globalThis.__cellhiveRequestId || "",
  });
  return {
    send(body, options = {}) { return stub.send(body, options, context()); },
    sendBatch(messages = []) { return stub.sendBatch(messages, context()); },
  };
}

function base64Queue(bytes) { let s = ""; for (const b of new Uint8Array(bytes)) s += String.fromCharCode(b); return btoa(s); }
function encodeQueueBody(body, requested) {
  const contentType = requested || (typeof body === "string" ? "text" : "json");
  if (contentType === "text") return { contentType, mime: "text/plain", body: String(body) };
  if (contentType === "bytes") { const b = body instanceof ArrayBuffer ? new Uint8Array(body) : ArrayBuffer.isView(body) ? new Uint8Array(body.buffer, body.byteOffset, body.byteLength) : null; if (!b) throw new TypeError("bytes contentType requires ArrayBuffer or view"); return { contentType, mime: "application/octet-stream", body: b }; }
  if (contentType === "v8") return { contentType, mime: "application/octet-stream", body: JSON.stringify({ __cellhive_v8: encode(body) }) };
  if (contentType !== "json") throw new TypeError("contentType must be json, text, bytes, or v8");
  return { contentType, mime: "application/json", body: JSON.stringify(body) };
}


// makeWorkflow exposes the supported Workflows subset: create/createBatch,
// instance lifecycle/status/delete/sendEvent, and instance listing.
// locationHint and cross-worker instances remain unsupported.
export function makeWorkflow(platform, ns, name, scopeToken) {
  const base = platform.url + "/v1/workflow/";
  const call = async (path, method, extra, body) => {
    const r = await pfetch(platform, base + path + "?" + q({ ns, workflow: name, ...(extra || {}) }), {
      method: method || "GET",
      headers: { ...headers(platform, scopeToken), "content-type": "application/json" },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    await must(r, "workflow." + path);
    return await r.json();
  };
  const decode = (b64) => {
    if (!b64) return undefined;
    try { return JSON.parse(new TextDecoder().decode(Uint8Array.from(atob(b64), (ch) => ch.charCodeAt(0)))); } catch (e) { return undefined; }
  };
  return {
    async create(options = {}) {
      const out = await call("create", "POST", { id: options.id || undefined }, options.params === undefined ? undefined : options.params);
      return { id: out.id };
    },
    async createBatch(items = []) {
      const out = await call("create-batch", "POST", {}, items.map((item) => ({
        id: item && item.id ? item.id : undefined,
        params: item ? item.params : undefined,
      })));
      return (out.instances || []).map((instance) => ({ id: instance.id }));
    },
    // CF-shaped instance: get(id) -> object with methods.
    async get(id) {
      return {
        id,
        async status() {
          const out = await call("get", "GET", { id });
          return { status: out.status === "sleeping" ? "waiting" : out.status, error: out.error || undefined, output: decode(out.output) };
        },
        async pause() { await call("pause", "POST", { id }); },
        async resume() { await call("resume", "POST", { id }); },
        async terminate() { await call("terminate", "POST", { id }); },
        async restart() { await call("restart", "POST", { id }); },
        async delete() { await call("delete", "POST", { id }); },
        async sendEvent(payload) { await call("event", "POST", { id }, payload); },
      };
    },
    async sendEvent(id, payload) { await call("event", "POST", { id }, payload); },
    async list(options = {}) { return await call("list", "GET", { limit: options.limit }); },
  };
}

// makeVectorize is the Cloudflare Vectorize binding over the platform index API
// (ADR-158). Values are plain JS arrays (Float32Array works too); the server
// stores float32 and returns float32-rounded values, matching Vectorize.
export function makeVectorize(platform, ns, name, scopeToken) {
  const base = platform.url + "/v1/vectorize/";
  const hdrs = () => headers(platform, scopeToken);
  const qp = (extra) => q(Object.assign({ ns, index: name }, extra || {}));
  async function json(path, body) {
    const r = await pfetch(platform, base + path + "?" + qp(), {
      method: "POST",
      headers: Object.assign({ "content-type": "application/json" }, hdrs()),
      body: JSON.stringify(body),
    });
    await must(r, "vectorize." + path);
    return await r.json();
  }
  function queryBody(vector, options) {
    const o = options || {};
    const body = { vector };
    if (o.topK !== undefined) body.topK = o.topK;
    if (o.returnValues !== undefined) body.returnValues = o.returnValues;
    if (o.returnMetadata !== undefined) body.returnMetadata = o.returnMetadata;
    if (o.namespace !== undefined) body.namespace = o.namespace;
    if (o.filter !== undefined) body.filter = o.filter;
    return body;
  }
  return {
    async insert(vectors) { return await json("insert", { vectors: vectors || [] }); },
    async upsert(vectors) { return await json("upsert", { vectors: vectors || [] }); },
    async query(vector, options) { return await json("query", queryBody(vector, options)); },
    async queryById(id, options) {
      const body = queryBody(undefined, options);
      body.id = id;
      return await json("query", body);
    },
    async getByIds(ids) {
      const r = await pfetch(platform, base + "get?" + qp(), {
        method: "POST",
        headers: Object.assign({ "content-type": "application/json" }, hdrs()),
        body: JSON.stringify({ ids: ids || [] }),
      });
      await must(r, "vectorize.getByIds");
      return await r.json();
    },
    async deleteByIds(ids) { return await json("delete", { ids: ids || [] }); },
    async describe() {
      const r = await pfetch(platform, base + "describe?" + qp(), { method: "GET", headers: hdrs() });
      await must(r, "vectorize.describe");
      return await r.json();
    },
    async listVectors(options) {
      const o = options || {};
      const r = await pfetch(platform, base + "list?" + qp({ count: o.count, cursor: o.cursor }), {
        method: "GET", headers: hdrs(),
      });
      await must(r, "vectorize.listVectors");
      return await r.json();
    },
  };
}

// D1 prepared statements must be local objects. A props-bound WorkerEntrypoint
// method returns an RpcPromise, but Cloudflare's prepare()/bind() are synchronous
// and ORMs pass their returned statements to batch().
export function makeD1FromStub(database) {
  function prepare(sql) {
    let params = [];
    const stmt = {
      sql,
      params,
      bind(...values) { params = values; stmt.params = values; return stmt; },
      async all() { const res = await database.query(sql, params); return { results: res.results || [], success: true, meta: res.meta }; },
       async first(column) { const res = await database.query(sql, params); const rows = res.results || []; if (!rows.length) return null; if (column) { if (!Object.prototype.hasOwnProperty.call(rows[0], column)) throw new Error("D1_ERROR: column not found: " + column); return rows[0][column]; } return rows[0]; },
       async run() { const result = await database.execute(sql, params); return { results: [], ...result }; },
      async raw(options = {}) { const res = await database.query(sql, params); const rows = res.raw || []; return options.columnNames ? [res.columns || [], ...rows] : rows; },
    };
    return stmt;
  }
  const out = {
    prepare,
    async batch(stmts) {
      return database.batchStatements(stmts.map((stmt) => stmt.spec ? stmt.spec() : ({ sql: stmt.sql, params: stmt.params || [] })));
    },
    exec(sql) { return database.exec(sql); },
    dump() { return database.dump(); },
  };
  out.session = (bookmark) => makeD1Session(out, bookmark);
  out.withSession = (bookmark) => makeD1Session(out, bookmark);
  return out;
}

function makeD1Session(database, initialBookmark) {
  let bookmark = initialBookmark || null;
  const wrap = (statement) => ({
    bind(...values) { statement.bind(...values); return this; },
    async all() { const out = await statement.all(); if (out.meta?.bookmark) bookmark = out.meta.bookmark; return out; },
    async first(column) { const out = await statement.first(column); return out; },
    async run() { const out = await statement.run(); if (out.meta?.bookmark) bookmark = out.meta.bookmark; return out; },
    async raw(options) { return statement.raw(options); },
    spec() { return statement.spec ? statement.spec() : { sql: statement.sql, params: statement.params || [] }; },
  });
  return {
    prepare(sql) { return wrap(database.prepare(sql)); },
    async batch(statements) { const out = await database.batch(statements); const last = out[out.length - 1]; if (last?.meta?.bookmark) bookmark = last.meta.bookmark; return out; },
    getBookmark() { return bookmark; },
  };
}

// buildBindings constructs the tenant-visible env from per-binding specs:
//   { KV: {kind:"kv", ns, name, token}, DB: {kind:"d1", ns, name, token}, ... }
// wrapR2Metadata re-attaches a local R2ObjectBody.writeHttpMetadata after the
// props-bound R2 entrypoint returns. A binding call crosses workerd RPC, where
// arguments are serialized by value, so a method running on the binding side
// cannot mutate the caller's Headers object; the function must run in the
// caller's isolate to write into the same Headers instance (Cloudflare parity).
export function wrapR2Metadata(bucket) {
  const map = {
    contentType: "content-type",
    contentLanguage: "content-language",
    contentDisposition: "content-disposition",
    contentEncoding: "content-encoding",
    cacheControl: "cache-control",
    cacheExpiry: "cache-expiry",
  };
  return new Proxy(bucket, {
    get(target, prop, receiver) {
      if (prop === "get") {
        return async (key, options) => {
          const obj = await target.get(key, options);
          if (!obj) return obj;
          const write = (hdrs) => {
            for (const [k, v] of Object.entries(obj.httpMetadata || {})) {
              if (v === undefined || v === null || v === "") continue;
              hdrs.set(map[k] || String(k).toLowerCase(), String(v));
            }
            return hdrs;
          };
          try {
            Object.defineProperty(obj, "writeHttpMetadata", {
              value: write, enumerable: true, configurable: true, writable: true,
            });
            return obj;
          } catch (e) {
            // RPC objects can be frozen: inherit the original and attach the
            // local method to a shallow prototype instead of losing the
            // tenant-side mutation semantics.
            const local = Object.create(obj);
            Object.defineProperty(local, "writeHttpMetadata", {
              value: write, enumerable: true, configurable: true, writable: true,
            });
            return local;
          }
        };
      }
      return Reflect.get(target, prop, receiver);
    },
  });
}

export function buildBindings(platform, specs) {
  const out = {};
  for (const [binding, spec] of Object.entries(specs)) {
    switch (spec.kind) {
      case "kv": out[binding] = makeKV(platform, spec.ns, spec.token); break;
      case "d1": out[binding] = makeD1(platform, spec.ns, spec.name, spec.token); break;
      case "r2": out[binding] = makeR2(platform, spec.ns, spec.name, spec.token); break;
      case "queue": out[binding] = makeQueue(platform, spec.ns, spec.name, spec.token); break;
      case "workflow": out[binding] = makeWorkflow(platform, spec.ns, spec.name, spec.token); break;
      case "vectorize": out[binding] = makeVectorize(platform, spec.ns, spec.index || spec.name, spec.token); break;
      case "do": out[binding] = makeDO(platform, spec); break;
      default: throw new Error("unknown binding kind: " + spec.kind);
    }
  }
  return out;
}
