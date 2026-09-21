// Platform capability entrypoints (WDL-style, ADR-090).
//
// Bindings are WorkerEntrypoint classes exported by the platform worker. The
// env builder creates a props-bound stub with `ctx.exports.KV({ props })` and
// places it in the LOADED tenant worker's env, so a tenant's fetch handler and
// its Durable Object facets get fully-formed `env.KV` natively (both this.env
// and the constructor-parameter env) — no HTTP facade shim needed.
//
// Each entrypoint runs in the (trusted) platform worker and calls cell-agent
// with the scoped token carried in props.

import { WorkerEntrypoint, RpcTarget } from "cloudflare:workers";
import {
  decode, encode, encodedSize, MAX_RPC_BYTES,
  RPC_METHOD_RE, RPC_RESERVED_METHODS, DO_ID_HEADER,
} from "rpc-codec.js";
import { makeDOTransport } from "facades.js";

function q(params) {
  return Object.entries(params)
    .filter(([, v]) => v !== undefined && v !== null)
    .map(([k, v]) => encodeURIComponent(k) + "=" + encodeURIComponent(v))
    .join("&");
}

// W3C trace context: the tenant isolate wrapper (queue-wrapper.js) sets the
// inbound request's traceparent on globalThis; every internal call carries it so
// cell-agent and the dispatch path stay in one trace (ADR-146).
export function setTraceContext(traceparent) {
  if (traceparent) globalThis.__cellhiveTraceparent = traceparent;
  else delete globalThis.__cellhiveTraceparent;
}

function call(env, path, init) {
  const url = env.CELL_URL.replace(/\/$/, "") + path;
  const opts = Object.assign({}, init || {});
  opts.headers = Object.assign({}, (init && init.headers) || {});
  const tp = globalThis.__cellhiveTraceparent;
  if (tp && !opts.headers["traceparent"]) opts.headers["traceparent"] = tp;
  if (env.PLATFORM && typeof env.PLATFORM.fetch === "function") return env.PLATFORM.fetch(url, opts);
  return fetch(url, opts);
}

function scopeHeaders(props) {
  return { "x-cellhive-scope-token": props.token || "" };
}

// must throws a Cloudflare-shaped binding error. The server returns
// {error, message}, so map the platform code onto err.code and a per-binding
// name (D1_ERROR/KVError/R2Error/...) instead of a bare Error.
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

// KV is the Phase-0 pilot capability.
export class KV extends WorkerEntrypoint {
  async get(key, type) {
    const p = this.ctx.props;
    const r = await call(this.env, "/v1/kv/get?" + q({ ns: p.ns, key }), { headers: scopeHeaders(p) });
    if (r.status === 404) return null;
    await must(r, "kv.get");
    return type === "json" ? r.json() : type === "arrayBuffer" ? r.arrayBuffer() : r.text();
  }

  async put(key, value, options = {}) {
    const p = this.ctx.props;
    // Cloudflare KV accepts string | ArrayBuffer | ArrayBufferView | Blob |
    // ReadableStream; pass those to fetch unchanged so a request body streams
    // through byte-for-byte. Plain objects/numbers keep the friendly
    // JSON-encode fallback (a CellHive extension).
    const body = typeof value === "string" || value instanceof ArrayBuffer ||
      ArrayBuffer.isView(value) || value instanceof Blob || value instanceof ReadableStream
      ? value
      : JSON.stringify(value);
    const r = await call(this.env, "/v1/kv/put?" + q({ ns: p.ns, key, ...kvPutOpts(options) }), {
      method: "POST", headers: scopeHeaders(p), body,
    });
    await must(r, "kv.put");
  }

  // getWithMetadata returns { value, metadata } like Cloudflare KV.
  async getWithMetadata(key, type) {
    const p = this.ctx.props;
    const r = await call(this.env, "/v1/kv/get?" + q({ ns: p.ns, key }), { headers: scopeHeaders(p) });
    if (r.status === 404) return { value: null, metadata: null };
    await must(r, "kv.getWithMetadata");
    const metadata = b64decJson(r.headers.get("x-cellhive-kv-metadata"));
    const value = type === "json" ? await r.json() : type === "arrayBuffer" ? await r.arrayBuffer() : await r.text();
    return { value, metadata };
  }

  async delete(key) {
    const p = this.ctx.props;
    const r = await call(this.env, "/v1/kv/delete?" + q({ ns: p.ns, key }), { method: "DELETE", headers: scopeHeaders(p) });
    await must(r, "kv.delete");
  }

  async list(options = {}) {
    const p = this.ctx.props;
    const r = await call(this.env, "/v1/kv/list?" + q({ ns: p.ns, prefix: options.prefix, limit: options.limit, cursor: options.cursor, with_metadata: options.includeMetadata ? 1 : undefined }), {
      headers: scopeHeaders(p),
    });
    await must(r, "kv.list");
    const out = await r.json();
    return {
      keys: (out.keys || []).map(kvKeyOut),
      list_complete: out.list_complete,
      cursor: out.cursor || undefined,
    };
  }
}

function rowsToObjects(res) {
  const cols = (res && res.columns) || [];
  return ((res && res.rows) || []).map((row) => {
    const o = {};
    for (let i = 0; i < cols.length; i++) o[cols[i]] = row[i];
    return o;
  });
}

async function d1call(env, props, path, body) {
  const r = await call(env, "/v1/d1/" + path + "?" + q({ ns: props.ns, db: props.name }), {
    method: "POST", headers: Object.assign({ "content-type": "application/json" }, scopeHeaders(props)), body: JSON.stringify(body),
  });
  await must(r, "d1." + path);
  return (await r.json()).results;
}

// d1Meta maps a server Result onto the Cloudflare D1Meta subset available
// here: changes/last_row_id/changed_db/duration and row counters (rows_read is
// engine-level in D1 and reported as the returned row count).
function d1Meta(res, readRows) {
  const changes = res.rows_affected || 0;
  return {
    changes,
    last_row_id: res.last_row_id || 0,
    changed_db: changes > 0,
    duration: res.duration_ms || 0,
    rows_read: readRows ? (res.rows || []).length : 0,
    rows_written: changes,
  };
}

class D1PreparedStatement extends RpcTarget {
  constructor(env, props, sql) {
    super();
    this.env = env;
    this.props = props;
    this.sql = sql;
    this.params = [];
  }
  bind(...p) {
    this.params = p;
    return this;
  }
  // spec lets batch() read a bound statement back over RPC (plain values only).
  spec() {
    return { sql: this.sql, params: this.params };
  }
  async all() {
    const [res] = await d1call(this.env, this.props, "query", { sql: this.sql, params: this.params });
    return { results: rowsToObjects(res), success: true, meta: d1Meta(res, true) };
  }
  async first(column) {
    const [res] = await d1call(this.env, this.props, "query", { sql: this.sql, params: this.params });
    const objs = rowsToObjects(res);
    if (!objs.length) return null;
    return column ? objs[0][column] : objs[0];
  }
  async run() {
    const [res] = await d1call(this.env, this.props, "exec", { sql: this.sql, params: this.params });
    return { success: true, meta: d1Meta(res, false) };
  }
}

export class D1Database extends WorkerEntrypoint {
  // D1 Sessions / bookmarks (read replication) are not supported: CellHive D1 is
  // a single-writer cell with no read replicas, so a "session" has no meaning
  // (ADR-153). Fail loudly instead of silently ignoring the option.
  session() {
    throw new Error("d1 sessions are not supported by CellHive (no read replication or bookmarks)");
  }
  withSession() {
    throw new Error("d1 sessions are not supported by CellHive (no read replication or bookmarks)");
  }
  prepare(sql) {
    return new D1PreparedStatement(this.env, this.ctx.props, sql);
  }
  async batch(stmts) {
    const specs = await Promise.all(stmts.map((s) => s.spec()));
    const results = await d1call(this.env, this.ctx.props, "batch", {
      statements: specs.map((x) => ({ sql: x.sql, params: x.params || [] })),
    });
    return results.map((res) => ({
      success: true, results: rowsToObjects(res),
      meta: d1Meta(res, false),
    }));
  }
  async exec(sql) {
    const [res] = await d1call(this.env, this.ctx.props, "exec", { sql });
    return { count: res.rows_affected, duration: res.duration_ms };
  }
}

// R2 object helpers: the server returns the full R2Object as JSON on put/head/
// list and as the x-cellhive-r2-meta header on get, so the binding can expose
// the Cloudflare R2Object/R2ObjectBody fields without extra calls.
function r2ObjectFromJSON(o) {
  const etag = o.etag || "";
  return {
    key: o.key,
    size: o.size,
    etag,
    httpEtag: o.httpEtag || (etag ? '"' + etag + '"' : ""),
    version: o.version || "",
    uploaded: o.uploaded ? new Date(o.uploaded) : undefined,
    httpMetadata: o.httpMetadata || {},
    customMetadata: o.customMetadata || {},
    checksums: o.checksums || {},
  };
}

function r2ObjectFromResponse(res, key) {
  const md = b64decJson(res.headers.get("x-cellhive-r2-meta")) || {};
  const etag = res.headers.get("etag") || md.etag || "";
  let size = Number(res.headers.get("content-length") || md.size || 0);
  const cr = res.headers.get("content-range"); // bytes a-b/total
  if (cr) {
    const total = cr.split("/")[1];
    if (total && total !== "*") size = Number(total);
  }
  const checksums = {};
  if (md.md5) checksums.md5 = md.md5;
  if (md.sha256) checksums.sha256 = md.sha256;
  return {
    key,
    size,
    etag,
    httpEtag: etag ? '"' + etag + '"' : "",
    version: "",
    uploaded: md.uploaded_ms ? new Date(md.uploaded_ms) : undefined,
    httpMetadata: md.http || {},
    customMetadata: md.custom || {},
    checksums,
  };
}

// r2ObjectBody returns the Cloudflare R2ObjectBody shape. workerd RPC
// serializes the data fields by value and the body methods as callable stubs,
// so `obj.size`/`obj.httpMetadata` AND `await obj.text()` both work across the
// isolate boundary. The methods are non-enumerable like Cloudflare's prototype
// methods, so JSON.stringify/spread/Object.keys see only the data fields (an
// enumerable RPC-stub function would break JSON serialization).
function r2ObjectBody(buf, info, range) {
  const body = Object.assign({}, info, { bodyUsed: false });
  if (range) {
    body.range = { offset: range.offset, length: range.length === undefined ? buf.byteLength : range.length };
  }
  body.arrayBuffer = async () => buf;
  body.text = async () => new TextDecoder().decode(buf);
  body.json = async () => JSON.parse(new TextDecoder().decode(buf));
  body.blob = async () => new Blob([buf]);
  // Cloudflare R2ObjectBody.body is a ReadableStream, and writeHttpMetadata
  // copies httpMetadata onto a Headers object (R2 binding compatibility).
  body.body = new Blob([buf]).stream();
  body.writeHttpMetadata = (headers) => {
    const md = info.httpMetadata || {};
    const map = {
      contentType: "content-type",
      contentLanguage: "content-language",
      contentDisposition: "content-disposition",
      contentEncoding: "content-encoding",
      cacheControl: "cache-control",
      cacheExpiry: "cache-expiry",
    };
    for (const [k, v] of Object.entries(md)) {
      if (v === undefined || v === null || v === "") continue;
      headers.set(map[k] || k.toLowerCase(), String(v));
    }
    return headers;
  };
  return body;
}

class R2MultipartUpload extends RpcTarget {
  constructor(env, props, key, uploadId) {
    super();
    this.env = env;
    this.props = props;
    this.key = key;
    this.uploadId = uploadId;
  }
  async uploadPart(partNumber, value) {
    const p = this.props;
    const r = await call(this.env, "/v1/r2/multipart/part?" +
      q({ ns: p.ns, bucket: p.name, key: this.key, upload_id: this.uploadId, part_number: partNumber }),
      { method: "PUT", headers: scopeHeaders(p), body: value });
    await must(r, "r2.uploadPart");
    const out = await r.json();
    return { partNumber: out.part_number, etag: out.etag };
  }
  async complete(uploadedParts = []) {
    const p = this.props;
    const parts = uploadedParts.map((x) => ({ part_number: x.partNumber, etag: x.etag }));
    const r = await call(this.env, "/v1/r2/multipart/complete?" +
      q({ ns: p.ns, bucket: p.name, key: this.key, upload_id: this.uploadId }),
      { method: "POST", headers: Object.assign({ "content-type": "application/json" }, scopeHeaders(p)), body: JSON.stringify({ parts }) });
    await must(r, "r2.complete");
    const out = await r.json();
    return { key: out.key, size: out.size, etag: out.etag };
  }
  async abort() {
    const p = this.props;
    const r = await call(this.env, "/v1/r2/multipart?" +
      q({ ns: p.ns, bucket: p.name, key: this.key, upload_id: this.uploadId }),
      { method: "DELETE", headers: scopeHeaders(p) });
    await must(r, "r2.abort");
  }
}

export class R2Bucket extends WorkerEntrypoint {
  async put(key, value, options = {}) {
    const p = this.ctx.props;
    const body = typeof value === "string" ? value : value;
    const headers = Object.assign({}, scopeHeaders(p));
    if (options.httpMetadata) headers["x-cellhive-r2-http-metadata"] = b64enc(JSON.stringify(options.httpMetadata));
    if (options.customMetadata) headers["x-cellhive-r2-custom-metadata"] = b64enc(JSON.stringify(options.customMetadata));
    if (options.md5) headers["x-cellhive-r2-md5"] = options.md5;
    if (options.sha256) headers["x-cellhive-r2-sha256"] = options.sha256;
    const r = await call(this.env, "/v1/r2/object?" + q({ ns: p.ns, bucket: p.name, key }), {
      method: "PUT", headers, body,
    });
    await must(r, "r2.put");
    return r2ObjectFromJSON(await r.json());
  }
  async get(key, options = {}) {
    const p = this.ctx.props;
    const h = Object.assign({}, scopeHeaders(p));
    if (options.range) {
      // A missing length means "from offset to the end": emitting
      // bytes=<off>-<off-1> would be a malformed (inverted) range.
      const end = options.range.length ? options.range.offset + options.range.length - 1 : "";
      h["range"] = "bytes=" + options.range.offset + "-" + end;
    }
    const r = await call(this.env, "/v1/r2/object?" + q({ ns: p.ns, bucket: p.name, key }), { headers: h });
    if (r.status === 404) return null;
    await must(r, "r2.get");
    const info = r2ObjectFromResponse(r, key);
    return r2ObjectBody(await r.arrayBuffer(), info, options.range);
  }
  async head(key) {
    const p = this.ctx.props;
    const r = await call(this.env, "/v1/r2/object?" + q({ ns: p.ns, bucket: p.name, key, head: 1 }), { headers: scopeHeaders(p) });
    if (r.status === 404) return null;
    await must(r, "r2.head");
    return r2ObjectFromJSON(await r.json());
  }
  async delete(key) {
    const p = this.ctx.props;
    const r = await call(this.env, "/v1/r2/object?" + q({ ns: p.ns, bucket: p.name, key }), { method: "DELETE", headers: scopeHeaders(p) });
    await must(r, "r2.delete");
  }
  async createMultipartUpload(key) {
    const p = this.ctx.props;
    const r = await call(this.env, "/v1/r2/multipart/create?" + q({ ns: p.ns, bucket: p.name, key }), {
      method: "POST", headers: scopeHeaders(p),
    });
    await must(r, "r2.createMultipartUpload");
    const out = await r.json();
    return new R2MultipartUpload(this.env, p, key, out.upload_id);
  }
  resumeMultipartUpload(key, uploadId) {
    return new R2MultipartUpload(this.env, this.ctx.props, key, uploadId);
  }
  async createPresignedUrl(key, options = {}) {
    const p = this.ctx.props;
    const r = await call(this.env, "/v1/r2/presign?" + q({ ns: p.ns, bucket: p.name, key, expires_in: options.expiresIn }), { headers: scopeHeaders(p) });
    await must(r, "r2.presign");
    return (await r.json()).url;
  }
  async list(options = {}) {
    const p = this.ctx.props;
    const r = await call(this.env, "/v1/r2/list?" + q({
      ns: p.ns, bucket: p.name, prefix: options.prefix, limit: options.limit,
      cursor: options.cursor, start_after: options.startAfter,
      delimiter: options.delimiter,
      include: Array.isArray(options.include) ? options.include.join(",") : options.include,
    }), { headers: scopeHeaders(p) });
    await must(r, "r2.list");
    const out = await r.json();
    // R2 list semantics: {objects, truncated, cursor?} with an exclusive cursor.
    const res = {
      objects: (out.objects || []).map(r2ObjectFromJSON),
      truncated: !!out.truncated,
      cursor: out.cursor,
    };
    if (options.delimiter !== undefined) res.delimitedPrefixes = out.delimitedPrefixes || [];
    return res;
  }
}

export class QueueProducer extends WorkerEntrypoint {
  async send(body, options = {}) {
    const p = this.ctx.props;
    const r = await call(this.env, "/v1/queue/send?" + q({ ns: p.ns, queue: p.name, delay_seconds: options.delaySeconds, idempotency_key: options.idempotencyKey }), {
      method: "POST",
      headers: Object.assign({ "content-type": typeof body === "string" ? "text/plain" : "application/json" }, scopeHeaders(p)),
      body: typeof body === "string" ? body : JSON.stringify(body),
    });
    await must(r, "queue.send");
    return { id: (await r.json()).id };
  }
}

function aiOpenAIBody(model, inputs) {
  const i = inputs || {};
  if (Array.isArray(i.messages)) return { model, messages: i.messages };
  const prompt = typeof i.prompt === "string" ? i.prompt : (typeof inputs === "string" ? inputs : "");
  return { model, messages: [{ role: "user", content: prompt }] };
}

// AI (BYO OpenAI-compatible): env.AI.run(model, inputs) -> POST <AI_URL>/chat/completions.
export class AI extends WorkerEntrypoint {
  async run(model, inputs) {
    const env = this.env;
    if (!env.AI_URL) throw new Error("ai: AI_URL is not configured");
    const r = await fetch(env.AI_URL.replace(/\/$/, "") + "/chat/completions", {
      method: "POST",
      headers: Object.assign({ "content-type": "application/json" }, env.AI_KEY ? { authorization: "Bearer " + env.AI_KEY } : {}),
      body: JSON.stringify(aiOpenAIBody(model, inputs)),
    });
    if (!r.ok) throw new Error("ai: " + r.status + " " + (await r.text()));
    const out = await r.json();
    const choice = (out.choices && out.choices[0]) || {};
    const text = choice.message ? choice.message.content : (choice.text || "");
    return { response: text, model: out.model || model, usage: out.usage };
  }
}

// Hyperdrive (ADR-125): the platform supplies the origin connection string. It
// does NOT proxy or pool connections: pool inside the isolate with the driver
// (node-postgres Pool / mysql2 pool), or point the origin at an operator-run
// pooler. Returned as a plain data object (like Cloudflare's binding), so the
// tenant can read `env.HYPERDRIVE.connectionString` directly.
export function Hyperdrive(props) {
  const cs = (props && props.connectionString) || "";
  const parts = parseConnectionString(cs);
  return {
    connectionString: cs,
    host: parts.host,
    port: parts.port,
    user: parts.user,
    password: parts.password,
    database: parts.database,
  };
}

function parseConnectionString(cs) {
  const out = { host: "", port: 0, user: "", password: "", database: "" };
  try {
    const u = new URL(cs);
    out.host = u.hostname;
    out.port = u.port ? Number(u.port) : (u.protocol.startsWith("postgres") ? 5432 : 3306);
    out.user = decodeURIComponent(u.username || "");
    out.password = decodeURIComponent(u.password || "");
    out.database = (u.pathname || "").replace(/^\//, "");
  } catch (e) {
    // Not a URL: leave the parsed fields empty; connectionString still works.
  }
  return out;
}

// serviceLoader, when set by the user-runtime loader, loads the pinned target
// worker in THIS workerd instance and returns its stub, so env.SVC.<method>()
// is a native JSRPC call (structured clone, RpcTarget, streams) with no
// cell-agent hop. Absent -> the HTTP/JSON fallback below is used.
let serviceLoader = null;
export function setServiceLoader(fn) { serviceLoader = fn; }

// ServiceBinding lets a tenant worker call another worker (same namespace):
// env.SVC.fetch() and, via a Proxy, env.SVC.<method>(...) RPC to the target's
// named entrypoint. With a loader injected, RPC is native and same-instance;
// otherwise cell-agent resolves the target and user-runtime runs it (JSON).
function serviceRpc(ctx, env, props) {
  return async (method, args) => {
    if (serviceLoader && props.version && env.CH_SERVICE_NATIVE !== "0") {
      // Native, same-instance JSRPC: the platform wrapper's callMethod takes JS
      // args (structured clone) and resolves the tenant's named entrypoint, so
      // RpcTarget/streams/typed values survive. No cell-agent hop, no JSON.
      const stub = await serviceLoader(ctx, env, { ns: props.ns, worker: props.target, version: props.version });
      const host = stub.getEntrypoint("CellHiveHost");
      return await host.callMethod(props.entrypoint || "", method, args, globalThis.__cellhiveTraceparent);
    }
    // props.ns is the (possibly cross-namespace) target; the scope token stays
    // bound to the caller, so the request carries both (ADR-144).
    const r = await call(env, "/v1/service/run?" + q({
      ns: props.caller_ns || props.ns,
      target_ns: props.caller_ns && props.caller_ns !== props.ns ? props.ns : undefined,
      worker: props.target,
    }), {
      method: "POST",
      headers: Object.assign({ "content-type": "application/json", "x-cellhive-scope-token": props.token }),
      body: JSON.stringify({ entrypoint: props.entrypoint || "", method, args: args || [] }),
    });
    if (!r.ok) throw new Error("service." + method + ": " + r.status + " " + (await r.text()));
    return (await r.json()).result;
  };
}

export class ServiceBinding extends WorkerEntrypoint {
  constructor(ctx, env) {
    super(ctx, env);
    const props = ctx.props || {};
    // Synthesize a forwarder for any unknown method so env.SVC.<anyMethod>(...)
    // reaches the target's entrypoint over RPC (fetch stays a real method).
    return new Proxy(this, {
      get(target, prop, receiver) {
        if (typeof prop !== "string" || prop === "then" || prop === "toJSON") {
          return Reflect.get(target, prop, receiver);
        }
        const real = Reflect.get(target, prop, receiver);
        if (real !== undefined) return real;
        const forward = serviceRpc(ctx, env, props);
        return (...args) => forward(prop, args);
      },
    });
  }

  async fetch(input, init) {
    const p = this.ctx.props;
    const req = input instanceof Request ? input : new Request(input, init);
    // Native same-instance path (ADR-102): load the pinned target locally and
    // call its fetch over JSRPC. Errors surface; we do not fall back after a
    // dispatch (that could duplicate side effects).
    if (serviceLoader && p.version && this.env.CH_SERVICE_NATIVE !== "0") {
      const stub = await serviceLoader(this.ctx, this.env, { ns: p.ns, worker: p.target, version: p.version });
      const host = stub.getEntrypoint("CellHiveHost");
      const clean = new Request(req);
      for (const k of [...clean.headers.keys()]) {
        if (k.toLowerCase().startsWith("x-cellhive-")) clean.headers.delete(k);
      }
      return p.entrypoint ? await host.callMethod(p.entrypoint, "fetch", [clean]) : await host.fetch(clean);
    }
    const hasBody = req.method !== "GET" && req.method !== "HEAD";
    const body = hasBody ? await req.arrayBuffer() : undefined;
    const r = await call(this.env, "/v1/service/fetch?" + q({
      ns: p.caller_ns || p.ns,
      target_ns: p.caller_ns && p.caller_ns !== p.ns ? p.ns : undefined,
      worker: p.target,
    }), {
      method: "POST",
      headers: Object.assign({
        "x-cellhive-scope-token": p.token,
        "x-cellhive-req-method": req.method,
        "x-cellhive-req-url": req.url,
        "x-cellhive-req-content-type": req.headers.get("content-type") || "text/plain",
        "x-cellhive-req-entrypoint": p.entrypoint || "",
      }),
      body,
    });
    if (!r.ok && r.status !== 404 && r.status !== 500) throw new Error("service.fetch: " + r.status);
    return new Response(await r.arrayBuffer(), {
      status: r.status,
      headers: { "content-type": r.headers.get("content-type") || "text/plain" },
    });
  }
}

// --- Durable Objects (WDL alignment, ADR-090/ADR-184) ----------------------
//
// The namespace entrypoint runs in the trusted platform worker: the :7001
// transport, the per-binding scoped token, and the owner-hint cache all stay
// here. Tenants only ever receive RPC stubs, so no platform transport (and no
// generic network binding) is needed in the tenant env.
//
// The object id travels as an ARGUMENT to a method on this entrypoint, never
// as an RpcTarget the tenant calls: a WebSocket upgrade can cross workerLoader
// RPC only as the return value of an entrypoint-stub method. Returning the same
// 101 from an RpcTarget instance fails ("Could not serialize object of type
// WebSocket"; pinned-workerd experiment). WDL's do-client uses the same
// fetch(request)/rpcObject shape; facades.js makeDOFromStub rebuilds the CF-shaped
// env.NS.get(id).fetch(...) on the tenant side over these two methods.

// Module-level transport cache: workerLoader may instantiate the entrypoint
// per RPC call, so an instance field would drop the owner-hint cache between
// calls. Keyed by the binding identity; bounded FIFO.
const doTransports = new Map();
const DO_TRANSPORT_MAX = 256;
function doTransportFor(env, props) {
  const key = [props.ns, props.worker, props.bundle_sha, props.version, props.class, props.storage_id || ""].join("\u0000");
  let t = doTransports.get(key);
  if (t === undefined) {
    t = makeDOTransport({ url: env.CELL_URL, token: "", fetcher: env.PLATFORM }, props);
    if (doTransports.size >= DO_TRANSPORT_MAX) {
      doTransports.delete(doTransports.keys().next().value);
    }
    doTransports.set(key, t);
  }
  return t;
}

export class DurableObjectNamespace extends WorkerEntrypoint {
  #transport() {
    return doTransportFor(this.env, this.ctx.props || {});
  }
  // fetch performs the DO fetch — including a WebSocket upgrade — and returns
  // the object's Response. The method MUST be named fetch and take the Request
  // as its only argument: workerd preserves fetch semantics (streaming, 101 +
  // WebSocket passthrough) only for that shape. With any other name, or with
  // the id as a leading argument, the return is a generic RPC value and a
  // WebSocket refuses to serialize. The object id therefore rides in a header
  // (DO_ID_HEADER), stripped before the call so tenant DO code never sees it.
  async fetch(request) {
    const id = String(request.headers.get(DO_ID_HEADER) || "");
    const clean = new Request(request);
    clean.headers.delete(DO_ID_HEADER);
    return await this.#transport().__call(id, clean);
  }
  // rpcObject forwards an arbitrary DO method as (method, args) data (ADR-162).
  async rpcObject(id, method, args) {
    if (typeof method !== "string" || !RPC_METHOD_RE.test(method) ||
        method.startsWith("__ch") || RPC_RESERVED_METHODS.has(method)) {
      throw new Error("do: rpc method " + method + " is not allowed");
    }
    return await this.#transport().__rpc(String(id), method, args);
  }
  idFromName(name) { return String(name); }
  idFromString(s) { return String(s); }
  newUniqueId() { return crypto.randomUUID(); }
}

// WorkflowInstanceTarget is the CF-shaped instance returned by get(id); every
// method is data-only so it survives workerLoader RPC.
class WorkflowInstanceTarget extends RpcTarget {
  #b;
  #id;
  constructor(b, id) {
    super();
    this.#b = b;
    this.#id = String(id);
  }
  async status() {
    const s = await this.#b.__call("get", "GET", { id: this.#id });
    return { status: s.status, error: s.error || undefined, output: WorkflowBinding.decode(s.output) };
  }
  async pause() { await this.#b.__call("pause", "POST", { id: this.#id }); }
  async resume() { await this.#b.__call("resume", "POST", { id: this.#id }); }
  async terminate() { await this.#b.__call("terminate", "POST", { id: this.#id }); }
  async restart() { await this.#b.__call("restart", "POST", { id: this.#id }); }
  async delete() { await this.#b.__call("delete", "POST", { id: this.#id }); }
  async sendEvent(payload) { await this.#b.__call("event", "POST", { id: this.#id }, payload); }
}

// WorkflowBinding is the platform-side workflow namespace (create/get/...):
// the tenant env holds only this stub.
export class WorkflowBinding extends WorkerEntrypoint {
  #base() { return this.env.CELL_URL.replace(/\/$/, "") + "/v1/workflow/"; }
  #headers() {
    const token = (this.ctx.props || {}).token;
    return token ? { "x-cellhive-scope-token": token, "content-type": "application/json" } : { "content-type": "application/json" };
  }
  async __call(path, method, extra, body) {
    const props = this.ctx.props || {};
    const r = await this.env.PLATFORM.fetch(
      this.#base() + path + "?" + q({ ns: props.ns, workflow: props.name, ...(extra || {}) }),
      {
        method: method || "GET",
        headers: this.#headers(),
        body: body === undefined ? undefined : JSON.stringify(body),
      },
    );
    await must(r, "workflow." + path);
    return await r.json();
  }
  static decode(b64) {
    if (!b64) return undefined;
    try { return JSON.parse(atob(b64)); } catch (e) { return undefined; }
  }
  async create(options = {}) {
    const out = await this.__call("create", "POST", { id: options.id || undefined },
      options.params === undefined ? undefined : options.params);
    return { id: out.id };
  }
  async get(id) { return new WorkflowInstanceTarget(this, id); }
  async sendEvent(id, payload) { await this.__call("event", "POST", { id }, payload); }
  async list(options = {}) { return await this.__call("list", "GET", { limit: options.limit }); }
}

// Vectorize is the platform-side index binding (ADR-158): data-only methods,
// so the tenant env needs no transport for it.
export class Vectorize extends WorkerEntrypoint {
  #base() { return this.env.CELL_URL.replace(/\/$/, "") + "/v1/vectorize/"; }
  #hdrs() {
    const token = (this.ctx.props || {}).token;
    return token ? { "x-cellhive-scope-token": token } : {};
  }
  #qp(extra) {
    const p = this.ctx.props || {};
    return q(Object.assign({ ns: p.ns, index: p.index || p.name }, extra || {}));
  }
  async #json(path, body) {
    const r = await this.env.PLATFORM.fetch(this.#base() + path + "?" + this.#qp(), {
      method: "POST",
      headers: Object.assign({ "content-type": "application/json" }, this.#hdrs()),
      body: JSON.stringify(body),
    });
    await must(r, "vectorize." + path);
    return await r.json();
  }
  static #queryBody(vector, options) {
    const o = options || {};
    const body = { vector };
    if (o.topK !== undefined) body.topK = o.topK;
    if (o.returnValues !== undefined) body.returnValues = o.returnValues;
    if (o.returnMetadata !== undefined) body.returnMetadata = o.returnMetadata;
    if (o.namespace !== undefined) body.namespace = o.namespace;
    if (o.filter !== undefined) body.filter = o.filter;
    return body;
  }
  async insert(vectors) { return await this.#json("insert", { vectors: vectors || [] }); }
  async upsert(vectors) { return await this.#json("upsert", { vectors: vectors || [] }); }
  async query(vector, options) { return await this.#json("query", Vectorize.#queryBody(vector, options)); }
  async queryById(id, options) {
    const body = Vectorize.#queryBody(undefined, options);
    body.id = id;
    return await this.#json("query", body);
  }
  async getByIds(ids) {
    const r = await this.env.PLATFORM.fetch(this.#base() + "get?" + this.#qp(), {
      method: "POST",
      headers: Object.assign({ "content-type": "application/json" }, this.#hdrs()),
      body: JSON.stringify({ ids: ids || [] }),
    });
    await must(r, "vectorize.getByIds");
    return await r.json();
  }
  async deleteByIds(ids) { return await this.#json("delete", { ids: ids || [] }); }
  async describe() {
    const r = await this.env.PLATFORM.fetch(this.#base() + "describe?" + this.#qp(), { method: "GET", headers: this.#hdrs() });
    await must(r, "vectorize.describe");
    return await r.json();
  }
  async listVectors(options) {
    const o = options || {};
    const r = await this.env.PLATFORM.fetch(this.#base() + "list?" + this.#qp({ count: o.count, cursor: o.cursor }), {
      method: "GET", headers: this.#hdrs(),
    });
    await must(r, "vectorize.listVectors");
    return await r.json();
  }
}

// PlatformBridge is the platform-side bridge for wrappers that receive an
// explicit capability over JSRPC. Identity (ns/worker/workflow/id/run) is bound
// in props, never taken from the caller.
export class PlatformBridge extends WorkerEntrypoint {
  // Fixed op -> (method, path) table for workflow step callbacks: the wrapper
  // picks an op, never a raw path, so the internal token cannot be turned into
  // an open relay or a cross-tenant tool.
  static #OPS = new Map([
    ["attempt.get", ["GET", "/v1/internal/workflow/attempt"]],
    ["attempt.put", ["PUT", "/v1/internal/workflow/attempt"]],
    ["attempt.delete", ["DELETE", "/v1/internal/workflow/attempt"]],
    ["state.get", ["GET", "/v1/internal/workflow/state"]],
    ["step.get", ["GET", "/v1/internal/workflow/step"]],
    ["step.put", ["PUT", "/v1/internal/workflow/step"]],
    ["sleep", ["POST", "/v1/internal/workflow/sleep"]],
    ["wait.get", ["GET", "/v1/internal/workflow/wait"]],
    ["wait.put", ["POST", "/v1/internal/workflow/wait"]],
    ["wait.delete", ["DELETE", "/v1/internal/workflow/wait"]],
    ["event.consume", ["POST", "/v1/internal/workflow/event/consume"]],
    ["finish", ["POST", "/v1/internal/workflow/finish"]],
    ["finish.error", ["POST", "/v1/internal/workflow/finish"]],
  ]);
  static #PARAM_RE = /^[a-z_]+$/;
  static #IDENTITY_KEYS = new Set(["ns", "workflow", "id", "run"]);

  async workflowStep(op, params, body) {
    const entry = PlatformBridge.#OPS.get(String(op));
    if (!entry) throw new Error("workflow: unknown step op " + op);
    const [method, path] = entry;
    const props = this.ctx.props || {};
    const qs = ["ns=" + encodeURIComponent(props.ns || "")];
    for (const k of ["workflow", "id", "run"]) {
      const v = props[k];
      if (v !== undefined && v !== null && v !== "") qs.push(k + "=" + encodeURIComponent(String(v)));
    }
    for (const [k, v] of Object.entries(params || {})) {
      if (!PlatformBridge.#PARAM_RE.test(k) || PlatformBridge.#IDENTITY_KEYS.has(k)) continue;
      if (v === undefined || v === null || v === "") continue;
      qs.push(encodeURIComponent(k) + "=" + encodeURIComponent(String(v)));
    }
    const r = await this.env.PLATFORM.fetch(
      this.env.CELL_URL.replace(/\/$/, "") + path + "?" + qs.join("&"),
      {
        method,
        headers: { "x-cellhive-internal-token": this.env.CELL_TOKEN || "" },
        body: body === undefined || body === null ? undefined : body,
      },
    );
    const text = await r.text();
    return { status: r.status, body: text, contentType: r.headers.get("content-type") || "text/plain" };
  }

  // Log ring ingest: ns/worker come from props, so a tenant cannot forge
  // entries for another namespace.
  async logSend(batch) {
    const props = this.ctx.props || {};
    const r = await this.env.PLATFORM.fetch(
      this.env.CELL_URL.replace(/\/$/, "") + "/v1/internal/logs?ns=" + encodeURIComponent(props.ns || "") +
        "&worker=" + encodeURIComponent(props.worker || ""),
      {
        method: "POST",
        headers: { "content-type": "application/json", "x-cellhive-internal-token": this.env.LOG_TOKEN || this.env.CELL_TOKEN || "" },
        body: batch,
      },
    );
    return r.status;
  }
}

// bindingStub materializes a binding spec as a props-bound entrypoint stub, or
// undefined when the kind is not yet migrated to this architecture.
export function bindingStub(ctx, spec) {
  const props = Object.assign({}, spec);
  switch (spec && spec.kind) {
    case "kv":
      return ctx.exports.KV({ props });
    case "d1":
      return ctx.exports.D1Database({ props });
    case "r2":
      return ctx.exports.R2Bucket({ props });
    case "queue":
      return ctx.exports.QueueProducer({ props });
    case "service":
      return ctx.exports.ServiceBinding({ props });
    case "ai":
      return ctx.exports.AI({ props });
    case "hyperdrive":
      return Hyperdrive(props);
    case "do":
      return ctx.exports.DurableObjectNamespace({ props });
    case "workflow":
      return ctx.exports.WorkflowBinding({ props });
    case "vectorize":
      return ctx.exports.Vectorize({ props });
    default:
      return undefined;
  }
}
