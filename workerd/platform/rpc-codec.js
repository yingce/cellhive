// CellHive DO RPC tagged codec (ADR-162).
//
// The client binding facade (facades.js) and the do-runtime host (host.js) share
// this module. It converts a JS value into a JSON-safe tagged tree and back,
// preserving the subset of structured clone that can survive the JSON hop
// between the tenant isolate and the DO host: undefined, -0/NaN/±Infinity,
// bigint, Date, RegExp, Map, Set, ArrayBuffer, TypedArray, DataView, Error,
// URL, URLSearchParams, plus shared references and cycles. Functions, symbols,
// promises, weak collections, streams and RPC stubs are rejected. Class
// instances degrade to plain objects (own enumerable string keys), matching
// structured clone's observable behaviour for prototypes.

export const MAX_RPC_BYTES = 8 * 1024 * 1024;
const MAX_RPC_DEPTH = 256;

// Method names allowed over RPC: identifiers only. Reserved names cover the DO
// protocol itself and JS object internals, so a tenant cannot reach the
// platform host methods or prototype plumbing through a stub.
export const RPC_METHOD_RE = /^[A-Za-z_$][A-Za-z0-9_$]*$/;
export const RPC_RESERVED_METHODS = new Set([
  "fetch", "alarm",
  "constructor", "__proto__", "prototype",
  "then", "toJSON", "toString", "valueOf",
  "hasOwnProperty", "isPrototypeOf", "propertyIsEnumerable", "toLocaleString",
]);

export class RpcCodecError extends Error {
  constructor(code, message) {
    super(message);
    this.name = "RpcCodecError";
    this.code = code;
  }
}

function unsupported(message) {
  return new RpcCodecError("do_rpc_unsupported_value", message);
}

const TEXT_ENCODER = new TextEncoder();

function bytesToBase64(bytes) {
  let bin = "";
  const chunk = 0x8000;
  for (let i = 0; i < bytes.length; i += chunk) {
    bin += String.fromCharCode.apply(null, bytes.subarray(i, i + chunk));
  }
  return btoa(bin);
}

function base64ToBytes(b64) {
  const bin = atob(b64);
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
}

const TYPED_ARRAYS = {
  Int8Array, Uint8Array, Uint8ClampedArray, Int16Array, Uint16Array,
  Int32Array, Uint32Array, Float32Array, Float64Array,
  BigInt64Array, BigUint64Array,
};

function ownStringKeys(v) {
  return Object.keys(v);
}

/**
 * Encode a value into a tagged JSON tree. Referenceable nodes carry an
 * incrementing `i` on first visit; later visits emit {t:"ref",i}. Callers size
 * the result with encodedSize() and enforce MAX_RPC_BYTES.
 * @param {unknown} value
 * @returns {unknown}
 */
export function encode(value) {
  const refs = new Map();
  let next = 0;

  function walk(v, depth) {
    if (depth > MAX_RPC_DEPTH) throw unsupported("value nesting too deep");
    if (v === null) return null;
    const tv = typeof v;
    if (tv === "string" || tv === "boolean") return v;
    if (tv === "number") {
      if (Number.isFinite(v) && !Object.is(v, -0)) return v;
      if (Object.is(v, -0)) return { t: "number", v: "-0" };
      if (Number.isNaN(v)) return { t: "number", v: "NaN" };
      return { t: "number", v: v > 0 ? "Infinity" : "-Infinity" };
    }
    if (tv === "undefined") return { t: "undefined" };
    if (tv === "bigint") return { t: "bigint", v: v.toString() };
    if (tv === "function" || tv === "symbol") {
      throw unsupported("cannot serialize a " + tv);
    }
    if (tv !== "object") throw unsupported("cannot serialize " + tv);

    if (refs.has(v)) return { t: "ref", i: refs.get(v) };
    const idx = next++;
    refs.set(v, idx);

    if (Array.isArray(v)) {
      const items = [];
      for (let k = 0; k < v.length; k++) items.push(walk(v[k], depth + 1));
      return { t: "a", i: idx, v: items };
    }
    if (v instanceof Date) return { t: "Date", i: idx, v: v.getTime() };
    if (v instanceof RegExp) return { t: "RegExp", i: idx, s: v.source, f: v.flags };
    if (v instanceof Map) {
      const entries = [];
      for (const [k, val] of v) entries.push([walk(k, depth + 1), walk(val, depth + 1)]);
      return { t: "Map", i: idx, v: entries };
    }
    if (v instanceof Set) {
      const items = [];
      for (const e of v) items.push(walk(e, depth + 1));
      return { t: "Set", i: idx, v: items };
    }
    if (v instanceof ArrayBuffer) {
      return { t: "ArrayBuffer", i: idx, b: bytesToBase64(new Uint8Array(v)) };
    }
    if (ArrayBuffer.isView(v)) {
      const bytes = new Uint8Array(v.buffer, v.byteOffset, v.byteLength);
      if (v instanceof DataView) return { t: "DataView", i: idx, b: bytesToBase64(bytes) };
      const name = v.constructor && v.constructor.name;
      if (!TYPED_ARRAYS[name]) throw unsupported("unsupported view " + name);
      return { t: "TypedArray", i: idx, c: name, b: bytesToBase64(bytes) };
    }
    if (v instanceof Error) {
      const node = { t: "Error", i: idx, name: v.name, message: v.message };
      if (typeof v.stack === "string") node.stack = v.stack;
      if ("cause" in v) node.cause = walk(v.cause, depth + 1);
      return node;
    }
    if (v instanceof URL) return { t: "URL", i: idx, v: v.href };
    if (v instanceof URLSearchParams) return { t: "URLSearchParams", i: idx, v: v.toString() };

    // Plain objects and (degraded) class instances: own enumerable string keys.
    const proto = Object.getPrototypeOf(v);
    if (proto !== null && proto !== Object.prototype) {
      // Structured clone drops the prototype; keep the data.
      const node = { t: "o", i: idx, v: {} };
      for (const k of ownStringKeys(v)) node.v[k] = walk(v[k], depth + 1);
      return node;
    }
    const node = { t: "o", i: idx, v: {} };
    for (const k of ownStringKeys(v)) node.v[k] = walk(v[k], depth + 1);
    return node;
  }

  return walk(value, 0);
}

/**
 * Decode a tagged tree produced by encode(). Handles cycles and shared
 * references by registering containers before filling them.
 * @param {unknown} node
 * @returns {unknown}
 */
export function decode(node) {
  const refs = [];

  function reserve(i, value) {
    if (i < 0) return;
    if (refs[i] !== undefined) throw unsupported("duplicate reference index");
    refs[i] = value;
  }

  function walk(n, depth) {
    if (depth > MAX_RPC_DEPTH) throw unsupported("value nesting too deep");
    if (n === null) return null;
    const tn = typeof n;
    if (tn === "string" || tn === "number" || tn === "boolean") return n;
    if (tn !== "object" || Array.isArray(n) || typeof n.t !== "string") {
      throw unsupported("invalid tagged node");
    }
    const i = Number.isInteger(n.i) ? n.i : -1;
    switch (n.t) {
      case "undefined":
        return undefined;
      case "number": {
        if (n.v === "NaN") return NaN;
        if (n.v === "Infinity") return Infinity;
        if (n.v === "-Infinity") return -Infinity;
        if (n.v === "-0") return -0;
        throw unsupported("invalid number tag " + n.v);
      }
      case "bigint":
        return BigInt(n.v);
      case "ref": {
        if (!Number.isInteger(n.i) || refs[n.i] === undefined) {
          throw unsupported("dangling reference");
        }
        return refs[n.i];
      }
      case "a": {
        const arr = [];
        reserve(i, arr);
        if (!Array.isArray(n.v)) throw unsupported("invalid array node");
        for (const e of n.v) arr.push(walk(e, depth + 1));
        return arr;
      }
      case "o": {
        const obj = {};
        reserve(i, obj);
        if (n.v === null || typeof n.v !== "object") throw unsupported("invalid object node");
        for (const k of Object.keys(n.v)) obj[k] = walk(n.v[k], depth + 1);
        return obj;
      }
      case "Map": {
        const m = new Map();
        reserve(i, m);
        for (const pair of n.v) {
          if (!Array.isArray(pair) || pair.length !== 2) throw unsupported("invalid Map entry");
          m.set(walk(pair[0], depth + 1), walk(pair[1], depth + 1));
        }
        return m;
      }
      case "Set": {
        const s = new Set();
        reserve(i, s);
        for (const e of n.v) s.add(walk(e, depth + 1));
        return s;
      }
      case "Date": {
        const d = new Date(n.v);
        reserve(i, d);
        return d;
      }
      case "RegExp": {
        const r = new RegExp(n.s, n.f);
        reserve(i, r);
        return r;
      }
      case "ArrayBuffer": {
        const buf = base64ToBytes(n.b).buffer;
        reserve(i, buf);
        return buf;
      }
      case "TypedArray": {
        const C = TYPED_ARRAYS[n.c];
        if (!C) throw unsupported("unsupported view " + n.c);
        const bytes = base64ToBytes(n.b);
        const ta = new C(bytes.buffer);
        reserve(i, ta);
        return ta;
      }
      case "DataView": {
        const dv = new DataView(base64ToBytes(n.b).buffer);
        reserve(i, dv);
        return dv;
      }
      case "Error": {
        const e = new Error(typeof n.message === "string" ? n.message : "");
        if (typeof n.name === "string" && n.name) e.name = n.name;
        if (typeof n.stack === "string" && n.stack) e.stack = n.stack;
        reserve(i, e);
        if ("cause" in n) e.cause = walk(n.cause, depth + 1);
        return e;
      }
      case "URL": {
        const u = new URL(n.v);
        reserve(i, u);
        return u;
      }
      case "URLSearchParams": {
        const p = new URLSearchParams(n.v);
        reserve(i, p);
        return p;
      }
      default:
        throw unsupported("unknown tag " + n.t);
    }
  }

  return walk(node, 0);
}

/** Byte length of the JSON encoding of a tagged tree. */
export function encodedSize(tree) {
  return TEXT_ENCODER.encode(JSON.stringify(tree)).length;
}
