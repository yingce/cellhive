// Purpose-isolated host credentials. Never inject these keys into tenant env.
const encoder = new TextEncoder();
function b64(bytes) {
  let value = "";
  for (const byte of bytes) value += String.fromCharCode(byte);
  return btoa(value).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}
function unb64(value) {
  if (typeof value !== "string" || !/^[A-Za-z0-9_-]+$/.test(value)) throw new Error("invalid base64url");
  const bytes = Uint8Array.from(atob(value.replace(/-/g, "+").replace(/_/g, "/")), c => c.charCodeAt(0));
  if (b64(bytes) !== value) throw new Error("noncanonical base64url");
  return bytes;
}
// Importing a CryptoKey is asynchronous and relatively expensive; every mint
// and verify call otherwise repeats it. The cache is keyed by the canonical
// base64url string and lives for the isolate, so credentials are never copied
// into tenant-visible state. Store the promise to coalesce concurrent imports.
const signingKeys = new Map();
function signingKey(value) {
  const cached = signingKeys.get(value);
  if (cached !== undefined) return cached;
  let pending;
  try {
    const bytes = unb64(value);
    if (bytes.length !== 32) throw new Error("JWT key must contain 32 bytes");
    pending = crypto.subtle.importKey("raw", bytes, {name: "HMAC", hash: "SHA-256"}, false, ["sign", "verify"]);
  } catch (e) {
    pending = Promise.reject(e);
  }
  signingKeys.set(value, pending);
  pending.catch(() => { if (signingKeys.get(value) === pending) signingKeys.delete(value); });
  return pending;
}
function claimsObject(segment) {
  const text = new TextDecoder("utf-8", {fatal: true}).decode(unb64(segment));
  const value = JSON.parse(text);
  if (!value || Array.isArray(value) || typeof value !== "object") throw new Error("invalid JWT object");
  // Runtime identities have only scalar fields. Count property occurrences
  // before JSON.parse can discard duplicates, including escaped property names.
  const tokens = text.match(/"(?:[^"\\]|\\.)*"|[{}\[\]:,]/g) || [];
  const keys = tokens.filter((token, i) => token.startsWith('"') && tokens[i + 1] === ":").map(token => JSON.parse(token));
  if (keys.length !== Object.keys(value).length || new Set(keys).size !== keys.length ||
      Object.values(value).some(v => v === null || typeof v === "object")) throw new Error("ambiguous JWT object");
  return value;
}
export async function mintJWT(key, kid, claims, now = Math.floor(Date.now() / 1000)) {
  const header = b64(encoder.encode(JSON.stringify({alg: "HS256", typ: "JWT", kid})));
  const payload = b64(encoder.encode(JSON.stringify({ver: 1, type: claims.type, sub: claims.sub,
    aud: "cellhive", iat: now, exp: now + 300, ...claims})));
  const input = header + "." + payload;
  return input + "." + b64(new Uint8Array(await crypto.subtle.sign("HMAC", await signingKey(key), encoder.encode(input))));
}
export async function verifyJWT(key, token, kid, type, now = Math.floor(Date.now() / 1000)) {
  try {
    if (typeof token !== "string" || token.length > 16384) return null;
    const parts = token.split(".");
    if (parts.length !== 3) return null;
    const header = claimsObject(parts[0]);
    if (header.alg !== "HS256" || header.typ !== "JWT" || header.kid !== kid || Object.keys(header).length !== 3) return null;
    if (!await crypto.subtle.verify("HMAC", await signingKey(key), unb64(parts[2]), encoder.encode(parts[0] + "." + parts[1]))) return null;
    const c = claimsObject(parts[1]);
    if (c.ver !== 1 || c.type !== type || c.aud !== "cellhive" || typeof c.sub !== "string" || !c.sub ||
        !Number.isSafeInteger(c.iat) || !Number.isSafeInteger(c.exp) || c.iat < 0 || c.iat > now + 30 || c.exp <= now || c.exp <= c.iat) return null;
    if (type === "internal" && kid !== "token/do-ticket" &&
        (c.exp - c.iat > 300 || Object.keys(c).some(k => !["ver", "type", "sub", "aud", "iat", "exp"].includes(k)))) return null;
    return c;
  } catch { return null; }
}
export async function internalHeaders(key, role = "internal") {
  return {authorization: "Bearer " + await mintJWT(key, "token/" + role, {type: "internal", sub: "workerd"})};
}
export function bearer(req) {
  for (const name of ["x-cellhive-internal-token", "x-cellhive-scope-token", "x-cellhive-admin-token", "x-cellhive-do-ticket"]) {
    if (req.headers.has(name)) return null;
  }
  const match = /^Bearer ([A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+)$/i.exec(req.headers.get("authorization") || "");
  return match ? match[1] : null;
}
export async function authorized(req, key, role = "internal") {
  const token = bearer(req);
  return !!token && !!await verifyJWT(key, token, "token/" + role, "internal");
}
