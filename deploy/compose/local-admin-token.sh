#!/usr/bin/env bash
# Print a five-minute bootstrap admin JWT. Invoke again before each request;
# never store the result as a long-lived service credential.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
set -a
. "$HERE/cellhive.local.env"
set +a
node --input-type=module -e '
import {createHmac, hkdfSync} from "node:crypto";
const root = (process.env.CELLHIVE_ROOT_KEY || "").trim();
let bytes;
if (/^(?:[0-9a-fA-F]{2}){16,}$/.test(root)) {
  bytes = Buffer.from(root, "hex");
} else if (/^[A-Za-z0-9+/_-]+={0,2}$/.test(root)) {
  const normalized = root.replace(/-/g, "+").replace(/_/g, "/");
  const decoded = Buffer.from(normalized, "base64");
  if (decoded.toString("base64").replace(/=+$/, "") === normalized.replace(/=+$/, "")) bytes = decoded;
}
if (!bytes || bytes.length < 16) {
  console.error("invalid CELLHIVE_ROOT_KEY: expected hex or base64 encoding of at least 16 bytes");
  process.exit(1);
}
const kid = "token/admin";
const key = Buffer.from(hkdfSync("sha256", bytes, Buffer.alloc(0), "cellhive/jwt/v1/" + kid, 32));
const now = Math.floor(Date.now() / 1000);
const encode = value => Buffer.from(JSON.stringify(value)).toString("base64url");
const data = encode({alg:"HS256", typ:"JWT", kid}) + "." + encode({ver:1, type:"admin", sub:"local-admin", aud:"cellhive", iat:now, exp:now+300});
process.stdout.write(data + "." + createHmac("sha256", key).update(data).digest("base64url"));
'
