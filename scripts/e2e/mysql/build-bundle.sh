#!/usr/bin/env bash
# Build the mysql2 worker bundle. Requires npm network access the first time;
# set MYSQL_E2E_BUNDLE to skip this step. Output: scripts/e2e/mysql/worker.bundle.js
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
HERE="$ROOT/scripts/e2e/mysql"
OUT="${MYSQL_E2E_BUNDLE:-$HERE/worker.bundle.js}"
TMP="$(mktemp -d "${TMPDIR:-/tmp}/cellhive-mysql-bundle.XXXXXX")"
trap 'rm -rf "$TMP"' EXIT
npm_config_cache="$TMP/npm-cache" npm install --prefix "$TMP" --no-audit --no-fund mysql2@3.14.1 esbuild@0.25.10 >/dev/null
cp "$HERE/worker.js" "$HERE/shim.js" "$TMP/"
BANNER="$(cat "$TMP/shim.js")"
"$TMP/node_modules/.bin/esbuild" "$TMP/worker.js" \
  --bundle --format=esm --platform=node --target=es2022 \
  --conditions=workerd --external:node:* \
  --banner:js="$BANNER" --outfile="$OUT" --log-level=warning
echo "built $OUT"
