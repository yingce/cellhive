#!/usr/bin/env bash
# Local do-runtime RPS smoke: one Durable Object storage writer and one
# WebSocket echo object. Requires a running CellHive stack (make compose-up)
# and wrk/node.
set -euo pipefail
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
ADMIN_URL=${CELLHIVE_ADMIN_URL:-http://127.0.0.1:8082}
TOKEN=${CELLHIVE_ADMIN_TOKEN:-$("$ROOT/bin/cellhive" creds admin)}
NS=${DO_RPS_NS:-do-rps}
HOST=${DO_RPS_HOST:-do-bench.local}
WORKER=do-rps-bench
THREADS=${DO_RPS_THREADS:-4}
CONNECTIONS=${DO_RPS_CONNECTIONS:-64}
DURATION=${DO_RPS_DURATION:-10}
WS_SOCKETS=${DO_RPS_WS_SOCKETS:-32}
WS_DURATION=${DO_RPS_WS_DURATION:-8}
SHARDS=${DO_RPS_SHARDS:-1}
WS_SHARDS=${DO_RPS_WS_SHARDS:-1}
FIXTURES="$ROOT/scripts/e2e/fixtures/do-rps"
TMP=$(mktemp -d "${TMPDIR:-/tmp}/cellhive-do-rps.XXXXXX")
trap 'rm -rf "$TMP"' EXIT
export HOME="${DO_RPS_HOME:-$TMP/home}"
export XDG_CONFIG_HOME="$TMP/config"
export npm_config_cache="$TMP/npm-cache"
mkdir -p "$HOME" "$XDG_CONFIG_HOME" "$npm_config_cache"

command -v wrk >/dev/null || { echo "wrk is required" >&2; exit 2; }
command -v node >/dev/null || { echo "node is required" >&2; exit 2; }
[ -x "$ROOT/bin/cellhive" ] || { echo "bin/cellhive is required (make build)" >&2; exit 2; }

export CELLHIVE_ADMIN_URL="$ADMIN_URL" CELLHIVE_ADMIN_TOKEN="$TOKEN"
"$ROOT/bin/cellhive" app create "$NS" >/dev/null 2>&1 || true
cp "$FIXTURES/worker.js" "$TMP/worker.js"
cp "$FIXTURES/wrangler.jsonc" "$TMP/wrangler.jsonc"

export CLOUDFLARE_API_BASE_URL="$ADMIN_URL/client/v4"
export CLOUDFLARE_API_TOKEN="$TOKEN"
export CLOUDFLARE_ACCOUNT_ID="$NS"
export WRANGLER_SEND_METRICS=false
if ! ( cd "$TMP" && npx --yes wrangler@4.133.0 deploy --config wrangler.jsonc >"$TMP/deploy.log" 2>&1 ); then
  echo "wrangler deploy failed:" >&2
  tail -40 "$TMP/deploy.log" >&2
  exit 1
fi
"$ROOT/bin/cellhive" domain add "$NS" "$HOST" >/dev/null 2>&1 || true
"$ROOT/bin/cellhive" route add "$NS" "$HOST" "$WORKER" >/dev/null 2>&1 || true

# Route/version projection is cached; wait for both /get and WebSocket path.
for _ in $(seq 1 40); do
  if curl -fsS -H "Host: $HOST" "http://127.0.0.1:8081/get" >/dev/null 2>&1; then break; fi
  sleep 1
done
curl -fsS -H "Host: $HOST" "http://127.0.0.1:8081/inc" >/dev/null

for shards in ${SHARDS//,/ }; do
  echo "== storage: shards=$shards wrk -t$THREADS -c$CONNECTIONS -d${DURATION}s /inc =="
  if [ "$shards" -gt 1 ]; then
    DO_RPS_SHARDS="$shards" wrk -t"$THREADS" -c"$CONNECTIONS" -d"${DURATION}s" --latency \
      -s "$FIXTURES/wrk-shards.lua" -H "Host: $HOST" "http://127.0.0.1:8081/inc" | tee "$TMP/storage-$shards.log"
  else
    wrk -t"$THREADS" -c"$CONNECTIONS" -d"${DURATION}s" --latency \
      -H "Host: $HOST" "http://127.0.0.1:8081/inc" | tee "$TMP/storage-$shards.log"
  fi
  # wrk exits zero even when requests fail. Do not let a successful benchmark
  # process hide failed durability gates or socket/time-out errors.
  python3 - "$TMP/storage-$shards.log" <<'PY'
import pathlib, re, sys
text = pathlib.Path(sys.argv[1]).read_text()
bad = re.search(r"Non-2xx or 3xx responses:\s*(\d+)", text)
sockets = re.search(r"Socket errors:\s*(.*)", text)
if (bad and int(bad[1])) or (sockets and any(int(n) for n in re.findall(r"\d+", sockets[1]))):
    sys.exit("DO-RPS: FAIL: HTTP or socket errors; see benchmark output")
PY
  echo
done
for shards in ${WS_SHARDS//,/ }; do
  echo "== websocket: shards=$shards sockets=$WS_SOCKETS for ${WS_DURATION}s =="
  NODE_PATH="$ROOT/cli/node_modules" node "$FIXTURES/ws-bench.cjs" \
    "$WS_SOCKETS" "$WS_DURATION" "ws://127.0.0.1:8081/" "$HOST" "$shards" | tee "$TMP/ws-$shards.log"
  echo
done
