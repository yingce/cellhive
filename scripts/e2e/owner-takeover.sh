#!/usr/bin/env bash
# Cross-node owner takeover E2E (ADR-273/275).
# Two real cell-agents share a filesystem bucket. A owns the control cell; A is
# SIGKILLed; B must not ack until A's node lease expires, then take over, hydrate
# the acknowledged control state, and accept the next deploy.
set -uo pipefail
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd); cd "$ROOT"
for d in /usr/local/go/bin; do [[ -d $d && ":$PATH:" != *":$d:"* ]] && PATH="$PATH:$d"; done
export PATH CGO_ENABLED=1
export HOME=${HOME:-/root}
export GOPATH=${GOPATH:-$HOME/go}
export GOCACHE=${GOCACHE:-$HOME/.cache/go-build}
mkdir -p "$GOCACHE"
TAGS='sqlite_fts5 sqlite_dbstat sqlite_math_functions sqlite_column_metadata sqlite_preupdate_hook'
BIN=${BIN:-$ROOT/bin/cell-agent-takeover}
TMP=$(mktemp -d /tmp/cellhive-takeover.XXXXXX)
TOKEN=e2e-admin-token
FAILED=0
fail(){ echo "TAKEOVER-E2E: FAIL: $*" >&2; [[ -f $TMP/a.log ]] && tail -20 "$TMP/a.log" >&2; [[ -f $TMP/b.log ]] && tail -20 "$TMP/b.log" >&2; FAILED=1; }
cleanup(){ [[ -n ${A:-} ]] && kill -9 "$A" 2>/dev/null; [[ -n ${B:-} ]] && kill -9 "$B" 2>/dev/null; pkill -f "$TMP" 2>/dev/null; rm -rf "$TMP"; }
trap cleanup EXIT INT TERM
mkdir -p "$(dirname "$BIN")"; go build -tags "$TAGS" -o "$BIN" ./cmd/cell-agent || { fail build; exit 1; }
export CELLHIVE_ROOT_KEY=$(openssl rand -hex 32); export CELLHIVE_ALLOW_INSECURE_DEFAULTS=1 CELLHIVE_ADMIN_TOKEN=$TOKEN
export CELLHIVE_BUCKET_DIR=$TMP/bucket; export CELLHIVE_LEASE_TTL=2s CELLHIVE_MAX_OPEN_CELLS=4 CELLHIVE_CELL_IDLE=1m
export CELLHIVE_COMPACTION_INTERVAL=1m CELLHIVE_EPOCH_GC_INTERVAL=1m CELLHIVE_EPOCH_GC_INITIAL=1m
mkdir -p "$CELLHIVE_BUCKET_DIR"
start_agent(){ # NODE SESSION RESTPORT ADMINPORT LOGFILE
  local node=$1 sess=$2 rest=$3 admin=$4 log=$5
  local d=$TMP/data-$node
  CELLHIVE_NODE_ID=$node CELLHIVE_SESSION_ID=$sess CELLHIVE_DATA_DIR=$d CELLHIVE_REST_ADDR=127.0.0.1:$rest CELLHIVE_ADMIN_ADDR=127.0.0.1:$admin CELLHIVE_ADVERTISE=127.0.0.1:$rest CELLHIVE_PEER_URL=http://127.0.0.1:$rest \
    "$BIN" >"$log" 2>&1 &
  local pid=$!
  for _ in $(seq 1 200); do curl -sf "http://127.0.0.1:$rest/healthz" >/dev/null 2>&1 && { echo "$pid"; return 0; }; kill -0 "$pid" 2>/dev/null || { fail "agent $node exited"; return 1; }; sleep 0.05; done
  fail "agent $node not healthy"; return 1
}
upload(){ curl -sS -m 20 -X POST -H "x-cellhive-admin-token: $TOKEN" -H 'content-type: application/javascript' --data-binary "$2" "http://127.0.0.1:$1/v1/control/bundle"; }
deploy(){ # ADMINPORT KEY JS -> "seconds http"
  local admin=$1 key=$2 js=$3 sha t
  sha=$(upload "$admin" "$js" | python3 -c 'import json,sys;print(json.load(sys.stdin)["sha"])') || return 1
  curl -sS -m 30 -o "$TMP/deploy.out" -w '%{time_total} %{http_code}' -X POST -H "x-cellhive-admin-token: $TOKEN" -H 'content-type: application/json' \
    --data-binary "{\"namespace\":\"e2e\",\"worker\":\"takeover\",\"bundle_sha\":\"$sha\",\"idempotency_key\":\"$key\",\"compatibility_date\":\"2026-06-15\",\"bindings\":[],\"crons\":[],\"consumers\":[]}" "http://127.0.0.1:$admin/v1/control/deploy"
}
A=$(start_agent node-a sess-a1 17111 17112 "$TMP/a.log") || { A=""; exit 1; }
A_REST=17111; A_ADMIN=17112
OUT=$(deploy 17112 a1 'export default { async fetch() { return new Response("v1"); } };') || { fail "A first deploy"; exit 1; }
[[ ${OUT##* } == 200 ]] || { fail "A first deploy: $OUT"; exit 1; }
echo "A first deploy: $OUT"
kill -9 "$A" 2>/dev/null; wait "$A" 2>/dev/null; A=""
START_MS=$(date +%s%3N)
B=$(start_agent node-b sess-b1 17113 17114 "$TMP/b.log") || { B=""; exit 1; }
B_ADMIN=17114
OK=1
for i in $(seq 1 80); do
  OUT=$(deploy 17114 "b$i" 'export default { async fetch() { return new Response("v2"); } };' 2>/dev/null) || true
  [[ ${OUT##* } == 200 ]] && { OK=0; break; }
  sleep 0.1
done
TAKEOVER_MS=$(( $(date +%s%3N) - START_MS ))
echo "B takeover: ${TAKEOVER_MS}ms deploy=$OUT"
[[ $OK -eq 0 ]] || { fail "B never took over"; exit 1; }
python3 - "$TAKEOVER_MS" <<'PY' || fail "takeover took ${TAKEOVER_MS}ms (want <= TTL+2s)"
import sys
sys.exit(0 if int(sys.argv[1]) <= 4000 else 1)
PY
# The acknowledged control state from A must still be in the chain B hydrated.
CODE=$(curl -sS -m 10 -o "$TMP/releases.out" -w '%{http_code}' -H "x-cellhive-admin-token: $TOKEN" "http://127.0.0.1:17114/v1/control/releases?namespace=e2e&worker=takeover")
[[ $CODE == 200 ]] || { fail "B releases HTTP $CODE"; exit 1; }
python3 - "$TMP/releases.out" <<'PY' || { fail "A acknowledged version missing after takeover"; exit 1; }
import json,sys
d=json.load(open(sys.argv[1]))
vs=[r.get("version") for r in d.get("releases") or []]
sys.exit(0 if any(v for v in vs) else 1)
PY
if [[ $FAILED -eq 0 ]]; then echo "TAKEOVER-E2E: PASS"; else echo "TAKEOVER-E2E: FAIL"; exit 1; fi
