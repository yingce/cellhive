#!/usr/bin/env bash
# Owner-liveness / restart E2E (ADR-273).
#
# Real cell-agent, real SQLite cellstore and real control-cell writes against a
# filesystem bucket. Exercises:
#   1. continuous captured writes across several lease periods: epoch changes = 0
#   2. no not_owner/503 while the node lease is published
#   3. SIGKILL + restart in the same data directory: first deploy < 2s, no TTL wait
# No user-runtime is needed: deploys only touch the control cell.
set -uo pipefail
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
cd "$ROOT"
for d in /usr/local/go/bin; do [[ -d $d && ":$PATH:" != *":$d:"* ]] && PATH="$PATH:$d"; done
export PATH
export CGO_ENABLED=1
export HOME=${HOME:-/root}
export GOPATH=${GOPATH:-$HOME/go}
export GOCACHE=${GOCACHE:-$HOME/.cache/go-build}
mkdir -p "$GOCACHE"
TAGS="sqlite_fts5 sqlite_dbstat sqlite_math_functions sqlite_column_metadata sqlite_preupdate_hook"
BIN=${BIN:-$ROOT/bin/cell-agent-e2e}
REST_PORT=${REST_PORT:-17011}
ADMIN_PORT=${ADMIN_PORT:-17012}
ADMIN="127.0.0.1:$ADMIN_PORT"
REST="127.0.0.1:$REST_PORT"
TOKEN=e2e-admin-token
TMP=$(mktemp -d /tmp/cellhive-owner-e2e.XXXXXX)
FAILED=0
fail(){ echo "OWNER-E2E: FAIL: $*" >&2; [[ -f $TMP/agent.log ]] && tail -30 "$TMP/agent.log" >&2; FAILED=1; }
cleanup(){ [[ -n ${AGENT:-} ]] && kill -9 "$AGENT" 2>/dev/null; pkill -f "$TMP" 2>/dev/null; rm -rf "$TMP"; }
trap cleanup EXIT INT TERM

mkdir -p "$(dirname "$BIN")"
go build -tags "$TAGS" -o "$BIN" ./cmd/cell-agent || { fail "build"; exit 1; }
export CELLHIVE_ROOT_KEY=$(openssl rand -hex 32)
export CELLHIVE_ALLOW_INSECURE_DEFAULTS=1
export CELLHIVE_ADMIN_TOKEN=$TOKEN
export CELLHIVE_NODE_ID=owner-e2e
export CELLHIVE_DATA_DIR=$TMP/data
export CELLHIVE_BUCKET_DIR=$TMP/bucket
export CELLHIVE_REST_ADDR=$REST
export CELLHIVE_ADMIN_ADDR=$ADMIN
export CELLHIVE_ADVERTISE=$REST
export CELLHIVE_PEER_URL=http://$REST
export CELLHIVE_LEASE_TTL=2s
export CELLHIVE_MAX_OPEN_CELLS=4
export CELLHIVE_CELL_IDLE=2s
export CELLHIVE_COMPACTION_INTERVAL=1m
export CELLHIVE_EPOCH_GC_INTERVAL=1m
export CELLHIVE_EPOCH_GC_INITIAL=1m
mkdir -p "$CELLHIVE_DATA_DIR" "$CELLHIVE_BUCKET_DIR"

start_agent(){ # SESSION
  export CELLHIVE_SESSION_ID=$1
  "$BIN" >"$TMP/agent.log" 2>&1 &
  AGENT=$!
  for _ in $(seq 1 200); do
    curl -sf "http://$REST/healthz" >/dev/null 2>&1 && return 0
    kill -0 "$AGENT" 2>/dev/null || { fail "agent exited"; return 1; }
    sleep 0.05
  done
  fail "agent not healthy"; return 1
}
adm(){ local m=$1 p=$2 b=${3:-}; if [[ -n $b ]]; then curl -sS -m 30 -X "$m" -H "x-cellhive-admin-token: $TOKEN" -H 'content-type: application/json' --data-binary "$b" "http://$ADMIN$p"; else curl -sS -m 30 -X "$m" -H "x-cellhive-admin-token: $TOKEN" "http://$ADMIN$p"; fi; }
upload(){ curl -sS -m 20 -X POST -H "x-cellhive-admin-token: $TOKEN" -H 'content-type: application/javascript' --data-binary "$1" "http://$ADMIN/v1/control/bundle"; }
deploy(){ # KEY BODY -> status:seconds
  local key=$1 js=$2 sha t
  sha=$(upload "$js" | python3 -c 'import json,sys;print(json.load(sys.stdin)["sha"])') || return 1
  t=$(curl -sS -m 30 -o "$TMP/deploy.out" -w '%{time_total} %{http_code}' -X POST -H "x-cellhive-admin-token: $TOKEN" -H 'content-type: application/json' \
      --data-binary "{\"namespace\":\"e2e\",\"worker\":\"owner\",\"bundle_sha\":\"$sha\",\"idempotency_key\":\"$key\",\"compatibility_date\":\"2026-06-15\",\"bindings\":[],\"crons\":[],\"consumers\":[]}" "http://$ADMIN/v1/control/deploy")
  echo "$t"
}
metric(){ curl -sS -m 5 "http://$REST/metrics" | awk -v k="cellhive_$1" 'index($0,k)==1 {print $NF}'; }
epochs(){ metric owner_epoch_changes_total; }

start_agent owner-e2e-s1 || exit 1
BASE=$(epochs); [[ -n $BASE ]] || { fail "missing epoch metric"; exit 1; }
OUT=$(deploy k0 'export default { async fetch() { return new Response("v0"); } };') || { fail "first deploy"; exit 1; }
echo "first deploy: $OUT"
BASE=$(epochs)
FAILS=0
START=$(date +%s)
DURATION=${OWNER_E2E_SECONDS:-8}
CALLS=0
while :; do
  NOW=$(date +%s)
  [[ $((NOW - START)) -ge $DURATION ]] && break
  CALLS=$((CALLS+1))
  OUT=$(deploy "k$CALLS" "export default { async fetch() { return new Response(\"v$CALLS\"); } };") || { FAILS=$((FAILS+1)); continue; }
  [[ $OUT == *" 200" ]] || FAILS=$((FAILS+1))
  sleep 0.15
done
ELAPSED=$(( $(date +%s) - START ))
AFTER=$(epochs)
echo "continuous: ${ELAPSED}s calls=$CALLS failures=$FAILS epochs_before=$BASE epochs_after=$AFTER"
[[ $FAILS -eq 0 ]] || fail "continuous writes failed=$FAILS"
[[ $AFTER -eq $BASE ]] || fail "epoch changed during continuous writes: $BASE -> $AFTER"

# Same directory SIGKILL, new session: the node lease proves the old session is
# gone, so the first captured write must not wait for LEASE_TTL.
kill -9 "$AGENT" 2>/dev/null; wait "$AGENT" 2>/dev/null; AGENT=""
start_agent owner-e2e-s2 || exit 1
FIRST=$(deploy restart-k 'export default { async fetch() { return new Response("restart"); } };')
echo "restart first deploy: $FIRST"
SECONDS=$(echo "$FIRST" | awk '{print $1}')
[[ $(echo "$FIRST" | awk '{print $2}') == 200 ]] || fail "restart deploy failed: $FIRST"
python3 - "$SECONDS" <<'PY' || fail "restart first write took ${SECONDS}s (want <2s)"
import sys
sys.exit(0 if float(sys.argv[1]) < 2.0 else 1)
PY


listops(){ curl -sS -m 5 "http://$REST/metrics" | awk '/^cellhive_bucket_ops_total\{op="list"\}/{print $NF}'; }

# A5: wipe the local cell directory and force the control cell to hydrate from
# the bucket. Hydration must be one scope listing at most (RestoreChain) plus
# the LatestEpoch lookup, not one List per epoch.
kill -9 "$AGENT" 2>/dev/null; wait "$AGENT" 2>/dev/null; AGENT=""
rm -rf "$CELLHIVE_DATA_DIR/cells"
start_agent owner-e2e-s3 || exit 1
BEFORE_LIST=$(listops)
COLD=$(deploy cold-k 'export default { async fetch() { return new Response("cold"); } };')
AFTER_LIST=$(listops)
echo "cold hydrate first deploy: $COLD list_delta=$((AFTER_LIST - BEFORE_LIST))"
COLD_SECONDS=$(echo "$COLD" | awk '{print $1}')
[[ $(echo "$COLD" | awk '{print $2}') == 200 ]] || fail "cold hydrate deploy failed: $COLD"
[[ $((AFTER_LIST - BEFORE_LIST)) -le 3 ]] || fail "cold hydrate used $((AFTER_LIST - BEFORE_LIST)) bucket Lists (want <=3)"
python3 - "$COLD_SECONDS" <<'PY' || fail "cold hydrate first write took ${COLD_SECONDS}s"
import sys
sys.exit(0 if float(sys.argv[1]) < 2.0 else 1)
PY

if [[ $FAILED -eq 0 ]]; then echo "OWNER-E2E: PASS"; else echo "OWNER-E2E: FAIL"; exit 1; fi
