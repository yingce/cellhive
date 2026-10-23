#!/usr/bin/env bash
# Crash/concurrency E2E for capture RPO=0.
#
# Phase 1: 30 parallel deploys to distinct workers; every 200 is an ack.
# Phase 2: another 30 parallel deploys, then SIGKILL the cell-agent mid-flight.
# Phase 3: wipe the local data dir (force a bucket hydrate) and restart; every
#          phase-1 ack and every phase-2 200 must still be present.
# Phase 4: retry the failed phase-2 requests with the same idempotency keys; each
#          worker must end with exactly one version (no duplicates/loss).
#
# Real components only: cell-agent capture loop, SQLite cellstore, filesystem
# bucket. No user-runtime is needed for the control-plane write path.
set -uo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"
for d in /usr/local/go/bin; do
  [[ -d "$d" && ":$PATH:" != *":$d:"* ]] && PATH="$PATH:$d"
done
export PATH CGO_ENABLED=1
if [[ -z ${HOME:-} ]]; then export HOME=/root; fi
export GOPATH=${GOPATH:-$HOME/go}
if [[ -z ${GOCACHE:-} ]]; then
  probe="$HOME/.cache/.cellhive-crash-e2e-$$"
  if mkdir -p "$HOME/.cache" 2>/dev/null && ( : >"$probe" ) 2>/dev/null; then
    rm -f "$probe"; export GOCACHE=$HOME/.cache/go-build
  else
    export GOCACHE=${TMPDIR:-/tmp}/cellhive-crash-e2e-gocache
  fi
fi
mkdir -p "$GOCACHE"
TAGS="sqlite_fts5 sqlite_dbstat sqlite_math_functions sqlite_column_metadata sqlite_preupdate_hook"

BINDIR=${BINDIR:-$ROOT/bin}
LABEL=${LABEL:-crash}
REST_PORT=${REST_PORT:-17101}
ADMIN_PORT=${ADMIN_PORT:-18182}
REST="127.0.0.1:$REST_PORT"
ADMIN="127.0.0.1:$ADMIN_PORT"
TOKEN=e2e-admin-token
N=${N:-40}
DELAY_MS=${DELAY_MS:-15}

fail() { echo "CRASH-E2E: FAIL: $*" >&2; [[ -f ${TMP:-}/agent.log ]] && tail -30 "$TMP/agent.log" >&2; exit 1; }

mkdir -p "$BINDIR"
if [[ ${SKIP_BUILD:-0} != 1 ]]; then
  echo "== building cell-agent =="
  go build -tags "$TAGS" -o "$BINDIR/cell-agent" ./cmd/cell-agent || fail "build"
fi
[[ -x $BINDIR/cell-agent ]] || fail "missing cell-agent"

TMP=$(mktemp -d "/tmp/cellhive-crash-${LABEL}.XXXXXX")
export CELLHIVE_ROOT_KEY=$(openssl rand -hex 32)
export CELLHIVE_ALLOW_INSECURE_DEFAULTS=1
export CELLHIVE_ADMIN_TOKEN=$TOKEN
export CELLHIVE_NODE_ID="crash-e2e-$LABEL"
export CELLHIVE_SESSION_ID="crash-e2e-$LABEL-$$"
export CELLHIVE_DATA_DIR="$TMP/data"
export CELLHIVE_BUCKET_DIR="$TMP/bucket"
export CELLHIVE_REST_ADDR="$REST"
export CELLHIVE_ADMIN_ADDR="$ADMIN"
export CELLHIVE_ADVERTISE="$REST"
export CELLHIVE_PEER_URL="http://$REST"
export CELLHIVE_CELL_URL="http://$REST"
export CELLHIVE_BASE_DOMAIN=""
export CELLHIVE_CAPTURE_SNAPSHOT_PART=65536
mkdir -p "$CELLHIVE_DATA_DIR" "$CELLHIVE_BUCKET_DIR" "$TMP/results"

cleanup() {
  [[ -n ${AGENT:-} ]] && kill -9 "$AGENT" 2>/dev/null
  sleep 0.2
  pkill -f "$TMP" 2>/dev/null
  if [[ ${KEEP:-0} == 1 ]]; then
    echo "CRASH-E2E: kept $TMP" >&2
  else
    rm -rf "$TMP"
  fi
}
trap cleanup EXIT INT TERM

start_agent() {
  ( cd "$ROOT" && exec "$BINDIR/cell-agent" ) >"$TMP/agent.log" 2>&1 &
  AGENT=$!
  # Keep the agent out of the shell's job table: launch_round's bare `wait`
  # must wait for the deploy clients, not for the long-lived server.
  disown "$AGENT" 2>/dev/null || true
  for _ in $(seq 1 200); do
    curl -sf "http://$REST/healthz" >/dev/null 2>&1 && return 0
    kill -0 "$AGENT" 2>/dev/null || fail "cell-agent exited at startup"
    sleep 0.1
  done
  fail "cell-agent not healthy"
}
adm() { curl -sS -m 30 -X "$1" -H "x-cellhive-admin-token: $TOKEN" -H 'content-type: application/json' --data-binary "${3:-}" "http://$ADMIN$2"; }
upload() {
  curl -sS -m 30 -X POST -H "x-cellhive-admin-token: $TOKEN" -H 'content-type: application/javascript' \
    --data-binary "$1" "http://$ADMIN/v1/control/bundle" |
    python3 -c 'import json,sys; print(json.load(sys.stdin)["sha"])'
}
# deploy_one ROUND INDEX -> writes the HTTP code to $TMP/results/R.INDEX.code
deploy_one() {
  local round=$1 idx=$2 body code
  body="{\"namespace\":\"stress\",\"worker\":\"w${round}_${idx}\",\"bundle_sha\":\"$SHA\",\"idempotency_key\":\"k-${round}-${idx}\"}"
  code=$(curl -sS -m 15 -o "$TMP/results/r${round}.${idx}.body" -w '%{http_code}' \
    -X POST -H "x-cellhive-admin-token: $TOKEN" -H 'content-type: application/json' \
    --data-binary "$body" "http://$ADMIN/v1/control/deploy" 2>/dev/null) || code=000
  echo "$code" >"$TMP/results/r${round}.${idx}.code"
}
launch_round() {
  local round=$1 i
  for i in $(seq 1 "$N"); do deploy_one "$round" "$i" & done
  wait
}
versions() { # WORKER -> number of versions
  local w=$1
  curl -sS -m 10 -H "x-cellhive-admin-token: $TOKEN" \
    "http://$ADMIN/v1/control/releases?namespace=stress&worker=$w" |
    python3 -c 'import json,sys; d=json.load(sys.stdin); print(len(d.get("releases") or []))'
}
active() {
  local w=$1
  curl -sS -m 10 -H "x-cellhive-admin-token: $TOKEN" \
    "http://$ADMIN/v1/control/releases?namespace=stress&worker=$w" |
    python3 -c 'import json,sys; d=json.load(sys.stdin); print(next((r["version"] for r in d.get("releases") or [] if r.get("active")), 0))'
}

start_agent
echo "== upload bundle =="
SHA=$(upload 'export default { async fetch() { return new Response("ok"); } };')
[[ -n $SHA ]] || fail "bundle upload"

echo "== phase 1: $N parallel deploys =="
launch_round 1
acked1=0
for i in $(seq 1 "$N"); do
  [[ $(cat "$TMP/results/r1.$i.code") == 200 ]] && acked1=$((acked1+1))
done
[[ $acked1 == "$N" ]] || fail "phase 1 acked $acked1/$N"
echo "phase 1: $acked1/$N acked"

echo "== phase 2: $N parallel deploys, SIGKILL after ${DELAY_MS}ms =="
for i in $(seq 1 "$N"); do deploy_one 2 "$i" & done
sleep "$(awk -v ms="$DELAY_MS" 'BEGIN{printf "%.3f", ms/1000}')"
kill -9 "$AGENT" 2>/dev/null
wait
acked2=0; failed2=""
for i in $(seq 1 "$N"); do
  if [[ $(cat "$TMP/results/r2.$i.code") == 200 ]]; then
    acked2=$((acked2+1))
  else
    failed2="$failed2 $i"
  fi
done
echo "phase 2: acked=$acked2 failed=$(echo "$failed2" | wc -w) (SIGKILL)"

echo "== phase 3: wipe local cells, restart, hydrate from bucket =="
rm -rf "$CELLHIVE_DATA_DIR/cells"
start_agent
for i in $(seq 1 "$N"); do
  [[ $(cat "$TMP/results/r1.$i.code") == 200 ]] || continue
  v=$(versions "w1_$i")
  [[ $v == 1 ]] || fail "phase-1 ack worker w1_$i lost after bucket restore (versions=$v)"
done
for i in $(seq 1 "$N"); do
  [[ $(cat "$TMP/results/r2.$i.code") == 200 ]] || continue
  v=$(versions "w2_$i")
  [[ $v == 1 ]] || fail "phase-2 ack worker w2_$i lost after bucket restore (versions=$v)"
done
echo "phase 3: every acked deploy survived the local wipe"

echo "== phase 4: retry failed phase-2 requests with the same key =="
retried=0
for i in $failed2; do
  deploy_one 2 "$i" || true
  [[ $(cat "$TMP/results/r2.$i.code") == 200 ]] || fail "retry w$i failed: $(cat "$TMP/results/r2.$i.code")"
  v=$(versions "w2_$i")
  [[ $v == 1 ]] || fail "retry w2_$i duplicated/lost versions=$v"
  [[ $(active "w2_$i") == 1 ]] || fail "retry w2_$i active=$(active "w2_$i")"
  retried=$((retried+1))
done
echo "phase 4: retried $retried failed request(s), exactly one version each"

# No worker may have duplicated state, and every worker must be active at v1.
for round in 1 2; do
  for i in $(seq 1 "$N"); do
    w="w${round}_$i"
    [[ $(versions "$w") == 1 ]] || fail "worker $w versions=$(versions "$w")"
    [[ $(active "$w") == 1 ]] || fail "worker $w active=$(active "$w")"
  done
done

echo "CRASH-E2E: PASS (N=$N, phase1_acked=$acked1, phase2_acked=$acked2, retried=$retried)"
