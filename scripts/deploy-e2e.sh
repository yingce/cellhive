#!/usr/bin/env bash
# Real single-node deploy/rollback E2E.
#
# Boots a real cell-agent (capture enabled, filesystem bucket) and a real
# user-runtime with the pinned workerd, publishes two worker versions through
# /v1/control/deploy, invokes the worker through the loader, rolls back, and
# asserts the durability-proof count on the real /metrics endpoint:
#
#   deploy  -> exactly one capturedWrite proof (ADR-271)
#   rollback-> exactly one capturedWrite proof
#   App auto-create is audited once, not on every deploy
#
# No mocks: SQLite cellstore, capture loop, workerd, and HTTP admin/loader are
# the production code paths. Local proofs are milliseconds, so this harness
# guards the proof COUNT (the production latency multiplier), not wall time.
#
# Usage:
#   bash scripts/deploy-e2e.sh            # build missing binaries, run, assert
#   BINDIR=/path/to/bin bash scripts/deploy-e2e.sh
#
# Environment (all optional):
#   BINDIR       binaries dir (default: <repo>/bin; built on demand)
#   LABEL        stack label for the temp dir/logs (default: e2e)
#   WORKERD      pinned workerd binary (default: deploy/workerd-fork/workerd)
#   REST_PORT/ADMIN_PORT/PUBLIC_PORT/INTERNAL_PORT  loopback ports
set -uo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

for d in /usr/local/go/bin; do
  [[ -d "$d" && ":$PATH:" != *":$d:"* ]] && PATH="$PATH:$d"
done
export PATH
export CGO_ENABLED=1
# Bare automation shells may have no HOME, which leaves Go without a module
# cache or build cache. Normalize to the conventional root locations.
if [[ -z ${HOME:-} ]]; then
  export HOME=/root
fi
export GOPATH=${GOPATH:-$HOME/go}
if [[ -z ${GOCACHE:-} ]]; then
  # A -w check lies under sandboxes, so probe with a real write.
  probe="$HOME/.cache/.cellhive-deploy-e2e-$$"
  if mkdir -p "$HOME/.cache" 2>/dev/null && ( : >"$probe" ) 2>/dev/null; then
    rm -f "$probe"
    export GOCACHE=$HOME/.cache/go-build
  else
    export GOCACHE=${TMPDIR:-/tmp}/cellhive-deploy-e2e-gocache
  fi
fi
mkdir -p "$GOCACHE"
TAGS="sqlite_fts5 sqlite_dbstat sqlite_math_functions sqlite_column_metadata sqlite_preupdate_hook"

BINDIR=${BINDIR:-$ROOT/bin}
LABEL=${LABEL:-e2e}
WORKERD=${WORKERD:-$ROOT/deploy/workerd-fork/workerd}
REST_PORT=${REST_PORT:-17001}
ADMIN_PORT=${ADMIN_PORT:-18082}
PUBLIC_PORT=${PUBLIC_PORT:-18081}
INTERNAL_PORT=${INTERNAL_PORT:-18088}
REST="127.0.0.1:$REST_PORT"
ADMIN="127.0.0.1:$ADMIN_PORT"
PUB="127.0.0.1:$PUBLIC_PORT"
TOKEN=e2e-admin-token

fail() { echo "DEPLOY-E2E: FAIL: $*" >&2; [[ -f ${TMP:-}/agent.log ]] && tail -20 "$TMP/agent.log" >&2; [[ -f ${TMP:-}/runtime.log ]] && tail -20 "$TMP/runtime.log" >&2; exit 1; }

# Build the binaries under test unless the caller explicitly skips it: a stale
# bin/ would silently test the wrong revision.
mkdir -p "$BINDIR"
if [[ ${SKIP_BUILD:-0} != 1 ]]; then
  echo "== building cell-agent + user-runtime into $BINDIR =="
  go build -tags "$TAGS" -o "$BINDIR/cell-agent" ./cmd/cell-agent || fail "go build cell-agent"
  go build -tags "$TAGS" -o "$BINDIR/user-runtime" ./cmd/user-runtime || fail "go build user-runtime"
fi
[[ -x $BINDIR/cell-agent && -x $BINDIR/user-runtime ]] || fail "missing binaries in $BINDIR"
[[ -x $WORKERD ]] || fail "workerd not found at $WORKERD (run deploy/build-workerd-fork.sh)"

TMP=$(mktemp -d "/tmp/cellhive-${LABEL}.XXXXXX")
export CELLHIVE_ROOT_KEY=$(openssl rand -hex 32)
export CELLHIVE_ALLOW_INSECURE_DEFAULTS=1
export CELLHIVE_ADMIN_TOKEN=$TOKEN
export CELLHIVE_NODE_ID="deploy-e2e-$LABEL"
export CELLHIVE_SESSION_ID="deploy-e2e-$LABEL-$$"
export CELLHIVE_DATA_DIR="$TMP/data"
export CELLHIVE_BUCKET_DIR="$TMP/bucket"
export CELLHIVE_REST_ADDR="$REST"
export CELLHIVE_ADMIN_ADDR="$ADMIN"
export CELLHIVE_ADVERTISE="$REST"
export CELLHIVE_PEER_URL="http://$REST"
export CELLHIVE_CELL_URL="http://$REST"
export CELLHIVE_BASE_DOMAIN=cell.test
export CELLHIVE_WORKERD="$WORKERD"
export CELLHIVE_USER_RUNTIME_PORT=$PUBLIC_PORT
export CELLHIVE_USER_RUNTIME_INTERNAL_PORT=$INTERNAL_PORT
export CELLHIVE_RUNTIME_DIR="$TMP/runtime"
# Force a multi-part control snapshot so the harness exercises the batched
# snapshot path and the batch-aware durable watermark.
export CELLHIVE_CAPTURE_SNAPSHOT_PART=65536
mkdir -p "$CELLHIVE_DATA_DIR" "$CELLHIVE_BUCKET_DIR" "$CELLHIVE_RUNTIME_DIR"

cleanup() {
  [[ -n ${RT:-} ]] && kill "$RT" 2>/dev/null
  [[ -n ${AGENT:-} ]] && kill "$AGENT" 2>/dev/null
  sleep 0.3
  pkill -f "$TMP" 2>/dev/null
  rm -rf "$TMP"
}
trap cleanup EXIT INT TERM

port_free() { ! ss -ltn 2>/dev/null | grep -q ":$1 "; }
for p in "$REST_PORT" "$ADMIN_PORT" "$PUBLIC_PORT" "$INTERNAL_PORT"; do
  port_free "$p" || fail "port $p is already in use"
done

echo "== deploy-e2e[$LABEL] starting cell-agent + user-runtime =="
( cd "$ROOT" && exec "$BINDIR/cell-agent" ) >"$TMP/agent.log" 2>&1 &
AGENT=$!
for _ in $(seq 1 200); do
  curl -sf "http://$REST/healthz" >/dev/null 2>&1 && break
  kill -0 "$AGENT" 2>/dev/null || fail "cell-agent exited"
  sleep 0.1
done
curl -sf "http://$REST/healthz" >/dev/null 2>&1 || fail "cell-agent not healthy"
( cd "$ROOT" && exec "$BINDIR/user-runtime" ) >"$TMP/runtime.log" 2>&1 &
RT=$!
for _ in $(seq 1 200); do
  curl -s -o /dev/null "http://$PUB/" 2>/dev/null && break
  kill -0 "$RT" 2>/dev/null || fail "user-runtime exited"
  sleep 0.1
done
curl -s -o /dev/null "http://$PUB/" 2>/dev/null || fail "user-runtime not listening"

adm() {
  local m=$1 p=$2 b=${3:-}
  if [[ -n $b ]]; then
    curl -sS -m 60 -X "$m" -H "x-cellhive-admin-token: $TOKEN" -H 'content-type: application/json' --data-binary "$b" "http://$ADMIN$p"
  else
    curl -sS -m 60 -X "$m" -H "x-cellhive-admin-token: $TOKEN" "http://$ADMIN$p"
  fi
}
# adm_time METHOD PATH BODY -> writes time_total to stdout and body to $TMP/last.body
adm_time() {
  curl -sS -m 60 -o "$TMP/last.body" -w '%{time_total}' -X "$1" \
    -H "x-cellhive-admin-token: $TOKEN" -H 'content-type: application/json' \
    --data-binary "$3" "http://$ADMIN$2"
}
proofs() { # durability proofs attributed to the single control cell
  local v
  v=$(curl -sS -m 10 "http://$REST/metrics" | awk -F' ' '/^cellhive_durability_proof_seconds_count\{ns="__platform__"\}/{print $2}')
  echo "${v:-0}"
}
ensure_mode() { # MODE -> capture.Ensure counter for the control cell
  local m=$1 v
  v=$(curl -sS -m 10 "http://$REST/metrics" |
    awk -v pat="cellhive_capture_ensure_total{ns=\"__platform__\",mode=\"$m\"}" 'index($0, pat)==1 {print $NF}')
  echo "${v:-0}"
}
active_ver() {
  curl -sS -m 10 -H "x-cellhive-admin-token: $TOKEN" \
    "http://$ADMIN/v1/control/releases?namespace=e2e&worker=app" |
    python3 -c 'import json,sys; d=json.load(sys.stdin); print(next((r["version"] for r in d.get("releases") or [] if r.get("active")), 0))'
}
upload() {
  curl -sS -m 30 -X POST -H "x-cellhive-admin-token: $TOKEN" -H 'content-type: application/javascript' \
    --data-binary "$1" "http://$ADMIN/v1/control/bundle" |
    python3 -c 'import json,sys; print(json.load(sys.stdin)["sha"])'
}
deploy() { # NS WORKER SHA KEY -> prints "<seconds> <proof-delta>"
  local ns=$1 wk=$2 sha=$3 key=$4 before after t
  before=$(proofs)
  t=$(adm_time POST /v1/control/deploy "{\"namespace\":\"$ns\",\"worker\":\"$wk\",\"bundle_sha\":\"$sha\",\"idempotency_key\":\"$key\",\"compatibility_date\":\"2026-06-15\",\"bindings\":[],\"crons\":[],\"consumers\":[]}")
  grep -q '"number"' "$TMP/last.body" || { echo "deploy body: $(cat "$TMP/last.body")" >&2; return 1; }
  sleep 0.2
  after=$(proofs)
  echo "$t $((after-before))"
}
invoke() { curl -sS -m 20 -H 'Host: e2e-app.cell.test' "http://$PUB/" 2>/dev/null; }
wait_body() { # WANT -> prints body, fails on timeout
  local want=$1 i out
  for i in $(seq 1 100); do
    out=$(invoke || true)
    [[ $out == *"$want"* ]] && { echo "$out"; return 0; }
    sleep 0.2
  done
  echo "timeout waiting for '$want' (last='$out')" >&2
  return 1
}

echo "== deploy-e2e[$LABEL] publishing =="
sha1=$(upload 'export default { async fetch() { return new Response("v1"); } };')
sha2=$(upload 'export default { async fetch() { return new Response("v2"); } };')
[[ -n $sha1 && -n $sha2 ]] || fail "bundle upload"

read -r t1 d1 <<<"$(deploy e2e app "$sha1" e2e-k1)" || fail "deploy v1"
[[ $(active_ver) == 1 ]] || fail "active after v1 = $(active_ver)"
read -r t2 d2 <<<"$(deploy e2e app "$sha2" e2e-k2)" || fail "deploy v2"
[[ $(active_ver) == 2 ]] || fail "active after v2 = $(active_ver)"

body2=$(wait_body v2) || fail "invoke v2: $body2"

before=$(proofs)
t3=$(adm_time POST /v1/control/rollback '{"namespace":"e2e","worker":"app"}')
grep -q '"active":1' "$TMP/last.body" || fail "rollback body: $(cat "$TMP/last.body")"
sleep 0.2
after=$(proofs); d3=$((after-before))
body1=$(wait_body v1) || fail "invoke after rollback: $body1"

audit_json=$(adm GET '/v1/control/audit?namespace=e2e')
audits=$(printf '%s' "$audit_json" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(sum(1 for a in d.get("audit") or [] if a.get("action")=="app.create"))')

# Restart the cell-agent (same data dir and bucket) and publish a third version.
# ADR-272: the new capture must continue the durable chain (mode=resume) and
# must not take another whole-database snapshot.
kill "$AGENT" 2>/dev/null
wait "$AGENT" 2>/dev/null
( cd "$ROOT" && exec "$BINDIR/cell-agent" ) >"$TMP/agent-restart.log" 2>&1 &
AGENT=$!
for _ in $(seq 1 200); do
  curl -sf "http://$REST/healthz" >/dev/null 2>&1 && break
  kill -0 "$AGENT" 2>/dev/null || fail "cell-agent exited after restart"
  sleep 0.1
done
curl -sf "http://$REST/healthz" >/dev/null 2>&1 || fail "cell-agent not healthy after restart"
# Counters are per process: take the post-restart baseline here.
snap_before=$(ensure_mode snapshot)
resume_before=$(ensure_mode resume)
sha3=$(upload 'export default { async fetch() { return new Response("v3"); } };')
read -r t4 d4 <<<"$(deploy e2e app "$sha3" e2e-k3)" || fail "deploy v3 after restart"
[[ $(active_ver) == 3 ]] || fail "active after v3 = $(active_ver)"
body3=$(wait_body v3) || fail "invoke v3: $body3"
sleep 0.2
snap_after=$(ensure_mode snapshot)
resume_after=$(ensure_mode resume)

echo "RESULT[$LABEL]"
echo "  deploy v1 (fresh ns):  ${t1}s  proof_delta=$d1"
echo "  deploy v2:             ${t2}s  proof_delta=$d2"
echo "  rollback v2->v1:       ${t3}s  proof_delta=$d3"
echo "  invoke v2:             ${body2}"
echo "  invoke after rollback: ${body1}"
echo "  app.create audits:     $audits"
echo "  deploy v3 (restart):   ${t4}s  proof_delta=$d4  ensure_snapshot=${snap_before}->${snap_after} ensure_resume=${resume_before}->${resume_after}"
echo "  invoke v3:             ${body3}"
echo "  active version:        $(active_ver)"

fail=0
[[ $d1 == 1 ]] || { echo "ASSERT: deploy v1 proofs=$d1, want 1" >&2; fail=1; }
[[ $d2 == 1 ]] || { echo "ASSERT: deploy v2 proofs=$d2, want 1" >&2; fail=1; }
[[ $d3 == 1 ]] || { echo "ASSERT: rollback proofs=$d3, want 1" >&2; fail=1; }
[[ $audits == 1 ]] || { echo "ASSERT: app.create audits=$audits, want 1" >&2; fail=1; }
[[ $body2 == *v2* ]] || { echo "ASSERT: invoke after v2 = '$body2'" >&2; fail=1; }
[[ $body1 == *v1* ]] || { echo "ASSERT: invoke after rollback = '$body1'" >&2; fail=1; }
[[ $d4 == 1 ]] || { echo "ASSERT: deploy v3 proofs=$d4, want 1" >&2; fail=1; }
[[ $snap_after == "$snap_before" ]] || { echo "ASSERT: restart took a snapshot ($snap_before -> $snap_after)" >&2; fail=1; }
[[ $resume_after -gt $resume_before ]] || { echo "ASSERT: restart did not resume (resume $resume_before -> $resume_after)" >&2; fail=1; }
[[ $body3 == *v3* ]] || { echo "ASSERT: invoke after restart = '$body3'" >&2; fail=1; }
[[ $fail == 0 ]] || exit 1
echo "DEPLOY-E2E: PASS"
