#!/usr/bin/env bash
# Single-cell vs dual-cell backend-A capture benchmark.
#
# Runs cell-agent with capture ON and CELLHIVE_DURABILITY=bucket (RPO=0), so each
# acked write is replicated as an LTX segment to the bucket before it returns.
# Compares one cell against two independent cells at the same total client
# concurrency (kvbench round-robins across namespaces), then verifies the LTX
# chain is actually in the bucket and restorable.
#
# Usage:
#   scripts/bench-capture-cells.sh
#   CONCS="1 4 16 32 64 128" DUR=10s scripts/bench-capture-cells.sh
#
# Env: BUCKET_DIR DATA_DIR ADDR CONCS DUR SINGLE DUAL MODE OUT INTERNAL ADMIN_TOKEN
#      BUCKET_MODE=fs|s3  S3_BUCKET_URI S3_ENDPOINT S3_ACCESS S3_SECRET
set -euo pipefail
ROOT=${ROOT:-$(cd "$(dirname "$0")/.." && pwd)}
cd "$ROOT"

BIN=./bin
# Derive the same role credentials as cell-agent (ADR-137); the dev root is
# enabled so no secret needs to be passed around.
export CELLHIVE_ALLOW_INSECURE_DEFAULTS=1
ADDR=${ADDR:-http://127.0.0.1:7001}
ADMIN=${ADMIN:-http://127.0.0.1:8082}
BUCKET_DIR=${BUCKET_DIR:-/tmp/capcells/bucket}
DATA_DIR=${DATA_DIR:-/tmp/capcells/data}
CONCS=${CONCS:-"1 4 16 32 64"}
DUR=${DUR:-10s}
MODE=${MODE:-put}
SINGLE=${SINGLE:-capA}
DUAL=${DUAL:-"capB1 capB2"}
INTERNAL=${INTERNAL:-$(CELLHIVE_ALLOW_INSECURE_DEFAULTS=1 "$BIN/cellhive" creds internal)}
ADMIN_TOKEN=${ADMIN_TOKEN:-$(CELLHIVE_ALLOW_INSECURE_DEFAULTS=1 "$BIN/cellhive" creds admin)}
OUT=${OUT:-docs/archive/bench/single-vs-dual-capture.txt}
CAPTURE=${CAPTURE:-on}
BINDING_CACHE=${BINDING_CACHE:-}
BUCKET_MODE=${BUCKET_MODE:-fs}
S3_BUCKET_URI=${S3_BUCKET_URI:-s3://cellhive}
S3_ENDPOINT=${S3_ENDPOINT:-http://127.0.0.1:9000}
S3_ACCESS=${S3_ACCESS:-minioadmin}
S3_SECRET=${S3_SECRET:-minioadmin}

RUN_DIR=$(dirname "$BUCKET_DIR")
LOG="$RUN_DIR/agent.log"
AGENT_PID=""
cleanup() { [[ -n "$AGENT_PID" ]] && kill "$AGENT_PID" 2>/dev/null || true; }
trap cleanup EXIT

claim() {
  curl -s -o /dev/null -w "%{http_code}" -H "x-cellhive-internal-token: $INTERNAL" \
    -X POST "$ADDR/v1/internal/claim" -d "{\"scope\":\"$1\"}"
}
setup_cell() {
  local ns=$1
  claim "$ns/__control__/main" >/dev/null
  claim "$ns/__kv__/default" >/dev/null
  curl -s -o /dev/null -H "x-cellhive-admin-token: $ADMIN_TOKEN" -X POST "$ADMIN/v1/control/app" -d "{\"namespace\":\"$ns\"}"
  curl -s -o /dev/null -H "x-cellhive-admin-token: $ADMIN_TOKEN" -X POST "$ADMIN/v1/control/resource" \
    -d "{\"namespace\":\"$ns\",\"kind\":\"kv\",\"name\":\"KV\",\"scope\":\"default\"}"
}
scan() { # label, space-separated namespaces
  local label=$1 cells=$2 ns arg=""
  for ns in $cells; do arg="$arg,$ns"; done; arg=${arg#,}
  echo "### $label ($(echo "$cells" | wc -w) cell(s): $arg)" | tee -a "$OUT"
  local c
  for c in $CONCS; do
    "$BIN/kvbench" -mode "$MODE" -ns "$arg" -c "$c" -d "$DUR" -addr "$ADDR" | tee -a "$OUT"
  done
}
verify_ltx() { # namespace
  local ns=$1 scope="$1/__kv__/default"
  if [[ "$BUCKET_MODE" != "fs" ]]; then
    echo "--- LTX verify skipped for $scope (BUCKET_MODE=$BUCKET_MODE; use a FS run for restoreverify) ---" | tee -a "$OUT"
    return
  fi
  echo "--- LTX in bucket for $scope (epoch 1) ---" | tee -a "$OUT"
  "$BIN/restoreverify" -bucket "$BUCKET_DIR" -scope "$scope" -epoch 1 -list 2>&1 | tail -5 | tee -a "$OUT"
  "$BIN/restoreverify" -bucket "$BUCKET_DIR" -scope "$scope" -epoch 1 \
    -out "$RUN_DIR/restored-$(echo "$ns" | tr / _).db" 2>&1 | tee -a "$OUT"
}

mkdir -p "$RUN_DIR" "$(dirname "$OUT")"
: > "$OUT"
echo "# single-cell vs dual-cell capture benchmark (capture ON, durability=bucket, RPO=0)" | tee -a "$OUT"
echo "# date=$(date -Is) host=$(hostname) nproc=$(nproc) go=$(go version | awk '{print $3}')" | tee -a "$OUT"
echo "# cmd: CONCS=\"$CONCS\" DUR=$DUR MODE=$MODE BUCKET_MODE=$BUCKET_MODE CAPTURE=$CAPTURE BINDING_CACHE=${BINDING_CACHE:-default}" | tee -a "$OUT"

pkill -x cell-agent 2>/dev/null || true
sleep 1
rm -rf "$RUN_DIR"; mkdir -p "$BUCKET_DIR" "$DATA_DIR"

AGENT_ENV=(CELLHIVE_NODE_ID=bench CELLHIVE_REST_ADDR=:7001 CELLHIVE_LEASE_TTL=300s)
AGENT_ENV+=(CELLHIVE_ALLOW_INSECURE_DEFAULTS=1 CELLHIVE_DATA_DIR="$DATA_DIR" CELLHIVE_DURABILITY=bucket)
AGENT_ENV+=(CELLHIVE_CAPTURE="$CAPTURE")
[[ -n "$BINDING_CACHE" ]] && AGENT_ENV+=(CELLHIVE_BINDING_CACHE="$BINDING_CACHE")
if [[ "$BUCKET_MODE" == "s3" ]]; then
  AGENT_ENV+=(CELLHIVE_BUCKET="$S3_BUCKET_URI" AWS_ENDPOINT_URL="$S3_ENDPOINT" \
    AWS_ACCESS_KEY_ID="$S3_ACCESS" AWS_SECRET_ACCESS_KEY="$S3_SECRET" CELLHIVE_S3_PATH_STYLE=true)
else
  AGENT_ENV+=(CELLHIVE_BUCKET_DIR="$BUCKET_DIR")
fi
env "${AGENT_ENV[@]}" "$BIN/cell-agent" > "$LOG" 2>&1 &
AGENT_PID=$!

for i in $(seq 1 40); do
  code=$(claim "__platform__/__control__/main" || true)
  [[ "$code" == "200" ]] && break
  sleep 0.25
done
if [[ "${code:-}" != "200" ]]; then echo "cell-agent did not become ready" >&2; tail -5 "$LOG" >&2; exit 1; fi

for ns in $SINGLE $DUAL; do setup_cell "$ns"; done

scan "single-cell" "$SINGLE"
scan "dual-cell"   "$DUAL"

if [[ "$CAPTURE" == "off" ]]; then
  echo "--- LTX verify skipped (CAPTURE=off; no replication) ---" | tee -a "$OUT"
else
  verify_ltx "$(echo "$SINGLE" | awk '{print $1}')"
  for ns in $DUAL; do verify_ltx "$ns"; done
fi

echo "wrote $OUT"
