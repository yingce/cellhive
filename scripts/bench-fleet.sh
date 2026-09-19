#!/usr/bin/env bash
# Two-node fleet benchmark: two cell-agent processes share one bucket and form a
# fleet, so an ack is proven by a peer fsync (fleet) instead of waiting for the
# bucket (single node). Verifies the fleet path by checking the follower's peer
# spool and the bucket's async upload after the run.
#
# Usage:
#   scripts/bench-fleet.sh
#   CONCS="1 16 32 64" DUR=10s scripts/bench-fleet.sh
#
# Env: BUCKET_DIR D1 D2 CONCS DUR OUT INTERNAL ADMIN_TOKEN LEASE_TTL
set -euo pipefail
ROOT=${ROOT:-$(cd "$(dirname "$0")/.." && pwd)}
cd "$ROOT"

BIN=./bin
# Derive the same role credentials as cell-agent (ADR-137); the dev root is
# enabled so no secret needs to be passed around.
export CELLHIVE_ALLOW_INSECURE_DEFAULTS=1
BUCKET_DIR=${BUCKET_DIR:-/tmp/fleet/bucket}
D1=${D1:-/tmp/fleet/d1}
D2=${D2:-/tmp/fleet/d2}
CONCS=${CONCS:-"1 16 32 64"}
DUR=${DUR:-10s}
OUT=${OUT:-docs/archive/bench/fleet-2node.txt}
INTERNAL=${INTERNAL:-$(CELLHIVE_ALLOW_INSECURE_DEFAULTS=1 "$BIN/cellhive" creds internal)}
ADMIN_TOKEN=${ADMIN_TOKEN:-$(CELLHIVE_ALLOW_INSECURE_DEFAULTS=1 "$BIN/cellhive" creds admin)}
LEASE_TTL=${LEASE_TTL:-10s}
NS=${NS:-capA}

A_REST=:7001; A_ADMIN=:8082
B_REST=:7011; B_ADMIN=:8092
A_URL=http://127.0.0.1:7001
A_CTL=http://127.0.0.1:8082

AP=""; BP=""
cleanup() { [[ -n "$AP" ]] && kill "$AP" 2>/dev/null || true; [[ -n "$BP" ]] && kill "$BP" 2>/dev/null || true; }
trap cleanup EXIT

mkdir -p "$(dirname "$OUT")" "$BUCKET_DIR"
: > "$OUT"
echo "# 2-node fleet benchmark (capture ON, durability=auto -> peer fsync; bucket async after ack)" | tee -a "$OUT"
echo "# date=$(date -Is) host=$(hostname) nproc=$(nproc) go=$(go version | awk '{print $3}')" | tee -a "$OUT"
echo "# cmd: CONCS=\"$CONCS\" DUR=$DUR LEASE_TTL=$LEASE_TTL" | tee -a "$OUT"

pkill -x cell-agent 2>/dev/null || true
sleep 1
rm -rf "$(dirname "$BUCKET_DIR")"; mkdir -p "$BUCKET_DIR" "$D1" "$D2"

CELLHIVE_NODE_ID=a CELLHIVE_REST_ADDR=$A_REST CELLHIVE_ADMIN_ADDR=$A_ADMIN \
  CELLHIVE_PEER_URL=$A_URL CELLHIVE_ADVERTISE=127.0.0.1:7001 \
  CELLHIVE_LEASE_TTL=$LEASE_TTL CELLHIVE_BUCKET_DIR="$BUCKET_DIR" CELLHIVE_DATA_DIR="$D1" \
  CELLHIVE_ALLOW_INSECURE_DEFAULTS=1 CELLHIVE_DURABILITY=auto \
  "$BIN/cell-agent" > "$(dirname "$BUCKET_DIR")/a.log" 2>&1 &
AP=$!
CELLHIVE_NODE_ID=b CELLHIVE_REST_ADDR=$B_REST CELLHIVE_ADMIN_ADDR=$B_ADMIN \
  CELLHIVE_PEER_URL=http://127.0.0.1:7011 CELLHIVE_ADVERTISE=127.0.0.1:7011 \
  CELLHIVE_LEASE_TTL=$LEASE_TTL CELLHIVE_BUCKET_DIR="$BUCKET_DIR" CELLHIVE_DATA_DIR="$D2" \
  CELLHIVE_ALLOW_INSECURE_DEFAULTS=1 CELLHIVE_DURABILITY=auto \
  "$BIN/cell-agent" > "$(dirname "$BUCKET_DIR")/b.log" 2>&1 &
BP=$!

claim() { curl -s -o /dev/null -w "%{http_code}" -H "x-cellhive-internal-token: $INTERNAL" \
  -X POST "$A_URL/v1/internal/claim" -d "{\"scope\":\"$1\"}"; }
for i in $(seq 1 40); do [[ "$(claim __platform__/__control__/main)" == "200" ]] && break; sleep 0.25; done
claim "$NS/__control__/main" >/dev/null
claim "$NS/__kv__/default" >/dev/null
curl -s -o /dev/null -H "x-cellhive-admin-token: $ADMIN_TOKEN" -X POST "$A_CTL/v1/control/app" -d "{\"namespace\":\"$NS\"}"
curl -s -o /dev/null -H "x-cellhive-admin-token: $ADMIN_TOKEN" -X POST "$A_CTL/v1/control/resource" \
  -d "{\"namespace\":\"$NS\",\"kind\":\"kv\",\"name\":\"KV\",\"scope\":\"default\"}"
# Let both leases settle so the owner sees the follower.
sleep 6

c_opt=""
for c in $CONCS; do
  "$BIN/kvbench" -mode put -ns "$NS" -c "$c" -d "$DUR" -addr "$A_URL" | tee -a "$OUT"
done

spool="$(find "$D2/peer-spool/$NS/__kv__" -type f 2>/dev/null | wc -l)"
batch="$(find "$BUCKET_DIR/cells/$NS/__kv__/default/ltx" -name '*.batch' 2>/dev/null | wc -l)"
echo "# fleet path proof: follower spool files=$spool  bucket .batch objects=$batch" | tee -a "$OUT"
echo "wrote $OUT"
