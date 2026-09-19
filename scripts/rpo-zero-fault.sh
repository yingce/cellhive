#!/usr/bin/env bash
# RPO=0 fault injection (celld-style): write keys, SIGKILL the owner mid-stream,
# DELETE its local data directory, then take over on another node and require
# every acknowledged key to come back exactly from the bucket replica.
#
# Usage: scripts/rpo-zero-fault.sh
# Env: N KILL_AT LEASE_TTL OUT
set -uo pipefail
ROOT=${ROOT:-$(cd "$(dirname "$0")/.." && pwd)}
cd "$ROOT"

BIN=./bin
# Derive the same role credentials as cell-agent (ADR-137); the dev root is
# enabled so no secret needs to be passed around.
export CELLHIVE_ALLOW_INSECURE_DEFAULTS=1
RUN=${RUN:-/tmp/rpozero}
BUCKET=$RUN/bucket
D1=$RUN/d1
D2=$RUN/d2
N=${N:-2000}
KILL_AT=${KILL_AT:-1200}
LEASE_TTL=${LEASE_TTL:-3s}
OUT=${OUT:-docs/archive/bench/rpo-zero-fault.txt}
A_URL=http://127.0.0.1:7001
A_ADMIN=http://127.0.0.1:8082
B_URL=http://127.0.0.1:7011
B_ADMIN=http://127.0.0.1:8092
# Root-derived credentials (ADR-137): the dev root is used when ALLOW_INSECURE
# is set, and the same derivation is applied by cell-agent.
INTERNAL=${INTERNAL:-$(CELLHIVE_ALLOW_INSECURE_DEFAULTS=1 "$BIN/cellhive" creds internal)}
ADMIN_TOKEN=${ADMIN_TOKEN:-$(CELLHIVE_ALLOW_INSECURE_DEFAULTS=1 "$BIN/cellhive" creds admin)}

A=""; B=""
cleanup() { [[ -n "$A" ]] && kill "$A" 2>/dev/null; [[ -n "$B" ]] && kill "$B" 2>/dev/null; }
trap cleanup EXIT

mkdir -p "$(dirname "$OUT")" "$BUCKET" "$D1" "$D2"
: > "$OUT"
echo "# RPO=0 fault injection: SIGKILL owner mid-write + delete local DB" | tee -a "$OUT"
echo "# date=$(date -Is) host=$(hostname) N=$N KILL_AT=$KILL_AT LEASE_TTL=$LEASE_TTL" | tee -a "$OUT"

pkill -x cell-agent 2>/dev/null || true
sleep 1
rm -rf "$RUN"; mkdir -p "$BUCKET" "$D1" "$D2"

start_node() { # name data_dir rest admin
  env CELLHIVE_NODE_ID="$1" CELLHIVE_REST_ADDR="$3" CELLHIVE_ADMIN_ADDR="$4" \
    CELLHIVE_ADVERTISE="${3/:/:}" CELLHIVE_PEER_URL="http://127.0.0.1${3}" \
    CELLHIVE_LEASE_TTL="$LEASE_TTL" CELLHIVE_BUCKET_DIR="$BUCKET" CELLHIVE_DATA_DIR="$2" \
    CELLHIVE_DURABILITY=bucket CELLHIVE_ALLOW_INSECURE_DEFAULTS=1 \
    "$BIN/cell-agent" > "$RUN/$1.log" 2>&1 &
  echo $!
}
claim() { curl -s -o /dev/null -w "%{http_code}" -H "x-cellhive-internal-token: $INTERNAL" -X POST "$1/v1/internal/claim" -d "{\"scope\":\"$2\"}"; }
wait_claim() { for _ in $(seq 1 60); do [[ "$(claim "$1" __platform__/__control__/main)" == "200" ]] && return 0; sleep 0.25; done; return 1; }

A=$(start_node a "$D1" :7001 :8082)
wait_claim "$A_URL" || { echo "node A not ready"; exit 1; }
for sc in capA/__control__/main capA/__kv__/default; do claim "$A_URL" "$sc" >/dev/null; done
curl -s -o /dev/null -H "x-cellhive-admin-token: $ADMIN_TOKEN" -X POST "$A_ADMIN/v1/control/app" -d '{"namespace":"capA"}'
curl -s -o /dev/null -H "x-cellhive-admin-token: $ADMIN_TOKEN" -X POST "$A_ADMIN/v1/control/resource" -d '{"namespace":"capA","kind":"kv","name":"KV","scope":"default"}'
TOK=$("$BIN/kvbench" -print-token -ns capA)

ACKED=$RUN/acked.txt
: > "$ACKED"
for i in $(seq 1 "$N"); do
  code=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$A_URL/v1/kv/put?ns=capA&key=k$i" \
    -H "x-cellhive-scope-token: $TOK" --data "v$i")
  if [[ "$code" == "200" ]]; then
    echo "k$i v$i" >> "$ACKED"
  else
    break
  fi
  if [[ "$i" == "$KILL_AT" ]]; then kill -9 "$A"; echo "killed node a at key $i" | tee -a "$OUT"; fi
done
ACKN=$(wc -l < "$ACKED")
echo "acked keys before crash: $ACKN (SIGKILL at $KILL_AT)" | tee -a "$OUT"

# Destroy the dead owner's local state: recovery can only come from the bucket.
rm -rf "$D1"
sleep 4   # owner lease expires

B=$(start_node b "$D2" :7011 :8092)
wait_claim "$B_URL" || { echo "node B not ready"; exit 1; }
for sc in capA/__control__/main capA/__kv__/default; do
  echo "takeover $sc -> $(claim "$B_URL" "$sc")" | tee -a "$OUT"
done

missing=0
wrong=0
while read -r k v; do
  got=$(curl -s "$B_URL/v1/kv/get?ns=capA&key=$k" -H "x-cellhive-scope-token: $TOK")
  if [[ -z "$got" ]]; then missing=$((missing+1)); elif [[ "$got" != "$v" ]]; then wrong=$((wrong+1)); fi
done < "$ACKED"
echo "verified acked keys on node B: missing=$missing wrong=$wrong (of $ACKN)" | tee -a "$OUT"

"$BIN/restoreverify" -bucket "$BUCKET" -scope capA/__kv__/default -epoch 1 -out "$RUN/restored.db" 2>&1 | tee -a "$OUT"

if [[ "$missing" == "0" && "$wrong" == "0" && "$ACKN" -gt 100 ]]; then
  echo "RPO-ZERO: PASS ($ACKN acked keys all present and exact)" | tee -a "$OUT"
  exit 0
fi
echo "RPO-ZERO: FAIL (missing=$missing wrong=$wrong acked=$ACKN)" | tee -a "$OUT"
exit 1
