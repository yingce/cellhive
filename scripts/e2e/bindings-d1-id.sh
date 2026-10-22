#!/usr/bin/env bash
# E2E: a D1 binding whose env key differs from the database resource id.
# Regression: loader used the binding name ("DB") as the database id, opening
# an empty cell and reporting "no such table".
set -u
source "$(dirname "$0")/lib.sh"
export CELLHIVE_CONTROL_URL="http://127.0.0.1:$E2E_AGENT_PORT"

DB=store-inspection-248e4119
e2e_log "base=$E2E_BASE agent=$E2E_AGENT_PORT runtime=$E2E_RT_PORT db=$DB"
e2e_start_agent e2ed "$E2E_AGENT_PORT" "$E2E_ADMIN_PORT" "http://127.0.0.1:$E2E_RT_INTERNAL" "$E2E_BASE/agent"
e2e_start_runtime rt "http://127.0.0.1:$E2E_AGENT_PORT" "$E2E_RT_PORT" "$E2E_RT_INTERNAL"

e2e_cli app create e2ed >/dev/null
e2e_cli domain add e2ed e2e-d1.test >/dev/null
e2e_cli resource create e2ed d1 "$DB" >/dev/null
e2e_cli d1 execute e2ed "$DB" \
  --command 'CREATE TABLE store_inspections (id INTEGER);' \
  --command 'INSERT INTO store_inspections (id) VALUES (1);' >/dev/null
SHA=$(e2e_upload "$E2E_ROOT/scripts/e2e/fixtures/d1-id.js")
[[ -n "$SHA" && "$SHA" != "null" ]] || e2e_fail "bundle upload failed"
cat >"$E2E_BASE/deploy.json" <<JSON
{"namespace":"e2ed","worker":"worker","bundle_sha":"$SHA","bindings":[{"type":"d1","name":"DB","id":"$DB"}]}
JSON
OUT=$(e2e_deploy "$E2E_BASE/deploy.json")
[[ "$(jq -r '.number // empty' <<<"$OUT")" = "1" ]] || e2e_fail "deploy: $OUT"
e2e_cli route add e2ed e2e-d1.test worker >/dev/null

call() { e2e_http "$E2E_RT_PORT" "$1" e2e-d1.test; }
out=""
for i in $(seq 1 40); do out=$(call / 2>/dev/null || true); echo "$out" | jq -e . >/dev/null 2>&1 && break; sleep .5; done
echo "response=$out"
[[ "$(jq -r .n <<<"$out" 2>/dev/null)" = "1" ]] || e2e_fail "D1 id binding: $out"
echo "E2E-BINDINGS-D1-ID: PASS"
