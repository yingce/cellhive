#!/usr/bin/env bash
# E2E: KV + D1 + R2 + local R2 metadata bundle through the worker env.
set -u
source "$(dirname "$0")/lib.sh"

e2e_log "base=$E2E_BASE agent=$E2E_AGENT_PORT runtime=$E2E_RT_PORT"
e2e_start_agent e2eb "$E2E_AGENT_PORT" "$E2E_ADMIN_PORT" "http://127.0.0.1:$E2E_RT_INTERNAL" "$E2E_BASE/agent"
e2e_start_runtime rt "http://127.0.0.1:$E2E_AGENT_PORT" "$E2E_RT_PORT" "$E2E_RT_INTERNAL"

e2e_cli app create e2eb >/dev/null
e2e_cli domain add e2eb e2e-b.test >/dev/null
e2e_cli resource create e2eb kv KV >/dev/null
e2e_cli resource create e2eb d1 DB >/dev/null
e2e_cli resource create e2eb r2 BUCKET >/dev/null
SHA=$(e2e_upload "$E2E_ROOT/scripts/e2e/fixtures/bindings.js")
[[ -n "$SHA" && "$SHA" != "null" ]] || e2e_fail "bundle upload failed"
cat >"$E2E_BASE/deploy.json" <<JSON
{"namespace":"e2eb","worker":"worker","bundle_sha":"$SHA","bindings":[{"type":"kv","name":"KV","id":"KV"},{"type":"d1","name":"DB","id":"DB"},{"type":"r2","name":"R2A","id":"BUCKET"},{"type":"r2","name":"R2B","id":"BUCKET"}]}
JSON
OUT=$(e2e_deploy "$E2E_BASE/deploy.json")
[[ "$(jq -r '.number // empty' <<<"$OUT")" = "1" ]] || e2e_fail "deploy: $OUT"
e2e_cli route add e2eb e2e-b.test worker >/dev/null

call() { e2e_http "$E2E_RT_PORT" "$1" e2e-b.test; }
out=""
for i in $(seq 1 40); do out=$(call / 2>/dev/null || true); echo "$out" | jq -e . >/dev/null 2>&1 && break; sleep .5; done
echo "response=$out"
[[ "$(jq -r .kv <<<"$out" 2>/dev/null)" = "v" ]] || e2e_fail "KV: $out"
[[ "$(jq -r .d1 <<<"$out" 2>/dev/null)" = "1" ]] || e2e_fail "D1: $out"
[[ "$(jq -r .r2 <<<"$out" 2>/dev/null)" = "text/x-r2" ]] || e2e_fail "R2 metadata: $out"
echo "E2E-BINDINGS: PASS"
