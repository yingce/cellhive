#!/usr/bin/env bash
# ADR-292: real agent + workerd HTTP, isolated files/ports, no cloud resources.
set -eu
source "$(dirname "$0")/lib.sh"

e2e_start_agent review "$E2E_AGENT_PORT" "$E2E_ADMIN_PORT" "http://127.0.0.1:$E2E_RT_INTERNAL" "$E2E_BASE/agent"
e2e_start_runtime rt "http://127.0.0.1:$E2E_AGENT_PORT" "$E2E_RT_PORT" "$E2E_RT_INTERNAL"
e2e_cli app create review >/dev/null
e2e_cli domain add review review.test >/dev/null
e2e_cli resource create review d1 DB >/dev/null
e2e_cli resource create review r2 BUCKET >/dev/null
e2e_cli resource create review queue jobs >/dev/null
SHA=$(e2e_upload "$E2E_ROOT/scripts/e2e/fixtures/review-fixes.js")
cat > "$E2E_BASE/deploy.json" <<JSON
{"namespace":"review","worker":"review","bundle_sha":"$SHA","bindings":[{"type":"d1","name":"DB","id":"DB"},{"type":"r2","name":"BUCKET","id":"BUCKET"}],"consumers":[{"queue":"jobs","max_concurrency":101}]}
JSON
code=$(curl -sS -o "$E2E_BASE/rejected.json" -w '%{http_code}' -X POST   -H "x-cellhive-admin-token: $CELLHIVE_ADMIN_TOKEN" -H 'content-type: application/json'   --data-binary @"$E2E_BASE/deploy.json" "http://127.0.0.1:$E2E_ADMIN_PORT/v1/control/deploy")
e2e_need "$code" 400 "oversized queue concurrency must be rejected"
jq '.consumers[0].max_concurrency = 100' "$E2E_BASE/deploy.json" > "$E2E_BASE/valid.json"
out=$(e2e_deploy "$E2E_BASE/valid.json")
[[ "$(jq -r '.number // empty' <<<"$out")" = 1 ]] || e2e_fail "boundary queue deploy failed: $out"
e2e_cli route add review review.test review >/dev/null
for i in $(seq 1 40); do
  ready=$(e2e_http "$E2E_RT_PORT" / review.test 2>/dev/null || true)
  [[ "$ready" = ok ]] && break
  sleep .2
done
[[ "$ready" = ok ]] || e2e_fail "route projection did not become ready"
out=$(e2e_http "$E2E_RT_PORT" /r2 review.test)
jq -e '.size == 16 and .etag and .cleared and .body == "longer new value" and .range == "long"' <<<"$out" >/dev/null || e2e_fail "R2 overwrite: $out"
e2e_log "R2 different/same-body overwrite, HEAD/GET/range PASS"
# Warm the isolate and DB, then collect five complete public HTTP observations.
out=$(e2e_http "$E2E_RT_PORT" '/d1?rows=1' review.test)
jq -e '.rows == 1' <<<"$out" >/dev/null || e2e_fail "D1 warmup: $out"
for i in 1 2 3 4 5; do
  elapsed=$(curl -sS -m 15 -o "$E2E_BASE/d1.json" -w '%{time_total}' -H 'Host: review.test'     "http://127.0.0.1:$E2E_RT_PORT/d1?rows=20000")
  jq -e '.rows == 20000 and .last == 20000' "$E2E_BASE/d1.json" >/dev/null || e2e_fail "D1 result invalid"
  e2e_log "D1 rows=20000 sample=$i HTTP_total_s=$elapsed binding_ms=$(jq -r .duration_ms "$E2E_BASE/d1.json")"
done
echo "E2E-REVIEW-FIXES: PASS"
