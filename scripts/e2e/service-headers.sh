#!/usr/bin/env bash
# E2E: service-binding JSON fallback preserves tenant request/response headers.
# The caller sets CH_SERVICE_NATIVE=0 so bindings.js takes the HTTP fallback
# instead of the native JSRPC path.
set -euo pipefail
E2E_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
export E2E_ROOT
source "$E2E_ROOT/scripts/e2e/lib.sh"

e2e_start_agent svc "$E2E_AGENT_PORT" "$E2E_ADMIN_PORT" "http://127.0.0.1:$E2E_RT_INTERNAL" "$E2E_BASE/agent"
e2e_start_runtime rt "http://127.0.0.1:$E2E_AGENT_PORT" "$E2E_RT_PORT" "$E2E_RT_INTERNAL"

e2e_cli app create svc >/dev/null
e2e_cli domain add svc svc.test >/dev/null
TARGET=$(e2e_upload "$E2E_ROOT/scripts/e2e/fixtures/service-target.js")
CALLER=$(e2e_upload "$E2E_ROOT/scripts/e2e/fixtures/service-caller.js")
cat >"$E2E_BASE/target.json" <<JSON
{"namespace":"svc","worker":"target","bundle_sha":"$TARGET"}
JSON
e2e_deploy "$E2E_BASE/target.json" >/dev/null
cat >"$E2E_BASE/caller.json" <<JSON
{"namespace":"svc","worker":"caller","bundle_sha":"$CALLER","bindings":[{"type":"service","name":"SVC","id":"target"}],"vars":{"CH_SERVICE_NATIVE":"0"}}
JSON
e2e_deploy "$E2E_BASE/caller.json" >/dev/null
e2e_cli route add svc svc.test caller >/dev/null

for _ in $(seq 1 50); do
  code=$(curl -s -o "$E2E_BASE/resp" -D "$E2E_BASE/hdr" -w '%{http_code}' \
    -H 'Host: svc.test' "http://127.0.0.1:$E2E_RT_PORT/")
  [[ "$code" = 200 ]] && break
  sleep .2
done
[[ "$code" = 200 ]] || e2e_fail "service caller returned $code"
jq -e '.cookie=="a=b" and .xtest=="yes"' "$E2E_BASE/resp" >/dev/null \
  || e2e_fail "target did not receive request headers: $(cat "$E2E_BASE/resp")"
grep -qi '^x-caller: yes' "$E2E_BASE/hdr" \
  || e2e_fail "caller did not receive target response header"
echo "E2E-SERVICE-HEADERS: PASS"
