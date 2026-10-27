#!/usr/bin/env bash
# Dual-agent KV forwarding must retain deletion/recreation and auth isolation.
set -euo pipefail
source "$(dirname "$0")/lib.sh"
e2e_start_agent perf "$E2E_AGENT_PORT" "$E2E_ADMIN_PORT" '' "$E2E_BASE/agent"
e2e_start_agent peer "$((E2E_PORT_BASE+4))" "$((E2E_PORT_BASE+5))" '' "$E2E_BASE/peer"
e2e_cli app create e2eroute >/dev/null
e2e_cli resource create e2eroute kv KV >/dev/null
TOKEN=$("$E2E_BIN/kvbench" -ns e2eroute -print-token)
OWNER="http://127.0.0.1:$E2E_AGENT_PORT"
PEER="http://127.0.0.1:$((E2E_PORT_BASE+4))"
request() { curl -sS --max-time 30 -o "$E2E_BASE/response" -w '%{http_code}' -H "x-cellhive-scope-token: $TOKEN" "$@"; }
[[ $(request -X POST --data-binary old "$OWNER/v1/kv/put?ns=e2eroute&key=old") = 200 ]] || e2e_fail 'initial put'
[[ $(request "$PEER/v1/kv/get?ns=e2eroute&key=old") = 200 && $(cat "$E2E_BASE/response") = old ]] || e2e_fail 'forward get'
[[ $(request -X POST --data-binary forwarded "$PEER/v1/kv/put?ns=e2eroute&key=forwarded") = 200 ]] || e2e_fail 'forward put'
[[ $(request -X POST -H 'content-type: application/json' --data-binary '{"keys":["old","forwarded"]}' "$PEER/v1/kv/get-batch?ns=e2eroute") = 200 ]] || e2e_fail 'forward get-batch'
[[ $(request "$PEER/v1/kv/get?ns=wrong-namespace&key=old") = 403 ]] || e2e_fail 'namespace isolation'
code=$(curl -sS --max-time 30 -o "$E2E_BASE/response" -w '%{http_code}' -H 'x-cellhive-scope-token: invalid' "$PEER/v1/kv/get?ns=e2eroute&key=old")
[[ $code = 403 ]] || e2e_fail 'invalid token'
e2e_cli resource delete e2eroute kv KV >/dev/null
# Existing cache validity remains bounded: wait beyond the one-second cache.
sleep 2
for base in "$OWNER" "$PEER"; do
 [[ $(request "$base/v1/kv/get?ns=e2eroute&key=old") = 403 ]] || e2e_fail 'deleted resource readable'
 [[ $(request -X POST --data-binary resurrect "$base/v1/kv/put?ns=e2eroute&key=old") = 403 ]] || e2e_fail 'deleted resource writable'
done
ready=0
for i in $(seq 1 90); do
 if e2e_cli resource create e2eroute kv KV >"$E2E_BASE/recreate.log" 2>&1; then ready=1; break; fi
 sleep 1
done
[[ $ready = 1 ]] || e2e_fail 'cleanup/recreate did not complete'
sleep 2
# Legacy namespace token can access the current registration, but old data
# must never reappear. Pinned-version auth retains its separate existing tests.
for base in "$OWNER" "$PEER"; do
 [[ $(request "$base/v1/kv/get?ns=e2eroute&key=old") = 404 ]] || e2e_fail 'old incarnation data reappeared'
done
[[ $(request -X POST --data-binary new "$PEER/v1/kv/put?ns=e2eroute&key=new") = 200 ]] || e2e_fail 'recreated put'
[[ $(request "$OWNER/v1/kv/get?ns=e2eroute&key=new") = 200 && $(cat "$E2E_BASE/response") = new ]] || e2e_fail 'recreated cross-node read'
echo 'E2E-KV-ROUTING-SAFETY: PASS (forward GET/PUT/batch, auth rejection, delete, recreate, old data absent)'
