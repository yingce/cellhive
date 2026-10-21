#!/usr/bin/env bash
# E2E: queue concurrency smoke (D1 dedupe + overlapping handler intervals).
set -u
source "$(dirname "$0")/lib.sh"

N=${E2E_QUEUE_MESSAGES:-100}
CONC=${E2E_QUEUE_CONCURRENCY:-4}
e2e_log "base=$E2E_BASE messages=$N max_concurrency=$CONC"
e2e_start_agent e2econc "$E2E_AGENT_PORT" "$E2E_ADMIN_PORT" "http://127.0.0.1:$E2E_RT_INTERNAL" "$E2E_BASE/agent"
e2e_start_runtime rt "http://127.0.0.1:$E2E_AGENT_PORT" "$E2E_RT_PORT" "$E2E_RT_INTERNAL"

e2e_cli app create e2econc >/dev/null
e2e_cli domain add e2econc e2e-conc.test >/dev/null
e2e_cli resource create e2econc d1 DB >/dev/null
e2e_cli queue create e2econc q1 >/dev/null
SHA=$(e2e_upload "$E2E_ROOT/scripts/e2e/fixtures/queue-concurrency.js")
[[ -n "$SHA" && "$SHA" != "null" ]] || e2e_fail "bundle upload failed"
cat >"$E2E_BASE/deploy.json" <<JSON
{"namespace":"e2econc","worker":"worker","bundle_sha":"$SHA","bindings":[{"type":"d1","name":"DB","id":"DB"},{"type":"queue","name":"QUEUE","id":"q1"}],"consumers":[{"queue":"q1","max_batch_size":10,"max_batch_timeout_seconds":1,"max_concurrency":$CONC,"max_retries":0,"retry_delay":0}]}
JSON
OUT=$(e2e_deploy "$E2E_BASE/deploy.json")
[[ "$(jq -r '.number // empty' <<<"$OUT")" = "1" ]] || e2e_fail "deploy: $OUT"
e2e_cli route add e2econc e2e-conc.test worker >/dev/null

call() { curl -sS -m "${2:-10}" -H "Host: e2e-conc.test" "http://127.0.0.1:$E2E_RT_PORT$1"; }
for i in $(seq 1 40); do [[ "$(call /ping 2>/dev/null || true)" = "ec-ok" ]] && break; sleep .3; done
[[ "$(call /ping)" = "ec-ok" ]] || e2e_fail "worker not reachable"
[[ "$(call /init)" = "init" ]] || e2e_fail "D1 init failed"

PARALLEL=${E2E_QUEUE_PARALLEL:-20}
[[ "$PARALLEL" -gt "$N" ]] && PARALLEL=$N
SEND=$(call "/send-parallel?n=$PARALLEL" 120)
e2e_log "parallel send: $SEND"
if [[ $((N - PARALLEL)) -gt 0 ]]; then
  SEND2=$(call "/send?n=$((N - PARALLEL))" 120)
  e2e_log "sequential send: $SEND2"
fi
consumed=0
for i in $(seq 1 120); do
  stats=$(call /stats 2>/dev/null || true)
  consumed=$(jq -r '.count // 0' <<<"$stats" 2>/dev/null || echo 0)
  [[ "$consumed" -ge "$N" ]] && break
  sleep .5
done
[[ "$consumed" -ge "$N" ]] || e2e_fail "consumed $consumed/$N"
stats=$(call /stats)
count=$(jq -r '.count' <<<"$stats")
maxc=$(jq -r '.max_concurrent' <<<"$stats")
missing=$(jq -r '.missing_end' <<<"$stats")
e2e_log "stats: $stats"
[[ "$count" = "$N" ]] || e2e_fail "count $count != $N"
[[ "$missing" = "0" ]] || e2e_fail "unfinished handlers: $missing"
[[ "$maxc" -ge 2 ]] || e2e_fail "max_concurrent=$maxc (expected >=2)"
[[ "$maxc" -le $((CONC * 10)) ]] || e2e_fail "max_concurrent=$maxc > configured batches*size $((CONC * 10))"
echo "E2E-QUEUE-CONCURRENCY: PASS (messages=$count max_concurrent=$maxc batches=$CONC batch_size=10)"
