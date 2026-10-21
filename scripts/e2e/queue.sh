#!/usr/bin/env bash
# E2E: queue consumer, R2 writeHttpMetadata, retry and DLQ.
set -u
source "$(dirname "$0")/lib.sh"

e2e_log "base=$E2E_BASE agent=$E2E_AGENT_PORT runtime=$E2E_RT_PORT"
e2e_start_agent e2eq "$E2E_AGENT_PORT" "$E2E_ADMIN_PORT" "http://127.0.0.1:$E2E_RT_INTERNAL" "$E2E_BASE/agent"
e2e_start_runtime rt "http://127.0.0.1:$E2E_AGENT_PORT" "$E2E_RT_PORT" "$E2E_RT_INTERNAL"

e2e_cli app create e2eq >/dev/null
e2e_cli domain add e2eq e2e-q.test >/dev/null
e2e_cli resource create e2eq kv KV >/dev/null
e2e_cli resource create e2eq r2 BUCKET >/dev/null
e2e_cli queue create e2eq q1 >/dev/null
e2e_cli queue create e2eq q1-dlq >/dev/null
SHA=$(e2e_upload "$E2E_ROOT/scripts/e2e/fixtures/queue.js")
[[ -n "$SHA" && "$SHA" != "null" ]] || e2e_fail "bundle upload failed"
cat >"$E2E_BASE/deploy.json" <<JSON
{"namespace":"e2eq","worker":"worker","bundle_sha":"$SHA","bindings":[{"type":"kv","name":"KV","id":"KV"},{"type":"r2","name":"BUCKET","id":"BUCKET"},{"type":"queue","name":"QUEUE","id":"q1"}],"consumers":[{"queue":"q1","max_batch_size":1,"max_batch_timeout_seconds":1,"max_concurrency":1,"max_retries":1,"retry_delay":0,"dead_letter_queue":"q1-dlq"}]}
JSON
OUT=$(e2e_deploy "$E2E_BASE/deploy.json")
[[ "$(jq -r '.number // empty' <<<"$OUT")" = "1" ]] || e2e_fail "deploy: $OUT"
e2e_cli route add e2eq e2e-q.test worker >/dev/null

call() { e2e_http "$E2E_RT_PORT" "$1" e2e-q.test; }
for i in $(seq 1 40); do [[ "$(call /ping 2>/dev/null || true)" = "q-ok" ]] && break; sleep .3; done
[[ "$(call /ping)" = "q-ok" ]] || e2e_fail "worker not reachable"

[[ "$(call /send)" = "sent" ]] || e2e_fail "send failed"
status=""
for i in $(seq 1 40); do status=$(call /status); [[ "$status" = "text/x-queue" ]] && break; sleep .5; done
[[ "$status" = "text/x-queue" ]] || e2e_fail "R2 metadata in queue: $status"
e2e_log "queue R2 metadata: text/x-queue"

[[ "$(call /fail)" = "retried" ]] || e2e_fail "fail enqueue"
dlq=0
for i in $(seq 1 40); do
  dlq=$(e2e_cli queue stats e2eq q1-dlq 2>/dev/null | jq -r '.stats.depth // 0' 2>/dev/null || echo 0)
  [[ "$dlq" -ge 1 ]] && break
  sleep 1
done
[[ "$dlq" -ge 1 ]] || e2e_fail "message did not reach DLQ"
e2e_log "retry exhausted -> DLQ depth=$dlq"

echo "E2E-QUEUE: PASS"
