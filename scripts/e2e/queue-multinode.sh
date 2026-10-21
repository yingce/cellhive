#!/usr/bin/env bash
# E2E: queue owner failover across two cell-agents (shared bucket), retry + DLQ.
set -u
source "$(dirname "$0")/lib.sh"

N1=$E2E_AGENT_PORT; A1=$E2E_ADMIN_PORT; R1=$E2E_RT_PORT; I1=$E2E_RT_INTERNAL
N2=$((E2E_PORT_BASE + 4)); A2=$((E2E_PORT_BASE + 5)); R2=$((E2E_PORT_BASE + 6)); I2=$((E2E_PORT_BASE + 7))
e2e_log "base=$E2E_BASE n1=$N1 n2=$N2"
e2e_start_agent n1 "$N1" "$A1" "http://127.0.0.1:$I1" "$E2E_BASE/n1"
e2e_start_agent n2 "$N2" "$A2" "http://127.0.0.1:$I2" "$E2E_BASE/n2"
e2e_start_runtime rt1 "http://127.0.0.1:$N1" "$R1" "$I1"
e2e_start_runtime rt2 "http://127.0.0.1:$N2" "$R2" "$I2"

e2e_cli app create e2emn >/dev/null
e2e_cli domain add e2emn e2e-mn.test >/dev/null
e2e_cli resource create e2emn kv KV >/dev/null
e2e_cli queue create e2emn q1 >/dev/null
e2e_cli queue create e2emn q1-dlq >/dev/null
SHA=$(e2e_upload "$E2E_ROOT/scripts/e2e/fixtures/queue-multinode.js")
[[ -n "$SHA" && "$SHA" != "null" ]] || e2e_fail "bundle upload failed"
cat >"$E2E_BASE/deploy.json" <<JSON
{"namespace":"e2emn","worker":"worker","bundle_sha":"$SHA","bindings":[{"type":"kv","name":"KV","id":"KV"},{"type":"queue","name":"QUEUE","id":"q1"}],"consumers":[{"queue":"q1","max_batch_size":2,"max_batch_timeout_seconds":1,"max_concurrency":1,"max_retries":1,"retry_delay":0,"dead_letter_queue":"q1-dlq"}]}
JSON
OUT=$(e2e_deploy "$E2E_BASE/deploy.json")
[[ "$(jq -r '.number // empty' <<<"$OUT")" = "1" ]] || e2e_fail "deploy: $OUT"
e2e_cli route add e2emn e2e-mn.test worker >/dev/null

call1() { e2e_http "$R1" "$1" e2e-mn.test; }
call2() { e2e_http "$R2" "$1" e2e-mn.test; }
for i in $(seq 1 40); do [[ "$(call1 /ping 2>/dev/null || true)" = "qm-ok" ]] && break; sleep .3; done
[[ "$(call1 /ping)" = "qm-ok" ]] || e2e_fail "worker not reachable"

for i in $(seq 1 5); do call1 /send >/dev/null; done
n=0
for i in $(seq 1 40); do n=$(call2 /status 2>/dev/null | jq -r .n 2>/dev/null || echo 0); [[ "$n" -ge 5 ]] && break; sleep .5; done
[[ "$n" -ge 5 ]] || e2e_fail "normal messages not consumed: $n"
e2e_log "consumed after 5: $n"

call1 /fail >/dev/null
dlq=0
for i in $(seq 1 40); do
  dlq=$(e2e_cli queue stats e2emn q1-dlq 2>/dev/null | jq -r '.stats.depth // 0' 2>/dev/null || echo 0)
  [[ "$dlq" -ge 1 ]] && break
  sleep 1
done
[[ "$dlq" -ge 1 ]] || e2e_fail "message did not reach DLQ"
e2e_log "DLQ depth=$dlq"

kill -9 "$(cat "$E2E_BASE/rt1.pid")" "$(cat "$E2E_BASE/n1.pid")" 2>/dev/null || true
e2e_log "killed n1; waiting lease"
sleep 12
for i in $(seq 1 3); do call2 /send >/dev/null; done
n2=0
for i in $(seq 1 60); do n2=$(call2 /status 2>/dev/null | jq -r .n 2>/dev/null || echo 0); [[ "$n2" -ge $((n + 3)) ]] && break; sleep .5; done
[[ "$n2" -ge $((n + 3)) ]] || e2e_fail "n2 did not consume after failover: $n2"
e2e_log "consumed after failover: $n2"
echo "E2E-QUEUE-MULTINODE: PASS"
