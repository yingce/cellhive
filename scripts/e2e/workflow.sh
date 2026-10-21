#!/usr/bin/env bash
# E2E: workflow long sleep, waitForEvent timeout, durable retry, R2 metadata.
set -u
source "$(dirname "$0")/lib.sh"

e2e_log "base=$E2E_BASE agent=$E2E_AGENT_PORT runtime=$E2E_RT_PORT"
e2e_start_agent e2ewf "$E2E_AGENT_PORT" "$E2E_ADMIN_PORT" "http://127.0.0.1:$E2E_RT_INTERNAL" "$E2E_BASE/agent"
e2e_start_runtime rt "http://127.0.0.1:$E2E_AGENT_PORT" "$E2E_RT_PORT" "$E2E_RT_INTERNAL"

e2e_cli app create e2ewf >/dev/null
e2e_cli domain add e2ewf e2e-wf.test >/dev/null
e2e_cli resource create e2ewf kv KV >/dev/null
e2e_cli resource create e2ewf r2 BUCKET >/dev/null
for w in sleep timeout retry r2; do e2e_cli workflow create e2ewf "$w" >/dev/null; done
SHA=$(e2e_upload "$E2E_ROOT/scripts/e2e/fixtures/workflow.js")
[[ -n "$SHA" && "$SHA" != "null" ]] || e2e_fail "bundle upload failed"
cat >"$E2E_BASE/deploy.json" <<JSON
{"namespace":"e2ewf","worker":"worker","bundle_sha":"$SHA","bindings":[{"type":"kv","name":"KV","id":"KV"},{"type":"r2","name":"BUCKET","id":"BUCKET"},{"type":"workflow","name":"WF_SLEEP","id":"sleep","class_name":"SleepWorkflow"},{"type":"workflow","name":"WF_TIMEOUT","id":"timeout","class_name":"TimeoutWorkflow"},{"type":"workflow","name":"WF_RETRY","id":"retry","class_name":"RetryWorkflow"},{"type":"workflow","name":"WF_R2","id":"r2","class_name":"R2Workflow"}]}
JSON
OUT=$(e2e_deploy "$E2E_BASE/deploy.json")
[[ "$(jq -r '.number // empty' <<<"$OUT")" = "1" ]] || e2e_fail "deploy: $OUT"
e2e_cli route add e2ewf e2e-wf.test worker >/dev/null

call() { e2e_http "$E2E_RT_PORT" "$1" e2e-wf.test; }
for i in $(seq 1 40); do [[ "$(call /ping 2>/dev/null || true)" = "w-ok" ]] && break; sleep .3; done
[[ "$(call /ping)" = "w-ok" ]] || e2e_fail "worker not reachable"

wait_status() { # kind id want attempts
  local kind=$1 id=$2 want=$3 n=${4:-60} got=""
  for i in $(seq 1 "$n"); do
    got=$(call "/status?kind=$kind&id=$id" | jq -r '.status // empty' 2>/dev/null || true)
    [[ "$got" = "$want" ]] && { printf '%s' "$got"; return 0; }
    sleep .5
  done
  printf '%s' "$got"
  return 1
}

SID=$(call /sleep | jq -r '.id // empty'); [[ -n "$SID" ]] || e2e_fail "sleep create"
wait_status sleep "$SID" waiting 30 >/dev/null || true
[[ "$(call "/status?kind=sleep&id=$SID" | jq -r .status)" = "waiting" ]] || e2e_fail "sleep did not park"
wait_status sleep "$SID" complete 60 >/dev/null || e2e_fail "sleep did not complete"
[[ "$(call "/status?kind=sleep&id=$SID" | jq -r '.output.slept')" = "true" ]] || e2e_fail "sleep output"
e2e_log "sleep: waiting -> complete"

TID=$(call /timeout | jq -r '.id // empty'); [[ -n "$TID" ]] || e2e_fail "timeout create"
wait_status timeout "$TID" errored 60 >/dev/null || e2e_fail "timeout did not error"
TD=$(call "/status?kind=timeout&id=$TID")
[[ "$(jq -r .status <<<"$TD")" = "errored" ]] || e2e_fail "timeout status"
grep -q "timed out" <<<"$(jq -r '.error // ""' <<<"$TD")" || e2e_fail "timeout error: $TD"
e2e_log "timeout: waiting -> errored"

RID=$(call /retry | jq -r '.id // empty'); [[ -n "$RID" ]] || e2e_fail "retry create"
wait_status retry "$RID" complete 90 >/dev/null || e2e_fail "retry did not complete"
RD=$(call "/status?kind=retry&id=$RID")
[[ "$(jq -r '.output.attempts // 0' <<<"$RD")" = "3" ]] || e2e_fail "retry attempts: $RD"
e2e_log "retry: failed twice -> complete on attempt 3"

R2ID=$(call /r2 | jq -r '.id // empty'); [[ -n "$R2ID" ]] || e2e_fail "r2 workflow create"
wait_status r2 "$R2ID" complete 60 >/dev/null || e2e_fail "r2 workflow did not complete"
R2D=$(call "/status?kind=r2&id=$R2ID")
[[ "$(jq -r '.output.ct // ""' <<<"$R2D")" = "text/x-workflow" ]] || e2e_fail "r2 metadata: $R2D"
e2e_log "r2 workflow metadata: text/x-workflow"

echo "E2E-WORKFLOW: PASS"
