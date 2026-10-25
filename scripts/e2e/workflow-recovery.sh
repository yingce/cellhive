#!/usr/bin/env bash
# Real workerd + HTTP response-loss + cold COS recovery (also runnable with FS).
set -eu
source "$(dirname "$0")/lib.sh"
e2e_start_agent e2ewfr "$E2E_AGENT_PORT" "$E2E_ADMIN_PORT" "http://127.0.0.1:$E2E_RT_INTERNAL" "$E2E_BASE/agent"
e2e_cli app create e2ewfr >/dev/null
e2e_cli domain add e2ewfr workflow-recovery.test >/dev/null
for w in event parallel; do e2e_cli workflow create e2ewfr "$w" >/dev/null; done
SHA=$(e2e_upload "$E2E_ROOT/scripts/e2e/fixtures/workflow-recovery.js")
cat >"$E2E_BASE/deploy.json" <<JSON
{"namespace":"e2ewfr","worker":"worker","bundle_sha":"$SHA","bindings":[{"type":"workflow","name":"EVENT","id":"event","class_name":"EventWorkflow"},{"type":"workflow","name":"PARALLEL","id":"parallel","class_name":"ParallelWorkflow"}]}
JSON
OUT=$(e2e_deploy "$E2E_BASE/deploy.json")
[[ "$(jq -r '.number // empty' <<<"$OUT")" = "1" ]] || e2e_fail "deploy: $OUT"
e2e_cli route add e2ewfr workflow-recovery.test worker >/dev/null
export E2E_BASE E2E_ROOT E2E_BIN E2E_AGENT_PORT E2E_ADMIN_PORT E2E_RT_PORT E2E_RT_INTERNAL
python3 "$E2E_ROOT/scripts/e2e/workflow-recovery.py" --namespace e2ewfr
