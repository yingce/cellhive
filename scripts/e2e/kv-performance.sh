#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/lib.sh"
e2e_start_agent perf "$E2E_AGENT_PORT" "$E2E_ADMIN_PORT" "http://127.0.0.1:$E2E_RT_INTERNAL" "$E2E_BASE/agent"
if [[ "${E2E_DUAL_AGENT:-0}" = 1 ]]; then
  e2e_start_agent peer "$((E2E_PORT_BASE+4))" "$((E2E_PORT_BASE+5))" "http://127.0.0.1:$E2E_RT_INTERNAL" "$E2E_BASE/peer"
  export PERF_PEER="http://127.0.0.1:$((E2E_PORT_BASE+4))"
  PERF_INTERNAL_TOKEN=$(node -e 'const c=require("crypto");process.stdout.write(Buffer.from(c.hkdfSync("sha256",Buffer.from(process.env.CELLHIVE_ROOT_KEY,"hex"),Buffer.alloc(0),"cellhive/token/internal",32)).toString("base64url"))')
  export PERF_INTERNAL_TOKEN
fi
e2e_start_runtime rt "http://127.0.0.1:$E2E_AGENT_PORT" "$E2E_RT_PORT" "$E2E_RT_INTERNAL"
e2e_cli app create e2eperf >/dev/null
e2e_cli domain add e2eperf e2eperf.test >/dev/null
e2e_cli resource create e2eperf kv KV >/dev/null
SHA=$(e2e_upload "$E2E_ROOT/scripts/e2e/fixtures/kv-performance.js")
cat > "$E2E_BASE/deploy.json" <<JSON
{"namespace":"e2eperf","worker":"perf","bundle_sha":"$SHA","bindings":[{"type":"kv","name":"KV","id":"KV"}]}
JSON
OUT=$(e2e_deploy "$E2E_BASE/deploy.json")
[[ "$(jq -r '.number // empty' <<<"$OUT")" = 1 ]] || e2e_fail "initial deployment failed"
e2e_cli route add e2eperf e2eperf.test perf >/dev/null
ready=''
for i in $(seq 1 80); do
  ready=$(e2e_http "$E2E_RT_PORT" /perf-ready e2eperf.test 2>/dev/null || true)
  [[ "$ready" = ok ]] && break
  sleep .25
done
[[ "$ready" = ok ]] || e2e_fail 'runtime route unavailable'
export PERF_AGENT="http://127.0.0.1:$E2E_AGENT_PORT" PERF_RUNTIME="http://127.0.0.1:$E2E_RT_PORT" PERF_NS=e2eperf PERF_HOST=e2eperf.test
PERF_SCOPE_TOKEN=$("$E2E_BIN/kvbench" -ns e2eperf -print-token)
export PERF_SCOPE_TOKEN
node "$E2E_ROOT/scripts/e2e/kv-performance.cjs" | tee "$E2E_BASE/performance.jsonl"
unset PERF_SCOPE_TOKEN PERF_INTERNAL_TOKEN
# Snapshot resource use and logs before cleanup.
ps -p "$(cat "$E2E_BASE/perf.pid"),$(cat "$E2E_BASE/rt.pid")" -o pid,pcpu,rss,etime,args
curl -fsS "$PERF_AGENT/metrics" > "$E2E_BASE/metrics.txt" || true
if [[ -n "${PERF_PEER:-}" ]]; then curl -fsS "$PERF_PEER/metrics" > "$E2E_BASE/peer-metrics.txt"; fi
export PERF_RUN_DIR="$E2E_BASE" PERF_ADMIN="http://127.0.0.1:$E2E_ADMIN_PORT" PERF_ROOT="$E2E_ROOT"
python3 "$E2E_ROOT/scripts/e2e/wrangler-performance.py" | tee "$E2E_BASE/wrangler-performance.jsonl"
echo 'E2E-KV-PERFORMANCE: PASS' 
