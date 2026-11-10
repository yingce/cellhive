#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/lib.sh"
e2e_start_agent perf "$E2E_AGENT_PORT" "$E2E_ADMIN_PORT" "http://127.0.0.1:$E2E_RT_INTERNAL" "$E2E_BASE/agent"
if [[ "${E2E_DUAL_AGENT:-0}" = 1 ]]; then
  e2e_start_agent peer "$((E2E_PORT_BASE+4))" "$((E2E_PORT_BASE+5))" "http://127.0.0.1:$E2E_RT_INTERNAL" "$E2E_BASE/peer"
  export PERF_PEER="http://127.0.0.1:$((E2E_PORT_BASE+4))"
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
node "$E2E_ROOT/scripts/e2e/kv-performance.cjs" | tee "$E2E_BASE/performance.jsonl"
# Snapshot resource use and logs before cleanup.
python3 - "$E2E_BASE" <<'PYRES'
import json,sys,subprocess
from pathlib import Path
root=Path(sys.argv[1])
ids={int(p.read_text()) for p in root.glob('*.pid') if p.read_text().strip().isdigit()}
rows=[line.split(None,4) for line in subprocess.check_output(['ps','-e','-o','pid=,ppid=,rss=,pcpu=,comm='],text=True).splitlines()]
while True:
 children={int(row[0]) for row in rows if int(row[1]) in ids}
 if children<=ids:break
 ids|=children
for pid,parent,rss,cpu,name in rows:
 if int(pid) not in ids:continue
 peak=None
 try:
  for line in Path('/proc/'+pid+'/status').read_text().splitlines():
   if line.startswith('VmHWM:'):peak=int(line.split()[1])
 except FileNotFoundError:pass
 print(json.dumps({'event':'process-resource','pid':int(pid),'parent':int(parent),'name':name,'rss_kib':int(rss),'peak_rss_kib':peak,'lifetime_cpu_percent':float(cpu)}))
PYRES
curl -fsS -H "Authorization: Bearer $(e2e_token internal)" "$PERF_AGENT/metrics" > "$E2E_BASE/metrics.txt" || true
if [[ -n "${PERF_PEER:-}" ]]; then curl -fsS -H "Authorization: Bearer $(e2e_token internal)" "$PERF_PEER/metrics" > "$E2E_BASE/peer-metrics.txt"; fi
export PERF_RUN_DIR="$E2E_BASE" PERF_ADMIN="http://127.0.0.1:$E2E_ADMIN_PORT" PERF_ROOT="$E2E_ROOT"
python3 "$E2E_ROOT/scripts/e2e/wrangler-performance.py" | tee "$E2E_BASE/wrangler-performance.jsonl"
echo 'E2E-KV-PERFORMANCE: PASS' 
