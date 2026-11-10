#!/bin/bash
set -euo pipefail
source "$(dirname "$0")/lib.sh"
# Accelerate only the disposable runtime's idle timer; production JS is unchanged.
cp -a "$E2E_ROOT/workerd" "$E2E_BASE/workerd"
python3 - "$E2E_BASE/workerd/do-runtime/host.js" <<'PYFIXTURE'
import pathlib,sys
p=pathlib.Path(sys.argv[1]);s=p.read_text()
for old,new in [('const FACET_IDLE_MS = 10 * 60 * 1000;', 'const FACET_IDLE_MS = 1000;'), ('const FACET_SWEEP_MS = 60 * 1000;', 'const FACET_SWEEP_MS = 100;')]:
    assert old in s, 'idle timer fixture no longer matches source'
    s=s.replace(old,new)
p.write_text(s)
PYFIXTURE
DO_A=$((E2E_PORT_BASE+6)); GATE_A=$((E2E_PORT_BASE+8))
export CELLHIVE_DO_RUNTIMES="http://127.0.0.1:$DO_A" CELLHIVE_DURABILITY=bucket
export CELLHIVE_COMPACTION_INTERVAL=1h
e2e_start_agent agent-a "$E2E_AGENT_PORT" "$E2E_ADMIN_PORT" '' "$E2E_BASE/agent-a"
e2e_cli app create e2epaging >/dev/null
SHA=$(e2e_upload "$E2E_ROOT/scripts/e2e/fixtures/do-paging-fleet.js")
printf '{"namespace":"e2epaging","worker":"paging","bundle_sha":"%s","bindings":[{"type":"do","name":"BENCH","id":"Bench"}]}' "$SHA" > "$E2E_BASE/deploy.json"
e2e_deploy "$E2E_BASE/deploy.json" > "$E2E_BASE/deployed.json"
start_do() { # name port gate-port state-directory
  local name=$1 port=$2 gate=$3 state=$4
  mkdir -p "$state"
  env -u COS_SECRET_ID -u COS_SECRET_KEY -u COS_ENDPOINT -u COS_BUCKET \
      -u AWS_ACCESS_KEY_ID -u AWS_SECRET_ACCESS_KEY -u AWS_SESSION_TOKEN \
      -u CELLHIVE_BUCKET -u CELLHIVE_BUCKET_DIR \
      CELLHIVE_CELL_URL="http://127.0.0.1:$E2E_AGENT_PORT" \
      CELLHIVE_DO_ADDR="127.0.0.1:$port" CELLHIVE_DO_NODE="e2epaging-$name" \
      CELLHIVE_DO_ADVERTISE="http://127.0.0.1:$port" \
      CELLHIVE_DATA_DIR="$state" CELLHIVE_RUNTIME_DIR="$state/render" \
      CELLHIVE_DO_RUNTIME_JS="$E2E_BASE/workerd/do-runtime" \
      CELLHIVE_DO_GATE_URL="http://127.0.0.1:$gate" \
      E2E_DO_GATE="$gate" E2E_DO_PORT="$port" E2E_DO_SCOPE="e2epaging/__do__" \
      setsid bash -c '
        cfg=$("$E2E_BIN/do-runtime" -render-only)
        exec "$E2E_BIN/do-supervisor" -workerd "$CELLHIVE_WORKERD" -config "$cfg" \
          -dir "$CELLHIVE_DATA_DIR/do" -scope-prefix "$E2E_DO_SCOPE" -listen "127.0.0.1:$E2E_DO_GATE" \
          -do-url "http://127.0.0.1:$E2E_DO_PORT" -owner "$CELLHIVE_CELL_URL"
      ' > "$E2E_BASE/$name.log" 2>&1 &
  echo $! > "$E2E_BASE/$name.pid"
  e2e_wait_http "http://127.0.0.1:$port/ready" 120 || e2e_fail "$name not ready"
}

invoke() {
 curl --fail-with-body -sS --max-time 15 -H "Authorization: Bearer $(e2e_token scope e2epaging do BENCH)" -H 'content-type: application/json' --data-binary "{\"namespace\":\"e2epaging\",\"worker\":\"paging\",\"class\":\"Bench\",\"id\":\"single\",\"request\":{\"path\":\"$1\"}}" "http://127.0.0.1:$E2E_AGENT_PORT/v1/do/invoke"
}
start_do rta "$DO_A" "$GATE_A" "$E2E_BASE/state-a"
e2e_need "$(invoke '/seed?rows=2')" seeded
kill -TERM "$(cat "$E2E_BASE/rta.pid")"
for i in $(seq 1 30); do kill -0 "$(cat "$E2E_BASE/rta.pid")" 2>/dev/null || break; sleep 1; done
start_do rtb "$DO_A" "$GATE_A" "$E2E_BASE/state-b"
# Exercise the lazy native constructor with simultaneous reads after recovery.
# Do not retry: a cached rejected constructor must fail the test visibly.
concurrent_read() {
  local token; token=$(e2e_token scope e2epaging do BENCH)
  E2E_READ_TOKEN="$token" E2E_READ_PORT="$E2E_AGENT_PORT" python3 - <<'PYREAD'
import concurrent.futures,json,os,urllib.request
body=json.dumps({"namespace":"e2epaging","worker":"paging","class":"Bench","id":"single","request":{"path":"/"}}).encode()
def read(_):
 req=urllib.request.Request('http://127.0.0.1:'+os.environ['E2E_READ_PORT']+'/v1/do/invoke',body,{'Authorization':'Bearer '+os.environ['E2E_READ_TOKEN'],'Content-Type':'application/json'})
 with urllib.request.urlopen(req,timeout=30) as r:return json.load(r)['v']
with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool: values=list(pool.map(read,range(16)))
assert len(set(values))==1,values
print('concurrent cold reads=16 value='+str(values[0])+' PASS',flush=True)
PYREAD
}
concurrent_read
if [[ ${E2E_OWNER_RECOVERY:-0} == 1 ]]; then
  start_do rtc "$((DO_A+1))" "$((GATE_A+1))" "$E2E_BASE/state-c"
  export E2E_OWNER_OTHER_RUNTIME="$((DO_A+1))" E2E_OWNER_OTHER_GATE="$((GATE_A+1))"
  export E2E_OWNER_DEPLOY="$E2E_BASE/deployed.json"
  E2E_OWNER_TOKEN="$(e2e_token internal)" E2E_OWNER_BINDING="$(e2e_token scope e2epaging do BENCH)" \
  E2E_OWNER_AGENT="$E2E_AGENT_PORT" E2E_OWNER_GATE="$GATE_A" E2E_OWNER_RUNTIME="$DO_A" \
  E2E_OWNER_PID="$(cat "$E2E_BASE/rtb.pid")" E2E_OWNER_NAMESPACE=e2epaging \
  python3 "$E2E_ROOT/scripts/e2e/do-owner-recovery.py"
fi

baseline=$(invoke /read | jq -r .v)
for i in $(seq 1 5); do
 expected=$((baseline+i))
 e2e_need "$(invoke /inc | jq -r .v)" "$expected"
 sleep 3
 concurrent_read
 e2e_need "$(invoke /read | jq -r .v)" "$expected"
 echo "idle-retire-reopen cycle=$i data=$expected PASS"
done
if grep -q 'capture activation epoch changed' "$E2E_BASE/rtb.log"; then e2e_fail 'epoch mismatch'; fi
echo "PASS retirement after owner change; artifacts=$E2E_BASE"
