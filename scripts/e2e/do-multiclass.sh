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
SHA=$(e2e_upload "$E2E_ROOT/scripts/e2e/fixtures/do-multiclass.js")
printf '{"namespace":"e2epaging","worker":"paging","bundle_sha":"%s","bindings":[{"type":"do","name":"BENCH","id":"CounterA"},{"type":"do","name":"OTHER","id":"CounterQ"}]}' "$SHA" > "$E2E_BASE/deploy.json"
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
      CELLHIVE_LOG_DIR="$state/logs" CELLHIVE_DATA_DIR="$state" CELLHIVE_RUNTIME_DIR="$state/render" \
      CELLHIVE_DO_RUNTIME_JS="$E2E_BASE/workerd/do-runtime" \
      CELLHIVE_DO_GATE_URL="http://127.0.0.1:$gate" \
      E2E_DO_GATE="$gate" E2E_DO_PORT="$port" E2E_DO_SCOPE="e2epaging/__do__" \
      setsid bash -c '

        exec "$E2E_BIN/do-runtime" \
          -scope-prefix "$E2E_DO_SCOPE" -listen "127.0.0.1:$E2E_DO_GATE"
      ' > "$E2E_BASE/$name.log" 2>&1 &
  echo $! > "$E2E_BASE/$name.pid"
  e2e_wait_http "http://127.0.0.1:$port/ready" 120 || e2e_fail "$name not ready"
}

invoke() {
 curl --fail-with-body -sS --max-time 15 -H "Authorization: Bearer $(e2e_token scope e2epaging do "$1")" -H 'content-type: application/json' --data-binary "{\"namespace\":\"e2epaging\",\"worker\":\"paging\",\"class\":\"$2\",\"id\":\"single\",\"request\":{\"path\":\"$3\"}}" "http://127.0.0.1:$E2E_AGENT_PORT/v1/do/invoke"
}
stop_do() {
 kill -TERM "$(cat "$E2E_BASE/$1.pid")"
 for i in $(seq 1 30); do kill -0 "$(cat "$E2E_BASE/$1.pid")" 2>/dev/null || return 0; sleep 1; done
 e2e_fail 'runtime failed to drain'
}
start_do rta "$DO_A" "$GATE_A" "$E2E_BASE/state-a"
e2e_need "$(invoke BENCH CounterA '/seed?value=11' | jq -r .v)" 11
stop_do rta
start_do rtb "$DO_A" "$GATE_A" "$E2E_BASE/state-b"
e2e_need "$(invoke BENCH CounterA /read | jq -r .v)" 11
e2e_need "$(invoke OTHER CounterQ '/seed?value=22' | jq -r .v)" 22
# One class has already changed owner; the other starts at its first epoch.
python3 - "$E2E_BASE/bucket" <<'PYOWN'
import pathlib,json,sys
root=pathlib.Path(sys.argv[1]);owners=[]
for cls in ['CounterA','CounterQ']:
 files=list((root/'cells/e2epaging/__do__').glob('paging~'+cls+'~shard*/own.json'));assert len(files)==1,files
 d=json.load(open(files[0]));owners.append((files[0].parent.name.rsplit('~',1)[1],d['epoch']))
assert owners[0][0]==owners[1][0],owners
assert owners[0][1]>owners[1][1],owners
print('same shard, independent epochs:',owners)
PYOWN
stop_do rtb
start_do rtc "$DO_A" "$GATE_A" "$E2E_BASE/state-c"
for i in $(seq 1 3); do
 e2e_need "$(invoke BENCH CounterA /inc | jq -r .v)" "$((11+i))"
 e2e_need "$(invoke OTHER CounterQ /inc | jq -r .v)" "$((22+i))"
 sleep 3
 e2e_need "$(invoke BENCH CounterA /read | jq -r .v)" "$((11+i))"
 e2e_need "$(invoke OTHER CounterQ /read | jq -r .v)" "$((22+i))"
 echo "multi-class cold/idle cycle=$i PASS"
done
python3 - "$E2E_BASE/bucket/dosupervisor" <<'PYMANIFEST'
import pathlib,json,sys
hosts={}
for p in pathlib.Path(sys.argv[1]).glob('*/hosts/*.json'):
 d=json.load(open(p));classes={o['class'] for o in d.get('objects',[])}
 assert len(classes)==1,(p,classes)
 for h in d['hosts']:
  assert '/'+next(iter(classes))+'/shard' in h,h
  hosts[h]=classes
assert len(hosts)==2,hosts
print('class-isolated host manifests:',len(hosts))
PYMANIFEST
if grep -q 'capture activation epoch changed' "$E2E_BASE/rtc.log"; then e2e_fail 'epoch mismatch'; fi
echo "PASS multi-class independent epochs, empty-cache recovery and retirement; artifacts=$E2E_BASE"
