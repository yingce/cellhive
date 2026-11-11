#!/usr/bin/env bash
# Two real agents + two supervised native runtimes; no owner stub or manual SyncAll.
set -euo pipefail
source "$(dirname "$0")/lib.sh"
for binary in do-runtime; do
  [[ -x "$E2E_BIN/$binary" ]] || e2e_fail "missing $binary; build current worktree first"
done
"$CELLHIVE_WORKERD" cellhive-build-info | grep -qx 'do-paging/1' || e2e_fail 'paging fork required'
"$CELLHIVE_WORKERD" cellhive-build-info | grep -qx 'do-paging-lifecycle/1' || e2e_fail 'paging lifecycle fork required'
DO_A=$((E2E_PORT_BASE+6)); DO_B=$((E2E_PORT_BASE+7))
GATE_A=$((E2E_PORT_BASE+8)); GATE_B=$((E2E_PORT_BASE+9))
export CELLHIVE_DO_RUNTIMES="http://127.0.0.1:$DO_A,http://127.0.0.1:$DO_B"
export CELLHIVE_DURABILITY=auto
export CELLHIVE_COMPACTION_INTERVAL=1h
ROWS=${E2E_PAGING_ROWS:-256}
[[ "$ROWS" =~ ^[0-9]+$ ]] && ((ROWS >= 256 && ROWS <= 16384)) || e2e_fail "invalid row count"

e2e_start_agent agent-a "$E2E_AGENT_PORT" "$E2E_ADMIN_PORT" '' "$E2E_BASE/agent-a"
e2e_start_agent agent-b "$((E2E_PORT_BASE+4))" "$((E2E_PORT_BASE+5))" '' "$E2E_BASE/agent-b"
e2e_cli app create e2epaging >/dev/null
SHA=$(e2e_upload "$E2E_ROOT/scripts/e2e/fixtures/do-paging-fleet.js")
cat > "$E2E_BASE/deploy.json" <<JSON
{"namespace":"e2epaging","worker":"paging","bundle_sha":"$SHA","compatibility_date":"2026-09-01","bindings":[{"type":"do","name":"BENCH","id":"Bench"}]}
JSON
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
      CELLHIVE_DO_RUNTIME_JS="$E2E_ROOT/workerd/do-runtime" \
      CELLHIVE_DO_GATE_URL="http://127.0.0.1:$gate" \
      E2E_DO_GATE="$gate" E2E_DO_PORT="$port" E2E_DO_SCOPE="e2epaging/__do__" \
      setsid bash -c '

        exec "$E2E_BIN/do-runtime" \
          -scope-prefix "$E2E_DO_SCOPE" -listen "127.0.0.1:$E2E_DO_GATE"
      ' > "$E2E_BASE/$name.log" 2>&1 &
  echo $! > "$E2E_BASE/$name.pid"
  e2e_wait_http "http://127.0.0.1:$port/ready" 120 || e2e_fail "$name not ready"
}
invoke() { # path; never retry mutations (no idempotency assumed).
  curl --fail-with-body -sS --max-time "${E2E_PAGING_REQUEST_TIMEOUT:-60}" -H "Authorization: Bearer $(e2e_token scope e2epaging do BENCH)" \
    -H 'content-type: application/json' \
    --data-binary "{\"namespace\":\"e2epaging\",\"worker\":\"paging\",\"class\":\"Bench\",\"id\":\"single\",\"request\":{\"path\":\"$1\"}}" \
    "http://127.0.0.1:$E2E_AGENT_PORT/v1/do/invoke"
}
owner_record() {
  local shard scope record
  for shard in $(seq 0 15); do
    scope="e2epaging/__do__/paging~Bench~shard$shard"
    record=$(curl -fsS --max-time 10 -G -H "Authorization: Bearer $(e2e_token internal)" \
      --data-urlencode "scope=$scope" "http://127.0.0.1:$E2E_AGENT_PORT/v1/internal/resolve")
    if [[ $(jq -r .owned <<<"$record") == true ]]; then
      jq --arg scope "$scope" '. + {scope:$scope}' <<<"$record"
      return
    fi
  done
  e2e_fail 'no real DO owner record'
}
crash_do() {
  local pid; pid=$(cat "$E2E_BASE/$1.pid")
  if [[ "${E2E_PAGING_CRASH_CHILD_ONLY:-0}" == 1 ]]; then
    local child
    mapfile -t children < <(pgrep -P "$pid")
    [[ ${#children[@]} == 1 ]] || e2e_fail 'expected exactly one supervised workerd child'
    child=${children[0]}
    [[ $(readlink "/proc/$child/exe") == $(readlink -f "$CELLHIVE_WORKERD") ]] || e2e_fail 'refusing to kill an unexpected child'
    echo 'crash_mode=workerd-only'
    kill -KILL "$child"
  else
    kill -KILL -- "-$pid"
  fi
  wait "$pid" 2>/dev/null || true
  rm "$E2E_BASE/$1.pid"
}
# Fault only this test runtime's sparse local backing path. The native VFS must
# fail-stop on the next missing page, and the actual supervisor must stop renewal.
failstop_do() {
  local name=$1 state=$2 pid child status=0 started=$SECONDS
  pid=$(cat "$E2E_BASE/$name.pid")
  mapfile -t children < <(pgrep -P "$pid")
  [[ ${#children[@]} == 1 ]] || e2e_fail 'expected one supervised child before paging fault'
  child=${children[0]}
  [[ $(readlink "/proc/$child/exe") == $(readlink -f "$CELLHIVE_WORKERD") ]] || e2e_fail 'unexpected paging fault child'
  python3 - "$state" <<'PYFAULT'
import pathlib,sys
root=(pathlib.Path(sys.argv[1])/'do').resolve()
files=[p for p in root.rglob('*.sqlite') if p.is_file() and p.stat().st_size>=16*(1<<20)]
assert len(files)==1, files
path=files[0]
assert path.resolve().is_relative_to(root), path
saved=path.with_suffix('.sqlite.paging-fault')
assert not saved.exists(), saved
path.rename(saved)
print('paging_fault=missing-local-backing-file')
PYFAULT
  invoke /proof > "$E2E_BASE/fault-response" 2> "$E2E_BASE/fault-request.log" || status=$?
  (( status != 0 )) || e2e_fail 'paging fault request unexpectedly succeeded'
  while kill -0 "$child" 2>/dev/null && ((SECONDS-started < 45)); do sleep .2; done
  ! kill -0 "$child" 2>/dev/null || e2e_fail 'poisoned workerd child remained alive'
  while kill -0 "$pid" 2>/dev/null && ((SECONDS-started < 50)); do sleep .2; done
  ! kill -0 "$pid" 2>/dev/null || e2e_fail 'supervisor did not exit after poisoned child'
  wait "$pid" 2>/dev/null || true
  grep -q 'workerd exited: .*exit status 74' "$E2E_BASE/$name.log" || e2e_fail 'native fail-stop exit74 not observed'
  grep -q 'retaining ownership until lease expiry' "$E2E_BASE/$name.log" || e2e_fail 'dead child ownership was not retained for expiry'
  rm "$E2E_BASE/$name.pid"
  echo 'paging_failstop=exit74 renewal_stopped=supervisor_exited ownership=lease-expiry'
}
wait_read() {
  local value=$1 result start=$SECONDS
  while (( SECONDS-start < 90 )); do
    result=$(invoke /get 2>/dev/null || true)
    if [[ $(jq -r '.v // -1' <<<"$result" 2>/dev/null) == "$value" ]]; then
      echo "takeover_read_seconds=$((SECONDS-start)) value=$value"
      return
    fi
    sleep 1
  done
  e2e_fail "takeover read never returned acknowledged value $value"
}
assert_sparse() {
  python3 - "$1" <<'PY'
import pathlib,sys
root=pathlib.Path(sys.argv[1])/'do'
assert (root/'.cellhive-paging').is_dir(), 'paging registry absent'
files=[p for p in root.rglob('*.sqlite') if p.is_file()]
large=[p.stat() for p in files if p.stat().st_size >= 16*(1<<20)]
assert large, '16MiB restored database missing'
logical=sum(s.st_size for s in large); allocated=sum(s.st_blocks*512 for s in large)
print(f'cold_sqlite_logical_bytes={logical} allocated_bytes={allocated}')
assert allocated < logical//2, 'cold scalar read materialized most of database'
PY
}
fleet_commits() {
  local port
  for port in "$E2E_AGENT_PORT" "$((E2E_PORT_BASE+4))"; do
    curl -fsS --max-time 10 -H "Authorization: Bearer $(e2e_token internal)" "http://127.0.0.1:$port/metrics"
  done | awk '/cellhive_commit_total\{mode="fleet"\}/ {n+=$NF} END {printf "%.0f\n",n}'
}
verify_proof() {
  local label=$1 started ended
  started=$(date +%s%N)
  if [[ "${E2E_PAGING_DIRECT_PROOF:-0}" == 1 ]]; then
    # Long native scans deliberately exceed the agent proxy's 30s request
    # deadline. Ownership, capture and durable gate still use real cell-agents.
    local port=$DO_A
    [[ "$label" == b ]] && port=$DO_B
    jq '{namespace:"e2epaging",worker:"paging",class:"Bench",storage_class:"Bench",id:"single",storage_id:.storage_id,bundle_sha:.bundle_sha,version:.number,request:{path:"/proof"}}' \
      "$E2E_BASE/deployed.json" > "$E2E_BASE/direct-proof.json"
    curl --fail-with-body -sS --max-time "${E2E_PAGING_REQUEST_TIMEOUT:-60}" \
      -H "Authorization: Bearer $(e2e_token internal)" -H 'content-type: application/json' \
      --data-binary "@$E2E_BASE/direct-proof.json" "http://127.0.0.1:$port/v1/do/invoke" > "$E2E_BASE/proof-$label"
    echo 'proof_transport=runtime-direct'
  else
    invoke /proof > "$E2E_BASE/proof-$label"
  fi
  ended=$(date +%s%N)
  cmp "$E2E_BASE/expected-proof" "$E2E_BASE/proof-$label"
  python3 - "$label" "$started" "$ended" "${E2E_PAGING_MIN_PROOF_SECONDS:-0}" <<'PYPROOF'
import sys
label,start,end,minimum=sys.argv[1:]
elapsed=(int(end)-int(start))/1e9
print(f'proof_node={label} seconds={elapsed:.3f}')
assert elapsed>=float(minimum), 'proof did not exercise requested lease-duration boundary'
PYPROOF
}
# HTTP readiness precedes remote-node discovery. Prove a follower can ACK
# before measuring the DO workload; probe commits are outside its baseline.
e2e_cli resource create e2epaging kv KV >/dev/null
FLEET_PROBE_BEFORE=$(fleet_commits)
FLEET_READY=0
for attempt in $(seq 1 90); do
  curl --fail-with-body -sS --max-time 30 -X POST --data-binary ready \
    -H "Authorization: Bearer $(e2e_token scope e2epaging kv KV)" \
    "http://127.0.0.1:$E2E_AGENT_PORT/v1/kv/put?ns=e2epaging&key=fleet-ready" >/dev/null
  if (( $(fleet_commits) > FLEET_PROBE_BEFORE )); then FLEET_READY=1; break; fi
  sleep 1
done
[[ "$FLEET_READY" = 1 ]] || e2e_fail 'no follower acknowledgement before DO workload'
echo 'fleet_ready=1'
FLEET_BEFORE=$(fleet_commits)
start_do rta "$DO_A" "$GATE_A" "$E2E_BASE/state-a"
e2e_need "$(invoke "/seed?rows=$ROWS")" seeded 'initial seed failed'
invoke /proof > "$E2E_BASE/expected-proof"
[[ $(awk 'END {print NR}' "$E2E_BASE/expected-proof") == "$ROWS" ]] || e2e_fail 'incomplete source proof'
for i in $(seq 1 10); do e2e_need "$(invoke /inc | jq -r .v)" "$i" 'acknowledged counter mismatch'; done
# Require new fleet commits during this workload, not historical control traffic.
FLEET_AFTER=$(fleet_commits)
(( FLEET_AFTER > FLEET_BEFORE )) || e2e_fail 'no new fleet commits during DO workload'
echo "fleet_commit_delta=$((FLEET_AFTER-FLEET_BEFORE))"
curl -fsS --max-time 10 "http://127.0.0.1:$GATE_A/metrics" > "$E2E_BASE/gate-a.metrics"
awk '/^cellhive_do_wal_captured_bytes_total / {if($NF>0) found=1} END {exit !found}' "$E2E_BASE/gate-a.metrics" || e2e_fail 'DO output gate captured no data'
owner_record > "$E2E_BASE/owner-1.json"
start_do rtb "$DO_B" "$GATE_B" "$E2E_BASE/state-b"
crash_do rta
wait_read 10
assert_sparse "$E2E_BASE/state-b"
owner_record > "$E2E_BASE/owner-2.json"
if [[ "${E2E_PAGING_FAILSTOP:-0}" != 1 ]]; then verify_proof b; fi
e2e_need "$(invoke /inc | jq -r .v)" 11 'post-takeover write failed'
start_do rta "$DO_A" "$GATE_A" "$E2E_BASE/state-a-fresh"
if [[ "${E2E_PAGING_FAILSTOP:-0}" == 1 ]]; then
  "$CELLHIVE_WORKERD" cellhive-build-info | grep -qx 'do-paging-fail-stop/1' || e2e_fail 'fail-stop fork required'
  assert_sparse "$E2E_BASE/state-b"
  failstop_do rtb "$E2E_BASE/state-b"
else
  crash_do rtb
fi
wait_read 11
assert_sparse "$E2E_BASE/state-a-fresh"
owner_record > "$E2E_BASE/owner-3.json"
verify_proof a
e2e_need "$(invoke /inc | jq -r .v)" 12 'second takeover write failed'
python3 - "$E2E_BASE" <<'PY'
import json,pathlib,sys
root=pathlib.Path(sys.argv[1]); records=[json.loads((root/f'owner-{i}.json').read_text()) for i in (1,2,3)]
assert len({r['scope'] for r in records})==1,records
epochs=[r['owner']['epoch'] for r in records]; nodes=[r['owner']['node'] for r in records]
assert epochs[0]<epochs[1]<epochs[2],epochs
assert nodes[0]!=nodes[1] and nodes[1]!=nodes[2],nodes
print('owner epochs=',epochs,'nodes=',nodes)
PY
echo 'E2E-DO-PAGING-FLEET: PASS'
