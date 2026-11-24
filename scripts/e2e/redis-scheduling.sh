#!/usr/bin/env bash
# Real Redis/Kvrocks authority + shared index, cell-agent and native workerd.
# Always discard inherited production configuration. Only local disposable FS storage.
set -euo pipefail
if [[ ${CELLHIVE_SCHEDULING_ISOLATED:-} != 1 ]]; then
  exec env -i PATH="$PATH" HOME="$HOME" TMPDIR="${TMPDIR:-/tmp}" \
    E2E_BIN="${E2E_BIN:-$PWD/bin}" E2E_PORT_BASE="${E2E_PORT_BASE:-29400}" E2E_KEEP="${E2E_KEEP:-1}" \
    CELLHIVE_WORKERD="${CELLHIVE_WORKERD:-$PWD/deploy/workerd-fork/workerd}" \
    E2E_FAST_RECOVERY="${E2E_FAST_RECOVERY:-0}" E2E_SCHEDULE_OUTAGE="${E2E_SCHEDULE_OUTAGE:-0}" CELLHIVE_SCHEDULING_ISOLATED=1 bash "$0" "$@"
fi
backend=${1:-redis}
case "$backend" in redis|kvrocks) ;; *) echo 'usage: redis-scheduling.sh [redis|kvrocks]' >&2; exit 2;; esac
source "$(dirname "$0")/lib.sh"
# Fail early if the supplied fork cannot run the current native DO protocol.
marker=$("$CELLHIVE_WORKERD" cellhive-build-info)
for feature in do-paging/1 do-paging-fail-stop/1 do-paging-lifecycle/1; do
  grep -Fxq "$feature" <<<"$marker" || e2e_fail "workerd lacks $feature; set CELLHIVE_WORKERD to a compatible fork"
done
container="cellhive-scheduling-${backend}-$$"
cleanup() { e2e_cleanup; docker rm -f "$container" >/dev/null 2>&1 || true; }
trap cleanup EXIT
if [[ $backend = redis ]]; then
  docker run -d --name "$container" -p 127.0.0.1::6379 redis:7.2-alpine redis-server --appendonly yes --maxmemory-policy noeviction >/dev/null
  container_port=6379
else
  docker run -d --name "$container" -p 127.0.0.1::6666 apache/kvrocks:latest --bind 0.0.0.0 >/dev/null
  container_port=6666
fi
redis_port=$(docker port "$container" "$container_port/tcp" | sed 's/.*://')
for i in $(seq 1 100); do
  if python3 - "$redis_port" <<'PYPING'
import socket, sys
try:
    with socket.create_connection(('127.0.0.1', int(sys.argv[1])), timeout=1) as sock:
        sock.sendall(b'*1\r\n$4\r\nPING\r\n')
        assert sock.recv(128).startswith(b'+PONG')
except Exception:
    sys.exit(1)
PYPING
  then break; fi
  sleep .2
done
export CELLHIVE_COORDINATION_BACKEND="redis://127.0.0.1:$redis_port"
if [[ ${E2E_FAST_RECOVERY:-0} == 1 ]]; then
  # Make a slow discovery pass unable to satisfy the fast recovery assertion.
  export CELLHIVE_WAKER_INTERVAL=10m CELLHIVE_WAKE_REPAIR_INTERVAL=10m
fi
export CELLHIVE_DURABILITY=bucket CELLHIVE_DO_RUNTIMES="http://127.0.0.1:$((E2E_PORT_BASE+6))"
mkdir -p "$E2E_BASE/bucket"
CELLHIVE_BUCKET_DIR="$E2E_BASE/bucket" CELLHIVE_DATA_DIR="$E2E_BASE/init" "$E2E_BIN/cell-agent" coordination-init --offline >"$E2E_BASE/init.log" 2>&1
e2e_log "backend=$backend base=$E2E_BASE redis=$redis_port"
e2e_start_agent scheduling "$E2E_AGENT_PORT" "$E2E_ADMIN_PORT" "http://127.0.0.1:$E2E_RT_INTERNAL" "$E2E_BASE/agent"
e2e_start_runtime rt "http://127.0.0.1:$E2E_AGENT_PORT" "$E2E_RT_PORT" "$E2E_RT_INTERNAL"
mkdir -p "$E2E_BASE/do"
env CELLHIVE_CELL_URL="http://127.0.0.1:$E2E_AGENT_PORT" \
  CELLHIVE_DO_ADDR="127.0.0.1:$((E2E_PORT_BASE+6))" CELLHIVE_DO_NODE=scheduling-do \
  CELLHIVE_DO_ADVERTISE="$CELLHIVE_DO_RUNTIMES" CELLHIVE_LOG_DIR="$E2E_BASE/do/logs" \
  CELLHIVE_DATA_DIR="$E2E_BASE/do" CELLHIVE_RUNTIME_DIR="$E2E_BASE/do/render" \
  CELLHIVE_DO_RUNTIME_JS="$E2E_ROOT/workerd/do-runtime" CELLHIVE_DO_GATE_URL="http://127.0.0.1:$((E2E_PORT_BASE+8))" \
  setsid "$E2E_BIN/do-runtime" -scope-prefix sched/__do__ -listen "127.0.0.1:$((E2E_PORT_BASE+8))" >"$E2E_BASE/do.log" 2>&1 &
echo $! >"$E2E_BASE/rt-do.pid"
e2e_wait_http "$CELLHIVE_DO_RUNTIMES/ready" 120 || e2e_fail 'DO runtime not ready'
e2e_cli app create sched >/dev/null
e2e_cli domain add sched sched.test >/dev/null
e2e_cli resource create sched kv KV >/dev/null
e2e_cli queue create sched q >/dev/null
sha=$(e2e_upload "$E2E_ROOT/scripts/e2e/fixtures/redis-scheduling.js")
cat >"$E2E_BASE/deploy.json" <<JSON
{"namespace":"sched","worker":"worker","bundle_sha":"$sha","bindings":[{"type":"kv","name":"KV","id":"KV"},{"type":"queue","name":"QUEUE","id":"q"},{"type":"do","name":"ALARM","id":"AlarmProbe"}],"crons":["* * * * *"],"consumers":[{"queue":"q","max_batch_size":1,"max_batch_timeout_seconds":1,"max_concurrency":1,"max_retries":3,"retry_delay":1}]}
JSON
e2e_deploy "$E2E_BASE/deploy.json" >"$E2E_BASE/deployed.json"
e2e_cli route add sched sched.test worker >/dev/null
call() { e2e_http "$E2E_RT_PORT" "$1" sched.test; }
invoke() {
  curl --fail-with-body -sS --max-time 15 -H "Authorization: Bearer $(e2e_token scope sched do ALARM)" \
    -H 'content-type: application/json' --data-binary "{\"namespace\":\"sched\",\"worker\":\"worker\",\"class\":\"AlarmProbe\",\"id\":\"$1\",\"request\":{\"path\":\"$2\"}}" "http://127.0.0.1:$E2E_AGENT_PORT/v1/do/invoke"
}
wait_alarm() {
  local id=$1 attempts=$2 completed=$3 polls=${4:-90} out
  for i in $(seq 1 "$polls"); do
    out=$(invoke "$id" /status)
    if jq -e --argjson a "$attempts" --argjson c "$completed" '.attempts == $a and .completed == $c' <<<"$out" >/dev/null; then echo "$out"; return; fi
    sleep 1
  done
  e2e_fail "alarm $id: $out"
}
e2e_need "$(call /send)" sent
e2e_need "$(call '/send?fail=1')" sent
for i in $(seq 1 60); do
  status=$(call /status)
  if jq -e '.queue.attempts == 1 and .queueRetry.attempts == 2' <<<"$status" >/dev/null; then break; fi
  sleep 1
done
jq -e '.queue.attempts == 1 and .queueRetry.attempts == 2' <<<"$status" >/dev/null || e2e_fail "queue: $status"
e2e_log 'Queue consumer + explicit retry PASS'
invoke normal '/arm?delay=1500' >/dev/null
wait_alarm normal 1 1
invoke postpone '/arm?delay=1500' >/dev/null
invoke postpone '/arm?delay=6000' >/dev/null
sleep 3
e2e_need "$(invoke postpone /status | jq .attempts)" 0
wait_alarm postpone 1 1
invoke cancel '/arm?delay=3000' >/dev/null
invoke cancel /cancel >/dev/null
sleep 5
e2e_need "$(invoke cancel /status | jq .attempts)" 0
invoke retry '/arm?delay=1500&fail=1' >/dev/null
wait_alarm retry 2 1
e2e_log 'DO Alarm set/reschedule/cancel/failure retry PASS'
for i in $(seq 1 90); do
  status=$(call /status)
  if jq -e '.cron.cron == "* * * * *" and .cron.scheduledTime > 0' <<<"$status" >/dev/null; then break; fi
  sleep 1
done
jq -e '.cron.cron == "* * * * *" and .cron.scheduledTime > 0' <<<"$status" >/dev/null || e2e_fail "natural Cron: $status"
printf '%s\n' "$status" | tee "$E2E_BASE/results.json"
e2e_log "Natural Cron scheduled() PASS"

if [[ ${E2E_SCHEDULE_OUTAGE:-0} == 1 ]]; then
  # Acknowledged delayed work survives backend unavailability plus agent crash.
  # Persisted Redis/Kvrocks state is retained; this is not an empty-DB rebuild.
  invoke outage '/arm?delay=10000' >/dev/null
  e2e_need "$(call '/send?key=outage&delay=10')" sent
  docker stop -t 3 "$container" >/dev/null
  kill -KILL "$(cat "$E2E_BASE/scheduling.pid")"
  sleep 12
  cp -a "$E2E_BASE/agent" "$E2E_BASE/agent-after-crash"
  docker start "$container" >/dev/null
  # Docker can reassign an ephemeral published port on restart.
  redis_port=$(docker port "$container" "$container_port/tcp" | sed 's/.*://')
  export CELLHIVE_COORDINATION_BACKEND="redis://127.0.0.1:$redis_port"
  ready=0
  for i in $(seq 1 60); do
    if python3 - "$redis_port" <<'PYREADY'
import socket, sys
try:
    with socket.create_connection(('127.0.0.1', int(sys.argv[1])), timeout=1) as sock:
        sock.sendall(b'*1\r\n$4\r\nPING\r\n')
        assert sock.recv(128).startswith(b'+PONG')
except Exception:
    sys.exit(1)
PYREADY
    then ready=1; break; fi
    sleep .2
  done
  [[ $ready = 1 ]] || e2e_fail 'backend did not recover'
  mv "$E2E_BASE/scheduling.log" "$E2E_BASE/scheduling-before-crash.log"
  recovery_start=$SECONDS
  e2e_start_agent scheduling "$E2E_AGENT_PORT" "$E2E_ADMIN_PORT" "http://127.0.0.1:$E2E_RT_INTERNAL" "$E2E_BASE/agent"
  polls=240
  [[ ${E2E_FAST_RECOVERY:-0} != 1 ]] || polls=15
  wait_alarm outage 1 1 "$polls"
  for i in $(seq 1 90); do
    status=$(call /status)
    if jq -e '.outage.attempts >= 1' <<<"$status" >/dev/null; then break; fi
    sleep 1
  done
  jq -e '.outage.attempts >= 1' <<<"$status" >/dev/null || e2e_fail "outage delayed Queue: $status"
  recovery_seconds=$((SECONDS-recovery_start))
  if [[ ${E2E_FAST_RECOVERY:-0} == 1 ]]; then
    [[ $recovery_seconds -le 20 ]] || e2e_fail "fast recovery exceeded 20s: $recovery_seconds"
  fi
  printf '{"backend":"%s","recovery_seconds":%d,"fast":%s}\n' "$backend" "$recovery_seconds" "${E2E_FAST_RECOVERY:-0}" | tee "$E2E_BASE/recovery-results.json"
  e2e_log "Backend stop/start + agent crash: delayed Queue and Alarm recovered PASS"
fi

e2e_log "E2E REDIS SCHEDULING $backend PASS"
