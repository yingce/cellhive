#!/usr/bin/env bash
# Dual-agent HTTP acknowledgement, owner crash, and cold COS restore.
set -euo pipefail
source "$(dirname "$0")/lib.sh"
export CELLHIVE_COMPACTION_INTERVAL=1h
start_owner() { e2e_start_agent perf "$E2E_AGENT_PORT" "$E2E_ADMIN_PORT" '' "$E2E_BASE/agent"; }
start_peer() { e2e_start_agent peer "$((E2E_PORT_BASE+4))" "$((E2E_PORT_BASE+5))" '' "$E2E_BASE/peer"; }
start_owner
start_peer
e2e_cli app create e2ebatch >/dev/null
e2e_cli resource create e2ebatch kv KV >/dev/null
TOKEN=$("$E2E_BIN/kvbench" -ns e2ebatch -print-token)
request() { curl -fsS --max-time 30 -H "x-cellhive-scope-token: $TOKEN" "$@"; }
URL="http://127.0.0.1:$E2E_AGENT_PORT/v1/kv"
PEER="http://127.0.0.1:$((E2E_PORT_BASE+4))"
# Wait for actual follower acknowledgements, not just two ready processes.
ready=0
for i in $(seq 1 90); do
 request -X POST --data-binary warm "$URL/put?ns=e2ebatch&key=warm" >/dev/null
 if curl -fsS "http://127.0.0.1:$E2E_AGENT_PORT/metrics" | awk '/cellhive_commit_total\{mode="fleet"\}/ {if($NF>0) found=1} END {exit !found}'; then ready=1; break; fi
 sleep 1
done
[[ $ready = 1 ]] || e2e_fail 'no follower acknowledgement'
sleep 1
curl -fsS "http://127.0.0.1:$E2E_AGENT_PORT/metrics" > "$E2E_BASE/before.metrics"
for i in $(seq 1 100); do
 request -X POST --data-binary "value-$i" "$URL/put?ns=e2ebatch&key=k$i" >/dev/null
done
curl -fsS "http://127.0.0.1:$E2E_AGENT_PORT/metrics" > "$E2E_BASE/after.metrics"
# Kill immediately with asynchronous COS work potentially outstanding.
kill -KILL "$(cat "$E2E_BASE/perf.pid")"
rm "$E2E_BASE/perf.pid"
ready=0
for i in $(seq 1 90); do
 if [[ "$(request "$PEER/v1/kv/get?ns=e2ebatch&key=k100" 2>/dev/null || true)" = value-100 ]]; then ready=1; break; fi
 sleep 1
done
[[ $ready = 1 ]] || e2e_fail 'follower takeover failed'
for i in $(seq 1 100); do
 [[ "$(request "$PEER/v1/kv/get?ns=e2ebatch&key=k$i")" = "value-$i" ]] || e2e_fail "lost ack k$i"
done
echo 'owner SIGKILL: 100/100 acknowledged values recovered'
# Graceful flush then remove only this test peer's SQLite cache.
pid=$(cat "$E2E_BASE/peer.pid")
kill -TERM "$pid"
for i in $(seq 1 90); do kill -0 "$pid" 2>/dev/null || break; sleep 1; done
kill -0 "$pid" 2>/dev/null && e2e_fail 'peer did not drain'
rm -rf "$E2E_BASE/peer/cells"
start_peer
COLD_TRACE=$(openssl rand -hex 16)
echo "cold_get_trace=$COLD_TRACE"
request -H "traceparent: 00-$COLD_TRACE-$(openssl rand -hex 8)-01" -w '\ncold_get_seconds=%{time_total}\n' "$PEER/v1/kv/get?ns=e2ebatch&key=k100"
for i in $(seq 1 100); do
 [[ "$(request "$PEER/v1/kv/get?ns=e2ebatch&key=k$i")" = "value-$i" ]] || e2e_fail "cold lost k$i"
done
echo 'cold COS restore: 100/100 values verified'
python3 - "$E2E_BASE" <<'PY'
import sys,pathlib
p=pathlib.Path(sys.argv[1])
def read(name):
 return {line.split()[0]:float(line.split()[1]) for line in (p/name).read_text().splitlines() if line.startswith('cellhive_upload_')}
a,b=read('before.metrics'),read('after.metrics')
print('upload delta (last batch may still be pending at crash):', {k:b[k]-v for k,v in a.items() if k in b})
PY
echo 'E2E-KV-BATCH-RECOVERY: PASS'
