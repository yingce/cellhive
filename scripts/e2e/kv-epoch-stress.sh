#!/usr/bin/env bash
# Large-cell multi-epoch stress: 100k acknowledged KV writes while the owner is
# SIGKILLed and ownership moves A -> B -> A -> B. Real HK COS is selected by the
# hk-review harness (CELLHIVE_BUCKET=cos://...). The final phase drains the
# survivor, removes its local SQLite cache, and verifies every acknowledged key
# after a cold COS restore.
#
# Knobs: E2E_STRESS_KEYS (default 100000), E2E_STRESS_VALUE_BYTES (96),
# E2E_STRESS_VERIFY_WORKERS (32).
set -euo pipefail
source "$(dirname "$0")/lib.sh"

export CELLHIVE_COMPACTION_INTERVAL=${CELLHIVE_COMPACTION_INTERVAL:-3s}
export CELLHIVE_COMPACTION_MIN_SEGMENTS=${CELLHIVE_COMPACTION_MIN_SEGMENTS:-16}
export CELLHIVE_COMPACTION_MIN_BYTES=${CELLHIVE_COMPACTION_MIN_BYTES:-2097152}
export CELLHIVE_COMPACTION_MIN_OBJECTS=${CELLHIVE_COMPACTION_MIN_OBJECTS:-8}
export CELLHIVE_EPOCH_GC_INTERVAL=${CELLHIVE_EPOCH_GC_INTERVAL:-1h}

TOTAL=${E2E_STRESS_KEYS:-100000}
VALUE_BYTES=${E2E_STRESS_VALUE_BYTES:-96}
VERIFY_WORKERS=${E2E_STRESS_VERIFY_WORKERS:-32}
NS=e2ebatch
RES=kv
CELL_ID=KV
# The suite runs longer than the default 300s scope-token TTL.
export E2E_TOKEN_TTL=${E2E_TOKEN_TTL:-3600}
A="http://127.0.0.1:$E2E_AGENT_PORT"
B="http://127.0.0.1:$((E2E_PORT_BASE+4))"
ACTIVE="$E2E_BASE/active-url"
LEDGER="$E2E_BASE/ledger.tsv"
WRITER_LOG="$E2E_BASE/writer.log"
WRITER_PID=""
: >"$LEDGER"
printf '%s\n' "$A" >"$ACTIVE"
TOKEN=$(e2e_token scope "$NS" "$RES" KV)
export TOKEN A B ACTIVE LEDGER NS TOTAL VALUE_BYTES WRITER_LOG

start_owner() { e2e_start_agent perf "$E2E_AGENT_PORT" "$E2E_ADMIN_PORT" '' "$E2E_BASE/agent"; }
start_peer()  { e2e_start_agent peer "$((E2E_PORT_BASE+4))" "$((E2E_PORT_BASE+5))" '' "$E2E_BASE/peer"; }
stop_writer() {
  [[ -n "$WRITER_PID" ]] || return 0
  kill "$WRITER_PID" 2>/dev/null || true
  wait "$WRITER_PID" 2>/dev/null || true
  WRITER_PID=""
}
cleanup_all() { stop_writer; e2e_cleanup; }
trap cleanup_all EXIT

put_one() { # key value
  curl -fsS --max-time 30 -H "Authorization: Bearer $TOKEN" \
    -X POST --data-binary "$2" "$1/v1/kv/put?ns=$NS&key=$2" >/dev/null 2>&1 || return 1
}

start_owner
start_peer
e2e_cli app create "$NS" >/dev/null
e2e_cli resource create "$NS" "$RES" "$CELL_ID" >/dev/null

echo "CELL_SCOPE=$NS/__kv__/$CELL_ID"
echo "BUCKET_PREFIX=cells/$NS/__kv__/$CELL_ID/"
echo "COS_BUCKET=cos://cellhive-e2e-hk-1376795072"
echo "WRITE_TOTAL=$TOTAL"
echo "SAFE_GC=${E2E_STRESS_SAFE_GC:-0}"

# Fleet acknowledgement: do not start the measured write phase until a follower
# actually commits in fleet mode.
ready=0
for i in $(seq 1 120); do
  put_one "$A" warm || true
  if curl -fsS -H "Authorization: Bearer $(e2e_token internal)" "$A/metrics" |
     awk '/cellhive_commit_total\{mode="fleet"\}/ {if($NF>0) found=1} END {exit !found}'; then
    ready=1
    break
  fi
  sleep 1
done
[[ $ready = 1 ]] || e2e_fail 'no follower acknowledgement'

python3 - "$WRITER_LOG" <<'PY' &
import http.client, os, sys, time
from urllib.parse import urlsplit

log = open(sys.argv[1], "a", buffering=1)
token = os.environ["TOKEN"]
ns = os.environ["NS"]
total = int(os.environ["TOTAL"])
value_bytes = int(os.environ["VALUE_BYTES"])
active_file = os.environ["ACTIVE"]
ledger_path = os.environ["LEDGER"]
endpoints = [os.environ["A"], os.environ["B"]]
conns = {}

def endpoint_url():
    try:
        with open(active_file) as f:
            return f.read().strip() or endpoints[0]
    except OSError:
        return endpoints[0]

def conn(url):
    u = urlsplit(url)
    key = (u.hostname, u.port)
    if key not in conns:
        conns[key] = http.client.HTTPConnection(u.hostname, u.port, timeout=10)
    return conns[key], u

def put(key, value):
    order = [endpoint_url()] + endpoints
    seen = set()
    for url in order:
        if url in seen:
            continue
        seen.add(url)
        try:
            c, u = conn(url)
            c.request("POST", f"/v1/kv/put?ns={ns}&key={key}", body=value,
                      headers={"Authorization": "Bearer " + token,
                               "Content-Type": "application/octet-stream"})
            r = c.getresponse()
            r.read()
            if r.status == 200:
                return
            c.close()
            conns.pop((u.hostname, u.port), None)
        except Exception:
            try:
                c.close()
            except Exception:
                pass
            conns.pop((u.hostname, u.port), None)
    raise RuntimeError("put failed: " + key)

start = time.time()
with open(ledger_path, "a", buffering=1) as ledger:
    for i in range(1, total + 1):
        key = f"k{i}"
        value = (f"v{i}-" + ("x" * value_bytes)).encode()
        for attempt in range(1, 1000):
            try:
                put(key, value)
                break
            except Exception:
                if attempt == 999:
                    raise
                time.sleep(0.2)
        ledger.write(f"{key}\t{value.decode()}\n")
        if i % 1000 == 0:
            log.write(f"PROGRESS {i} elapsed={time.time()-start:.1f}s\n")
log.write(f"WRITER_DONE {total} elapsed={time.time()-start:.1f}s\n")
PY
WRITER_PID=$!

wait_acks() { # count
  local want=$1 i
  for i in $(seq 1 1800); do
    [[ $(wc -l <"$LEDGER") -ge $want ]] && return 0
    kill -0 "$WRITER_PID" 2>/dev/null || e2e_fail "writer exited before $want acks"
    sleep .5
  done
  e2e_fail "timeout waiting for $want acknowledged writes"
}

epoch_changes() { # url
  curl -fsS -H "Authorization: Bearer $(e2e_token internal)" "$1/metrics" 2>/dev/null |
    awk '/cellhive_owner_epoch_changes_total\{role="owner"\}/ {print $2}' || true
}

switch_owner() { # old-name new-url phase
  local old=$1 newurl=$2 phase=$3 pid i code
  pid=$(cat "$E2E_BASE/$old.pid")
  echo "SWITCH $phase: SIGKILL $old at $(wc -l <"$LEDGER") acknowledged writes"
  kill -KILL "$pid" 2>/dev/null || true
  rm -f "$E2E_BASE/$old.pid"
  for i in $(seq 1 120); do
    code=$(curl -s -o /dev/null -w '%{http_code}' -m 5 \
      -H "Authorization: Bearer $TOKEN" "$newurl/v1/kv/get?ns=$NS&key=warm" 2>/dev/null || true)
    [[ "$code" = 200 ]] && break
    sleep 1
  done
  [[ "$code" = 200 ]] || e2e_fail "survivor did not take over in phase $phase"
  printf '%s\n' "$newurl" >"$ACTIVE.tmp"
  mv "$ACTIVE.tmp" "$ACTIVE"
  echo "SWITCH $phase: $newurl is owner; owner_epoch_changes_total=$(epoch_changes "$newurl")"
  if [[ $old = perf ]]; then start_owner; else start_peer; fi
  sleep 2
}

wait_acks $((TOTAL / 4))
switch_owner perf "$B" 1
wait_acks $((TOTAL / 2))
switch_owner peer "$A" 2
wait_acks $((TOTAL * 3 / 4))
switch_owner perf "$B" 3

# Wait for the writer to finish, not just for its last progress marker.
for i in $(seq 1 1800); do
  kill -0 "$WRITER_PID" 2>/dev/null || break
  sleep 1
done
wait "$WRITER_PID" || e2e_fail "writer failed; see $WRITER_LOG"
WRITER_PID=""
ACKED=$(wc -l <"$LEDGER")
[[ $ACKED = $TOTAL ]] || e2e_fail "acknowledged $ACKED, want $TOTAL"
echo "WRITE_DONE acked=$ACKED epochs_switched=3"

# Verify every acknowledged key against the current owner.
python3 - "$ACTIVE" "$LEDGER" "$NS" "$TOKEN" "$VERIFY_WORKERS" "$A" "$B" <<'PY'
import http.client, os, sys, threading
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlsplit

active_file, ledger_path, ns, token, workers, a, b = sys.argv[1:8]
workers = int(workers)
with open(ledger_path) as f:
    want = {}
    for line in f:
        key, value = line.rstrip("\n").split("\t", 1)
        want[key] = value
urls = [a, b]
def endpoint_order():
    try:
        with open(active_file) as f:
            first = f.read().strip()
    except OSError:
        first = a
    return [first] + [u for u in urls if u != first]
local = threading.local()
def conn_for(url):
    u = urlsplit(url)
    key = (u.hostname, u.port)
    c = getattr(local, "conn", None)
    if c is not None and getattr(local, "key", None) == key:
        return c
    if c is not None:
        try: c.close()
        except Exception: pass
    c = http.client.HTTPConnection(u.hostname, u.port, timeout=30)
    local.conn, local.key = c, key
    return c
def get_one(item):
    key, value = item
    for url in endpoint_order():
        try:
            c = conn_for(url)
            c.request("GET", f"/v1/kv/get?ns={ns}&key={key}",
                      headers={"Authorization": "Bearer " + token})
            r = c.getresponse()
            body = r.read().decode(errors="replace")
            if r.status == 200 and body == value:
                return None
            try: c.close()
            except Exception: pass
            local.conn = None
        except Exception:
            try:
                if getattr(local, "conn", None) is not None:
                    local.conn.close()
            except Exception:
                pass
            local.conn = None
    return key
with ThreadPoolExecutor(max_workers=workers) as pool:
    missing = [k for k in pool.map(get_one, want.items()) if k]
if missing:
    print(f"VERIFY_FAIL missing={len(missing)} sample={missing[:10]}", flush=True)
    raise SystemExit(1)
print(f"VERIFY_OK {len(want)}/{len(want)}", flush=True)
PY

# Optional native epoch GC: dry-run, then retry after the test upload-age margin.
# No tombstones are written; the direct predecessor and required chain survive.
if [[ "${E2E_STRESS_SAFE_GC:-0}" = 1 ]]; then
  GC_ADMIN="http://127.0.0.1:$((E2E_PORT_BASE+5))"
  SCOPE="$NS/__kv__/$CELL_ID"
  gc_owner() { CELLHIVE_ADMIN_URL="$GC_ADMIN" "$E2E_BIN/cellhive" gc scope "$SCOPE" "$@"; }
  gc_owner --dry-run --keep-previous "${E2E_STRESS_GC_KEEP:-0}" >"$E2E_BASE/gc-dry-run.json"
  cat "$E2E_BASE/gc-dry-run.json"
  gc_owner --safe --grace 1 --keep-previous "${E2E_STRESS_GC_KEEP:-0}" >"$E2E_BASE/gc-pass1.json"
  cat "$E2E_BASE/gc-pass1.json"
  sleep 2
  gc_owner --safe --grace 1 --keep-previous "${E2E_STRESS_GC_KEEP:-0}" >"$E2E_BASE/gc-pass2.json"
  cat "$E2E_BASE/gc-pass2.json"
  echo "SAFE_GC_DONE scope=$SCOPE keep_previous=${E2E_STRESS_GC_KEEP:-0}"
fi

# Stop the standby as well: cold verification must not fall back or forward
# to another node's warm SQLite cache.
STANDBY_PID=$(cat "$E2E_BASE/perf.pid")
kill -TERM "$STANDBY_PID"
for i in $(seq 1 120); do kill -0 "$STANDBY_PID" 2>/dev/null || break; sleep 1; done
kill -0 "$STANDBY_PID" 2>/dev/null && e2e_fail 'standby did not drain'
rm "$E2E_BASE/perf.pid"

# Cold COS restore after removing only the survivor's local SQLite cache.
ACTIVE_PID=$(cat "$E2E_BASE/peer.pid")
echo "COLD_RESTORE: draining peer owner pid=$ACTIVE_PID"
kill -TERM "$ACTIVE_PID"
for i in $(seq 1 120); do kill -0 "$ACTIVE_PID" 2>/dev/null || break; sleep 1; done
kill -0 "$ACTIVE_PID" 2>/dev/null && e2e_fail 'peer did not drain'
rm -rf "$E2E_BASE/peer/cells"
start_peer
COLD_TRACE=$(openssl rand -hex 16)
echo "cold_get_trace=$COLD_TRACE"
curl -fsS -m 120 -H "Authorization: Bearer $TOKEN" -H "traceparent: 00-$COLD_TRACE-$(openssl rand -hex 8)-01" \
  -w '\ncold_get_seconds=%{time_total}\n' "$B/v1/kv/get?ns=$NS&key=warm"
python3 - "$ACTIVE" "$LEDGER" "$NS" "$TOKEN" "$VERIFY_WORKERS" "$A" "$B" <<'PY'
import http.client, sys, threading
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlsplit
active_file, ledger_path, ns, token, workers, a, b = sys.argv[1:8]
workers = int(workers)
with open(ledger_path) as f:
    want = {}
    for line in f:
        key, value = line.rstrip("\n").split("\t", 1)
        want[key] = value
local = threading.local()
def conn_for(url):
    u = urlsplit(url)
    key = (u.hostname, u.port)
    c = getattr(local, "conn", None)
    if c is not None and getattr(local, "key", None) == key:
        return c
    if c is not None:
        try: c.close()
        except Exception: pass
    c = http.client.HTTPConnection(u.hostname, u.port, timeout=60)
    local.conn, local.key = c, key
    return c
def get_one(item):
    key, value = item
    for url in (b,):
        try:
            c = conn_for(url)
            c.request("GET", f"/v1/kv/get?ns={ns}&key={key}",
                      headers={"Authorization": "Bearer " + token})
            r = c.getresponse()
            body = r.read().decode(errors="replace")
            if r.status == 200 and body == value:
                return None
            try: c.close()
            except Exception: pass
            local.conn = None
        except Exception:
            try:
                if getattr(local, "conn", None) is not None:
                    local.conn.close()
            except Exception:
                pass
            local.conn = None
    return key
with ThreadPoolExecutor(max_workers=workers) as pool:
    missing = [k for k in pool.map(get_one, want.items()) if k]
if missing:
    print(f"COLD_VERIFY_FAIL missing={len(missing)} sample={missing[:10]}", flush=True)
    raise SystemExit(1)
print(f"COLD_VERIFY_OK {len(want)}/{len(want)}", flush=True)
PY

echo "E2E-KV-EPOCH-STRESS: PASS keys=$ACKED switches=3 scope=$NS/__kv__/$CELL_ID"
