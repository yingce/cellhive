#!/usr/bin/env bash
# Real native DO SQLite export/import with fresh runtime caches and object storage.
set -euo pipefail
source "$(dirname "$0")/lib.sh"
for binary in do-runtime; do
  [[ -x "$E2E_BIN/$binary" ]] || e2e_fail "missing $binary; build current worktree first"
done
"$CELLHIVE_WORKERD" cellhive-build-info | grep -qx 'do-paging/1' || e2e_fail 'paging fork required'
"$CELLHIVE_WORKERD" cellhive-build-info | grep -qx 'do-paging-lifecycle/1' || e2e_fail 'paging lifecycle fork required'
DO_A=$((E2E_PORT_BASE+6))
GATE_A=$((E2E_PORT_BASE+8))
export CELLHIVE_DO_RUNTIMES="http://127.0.0.1:$DO_A"
export CELLHIVE_DURABILITY=bucket
export CELLHIVE_COMPACTION_INTERVAL=1h
e2e_start_agent agent-a "$E2E_AGENT_PORT" "$E2E_ADMIN_PORT" '' "$E2E_BASE/agent-a"
e2e_cli app create e2epaging >/dev/null
SHA=$(e2e_upload "$E2E_ROOT/scripts/e2e/fixtures/do-paging-fleet.js")
cat > "$E2E_BASE/deploy.json" <<JSON
{"namespace":"e2epaging","worker":"paging","bundle_sha":"$SHA","compatibility_date":"2026-09-01","bindings":[{"type":"do","name":"BENCH","id":"Bench"}]}
JSON
e2e_deploy "$E2E_BASE/deploy.json" > "$E2E_BASE/deployed.json"
# Import through the non-control-owner admin endpoint to exercise forwarding.
IMPORT_ADMIN_PORT=$((E2E_PORT_BASE+5))
e2e_start_agent agent-b "$((E2E_PORT_BASE+4))" "$IMPORT_ADMIN_PORT" '' "$E2E_BASE/agent-b"
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
      E2E_DO_GATE="$gate" E2E_DO_PORT="$port" E2E_DO_SCOPE="e2epaging/__do__" \
      setsid bash -c '

        exec "$E2E_BIN/do-runtime" \
          -scope-prefix "$E2E_DO_SCOPE" -listen "127.0.0.1:$E2E_DO_GATE"
      ' > "$E2E_BASE/$name.log" 2>&1 &
  echo $! > "$E2E_BASE/$name.pid"
  e2e_wait_http "http://127.0.0.1:$port/ready" 120 || e2e_fail "$name not ready"
}
invoke() { # path, object id; never retry mutations (no idempotency assumed).
  curl --fail-with-body -sS --output "$E2E_BASE/invoke-response" --max-time "${E2E_PAGING_REQUEST_TIMEOUT:-60}" -H "Authorization: Bearer $(e2e_token scope e2epaging do BENCH)" \
    -H 'content-type: application/json' \
    --data-binary "{\"namespace\":\"e2epaging\",\"worker\":\"paging\",\"class\":\"Bench\",\"id\":\"${2:-single}\",\"request\":{\"path\":\"$1\"}}" \
    "http://127.0.0.1:$E2E_AGENT_PORT/v1/do/invoke" || { local status=$?; cat "$E2E_BASE/invoke-response" >&2; return "$status"; }
  cat "$E2E_BASE/invoke-response"
}


stop_do() {
 local pid; pid=$(cat "$E2E_BASE/rta.pid")
 kill -TERM "$pid"
 for i in $(seq 1 120); do kill -0 "$pid" 2>/dev/null || break; sleep .5; done
 kill -0 "$pid" 2>/dev/null && e2e_fail 'DO runtime did not drain'
 wait "$pid" 2>/dev/null || true
 rm "$E2E_BASE/rta.pid"
}
scope_for() {
 python3 - "$(jq -r .storage_id "$E2E_BASE/deployed.json")" "$1" <<'PYSCOPE'
import base64,sys
raw=f"{sys.argv[1]}/Bench/Bench/{sys.argv[2]}".encode()
print("e2epaging/Bench/"+base64.urlsafe_b64encode(raw).decode().rstrip("="))
PYSCOPE
}
start_do rta "$DO_A" "$GATE_A" "$E2E_BASE/state-source"
e2e_need "$(invoke '/seed?rows=8')" seeded 'source seed failed'
for i in $(seq 1 7); do e2e_need "$(invoke /inc | jq -r .v)" "$i" 'source counter mismatch'; done
invoke /proof > "$E2E_BASE/source-proof"
stop_do
SOURCE_SCOPE=$(scope_for single)
TARGET_ID=$(python3 - <<'PYID'
def shard(s):
 h=2166136261
 for b in s.encode(): h=((h^b)*16777619)&0xffffffff
 return h%16
prefix="e2epaging/paging/Bench/"
print(next("imported"+str(n) for n in range(1000) if shard(prefix+"imported"+str(n))==shard(prefix+"single")))
PYID
)
TARGET_SCOPE=$(scope_for "$TARGET_ID")
echo "source_scope=$SOURCE_SCOPE target_scope=$TARGET_SCOPE"
e2e_cli cell export "$SOURCE_SCOPE" --output "$E2E_BASE/do-export.sqlite"
python3 - "$E2E_BASE/do-export.sqlite" <<'PYDB'
import sqlite3,sys
with sqlite3.connect(sys.argv[1]) as db:
 assert db.execute("PRAGMA integrity_check").fetchone()[0]=="ok"
 assert db.execute("SELECT v FROM counter").fetchone()[0]==7
 assert db.execute("SELECT count(*) FROM payload").fetchone()[0]==8
print("PASS native DO SQLite export: counter=7 payload_rows=8")
PYDB
# Force a transport-owner generation change before importing. Native capture
# uses its shard generation independently of this transport owner generation.
claim=$(curl -fsS -H "Authorization: Bearer $(e2e_token internal)" -H 'Content-Type: application/json' \
 --data "{\"scope\":\"$TARGET_SCOPE\"}" "http://127.0.0.1:$E2E_AGENT_PORT/v1/internal/claim")
prior_epoch=$(jq -r .owner.epoch <<<"$claim")
curl -fsS -H "Authorization: Bearer $(e2e_token internal)" -H 'Content-Type: application/json' \
 --data "{\"scope\":\"$TARGET_SCOPE\",\"epoch\":$prior_epoch}" \
 "http://127.0.0.1:$E2E_AGENT_PORT/v1/internal/release" >/dev/null
start_do rta "$DO_A" "$GATE_A" "$E2E_BASE/state-imported"
# Expect/100-continue is a deterministic barrier: Go sends 100 only when the
# handler reads the body, after acquiring the native shard owner.
python3 - "$IMPORT_ADMIN_PORT" "$E2E_AGENT_PORT" "$TARGET_SCOPE" "$TARGET_ID" "$E2E_BASE/do-export.sqlite" "$(e2e_token admin)" "$(e2e_token scope e2epaging do BENCH)" <<'PYRACE'
import http.client,json,socket,sys,urllib.parse,time,os
admin,agent,scope,object_id,path,admin_token,do_token=sys.argv[1:]
data=open(path,"rb").read()
with socket.create_connection(("127.0.0.1",int(admin)),timeout=60) as sock:
 target="/v1/control/cell/import?scope="+urllib.parse.quote(scope,safe="")
 sock.sendall((f"POST {target} HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {admin_token}\r\nContent-Length: {len(data)}\r\nExpect: 100-continue\r\nConnection: close\r\n\r\n").encode())
 response=b""
 while not response.endswith(b"\r\n\r\n"):
  byte=sock.recv(1)
  assert byte,"import closed before body admission"
  response+=byte
 assert response.startswith(b"HTTP/1.1 100 "),response
 conn=http.client.HTTPConnection("127.0.0.1",int(agent),timeout=30)
 payload=json.dumps({"namespace":"e2epaging","worker":"paging","class":"Bench","id":object_id,"request":{"path":"/read"}})
 conn.request("POST","/v1/do/invoke",payload,{"Authorization":"Bearer "+do_token,"Content-Type":"application/json"})
 blocked=conn.getresponse(); body=blocked.read(); conn.close()
 assert blocked.status in (409,503), (blocked.status,body)
 print("PASS concurrent native activation blocked during import:",blocked.status)
 midpoint=len(data)//2
 sock.sendall(data[:midpoint])
 pause=float(os.environ.get("E2E_IMPORT_PAUSE_SECONDS","0"))
 if pause: time.sleep(pause)
 sock.sendall(data[midpoint:])
 finished=http.client.HTTPResponse(sock);finished.begin();body=finished.read()
 assert finished.status==200,(finished.status,body)
 result=json.loads(body)
 assert result["owner_epoch"]>1 and result["epoch"]>1,result
 print("PASS forwarded import completed and released shard; upload_pause_seconds=",pause)
PYRACE
e2e_need "$(invoke /read "$TARGET_ID" | jq -r .v)" 7 'imported native DO read mismatch'
invoke /proof "$TARGET_ID" > "$E2E_BASE/imported-proof"
cmp "$E2E_BASE/source-proof" "$E2E_BASE/imported-proof"
e2e_need "$(invoke /inc "$TARGET_ID" | jq -r .v)" 8 'imported native DO write mismatch'
ACTIVE_TARGET_ID=$(python3 - "$TARGET_ID" <<'PYID'
import sys
def shard(s):
 h=2166136261
 for b in s.encode(): h=((h^b)*16777619)&0xffffffff
 return h%16
prefix="e2epaging/paging/Bench/"
print(next("busy"+str(n) for n in range(1000) if shard(prefix+"busy"+str(n))==shard(prefix+sys.argv[1])))
PYID
)
ACTIVE_TARGET_SCOPE=$(scope_for "$ACTIVE_TARGET_ID")
status=$(curl -sS --max-time 30 -o "$E2E_BASE/active-import.json" -w '%{http_code}' \
 -H "Authorization: Bearer $(e2e_token admin)" --data-binary @"$E2E_BASE/do-export.sqlite" \
 "http://127.0.0.1:$E2E_ADMIN_PORT/v1/control/cell/import?scope=$ACTIVE_TARGET_SCOPE")
e2e_need "$status" 409 'import admitted an already active native shard'
e2e_need "$(jq -r .error "$E2E_BASE/active-import.json")" import_shard_unavailable 'wrong reason for active-shard rejection'
echo 'PASS active native shard rejects import into unused data scope'

stop_do
start_do rta "$DO_A" "$GATE_A" "$E2E_BASE/state-cold"
e2e_need "$(invoke /read "$TARGET_ID" | jq -r .v)" 8 'native DO cold restart lost update'
invoke /proof "$TARGET_ID" > "$E2E_BASE/cold-proof"
cmp "$E2E_BASE/source-proof" "$E2E_BASE/cold-proof"
stop_do
e2e_cli cell export "$TARGET_SCOPE" --output "$E2E_BASE/do-reexport.sqlite"
python3 - "$E2E_BASE/do-reexport.sqlite" <<'PYDB'
import sqlite3,sys
with sqlite3.connect(sys.argv[1]) as db:
 assert db.execute("PRAGMA integrity_check").fetchone()[0]=="ok"
 assert db.execute("SELECT v FROM counter").fetchone()[0]==8
print("PASS native DO reexport after cold restart: counter=8")
PYDB
echo 'E2E-DO-TRANSFER: PASS'
