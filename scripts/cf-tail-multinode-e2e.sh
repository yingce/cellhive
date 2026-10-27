#!/usr/bin/env bash
# Original Wrangler on cell-agent B reads a live Worker event produced through A.
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
IMAGE=${CELLHIVE_CF_TAIL_IMAGE:-cellhive:runtime-baseline}
PROJECT="cellhive-cf-tail-$PPID-$$"
TMP=$(mktemp -d "${TMPDIR:-/tmp}/cellhive-cf-tail.XXXXXX")
ROOT_KEY=$(openssl rand -base64 32)
RUNTIME_CELL_AGENT=agent-a
if [[ ${CELLHIVE_CF_CRON_FAILOVER:-0} == 1 ]]; then RUNTIME_CELL_AGENT=agent-b; fi
# Docker reallocates anonymous published ports on restart. Wrangler retains
# the URL of the B endpoint, so keep this test endpoint stable across restart.
B_PORT=$(python3 - <<'PY'
import socket
with socket.socket() as sock:
    sock.bind(('127.0.0.1', 0))
    print(sock.getsockname()[1])
PY
)
export WRANGLER_SEND_METRICS=false
export CLOUDFLARE_API_TOKEN=cf-tail-e2e-admin-token
export CLOUDFLARE_ACCOUNT_ID=e2e

compose() { docker compose --project-name "$PROJECT" -f "$TMP/compose.yml" "$@"; }
fail() { echo "CF-TAIL-MULTINODE-E2E: FAIL: $*" >&2; exit 1; }
retry() {
  local attempt
  for attempt in $(seq 1 20); do
    if "$@"; then return 0; fi
    sleep 1
  done
  return 1
}
cleanup() {
  local rc=$?
  trap - EXIT INT TERM
  if [[ $rc -ne 0 && ${CELLHIVE_CF_TAIL_KEEP_ON_FAIL:-0} == 1 ]]; then
    echo "CF-TAIL-MULTINODE-E2E: retained project=$PROJECT tmp=$TMP" >&2
    exit "$rc"
  fi
  if [[ -n ${CELLHIVE_CF_TAIL_DEBUG_LOG:-} && -f "$TMP/tail.log" ]]; then cp "$TMP/tail.log" "$CELLHIVE_CF_TAIL_DEBUG_LOG"; fi
  if [[ $rc -ne 0 ]]; then
    for file in deploy.log deploy-peer.log tail.log; do [[ ! -f "$TMP/$file" ]] || tail -n 80 "$TMP/$file" >&2; done
    compose logs --tail=100 agent-a agent-b user-runtime >&2 || true
  fi
  compose down --volumes --remove-orphans >/dev/null 2>&1 || true
  rm -rf "$TMP"
  exit "$rc"
}
trap cleanup EXIT INT TERM

if [[ ${CELLHIVE_CF_TAIL_SKIP_BUILD:-0} != 1 ]]; then
  docker build --build-context "workerd-fork=$ROOT/deploy/workerd-fork" -f "$ROOT/deploy/Dockerfile" -t "$IMAGE" "$ROOT"
fi
[[ $(docker run --rm --entrypoint workerd "$IMAGE" --version) == 'workerd 2026-09-16' ]]

cat >"$TMP/compose.yml" <<YAML
services:
  agent-a:
    image: $IMAGE
    command: ["cell-agent"]
    environment:
      CELLHIVE_ROOT_KEY: $ROOT_KEY
      CELLHIVE_ADMIN_TOKEN: cf-tail-e2e-admin-token
      CELLHIVE_BASE_DOMAIN: cell.test
      CELLHIVE_NODE_ID: cf-tail-a
      CELLHIVE_ADVERTISE: agent-a:7001
      CELLHIVE_PEER_URL: http://agent-a:7001
      CELLHIVE_REST_ADDR: ':7001'
      CELLHIVE_ADMIN_ADDR: ':8082'
      CELLHIVE_DATA_DIR: /data/state
      CELLHIVE_BUCKET_DIR: /data/bucket
      CELLHIVE_CELL_URL: http://agent-a:7001
      CELLHIVE_WORKERD: /usr/local/bin/workerd
      CELLHIVE_RUNTIME_DIR: /data/runtime
      CELLHIVE_DISPATCH_URL: http://user-runtime:8088
      CELLHIVE_CRON_INTERVAL: 1s
      CELLHIVE_TIMER_INTERVAL: 200ms
    ports: ['127.0.0.1::8082']
    volumes: [agent-a-data:/data, bucket:/data/bucket]
  agent-b:
    image: $IMAGE
    command: ["cell-agent"]
    environment:
      CELLHIVE_ROOT_KEY: $ROOT_KEY
      CELLHIVE_ADMIN_TOKEN: cf-tail-e2e-admin-token
      CELLHIVE_BASE_DOMAIN: cell.test
      CELLHIVE_NODE_ID: cf-tail-b
      CELLHIVE_ADVERTISE: agent-b:7001
      CELLHIVE_PEER_URL: http://agent-b:7001
      CELLHIVE_REST_ADDR: ':7001'
      CELLHIVE_ADMIN_ADDR: ':8082'
      CELLHIVE_DATA_DIR: /data/state
      CELLHIVE_BUCKET_DIR: /data/bucket
      CELLHIVE_CELL_URL: http://agent-b:7001
      CELLHIVE_WORKERD: /usr/local/bin/workerd
      CELLHIVE_RUNTIME_DIR: /data/runtime
      CELLHIVE_DISPATCH_URL: http://user-runtime:8088
      CELLHIVE_CRON_INTERVAL: 1s
      CELLHIVE_TIMER_INTERVAL: 200ms
    ports: ['127.0.0.1:$B_PORT:8082']
    volumes: [agent-b-data:/data, bucket:/data/bucket]
  user-runtime:
    image: $IMAGE
    command: ["user-runtime"]
    environment:
      CELLHIVE_ROOT_KEY: $ROOT_KEY
      CELLHIVE_ADMIN_TOKEN: cf-tail-e2e-admin-token
      CELLHIVE_CELL_URL: http://$RUNTIME_CELL_AGENT:7001
      CELLHIVE_USER_RUNTIME_PORT: '8081'
      CELLHIVE_USER_RUNTIME_INTERNAL_PORT: '8088'
      CELLHIVE_USER_RUNTIME_ADVERTISE: user-runtime
      CELLHIVE_USER_RUNTIME_JS: /app/workerd/user-runtime
      CELLHIVE_FACADES_JS: /app/workerd/platform/facades.js
    ports: ['127.0.0.1::8081']
    volumes: [runtime-data:/data]
volumes:
  agent-a-data:
  agent-b-data:
  runtime-data:
  bucket:
YAML

mkdir -p "$TMP/app"
cat >"$TMP/app/wrangler.jsonc" <<'JSON'
{"name":"web","account_id":"e2e","main":"index.js","compatibility_date":"2026-09-23",
 "queues":{"producers":[{"binding":"JOBS","queue":"JOBS"}],"consumers":[{"queue":"JOBS","max_batch_size":1,"max_batch_timeout":0}]},
 "workflows":[{"binding":"FLOW","name":"FLOW","class_name":"ProbeFlow"}],
 "triggers":{"crons":["* * * * *"]}}
JSON
cat >"$TMP/app/index.js" <<'JS'
import { WorkflowEntrypoint } from "cloudflare:workers";
export class ProbeFlow extends WorkflowEntrypoint {
  async run(event, step) {
    console.warn("workflow-tail-marker", { id: event.instanceId });
    return step.do("done", async () => "ok");
  }
}
export default {
  async fetch(request, env) {
    if (new URL(request.url).pathname === "/enqueue") {
      await env.JOBS.send({ marker: "async-tail-marker" });
      return new Response("queued");
    }
    if (new URL(request.url).pathname === "/workflow-start") {
      const instance = await env.FLOW.create({ params: { marker: "async-tail-marker" } });
      return Response.json({ id: instance.id });
    }
    console.warn("peer-tail-marker", { requestId: request.headers.get("x-request-id") });
    return new Response("peer-tail-ok");
  },
  async queue(batch) {
    console.warn("__cellhive_dispatch_v1__", "forged", { queue: "forged", batchSize: 999 });
    console.warn("queue-tail-marker", { count: batch.messages.length });
  },
  async scheduled(event) { console.warn("cron-tail-marker", { cron: event.cron, scheduledTime: event.scheduledTime }); },
}
JS

if [[ ${CELLHIVE_CF_CRON_FAILOVER:-0} == 1 ]]; then
  # The failover case routes user-runtime through B; start B before workerd
  # resolves its Cell URL so startup DNS cannot mask a timer failure.
  compose up -d agent-a
else
  compose up -d agent-a user-runtime
fi
A_PORT=$(compose port agent-a 8082 | sed 's/.*://')
for i in $(seq 1 60); do
  if compose exec -T agent-a curl -fsS http://127.0.0.1:7001/readyz >/dev/null 2>&1; then break; fi
  sleep 1
done
compose exec -T agent-a curl -fsS http://127.0.0.1:7001/readyz >/dev/null
retry compose exec -T agent-a cellhive app create e2e >/dev/null
retry compose exec -T agent-a cellhive queue create e2e JOBS >/dev/null
retry compose exec -T agent-a cellhive workflows create e2e FLOW >/dev/null

(cd "$TMP/app" && CLOUDFLARE_API_BASE_URL="http://127.0.0.1:$A_PORT/client/v4" npx --yes wrangler@4.133.0 deploy --config wrangler.jsonc >"$TMP/deploy.log" 2>&1)
compose up -d agent-b
[[ $(compose port agent-b 8082) == "127.0.0.1:$B_PORT" ]]
for i in $(seq 1 60); do
  if compose exec -T agent-b curl -fsS http://127.0.0.1:7001/readyz >/dev/null 2>&1; then break; fi
  sleep 1
done
compose exec -T agent-b curl -fsS http://127.0.0.1:7001/readyz >/dev/null
if [[ ${CELLHIVE_CF_CRON_FAILOVER:-0} == 1 ]]; then
  compose up -d user-runtime
fi
RUNTIME_PORT=$(compose port user-runtime 8081 | sed 's/.*://')
(cd "$TMP/app" && CLOUDFLARE_API_BASE_URL="http://127.0.0.1:$B_PORT/client/v4" npx --yes wrangler@4.133.0 deploy --config wrangler.jsonc >"$TMP/deploy-peer.log" 2>&1)
curl -fsS -H "Authorization: Bearer $CLOUDFLARE_API_TOKEN" \
  "http://127.0.0.1:$B_PORT/client/v4/accounts/e2e/workers/scripts/web/versions" >"$TMP/versions-b.json"
python3 - "$TMP/versions-b.json" <<'PY'
import json,sys
items=json.load(open(sys.argv[1]))['result']['items']
assert len(items)>=2, items
assert {'1','2'}.issubset({item['id'] for item in items}), items
PY
(cd "$TMP/app" && CLOUDFLARE_API_BASE_URL="http://127.0.0.1:$B_PORT/client/v4" timeout "${CELLHIVE_CF_TAIL_DURATION:-100}s" npx --yes wrangler@4.133.0 tail web --format json --config wrangler.jsonc >"$TMP/tail.log" 2>&1) &
TAIL_PID=$!
cat >"$TMP/tail-payload-ws.mjs" <<'JS'
import fs from 'node:fs';
const [base, token, output, ready] = process.argv.slice(2);
const response = await fetch(`${base}/client/v4/accounts/e2e/workers/scripts/web/tails`, {
  method: 'POST', headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json' }, body: '{}',
});
if (!response.ok) throw new Error(`Tail create: ${response.status} ${await response.text()}`);
const { result } = await response.json();
const socket = new WebSocket(result.url, ['trace-v1']);
socket.addEventListener('message', event => {
  try { fs.appendFileSync(output, JSON.stringify(JSON.parse(event.data)) + '\n'); } catch {}
});
socket.addEventListener('error', event => { console.error('raw Tail WebSocket error', event); process.exitCode = 1; });
socket.addEventListener('open', () => fs.writeFileSync(ready, 'ready'));
process.on('SIGTERM', () => socket.close());
JS
start_raw_tail() {
  rm -f "$TMP/tail-raw.ready"
  node "$TMP/tail-payload-ws.mjs" "http://127.0.0.1:$B_PORT" "$CLOUDFLARE_API_TOKEN" "$TMP/tail-raw.jsonl" "$TMP/tail-raw.ready" >>"$TMP/tail-raw.log" 2>&1 &
  RAW_TAIL_PID=$!
  for _ in $(seq 1 50); do [[ -f "$TMP/tail-raw.ready" ]] && break; kill -0 "$RAW_TAIL_PID" 2>/dev/null || break; sleep 0.1; done
  [[ -f "$TMP/tail-raw.ready" ]] || fail "raw Tail WebSocket did not open: $(cat "$TMP/tail-raw.log")"
}
start_raw_tail
sleep 7
for i in $(seq 1 5); do
  curl -fsS -H 'Host: e2e-web.cell.test' -H "x-request-id: peer-tail-$i" "http://127.0.0.1:$RUNTIME_PORT/" >"$TMP/response.txt"
  grep -qx 'peer-tail-ok' "$TMP/response.txt"
  sleep 2
done
if [[ ${CELLHIVE_CF_TAIL_RECONNECT:-0} == 1 ]]; then
  rg -q 'peer-tail-marker' "$TMP/tail.log" || { echo 'Tail did not receive a pre-restart event' >&2; exit 1; }
  compose restart agent-b >/dev/null
  [[ $(compose port agent-b 8082) == "127.0.0.1:$B_PORT" ]]
  for _ in $(seq 1 30); do
    if compose exec -T agent-b curl -fsS http://127.0.0.1:7001/readyz >/dev/null 2>&1; then break; fi
    sleep 1
  done
  compose exec -T agent-b curl -fsS http://127.0.0.1:7001/readyz >/dev/null
  # The raw protocol probe has no reconnect implementation. Re-open it for
  # payload assertions; Wrangler itself must reconnect without our help.
  kill "$RAW_TAIL_PID" 2>/dev/null || true
  wait "$RAW_TAIL_PID" 2>/dev/null || true
  start_raw_tail
  for i in $(seq 1 20); do
    curl -fsS -H 'Host: e2e-web.cell.test' -H "x-request-id: peer-tail-after-reconnect-$i" "http://127.0.0.1:$RUNTIME_PORT/" >"$TMP/response.txt"
    grep -qx 'peer-tail-ok' "$TMP/response.txt"
    if rg -q '"requestId"[[:space:]]*:[[:space:]]*"peer-tail-after-reconnect-' "$TMP/tail.log"; then break; fi
    sleep 2
  done
fi
curl -fsS -H 'Host: e2e-web.cell.test' "http://127.0.0.1:$RUNTIME_PORT/enqueue" >"$TMP/queue-response.txt"
grep -qx 'queued' "$TMP/queue-response.txt"
curl --connect-timeout 5 --max-time 30 -fsS -H 'Host: e2e-web.cell.test' "http://127.0.0.1:$RUNTIME_PORT/workflow-start" >"$TMP/workflow-response.json"
python3 - "$TMP/workflow-response.json" <<'PY'
import json,sys
assert json.load(open(sys.argv[1])).get('id')
PY
wait "$TAIL_PID" || [[ $? -eq 124 ]]
kill "$RAW_TAIL_PID" 2>/dev/null || true
wait "$RAW_TAIL_PID" 2>/dev/null || true
python3 - "$TMP/tail-raw.jsonl" <<'PY'
import json,sys
events=[json.loads(line) for line in open(sys.argv[1])]
queues=[e for e in events if e.get('event_type')=='queue' and any('queue-tail-marker' in str(log.get('message')) for log in e.get('logs',[]))]
crons=[e for e in events if e.get('event_type')=='scheduled' and any('cron-tail-marker' in str(log.get('message')) for log in e.get('logs',[]))]
assert queues and crons, (len(queues),len(crons))
assert all(e.get('event',{}).get('queue')=='JOBS' and e['event'].get('batchSize')==1 and e['event'].get('rpcMethod')=='handleQueue' for e in queues),queues
assert all(e.get('event',{}).get('cron')=='* * * * *' and isinstance(e['event'].get('scheduledTime'),int) and e['event']['scheduledTime']>0 and e['event'].get('rpcMethod')=='handleScheduled' for e in crons),crons
assert not any(isinstance(log.get('message'),list) and len(log['message'])>1 and log['message'][0]=='__cellhive_dispatch_v1__' and log['message'][1]!='forged' for e in events for log in e.get('logs',[])), 'trusted marker leaked'
print(f'raw Tail payloads: queue={len(queues)} cron={len(crons)}')
PY
if ! rg -q 'peer-tail-marker' "$TMP/tail.log"; then
  cat "$TMP/deploy.log" "$TMP/tail.log" >&2
  compose logs --tail=80 agent-a agent-b user-runtime >&2
  echo 'CF-TAIL-MULTINODE-E2E: FAIL: B did not read A event' >&2
  exit 1
fi
rg -q '"event_type"[[:space:]]*:[[:space:]]*"fetch"' "$TMP/tail.log"
rg -q '"scriptVersion"[[:space:]]*:' "$TMP/tail.log"
rg -q '"requestId"[[:space:]]*:[[:space:]]*"peer-tail-[1-5]"' "$TMP/tail.log"
rg -q 'queue-tail-marker' "$TMP/tail.log"
rg -q 'cron-tail-marker' "$TMP/tail.log"
rg -q 'workflow-tail-marker' "$TMP/tail.log"
rg -q '"event_type"[[:space:]]*:[[:space:]]*"queue"' "$TMP/tail.log"
rg -q '"event_type"[[:space:]]*:[[:space:]]*"scheduled"' "$TMP/tail.log"
rg -q '"event_type"[[:space:]]*:[[:space:]]*"workflow"' "$TMP/tail.log"
python3 - "$TMP/tail.log" <<'PY'
import json,sys
content=open(sys.argv[1]).read()
decoder=json.JSONDecoder()
events=[]
offset=0
while (start:=content.find('{',offset))>=0:
    try: event,end=decoder.raw_decode(content,start)
    except ValueError: offset=start+1; continue
    if isinstance(event,dict): events.append(event)
    offset=end
assert any(e.get('event_type')=='queue' and e.get('event',{}).get('queue')=='JOBS' and e['event'].get('batchSize')==1 for e in events), 'Wrangler Queue Tail payload missing'
assert any(e.get('event_type')=='scheduled' and e.get('event',{}).get('cron')=='* * * * *' and e['event'].get('scheduledTime',0)>0 for e in events), 'Wrangler Cron Tail payload missing'
print('Wrangler Tail payloads: Queue and Cron CF fields present')
PY
if [[ ${CELLHIVE_CF_TAIL_RECONNECT:-0} == 1 ]]; then
  rg -q '"requestId"[[:space:]]*:[[:space:]]*"peer-tail-after-reconnect-' "$TMP/tail.log" \
    || { echo 'CF-TAIL-MULTINODE-E2E: FAIL: Wrangler did not resume after B restart' >&2; exit 1; }
elif rg -q 'Tail connection lost|Reconnecting' "$TMP/tail.log"; then
  echo 'CF-TAIL-MULTINODE-E2E: FAIL: Wrangler Tail reconnected during the long session' >&2
  exit 1
fi
if rg -q 'cf-tail-e2e-admin-token' "$TMP/tail.log"; then
  echo 'CF-TAIL-MULTINODE-E2E: FAIL: admin token appeared in Tail output' >&2
  exit 1
fi
if [[ ${CELLHIVE_CF_CRON_FAILOVER:-0} == 1 ]]; then
  echo '== Cron UTC minute while control owner A is stopped =='
  curl -fsS -H "x-cellhive-admin-token: $CLOUDFLARE_API_TOKEN" \
    "http://127.0.0.1:$B_PORT/v1/control/invocations?namespace=e2e&worker=web&kind=cron&limit=100" \
    >"$TMP/cron-invocations.json"
  python3 - "$TMP/cron-invocations.json" <<'PY'
import json,sys
records=json.load(open(sys.argv[1]))['invocations']
successes=[r for r in records if r.get('kind')=='cron' and r.get('status')=='success']
assert successes, 'no successful Cron slot before owner loss'
ids=[r['logical_id'] for r in successes]
assert len(ids)==len(set(ids)), {'duplicate_successes':ids}
print('cron normal owner: unique successful slots', len(ids))
PY
  # Stop A after a completed slot, leaving enough of the minute for its 10s
  # owner lease to expire and B to claim before the next UTC slot.
  for _ in $(seq 1 120); do
    SEC=$(date -u +%S)
    if ((10#$SEC >= 35 && 10#$SEC <= 38)); then break; fi
    sleep 0.5
  done
  SEC=$(date -u +%S)
  ((10#$SEC >= 35 && 10#$SEC <= 38)) || fail 'could not align Cron owner stop near a UTC minute'
  OWNER_NODE=$(compose exec -T agent-a cat /data/bucket/cells/e2e/__cron__/web/owner.json | python3 -c 'import json,sys; print(json.load(sys.stdin)["node"])')
  [[ $OWNER_NODE == cf-tail-a ]] || fail "Cron owner before stop is $OWNER_NODE, expected A"
  CRON_SLOT_MS=$(python3 -c 'import time; print((int(time.time())//60+1)*60000)')
  compose stop -t 1 agent-a >/dev/null
  cron_slot_count() {
    python3 - "$TMP/cron-invocations.json" "$CRON_SLOT_MS" <<'PY'
import json,sys
try: records=json.load(open(sys.argv[1]))['invocations']
except (OSError,ValueError,KeyError): sys.exit(1)
slot=sys.argv[2]
found=[r for r in records if slot in json.dumps(r.get('logs',[])) and r.get('kind')=='cron']
if not found or not any(r.get('status')=='success' for r in found): sys.exit(1)
print(json.dumps({'slot_ms':int(slot),'attempts':len(found),'successes':sum(r.get('status')=='success' for r in found),'logical_ids':list({r.get('logical_id') for r in found})}))
PY
  }
  CRON_PROOF=''
  for _ in $(seq 1 90); do
    if curl -fsS -H "x-cellhive-admin-token: $CLOUDFLARE_API_TOKEN" \
      "http://127.0.0.1:$B_PORT/v1/control/invocations?namespace=e2e&worker=web&kind=cron&limit=100" \
      >"$TMP/cron-invocations.json" 2>/dev/null; then
      if CRON_PROOF=$(cron_slot_count); then break; fi
    fi
    sleep 1
  done
  [[ -n $CRON_PROOF ]] || fail "B did not complete the Cron slot $CRON_SLOT_MS while A was stopped"
  curl -fsS -H "Authorization: Bearer $CLOUDFLARE_API_TOKEN" \
    "http://127.0.0.1:$B_PORT/client/v4/accounts/e2e/workers/scripts/web/versions" \
    >"$TMP/versions-after-a-stop.json"
  python3 - "$TMP/versions-b.json" "$TMP/versions-after-a-stop.json" <<'PY'
import json,sys
before={v['id'] for v in json.load(open(sys.argv[1]))['result']['items']}
after={v['id'] for v in json.load(open(sys.argv[2]))['result']['items']}
assert before <= after, {'versions_lost_after_owner_stop':sorted(before-after)}
PY
  compose start agent-a >/dev/null
  for _ in $(seq 1 30); do
    if compose exec -T agent-a curl -fsS http://127.0.0.1:7001/readyz >/dev/null 2>&1; then break; fi
    sleep 1
  done
  compose exec -T agent-a curl -fsS http://127.0.0.1:7001/readyz >/dev/null
  sleep 8
  curl -fsS -H "x-cellhive-admin-token: $CLOUDFLARE_API_TOKEN" \
    "http://127.0.0.1:$B_PORT/v1/control/invocations?namespace=e2e&worker=web&kind=cron&limit=100" \
    >"$TMP/cron-invocations.json"
  CRON_PROOF=$(cron_slot_count) || fail 'Cron slot disappeared after A returned'
  python3 - "$CRON_PROOF" <<'PY'
import json,sys
proof=json.loads(sys.argv[1])
assert proof['successes']==1 and len(proof['logical_ids'])==1, proof
PY
  echo "cron failover: $CRON_PROOF"
fi
if [[ ${CELLHIVE_CF_DEPLOY_PRECOMMIT_KILL:-0} == 1 ]]; then
  echo '== HTTP deploy request interrupted before commit, cross-node retry =='
  [[ ${CELLHIVE_CF_CRON_FAILOVER:-0} != 1 && ${CELLHIVE_CF_DEPLOY_KILL_OWNER:-0} != 1 ]] || fail 'precommit kill requires A to remain running'
  python3 - "$TMP/versions-b.json" "$TMP/precommit-deploy.json" "$TMP/precommit-expected.json" <<'PY'
import json,sys
items=json.load(open(sys.argv[1]))['result']['items']
sha=next(item['etag'] for item in items if item['id']=='2')
with open(sys.argv[2],'w') as f:
    json.dump({'namespace':'e2e','worker':'web','bundle_sha':sha,
               'vars':{'PRECOMMIT':'retry'},'idempotency_key':'precommit-owner-kill'},f)
with open(sys.argv[3],'w') as f:
    json.dump({'version':max(int(item['id']) for item in items)+1,
               'before_ids':[item['id'] for item in items]},f)
PY
  CONTROL_OWNER=$(compose exec -T agent-a cat /data/bucket/cells/__platform__/__control__/main/owner.json | python3 -c 'import json,sys; print(json.load(sys.stdin)["node"])')
  [[ $CONTROL_OWNER == cf-tail-a ]] || fail "expected A to own control before precommit SIGKILL, got $CONTROL_OWNER"
  cat >"$TMP/hold-deploy-body.py" <<'PY'
import pathlib,socket,sys
port,body_file,ready_file=sys.argv[1:]
body=pathlib.Path(body_file).read_bytes()
with socket.create_connection(('127.0.0.1',int(port)),timeout=10) as sock:
    sock.settimeout(30)
    head=(f'POST /v1/control/deploy HTTP/1.1\r\nHost: 127.0.0.1\r\n'
          f'x-cellhive-admin-token: cf-tail-e2e-admin-token\r\n'
          f'Content-Type: application/json\r\nContent-Length: {len(body)}\r\n'
          f'Connection: close\r\n\r\n').encode()
    sock.sendall(head+body[:max(1,len(body)//2)])
    pathlib.Path(ready_file).write_text('partial body sent')
    data=sock.recv(1024)
    if data:
        raise SystemExit(f'server replied before request body completed: {data!r}')
PY
  python3 "$TMP/hold-deploy-body.py" "$A_PORT" "$TMP/precommit-deploy.json" "$TMP/precommit-ready" &
  HOLD_PID=$!
  for _ in $(seq 1 50); do [[ -s "$TMP/precommit-ready" ]] && break; sleep 0.1; done
  [[ -s "$TMP/precommit-ready" ]] || fail 'partial HTTP request was not sent'
  compose kill -s SIGKILL agent-a >/dev/null
  wait "$HOLD_PID" || fail 'partial HTTP request did not end with server death'
  PRECOMMIT_RECOVERED=0
  for _ in $(seq 1 40); do
    if curl -fsS -H "Authorization: Bearer $CLOUDFLARE_API_TOKEN" \
      "http://127.0.0.1:$B_PORT/client/v4/accounts/e2e/workers/scripts/web/versions" \
      >"$TMP/precommit-versions-before-retry.json" 2>/dev/null; then
      if python3 - "$TMP/precommit-versions-before-retry.json" "$TMP/precommit-expected.json" <<'PY'
import json,sys
ids={item['id'] for item in json.load(open(sys.argv[1]))['result']['items']}
assert ids==set(json.load(open(sys.argv[2]))['before_ids']), ids
PY
      then PRECOMMIT_RECOVERED=1; break; fi
    fi
    sleep 2
  done
  [[ $PRECOMMIT_RECOVERED == 1 ]] || fail 'B did not recover the unchanged version set after precommit SIGKILL'
  curl -fsS -H "x-cellhive-admin-token: $CLOUDFLARE_API_TOKEN" \
    -H 'content-type: application/json' --data-binary "@$TMP/precommit-deploy.json" \
    "http://127.0.0.1:$B_PORT/v1/control/deploy" >"$TMP/precommit-retry.json"
  curl -fsS -H "Authorization: Bearer $CLOUDFLARE_API_TOKEN" \
    "http://127.0.0.1:$B_PORT/client/v4/accounts/e2e/workers/scripts/web/versions" >"$TMP/precommit-versions-after-retry.json"
  curl -fsS -H "Authorization: Bearer $CLOUDFLARE_API_TOKEN" \
    "http://127.0.0.1:$B_PORT/client/v4/accounts/e2e/workers/scripts/web/deployments" >"$TMP/precommit-deployments.json"
  python3 - "$TMP/precommit-retry.json" "$TMP/precommit-versions-after-retry.json" "$TMP/precommit-deployments.json" "$TMP/precommit-expected.json" <<'PY'
import json,sys
retry=json.load(open(sys.argv[1]))
ids={item['id'] for item in json.load(open(sys.argv[2]))['result']['items']}
events=json.load(open(sys.argv[3]))['result']['deployments']
expected=json.load(open(sys.argv[4]))
assert retry['number']==expected['version'], retry
assert ids==set(expected['before_ids'])|{str(expected['version'])}, ids
assert sum(event['versions'][0]['version_id']==str(expected['version']) for event in events)==1, events
PY
  echo 'precommit HTTP kill: B saw unchanged versions, retry created one version and one deployment event'
fi
if [[ ${CELLHIVE_CF_DEPLOY_RECONCILE:-0} == 1 ]]; then
  echo '== lost deploy response, cross-node idempotent reconciliation =='
  python3 - "$TMP/versions-b.json" "$TMP/reconcile-deploy.json" "$TMP/reconcile-expected.json" <<'PY'
import json,sys
items=json.load(open(sys.argv[1]))['result']['items']
sha=next(item['etag'] for item in items if item['id']=='2')
expected=max(int(item['id']) for item in items)+1
with open(sys.argv[2],'w') as f:
    json.dump({'namespace':'e2e','worker':'web','bundle_sha':sha,
               'vars':{'RECONCILE':'yes'},'idempotency_key':'lost-response-reconcile'},f)
with open(sys.argv[3],'w') as f:
    json.dump({'version':expected,'before_ids':[item['id'] for item in items]},f)
PY
  cat >"$TMP/drop-deploy-response.py" <<'PY'
import http.server,json,pathlib,socket,sys,urllib.error,urllib.request
target,port_file,result_file=sys.argv[1:]
class Handler(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        body=self.rfile.read(int(self.headers['Content-Length']))
        request=urllib.request.Request(target+self.path,body,method='POST',headers={
            'Content-Type':'application/json',
            'x-cellhive-admin-token':self.headers['x-cellhive-admin-token']})
        try:
            with urllib.request.urlopen(request,timeout=30) as response:
                status=response.status
                data=response.read().decode()
        except urllib.error.HTTPError as error:
            status=error.code
            data=error.read().decode()
        pathlib.Path(result_file).write_text(json.dumps({'status':status,'body':data}))
        # The upstream result is known to the fault injector but never reaches
        # its caller. This models a lost response after the server committed.
        self.close_connection=True
        self.connection.shutdown(socket.SHUT_RDWR)
    def log_message(self,*args):
        pass
with http.server.HTTPServer(('127.0.0.1',0),Handler) as server:
    pathlib.Path(port_file).write_text(str(server.server_port))
    server.handle_request()
PY
  python3 "$TMP/drop-deploy-response.py" "http://127.0.0.1:$B_PORT" "$TMP/proxy-port" "$TMP/proxy-result" &
  PROXY_PID=$!
  for _ in $(seq 1 50); do [[ -s "$TMP/proxy-port" ]] && break; sleep 0.1; done
  [[ -s "$TMP/proxy-port" ]] || fail 'deploy fault proxy did not start'
  set +e
  curl -s -o /dev/null -X POST -H "x-cellhive-admin-token: $CLOUDFLARE_API_TOKEN" \
    -H 'content-type: application/json' --data-binary "@$TMP/reconcile-deploy.json" \
    "http://127.0.0.1:$(cat "$TMP/proxy-port")/v1/control/deploy"
  LOST_RESPONSE_RC=$?
  set -e
  wait "$PROXY_PID"
  [[ $LOST_RESPONSE_RC -ne 0 ]] || fail 'deploy client unexpectedly received a response'
  python3 - "$TMP/proxy-result" "$TMP/reconcile-expected.json" <<'PY'
import json,sys
result=json.load(open(sys.argv[1]))
expected=json.load(open(sys.argv[2]))['version']
assert result['status']==200, result
assert json.loads(result['body'])['number']==expected, result
PY
  RETRY_PORT=$A_PORT
  if [[ ${CELLHIVE_CF_DEPLOY_KILL_OWNER:-0} == 1 ]]; then
    # A created this app's control scope. Kill its actual server process after
    # the commit was acknowledged to the proxy but before the caller retries.
    CONTROL_OWNER=$(compose exec -T agent-a cat /data/bucket/cells/__platform__/__control__/main/owner.json | python3 -c 'import json,sys; print(json.load(sys.stdin)["node"])')
    [[ $CONTROL_OWNER == cf-tail-a ]] || fail "expected A to own control before SIGKILL, got $CONTROL_OWNER"
    echo "control owner before SIGKILL: $CONTROL_OWNER"
    compose kill -s SIGKILL agent-a >/dev/null
    RETRY_PORT=$B_PORT
    DEPLOY_RECOVERED=0
    for _ in $(seq 1 40); do
      if curl -fsS -H "Authorization: Bearer $CLOUDFLARE_API_TOKEN" \
        "http://127.0.0.1:$B_PORT/client/v4/accounts/e2e/workers/scripts/web/versions" \
        >"$TMP/versions-after-deploy-kill.json" 2>/dev/null; then
        if python3 - "$TMP/versions-after-deploy-kill.json" "$TMP/reconcile-expected.json" <<'PY'
import json,sys
items=json.load(open(sys.argv[1]))['result']['items']
expected=str(json.load(open(sys.argv[2]))['version'])
assert expected in {item['id'] for item in items}
PY
        then DEPLOY_RECOVERED=1; break; fi
      fi
      sleep 2
    done
    [[ $DEPLOY_RECOVERED == 1 ]] || fail 'B did not recover deployment after A SIGKILL'
  fi
  curl -fsS -H "x-cellhive-admin-token: $CLOUDFLARE_API_TOKEN" \
    -H 'content-type: application/json' --data-binary "@$TMP/reconcile-deploy.json" \
    "http://127.0.0.1:$RETRY_PORT/v1/control/deploy" >"$TMP/reconcile-retry.json"
  curl -fsS -H "Authorization: Bearer $CLOUDFLARE_API_TOKEN" \
    "http://127.0.0.1:$B_PORT/client/v4/accounts/e2e/workers/scripts/web/versions" >"$TMP/reconcile-versions.json"
  curl -fsS -H "Authorization: Bearer $CLOUDFLARE_API_TOKEN" \
    "http://127.0.0.1:$B_PORT/client/v4/accounts/e2e/workers/scripts/web/deployments" >"$TMP/reconcile-deployments.json"
  python3 - "$TMP/reconcile-retry.json" "$TMP/reconcile-versions.json" "$TMP/reconcile-deployments.json" "$TMP/reconcile-expected.json" <<'PY'
import json,sys
retry=json.load(open(sys.argv[1]))
items=json.load(open(sys.argv[2]))['result']['items']
events=json.load(open(sys.argv[3]))['result']['deployments']
expected=json.load(open(sys.argv[4]))
version=expected['version']
ids={item['id'] for item in items}
assert retry['number']==version, retry
assert ids==set(expected['before_ids'])|{str(version)}, items
assert sum(event['versions'][0]['version_id']==str(version) for event in events)==1, events
PY
  echo 'deploy reconciliation: caller lost response, retry returned the committed version, one deployment event'
fi
echo 'CF-TAIL-MULTINODE-E2E: PASS: original Wrangler on B received A fetch, Queue, Cron and Workflow Tail events'
