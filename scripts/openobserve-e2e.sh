#!/usr/bin/env bash
# Real OpenObserve end-to-end: the repo's reference otel-collector config in
# front of OpenObserve, a real cell-agent + user-runtime + probe worker, and
# queries against the OpenObserve API proving the backend hop stores and serves
# the tenant's logs and traces (ADR-178/179 backend hop).
#
#   logs   : the collector logs pipeline has no sampler, so every line lands.
#   traces : the reference config tail-samples (errors / >1s / 1%); the probe
#            therefore makes one slow (>1s) request so keep-slow retains the
#            whole trace.
#
# Requires: docker, python3, curl, and the pinned cellhive:runtime-baseline
# image (for esbuild). Self-contained; kills what it starts unless
# KEEP=1 (then the temp dir and containers are left for inspection).
set -euo pipefail
SRC="$(cd "$(dirname "$0")/.." && pwd)"
export PATH="$PATH:/usr/local/go/bin"
make -C "$SRC" build >/dev/null
TMP="$(mktemp -d)"; NET=ch-obs-e2e
OO_USER=root@example.com; OO_PASS='Complexpass#123'; OO_ORG=default
RK=00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff
REST=${CELLHIVE_OBS_REST_PORT:-18001}; ADMIN=${CELLHIVE_OBS_ADMIN_PORT:-18002}
UPUB=${CELLHIVE_OBS_PUBLIC_PORT:-18081}; UINT=${CELLHIVE_OBS_INTERNAL_PORT:-18088}
OO=${CELLHIVE_OBS_BACKEND_PORT:-5080}; COLL=${CELLHIVE_OBS_COLLECTOR_PORT:-4318}
NS=obs; HOST="obs.test"
TP=00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01
cleanup() {
  local rc=$?
  set +e
  if [ "$rc" -ne 0 ]; then
    for f in cell-agent user-runtime; do [ ! -f "$TMP/$f.log" ] || { echo "== $f log ==" >&2; tail -n 80 "$TMP/$f.log" >&2; }; done
  fi
  for f in cell-agent user-runtime; do [ -f "$TMP/$f.pid" ] && kill "$(cat "$TMP/$f.pid")" 2>/dev/null; done
  docker rm -f ch-oo-e2e ch-otelcol-e2e >/dev/null 2>&1
  docker network rm $NET >/dev/null 2>&1
  rm -rf "$TMP"
}
[ "${KEEP:-0}" = 1 ] || trap cleanup EXIT

mkdir -p "$TMP/oo" "$TMP/rt" "$TMP/bucket" "$TMP/data"
TOOLS_CONTAINER=$(docker create cellhive:runtime-baseline)
docker cp "$TOOLS_CONTAINER:/usr/local/bin/esbuild" "$TMP/esbuild"
docker rm "$TOOLS_CONTAINER" >/dev/null
chmod 0755 "$TMP/esbuild"
export CELLHIVE_ESBUILD="$TMP/esbuild"
docker network create $NET >/dev/null
docker run -d --name ch-oo-e2e --network $NET --network-alias openobserve --user 0:0 \
  -e ZO_ROOT_USER_EMAIL=$OO_USER -e ZO_ROOT_USER_PASSWORD=$OO_PASS -e ZO_TELEMETRY=false \
  -v "$TMP/oo:/data" -p $OO:5080 public.ecr.aws/zinclabs/openobserve:latest >/dev/null
for i in $(seq 1 60); do curl -sf "http://127.0.0.1:$OO/healthz" >/dev/null 2>&1 && break; sleep 1; done
echo "openobserve up on :$OO"

AUTH=$(printf '%s:%s' "$OO_USER" "$OO_PASS" | base64 | tr -d '\n')
docker run -d --name ch-otelcol-e2e --network $NET --user 0:0 \
  -e OPENOBSERVE_BASIC_AUTH="$AUTH" -p $COLL:4318 \
  -v "$SRC/deploy/observability/otel-collector.yaml:/etc/otelcol-contrib/config.yaml:ro" \
  otel/opentelemetry-collector-contrib:latest --config=/etc/otelcol-contrib/config.yaml >/dev/null
for i in $(seq 1 40); do docker logs ch-otelcol-e2e 2>&1 | grep -q "Everything is ready" && break; sleep 0.5; done
docker logs ch-otelcol-e2e 2>&1 | grep -q "Everything is ready" || { echo "collector failed:"; docker logs ch-otelcol-e2e 2>&1 | tail -5; exit 1; }
echo "collector up on :$COLL -> openobserve/$OO_ORG"

COMMON=(env CELLHIVE_ROOT_KEY=$RK CELLHIVE_RUNTIME_DIR=$TMP/rt
  CELLHIVE_OTLP_ENDPOINT=http://127.0.0.1:$COLL CELLHIVE_OTLP_LOGS=all CELLHIVE_TRACES_SAMPLE_RATIO=1)
"${COMMON[@]}" CELLHIVE_NODE_ID=n1 CELLHIVE_ADVERTISE=127.0.0.1:$REST CELLHIVE_REST_ADDR=:$REST \
  CELLHIVE_ADMIN_ADDR=:$ADMIN CELLHIVE_BUCKET_DIR=$TMP/bucket CELLHIVE_DATA_DIR=$TMP/data \
  CELLHIVE_DISPATCH_URL=http://127.0.0.1:$UINT \
  "$SRC/bin/cell-agent" >"$TMP/cell-agent.log" 2>&1 & echo $! > "$TMP/cell-agent.pid"
"${COMMON[@]}" CELLHIVE_CELL_URL=http://127.0.0.1:$REST CELLHIVE_USER_RUNTIME_PORT=$UPUB \
  CELLHIVE_USER_RUNTIME_INTERNAL_PORT=$UINT CELLHIVE_USER_RUNTIME_JS=$SRC/workerd/user-runtime \
  CELLHIVE_FACADES_JS=$SRC/workerd/platform/facades.js \
  "$SRC/bin/user-runtime" >"$TMP/user-runtime.log" 2>&1 & echo $! > "$TMP/user-runtime.pid"
for i in $(seq 1 60); do
  [ "$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:$REST/readyz)" = 200 ] && \
  [ "$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:$UPUB/ready)" = 200 ] && break; sleep 0.5
done
if [ "$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:$REST/readyz)" != 200 ] || \
   [ "$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:$UPUB/ready)" != 200 ]; then
  echo "cell-agent or user-runtime did not become ready" >&2
  exit 1
fi

export CELLHIVE_ADMIN_URL=http://127.0.0.1:$ADMIN CELLHIVE_CONTROL_URL=http://127.0.0.1:$REST CELLHIVE_ROOT_KEY=$RK
"$SRC/bin/cellhive" app create $NS >/dev/null
"$SRC/bin/cellhive" resource create $NS kv KV >/dev/null
"$SRC/bin/cellhive" resource create $NS queue JOBS >/dev/null
"$SRC/bin/cellhive" workflows create $NS FLOW >/dev/null
mkdir -p "$TMP/w"
cat > "$TMP/w/index.js" <<'JS'
import { WorkflowEntrypoint } from "cloudflare:workers";

export class ProbeFlow extends WorkflowEntrypoint {
  async run(event, step) {
    console.warn("openobserve-workflow", { marker: event.payload.marker });
    await step.do("write", async () => {
      await this.env.KV.put("workflow-done", "yes");
      return "ok";
    });
    return "ok";
  }
}

export default {
  async fetch(req, env) {
    const p = new URL(req.url).pathname;
    if (p === "/enqueue") {
      await env.JOBS.send({ marker: "async-otel-probe" });
      return new Response("queued");
    }
    if (p === "/status") {
      return Response.json({ queue: await env.KV.get("queue-done"), cron: await env.KV.get("cron-done"), workflow: await env.KV.get("workflow-done") });
    }
    if (p === "/workflow-start") {
      const instance = await env.FLOW.create({ params: { marker: "async-otel-probe" } });
      return Response.json({ id: instance.id });
    }
    console.log("openobserve-e2e", { path: p, structured: true, tenantTrace: req.headers.get("traceparent") });
    await env.KV.put("k", "v-" + Date.now());
    // keep-slow (>1s) so the reference tail_sampling keeps this trace.
    if (p === "/slow") await new Promise((r) => setTimeout(r, 1500));
    return new Response("kv:" + (await env.KV.get("k")));
  },
  async queue(batch, env) {
    console.warn("openobserve-queue", { count: batch.messages.length });
    // Keep this consumer trace under the reference Collector's >1s policy so
    // the backend can verify standard OTLP links for a mixed-producer batch.
    await new Promise((resolve) => setTimeout(resolve, 1200));
    await env.KV.put("queue-done", "yes");
  },
  async scheduled(event, env) {
    console.warn("openobserve-cron", { cron: event.cron });
    await env.KV.put("cron-done", "yes");
  },
};
JS
cat > "$TMP/w/wrangler.jsonc" <<JSON
{ "name": "obsprobe", "main": "index.js", "compatibility_date": "2026-01-01",
  "kv_namespaces": [ { "binding": "KV", "id": "$NS/__kv__/KV" } ],
  "queues": { "producers": [ { "binding": "JOBS", "queue": "JOBS" } ],
    "consumers": [ { "queue": "JOBS", "max_batch_size": 2, "max_batch_timeout": 10 } ] },
  "workflows": [ { "binding": "FLOW", "name": "FLOW", "class_name": "ProbeFlow" } ],
  "triggers": { "crons": [ "* * * * *" ] } }
JSON
"$SRC/bin/cellhive" deploy $NS - --config "$TMP/w/wrangler.jsonc" >/dev/null
"$SRC/bin/cellhive" domain add $NS $HOST >/dev/null
"$SRC/bin/cellhive" route add $NS $HOST obsprobe >/dev/null
r=""; for i in $(seq 1 25); do r=$(curl -s -m3 -H "Host: $HOST" -H "traceparent: $TP" -H 'x-request-id: obs-request-1' http://127.0.0.1:$UPUB/hi || true); case "$r" in kv:v-*) break;; esac; sleep 0.5; done
echo "fast response: $r"
r2=$(curl -s -m5 -H "Host: $HOST" -H "traceparent: $TP" -H 'x-request-id: obs-request-2' http://127.0.0.1:$UPUB/slow || true)
echo "slow response: $r2"
QUEUE_TP=00-11111111111111111111111111111111-2222222222222222-01
QUEUE_TP2=00-33333333333333333333333333333333-4444444444444444-01
queued=$(curl -fsS -H "Host: $HOST" -H "traceparent: $QUEUE_TP" -H 'x-request-id: obs-queue-producer' http://127.0.0.1:$UPUB/enqueue)
[ "$queued" = queued ] || { echo "queue send failed: $queued"; exit 1; }
queued2=$(curl -fsS -H "Host: $HOST" -H "traceparent: $QUEUE_TP2" -H 'x-request-id: obs-queue-producer-2' http://127.0.0.1:$UPUB/enqueue)
[ "$queued2" = queued ] || { echo "second queue send failed: $queued2"; exit 1; }
workflow_start=$(curl -fsS -H "Host: $HOST" http://127.0.0.1:$UPUB/workflow-start)
python3 -c 'import json,sys; assert json.loads(sys.argv[1]).get("id")' "$workflow_start"
async_ready=0
for i in $(seq 1 80); do
  status=$(curl -fsS -H "Host: $HOST" http://127.0.0.1:$UPUB/status)
  if python3 -c 'import json,sys; s=json.loads(sys.argv[1]); sys.exit(0 if s.get("queue")=="yes" and s.get("cron")=="yes" and s.get("workflow")=="yes" else 1)' "$status"; then async_ready=1; break; fi
  sleep 1
done
[ "$async_ready" = 1 ] || { echo "async handlers did not finish: $status"; exit 1; }
echo "async response: $status"
echo "waiting for tail_sampling(10s)+batch(5s) ..."; sleep 22

python3 - "$OO" "$OO_ORG" "$OO_USER" "$OO_PASS" "$NS" "$TP" <<'PY'
import json,sys,base64,time,urllib.request,urllib.error
oo,org,user,pw,ns,tp=sys.argv[1:7]
tid=tp.split("-")[1]
auth="Basic "+base64.b64encode(f"{user}:{pw}".encode()).decode()
now=int(time.time()*1e6); start=now-int(3*3600*1e6)
def api(path, body=None):
    req=urllib.request.Request(f"http://127.0.0.1:{oo}/api/{org}{path}",
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Content-Type":"application/json","Authorization":auth})
    try: return urllib.request.urlopen(req,timeout=30).read()
    except urllib.error.HTTPError as e: return json.dumps({"code":e.code,"message":e.read()[:200].decode()}).encode()
streams=json.loads(api("/streams"))
lst=streams.get("list",streams) if isinstance(streams,dict) else streams
print("=== streams ===")
for s in lst: print("  ",s.get("name"),s.get("stream_type"))
def search(sql, stype):
    r=json.loads(api("/_search?type="+stype,{"query":{"sql":sql,"start_time":start,"end_time":now,"from":0,"size":20}}))
    if "hits" not in r: return None,r
    return r["hits"],r
fail=0
print("=== logs: tenant namespace + trace id ===")
hits,r=search(f"select * from \"default\" where cellhive_namespace='{ns}' and cellhive_worker='obsprobe' and cellhive_request_id='obs-request-2' and cellhive_trace_id='{tid}'","logs")
matching=[h for h in (hits or []) if h.get("cellhive_trace_id")==tid and h.get("cellhive_request_id")=="obs-request-2" and h.get("cellhive_event_type")=="fetch" and h.get("cellhive_outcome")=="ok" and h.get("cellhive_script_version")]
structured=[h for h in matching if h.get("body","").startswith("openobserve-e2e") and (lambda a: len(a)==2 and a[1].get("path")=="/slow" and a[1].get("structured")==True)(json.loads(h.get("cellhive_message_args","[]")))]
if structured:
    print(f"  ok: {len(hits)} log row(s), structured args/version/request id/trace id matched"); print("  sample:",json.dumps(structured[0])[:450])
else:
    print("  FAIL logs:",json.dumps(r)[:1400]); fail=1
for kind in ("queue","cron","workflow"):
    async_hits,async_result=search(f"select * from \"default\" where cellhive_namespace='{ns}' and cellhive_event_type='{kind}' and body='invocation'","logs")
    async_rows=[h for h in (async_hits or []) if h.get("cellhive_request_id") and h.get("cellhive_trace_id") and h.get("cellhive_outcome")=="success"]
    if async_rows:
        print(f"  ok: {kind} invocation request id and trace id matched; sample:",json.dumps(async_rows[0])[:350])
        if kind=="queue":
            expected={"obs-queue-producer":"00-11111111111111111111111111111111-2222222222222222-01",
                      "obs-queue-producer-2":"00-33333333333333333333333333333333-4444444444444444-01"}
            mixed=[]
            for row in async_rows:
                links=json.loads(row.get("cellhive_producer_links") or "[]")
                if len(links)==2 and len({link.get("message_id") for link in links})==2 and {link.get("request_id"):link.get("traceparent") for link in links}==expected:
                    mixed.append(row)
            if len(mixed)!=1:
                print("  FAIL mixed Queue producer links:",json.dumps(async_rows)[:1600]); fail=1
            else:
                queue_trace_id=mixed[0]["cellhive_trace_id"]
                print("  ok: one Queue batch linked two producer traces; consumer trace:",queue_trace_id)
    else:
        print(f"  FAIL {kind} invocation correlation:",json.dumps(async_result)[:1400]); fail=1
print("=== traces: same trace retained by tail_sampling ===")
hits,r=search(f"select * from \"default\" where trace_id='{tid}'","traces")
if hits:
    names=[h.get("operation_name") or h.get("span_name") or h.get("name") for h in hits]
    print(f"  ok: {len(hits)} span(s) in trace {tid}; names={names}"); print("  sample:",json.dumps(hits[0])[:260])
else:
    print("  FAIL traces:",json.dumps(r)[:300]); fail=1
if 'queue_trace_id' in locals():
    queue_spans,queue_result=search(f"select * from \"default\" where trace_id='{queue_trace_id}'","traces")
    consumer=[h for h in (queue_spans or []) if (h.get("operation_name") or h.get("span_name") or h.get("name"))=="queue.consume"]
    if len(consumer)!=1:
        print("  FAIL Queue consumer span:",json.dumps(queue_result)[:1600]); fail=1
    else:
        span=consumer[0]
        try: span_links=json.loads(span.get("links") or "[]")
        except (TypeError,ValueError): span_links=[]
        got={(link.get("cellhive.producer_request_id"),link.get("context",{}).get("traceId"),link.get("context",{}).get("spanId"),link.get("messaging.message.id")) for link in span_links}
        message_ids={link["request_id"]:link["message_id"] for link in json.loads(mixed[0]["cellhive_producer_links"])}
        want={("obs-queue-producer","11111111111111111111111111111111","2222222222222222",message_ids["obs-queue-producer"]),
              ("obs-queue-producer-2","33333333333333333333333333333333","4444444444444444",message_ids["obs-queue-producer-2"])}
        if got!=want or span.get("parent_span_id"):
            print("  FAIL Queue consumer span links/parent:",json.dumps(span)[:1800]); fail=1
        else:
            print("  ok: Queue consumer span has two per-message OTLP links and no parent:",json.dumps(span)[:1400])
sys.exit(fail)
PY
rc=$?
[ "$rc" = 0 ] && echo "OPENOBSERVE-E2E: PASS" || { echo "OPENOBSERVE-E2E: FAIL"; exit 1; }
