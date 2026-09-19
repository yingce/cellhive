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
# Requires: docker, python3, curl. Self-contained; kills what it starts unless
# KEEP=1 (then the temp dir and containers are left for inspection).
set -euo pipefail
SRC="$(cd "$(dirname "$0")/.." && pwd)"
export PATH="$PATH:/usr/local/go/bin"
make -C "$SRC" build >/dev/null
TMP="$(mktemp -d)"; NET=ch-obs-e2e
OO_USER=root@example.com; OO_PASS='Complexpass#123'; OO_ORG=default
RK=00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff
REST=18001; ADMIN=18002; UPUB=18081; UINT=18088; OO=5080; COLL=4318
NS=obs; HOST="obs.test"
TP=00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01
cleanup() {
  set +e
  for f in cell-agent user-runtime; do [ -f "$TMP/$f.pid" ] && kill "$(cat "$TMP/$f.pid")" 2>/dev/null; done
  docker rm -f ch-oo-e2e ch-otelcol-e2e >/dev/null 2>&1
  docker network rm $NET >/dev/null 2>&1
  rm -rf "$TMP"
}
[ "${KEEP:-0}" = 1 ] || trap cleanup EXIT

mkdir -p "$TMP/oo" "$TMP/rt" "$TMP/bucket" "$TMP/data"
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
  "$SRC/bin/cell-agent" >"$TMP/cell-agent.log" 2>&1 & echo $! > "$TMP/cell-agent.pid"
"${COMMON[@]}" CELLHIVE_CELL_URL=http://127.0.0.1:$REST CELLHIVE_USER_RUNTIME_PORT=$UPUB \
  CELLHIVE_USER_RUNTIME_INTERNAL_PORT=$UINT CELLHIVE_USER_RUNTIME_JS=$SRC/workerd/user-runtime \
  CELLHIVE_FACADES_JS=$SRC/workerd/platform/facades.js \
  "$SRC/bin/user-runtime" >"$TMP/user-runtime.log" 2>&1 & echo $! > "$TMP/user-runtime.pid"
for i in $(seq 1 60); do
  [ "$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:$REST/readyz)" = 200 ] && \
  [ "$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:$UPUB/ready)" = 200 ] && break; sleep 0.5
done

export CELLHIVE_ADMIN_URL=http://127.0.0.1:$ADMIN CELLHIVE_CONTROL_URL=http://127.0.0.1:$REST CELLHIVE_ROOT_KEY=$RK
"$SRC/bin/cellhive" app create $NS >/dev/null
"$SRC/bin/cellhive" resource create $NS kv KV >/dev/null
mkdir -p "$TMP/w"
cat > "$TMP/w/index.js" <<'JS'
export default {
  async fetch(req, env) {
    const p = new URL(req.url).pathname;
    console.log("openobserve-e2e", p);
    await env.KV.put("k", "v-" + Date.now());
    // keep-slow (>1s) so the reference tail_sampling keeps this trace.
    if (p === "/slow") await new Promise((r) => setTimeout(r, 1500));
    return new Response("kv:" + (await env.KV.get("k")));
  },
};
JS
cat > "$TMP/w/wrangler.jsonc" <<JSON
{ "name": "obsprobe", "main": "index.js", "compatibility_date": "2026-01-01",
  "kv_namespaces": [ { "binding": "KV", "id": "$NS/__kv__/KV" } ] }
JSON
"$SRC/bin/cellhive" deploy $NS - --config "$TMP/w/wrangler.jsonc" >/dev/null
"$SRC/bin/cellhive" domain add $NS $HOST >/dev/null
"$SRC/bin/cellhive" route add $NS $HOST obsprobe >/dev/null
r=""; for i in $(seq 1 25); do r=$(curl -s -m3 -H "Host: $HOST" -H "traceparent: $TP" http://127.0.0.1:$UPUB/hi || true); case "$r" in kv:v-*) break;; esac; sleep 0.5; done
echo "fast response: $r"
r2=$(curl -s -m5 -H "Host: $HOST" -H "traceparent: $TP" http://127.0.0.1:$UPUB/slow || true)
echo "slow response: $r2"
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
hits,r=search(f"select * from \"default\" where cellhive_namespace='{ns}'","logs")
if hits and any(h.get("trace_id")==tid for h in hits):
    print(f"  ok: {len(hits)} log row(s), one carries trace_id={tid}"); print("  sample:",json.dumps(hits[0])[:260])
else:
    print("  FAIL logs:",json.dumps(r)[:300]); fail=1
print("=== traces: same trace retained by tail_sampling ===")
hits,r=search(f"select * from \"default\" where trace_id='{tid}'","traces")
if hits:
    names=[h.get("operation_name") or h.get("span_name") or h.get("name") for h in hits]
    print(f"  ok: {len(hits)} span(s) in trace {tid}; names={names}"); print("  sample:",json.dumps(hits[0])[:260])
else:
    print("  FAIL traces:",json.dumps(r)[:300]); fail=1
sys.exit(fail)
PY
rc=$?
[ "$rc" = 0 ] && echo "OPENOBSERVE-E2E: PASS" || { echo "OPENOBSERVE-E2E: FAIL"; exit 1; }
