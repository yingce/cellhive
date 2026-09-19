#!/usr/bin/env bash
# End-to-end OTLP smoke: real opentelemetry-collector-contrib in front of a real
# cell-agent + user-runtime, a deployed worker that logs and writes KV, and
# assertions that the collector received traces, logs (with trace_id) and that
# /metrics carries the ns label (ADR-178/179).
#
# Requires: docker (image otel/opentelemetry-collector-contrib), curl, and
# bin/ built (`make build`). Self-contained: uses a temp dir and kills what it starts.
set -euo pipefail
SRC="$(cd "$(dirname "$0")/.." && pwd)"
export PATH="$PATH:/usr/local/go/bin"
make -C "$SRC" build >/dev/null   # smoke must exercise the current source, not a stale bin/
TMP="$(mktemp -d)"
RK=00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff
REST=18001; ADMIN=18002; UPUB=18081; UINT=18088
COLL=4318; NS=otlp; HOST="otlp.test"
cleanup() {
  set +e
  for f in cell-agent user-runtime; do [ -f "$TMP/$f.pid" ] && kill "$(cat "$TMP/$f.pid")" 2>/dev/null; done
  pgrep -a -x workerd | grep "$TMP" | awk '{print $1}' | xargs -r kill 2>/dev/null
  docker rm -f ch-otelcol-smoke >/dev/null 2>&1
  rm -rf "$TMP"
}
trap cleanup EXIT

cat > "$TMP/collector.yaml" <<YAML
receivers:
  otlp:
    protocols: { http: { endpoint: 0.0.0.0:$COLL } }
exporters:
  debug: { verbosity: detailed }
  file: { path: /out/collector.out }
service:
  pipelines:
    traces: { receivers: [otlp], exporters: [file] }
    logs:   { receivers: [otlp], exporters: [file] }
YAML
docker rm -f ch-otelcol-smoke >/dev/null 2>&1 || true
docker run -d --name ch-otelcol-smoke --network host --user 0:0 \
  -v "$TMP/collector.yaml:/etc/otelcol-contrib/config.yaml:ro" -v "$TMP:/out" \
  otel/opentelemetry-collector-contrib:latest --config=/etc/otelcol-contrib/config.yaml >/dev/null
for i in $(seq 1 30); do docker logs ch-otelcol-smoke 2>&1 | grep -q "Everything is ready" && break; sleep 0.5; done
echo "collector up on :$COLL"

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
    console.log("otlp-smoke log", new URL(req.url).pathname);
    await env.KV.put("k", "v-" + Date.now());
    const v = await env.KV.get("k");
    return new Response("kv:" + v);
  },
};
JS
cat > "$TMP/w/wrangler.jsonc" <<JSON
{ "name": "kvprobe", "main": "index.js", "compatibility_date": "2026-01-01",
  "kv_namespaces": [ { "binding": "KV", "id": "$NS/__kv__/KV" } ] }
JSON
"$SRC/bin/cellhive" deploy $NS - --config "$TMP/w/wrangler.jsonc" >/dev/null
"$SRC/bin/cellhive" domain add $NS $HOST >/dev/null
"$SRC/bin/cellhive" route add $NS $HOST kvprobe >/dev/null
TRACEPARENT=00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01
for i in $(seq 1 20); do r=$(curl -s -m 3 -H "Host: $HOST" -H "traceparent: $TRACEPARENT" http://127.0.0.1:$UPUB/hi || true); case "$r" in kv:v-*) break;; esac; sleep 0.5; done
echo "worker response: $r"
sleep 10   # let the OTLP batches flush (2s log interval / 5s span batch)

FAIL=0
check() { if grep -qF "$1" "$TMP/collector.out"; then echo "  ok: $2"; else echo "  MISSING: $2"; FAIL=1; fi; }
echo "=== collector assertions ==="
check "http.server" "span http.server"
grep -qE '"key":"cellhive.namespace","value":\{"stringValue":"'"$NS"'"\}' "$TMP/collector.out" && echo "  ok: span carries cellhive.namespace=$NS" || { echo "  MISSING: namespace span attribute"; FAIL=1; }
check "otlp-smoke log" "tenant log line"
# ADR-178 correlation: the exported log record inherits the caller's trace id.
grep -qF '"traceId":"4bf92f3577b34da6a3ce929d0e0e4736"' "$TMP/collector.out" && echo "  ok: traceparent propagated to exported span/log" || { echo "  MISSING: traceparent trace id"; FAIL=1; }
echo "=== ns metric ==="
curl -s http://127.0.0.1:$REST/metrics | grep -E "cellhive_binding_calls_total\{ns=\"$NS\"" || { echo "  MISSING ns label"; FAIL=1; }
if [ "$FAIL" = 0 ]; then echo "OTLP-SMOKE: PASS"; else echo "OTLP-SMOKE: FAIL"; tail -30 "$TMP/collector.out" 2>/dev/null; exit 1; fi
