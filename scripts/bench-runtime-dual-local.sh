#!/usr/bin/env bash
set -euo pipefail
# Local standard: two cell-agents with one KV cell on each, two user-runtimes.
# Requires built bin/{cell-agent,user-runtime,cellhive} and pinned workerd/esbuild.
root=$(cd "$(dirname "$0")/.." && pwd)
cd "$root"
bench_bin=${CELLHIVE_BENCH_BIN:-$root/bin}
workerd_bin=${CELLHIVE_WORKERD_BIN:-$(command -v workerd || true)}
esbuild_bin=${CELLHIVE_ESBUILD_BIN:-$(command -v esbuild || true)}
for file in "$bench_bin/cell-agent" "$bench_bin/user-runtime" "$bench_bin/cellhive" "$workerd_bin" "$esbuild_bin"; do
  test -x "$file" || { echo "missing executable: $file" >&2; exit 1; }
done
for port in 17101 17102 18101 18102 18088 18089 18280 18281 18282; do
  if (echo > "/dev/tcp/127.0.0.1/$port") >/dev/null 2>&1; then
    echo "benchmark port already in use: $port" >&2
    exit 1
  fi
done
bench_tmp=$(mktemp -d /tmp/cellhive-local-rps.XXXXXX)
children=()
cleanup() {
  for pid in "${children[@]}"; do kill "$pid" 2>/dev/null || true; done
  for pid in "${children[@]}"; do wait "$pid" 2>/dev/null || true; done
  echo "STOPPED processes; benchmark artifacts=$bench_tmp"
}
trap cleanup EXIT

export CELLHIVE_ROOT_KEY=$(openssl rand -hex 32)
export CELLHIVE_ESBUILD="$esbuild_bin"
go build -tags 'sqlite_fts5 sqlite_dbstat sqlite_math_functions sqlite_column_metadata sqlite_preupdate_hook' -o "$bench_tmp/runtimebench" ./cmd/runtimebench
for n in 1 2; do
  rest=$((17100+n)); admin=$((18100+n))
  env CELLHIVE_NODE_ID="local-$n" CELLHIVE_ADVERTISE="127.0.0.1:$rest" \
    CELLHIVE_PEER_URL="http://127.0.0.1:$rest" CELLHIVE_REST_ADDR="127.0.0.1:$rest" \
    CELLHIVE_ADMIN_ADDR="127.0.0.1:$admin" CELLHIVE_DATA_DIR="$bench_tmp/agent$n" \
    CELLHIVE_BUCKET_DIR="$bench_tmp/bucket" "$bench_bin/cell-agent" \
    >"$bench_tmp/agent$n.log" 2>&1 & children+=("$!")
done
for i in $(seq 1 120); do
  if [ "$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:17101/readyz)" = 200 ] && \
     [ "$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:17102/readyz)" = 200 ]; then break; fi
  sleep 0.5
done
curl -fsS http://127.0.0.1:17101/readyz >/dev/null
curl -fsS http://127.0.0.1:17102/readyz >/dev/null
echo 'READY agents=2 FS bucket'
sleep 5

for n in 1 2; do
  pub=$((18280+n)); internal=$((18087+n))
  env CELLHIVE_CELL_URL="http://127.0.0.1:$((17100+n))" CELLHIVE_USER_RUNTIME_PORT="$pub" \
    CELLHIVE_USER_RUNTIME_INTERNAL_PORT="$internal" CELLHIVE_RUNTIME_DIR="$bench_tmp/runtime$n" \
    CELLHIVE_USER_RUNTIME_JS="$root/workerd/user-runtime" \
    CELLHIVE_FACADES_JS="$root/workerd/platform/facades.js" \
    CELLHIVE_WORKERD="$workerd_bin" "$bench_bin/user-runtime" \
    >"$bench_tmp/runtime$n.log" 2>&1 & children+=("$!")
done
for i in $(seq 1 120); do
  if [ "$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:18281/ready)" = 200 ] && \
     [ "$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:18282/ready)" = 200 ]; then break; fi
  sleep 0.5
done
curl -fsS http://127.0.0.1:18281/ready >/dev/null
curl -fsS http://127.0.0.1:18282/ready >/dev/null
echo 'READY runtimes=2'

export CELLHIVE_ADMIN_URL=http://127.0.0.1:18101 CELLHIVE_CONTROL_URL=http://127.0.0.1:17101
"$bench_bin/cellhive" app create loadbench >/dev/null
"$bench_bin/cellhive" resource create loadbench kv A --scope loadbench/__kv__/A >/dev/null
"$bench_bin/cellhive" resource create loadbench kv B --scope loadbench/__kv__/B >/dev/null
mkdir -p "$bench_tmp/worker"
cat > "$bench_tmp/worker/index.js" <<'WORKER'
export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const n = Number(url.searchParams.get('n') || '0');
    if (url.pathname === '/ping') return new Response('pong');
    if (url.pathname === '/fast') {
      const bytes = new Uint8Array(65536);
      bytes.fill(n & 255);
      return new Response('ok-' + bytes[123], {headers: {'content-type': 'text/plain'}});
    }
    const kv = n % 2 ? env.A : env.B;
    if (url.pathname === '/read') return new Response((await kv.get('hot')) || 'missing');
    if (url.pathname === '/write') {
      await kv.put('key-' + (n % 64), String(n));
      return new Response('ok');
    }
    return new Response('not found', {status: 404});
  }
};
WORKER
cat > "$bench_tmp/worker/wrangler.jsonc" <<'JSON'
{"name":"loadbench","main":"index.js","compatibility_date":"2026-01-01","kv_namespaces":[{"binding":"A","id":"loadbench/__kv__/A"},{"binding":"B","id":"loadbench/__kv__/B"}]}
JSON
"$bench_bin/cellhive" deploy loadbench - --config "$bench_tmp/worker/wrangler.jsonc" >/dev/null
"$bench_bin/cellhive" domain add loadbench bench.cellhive.test >/dev/null
"$bench_bin/cellhive" route add loadbench bench.cellhive.test loadbench >/dev/null
"$bench_bin/cellhive" kv key put loadbench A hot value-a >/dev/null
CELLHIVE_CONTROL_URL=http://127.0.0.1:17102 "$bench_bin/cellhive" kv key put loadbench B hot value-b >/dev/null
"$bench_tmp/runtimebench" proxy >"$bench_tmp/proxy.log" 2>&1 & children+=("$!")
for port in 18280 18281 18282; do
  for path in ping fast read write; do
    code=000
    for i in $(seq 1 60); do
      code=$(curl -s -o "$bench_tmp/response" -w '%{http_code}' -H 'Host: bench.cellhive.test' "http://127.0.0.1:$port/$path?n=1")
      [ "$code" = 200 ] && break
      sleep 0.5
    done
    echo "CHECK port=$port path=$path code=$code"
    if [ "$code" != 200 ]; then cat "$bench_tmp/response"; exit 1; fi
  done
done
echo 'OWNERSHIP'
for spec in '1 A' '2 B'; do
  read -r n resource <<< "$spec"
  test -f "$bench_tmp/agent$n/cells/loadbench/__kv__/$resource.db" || {
    echo "missing owner: agent$n $resource" >&2; exit 1;
  }
  echo "agent$n $resource.db"
done
echo "WORKERD $($workerd_bin --version 2>&1 | head -1)"
echo "HOST $(uptime)"
echo 'BEGIN local benchmark'
bench_specs=${CELLHIVE_BENCH_SPECS:-$'ping 64 30\nfast 64 30\nread 64 30\nwrite 16 30'}
while read -r path conc secs; do
  test -n "$path" || continue
  echo "TEST path=$path concurrency=$conc duration=${secs}s"
  "$bench_tmp/runtimebench" load -url "http://127.0.0.1:18280/$path" -host bench.cellhive.test -duration "${secs}s" -c "$conc"
done <<< "$bench_specs"
echo 'HEALTH after benchmark'
for port in 17101 17102; do curl -fsS "http://127.0.0.1:$port/readyz" >/dev/null && echo "agent:$port ready"; done
for port in 18281 18282; do curl -fsS "http://127.0.0.1:$port/ready" >/dev/null && echo "runtime:$port ready"; done
for n in 1 2; do
  find "$bench_tmp/agent$n/peer-spool" -name fragments.log -type f -printf "agent$n peer-spool %s bytes\n" 2>/dev/null || true
done
