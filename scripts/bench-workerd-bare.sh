#!/usr/bin/env bash
set -euo pipefail
# Direct stock-workerd baseline: no cell-agent, user-runtime, Go proxy or Go load driver.
workerd_bin=${CELLHIVE_WORKERD_BIN:-$(command -v workerd || true)}
test -x "$workerd_bin" || { echo "missing workerd executable: $workerd_bin" >&2; exit 1; }
command -v wrk >/dev/null || { echo 'wrk is required' >&2; exit 1; }
for port in 19081 19082; do
  if (echo > "/dev/tcp/127.0.0.1/$port") >/dev/null 2>&1; then
    echo "benchmark port already in use: $port" >&2; exit 1
  fi
done
run_dir=$(mktemp -d /tmp/cellhive-bare-workerd.XXXXXX)
children=()
cleanup() {
  for pid in "${children[@]}"; do kill "$pid" 2>/dev/null || true; done
  for pid in "${children[@]}"; do wait "$pid" 2>/dev/null || true; done
  echo "ARTIFACTS=$run_dir"
}
trap cleanup EXIT
cat > "$run_dir/worker.js" <<'JS'
export default {
  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === '/ping') return new Response('pong');
    if (url.pathname === '/fast') {
      const bytes = new Uint8Array(65536);
      bytes.fill(1);
      return new Response('ok-' + bytes[123], {headers: {'content-type': 'text/plain'}});
    }
    return new Response('not found', {status: 404});
  }
};
JS
for n in 1 2; do
  port=$((19080+n))
  cat > "$run_dir/workerd$n.capnp" <<EOF
using W = import "/workerd/workerd.capnp";
const config :W.Config = (
  services = [(name = "main", worker = (
    modules = [(name = "worker.js", esModule = embed "worker.js")],
    compatibilityDate = "2026-01-01",
  ))],
  sockets = [(name = "http", address = "127.0.0.1:$port", http = (), service = "main")],
);
EOF
  "$workerd_bin" serve "$run_dir/workerd$n.capnp" >"$run_dir/workerd$n.log" 2>&1 & children+=("$!")
done
for port in 19081 19082; do
  for i in $(seq 1 60); do
    if [ "$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$port/ping")" = 200 ]; then break; fi
    sleep 0.25
  done
  for path in ping fast; do
    body=$(curl -fsS "http://127.0.0.1:$port/$path")
    echo "CHECK port=$port path=$path body=$body"
  done
done
echo "WORKERD $($workerd_bin --version)"
echo "HOST $(uptime)"
for path in ping fast; do
  echo "SINGLE path=$path threads=4 connections=64 duration=30s"
  wrk -t4 -c64 -d30s --latency "http://127.0.0.1:19081/$path" >"$run_dir/single-$path.txt" 2>&1 & p=$!; children+=("$p")
  if command -v pidstat >/dev/null; then pidstat -u -p "${children[0]},${children[1]}" 2 3 >"$run_dir/cpu-single-$path.txt" 2>&1 || true; fi
  wait "$p"
  cat "$run_dir/single-$path.txt"
  test ! -f "$run_dir/cpu-single-$path.txt" || tail -5 "$run_dir/cpu-single-$path.txt"
done
for path in ping fast; do
  echo "DUAL path=$path each_threads=2 each_connections=32 duration=30s"
  wrk -t2 -c32 -d30s --latency "http://127.0.0.1:19081/$path" >"$run_dir/dual-a-$path.txt" 2>&1 & a=$!; children+=("$a")
  wrk -t2 -c32 -d30s --latency "http://127.0.0.1:19082/$path" >"$run_dir/dual-b-$path.txt" 2>&1 & b=$!; children+=("$b")
  if command -v pidstat >/dev/null; then pidstat -u -p "${children[0]},${children[1]}" 2 3 >"$run_dir/cpu-dual-$path.txt" 2>&1 || true; fi
  wait "$a"; wait "$b"
  cat "$run_dir/dual-a-$path.txt"
  cat "$run_dir/dual-b-$path.txt"
  test ! -f "$run_dir/cpu-dual-$path.txt" || tail -5 "$run_dir/cpu-dual-$path.txt"
done
