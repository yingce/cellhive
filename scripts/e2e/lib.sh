#!/usr/bin/env bash
# Shared helpers for the in-repo e2e scripts.
# Requirements: bash, curl, jq, node, and a built bin/cell-agent + bin/user-runtime.
set -u

E2E_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
E2E_BIN="${E2E_BIN:-$E2E_ROOT/bin}"
E2E_PORT_BASE="${E2E_PORT_BASE:-19100}"
E2E_KEEP="${E2E_KEEP:-0}"
CELLHIVE_WORKERD="${CELLHIVE_WORKERD:-$E2E_ROOT/cli/node_modules/.bin/workerd}"
if [[ -z "${CELLHIVE_ROOT_KEY:-}" ]]; then
  CELLHIVE_ROOT_KEY="$(openssl rand -hex 32)"
fi
export CELLHIVE_ROOT_KEY CELLHIVE_WORKERD
if [[ ! -x "$CELLHIVE_WORKERD" ]]; then
  echo "missing workerd: $CELLHIVE_WORKERD" >&2
  exit 2
fi
for f in cell-agent user-runtime cellhive; do
  [[ -x "$E2E_BIN/$f" ]] || { echo "missing $E2E_BIN/$f (run make build)" >&2; exit 2; }
done
for c in curl jq node openssl; do
  command -v "$c" >/dev/null || { echo "missing $c" >&2; exit 2; }
done

CELLHIVE_ADMIN_TOKEN="$(node --input-type=module -e 'import {hkdfSync} from "node:crypto";const k=Buffer.from(process.env.CELLHIVE_ROOT_KEY,"hex");process.stdout.write(Buffer.from(hkdfSync("sha256",k,Buffer.alloc(0),"cellhive/token/admin",32)).toString("base64url"))')"
export CELLHIVE_ADMIN_TOKEN
export PATH="$E2E_BIN:$PATH"

E2E_BASE="$(mktemp -d "${TMPDIR:-/tmp}/cellhive-e2e.XXXXXX")"
E2E_AGENT_PORT=$((E2E_PORT_BASE))
E2E_ADMIN_PORT=$((E2E_PORT_BASE + 1))
E2E_RT_PORT=$((E2E_PORT_BASE + 2))
E2E_RT_INTERNAL=$((E2E_PORT_BASE + 3))

e2e_log() { printf '[e2e] %s\n' "$*"; }
e2e_fail() { printf '[e2e] FAIL: %s\n' "$*" >&2; exit 1; }
e2e_need() { [ "$1" = "$2" ] || e2e_fail "${3:-expected '$2', got '$1'}"; }

e2e_cleanup() {
  local pid f i
  # Stop runtimes before agents. A normal agent exit drains uploads and seals
  # its node-log; SIGKILL here strands the next HK run's recovery coordinates.
  for f in "$E2E_BASE"/rt*.pid "$E2E_BASE"/perf.pid "$E2E_BASE"/*.pid; do
    [[ -f "$f" ]] || continue
    pid=$(cat "$f" 2>/dev/null || true)
    [[ -n "$pid" ]] || continue
    if kill -0 "$pid" 2>/dev/null; then
      kill -TERM "$pid" 2>/dev/null || true
      for i in $(seq 1 90); do
        kill -0 "$pid" 2>/dev/null || break
        sleep 1
      done
      if kill -0 "$pid" 2>/dev/null; then
        e2e_log "cleanup failed: process $pid did not drain; retained for recovery"
        return 1
      fi
    fi
  done
  pkill -9 -f "workerd serve --experimental $E2E_BASE" 2>/dev/null || true
  if [[ "$E2E_KEEP" != "1" ]]; then
    rm -rf "$E2E_BASE"
  else
    e2e_log "kept $E2E_BASE"
  fi
}
trap e2e_cleanup EXIT

e2e_wait_http() { # url, attempts
  local url=$1 n=${2:-100} i code
  for i in $(seq 1 "$n"); do
    code=$(curl -s -o /dev/null -m 1 -w '%{http_code}' "$url" 2>/dev/null || true)
    [[ "$code" = 200 ]] && return 0
    sleep .2
  done
  return 1
}

e2e_start_agent() { # node-id agent-port admin-port dispatch-url data-dir
  local node=$1 port=$2 admin=$3 dispatch=$4 dir=$5
  mkdir -p "$dir"
  env CELLHIVE_NODE_ID="$node" CELLHIVE_ADVERTISE="127.0.0.1:$port" \
    CELLHIVE_PEER_URL="http://127.0.0.1:$port" CELLHIVE_REST_ADDR="127.0.0.1:$port" \
    CELLHIVE_ADMIN_ADDR="127.0.0.1:$admin" CELLHIVE_DATA_DIR="$dir" \
    CELLHIVE_BUCKET_DIR="$E2E_BASE/bucket" CELLHIVE_DISPATCH_URL="$dispatch" \
    CELLHIVE_QUEUE_INTERVAL=1s CELLHIVE_TIMER_INTERVAL=1s \
    setsid nohup "$E2E_BIN/cell-agent" >"$E2E_BASE/$node.log" 2>&1 </dev/null &
  echo $! >"$E2E_BASE/$node.pid"
  disown 2>/dev/null || true
  e2e_wait_http "http://127.0.0.1:$port/readyz" 120 || e2e_fail "cell-agent $node not ready (see $E2E_BASE/$node.log)"
}

e2e_start_runtime() { # name cell-url public-port internal-port
  local name=$1 cell=$2 pub=$3 int=$4
  mkdir -p "$E2E_BASE/$name"
  env CELLHIVE_CELL_URL="$cell" CELLHIVE_USER_RUNTIME_PORT="$pub" \
    CELLHIVE_USER_RUNTIME_INTERNAL_PORT="$int" CELLHIVE_RUNTIME_DIR="$E2E_BASE/$name" \
    CELLHIVE_USER_RUNTIME_JS="$E2E_ROOT/workerd/user-runtime" \
    CELLHIVE_FACADES_JS="$E2E_ROOT/workerd/platform/facades.js" \
    CELLHIVE_WORKERD="$CELLHIVE_WORKERD" \
    setsid nohup "$E2E_BIN/user-runtime" >"$E2E_BASE/$name.log" 2>&1 </dev/null &
  echo $! >"$E2E_BASE/$name.pid"
  disown 2>/dev/null || true
  e2e_wait_http "http://127.0.0.1:$pub/ready" 160 || e2e_fail "user-runtime $name not ready (see $E2E_BASE/$name.log)"
}

e2e_upload() { # file -> sha
  curl -sS -X POST -H "x-cellhive-admin-token: $CELLHIVE_ADMIN_TOKEN" --data-binary @"$1" \
    "http://127.0.0.1:$E2E_ADMIN_PORT/v1/control/bundle" | jq -r '.sha'
}

e2e_deploy() { # json-file
  curl -sS -X POST -H "x-cellhive-admin-token: $CELLHIVE_ADMIN_TOKEN" -H 'content-type: application/json' \
    --data-binary @"$1" "http://127.0.0.1:$E2E_ADMIN_PORT/v1/control/deploy"
}

e2e_cli() { CELLHIVE_ADMIN_URL="http://127.0.0.1:$E2E_ADMIN_PORT" "$E2E_BIN/cellhive" "$@"; }

e2e_http() { # public-port path [host]
  curl -sS -m 10 -H "Host: ${3:-e2e.test}" "http://127.0.0.1:$1$2"
}
