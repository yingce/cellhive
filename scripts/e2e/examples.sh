#!/usr/bin/env bash
# Exercise checked-in example sources through the production CLI and runtimes.
set -euo pipefail
source "$(dirname "$0")/lib.sh"
export CELLHIVE_SERVICE_NATIVE=1
export CELLHIVE_AI_URL="http://127.0.0.1:$((E2E_PORT_BASE+9))"
export CELLHIVE_AI_KEY=example-fixture-key
export CELLHIVE_DO_RUNTIMES="http://127.0.0.1:$((E2E_PORT_BASE+6))"
python3 "$E2E_ROOT/scripts/e2e/examples.py" ai "$((E2E_PORT_BASE+9))" > "$E2E_BASE/ai.log" 2>&1 &
echo $! > "$E2E_BASE/ai.pid"
e2e_start_agent examples "$E2E_AGENT_PORT" "$E2E_ADMIN_PORT" "http://127.0.0.1:$E2E_RT_INTERNAL" "$E2E_BASE/agent"
e2e_start_runtime rt "http://127.0.0.1:$E2E_AGENT_PORT" "$E2E_RT_PORT" "$E2E_RT_INTERNAL"
e2e_cli app create demo >/dev/null
for spec in 'kv KV' 'd1 DB' 'r2 FILES'; do read -r kind name <<<"$spec"; e2e_cli resource create demo "$kind" "$name" >/dev/null; done
for name in JOBS DLQ; do e2e_cli queue create demo "$name" >/dev/null; done
e2e_cli workflow create demo report-builder >/dev/null
e2e_cli vectorize create demo VEC --dimensions 3 --metric cosine >/dev/null
e2e_cli resource create demo hyperdrive HYDR --connection-string 'postgres://example:example@127.0.0.1:5432/example' >/dev/null
mkdir -p "$E2E_BASE/do"
env -u CELLHIVE_BUCKET -u CELLHIVE_BUCKET_DIR \
 CELLHIVE_CELL_URL="http://127.0.0.1:$E2E_AGENT_PORT" \
 CELLHIVE_DO_ADDR="127.0.0.1:$((E2E_PORT_BASE+6))" CELLHIVE_DO_NODE=examples-do \
 CELLHIVE_DO_ADVERTISE="http://127.0.0.1:$((E2E_PORT_BASE+6))" \
 CELLHIVE_DATA_DIR="$E2E_BASE/do" CELLHIVE_RUNTIME_DIR="$E2E_BASE/do/render" \
 CELLHIVE_DO_RUNTIME_JS="$E2E_ROOT/workerd/do-runtime" \
 CELLHIVE_DO_GATE_URL="http://127.0.0.1:$((E2E_PORT_BASE+8))" \
 E2E_GATE="$((E2E_PORT_BASE+8))" E2E_DO_PORT="$((E2E_PORT_BASE+6))" \
 setsid bash -c '
 cfg=$("$E2E_BIN/do-runtime" -render-only)
 exec "$E2E_BIN/do-supervisor" -workerd "$CELLHIVE_WORKERD" -config "$cfg" \
 -dir "$CELLHIVE_DATA_DIR/do" -scope-prefix demo/__do__ -listen "127.0.0.1:$E2E_GATE" \
 -do-url "http://127.0.0.1:$E2E_DO_PORT" -owner "$CELLHIVE_CELL_URL"
 ' > "$E2E_BASE/do.log" 2>&1 &
echo $! > "$E2E_BASE/do.pid"
e2e_wait_http "http://127.0.0.1:$((E2E_PORT_BASE+6))/ready" 120 || e2e_fail 'DO startup'
export E2E_BASE E2E_ROOT E2E_ADMIN_PORT E2E_RT_PORT
python3 "$E2E_ROOT/scripts/e2e/examples.py" run

if [[ ${E2E_RUN_DO_RPS:-0} == 1 ]]; then
  CELLHIVE_ADMIN_URL="http://127.0.0.1:$E2E_ADMIN_PORT" DO_RPS_URL="http://127.0.0.1:$E2E_RT_PORT" \
    bash "$E2E_ROOT/scripts/e2e/do-runtime-rps.sh"
fi
