#!/bin/sh
# do-runtime with the ADR-083 output gate (DO RPO=0): render the workerd config
# with the gate URL pointing at the local supervisor, then let do-supervisor
# spawn workerd and gate every response on cell-agent durability (it also owns
# lease renew/drain). Select with `command: ["do-runtime-gated"]`.
set -e

: "${CELLHIVE_DATA_DIR:=/data/state}"
gate="${CELLHIVE_DO_GATE_LISTEN:-:18901}"
# The host actor calls the gate at this URL; keep it on loopback.
export CELLHIVE_DO_GATE_URL="${CELLHIVE_DO_GATE_URL:-http://127.0.0.1${gate}}"

cfg="$(/usr/local/bin/do-runtime -render-only)"

exec /usr/local/bin/do-supervisor \
  -workerd "${CELLHIVE_WORKERD:-/usr/local/bin/workerd}" \
  -config "$cfg" \
  -dir "${CELLHIVE_DATA_DIR}/do" \
  -listen "$gate" \
  -do-url "http://127.0.0.1:${CELLHIVE_DO_PORT:-8788}" \
  -owner "${CELLHIVE_CELL_URL:-http://127.0.0.1:7001}"
