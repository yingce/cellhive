#!/usr/bin/env bash
# Rehearse a workerd fork upgrade and rollback without mutating a deployment.
# Each candidate is checked with the same compatibility probe used by CI and its
# version/hash are recorded so the result is reproducible.
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
CURRENT=${CELLHIVE_WORKERD_FORK_CURRENT:-${1:-}}
ROLLBACK=${CELLHIVE_WORKERD_FORK_ROLLBACK:-${2:-}}
OUT=${CELLHIVE_WORKERD_REHEARSAL_LOG:-}

if [[ -z "$CURRENT" || -z "$ROLLBACK" ]]; then
  cat >&2 <<'USAGE'
usage: CELLHIVE_WORKERD_FORK_CURRENT=/path/new \
       CELLHIVE_WORKERD_FORK_ROLLBACK=/path/old \
       bash scripts/workerd-fork-upgrade-rehearsal.sh

The two binaries are only started by the compatibility probe; no service,
database, or object-store state is changed.
USAGE
  exit 2
fi

for binary in "$CURRENT" "$ROLLBACK"; do
  [[ -x "$binary" ]] || { echo "not executable: $binary" >&2; exit 1; }
done

if [[ -z "$OUT" ]]; then
  OUT=$(mktemp "${TMPDIR:-/tmp}/cellhive-workerd-rehearsal.XXXXXX.log")
  cleanup() { rm -f "$OUT"; }
  trap cleanup EXIT
fi

run_candidate() {
  local label=$1 binary=$2 version sha
  version=$($binary --version 2>&1)
  sha=$(sha256sum "$binary" | awk '{print $1}')
  {
    echo "[$label] binary=$binary"
    echo "[$label] version=$version"
    echo "[$label] sha256=$sha"
    CELLHIVE_WORKERD="$binary" bash "$ROOT/scripts/workerd-compat-probe.sh"
    echo "[$label] result=PASS"
  } | tee -a "$OUT"
}

: >"$OUT"
run_candidate upgrade "$CURRENT"
run_candidate rollback "$ROLLBACK"
echo "workerd fork upgrade/rollback rehearsal passed; log=$OUT"
