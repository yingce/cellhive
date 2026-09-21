#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
MANIFEST="$ROOT/internal/workerdcompat/manifest.json"
WORKERD=${CELLHIVE_WORKERD:-}

if [[ -z "$WORKERD" ]]; then
  echo "CELLHIVE_WORKERD must name the pinned workerd binary" >&2
  exit 1
fi
if [[ ! -x "$WORKERD" ]]; then
  echo "workerd binary is not executable: $WORKERD" >&2
  exit 1
fi

VERSION=$(sed -n 's/.*"workerd_version": "\([^"]*\)".*/\1/p' "$MANIFEST")
MAX_DATE=$(sed -n 's/.*"max_compatibility_date": "\([^"]*\)".*/\1/p' "$MANIFEST")
EXPECTED_DATE=${VERSION#1.}
EXPECTED_DATE=${EXPECTED_DATE%.*}
EXPECTED_DATE="${EXPECTED_DATE:0:4}-${EXPECTED_DATE:4:2}-${EXPECTED_DATE:6:2}"
ACTUAL_VERSION=$($WORKERD --version 2>&1)
if [[ "$ACTUAL_VERSION" != "workerd $EXPECTED_DATE" ]]; then
  echo "workerd version mismatch: manifest=$VERSION expected-date=$EXPECTED_DATE actual=$ACTUAL_VERSION" >&2
  exit 1
fi

TMP=$(mktemp -d "${TMPDIR:-/tmp}/cellhive-workerd-compat.XXXXXX")
trap 'rm -rf "$TMP"' EXIT

write_config() {
  local date=$1
  local flag=${2:-}
  local flags=""
  if [[ -n "$flag" ]]; then
    flags=", compatibilityFlags = [\"$flag\"]"
  fi
  cat >"$TMP/probe.capnp" <<EOF
using Workerd = import "/workerd/workerd.capnp";
const config :Workerd.Config = (
  services = [(name = "main", worker = (
    modules = [(name = "worker.js", esModule = "export default { fetch() { return new Response('ok'); } };")],
    compatibilityDate = "$date"$flags,
  ))],
  sockets = [(name = "probe", address = "127.0.0.1:0", http = (), service = "main")],
);
EOF
}

serve_ok() {
  local date=$1
  local flag=${2:-}
  write_config "$date" "$flag"
  set +e
  timeout 0.25s "$WORKERD" serve "$TMP/probe.capnp" >"$TMP/output" 2>"$TMP/error"
  local rc=$?
  set -e
  if [[ $rc -ne 124 ]]; then
	 echo "workerd rejected expected-compatible date=$date flag=${flag:-<none>}" >&2
    sed -n '1,80p' "$TMP/error" >&2
    return 1
  fi
}

serve_rejected() {
  local date=$1
  local flag=${2:-}
  local expected=$3
  write_config "$date" "$flag"
  set +e
  timeout 2s "$WORKERD" serve "$TMP/probe.capnp" >"$TMP/output" 2>"$TMP/error"
  local rc=$?
  set -e
  if [[ $rc -eq 0 || $rc -eq 124 ]]; then
    echo "workerd accepted rejected probe date=$date flag=${flag:-<none>}" >&2
    return 1
  fi
  if ! grep -Fq "$expected" "$TMP/error"; then
    echo "workerd failed for the wrong reason date=$date flag=${flag:-<none>}" >&2
    sed -n '1,80p' "$TMP/error" >&2
    return 1
  fi
}

UTC_TODAY=$(date -u +%F)
PROBE_DATE=$MAX_DATE
if [[ "$UTC_TODAY" < "$MAX_DATE" ]]; then
  PROBE_DATE=$UTC_TODAY
fi
serve_ok "$PROBE_DATE"
NEXT_DATE=$(date -u -d "$MAX_DATE +1 day" +%F)
serve_rejected "$NEXT_DATE" "" "newest date supported"

mapfile -t ALLOWED_FLAGS < <(awk '
  /"name":/ { name=$0; sub(/^.*"name": "/, "", name); sub(/".*$/, "", name) }
  /"cellhive_allowed": true/ { print name }
' "$MANIFEST")
if [[ ${#ALLOWED_FLAGS[@]} -eq 0 ]]; then
  echo "manifest contains no CellHive-allowed flags" >&2
  exit 1
fi
for flag in "${ALLOWED_FLAGS[@]}"; do
  serve_ok "$PROBE_DATE" "$flag"
done

serve_rejected "$PROBE_DATE" "cellhive_unknown_flag_probe" "No such compatibility flag"
echo "workerd compatibility probe passed: $ACTUAL_VERSION max=$MAX_DATE probe_date=$PROBE_DATE allowed_flags=${#ALLOWED_FLAGS[@]}"
