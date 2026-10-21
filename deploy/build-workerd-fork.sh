#!/usr/bin/env bash
# Build/copy the cellhive-workerd fork into deploy/workerd-fork/workerd for
# Docker's named build context.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT="$ROOT/deploy/workerd-fork"
mkdir -p "$OUT"
SRC="${CELLHIVE_WORKERD_FORK_DIR:-/opt/cellhive-workerd-fork}"
BIN="$SRC/bazel-bin/src/workerd/server/workerd"
if [[ -f "$BIN" ]]; then
  cp "$BIN" "$OUT/workerd"
  chmod 0755 "$OUT/workerd"
  "$OUT/workerd" --version
  exit 0
fi
REF="${CELLHIVE_WORKERD_FORK_REF:-cellhive-v1.20260916.1-3}"
TMP="$(mktemp -d "${TMPDIR:-/tmp}/cellhive-workerd.XXXXXX")"
trap 'rm -rf "$TMP"' EXIT
git clone --depth 1 --branch "$REF" https://github.com/yingce/cellhive-workerd "$TMP"
(cd "$TMP" && bazel build --config=release //src/workerd/server:workerd)
cp "$TMP/bazel-bin/src/workerd/server/workerd" "$OUT/workerd"
chmod 0755 "$OUT/workerd"
"$OUT/workerd" --version
