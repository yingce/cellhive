#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
mkdir -p "$TMP/repo/deploy" "$TMP/fork/bazel-bin/src/workerd/server" "$TMP/bin"
cp "$ROOT/deploy/build-workerd-fork.sh" "$TMP/repo/deploy/"
printf '#!/bin/sh\necho fastbuild\n' > "$TMP/fork/bazel-bin/src/workerd/server/workerd"
chmod +x "$TMP/fork/bazel-bin/src/workerd/server/workerd"
cat > "$TMP/bin/bazel" <<'BAZEL'
#!/bin/sh
[ "$1" = build ] && [ "$2" = --config=release ] && [ "$3" = --jobs=4 ] || exit 9
printf '#!/bin/sh\necho release\n' > bazel-bin/src/workerd/server/workerd
BAZEL
chmod +x "$TMP/bin/bazel"
PATH="$TMP/bin:$PATH" CELLHIVE_WORKERD_FORK_DIR="$TMP/fork" bash "$TMP/repo/deploy/build-workerd-fork.sh" > "$TMP/result"
grep -qx release "$TMP/result"
echo 'release build selection passed'
