#!/usr/bin/env bash
# CellHive full gate: every check we require before shipping. Runs everything,
# reports a summary, and exits non-zero if any required step failed.
#
# Skips (never fails) the steps whose tool is unavailable; those are recorded as
# "skip" in the summary. Force a hard failure instead with REQUIRE_ALL=1.
#
#   bash scripts/ci.sh            # local full gate
#   REQUIRE_ALL=1 bash scripts/ci.sh
set -uo pipefail
cd "$(dirname "$0")/.."

# Make the tools discoverable even in a bare shell.
for d in /usr/local/go/bin /root/.bun/bin; do
  [ -d "$d" ] && case ":$PATH:" in *":$d:"*) ;; *) PATH="$PATH:$d" ;; esac
done
export PATH

REQUIRE_ALL=${REQUIRE_ALL:-0}
FAILED=0
declare -a RESULTS=()

have() { command -v "$1" >/dev/null 2>&1; }

step() { # name, command...
  local name="$1"; shift
  printf '\n\033[1m== %s ==\033[0m\n' "$name"
  if "$@"; then
    RESULTS+=("ok    $name")
  else
    RESULTS+=("FAIL  $name")
    FAILED=1
  fi
}

skip() { # name, reason
  printf '\n\033[1m== %s ==\033[0m\n' "$1"
  echo "skip: $2"
  RESULTS+=("skip  $1 ($2)")
}

require_or_skip() { # name, tool
  if have "$2"; then return 0; fi
  if [ "$REQUIRE_ALL" = "1" ]; then
    RESULTS+=("FAIL  $1 (missing $2)"); FAILED=1
  else
    skip "$1" "missing $2"
  fi
  return 1
}

# --- Go ---------------------------------------------------------------------
# SQLite is the CGo driver mattn/go-sqlite3 with feature tags (ADR-159); keep
# these in sync with the Makefile TAGS variable.
export CGO_ENABLED=1
SQLITE_TAGS="sqlite_fts5 sqlite_dbstat sqlite_math_functions sqlite_column_metadata sqlite_preupdate_hook"
step "gofmt" bash -c 'out=$(gofmt -l .); [ -z "$out" ] || { echo "$out"; exit 1; }'
step "go vet" go vet -tags "$SQLITE_TAGS" ./...
step "go test" go test -tags "$SQLITE_TAGS" ./...
step "go build" make build

# --- workerd / JS -----------------------------------------------------------
if [ -d cli/node_modules ] || [ -f workerd/user-runtime/loader.js ]; then
  if have workerd || ls /opt/vwork/node_modules/.pnpm/@cloudflare+workerd-linux-64@* >/dev/null 2>&1; then
    step "js-test (real workerd)" make js-test
  else
    skip "js-test" "workerd not found"
  fi
fi

# --- CLI (Bun) --------------------------------------------------------------
if require_or_skip "cli-test (bun)" bun; then
  step "cli-test (bun)" make cli-test
fi

# --- performance / RPO ------------------------------------------------------
step "perf-test" make perf-test
step "rpo-test" make rpo-test

# --- deployment artifacts ---------------------------------------------------
if require_or_skip "s3-test (docker)" docker; then
  step "s3-test (MinIO)" make s3-test
fi
if require_or_skip "docker-build" docker; then
  step "docker-build" make docker-build
  step "compose-config" make compose-config
fi
if require_or_skip "k8s-render (kubectl)" kubectl; then
  step "k8s-render" make k8s-render
fi
if require_or_skip "helm-lint (helm)" helm; then
  step "helm-lint" make helm-lint
fi

# --- summary ----------------------------------------------------------------
printf '\n\033[1m===== summary =====\033[0m\n'
for r in "${RESULTS[@]}"; do echo "  $r"; done
if [ "$FAILED" != "0" ]; then
  echo "GATE: FAIL"
  exit 1
fi
echo "GATE: PASS"
