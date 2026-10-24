#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
TAGS='sqlite_fts5 sqlite_dbstat sqlite_math_functions sqlite_column_metadata sqlite_preupdate_hook'
SESSION_E2E_BIN=$(mktemp /tmp/cellhive-session-e2e.XXXXXX)
trap 'rm -f "$SESSION_E2E_BIN"' EXIT
go build -tags "$TAGS" -o "$SESSION_E2E_BIN" ./cmd/cell-agent
CELLHIVE_SESSION_E2E_BIN="$SESSION_E2E_BIN" go test -tags "$TAGS" ./cmd/cell-agent -run '^TestSessionRecoveryProcesses$' -count=1 -v -timeout=3m
