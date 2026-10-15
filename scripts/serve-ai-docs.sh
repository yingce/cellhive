#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PORT="${CELLHIVE_DOCS_PORT:-8788}"
HOST="${CELLHIVE_DOCS_HOST:-127.0.0.1}"
echo "CellHive AI docs: http://${HOST}:${PORT}/docs/ai/"
echo "CellHive Skill manifest: http://${HOST}:${PORT}/skills/manifest.json"
exec python3 -m http.server "$PORT" --bind "$HOST" --directory "$ROOT"
