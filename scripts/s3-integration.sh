#!/usr/bin/env bash
# S3 integration: run the object-store contract (conditional create / CAS /
# reject-stale / ranged read / presign) plus the full cell replication+restore
# chain against a local S3-compatible server (MinIO or rustfs).
#
# If CELLHIVE_S3_TEST_ENDPOINT is set, it is used as-is (no container). Else a
# MinIO container is started (requires docker) and removed afterward.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

GO="${GO:-go}"
# SQLite is the CGo driver + feature tags (ADR-159); keep in sync with Makefile.
export CGO_ENABLED=1
SQLITE_TAGS="${SQLITE_TAGS:-sqlite_fts5 sqlite_dbstat sqlite_math_functions sqlite_column_metadata sqlite_preupdate_hook}"
ENDPOINT="${CELLHIVE_S3_TEST_ENDPOINT:-}"
ACCESS="${CELLHIVE_S3_TEST_ACCESS:-minioadmin}"
SECRET="${CELLHIVE_S3_TEST_SECRET:-minioadmin}"
BUCKET="${CELLHIVE_S3_TEST_BUCKET:-cellhive}"
IMAGE="${S3_IMAGE:-quay.io/minio/minio:RELEASE.2025-02-18T16-25-55Z}"
PORT="${S3_PORT:-9000}"
CONTAINER="${S3_CONTAINER:-cellhive-minio}"
started=""

cleanup() { if [ -n "$started" ]; then docker rm -f "$CONTAINER" >/dev/null 2>&1 || true; fi; }
trap cleanup EXIT

if [ -z "$ENDPOINT" ]; then
  if ! command -v docker >/dev/null 2>&1; then
    echo "SKIP: no CELLHIVE_S3_TEST_ENDPOINT and docker unavailable" >&2
    exit 0
  fi
  docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
  docker run -d --name "$CONTAINER" -p "${PORT}:9000" \
    -e MINIO_ROOT_USER="$ACCESS" -e MINIO_ROOT_PASSWORD="$SECRET" \
    "$IMAGE" server /data >/dev/null
  started=1
  ENDPOINT="http://127.0.0.1:${PORT}"
  for _ in $(seq 1 30); do
    curl -sf --noproxy '*' --max-time 2 "$ENDPOINT/minio/health/live" >/dev/null && break
    sleep 1
  done
fi

echo "== S3 endpoint: $ENDPOINT =="
"$GO" run -tags "$SQLITE_TAGS" ./cmd/s3init -endpoint "$ENDPOINT" -access "$ACCESS" -secret "$SECRET" -bucket "$BUCKET"
"$GO" run -tags "$SQLITE_TAGS" ./cmd/s3probe -endpoint "$ENDPOINT" -access "$ACCESS" -secret "$SECRET" -bucket "$BUCKET" -path-style
CELLHIVE_S3_TEST_ENDPOINT="$ENDPOINT" CELLHIVE_S3_TEST_ACCESS="$ACCESS" \
CELLHIVE_S3_TEST_SECRET="$SECRET" CELLHIVE_S3_TEST_BUCKET="$BUCKET" \
  "$GO" test -tags "$SQLITE_TAGS" ./internal/bucket/ ./internal/replica/ -run 'S3' -count=1 -v
echo "OK: S3 integration passed"
