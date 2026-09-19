# syntax=docker/dockerfile:1

# CellHive single image: the cell-agent state/control node, the cellhive CLI and
# both workerd runtimes, plus the pinned stock workerd binary and the platform
# JS. Select a service with `command:` (default: cell-agent).

# --- pinned workerd (glibc) -------------------------------------------------
FROM debian:bookworm-slim AS workerd
ARG WORKERD_VERSION=1.20260615.1
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl \
 && rm -rf /var/lib/apt/lists/*
RUN curl -fsSL "https://registry.npmjs.org/@cloudflare/workerd-linux-64/-/workerd-linux-64-${WORKERD_VERSION}.tgz" -o /tmp/workerd.tgz \
 && mkdir -p /out && tar -xzf /tmp/workerd.tgz -C /out --strip-components=1 \
 && install -m 0755 /out/bin/workerd /out/workerd \
 && /out/workerd --version

# --- Go binaries ------------------------------------------------------------
FROM golang:1.27-bookworm AS build
WORKDIR /src
# SQLite is the CGo driver mattn/go-sqlite3 plus the vendored vec1 extension
# (ADR-159): the builder needs a C toolchain. TAGS mirror the Makefile.
RUN apt-get update && apt-get install -y --no-install-recommends gcc libc6-dev \
 && rm -rf /var/lib/apt/lists/*
# go.sum is required: there are real module dependencies (AWS SDK v2, CGo sqlite).
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=1 go build -tags "sqlite_fts5 sqlite_dbstat sqlite_math_functions sqlite_column_metadata sqlite_preupdate_hook" \
      -trimpath -ldflags="-s -w" -o "/out/cell-agent" ./cmd/cell-agent \
 && for svc in cellhive user-runtime do-runtime do-supervisor; do \
      CGO_ENABLED=1 go build -tags "sqlite_fts5 sqlite_dbstat sqlite_math_functions sqlite_column_metadata sqlite_preupdate_hook" \
        -trimpath -ldflags="-s -w" -o "/out/$svc" "./cmd/$svc" || exit 1; \
    done

# --- runtime ----------------------------------------------------------------
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl \
 && rm -rf /var/lib/apt/lists/* \
 && mkdir -p /data/state /data/runtime /data/bucket \
 && chown -R 65532:65532 /data
COPY --from=workerd /out/workerd /usr/local/bin/workerd
COPY --from=build /out/ /usr/local/bin/
COPY workerd/ /app/workerd/
COPY deploy/entrypoint.sh /usr/local/bin/entrypoint.sh
COPY deploy/do-runtime-gated.sh /usr/local/bin/do-runtime-gated.sh
RUN chmod 0755 /usr/local/bin/entrypoint.sh /usr/local/bin/do-runtime-gated.sh

ENV CELLHIVE_WORKERD=/usr/local/bin/workerd \
    CELLHIVE_USER_RUNTIME_JS=/app/workerd/user-runtime \
    CELLHIVE_DO_RUNTIME_JS=/app/workerd/do-runtime \
    CELLHIVE_FACADES_JS=/app/workerd/platform/facades.js \
    CELLHIVE_DATA_DIR=/data/state \
    CELLHIVE_RUNTIME_DIR=/data/runtime \
    CELLHIVE_BUCKET_DIR=/data/bucket

# Run unprivileged: /data and /app belong to the fixed runtime UID/GID.
RUN chown -R 65532:65532 /app
USER 65532:65532

VOLUME ["/data"]
# Health: GET http://127.0.0.1:7001/readyz (no auth).
# `command: ["user-runtime"]` (or /usr/local/bin/...) selects a service; no
# command runs cell-agent.
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
