# syntax=docker/dockerfile:1

# CellHive single image: the cell-agent state/control node, the cellhive CLI and
# both workerd runtimes, plus the pinned stock workerd binary and the platform
# JS. Select a service with `command:` (default: cell-agent).

# --- pinned runtime tools (glibc) -------------------------------------------
FROM debian:bookworm-slim AS runtime-tools
ARG WORKERD_VERSION=1.20260916.1
ARG WORKERD_INTEGRITY_SHA512=6b88111ce23622ff7045b164486a9c0c5c9f828e29b614ca78152474a84bedfc723548213cdb3dac7c3e77643d699c9e396d930dad8523b10b834f5e0c938fc4
ARG ESBUILD_VERSION=0.28.2
ARG ESBUILD_INTEGRITY_SHA512=e314d9af5154992a105b85c85a68addedcd0ad44d933e3773f45d5f3144e298179d177c8ef178ef74fb56d9bcdc3122e7d0f610d0551247e588603d517c03f1d
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl \
 && rm -rf /var/lib/apt/lists/*
RUN curl -fsSL "https://registry.npmjs.org/@cloudflare/workerd-linux-64/-/workerd-linux-64-${WORKERD_VERSION}.tgz" -o /tmp/workerd.tgz \
 && echo "${WORKERD_INTEGRITY_SHA512}  /tmp/workerd.tgz" | sha512sum -c - \
 && curl -fsSL "https://registry.npmjs.org/@esbuild/linux-x64/-/linux-x64-${ESBUILD_VERSION}.tgz" -o /tmp/esbuild.tgz \
 && echo "${ESBUILD_INTEGRITY_SHA512}  /tmp/esbuild.tgz" | sha512sum -c - \
 && mkdir -p /out /tmp/workerd /tmp/esbuild \
 && tar -xzf /tmp/workerd.tgz -C /tmp/workerd --strip-components=1 \
 && tar -xzf /tmp/esbuild.tgz -C /tmp/esbuild --strip-components=1 \
 && install -m 0755 /tmp/workerd/bin/workerd /out/workerd \
 && install -m 0755 /tmp/esbuild/bin/esbuild /out/esbuild \
 && /out/workerd --version \
 && test "$(/out/esbuild --version)" = "${ESBUILD_VERSION}" \
 && /out/esbuild --version

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
COPY --from=runtime-tools /out/workerd /usr/local/bin/workerd
COPY --from=runtime-tools /out/esbuild /usr/local/bin/esbuild
COPY --from=build /out/ /usr/local/bin/
COPY third_party/workerd/LICENSE /usr/share/licenses/cellhive/workerd/LICENSE
COPY third_party/esbuild/LICENSE /usr/share/licenses/cellhive/esbuild/LICENSE
COPY THIRD_PARTY_NOTICES.md /usr/share/doc/cellhive/THIRD_PARTY_NOTICES.md
COPY workerd/ /app/workerd/
COPY deploy/entrypoint.sh /usr/local/bin/entrypoint.sh
COPY deploy/do-runtime-gated.sh /usr/local/bin/do-runtime-gated.sh
RUN chmod 0755 /usr/local/bin/entrypoint.sh /usr/local/bin/do-runtime-gated.sh

ENV CELLHIVE_WORKERD=/usr/local/bin/workerd \
    CELLHIVE_ESBUILD=/usr/local/bin/esbuild \
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
