# syntax=docker/dockerfile:1

# CellHive runtime image. The compute layer is the CellHive workerd fork:
#   https://github.com/yingce/cellhive-workerd
# Build the fork binary first with deploy/build-workerd-fork.sh, then:
#
#   docker build --build-context workerd-fork=deploy/workerd-fork \
#     -f deploy/Dockerfile -t cellhive:dev .
#
# The fork is built against glibc 2.38+ (Ubuntu 24.04 builder); the runtime
# stage therefore uses Ubuntu 24.04, while the Go binaries are built on
# bookworm and remain forward-compatible with the newer glibc.

# --- esbuild (pinned stock tool, independent of workerd) --------------------
FROM debian:bookworm-slim AS runtime-tools
ARG ESBUILD_VERSION=0.28.2
ARG ESBUILD_INTEGRITY_SHA512=e314d9af5154992a105b85c85a68addedcd0ad44d933e3773f45d5f3144e298179d177c8ef178ef74fb56d9bcdc3122e7d0f610d0551247e588603d517c03f1d
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl \
 && rm -rf /var/lib/apt/lists/*
RUN curl -fsSL "https://registry.npmjs.org/@esbuild/linux-x64/-/linux-x64-${ESBUILD_VERSION}.tgz" -o /tmp/esbuild.tgz \
 && echo "${ESBUILD_INTEGRITY_SHA512}  /tmp/esbuild.tgz" | sha512sum -c - \
 && mkdir -p /out /tmp/esbuild \
 && tar -xzf /tmp/esbuild.tgz -C /tmp/esbuild --strip-components=1 \
 && install -m 0755 /tmp/esbuild/bin/esbuild /out/esbuild \
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
FROM ubuntu:24.04
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl libstdc++6 \
 && rm -rf /var/lib/apt/lists/* \
 && groupadd -g 65532 cellhive \
 && useradd -u 65532 -g 65532 -M -s /usr/sbin/nologin cellhive \
 && mkdir -p /data/state /data/runtime /data/bucket \
 && chown -R 65532:65532 /data
COPY --from=runtime-tools /out/esbuild /usr/local/bin/esbuild
COPY --from=build /out/ /usr/local/bin/
COPY --from=workerd-fork /workerd /usr/local/bin/workerd
COPY third_party/workerd/LICENSE /usr/share/licenses/cellhive/workerd/LICENSE
COPY third_party/esbuild/LICENSE /usr/share/licenses/cellhive/esbuild/LICENSE
COPY THIRD_PARTY_NOTICES.md /usr/share/doc/cellhive/THIRD_PARTY_NOTICES.md
COPY workerd/ /app/workerd/
COPY deploy/entrypoint.sh /usr/local/bin/entrypoint.sh
COPY deploy/do-runtime-gated.sh /usr/local/bin/do-runtime-gated.sh
RUN chmod 0755 /usr/local/bin/workerd /usr/local/bin/entrypoint.sh /usr/local/bin/do-runtime-gated.sh \
 && chown -R 65532:65532 /app

ENV CELLHIVE_WORKERD=/usr/local/bin/workerd \
    CELLHIVE_ESBUILD=/usr/local/bin/esbuild \
    CELLHIVE_USER_RUNTIME_JS=/app/workerd/user-runtime \
    CELLHIVE_DO_RUNTIME_JS=/app/workerd/do-runtime \
    CELLHIVE_FACADES_JS=/app/workerd/platform/facades.js \
    CELLHIVE_DATA_DIR=/data/state \
    CELLHIVE_RUNTIME_DIR=/data/runtime \
    CELLHIVE_BUCKET_DIR=/data/bucket

USER 65532:65532
VOLUME ["/data"]
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
