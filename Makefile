GO ?= go
BUN ?= bun
BIN := ./bin

# SQLite is the CGo driver mattn/go-sqlite3 (ADR-159). The feature tags mirror
# the previous pure-Go build (FTS5 for D1 tenants, dbstat for stats, math
# functions, column metadata, preupdate hooks). The require_tags.go guard fails
# the build loudly if they are missing.
TAGS := sqlite_fts5 sqlite_dbstat sqlite_math_functions sqlite_column_metadata sqlite_preupdate_hook
GOTAGS := -tags "$(TAGS)"
export CGO_ENABLED := 1

.PHONY: e2e-do-rps e2e-service-headers deploy-e2e crash-e2e owner-e2e takeover-e2e all build test js-test cli-test perf-test rpo-test vet fmt clean run diagnose s3-test docker-build compose-config compose-up k8s-render helm-lint ci

all: build

# --- deployment artifacts -----------------------------------------------------

# Build the single image (cell-agent + cellhive + both runtimes + pinned workerd).
docker-build:
	@test -f deploy/workerd-fork/workerd || bash deploy/build-workerd-fork.sh
	docker build --build-context workerd-fork=deploy/workerd-fork -f deploy/Dockerfile -t cellhive:dev .

# Validate the compose file (no containers started).
compose-config:
	CELLHIVE_ROOT_KEY=$${CELLHIVE_ROOT_KEY:-dummy} docker compose -f deploy/compose/docker-compose.yml config >/dev/null && echo "compose config ok"

# Start the local stack (cell-agent + user-runtime + do-runtime).
compose-up:
	@test -f deploy/workerd-fork/workerd || bash deploy/build-workerd-fork.sh
	docker compose -f deploy/compose/docker-compose.yml up --build

# Render the k8s manifests without a cluster (client-side kustomize).
k8s-render:
	kubectl kustomize deploy/k8s >/dev/null && echo "kustomize ok"
	kubectl kustomize deploy/k8s/overlays/rpo0 >/dev/null && echo "kustomize rpo0 ok"

# Full gate (all checks, with skips for missing tools): scripts/ci.sh.
ci:
	bash scripts/ci.sh

# Validate the Helm chart offline (no cluster).
helm-lint:
	helm lint deploy/helm/cellhive
	helm template cellhive deploy/helm/cellhive --set rootKey=$${CELLHIVE_ROOT_KEY:-dummy} --set config.bucket=$${CELLHIVE_BUCKET:-s3://cellhive} >/dev/null && echo "helm template ok"

build:
	@mkdir -p $(BIN)
	$(GO) build $(GOTAGS) -o $(BIN)/cell-agent ./cmd/cell-agent
	$(GO) build $(GOTAGS) -o $(BIN)/cellhive ./cmd/cellhive
	$(GO) build $(GOTAGS) -o $(BIN)/user-runtime ./cmd/user-runtime
	$(GO) build $(GOTAGS) -o $(BIN)/do-runtime ./cmd/do-runtime
	$(GO) build $(GOTAGS) -o $(BIN)/do-supervisor ./cmd/do-supervisor
	$(GO) build $(GOTAGS) -o $(BIN)/cellbench ./cmd/cellbench
	$(GO) build $(GOTAGS) -o $(BIN)/realbench ./cmd/realbench
	$(GO) build $(GOTAGS) -o $(BIN)/sqlbench ./cmd/sqlbench
	$(GO) build $(GOTAGS) -o $(BIN)/s3init ./cmd/s3init
	$(GO) build $(GOTAGS) -o $(BIN)/s3probe ./cmd/s3probe
	$(GO) build $(GOTAGS) -o $(BIN)/walscan ./cmd/walscan
	$(GO) build $(GOTAGS) -o $(BIN)/cell-supervisor ./cmd/cell-supervisor
	$(GO) build $(GOTAGS) -o $(BIN)/gatebench ./cmd/gatebench
	$(GO) build $(GOTAGS) -o $(BIN)/kvbench ./cmd/kvbench
	$(GO) build $(GOTAGS) -o $(BIN)/restoreverify ./cmd/restoreverify
	$(GO) build $(GOTAGS) -o $(BIN)/recoververify ./cmd/recoververify

# Real single-node deploy/rollback E2E (capture + workerd, asserts one proof
# per deploy). See scripts/deploy-e2e.sh.
deploy-e2e:
	bash scripts/deploy-e2e.sh

# Concurrent deploy load + SIGKILL + local-wipe bucket restore (RPO=0).
# See scripts/crash-e2e.sh.
crash-e2e:
	bash scripts/crash-e2e.sh

# Real cell-agent owner-liveness/restart E2E (ADR-273).
# See scripts/e2e/owner-lease.sh.
owner-e2e:
	bash scripts/e2e/owner-lease.sh

# Two real cell-agents sharing a bucket; kill A and verify B takes over after
# the node-lease window while preserving A's acked control state (ADR-273).
takeover-e2e:
	bash scripts/e2e/owner-takeover.sh

test:
	# doruntime and userruntime spawn workerd on freePort() listeners; serialize
	# packages so their real-process integration tests do not race those ports.
	$(GO) test $(GOTAGS) -p 1 ./...

# js-test checks the platform JS for syntax and runs the workerd integration
# tests that exercise the binding facades (KV TTL/metadata, D1/R2/Queue, DO,
# workflows, service bindings). It skips cleanly when workerd is unavailable.
rpo-test:
	bash scripts/rpo-zero-fault.sh

perf-test:
	CELLHIVE_PERF_GATE=1 $(GO) test $(GOTAGS) ./internal/cellstore/ -run PerfGate -count=1 -v

js-test:
	node --check workerd/platform/bindings.js
	node --check workerd/platform/facades.js
	node --check workerd/platform/rpc-codec.js
	node --check workerd/platform/log-tail.js
	node --check workerd/platform/telemetry.js
	node --check workerd/platform/lifecycle.js
	node --test workerd/platform/telemetry.test.mjs
	node --test workerd/platform/lifecycle.test.mjs
	node --test workerd/platform/queue-codec.test.mjs
	# -p 1: userruntime and doruntime each spawn real workerd processes on
	# freePort() ports; running the two packages in parallel races those ports
	# (Address already in use). Serialize the packages.
	$(GO) test $(GOTAGS) ./internal/userruntime/ ./internal/doruntime/ -count=1 -p 1

# cli-test runs the dev CLI (Bun + Miniflare, ADR-065) unit/e2e tests, including
# the assets pipeline parity checks (ADR-069/071/114).
cli-test:
	cd cli && $(BUN) test

s3-test:
	bash scripts/s3-integration.sh

# e2e-workflow / e2e-queue run the in-repo workflow and queue e2e scripts.
# Requires make build, curl, jq, node, openssl, and the pinned workerd.
e2e-bindings:
	bash scripts/e2e/bindings.sh

e2e-workflow:
	bash scripts/e2e/workflow.sh

e2e-queue:
	bash scripts/e2e/queue.sh

e2e-queue-multinode:
	bash scripts/e2e/queue-multinode.sh

e2e-queue-concurrency:
	bash scripts/e2e/queue-concurrency.sh

e2e-service-headers:
	bash scripts/e2e/service-headers.sh

# Docker + npm required for MySQL/ProxySQL auto-config e2e.
e2e-do-rps:
	bash scripts/e2e/do-runtime-rps.sh

e2e-mysql:
	bash scripts/e2e/mysql/build-bundle.sh
	bash scripts/e2e/mysql-auto.sh

e2e: build e2e-bindings e2e-workflow e2e-queue e2e-queue-multinode e2e-queue-concurrency e2e-service-headers

vet:
	$(GO) vet $(GOTAGS) ./...

fmt:
	gofmt -w .

clean:
	rm -rf $(BIN)

run: build
	CELLHIVE_ALLOW_INSECURE_DEFAULTS=1 \
	CELLHIVE_NODE_ID=$${CELLHIVE_NODE_ID:-node-1} \
	CELLHIVE_BUCKET_DIR=$${CELLHIVE_BUCKET_DIR:-./.cellhive/bucket} \
	CELLHIVE_REST_ADDR=$${CELLHIVE_REST_ADDR:-:7001} \
	$(BIN)/cell-agent

diagnose: build
	$(BIN)/cellhive diagnose
