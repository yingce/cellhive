# Contributor Reading Path

This document provides the minimum checklist for “what to read before changing code in a given area, what must be run, and what invariants must be preserved.” **When documentation and code conflict, the documentation is authoritative and both must be synchronized**; change [`decisions.md`](decisions.md) before changing the design.

## Common Entry Points (Everyone Reads These First)

1. [`architecture.md`](architecture.md) — components, ports, trust boundaries, state ownership;
2. [`cell-protocol.md`](cell-protocol.md) — authoritative model for owner/epoch/LTX/RPO=0;
3. [`decisions.md`](decisions.md) — why it is this way, and the cost of each choice;
4. The **change type** entry below that applies to this work.

**Iron Laws** (no change may violate these):

- **Do not fork / do not modify workerd**; extend only through `workerLoader`, bindings, and capnp.
- **No List on the hot path**; object storage is used only for point lookups and conditional writes. `List` is only for diagnostics/operations.
- **Authorization before forwarding**; tenants receive only binding facades and scoped tokens, never platform secrets.
- **All persistence goes through `cell-agent`**; runtime does not access object storage directly.
- Submission gate: `bash scripts/ci.sh` must output `GATE: PASS`.

## By Change Type

### Protocol / Replication / Recovery (owner, epoch, LTX, snapshots, RPO=0)

- **Read first**: [`cell-protocol.md`](cell-protocol.md), [`protocol-formats.md`](protocol-formats.md), [`storage-and-s3.md`](storage-and-s3.md);
- **Must preserve**: single writer + monotonic epoch; ack must come after durability proof; takeover must first recover the dead owner’s open log; bucket conditional writes are the only arbitration mechanism;
- **Test anchors**: unit tests for `internal/owner`, `internal/replica`, `internal/ltx`, and `internal/recovery`; `make rpo-test` (per-key RPO=0 verification after kill -9).

### Durable Objects (facet, alarm, WebSocket, migration, session policy)

- **Read first**: [`durable-objects.md`](durable-objects.md), [`workerd-integration.md`](workerd-integration.md);
- **Must preserve**: a DO is a cell (one lease per object); facet must come from `workerLoader.getDurableObjectClass()`; alarms go through the shim + unified timer; WebSocket migration/restart uses 1012;
- **Test anchors**: `internal/doruntime` (real workerd e2e), `make js-test`.

### binding / facade (KV, D1, R2, Queue, Workflow, AI, Hyperdrive, Vectorize)

- **Read first**: [`bindings.md`](bindings.md), [`compatibility-matrix.md`](compatibility-matrix.md), [`wrangler-compat.md`](wrangler-compat.md);
- **Must preserve**: binding name = registered resource name; scoped token validation; strong read consistency (forward to owner); CF rejections must be **explicitly rejected rather than silently accepted**;
- **Test anchors**: `internal/userruntime` (real workerd), `internal/server`, `make js-test`.

### Timing / Scheduling (cron, queue, DO alarm, wake index, waker)

- **Read first**: [`timers-and-dispatch.md`](timers-and-dispatch.md);
- **Must preserve**: due records live in cell SQLite (authoritative), bucket `wake/` is only a discovery index; **the index must not lag behind the timer** (publish first, then commit); dispatch is at-least-once, and mark fired only on success;
- **Test anchors**: `internal/timer`, `internal/dispatch`, `internal/cron`, `internal/wake`.

### Control Plane / Release / Routing

- **Read first**: [`control-plane.md`](control-plane.md), [`routing.md`](routing.md);
- **Must preserve**: versions are immutable, releases are atomic, routing projections are pure pull; control-plane writes also go through `capturedWrite` (RPO=0);
- **Test anchors**: `internal/control`, `internal/server`, `cmd/cellhive` e2e.

### Deployment / Configuration / Operations

- **Read first**: [`deployment.md`](deployment.md), [`configuration.md`](configuration.md), [`operations.md`](operations.md);
- **Must preserve**: default behavior must not regress; any new env must have a default value and be registered in `configuration.md`; probes use `/ready`;
- **Test anchors**: `make compose-config`, `make k8s-render`, `make helm-lint`, `make docker-build`.

### Observability / Logging / Tracing

- **Read first**: [`observability.md`](observability.md), [`tracing.md`](tracing.md);
- **Must preserve**: register the metric name and data source before adding a new metric; tracing is out-of-band, best-effort, and must not affect requests;
- **Test anchors**: unit or e2e tests for the corresponding metric/span.

### CLI / Packaging / wrangler Compatibility / dev

- **Read first**: [`wrangler-compat.md`](wrangler-compat.md), [`dev-mode.md`](dev-mode.md);
- **Must preserve**: production packaging uses Go + esbuild (no Node); dev CLI (Bun + Miniflare) is only a development tool, is not part of the production artifact, and must be in sync with pinned workerd;
- **Test anchors**: `make cli-test`, contract tests for `internal/bundler` and `internal/wrangler`.

## Pre-Submission Checks

```bash
gofmt -l .          # or make fmt
make vet            # go vet
make test           # unit tests (must include SQLite build tags; make wraps this)
bash scripts/ci.sh  # full gate, outputs GATE: PASS
```

- Commit only files **related to this change**; do not commit build artifacts, secrets, or local data directories.
- Design/protocol changes: update [`decisions.md`](decisions.md) (ADR) first, then change the code and corresponding documentation.
- New documentation: register it in the group map in [`docs/README.md`](README.md).

_Last updated: 2026-09-19_