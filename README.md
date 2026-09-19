<p align="center">
  <img src="./docs/assets/logo.png" alt="CellHive" width="340">
</p>

<p align="center"><strong>A self-hosted, Cloudflare Workers-compatible runtime.</strong></p>

<p align="center">
  Unmodified <a href="https://github.com/cloudflare/workerd">workerd</a> for compute ·
  a Go "cell" state layer on SQLite + an S3-compatible object store ·
  RPO=0 with durable addressing and replaceable nodes.
</p>

---

## Features

**Workers & compute**
- ES-module Workers: `fetch`, `scheduled` (Cron), `queue` (consumers with batching, retries, DLQ).
- Service bindings and worker-to-worker RPC via same-isolate native JSRPC; Durable Object RPC (`env.NS.get(id).method(...)`).
- `nodejs_compat`; static assets with `_headers` / `_redirects` / SPA fallback.

**Durable Objects**
- Native facets, synchronous SQLite SQL, alarms, hibernating WebSockets, migrations (`new/renamed/transferred/deleted` classes), session policy, DO-inside-DO bindings, cross-node activation and takeover.

**Bindings**
- KV (strongly consistent, TTL, metadata, list), D1 (SQL, `batch`/`exec`, `meta`), R2 (get/put/head/list, range, multipart, presigned URLs), Queues, Workflows, Cron, Assets, Vars/Secrets (envelope-encrypted), AI (bring your own endpoint), Hyperdrive, Vectorize.

**Deploy & control plane**
- `wrangler.jsonc` compatible; immutable versions; atomic deploy / promote / rollback; domains and routes; custom-host authorization; audit log; per-namespace rate limiting; resource registry.

**Platform & operations**
- Cells are replicated and durable (RPO=0); a single object-store probe (`cellhive diagnose`) validates the bucket contract at startup.
- `GET /metrics`, a bounded worker log tail, optional OTLP traces and logs.
- One image with all services and the pinned workerd; Docker Compose, Kubernetes (kustomize), Helm; graceful drain and readiness probes.

**Multi-tenancy & isolation**
- Per-binding scoped tokens (HMAC); tenant workers get binding facades, never platform secrets; tenant outbound is public-only by default; only `cell-agent` holds bucket credentials.

## Architecture

![CellHive architecture](./docs/assets/architecture-minimal-detail.png)

| Component | Runtime | Ports | Role |
|---|---|---|---|
| **Traefik / edge proxy** | external | 80/443 | TLS + host routing (statically configured by the operator) |
| **`user-runtime`** | workerd | `:8081` public, `:8088` internal | Entry loader: host/version routing and tenant execution |
| **`do-runtime`** | workerd + Go supervisor | `:8788` | Native Durable Objects (facets); elastic; holds a working copy, not the authority |
| **`do-supervisor`** (optional) | Go | `:18901` | Output gate for Durable Objects: gates responses on durability (RPO=0) |
| **`cell-agent`** | Go | `:7001` internal REST, `:8082` admin | Cell storage + replication + owner/epoch leases + control plane + timer dispatch + the **only** object-store credentials |
| **Object storage** | S3-compatible | — | Authority for cell state, worker bundles and assets |

**State model.** Every unit of state is a **cell** (`<namespace>/<class>/<id>`) backed by its own SQLite database. KV namespaces, queues, D1 databases, workflows and Durable Objects are all cells, so they share one lease, replication and failover story. Committed SQLite writes are captured as **LTX** and replicated to peers; a **conditional bucket write** elects the owner, and an **ownership epoch** fences writers. A single node proves durability by uploading to the bucket; a fleet proves it sooner by having peers hold the data (quorum-1 fsync). Ownership moves with a recovered log, so no acknowledged write is lost.

## Design principles

1. **Do not modify workerd.** CellHive uses only `workerLoader`, bindings and capnp; it never forks or patches the runtime.
2. **Separate compute from state.** Runtimes execute; all persistence flows through `cell-agent`, which owns the cell's SQLite, replication and bucket credentials. Runtimes never touch the object store directly.
3. **Everything is a cell.** One uniform unit (namespace, class, id) with one SQLite per cell means one mechanism for leases, fencing, replication, snapshots, recovery and cleanup.
4. **The object store is the authority; no consensus service.** A conditional write decides ownership; LTX plus peer fsync gives RPO=0; nodes are replaceable because durable state lives in the bucket and in replicas, while local disk is only a working copy/cache.
5. **Fail closed on compatibility.** Unknown `wrangler` fields, unknown compatibility flags and dates newer than the pinned workerd are rejected at deploy time rather than silently accepted.
6. **Least privilege.** Tenant code sees binding facades and scoped tokens, not platform secrets; object-store credentials exist only in `cell-agent`.
7. **Evidence over claims.** Every behavior is covered by tests; `bash scripts/ci.sh` (lint, vet, unit, real-workerd integration, perf, RPO fault injection, S3, image/compose/k8s/helm) must print `GATE: PASS`.

## Quick start

Prerequisites: Go 1.27+, a C toolchain (SQLite is built via CGo), and `make`.

```bash
make build     # build all binaries (with the required SQLite build tags)
make test      # unit tests
make vet       # go vet
```

Single-node run (filesystem bucket, no external services):

```bash
make run       # starts cell-agent on :7001 with a local bucket
CELLHIVE_CONTROL_URL=http://127.0.0.1:7001 make diagnose   # object-store probe
```

Full local stack:

```bash
export CELLHIVE_ROOT_KEY=$(openssl rand -base64 32)   # the single required secret
docker compose -f deploy/compose/docker-compose.yml up --build
# or: make compose-up
```

`CELLHIVE_ROOT_KEY` is the only required secret — every role credential is HKDF-derived from it.

Dev CLI (Bun + Miniflare, **development only**, not a production artifact):

```bash
cd cli && bun install && bun run src/index.ts dev <dir>
```

## Repository layout

```
cmd/        service and tool entrypoints (cell-agent, cellhive, user-runtime, do-runtime, do-supervisor, benches)
internal/   reusable Go packages (cellstore, bucket, owner, lease, replica, ltx, server, control, ...)
workerd/    workerd JS platform code + capnp configs (user-runtime/, do-runtime/, platform/)
cli/        dev CLI (Bun + Miniflare; dev-only)
deploy/     Dockerfile, docker-compose, Kubernetes (kustomize), Helm
docs/       design docs and decisions
scripts/    CI and ops scripts
```

## Documentation

> **Language:** the design docs under [`docs/`](./docs/) are written in **Chinese**, the project's primary documentation language. This README (and the module/example code) are the English entry points; for behavior, the source and tests are authoritative.

- **Docs home / start here:** [`docs/README.md`](./docs/README.md) — audience paths (operate · build Workers · contribute · protocol deep-dive) and a grouped map of every doc.
- **Module docs:** [`docs/modules/`](./docs/modules/README.md) — per module: role, interfaces, an environment-variable table (with defaults), invariants, source layout and test anchors.
- **Examples:** [`examples/`](./examples/README.md) — one runnable project per feature (KV, D1, R2, Queues, Cron, Durable Objects, Workflows, service bindings, assets, AI, Vectorize, …) plus a feature→source→doc code map.
- **Concepts & design:** [`architecture.md`](./docs/architecture.md), [`cell-protocol.md`](./docs/cell-protocol.md), [`durable-objects.md`](./docs/durable-objects.md), [`protocol-formats.md`](./docs/protocol-formats.md), [`glossary.md`](./docs/glossary.md).
- **Bindings & compatibility:** [`bindings.md`](./docs/bindings.md), [`compatibility-matrix.md`](./docs/compatibility-matrix.md), [`wrangler-compat.md`](./docs/wrangler-compat.md).
- **Storage, replication & scaling:** [`storage-and-s3.md`](./docs/storage-and-s3.md), [`scaling-and-ha.md`](./docs/scaling-and-ha.md), [`timers-and-dispatch.md`](./docs/timers-and-dispatch.md).
- **Operations:** [`deployment.md`](./docs/deployment.md), [`configuration.md`](./docs/configuration.md), [`operations.md`](./docs/operations.md), [`observability.md`](./docs/observability.md), [`testing.md`](./docs/testing.md).
- **Decisions:** [`docs/decisions.md`](./docs/decisions.md).

## Contributing

- Read [`docs/contributing.md`](./docs/contributing.md): a reading path and the test anchors for each change type.
- Run `bash scripts/ci.sh` before opening a change; it must print `GATE: PASS`.
- Keep design and code in sync: change the docs (and `docs/decisions.md`) with the code.
- Never fork or patch workerd; extend through `workerLoader`, bindings and capnp only.

## Acknowledgements

CellHive's design is informed by several open-source projects in the same space — **celld** (`denoland/celld`) for the cell model, bucket conditional-write ownership, epoch fencing, LTX and RPO=0; **WDL** (`wdl-dev/wdl`) for multi-tenant `workerLoader` loading, the binding host adapter, the DO host-actor + facets approach and the alarm shim; and **LiteFS / superfly-ltx** for the Go-side SQLite replication and LTX format reference. What was borrowed (design and contracts, not implementations) and what was deliberately not borrowed is documented in [`docs/acknowledgements.md`](./docs/acknowledgements.md).

## License

CellHive reuses some Apache-2.0-licensed code and ideas; keep the corresponding `LICENSE`/`NOTICE` files and attribution when you use or redistribute such parts (see [`docs/decisions.md`](./docs/decisions.md) ADR-016).

中文说明见 [`README.zh.md`](./README.zh.md)。
