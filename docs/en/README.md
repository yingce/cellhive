# CellHive Documentation

CellHive is a **self-hosted, Cloudflare Workers-compatible** runtime: the **compute layer uses stock workerd** (unmodified), and the **state layer is a self-developed Go implementation of cell storage**—each state unit is a SQLite database, **S3-compatible object storage is authoritative**, bucket conditional writes determine the owner, ownership epochs fence writers, committed writes are captured as LTX and replicated, achieving **RPO=0**, durable addressing, and replaceable nodes.

This directory mirrors the **finalized design** docs in English (the Chinese originals are authoritative). When code and documentation conflict, the documentation takes precedence and both are synchronized; to change the design, update [`../decisions.md`](../decisions.md) first and update the matching docs with the code.

> This `docs/en/` tree is the **English mirror** of the design docs. The **Chinese originals under [`docs/`](../README.md) are authoritative** and may be newer; `decisions.md` here is an English index that links to the full Chinese ADR record.

## Start Here

Choose a path based on your role; the first two steps are enough to get started:

| You are | Recommended order |
|---|---|
| **Running it / operations** | [`deployment.md`](deployment.md) → [`configuration.md`](configuration.md) → [`operations.md`](operations.md); for a local trial run, see [`dev-mode.md`](dev-mode.md) |
| **Writing Worker applications** (bindings/compatibility) | [`bindings.md`](bindings.md) → [`compatibility-matrix.md`](compatibility-matrix.md) → [`wrangler-compat.md`](wrangler-compat.md) |
| **Platform contributor** | [`architecture.md`](architecture.md) → [`cell-protocol.md`](cell-protocol.md) → [`contributing.md`](contributing.md) (reading paths by change type) → [`testing.md`](testing.md) |
| **Protocol / replication / DO deep dive** | [`cell-protocol.md`](cell-protocol.md) → [`protocol-formats.md`](protocol-formats.md) → [`durable-objects.md`](durable-objects.md) → [`storage-and-s3.md`](storage-and-s3.md) |
| **Want to run it directly and read code** | [`../examples/`](../../examples/README.md): one runnable example per feature (KV/D1/R2/Queue/DO/Workflow/service/assets/AI/Vectorize…) + code navigation |

## Core Concepts (read these 7 terms first)

- **cell**: A named state unit with its own independent SQLite database (`<namespace>/<class>/<id>`). KV namespaces, queues, D1 databases, workflows, and Durable Objects are all cells.
- **node and fleet**: A node is a process; nodes sharing the same bucket form a fleet. Any node can serve any cell; add capacity by adding another node.
- **owner / epoch / fence**: At any moment, only one node serves a cell; **bucket conditional writes** determine the owner, the monotonically increasing **epoch** provides fencing, and lease expiration releases ownership without a consensus service.
- **LTX**: The replication format for SQLite transaction logs (implementation follows LiteFS/`superfly/ltx`). Committed writes are captured, replicated, and compacted into snapshots.
- **Durability proof (RPO=0)**: On a single node, a write only counts after it is uploaded to the bucket; with ≥2 nodes, a peer first holds it (quorum-1 fsync) before acking, and the bucket is backfilled afterward. **Acknowledged writes are never lost**.
- **Bucket is authoritative**: Long-term state, bundles, and assets all live in object storage; local disks are only working copies/caches, so nodes are replaceable.
- **Compute and state are separated**: workerd only executes; all persistence goes through `cell-agent`, which is the only process holding bucket credentials.

For a more complete description, see [`architecture.md`](architecture.md).

## Architecture in One Sentence

```
接入   Traefik（运维 ingress）           TLS + host 分流
计算   user-runtime(workerd)             入口 = Worker 路由/版本解析/执行
       do-runtime(workerd + supervisor)  原生 DO，分布式弹性
状态   cell-agent(Go, 固定集群)          数据 + 控制 + owner 解析 + 计时派发 + 桶凭据
       S3 兼容对象存储                   权威状态 + code + assets
控制   并入 cell-agent（:8082 admin）
```

**Three key boundaries**: ① compute and state are separated; ② `cell-agent` is fixed while `do-runtime` is elastic (state authority resides in cell-agent); ③ `cell-agent` is the only long-term object-storage credential holder and the only persistence endpoint.

## Documentation Map

### Concepts and Design

| Document | Contents |
|---|---|
| [`architecture.md`](architecture.md) | Overall architecture, components and ports, topology, data flow, trust boundaries, and state ownership |
| [`cell-protocol.md`](cell-protocol.md) | Cell identity and durable addressing, bucket conditional-write owner, epoch fence, replication and RPO=0, bucket hard requirements, discovery |
| [`durable-objects.md`](durable-objects.md) | DO: native facet + per-object lease + WAL→cell-agent + output gates; equivalence boundaries with KV |
| [`protocol-formats.md`](protocol-formats.md) | Wire formats: owner/lease/node-log/LTX segments/bundle manifest/route projection/REST/error codes |
| [`glossary.md`](glossary.md) | Glossary |
| [`decisions.md`](decisions.md) | Decision records (ADR)—the reasons and trade-offs for each choice |
| [`contributing.md`](contributing.md) | Contributor reading paths: by change type, specifies "what to read first, what must be run, and what invariants must be preserved" |
| [`acknowledgements.md`](acknowledgements.md) | Acknowledgements and design references: which designs and engineering techniques were borrowed from celld, WDL (and LiteFS/superfly-ltx), and which were not |
| [`modules/`](modules/README.md) | **Module documentation**: per-module responsibilities, key interfaces, environment-variable tables (defaults), invariants, and source/test anchors |

### Bindings, Compatibility, and Packaging

| Document | Contents |
|---|---|
| [`bindings.md`](bindings.md) | Each CF binding → cell mapping, host adapter, sync/async boundaries |
| [`compatibility-matrix.md`](compatibility-matrix.md) | Support matrix for the CF runtime surface and Wrangler configuration surface (Supported/Partial/Rejected) |
| [`wrangler-compat.md`](wrangler-compat.md) | Wrangler compatibility: Go+esbuild packaging, configuration parsing, DO lifecycle, routing, assets |
| [`workerd-integration.md`](workerd-integration.md) | stock workerd integration: workerLoader, wrapper, host adapter, networking/limits, version pin |
| [`routing.md`](routing.md) | Entrypoints, route projections, version resolution, host forms, reserved namespaces |

### Storage, Replication, Scheduling, and Scaling

| Document | Contents |
|---|---|
| [`storage-and-s3.md`](storage-and-s3.md) | Object-storage hard requirements, bucket roles, key layout, credentials, lifecycle |
| [`scaling-and-ha.md`](scaling-and-ha.md) | Layered deployment modes, HA, handoff/drain, autoscaling, discovery |
| [`timers-and-dispatch.md`](timers-and-dispatch.md) | Unified timers, deduplication, cron/queue/workflow/DO alarm semantics |
| [`benchmarks.md`](benchmarks.md) | Benchmark regressions (commands + results, including FS / local S3-compatible tiers) |

### Runtime and Operations

| Document | Contents |
|---|---|
| [`deployment.md`](deployment.md) | Services/ports, K8s, Compose, systemd, rolling upgrades |
| [`configuration.md`](configuration.md) | **All environment variables, defaults, and purposes** (grouped by subsystem) |
| [`operations.md`](operations.md) | Operations runbook: startup/diagnostics/drain/takeover/upgrade/troubleshooting |
| [`networking.md`](networking.md) | End-to-end links and protocol recommendations, hot paths, port allocation |
| [`observability.md`](observability.md) | Metrics, logs, tracing, alerting recommendations |
| [`tracing.md`](tracing.md) | OpenTelemetry OTLP tracing: configuration, span catalog, backend integration |
| [`testing.md`](testing.md) | Testing strategy, compatibility suites, fault injection, performance gates, upgrade rollback |
| [`security.md`](security.md) | Trust boundaries, multi-tenant isolation, binding authorization, network policies, admin console |
| [`control-plane.md`](control-plane.md) | Admin console, deployment pipeline, resource lifecycle, control-plane request routing |
| [`dev-mode.md`](dev-mode.md) | `cellhive dev` local development mode (single node + filesystem bucket + hot reload) |

### Status and Plans

| Document | Contents |
|---|---|
| [`roadmap.md`](roadmap.md) | Roadmap and exit criteria |
| [`known-issues.md`](known-issues.md) | All review issues and resolution closure + unverified/to-be-implemented list |
| [`release-notes.md`](release-notes.md) | Release notes |
| [`archive/`](../archive) | Historical documents (such as `p0-tasks.md`, `p0-report.md`) |

## Find Documents by Change Type

Before changing code, read the corresponding contracts and test anchors; see [`contributing.md`](contributing.md) for the complete list.

| Change type | Read first | Must run |
|---|---|---|
| Protocol / replication / recovery (owner, epoch, LTX, RPO) | [`cell-protocol.md`](cell-protocol.md), [`protocol-formats.md`](protocol-formats.md) | Unit tests + `make rpo-test` |
| Durable Objects (facet, alarm, WS, migration) | [`durable-objects.md`](durable-objects.md), [`workerd-integration.md`](workerd-integration.md) | `make js-test` + DO e2e |
| binding / facade (KV/D1/R2/Queue/Workflow/AI…) | [`bindings.md`](bindings.md), [`compatibility-matrix.md`](compatibility-matrix.md) | `make js-test` + corresponding integration |
| Timing / scheduling (cron, queue, DO alarm, wake index) | [`timers-and-dispatch.md`](timers-and-dispatch.md) | `internal/timer`, `internal/dispatch` |
| Control plane / release / routing | [`control-plane.md`](control-plane.md), [`routing.md`](routing.md) | `cmd/cellhive` e2e |
| Deployment / configuration / operations | [`deployment.md`](deployment.md), [`configuration.md`](configuration.md) | `make compose-config` / `k8s-render` / `helm-lint` |
| Observability / logs / tracing | [`observability.md`](observability.md), [`tracing.md`](tracing.md) | Corresponding metrics/span tests |
| CLI / packaging / wrangler compatibility | [`wrangler-compat.md`](wrangler-compat.md), [`dev-mode.md`](dev-mode.md) | `make cli-test` |

## Documentation Rules

- **Language**: `docs/` currently uses Chinese; the root `README.md` defaults to English, and `README.zh.md` is Chinese.
- **Write contracts, not tutorials**: Internal documentation records ownership, interfaces, storage keys, failure semantics, deployment order, observability, and **test anchors**, rather than user-facing tutorials.
- **Keep in sync with code**: When code changes, update the corresponding documentation; for design changes, update [`decisions.md`](decisions.md) first and append an ADR.
- **Status terminology**: `Supported` (usable by ordinary applications) · `Partial` (with explicit boundaries) · `Rejected` (explicitly rejected during deployment/configuration) · `Internal` (platform surface, not a tenant API).
- **Entrypoints first**: New documents must be registered in the grouped map on this page and state "when to read it".

## Terminology Conventions

- **cell**: A named state unit with an independent SQLite database, equivalent to a Durable Object.
- **stock workerd**: The officially released Cloudflare workerd, without patches and not forked.
- **cell-agent**: A self-developed Go process (fixed cluster): owns and replicates cells for KV/D1/Queue/Workflows/Cron, and also provides the control plane, owner resolution, timer dispatch, and the only bucket credentials.
- **do-runtime**: The **distributed elastic execution layer** that hosts workerd-native Durable Objects.
- **owner / epoch / fence**: The single-writer lease, monotonically increasing generation, and fence for a cell.
- **LTX**: The replication format for SQLite transaction logs (implementation follows LiteFS/`superfly/ltx`).

_Last updated: 2026-09-19_