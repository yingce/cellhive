# Module Documentation

This directory splits the design notes by **module**: each module provides a one-sentence purpose, key interfaces, an **environment variables table (variable / default / purpose)**, invariants that must be preserved, source locations, and test anchors. For the **complete list** of configuration and parsing rules, see [`../configuration.md`](../configuration.md) (the authoritative source is the code).

## Module Navigation

| Module | Purpose | Documentation |
|---|---|---|
| `cell-agent` | The state and control node for a fixed cluster: owns and replicates cells for KV/D1/Queue/Workflow/Cron (one SQLite per cell), maintains owner/epoch leases, dispatches timers, hosts the control plane, and is the **only process that holds object storage credentials long-term**. | [`cell-agent.md`](cell-agent.md) |
| `user-runtime` | Public entry point `:8081` + internal privileged dispatch `:8088`: resolves routes by host/version, dynamically loads immutable bundles with `workerLoader`, injects binding facades, and runs tenant `fetch`/`scheduled`/`queue`; tenant egress is public Internet only. | [`user-runtime.md`](user-runtime.md) |
| `do-runtime` | Hosts native Durable Objects using fixed **host actors + facets** (per-object SQLite, alarm shim, hibernatable WebSocket), with distributed elasticity; holds only working copies and is not authoritative. Optional `do-supervisor` provides an **output gate** to confirm response durability (RPO=0). | [`do-runtime.md`](do-runtime.md) |
| `bindings` | Maps each Cloudflare binding to platform resources and host adapters, and defines sync/async boundaries and rejected items: KV, D1, R2, Queue, Cron, Workflows, Assets, Vars/Secrets, AI (BYO), Hyperdrive, Vectorize, Service. | [`bindings.md`](bindings.md) |
| `timers-and-dispatch` | Unified timer abstraction: cron, queue delay/retry, DO alarm, workflow sleep/timeout, KV expiration; each owner dispatches its own cell, and a single fleet waker covers items whose “owner is dead/unowned”. | [`timers-and-dispatch.md`](timers-and-dispatch.md) |
| `storage-and-replication` | Object storage is authoritative: bucket conditional writes elect the owner, LTX capture/replication, snapshots/compaction, cold restore by page (paged VFS), GC. Local disk is only a working copy/cache. | [`storage-and-replication.md`](storage-and-replication.md) |
| `control-plane` | Application/version/route/domain/resource/secret/audit and release pipeline; single control database (`ns` in tables is the app), immutable versions, atomic release, resources are registered before deploy. | [`control-plane.md`](control-plane.md) |
| `observability` | Metrics, tenant log tailing, and OpenTelemetry OTLP traces and logs export. | [`observability.md`](observability.md) |
| `cli-and-packaging` | `cellhive` CLI (deployment/operations/resources/queues/vectors/workflows), Go+esbuild packaging, and `wrangler` configuration compatibility; `cli/` is a Bun+Miniflare **dev-only** tool. | [`cli-and-packaging.md`](cli-and-packaging.md) |

## Reading Path

- To understand the overall system: [`architecture.md`](../architecture.md) → [`cell-protocol.md`](../cell-protocol.md) → this directory’s [`cell-agent`](cell-agent.md) / [`storage-and-replication`](storage-and-replication.md).
- To deploy and operate: [`deployment.md`](../deployment.md) → [`configuration.md`](../configuration.md) → [`operations.md`](../operations.md).
- To write Workers: [`bindings`](bindings.md) → [`compatibility-matrix.md`](../compatibility-matrix.md) → [`wrangler-compat.md`](../wrangler-compat.md).
- To modify code: [`../contributing.md`](../contributing.md) (reading paths and test anchors by change type).

_Last updated: 2026-09-19_