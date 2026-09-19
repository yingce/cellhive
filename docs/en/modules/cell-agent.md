# Module: cell-agent (State / Control / Replication / Dispatch)

The status and control node for a fixed cluster: owns and replicates cells for KV/D1/Queue/Workflow/Cron (one SQLite per cell), maintains owner/epoch leases, dispatches timers, hosts the control plane, and is the **only process that holds object storage credentials long term**.

> See [`../configuration.md`](../configuration.md) and the code for the authoritative configuration source; the table below is the subset relevant to this module.
## Key Interfaces

`:7001` internal REST (shared by Go↔Go and workerd bindings, no gRPC) / `:8082` admin. Key endpoints: `/readyz`, `/metrics` (binding counters and the durability-proof histogram carry a **bounded `ns` label**, ADR-179), `/v1/diagnose`, `/v1/internal/{resolve,claim,renew,release,commit(_binary),worker/bindings,do/*,logs,telemetry/spans}`, `/v1/{kv,d1,r2,queue,workflow,vectorize}/*`, `/v1/control/*`.

## Configuration (Environment Variables)

| Variable | Default | Purpose |
|---|---|---|
| `CELLHIVE_ADMIN_ADDR` | `:8082` | admin/control-plane listener |
| `CELLHIVE_ADVERTISE` | `127.0.0.1:7001` | Internal advertised address (owner forwarding target); internally only REST `:7001` |
| `CELLHIVE_AUDIT_RETENTION` | `720h` (0 forever) | Audit retention |
| `CELLHIVE_AUTOSCALE_COOLDOWN` | `5m` | Cooldown window for action **changes** (debounce; 0=off, ADR-151). During the cooldown period, returns `hold/cooldown` + `cooldown_remaining_ms` |
| `CELLHIVE_AUTOSCALE_INTERVAL` | `30s` | Evaluation interval |
| `CELLHIVE_AUTOSCALE_MAX` | `0` unlimited | Target upper bound |
| `CELLHIVE_AUTOSCALE_MIN` | `1` | Target lower bound |
| `CELLHIVE_AUTO_CREATE_APP` | `true` | Automatically create app on first deploy |
| `CELLHIVE_BASE_DOMAIN` | empty | Built-in domain `<ns>-<worker>.<base>`; empty=disabled |
| `CELLHIVE_BINDING_CACHE` | `1s` (0 off) | Binding declaration cache |
| `CELLHIVE_BUNDLE_GC_GRACE` | `24h` | Retention period for unreferenced items |
| `CELLHIVE_BUNDLE_GC_INTERVAL` | `0` off | bundle/assets GC |
| `CELLHIVE_CELLS_PER_NODE` | `100` | Per-node capacity target |
| `CELLHIVE_CELL_DISK_MAX` | `0` unlimited | Cell file byte budget (LRU deletes non-owned files) |
| `CELLHIVE_CELL_DISK_SWEEP` | `1m` | janitor interval |
| `CELLHIVE_CELL_IDLE` | `0` do not clean up | Close idle handles |
| `CELLHIVE_COMPACTION_INTERVAL` | `30s` (0 off) | L0→L1 compaction |
| `CELLHIVE_COMPACTION_MIN_BYTES` | `64MiB` | Trigger bytes |
| `CELLHIVE_COMPACTION_MIN_SEGMENTS` | `64` | Trigger segment count |
| `CELLHIVE_CRON_INTERVAL` | `30s` (0 off) | Materialize cron→timer |
| `CELLHIVE_DATA_DIR` | `./.cellhive/data` | Local working directory root (spool, DO disk, etc. are derived from here) |
| `CELLHIVE_DISK_HIGH` | `0` off | High watermark→overloaded; reject claim |
| `CELLHIVE_DISPATCH_URL` | empty | Dispatch target for timer/queue/cron/workflow — must be a **user-runtime internal privileged endpoint** (`http://user-runtime:8088`). If empty, the dispatch loop does not start (compose/k8s/Helm defaults are already set) |
| `CELLHIVE_DO_EAGER_RESTART` | `false` | Eagerly restart DO on deploy |
| `CELLHIVE_DO_RUNTIMES` | empty | do-runtime list (DO placement); if empty, disables `/v1/do/invoke` |
| `CELLHIVE_DRAIN_TTL` | `30s` | Drain token validity period (shutdown wait = same value) |
| `CELLHIVE_LOG_BUFFER` | `1000:200` | Log tail buffer `<entries>:<workers>` (`cellhive tail --worker`) |
| `CELLHIVE_LTX_COMPRESSION` | bool, default `true` | Use LZ4 for the LTX page-map (`WAL3`); `false` falls back to `WAL2` fixed frames without compression (saves CPU, more bucket bytes; both decoding/GC paths are compatible, ADR-161) |
| `CELLHIVE_MAX_OPEN_CELLS` | `0` unlimited | Cached handle upper bound |
| `CELLHIVE_MAX_RESIDENT_CELLS` | `0` unlimited | Resident handle upper bound |
| `CELLHIVE_NODE_ID` | `node-1` | Stable node identity (owner/handoff key), required |
| `CELLHIVE_NS_RATE` | empty (off) | Per-ns write admission `rps[/burst]` (default burst=rps) |
| `CELLHIVE_PAGED_HYDRATE_MBPS` | integer, default `16` | Background fill rate for paged cells (MiB/s, one at a time per node); `0` = keep sparse, read the bucket for every cold page |
| `CELLHIVE_PAGED_MIN_BYTES` | bytes, default `256MiB` | Clone chains smaller than this whole; `0` = always paged |
| `CELLHIVE_PAGED_PREFETCH_WORKERS` | integer, default `4` | Number of child concurrent prefetch workers |
| `CELLHIVE_PAGED_RESTORE` | bool, default `true` | cell-agent: during cold restore, use the fault-in VFS to load pages for cells that are "compacted and have chain ≥ MIN_BYTES" (ADR-160) |
| `CELLHIVE_PAGED_WINDOW_PAGES` | integer, default `64` | Window prefetch: maximum number of adjacent pages to fetch in one ranged read (also subject to a 256KiB budget) |
| `CELLHIVE_PEER_URL` | `http://127.0.0.1:7001` | This node's REST callback address (peer replication target) |
| `CELLHIVE_PLACEMENT_AZ` | empty | Failure domain for this node (rack/zone); when set, follower selection **prefers a different AZ** (ADR-151). empty=no preference |
| `CELLHIVE_PLACEMENT_WEIGHT` | `0`→CPU | Ownership share |
| `CELLHIVE_QUEUE_BATCH` / `_LEASE` | `0` (store default) | Messages per batch / visibility lease |
| `CELLHIVE_QUEUE_INTERVAL` | `1s` (0 off) | Queue consumption |
| `CELLHIVE_QUEUE_RETRY_DELAY` | `30s` | Redelivery delay for failed batches (retry limit/DLQ are configured per consumer) |
| `CELLHIVE_REBALANCE_INTERVAL` | `0` off | Rebalance loop |
| `CELLHIVE_REBALANCE_MAX_MOVE` | `32` | Maximum releases per round |
| `CELLHIVE_REST_ADDR` | `:7001` | Internal REST (shared by Go↔Go and workerd bindings, no gRPC) |
| `CELLHIVE_RUNTIME_DIR` | `$TMPDIR/cellhive` | Ephemeral runtime directory root; each runtime uses its subdirectory |
| `CELLHIVE_SESSION_ID` | startup nanoseconds | Current session id; new after restart |
| `CELLHIVE_TIMER_BATCH` / `_FIRED_TTL` | `256` / `24h` | Due upper bound per pass / fired marker retention |
| `CELLHIVE_TIMER_INTERVAL` | `1s` (0 off) | Due timer dispatch |
| `CELLHIVE_WAKER_BACKOFF_MAX` | `1m` | Backoff cap on consecutive errors (base=interval, `interval·2^fails`) |
| `CELLHIVE_WAKER_BATCH` / `_FIRED_TTL` | `256` / `24h` | Upper bound per pass / fired marker retention |
| `CELLHIVE_WAKER_INTERVAL` | `5s` (0 off) | Fleet waker (TTL automatically set to 2×) |
| `CELLHIVE_WAKE_REPAIR_BATCH` | `256` | Number of local cells checked per pass (rotating window, covers all in a few passes) |
| `CELLHIVE_WAKE_REPAIR_INTERVAL` | `5m` (0 off) | Local wake index repair scan (ADR-177); runs once at startup first |
| `CELLHIVE_WORKFLOW_RETENTION` | `0` forever | Prune terminal-state instances |


> For parsing rules (strings/booleans/durations/bytes/lists), see [`../configuration.md`](../configuration.md#parsing-rules).

## Key Invariants

- No List on the hot path; only point lookups and conditional writes.
- **ack is later than durability proof** (capture wraps every store mutation).
- **authorization precedes forwarding** (scoped token).
- Does not execute tenant code (execution is in workerd).

## Source Locations

`cmd/cell-agent/`; `internal/{config,cell,cellstore,bucket,objectstore,owner,lease,replica,ltx,compaction,pagedvfs,capture,upload,peer,nodelog,recovery,server,control,timer,dispatch,cron,waker,wake,queue,r2,d1,vectorize,workflow,cron,telemetry,...}`.

## Test Anchors

`make test`, `make rpo-test` (verify RPO=0 key by key after kill -9), `internal/server`, `internal/cellstore`, `cmd/cellhive` e2e.

## Related Documents

[`architecture.md`](../architecture.md), [`cell-protocol.md`](../cell-protocol.md), [`control-plane.md`](../control-plane.md), [`storage-and-s3.md`](../storage-and-s3.md), [`configuration.md`](../configuration.md)

_Last updated: 2026-09-19_