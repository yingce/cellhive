# Release Notes

## Unreleased (main)

This release closes out the remaining items in roadmap P3–P5, all backed by real runtime/test evidence.

### Stock-workerd Runtime Baseline Hardening (ADR-186)
- The production and contract baseline is exactly pinned to stock workerd `1.20260916.1` and esbuild `0.28.2`. The dev CLI uses contemporaneous Miniflare `5.20260916.0-alpha`, with its workerd override also fixed at `1.20260916.1`. The image build verifies registry SHA-512 values and ships workerd/esbuild license texts.
- Platform URLs/tokens no longer enter final WorkerCode or rendered capnp. Trusted host bindings use `fromEnvironment`, and user-runtime, do-runtime, and do-supervisor start workerd with an explicit environment. Tenant env still has zero platform keys, so users may use valid names such as `CELL_*` and `CH_*`.
- Compatibility authority is generated from a fixed upstream workerd revision (current ceiling `2026-09-23`). The Go control plane and Bun CLI consume the same generated manifest; a real-binary probe verifies the maximum date, every allowed flag, and unknown-flag rejection.
- Final WorkerCode is limited to 64 MiB. workerLoader env uses the 1 MiB upstream limit minus 8 KiB headroom, for a 1016 KiB limit. The control plane checks before its release transaction, and every dynamic-load path rechecks through one shared guard; errors expose only code/actual/max. Until Task 11's Docker Compose/no-skip acceptance passes, this entry claims implemented code paths and real-workerd package tests only.

### Tenant Env Is Entirely User-Owned (ADR-185)
- Tenant Worker and DO-facet `env` objects now contain only user-declared vars/bindings. There are zero platform keys; `CH_*`, `CELL_*`, `__cellhive*`, and historical platform names all belong to the user.
- The workflow dispatcher passes a trusted `WorkflowBridgeTarget extends RpcTarget` fixed-op callback as a JSRPC argument to the dynamic workflow wrapper. The host env, private transport, credentials, and dispatcher-bound identity stay in the trusted internal host. Step/retry/wait/pause/error paths pass on pinned stock workerd `1.20260916.1`; the implementation neither transfers the incompatible `ServiceStub` across the dynamic-loader boundary nor enables the experimental flag.
- Native Tail Workers for dynamically loaded Workers are unsupported on the current pin, so platform capture of tenant console output is temporarily disabled instead of restoring an env transport.

### ADR-184 platform-side stubs (historical; env conclusions superseded by ADR-185)
- ADR-184's platform-side DO/Workflow/Vectorize stubs, DO `fetch(request)` WebSocket path, and `rpcObject(...)` data path remain current; their `:7001` transport and scoped tokens stay in the trusted platform host.
- Its historical `CH_PLATFORM`/`PlatformBridge`, platform-env-key, and reserved-namespace conclusions are superseded by ADR-185: tenant env now has zero platform keys and zero reserved names. Workflow uses a dispatcher-bound `WorkflowBridgeTarget extends RpcTarget` JSRPC argument, and dynamic Tail Workers are unsupported on the current pin, so platform capture of tenant console output is disabled.
- Verified workerd semantics: `ctx.exports` is the host worker's mainModule export set; a Proxy returned from a method is not recognised as an `RpcTarget` by RPC (so the platform side exposes explicit methods only, and arbitrary DO method names are forwarded as `(method,args)` data by a tenant-side Proxy).

### KV TTL: 60s Floor Removed (ADR-183)
- `expirationTtl` now accepts any positive integer seconds (previously ≥60s, an artificial tightening to match Cloudflare); absolute `expiration` must still be in the future. Expiry takes effect lazily on read (second granularity); the timer sweep only reclaims space. Explicit divergence from Cloudflare Workers KV: CF rejects <60s, this platform accepts it.

### Queue DLQ Replay Preserves the Idempotency Key
- Dead-letter delivery and DLQ replay (both the local path and the remote forwarding chain) now **pass the message's `idempotency_key` through** (previously hard-coded empty): a replayed message dedupes against a still-live twin via the unique index instead of duplicating it. `queue.Message` gains an `IdempotencyKey` field (populated at claim time).

### KV Conditional Write & Atomic Increment
- `POST /v1/kv/put` supports **`if_exists=absent|present`** (atomic existence-conditioned write, ADR-182, the platform-facing "onlyIf" semantics): `absent` = create-if-absent (locks/idempotency), `present` = swap on a live key. The existence check and the write run in one transaction (single writer + `writeMu` serialization); a failed condition returns `200 {applied:false}` (boolean semantics, not an error); the key and the cell txid are untouched; unrelated keys never interfere. An expired-but-present row counts as absent (a TTL lock can be re-acquired after expiry).
- New **`POST /v1/kv/incr`**: `by` (default 1, may be negative) atomically adds to a decimal integer value, creating the key at the delta when missing; the read-add-write runs in one transaction (no lost updates); non-integer targets get `400 not_integer` (txid untouched); optional `if_exists` guard (failed condition also yields `200 {applied:false}`), `metadata` (applies on create/refresh; a pure add keeps existing metadata), `expiration`/`expiration_ttl`.
- Storage layer: `cellstore.PutTxIf` (`PutTxCondition` existence check, sentinel `ErrConditionFailed`), `cellstore.IncrTx/IncrTxIn` (overflow checks). Zero schema changes; the cell txid semantics are unchanged (capture watermark, decoupled from conditional writes).

### Scoped Token Range Matching + Delegated Issuance (ADR-181)
- Scoped token `kind`/`name` support **segment-level globs** (`*` any, `pre*` prefix, anchored per segment); `ns` stays exact (the isolation boundary).
- New `iss` claim and **delegated issuance**: a trusted entry holds a derived issuer key (`cellhive creds issuer <name>`) and signs short-lived scoped tokens itself (`cellhive token ... --iss <name> --ttl 5m`); delegated tokens **require an expiry** and authorize by ns/scope (skipping `HasBinding`), while platform tokens (empty `iss`) are unchanged.
- New **single issuance entry point** `cellhive token`; `--key` lets a delegate sign without root; field order `ns,kind,name,iss,exp_ms`, empty `iss` keeps the ADR-074 canonical bytes unchanged.
- Boundary: token-derived resources (kv/vectorize/service/do) require a concrete name; the external data-plane listener and API-key management are not built.

### Optional OTLP Export for Trusted Platform Logs (ADR-172)
- `CELLHIVE_OTLP_LOGS=off|tail|all` (default `off`): exports logs from **trusted platform producers** to the same OTLP endpoint/headers as traces (`/v1/logs`); `tail` exports only while the TTL subscription of `cellhive tail --worker` is active, and stops within ≤60s after exit.
- Export is side-channel, best-effort, and batched in the background; it does not affect requests. The in-memory ring and `cellhive tail` behavior likewise apply only to trusted platform producers. Under ADR-185, tenant `console.*` does not enter the ring, tail, or OTLP.
- **Fleet broadcast (ADR-173)**: subscriptions for trusted platform producers are broadcast to other cell-agent instances via the lease node list (internal endpoint + throttling), so `tail` gating covers the same-named worker served by any node (no need to switch to `all`).

### Wake Index Fix (ADR-177)
- The timer wake index is changed to **publish before commit** (the index may only be ahead, never behind); if index write fails, the timer is not committed (fail-closed).
- The index is rebuilt when a node registers/claims a timer scope; a bounded rotating local repair scan is added (`CELLHIVE_WAKE_REPAIR_INTERVAL=5m`, `_BATCH=256`, 0 disables) to repair missing entries caused by crashes/bucket write failures.

### KV Write Validation Aligned with Cloudflare (ADR-176)
- `put` now rejects: key >512B, metadata >1KiB, `expirationTtl` <60s, and `expiration` in the past (`key_too_large`/`metadata_too_large`/`bad_expiration`, respectively); value ≤25MiB is unchanged. Previously these were allowed (more permissive than CF).

### Compatibility Closure (ADR-175)
- **DO WebSocket**: `env.DO.get(id).fetch(upgrade request)` now connects directly to the owner via `/v1/do/connect` (owner lookup + ticket); the WS echo in the `wsecho` example works.
- **R2 `writeHttpMetadata`**: local Proxy wrapping writes metadata into the caller’s `Headers` (`content-type` works correctly).
- **DO alarm**: dispatch now fills in `storage_id`/`storage_class` (previously alarms landed on a different facet than fetch, so `fires` was always 0) and fixes per-tick URL parsing failures caused by addresses without a scheme.

### Example Compatibility Fixes (ADR-174)
- **KV `put`** accepts `ReadableStream`/`ArrayBuffer`/`ArrayBufferView`/`Blob` (previously the request body was JSON-encoded as `"{}"`).
- **Legacy DO classes** (not `extends DurableObject`, only `fetch/alarm/webSocket*`) are now wrapped as facets, so examples such as counter/router/body/async/alarm can run directly.
- **DO `fetch` non-2xx** returns the status and content-type as-is (no longer converted to 500); cell-agent passes them through.
- **R2ObjectBody** adds `body` and `writeHttpMetadata` (the latter is subject to RPC limitations; see known-issues).
- Re-tested the example suite + Hono 4.13.8 (`node_modules` packaging, multiple paths, `--path` mounts, KV binding): all passed except registered gaps (DO WS in wsecho, alarm requires gate) and policy rejections (compat date/unregistered fields).

### `cellhive wrangler` Command/Argument Compatibility Closure
- `translateWrangler` is changed to be **table-driven**: known wrangler commands either map to native commands or produce an executable alternative (`wranglerReject`); no known command falls through to `unknown command` anymore. Subcommand-level alternatives are completed (`triggers deploy`/`queues consumer`/`workflows status|describe|trigger`, etc.).
- `test`: `TestWranglerKnownCommandsHaveOutcomes` (coverage table), `TestWranglerRejectionsAreActionable`.

### `cellhive wrangler` Mapping Fixes
- The `wrangler d1|r2|kv` prefixes previously fell through to `unknown command`; they now forward to the native per-domain commands (`d1`/`r2`/`kv`, ADR-157), and only data-plane verbs (`d1 execute`/`r2 object`/`kv key`) return structured rejections (ADR-148).
- `wrangler triggers deploy` now explicitly prompts using `cellhive deploy --config` (crons are applied atomically with the version); hints for unknown commands now complete compatibility groups.

### Upload Spool Power-Loss Safety (ADR-171)
- Async upload spool append is changed to atomic writes: tmp fsync → rename → directory fsync (leaf + parent, memoized once per process); power loss no longer drops already spooled segments.
- `/metrics` adds `cellhive_upload_spool_file_syncs_total`/`_dir_syncs_total`/`_sync_seconds_total`.

### DO Output Gate Concurrent Capture + Ordered Sweep (ADR-170)
- `dosupervisor.SyncAll` commits multiple facet files with **bounded concurrency** (`Concurrency` default 8), proving persisted RTT for overlapping work across scopes; within the same file it remains serial by txid, and gate semantics are unchanged.
- `orderedDispatcher.sweep` is wired into `Do`: idle scopes caused by permanent txid gaps are proactively evicted and buffered callers are released.

### Purge Cursor-Paginated Deletion (ADR-169)
- `objectstore.Objects.ListPage` + `bucket.PagedLister`: purge data side no longer materializes the entire key list for a range (S3 `StartAfter`, bounded memory). It advances while deleting and resumes in the next round when the budget is exhausted.

### R2 list: include and delimiter (ADR-168)
- `bucket.list({include:["httpMetadata","customMetadata"]})` backfills per-object metadata **on demand** according to CF semantics (one sidecar read per object; zero cost if not requested); unknown include values return 400.
- `bucket.list({delimiter:"/"})` returns `delimitedPrefixes` (common prefix coalescing; scan limit 10000, with `truncated`+`cursor` for continuation when exceeded).

### Standard OpenTelemetry Tracing (ADR-167)
- Adds OTLP/HTTP export (official `go.opentelemetry.io/otel` SDK): `CELLHIVE_OTLP_ENDPOINT` (empty=off, zero overhead), `CELLHIVE_OTLP_HEADERS`, `CELLHIVE_TRACES_SAMPLE_RATIO` (default 0.01, `ParentBased` honors upstream sampled). Switching backends only changes the endpoint; CellHive does not store traces itself.
- Spans: cell-agent `http.server`/`cell.durability_proof`/`peer.append`; do-supervisor gate/restore; workerd-side loader `http.server`, do host `do.invoke`/`do.gate` (JS aggregates through `/v1/internal/telemetry/spans`, `workerd/platform/telemetry.js`).
- Fixes ADR-146 residue: props-bound binding calls now carry `traceparent`.
- Integration guide: `docs/tracing.md` (including the OpenObserve compose `tracing` profile and Basic auth example).

### Observability Metrics Completion Batch 2 (ADR-166)
- cell-agent `/metrics` adds `cellhive_replication_bytes_total{kind=shipped|received}` and `cellhive_waker_fires_total{kind,outcome}`.
- do-supervisor (`-listen`, default `:18901`) adds `GET /metrics`: `cellhive_do_wal_captured_bytes_total`, `cellhive_do_restore_seconds` (summary), `cellhive_do_output_gate_timeouts_total`, `cellhive_do_alarms_fired_total{outcome}`, `cellhive_do_ws_sessions`; the host actor reports increments best-effort after each invoke.
- The "planned" metrics list in `docs/observability.md` has all been implemented (no leftovers).

### Observability Metrics Completion (ADR-165)
- `/metrics` adds: `cellhive_binding_calls_total{kind,outcome}` (ok/denied/error), `cellhive_durability_proof_seconds` (histogram of persistence proof latency on the write path), `cellhive_owner_epoch_changes_total{role}`, `cellhive_takeover_total{outcome=success|failed|blocked}`, `cellhive_route_projection_version`, `cellhive_peer_hedge_fired_total`/`_won_total`.
- Alerting recommendations in `docs/observability.md` now use metrics that actually exist; pending items (replication_bytes, waker_fires, do_*) continue to be clearly marked as "planned".

### Peer Adaptive Hedge (ADR-164)
- Fleet replication writes the primary follower first. If it exceeds the wait time (adaptive = `max(250ms, 4×recent slowest append)`, capped by `CELLHIVE_PEER_HEDGE_MAX_MS`), a **second copy** is sent to the next follower and the first ack wins; `CELLHIVE_PEER_HEDGE_MS` defaults to `adaptive`, `0`=primary only (single copy; fail over only if primary fails), `>0`=fixed ms.
- **Prerequisite: spool idempotent by sequence**. `Spool.AppendBatch` now deduplicates by `ltx.Header.ID()` (epoch/kind/txid range/CRC); duplicate/retry/reconnect/hedge copies are no-ops. Otherwise restore would fail with "non-contiguous chain" because of duplicate txids.
- The primary frame is still **written synchronously** inside the sender goroutine (preserving lane batch order); only hedge copies are sent after a delay. The restore path sorts by `rank+StartTxID`, so out-of-order arrival of late copies is tolerated.
- Tests: `internal/peer TestSpoolIdempotentPerSequence`, `TestShipBatcherHedgeSkipsSlowBackupWhenPrimaryFast`, `TestShipBatcherHedgeFiresOnSlowPrimary`, `TestShipBatcherHedgeOffSingleCopy`, `TestShipBatcherHedgeAllFail`, `TestShipBatcherHedgeAsync`, `TestShipBatcherAdaptiveHedgeWait`, `internal/config TestPeerHedgeMSEnv`.

### R2/D1 Contract Fidelity (ADR-163)
- **R2**: `get()` returns a complete `R2ObjectBody` (`key/size/etag/httpEtag/version/uploaded/httpMetadata/customMetadata/checksums` + `text/json/arrayBuffer/blob`); `put(key,value,{httpMetadata,customMetadata,md5,sha256})` stores the metadata sidecar and verifies checksums; adds `head()`; `list()` passes through `truncated/cursor` and objects include `httpEtag/version`. Implementation note: workerd RPC serializes **enumerable function properties into callable stubs**, so objects can have both data fields and body methods.
- **D1**: `run()/batch()` `meta` adds `last_row_id` (from SQLite `LastInsertId`) and `changed_db`; binding errors are thrown in CF shape (`err.name = D1_ERROR/KVError/R2Error/QueueError` + `err.code`=platform code).
- **Backend**: `bucket.Statter` (S3 `HeadObject`, FS fallback reads the body) is used for `head()`/full length for ranges; `r2meta/` is registered as a reserved prefix (`objectstore`, owner `r2`).
- Tests: `internal/r2 TestR2MetadataStatAndChecksums`, `internal/d1 TestLastInsertID`, `internal/userruntime TestUserRuntimeR2ObjectFidelityAndD1Meta` (real workerd end-to-end).

### DO Calls Support RPC (ADR-162)
- `env.NS.get(id)` / `env.NS.getByName(id)` now return method-callable stubs: `await env.ROOM.getByName("r1").addMessage("u1", "hi")`.
- **Implementation**: `kind:"rpc"` tagged JSON envelopes reuse `/v1/do/invoke` (owner-hint/ticket/`409/5xx not replayed` semantics are the same as fetch); the host calls facet methods using **native JSRPC**, so Map/Date/ArrayBuffer and shared references/cycles are preserved; the Go side passes through `json.RawMessage` bytes.
- **Fidelity/limits**: tagged encoding/decoding supports `undefined`, `-0/NaN/±Inf`, bigint, Date, RegExp, Map, Set, ArrayBuffer, TypedArray/DataView, Error (including cause), URL, URLSearchParams, shared references/cycles; unsupported: functions/symbols/Promise/weak collections/streams/`RpcTarget`/stub; class instances degrade to plain objects; args and results are each capped at **8 MiB** (the original route body limit of 1 MiB has been raised accordingly); reserved method names: `fetch`/`alarm`/prototype internals/`__ch*`.
- **Tests**: real-workerd `internal/doruntime` / `internal/userruntime` e2e + `internal/server` pass-through/limits; see `docs/testing.md`.

### cell-agent Page-Based Cold Start (ADR-160)
- Adds fault-in SQLite VFS `internal/pagedvfs` (`?vfs=cellhive-paged`): the main database is a **sparse file by pinned cut**; on `xRead` page miss, it **synchronously** fetches that page from the replication chain and writes it locally; `xWrite`/`xTruncate` maintain the hydration bitmap; faults fail closed.
- Page source = `replica.PageFetcher` (ranged reads by page from L1 `index.bin` + updated L0 tail); chain < `CELLHIVE_PAGED_MIN_BYTES` (default 256MiB) or no compaction → still full clone.
- Measured (real-path test): cold point read **8 page faults / 771 pages, 8 ranged reads, 3,158B full-object reads (mirror 3,158,016B) ≈ 1000× less**; forced `HydrateAll` before `SnapshotPages` to guarantee LTX snapshots contain no sparse zero pages.
- Switches: `CELLHIVE_PAGED_RESTORE` (default on), `CELLHIVE_PAGED_MIN_BYTES`, `CELLHIVE_PAGED_HYDRATE_MBPS` (default 16, 0=remain sparse), `CELLHIVE_PAGED_WINDOW_PAGES` (default 64).
- **Window prefetch**: `replica.PageFetcher.ReadRun` uses one ranged read to fetch adjacent pages within the same L1 object (256KiB budget), and pagedvfs caches the window → full scan of 336 pages needs only 6 bucket reads (previously one per page); the `xTruncate` callback discards stale window pages.
- **Sparse-aware disk accounting**: `DiskFile.AllocBytes` (`st_blocks*512`) feeds into `DiskUsage`/`/metrics` and the disk eviction budget, so paged cells no longer incorrectly trigger backpressure due to apparent size.
- **b-tree-aware fault strategy** (fetch a single child page for internal pages, avoiding pulling the entire overflow chain; prefetch sequential children into the in-memory cache with `SCAN_AHEAD`=64, coalesce contiguous children into a single read), `HydrateAll`/background backfill bulk fetches, and six `cellhive_paged_*` metrics under `/metrics`.
- **L1 per-frame LZ4 compression** (`WAL3` page-map v2 + `CID2` page index): L1 3.16MB → 140KB (synthetic) / 315KB → 86KB (real stack), page decode at 1.1GB/s; `WAL2`/`CIDX` backward-compatible.
- **Concurrent child prefetch**: `CELLHIVE_PAGED_PREFETCH_WORKERS` (default 4), reducing hashed child prefetch from ~90ms to 45.7ms.
- **GC compatibility**: LTX L0/L1 GC is based on manifest key + txid watermark and independent of the encoding format; old `WAL2` baselines mixed in can also be folded and reclaimed. `index.bin`/`manifest.json` are overwrite writes for the same key and are not added to the deletion set. After continuous folding on the real stack, `L1/` retains only 1 compressed snapshot + index + manifest, with L0 count 0; compaction logs now include `deleted_l1`/`deleted_l0`/`stored_bytes`.
- **Fixed a real bug**: after cross-process restart/eviction, a sparse paged cache opened by the base VFS would read zero pages (observed `database disk image is malformed`). Added the `<cell>.db.paged` marker: if the marker exists, "cache ≠ database"; on open, reuse the cache by cut and re-register it (discard and rebuild from the bucket if it does not match or there is no pageable source), and delete the marker after full materialization.

### LTX Compression Toggle (ADR-161)
- Added `CELLHIVE_LTX_COMPRESSION` (default `true`): LTX page-map uses LZ4 (`WAL3`); set to `false` to fall back to `WAL2` fixed frames (uncompressed). The decoder/`PageLocs`/GC are compatible with both formats, and `WAL2` and `WAL3` can be mixed in a chain, folded, and reclaimed.
- **Performance recheck (2026-09-18, same old methodology)**:
  - KV/D1 read/write (single cell, single machine 8 cores, FS bucket): reads **+13~29%**, local writes **+7~29%** (CGo driver), capture-ON writes ≈ flat (write ceiling = bucket proof); p50(c=64) put 5.11→3.98ms, get 1.71→1.40ms. Raw: `docs/archive/bench/kv-d1-readwrite-cgo.txt`.
  - Single/dual cell (capture ON, FS): dual/single ≈1.55x (same as before); S3/MinIO tier c≥16 **+33~59%**, dual cell still has no gain on single-node MinIO; capture OFF is consistent with the FS tier (writes do not hit the bucket per operation).
  - **Compression A/B**: under capture-ON, throughput difference between `CELLHIVE_LTX_COMPRESSION` on vs off is **within 1~3%** (repeated runs on the same box state) → encoding is not on the ack critical path and has no visible cost; bucket objects are still 3.7× smaller. Raw: `docs/archive/bench/ltx-compression-ab.txt`, `single-vs-dual-capture-cgo-on-repeats.txt`.

### Vectorize Migrated to SQLite Official vec1 (ADR-159)
- **SQLite driver switched to CGo**: `modernc.org/sqlite` → `mattn/go-sqlite3` (both SQLite 3.53.4, **file/WAL format unchanged**), builds with `sqlite_fts5/sqlite_dbstat/...` tags + `#error` guards; Dockerfile installs gcc and sets `CGO_ENABLED=1`.
- **Official ANN extension `vec1` statically linked** (`sqlite3_auto_extension`, no `.so` required): IVFADC+OPQ, AVX2/NEON.
- **Performance**: 20k×256/K=10 — old Go KNN ≈48ms → **flat exact ≈6.3ms** → **≈0.22ms** after `vectorize rebuild` builds ANN.
- **API unchanged**: facade/endpoints/CLI shapes unchanged; added `cellhive vectorize rebuild|drop-ann` and `POST /v1/vectorize/rebuild`, `DELETE /v1/vectorize/ann`; `describe/stats` add an `ann` field; old ADR-158 cells auto-migrate.
- **Differences**: `dot-product` is not supported (cos/l2 only); metadata filtering is post-filtering; `dev` still does not support vectorize.

### Vectorize (ADR-158)
- **Cloudflare-compatible vector index**: registered resource (`--dimensions` 1..1536 / `--metric cosine|euclidean|dot-product`, config envelope-encrypted and immutable) + facade `env.<BINDING>`: `insert/upsert/query/queryById/getByIds/deleteByIds/describe` (+ `listVectors`).
- **score/filter/namespace aligned with CF**: cosine=similarity (larger is nearer), euclidean=distance (smaller is nearer), dot-product=inner product; `$eq/$ne/$in/$nin/$lt/$lte/$gt/$gte` + nested paths + implicit AND; `topK` and `returnMetadata` limits match CF.
- **Honest implementation**: SQLite has no vector type/ANN (FTS5 is only full-text; C extensions cannot be loaded in the pure Go driver; workerd also has no vectorize binding type), so it is implemented as **float32 in the cell + exact Go KNN**; default limit is 100k vectors, measured 20k×256≈47ms and 5k×1536≈50ms.
- **CLI**: `cellhive vectorize create|list|delete|info|stats|insert|upsert|query|get-vectors|delete-vectors|list-vectors|create-metadata-index|list-metadata-index|delete-metadata-index` (data plane mints scoped tokens on the fly with the root key); `wrangler vectorize` prefix pass-through; `--config` `vectorize[{binding,index_name}]` works directly (resource=index_name).
- **dev explicitly unsupported**: Miniflare has no local implementation; `cellhive dev` exits with an error when vectorize is present (no longer pretending to be usable).

### Per-Domain Resource Management and stats (ADR-157)
- **Per-domain registration**: `POST|GET|DELETE /v1/<kind>/resources` (kv/d1/queue/r2/workflow/hyperdrive), with server-side default scope; queue dead-letter replay via `POST /v1/queue/dead-letters/replay`; old `/v1/control/*` paths retained as compatible aliases.
- **Per-domain stats** (metadata-only, no read amplification): `GET /v1/<kind>/stats` — page/file/WAL bytes, `kv_expires` indexes and expirations, D1 table list (`?tables=1` dbstat details), queue backlog + lag, R2 bounded approximation, workflow instance estimate, hyperdrive registration, DO index aggregation.
- **Fix**: new databases were missing the `kv_expires` partial index (a real bug that made the TTL cleanup sweeper scan the whole table).
- **Metrics**: `/metrics` adds `cellhive_bucket_ops_total{op}`, `cellhive_list_calls_total`, `cellhive_owned_cells`, `cellhive_resident_cells`; `observability.md` aligned with code and marks planned metrics.
- **CLI**: `cellhive kv namespace|d1|r2 bucket|queue|workflows|hyperdrive create|list|delete|stats` (`resource *` remains the generic entry point).

### vwork Operations Interfaces (ADR-156)
- **Resource revocation**: `DELETE /v1/control/resource` (409 `resource_in_use` + `referenced_by`, `--force` to force) + `revoked_resources` tombstones (immediate fail-closed, re-registration unblocks) + CLI `resource delete`.
- **Queue operations**: `GET /v1/control/queue/status` (depth/visible/leased + DLQ depth), `POST /v1/control/queue/replay-dlq` (re-enqueue dead letters to the main queue) + CLI `queue status|replay-dlq`.
- **Readiness/drain probes**: user-runtime `/ready` + `/drain` (SIGTERM drains automatically, 3s grace period), do-runtime `/ready`; compose/k8s/Helm probes switched to `/ready`.

### P3 — DO Completion (Wrap-up)
- HTTPCommitter durability proof relaxed to `fleet`/`bucket`/`bucket-batch` (reject `bucket-async`).
- **WebSocket cross-node forwarding**: `proxyConnect` proxies to owner, 1012 passes through (`TestDoRuntimeWebSocketCrossNodeForward`).
- **`transferred_classes`**: same-worker `{from,to}` = rename alias; cross-worker `script_name` rejected.
- Runtime VFS lazy read: boundaries finalized (ADR-085), alternative = materialize on cold start by object/page.
- Incremental cache for `refreshFacets`; `deleteAll()` shim (KV+SQL).
- `TestDOCompatSuite`: 15 subtests.

### P2 — Bindings and Assets
- Deployment-time cron validation (`invalid_cron`).
- Asset write side (CLI `asset put`/`bundle put`/`deploy --assets-*`).
- **Workflows Partial (ADR-086)**: self-developed engine (`__workflow__` cell, memoized `step.do`, `sleep` via timer, facade `env.WF`, cell-agent API, `cellhive workflow create`).

### P4 — Multi-Tenancy + Scaling (ADR-087)
- Release log `Releases` + idempotent deploy `idempotency_key`.
- Admission: per-namespace write rate limiting (`CELLHIVE_NS_RATE=rps[/burst]`) → 429.
- Autoscaler **signals**: `internal/autoscaler` + `/v1/control/capacity`.
- CLI command surface completed (`status`/`capacity`/`releases`/`workflow create`).

### P5 — Operations and Hardening (ADR-088)
- Protocol versioning with reader-before-writer (`cell.SupportedProtoVersion`, claim fail-closed).
- `/v1/diagnose` enhanced (proto/admission/capacity/counters).
- Vendor matrix regression guard `TestVendorMatrixContract`; load tests `cmd/*bench`; recovery chaos tests.


### Addendum (Five Closing Items)
- **ServiceBinding RPC (ADR-090)**: `env.SVC.<method>()` is forwarded via Proxy to the named entrypoint of the target worker; `fetch()` is retained.
- **Workflows subset (ADR-086)**: `pause/resume/terminate/restart`, instance `list`, `step.do` `retry/backoff`, `step.waitForEvent`; facade `env.WF.get(id)` is a CF-shaped instance object.
- **S3 vendor validation (ADR-091/testing)**: `make s3-test` runs local MinIO conditional writes/CAS/range/presign + replication recovery chain in one command (`CELLHIVE_S3_TEST_ENDPOINT` gated). Cloud object storage still lacks credentials → blocker.
- **Benchmark regression (docs/benchmarks.md)**: p50/p99 for cellbench / realbench / sqlbench (FS and S3 tiers), raw outputs archived under `docs/archive/bench/`.
- **CLI (ADR-091)**: `app list`, `resource list`, `tail` (follow audit logs), `deploy --assets-dir` (version-level assets token).

### Control Plane, Domains, and Runtime (ADR-127~133)
- **Internal dispatch version key (ADR-127/128)**: internal dispatch body carries `version`; queue/scheduled/workflow dispatch injects bindings (`GET /v1/internal/worker/bindings`, fail-open); internal loader id format is unified with the public entry format.
- **Hyperdrive (ADR-129)**: origin URL is stored as a registered resource with **envelope encryption**, and `/v1/internal/hyperdrive` resolves by name; CLI `resource create --connection-string`; the platform does not provide connection pooling.
- **Local connection reuse (ADR-130)**: measurements show normal workers cannot reuse sockets across requests → reuse depends on holding `cloudflare:sockets` connections inside their own DOs.
- **Control plane schema v2 + domains/routes + multi-tenant auth (ADR-131)**: soft delete + purge jobs, `bindings` derived table, `hosts`, JWT `cellhive_ns` authorization, paginated listing, `domain`/JWT CLI; custom domain mount prefixes are **always stripped**, with segment-boundary matching.
- **Removed edge config delivery (ADR-132)**: deleted `GET /v1/internal/traefik` and `CELLHIVE_ADMIN_HOST`/`_ADMIN_BACKEND_URL`; edge is statically configured by operations, while the platform only guarantees loader-level host gating.
- **No DNS validation for domains (ADR-133)**: registration implies authorization (`verify_state` field and validation loop removed), built-in domains are only `<ns>-<worker>.<base>`.

### Review Fix Batch (ADR-134/135)
- **ADR-134**: authorization **before** forwarding (otherwise forwarding after swapping to an internal token could bypass ns validation); all captured writes must go through `Cell.Tx` (RPO=0 watermark); `compatibility_date`/`compatibility_flags` actually take effect (`nodejs_compat` e2e can falsify it); `Bucket.ConditionalDelete` + owner conditional release; `Forget` drain + `ForgetWithVerify`; automatic WAL truncation; bounded retries for uploads; `ParseScope` rejects `..`; `r2.List` uses `SizeLister`.
- **ADR-135**: **purge vs redeploy** (Deploy revives worker/app and cancels the purge job; purge only applies to entities that are still soft-deleted); **captured-commit epoch fence**; DO WS `connect` identity matches invoke (including storage_class lease key); `DeleteNamespace` drains via `forget`; `cellcapture.Ensure` snapshot moved out of the global lock; JWKS refresh moved out of the lock + singleflight + use cached keys while in flight, JWTs without `exp` are rejected; `internal.js` bundle cache 256 + singleflight; `_headers` match by request path, log-tail attaches to `ctx.waitUntil`, R2 range without length omits end; `cellhive diagnose` adds **conditional-delete probing**; CLI subcommand arity validation.

### Internal Protocol and Wiring Fixes (ADR-136)
- **Removed the never-implemented gRPC plane**: internal Go↔Go has always used HTTP (LTX is length-prefixed binary frames + HTTP 101 persistent stream, ADR-042); removed `CELLHIVE_GRPC_ADDR`/`:7000` and `Config.GRPCAddr`.
- **`CELLHIVE_ADVERTISE` default changed to `127.0.0.1:7001`** (old `:7000` had no listener, so owner forwarding pointed to a dead port by default).
- **Deleted `CELLHIVE_CELL_AGENTS`/`Config.CellAgents`**: `ownerclient.New` seed discovery had no callers (the library keeps only `Hint`/`OwnerURL`).
- **Wired the log tail buffer**: `cmd/cell-agent` now constructs `logbuf.New(cfg.LogBufferEntries, cfg.LogBufferWorkers)` → `/v1/internal/logs` and `cellhive tail --worker` are usable in production (previously always 503).
- **Unified `CELLHIVE_DO_OBJECT_INDEX` parsing** (`true`/`1`), eliminating the inconsistency between cell-agent and do-runtime.

### Configuration surface simplification (ADR-137)
- **Single root key**: `CELLHIVE_ROOT_KEY` is used with HKDF to derive peer/internal/dispatch/log/admin/scope/do-ticket/secrets-root; 8 independent secret variables were removed, and `CELLHIVE_ADMIN_TOKEN` is only an optional override.
- **Standard AWS bucket credential names**: `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`/`AWS_ENDPOINT_URL`/`AWS_REGION`.
- **Tools and scripts updated accordingly**: `cellhive creds [role]` prints credentials derived from root; bench/probe tools (kvbench/realbench/sqlbench/recoververify/*-supervisor) now default to root-derived tokens; the full `make rpo-test` flow (cross-process + tools) now uses root.
- **Durations unified to Go syntax** (`PEER_LATENCY`/`BINDING_CACHE`/`CELL_IDLE`/`CAPTURE_*`/`DO_LEASE`); directory derivation (`CELLHIVE_RUNTIME_DIR`, spool, DO disk); merged `CELLHIVE_LOG_BUFFER`, `CELLHIVE_NS_RATE`, derived `WAKER_TTL`/`DRAIN_WAIT`; removed `CELLHIVE_COMMIT_MODE`. Total env count: 99 → 85.

### wrangler-style usage (ADR-138)
- `cellhive wrangler <command>`: `deploy` (auto-discovers `wrangler.jsonc`, translates `--namespace`/`--name`/`--env`), `delete`→`worker delete`, `versions|deployments list`→`releases`, `secret`/`tail`/`rollback`/`promote`; unsupported wrangler flags/commands return a clear alternative or "unsupported".
- Official wrangler using `CLOUDFLARE_API_BASE_URL` to connect directly (CF API subset) is an alternative, not implemented.

### Deployable artifacts (ADR-139)
- Single image (5 binaries + **pinned workerd 1.20260916.1** + `workerd/` JS, glibc base image), `deploy/entrypoint.sh` dispatches services by short name; `deploy/compose/docker-compose.yml` (cell-agent + user-runtime + do-runtime, `s3`/`edge` profiles, only requires `CELLHIVE_ROOT_KEY`); `deploy/k8s/` kustomize manifests (StatefulSet/Deployment/HPA/ConfigMap/Secret examples); Makefile `docker-build`/`compose-config`/`compose-up`/`k8s-render`.

### DO output-gate deployment (ADR-140)
- `do-runtime -render-only` + `do-supervisor` takes over the workerd lifecycle (lease renewal/drain); compose profile `rpo0`, k8s `overlays/rpo0`; container end-to-end verification that DO calls go through the gate and land in the bucket.

### Deployment hardening and gates (ADR-141)
- Run as non-root (UID/GID 65532 + fsGroup), ServiceAccount/PDB/NetworkPolicy/startup probes; Helm chart (`deploy/helm/cellhive`, `doRuntime.gate` toggles RPO=0); `scripts/ci.sh`/`make ci` one-command gates + GitHub Actions (go/js/cli/deploy four jobs). Latest run: `GATE: PASS 13/13`.

### purge closure (ADR-142)
- The data-side hook for `RunPurgeLoop` has been wired: deleting a worker clears that worker's DO storage and assets, deleting an app clears the entire ns; executed by every cell-agent (cross-node local replica drain + idempotent bucket deletion), resumable (bounded deletes/round). Fixed a portability bug where the FS bucket could not list keys by in-segment prefixes.

### async upload persistence (ADR-143)
- Pre-write spool (`<DATA_DIR>/upload-spool`) + startup/periodic replay: failures changed from `dropped` to `deferred` (not lost on process restart); `/metrics` exposes `cellhive_upload_{dropped,deferred,replayed}_total` and `upload_spool`.

### service binding ACL (ADR-144)
- Target-side `service_acls` allowlist + deploy-time `service_binding_denied` rejection + runtime revalidation; supports `ns/worker` cross-namespace service binding (`target_ns` protocol, caller scope token unchanged); CLI `cellhive service-acl add|ls|rm`.

### R2 list pagination (ADR-145)
- `bucket.PagedLister` + `r2.ListPage`: S3 uses `StartAfter`+`MaxKeys`, FS uses bounded selection, no longer materializes the full prefix; `/v1/r2/list` and facade return `truncated`/`cursor` (R2 semantics).

### trace context propagation (ADR-146)
- W3C `traceparent`: entry generation/forwarding → tenant handler → facades → cell-agent service/DO outbound; no OTLP/sampling (residuals in known-issues).

### secrets management (ADR-147)
- `DELETE /v1/control/secret` + `GET /v1/control/secrets` (keys only) + CLI `cellhive secret delete|list` + audit `secret.delete`.

### CLI compatibility (ADR-148)
- Server-side `dry_run` + `deploy --dry-run/--var/--secrets-file`; `versions`/`deployments`/`triggers`/`workflows`/`queues`/`types`/`init`/`d1`/`kv`/`r2` compatibility groups (mapping or structured rejection + alternative); `cellhive wrangler` passes through these three flags and forwards compatibility groups.

### Class C environments (ADR-149)
- All local substitutes PASS: MinIO conditional write/delete + ListPage, `make rpo-test` multi-process RPO, peer latency injection, mock OIDC(JWKS/JWT), owner simulation, vendor contracts; real cloud/second host/real IdP/orchestration/certificates are listed as residuals.

### Root key source and mTLS decision (ADR-150)
- `CELLHIVE_ROOT_KEY_FILE` (Docker/K8s secret mount) + inline env as two sources, fail closed; cloud KMS seam left but not implemented; mTLS/internal CA decision is to not implement for now (rationale and triggers recorded in security.md).

### scaling and multi-AZ (ADR-151)
- Autoscaler cooldown `CELLHIVE_AUTOSCALE_COOLDOWN` (default 5m, debounce); `CELLHIVE_PLACEMENT_AZ` + follower cross-AZ preference (a single-AZ failure does not take away all replicas); rebalance interval/batch size finalized.

### dispatch finalization (ADR-152)
- Configurable queue batch/lease/redelivery delay, timer/waker batch size and fired TTL, waker exponential backoff (capped at 1m); timer cell explicitly does not do pinning (evictable + bucket wake index lookup).

### Compatibility matrix closure (ADR-153)
- **Bug fix**: the platform bundler now externals `cloudflare:*` by default (`node:*` only with nodejs_compat), so prebuilt artifacts from frameworks such as OpenNext can `deploy --config` normally.
- Precise list of `compatibility_flags`, mirrored with the dev CLI and enforced by tests; pin `1.20260916.1` ↔ compatibility ceiling `2026-09-23` binding; OpenNext/SvelteKit/Astro layout acceptance; D1 sessions/bookmarks explicitly rejected.

### dispatch defect fixes (ADR-154)
- Fixed: timer body missing `namespace` (cron/scheduled always 400), deployment artifact missing `CELLHIVE_DISPATCH_URL` (queue/timer loops silently not starting), empty `event.cron`, queue batch not in CF shape (`.messages`/`.queue`/`ackAll`/`retryAll`) and properties/trace lost across isolates.
- Full-stack e2e passed: fetch+KV+DO, queue batch consumption, cron trigger, 0 cell-agent dispatch failures.

### Queue per-message semantics (ADR-155)
- `message.ack()` / `message.retry({delaySeconds})` / `batch.retryAll({delaySeconds})` (unhandled messages implicitly ack, over-limit messages go to DLQ) + delayed production `send(body,{delaySeconds})` + `message.body` typed by content-type; validated by real full-stack e2e (retry 5s / delay 6s both match timing).

### Known boundaries
- Class C environment validation lacks environments (cloud object storage conditional write and conditional delete, real cross-host RTT, multi-host chaos/takeover, real autoscaling orchestration) — recorded truthfully, not fabricated.
- When S3-compatible storage ignores `If-Match`, conditional delete degrades to unconditional delete (owner/lease fence depends on it); `cellhive diagnose` includes a self-check probe.
- async upload only has bounded retry (3 attempts with backoff) + `dropped` counter alerting, **no persistent retry queue**.
- The cross-node purge hook for cell-data/bucket cleanup is still not closed (ADR-131 residual).
- Workflows Partial (does not support pause/resume/terminate/restart, retry configuration, waitForEvent, delete/locationHint/cross-worker).
- Runtime SQLite VFS lazy reads are not feasible with stock workerd (ADR-085).

**Verification**: `gofmt` / `go vet` / `go test ./...` all green; `make build`.
