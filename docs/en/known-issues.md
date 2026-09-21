# Design Decisions (Review Issue Closure)

This document records all issues found during the system review, as well as **the resolution decision for each issue**. See the appendix at the end for the original severity levels. The design documents (architecture / cell-protocol / durable-objects / networking / decisions) have been synchronized accordingly.

_Last updated: 2026-09-19_

---

## Decision Overview

| ID | Topic | Decision Status |
|---|---|---|
| C-01 | DO owner claim flow | ✅ Finalized (durable-objects.md) |
| C-02 | bundle/assets read path | ✅ Finalized: short-lived scoped credentials directly access object storage (decisions.md TBD #1) |
| C-03 | node-log / recovery | ✅ **Fully implemented** (ADR-057: `RecoverNode` lists node columns + fast path; graceful shutdown seal; E2E kill -9 restores 128 segments, latency 0.04s, RPO=0/keys=1000. **ADR-066**: automatic orchestration for node death has been wired) |
| C-04 | DO cold activation / restore | ✅ Finalized (durable-objects.md) |
| C-05 | DO alarm recovery | ✅ Finalized (see below) |
| C-06 | Route projection delivery | ✅ Finalized: **pure pull + 5–10s TTL (no push)** (ADR-031) |
| C-07 | In-flight DO request migration | ✅ Finalized (durable-objects.md) |
| I-01 | Tenant binding authorization | ✅ Implemented (ADR-061: scope claims + scoped token; enforced on KV endpoints) |
| I-03 | WAL checkpoint/truncation | ✅ Finalized (see below) |
| I-04 | DO WebSocket reconnect | ✅ Finalized (see below) |
| I-05 | Timer dispatch deduplication | ✅ Finalized (see below) |
| I-07 | scope-hash routing hotspot/failover | ✅ Finalized: best-effort affinity (see below) |
| I-09 | Multi-tenant isolation security | ✅ Finalized (see below + security) |
| M-01 | MinIO version note | ✅ Finalized |
| M-02 | :7000 protocol multiplexing | ✅ Finalized: **:7001 single internal REST** (shared by Go↔Go and JS↔cell); **gRPC plane canceled** (ADR-136) |
| M-03 | owner expiry vs node lease TTL | ✅ Finalized: expiry ≤ node lease TTL |
| M-04 | workerd multi-file actor storage | ✅ Confirmed (P0.7): `metadata.sqlite` + per-actor `<hash>.sqlite`(+wal/shm); see below |
| M-05 | Static asset serving path | ✅ Finalized: direct access to object storage/CDN (consistent with C-02) |
| M-06 | Internal vs external API | ✅ Finalized: same REST interface, authorization only differs |
| M-07 | drain token protocol | ✅ **Implemented** (ADR-057: `internal/drain`, conditional create/CAS/TTL/Release; used by cell-agent graceful shutdown) |
| M-08 | cellhive dev | ✅ Finalized (see below) |
| M-09 | cell protocol versioning | ✅ Finalized (see below) |
| M-10 | Rate limiting/quota/admission | ✅ Finalized; **admission implemented** (ADR-087), remaining items below |

---

## C-05 DO Alarm Recovery (Finalized)

1. alarm rows are replicated to cell-agent with the actor SQLite (durable).
2. **supervisor only reads workerd's alarm table and extracts due times**, then reports to cell-agent: `alarm_upsert(scope, dueAt, token)` / `alarm_delete(scope, token)`.
3. cell-agent maintains the due index in the **waker cell** (bucketed by time).
4. When waker finds an item whose "owner is dead + due", it **targets takeover of that DO** (claim + restore) to an available do-runtime. The new owner reads the alarm row and dispatches it; after successful dispatch, it atomically advances/deletes the index item.
5. **DOs with alarms are prioritized for residency** (placement policy / similar to `DO_PREVENT_EVICTION`) to reduce cold activation.
6. If the alarm table cannot be reliably extracted (P0 schema validation), fall back: waker periodically queries live supervisors for their earliest due alarm, and only performs takeover for owners that are dead.

## I-01 Tenant Binding Authorization (Finalized)

- **The primary line of defense is network isolation** (see I-09): tenant Workers **cannot** access the internal network cell-agent :7001.
- The internal token **only exists between the host adapter (platform code) and cell-agent**, and is **never injected into the tenant env**; tenants only receive the binding facade.
- cell-agent validates the **scope claims** injected by the host adapter (`ns` + `binding type/id`); missing claims or privilege escalation is rejected.
- No new per-load token is added (can be added later as defense in depth).
- **See ADR-036 for the admin console identity model**: no distinction between administrators and tenants; a single admin console, with tenant identity passed as a request parameter; the admin console is never exposed to tenants/the public Internet.
- See the planned `security.md` for details.

## I-03 WAL Checkpoint/Truncation (Finalized)

- supervisor polls `(salt, frame)`; salt changes or frame resets to zero = checkpoint occurred.
- On detection → **pause the delta stream** → use the **SQLite Online Backup API** (read-only connection) to take a consistent snapshot → upload it as an **L9 snapshot** (temporary key + atomic commit) and record txid → resume delta from the new salt/frame 0, and write the **snapshot→delta handoff marker**.
- cell-agent validates snapshot integrity and handoff continuity.
- P0 must test: detection and alignment when a checkpoint is triggered **during active writes**.

## I-04 DO WebSocket Semantics (Finalized, CF-compatible)

- **Deployment / promote / migration all restart the DO**: the old owner closes WS with **1012**; **the client reconnects**; the new owner reconstructs, and **the handler restarts** (`webSocketOpen` is triggered again).
- **No resume**: the platform does not guarantee session continuity across restarts; DO session state must be persisted to storage.
- The host adapter is only responsible for: detecting disconnect → resolving the owner again → having the client reconnect.
- Consistent with Cloudflare behavior (ADR-032).

## I-05 Timer Dispatch Deduplication (Finalized)

- Deduplication is on the **cell side**: the timer carries `token = hash(scope, kind, dueAt, occurrence)`; cell SQLite records `fired:<token>` (TTL).
- **Tokens already fired are skipped**; cron triggers once per slot unique key; DO alarm is based on the alarm row in actor SQLite, and after successful dispatch the due index is atomically advanced/deleted.
- user-runtime **does not perform deduplication** (stateless).

## I-07 scope-hash Routing (Finalized)

- The hash is only **best-effort affinity (performance hint)**, not a hard constraint.
- If the preferred cell-agent is unavailable → **switch to the next available node**; segments uploaded by different nodes are merged by **epoch prefix** without ambiguity; the owner record is authoritative.
- An optional consistent hashing ring can reduce churn when membership changes.

## I-09 Multi-tenant Isolation (Finalized)

- workerd capnp: `globalOutbound` of a tenant loaded worker = **public-Internet-only network service** (excluding RFC1918 / cell-agent addresses).
- **The host adapter's internal-network binding is not exposed to tenants**.
- Each worker is configured with `limits` (`cpu_ms`/`subrequests`) + V8 heap limit.
- See the planned `security.md` for details.

## M-02 Port Allocation (Finalized)

| Port | Protocol | Purpose |
|---|---|---|
| `:7001` | **REST/JSON + binary frames** | All internal traffic: shared by Go↔Go (peer LTX, DO WAL/claim/restore, owner forwarding) and workerd(JS) bindings; the LTX hot path is a length-prefixed binary frame over an HTTP 101 persistent stream (ADR-042) |
| `:8082` | REST/JSON | External admin/control plane (via Traefik) |

## M-06 Internal/External API (Finalized)

**Same REST interface, authorization only differs**: internal uses `:7001` (platform token + scope claims), external uses Traefik→`:8082` (tenant token). "External postponed" in ADR-013 = not yet externally exposed, **not a separate API**.

## M-08 cellhive dev (Finalized; full design in dev-mode.md)

Status: **M1/M2 implemented** (`cli/` Bun dev + `internal/wranglercompat` server-side interception, 2026-09-15, see below); remaining items below. Key points:

- **dev = Bun CLI + Miniflare**: Miniflare starts real workerd + locally simulated bindings; **zero Go backend processes on the dev machine** (ADR-065). Production remains Go workerd + cell-agent.
- **Version pin**: `miniflare@5.20260916.0-alpha` is exactly overridden to platform-pinned workerd `1.20260916.1`; lockfile and installed-version tests reject drift.
- **Zero external dependencies**: no Docker/MinIO/Traefik required; data is in `./.cellhive-dev/` (Miniflare persist).
- **Standard CF shape**: `wrangler.jsonc` + KV/D1/R2/Queue/DO/`vars`/`secrets`/assets; hot reload/inspector/persist use Miniflare's built-ins.
- **Contract comparison**: Miniflare serves as the CF semantics oracle; golden comparisons constrain our Go-side behavior (ADR-014 approach).
- **deploy server-side functionality/compatibility interception (ADR-065)**: authoritative server-side validation (bundle/date/flag/binding matrix/resources registered/DO lifecycle/unknown fields); **platform rejects `images/ai/browser/vectorize/...` supported by Miniflare** → CLI/dev use the same validation library for early warnings.
- **User code compatibility is the platform's responsibility**: fill facade gaps (KV list metadata, R2 `R2Object` fields, D1 `meta.last_row_id`, error shapes).
- **M1 implemented and validated (2026-09-15) ✅**: `cli/` (Bun) created; `cellhive dev` reads the wrangler config subset → starts the Miniflare dev server, and **KV/D1/R2/`vars` all pass via HTTP**; preflight correctly rejects `compat_date_too_new`/`unknown_flag` and reports `unsupported_binding` warnings for `images`/`ai`, etc.
- **pin rule (important correction)**: Miniflare and the platform-pinned workerd must be exactly aligned from the same period. The current pair is **`miniflare@5.20260916.0-alpha` + `overrides.workerd=1.20260916.1`**, with real module/KV/D1/R2 and version-readback evidence.
- **M2 implemented and validated (2026-09-15) ✅**: Go `internal/wranglercompat` (binding matrix/`compat_date`/flags/resource registration/unknown fields + stable codes + unit tests); `/v1/control/deploy` server-side interception wired (`deploy_rejected` + findings + bundle existence point check); CLI preflight aligned with the same code set; `internal/server` interception test cases pass.
- `images:{binding:"IMAGES"}` can be constructed locally and starts **without credentials** → dev will actually make platform-rejected `env.IMAGES` "usable" (exactly what server-side interception must cover). Remaining tests: actual local behavior of Miniflare `ai`/`browser` (whether CF credentials are required).
- dev: hot reload/Queue/`rules`/assets/`--strict-build` are implemented. Miniflare 5's built-in `has_user_worker` router cannot satisfy not-found handling and worker fallback together; ADR-114's same-instance, no-extra-listener, dev-only service-binding router is implemented, and `_headers`/`_redirects`/404-page/SPA/worker fallback/`run_worker_first`/binding/hot reload all pass real tests.

See [`dev-mode.md`](dev-mode.md) for details.

## M-09 Protocol Versioning (Finalized)

- owner record adds `proto_version`; **LTX segment header carries a version byte**.
- Upgrades follow **reader-before-writer**; **rollback-safe: read/write version difference ≤1**; incompatible changes use an explicit migration process.

## M-10 Rate Limiting/Quota/Admission (admission implemented, ADR-087)

- per-namespace write rate limiting; cell-agent returns `503 overloaded` when overloaded.
- **Per-DO WAL size limit**; worker `limits` (cpu/mem); consume `pressured`/`shed_cells` for backpressure.
- queue DLQ limit.
- Required before multi-tenant production. **Current status (2026-09-16): admission implemented** (`internal/admission`: per-namespace token bucket + write endpoint 429, ADR-087). **Remaining**: per-DO WAL size limit, worker `limits` (cpu/mem), `pressured`/`shed_cells` backpressure consumption, queue DLQ limit (DLQ itself is done, ADR-072).

## M-04 workerd Multi-file Actor Storage (✅ Confirmed)

P0 first enumerates workerd's actual file layout (shared metadata + per-actor SQLite); initially use whole-directory copy + directory-level snapshot to ensure consistency; WAL is per SQLite file.

---

## Unverified / To Be Implemented (Design Fixed, Requires P0 or Implementation Validation)

The following are **not unresolved design issues**, but "design is fixed, while conclusions depend on empirical validation or are not yet implemented":

### P0 Must Be Tested (Feasibility/Performance Gate)

| Item | Status | Evidence / Fallback if Failed |
|---|---|---|
| Whether workerd actor SQLite is in WAL mode and can be opened read-only externally | ✅ Verified | P0.7: `walscan` parsed real workerd actor `-wal` (workerd 2026-06-15) |
| workerd actor file layout (shared metadata + per actor, M-04) | ✅ Confirmed | P0.7: `metadata.sqlite` + `<hash>.sqlite`(+wal/shm) |
| LTX encode/decode / replication / epoch restore | ✅ Verified | P0.3: `internal/ltx`, `internal/replica` tests |
| Two-node quorum-1 fsync ack + bucket degradation | ✅ Verified | P0.4: `internal/peer`, `internal/server` tests |
| node-log + recovery (acked but not uploaded → collect and land in bucket) | ✅ Verified (simulation) | P0.5: `internal/recovery` tests |
| PUTs per ack ≪ 1 (group commit) | ✅ Achieved (fleet ≈0.09) | P0.6: `internal/upload` + `cmd/cellbench` |
| List prohibited on hot path | ✅ Meets target | P0.6: node sample 5s cache; `list` only occurs on refresh |
| checkpoint/truncation detection and alignment (I-03) | ✅ Tested in practice (workerd 2026-06-15): DO SQLite defaults to **1000-page autocheckpoint**; each checkpoint **increments salt1 (salt rotation)**, resets `total_frames` to 1000, and the WAL file is **reused at the same size ~4.12MB (not TRUNCATE shrink)**. Our `wal.Cursor` reports `Checkpoint=true` on salt rotation and re-bases → aligned with workerd | Observed: triggers once every ~1000 writes; `walscan` observed salt sequence `...889→890→891...`, size constant 4120032. **Implication**: stock workerd cannot disable autocheckpoint, so per-actor capture will create one snapshot boundary about every ~1000 writes (handled by ADR-049); fallback: periodic snapshots |
| Reliably extract due alarms from the workerd alarm table (C-05) | ✅ Tested (workerd 2026-06-15): alarms are stored in per-namespace `metadata.sqlite` as **`_cf_ALARM(actor_id TEXT PRIMARY KEY, scheduled_time INTEGER) WITHOUT ROWID`**; `scheduled_time` is **Unix nanoseconds** (`Date.now()+Δms` is stored as ns; observed row `actor_id=862b…a8112, scheduled_time=1789453754754000000`); extracting due alarms = `SELECT actor_id FROM _cf_ALARM WHERE scheduled_time <= <now_ns>` | Trigger: real DO `storage.setAlarm(Date.now()+Δ)`; actor DB `<hash>.sqlite` is in the same directory as metadata. Fallback: waker asks live supervisor |
| Real S3 conditional writes + presigned | ✅ Tested with local S3-compatible storage (MinIO); ◐ cloud object storage differences not tested | `cmd/s3probe` / `cmd/s3init`; confirm cloud side with `diagnose` probes |
| **fleet commit performs one S3 GET per commit (owner epoch)** | ✅ Fixed | `owner.ResolveCached` (1s TTL, invalidated on claim/release); with remote S3, fleet c=1 p50 **1.26ms** |
| **follower spool fsyncs each segment separately (fleet ack throughput capped at ~1.1k RPS)** | ✅ Fixed | Changed to single append-log `segments.log` + one batched fsync (`Spool.AppendBatch` + follower batcher); 2-node fleet **~1.1k → ~13.8k → ~45k RPS (framed batching another 3.3x; on par with single-node bucket async)** |
| Persistent peer streams / pipeline | ✅ Implemented (ADR-042) | HTTP 101 custom binary stream; 4 scope-hashed lanes × 4 batches in flight; single scope 45.0k→47.1k (+4.5%), 8 scopes p99 ~63–65ms→48–52ms; throughput bottleneck has shifted to local CPU/single disk |
| Real SQL→WAL→LTX→fleet TPS | ✅ Full Go SQLite path (ADR-043~050); ✅ **real workerd DO output gate end-to-end** (ADR-051, `cmd/cell-supervisor` + `workerd/spikes/p0/gate/` + `cmd/gatebench`) | Go SQLite: loopback c=128 29.6k TPS/p99 9.1ms. workerd **process-level** ceiling: bare DO ≈**800–855 req/s**, with output gate N=1 553 / N≥4≈790; **multiple DOs do not scale horizontally within a single workerd** (1→8 actors all ~800, fsync constrained). Non-DO `sqlbench`: **durable 28.9k / pure local 43.7k TPS** (c=128). cell-agent KV (after ADR-052 cell caching): PUT ~**5.4k** / GET ~**12.9k** rps (c=32). Recovered actor DB `t.n` exactly matches, integrity ok |
| SQL capture performance optimization | ✅ ADR-044/045/046: incremental WAL + raw binary commit + in-memory ticket + WAL2 page-map + prepared statement | loopback c=128 **24.8k TPS / p99 9.3ms**; pure SQL ceiling ~107k (prepared). Throughput stays flat at c≥256, latency rises |
| snapshot + restore apply | ✅ ADR-049: after checkpoint, read full-page DB file snapshot + `restore.ApplyFile` page-by-page reconstruction | **End-to-end data correctness validation passed**: 5,000 unique keys restored from follower replication chain → 5000/5000 rows, 0 missing/wrong values, integrity=ok. ◐ Controlled checkpoint must be called during a quiescent period |
| Automatic WAL truncation under continuous writes | ✅ ADR-050: pause writer → short TRUNCATE → watermark → DB file snapshot → new WAL continues delta | 5000 unique keys under continuous writes + automatic ckpt → chain restore 5000/5000, 0 missing/wrong values, integrity ok; loopback c=128 ckpt=4MiB 22.2k TPS (vs 29.6k without ckpt)|
| fleet cross-network RTT | ✅ ADR-048: capture-level ordered pipelining implemented (adaptive; loopback uses serial path) | 20ms one-way c=1024 **8.6k → 13.3k TPS (+55%)**; 5ms c=1024 24.0k → 29.3k (+22%); loopback flat. Controlled checkpoint still awaits snapshot/link apply |
| Single-node commit mode | ✅ Implemented: `CELLHIVE_DURABILITY`=`auto`\|`fleet`\|`bucket` + `CELLHIVE_BUCKET_WAIT` (default true). `fleet` degrades to waiting for bucket when there is no peer and logs a warning, not silent ack; `fleet`+`WAIT=false` refuses to start | FS c=64 (3 rounds): async median ~38.5k / batch ~29.2k RPS (async ~1.3x); **MinIO loopback noise ±3x, the two are basically tied**, single-round ranking is not trustworthy |
| Cloud object storage tail latency | ⚠️ Observed: remote object storage single-node batch p50 can reach hundreds of ms to seconds, with occasional timeouts; async has low p50 but RPO>0 | Bucket commit only in background; use fleet with ≥2 nodes; same-region/nearby |
| **S3 conditional write/range semantics** | ✅ **Repeatable script `make s3-test` (local MinIO)**: all four `s3probe` checks OK (conditional create / reject-create / CAS / reject-stale) + ranged read; `TestS3BucketIntegration`, **`TestS3ReplicationRestoreChain`** (snapshot→Restore→ApplyFile 500 rows; Compact→PageFetcher **ranged**→Materialize 500 rows)| Use local `minio`/`rustfs`: see `docs/testing.md` "S3 integration tests". Remaining: cloud vendor/region differences not tested |
| Cross-node "capture→cell-agent→proof" p50/p99 | ✅ Real multi-process + real TCP (loopback) tested; ◐ cloud/cross-host not tested | Evaluate FUSE or adjust placement |
| do-runtime cold activation/restore time | ◐ Not verified (restore 500 segments 5ms, non-workerd actor) | paging/residency strategy |
| Compatibility date/flag table and pinned workerd | ✅ Tested (workerd 2026-06-15): supports `compatibilityDate` up to **2026-06-22** (using a newer date hard-errors with "newest date supported ... 2026-06-22"); unknown `compatibilityFlags` hard-error with `No such compatibility flag: <x>`, known flags (such as `nodejs_compat`) pass config validation | Probe method: temporarily set a future date/pseudo flag in capnp + `workerd serve`. Meaning: the flag set of pinned workerd must be validated item by item at load time; do not blindly trust wrangler defaults |
| `workerLoader` loads tenant modules + real KV round trip | ✅ Verified | P0.7: `workerd/spikes/p0/`, `put:200` / `get:hello-world` |
| `globalOutbound` public-only isolation enforcement | ✅ Tested (workerd 2026-06-15): when outbound `network.allow=["public"]`, tenant `fetch("http://127.0.0.1:7001")` is rejected: `connect() blocked by restrictPeers()` (host returns 500). **Isolation can be enforced without modifying workerd** | P0.7 loader defaults to `allow=["public","private"]` so it can connect to local cell-agent; production should set `["public"]`, with cell-agent accessed via service binding/HMAC rather than public fetch (**implemented: ADR-073**—tenant outbound public-only, facade egresses via `PLATFORM` service binding, real workerd e2e) |
| snapshot watermark and mirror consistency | ✅ **Fixed** (ADR-055): during `poll`, snapshot was taken immediately after detecting checkpoint without backfilling → empty/stale mirror was marked with high watermark → silently lost 1..N transactions. Fixed as `rebaseline` (pause writer + TRUNCATE backfill + read W0 before reading pages). Regression `TestRebaselineSnapshotReflectsCommittedWrites`; E2E full restore `integrity=ok` |
| Cold-start on-demand fetch (bucket) | ✅ ADR-056 + GC: `compaction` writes `L1/index.bin` (page→object offset), `replica.PageFetcher` uses `RangedGet` to fetch a single page (L0 override takes precedence); `restoreverify -bucket` observed full `integrity=ok`, `-pages 1-2` sparse holes correct |
| P1 cell replication complete (LTX/snapshot/compaction/paging) | ✅ Snapshot paging, compaction core + orchestration, txid continuity, **sparse on-demand paging** (ADR-055/056), and **L0/L1 GC** (ADR-056) are all complete (see "P1 replication gap checklist" at the end for details) | Snapshot paging: `ltx.EncodeSnapshotParts` (flags shards, `<txid>.p<part>.snapshot`) + `restore` merge + capture/supervisor integration. compaction: `restore.Compact` + `internal/compaction.Compact` (thresholds `CELLHIVE_COMPACTION_MIN_SEGMENTS`=64 / `_MIN_BYTES`=64MiB) + `replica` L1 prefix/manifest + `Restore` prioritizes L1 + cell-agent background loop (`CELLHIVE_COMPACTION_INTERVAL`=30s, Verify rechecks owner/epoch before write). **Added (ADR-055/056)**: sparse file on-demand paging (`replica.PageFetcher` + `L1/index.bin` ranged-get), old L0/L1 object GC. Note: **cell-agent runtime-side paged VFS is implemented (ADR-160, `internal/pagedvfs`, enabled by default; chains ≥ `CELLHIVE_PAGED_MIN_BYTES` use paging, small chains are fully cloned)**; cold restore for KV/D1/Queue/Workflow/Vectorize cells loads by page; do-supervisor and `restoreverify` also use the same paging primitives. **E2E**: dual cell-agent + `COMPACTION_INTERVAL=1s`, after 5000 sqlbench transactions, 159 L0 segments → 1 L1 object + manifest (min=32/max=5000), restore fails closed on txid continuity |
| DO capabilities (facets / preventEviction, pinned 2026-06-15) | ✅ Tested: `ctx.facets` **exists** (host-actor-managed multi-facet is feasible; `class` must come from `workerLoader.getDurableObjectClass()`, bare classes/plain namespace bindings throw TypeError); capnp `preventEviction = true` **can be parsed and started** (resident vs evictable variants feasible) | See **ADR-053**; implementation is **P3** (roadmap). |
| DO write coalescing / reducing fsync (stock workerd) | ✅ Tested (workerd 2026-06-15): **all `sql.exec` writes within one DO event (request) are automatically coalesced by workerd into one atomic commit** (exact error text: "Durable Objects' automatic atomic write coalescing"). Therefore `req/s` is fixed at ~900–970, while `writes/s` scales linearly with "writes per request k": k=1→928, k=4→3.9k, k=16→14.4k, k=64→52k, k=256→**196k** writes/s. `sql.exec("BEGIN")` is **rejected** (requires using `state.storage.transaction()` / `transactionSync()` instead); multi-statement `exec("a;b;c")` works but does not change the number of commits | The only stock way to reduce fsync = **increase writes per request** (batching), without modifying workerd (ADR-001); `BEGIN` is unavailable → use `transactionSync` for atomicity. **Verified**: with output gate, one gate covers the entire batch, k=256 reaches **110k writes/s** (vs 531 for 1 write/request, ~207×), and recovered `t.n` exactly matches. Platform-side wrapper `workerd/wrapper/groupcommit.js` implemented (1 op/request → 3,637 req/s, ~6.9×, avg_batch≈23)|

### Implemented (P0 code, historical checklist)

`internal/`: `config`, `cell`, `bucket` (FS + diagnostics + counting + **S3Bucket**), `owner`, `lease`, `server`, `cellstore` (SQLite), `wal`, `ltx`, `replica`, `peer` (spool/transport), `nodelog`, `recovery`, `upload`. `cmd/`: `cell-agent`, `cellhive`, `cellbench`, `realbench`, `s3init`, `s3probe`, `walscan`. See [`p0-report.md`](../archive/p0-report.md) and [`p0-tasks.md`](../archive/p0-tasks.md).

### Documents to implement

None (`docs/` current documents have been created, including `tracing.md`; details for `routing`/`security`, etc. will be added along with later implementation).

### Confirmed recommendations (user interaction confirmed, 2026-09-14)

- **ADR-030**: bundle/assets use **short-lived scoped credentials to access object storage directly** (not proxied through cell-agent).
- **ADR-031**: route projection is **pure pull + 5–10s TTL (no push)**.
- **ADR-028**: port split: `:7001` internal REST (shared by Go↔Go and JS↔cell; `:7000` gRPC plane has been removed, ADR-136) / `:8082` admin.
- **ADR-035**: rate limiting/quota/admission **implemented in P1**, conservative defaults + configurable.
- **ADR-036**: admin console **does not distinguish admin and tenant**, tenant identity is passed as a request parameter.
- **ADR-032 (A)**: DO WebSocket = **CF-compatible** (deployment/migration both restart DO, disconnect, client reconnects, handler restarts; no resume).
- **ADR-029 (B)**: binding auth = **network isolation + scope declaration** (scoped token deferred to P1).
- **ADR-037 (C/D/E)**: **full node-log/recovery implementation**; **DO claim driven by cell-agent**; cold activation/takeover uses **paged lazy loading**.

### Review scope note

This checklist comes from **one round of systematic review**. The design still rests on several **unfalsified assumptions** (observability of stock workerd DO/WAL, correctness of the self-developed cell replication protocol); new gaps may still appear during implementation. **"Finalized" = design decisions have been recorded; it does not mean validation passed or implementation is complete.**

---

## Appendix: documentation plan (all created, see `docs/`)

| Document | Coverage | Priority |
|---|---|---|
| `routing.md` | Route projection, version resolution, custom domains | P1 |
| `security.md` | Tenant isolation, binding auth, network policy | P1 |
| `control-plane.md` | CLI resource management, deployment pipeline, identity model | P1 |
| `bindings.md` | Cell mapping for each CF binding | P1 |
| `timers-and-dispatch.md` | timer abstraction, deduplication, cron/queue/workflow | P2 |
| `scaling-and-ha.md` | autoscaler, handoff, balancing | P2 |
| `deployment.md` | K8s/Compose/systemd examples | P2 |
| `compatibility-matrix.md` | CF/Wrangler compatibility matrix | P2 |
| `testing.md` | testing strategy, fault injection | P2 |
| `glossary.md` | glossary | P3 |

## P1 Replication Gap Checklist (LTX / snapshots / compaction / paging)

Inventory against roadmap P1 “complete cell replication” and the current status of ADR-043~054:

| Gap | Status |
|---|---|
| LTX segment format (WAL1/WAL2, batch, link) | ✅ Available (ADR-043~045) |
| Snapshots (KindSnapshot full-page image) | ✅ Available (ADR-049) |
| **Snapshot pagination** (split large databases into segments; snapshots possible when > maxSegmentBytes) | ✅ ADR-054 (`ltx.EncodeSnapshotParts`) |
| **compaction folding kernel** (chain → single snapshot) | ✅ ADR-054 (`restore.Compact`) |
| **compaction orchestration** (L1 prefix + manifest + background trigger + takeover-prefers-reading-L1) | ✅ ADR-054 (`internal/compaction` + cell-agent loop) |
| txid continuity fail-closed (prevent “silently stale” restores) | ✅ ADR-054 |
| **Sparse-file on-demand paging** (cold start without downloading the whole database) | ✅ **Primitives delivered** (ADR-055/056; `internal/replica TestColdHydrateIsWholeObjectNotPaged` shows in practice that cell-agent hydrate is a whole-object download + 0 ranged reads): `restore.IndexChain`/`SparseFile`, `replica.PageFetcher` + `L1/index.bin` ranged-get, `restoreverify -bucket`; **GC ✅** (ADR-056). **Consumers**: do-supervisor (DO files), `restoreverify`, and **cell-agent cell cold restore (ADR-160, `CELLHIVE_PAGED_RESTORE` on by default; chains ≥ `CELLHIVE_PAGED_MIN_BYTES` (default 256MiB) use fault-in VFS for page-by-page loading, small chains are fully cloned, background fill completes at `CELLHIVE_PAGED_HYDRATE_MBPS`)**; steady-state reads local |
| L0/L1 old-object GC | ✅ ADR-056 (`compaction.Options.GC`: delete old L1 superseded by manifest + folded L0; on by default in cell-agent). After E2E folding, `ltx/e1/` only has L1 left (L0=0) |
| drain token + graceful handoff (§7/M-07) | ✅ ADR-057 (`internal/drain`; SIGTERM→drain→seal→release owners→release token; draining rejects new claims) |
| node-log/recovery completion (§6/C-03) | ✅ **Complete**: ADR-057 (`RecoverNode`, sealed fast path, `cmd/recoververify`) + **ADR-066 automatic orchestration** (waker leader-only calls `recovery.Runner.Pass` once per pass: `nodelog.Nodes` enumeration, lease liveness check, `RecoverNode` for dead nodes; idempotent, fail-safe). Tests: `internal/recovery/runner_test.go`, `internal/waker`, `internal/nodelog` |
| Mixed-granularity segment deduplication (recovery batch vs single segment) | ✅ ADR-057 (`ltx.Header.ID` + Restore/compaction/PageFetcher deduplication) |
| Unified timer + local dispatch (timers-and-dispatch) | ✅ ADR-058 (`internal/timer`: cell SQLite timers/fired, token deduplication, at-least-once runner, `/v1/internal/timer/upsert`); E2E exactly-once + dedup |
| Single fleet waker (dead-owner fallback) | ✅ ADR-058 (`internal/waker`: `fleet/waker.json` election + TTL; leader only dispatches scopes whose owner has expired); E2E single leader, takeover within TTL after kill |
| Owner resolution library + non-owner forwarding (ADR-003) | ✅ ADR-059: `internal/ownerclient` (resolve+TTL cache+forward+409 retry); server-side non-owner forwarding (commit/commit_binary/append) + loop protection fail-closed; `requireOwnerEpoch` adds “local node must be owner”. E2E submit to non-owner → forward → fleet (failures=0) |
| Control-plane skeleton (apps/versions/routes/secrets) | ✅ ADR-060: `internal/control` (control cell sharded by app, immutable versions, transactional deploy/pointer switch, envelope-encrypted secrets, audit, route projection ETag) |
| Separate admin listener + data-plane isolation | ✅ ADR-060: admin `:8082` (`CELLHIVE_ADMIN_TOKEN`) is the operator entry point; control writes are also exposed on the **internal listener** (internal token) so non-owner nodes can forward to the owner (ADR-118) |
| Routing projection (pull-only) | ✅ ADR-060: `GET /v1/control/routes` + stable ETag + `if-none-match` 304 (ADR-031) |
| security: scope declaration validation / per-load scoped token | ✅ **ADR-061**: `internal/scopedtoken` (HMAC scope token) + `control.HasBinding` + `scopeAuth` enforcing KV endpoints; `RequireScope` defaults to true. Other bindings use the same middleware when implemented |
| bundle/assets content addressing + read path | ✅ ADR-062 (`internal/artifacts`: `bundles/sha256/<aa>/<sha>` deduplication, `assets/<ns>/<worker>/<token>/<path>`, internal presign/streaming read; CLI `bundle/asset put`, `deploy --bundle`) |
| Edge config distribution | ❌ **Removed (ADR-132)**: the platform does not provide external proxy functionality; edge is statically configured by operations |
| Audit retention/format | ✅ ADR-062 (`AuditQuery` limit/since, `PruneAudit`, hourly background prune, `CELLHIVE_AUDIT_RETENTION` default 720h) |
| OIDC/JWT admin auth | ✅ ADR-062 (`internal/auth`: StaticToken + JWTBearer(RS256 JWKS/HS256) + Any fallback; `CELLHIVE_OIDC_*`) |
| D1 binding (SQL API) | ✅ ADR-063 (`internal/d1`: query/exec/batch single transaction, `?` parameters, `/v1/d1/*` + scope kind=d1) |
| R2 binding (object API) | ✅ ADR-063 (`internal/r2`: `r2/<ns>/<bucket>/<key>`, PUT/GET(+Range)/DELETE/list; scope kind=r2) |
| Queue binding (producer + consumer) | ✅ ADR-063 (`internal/queue`: send+delay+idempotency, claim/lease, ack, retry); ✅ **consumer dispatch ADR-067** (`Version.Consumers` + `Projection.QueueTargets()` → `queue.Runner` Claim → POST `/v1/queues/dispatch` → **user-runtime workerd loads bundle and calls `queue()`** → Ack/Retry; `internal/userruntime` passes real workerd e2e) |
| KV delete/list | ✅ ADR-063 (`/v1/kv/delete`, `/v1/kv/list` bounded cursor) |
| binding facade (host adapter) | ✅ ADR-063 (`workerd/platform/facades.js`); key point: loader env cannot contain functions → platform injects wrapper module (source string + text bindings), facade constructs inside the loaded worker |
| user-runtime public entry point (routing/version/loading) | ✅ ADR-068 (`workerd/user-runtime/loader.js`: projection pull TTL 5s, Host routing, active version bundle loading, facade env, strip `x-cellhive-*`; real workerd e2e) |
| user-runtime internal dispatch (queue/scheduled) | ✅ ADR-067 (`internal.js` + wrapper `CellHiveHost` RPC; real workerd e2e) |
| Static asset serving (loader side) | ✅ ADR-069 + **ADR-071** (`_redirects`/`_headers`/ETag/304/index/fallback worker + `not_found_handling`(SPA/404-page) + `run_worker_first`(bool/paths); real workerd e2e 4 cases) |
| Cron → `scheduled()` dispatch | ✅ ADR-070 (`Version.Crons` + `Projection.CronTargets()` + cell-agent `cronEnricher` + user-runtime `/v1/timers/dispatch` → `handleScheduled`; real workerd e2e); parsing/evaluation/scheduling are implemented in both validation and runtime (`internal/cron` `Parse`/`Matches` + `Scheduler.Pass`, deploy-time `ValidateCrons` → `invalid_cron`; CLI `triggers list`) |
| Workflows/Cron binding | ✅ **ADR-086** (Workflows: `__workflow__` cell + `env.WF` + `step.do/sleep` + lifecycle/events) + **ADR-070/076** (Cron: `Version.Crons` projection + scheduler materializes slots + `scheduled()` dispatch) |

## Observability (after ADR-146)

- **tracing**: `traceparent` propagation has been implemented (entry/wrapper/facades/cell-agent service+DO), but there is **no OTLP export and no sampling**; props-bound facades (KV/D1/R2/Queue, loader isolate) cannot access per-request context, so their calls do not carry tenant traces; background queue/timer dispatch starts a new trace by default.

## Deployment Artifacts (after ADR-139)

- **Terraform not done** (orchestration is provided by Helm chart + kustomize base/overlay; `make ci` performs offline validation).
- **Cloud KMS not integrated** (root key can be env or file, ADR-150); **mTLS/internal CA intentionally not implemented** (independently derived secrets + private-network isolation are the main defense lines; trigger conditions are in security.md).

## purge / uploads / WAL / r2.List (updates after ADR-134)

- **Delete/purge**: soft delete + `purges` jobs + `RunPurgeLoop` have landed (ADR-131), and purge only applies to entities that are **still in soft-deleted state**; deploy/create cancels the job (ADR-135); **data-side hook is wired (ADR-142)**: worker deletion removes that worker’s DO segments and assets, app deletion removes the entire namespace’s `cells/`+`assets/`, every cell-agent executes it (cross-node local replica drain + bucket deletion are idempotent), and the hook is resumable (bounded deletes per pass, `done=false` keeps it pending). **Completed (ADR-169)**: data side switched to `objectstore.Objects.ListPage` + `bucket.PagedLister` cursor-paginated deletion (S3 `StartAfter`; bounded memory); `Purger.delete` advances while deleting, and the next pass resumes when the budget is exhausted. **Remaining**: each pass rescans from the target prefix start (no cross-pass persisted cursor; cold path); FS dev still walks the tree per page (bounded memory).
- **async bucket uploads**: there is already a **persistent retry queue** (ADR-143): pre-write spool (`<DATA_DIR>/upload-spool`) + replay at startup/periodically, failures counted as `deferred` (no longer lost; only counted as `dropped` if both local persistence and upload fail), `/metrics` exposes spool/deferred/dropped. **Completed (ADR-171)**: spool switched to atomic writes (tmp fsync → rename → directory fsync, memoized directory chain), **power-loss safe**; `Remove` does not fsync (replay is idempotent, safe).
- **S3 conditional delete**: `ConditionalDelete` relies on `If-Match` on `DeleteObject`; compatible storage that ignores this header degrades to unconditional delete (owner/lease fencing depends on it). **Self-check method**: `cellhive diagnose` now includes this probe (stale etag must be rejected + correct etag must delete + object must disappear); the failure message explicitly names "conditional delete (reject-stale)"; FS/local buckets are covered by unit tests, while cloud is a C-class environment pending real-world validation.
- **`r2.List`**: cursor pagination + sizing implemented (ADR-145): `bucket.PagedLister` (S3 `StartAfter`+`MaxKeys`, FS bounded selection), `/v1/r2/list` returns `truncated`/`cursor`, facade passes through. **Completed (ADR-168)**: `include=httpMetadata,customMetadata` (only reads per-object sidecar when explicitly requested) and `delimiter`/`delimitedPrefixes` (scan limit 10000; beyond that returns `truncated`+cursor). **Remaining**: FS backend still reads object body to compute etag (dev backend); object versioning/SSE-C/conditional put are still not implemented.

## Deletion/Cleanup (ADR-131/142, implemented)

Deletion is **two-phase**: the request path only performs control-plane soft delete (revocation + audit); data cleanup is completed by an idempotent, rerunnable, paginated purge loop:

1. `DELETE /v1/control/worker|app` soft-deletes in the same transaction and writes `purges(ns, worker, state, requested_ms, attempts, last_error)` via `enqueuePurgeTx`; worker/app immediately becomes unroutable.
2. cell-agent’s `RunPurgeLoop` (default 5s tick, up to 20 `PendingPurges` per pass) calls `Purger.Run(ns, worker)`: `MaxDeletes` (default 500) cleans bucket prefixes `cells/<ns>/`, `assets/<ns>/` in a **bounded** way; `worker==""` (app-level) cleans **all resources + DO storage**, otherwise it only cleans that worker’s **DO storage (`cells/<ns>/__do__/…`) and its assets** (KV/D1/R2/Queue resources survive across workers, same as CF). If not fully cleaned, it returns `done=false` and the job remains for the next tick; failures call `FailPurge` to record attempts/last_error and then retry; completion calls `FinishPurge` to delete the job + audit.
3. Redeploy cancels pending purge **in the same transaction**, and purge skips workers/apps already `resurrected` when it runs (`TestPurgeSkipsResurrectedWorker`/`App`).
4. Local replicas are handled by each owner (`LocalCells.DeleteNamespace`/`ForgetPrefix`).

Tests: `internal/purge TestAppPurgeDropsNamespaceData`/`TestWorkerPurgeDropsOnlyThatWorker`, `internal/control TestPurgeSkipsResurrectedWorker`/`App`/`TestRunPurgeLoopResumableHook`, `internal/server TestWorkerDeletePurgesAssets`.
---

## P5 Operations/Hardening (ADR-088)

- Protocol versioning (reader-before-writer), diagnostic enhancements, matrix regression guards, and load-testing/chaos tools are all in place (ADR-088; recovery chaos is covered in ADR-057/066). **Boundary**: real multi-host chaos and cloud-provider matrix regression require environments (Class C).
- **Object GC (ADR-110/111)**: `internal/objgc` two-phase marking + grace period, reclaiming **bundles** and **asset versions** not referenced by any version; admin `POST /v1/control/gc/{bundles,assets}` / CLI `cellhive gc bundles|assets`; cell-agent background loop (bundle+assets) is off by default.

## P4 Multi-tenancy/Scaling (ADR-087)

- Release logs/idempotent deployment/Admission/Autoscaler **signals** have been implemented and unit-tested (ADR-087).
- **Boundary**: the autoscaler only emits signals; actual node add/remove belongs to external orchestration (no orchestrator/cloud driver) → Class C environment validation; admin backend = admin API (no Web UI).

## P2 Workflows Binding (ADR-086, Partial)

**Implemented (real workerd e2e)**: `internal/workflow` (`__workflow__` cell: instances/steps/events, step memoization); facade `env.WF.create/get/sendEvent`; cell-agent API (tenant create/get/event, internal step/sleep/finish); user-runtime `/v1/workflows/run` + `cellhive-workflow.js` base + `step.do/sleep` (shim: rewrites `cloudflare:workers`, constructed by the platform, because workerd's `WorkflowEntrypoint` cannot be constructed outside the engine); sleep reuses the unified timer (`KindWorkflowSleep`); CLI `workflow create`; `wranglercompat` requires `class_name`.

**Partial boundaries (not done)**: cross-worker instances; `locationHint` is accepted but **ignored** (best-effort placement hint, no effect).
**Implemented** (was previously mislisted as not done): `pause`/`resume`/`terminate`/`restart` (`internal/server/workflow.go:126` + facade), `waitForEvent` (`handleWorkflowWait` + `env.WF.sendEvent`), instance listing (`handleWorkflowList` + `env.WF.list`), `instance.delete()` (`Store.Delete` + `/v1/workflow/delete` + facade), per-step `retries{limit,delay,backoff}` (`workerd/user-runtime/workflow-wrapper.js:74-152`, durable attempt).

## backend-A Capture and Cold Recovery (ADR-092)

KV/D1/Queue/Workflow previously wrote only to local cellstore and were **not captured**. They are now fully wired: `internal/cellcapture` + `commitSegmentCore`; `capturedWrite` covers **KV/D1/Queue/Workflow/timer/control**. Also completed:
- **Cold recovery**: `cellstore.Store.Hydrate` + `replica.LatestEpoch` — when a new owner opens a cell for the first time and has no local files, it uses `replica.Restore` + `restore.ApplyFile` from the bucket (previously only do-supervisor recovered; cell-agent could start on an empty disk and return `binding_not_registered`).
- **Owner renewal**: cell-agent renews `OwnedScopes` at TTL/3 (previously it did not renew; once the lease expired, ownership was lost, capture stopped, and writes returned 503).
- **D1 txid alignment**: `cellstore.Cell.Tx` makes D1 writes advance `cell_meta.txid` in the same transaction, just like KV (previously D1 did not advance it → `Wait` returned immediately → acked but not durable).

Tests: `internal/cellcapture`, `TestKVCaptureReplicatesAndRestores`, `TestKVCaptureConsistencyN`, `TestD1CaptureConsistency`, `TestLatestEpoch`, `TestHydrateOnFirstOpen`; live: kill owner → takeover reads back all acked keys.

## Benchmark Environment Limitations (Not Defects, Need to Know)

- The local **FS bucket uses a single global mutex** (`internal/bucket/fsbucket.go`) to serialize `Put/CAS/List`, and shares the same disk with SQLite → **multiple cells do not add up when capture is ON** (FS single ~5.4k, dual ~8k peak ≈1.47x; local MinIO dual-cell has no gain). Near-linear scaling is only possible in production with real distributed object storage + separate disks.
- The upload layer is already sharded by scope (`upload.NewSharded`, order-preserving), but sharding is not useful when sink parallelism is insufficient.
- The single-cell write ceiling is limited by "one commit proof round trip per write"; throughput is limited by the single writer and "one proof per write" (peak CPU only ~2/8 cores).

## P3 DO Durability and Cross-node Gap List (ADR-083/084)

| Item | Status |
|---|---|
| facet discovery (`.facets`) | ✅ Parsing has been tested (ADR-084): magic `57efb0c55bcecdc4` + `[flag][len][name]`; entry i↔`<hosthash>.<i+1>.sqlite` (verified by dual-facet probe). `ParseFacets`/`Supervisor.Facets()`/status/manifest all include it |
| Path-based WAL capture (arbitrary SQLite path) | ✅ `internal/wal` cursor + `snapshot`/`delta` (ADR-083) |
| Output gate (RPO=0) | ✅ `POST /internal/do/gate`; failure/timeout returns `result_unknown` (ADR-083, real workerd) |
| Cross-node cold activation + on-demand paging | ✅ `RestoreAll` + `CompactAll` + `PageFetcher.Materialize`; dual-runtime e2e + `TestPagedRestoreUsesRangedReads` (ADR-084) |
| Process-level takeover recovery | ✅ `TestDoRuntimeTakeoverAfterCrash`: A crashes (`exec.CommandContext` → SIGKILL workerd) → lease expires → B restores from bucket and takes over (count 1→2) |
| WebSocket cross-node forwarding | ✅ `proxyConnect` (host.js proxies to owner + 1012 passthrough); `TestDoRuntimeWebSocketCrossNodeForward` (B→A, abort→`CLOSE:1012`) |
| `deleteAll()` | ✅ **shim (ADR-079 extension)**: stock workerd throws an internal error (`expected parent == kj::none`) when calling `deleteAll()` on a SQLite-backed facet; `cellhive-do.js` instead clears all KV + `DROP`s user SQL tables (preserving `_cf_*`/`sqlite_*`) + clears alarm. `TestDoRuntimeDeleteAllCaptured`, `TestDoRuntimeDeleteAllSQLCaptured` (including empty after cold-start recovery) |
| Runtime SQLite VFS lazy reads | ✅ **Implemented on the cell-agent side (ADR-160)**: `internal/pagedvfs` fault-in VFS (sparse cut + hydration bitmap + synchronous fault + IOERR fail-closed) + `replica.PageFetcher` page source; `CELLHIVE_PAGED_RESTORE` (default on)/`_MIN_BYTES` (default 256MiB, full clone below this)/`_HYDRATE_MBPS` (default 16, 0=keep sparse); force `HydrateAll` before `SnapshotPages`; **b-tree-aware fault strategy** (internal-page child single-page fetch, sequential children prefetched into memory cache with `scanAhead`=64, contiguous children merged into runs; windowing only for sequential access); **cache marker bit** `<cell>.db.paged` (marker present = sparse cache must use paged VFS; if cut mismatches/no page source, discard and rebuild; marker removed after full hydrate or background fill completes — fixes the bug where after restart/eviction a sparse file was opened with the base VFS and zero pages were read); **bulk hydrate** (`ReadRun`); **LZ4 frame-by-frame compression** (`WAL3` page-map v2 + `CID2` index; L1 3.1MB→140KB/real stack 315KB→86KB; decode 1.1GB/s); **child concurrent prefetch** (`CELLHIVE_PAGED_PREFETCH_WORKERS`=4); six `cellhive_paged_*` metrics in **`/metrics`**; **sparse-aware disk accounting** (`AllocBytes`/`DiskBytes`). `WAL2`/`CIDX` remain backward-compatible/readable; delta (`WAL1`) is not compressed (low payoff for many small per-record segments); LZ4 encoding is single-threaded at ~47MB/s (3MB snapshot ~70ms, cold path); **GC is independent of compression** (manifest key + txid watermark; mixed-format chains can be folded and reclaimed, real-stack L0 drops to zero), but `MinBytes` is now based on compressed bytes (closer to IO budget), while `EncodeSnapshotParts` sharding budget still uses the v1 estimate (smaller shards, safe). Measured: point reads 8/771 pages, full scan 336 pages = 6 window reads; real-stack cold request 123ms, `cells=1 faults=72 runs=9 prefetch_hits=3 hydrated=12/77`. ⛔ **DO-side boundary (ADR-085, pending FUSE)**: cannot be implemented in stock workerd (requires FUSE or modifying workerd); alternative = materialize by object/by page on cold start (`RestoreObject` + `PageFetcher.Materialize` range reads). **Revision (ADR-159/160)**: runtime paged VFS has been implemented on the cell-agent side (`internal/pagedvfs`, ADR-160); force `HydrateAll` before capture/snapshot to avoid silent zero-page corruption |
| DO internal bindings | ✅ **Fixed (ADR-089)**: facet `this.env` injects worker bindings (KV/D1/R2/Queue/Workflow/DO); `TestDoRuntimeBindingInsideDO`. Boundary: through `this.env` (constructor parameter `env` is host env) |
| DO compatibility suite | ✅ `TestDOCompatSuite` unified entry point, 16 real workerd subtests (sync SQL/transactions, alarm, WS 1012, migrations, register/delete, forwarding/`result_unknown`, gate, cold activation, takeover, resident/evictable) |
| Recovery without bucket credentials | ✅ `HTTPStore` via cell-agent `/v1/internal/segments`+`/segment`(Range 206)+`/blob`; `TestRestoreViaAgentNoBucketCreds`, `TestAgentPagedRestoreUsesRangedReads`, `TestInternalBlobRoundTrip`, `TestReadSegmentRange` |
| gate hot-path blob writes | ✅ **Optimized**: manifest/sidecar **content-hash deduplication** (`blobUnchanged`), zero bucket PUTs in steady state (`TestSyncAllSkipsUnchangedBlobs`); `refreshFacets` changed to **incremental cache** (directory mtime detects additions + `.facets` (mtime,size) detects content changes; steady state only a few stat calls, no longer walks the full directory on every request; `TestRefreshFacetsPicksUpNewFacet`) |
| HTTPCommitter proof too narrow | ✅ **Fixed**: commit proof accepts `fleet`/`bucket`/`bucket-batch` (RPO=0), rejects `bucket-async` (RPO>0) and unknown; `TestHTTPCommitterProofModes`. Pure bucket deployments no longer fail the gate |
| **Object key `storage_id/<class>/<objectName>`** | ✅ **Resolved (ADR-084, spike reversed)**: hosthash **can be determined from inside the DO** — `this.ctx.id.toString()` = `<hosthash>`, `this.ctx.id.name` = hostId (contains storage_id). host.js dual reports (router: host_id→storage_id/class; actor: host_id→host_hash) → supervisor `/internal/do/bind`; facet files are copied by object scope `workerd/<class>/<base64url(storage_id/class/objectName)>`. Verified by `TestObjectScopesAndRestoreObject` + `TestDoRuntimePerObjectColdStart` |
| Per-object-granularity recovery | ✅ `Supervisor.RestoreObject` (facet file + host actor + `.facets`, paged reads); `TestDoRuntimePerObjectColdStart` only restores that object → state continues |

| Vectorize's two query modes (ADR-159) | ✅ **Default flat exact** (20k×256/K=10 ≈ 6.3ms, ~8× faster than old Go KNN); `vectorize rebuild` builds **vec1 IVFADC/OPQ** ANN, then ≈ 0.22ms (≈210×), but ANN is approximate (requires representative training data and tuning `nprobe`/`codesize`; official 1M×128d recall@10 ≈ 0.92). Training is an explicit operator action; OPQ training is slower (a few seconds locally for 20k, nthread≤16) |
| Vectorize dot-product and filtering semantics (ADR-159) | ⚠️ **Boundary**: `dot-product` metric is unsupported (vec1 only has l2/cos; creation returns `vectorize_bad_config`); namespace is pushed down into the index, while other metadata filters are **post-filters** (4× oversampling; extreme filters may return fewer than topK; CF filters first); vec1 has no partition key (namespace borrows a metadata column) |
| vec1 v0.7 rowid UPDATE segfault (ADR-159) | ✅ **Mitigated**: rowid-targeted `UPDATE` on `vec` crashes v0.7 (found by store unit test), so upsert = `DELETE` + `INSERT` (same transaction, semantically equivalent); can be reevaluated after upgrading vec1 |
| Vectorize CF differences (ADR-158) | ⚠️ **Superset/boundary**: metadata index is **not enforced** (CF requires creating a metadata index before filtering; we allow direct filtering over full metadata and do not simulate the 64B truncation for indexed strings); `describe`/`insert` return `{mutationId,count,ids}` (CF only guarantees `mutationId`); `--deprecated-v1` has no V1 semantics (handled as V2); batch limit 1000; `listVectors` is an extension (CF only via wrangler) |
| L1 snapshot sharding budget is conservative (ADR-160) | ✅ **Intentionally accepted, not doing**: `EncodeSnapshotParts` estimates with v1 "4KiB per page" → shards are smaller (safe). Note that **sharding itself is CellHive's HTTP `maxSegmentBytes` transport constraint** (LTX is "one file per object, no sharding concept"; object count is controlled by compaction level); exact sharding by actual compressed-frame bytes only saves a few objects for "large and highly compressible" snapshots, with small benefit and not worth the added complexity |
| Tenant log retention (ADR-136/172) | ⚠️ **Boundary**: in-memory ring is non-durable, single-node, and drops oldest entries under load; OTLP logs export is off by default (`off`), and `tail` mode exports only during the TTL subscription of `cellhive tail`; subscriptions are **broadcast to other live nodes** via the lease list (ADR-173, best-effort + throttled to ≤25s per worker, disconnected nodes catch up on next renewal); export is best-effort (dropped when queue is full/backend unavailable); log lines have no `request_id` |
| Tenant console logs (ADR-185) | ⚠️ **Unsupported for now**: tested pinned stock `workerd 1.20260615.1` rejects a service designator in `tails: [{name:"tenant-tail"}]` for a dynamic `workerLoader` Worker: `TypeError: Incorrect type for array element 0: the provided value is not of type 'Fetcher'.` The old `log-tail.js`/`PlatformBridge.logSend` bridge is removed; tenant requests and DO facets still execute, but `console.*` does not enter the platform ring or OTLP. Do not restore this path with a tenant-env token, URL, or transport; rerun the real dynamic-tail spike after upgrading the pin. |
| OTLP tracing coverage (ADR-167) | ⚠️ **Boundary**: **DO-internal `this.env.<binding>` (props-bound) calls do not carry the DO traceparent and become their own trace** (workerd has three isolates with separate globalThis + CF binding APIs have no per-request parameters; `TestDoRuntimeBindingInsideDO` asserts current behavior is empty); **`peer.append` is an independent trace per capture-pipeline batch** (group commit merges multiple requests, cannot attribute to a single trace); tenant isolate `facades.js` fallback path has no JS span (server span can still attach); DO `/v1/do/connect`·abort and compaction/upload background loops have no span; JS span has millisecond precision; entry headers control sampling; no-op when endpoint is empty |
| peer replication redundancy (ADR-164) | ⚠️ **Default semantics**: under adaptive hedge, the steady state is **owner local + 1 follower** (the 2nd copy is added only when slow); `CELLHIVE_PEER_HEDGE_MS=0` is always single-copy. To "always have 2 follower copies", you must explicitly accept steady-state double-send (that mode is not currently provided). A single-node failure is recoverable (owner dies → recover from follower spool `/v1/peer/held`; follower dies → owner local segments are uploaded in the background). Data is lost only if both the owner and follower fail. Consistency is unaffected (single writer/order/epoch/quorum-1 unchanged) |
| R2 object fields and metadata (ADR-163) | ⚠️ **Boundary**: `R2ObjectBody` data fields returned by `get()` and `text/json/arrayBuffer/blob` are all available (workerd RPC serializes data by value and serializes functions as callable stubs; methods are designed to be enumerable to cross RPC, while JSON.stringify skips functions); `bodyUsed` **does not flip** after reading (snapshot value); `list()` returns only `key/size/etag/httpEtag/version` by default and **does not backfill** http/custom metadata/uploaded/checksums per object; only explicit `list({include:["httpMetadata","customMetadata"]})` reads sidecars per object on demand (ADR-168); `get()` incurs one additional metadata sidecar read; metadata from `put` is stored in the `r2meta/<ns>/<bucket>/<key>` sidecar (registered reserved prefix); deleting an object also deletes the sidecar; `md5/sha256` are verified when provided |
| R2 `writeHttpMetadata` (ADR-174→ADR-175 fixed) | ✅ **Resolved**: `loader.js`/`buildFacetEnv` passes the R2 binding names via the module-scope const `__cellhivePlatform.r2Bindings` (no env key); `queue-wrapper`/`bindings-wrapper` wrap each R2 binding with a local Proxy (`facades.js wrapR2Metadata`); `writeHttpMetadata` mutates the caller’s `Headers` inside the tenant isolate, and content-type works correctly; only `get()` is proxied, all other methods pass through |
| Large KV values (ADR-176 record) | ⚠️ **Gap**: values ≤25MiB are currently **fully inlined** in the cell’s SQLite and are captured/replicated once with LTX (the aligned approach is to move >1MiB to fleet bucket blob `v2:e<epoch>:<digest>` and run GC in the same alarm; measurements show 1MiB is the latency crossover point). The cost of large-value `put` grows linearly with value size; correctness is unaffected. Alignment requires the full chain of "small values inline / large values blob + epoch reference + GC" |
| wake index lagging behind timer (ADR-177 fixed) | ✅ **Resolved**: `Store.Upsert` publishes the index before commit (the index does not lag; failures fail closed); `SyncIndex` rebuilds during `registerTimerScope`/claim; `CELLHIVE_WAKE_REPAIR_INTERVAL` performs bounded round-robin repair for local cells (read-only scan; sparse paged unmounted cells are skipped). Remaining: repair only covers local cells on this node (dead-node disks are unreachable; rebuilt when the new owner claims) |
| WebSocket upgrade on DO stub (ADR-174→ADR-175 fixed) | ✅ **Resolved (no tenant transport needed since ADR-184)**: the tenant facade stamps `DO_ID_HEADER` on the request → the platform-side `DurableObjectNamespace.fetch(request)` (fetch-shaped RPC, so 101/WebSocket crosses RPC) → cell-agent `GET /v1/do/connect` (scoped) to get `{owner,ticket}` → owner do-runtime `/v1/do/connect` (ticket is only for this shard); `TestTenantDoWebSocketCrossesRpc` covers it end to end and the `wsecho` example WS echo passes. `CH_DO_CONNECT`/`CELLHIVE_CAP_WS` are gone. Alarm identity (`storage_id/storage_class`) and do-runtime address scheme were fixed in the same batch |
| DO RPC values and limits (ADR-162) | ⚠️ **Boundary**: supports `undefined`/`-0`/NaN/±Inf/bigint/Date/RegExp/Map/Set/ArrayBuffer/TypedArray/DataView/Error(including cause)/URL/URLSearchParams + shared references/cycles; **does not support** function/Symbol/Promise/weak collections/streams/`RpcTarget`/DO stub; class instances degrade to plain objects; args and results are each ≤ **8 MiB** (after tagged encoding; binary base64 is about ×4/3); method names must be identifiers and must not be `fetch`/`alarm`/`constructor`/`__proto__`/`then`/`toString`, etc., or `__ch*`; no cross-process traceparent (requestId only) |
| SQLite is now CGo + build tags (ADR-159) | ⚠️ **Must know**: `make build/test` (or `-tags "sqlite_fts5 sqlite_dbstat sqlite_math_functions sqlite_column_metadata sqlite_preupdate_hook"`) is mandatory—omitting it triggers the `#error` guard in `internal/cellstore`; binaries include AVX2 code (`-march=x86-64-v3`); for non-AVX2 CPUs use `-tags cellhive_vec1_portable` (scalar, slower); arm64 uses NEON |
| Auth level for the Vectorize binding surface (ADR-158) | ✅ **Aligned**: `/v1/vectorize/*` binding endpoints, like KV/D1/R2/Queue, **require only a scoped token** (no internal token); writes use `forwardOrClaim`, reads use `forwardRead`. Container smoke previously found an erroneous `s.auth` wrapper causing 401; fixed + regression tested |
| Vectorize unavailable in dev (ADR-158) | ⚠️ **Boundary**: Miniflare only parses `vectorize` bindings (workerd has no such service); `cellhive dev` exits with an explicit error when it sees one; for local debugging, use `cellhive vectorize` against a real namespace |
| R2 stats are diagnostic approximations (ADR-157) | ⚠️ **Intentional**: R2 objects have no local index, so `/v1/r2/stats` uses a **bounded List** (`limit` defaults to 1000/max 10000); large buckets return `truncated`+`cursor` rather than exact totals; it counts toward `cellhive_list_calls_total` (ops path, allowed but should be deliberate) |
| Row counts can only be estimates (ADR-157) | ⚠️ **No SQLite row-count metadata**: by default, dbstat leaf-page `ncell` estimates are returned (KV `rows_estimate`, D1 per table, workflow `instances_estimate`); exact counts require `?exact=1` (`count(*)`; KV uses the smallest index/index-only, still O(rows)). No "always exact and always cheap" option is provided (the `cell_meta` counter approach was not adopted) |
| Preconditions for Workflow `by_status`/DO stats (ADR-157) | ⚠️ **Boundary**: workflow status breakdown is returned only with `?exact=1` (no `instances(status)` index, to avoid write amplification); DO stats are available only when the object index is enabled (`CELLHIVE_DO_OBJECT_INDEX`, ADR-109), otherwise `available:false` |
| `resource delete` semantic boundary (ADR-156) | ⚠️ **Intentional**: it only revokes the **registration** (tombstone fail-closed) and **does not delete** data cells (KV entries/queue messages/DO SQLite remain)—vwork "shutdown" is not "destruction"; full reclamation requires deleting the corresponding cell (no ops endpoint currently). `replay-dlq` is **at-least-once** (if main-queue enqueue succeeds but DLQ ack fails, replay can be duplicated), and requires the queue to have been registered by a producer binding and declared `dead_letter_queue` |
| `do-runtime` non-gated mode does not drain on SIGTERM (ADR-156) | ⚠️ **Boundary**: only `do-runtime-gated` (do-supervisor, ADR-140) takes over drain; bare `do-runtime` relies on readiness removal + lease expiry + takeover recovery (`TestDoRuntimeTakeoverAfterCrash`). Use gated in production |
| Coverage of entry `/ready` (ADR-156) | ⚠️ **Boundary**: it describes readiness of the **entry/DO host** (projection freshness, drain state), not tenant worker internal health; props-bound facades (KV/D1/R2/Queue in the loader isolate) have no independent probes |

**Meaning (updated 2026-09-15)**: P3.7/P3.8 deliverables (facet discovery, object keys, per-object recovery, gates, cross-node cold activation/takeover, compat suite) **have all been implemented and verified**. All P3 items have been implemented/finalized (WS cross-node forwarding, `transferred_classes` within the same worker, runtime VFS lazy reads per ADR-160, `refreshFacets` incremental cache); only **Class C environment validation** remains (real cross-host RTT, cloud object storage, multi-host chaos/takeover) and cross-worker `transferred_classes` (intentionally rejected).
