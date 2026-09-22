# Cell Protocol

This document defines the core protocol of the CellHive state layer: identity and addressing, single-writer lease, epoch fencing, replication and durability proofs, discovery mechanism, and bucket constraints. The implementation language is Go, and the replication implementation follows LiteFS/`superfly/ltx`.

## 1. What is a cell

**cell = a named state unit with its own independent SQLite database.** It is the smallest unit of consistency, failure, and migration, equivalent to a Durable Object.

```
scope = <namespace>/<class>/<id>
```

| class example | Hosts |
|---|---|
| `__kv__` | A KV namespace |
| `__d1__` | A D1 database |
| `__queue__` | A queue |
| `__workflow__` | A Workflow definition instance set |
| `__cron__` | cron projection (timers) |
| `<DOClass>` | User Durable Object class |

**Persistent addressing**: `(class, id)` remains stable; at runtime the owner is resolved by scope. When an object is not active, it exists only in object storage, with near-zero cost.

## 2. Two storage backends, one protocol

There is only one cell protocol (lease / epoch / replication / RPO / addressing / discovery / diagnostics), but there are two SQLite owners:

| Backend | Working SQLite owner | Used for | Capture method | owner node |
|---|---|---|---|---|
| **A** | `cell-agent` (Go) | KV / D1 / Queue / Workflows / Cron | Controls transactions and WAL directly, with precise capture | `cell-agent` node |
| **B** | workerd (`do-runtime`) | Durable Object | The supervisor of do-runtime observes the WAL read-only | `do-runtime` node |

Unified rule: **owner = the node that holds the "working SQLite" for this cell.** For backend B, the owner record is coordinated and written by `cell-agent` (pointing to the do-runtime address + epoch). The differences and risks of backend B are described in [`durable-objects.md`](durable-objects.md).

### A/B protocol consistency and differences

**Consistent (the same cell protocol)**: scope/identity, conditional writes for owner records, epoch fence, LTX replication and `e<epoch>` prefix, durability proofs and output gate (RPO=0), epoch chain/snapshot recovery, takeover/recovery, hard bucket requirements, no LIST, timer abstraction.

**Differences (backend B)**:

| Dimension | A (ordinary cell) | B (DO) |
|---|---|---|
| Working SQLite owner | `cell-agent` | workerd (local to do-runtime) |
| Capture method | Controls transactions/WAL directly (hookable, checkpoint tunable) | supervisor externally observes WAL read-only |
| owner node | `cell-agent` node | `do-runtime` node (owner record includes `role`) |
| owner record writer | cell-agent itself | cell-agent writes on behalf of do-runtime |
| LTX uploader | cell-agent uploads directly | do-runtime captures → reports to cell-agent → cell-agent uploads |
| write ack path | Internal to cell-agent | One extra hop (capture→cell-agent, possibly cross-node) |
| Storage layout | One SQLite per cell | workerd actor storage with multiple files (shared metadata + per actor) |
| Snapshot | Precisely controllable | Constrained by workerd checkpoint behavior |
| FUSE option | Not applicable | Optional (self-managed environments) |

## 3. owner record and single writer

Each cell has an owner record in object storage:

```
cells/<scope>/owner.json
{ "node": "<owner-node-id>", "role": "cell-agent|do-runtime",
  "session": "<session-id>", "epoch": 42,
  "expiry": 1730000000000, "address": "10.0.0.12:8788" }
```

**Acquisition**: conditional write.

- No record → **conditional create** (`If-None-Match: *`);
- Existing record → **CAS** (`If-Match: <etag>`).

Object storage accepts only one of the writes, so two nodes cannot own the same cell at the same time.

**Writer**: backend A is written by the owner `cell-agent` itself; backend B is written by `cell-agent` on behalf of it (do-runtime does not hold bucket credentials, and passes its advertise address to cell-agent when claiming).

**Renewal**: the owner record of a cell is **not renewed on the hot path**; it is written only during activation/takeover/migration. Liveness is determined by the node lease / replication activity (see §4).

**Self-fence**: if a node cannot renew before its lease expires and cannot replicate, it stops all cells under its ownership, rejects unfinished requests, and exits (to be restarted by an external supervisor). The fence **does not write to storage**: the peer has already determined it is dead based on the lease record.

## 4. Node lease and liveness determination

### cell-agent cluster (fixed, self-registered)

```
nodes/<node>.json
{ "node": "...", "session": "...", "advertise": "10.0.0.12:7001",
  "expiry": ..., "format": ..., "load": { ... } }
```

- One record per node, renewed at TTL/3; the default TTL is 10s (configurable).
- The node lease is the **liveness proof**: if a node becomes unreachable → all cells under it can be taken over.
- `load` exposes scaling signals: `owned_cells`, `placement_weight`, `resident_cells`, `host_websockets`, `rss_bytes`, `cpu_percent_x100`, `pressured`, `memory_headroom`, `shed_cells`, `restoring`, `sampled_ms`.
- **fleet capacity sample**: each node reads a shared snapshot every 5s; one node refreshes it (by reading each lease), and the others read the result, avoiding O(cells).

### do-runtime (elastic, no self-registration, no heartbeat)

- do-runtime **does not hold bucket credentials**, so it cannot write `nodes/*` like cell-agent;
- Its liveness is **implicitly determined by replication activity**: as long as it continues reporting WAL / renewing owner to cell-agent, it is considered live; if reporting stops → the owner lease expires → it can be taken over;
- **Takeover candidates** do not rely on heartbeats; they use platform service discovery (K8s Endpoints / mesh); see §4.1.

### 4.1 Discovery mechanism

| Target | Mechanism |
|---|---|
| `cell-agent` cluster | `nodes/*` bucket lease (self-registration) |
| Class A cell owner | bucket owner record + local cache (**point lookup, no LIST**) |
| DO owner | owner record (points to do-runtime address + epoch), written by cell-agent |
| `user-runtime` dispatch | Logical service name (mesh), any healthy replica |
| do-runtime liveness | **Implicit liveness via replication activity** (no dedicated heartbeat) |
| do-runtime takeover candidates | Platform service discovery; probe on demand (after selection, ask it to claim) |

**How callers (workerd / supervisor) find cell-agent**: use a **logical service name** (mesh / K8s Service), any replica is acceptable; a non-owner replica resolves the owner and **forwards the request to the owner**, so the caller does not need to know the specific machine.

**How peers (between cell-agents) find specific nodes**:

- Each node writes `nodes/<node>.json` (bucket) on startup/renewal, including the **advertise private address (or pod DNS), session, epoch, format, load**;
- When a peer needs to be found, read these leases (**point lookup**) + the shared fleet sample (every 5s, refreshed by one node, avoiding O(n) reads);
- Ensemble followers: the owner selects from **live leases** (excluding itself, at most 2);
- Takeover/forwarding: use the address in the owner record (DO points to do-runtime, class A points to cell-agent); if the address is invalid, first determine death by lease TTL, then reselect.

**Address contents and failure detection**: `advertise` is an internally reachable address; `session`/`epoch` distinguish instance generations; **`expiry` is always ≤ the node lease TTL** (default 10s), to ensure consistent semantics of "node lease expiration → owner record also becomes invalid"; renewal happens at TTL/3. The node lease is the primary liveness proof; the expiry time of the owner record is bound to it for fast-path checks.

**Across orchestrators**: the bucket node lease is **authoritative**; K8s headless Service / pod DNS provides only **transport reachability** and does not provide identity or epoch.

**Difference from do-runtime**: do-runtime does not hold bucket credentials and does not self-register, so its liveness relies on replication activity, and its candidates rely on platform service discovery (see §4 above).

**Non-K8s environments (including do→cell endpoint discovery)**:

- **No third-party registry by default**: use a **configurable service name (one or more)**, resolved by **K8s / Docker Compose built-in DNS**—the Compose `<service>` name returns A records for all replicas; K8s uses a Service (ClusterIP, or preferably **headless**, which returns pod IPs). After resolving all A records, the client (Go HTTP) performs **round-robin / failure retry**; for DO coordination, it is recommended to **select cell-agent by hashing scope**, ensuring that the WAL of the same DO is coordinated by the same cell-agent. Bare metal/VMs use a **seed list** or local DNS.
- **do-runtime → cell-agent (finding the cluster entrypoint)**: use the same service name / seed list described above; do-runtime **does not hold bucket credentials and cannot** self-discover through `nodes/*`. Consider **Consul/Nomad**, **read-only scoped credentials limited to `nodes/*`** for do-runtime, or **reverse-pushed** endpoints from cell-agent only when dynamic/health-aware discovery is required.
- **do-runtime takeover candidates**: prefer platform discovery (K8s/Consul/Nomad); without platform discovery, use **do-runtime registration/heartbeat to cell-agent**, or a configured list.
- Core peer discovery (`nodes/*` lease) and owner records are still bucket-based and **do not depend on K8s**. do→cell uses internal direct connections (HTTP) and **does not go through Traefik**.

## 5. epoch fence

- Each activation/takeover/migration: `epoch += 1`.
- Replication data is written to `cells/<scope>/ltx/e<epoch>/...`.
- Even if a node that lost ownership continues writing, its data only lands under a **discarded epoch prefix**; recovery selects only the current lineage.

That is: **epoch is the fence for "write validity"**. Combined with generation checks on the owner record, it makes writes from old owners harmless.

## 6. Replication and RPO=0

### Replication format

- SQLite committed transactions are captured as **LTX** (transaction log segments) and uploaded to the current epoch prefix (ordinary PUT).
- Backend A: `cell-agent` directly captures the SQLite it owns (**ADR-092**: KV/D1/Queue/Workflow/Vectorize/timer/control write paths go through `internal/cellcapture` into `sqlcapture` → LTX → fleet proof, bucket async; cold recovery goes through `cellstore.Hydrate` + `replica.LatestEpoch`; `/v1/internal/{append,commit}` is also available to external committers).
- Backend B: the supervisor of `do-runtime` captures WAL segments, **reports them to cell-agent**, and cell-agent performs replication and proof (see [`durable-objects.md`](durable-objects.md)).
- Snapshot: for large-data cells, restore a full-database snapshot (L9) first during takeover to avoid replaying the entire history; otherwise materialize the L0 chain.
- **compaction**: the background folds many L0 segments into L1 (triggered by `min_txids` / `min_mb`), so takeover reads only a small number of objects instead of thousands.
- **paging**: cells above a threshold use sparse files + on-demand paging, so cold start does not need to download the full database.
- **checkpoint alignment (I-03)**: the supervisor of backend B polls `(salt, frame)`; salt changes or frame resets to zero = a checkpoint has occurred → pause delta, use the **SQLite Online Backup API** (read-only connection) to take a consistent snapshot, upload it as an **L9 snapshot** and record txid, then resume delta from frame 0 and write a **snapshot→delta linkage marker**. P0 must test "checkpoint during active writes".

### Durability proof (output gate)

Before a write is acknowledged to the caller, it must be proven durable:

| Mode (`CELLHIVE_DURABILITY`) | Proof |
|---|---|
| **fleet** (preferred; `auto` selects it when there is a live follower) | The owner sends the write to 1–2 followers (other `cell-agent` nodes), and **acknowledges once at least 1 follower has fsynced it to disk**; object storage upload happens asynchronously after acknowledgement. **If no peer is available, it does not error**, and degrades to "wait for bucket" (see below), ensuring RPO=0 |
| **bucket** | Use only object storage: upload by block and wait for that block to complete (`CELLHIVE_BUCKET_WAIT=true`, ~90ms in-region) |

> `fleet`/`bucket` is a **durability requirement**, not "a peer must exist": when `fleet` has no peer, it degrades to waiting for the bucket (writes wait for the bucket), and does not silently ack.

- The owner's local commit counts as 1 copy; **plus ≥1 follower landing on disk** constitutes the proof, so **2 cell-agent nodes are sufficient** (owner + 1 follower).
- The owner recruits **at most 2 followers** (at most 3 replicas); as long as 1 follower remains, fleet acknowledgement continues, with no need to fall back to waiting for bucket per write.
- The follower must be **another node** (the owner never counts itself as a follower).
- **S3 is not on the write ack path** (fleet mode): steady-state in-region write latency is about ~25ms, determined by follower fsync.
- In multi-node deployments, **bucket upload is asynchronous in the background and can be batched/periodic** (LTX segment merge / group commit), **not one PUT per write**; the bucket remains the long-term authority and recovery source.
- Only when **no follower is available** (single node, or ensemble unavailable and degraded) does ack degrade to **waiting for bucket block upload** (`bucket-batch`, ~90ms in-region); explicit `BUCKET_WAIT=false` changes it to ack-on-enqueue (RPO>0).
- Backend B (DO): after do-runtime reports to cell-agent, that cell-agent acts as owner and performs the same ensemble proof with other cell-agents (followers).
- The write ack path for backend B is: workerd commit → supervisor capture → **cell-agent (possibly cross-node)** → proof → output gate; cross-node intranet latency must be included in the P0 performance gate.
- When the response body is streamed, each chunk is also constrained by the output gate—the caller will not act on a value that "may be lost".
- **RPO=0**: acknowledged writes will not be lost due to a single-node failure.

### Single-node (bucket) mode (`CELLHIVE_DURABILITY` / `CELLHIVE_BUCKET_WAIT`)

A single node has no follower, so write acknowledgement degrades to object storage proof. Configuration has two axes:

- **`CELLHIVE_DURABILITY`** = `auto` (default) | `fleet` | `bucket`: selects the proof method.
  - `auto`: uses fleet when there is a live follower; otherwise uses bucket.
  - `bucket`: uses only bucket and does not attempt peers.
  - `fleet`: **requires RPO=0**; with peers, uses follower fsync; **without peers, degrades to "wait for bucket" and alerts** (does not silently ack). Therefore `fleet` + `CELLHIVE_BUCKET_WAIT=false` is a contradictory configuration and is rejected at startup.
- **`CELLHIVE_BUCKET_WAIT`** = `true` (default) | `false`: in bucket posture, **whether to wait for that block upload to complete**. (`fleet` always waits.)
bucket commits are performed **in "blocks"** (a batch of writes for the same `(scope, epoch)` is accumulated into one object; block names use ordered txid ranges):

| bucket sub-mode | ack timing | RPO | Trigger |
|---|---|---|---|
| `batch` (default) | Accumulate blocks; ack **after that block has been committed** (flush immediately when idle; concurrency naturally batches) | 0 | `BUCKET_WAIT=true`, or `DURABILITY=fleet` downgrade |
| `async` | **Ack on enqueue**; blocks are **committed serially and asynchronously** in the background (bounded queue; overflow falls back to synchronous) | **>0** (a crash may lose the window) | `BUCKET_WAIT=false` |

- Both modes share the `internal/upload` batcher; `batch` uses `EnqueueWait`, and `async` uses `Enqueue`. When **no batcher is configured**, it degrades to direct per-write PUT (`mode:"bucket"`).
- There is **no longer a separate `sync` mode**: with single concurrency (flush immediately every time), `batch` is equivalent to per-write synchronous PUT and has the same RPO, so it is folded into `batch`.
- **Ordering guarantee**: the batcher is a **single loop**—it will not accept the next batch before `AppendBatch` completes, so the same `(scope, epoch)` **will not have two blocks uploading concurrently**; writes are packed in write order within a block, block names use increasing `txid ranges`; `Restore` reorders by txid. Covered by test `TestBatcherBlocksOrderedAndSerial` (forms blocks `< N`, order `0..N-1`, `maxInflight == 1`).
- **Constraint**: block objects are **create-once (If-None-Match) and named by txid range**, so txids for the same `(scope, epoch)` must be **monotonically increasing and non-reusable**; load tests/replays that reuse txid ranges will hit an object with the same name and get 412.
- With **remote/transoceanic object storage**, `bucket` (RPO=0) tail latency of several seconds is unusable; in that case either run ≥2 nodes with fleet, or explicitly accept `async` with RPO>0.

### Takeover

1. Check the predecessor owner's node-log records; if open/recovering, first perform **recovery**: fence the record, seal followers, upload its retained segments, and mark sealed;
2. Restore along the epoch chain (from the snapshot start point or the predecessor handoff point);
3. Claim the owner record (epoch+1) and then start serving.

Large numbers of dead nodes may make recovery take several minutes; requests wait during this period, retry with backoff, and fail only after exceeding the budget (configurable).

### node-log and recovery protocol (C-03 fix)

**node-log** is a session record maintained by each cell-agent node in **fleet mode**. It ensures that writes for which "follower fsync has been acknowledged but bucket upload has not completed" can be recovered after a node crash.

**Storage location**: bucket `node-logs/<node>/<session>.json`, written by this node.

**Lifecycle**:
- **open**: conditionally created when the node starts and receives its first fleet-mode write ack. Records: `{node, session, epoch, followers, status="open"}`.
- **recovering**: set to recovering (CAS) when another node sees an open record and starts recovery.
- **sealed**: set to sealed after recovery completes. The node also proactively seals it on graceful stop.
- **Invalidation**: after a node restarts, it does not use the old session; the old log is left for recovery to process.

**Recovery execution** (before the new owner takes over):
1. Read all open/recovering records under bucket `node-logs/<dead-node>/`;
2. For each open session, set it to recovering with CAS;
3. Contact the followers recorded in the session (addresses come from their node leases at that time) and request seal (streaming the unuploaded segments they hold);
4. Upload the received segments to the corresponding `cells/<scope>/ltx/e<epoch>/`;
5. After confirming completeness, mark the session sealed;
6. Continue the normal takeover flow.

**fast path**: if the node stops gracefully (SIGTERM completes handoff), it seals its own log and ensures upload is complete, so takeover skips the recovery step directly.

**Automatic orchestration (ADR-066)**: the recovery above no longer depends on manual triggering. On each pass, the fleet waker leader also executes `recovery.Runner.Pass`: use `nodelog.Nodes` to enumerate nodes with node-logs, **use node leases for liveness** (alive → skip, never recover; missing/expired → considered dead; lease read error → skip to remain fail-safe), and execute steps 1–6 above for dead nodes. The entire process is **idempotent** (sealed fast path, one-way open→recovering→sealed) and can share the same leader election as timer dispatch.

**follower seal unreachable**: if a follower also dies during recovery, its retained segments cannot be fetched → recovery restores from the already-uploaded portion in the bucket (potentially losing the last segment that follower acknowledged but that was not uploaded) → RPO degrades to the exact point of "uploaded segments". This is the extreme case of two nodes crashing simultaneously in fleet mode; with ≥3 nodes, a second follower provides fallback.

## 7. Graceful handoff

When a node shuts down (SIGTERM):

1. Mark draining and stop accepting new cells;
2. In batches (for example, at most 32), for each cell: stop new local routing → wait for in-flight requests to complete → close the database and publish a complete snapshot, verify it can be restored → release the owner record → ask a compatible peer to claim it (after peer confirmation, keep dormant);
3. `cell-agent`: exit only after confirming replication/snapshot completion; `do-runtime`: stop workerd only after confirmation.

Simultaneous shutdowns are serialized by the **bucket drain token** to avoid an instantaneous shock to surviving nodes.

**drain token protocol (M-07 fix)**:
- The drain token is the bucket object `fleet/drain-token.json`, containing the holder node name + session + expiry (default 30s);
- During shutdown, the node acquires the token via conditional create/CAS; only after acquiring it does it start migrating cells in batches;
- After a graceful stop, the node deletes the token (or lets it expire), and the next node can continue;
- If the holding node crashes, the token automatically expires after TTL, and a new node can acquire it;
- During concurrent multi-node shutdowns (such as a rolling upgrade), at most one node is in the active drain phase; the rest spin and retry in the wait queue (backoff interval 5–10s).

## 8. bucket hard requirements

Object storage must provide:

1. **Conditional create** (write succeeds only when the object does not exist);
2. **Conditional overwrite** (write fails if the object was modified after it was read);
3. **read-after-write consistency**;
4. **ranged read** (returns the requested byte range);
5. **conditional delete** (a stale version must not delete a newer owner/lease; S3 uses `If-Match`, native append providers use a position-checked tombstone).

| Provider | Qualified |
|---|---|
| S3 API object storage | Verify conditional create, target CAS, conditional delete, and ranged read per instance; the protocol name alone is insufficient |
| Pinned MinIO `RELEASE.2025-02-18T16-25-55Z` | ❌ 2026-09-22: stale `DeleteObject If-Match` was ignored despite four successful write checks |
| Tested COS/OSS S3 endpoints | ❌ Ordinary S3 conditional create failed; not valid for owner/lease |
| Native `oss://` | Isolated concurrent claims, owner takeover and startup probe passed on the tested Beijing bucket; verify other buckets separately |
| Native `cos://` | Concurrent append claims, CAS/conditional delete, and the startup probe passed on the tested Hong Kong `cell-1376795072` bucket; the earlier `vwork-hk-1376795072` bucket returned 405; verify other buckets separately |

**Startup probe**: Every node checks create/reject-create/CAS/reject-stale, ranged read, rejection of stale conditional delete, and successful current-version delete; any failure terminates startup. `cellhive diagnose` repeats this **write-based** probe on a running node; it is not read-only. See `storage-and-s3.md` / ADR-188.

### Bucket roles (topology)

| Role | Default | Contents | Can be split into an independent bucket |
|---|---|---|---|
| `state` | Primary bucket | cell replication data, owner/lease, nodes | ✅ |
| `code` | Primary bucket `bundles/`, `deploy/` | Worker bundle (content-addressed), version pointers | ✅ |
| `assets` | Primary bucket `assets/` | Static assets (can be put behind a CDN) | ✅ |
| `r2` | Primary bucket `r2/` | Tenant R2 virtual buckets | ✅ |
| `backup` | None | Optional archive | ✅ |

**Default is a single bucket + reserved prefixes; each role can be overridden with an independent bucket/credentials/lifecycle.** Long-lived credentials are held only by `cell-agent` (ADR-023).

## 9. No scanning (bucket interaction budget)

- Hot paths **may only do point lookups by key**; **LIST is prohibited**.
- bucket interaction budget = **O(number of nodes + number of activations/migrations)**, decoupled from the number of cells, requests, and timers.
- Enumeration (such as listing instances of a class) is allowed only in operations commands, with bounded pagination (for example, ≤1000/page, cursor-based continuation).

## 10. Timers (unified due abstraction)

All scheduled events are unified as:

```
timer = { dueAt, kind, scope, token }
kind ∈ { do-alarm, cron, queue-delay, queue-retry, workflow-sleep, workflow-timeout }
```

- **Storage**: due records are stored in **the SQLite of their owning cell** (indexed by time/time bucket). **No bucket timer index is created.**
- **Triggering**: two layers—
  - **Local due**: each `cell-agent` maintains a local earliest-deadline structure for the timer cells it owns; when due, it dispatches locally to the target (`do-runtime` / `user-runtime.scheduled()` / `user-runtime.queue()` / workflow execution), with zero bucket interaction;
  - **Single fleet waker**: a leader elected by bucket lease, which serves as a fallback for due items whose **owner is dead/unreachable**—it first takes over the corresponding cell based on the owner record, then reads its SQLite and dispatches.
- **Dispatch addressing**: cron/queue/workflow are uniformly sent to the **logical service name** of `user-runtime` (mesh), any healthy replica, with no node table needed; duplicate dispatch is prevented by the **queue cell's claim/lease**, independent of the target replica.
- **Prerequisite**: cells containing timers to be triggered must be **resident/pinned**, otherwise no one ticks them; owner invalidation is taken over and activated by the waker.
- **Guarantees**: at-least-once + `token/epoch` deduplication; cron is minute-aligned, best-effort, and **does not backfill missed runs**; queue is at-least-once + DLQ; workflow sleep uses absolute time.

## 11. Failure semantics

- Non-idempotent operations are **not blindly replayed** after owner transfer failure: return `result-unknown`, and let the caller decide whether to retry with a stable operation id.
- When a stale owner returns a dedicated control error, the caller may **resolve the owner again and retry once** (only when it can prove the handler has not started).
- Cross-version operation follows the reader-before-writer rolling order.

### Protocol versioning (M-09)

- **owner records** contain `proto_version` (for example, `"v1"`); **LTX segment headers carry a version byte**.
- Upgrades follow **reader-before-writer**: new readers are rolled out first (able to read the old format), and the new writer is enabled only after all old readers are offline.
- **Rollback safety rule: read/write version difference ≤ 1**; incompatible changes across ≥2 versions must use explicit migration (offline/downtime or dual-write), with no implicit compatibility.

## 12. Inter-node communication and latency

### 12.1 Communication contents

| Type | Direction | Hot path? | Description |
|---|---|---|---|
| **LTX append / tail** | owner → follower | ✅ Hot path | Stream-append LTX segments; follower acks after fsync |
| ack | follower → owner | ✅ | The first ack constitutes proof |
| seal / tail (recovery) | recovery node ↔ followers | Cold path | Collect predecessor's unuploaded data during takeover |
| acquire / release | peer → peer | Cold path | Graceful handoff, takeover |
| fleet sample | bucket (refreshed by one node) | No | Non-peer read, avoids O(n) |
| owner resolution/hint | bucket + cache | No | Non-peer |

### 12.2 Transport choices

- **Persistent connections + multiplexing** within the private network (HTTP/2 or custom binary framed streams), **avoiding a new handshake for every write**;
- **Separate control and data channels** to avoid head-of-line blocking (large LTX segments do not block owner hint/acquire);
- Plaintext in the private network + fleet HMAC/internal token (use mTLS when security requirements are high);
- Binary/length-prefixed; **JSON is prohibited** on hot paths.

**Protocol choice (internally unified on REST/HTTP, canceling the gRPC plane)**:

> **Revision (ADR-136)**: the `:7000` plane originally planned in this section for Go↔Go using gRPC (ADR-026/028) **was never implemented**, and it has been explicitly decided that it **will not be implemented**. All current internal production traffic uses HTTP: the peer LTX hot path is **length-prefixed binary frames + HTTP 101 persistent stream** (ADR-042), and everything else is REST/JSON. The table below describes the **current implementation**.

| Link | Protocol | Description |
|---|---|---|
| `cell-agent` ↔ `cell-agent` (peer RPC: acquire/release/seal/tail/recovery) | **REST/JSON (HTTP/1.1)** | Same connection pool; no HTTP/2/gRPC |
| `cell-agent` → `cell-agent` (**LTX append/tail hot path**) | **length-prefixed binary frames + HTTP 101 persistent stream**, fallback to `/append_batch` on failure | Multiple scope-hashed lanes per follower, multiple in-flight batches per lane (ADR-042) |
| do-runtime supervisor (Go) → `cell-agent` (WAL report/claim/restore) | **REST/JSON** | Go↔Go, same as above |
| `cell-agent` → `user-runtime` :8088 (scheduled/queue/workflow dispatch) | **REST/JSON** | Target is workerd(JS) |
| `user-runtime` (workerd JS) → `cell-agent` :7001 (bindings) | **REST/JSON** | workerd JS; see ADR-013 |
| `user-runtime` (host adapter) → owner `do-runtime` :8788 (DO call/WS proxy) | **HTTP / JSRPC** | JS↔JS |
| do-runtime internal supervisor ↔ workerd | Local (loopback/JSRPC) | Same Pod; WAL capture is **file reading** |
| External/tenant API | **REST/JSON** | See ADR-013 |

- **No internal gRPC**; all internal links involving workerd(JS) or Go use HTTP/REST/JSRPC.
- Large LTX segments use **length-prefixed binary frames** for streaming framing, avoiding head-of-line blocking from a single oversized message.
- If HTTP becomes a bottleneck in the future, optional approaches are to **push the same interface down to raw TCP framed streams** or use Connect(Buf) for unified definitions; not introduced by default.

### 12.3 Reducing latency (layered)

| Layer | Approach |
|---|---|
| Network | **Same region/same AZ/same rack**; NVMe; avoid cross-AZ/cross-region; owner and follower in the same AZ but on different machines (low latency + failure independence) |
| Connections | Long-lived connections, connection pools, keepalive, multiplexing |
| Batching | ✅ **group commit**: owner-side `ShipBatcher` (1ms window, ≤256 segments/batch) combines concurrent commits into **one frame containing multiple segments** in a single POST (`/v1/peer/append_batch`); follower-side `Spool.AppendBatch` performs one log write + one fsync (ADR-040/041) |
| Redundancy | ✅ **adaptive hedge (ADR-164, default)**: first write the primary (1 copy); after exceeding `max(250ms, 4×recent slowest append)` (cap `CELLHIVE_PEER_HEDGE_MAX_MS`=2000), send a **second copy** to the next follower, and take the first to arrive (quorum 1); `CELLHIVE_PEER_HEDGE_MS=0` = send only to primary (single copy), and sequentially fail over only if primary fails. Duplicate replicas are safe: spool is **idempotent by `ltx.Header.ID()`** |
| Protocol | ✅ length-prefixed binary frames + **HTTP 101 persistent stream**; 4 scope-hashed lanes per follower, up to 4 in-flight batches per lane, fallback to `/append_batch` on failure (ADR-042); ❌ gRPC plane **has been canceled** (ADR-136; all internal traffic is HTTP) |
| Storage | NVMe + batched fsync; avoid write amplification |
| Read path | Local read-only; does not touch peer/bucket |
| Deployment | Node affinity (same failure domain); bucket upload only in the background |

### 12.4 Planned Adoption

- ✅ **Persistent peer stream + group commit + first ack + adaptive hedge (ADR-164, default; normally 1 copy, add a 2nd copy when slow)**;
- owner and follower **same-AZ affinity**;
- bucket uploads only in background batches;
- separate control/data channels; internal token/HMAC.

## 13. P0 Validation Items (and Performance Gates)

| Metric | Threshold (recommended) |
|---|---|
| cell-agent two-node steady-state write p50 / p99 | ≤ 30ms / ≤ 100ms (same-region bucket) |
| single-node degraded write p50 | ≤ 120ms (recorded, not default) |
| cross-region bucket write p50 | recorded (expected ~600ms) |
| **DO: capture→cell-agent→proof (cross-node) p50 / p99** | requires unit tests and gates |
| cold-start restore: 100 MiB cell | p50 ≤ 5s, p99 ≤ 15s |
| `kill -9` takeover | RPO=0, takeover ≤ 5s |
| Number of object PUTs per ack | significantly < 1 under high concurrency (group commit effective) |
| conditional write conflict/retry rate | < 1% |
| Number of objects read during large DB takeover | ≤ ~50 with L1 compaction |
| LIST count on hot path | 0 |
| **peer append round-trip p50 / p99** | requires unit tests and gates (same AZ) |
| **hedge hit / ineffective ratio** | `/metrics` `cellhive_peer_hedge_fired_total` (replica issued) / `cellhive_peer_hedge_won_total` (replica won the ack); wait is derived from the most recent slowest append in a rolling window (ADR-164/165) |
| **group commit batching ratio** | append/fsync count per ack significantly < 1 under high concurrency |

_Last updated: 2026-09-19_