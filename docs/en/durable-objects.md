# Durable Objects

> **Status (2026-09-15)**: **host-actor skeleton + ownership/fencing/draining/residency implemented and verified** (ADR-077/078: facets + `workerLoader.getDurableObjectClass` + localDisk; `/v1/do/invoke`; shard, owner record + **monotonic generation** (`owner-gen`), pre-dispatch guard/renew, `/v1/do/drain`, `DO_PREVENT_EVICTION` resident/evictable; real workerd e2e).
> **alarm (ADR-079)**: facet-native alarms are unavailable (pinned `alarms are not yet implemented for SQLite-backed Durable Objects`; latest 2026-09-15 still `Facets currently cannot set alarms`) → platform shim: the `cellhive-do.js` base class maps setAlarm/getAlarm/deleteAlarm to reserved keys in object storage, the host reports them to cell-agent, unified timer (`KindDOAlarm`) + `DoAlarmDispatcher` → do-runtime `kind:"alarm"` calls `alarm()`; real workerd e2e passes. **Differences from CF**: no native retry count, unsupported inside `transactionSync`, no per-alarm lease.
> **WebSocket (ADR-080)**: `GET /v1/do/connect` upgrades to the facet as-is (hibernation API); before restart/abort, call `__chCloseAll(1012)` and then `facets.abort()` → client receives 1012 (real bun e2e). **Tenant wiring (ADR-175)**: for `env.DO.get(id).fetch(upgrade request)`, the facade detects `Upgrade`, first calls cell-agent `GET /v1/do/connect` to obtain `{owner,ticket}` (scoped token), then connects directly to the owner's `/v1/do/connect` with the ticket; do-runtime allows the ticket for connect and verifies it only for the local shard.

> **legacy DO classes (ADR-174)**: classes that only implement `fetch/alarm/webSocket*` and do not `extends DurableObject` are supported after being wrapped by do-runtime's facet entry module (`renderFacetModule`); non-2xx responses from `env.DO.get(id).fetch()` are returned as-is.
> **DO client binding (ADR-080)**: `env.DO.get(...).fetch()` → `scopeAuth("do")` proxy places requests onto `CELLHIVE_DO_RUNTIMES` by shard; owner conflicts are forwarded once + hint; `result_unknown` semantics.
> **Storage lifecycle (ADR-081)**: code changes use **facet-level lazy restart** (version stamp → `facets.abort()` a single object, storage unchanged); `migrations` only allows `new_classes`/`new_sqlite_classes`; `renamed/deleted/transferred` are rejected; **`doStorageId`** is the stable storage identity (unchanged across redeployments).
> **migrations v2 (ADR-082)**: **object registry** (`GET /v1/do/objects` + cell-agent aggregation); **rename** (`renamed_classes`, alias `codeClass→storageClass`, no file migration, data preserved); **delete** (`deleted_classes` marker + `POST /v1/do/delete` physical reclamation); **eager restart** (`CELLHIVE_DO_EAGER_RESTART`, lazy by default).
> **Durability (ADR-083, partial)**: `do-supervisor` captures all `*.sqlite` at **shard (host actor directory) granularity** (LTX snapshots + increments → fleet/bucket proof), and **gates** do-runtime responses (no ack unless proven; failures return `result_unknown`). **Discovery**: `.facets` is a workerd internal binary format, so it is not parsed and is handled by shard.
> **Cross-node cold activation (ADR-084, partial)**: reversible `scopeID` (base64url) + manifest + **`RestoreAll`** (LTX `replica.Restore` + `restore.ApplyFile`, sidecar writes back byte-for-byte); **dual runtime e2e** passes (A runs → captured to bucket → B restores → state continues). Deterministic host filenames across processes are the prerequisite for "restore to the original path" to hold.
> **Bindings inside DO ✅ (ADR-090)**: the platform exports the `WorkerEntrypoint` capability class; `ctx.exports.X({props})` stubs are placed into the facet env; KV/D1/R2/Queue use stubs, DO/Workflow go through env-patch → **both `this.env` and the constructor parameter `env` work**.
> **`deleteAll()` shim ✅**: stock workerd throws an internal error for `deleteAll()` on SQLite facets; the base class changes it to clear KV + drop user tables + clear alarms (`cellhive-do.js`).
> **Facet discovery ✅**: `.facets` format has been parsed (magic + `[flag][len][name]`, entry i → `<hash>.<i+1>.sqlite`); **on-demand paging ✅** (`CompactAll` + `PageFetcher.Materialize`, ranged read per page); **takeover ✅** (`TestDoRuntimeTakeoverAfterCrash`); **compat suite ✅** (`TestDOCompatSuite`, 19 subtests).
> **Object keys ✅**: `ctx.id.toString()` = `<hosthash>`, `ctx.id.name` = hostId (including storage_id) → reported by host.js → supervisor replicates facet files by `storage_id/<class>/<objectName>`; `RestoreObject` cold-starts by object (ADR-084).
> **Remaining P3 items (non-functional gaps)**: ① runtime SQLite VFS lazy read — **on the DO side**, not implementable under stock workerd (ADR-085, alternative = cold-start materialization by object/by page); implemented on the cell-agent side (ADR-160, `internal/pagedvfs`; force hydrate before capture/snapshot); ② **class C environment validation** (real cross-host RTT, cloud object storage, multi-host chaos/takeover) lacks environment; ③ cross-worker `transferred_classes` intentionally rejected (same-worker supported, see wrangler-compat); ④ worker↔worker is already native JSRPC in the same instance (ADR-102), DO RPC already supports JSON+tagged (ADR-162); the only remaining item is RPC transfer of `ReadableStream`/`RpcTarget`.

## Positioning

DO and KV are **equivalent in outcome** (object storage authority + bucket lease/epoch + durable addressing + RPO=0), but the **mechanism is one level weaker**: KV/D1 SQLite is owned by `cell-agent` itself (precise transaction and capture control), while DO SQLite is owned by workerd and is only a **working copy** local to `do-runtime`; persistence goes through `cell-agent`.

This is the inevitable cost of the three constraints: "keep stock workerd + do not modify workerd + preserve DO synchronous SQL".

## Why native facets are required

Cloudflare/workerd's `ctx.storage.sql.exec()` and `storage.transactionSync()` are **synchronous** APIs; KV/D1/Queue/Workflows/R2 are all asynchronous Promise APIs.

- Out-of-process storage + synchronous SQL: workerd does not provide synchronous JS→host calls (no synchronous FFI, no SharedArrayBuffer/Atomics blocking), so this cannot be implemented without modifying workerd;
- Therefore DO SQLite must stay inside the workerd process → use **workerd native Durable Object facets**;
- Only the remaining asynchronous bindings can go through `cell-agent`.

## Composition of do-runtime

`do-runtime` is a **distributed elastic execution layer**: nodes can be added or removed at any time, and local disks may be **temporary** (authoritative state is in cell-agent).

| Component | Role |
|---|---|
| **workerd** | Native DO host actor (platform-owned worker): owner routing entrypoint + facet executor; configuration includes `durableObjectNamespaces` + localDisk; uses `workerLoader` to load immutable tenant bundles and `getDurableObjectClass()` to resolve user classes |
| **Go supervisor (PID1)** | Starts/monitors/restarts workerd; claims/renews owner with cell-agent; drains on SIGTERM; **WAL capture module** (formerly "sidecar", **merged into supervisor, not a standalone program**) uploads WAL segments to cell-agent; exposes `/state` |

The supervisor **does not hold object storage credentials**; captured WAL segments are always handed to `cell-agent`.

## Request lifecycle (cross-node)

```
Tenant Worker (user-runtime)
  env.NS.get(id).fetch(req) / RPC / WS
    → host adapter (binding-scoped, immutable props)
  cell-agent resolves owner: (ns, class, objectId) → owner **do-runtime node** address + epoch
    → connect to owner do-runtime
  owner do-runtime:
    1. Verify node liveness + owner record + epoch
    2. workerLoader loads immutable tenant bundle (cached)
    3. WorkerStub.getDurableObjectClass() obtains the user DO class
    4. Execute with native facet (input gate / synchronous SQL / transactions / alarm / WS)
    5. Write local working-copy SQLite
    6. supervisor captures WAL → uploads to cell-agent → cell-agent writes e<epoch> prefix + durability proof
    7. [output gate] wait until cell-agent confirms this commit is durable → return
```

- **Execution is always on the owner do-runtime node**; non-owners forward only once (and return a trusted owner hint for cache refresh).

## DO RPC (ADR-162)

The stub returned by `env.NS.get(id)` / `env.NS.getByName(id)` supports **directly calling tenant DO methods** in addition to `fetch()`:

```js
import { DurableObject } from "cloudflare:workers";

export class Room extends DurableObject {
  async addMessage(user, text) {
    const n = (await this.ctx.storage.get("n")) || 0;
    await this.ctx.storage.put("n", n + 1);
    return { n: n + 1, user, text, at: new Date() };
  }
}

export default {
  async fetch(req, env) {
    const stub = env.ROOM.getByName("room-1");
    return Response.json(await stub.addMessage("u1", "hi"));
  },
};
```

- **Path**: the stub encodes `method`/`args` as tagged JSON → `POST /v1/do/invoke` (`kind:"rpc"`, sharing the same owner-hint/ticket/`409/5xx no replay` path as fetch) → the do-runtime host calls the facet method using **native JSRPC** → result is returned as tagged data. The host calls the facet using structured clone, so Map/Date/ArrayBuffer and so on are lossless; the cross-process segment is carried by tagged encoding/decoding.
- **Supported values**: `undefined`, `-0`/NaN/±Infinity, bigint, Date, RegExp, Map, Set, ArrayBuffer, TypedArray/DataView, Error (including `cause`), URL, URLSearchParams, shared references/cycles. Class instances degrade to plain objects (consistent with the observable behavior of structured clone).
- **Limitations**: functions, Symbol, Promise, WeakMap/WeakSet, `ReadableStream`/Response, `RpcTarget`, and DO stubs cannot be passed; `args` and result are each ≤ **8 MiB** (after tagged encoding; binary expands by ~4/3 via base64); method names must be identifiers and must not be reserved names such as `fetch`/`alarm`/`constructor`/`__proto__`/`then`/`toString` or `__ch*` platform methods; handler exceptions appear at the caller as `Error` with `code`/`name`/`message`/`stack`. RPC has no tenant `Request` object, so it does not carry traceparent (only requestId correlation).
- **DOs are not externally addressable**: public WebSockets are accepted by the tenant Worker (`user-runtime`), then proxied to the owner through the binding; `Traefik`/the public Internet do not directly touch do-runtime.
- The **output gate** completes before owner do-runtime returns, guaranteeing RPO=0.

## cell-agent ↔ do-runtime internal contract

What `cell-agent` gives DO is **not just the owner address**, but the complete durable read/write endpoints. do-runtime **does not touch object storage**; local SQLite is only a working copy; all LTX reads and writes go through cell-agent.

| Capability | Internal API | Description |
|---|---|---|
| Resolve owner | `resolve(scope)` | Returns owner do-runtime address + epoch |
| claim / renewal | `claim(scope, advertise)` / renew | Coordinates and writes owner record (bucket); handles takeover |
| **LTX write ingestion** | `append(scope, epoch, segment)` / `sync(scope)` | Receives WAL/LTX segments captured by supervisor, writes `cells/<scope>/ltx/e<epoch>/` + durability proof |
| **Restore reads** | `restore(scope, epoch)` | Sends snapshot / epoch chain; do-runtime materializes local SQLite |
| Output-gate confirmation | `sync(scope)` | Returns proof that "this commit is durable" |
| alarm fallback | waker | Legacy alarms after owner death are taken over by the cell-agent-side waker |

**Write path**: `workerd commit → supervisor captures WAL segment → cell-agent.append/sync → cell-agent writes e<epoch> prefix + proof → output gate → return`
**Read path (cold activation/takeover)**: `do-runtime → cell-agent.restore → cell-agent reads object storage (epoch chain/snapshot) → sends down → materializes local SQLite`

Physical data lives in object storage under `cells/<scope>/ltx/e<epoch>/...`; the **only accessor is cell-agent**.

## DO owner claim protocol (C-01 fix)

The do-runtime supervisor actively initiates claim requests to cell-agent. Flow:

1. **Cold start/activation trigger (D final, cell-agent driven)**: user-runtime's host adapter receives the first call to DO `(ns, class, id)` → cell-agent resolve finds no owner record (or it is expired) → cell-agent **selects a do-runtime candidate node** (platform service discovery/config list) → sends an `activate(scope)` notification to that node; after receiving it, do-runtime sends `claim(scope, advertise)` to cell-agent. (do-runtime self-claim is only an internal fallback, not the primary path.)
2. **cell-agent executes claim**: conditionally write `cells/<scope>/owner.json` (`If-None-Match: *` or CAS). Content: `{node, session, epoch+1, expiry, address=do-runtime-advertise}`. **cell-agent is the only bucket writer**, completing this on behalf of do-runtime.
3. **claim competition**: if multiple do-runtime instances issue claim at the same time, the bucket conditional write guarantees only one succeeds; losers read the winner from the response and forward requests.
4. **claim success response**: cell-agent returns `{epoch, expiry}` to supervisor; after supervisor accepts it, it starts restore (if data already exists) and then enters ready state.
5. **renewal**: supervisor continuously sends renew to cell-agent (carrying session+epoch), and cell-agent updates expiry via CAS. **Renewal failure (cell-agent rejection) means epoch has changed (taken over)** → supervisor must self-fence and restart.
6. **claim timeout**: the output gate has a timeout (see known-issues.md #C-07); if proof cannot be obtained within the deadline, return `result_unknown`.

## DO cold activation / restore flow (C-04 fix)

| Step | Operation | Notes |
|---|---|---|
| 1 claim succeeds | cell-agent writes owner record | epoch = prev+1 |
| 2 check restore needed | cell-agent checks whether object storage has `e<prev_epoch>` data | Brand-new DO → skip to step 5 |
| 3 restore stream | `cell-agent.restore(scope, epoch)` streams to supervisor: first snapshot (if any), then the delta LTX chain | supervisor receives and writes to a local temporary file |
| 4 materialize SQLite | supervisor notifies workerd (load the file through localDisk configuration) or replaces the local SQLite file and restarts the workerd actor | workerd reads the correct state |
| 5 ready | supervisor notifies cell-agent `ready(scope, epoch)` | Requests start being dispatched |

- **Restore failure/interruption**: supervisor returns to step 1 and claims again (new epoch); cell-agent discards incomplete data.
- **Concurrent requests**: requests arriving during restore are queued on the cell-agent side until `ready`.
- **Large snapshots**: streaming paging is supported (sparse files are downloaded on demand); supervisor can become ready before restore is complete (only downloaded pages are usable).

## In-flight requests during the DO lifecycle (C-07 fix)

| Case | Behavior |
|---|---|
| Request in-flight, owner lease expires | The output gate detects that cell-agent returns epoch mismatch → return `result_unknown` to host adapter |
| Non-idempotent request, host adapter receives `result_unknown` | **Do not replay**; return `result_unknown` to Worker (Worker decides whether to retry) |
| Idempotent GET/HEAD class, stale-owner error received | host adapter re-resolves owner and retries **once** |
| Output gate waits for proof and times out | cell-agent returns `timeout`; supervisor replies to host adapter with `result_unknown` |
| WS connection, owner migration | owner closes WS with **1012**; **host adapter receives 1012 → re-resolves owner → establishes a new WS → continues proxying** (transparent to browser client); stable operation id is used for idempotency |

**output gate timeout defaults to 10s** (configurable via `DO_OUTPUT_GATE_TIMEOUT_MS`); after timeout, supervisor self-fences, and host adapter receives `result_unknown`.

## ownership and persistent addressing

- Granularity: **one owner/lease per DO object** (scope = `(ns, DoClass, objectId)`), (one cell is one DO).
- **The owner record points to the do-runtime execution node** (including advertise address + epoch), written by **cell-agent**.
- **No dedicated heartbeat**: do-runtime liveness is determined **implicitly by replication activity**—continuously reporting WAL/renewal to cell-agent counts as alive; stopping reports → owner lease expires → takeover is allowed.
- **Takeover candidates** use **platform service discovery** (K8s Endpoints / mesh) or on-demand probing; no self-registration table is maintained.
- **router cache**: normal requests only query the owner resolution cache in cell-agent; the bucket is read only for cold activation, cache invalidation, or takeover (point lookup).
- WebSocket `connect` must land on the owner; during owner migration, it is closed with **1012**, and the client reconnects with a **stable operation id**.

## Replication: WAL capture → cell-agent (default) and FUSE (optional)

### Default: supervisor captures WAL → cell-agent (portable)

- supervisor and workerd are on the same node and the same volume, and open the actor SQLite and `-wal` **read-only**;
- read and record WAL salt/frame positions, and **report** newly committed frames **to cell-agent** (possibly cross-node, over the internal network);
- `cell-agent` executes the cell protocol: writes the `e<epoch>` prefix + persistence proof; when a checkpoint/truncation is detected, it switches to a full snapshot for realignment;
- **Output gate**: before host adapter returns, it calls cell-agent `sync(scope)`.

### Optional: FUSE volume (self-managed environments only)

- Put the workerd localDisk directory on a FUSE volume, and let the FUSE layer handle "write locally + replicate + `fsync` means durable" (LiteFS mode);
- Advantages: intercept writes; proof is available when `fsync` returns; no separate output gate is needed;
- Cost: requires `/dev/fuse` + `SYS_ADMIN`/privileged (often disabled in hosted/restricted clusters), no precedent for "workerd on FUSE", and SQLite semantics and performance must be validated.

> **Default WAL capture → cell-agent** (fits hosted/restricted clusters and is portable); FUSE is an optional acceleration for self-managed environments and is not on the P0 critical path.

## Why it can be captured but is not "interception"

- **Interception (hook)** (SQLite `update_hook`/session extension) must be inside the process that holds the connection → we cannot reach workerd's connection;
- **Observation (read)**: open another read-only connection to read the WAL file (Litestream style) → supervisor uses this path;
- RPO=0 does not rely on "capture events", but on **gating**: after the handler returns, read the current WAL tail, and have cell-agent confirm it is durable (committed ordered append, covering the just-committed transaction).

## Alarms

- alarm rows exist in actor SQLite (replicated together), with no additional bucket index;
- each owner's supervisor maintains a local due structure for objects it **owns and keeps resident**, and wakes them locally when due → zero bucket interaction;
- a single fleet **waker** (bucket lease on the cell-agent side) covers leftover alarms for objects whose owner is dead/unreachable;
- delivery is at-least-once; during migration, alarm state is replicated to the new owner together with the object's working copy.

### alarm recovery (C-05 finalized)

1. **supervisor reads workerd's alarm table read-only**, extracts due times, and reports to cell-agent: `alarm_upsert(scope, dueAt, token)` / `alarm_delete(scope, token)`.
2. cell-agent maintains the due index in a **waker cell** (time-bucketed, **does not scan the bucket**).
3. When waker finds an item whose "owner is dead + due" → **directly triggers takeover of that DO** (claim + restore) to an available do-runtime; the new owner reads the alarm row and dispatches it; after success, it atomically advances/deletes the index item.

> **Status (updated 2026-09-15)**: **Implemented (ADR-079, mechanism changed to shim)**. The original plan of "supervisor reads workerd alarm table read-only" was replaced: pinned workerd does not support `setAlarm` for SQLite-backed DO, so the `cellhive-do.js` base class stores `set/get/deleteAlarm` in the object storage reserved key `__cellhive_alarm`; host reports `/v1/internal/do/alarm/upsert` → unified `KindDOAlarm` timer (scope `ns/__timer__/do`) → `DoAlarmDispatcher` (`internal/dispatch/doalarm.go`) → resolve owner → do-runtime invokes `alarm()` with `kind:"alarm"`. **Tested** (this page "Verified": `TestDoRuntimeAlarmShim`; `_cf_ALARM` schema see known-issues C-05).
4. **DOs with alarms are prioritized for residency** (placement policy), reducing cold activation.
5. If P0 finds that alarms cannot be reliably extracted from the alarm table, fall back: waker periodically asks live supervisors for their earliest due alarm, and performs takeover only for dead owners.

### WebSocket cross-node forwarding (ADR-084 ✅)

- When a connection lands on a **non-owner** node, `host.js` **proxies** the upgraded socket to the owner's do-runtime (`proxyConnect`: `fetch(owner/v1/do/connect)` obtains `response.webSocket`, and `WebSocketPair` is used to relay frames bidirectionally).
- **No resume**: **1012** from `facets.abort()` on the owner side is **passed through** by the proxy to the client (`TestDoRuntimeWebSocketCrossNodeForward`: B proxies to A, A aborts → client receives `CLOSE:1012`).
- Owner death → after claim expires, a new node takes over (cold activation) and establishes the socket locally; the client reconnects according to CF semantics.

### WebSocket semantics (A finalized, **CF compatible**)

- **Deploy / promote / migration all restart DO**: the old owner closes WS with **1012**; **the client reconnects**; the new owner **reconstructs** the DO, and **the handler restarts** (`webSocketOpen` will be triggered again).
- **No resume**: the platform **does not guarantee** session continuity across restarts; DO session state must be persisted to storage (hibernation model), and in-memory state is not retained.
- The user-runtime host adapter is only responsible for: detecting 1012/disconnect → re-resolving owner → letting the client reconnect (**does not restore the session by itself**).
- Consistent with Cloudflare behavior (deployment restarts objects).

## DO lifecycle (deploy side)

- Supports the new declarative `exports` (`type: durable-object` + `storage: sqlite`) and legacy imperative `migrations` (`new_classes` / `new_sqlite_classes`);
- **Reject**: `renamed_classes`, `deleted_classes`, `transferred_classes`, cross-worker `script_name` (only same-worker class is supported);
- See [`wrangler-compat.md`](wrangler-compat.md) for details.

## P0 validation items

1. Whether workerd's actor SQLite is in **WAL mode**;
2. Whether it can be **opened read-only by another process** and read committed transactions;
3. Whether checkpoint/truncation behavior can be detected and realigned by supervisor;
4. p50/p99 for **cross-node** "capture→cell-agent→proof" (write ack hot path);
5. do-runtime **cold activation/restore** time (by object size; temporary disk may cold start every time);
6. Losslessness and duration of DO migration when do-runtime instances are added/removed.

If 1–3 do not hold: degrade to periodic snapshots (RPO=interval) or (self-managed environments) FUSE.

## Comparison with KV (important)

| Dimension | KV / D1 (cell-agent) | DO (workerd native + cell-agent persistence) |
|---|---|---|
| Working SQLite writer | cell-agent (Go, precise control) | workerd (do-runtime local) |
| Capture | Controls its own transactions/WAL (can hook, can tune checkpoint) | supervisor externally observes WAL read-only |
| Persistence endpoint | cell-agent itself | cell-agent (do-runtime has no bucket credentials) |
| Authoritative state in object storage | Yes | Yes |
| RPO=0 | Gating inside cell-agent | Gating in host adapter + supervisor + cell-agent |
| owner node | cell-agent node | **do-runtime node** |
| User-visible result | Baseline | **Equivalent** (data in object storage, can be taken over, RPO=0) |
| Mechanism | Fully controllable | One level weaker, depends on WAL observability and cross-node reporting |

## Risks

- workerd's WAL/checkpoint semantics have not been validated (eliminated in P0);
- actor storage is multi-file (shared metadata + per-actor), making consistent replication more complex;
- **Cross-node reporting is on the write ack hot path**, requiring low internal-network latency and capacity planning;
- Temporary disk ⇒ cold activation requires restore from cell-agent/object storage, and cold start cost is the price of elasticity;
- The in-house PID1 supervisor must correctly orchestrate cross-process shutdown order.

_Last updated: 2026-09-19_