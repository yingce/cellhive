# Timers and Dispatch

## Unified Timer Abstraction

All timed events are unified as:

```
timer = { dueAt, kind, scope, token }
kind ∈ { do-alarm, cron, queue-delay, queue-retry, workflow-sleep, workflow-timeout }
```

- **Storage**: due records reside in the **SQLite of their respective owning cell** (time index/time bucket); **no bucket timer index is created**;
- **Deduplication**: `token = hash(scope, kind, dueAt, occurrence)`; the cell SQLite records `fired:<token>` (TTL); **already-fired entries are skipped** (ADR-033).

## Triggering: Two Layers

| Layer | Who | Coverage | bucket |
|---|---|---|---|
| **Local due** | Each `cell-agent` maintains a local earliest-due structure for the timer cells it owns | Entries whose owner is alive | 0 |
| **Single fleet waker** | One leader elected by the bucket lease | Entries whose owner is dead/unreachable | Checks the owner record at takeover time |

- **Dispatch addressing**: send to the **logical service name** of `user-runtime` (mesh), any healthy replica; **no node table is required**;
- **Prerequisite**: the cell containing timers to be triggered must be **resident/pinned**.

## Semantics by Type

| kind | Semantics | Deduplication |
|---|---|---|
| **cron** | Minute-aligned, **best-effort**, **no backfill** | Single execution by unique key per slot |
| **queue-delay / retry** | At least once; delay/retry/DLQ | Claim/ack in the queue cell |
| **workflow-sleep / timeout** | Absolute time; at least once | By run token/step state |
| **do-alarm** | See below | Governed by the actor SQLite alarm row |

## Queue Dispatch

1. The producer writes messages to the queue cell;
2. The cell-agent's local due loop fetches batches according to the consumer configuration (`max_batch_size`, etc.) and calls `user-runtime.queue()`;
3. Result → ack / retry / DLQ; delayed messages enter the delayed index;
4. Duplicate dispatch is suppressed by the queue cell's claim.

## Cron Dispatch

> **Status (2026-09-15)**: **The full path has been implemented** — scheduler (ADR-076: `internal/cron`, 5-field UTC, materializes slots as `KindCron` timers, `CELLHIVE_CRON_INTERVAL` defaults to 30s) → dispatch (ADR-070: `Projection.CronTargets()` → `cronEnricher` fills worker/bundle → user-runtime `POST /v1/timers/dispatch` → `scheduled(event, env, ctx)`, real workerd e2e). Scope convention is `<ns>/__cron__/<worker>`, occurrence=slot, token deduplication, best-effort with no backfill, runnable on every node.

- The cron configuration is stored in the `__cron__` cell; the owner's cell-agent computes the next trigger time;
- A unique key per minute slot guarantees **single execution per slot**; missed slots are not backfilled.

## DO alarm

> **Status (2026-09-15)**: **The platform shim has been implemented** (ADR-079) — native facet alarm is unavailable, so it uses the `cellhive-do.js` shim instead (set/get/deleteAlarm write the reserved key in object storage) + host reports to cell-agent + unified `KindDOAlarm` timer + `DoAlarmDispatcher` → do-runtime `kind:"alarm"` invokes `alarm()`. The following original design (supervisor read-only access to `_cf_ALARM`) is retained for comparison.

- The alarm row is in actor SQLite (persisted with replication);
- **supervisor reads only the workerd alarm table** to extract due → reports to cell-agent (`alarm_upsert/delete`);
- cell-agent creates the due index in the **waker cell**;
- For items where "owner is dead + due", waker performs **targeted takeover** to an available do-runtime, and the new owner reads the alarm and dispatches it;
- **DOs with alarms are prioritized for residency**.

## Why There Is No Separate scheduler Service

Timing/dispatch is unified in `cell-agent` (ADR-009): it is consistent with cell ownership (the owner dispatches its own cells), avoiding a central dispatch point and global scans; waker only covers dead owners.

## Finalized (ADR-152)

**Queue (Consumers)**
- Polling `CELLHIVE_QUEUE_INTERVAL` (default 1s, 0=off); single batch size `CELLHIVE_QUEUE_BATCH` (0=store default); visibility lease `CELLHIVE_QUEUE_LEASE` (0=store default).
- Failed redelivery delay `CELLHIVE_QUEUE_RETRY_DELAY` (default **30s**, batch-level Retry); retry limit and DLQ (`dead_letter_queue`) are determined by **per-consumer configuration** (`max_retries`/`dead_letter_queue`/`max_batch_size`/`max_batch_timeout_seconds`/`max_concurrency`, ADR-072/112), and over-limit messages go to the DLQ.
- Semantics: at-least-once (claim→dispatch→ack/retry; if the whole batch fails, the whole batch is redelivered), only the owner consumes (`OwnerGate`), and every store mutation goes through capture proof (ADR-119).

**Timer (Dispatch)**
- Polling `CELLHIVE_TIMER_INTERVAL` (default 1s, 0=off); per-pass due limit `CELLHIVE_TIMER_BATCH` (default **256**); fired marker retention `CELLHIVE_TIMER_FIRED_TTL` (default **24h**, prevents duplicate dispatch).
- Dispatch is at-least-once: mark as fired only after success; crashes/failures are left for the next round.

**Waker (Single fleet wake + dead-node recovery)**
- Polling `CELLHIVE_WAKER_INTERVAL` (default 5s, 0=off); lease TTL = **2×interval**; per-pass `CELLHIVE_WAKER_BATCH` (default 256); fired TTL `CELLHIVE_WAKER_FIRED_TTL` (default 24h).
- **Backoff**: on consecutive errors, the loop delay grows by `interval·2^fails`, capped by `CELLHIVE_WAKER_BACKOFF_MAX` (default **1m**); it resets immediately after success. Idleness (0 dispatches) does not trigger backoff.

**resident/pinned Policy for timer cells (Decision)**
- **No pinning**: a timer cell is a **normal evictable cell**, managed by the unified `CELLHIVE_MAX_RESIDENT_CELLS`/`CELLHIVE_CELL_IDLE`/disk budget; correctness does not depend on residency — due items exist in the cell (authoritative) and in the bucket `wake/` index, and waker uses the `wake` index to locate by scope (without listing everything), reopening on demand when necessary.
- Capacity: follows `CELLHIVE_CELLS_PER_NODE` and the disk budget; there is no separate timer quota.

_Last updated: 2026-09-17_