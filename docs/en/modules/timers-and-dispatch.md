# Module: timers and dispatch

Unified timer abstraction: cron, queue delay/retry, DO alarm, workflow sleep/timeout, KV expiration; each owner dispatches its own cell, with a single fleet waker covering items whose "owner is dead/ownerless".

> The authoritative source for configuration is [`../configuration.md`](../configuration.md) and the code; the table below is the subset relevant to this module.
## Key Interfaces

Timer rows are stored in the owning cell SQLite; `POST /v1/internal/kv/expire`, `POST /v1/internal/do/alarm/upsert`, `POST /v1/timers/dispatch` (to user-runtime).

## Configuration (Environment Variables)

| Variable | Default | Purpose |
|---|---|---|
| `CELLHIVE_CRON_INTERVAL` | `30s` (0 off) | Materialize cron→timer |
| `CELLHIVE_DRAIN_TTL` | `30s` | Drain token validity period (shutdown wait = same value) |
| `CELLHIVE_QUEUE_BATCH` / `_LEASE` | `0` (store default) | Messages per batch / visibility lease |
| `CELLHIVE_QUEUE_INTERVAL` | `1s` (0 off) | Queue consumption |
| `CELLHIVE_QUEUE_RETRY_DELAY` | `30s` | Redelivery delay for failed batches (retry limit/DLQ are configured per-consumer) |
| `CELLHIVE_TIMER_BATCH` / `_FIRED_TTL` | `256` / `24h` | Due limit per pass / fired marker retention |
| `CELLHIVE_TIMER_INTERVAL` | `1s` (0 off) | Due timer dispatch |
| `CELLHIVE_WAKER_BACKOFF_MAX` | `1m` | Backoff cap on consecutive errors (base=interval, `interval·2^fails`) |
| `CELLHIVE_WAKER_BATCH` / `_FIRED_TTL` | `256` / `24h` | Limit per pass / fired marker retention |
| `CELLHIVE_WAKER_INTERVAL` | `5s` (0 off) | Fleet waker (TTL is automatically 2×) |
| `CELLHIVE_WAKE_REPAIR_BATCH` | `256` | Number of local cells checked per pass (rotating window, covers all in a few passes) |
| `CELLHIVE_WAKE_REPAIR_INTERVAL` | `5m` (0 off) | Local wake index repair scan (ADR-177); runs once at startup |


> Parsing rules (strings/booleans/durations/bytes/lists) are in [`../configuration.md`](../configuration.md#parsing-rules).

## Key Invariants

- Due records are in cell SQLite (**authoritative**); bucket `wake/` is only a **discovery index**.
- **Index must not lag behind timer** (publish before commit; fail-closed on failure).
- Dispatch is at-least-once; mark fired only after success (TTL deduplication).
- Cron is minute-aligned, best-effort, and does not backfill missed runs.

## Source Locations

`internal/{timer,dispatch,cron,waker,wake,queue}`; `cmd/cell-agent/main.go` (runner/waker loops).

## Test Anchors

`internal/timer`, `internal/dispatch`, `internal/cron`, `internal/wake`.

## Related Documents

[`timers-and-dispatch.md`](../timers-and-dispatch.md)

_Last updated: 2026-09-19_