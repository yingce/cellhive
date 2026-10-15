# Resource operations and Queue/Cron invocation observability

Implementation contract (2026-10-15): CellHive control-plane extensions, not a
claim that Cloudflare exposes the identical APIs.

Implementation and final acceptance verified, including
[closed review follow-ups](../plans/2026-10-15-resource-review-followups.md).
See [acceptance evidence](../../archive/resource-operations-invocations-2026-10-15.md),
ADR-205 and [runbook](../../operations.md).

## DLQ operations

- Preview resolves a main queue's configured independent DLQ and uses SELECT
  with keyset pagination, returning id/body/type/metadata/timestamps/attempts.
  No Claim, lease, visibility or attempts changes; initialized reads do not
  advance the cell txid. Cursor belongs to namespace and DLQ.
- Selected ack is idempotent, captured and routed to the DLQ cell owner;
  authorization is checked before forwarding/reading. Acknowledging cannot
  cancel an already executing consumer but removes future delivery.
- Preview/ack may also address an independent DLQ queue resource directly.

## Resource cleanup

- Deletion atomically revokes resource access and enqueues a durable cleanup job
  with the resolved physical scope. Job state includes pending/running/failed/
  complete, attempts, errors and retry eligibility. Missing/repeated delete
  returns the existing job; failed jobs can be manually retried.
- New data requests and consumer claims check revocation authoritatively;
  cached bindings or delegated credentials must not reopen revoked data.
- Cleanup runs at the physical scope owner, drains in-flight requests, stops
  capture/upload, removes local data and authoritative objects/wake state using
  bounded administrative prefix traversal. Cleanup cannot be declared complete
  merely because a local cache file was deleted.
- A deleting resource/scope cannot be re-created until cleanup is complete;
  stale jobs must not delete a new incarnation. Shared physical scopes must not
  be removed while another live resource still owns them.
- Deleting Q removes Q's own pending/delayed/DLQ-role messages and dedup data,
  plus source-attributed dead letters from Q in its configured DLQs. An
  independent DLQ D remains a resource; other source/independent messages remain.
  Old un-attributed dead letters report failed cleanup for operator resolution.
  Purge retains Q and its registration, clearing only messages.

## Invocation records

- Each Queue batch attempt/Cron attempt has an immutable invocation ID, with
  a separate stable logical/retry key. Record namespace/worker/type, start/end,
  status, attempt, error and console logs in cell SQLite. Namespace-scoped,
  paginated queries filter worker/time/invocation ID.
- Console capture is invocation-local, including overlapping async handlers;
  no module-global mutable current-ID. Result and logs are delivered through the
  trusted runtime host, not tenant-controlled generic telemetry endpoints.
- Log count/bytes and retention are bounded and truncation is explicit. Runtime
  crashes leave a visible started/unknown attempt instead of fabricating success.

## Implemented APIs and limits

- `GET /v1/queue/dead-letters`, `POST /v1/queue/dead-letters/ack`;
  `namespace` + main `queue`, or `direct=1` + independent DLQ name.
- `GET /v1/control/resource/cleanup`, `POST /v1/control/resource/cleanup/retry`;
  `namespace/kind/name` required. Retry/backoff/lease state is durable.
- `GET /v1/control/invocations`; namespace required, optional worker/kind/
  from_ms/to_ms/invocation_id, limit <=100, exclusive cursor. Result/error/stack
  and console logs are returned together.
- History is capped at seven days/10,000 attempts per namespace by bounded
  write-time pruning. Console output is capped at 256 entries/8192 characters;
  record bytes <=128 KiB, truncation is explicit. ctx/global importable
  waitUntil tasks are included before finalizing the attempt.
- Queue cleanup preserves source queue/id/timestamp/attempts when moving into a
  DLQ. Missing historic provenance fails rather than removing unrelated data.
- Replica payload cleanup retains small owner generation/fence metadata. Late
  uploads and recovery for fenced epochs cannot resurrect data. Async upload
  spool payloads are discarded for permanently deleted epochs; historical
  follower session logs follow existing sealed-session reclamation.
- Incarnation markers survive local eviction and are checked under a transition
  lock with authoritative generation-bound request admission. Claimed batches
  carry their consumed Queue generation separately from original source identity.
- Source-generation transfer intents are committed before receiving data; the
  destination validates provenance inside its own message transaction. Deletion
  snapshots all historical and transfer-only destinations. Retirement GC survives
  namespace purge and removes late old-epoch uploads during successor lifetimes.
- Results use bounded tagged JSON (8 KiB) with explicit encoding/truncation;
  Queue ack/retry IDs and delays, message lineage and predecessor invocation IDs
  survive partial retries even if subsequent batches have different membership.
- Shared registered physical scopes reject destructive deletion. All writer,
  consumer and recovery nodes must be upgraded before opening deletion APIs.

## Acceptance

- Read-only peek proves unchanged attempts/lease/txid across pagination;
  repeat selected ack is safe and does not affect unselected messages.
- Delete revokes reads/writes/consumption immediately; real SQLite and bucket
  data eventually disappear. Failure injection, restart and retry are covered.
  Independent DLQ data remains, deleting the DLQ itself removes its own data.
- Real pinned workerd and Docker startup/deploy paths execute Queue and Cron
  success/failure/retry with console output; query any invocation ID to verify
  result/error/log association and filters, including concurrent invocations.
