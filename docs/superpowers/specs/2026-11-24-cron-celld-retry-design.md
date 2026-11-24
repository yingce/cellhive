# Cron failure retry parity with celld

Approved scope: user explicitly requests celld semantics, including losing pending retries on eviction, restart and owner transfer. Changes remain local; no commit or deployment.

- Scheduled handler failure uses 4/8/16/32/64 second backoffs, then stops on the sixth failure. noRetry consumes the occurrence.
- Store original timer/expression, failure count and next-occurrence cutoff only in the resident cell's memory. Reopening a timer wrapper must retain them; closing/invalidating the cell must not.
- Persist only a retry wake marker/deadline through the existing durability gate and wake outbox. No failed expression or failure count is recoverable from it. An orphan marker is consumed without dispatch. Legacy durable retry rows are also consumed, not replayed.
- The next occurrence across all expressions for the worker wins ties and cancels old retries. Only failed expressions retry; scheduledTime remains the original time. Delayed polling must not replay a retry past its cutoff.
- Retain SQLite/replication fencing, existing normal-slot materialization, noRetry and non-Cron timer behavior. This scoped change does not redesign downtime backfill or normal Cron scheduling.
- Remove the configurable Cron retry budget from production: the celld ceiling is fixed at six. Old environment settings no longer affect Cron retries.

Validation: exact backoff/ceiling, cutoff ties and overdue polling, multiple expressions, successful/noRetry settlement, wrapper reopen vs resident-cell replacement, legacy orphan cleanup, durability gate failures, wake deadline and race tests.

Status: implemented locally and validated; see the implementation plan for evidence and limits. Not deployed.
