# Cron celld retry implementation plan

Goal: implement the approved Cron retry contract in the adjacent spec.

1. Add resident-cell ephemeral storage and a Cron retry state machine in internal/timer. Keep durable markers distinct from runnable occurrences; preserve commit and wake outbox boundaries.
2. Wire next-occurrence lookup across a worker's expressions through the production timer-store factory. Remove the configurable production retry limit.
3. Add lifecycle, retry timing, cutoff, legacy-marker and commit-failure tests; adjust integration assertions to fixed celld semantics.
4. Update decisions/known issues and run tagged Go tests for timer, cron, dispatch, config, waker and cell-agent plus race checks for the state machine. Inspect final diff; no commit/push/deploy.

## Validation completed

- Tagged `go build ./...`: passed.
- Tagged race tests for internal/timer, internal/cron, internal/dispatch, internal/config, internal/waker and cmd/cell-agent: passed.
- After final retry diagnostics/row-existence checks, focused race tests for timer and cellstore close/reopen paths: passed.
- Covers backoff/ceiling, same-token rematerialization, resident-wrapper reuse, database reopen losing pending retries, legacy cleanup, cutoff ties and delayed polls, original scheduledTime, slow failure completion timing, concurrent passes, failed-expression-only retry, and wake outbox atomicity.
- Helm lint and diff whitespace checks: passed. No production deployment or full runtime E2E performed.
- Existing untracked local cold-epoch report was preserved. No commit/push/deploy.
