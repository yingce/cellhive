# Resource operations and invocation acceptance — 2026-10-15

**Status: final acceptance passed.** Independent review's race/result-correlation
findings were fixed and re-reviewed; see
[closed follow-ups](../superpowers/plans/2026-10-15-resource-review-followups.md).
The full Go suite, related race suite, JS/runtime, vet/build and real Docker
startup/deployment acceptance were rerun after the fixes.

## Prompt-to-artifact completion audit

| Explicit requirement | Implementation and verified evidence |
|---|---|
| namespace/Queue DLQ preview; ID/content/time/attempts | `Queue.PeekPage`, `queue_admin.go`, `queue_dlq_test.go`; actual Docker poison-body/timestamp/source-attempt assertions |
| no claim/attempt/visibility mutation | preview test compares full message records, lease/visibility/attempts and SQLite txid; Docker repeated response equality |
| selected idempotent acknowledge; pagination | `AckMany`, namespace/DLQ keyset cursor, unselected delayed/leased records preserved; repeated missing-ID ack unit + Docker |
| immediate revoke access/consumption | control tombstone + durable replica resource fence + generation-bound admissions; claimed predecessor batch rejected before handler starts |
| final data release; retry/status | owner-routed SQLite/WAL/wake/LTX cleanup, fault bucket retry test, actual Docker file/payload absence; persistent postcompletion retired-epoch GC |
| delayed and source-specific DLQ cleanup, independent resource retained | source Queue generation/intent table + historical versions; D1/D2/D3 regression; independent body/resource remains; purge registration test |
| stable invocation ID and Queue/Cron result/error/console | durable UUID attempts, tagged bounded result, error/stack, ack/retry detail; concurrent native workerd test and Docker query by returned ID |
| retry association across changed batch membership | invocation_messages + per-message logical IDs/attempts and predecessor IDs; real workerd [a,b]→[a,c] partial retry |
| namespace/Worker/time/ID query | `invocations.go`, namespace JWT/filter tests; Docker list and ID-only query |
| developed/tested/real E2E startup | actual image build, isolated Compose start, image CLI TypeScript deploy; final `RUNTIME-BASELINE-E2E: PASS` |
| docs and Cloudflare distinction | ADR-205, runbooks, known-issues, spec and this acceptance report; these are CellHive admin extensions |
| preserve unrelated work | pre-existing `docs/en/decisions.md` untouched; no commit/push performed |

Review follow-up tests additionally include local marker restart/unowned/newer
owner and stale-generation observations, transactional alias/update exclusion,
retired payload arriving after job completion, actual namespace data purger,
receiver validation after delete/recreate and historical/transfer-only DLQs.
Independent review session `ses_ec16b092cffePloruA1Dp6JSLc` cleared all remaining
concrete blockers before final acceptance reruns.

## Delivered

1. DLQ keyset preview without Claim/lease/visibility/attempt mutations; selected
   atomic idempotent acknowledgements, owner routing/capture and namespace ACLs.
2. Durable resource cleanup jobs with resolved physical KV/Queue identity,
   deletion fences, status/retry/backoff/leases, namespace-safe bounded owner
   cleanup, source-specific DLQ message removal and independent DLQ retention.
   Purge retains registration. Recreate waits for completion; late old-epoch
   writes/compaction/recovery are fenced.
3. Durable Queue/Cron attempt UUID, logical retry key, start/end/outcome/error/
   stack/console query. AsyncLocalStorage isolates overlapping handlers and
   tracks ctx and importable waitUntil logs. No platform credentials enter
   tenant env; production user-runtime enables invocation persistence.

These are CellHive control-plane extensions, not an assertion of identical
Cloudflare management endpoints.

## Unit/integration evidence

- `TestDLQPreviewIsReadOnlyAndAckIsIdempotent`: pages visible, delayed and leased
  messages, compares all pre/post fields and SQLite txid, confirms only selected
  IDs and repeats acknowledgement safely.
- `TestResourceCleanupSnapshotRetryAndStaleLease`: resolved KV scope, repeat
  delete identity, lease exclusion, failure/backoff/manual retry, stale job
  fencing and recreation.
- `TestKVResourceDeleteReleasesLocalAndAuthoritativeData`: actual SQLite and
  replica objects disappear; generation fence remains and reactivation works.
- `TestQueueDeletionCleansOwnDataAndSourceDeadLettersOnly`: pending/delayed
  source data removed, independent DLQ resource/messages preserved, recreated
  Queue can send, purge retains registration, deleted DLQ cannot be reopened
  by preview/ack/replay/status paths.
- `TestResourceCleanupRunsAtActualDataOwner`: two real HTTP servers, shared
  coordination bucket, root job routes to the remote data owner and deletes
  its actual SQLite file.
- `TestResourceCleanupFailureCanBeQueriedAndRetried`: injected bucket Delete
  failure is visible in status; manual retry completes cleanup.
- `TestResourceFenceRejectsLateUploadsAndSurvivesReopen`: fence persists across
  manager reconstruction; old epoch rejected, new incarnation can write.
- `TestRecoverySkipsDeletedResourcesButRecoversLiveScopes`: sealed and legacy
  follower recovery do not resurrect deleted payloads or block live cells.
- `TestDeletedResourceUploadSpoolIsDiscarded`: a permanently fenced async upload
  is removed from the real spool rather than retained/retried forever.
- `TestInvocationQueryFiltersAndNamespaceAuthorization`: Worker/kind/time/ID
  filters and JWT namespace isolation return the associated result and logs.
- `TestInvocationRetryIdentityAndHistoryBounds`: monotonic/idempotent attempt
  identity, old record pruning and 10,000-record quota.
- `TestQueueCronInvocationLogsAreDurableAndIsolated`: real pinned workerd,
  concurrent Queue/Cron success/failure/retry, exception stack and asynchronous
  console lines including both ctx.waitUntil and importable waitUntil; records
  remain queryable after recreating the Go store. Also verifies partial retries
  across changed batch membership, returned result and ack/retry delays.

## Real Docker startup/deploy acceptance

Command: `bash scripts/runtime-baseline-e2e.sh`.

Built image with stock `workerd 2026-09-16` and `esbuild 0.28.2`; started an
isolated Compose cell-agent + user-runtime + gated do-runtime stack, deployed
actual TypeScript using the in-image CLI, and exercised live HTTP paths.

- Poison message reaches DLQ; two preview results are identical, body/source
  attempt/timestamp/lease are asserted; selected ack is repeated and no
  acknowledged message remains. A new poison message still replays correctly.
- Queue invocation deliberately throws on first attempt, retries successfully;
  stored failure/success share logical_id and have different attempt IDs.
- Cron success and noRetry failure are separately visible with console lines.
- An invocation ID obtained from the result list is queried alone and returns
  exactly its failure and associated console output.
- KV and Queue receive real payloads. Delete jobs reach complete and shell
  assertions verify both SQLite files and authoritative LTX payloads are absent.
  Independent DLQ content/resource remains, late writes to deleted Queue get 410.
- Existing KV TTL, sendBatch metadata/idempotency, assets, Queue/DLQ, Workflow,
  alarm retry/rearm, gated DO/WebSocket restart persistence and release journeys
  remain passing.

Final output: `RUNTIME-BASELINE-E2E: PASS`.

The first operations run failed with 401 because the test used a Cloudflare
Bearer header on a native admin endpoint; the harness was corrected to use
`x-cellhive-admin-token`. That failed run is not counted as a pass.

## Gates

SQLite build tags:
`sqlite_fts5 sqlite_dbstat sqlite_math_functions sqlite_column_metadata sqlite_preupdate_hook`.

Passed:

```text
go test -tags '<SQLite tags>' ./... -count=1 -p 1
go test -race -tags '<SQLite tags>' ./internal/control ./internal/invocation ./internal/queue ./internal/replica ./internal/upload ./internal/recovery ./internal/server -count=1 -p 1
make js-test
go vet -tags '<SQLite tags>' ./...
go build -tags '<SQLite tags>' ./...
bash scripts/runtime-baseline-e2e.sh
git diff --check
```

The original CLI test treated repeated deletion of an already deleted resource
as unknown; it was updated to distinguish idempotent deletion with a retained
job from deletion of a never-registered resource. The full suite then passed.

Final race command additionally included `./internal/cellstore` for persisted
generation admission. A parallel full/race run once hit the known workerd
post-PASS `WaitDelay expired before I/O complete`; that run is not counted as a
pass. The final sequential `go test ./...` → `make js-test` → vet/build chain
completed successfully on the final implementation.

## Boundaries

- Preview is live keyset pagination, not a frozen queue snapshot; ack cannot
  cancel an already executing handler.
- Un-attributed legacy DLQ messages require operator resolution; shared physical
  resource aliases are rejected rather than destructively deleting shared data.
- Minimal generation/fence metadata remains; historical follower session data
  follows the existing sealed-session reclamation lifecycle.
- Unfinished crashed invocations retain running state; history/truncation limits
  are explicit. Native Tail disable is separate from invocation persistence.
- Acceptance is local Docker and pinned workerd, plus real two-server owner
  routing; it is not a cloud/TKE multi-node deployment claim.
