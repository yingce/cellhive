# ADR-205 review follow-ups (must finish before Goal completion)

**Closed after implementation, regression tests and independent re-review.**
The sections below preserve the original required fix descriptions. Completed:
C1 stale job re-block prevention; I1 durable marker + authoritative locked
admission; I2 transactional scope/alias snapshot; I3 persistent epoch-bounded GC
and postpublication cleanup, including actual namespace purge; I4/I5 receiver
source-generation validation + durable/historical destination discovery; I6
per-message retry lineage; I7 tagged result and detailed outcomes. Final review
also caught and cleared consumed Queue `ClaimGeneration` dispatch admission.
Final full Go/race/JS/vet/build and actual Docker E2E reruns passed; see
`docs/archive/resource-operations-invocations-2026-10-15.md`.

Review session: `ses_ec16b092cffePloruA1Dp6JSLc` (read-only review). Baseline
Docker/full Go/race/js-test/vet/build passed, but review found untested deletion
and retry-correlation cases. These are required work, not deferred polish.

## C1 — stale cleanup / recreation

Initial correction made: `BlockResource` now permanently rejects re-blocking an
already allowed **same deletion job ID**, `revokeResource` does not re-block
completed jobs, the cleanup pass validates its lease before destructive work,
and purge only deletes replica epochs <= the deletion floor. A regression first
observed `BlockResource(oldJob)` succeeding after `AllowResource(oldJob)`.

Finish deterministic stale-execution tests and re-review. Current fence job ID
is also a useful incarnation tag: empty for never-deleted legacy resources,
otherwise the most recent completed/allowed deletion ID. Source messages must
capture that tag before deletion changes it to the next job.

## I1 — offline local lineage (pending)

Replace current-owner heuristics with a persisted local resource-incarnation
marker. Add `cellstore.Store.EnsureResourceGeneration(ctx, scope, generation)`
called outside BeginRequest by PrepareResourceAccess, consumer Filter and timer
opening. A missing marker means legacy generation `""`; a changed generation
must drain/forget old SQLite/WAL and durably publish the new marker. Preserve
this marker during ordinary `Forget`/eviction so hydration cannot relabel an old
file. Namespace directory deletion removes it naturally. A new Store reads the
marker instead of trusting its current owner cache. Use a bounded per-scope
cache and serialize marker changes, then test unowned and newer-owner cases.

## I2 — transactional resource snapshot / shared-scope checks (pending)

DeleteResource currently resolves scope, checks aliases and snapshots DLQs
before its transaction. Move registration lookup, reference checks and cleanup
snapshot creation into one transaction. Serialize create/update lifecycle hooks
with delete so a post-commit activation cannot race the next delete. Add
controlled concurrent scope-update/alias-registration tests.

## I3 — cross-node in-flight publications (pending)

Per-manager RWMutex is not a distributed fence. Keep durable retirement rows
per physical scope containing old epoch floor (and lineage identity as needed),
and run bounded, cursor-based, round-robin post-fence GC even after a job is
complete. Only remove epochs <= retired floor, never a recreated resource's new
epoch. Preserve GC targets when a resource name is later pointed elsewhere.
Also validate the fence after Put and conditionally remove a late publication
when the writer is alive, but do not rely solely on that best effort. Test an
old object published after cleanup completion and a concurrent valid new epoch.

## I4 / I5 — source incarnation and all historical destinations (pending)

- Extend Queue message provenance with immutable `source_generation` (the active
  source incarnation tag). Obtain it when a producer enqueues, preserve it
  through DLQ/replay, and include it in transfer identity as appropriate.
- Validate source generation at the receiving destination owner **inside the
  destination Queue write transaction**, which serializes with RemoveSource.
  Sender-only preflight does not protect delayed HTTP transfer execution.
  Reject a predecessor after same-name source recreation.
- Record source-generation -> destination relationships durably *before*
  accepting transfers. The control transaction must reject intents after source
  revocation, so delete snapshots cannot miss a committed/new destination.
  Snapshot all destinations at delete, including historical consumer versions
  for upgrade coverage; keep independent DLQ resources and unrelated messages.
- Route these metadata writes through the control owner and capturedWrite.
  Ensure destination discovery cannot race deletion, and do not erase unrelated
  old DLQ messages when provenance is absent.
- Test Q->D1, redeploy Q->D2, delete Q; delayed transfer after delete/recreate;
  and D-transaction ordering with source removal.

## I6 — Queue regrouping retry lineage (pending)

Batch-membership hashes are not sufficient for partial retries. Persist each
invocation's message membership with stable per-message logical IDs/attempts and
predecessor invocation links. Retain batch logical key if useful, but expose the
message lineage and allow querying it. A retry of A from [A,B] to [A,C] must link
to the previous attempt of A without claiming C or B is the same execution.

## I7 — actual results / detailed outcomes (pending)

Persist bounded handler result data with explicit encoding/truncation and Queue
ack/retry message IDs + retry delays. Aggregate status alone is insufficient.
Use the existing tagged RPC codec for non-plain values where appropriate; never
silently serialize unsupported values as misleading empty data. Query by
invocation ID must return the result/outcomes with its logs and error.

## Finish

Update ADR-205/runbook/acceptance evidence, run focused tests then full Go/race/
js-test/vet/build and the expanded actual Docker startup E2E. Resume the review
session with fixes and evidence. Do not mark the Goal complete or submit these
uncommitted changes while Critical/Important findings remain.
