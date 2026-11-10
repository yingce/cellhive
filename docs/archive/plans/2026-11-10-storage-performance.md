> 历史实施记录，已归档。进度、待办和运行环境描述只代表当时状态；当前契约及验收以 [known-issues](../../known-issues.md)、[decisions](../../decisions.md) 和对应报告为准。

# Storage and request performance implementation plan

> Execute task-by-task with verification checkpoints. User authorized all seven items; no further scope approvals, commits or deployments.

**Goal:** Reduce avoidable storage and request work while preserving native LTX, RPO=0, exact epoch continuation and authorization boundaries.

**Architecture:** Keep native page versions and immutable objects. Optimize scheduling, capture boundaries and range discovery rather than introducing new metadata files. Measure individual stages and validate failures as well as successful paths.

**Tech Stack:** Go, SQLite/cgo, workerd C++ fork, COS multipart, local fleet E2E.

## Global constraints

- CellHive edits only `/opt/cellhive-worktrees/celld-ltx-migration`; fork edits only `/opt/cellhive-workerd-fork`.
- CPU affinity 0–3; build concurrency at most4. Serialize real-COS suites with existing bucket lock.
- Bucket `cellhive-e2e-hk-1376795072`; credentials remain in `/opt/.secrets/s3`, never in artifacts.
- One native LTX format; no new remote markers or legacy object-format compatibility.
- Preserve pinned auth TTL and resource deletion/recreation fencing. No server JWT cache.
- Initial worktree/fork status and source hashes recorded under `docs/reports/artifacts/2026-11-10-optimization/`.

## 1. Performance baseline and idle-gap diagnosis

Files: `scripts/e2e/kv-performance.cjs`, `scripts/e2e/kv-performance.sh`, new `scripts/e2e/kv-idle-performance.cjs`, `internal/owner/route.go`, request trace sites under `internal/server/`.

- [x] Record exact old/new source/build identities, runtime, affinity, endpoints, durability, payload, concurrency and client signing behavior. Retain previous accepted binaries before replacing any output.
- [x] Add same-key direct/forwarded/worker measurements after gaps `[0,1,5,10,30]` seconds, with traces for each measured request and startup/warm phases separated. No mutation retries in throughput measurements.
- [x] Attribute auth, route/owner, activation, connection, SQLite and fleet time using trace spans and metrics, not guessed TTLs.
- [x] Reproduce a suspected expensive refresh in a focused counting-provider regression before changing it; preserve stale owner, deletion and expired authorization negative cases.
- [x] Run interleaved same-environment baseline/candidate rounds; report variance, errors and request counts as well as RPS.

## 2. Bounded concurrent multipart

Files: `internal/bucket/cos_multipart.go`, `internal/bucket/cos_multipart_test.go`, snapshot and streaming HTTP stage timing sites.

- [x] Regression: fake COS blocks part1 until part2 is received; serial implementation must fail, concurrent implementation must finish with byte-identical ordered content.
- [x] Keep `multipartPartBytes(size) = max(32MiB, ceil(size/10000))`. Use4 bounded in-flight part buffers; sequential source reader overlaps uploads without reading source concurrently. Record completion slots by part number.
- [x] First read/upload failure cancels outstanding uploads; wait for every worker before abort. Never Complete on short/long source, canceled context or missing part ETag. Preserve no-overwrite completion and caller-owned spool.
- [x] Verify ordered completion under out-of-order responses, concurrency/buffer bound, cancellation, source mismatch, abort ordering and existing conflict tests; run race checks.
- [x] Add separate encode/spool, HTTP transport/provider upload timings. Compare real COS fixed data/part size before and after; include memory and10GiB correctness.

## 3. DO checkpoint continuation

Files: `internal/dosupervisor/dosupervisor.go`, `internal/dosupervisor/paging.go`, `internal/wal/wal.go`; fork `src/workerd/util/sqlite.c++`, `sqlite-paging.h`, native tests.

- [x] Trace real checkpoint/WAL reset/main-file mutation ordering and capture gates; document the pre-reset durable boundary before implementing.
- [x] Regressions: checkpoint with uncaptured commits, reset plus immediate new commit, crash before/after ACK, restart and overwrite/truncate/regrow. Compare every acknowledged value after independent restore.
- [x] Coordinate durable capture before WAL history becomes unavailable; retain full snapshot fallback only when continuity cannot be proven. Do not merely clear checkpointPending or disable checkpoint.
- [x] Assert ordinary proven checkpoint does not invoke full hydration or snapshot upload on a large paged DB; test negative/unproven cases fail closed or safely rebaseline.

## 4. Fork review closure

Source review: `/tmp/cellhive-workerd-fork/sqlite-paging-review.md`.

- [x] Map each numbered review item to current fork and supervisor behavior. Explicitly document nonissues: supervisor rejects unaligned truncation and poisons deterministic hydration failures.
- [x] Protect SQLite callback exception boundaries; move network negotiation outside global registry lock without creating duplicate independent states for one file.
- [x] Enforce overall request budget, preserve per-file mutation serialization, mark successfully written hydrated pages locally, validate metadata against legitimate file growth/truncation semantics.
- [x] Audit/intercept file-control growth/mmap bypass, add explicit includes, zero-length handling, registry lifecycle and capability semantics.
- [x] Run native VFS tests plus real SQLite WAL/checkpoint integration; four-core rebuild once related C++ changes are integrated, then filesystem and COS native DO tests.

## 5. Cold restore/epoch scan seek

Files: `internal/replica/epoch_chain.go`, `internal/replica/replica.go`, `internal/replica/levels.go`, restore and listing tests.

- [x] Count provider List calls/keys for100000 retained covered L0 files and a short uncovered tail; establish failing optimization regression with correct restored contents.
- [x] Discover verified high-level coverage first; lower-level seeks must retain crossing intervals and detect gaps. Preserve exact predecessor cut and ignore late fenced intermediate epochs.
- [x] Exercise snapshotless successor epoch, stale/overlapping compactions, pagination, concurrent publication, missing predecessor and provider fallback. Compare selected pages/target TXID with exhaustive planner.
- [x] Real COS object inventory test records request reduction; no new marker/index format.

## 6. Integrated acceptance and report

- [x] Full tagged Go test/build/vet and relevant race tests; native SQLite tests and paging capability check.
- [x] Real COS functional, fleet crash/owner/epoch, export/import, legacy plain SQLite migration and100k confirmed-value checks.
- [x] Actual10GiB native DO: upload stage times, first page/query, checkpoint continuation, changed-page recapture, independent full restore and all BLOB hashes.
- [x] Report old/new RPS distributions, idle-gap breakdown, upload wall time/throughput/RSS, list counts and failure-recovery proofs. Do not claim completion from short benchmark success alone.

## Status

Implementation, source review, tagged Go/native tests and actual10GiB new-binary COS acceptance are complete. Final queue/workflow/batch/100k epoch suites passed. Refreshed multipart throughput/hash/RSS comparison and all four final-binary ABBA RPS rounds passed. A30s worker-PUT follow-up did not reproduce the short-window5.45% drop (mean−0.37%,0failures). Final report and evidence audit are complete; no commit or deployment. Completed boxes refer to evidence in the current report and its retained artifacts, not deployment acceptance.

Cold scan scope is deliberately exact: historical predecessor with a verified upper cut skips covered lower levels (real100002-object COS inventory:2listing pages/2keys,3prefix calls). An unconstrained latest epoch may contain arbitrarily crossing intervals and still requires exhaustive discovery. The crossing-interval oracle rejects unsafe skipping; no marker/index or restrictive new LTX contract was introduced.

Full Go test/vet/build identities: `lifecycle-reviewed-{source,binaries}.json`; latest test-only managed lease fixture: `lifecycle-managed-source.json`. Native46/46 PASS. Actual10GiB733.371sPASS: all163840 hashes/counters, checkpoint continuation with no new snapshots, four zero-hydration retirements and both lifecycle scheduling regressions; see `native-lifecycle-cos-10gib-managed-acceptance.log`. The earlier unmanaged-fixture lease failure is preserved separately.

### Sequential materialization request budget

The actual10GiB independent restore exposes a1MiB encoded run cap shared with random paging. Sequential Materialize will use an8MiB encoded budget while retaining the8MiB decoded cap. Public ReadRun retains its1MiB encoded cap. This changes neither native LTX nor newest-page ownership, ETag/frame validation or sparse range boundaries. At most one bounded encoded and decoded run is retained per materialization. Validate using incompressible data and request counts plus byte equality, then realCOS; existing running10GiB baseline remains untouched.
