> 历史实施记录，已归档。进度、待办和运行环境描述只代表当时状态；当前契约及验收以 [known-issues](../../known-issues.md)、[decisions](../../decisions.md) 和对应报告为准。

# COS/fleet acceptance goal

Status: complete; final evidence in `docs/reports/2026-11-09-final-acceptance.md`. Dedicated bucket: cellhive-e2e-hk-1376795072. Primary runtime: official workerd 1.20260916.1. Local multi-process compute with real remote COS; isolate namespace and serialize bucket suites. Preserve existing changes. No deployment or commits.

1. Build current Go binaries; make runner select and record explicit workerd. Run review, queue, multi-node failover and workflow/recovery suites.
2. Run fleet routing-safety, batch-recovery, epoch-stress with acknowledged-write ledger and full cold restore checks. Diagnose/fix failures before marking passed.
3. Exercise SQLite export/import against real COS, including independent restore and content/integrity verification.
4. Run fleet concurrency performance (RPS, p50/p95/p99, errors), official workerd cold activation and 10GiB restore measurements. Ensure disk budget before large fixtures; preserve unrelated caches.
5. Finish fork build and native paging DO test, then COS-backed native DO recovery with changed-page recapture; measure paging separately from full restore.
6. Review implementation, run relevant regressions, publish report with exact topology, runtime versions, commands, counts and artifacts. Mark goal complete only after required cases pass.

Initial evidence: official workerd doruntime/dosupervisor test matrix passed; not counted as real-COS/fleet acceptance. Full tagged Go tests and vet passed at previous checkpoint. Native sqlite tests passed after fixing missing bitmap declarations; opt build ongoing.

## Current checkpoint (2026-11-09, supersedes historical process notes below)

Previous goal turn: progress (bounded sequential prefetch regression/implementation, current Go build, full Go test/vet/race pass). Current native binary is `/opt/cellhive-workerd-fork/bazel-bin/src/workerd/server/workerd`, with `do-paging/1`; four-core build is complete. Environment listeners and COS are available.

Final gates closed:
- Actual native10GiB/all163840 hashes/native object inventory PASS; native proxy fleet and128MiB long-query/child-only-crash fleet PASS.
- Official review/workflow/routing/batch/transfer/100k PASS.100k checked online and cold100000/100000 after3 kills.
- Original80445 stopped on performance bind conflict; retry31094 on26700 EXIT0,36 cases/153960 requests/zero errors;3 actual Wrangler runtime versions verified. Both attempts retained.
- Final full tagged Go test+vet4056 EXIT0 with native paging enabled. Binaries match hashes; report, performance table, evidence manifest and current known issues/decisions synchronized. No commit/deployment.
- Historical continuation notes below remain chronology, not pending work.

All CellHive work remains in `/opt/cellhive-worktrees/celld-ltx-migration`; `/opt/cellhive` was clean at the initial check; a later read-only check showed changes in cmd/user-runtime/main.go and internal/workerdpool/supervisor.go. This run made no writes there and leaves those changes untouched. All commands pinned to CPUs0–3.

## Live execution

- Current Go build complete: /tmp/cos-fleet-acceptance-build.log.
- Runner now accepts --workerd and records resolved path/version.
- Official real-COS review batch running (session83361), log /tmp/cos-fleet-official-review.log; R2/review and queue passed with cleanup; queue-multinode underway.
- Sequential workflow, routing-safety, batch-recovery, epoch-stress (100k default), performance-dual pipeline started (session36751). Each writes /tmp/cos-fleet-official-<suite>.log; stops at first failure. Shared bucket lock enforces serialization.
- Fork opt build session1687 remains running; no new binary acceptance yet.
- General binary SQLite export/import endpoint is absent in current code (existing D1 API is SQL text); must implement and test rather than counting SQL export as that requirement.
- /tmp has16GiB free at start; /opt has9.4GiB. Use /tmp for10GiB fixture and recheck free space before execution.

## Acceptance progress, continuation 1

- Previous goal turn classified progress: built current binaries, enabled explicit official runtime selection, started real COS suites.
- review + queue + queue-multinode PASS, workflow + workflow-recovery PASS, routing-safety PASS. Each unique namespace cleanup HTTP200.
- batch-recovery PASS: fleet acknowledgement, owner SIGKILL,100/100 confirmed values; local-cache removal then100/100 cold COS restore, first GET2.822433s (/tmp/cos-fleet-official-batch-recovery.log).
- Added reusable scripts/e2e/do-cold-cos.py and generalized native cold fixture for official full restore or native paging plus optional real COS. Unique namespaces and bounded cleanup; runtime child environment remains credential-free.
- Official DO + real COS first run PASS6.84s, cold restore+start+point read1.587s, then update+independent third-disk restore. SHA256 enhancement now checks every one of256 random64KiB BLOBs; filesystem version passes2.22s (/tmp/official-do-full-sha-test.log). Real COS enhanced rerun pending lock (session70221, /tmp/official-do-cold-cos-sha.log).
- Sequential pipeline session36751 currently executing100k epoch-stress; live writer ledger at /tmp/cellhive-e2e.KzfZTR/ledger.tsv. Performance-dual follows if stress succeeds.
- 10GiB indexed read/full audit queued under same bucket lock, session56846, /tmp/cos-fleet-10g-paged.log. Actual DO native full fixture still16MiB; do not label storage benchmark native DO10GiB.
- Fork build session1687 remains live (around8932/10704 dynamic actions), then native FS E2E. Native sqlite tests passed. Need COS paged run after new binary.
- SQLite binary export/import design research: existing API only D1 SQL-text export/import; need new admin/CLI integration. Store.ForgetWithVerify drains local requests but does not stop capture until after callback, so publishing imported snapshot in that callback is unsafe. Owner fencing + exclusive activation/capture lifecycle must be handled before adding import; no raw bucket replacement shortcut.

## Acceptance progress, continuation 2

Previous turn classified progress (new real-COS DO fixture/evidence, running suites). Implemented admin/CLI binary SQLite export/import in internal/server/cell_transfer.go and cmd/cellhive/cell_transfer.go. Imported targets must have no local activation/file and no native chain; local admission gate covers final recheck and upload, with owner/session/liveness/epoch and resource fence checks. No capture loop is allowed on import targets. Invalid/truncated SQLite rejected before publication; stream native snapshot encoding,64GiB input cap,2-operation concurrency limit. Export uses consistent resident snapshot or flushes queued uploads before materializing fixed remote cut. CLI output uses sibling temporary plus exclusive link.

Unit tests verify independent replica restore of binary contents, duplicate/active-target rejection, invalid inputs, scope authorization without binary-body consumption and spoofed namespace grants. PASS; race PASS /tmp/cell-transfer-race.log. Broader server+CLI regression session84312, /tmp/cell-transfer-regression.log pending. CLI real-COS suite added to hk-review.py; queued session90955 /tmp/cos-cell-transfer.log using /tmp/cellhive-transfer-bin (separate binaries, preserves running fleet test executables). Last rebuilt agent session35434 includes final fence/budget changes; verify exit status before interpreting suite result.

100k epoch stress still live, all3 forced owner switches occurred; writer progressed past82k.10GiB and SHA-enhanced DO COS still await shared lock. Native build still live ~8997/10704 actions; source changed since initial build means final native unit/E2E needs inspect actual logs. General existing-target replacement is intentionally rejected; migration uses a fresh target.

Latest: broader server+CLI regression passed (48.028s/2.321s), /tmp/cell-transfer-regression.log. After publishing import, re-register pending timers using the existing claim hook; final focused race+agent rebuild session90452, /tmp/cell-transfer-race.log. COS transfer queued session90955 still waits lock.100k writer finished1034.6s with3 forced owner switches, full verification/cold restore ongoing; not yet PASS. New endpoint full imports target fresh cells only, current admin URL must be owner. New scripts syntax and both diff checks clean.

## Acceptance progress, continuation 3

Previous turn progress: admin+CLI transfer implementation and tests. Current turn:100k stress PASS100000/100000 hot/cold;10GiB partial/audit PASS0.808s cold first row/49.709s full audit; SHA-enhanced official DO COS PASS all256 BLOBs. Strengthened transfer harness with post-import cold cache deletion and plain SQLite(no CellHive metadata) migration. Functional case PASS and cleanupHTTP200; verify runner terminal session90955.

CLI now rejects nonempty sibling WAL (requires standalone consistent input), fsyncs output parent after atomic link, and tests truncated downloads never publish partial files. New CLI tests and build PASS session17581; final full Go test session14246 PASS /tmp/cos-fleet-full-regression.log (before tiny CLI-only guard change).

Native build paused with SIGSTOP to isolate performance; PID list /tmp/cellhive-acceptance-paused-build.json (server31604 plus6 descendants). MUST SIGCONT surviving listed processes after performance/full-restore measurement; do not restart build. Original exec session1687 still owns build then native FS E2E. No goal pause requested or set.

Sequential pipeline36751 now performance-dual; log /tmp/cos-fleet-official-performance-dual.log. Additional10GiB FULL materialization queued session reported by exec, /tmp/cos-fleet-10g-full.log, same bucket lock. Need collect it, resume native build, native COS paging, review/full vet and final completion audit.

## Acceptance progress, continuation 4

Confirmed transfer runner terminal PASS, cleanupHTTP200. Updated report table and decisions/known-issues with verified results. Full10GiB session69601 now completed materialization100.043s (1285ranges/761434665bytes), preceding full-chain proof50.661s/index0.841s; all-row/hash final verification still running. Log /tmp/cos-fleet-10g-full.log.

Performance-complete session33047 queued under shared lock; uses latest transfer binaries and installed Wrangler4.133.0, includes workerd descendant RSS/CPU records. Native build remains intentionally stopped until measurement ends. Added automatic resume watcher session34265, /tmp/cellhive-resume-build-after-perf.py and .log: watches terminal performance log then SIGCONT only saved PIDs whose /proc start time still matches. Original build session1687 remains authoritative; watcher avoids leaving build stopped across continuation. No goal completion yet.

Full10GiB session69601 now EXIT0/PASS601.34s: all10240 rows/full image SHA256/integrity checked. Report updated. Performance33047 running, artifacts /tmp/hk-review-1794212497-27e732-9aeu_mnm. Resume watcher34265 active.

## Acceptance progress, continuation 5

Previous turn progress: full10GiB acceptance and report update. Performance33047 terminal PASS36 warm cases/no failures, all6 Wrangler actions(3 rounds), cleanupHTTP200. Report now contains all concurrency1/8/32 RPS/percentiles, workerd+Go RSS/CPU and upload/deploy timings. Resume watcher34265 sent SIGCONT to7 verified original processes; native build1687 live ~9048/10704. Final Go full test/vet session90309 EXIT0 (/tmp/cos-fleet-final-go-test.log, /tmp/cos-fleet-final-go-vet.log; unchanged packages cached).

Added explicit DO acceptance inventory assertion: canonical sparse levels/path + native100-byte header for every cells namespace object, no auxiliary markers. Local official test PASS2.36s,9 objects (/tmp/official-do-object-audit.log); COS enhanced test running /tmp/official-do-cos-object-audit.log. Inventory List is test-only. Native build chain will automatically execute same enhanced test once binary links. Remaining: native FS/COS acceptance, final report completion audit.

COS object audit session26368 EXIT0, PASS5.71s,9 native LTX objects/all256 BLOB hashes; cold1.414s. Native build1687 live server31604, ~9055/10704. No source edits to fork this turn.

## Acceptance progress, continuation 6

Previous turn progress: complete performance report, COS canonical-object audit, final Go checks. Original build1687 re-polled LIVE (server31604); no restart. Native sqlite fastbuild binary (mtime after all3 modified util sources) freshly executed,37/37 PASS exit0, /tmp/do-paging-native-sqlite-rerun.log. Build still compiles V8 ~9070/10704; no native server acceptance yet.

Queued automatic dependent gates: session92524 /tmp/cellhive-native-cos-after-build.py waits for actual native FS test PASS, verifies new binary do-paging/1, then executes COS paged suite to /tmp/native-do-cos-final.log. Its status is /tmp/cellhive-native-cos-after-build.log. A second watcher /tmp/cellhive-native-regression-after-cos.py waits for COS exit0, then runs fresh full doruntime/dosupervisor tests using the new fork with paging enabled, logs /tmp/native-do-full-regression.log and status /tmp/cellhive-native-regression-after-cos.log. Failures stop dependent gates; inspect and fix, do not count queued work as passed.

## Acceptance progress, continuation 7

Previous turn progress:37 native SQLite tests rerun and chained remaining gates. Revalidated original build1687 LIVE, ~9096/10704; both dependent watcher handles92524 and77162 LIVE. No build restart or fork edits. Report stale pending statements corrected, full10GiB table entry added, reproduction commands and SHA256 identity of actual official/latest Go test binaries recorded. Earlier functional-suite binaries are explicitly distinguished from later transfer/performance binaries. Remaining requirement still native server build + FS/COS paged acceptance + full native DO regression; none claimed passed.

## Acceptance progress, continuation 9

Previous turn classified verified wait: original build shell304732/server31604 remained live and advanced, no restart. Current turn closes a fixture coverage gap: native cold scalar/length query did not necessarily fetch overflow BLOB pages. Added /blob point read with actual64KiB SHA256 match against original, before recapture, retaining4MiB hydration cap in paged mode. Official filesystem PASS2.90s session58765 and realCOS PASS6.21s session16923;9 native objects and all256 restored BLOB hashes still pass. Logs /tmp/official-do-cold-blob.log and /tmp/official-do-cos-cold-blob.log. Native build1687 still live~9125/10704; original FS chain and watcher92524/COS then77162/full regression will pick up strengthened fixture.

## Execution environment replacement

Original exec1687 is now UNKNOWN and old Bazel PID31604 is absent after environment/permissions replacement; old log ends9512/11023, no successful build and no native FS/COS/regression logs. Prior watcher sessions cannot be relied upon. New writable roots are /opt/cellhive and /tmp; fork and original Bazel cache read-only. Network restricted: local127.0.0.1 bind returns EPERM, external dependency DNS fails. This is the first goal turn auditing this new environmental restriction; not completion or blocked status yet.

Copied3.6GiB old output base to /tmp/cellhive-native-bazel-resume, rewrote17844 internal symlinks; original untouched. Bazel server fails socket setup; --batch avoids server, --noexperimental_collect_system_network_usage avoids network-metrics startup exception. First dependency fetch failedDNS; restored6 removed external cache entries and retrying --nofetch. New build exec21759 LIVE at last handle poll; log /tmp/do-paging-workerd-resumed-build.log. Dependency content-addressed download cache also being copied to /tmp/cellhive-bazel-download-cache for offline use. New binary path after successful build will be /tmp/cellhive-native-bazel-resume/execroot/_main/bazel-out/k8-opt/bin/src/workerd/server/workerd. Original watchers reference old paths and must not be treated as active acceptance. Remaining native network-based E2E requires a permitted local listener and COS access.

Environment-restriction audit, turn2: local listener still EPERM. Old recovery21759 terminated exit32 trying BCR registry. Restored registry hash49075197960c924c0a4d759b7c765c3d00a41d2fdd4a943b42823c1d016ab4ec matches cached rules_oci MODULE. Correct Bazel9 cache layout is repository_cache/content_addressable/sha256, not repository_cache/sha256. Added confined symlinks under /tmp/cellhive-bazel-download-cache/content_addressable to restored hash directories. New offline build3646 now passed registry resolution and entered analysis393packages/15190targets; log /tmp/do-paging-workerd-offline-cache-build.log. Flags: --batch --output_user_root=/tmp/cellhive-bazel-user --output_base=/tmp/cellhive-native-bazel-resume build -c opt //src/workerd/server:workerd --jobs=2 --lockfile_mode=error --symlink_prefix=/tmp/cellhive-native-links/ --repository_cache=/tmp/cellhive-bazel-download-cache --noexperimental_collect_system_network_usage --nofetch --repository_disable_download --experimental_repository_downloader_retries=0. Goal remains active because offline build is making progress; native network-based gates cannot run in current restricted environment.


## User-directed binary switch

User explicitly requested no further compilation and direct testing with /opt/cellhive/deploy/workerd-fork/workerd. Stopped build3646, exit130. Selected binary version2026-09-16/build-info cellhive-workerd-fork/0 lacks do-paging/1. Focused native/full-cold tests executed, exit1: native capability gate failed and full-cold listener failed EPERM. Evidence /tmp/deploy-workerd-do-acceptance.log. Do not restart compilation. Network restriction has recurred for more than three goal turns; with offline build stopped by request, remaining native E2E is blocked pending an environment allowing local listeners/COS and a paging-capable prebuilt binary. Goal is not complete.


## Deploy binary investigation resumed

Environment now permits local listeners. Same deploy binary passes filesystem full cold restore2.04s and realCOS full cold restore8.40s, both exit0/all256 hashes/9 canonical objects. Native paging diagnostic with temporary overlay bypassing only marker check crashes SQLITE_NOTADB, confirming the binary lacks native page hydration; formal gate unchanged. Logs /tmp/deploy-workerd-full-cold-retest.log, /tmp/deploy-workerd-cos-cold-retest.log, /tmp/deploy-workerd-paging-diagnostic.log. Full doruntime/dosupervisor regression running session67198, log /tmp/deploy-workerd-runtime-regression.log. No compilation restarted.

Deploy runtime/supervisor regression67198 EXIT0,93 top-level PASS/4 initial skips. Investigated skips: COS separately PASS,2000 DO memory stress49250 EXIT0/PASS24.34s; repaired countingBucket test wrapper to retain FSBucket optional interfaces and replaced compaction Skipf with Fatalf. Red63871 EXIT1 bucket:not supported, green91982 EXIT0 full dosupervisor/no skips. Remaining native paging gate is an actual missing binary capability, confirmed SQLITE_NOTADB with diagnostic-only overlay. No workerd build restarted.


## User authorized multicore native build

User now explicitly requests building the paging fork with multiple cores, superseding the earlier no-build instruction. Started session23337 with the existing /tmp/cellhive-native-bazel-resume cache, jobs8, local cpu8 and Bazel memory scheduling budget6144MiB. Host reports8 CPUs and about8.3GiB available RAM. Log /tmp/do-paging-workerd-multicore-build.log. Analysis completed; compilation ongoing. Expected binary /tmp/cellhive-native-bazel-resume/execroot/_main/bazel-out/k8-opt/bin/src/workerd/server/workerd. Must verify successful build exit and do-paging/1 before native FS/COS and full runtime regression. Network/local listeners now work. Do not count build startup as completed acceptance.


## Workspace relocation required by user

All current CellHive changes moved from /opt/cellhive to /opt/cellhive-worktrees/celld-ltx-migration on branch perf/celld-ltx-storage at base7710ef04d609fe135965b2e94c3c307f4004d1c3. Verified1572 snapshot paths (contents, modes and absence) and exact Git status before cleaning source; source is now clean. Continue all CellHive edits and tests from the destination. Original destination dirty worktree was preserved intact at /opt/cellhive-worktrees/celld-ltx-migration-before-20261109-203023; snapshot, patches and manifests at /opt/cellhive-worktrees/celld-transfer-backup-20261109-203023. No commits made. Workerd C++ source remains in the independent /opt/cellhive-workerd-fork repository as required; current4-core build session69317 log /tmp/do-paging-workerd-4core-build.log.
