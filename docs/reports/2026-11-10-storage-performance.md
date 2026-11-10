# Storage/request optimization — verified

## Final acceptance

The seven-objective implementation and acceptance plan is complete. Prioritized independent source review, current-binary functional/COS/native10GiB acceptance, interleaved fleet throughput and refreshed multipart measurements are verified. No commit or deployment. The sections below this checkpoint preserve chronological evidence; their historical “running” statements are not current process status.

| Gate | Verified status / evidence |
| --- | --- |
| Current Go source | Full tagged tests and vet PASS; five frozen binaries in `/tmp/cellhive-lifecycle-reviewed-20261110-bin`; `lifecycle-reviewed-{source,binaries}.json` |
| Native fork | `/tmp/cellhive-workerd-lifecycle-20261110`, SHA256 `f711d235ba3cbaa28e39b05cac684c17305a0348fa6f5a60594fd441b7943657`; all 46 SQLite KJ cases PASS |
| Current COS routing | PASS, including forwarding, invalid authorization, delete/recreate and old-data absence; `hk-review-1794262692-ca14fa-81g9_nup` |
| Current COS SQLite transfer | PASS: export/import, all 100 values, cache deletion/cold read, duplicate rejection, plain SQLite BLOB/index migration; `hk-review-1794263517-560631-n1mmy3hp` |
| Current COS native fleet | PASS: ACK10→11 retained, owners A→B→A / epochs1→2→3, process fault and lease takeover; `hk-review-1794263588-640f9e-fr5re8xu` |
| Actual 10GiB | PASS733.371s: all163840 BLOB SHA256 values/counters after independent restore; initial snapshot174.380s, index1.028s, scalar cold1.611s/290816 restored bytes, two checkpoints/zero extra snapshot bytes, four retirement cycles without hydration; 26 canonical LTX objects/no auxiliary markers. Full restore255.574s, restore+audit309.160s |
| Final functional suites | Review/queue/multinode PASS (`hk-review-1794264512-3d8dc5-aqvskihb`); workflow/recovery PASS (`hk-review-1794264954-f3311c-yj0rdfib`), including lost response and empty-local-directory restore. Batch recovery PASS (`hk-review-1794265108-9967eb-u6mqlyvk`),100/100 values after cold restore1.302125s.100000-write epoch stress PASS (`hk-review-1794265170-e8aca9-pt1mq3nx`):3 SIGKILL switches, online and cold100000/100000 values; cold first read1.721625s |
| Final performance | PASS: four full ABBA runs/528206 warm requests/0failures, plus four30s worker-PUT runs/22832requests/0failures; multipart c1/c4/c1/c4 full hashes pass. Baseline is the preserved continuous-lease build, not a pre-migration release |

The first final 10GiB run failed with `ownership_lost_after_dispatch`: the ordinary native test fixture used an unmanaged 30s lease. It now uses the existing production supervisor background `OwnerLeases`, without changing the default TTL, dispatch checks or write retries. Targeted retirement/slow-lease native tests passed60.561s. Only `internal/doruntime/paging_integration_test.go` changed after the full-source verification; `lifecycle-managed-source.json` records that test-only delta. The failed log is retained as `native-lifecycle-cos-10gib-acceptance.log`; the retry has a distinct `native-lifecycle-cos-10gib-managed-acceptance.log`.

Data format remains native LTX without new snapshot/fence markers. COS owner authority uses the explicitly approved native conditional-append representation because ordinary COS conditional PUT was experimentally shown unsafe. Latest-epoch arbitrary overlapping intervals still require exhaustive discovery; bounded predecessor cuts skip covered lower levels with verified 100000-object evidence. Logical memory quotas are not whole-process RSS guarantees. Checkpoint ambiguity retains process-wide fail-stop, so colocated DOs can be interrupted and recovered through lease takeover. Ordinary delayed unconditional wake publication remains a separately documented limitation.

## Final multipart acceptance

Current source, including process-wide upload admission, passed four alternating serial/concurrent real-COS runs. Same256MiB random input,32MiB parts; every full download matched SHA256/length and cleanup passed. Serial comparison changes only the compile-time in-flight constant via a Go overlay; it is not a historical-release benchmark. Prebuilt test executables exclude compiler memory from the RSS observation.

| Round | In flight | Upload seconds | MiB/s | Test-process peak RSS MiB |
| --- | ---: | ---: | ---: | ---: |
| 1 | 1 | 5.604958 | 45.674 | 50.38 |
| 2 | 4 | 1.728221 | 148.129 | 147.25 |
| 3 | 1 | 5.261422 | 48.656 | 50.50 |
| 4 | 4 | 1.437693 | 178.063 | 147.88 |

Mean upload wall-time ratio: 3.43x. RSS includes hashing/upload/download/test runtime; it is not a whole-service hard limit. Production buffers remain bounded by4 default32MiB parts per upload and2 process-wide admitted uploads (256MiB default part-buffer budget, excluding other allocations). Actual10GiB correctness and process-specific memory observations are recorded separately in `lifecycle-large-summary.json`. Evidence: `cos-upload-final-comparison.json`, per-round logs and `/usr/bin/time` output under the artifact directory.

## Final interleaved fleet throughput

ABBA order: preserved continuous-lease baseline, final candidate, final candidate, baseline. All four full suites exited0 with cleanup; 528206 measured warm requests,0failures. PUT rows recorded fleet commits and0bucket-batch commits. Concurrency1/8/32 means simultaneous persistent HTTP requests from one Node client process, not OS threads. Compute and endpoints are localhost on one shared host, CPU0–3/GOMAXPROCS4; storage is the specified HK COS bucket, value size1KiB, minimum3s per row with the existing request-count minimum. Each run has two cell-agents and user-runtime/workerd. Client signing behavior and exact binaries/scripts are in `lifecycle-abba-manifest.json`. Wrangler subprocess testing was skipped because no WRANGLER_CLI_PATH was configured; this is the CellHive native fleet measurement.

The baseline already contains migration and continuous node-lease fixes; it is not a pre-migration historical release. Two samples per build are descriptive and do not establish statistical significance or production capacity. Retain the raw p99/request counts in `lifecycle-abba-results.json`.

| Path | Operation | Concurrency | Baseline RPS samples | Candidate RPS samples | Mean change |
| --- | --- | ---: | --- | --- | ---: |
| direct | put | 1 | [174.7, 162.64] | [171.79, 157.98] | -2.2% |
| direct | get | 1 | [2312.19, 2398.12] | [2186.57, 2289.82] | -5.0% |
| forwarded | put | 1 | [165.78, 167.66] | [170.62, 165.66] | +0.9% |
| forwarded | get | 1 | [1577.08, 1573.33] | [1550.57, 1572.09] | -0.9% |
| worker | put | 1 | [82.99, 81.42] | [81.22, 80.76] | -1.5% |
| worker | get | 1 | [166.65, 170.48] | [169.19, 169.4] | +0.4% |
| direct | put | 8 | [1363.57, 1300.45] | [1368.71, 1324.86] | +1.1% |
| direct | get | 8 | [5397.61, 6040.25] | [5933.22, 6050.38] | +4.8% |
| forwarded | put | 8 | [1213.28, 1177.32] | [1202.58, 1158.69] | -1.2% |
| forwarded | get | 8 | [4638.11, 4892.75] | [4870.84, 4861.94] | +2.1% |
| worker | put | 8 | [189.41, 192.02] | [187.55, 191.3] | -0.7% |
| worker | get | 8 | [192.16, 192.23] | [195.3, 195.73] | +1.7% |
| direct | put | 32 | [3396.22, 3492.63] | [3408.53, 3397.54] | -1.2% |
| direct | get | 32 | [6151.81, 6275.96] | [6161.45, 6248.98] | -0.1% |
| forwarded | put | 32 | [2889.88, 2875.8] | [2665.31, 2868.79] | -4.0% |
| forwarded | get | 32 | [5299.83, 5092.98] | [5415.71, 5134.08] | +1.5% |
| worker | put | 32 | [196.93, 196.73] | [187.12, 185.08] | -5.5% |
| worker | get | 32 | [195.97, 197.51] | [198.8, 198.05] | +0.9% |

Continuous RPS does not show a general speedup. The short c32 worker PUT rows are5.45% lower for the candidate, so a targeted30s ABBA follow-up was run with the same immutable binaries,1KiB values,32concurrent requests and fleet mode. Baseline191.33/188.89RPS (mean190.11), candidate189.69/189.12RPS (mean189.405), mean change−0.37%;22832requests,0failures, fleet commits present and no bucket-batch fallback. Baseline p99=303.370/308.393ms; candidate308.688/314.801ms. The short-window5.45% drop was not reproduced at the longer duration; these few samples still do not prove equivalence or a throughput gain. Evidence: `lifecycle-worker-put-long-{manifest,runs,results}.json`. The test-only `PERF_WORKER_PUT_DIAG=1` selects this one case; the standard matrix is unchanged.

The demonstrated request-path gain is idle latency: matched earlier traces after1/5/10/30s gaps reduced direct request latency from16.036/17.809/24.154/37.813ms to1.096/1.101/1.628/1.160ms, with ordinary resource-fence reads removed from healthy local admission. Authorization TTL, owner/resource-generation guards and deletion/recreation denial remain enforced. Those trace identities are retained separately from final throughput identities. Upload wall time and idle-gap latency are separate measured outcomes; neither is represented as an overall RPS multiplier.

## Historical implementation and verification record


All seven items remain in scope; plan: `../archive/plans/2026-11-10-storage-performance.md`. No commit or deployment. Four-core CPU affinity, dedicated HK COS bucket.

## Multipart checkpoint

Implementation: one source reader and at most4 in-flight owned part buffers,32MiB default, dynamic10000-part sizing unchanged. Results indexed by part number; first failure cancels siblings, joins them before abort, and never completes a mismatched-length input. Exact overlap regression failed on old serial behavior, then passed. Bucket full suite and focused race passed.

Real COS provider-only comparison, same256MiB random source/32MiB parts, alternating compile-time concurrency1/4/1/4, same machine/credentials/bucket:

| Round | In flight | Upload seconds | MiB/s |
| --- | ---: | ---: | ---: |
|1|1|5.482183|46.697|
|2|4|2.078254|123.180|
|3|1|5.975312|42.843|
|4|4|2.022141|126.598|

Every object downloaded completely with matching size/SHA256 and cleaned. Serial baseline uses a Go overlay setting only concurrency=1 in the new pipeline, not an unrelated historical binary. This isolates scheduling; it is not a whole-snapshot10GiB speedup measurement. Mean upload time improved about2.79x in this small comparison. Four default buffers require up to128MiB per upload, apart from SDK/HTTP memory; process admission and actual peak memory remain to audit.

Evidence: `artifacts/2026-11-10-optimization/cos-upload-comparison.json`, individual COS logs, `multipart-overlap-red.log`, `multipart-green.log`, `multipart-race.log`, `bucket-full.log`.

## Idle gap investigation

Current node lease default10s; its health margin is20 percent. Local owner-route cache records its observed safe deadline and only extends when a request arrives before that deadline. Continuous background lease renewals can therefore coexist with a route-cache miss after an idle gap. This is a code-level candidate, not a proven explanation of the user's measured extra50ms. Preserve permanent self-fence, release/invalidation and pinned resource auth checks; do not simply enlarge TTL.

Remaining: request traces and equal-environment old/new fleet comparison, pipeline stage timing/10GiB acceptance, fork review and checkpoint changes, cold-restore range planning, full combined regression.

### Idle trace exploratory evidence (still running)

Baseline session39391, artifacts `artifacts/2026-11-10-optimization/hk-review-1794241553-a71d39-sr7rp75n/0/`, OTLP protobuf batches in `idle-baseline-traces/`. Direct round2/gap10s took20.736ms, including19.293ms `cell.fence_bucket_read`; immediate repeat0.775ms. Both reuse a connection and neither trace contains owner bucket read. Forwarded gaps1/5/10/30s likewise show roughly11–17ms resource fence reads. This supersedes the initial unproven owner-cache hypothesis for these measured requests.

Fixture caveat found while inspecting trace: an initial worker GET before idle probes starts pinned authorization refresh, temporarily warming the shared owner fence and masking first-round direct misses. Moved idle probes before that startup worker GET for the subsequent clean rerun. Preserve current run as exploratory, not definitive old/new baseline. Ordinary KV background warming remains removed; do not reinstate it to hide this cost. Need preserve resource delete/recreate safety when optimizing request-side fence checks.

### Clean idle baseline and follow-up implementation

Clean run completed successfully in323.548s:90 requests,162 OTLP batches, all90 request trace IDs matched. Baseline Go binaries remain `bin/native-acceptance`, official workerd1.20260916.1, local direct/forwarded/worker endpoints and real HK COS. Two rounds at gaps0/1/5/10/30s, same1KiB value, keepalive, no request retries. This isolates backend behavior; it does not reproduce the user network RTT.

For nonzero gaps: direct after-gap median15.5915ms versus immediate-repeat0.834ms; forwarded20.325ms versus1.3505ms; worker4.897ms versus3.351ms. Seven of eight direct and all eight forwarded after-gap requests had resource-fence reads, costing10.438–21.503ms and11.875–36.337ms respectively. None of those immediate repeats had a fence read. Worker pinned authorization refresh is an intentional separate path and must not be generalized to ordinary KV. This is diagnosis, not yet a fix.

Evidence: `idle-baseline-clean-result.json`, `idle-baseline-clean-spans.json`, raw trace batches and corresponding hk-review result.log under this report artifact directory. `scripts/e2e/idle-trace-report.py` reproduces the request/trace join; span durations are inclusive and must not be summed.

Independent multipart review found per-object buffering was insufficient for concurrent byte-backed foreground uploads. Added shared provider admission of two multipart uploads (including byte-backed objects), cancellation-aware before initiating COS state; single PUT remains unaffected. Dynamic large parts reduce parallelism to keep each upload at128MiB, except a required single part larger than that budget. Default live part buffers are therefore bounded at256MiB across admitted uploads, excluding source/SDK/HTTP memory. Race-tested COS multipart cases passed9.845s including queued cancellation. The earlier real-COS comparison predates this admission addition and needs final refresh.

Snapshot stage instrumentation now separates hydration, encode/spool, and durable commit in trace spans and logs encoded size plus timing. Tagged supervisor Snapshot/Spool tests passed6.925s. An initial untagged command failed the repository feature-tag guard; rerun used all five required SQLite tags.

Cold scan tests establish the unresolved contract constraint:100000 covered L0 objects still require101 pages/100001 keys without a tail,102/100004 with three tail objects. Arbitrarily earlier minTXID may have maxTXID beyond coverage; safe skipping cannot assume ordered/disjoint ends. Added real-publication crossing counterexamples and32768 interval-subset oracle cases; full tagged replica tests passed1.507s. No unsafe seek enabled and no new remote metadata created.

Fork review code and regression sources are implemented; native Bazel sqlite tests are now running with CPU affinity0–3 and jobs4. Checkpoint continuity optimization remains unimplemented pending durable pre-reset proof; do not equate CKPT_DONE with durability.

Fork native sqlite-test@ now PASS (rerun4.243s; log fork-sqlite-tests-rerun.log). Initial run failed one direct-VFS negative-open test because SQLite diagnostics allocate outside SqliteDatabase memory scope. Added explicit SqliteMemoryScope and direct BUILD dependency in fork test only; all native tests pass, including real WAL checkpoint. No running root test sessions at this checkpoint.

### Checkpoint protocol implementation progress

Go now records exact committed WAL positions and uses activation-local certificates bound to that prefix. Preparation durably captures before issuing the certificate; copy mutations carry it, arbitrary writes invalidate it, post-copy truncate must match certified size, and an exact old-position rotation consumes the certificate once. The gate preserves and captures new-generation transactions. No remote metadata or LTX fields change. Native callback wiring is still pending, so this is not yet an active end-to-end performance improvement.

Focused Go race tests passed (supervisor1.892s, WAL1.255s). TestCheckpointDurablePrefixRetainsImmediateNewCommit runs real SQLite checkpoint/reset, simulates native control callbacks, rejects further full snapshots, writes42 immediately in the new WAL, and independently restores42 from native objects. Injected lost durable ACK issues no certificate; retry succeeds. It does not replace forthcoming native callback/crash/COS acceptance.

Review found an additional existing correctness risk: publishing a raw-main fallback snapshot before replaying a still-authoritative WAL can temporarily supersede already acknowledged data with an older image. A coherent main+committed-WAL streaming snapshot is being implemented; unsafe skipping or a passing normal-path test cannot close this issue.

Fallback defect now has red/green evidence: `TestFallbackSnapshotLostAckPreservesAcknowledgedWAL` recovered1 instead of already acknowledged2 before the change. Snapshot encoding now overlays checksum-verified committed WAL page references and validates source identity after encoding. The test now recovers2 without any subsequent gate/replay; focused snapshot tests passed14.136s. It uses real SQLite and independent object restore with lost-ACK injection, but native concurrent mutation and COS fault acceptance remain pending. Scanner memory scales with changed-page references, not full page payloads; final10GiB memory measurement is still required.

## Controlled interleaved comparison (2026-11-10)

Four runs baseline/candidate/baseline/candidate completed successfully. Same scripts, official workerd1.20260916.1, local two-cell-agent fleet plus user-runtime, real HK COS, four-core affinity,1KiB, minimum3seconds per warm sample (and minimum request count). Binary/runtime/script hashes are in `artifacts/2026-11-10-optimization/throughput-interleaved/manifest.json`; exact rows including p50/p95/p99 and fleet ACK counters in `summary.json`.

Total measured requests including warmup: 538930; failures: 0. All four functional version-upload/activation checks and cleanup passed.

| Path c32 | Baseline mean RPS | Candidate mean RPS | Change |
|---|---:|---:|---:|
| direct PUT | 3172.42 | 3094.84 | -2.4% |
| direct GET | 6209.72 | 6129.93 | -1.3% |
| forwarded PUT | 2822.06 | 2967.61 | +5.2% |
| forwarded GET | 5303.20 | 5177.15 | -2.4% |
| worker PUT | 809.44 | 810.13 | +0.1% |
| worker GET | 790.99 | 808.14 | +2.2% |

Two samples per build do not establish significance: observed c32 differences span−2.4%..+5.2%, with workerPUT effectively unchanged. This is not evidence of a major request-throughput gain. It does not compare the final pending native checkpoint/fail-stop integration; source changed after candidate build and manifest hashes identify what was actually measured. Historical10000+RPS has not been established as a comparable workload.

Follow-up Go WAL/supervisor/bucket/cmd tests passed (0.170s/7.987s/2.105s/0.005s) after owner-context propagation, bounded overlay retry and transport-join regression. Initial untagged invocation failed SQLite feature guard; corrected invocation used all Makefile SQLite tags. First transport test fixture exposed SDK retry and was corrected with idempotent channel notifications; production algorithm unchanged. Native fail-stop tests/build and final COS integration are pending.

Follow-up verification: full tagged `go test ./...` and `go vet ./...` passed. Focused race passed for WAL/supervisor/bucket/cmd. Claimed restore→paging activation→checkpoint capture→HTTP fleet identity integration passed. Native SQLite target (including fail-stop subprocess faults) passed; full workerd build remains active. Whole-workerd fail-stop availability cost and final crash/COS acceptance remain explicit; no deployment.

## Historical epoch scan and current functional acceptance

Historical predecessor discovery now uses the successor's exact cut as an upper bound. If high levels alone prove a complete contiguous snapshot chain through that cut, lower directories are skipped. Synthetic100000-covered-L0 historical inventory requires2listing pages/2keys including the successor, without changing native object names. Latest-epoch discovery remains exhaustive; arbitrary crossing intervals prevent assuming the latest upper bound. Full replica tests and focused race passed. Real HK COS inventory acceptance passed (195.637s overall): 100002 native objects seeded in110.712s, historical restore listed2pages/2keys plus3prefix calls, restored value and SQLite integrity verified, and all scoped objects deleted with an empty-prefix check. Evidence: artifacts/2026-11-10-optimization/cos-inventory-100000.log. This proves the bounded predecessor case, not latest-epoch skipping or a large-database throughput benchmark.

Latest Go binaries in `/tmp/cellhive-storage-optimization-20261110-bin` (hashes in current-go-binaries.json) passed real COS SQLite export/import, duplicate-target rejection, updated-value cold restore, and plain SQLite migration with BLOB/index preservation. Routing safety and100-value owner-crash/cold-restore suites passed. Broader review/workflow/100k epoch stress and new fork checkpoint/10GiB tests remain running or pending. Native integration fixture now explicitly negotiates checkpoint and requires real callback activity with no extra full snapshot; it has compiled but has not yet run against the pending fork build.

Forwarding follow-up: full server tests PASS54.297s; focused forwarding race PASS1.698s. Five Go binaries rebuilt; new hashes current-go-binaries.json, previous preserved pre-forwarding-go-binaries.json. Real COS queue74358 FAILED with decisive pre-cleanup diagnostics (queue-owner-fence-fixed.log; hk-review-1794246359-eeac02-lzfm3xgc/0/result.log). runtime1 n=5/runtime2 n=0; node1 direct KV5/node2 not_found; both internal resolve point at node2 epoch2; local SQLite node1 consumed5/node2empty. Thus forwarding fix does NOT close Queue failure. ACKs2+2+1 success; n1 logged KV epoch1 commit while n2 owner is epoch2. Investigate owner stale-local admission/lease takeover and first-claim race; do not hide with KV priming. Cleanup passed; COS lock free. Root sessions40441/1447/13036/74358 terminal. Only remaining known live root process full native build20854, latest~911/1914; poll same handle. No final acceptance claim.

2026-11-10 critical owner authority regression: real COS bucket cellhive-e2e-hk-1376795072 ignores S3 PutObject If-None-Match and If-Match on own.json; repeated create and stale CAS both overwrite successfully (cos-owner-conditions.log). Nodes use append authority, so the existing nodes-only startup probe misses the distinct raw owner path. Startup diagnostics must test both nodes and cells/.../own.json conditional semantics and fail closed before serving. Queue multinode acceptance failed with node1 epoch1 writes and node2 epoch2 empty reads. Prior passing suites do not prove owner safety. LTX format remains unchanged; provider authority remedy pending resolution of COS versus strict plain owner format requirement.

2026-11-10 root cause confirmed and correction authorized: TestCOSOwnerConditions on real designated HK bucket proved raw owner S3 If-None-Match and stale If-Match both ignored (two writes returnednil and overwrote first). Existing startup Diagnose only used nodes/, whose segmented authority was safe, so missed own.json raw path. User explicitly approved native conditional append for owner; provider ownKey now uses existing segmented authority across Get/create/CAS/delete/stat/range/List, no new implementation/remote LTX marker. Physical owner authority uses hourly bounded segments, a documented exception to celld plain owner JSON; no unsafe raw-owner compatibility added. Startup diagnostic now probes actual cells/.../own.json as well as nodes. New fault-injection regression RED for ignored create/CAS, bucket full suite GREEN1.917s, focused authority race PASS1.420s. RealCOS conditions and delete tests PASS4.432s including16concurrent claims exactly1winner, staleCAS rejection, validCAS, startup and stale/current delete. Logs cos-owner-conditions.log(red),cos-owner-conditions-fixed.log(green). FiveGo binaries rebuilt hashes current-go-binaries.json; pre-append-owner-go-binaries.json preserved.
LIVE: Queue realCOS8084 with newnativeowner, logqueue-native-owner-fixed.log, artifacthk-review-1794246759-1fe270-jlh1lfhu: normal5consumed andDLQ1 passed, currently killednode1 waitinglease/recovery. Protocolowner/lease/replica/server tests26488 live, logowner-authority-protocol.log. Forkfull20854 stilllive~969/1914 fourcores. Probe93103/bucket37257/race3062/build6598 terminalPASS. Queue cleanup may warn about rawowner records from earlier failed fixtures; no compatibility added, preserve diagnostic distinction and do not delete unrelated objects. Remaining allsevengoal scope unchanged; no completion/commit/deployment.

Native-owner Queue8084 terminalPASS:5initialconsumed,DLQ1,ownerSIGKILL,8totalafterfailover,cleanupHTTP200. Fullserialreview/workflow/routing-safety/batch-recovery/cell-transfer/epoch-stress now launched (currentexecsession), logs native-owner-<suite>.log and native-owner-functional-runs.json. This repeats acceptance on corrected owner semantics; earlier rawowner passing results are not safety evidence. Protocol26488 stilllive; owner39.142s/lease0.290s/replica3.962s alreadyPASS.

2026-11-10 additional authority boundary regression: deterministic TestSegmentedRolloverRejectsInterveningOldWindowWrite fails: rollover copies from an old token into a new physical segment without consuming that token on the predecessor. An intervening acknowledged old-window CAS can be overwritten. This affects reused segmented authority, including node leases, and blocks final correctness acceptance even though current-window COS/Queue tests pass. Evidence authority-rollover-red.log. Native append provider remedy remains under development; no deployment.

Continuation progress: protocol26488 terminalPASS owner39.142s/lease0.290s/replica3.962s/server52.158s. SerialCOS66090 stillLIVE; review,workflow,routing-safety,batch-recovery nowPASS; cell-transfer thenepochstressremaining. batch coldrestore100/100,1.236317s non-uncontended timing. Native20854 stillLIVE~1032/1914, no source edits/restarts.
Two decisive authority regressions in internal/bucket/segmented_rollover_race_test.go nowRED: intervening old-window CAS overwritten by rollover; simultaneous initialcreate acrossboundary gives2winners. Existing window selection structurally unsafe. Agent authority_rollover read-only confirmed; proposed stable capped appendroot+sealed generation protocol with retained1byte retired-generationstubs (prevents delayedhelper recreationABA). Draft docs/archive/plans/2026-11-10-native-authority-correction.md, docs decisions updated before implementation. /tmp/authority-followup.md contains scoped implementation assignment and rootcap128MiB, segmentcap8MiB. Agent follow-up requested; verify acknowledgement/status because collaboration tool messages have sometimes arrived as unreadable encrypted blobs. Agent knows /tmp/authority-design.md and /tmp/authority-followup.md from shared files. Do not modify agent-owned internal/bucket/segmented*.go concurrently if implementer active. Root owns provider integration/COS probes/docs/runtime acceptance. User approved COS-native owner exception; optional constraints question is nonblocking and must not trigger repeated scope confirmation. No goalcomplete/commit/deployment.

Authority implementation continuation: agent authority_rollover completed new CHA3 stable root/sealed generation protocol, initialcreate and oldtoken rollover regressions nowpass, crash/helper/ABA/largeframe tests added. Root integrated provider List/Get/listSizes/ListPage root selection and ObjectNotAppendable->ErrPrecondition (new regressions red/green). Fullbucket race76194 PASS14.608s before reviewer followups. Independent authority_review reported no concrete stale-success counterexample but2P2: rootcapacity optionaltimerollover blocks fittingwrites; retirebetweenTailGet/RangedGet returns spuriousrangeerror. Root added segmented_review_test.go, bothRED, fixed optionaltimerollover gating and retry onlywhen same-lifetime rootprovesnewergeneration. Fullbucket race11354 nowLIVE, logauthority-review-green.log. Docs plan synced128MiB rootcap/64bytedescriptors andretiredstubs. New fork-review followup report docs/reports/2026-11-10-sqlite-paging-review-followup.md mapsall8items; remainingregistrycardinalitycap assessment noted, do notmodifyforkduringbuild.
LIVEother: COSserial66090 nowepochstress100000, firstSIGKILL at25036ACKs followedownertransfer; priorreview/workflow/routing/batchrecovery/celltransferPASS. Fork20854stilllive~1060/1914. No newGo binaries built forCHA3yet; currentE2ErunsoldCHA2binarysoresultsnotfinalprotocolacceptance. Providerrealtests needexclusiveCOSlock and testfixturelegacyCHA2cleanup/migration, notruntimecompatibility. Agentworkdone,noimplementeractivelyownssegmentedfilesnow. /tmp/authority-implementation-feedback.md containspotentialboundeddescriptorcacheoptimization(toavoid2COSreadsoneachGet); notimplemented. Goalactive,nocommit/deploy.

### COS authority follow-up: sealed generations

The approved COS owner exception now uses the CHA3 permanent root and sealed generation protocol. Ordinary conditional PUT is not used for owner exclusion. Deterministic races cover competing initial creators across clock windows, intervening old-generation writes, crash recovery and delayed writes against retired generations. A bounded 4096-entry descriptor cache stores only generation locators: a warm read still reads the authoritative generation tail, reducing discovery from two suffix reads to one without caching authorization or owner values. Warm-cache seal recovery, retirement during large-frame range reads, current-generation corruption and monotonic concurrent-reader tests pass under the race detector (`authority-cache-fault-race.log`).

The full tagged Go suite (`native-authority-full-go.log`), build and vet (`native-authority-build-vet.log`) passed. Candidate identities are in `cha3-go-binaries.json`; the earlier native-owner functional suites ran the previous authority implementation and do not establish final CHA3 acceptance. Real COS rollover and publication-stage fault tests have been prepared and are queued behind the serialized epoch-stress suite.

Idle-gap review confirms that continuous owner/node lease health cannot replace resource-retirement checks: deletion can publish a resource fence before the owner's cleanup RPC arrives. This batch preserves that safety window. The descriptor cache removes redundant root lookup; eliminating the final fence network read would require owner-mediated admission closure and draining before conditional retirement publication, with takeover and partition proofs. No zero-network idle-read improvement is claimed.

Latest-epoch scanning was rechecked against the local celld reference: `crates/ltx/src/client/epochs.rs:list_levels` and `crates/ltx/src/replica.rs:calc_restore_plan_with_slots` both request levels with `TXID(0)` for cold discovery. Provider `ltx_files_bounded` supports offset seek, but that does not make an unbounded newest-epoch discovery safe to seek beyond a snapshot: an earlier-starting interval may extend beyond its TXID. CellHive retains that crossing-range safety, with explicit near/far crossing regressions; historical exact-cut pruning is the additional verified optimization. The existing real-COS historical inventory acceptance listed only two keys from 100002 native objects. No claim that newest-epoch cold discovery skips all covered L0 objects.

Real COS CHA3 probe passed (`cos-cha3-authority-acceptance.log`,9.805s):8 independent initial creators with divergent clocks produced one winner; rollover retained the current value; COS rejected delayed append at positions0 and28 against the retired generation; delete/recreate and cold read passed; injected post-commit failures after bootstrap/seal/genesis/descriptor all recovered; startup conditions and16-contender owner claim passed. Exact unique CHA3 fault-test objects were removed after all callers joined.

The pre-CHA3 binary functional batch completed with100000 acknowledged writes,3 forced owner switches,100000/100000 online verification and100000/100000 cold verification; first cold GET2.250916s and scoped cleanup passed. This confirms that batch only, not final CHA3 or new workerd acceptance.

New CHA3 binary runtime acceptance has now passed review/Queue/multinode, workflow/recovery, routing safety and batch recovery. Batch recovery independently matched100/100 values after clearing local data; first cold GET1.721228s (concurrent native build, not an uncontended benchmark). Cell transfer and100k epoch stress remain in progress. Manifests/results: `cha3-go-binaries.json`, `cha3-functional-runs.json`.

Native checkpoint acceptance now checks mutated counters as well as BLOB hashes. New failure integration fixture injects a lost checkpoint-end response after successful handler completion, requires actual process exit74, then audits independent restored value/BLOB hashes/SQLite integrity. It compiled but has not run against the pending new native binary. Owner-stub/manual-capture fixture coverage must not be presented as production lease-expiry recovery acceptance.

### Supervisor hydration bitmap

Replaced one `map[uint32]bool` entry per hydrated original page with sparse1024-page/128-byte bitmap blocks; exact unique-page count preserves hydration telemetry. The truncation floor remains independent, so missing pages discarded by truncation cannot be fetched from the old snapshot on regrowth. Added explicit missing-page truncate/regrow regression in addition to existing all-hydrated truncate and write-preservation tests. Independent source review found no correctness blockers.

Three one-iteration bookkeeping benchmarks, 10GiB at4KiB/page (2,621,440 pages), four-core affinity, concurrent native build: old map total allocation77,927,128–77,938,392 bytes/op and1.560–1.846s; bitmap476,024 bytes/op and0.198–0.280s. These are allocation volumes and local bookkeeping times, not retained RSS, upload throughput or cold-restore latency. Extremely sparse occupancy (one page per1024-page block) is deliberately a tradeoff: old map76,104 bytes/op versus bitmap476,024–476,040 bytes/op, about0.22–0.26ms versus0.37–0.57ms. Dense large-cell scans benefit substantially; sparse point access does not always allocate less. Evidence: paging-bitmap-benchmark.log and paging-bitmap-sparse-benchmark.log.

Focused bitmap boundary and previously-unhydrated truncate/regrow race tests pass1.083s. Full supervisor race suite is still running; final native COS/10GiB acceptance remains outstanding. This change does not resolve cumulative registry lifetime by itself.

2026-11-10 supervisor bitmap full-package race PASS256.652s (`paging-bitmap-race.log`); focused previously-unhydrated truncate/regrow and boundary race PASS1.083s. Independent source review found no blockers. Full tagged Go/build/vet refresh and native integration remain pending.

Post-bitmap full tagged Go tests, build and vet all passed. Updated five-binary identities are recorded in `cha3-bitmap-go-binaries.json`, stored under `/tmp/cellhive-cha3-bitmap-20261110-bin`. Native paging/checkpoint/10GiB acceptance still requires the pending fork build.

### Fence publication atomicity follow-up

A deterministic two-manager interleaving reproduced two correctness failures: stale cleanup could reblock a recreated resource, and an allow operation could lower the retired epoch floor from9to4. The cause was two owner reads: mutation decisions used the first value, while publication copied those stale fields into the second value and used its newer CAS token. The fix uses one owner read for both decision and token; conflicts rerun the decision. This also removes one provider read from each fence mutation, without changing read-cache TTL.

Focused regression passed0.038s. Full replica race passed34.231s and full server race passed96.549s (fence-atomic-protocol-race.log). The obsolete opt-in old-format migration test was replaced with native owner create/block, concurrent monotonic floor updates, allow, stale cleanup rejection, old/new epoch guards, and unchanged owner identity. Its real COS run is queued behind the existing100k runtime suite. Full tagged Go/build/vet is being refreshed for this source; earlier binary manifests must not be presented as including this fix.

### Provider suffix version correction and COS revalidation

Real COS uncovered a deeper authority-read bug after the fence decision fix: eight concurrent successful floor updates left floor6. TailGet read an older GET payload/ETag, then paired it with a later HEAD length when the server returned a whole object without Content-Range. That fabricated a newer append position for stale state. The fix derives payload and length from the same HTTP response; a206 response must contain a valid suffix Content-Range matching bytes received, and HEAD/explicit-range fallback must match ETags. Whole200 responses, including chunked bodies, require no extra HEAD.

Three deterministic regressions failed before the fix and pass after it. Valid/malformed suffix and chunked-response boundary tests pass. Full bucket race passed14.124s. The previously failing real COS native fence lifecycle passed five consecutive repetitions in8.963s, including concurrent monotonic floor updates and stale cleanup rejection. Evidence: s3-tail-version-red.log, s3-tail-version-green.log, s3-tail-version-boundaries.log, s3-tail-version-bucket-race.log, cos-native-fence-tail-fixed.log. Independent review and final full-binary functional/performance refresh remain required.

The earlier CHA3 binary functional batch completed all six suites. Its100k stress confirmed all100000 values online and again after removing local data, across three forced owner switches; first cold GET3.552078s and cleanup200. These passing workloads do not supersede the concurrent authority failure and are explicitly tied to cha3-go-binaries.json, which predates the suffix fix.

### Explicit-range fallback and startup discovery follow-up

Independent review found that TailGet's explicit-range fallback dropped status/Content-Range validation. It now validates exact requested offset, end and total together with ETag; an ignored-range 200 is accepted only for an offset-zero whole-object request. NotFound normalization is preserved. Wrong offsets, missing ranges, wrong totals, version changes, full-response boundaries and concurrent disappearance have deterministic coverage. Full bucket race passed13.739s before the final NotFound-only correction; focused race after that correction passed1.245s. Real COS native fence lifecycle passed another five consecutive runs in9.020s (`cos-native-fence-fallback-validated.log`).

The tail-intent runtime binary passed review, workflow and routing safety, but batch recovery stopped before peer readiness. The peer startup log shows protocol discovery resolving historical nested `nodes/<node>/recovery/<session>` authorities until the harness cancelled startup. It did not reach recovery assertions. `ProviderBucket.List(nodes/)` resolved all nested authorities before `lease.Sample` filtered top-level leases.

New optional immediate logical listing filters nested logical keys before authority reads while preserving stable-root selection, tombstones and recursive List semantics. Sample retains its generic fallback. Individual Get/decode errors now return failure rather than a partial peer set; concurrent NotFound remains skippable. Bucket/lease race passed14.086s/1.352s. Physical key listing remains recursive, so this removes irrelevant authority GETs, not all listing pagination. No readiness timeout increase or historical-object deletion was used. Updated full Go/vet/build and serial COS validation are running with `discovery-source.json`; final acceptance remains pending.

### Staged owner-mediated retirement primitives

Admission-only retirement closes new requests while allowing previously admitted Cell/capture work to finish. Exact job/generation/owner identity is retained on cancellation and ordinary Forget; request release is once-only. Full cellstore race passed7.388s and independent review found no blocking primitive defect. A two-request duplicate-release regression strengthens the drain proof.

A separate exact-owner publication primitive validates the drained node/session/epoch and source generation on every CAS attempt, then publishes retired/job/floor together. Tests cover lease renewal conflicts, node/session/epoch successors, recreation, another cleanup job, lost-response retries and stale retries after AllowResource; focused race passed1.187s. Neither primitive is wired into production deletion yet. Coordinator authentication, owner transition serialization, remaining admission paths, bounded lifecycle reclamation and the local fast proof are still required. Existing resource-fence TTL checks remain enabled; there is no zero-network idle-latency claim.

Discovery binary full tagged Go/vet/build passed (`cha3-discovery-go-binaries.json`). The previously failing real COS batch suite now passed, with100/100 values restored after local removal and cold first GET1.294453s under concurrent build. Cell transfer passed100 imported values, post-import updates and SQLite reexport, plus plain SQLite BLOB/index migration. Review/queue/multinode also passed; the rest of this serial run remains pending.

Exact-owner retirement replay tests were strengthened following review: same-job successor tuples, concurrent Allow during replay, and floor-only source changes reject without changing the successor. Focused race passed1.183s. A staged cleanup-only owner claim now handles expired retired owners and explicit missing/unowned lineage while retaining recovery/reset/validation-before-CAS and ordinary-claim rejection. Initial focused race passed1.080s; expanded full owner race is running. No production coordinator or idle fast path is enabled by these helpers.

Cleanup-only owner full-package race passed161.884s, including existing600k-step ownership simulation and new lease-error/fresh-live cases (`cleanup-owner-full-race.log`). It remains a staged API pending coordinator integration.

Additional admission integration now covers KV expiry, KV stats, queue DiskStats and Cloudflare queue purge. Closed-admission regression plus related handler tests passed race1.944s. Queue readiness now has a separate read admission released before claim, and dispatch preparation happens before its admission; an owner-only dispatch check runs inside admission without claim/reset. Full queue race passed3.264s. Capture shutdown Drain now pins only resident cells and reports closed admission instead of reopening forgotten resources; full cellcapture race passed2.283s and cell-agent package race/compile passed3.712s. Timer/waker and disk-GC paths, coordinator/authentication, lifecycle reclamation and fast proof remain outstanding.

Timer/waker lifetime admission is now wired through the full open/dispatch/commit/read pass. Resource preparation runs before admission; capture Ensure runs after admission, and opening a timer no longer performs destructive generation reset inside a held request. Cron's existing capture preparation is preserved separately. Timer upsert and startup/wake-repair registration use resource admission. Disk-GC eligibility reads are pinned, with the pin released before Forget waits; the final verification reuses a resident activation instead of reopening it.

The first refactor test caught a doubled waker dispatch count; the duplicate increment was removed. Full timer/waker/cell-agent race passed1.374s/1.117s/3.641s, then focused maintenance/server race passed1.062s/1.212s. A real closed-admission wake maintenance regression passed1.042s, proving neither timer open nor wake publication occurs after the gate closes. Full Go/vet/build with the fixed native workerd is now running under the admission source manifest. Production owner-mediated retirement coordination and zero-network proof remain disabled/incomplete.

### Native COS and discovery acceptance update

The discovery binary completed all six serial COS suites: batch recovery, SQLite transfer, review/queue/multinode, workflow, routing safety and epoch stress. Epoch stress acknowledged 100000 writes across three SIGKILL owner switches; online and independent cold verification both matched 100000/100000. First cold GET was 2.441453s. Evidence: `discovery-functional-runs.json` and `hk-review-1794252954-6bbfca-h5vgair7/0/result.log`. This binary predates later maintenance admission and coordinator primitives.

The admission source completed full tagged Go tests, vet and binary builds using the fixed native workerd (`cha3-admission-go-binaries.json`). Native COS small tests then passed: fail-stop exit74 without test cancellation and three independent restored SQLite integrity checks; scalar cold read0.959s with286720 hydrated bytes; two checkpoint callbacks with zero additional snapshot bytes; all256 BLOB SHA256 values matched after independent restore;17 canonical LTX objects with no auxiliary remote markers. The read-outliving-lease case also passed. These Go integration fixtures do not establish real fleet lease-expiry takeover.

Actual10GiB native COS test is running (163840 rows x65536-byte BLOB), with initial seed/hash126.108s. Its BLOB payload repeats random4KiB blocks within each64KiB BLOB, so encoded transfer is compressible; logical database size must not be reported as uploaded bytes. Native production fleet fail-stop using admission binaries is queued behind the same exclusive COS lock. Neither pending result is a pass yet.

### Cleanup authority validation prerequisite

Cleanup lease validation now requires the durable scope, source generation and exact deadline as well as a nonempty matching token and current running job. The internal-only validation envelope carries lease credentials explicitly; public cleanup JSON continues to omit them. Validation resolves the current control owner, rejects stale local/remote owners and rechecks ownership after reading SQLite. It performs no claims or capture writes and remains a staged primitive; production deletion still uses the existing fence checks.

Combined targeted control/server/owner race tests passed1.113s/2.882s/1.041s. Independent cleanup-only-claim review and expanded21-case tests cover unretired predecessor acquisition, incarnation mismatches and retry against a live CAS successor. Still required: deadline-bounded coordinator publication/ACK, real retired-session recovery, reliable capture quiescence, safe gate reclamation and final runtime acceptance. No idle fast proof is enabled.

10GiB intermediate measurements: initial capture181.716s, encoded snapshot10853709218bytes, restore index3.431s, cold scalar4.426s with290816hydrated bytes, following BLOB0.010s. Despite repeated payload blocks, the measured encoded object is about10.85GB; do not infer compression savings. Checkpoint and full independent restore remain pending, so this is partial acceptance only.

Capture shutdown now retains cancelled instances until initialization, loop, background baseline and pending asynchronous commit futures complete. Timeout/retry cannot confuse cancellation with completion. Full sqlcapture/cellcapture race passed19.311s/2.082s, with an additional hint-invalidating regression passing1.116s. Owner retirement now serializes local claim/release transitions; an added lost-ACK regression first failed on retry, then passed after accepting the same job on the same already-retired tuple while rejecting recreation. Focused owner race passed1.442s. Production coordinator and fast local proof remain incomplete.

The10GiB baseline full independent materialization measured613.158s; final hash audit is still pending. The sequential path shared a1MiB encoded Range GET cap with random paging. Materialize now permits8MiB encoded per run while retaining8MiB decoded bounds, exact object ownership and frame/ETag checks; random paging remains at1MiB. An incompressible2300-page regression verifies2 sequential reads (3 after one latest-page overlap) and every restored page. Focused replica race passed6.519s. A realCOS10GiB comparison is queued; no speedup claim yet. Full Go/vet/build refresh is running under `quiescence-source.json`.

10GiB native COS acceptance completed successfully in1006.871s. Independent restore plus audit682.989s verified all163840 BLOB SHA256 values and checkpoint counters. Inventory contained17 canonical native LTX objects and no auxiliary storage markers. This establishes the tested fixture's full correctness; fleet lease-expiry acceptance is running separately. Latest quiescence source full tagged Go tests, vet and five-binary build all passed (`cha3-quiescence-go-binaries.json`). Optimized sequential-range10GiB performance rerun remains queued, so613.158s is still the measured baseline full-restore time.

### Completed native 10GiB comparison and latest fleet run

The optimized fixed test binary (`native-cos-10gib-expanded-identity.json`) passed in 666.539s. Initial capture was 188.831s for 10853708508 encoded bytes. Independent full materialization was 277.553s; restore plus all 163840 BLOB SHA256 checks was 334.295s. The prior observed materialization was 613.158s (54.7% lower elapsed time in this run). This is a serial real-COS comparison, not a final repeated uncontended throughput benchmark. Random cold read was 1.316s with 290816 hydrated bytes, index preparation 0.896s; its path did not change, so that improvement cannot be attributed to the sequential range budget. Two checkpoint callbacks added zero snapshot bytes; all 17 objects were canonical native LTX without auxiliary markers.

Evidence limitation: the comparison launcher accidentally truncated the baseline raw-log filename. Before truncation, baseline terminal PASS and timings had already been observed. `native-cos-10gib-baseline-observations.md` records those observations and explicitly is not a recovered raw log. The comparison log was moved to `native-cos-10gib-expanded-runs.log` while its file descriptor remained open, preserving the entire comparison output.

Latest quiescence-binary fleet test passed (`hk-review-1794255756-d4d53a-liizj5mh/0/result.log`). It verified 12 fleet commits, takeover after 32s with acknowledged value 10, sparse logical 16920576 / allocated 135168 bytes, missing-backing-file fail-stop exit 74, supervisor termination and stopped renewal, then takeover after 32s with acknowledged value 11. Epochs advanced 1 -> 2 -> 3 across A -> B -> A; final proof took 8.463s and cleanup returned HTTP 200. These immutable binaries predate subsequent retirement coordinator and cellstore removal edits.

Current removal regression: Drop failures now preserve resident handle/LRU/files for retry, and destructive verification runs again after Drop. Cellstore race passed 4.785s and cellcapture race passed 2.285s. The combined sqlcapture run failed `TestCaptureEmitsSnapshotOnCheckpoint`; five isolated repeats passed, so investigation remains open rather than treating the suite as passed. Independent review also identified failed-initialization readiness publication racing a warm Ensure; correction and regression are in progress. Owner-mediated cleanup production integration and native paging lifecycle reclamation remain incomplete; the goal is not complete.

Checkpoint follow-up: deterministic pre-fix regressions demonstrated public checkpoint crossing an active WAL poll boundary and both SQLite checkpoint helpers accepting busy results. Public checkpoint now serializes against poll; helpers inspect the result row. Three-repeat race tests passed sqlcapture97.685s/cellstore2.638s, then final focused refresh passed1.284s/1.570s. The original intermittent failure lacked capture.Err output, so its exact cause remains unproven; diagnostics now include it. Independent staged coordinator review found no new blocker, with during/post-CAS authority fault injection still needed in integration. Focused server cleanup/retirement race passed1.330s.

Current whole-tree verification: tagged go vet passed. Tagged full Go suite failed only the hydrate-priority test's TempDir cleanup: its background goroutine was not joined before Store.Close/directory removal. Test lifetime repair and rerun are in progress. Do not count this whole-tree run as passing.

Final verification of this batch: hydrate test now cancels/releases/joins both workers before Store.Close, including early failures. Focused race x20 passed1.289s; full cellstore race passed5.275s. Full tagged Go retry session29838 terminal PASS exit0; tagged vet47017 terminal PASS exit0. Logs copied into reports/artifacts/2026-11-10-optimization (cellhive-retirement-checkpoint-full-go-retry.log, cellhive-retirement-checkpoint-vet.log and targeted race logs). No tests remain running from this batch. Source coordinator review /tmp/coordinator-current-review.md found no new blocker; production wiring and during/post-CAS fault injection remain pending. All seven goal objectives remain active; no commit/deployment, no new remote markers. Next: production owner-mediated cleanup integration including gate reclamation and retired-session takeover; native paging lifecycle retention; final current-binary COS/performance acceptance.

### Owner-mediated production cleanup integration

Both production `BlockResource` entry points are removed. Synchronous revoke now captures the cleanup lease after intent durability, prepares the resolved data owner, and returns the lease to pending; physical completion remains asynchronous. Cleanup-data carries explicit internal lease credentials and the expected owner, and cannot ordinary-forward/claim a retired scope. Dead/unowned cleanup uses the administrative acquisition, full predecessor recovery, capture/admission drain and same-job gate reconciliation after a lost acknowledgment. Administrative epochs are retired through their own epoch in the acquisition CAS.

Purge now requires exact prepared local ownership and control validation before invalidation, wake removal and object deletion. Pin cannot keep deleted user data resident. Release preserves retirement lineage; an owner transition proof guards reclamation of the exact local gate after files/capture are gone. Control completion reads a fresh authoritative retirement floor, avoiding the initiating node's stale fence cache after remote preparation.

Evidence: real HTTP sealed-follower/spool/nodelog recovery integration retains every acknowledged live-scope page while skipping the resource-retired scope in the same predecessor session. Seal/tail/final-session-CAS failures prevent takeover and retry safely. Focused repeat5 passed1.806s; complete recovery race2.896s and owner race87.810s. Cellstore lifecycle proof tests passed full race5.374s; released-owner proof tests passed repeat5 race2.503s. Routing tests cover delayed remote barrier, post-response lease/owner changes and lost responses; acquisition tests cover absent/unowned/dead/already-retired predecessors, unreachable live owners, recreated lineage and lost claim acknowledgment (race2.039s).

Five full delete/cleanup/recreate cycles pass with zero retained gates after each completed purge; replaying old cleanup RPCs cannot gate or delete a new incarnation. First full server run caught fixtures that seeded SQLite without ownership; fixtures now establish real authority before direct writes, preserving existing data/auth assertions. The complete server race rerun passed60.423s. A source-identified full Go/vet/five-binary build is running under `retirement-integration-source.json`.

Still incomplete: failed/stale gates on former owners need guarded loss-of-authority reclamation and fresh activation handling; the local generation fast proof is not enabled. Native pager lifecycle retention, final same-environment performance and latest-binary COS/fleet acceptance are also outstanding. These changes are not deployed.

The identified production-integration source completed full tagged Go tests, vet and all five service binary builds successfully (runner exit0 for each). Fixed binary directory: /tmp/cellhive-retirement-integration-20261110-bin; identities: retirement-integration-source.json and retirement-integration-binaries.json. Real HK COS review/queue/queue-multinode acceptance has started against these binaries and fixed native workerd; results remain pending. Earlier COS acceptance must not be substituted for this revision.

### Former-owner gate reclamation and COS integration acceptance

The fixed retirement-integration binaries passed all three real HK COS suites: review-fixes, queue, queue-multinode. Artifact directory: `hk-review-1794257289-2cede0-5u24zh8q`. Multinode consumed five messages, verified one DLQ message, killed node1, then consumed through eight after failover. Each namespace cleanup returned HTTP200; the runner exited0. This acceptance predates the following gate-reclamation changes.

All nodes now page their local retirement gates before control-owner-only cleanup, bounded to20 candidates and a2s batch with1s per candidate. Request preparation can reclaim its own stale gate. Reclamation requires actual absent/foreign owner authority under local claim exclusion; same-process ownership at every epoch, including expired leases, remains closed. It drains admission, joins capture, removes obsolete local files, refreshes generation evidence and clears only the exact gate. No remote objects or bucket List are added. Drop errors/authority returning locally preserve the gate and resident DB.

Focused race proof passed owner1.041s/server1.131s; combined lifecycle/coordinator tests passed cellstore1.092s/owner1.374s/server2.985s. Final3-repeat scope-pagination and local-claim exclusion tests passed owner1.161s/cellstore1.038s. Source-identified full checks are running under retirement-reclaim-source.json. Local-generation fast proof is still disabled; pinned authorization TTL is unchanged.

Latest gate-reclamation source completed full tagged Go tests, vet and all five binaries successfully (runner80081 exit0 for every stage). Fixed build: /tmp/cellhive-retirement-reclaim-20261110-bin; artifacts retirement-reclaim-source.json and retirement-reclaim-binaries.json. No local-generation fast proof has been enabled, and these binaries still require final COS/performance acceptance after the remaining optimization work.

### Latest reclaim binary COS acceptance refresh

The fixed `/tmp/cellhive-retirement-reclaim-20261110-bin` and native workerd passed review-fixes, queue, and queue-multinode against the dedicated HK COS bucket. All namespace cleanup calls returned200; runner27542 exited0. Artifact: `hk-review-1794258034-171d1a-ksx5hz5c`. The multinode test consumed5, verified DLQ1, killed node1, and consumed8 after takeover.

An earlier run failed on a reused test hostname and an occupied runtime port. The runner now isolates test hosts and checks its port window before remote writes; earlier failures are retained in `hk-review-1794257905-1d9a03-_mzoz105`. No unrelated namespace or process was deleted.

Separate real-COS authority race checks are not yet accepted: CHA3 rollover/recovery passed, but one concurrent-create contender received an invalid authority object length. The decoder remains fail-closed; additional key/length diagnostics and repeated current-source runs are underway. This is independent of the successful fixed-binary functional run.

Authority race diagnosis: repeated COS reads observed zero-byte root or generation during concurrent initialization. The bounded decoder now rereads only exact `(total=0, tail=0)` responses up to four total reads with10/20/40ms waits, honoring cancellation. Persistent empty objects remain errors, never absence; no helper write or checksum relaxation was added. Deterministic regressions failed before the change and passed afterward; the full bucket race suite passed12.198s. Ten-round real COS verification is pending; fixed service binaries above predate this patch. Raw reproduction excerpt: `authority-empty-reproduction.log`.

Authority empty-publication fix: real COS10 repetitions terminalPASS66.564s, both TestCOSOwnerConditions and TestCOSAuthorityCHA3 including concurrent claims, stale CAS, rollover, retired-generation refusal and ambiguous-write recovery. Artifact authority-empty-cos-fixed.log; full bucket race authority-empty-bucket-race.log. No running tests remain from this turn. Fixed service binaries predate the bounded-empty-read patch; latest source needs subsequent build/integrated acceptance. Goal remains active: continuous-lease local generation proof, paging lifecycle and final performance/acceptance still outstanding.

### Continuous lease proof and idle local route reuse

Node publications now maintain a local health revision. They detect safety-margin gaps even without intervening requests, while real expiry stays irreversibly fenced. Local route and local control authorization proofs bind this revision alongside exact owner and cache revisions. A request-idle route can therefore follow uninterrupted node renewal without another owner GET. Foreign routes keep fixed deadlines. The idle regression spans the original route deadline with six node renewals and asserts one total owner GET; recovered-margin, delayed-response, release and session tests reject stale proof. Resource-generation reads are still present; this is not evidence that final idle latency is fixed.

Full lease/owner race passed1.286s/70.260s before the final LocalReadGuard extension. Final route/guard race repeated3 times passed12.365s; the new recovered-health guard test repeated5 times passed1.029s. Complete final-source tagged Go tests, vet and five-service build all passed. Fixed binary directory `/tmp/cellhive-continuous-lease-20261110-bin`, identities `continuous-lease-source.json` and `continuous-lease-binaries.json`. Current-binary COS routing-safety verification is running; no deployment.

Current fixed continuous-lease binaries COS routing-safety runner61360 terminalPASS exit0. Dual-agent forwarded GET/PUT/batch, namespace/token rejection, deletion, recreation and old-data absence all passed; namespace cleanup200. Artifact hk-review-1794258759-e48381-08rwnavx. No active tests/builds remain from this turn. Goal active; resource-generation fast admission and native paging lifecycle still pending.

### Retained resource authority proof

KV/queue scope admission now retains an exact local owner/retirement/lease-health proof with separate validation and release. Preparation, gated admission and response publication validate the same proof. Hot local requests derive generation directly from that owner; cold and foreign requests retain the authoritative fallback. Existing background and durable-write checks remain. Tests verify zero fence GETs with the proof, rejection between preparation/admission and before response, and real owner-mediated deletion waiting for an admitted response before publishing retirement. The existing node liveness compatibility identifier now excludes older unilateral-retirement writers; no COS marker or LTX field was added.

Focused three-repeat retirement/proof/pinned tests passed (server9.777s). Full final tagged tests, vet and five-binary build passed. Full race: server74.629s, owner107.979s, lease1.399s. Fixed binaries `/tmp/cellhive-resource-proof-20261110-bin`, identities `resource-proof-source.json` and `resource-proof-binaries.json`. No deployment. Real COS candidate idle traces, previous continuous-lease binary idle baseline and latest routing-safety acceptance are running serially under the bucket lock. Idle measurements use1KiB values, one keep-alive client per direct/forwarded/worker path, native workerd, two local agents, real HK COS and a verified fleet ACK; measured requests are not retried. This is not yet final repeated RPS acceptance.

### Same-environment idle comparison (one paired round)

Both runs used the same fixed native workerd, two local cell-agents on CPU0–3,1KiB values and real HK COS, with fleet ACK verified before measurement. Candidate ran before baseline, sequentially under the bucket lock; no compilation/tests overlapped measurement. Client signing happens before the measured HTTP interval. Each run has45 successful probes and45 matching traces; no measured request retries. These are individual samples, not percentile estimates or final RPS acceptance.

| Entry | Idle seconds | Baseline ms | Candidate ms | Baseline fence read ms | Candidate fence reads |
|---|---:|---:|---:|---:|---:|
| direct | 0 | 0.758 | 0.895 | 0.000 | 0 |
| direct | 1 | 16.036 | 1.096 | 14.715 | 0 |
| direct | 5 | 17.809 | 1.101 | 16.171 | 0 |
| direct | 10 | 24.154 | 1.628 | 21.409 | 0 |
| direct | 30 | 37.813 | 1.160 | 36.504 | 0 |
| forwarded | 0 | 1.070 | 1.138 | 0.000 | 0 |
| forwarded | 1 | 20.037 | 2.021 | 18.066 | 0 |
| forwarded | 5 | 17.368 | 1.934 | 15.145 | 0 |
| forwarded | 10 | 18.770 | 2.033 | 16.547 | 0 |
| forwarded | 30 | 19.378 | 1.942 | 17.243 | 0 |
| worker | 0 | 8.793 | 9.290 | 0.000 | 0 |
| worker | 1 | 9.705 | 10.232 | 0.000 | 0 |
| worker | 5 | 10.079 | 10.153 | 0.000 | 0 |
| worker | 10 | 11.238 | 10.599 | 0.000 | 0 |
| worker | 30 | 11.164 | 10.454 | 0.000 | 0 |

The baseline is `/tmp/cellhive-continuous-lease-20261110-bin`; candidate is `/tmp/cellhive-resource-proof-20261110-bin`. Worker baseline already has pinned authorization refresh and no foreground fence read, so this change does not show a meaningful worker-path improvement. Trace durations are inclusive, not additive. Both test namespace cleanups returned200. Wrangler subprocess testing was explicitly skipped because WRANGLER_CLI_PATH was not configured; it is not counted as acceptance.

Candidate latest routing-safety also passed: hk-review-1794259339-a51fb7-p9mrxwyb, forward GET/PUT/batch, invalid authorization, deletion/recreation and old-data absence. These binaries predate the subsequent review fixes for transient503 classification and guard-aware route eviction.

Latest reviewed build verification: full tagged Go suite, vet and five binaries passed (resource-proof-reviewed-{source,binaries}.json). Scoped re-review found no correctness blocker in transient 503 classification and pinned-route capacity handling; HTTP classification assertion remains a nonblocking test gap. Fixed binaries /tmp/cellhive-resource-proof-reviewed-20261110-bin passed real COS routing-safety including delete/recreate, old-data absence and cleanup200. Artifact hk-review-1794259728-0124bd-_6epq_38. These results do not cover forthcoming paging lifecycle changes.

### Reviewed proof build: two-round fleet throughput comparison

Local compute/client on CPU0–3, two agents, native workerd, real HK COS,1KiB, one hot cell. Baseline then candidate, each two rounds; fleet ACK verified, no measured request retry. Client concurrency1/8/32. Unrelated host processes were not stopped. This is not an interleaved statistical study.

| Path | Operation | Concurrency | Baseline RPS rounds | Candidate RPS rounds | Mean change |
|---|---|---:|---|---|---:|
| direct | put | 1 | [179.36, 173.16] | [171.08, 169.28] | -3.4% |
| direct | get | 1 | [2286.23, 2463.25] | [2273.76, 2421.2] | -1.1% |
| forwarded | put | 1 | [167.5, 166.88] | [168.91, 169.79] | 1.3% |
| forwarded | get | 1 | [1620.75, 1587.08] | [1538.35, 1557.87] | -3.5% |
| worker | put | 1 | [82.44, 84.11] | [80.66, 79.3] | -4.0% |
| worker | get | 1 | [167.73, 167.89] | [172.46, 168.74] | 1.7% |
| direct | put | 8 | [1368.36, 1290.71] | [1350.96, 1348.77] | 1.5% |
| direct | get | 8 | [5952.91, 5654.75] | [6001.78, 5510.39] | -0.8% |
| forwarded | put | 8 | [1197.92, 1234.03] | [1205.1, 1149.44] | -3.2% |
| forwarded | get | 8 | [4810.07, 4497.74] | [4838.19, 4551.75] | 0.9% |
| worker | put | 8 | [183.89, 162.29] | [198.03, 153.05] | 1.4% |
| worker | get | 8 | [192.13, 193.86] | [194.8, 196.18] | 1.3% |
| direct | put | 32 | [3336.64, 3503.17] | [3519.61, 3268.41] | -0.8% |
| direct | get | 32 | [6241.45, 5448.29] | [6028.07, 5785.42] | 1.1% |
| forwarded | put | 32 | [2809.06, 2668.38] | [2997.37, 2866.82] | 7.1% |
| forwarded | get | 32 | [5431.26, 5052.29] | [5291.31, 5045.09] | -1.4% |
| worker | put | 32 | [196.48, 196.77] | [197.78, 198.03] | 0.7% |
| worker | get | 32 | [195.17, 197.43] | [196.03, 198.55] | 0.5% |

All measured requests succeeded and values were checked. Detailed p99, request counts, artifact paths and limits: `artifacts/2026-11-10-optimization/resource-proof-reviewed-perf-pair.json`. Fixed builds: continuous-lease baseline and resource-proof-reviewed candidate. Neither result establishes acceptance of later paging lifecycle or cleanup validator changes. Wrangler subprocess testing was skipped.

Cleanup operation-level lease revalidation now tested: mandatory RetireOwnedResource/PurgeResource validators prevent renewed CAS attempts and subsequent payload deletes after durable token release. Server tests use separate control/data nodes and real ContinueResourceCleanup: conflict then loss leaves owner unretired; loss after successful publication rejects ACK while keeping admission closed; mid-purge loss leaves remaining payloads and job incomplete. Three-repeat race passed server1.723s/replica1.130s, artifact cleanup-authority-fault-race.log. Existing focused owner/retirement tests passed separately. This does not close the mutable wake pointer race; actual raw COS If-Match deletion reverified three times but content-ETag ABA requires a non-reused hint publication identity and candidate/authority ordering. No full newest-source acceptance or deployment.

Wake deletion repair in progress: deterministic same-minute/same-payload republication regression failed both pointer/due on old source (wake-delete-red.log). Existing wake JSON now carries fresh publication revision, no new files/LTX fields. All wake Delete/Put-old-due/Migrate/Cleanup deletes use captured ETag ConditionalDelete, never refresh-and-retry on conflict. Cleanup counts only actual removals. DeleteValidated captures all candidates before authority callback; production resource cleanup passes lease/owner callback and main timer normal delete checks local healthy owner, deleted-scope Forget rechecks authoritative retired owner. Full wake race3 PASS1.063s including reschedule/orphan cleanup and authority rejection. Current real COS wake race3 PASS3.611s, both pointer and due exact successor bytes preserved. Focused server/main/waker testsPASS; timer regex matchednone (doNOTcount as timerpass). Full related race runner79151 active /tmp/wake-delete-full-race.log affinity0-1GOMAX2. Review package /tmp/wake-delete-review.diff + brief /tmp/wake-delete-review-brief.md prepared, reviewer NOTdispatched yet. Need independent review. Existing delayed ordinary wake PUT race is separate, no claim of PUT CAS. Lifecycle agent implementing expanded loader/supervisor retirement; latest feedback root decision accepted; no current full native build. All seven objectives still active.

Wake follow-up verification: full related tagged race suite completed successfully (wake 1.039s, timer 1.252s, waker 1.092s, cell-agent 3.675s, server 61.265s); see `artifacts/2026-11-10-optimization/wake-delete-full-race.log`. Unsupported conditional deletion now safely defers hint reclamation and never falls back to unconditional deletion; focused three-repeat race passed 1.073s (`wake-provider-review-fix.log`). Independent scoped re-review accepted that provider compatibility correction. These checks do not validate the separate pre-existing ordinary delayed PUT publication boundary or the still-evolving native lifecycle implementation. The final-source COS/10GiB acceptance remains outstanding.

### Native lifecycle acceptance continuation

New fork binary `/tmp/cellhive-workerd-lifecycle-20261110` (SHA256 `f711d235ba3cbaa28e39b05cac684c17305a0348fa6f5a60594fd441b7943657`) built incrementally in21.207s. Full native SQLite suite passes46cases after updating old fixture response headers; log `artifacts/2026-11-10-optimization/native-lifecycle-kj-pass.log`. A scoped lifecycle review found and then accepted fixes for historical claims retention, queued capture resurrecting retired state, partial-restore guard reclamation, and an overly broad failed-admission disposal shortcut. Three-repeat negative-disposal/queued-capture race passes1.082s (`lifecycle-disposal-race.log`). Existing dirty/poisoned descriptors cannot acquire disposable partial-cache authority.

Real COS small native suite passes82.903s using this binary: checkpoint fail-stop and independent restore, cold read/recapture, and a query outliving its owner lease. All256 BLOB hashes and counters were checked after independent restore; two native checkpoints emitted zero additional snapshot bytes,17 canonical native LTX objects and no auxiliary storage markers. Log `native-lifecycle-cos-small-initial.log`. This run predates the subsequent render-only lifecycle binding correction, so it is intermediate evidence.

Real COS repeated loader retirement initially FAILED at the first post-retirement256-row counter query: the10s HTTP deadline expired after cache deletion. Initial scalar cold read was0.992s. Log `native-lifecycle-cos-retirement-initial.log`. Re-activation versus non-contiguous page I/O attribution is ongoing; this is not a passed lifecycle or final10GiB acceptance.
