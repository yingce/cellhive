# CellHive final acceptance — PASS

All CellHive edits are in `/opt/cellhive-worktrees/celld-ltx-migration`, branch `perf/celld-ltx-storage`, base `7710ef04d609fe135965b2e94c3c307f4004d1c3`. No commit or deployment. Compute is local, pinned to CPUs0–3; object storage is the real HK COS bucket `cellhive-e2e-hk-1376795072`. These are local-to-HK network observations, not production intra-VPC performance claims.

## Current evidence

| Requirement | Current result | Persistent evidence under `artifacts/` |
| --- | --- | --- |
| Native workerd paging build | PASS, `do-paging/1`; no new fork build in this final phase | binary SHA256 below |
| Actual native10GiB DO, cold point read, recapture and independent full restore | PASS, all163840 BLOB SHA256 values;9 canonical native LTX objects | `do-cold-1794233192852804669.log` |
| Real native fleet, ordinary proxy path | PASS, two crashes, epochs1→2→3, values10→11→12, all256 hashes after each cold takeover | `native-fleet-final-proxy.log` |
| Long synchronous native paging outlives lease | PASS,128MiB/2048 rows;54.056s/52.484s scans with30s lease; two child-only crashes, cleanupHTTP200 | `native-fleet-long-query-lease.log` |
| Independent lease negative control | FAIL as expected when only Go renewal is disabled: competitor acquired owner, request409 | `native-long-paging-negative.log` |
| Large native incremental over64MiB | PASS on same128MiB single transaction; streamed native L0, no transaction splitting | long-query fleet log; `native-delta-stream-tests.log` |
| Current official workerd DO snapshot/full restore | PASS, all256 hashes, recapture and9 canonical objects | `official-do-current-snapshot.log` |
| Full tagged Go tests with native runtime available | PASS | `final-audit-go-test.log` |
| Tagged vet, snapshot/paging/lease related race checks | PASS | `vet-native-file-lease.log`, `native-file-lease-race.log`, `independent-lease-race.log` |
| Latest exact dispatched-claim/gate/takeover checks | PASS | `native-final-claim-identity.log` |
|2000DO pressure | PASS26.375s, separately opt-in | `native-2000do-final.log` |
| Official review/queue/multinode/workflow/routing/batch recovery | PASS, all unique app cleanupsHTTP200 | `official-current-*.log`, `official-current-results.json` |
| SQLite export/import and plain old SQLite migration | PASS, export integrity,100 values,cold restore,post-import update,BLOB/index preservation | `official-current-cell-transfer.log` |
|100000-write crash/epoch/cold recovery | PASS,3 owner kills;100000/100000 online and cold; cleanupHTTP200 | `official-current-epoch-stress.log` |
| Fleet RPS/percentiles/errors/resources/Wrangler | PASS36 cases/153960 requests, zero errors; all3 actual runtime version checks | `official-current-performance-dual-retry.log`, `official-current-performance-summary.json` |

Native binary: `/opt/cellhive-workerd-fork/bazel-bin/src/workerd/server/workerd`, SHA256 `f9c2641c09e2a2b2fc3c69e0b73a85d7dca0e71723032fb9d6d65b40183cafcc`. Official runtime is1.20260916.1 via `/opt/cellhive/cli/node_modules/.bin/workerd` (read-only invocation). Final Go binaries are in this worktree's `bin/native-acceptance/`.

## Native performance and correctness

| Operation | Measured result |
| --- | --- |
|10GiB BLOB payload |10,737,418,240bytes; native snapshot10,853,709,188bytes |
| Initial10GiB capture+upload,32MiB multipart |364.457s |
| Cold index |1.248s |
| Cold restore/start/first query, includes index |2.170s;94208 local hydrated bytes |
| Selected64KiB BLOB read/hash |0.020s; no additional hydration |
| Independent complete10GiB restore |672.628s |
| Restore plus all163840 BLOB hashes |719.742s; recaptured changed value999 preserved |
| Go test process peak RSS sampled after materialization |418416KiB; not aggregate workerd/host RSS |
|16MiB real fleet full cold proof scans |5.258s/5.075s through cell-agent proxy |
|128MiB real fleet full cold proof scans |54.056s/52.484s through authenticated direct runtime |
| Child-only crash to first correct read |34s/32s; includes owner lease expiration |

The10GiB timings precede the later independent-renewal and large-increment transport changes; those paths are covered by the subsequent128MiB real fleet test. The10GiB fixture is effectively incompressible at SQLite page boundaries, unlike the earlier cellstore fixture that produced a787MB LTX. Full transfer times are not comparable across those fixtures. Hydrated bytes are local SQLite page bytes, not measured network traffic.

## Fixes and limits

- Initial DO snapshots no longer load/copy the entire SQLite image and encoded LTX in memory. Pages stream to an fsynced local spool, then upload with bounded buffers.
- Sequential missing-page reads prefetch up to128KiB without extending windows on cache hits, overwriting local dirty pages, or crossing truncation bounds.
- Supervisor renews the exact confirmed DO scope/node/epoch independently of synchronous SQLite. Release, renewal failure and child exit stop renewal. JS validates the exact dispatched claim and current expiry.
- Oversized native increments use streamed native L0 publication and bucket RPO=0 proof. Small increments retain fleet ACK; failed uploads retain the exact local spool until retry succeeds. WAL polling still holds transaction frames, so this is not a claim of constant memory for arbitrarily large transactions.
- COS keys/native headers/levels remain the existing single native LTX format. No new remote marker files. Multipart defaults32MiB and grows above10000-part capacity; single-PUT threshold remains5MiB.
- The cell-agent DO proxy retains its existing30s HTTP request deadline. Long-native proof requests deliberately use direct authenticated runtime HTTP; seed, mutation and takeover checks still pass through agents and real owner/fleet/gates. This is not public-proxy support for requests longer than30s.

## Completion audit

Fresh official-runtime suites all completed; terminal exit0 and cleanupHTTP200 verified for every suite. Performance first attempt failed on a startup port conflict and the complete retry passed (both retained). Fresh raw logs and performance/ledger artifacts are persistent. Earlier historical `/tmp` evidence is not used to claim the refreshed suite outcomes. Final tagged full Go test and vet recheck exited0 with native paging enabled; logs `artifacts/final-audit-go-test.log` and `artifacts/final-audit-go-vet.log`. Latest binaries match the recorded hashes. Evidence hashes: `artifacts/final-acceptance-evidence-manifest.json`. All scoped acceptance gates are closed.

Requirement mapping: official pinned runtime/functionality → review, queue, multi-node, workflow and routing logs; fleet durability/crash/epoch/cold correctness → batch and100k ledgers; export/import/legacy SQLite → transfer log; native build/capability → binary identity plus native paging executions;10GiB/native format → real COS full-hash/object inventory log; native fleet/long lease → ordinary proxy and128MiB child-only-crash logs; throughput/resources →36-case JSON and tables below. Test-only deployment/cleanup is confined to unique namespaces in the dedicated E2E bucket; no production deployment or Git commit.

Boundaries retained: full10GiB transfer remains672.628s; public DO proxy deadline30s; arbitrarily large WAL transaction capture still scales with transaction size; RPS samples are short and local-loopback, not production network capacity or controlled historical speedup. None of these are represented as solved by this acceptance.


Current official functional refresh: review/queue/multinode149.936s, workflow/recovery149.957s, routing-safety35.064s, batch-recovery64.360s, all exit0. Workflow verifies stale callbacks rejected,16 concurrent waits consume distinct events, and kill-after-durable-consume/before-response restores output42 in fresh directories. Batch recovery verifies100/100 acknowledged values after crash and after local-cache removal; cold firstGET0.835523s. The official runtime is1.20260916.1; both batch recovery and routing use two local agents with real COS and durability auto.

Final Wrangler performance checks pin the fixture compatibility date to2026-09-16. Every deploy must serve its unique per-round worker variable through real HTTP, in addition to active-version pointer checks. This avoids accepting a stale or unexecutable activated worker.

Fresh official100k stress completed in916.310s (writer817.0s). Owner kills occurred after25040,50050,75063 acknowledged writes. Both100000-key value comparisons passed; cold firstGET2.642489s after stopping both nodes and removing the survivor cache. Verification used32 threads, writes one sequential client with retries of identical key/value; this is a durability stress test, not saturation RPS. Persistent acknowledged ledger (gzip), SHA256/count and writer progress are under `artifacts/hk-review-1794236858-d15bde-uqzgnkrd/0/`.

Performance attempt1 failed before measurements: official workerd could not bind public port34702 (`Address already in use`), see `artifacts/hk-review-1794237774-a7c93c-_dw4fyzs/0/runtime-startup-failure.log`. No conflicting listener remained at investigation time; exact prior port holder is unproven. Port34702 lies within host ephemeral range32768–60999. Attempt2 uses checked-free26700–26705 outside that range, same binaries/workload/bucket, and retains both attempts in the result ledger.

## Fresh official fleet performance

Two local agents and official workerd on loopback; real HK COS; all test processes pinned to CPUs0–3. Direct owner `127.0.0.1:26700`, forwarded entry `127.0.0.1:26704`, worker public `127.0.0.1:26702`. Unique namespace `hk-review-1794237860-db0f64-0`. No new compiler workload. Shared host, not an isolated capacity ceiling.

One Node process runs1/8/32 concurrent asynchronous request loops with persistent HTTP connections. Values1KiB across32 keys. Two rounds; PUT samples60/200/400 per case, GET5000/10000/10000. These are short samples, not a sustained soak. GET bodies are compared exactly. HMAC tokens generated per request; only signing key objects cached. Percentiles cover request through complete response body; RPS also includes client token/preparation overhead.

PASS36 warm cases, 153960 requests, zero failures. All18 PUT cases recorded positive fleet commit deltas, total842; no bucket-batch deltas. Group commits mean commit count differs from request count.

| Path | c | RPS round1 /2 | p50 ms round1 /2 | p95 ms round1 /2 | p99 ms round1 /2 |
| --- | ---: | ---: | ---: | ---: | ---: |
| direct GET | 1 | 2046.01 / 2385.7 | 0.412 / 0.352 | 0.557 / 0.507 | 0.984 / 1.238 |
| direct PUT | 1 | 168.59 / 168.75 | 5.738 / 5.621 | 6.702 / 7.796 | 7.511 / 8.341 |
| forwarded GET | 1 | 1416.63 / 1538.46 | 0.603 / 0.575 | 0.965 / 0.75 | 1.817 / 1.528 |
| forwarded PUT | 1 | 136.29 / 160.87 | 6.44 / 6.092 | 7.489 / 7.256 | 51.388 / 8.343 |
| worker GET | 1 | 513.04 / 498.11 | 1.756 / 1.726 | 2.626 / 2.764 | 4.372 / 4.084 |
| worker PUT | 1 | 124.46 / 119.21 | 7.678 / 8.078 | 9.918 / 9.996 | 11.81 / 10.523 |
| direct GET | 8 | 5350.65 / 5693.4 | 1.231 / 1.149 | 2.773 / 2.54 | 3.922 / 3.651 |
| direct PUT | 8 | 1147.08 / 1372.52 | 6.507 / 5.675 | 10.258 / 7.717 | 13.093 / 8.483 |
| forwarded GET | 8 | 4513.3 / 4416.64 | 1.58 / 1.583 | 3.114 / 3.312 | 4.274 / 4.499 |
| forwarded PUT | 8 | 1177.44 / 1083.25 | 6.312 / 6.454 | 10.305 / 12.522 | 12.094 / 13.646 |
| worker GET | 8 | 823.16 / 817.36 | 8.728 / 8.914 | 14.72 / 14.162 | 26.252 / 31.035 |
| worker PUT | 8 | 638.66 / 615.51 | 11.622 / 11.548 | 19.216 / 18.013 | 19.984 / 42.198 |
| direct GET | 32 | 5779.89 / 6187.54 | 4.896 / 4.626 | 10.137 / 9.496 | 13.688 / 12.203 |
| direct PUT | 32 | 2615.93 / 2830.5 | 11.047 / 10.606 | 17.396 / 16.727 | 20.631 / 18.459 |
| forwarded GET | 32 | 5187.14 / 5090.7 | 5.408 / 5.537 | 11 / 11.491 | 13.874 / 15.085 |
| forwarded PUT | 32 | 1808.82 / 2522.15 | 13.792 / 12.109 | 37.316 / 17.579 | 46.807 / 22.056 |
| worker GET | 32 | 803.85 / 773.69 | 40.219 / 41.194 | 57.105 / 63.247 | 79.104 / 111.149 |
| worker PUT | 32 | 848.71 / 816.99 | 38.52 / 34.162 | 49.423 / 55.273 | 55.373 / 58.887 |

Resource samples after traffic; CPU is process-lifetime ps percentage, not per-case utilization. Peaks are per process, not simultaneous aggregate.

| Process | PID | RSS KiB | Peak RSS KiB | CPU % |
| --- | ---: | ---: | ---: | ---: |
| cell-agent | 1024685 | 58136 | 67012 | 75.4 |
| cell-agent | 1025226 | 52232 | 57732 | 22.1 |
| user-runtime | 1025770 | 18588 | 18588 | 0.0 |
| workerd | 1025783 | 378216 | 409676 | 63.7 |

Wrangler4.133.0: all3 uploads retained the previous active version; all3 deployments activated the returned version and served the unique marker through actual worker HTTP. Subprocess times exclude the subsequent verification reads.

| Round | Upload ms | Deploy ms |
| --- | ---: | ---: |
| 1 | 1841.06 | 1106.06 |
| 2 | 1227.16 | 1057.58 |
| 3 | 1138.69 | 1100.38 |

Raw results/resources: `artifacts/official-current-performance-summary.json`; terminal runner log: `artifacts/official-current-performance-dual-retry.log`. Runtime/agent metrics and per-request-group JSONL are preserved in the case directory. Historical measurements used different builds/time windows; no controlled before/after speedup claim.
