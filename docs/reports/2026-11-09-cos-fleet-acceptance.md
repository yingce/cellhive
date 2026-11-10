# COS/fleet acceptance chronology

Current acceptance status and persistent evidence: [final acceptance report](2026-11-09-final-acceptance.md). This document preserves the execution history; earlier tables and pending/failure statements describe their respective checkpoints, not current status. Historical `/tmp` logs did not survive environment replacement.

Runtime: official workerd1.20260916.1, current Go binaries. Local compute processes, COS bucket cellhive-e2e-hk-1376795072. Shared-bucket suites serialized; unique application namespaces removed after each successful run. No production deployment.

| Suite | Current result | Evidence |
| --- | --- | --- |
| R2 overwrite/metadata/range, D1 20k rows, queue concurrency | PASS | /tmp/cos-fleet-official-review.log |
| Queue retry/DLQ, multi-node owner failure | PASS | same review log |
| Workflow sleep/timeout/retry/bindings | PASS | /tmp/cos-fleet-official-workflow.log |
| Workflow stale callback rejection,16 concurrent waits, crash after durable event before response, fresh-dir restore | PASS | same workflow log |
| Fleet routing/auth/deletion/recreation | PASS | /tmp/cos-fleet-official-routing-safety.log |
| Fleet batch crash recovery and cold COS restore | PASS100/100 twice; cold first GET2.822433s | /tmp/cos-fleet-official-batch-recovery.log |
| Official DO full cold restore, point read, changed-value recapture, independent third-disk restore (16MiB random BLOB fixture) | PASS6.84s; cold restore/start/read1.587s | /tmp/official-do-cold-cos.log |
| Same fixture, all256 BLOB SHA256 values | Filesystem PASS2.22s; COS PASS7.95s | /tmp/official-do-full-sha-test.log; /tmp/official-do-cold-cos-sha.log |
|100k epoch stress | PASS100000/100000 hot and cold | /tmp/cos-fleet-official-epoch-stress.log |
|10GiB indexed read and full audit | PASS; first row0.808s | /tmp/cos-fleet-10g-paged.log |
|10GiB full restore, SQLite integrity, all10240 row hashes | PASS; materialize100.043s | /tmp/cos-fleet-10g-full.log |
| Fleet performance including Wrangler | PASS36 warm cases and3 Wrangler upload/deploy rounds | /tmp/cos-fleet-official-performance-complete.log |
| General SQLite binary export/import and plain SQLite migration | PASS including cold restore and cleanupHTTP200 | /tmp/cos-cell-transfer.log |
| Native paged DO COS | new fork PASS, 16MiB fixture; first cold query 1.161s, 88KiB hydrated; see latest section | /tmp/native-do-paging-cos.log |

Timing includes the named operation and uses local compute against remote COS. Native fork compilation is concurrent during functional runs; do not use these timings as an uncontended performance comparison. Record uncontended fleet measurements separately.

## Verified updates

Official-runtime100k epoch stress PASS:3 owner SIGKILLs at25067,50001,75008 acknowledged writes, final100000/100000 read verification, then both nodes stopped, survivor SQLite cache removed,100000/100000 cold COS verification. Cold first GET2.894032s; namespace cleanupHTTP200. Log /tmp/cos-fleet-official-epoch-stress.log. The workload is one writer with failover retries and32 verification workers, not an RPS saturation benchmark.

10GiB indexed cold query PASS: logical SQLite10,751,451,136 bytes, actual inserted BLOB10,737,418,240 bytes, encoded LTX787,262,031 bytes. First verified row0.808s, subsequent sampled rows0.134s/0.078s, repeat row0.003s with zero COS calls. Local allocated bytes3,182,592 before audit. Post-query full native audit49.709s,753 ranges/787,262,590 bytes, cut42 and changed marker correct. Test357.73s; outer process maximum RSS421632KiB (includes Go tool), test-reported maxRSS351924KiB. Log /tmp/cos-fleet-10g-paged.log. Fork compilation suspended during these measurements; this is cellstore/COS paging, not native workerd10GiB paging. The separate full materialization run also passed; see full recovery results below.

Official native DO real-COS16MiB fixture SHA-enhanced PASS7.95s; cold full-restore/start/first-read2.432s; independent third-disk restore preserves the changed row and all256 random64KiB BLOB SHA256 values. Log /tmp/official-do-cold-cos-sha.log.

SQLite transfer COS functional case PASS: export integrity and100 values, import, duplicate rejection, post-import write, process drain plus local-cache removal,100 values cold-restored, re-export integrity, plain SQLite migration preserving BLOB and index. /tmp/cos-cell-transfer.log; artifacts /tmp/hk-review-1794211708-4bb068-wu2zf95c. Runner terminal result PASS with namespace cleanupHTTP200 and exit0.

## Initial fleet throughput (superseded by complete rerun below)

Two local agent processes, local HTTP, real HK COS, official workerd1.20260916.1; durability auto with fleet ACK deltas checked. Closed-loop Node generator,32 keys,1KiB values; concurrency is asynchronous loops in one Node process, not OS threads. Two rounds at concurrency1/8/32;36 warm cases, zero errors. GET counts5000 at c1 and10000 at c8/c32; PUT60/200/400 respectively.

| Path, c32 | Round1 RPS | Round2 RPS | p99 ms, round1/2 |
| --- | ---: | ---: | ---: |
| Direct GET |6394.49|6112.05|12.518 /11.494|
| Forwarded GET |5420.19|5726.38|13.948 /11.926|
| Worker GET |823.37|812.82|111.641 /115.440|
| Direct PUT |3807.92|3254.92|14.205 /16.241|
| Forwarded PUT |3255.86|3101.57|16.681 /20.472|
| Worker PUT |886.35|866.31|51.273 /72.404|

Evidence: /tmp/cos-fleet-official-performance-dual.log. These short cases are not a sustained capacity limit or a controlled before/after comparison. Worker HTTP overhead is visible; this report does not establish its cause. Initial Wrangler stage was skipped; complete rerun explicitly supplies Wrangler4.133.0 and records descendant workerd resources. The complete rerun results appear below.

## Full10GiB recovery acceptance

PASS, exit0: /tmp/cos-fleet-10g-full.log, TestCOSLargeSQLiteStreaming601.34s, total10:04.69 including fixture creation/upload/verification. Full-chain proof50.661s, native page index0.841s, materialize100.043s (1285ranges/761434665bytes). SHA256, SQLite integrity_check and all10240 row hashes verified; local verification154.776s with no COS reads. Restored image SHA256 b390dd5cf30deb59efb6a4761836aa67e652928ee9d2cdb68d759c6a2850021c, cut42. Test maxRSS344052KiB; outer Go tool maxRSS420584KiB. Scratch peak increase11662655488bytes, sampled1s on shared filesystem. L9 logical-size budget correctly skips optional rewrite and preserves the restorable chain. Full recovery and integrity checks are distinct from the0.808s on-demand first query.

## Complete fleet performance rerun

PASS36 warm cases, zero errors; Wrangler4.133.0 ran all3 upload/deploy rounds. Upload retained active version; deploy activated the returned version each round. CleanupHTTP200, runner exit0. Same topology/workload as above, latest Go binaries in /tmp/cellhive-transfer-bin. Native fork compiler suspended throughout. Host is shared with unrelated processes; values are observations, not an isolated-host throughput ceiling.

| Path | Concurrency | RPS round1/2 | p50 ms round1/2 | p95 ms round1/2 | p99 ms round1/2 |
| --- | ---: | ---: | ---: | ---: | ---: |
| direct GET | 1 | 2148.91 / 1526.52 | 0.399 / 0.415 | 0.558 / 1.371 | 0.767 / 3.783 |
| direct PUT | 1 | 158.15 / 129.56 | 5.894 / 6.438 | 7.317 / 11.648 | 11.63 / 31.536 |
| forwarded GET | 1 | 1611.98 / 1011.84 | 0.554 / 0.682 | 0.71 / 2.132 | 1.029 / 4.643 |
| forwarded PUT | 1 | 154.27 / 104.1 | 5.705 / 8.489 | 7.369 / 13.229 | 39.212 / 37.751 |
| worker GET | 1 | 508.29 / 507.46 | 1.75 / 1.753 | 2.704 / 2.475 | 4.831 / 3.34 |
| worker PUT | 1 | 127.43 / 88.42 | 7.566 / 8.364 | 9.003 / 10.41 | 11.86 / 164.425 |
| direct GET | 8 | 5899.87 / 6144.46 | 1.113 / 1.044 | 2.336 / 2.256 | 3.015 / 3.281 |
| direct PUT | 8 | 1194.03 / 1146.81 | 6.193 / 6.75 | 10.016 / 8.844 | 12.268 / 10.352 |
| forwarded GET | 8 | 4732.94 / 4728.6 | 1.503 / 1.485 | 3.097 / 3.179 | 4.65 / 4.789 |
| forwarded PUT | 8 | 1132.48 / 1027.14 | 6.534 / 7.724 | 10.176 / 9.895 | 11.244 / 11.848 |
| worker GET | 8 | 811.04 / 761.01 | 8.958 / 9.396 | 15.177 / 16.682 | 24.033 / 32.881 |
| worker PUT | 8 | 608.74 / 594.99 | 12.962 / 12.97 | 18.401 / 19.593 | 20.566 / 20.405 |
| direct GET | 32 | 6246.71 / 5647.12 | 4.697 / 4.959 | 8.933 / 10.126 | 10.383 / 14.539 |
| direct PUT | 32 | 3410.99 / 3142.52 | 8.952 / 9.012 | 13.683 / 14.789 | 16.714 / 19.522 |
| forwarded GET | 32 | 4955.42 / 4739.61 | 5.422 / 5.609 | 11.598 / 12.176 | 15.417 / 18.639 |
| forwarded PUT | 32 | 2899.85 / 2606.53 | 10.184 / 11.109 | 16.71 / 20.546 | 19.459 / 22.387 |
| worker GET | 32 | 453.67 / 735.48 | 51.192 / 42.22 | 188.797 / 67.177 | 399.334 / 121.424 |
| worker PUT | 32 | 817.21 / 743.86 | 36.935 / 39.984 | 56.939 / 64.907 | 62.831 / 71.832 |

Resource sample after workload; CPU is ps lifetime percent, not per-case CPU. VmHWM is process peak RSS.

| Process | RSS KiB | Peak RSS KiB | Lifetime CPU % |
| --- | ---: | ---: | ---: |
| cell-agent PID734863 | 52000 | 71996 | 73.3 |
| cell-agent PID735744 | 39660 | 54004 | 22.5 |
| user-runtime PID736278 | 11580 | 17280 | 0.0 |
| workerd PID736333 | 404380 | 412652 | 59.0 |

Wrangler subprocess wall times (ms):

| Round | Upload | Deploy |
| --- | ---: | ---: |
| 1 | 1691.43 | 1083.06 |
| 2 | 1244.31 | 1084.55 |
| 3 | 1182.93 | 1105.31 |

Raw evidence: /tmp/cos-fleet-official-performance-complete.log; artifacts /tmp/hk-review-1794212497-27e732-9aeu_mnm; process/data directory /tmp/cellhive-e2e.DR3qlM.

## Object format audit and final Go checks

DO fixture additionally inventories only its test namespace after full cold restore, update/recapture and independent third-disk restore. All9 database objects match native epoch/level/TXID filenames and parse as native100-byte LTX headers; no auxiliary storage markers. Official filesystem PASS2.36s (/tmp/official-do-object-audit.log); real COS PASS5.71s, cold1.414s (/tmp/official-do-cos-object-audit.log). All256 random BLOB SHA256 values remain identical. This inventory List belongs only to the acceptance test.

Final tagged whole-repository go test and go vet EXIT0 (/tmp/cos-fleet-final-go-test.log, /tmp/cos-fleet-final-go-vet.log), with unchanged packages cached. Subsequent fixture changes add the object inventory assertion and a complete64KiB BLOB read before recapture; both are covered by the latest official filesystem/COS runs below. The whole-repository checks predate these fixture-only additions. Both repositories git diff --check clean. Native binary build remains live; do not count fork paging E2E as passed yet.

Native SQLite unit binary fresh rerun:37/37 PASS, exit0, /tmp/do-paging-native-sqlite-rerun.log. Compiled after current sqlite-paging.h/sqlite.c++/sqlite-test.c++ edits. Server build and native DO integration gates remain pending.

## Reproduction and build identity

Checkout: /opt/cellhive, branch perf/large-cell-bounded-storage, base commit7710ef04d609fe135965b2e94c3c307f4004d1c3 plus the uncommitted worktree changes. No commit or deployment performed. Go1.27.1, linux/amd64. Tags: sqlite_fts5 sqlite_dbstat sqlite_math_functions sqlite_column_metadata sqlite_preupdate_hook.

SHA256 of binaries used by the complete performance run and transfer suite:

| Binary | SHA256 |
| --- | --- |
| official workerd1.20260916.1 | fcaac1783cbd1cafd6e400ba73250aca04464cf4222cd9288404358fec7d5c29 |
| /tmp/cellhive-transfer-bin/cell-agent | 8f9245e1d330bfa1598cfc2829f8f4bcbee5f44522d2269dc0e5428f23215b92 |
| /tmp/cellhive-transfer-bin/user-runtime | d57bc57aee67e5189126a760eb04ffa27be816a12e378beab93f8d8c0403e9a8 |
| /tmp/cellhive-transfer-bin/cellhive | 2541083044ccc09bf1ab856059986391f1a12cdbb981dcfcf3f6be79f8c23232 |

Commands from repository root (credentials are loaded by the runner, not command-line arguments):

```bash
# Choose each suite: review workflow routing-safety batch-recovery epoch-stress cell-transfer
E2E_BIN=/tmp/cellhive-transfer-bin python3 scripts/e2e/hk-review.py --credentials /opt/.secrets/s3 --workerd /opt/cellhive/cli/node_modules/.bin/workerd --suite cell-transfer --port-base 31900
WRANGLER_CLI_PATH=/root/.npm/_npx/d524a3d443688c4c/node_modules/wrangler/bin/wrangler.js E2E_BIN=/tmp/cellhive-transfer-bin python3 scripts/e2e/hk-review.py --credentials /opt/.secrets/s3 --workerd /opt/cellhive/cli/node_modules/.bin/workerd --suite performance-dual --port-base 32000
python3 scripts/e2e/large-cell-cos.py --credentials /opt/.secrets/s3 --payload-mib 10240 --scratch /tmp --partial-query
python3 scripts/e2e/large-cell-cos.py --credentials /opt/.secrets/s3 --payload-mib 10240 --scratch /tmp
python3 scripts/e2e/do-cold-cos.py --workerd /opt/cellhive/cli/node_modules/.bin/workerd
```

Earlier functional suites used /opt/cellhive/bin before the transfer endpoints were added; these suite results do not verify the later transfer code. The transfer suite and complete performance rerun used the separate latest binaries above. Native paging commands and binary identity will be recorded only after the new fork build and tests finish.

The cold DO fixture now also reads and SHA256-checks one complete64KiB BLOB before recapture. The previous scalar/length query alone could avoid SQLite overflow pages. The fork mode asserts total hydration stays below4MiB after this payload read. Official full-restore baseline freshly passed filesystem2.90s and COS6.21s (/tmp/official-do-cold-blob.log, /tmp/official-do-cos-cold-blob.log); COS restore/start/scalar read1.596s and subsequent BLOB read0.003s. These official figures do not prove native sparse paging; the pending fork gates must use the same strengthened fixture. The earlier watcher processes were lost with the environment replacement and are not active.

Execution environment changed after the above tests: the original build and dependent watchers disappeared without producing native server acceptance results. A new offline build uses a copied writable /tmp cache. Current sandbox rejects local listener creation with EPERM and blocks external network access; earlier COS/official-runtime results remain historical evidence, not a successful native paging run.


## Remaining native acceptance commands (not yet executed)

Use only the newly linked server after the offline Bazel command exits successfully. These commands require an environment that permits local HTTP listeners; the COS step additionally requires access to the dedicated HK bucket. Do not reuse the old watcher scripts or their old binary path.

```bash
native_workerd=/tmp/cellhive-native-bazel-resume/execroot/_main/bazel-out/k8-opt/bin/src/workerd/server/workerd
"$native_workerd" --version
"$native_workerd" cellhive-build-info # must contain the exact do-paging/1 line
sha256sum "$native_workerd"

# Filesystem-backed native DO cold read, overflow BLOB, update and recapture.
CELLHIVE_WORKERD="$native_workerd" CELLHIVE_DO_PAGING_TEST=1 CELLHIVE_DO_COS_TEST=0 go test -p 2 -tags 'sqlite_fts5 sqlite_dbstat sqlite_math_functions sqlite_column_metadata sqlite_preupdate_hook' ./internal/doruntime -run '^TestNativeDOPagedColdReadAndRecapture$' -count=1 -v -timeout=8m

# Same native fixture against real COS, with namespace object inventory.
python3 scripts/e2e/do-cold-cos.py --workerd "$native_workerd" --paged

# Full native runtime/supervisor regression after both focused gates pass.
CELLHIVE_WORKERD="$native_workerd" CELLHIVE_DO_PAGING_TEST=1 CELLHIVE_DO_COS_TEST=0 go test -p 2 -tags 'sqlite_fts5 sqlite_dbstat sqlite_math_functions sqlite_column_metadata sqlite_preupdate_hook' ./internal/doruntime ./internal/dosupervisor -count=1 -v -timeout=20m
```

The focused native fixture is16MiB with a stub owner and manual capture. It does not prove native workerd10GiB paging or real-agent fleet ownership; the separate cellstore10GiB and official fleet tests above cover those respective paths only.


## User-selected deploy binary acceptance

The user requested stopping compilation and testing `/opt/cellhive/deploy/workerd-fork/workerd` directly. Offline build session3646 was interrupted and exited130; do not restart it without a new user instruction. This supersedes the pending new-build instructions above.

Selected binary reports `workerd 2026-09-16`, build info `cellhive-workerd-fork/0`, SHA256 `63009b9b507a7b4fefb77de9ecb4ef87252476783da96054502965fbb984760c`.

Focused tests actually executed with this binary, exit1, evidence `/tmp/deploy-workerd-do-acceptance.log`:

- `TestNativeDOPagedColdReadAndRecapture` failed the required `do-paging/1` capability check.
- `TestDOFullColdReadAndRecapture` failed before runtime startup because `httptest` could not open a local socket: `operation not permitted`.

Neither is a passing native acceptance result. The environment's repeated local-listener restriction now prevents further DO E2E execution, and the supplied binary cannot satisfy the native paging capability gate. Previous official/COS successes remain unchanged historical evidence.


## Deploy binary investigation after network access restored

Local listener probe now succeeds. With the same selected binary (SHA256 above), both focused full-restore gates were rerun and exited0:

- Filesystem `TestDOFullColdReadAndRecapture`: PASS2.04s; cold restore/start/point read0.557s, subsequent BLOB read0.002s. Evidence `/tmp/deploy-workerd-full-cold-retest.log`.
- Real HK COS `TestDOCOSColdReadAndRecapture`: PASS8.40s; cold restore/start/point read1.873s, subsequent BLOB read0.003s. Evidence `/tmp/deploy-workerd-cos-cold-retest.log`.
- Both verify all256 BLOB SHA256 values after independent restore and9 canonical native LTX objects without auxiliary storage markers.

Paging investigation confirms an actual missing runtime feature, not only missing metadata. The selected binary contains none of `do-paging/1`, `CELLHIVE_DO_PAGING_URL`, `CELLHIVE_DO_PAGING_TOKEN`, or `.cellhive-paging`. A temporary Go overlay removed only the capability assertion for diagnosis; repository tests were unchanged. Forced sparse restore then crashed the runtime with `SQLITE_NOTADB` and failed the health check, exit1 after25.80s. Evidence `/tmp/deploy-workerd-paging-diagnostic.log`; overlay `/tmp/paging-diagnostic-overlay.json`. This diagnostic failure is not an accepted fallback or a passing paging test. The ordinary full-restore path is functional with this binary. No workerd compilation was restarted.


Runtime/supervisor full regression with the selected deploy binary exited0:93 top-level tests passed,4 skipped initially, evidence `/tmp/deploy-workerd-runtime-regression.log`. The skips were individually investigated rather than counted as passes:

- COS cold restore was separately enabled and passed above.
- Memory stress was separately enabled at the default2000 DO instances and passed24.34s, exit0, `/tmp/deploy-workerd-memory-stress.log`. Its RSS sampler aggregates all host workerd processes and cgroup data was absent; these samples are not isolated memory measurements.
- `TestPagedRestoreUsesRangedReads` incorrectly skipped because its counting wrapper embedded only `bucket.Bucket`, hiding the underlying FSBucket optional Stat/streaming interfaces. Changed the skip into a hard failure, reproduced `bucket: not supported` (`/tmp/deploy-workerd-range-red.log`), then embedded concrete FSBucket in this test wrapper. The ranged-read test and full dosupervisor package now pass with no skips, exit0 (`/tmp/deploy-workerd-range-green.log`). No production storage behavior changed.
- Native paged DO remains unsupported by this selected binary, with the separate actual SQLITE_NOTADB diagnostic above. It is not counted as passed.


## 2026-11-09 新版原生 workerd 验收（本轮新证据）

此前构建/环境阻塞已解除。4 核 Bazel 构建退出成功，11846/11846 actions，elapsed 4169.099s。新二进制 `/opt/cellhive-workerd-fork/bazel-bin/src/workerd/server/workerd`，SHA256 `f9c2641c09e2a2b2fc3c69e0b73a85d7dca0e71723032fb9d6d65b40183cafcc`，build-info 包含 `do-paging/1`。没有替换部署二进制。

测试从 `/opt/cellhive-worktrees/celld-ltx-migration` 执行，CPU 0–3；真实 COS 使用 `cellhive-e2e-hk-1376795072`。

| 测试 | 本轮结果 | 日志 |
| --- | --- | --- |
| 原生 DO 分页 FS | PASS，冷启动+首次查询 0.584s，hydrate 90112B，完整 64KiB BLOB 校验 0.002s | `/tmp/native-do-paging-fs.log` |
| 原生 DO 分页 COS | PASS，冷启动+首次查询 1.161s，hydrate 90112B，BLOB 校验 0.003s | `/tmp/native-do-paging-cos.log` |
| 新 fork 普通全量恢复 COS | PASS，冷启动+首次查询 1.862s，BLOB 校验 0.003s | `/tmp/native-do-full-cos.log` |
| runtime/supervisor 全包 | PASS，95 个顶层测试通过，2 个 opt-in 跳过，已分别补跑 COS 和压力测试 | `/tmp/native-do-paging-regression.log` |
| 2000 DO 压力 | PASS，25.04s；只证明本用例请求成功，不证明长期无内存泄漏。日志 RSS 汇总所有 workerd 进程，不能当成单进程内存 | `/tmp/native-do-paging-stress.log` |
| 默认32MiB COS multipart/compaction | PASS，9.077s；不可变重试、metadata、L1/L2/L3/L9、跨 epoch HTTP 恢复和完整 SQLite SHA 一致 | `/tmp/native-cos-default32-stream.log` |

三个 DO 恢复用例均检查写入后 recapture、第三个独立目录完整恢复、全部 256 条 BLOB SHA256；对象 inventory 为 9 个 canonical native LTX，无辅助标记文件。Fixture 约 16MiB，stub owner、手动 SyncAll，不能描述为真实 fleet 自动选主或 10GiB 原生 workerd 验收。COS multipart 用例约 8MiB，其一个尾部分片小于32MiB；多分片32MiB吞吐及完整内容校验另见 multipart benchmark 报告。

剩余：10GiB 的现有结果属于 cellstore/replica 分页，原生 workerd 10GiB 及真实 fleet 分页 owner 切换仍未覆盖；不得将本轮小库成功泛化。历史日志中“缺少分页二进制”“构建未完成”描述是历史状态，已由本节取代。未提交、未部署。


## 10GiB 原生 DO / fleet 后续测试入口（尚未通过验收）

环境切换导致原生10GiB测试中断：最后观察到163840条64KiB BLOB写入并完成源校验（10,737,418,240字节、99.027秒）。进程句柄71902及临时日志已丢失，没有完整冷读/恢复结果，不计PASS。新版workerd二进制仍存在且SHA256与上节一致。

当前已补齐：

- `scripts/e2e/do-cold-cos.py --workerd <新fork> --large`：分批写入/校验10GiB，实际DO分页冷读、写入recapture、独立全量恢复及全部BLOB校验。默认日志持久写入`docs/reports/artifacts/`并逐行fsync；测试先记录namespace，分别报告初始capture、索引恢复、首次读和完整恢复/审计耗时。监听不可用时在接触COS之前退出。该fixture为重复随机块，逻辑数据量不能冒充编码上传量；hydrated_bytes为SQLite补读量，不是COS网络字节数。
- `scripts/e2e/hk-review.py --credentials /opt/.secrets/s3 --workerd <新fork> --suite do-paging-fleet`：新增待实跑套件，两个真实cell-agent、两个gated runtime；A→B→空磁盘A两次崩溃接管、真实owner epoch递增、fleet commit指标、稀疏数据库检查、全部256条BLOB SHA以及接管后的计数写入。runtime环境移除COS/AWS桶凭证，持久化经cell-agent门禁；不重试修改请求。使用独立namespace/capture prefix并沿用HK cleanup。

新增fleet脚本仅完成shell/JS/Python语法检查；10GiB Go入口编译检查不执行测试。当前执行环境本地bind报`EPERM`，写权限虽已恢复，真实E2E仍不可运行。不得将上述入口当成已完成的fleet/大库验收，也不得将旧owner-takeover或KV batch-recovery的PASS替代原生DO分页证明。未来恢复监听及COS网络权限后优先执行这两条命令并直接修复失败。

后续静态审查：fleet断言已改为两agent在本轮DO工作负载期间的fleet commit计数增量，并额外要求DO输出门禁确有捕获字节，避免仅凭历史累计计数判定；这仍是进程级指标，最终还须结合接管与内容校验验收。大库fixture的源runtime增加幂等defer清理，避免seed/哈希失败后泄漏测试进程。Go编译、shell语法和diff检查通过，实际监听仍被EPERM拒绝，没有新增E2E通过结果。


## 2026-11-09 fresh native acceptance (active)

Environment listener/COS restrictions have been removed; older environment-blocked notes above are superseded. Current checkout is `/opt/cellhive-worktrees/celld-ltx-migration`, branch `perf/celld-ltx-storage`; all new edits and Go binaries are from this tree.

Actual native 10GiB DO run exposed unbounded initial snapshot memory (>10GiB RSS); it was stopped before host OOM. The streaming spool/HTTP snapshot correction passed focused snapshot tests and a real COS 16MiB run (`artifacts/do-cold-1794233132824780924.log`). A fresh 10GiB run is active (`artifacts/do-cold-1794233192852804669.log`), using 32MiB multipart. Do not count it as passed before the independent restore and full BLOB hash checks finish.

Real two-agent/two-runtime native fleet test failed during the first takeover's full BLOB proof scan with HTTP409; renewal HTTP requests timed out during synchronous SQLite paging (`artifacts/native-fleet-run.log`). Sequential missing-page reads now prefetch at most128KiB, retaining random point-read behavior, local dirty pages and truncation bounds. The new regression first failed on the old exact-page implementation and now passes; real fleet rerun is queued in `artifacts/native-fleet-readahead.log`. This does not yet establish lease safety for arbitrary long synchronous queries.

Fresh checks after sequential prefetch: tagged `go test -p 2 ./...`, tagged `go vet -p 2 ./...`, and targeted race tests for dosupervisor/sqlcapture/server all exited0. Logs: `artifacts/full-go-20261109.log`, `artifacts/vet-20261109.log`, `artifacts/snapshot-paging-race.log`. All commands pinned to CPU0–3.

10GiB live stage results (not final acceptance): initial capture364.457s, encoded snapshot10,853,709,188bytes; restore index1.248s; first cold query2.170s,94,208hydrated bytes; selected BLOB hash0.020s with no additional hydration. These bytes describe local page hydration, not measured wire traffic. Independent full restore/hash audit still active. This run predates the new sequential-prefetch change.

Additional stream owner fence regression passed under race: a valid DO lease released during snapshot body transfer causes rejection and publishes no LTX (`artifacts/snapshot-do-fence-race.log`). Native workerd filesystem regression with the new prefetch passed1.821s, cold0.401s/286,720hydrated bytes; independent restore retained all256 BLOB hashes and9 canonical native objects (`artifacts/native-paging-readahead-fs.log`).

Latest full native runtime/supervisor regression exited0:98 top-level tests passed,3 explicit opt-in skips (2000DO pressure, separate COS full restore, separate native10GiB). This command uses the new fork and current snapshot/prefetch implementation; evidence `artifacts/native-runtime-final-regression.log`. Separate10GiB remains active; skipped tests are not counted as this command's passes.

Explicit2000DO pressure rerun passed26.375s (`artifacts/native-2000do-final.log`); workerdRSS at2000loaded DOs1,173,147,648bytes, after10sidle1,126,174,720bytes. This is an opt-in lifecycle/memory observation, not a throughput benchmark or proof of low per-DO memory.


## Actual native10GiB DO final result

PASS, exit0,1185.057s, namespace `do-cold-1794233195930635318`, native fork SHA256 `f9c2641c09e2a2b2fc3c69e0b73a85d7dca0e71723032fb9d6d65b40183cafcc`. Evidence: `artifacts/do-cold-1794233192852804669.log`. User BLOB payload10,737,418,240bytes across163,840rows; native snapshot10,853,709,188bytes. This fixture is effectively incompressible at SQLite page boundaries, unlike the earlier cellstore fixture's787MB LTX; their complete transfer timings are not like-for-like.

| Phase | Seconds | Correctness evidence |
| --- | ---: | --- |
| Source insert and all BLOB hashes |96.237|163840 rows, actual10GiB BLOB payload|
| Initial capture including32MiB multipart upload |364.457|Native snapshot accepted|
| Cold index construction |1.248|Fixed epoch cut|
| Cold restore/start/first query (includes index) |2.170|Expected row200, only94208local hydrated bytes|
| Selected64KiB BLOB read/hash |0.020|Hash matches source, no extra hydration|
| Independent complete restore |672.628|Fresh third directory|
| Independent restore plus full audit (includes restore) |719.742|Updated value999 and every163840BLOB SHA256 match|

All9 stored database objects have canonical native epoch/level/TXID keys and native100-byte headers; no auxiliary storage markers. Test cleanup completed without error. Sampled Go VmHWM418416KiB after complete materialization (about409MiB), VmRSS246808KiB during final audit; this is the Go test process, not aggregate workerd/host RSS. Evidence `artifacts/native10g-bounded-memory.json`. Native point-query figures come from the compiled pre-prefetch test; the subsequent fleet rerun uses the latest128KiB prefetch implementation.


## Fresh native fleet result and remaining renewal boundary

PASS, runner exit0 and app cleanupHTTP200: `artifacts/native-fleet-readahead.log`, detailed persistent evidence `artifacts/hk-review-1794234380-949283-wz859gyu/0/`. Two real cell-agents and two gated native workerd runtimes, local compute/HK COS,16MiB user BLOB payload. No manual capture, owner stub or mutation retries. Fleet commit metric increased11. Two process-group SIGKILLs retained acknowledged values10 then11; each survivor completed all256 BLOB hashes and the subsequent write, final counter12. Actual owner epochs1→2→3 and nodesA→B→A. The second A started in a fresh directory. Takeover first-read elapsed32s/33s includes the existing30s owner lease expiration; restored SQLite logical16,920,576bytes, local allocated135,168bytes before full proof scan.

The previously failing full-scan409 did not recur. However, second-scan runtime log contains one renew request timeout at22:28:06, before the proof completed22:28:08. It is NOT merely teardown noise. Sequential prefetch removes enough remote roundtrips for this case to pass, but workerd's synchronous SQLite still blocks `/v1/do/renew` servicing; arbitrary longer queries can still expire the lease. This remaining lifecycle boundary is not considered resolved by a passing16MiB test. Goal stays active pending independent in-flight lease renewal and a >lease-duration real runtime regression.

Current official workerd1.20260916.1 COS snapshot/full-cold rerun also PASS5.727s: all256 hashes, changed-value recapture, independent restore,9 canonical native objects and cleanup. Log `artifacts/official-do-current-snapshot.log`. It uses the current bounded snapshot implementation; official workerd has no native paging.


## Independent DO renewal and oversized native increments (current implementation)

The supervisor now renews confirmed DO scope/node/epoch identities independently of the workerd event loop, negotiated during claimed-host restoration. It never claims/takes over and does not increase the configured TTL. Renewal failure removes that identity; release/drain and child exit stop renewal. JS checks the exact dispatched claim and fetches its current expiry for long operations. Ordinary short requests do not add begin/end HTTP roundtrips. No new COS object or format.

A real-owner/native-VFS test with1s TTL and2.8s cold read passed; the competing node was denied and all256 BLOB hashes/recapture matched. Removing only independent renewal with a temporary Go overlay reproduced loss of ownership and409 (`artifacts/native-long-paging-negative.log`); source was not reverted. Positive log `artifacts/native-long-paging-lease.log`. Unit/race coverage also verifies renewal past TTL without any workerd call, released identity not resurrected, old forget not removing a newer epoch, and takeover after manager shutdown.

Increasing the real fleet fixture to one128MiB transaction exposed HTTP413 on its native WAL incremental after successful snapshot publication (`artifacts/native-fleet-independent-lease.log`). Large increments now encode to a local fsynced spool and use bounded streaming HTTP/native L0 publication, retaining the single transaction's native range and retry file. Small increments retain fleet ACK. No splitting mid-transaction or larger buffered transport cap. Targeted forwarding/idempotency and lost-ack exact-spool tests passed (`artifacts/native-delta-stream-tests.log`). The same128MiB single transaction then succeeded (`artifacts/native-fleet-large-delta-lease.log`). WAL polling still retains transaction frames; this change removes the additional whole encoded LTX buffer, not all memory proportional to a transaction.

The next long scan hit the existing cell-agent DO proxy's30s HTTP deadline, with an explicit `Client.Timeout exceeded while awaiting headers` envelope; this is distinct from owner lease loss. The default proxy deadline is unchanged. A dedicated long-native-proof mode calls the authenticated runtime directly with the same deployed storage identity; seeds, writes and takeover reads still go through cell-agent, and ownership/fleet/output gates still use real agents/COS. The >30s proof test is in progress (`artifacts/native-fleet-long-query-lease.log`), including workerd-child-only SIGKILLs. Do not label this direct long-call test public-proxy >30s acceptance.

Fresh tagged full Go tests (with the paging fork enabled), vet and targeted races passed after these changes: `artifacts/full-go-native-file-lease.log`, `artifacts/vet-native-file-lease.log`, `artifacts/native-file-lease-race.log`, `artifacts/independent-lease-race.log`. The exact dispatched-claim identity check received a subsequent JS-only tightening and is exercised by the live long fleet run.


## Final native lease gates: PASS

128MiB single-transaction native fleet test passed with2048 BLOB hashes at each of two independent cold takeovers. Long proof scans were54.056s and52.484s, both explicitly required to exceed the unchanged30s lease. These proof requests use authenticated direct runtime HTTP, as described above. Seeds, counter writes and takeover reads use the normal cell-agent proxy. Two workerd-child-only SIGKILLs stopped independent renewal and allowed takeover in34s/32s; epochs1→2→3, acknowledged values10→11→12. Both fresh restores allocated135168bytes for logical135278592bytes before full scans. Fleet commit delta12, cleanupHTTP200, runner exit0. Evidence `artifacts/native-fleet-long-query-lease.log`, persistent runtime/owner/proof artifacts `artifacts/hk-review-1794236098-f4e0ec-4rq813zb/0/`.

Final16MiB fleet rerun entirely through the normal cell-agent proxy also PASS: two cold proof scans5.258s/5.075s, epochs1→2→3, values10→11→12, both256-BLOB hashes identical, cleanupHTTP200, exit0. Evidence `artifacts/native-fleet-final-proxy.log` and `artifacts/hk-review-1794236323-f748e0-57hmc2ph/0/`. This uses the final dispatched-claim identity check. Targeted native identity/gate/takeover regression passed6.513s (`artifacts/native-final-claim-identity.log`).

Historical official-runtime raw logs under/tmp did not survive the environment replacement. Their historical results above remain labeled historical; current official review/workflow/routing/batch/transfer/100k/performance suites are being rerun with persistent artifacts and current binaries. State is in `artifacts/official-current-results.json`; logs `artifacts/official-current-<suite>.log`. Do not complete the overall goal until this fresh batch and report audit finish.
