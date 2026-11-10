# 大 cell 存储优化与 10 GiB COS 验收

状态：最终 10 GiB COS 存储验收、全量 Go test/build/vet 和相关 race 检查通过。修改位于 `perf/large-cell-bounded-storage`，未提交、未部署。

## 实现

原生 LTX 文件格式、快照判定、epoch 续链、不可变发布和 CRC 语义保持不变，没有新增远端 marker。

- 相邻 level 每轮最多 **256 个输入／64MiB**；不可拆的超大输入单独处理。单进程同时执行一个压缩任务。创建输出前检查临时盘余量：输入大小 + 12.5% + 512MiB；实际写入或 fsync 失败仍禁止发布。
- 可选 L9 在读取正文前检查最新原生 header。通用逻辑大小上限为 **80MiB**，owner 释放使用现有 **256MiB** 配置；超过逻辑大小、完整链输入数量或字节预算则跳过，保留续链，不发布部分快照、不删除前驱。
- 单进程最多两个 native 读取／证明任务；超过 16 个输入时使用带 ETag 校验的短 Range 和延迟分配的 64KiB 缓冲，避免保持全部远端连接。文件校验／上传、索引构建／物化分别限制两个任务。索引分批处理后释放中间结果。
- 本地 LTX 磁盘缓存上限 **64MiB／256 项**。只复用成功新建的文件型快照和压缩输出，远端 Stat 的大小与 ETag 必须一致；在用文件计入预算，缓存竞争时跳过填充。普通字节增量不为缓存额外写文件。缓存失败不改变权威发布结果。
- 生产 DO 分页恢复失败直接返回错误，不再悄悄退回完整对象缓冲。文件读写使用 256KiB 缓冲；索引尾部比较使用不超过 64KiB 的块，不再复制整个索引。完整 CRC 校验保留。
- LZ4 查找表复用并使用代际标记。回归验证与改动前编码器输出**逐字节相同**。共享宿主上的 4KiB 页面微基准：约 529KB／次、0.93–1.17ms，降至约 5KB／次、10.7–11.9μs。这不是端到端吞吐或 HTTP RPS。
- 完整物化把解码内存和网络请求预算分开：每批最多 **8MiB 解码页面／1MiB 编码数据**，仍受同一源对象、ETag、连续帧及请求页面范围约束。

## 测试条件

- 当前宿主上的 Go 测试进程，实际 HK COS 桶 `cellhive-e2e-hk-1376795072`。
- 真实鉴权的 cell-agent HTTP Handler 使用本机回环地址；无桶凭据的 `HTTPStore` 执行跨 epoch 证明与恢复。
- 命令：`python3 scripts/e2e/large-cell-cos.py --scratch /tmp`。共享桶测试锁串行化运行，凭据不写入日志。
- 插入 **10,240 行、每行 1MiB** BLOB。SQLite 文件 **10,751,451,136 字节**，实际磁盘分配约 10.75GB，非稀疏空文件。
- 数据可压缩，快照 LTX 为 **787,262,031 字节**，压缩比 **13.638938**。这是 10 GiB 数据库测试，不代表传输了 10 GiB，也不代表不可压缩数据性能。

测试先上传 TXID 40 快照，修改真实 SQLite marker 表并上传 TXID 41 的两个变化页面，然后删除本地原库和 spool。随后执行 L1 压缩、验证大 L9 被拒绝、发布 epoch 2 的 TXID 42 零页续链，再从 HTTP/COS 冷恢复。最终校验完整 SQLite SHA-256、`integrity_check`、更新后的 marker 和全部 10,240 行哈希。

## 性能结果

两个完整阶段的单次运行使用相同数据和拓扑；共享宿主负载不同，不作为统计学基准。第一轮已包含 LZ4 查找表优化，最终版额外采用 8MiB 连续页面批次和分块索引尾部比较。

| 阶段 | 第一轮，1MiB 解码批次 | 最终版，8MiB 解码批次 |
| --- | ---: | ---: |
| 生成实库 | 76.139s | 64.117s |
| 编码原生快照 | 75.977s | 55.796s |
| 校验并分片上传 | 47.782s | 37.100s |
| 生成增量并删除原库 | 49.334s | 43.644s |
| L1 压缩 | 109.839s | 80.294s |
| L9 预算检查并跳过 | 1.158s | 0.293s |
| HTTP 续链证明、索引与完整冷恢复 | **502.646s** | **107.672s** |
| 全库 SHA、完整性与逐行校验 | 132.690s | 147.199s |

最终版冷恢复阶段耗时降低约 **78.6%**，相当于这两次运行的耗时比 **4.67 倍**。该阶段包括完整续链证明，不是首个查询的延迟。

最终版 L9 仅读取 **一次、100 字节 header**，没有读取正文。L1 读取一个 787,262,031 字节输入流。HTTP 证明和恢复阶段合计 **2,044 次 Range，1,574,787,400 字节**；这是应用层计数，不包含 SDK 内部重试或 multipart 子请求。

第一轮总测试时间 **996.19s**，测试进程峰值 RSS **778,752KiB（760.5MiB）**；共享文件系统采样峰值增量约 11.97GB，不是进程独占磁盘指标。第一轮所有校验通过，测试对象清理成功。最终版全部校验通过，总测试时间 **536.62s**（含启动的 runner 时间 8:58.03），测试进程峰值 RSS **762,564KiB（744.7MiB）**；共享文件系统采样峰值增量 **11,727,351,808 字节**。

两轮完整恢复 SHA 均为：`b390dd5cf30deb59efb6a4761836aa67e652928ee9d2cdb68d759c6a2850021c`。行哈希汇总：`aa8a0181b6a31ff386fa7e74d0e619bd256ee5020288b789d1ee4f76722c460b`。

证据：`/tmp/large-cell-10g-optimized-cos.log`、`/tmp/large-cell-10g-final-cos.log`。最早使用旧 LZ4 编码器的尝试在编码阶段主动终止，不是完整基线。

## 验证与边界

全量 Go test/build/vet 和 LTX、LZ4、复制、压缩、DO 恢复相关 race 检查已通过。最后的缓存热路径修订独立 race 通过；D1 清理修订后 KV/D1 用例各连续20次通过，最终全量测试通过。缓存填充修订不影响此大库 fixture（其中未启用本地缓存）。日志为 `/tmp/large-cell-complete-test.log`、`/tmp/large-cell-release-{build,vet}.log`、`/tmp/large-cell-latest-race.log`、`/tmp/large-cell-cache-hotpath-race.log`。

- 这是存储集成验收，不是部署后的 fleet RPS 测试，也不是 100GB 验收。
- 并发、输入批次和缓存有上限；最终页索引仍随库大小增长，**不是固定内存或精确的进程级索引字节硬上限**。
- 默认强制 capture 快照上限仍为 **8GiB**（`CELLHIVE_CAPTURE_SNAPSHOT_MAX`）。本测试直接编码已有 SQLite 文件；生产初始 capture 10GiB 库需配置足够额度，不能把该结果说成默认 capture 配置支持 10GiB。
- L9 被跳过后仍依赖历史链，不能据此执行前驱 GC。没有新增绕过验证的 owner／水位缓存。
- 通用完整正文 Restore API 和外部非分页适配器仍存在；生产分页恢复错误不会回退到它们。
- 此前的通用 SQLite export/import 产品入口仍是独立未完成事项。


收尾发现并修正了三个测试时序问题，均保留正确性断言：KV 写入 TXID 测试先完成 owner 实际激活及后台注册；压缩取消隔离测试不再要求正常磁盘写入在20ms内完成，改为500ms；KV/D1 capture 测试在删除临时目录前 Drop 并等待 capture 退出，避免最后的本地 LTX 写入与目录删除竞争。它们不放宽生产持久性或恢复校验。

## 追加：10 GiB 部分行读取实测

命令：`python3 scripts/e2e/large-cell-cos.py --partial-query --scratch /tmp`。
日志：`/tmp/large-cell-partial-query-cos.log`，测试退出码0，330.20s（包含数据生成、上传和压缩）。同一真实 HK COS 桶、loopback 鉴权 HTTP Handler 和无凭据 HTTPStore；不是部署后的 fleet 请求测试。无本地 LTX 缓存，原库和 spool 在冷读取前删除。

本测试把 `ProvenWatermark`、`PageFetcher` 和 SQLite paged VFS 串接并分段测量，以区分全链证明与按需查询的成本。这不是某个生产接口完整激活流程的复刻：cell-agent 的分页回调直接构建 PageFetcher；DO 的 restoreScope 目前仍调用 Materialize。不能把下表总时间直接声称为所有 cell 的线上首响应时间。

| 阶段 | 耗时 | COS Range 次数 | COS 读取字节 |
| --- | ---: | ---: | ---: |
| 全链证明 ProvenWatermark | 46.930s | 753 | 787,262,590 |
| 页索引构建 | 1.260s | 6 | 26,090,145 |
| 建立稀疏文件及 sql.Open | <0.001s | 0 | 0 |
| 查询 id=1，返回1MiB BLOB | 1.450s | 6 | 83,360 |
| 查询 id=5120，返回1MiB BLOB | 0.496s | 7 | 93,673 |
| 查询 id=10240，返回1MiB BLOB | 0.087s | 3 | 74,861 |
| 查询增量更新后的 marker | 0.018s | 1 | 51 |
| 重复查询 id=1 | 0.003s | 0 | 0 |

SQL 为 `SELECT value FROM blobs WHERE id=?`，id 是 INTEGER PRIMARY KEY。三条 BLOB 均逐字节核对，marker 为 `after!`，恢复水位为42，全部通过。查询阶段累计读取251,945字节；不包含前置证明和索引。数据库逻辑大小10,751,451,136字节，查询后主库实际磁盘分配3,182,592字节。证明结束至首条核对成功约2.71s；包含证明的本测试串接路径首条核对成功49.641s。sql.Open 为延迟打开，实际连接初始化计入首条查询；阶段耗时还包括结果核对和少量测试计量开销。

结论：按页查少量记录确实不需要物化全库，但前置全链证明仍读取整个压缩链；全局页索引也需要约26.1MB下载，构建后Go堆约485MB。不能宣称大库冷启动已经解决，也不能未经等价正确性方案直接删除证明。该数据可压缩，结果不代表不可压缩库性能；这是共享宿主单次测量。

## 追加：索引冷激活与紧凑页映射

执行 `python3 scripts/e2e/large-cell-cos.py --partial-query --scratch /tmp`，日志 `/tmp/cold-activation-10g-cos.log`，退出码0，340.58s（含准备与查询后审计）。与上一轮使用相同实库、压缩数据和COS桶。本轮以 `PageFetcher` 的固定cut激活，走实际 `cellstore.Cell` 打开流程（含schema迁移和恢复TXID接续），禁止后台全量补齐干扰计量；先返回并核对查询结果，再单独完整CRC审计。

| 阶段 | 耗时 | Range次数 | COS字节 |
| --- | ---: | ---: | ---: |
| 加载页索引 | 0.646s | 6 | 26,090,145 |
| cellstore打开及TXID接续 | 0.031s | 1 | 238 |
| id=1完整1MiB BLOB | 0.255s | 5 | 83,122 |
| id=5120完整1MiB BLOB | 0.159s | 7 | 93,673 |
| id=10240完整1MiB BLOB | 0.086s | 3 | 74,861 |
| 增量marker | 0.019s | 1 | 51 |
| 重复id=1 | 0.004s | 0 | 0 |
| 查询后完整CRC审计 | 50.543s | 753 | 787,262,590 |

冷读取起点至首条核对成功 **0.934s**。索引和查询仍须26MB左右网络数据；未在首请求前下载787MB正文。三个BLOB逐字节相符，marker正确，索引/本地/完整审计TXID均为42。主库实际分配3,182,592字节。索引构建后Go堆192,982,736字节，进程峰值RSS354,804KiB；前轮相应Go堆484,793,560字节、峰值RSS594,140KiB。共享宿主单次测量，阶段差异不可全部归因于代码。

实现使用按实际索引项分配的1024页块，位置16字节/页，对象key/ETag只保留一份；避免按不可信Commit分配巨量内存。独立10GiB页映射存储微基准42,107,768 B/op，不含下载/解析临时分配。保持版本固定、页覆盖、截断重扩展和源文件边界检查。

DO捕获水位初始化改用现有PageSource索引cut；ProvenWatermark仍保持全文件CRC审计。该改动不代表DO已支持按需打开：当前DO仍完整Materialize。fork审查发现DO capture直接读SQLite文件生成baseline/checkpoint快照，绕过VFS；必须在原生分页接入同时修正该边界，否则稀疏空洞会被写入权威快照。未交付/启用不完整的DO分页实现。本轮仍不是公开HTTP/fleet首请求或owner切换的完整验收。

### Native DO paging continuation: capture correctness checks

Main-file writes and truncations now use begin/end mutation tracking; raw snapshot generation validation rejects overlapping mutations and poisoned views. The local sparse-cache guard parent is fsynced before replacing a database image. The guard is excluded from object replication.

A separate checkpoint capture regression was reproduced: after checkpoint plus a new WAL update, the prior gate returned success but immediate independent restore returned 1 instead of 42. Capture now re-polls after snapshot durability and persists current WAL within the same gate. The new restore regression passes, as do the full tagged Go suite, full vet and dosupervisor race checks. Evidence: `/tmp/do-paging-full-go.log`, `/tmp/do-paging-vet-final.log`, `/tmp/do-paging-race-final.log`.

The optimized workerd build and native SQLite tests remain pending. A final incremental build followed by opt-in native DO cold read/write/recapture E2E is queued; no native DO latency or COS/fleet acceptance is claimed by these Go results. Large checkpoint snapshots still require full hydration and in-memory encoding.

### Official workerd baseline (2026-11-09)

Per request, ran doruntime and dosupervisor with explicit official binaries and `-count=1`, tagged SQLite features, without enabling the fork paging test. Official pinned workerd `1.20260916.1` at `cli/node_modules/.bin/workerd`: both packages PASS, doruntime 31.306s and dosupervisor 0.722s. The system official `1.20260914.1` also passes both (31.335s / 0.651s). Each run records 153 passing test/subtest events and 3 skips, zero failures.

Pinned binary: DO compatibility suite PASS (6.59s); restore claimed host before dispatch PASS (0.24s); cross-node cold activation PASS (0.47s). These are complete test durations, not pure cold-query latency. Topology: local workerd processes, test owner HTTP stub and filesystem object bucket; full materialization restore, not real COS or fleet performance.

Skipped: opt-in memory stress; fork-only native DO paging; ranged restore test whose fixture compaction returns bucket-not-supported. These skips are not acceptance. Logs: `/tmp/official-workerd-pinned-do-tests.jsonl` and `/tmp/official-workerd-do-tests.jsonl`. Production do-supervisor's new paging-enabled launch still requires the fork marker; the official tests exercise the existing direct runtime/full-restore harness.
