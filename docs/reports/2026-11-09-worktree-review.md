# Celld LTX worktree 改动审查（2026-11-09）

## 范围

工作目录 `/opt/cellhive-worktrees/celld-ltx-migration`，分支 `perf/celld-ltx-storage`，基线 `7710ef04d609fe135965b2e94c3c307f4004d1c3`。统计包含未跟踪源码，不只看 `git diff`。本轮未提交、未部署；`/opt/cellhive` 保持干净。workerd C++ 改动属于独立 `/opt/cellhive-workerd-fork`，不包含在本仓差异内。

## 改动整理

| 范围 | 当前改动及目的 |
| --- | --- |
| LTX 格式 | 单一原生格式、100 字节 header、原生页压缩/index/checksum；移除私有 WAL2/WAL3 和分片快照实现。新增 celld 生成的互操作样本。 |
| owner/epoch | canonical `own.json`；原生零页 continuation；从当前 epoch 回溯恢复链；退休状态并入 owner。仍有 celld 标准 `ltx/retired.json`，不能描述为桶内只有 LTX。 |
| 压缩/GC | level 0000/0001/0002/0003/0009；30 秒/5 分钟/1 小时调度；单批 256 输入/64MiB、进程内一个压缩任务；大库跳过可选全量快照并保留完整恢复链；历史 epoch 删除带宽限期和 owner/readiness 检查。 |
| 大库恢复 | 流式编解码、紧凑页索引、有限并行拉取、Range 合并、本地有界版本缓存；生产分页失败不回退为静默全库下载。 |
| SQLite 迁移 | CLI 和鉴权 HTTP 导入/导出；导入仅允许新目标，完整性检查、原生快照发布、后续写入与冷恢复；无需旧私有对象格式兼容。 |
| DO 分页与捕获 | supervisor 分页桥接、稀疏文件登记、写入/截断代次约束、checkpoint 后重新采集 WAL；端到端能力依赖新版 fork。 |
| Provider/工具 | COS 原子 multipart 发布及失败 abort、metadata/流式接口；删除旧 cmd/cell-supervisor；增加测试、性能脚本与验收文档。 |

## 本轮发现和修正

1. **互操作样本漏入版本控制**：全量测试中 `internal/ltx` 失败，原因是 `testdata/native-{snapshot,delta,compacted}.ltx` 缺失。`.gitignore` 的 `*.ltx` 连测试样本也忽略，搬迁时未带入。已从原始样本恢复，新增三个精确例外；没有用 Go 编码器重造 golden，保留独立 Rust 输出。`go test ./internal/ltx -count=1` 复测通过。
2. **E2E owner 路径陈旧**：`scripts/k3d-runtime-e2e.sh` 一处、`scripts/cf-tail-multinode-e2e.sh` 三处仍查 `owner.json`。已改为 `own.json`，两份脚本语法检查通过。没有把语法检查当作 Kubernetes/Compose 实跑。
3. **测试文档陈旧**：中英文测试说明中的旧 WAL2/WAL3 和压缩开关已标记历史或替换为当前 native 测试；澄清历史验收不能证明当前改动通过。

## 本轮验证

运行日志保存在 `/tmp/celld-worktree-review/`；测试和构建限制在 CPU 0–3。Go 使用完整 SQLite build tags：`sqlite_fts5 sqlite_dbstat sqlite_math_functions sqlite_column_metadata sqlite_preupdate_hook`，`GOMAXPROCS=2`、`-p 1`。普通 runtime 测试使用 `/opt/cellhive/deploy/workerd-fork/workerd`。

- build：通过，48.91 秒；vet：通过，30.31 秒。
- 全量 Go：结果见下方最终统计；首次发现上述缺失样本，修复后复测该包。
- 重点 race：replica、cellcapture、dosupervisor、owner、server、cmd/cellhive 中 continuation/epoch/paging/checkpoint/transfer/import/export/retirement 匹配用例通过。并非所有包全量 race。
- JS：147/147 通过，零跳过。
- 真实 HK COS SQLite transfer：通过。覆盖导出/导入、清空本地缓存后 100 项读取与导入后更新、再次导出，以及普通 SQLite 的 BLOB/index 迁移；测试 namespace 清理成功。
- 真实 HK COS follower recovery：通过。两个 HTTP follower spool，writer 未上传 cell LTX 时停止；恢复 20 行、watermark=21、4 个输入，恢复加冷读校验约 737ms。此测试不等同物理多机断电或 lease 选主。
- `git diff --check`、两份修改的 shell 脚本 `bash -n`：通过。

## 尚未闭环的边界

- **原生 workerd DO 分页后续验收**：新 fork 已构建并通过本地/真实 COS 的 16MiB 原生分页、写入回捕及独立恢复；完整 runtime/supervisor 和 2000 DO 压力用例通过。原始全量 Go 的跳过清单保持原始事实，后续结果见 COS fleet 报告最新一节。10GiB 原生 DO 和真实 fleet 分页 owner 切换仍待覆盖。
- **COS 极大对象后续修正**：已按用户确认改为 `max(32MiB, ceil(size/10000))`，移除固定分片带来的约 48.83GiB 代码限制。边界测试覆盖阈值前后、60GiB、100GiB 及整数溢出边界；超过旧阈值的真实 COS 上传尚未实测，不能承诺 100GiB 完整端到端验收。
- 本轮没有重跑全 fleet 性能矩阵、10GiB、Kubernetes/Compose 套件；历史结果单列于 `2026-11-09-cos-fleet-acceptance.md`，不是本次新证据。
- 历史磁盘对象需通过 SQLite 导出/导入迁移，不提供旧对象格式回退。本轮测试不能证明所有故障交错或生产发布安全。

## 全量 Go 最终统计

首次执行耗时 428.39 秒、退出码 1：包级通过 76、失败 1、无测试/跳过 12。包含子测试的事件统计：通过 2341、失败 5、跳过 39。全部失败事件均来自上述缺失 golden 文件的 `internal/ltx`；恢复样本后整个 LTX 包复测退出码 0。其余包已通过，未再次重跑全仓，不能把首次命令描述为 exit 0。

跳过的测试（不计为已验收）：

- `cellhive/cmd/cell-agent/TestSessionRecoveryProcesses`
- `cellhive/cmd/cellhive/TestBuildBundleConcurrentTempIsolation`
- `cellhive/internal/bucket/TestAuthorityKeyDoesNotCaptureTenantObjects`
- `cellhive/internal/bucket/TestCOSConditionalDelete`
- `cellhive/internal/bucket/TestCOSDataContract`
- `cellhive/internal/bucket/TestNativeAppendCloud`
- `cellhive/internal/bucket/TestProviderResourceFence`
- `cellhive/internal/bucket/TestProviderPointResourceFence`
- `cellhive/internal/bucket/TestProviderHistoricalNodeListDoesNotAmplifyLists`
- `cellhive/internal/bucket/TestProviderCloudDiagnose`
- `cellhive/internal/bucket/TestProviderListCloud`
- `cellhive/internal/bucket/TestCOSAssetImmutableCreateLive`
- `cellhive/internal/bucket/TestS3BucketIntegration`
- `cellhive/internal/bucket/TestSegmentedAuthorityRotatesAndCollects`
- `cellhive/internal/bucket/TestParsePhysicalAuthorityKey`
- `cellhive/internal/bucket/TestSegmentedAuthorityLegacyResourceFence`
- `cellhive/internal/bucket/TestAuthorityKeyRoutesResourceFence`
- `cellhive/internal/bundler/TestBuildBundlesModulesAndTextRule`
- `cellhive/internal/bundler/TestBuildExternalizesPlatformModules`
- `cellhive/internal/cellstore/TestPerfGatePutTx`
- `cellhive/internal/doruntime/TestFacetMemoryPressure`
- `cellhive/internal/doruntime/TestNativeDOPagedColdReadAndRecapture`
- `cellhive/internal/doruntime/TestDOCOSColdReadAndRecapture`
- `cellhive/internal/owner/TestOSSOwnerTakeover`
- `cellhive/internal/replica/TestResourceFenceMigratesCOSLegacyFormats`
- `cellhive/internal/replica/TestCOSLargeSQLiteStreaming`
- `cellhive/internal/replica/TestCOSLargePagedRestore`
- `cellhive/internal/replica/TestCOSNativeLevelsColdRestore`
- `cellhive/internal/replica/TestCOSNativeFollowerRecovery`
- `cellhive/internal/replica/TestCOSNativeStreamingSnapshotMultipart`
- `cellhive/internal/replica/TestS3ReplicationRestoreChain`
- `cellhive/internal/server/TestReviewWranglerAssetsRealCLI`
- `cellhive/internal/server/TestReviewWranglerRealCLI`
- `cellhive/internal/server/TestStockWranglerControlE2E`
- `cellhive/internal/server/TestStockWranglerVectorizeE2E`
- `cellhive/internal/userruntime/TestDynamicLoadMemoryPressure`
- `cellhive/internal/wrangler/TestFrameworkPrebuiltLayouts/opennext`
- `cellhive/internal/wrangler/TestFrameworkPrebuiltLayouts/sveltekit`
- `cellhive/internal/wrangler/TestFrameworkPrebuiltLayouts/astro`
