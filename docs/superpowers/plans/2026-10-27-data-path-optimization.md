# CellHive 数据路径优化实施计划

> **For agentic workers:** 使用 `superpowers:executing-plans` 分阶段执行；采用下列复选框记录进度。本计划不要求并行子代理，不授权自动上线。

**Goal:** 在保持 RPO=0、连续恢复链和权限撤销约束的前提下，降低双 agent 热写长尾、后台 COS 读写放大、空闲 cell CPU 和冷恢复成本。

**Architecture:** 第一轮保持桶格式与确认条件不变，先改 compaction 候选筛选和调度，再优化上传 spool 的批量持久化与 capture 空闲调度。大 cell 增量分层、跨 cell bundle 是独立后续项目，必须先完成读写兼容、恢复、GC 和回滚设计，不能直接照搬 celld 格式。

**Tech Stack:** Go、SQLite WAL/LTX、workerd、腾讯云 HK COS、双 cell-agent、Helm/Compose。

## 状态与依据

- 状态：分阶段实施中（详见末尾进度）；本文件中的性能指标是验收目标，不是已测结果。
- 源码对照：CellHive `056ce86`，celld `/opt/celld` `90b4301`（v0.6.2）。celld 作为实现参考，不作为相同硬件下的性能证明。
- CellHive 已有 scope/epoch 上传合批、follower fsync 合批、复制流水线、owner 本地读证明、固定版本 binding、恢复并行 GET；不重复实现这些能力。
- 已确认的代码路径：`internal/compaction/compaction.go` 下载 L1/L0 后才判阈值；阈值字节包含旧 L1。`internal/upload/upload.go` 的 Enqueue 同步写 spool、满队列同步上传；`internal/upload/spool.go` 持全局锁并执行文件与目录 fsync。`internal/sqlcapture/capture.go` 每 capture 默认 2ms 轮询。
- celld 参考：`crates/celld/ltx_repl.rs` 的 maybe_queue_compaction、enqueue_compaction、sync_loop、bundle_loop；`crates/logic/output_gate.rs` 的统一可见性屏障。

## Global Constraints

- RPO=0：成功确认的写入，必须有当前有效 follower 恢复证明或 COS 持久化证明；不以本机内存队列替代它们。
- 保留 node/session、epoch、resource incarnation fence；不存在从旧资源恢复到同名新资源的路径。
- 保留连续 txid 屏障；响应丢失时不自动重放非幂等业务请求。
- 读取若观察到未证明持久化的写入，不能通过绕过屏障降低读延迟。
- 热路径不新增 bucket List；只用生产 cos:// provider，不能替换成原生 COS S3 条件写。
- 真实存储验收必须使用专用 HK COS，双 agent 为主，单 agent/失去 follower 为降级对照；MinIO 不能代替 COS 验收。
- 不删除生产 PVC、spool、bucket 前缀；故障注入和清缓存只针对隔离测试资源。
- 观测字段不记录 token、凭据、用户值；scope 不作为无界 metrics label。
- 修改设计先更新 `docs/decisions.md`；阶段状态同步到 `docs/known-issues.md`。新 ADR 编号以执行时末尾编号为准。
- 不修改 workerd fork，不引入新状态服务。每阶段独立提交、独立验收；协议变化不能借由配置开关声称可回滚。

## 交付顺序与依赖

| 阶段 | 交付 | 前置条件 | 发布风险 |
|---|---|---|---|
| P0 | 可复现基线与阶段耗时 | 无 | 只增加有界观测 |
| P1a | compaction 先判断、按新增量计费 | P0 | 保持对象格式 |
| P1b | dirty 候选队列、预算和公平调度 | P1a | 调度变化，保留核对 |
| P2 | upload spool 批量持久化与背压 | P0，P1 验收后再比较 | 本地日志与恢复变化 |
| P3 | capture 空闲退避、可靠唤醒 | P0，P2 稳定 | 涉及后台写捕获 |
| P4 | 大 cell 增量分层独立设计与实验 | P1–P3、真实大 cell 基线 | 桶恢复协议变化 |
| P5 | 跨 cell bundle 独立设计与实验 | P4 格式边界、跨 cell PUT 证据 | 节点日志/索引/GC 变化 |

第一批只交付 P0–P3。P4/P5 纳入总路线，但通过独立设计与故障测试门槛后才进入生产实施；即使推迟，也不能阻止前面已验证的低风险优化发布。

## P0：统一观测与双节点基线

**文件：**修改 `internal/server/server.go`、`internal/upload/{upload.go,spool.go}`、`internal/compaction/compaction.go`、`internal/sqlcapture/capture.go`；复用 `scripts/e2e/{hk-review.py,kv-performance.cjs,kv-performance.sh,kv-batch-recovery.sh}`；新增 `scripts/e2e/data-path-matrix.py` 和 `docs/archive/data-path-baseline-2026-10-27.md`。

- [ ] 新增 spool 锁等待、编码、write、file sync、rename、dir sync、enqueue、队列阻塞耗时；标注 fleet/bucket、queued/sync-fallback，区分重叠时间，不能将并发 span 简单相加。
- [ ] compaction 记录 skipped 原因、实际 GET/List/PUT/DELETE 数与字节、输入 L0/L1、输出字节、峰值预算和运行时间；测试没有执行合并时仍发生的读取。
- [ ] capture 记录空轮询、有效 poll、通知到捕获耗时、group 大小、durability wait；记录上传 backlog 条数、字节、最老年龄、重试与回退。
- [ ] 用可控延迟的存储测试验证指标覆盖 fsync 和队列满同步分支；所有指标只描述事实，不修改确认顺序。
- [ ] 固定镜像、CPU/内存、workerd 版本、COS endpoint、agent 延迟、采样率；记录客户端与 agent 是否同机，HTTP keepalive 和 RTT。
- [ ] 实现矩阵 runner：每场预热30秒、采样120秒、重复3次；保留逐请求 JSONL，不用平均值掩盖 p99。每次创建唯一 namespace，finally 清理并验证删除结果。
- [ ] 基线分开测 owner/follower 入口、agent 直连/Worker binding、GET/PUT/TTL PUT、1/16/64并发、1/32/128个cell、1KiB/64KiB值。大值1MiB单列，不能与普通KV混算。
- [ ] 对单热点 cell 执行 5 分钟持续写、10 分钟低流量及空闲观察；采集 CPU/RSS、COS 操作数、fsync 次数、复制确认方式和后台积压。
- [ ] 保存命令、退出码、版本、参数、样本数、错误率、p50/p95/p99/max、RPS，形成后续对照。审查观测开销后单独提交。

**验收：**相同请求能够定位时间在 SQLite、capture、peer、spool、COS 或 owner/auth；observer 开关对照的吞吐/延迟变化不超过5%，超出则降低采样或改用直方图。

## P1a：compaction 不做无效下载

**文件：**修改 `internal/compaction/compaction.go`、`internal/replica/replica.go`；新增 `internal/compaction/preflight_test.go`；文档先补决策契约。

- [ ] 在现有测试 bucket 外包装 Get/List/Put 计数器，构造“已有L1、无新L0”“大L1、少量新L0”“batch包含多段”“marker/快照独立对象”“部分GC失败后残留旧L0”五类用例。
- [ ] 先跑失败断言：无新L0时旧L1 payload GET=0；阈值以下旧L1 payload GET=0；64MiB旧L1不能独自触发少量增量合并；已被manifest覆盖的残留L0不重复计数。
- [ ] 拆分轻量候选判断与实际合并。先读manifest/对象范围元数据，过滤已覆盖范围；batch段数不可从对象数推测。新增字节取可靠对象metadata或上传记账，未知信息不得按0判定安全；允许后台补充元数据。
- [ ] 只有确定有候选后再读取基线，完整保留链校验、快照完整性、epoch marker规则、manifest提交后GC及owner验证。
- [ ] 加入临界值与metadata过时测试：不得漏掉新增L0、跨epoch依赖、恢复补传；候选误判最多额外扫描，不得错误回收。
- [ ] 跑 compaction/replica/restore 单元与race；同一HK数据集重复闲置和少量写入，比较GET字节、快照重写次数与恢复校验。
- [ ] 同步 known-issues，提交这一独立优化，暂不叠加其他性能调整。

**验收：**无新增数据时不下载L1；低于阈值不重写快照；同一输入恢复逐键/逐行一致；空闲/少写场景后台payload下载目标下降至少90%。List/manifest查询仍分别计量，不将payload降幅写成全部请求降幅。

## P1b：dirty 队列与资源预算

**文件：**新增 `internal/compaction/scheduler.go`、`internal/compaction/scheduler_test.go`；修改 `cmd/cell-agent/main.go`、`internal/config/config.go`，为 `internal/upload/upload.go`、`internal/recovery/recovery.go` 增加已成功持久化通知接入。

**拟定接口（仅调度提示，不作为恢复权威）：**

```go
type DirtyKey struct { Scope cell.Scope; Epoch uint64 }
type Scheduler interface {
    MarkDirty(key DirtyKey)
    Forget(key DirtyKey)
    Run(ctx context.Context)
}
```

- [ ] 先测同key重复通知只排一项、运行时新增写不会丢失、失败指数退避、旧epoch取消、一个慢cell不饿死其他cell、队列有界溢出会触发轮转核对。
- [ ] 上传/恢复持久化成功后MarkDirty；owner接管加入初次核对；release/删除失效。不得在每次请求中List或同步执行合并。
- [ ] 每次任务冻结候选版本；完成后仅消费本轮覆盖部分，不能清除运行期间的新脏标记。重启丢失提示由manifest/后台核对重建。
- [ ] 初始并发1；增加有限超时、抖动、失败退避、低频有界全量核对。小cell按候选排队，不能为追求并发直接把所有owner开成goroutine。
- [ ] 对单次输入设置明确预算。第一轮超预算跳过并记录，不把部分链写成完整快照；记录oldest-uncompacted与backlog告警，避免无限跳过被当成成功。
- [ ] 32/128 owned cell 空闲10分钟、混合热点/冷点，以及进程重启后补传测试。若核对周期导致恢复对象数长期增长，调整队列公平性，不取消预算。
- [ ] 更新配置默认值及运维说明，单独提交。

**验收：**稳定空闲cell不再每30秒读取其数据对象；重启后仍能发现未合并尾部；任务数和内存有界；失败不会高频重试；热PUT p99不能因合并出现显著回归。

## P2：上传 spool 批量持久化，保持确认契约

**文件：**修改 `internal/upload/{spool.go,upload.go}`、`internal/server/server.go`；新增 `internal/upload/spool_group.go`、`internal/upload/spool_group_test.go`、`internal/upload/spool_crash_test.go`；扩展现有恢复E2E。

- [ ] P0先确认本机spool锁/fsync占比；若不是显著瓶颈，仍保留量化结论，但不以零收益格式迁移作为发布目标。
- [ ] 第一版保留现有每条 `.spool` 文件格式和重放兼容性：有界队列收集等待者，允许批内文件准备并复用一次目录sync；每条file sync仍完成后才给出durable成功。不开无界goroutine，不在ACK之前省略必要sync。
- [ ] 先测写/Sync/Rename/目录Sync各阶段失败、并发Append/Replay/Delete/Stop、同scope顺序、容量满与context取消；ACK不能覆盖未完成持久化的条目。
- [ ] 将内存记账锁与慢磁盘阶段分离，明确ID分配、发布、删除和重放的状态机；单独记录队列满触发的COS同步回退，先保留其安全行为。
- [ ] 批窗口只在有并发积压时启用；孤立请求不人为等待长窗口。请求已被接受后context取消不能导致丢弃已承诺durable的数据。
- [ ] 在2/16/64并发下与旧实现对照fsync次数与p99；没有收益或回归则停留现有格式，不强行切换。
- [ ] 若证据仍表明逐文件file sync为主因，再编写独立追加日志格式决策：长度/CRC/提交footer、torn-tail截断、去重、reclaim水位、双格式读取和离线迁移。通过该设计与断电模拟后才能替换逐文件实现。
- [ ] 第一轮不把fleet ACK直接提前到spool之前。若未来要提前，需另证明确认后owner强杀、follower重启、session seal/reclaim以及handoff drain仍有连续完整恢复来源。
- [ ] 单独提交保格式批处理及证据。追加日志不能混在同一次低风险发布。

**验收：**持久化完整性100%；无新增静默drop；多cell写入下lock wait与目录sync放大下降；目标写入p99改善至少20%，p50/RPS无超过5%的回归。门槛不达则如实报告并重新定位，不承诺任意绝对毫秒值。

## P3：capture 空闲退避与可靠唤醒

**文件：**修改 `internal/sqlcapture/capture.go`、`internal/cellcapture/cellcapture.go`；新增 `internal/sqlcapture/idle_poll_test.go`；核查 `internal/server`、`internal/timer`、`internal/cron`、`internal/queue` 的写入通知调用点。

- [ ] 列出全部写入入口，标明是否可发通知：HTTP、Worker binding、TTL清理、Queue/Cron/Workflow、checkpoint以及非本进程WAL写入。未覆盖入口不能删除兜底轮询。
- [ ] 写回归：通知发生在休眠切换前/中/后；并发通知合并；空闲后首写；无通知后台写；checkpoint重置；取消/关闭；乱序ACK不越过缺口。
- [ ] 引入可注入时钟或ticker工厂，测试状态转移不依赖脆弱的毫秒sleep断言。
- [ ] 活跃保留现有2ms group行为；连续空poll后逐级退避，初始试验上限25ms；通知立即唤醒并恢复活跃。针对无法通知的capture类型保留现有频率，或以独立延迟预算决定退避上限。
- [ ] 跑32/128个idle cell及冷热混合实验；测CPU、poll次数、首写p99、后台定时派发延迟；不得只比较平均CPU。
- [ ] race与原capture回归通过后独立提交，保留一键恢复旧调度的配置。

**验收：**空闲poll次数目标下降至少80%；CPU下降以实际测量为准；热读写p99不退化超过5%；空闲首写不出现25ms固定附加等待；TTL/任务捕获没有遗漏。

## P4：大 cell 增量分层（独立后续项目）

**文件范围：**`internal/ltx/`、`internal/replica/{replica.go,fetch.go}`、`internal/restore/`、`internal/compaction/`、`internal/pagedvfs/`；新增独立设计与实施计划后才改生产格式。

- [ ] 用64/256/1024MiB数据库，每轮修改1%页面，记录旧L1重读/重写放大、RSS和冷GET。
- [ ] 设计完整基线+增量层manifest，明确层数上限、连续txid、同页覆盖顺序、分页读优先级及最终全量fold。
- [ ] 设计有界流式输入，不能仅限制输出part大小而将全部输入放进内存；budget超限可断点继续，不发布不完整基线。
- [ ] 先做隔离原型：旧数据读取、新数据恢复、manifest发布前后强杀、GC并发Range读取、跨epoch接管和删除重建全部验证。
- [ ] 明确升级顺序：所有读者先支持新格式，再启用新writer；旧二进制回滚必须先完成兼容降级/物化，不能只关闭开关。
- [ ] 若真实COS小更新的上传字节未显著下降，或冷GET明显退化，停止格式上线并保留实验结论。

## P5：跨 cell bundle（独立后续项目）

**文件范围：**`internal/upload/`、`internal/nodelog/`、`internal/recovery/`、`internal/replica/`、`internal/objectstore/`；先登记新对象前缀属主，禁止泛用bucket绕过隔离。

- [ ] 使用128个低频cell对照单热点，确认同scope合批后COS PUT数量仍占主要成本。
- [ ] 设计bundle目录：scope、epoch、resource incarnation、txid区间、偏移/长度/checksum；对象不可变，索引发布与证明必须有明确顺序。
- [ ] 证明每个cell可点查定位并Range读取，不在热路径全桶List；未发布索引的对象可回收，已发布但正文缺失必须失败关闭。
- [ ] 先实验write/read/recovery/GC全链路：单个cell删除不影响其他cell，坏记录不能误跳过，owner切换与重复补传不丢写，慢cell不阻塞其他cell永久回收。
- [ ] 对照小对象PUT数量、冷GET读放大、reclaim滞后、最老backlog及CPU；不把减少PUT等同于减少总请求或降低热写延迟。
- [ ] 明确读者先升级与旧版本不可直接回滚边界，通过后再另行发布。

## 统一测试命令与故障矩阵

```bash
export CELLHIVE_WORKERD=/opt/cellhive/cli/node_modules/@cloudflare/workerd-linux-64/bin/workerd
export CH_TAGS='sqlite_fts5 sqlite_dbstat sqlite_math_functions sqlite_column_metadata sqlite_preupdate_hook'
git ls-files '*.go' -z | xargs -0 gofmt -l
make vet
make test
make build
make perf-test
make rpo-test
go test -tags "$CH_TAGS" -race -count=3 ./internal/compaction ./internal/upload ./internal/sqlcapture ./internal/recovery ./internal/replica
python3 scripts/e2e/cos-contract.py --credentials /opt/.secrets/s3
python3 scripts/e2e/hk-review.py --credentials /opt/.secrets/s3 --suite performance-dual
python3 scripts/e2e/hk-review.py --credentials /opt/.secrets/s3 --suite batch-recovery
```

上述是现有入口；执行前核对参数与固定workerd版本，不能修改运行中的脚本。新增矩阵runner须调用相同生产COS provider。GitHub CI全部绿；不能以本地缓存测试代替新路径测试。

| 故障/场景 | 必须断言 |
|---|---|
| owner收到follower确认后SIGKILL | 已ACK写全部恢复，最终值和单调版本一致 |
| follower不可用、ACK延迟/丢失 | 正确降级或明确失败；不得提前成功、不重复执行业务 |
| COS超时/503、spool满、磁盘写/sync失败 | 有界背压和可观察失败，未持久化写不成功 |
| compaction与新写/接管并发 | 新尾部不被GC、旧owner不发布有效新manifest |
| manifest发布或spool持久化各阶段强杀 | 旧/新完整状态可恢复，损坏与缺口不能被跳过 |
| 测试节点本地盘丢失后COS冷恢复 | ACK集合逐键/逐行100%完整；单独统计恢复阶段 |
| 资源删除后同名重建 | 旧epoch/incarnation不能复活到新资源 |
| SIGTERM、旧新agent混跑 | Flush/seal/release顺序正确，所有存量格式可读 |
| DO、Queue、Cron、Workflow、TTL | 任务可恢复；重复尝试按原有幂等契约处理；不声称exactly-once |

每个关键强杀点至少10轮；ACK清单持久化在测试客户端之外的独立文件，明确不确定结果与成功ACK的区别。SIGKILL不能证明断电安全：fsync/目录丢失另用故障注入或文件系统模型验证。

## 发布与回滚门槛

- [ ] 每阶段独立review和证据归档，记录未覆盖项；无数据正确性错误、无静默丢写后才允许发布。
- [ ] 性能每场3轮：误差范围内的变化不宣称收益；任何有效负载p99回退>10%或RPS下降>5%必须解释并阻止默认开启。
- [ ] 冷恢复必须分别记录完整首个HTTP、控制cell等待、数据对象下载、SQLite应用，不能以open耗时代替用户延迟。
- [ ] 第一轮保持桶格式不变；新行为先默认关闭或有旧路径配置，完成本地+HK再逐区启用。
- [ ] 用户要求发布后，按本地→SG→USW，区域内agent先于runtime，使用版本化镜像和checked-in Helm values，不全量同时重启。
- [ ] 每区发布后验证实际imageID/Ready/restarts、KV/TTL、Worker binding、DO持久写、队列/定时调用；观察30分钟与下一日低流量窗口。
- [ ] 检查spool oldest age持续回落、无持续bucket fallback、无compaction资源尖峰。若错误或性能回退，保格式阶段停用新调度/回到已验证镜像；不删除spool。
- [ ] P4/P5必须执行各自格式兼容回滚流程，不能套用第一轮镜像回滚。

## 完成定义

P0–P3分别有：设计契约、最小失败回归、修复、完整测试、HK COS双agent对照、故障恢复、CI绿、提交与发布状态。P4/P5分别有可执行设计和隔离实验结论；未完成协议验收的项目保持“实验/未发布”，不计入生产收益。

## 执行进度：2026-10-27 第一批

- P0：已建立compaction对象读取回归计数器，补spool/Enqueue分段累计观测及首次HK双节点基线；完整分段观测/性能矩阵未完成。见 [初步基线](../../archive/data-path-baseline-2026-10-27.md)。
- P1a：已完成“先读L0判阈值、后下载L1”的第一步；无新尾部、残留L0、字节边界、batch段数与损坏拒绝测试通过。真实COS空合并基线GET=0。尚未实现L0元数据预筛，不能将P1a全部勾选。
- P1b/P2/P3/P4/P5：未实施。
- 证据：[compaction预检](../../archive/compaction-preflight-2026-10-27.md)。不自动推送或部署。
