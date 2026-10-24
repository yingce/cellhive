# 2026-10-24 COS 冷写栅栏与恢复实测

测试对象：`/opt/cellhive`，基线 HEAD `14198bd`（本次修复尚未提交）；COS `vwork-e2e-hk-1376795072`，同区内网，3 MiB KV cell，`LEASE_TTL=10s`，顺序 put+get 稳态 90 秒。脚本 `scripts/e2e/cold-write-cos.sh`，每阶段回读 ledger 中全部最新已确认值。旧数据见 `/tmp/cold-cos-new.log`；修复版见 `/tmp/cellhive-fence-v2-cos-fixed.log`，诊断原始日志 `/tmp/cellhive-cold-e2e.8Vrq0l/agent-a.log`。这些 `/tmp` 文件只是本机证据，可能随环境清理消失。

| 场景 | 旧版 | 修复版 | 目标 | 结论 |
|---|---:|---:|---:|---|
| 稳态 put 数 / 90 秒 | 608 | 1217 | 持续 ≥180 秒 | 90 秒对照；完整 180 秒门尚未跑 |
| 稳态 put p50 / p99 | 96.2 / 1092.9 ms | 52.7 / 434.7 ms | p99 <200 ms | 改善，但不达标 |
| 稳态全局 List / Get | 769 / 1481 | 353 / 1869 | 写热路径 List=0 | 全局 List 仍高；不能从该计数推出热写 List |
| 稳态 epoch 变化 | 0 | 0 | 0 | 通过 |
| 淘汰后首写 | 413.1 ms | 143.1 ms | <300 ms | 单样本通过 |
| 同节点重启首写 | 2561.1 ms | 2346.4 ms | <2000 ms | 未达标；修复版 epoch 变化仍为 2 |
| 跨节点接管首个成功 | 12906.7 ms | 10732.9 ms | 约 TTL + 恢复 | 10s TTL 主导；接管写耗时 1172.2ms |
| 接管后 50 次 put p99 | 392.9 ms | 633.6 ms | <1000 ms | 通过但较旧版慢；样本小 |
| 本地目录删除后冷恢复首写 | 3394.9 ms | 3373.9 ms | <1000 ms、List ≤3 | 未达标；List 27、Get 141 |
| 已确认值 / SQLite | missing=0、wrong=0、integrity=ok | missing=0、wrong=0、integrity=ok | 全部正确 | 修复版通过本次场景 |

诊断版在 `S3Bucket.List` 临时记录了调用前缀（记录后已从源码移除）。新 fence 的旧键只在首次迁移时列举一次。持续的 `nodes/` List 来自后台节点租约枚举；共享测试桶有历史 `nodes/probe/...` 和旧 `e2e-cold-*` 节点，即使租约过期，列表归并仍逐个读取并在旧窗口缺失时触发后备 List。这解释了总 List 不能归零，也可能造成 COS 资源竞争；尚未证明它解释了全部 p99 尾延迟。`cells/<scope>/ltx/` 的列举集中在激活、恢复和 compaction，冷恢复的 141 次 Get 仍需优化。

第一次 v2 COS 运行（`/tmp/cellhive-fence-v2-cos-e2e-rerun.log`）在 90 秒约 1199 次 put、p99 391.8ms 后，重启时 80 个已确认 key 全部 404。`agent-a.log` 显示 compaction 折叠 713 个 L0 后，重启只从 txid 2 续链；`RestoreChain` 把仅剩 `L1/manifest.json` 的 epoch 过滤掉。ADR-277 修复并用 `TestRestoreChainKeepsCompactedEpochWithOnlyL1Manifest` 锁定。第一次失败的 SQLite `integrity_check=ok`，所以不能以结构完整性替代数据回读。

本地 FS 对照（`/tmp/cellhive-fence-v2-fs-repro.log`，3 秒稳态）五场景通过：稳态 p99 6.6ms、淘汰 12.5ms、同节点重启 94.8ms、接管 2499.7ms、冷恢复 111.4ms，missing/wrong=0、integrity=ok。另一次 8 秒 FS 运行在重启后的 warmup 持续 503，未保留响应正文；短时复跑未复现，不能据此判定该现象已解决。

加上 GET 延迟记录后的 FS 多 cell 复验暴露了另一个 RPO 边界：初始快照的 `0.snapshot` 和 `0.snapshot-started` 与 `1.snapshot-done` 不同水位，新 owner 过滤该快照，filler `fill1/fill3/fill5` 返回 `restore: no snapshot in chain`。主 KV 的值正确不能替代对所有已确认资源的验证。ADR-278 改为在 snapshot part 上传前用实际 checkpoint 水位写 started，并以同一返回水位写 done；started 失败则拒绝上传/写入。`/tmp/cellhive-fence-v2-fs-marker-fixed.log` 的 3 秒 FS 复测五场景通过：稳态 put p99 5.9ms、GET p99 1.4ms、淘汰 10.5ms、同节点重启 108.8ms、接管 2436ms、冷恢复 125ms，最终 140 个已确认 key missing/wrong=0、integrity=ok。

`TestProviderPointResourceFence` 在真实 COS 上验证 stable append-position 键的条件创建、CAS、读回；`TestResourceFenceMigratesCOSLegacyFormats` 用真实 COS 分别验证旧分段和旧 plain JSON 栅栏迁移、`epoch_floor` 保留与旧 epoch 拒绝。Go 定向测试还覆盖并发迁移、跨管理器删除可见、容量上限和 app purge 保留两种栅栏。该次 COS 仅运行 90 秒，尚不满足 A1 的 ≥3 分钟门；所有延迟未达标项仍需继续排查。

## 180 秒 COS 复验（ADR-278 后）

原始输出 `/tmp/cellhive-fence-v2-cos-180s.log`，二进制 `/tmp/cell-agent-fence-v2-final2`，测试临时文件 `/tmp/cellhive-cold-e2e.6zwG0z`。这次设置 `P99_LIMIT_MS=10000` 以让脚本跑完所有数据校验场景；脚本的 `COLD-E2E: PASS` **只代表该宽松门槛及完整性检查通过，不代表下表的设计性能验收通过**。结束时产品删除接口清理了本次测试 namespace（1007 个 segment）。

| 场景 | 结果 | 设计目标/判断 |
|---|---:|---|
| 稳态 180 秒 | 2341 次 put，put p50/p99/max = 53.0/511.8/1174.1ms；GET p50/p99/max = 0.9/65.3/731.2ms；epoch 变化 0 | put p99 <200ms：**未达标**；读 p99 首次记录，无既定验收线 |
| 稳态全局 bucket 操作 | List 1243、Get 5074、Put 4 | 包含后台节点扫描和恢复/维护，不能据此声称写热路径 List=1243 |
| LRU 淘汰后首写 | 141.2ms，List 0、Get 2 | <300ms：单样本通过 |
| 同节点重启首写 | 3874.2ms，List 46、Get 115、epoch 变化 2 | <2s：**未达标**；较 90 秒复测的 2346.4ms 更慢 |
| 跨节点接管 | 首次成功窗口 10717.9ms（TTL 10s），成功写本身 1368.9ms；之后 50 次 put p99 521.6ms | 窗口主要由 TTL 决定；后续 p99 <1s 通过 |
| 删除本地目录冷恢复首写 | 4182.9ms，List 49、Get 171 | <1s、List ≤3：**未达标** |
| 已确认数据 | 每阶段 missing=0、wrong=0；最终 140 个 key；SQLite `integrity_check=ok` | 本次五场景完整性通过 |

90 秒与 180 秒是不同负载时段，不能把单次延迟差归因于 ADR-278。稳定点查 fence 消除了原先每次缺失键读取触发的 List，但后台 `nodes/` 枚举、重启/冷恢复的多次列举与逐对象读取，以及写入尾延迟仍需定位和优化。Cron 的故障验证另由 `scripts/runtime-baseline-e2e.sh` 覆盖：SIGKILL/重启后两个表达式产生新的分钟 slot 且未重复；它不验证停机跨完整分钟后的补发，现行调度也不提供这种补发。

## 180 秒 COS 节点列举优化对照（ADR-279）

原始输出 `/tmp/cellhive-list-window-cos-180s.log`，二进制 `/tmp/cell-agent-list-window-final`，测试临时文件 `/tmp/cellhive-cold-e2e.W832Hx`。代码让 `ProviderBucket.List` 复用首次物理列举中已知的每个 authority 最新分段，避免对历史节点逐个重新 List；独立 COS 定向测试中，3 个历史节点的逻辑列举仅发生 1 次物理 List。下表与上节的同日 180 秒测试参数相同，但不是并发 A/B，也不能将每项差值都归因于这一个修改。

| 场景 | ADR-278 后 | 加 ADR-279 | 判断 |
|---|---:|---:|---|
| 稳态 put 次数 / p50 / p99 | 2341 / 53.0 / 511.8ms | 2527 / 50.5 / 450.7ms | List 明显下降，p99 仍超过 200ms |
| 稳态 GET p50 / p99 | 0.9 / 65.3ms | 0.8 / 56.1ms | 无既定 GET 验收线 |
| 稳态全局 List / Get | 1243 / 5074 | 421 / 4669 | 全局 List 减少约 66%；余量不能直接归因于写热路径 |
| LRU 淘汰首写 | 141.2ms | 140.9ms | 单样本符合 <300ms |
| 同节点重启首写 | 3874.2ms | 3892.9ms | **仍未达标** |
| 跨节点接管首个成功 | 10717.9ms | 10060.9ms | 10s TTL 主导，单样本 |
| 接管后 50 次 put p99 | 521.6ms | 815.5ms | 小样本波动，均 <1s |
| 删除本地目录冷恢复首写 | 4182.9ms | 2867.5ms | **仍未达标**；本次 List 17、Get 145 |
| 最终已确认数据 / SQLite | 140 个 key 正确 / ok | 140 个 key 正确 / ok | 两次 missing=0、wrong=0、`integrity_check=ok` |

第二次 e2e 也设置 `P99_LIMIT_MS=10000`，所以 `COLD-E2E: PASS` 仍只代表宽松脚本门和本次数据完整性。重启日志显示 compaction 在停止前因 context canceled 中断，当时留有近千个 segment，重启 hydrate 记录 `segments=999 elapsed=4.609s`；这为重启/冷恢复慢提供了直接线索，但尚未证明是唯一原因。默认恢复并行数 4 是先前 mass-hydrate OOM 后的保护，不能不经全局并发/内存验证就照搬 celld 的 64。
