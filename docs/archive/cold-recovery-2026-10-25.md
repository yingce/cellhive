# 冷恢复首写优化（2026-10-25，ADR-293）

本次在 `fix/review-fixes-20261025` 工作区继续修复，保留之前 F1–F9 未提交代码，未部署生产。前测见 [HK E2E](hk-review-2026-10-25.md)。

## 根因

临时 Go overlay 在 bindingInfo、owner gate、cellstore.Cell、capture.Ensure、DurableWatermarkChain、RestoreChain 周围记录耗时，未把诊断代码编入最终产物。[分段证据](cold-recovery-2026-10-25/before-trace.txt)：重启首写中，控制 cell 的 `invalidateStaleLocal` 为 9.588s，外层绑定查询 11.382s；目标 KV 接管仅 415.6ms，capture 后续没有同量级慢阶段。

`lease.NodeLease` 本意是查一个节点，但调用 `SampleCached`。后者在首次缓存为空时同步执行 `Sample`，List 整个 nodes/ 后逐条 Get 历史节点；因此首次查询被全节点数量放大，违反“节点点查不在请求路径 List”的契约。测试桶历史节点多，形成 9–11s 延迟。不是简单的 SQLite 重新打开，也不能直接归因为等待 lease 到期。

另外，`replica.restoreOnceKeys` 在 L1 manifest 已覆盖部分 L0 日志时仍下载所有 L0，再在解码后过滤；无效 COS GET 增加恢复成本。

## 实现与边界

- NodeLease 仅读已有内存快照，未命中则点查目标节点；不触发全量发现。所有权切换的 NodeLeaseFresh 仍查权威。
- SampleCached 首次和过期刷新均异步、单飞、有 15s 超时和既有失败退避；没有快照时按原有逻辑使用 bucket 持久性证明，不能假称 peer fsync。缓存刷新不依赖首个请求的存活期。
- 日志订阅属于管理员诊断路径，冷缓存时显式采样，保留跨节点订阅传播；Cloudflare tail 周期轮询随后可看到后台快照。
- L1 manifest 覆盖的、对象名能确定范围的 L0 在下载前跳过。保留 epoch marker、未知/混合 batch、跨越水位的日志，已有解码与恢复链检查不变。没有引入同内容缓存或跨 epoch 的推测。
- 本次不提高 restore/hydrate 并发、不跳过 RPO=0、不修改租期和 fencing。后台历史节点枚举自身的开销尚未解决。
- 参考 celld `crates/logic/restore.rs` 的安全恢复源选择：本地旧文件不能因性能原因当作最新数据。此次改善元数据查询和已覆盖日志的冗余读取，不引入不安全的本地复用。

## 回归

[旧代码失败证据](cold-recovery-2026-10-25/lease-red.txt)：单节点 lookup 实际 List=1；阻塞 List 会阻塞首次 SampleCached。修复后 lease 全包 race 连续 10 次通过。

新 manifest 回归记录每个实际 Get，验证已覆盖的 10 个 L0 不被下载，同时水位以上、跨界 delta、mixed batch 和 epoch marker 仍读取。旧实现实际下载全部已覆盖对象；修改后通过。[相关包 race](cold-recovery-2026-10-25/affected-race.txt) 覆盖 lease、owner、replica、recovery、server。

原 follower 自动选择测试扩展为两次写入：冷发现时第一笔由 bucket 确认，后台发现完成后第二笔由 fleet 确认；没有重放第一笔。日志订阅 fanout 回归保持通过。

## 真实 HK 对照

同本机进程、HK COS 专用测试桶、bucket 持久性、预载 3 MiB、restore parallel=4、1s 门槛，独立测试 namespace，未部署到 HK 机器。单次故障场景不能推断生产长期 p99。

| 阶段 | 原版 | 仅 lease 修复第一轮 | 最终版 |
| --- | ---: | ---: | ---: |
| 句柄回收后首写 | 119ms | 109ms | 130ms |
| SIGKILL 重启首写 | 8698ms | 853ms | 974ms |
| 换节点首请求 | 9471ms | 2046ms | 1673ms |
| 换节点故障到成功 TTFS | 18666ms | 13538ms | 11909ms |
| 清空 cells 重建首写 | 9177ms | 1700ms | 2318ms |

[第一轮 lease 修复](cold-recovery-2026-10-25/lease-only-hk.txt) 消除了约 9s 的同步全量扫描，重启首写降至 1s 内，但重建 1.7s 仍未通过原门槛，未掩盖此失败。最终无插桩产物结果见 [HK cold](cold-recovery-2026-10-25/final-hk.txt)：重启首写 974ms，完全冷重建 2318ms，原 1s 门槛仍失败。140 个已确认键 SHA256 全部一致，SQLite integrity 正常；清理测试应用 HTTP 200。稳态 PUT p50/p99 为 41.8/333.1ms，GET p99 为 1.4ms。

## 最终本地验证

`make test vet build`、相关 lease/owner/replica/recovery/server race 均 exit 0；lease race 连续 10 次通过。最终无插桩 cell-agent SHA256：`8df9353c461e7f5075fd5e1d17cba5e950eca0fbfe90a69eb0789c0ca42f4e52`。未提交、未部署。

诊断复测冷首写 2515ms，其中 owner gate 384ms、RestoreChain 635ms、Cell 694ms、CaptureEnsure 826ms；这些是嵌套阶段，不能直接相加。不存在此前全节点扫描的 9s 停顿，首次持久性等待的补充测量见下。

最终无插桩产物再次运行 [真实 HK bindings/Queue E2E](cold-recovery-2026-10-25/final-bindings.txt)：R2 不同/同内容覆盖、HEAD/GET/range，D1 20000 行，以及 Queue 重试→DLQ、双 agent 杀 owner 后消费从 5 增至 8 全部通过（exit 0）；三个独立测试应用均清理 HTTP 200。D1 五次 HTTP 总耗时 71–103ms。

## 剩余性能边界

补充临时插桩复测（不是最终产物性能验收）冷重建首写 1319ms，重启 785ms，仍未达到冷重建 1s 门槛。140 个确认键校验、SQLite integrity 和清理再次成功。见 [完整结果](cold-recovery-2026-10-25/wait-trace-hk.txt)、[节点分段](cold-recovery-2026-10-25/wait-trace-agent-b.txt)。

该次冷重建 owner gate 430ms、Cell 368ms（内含 RestoreChain 309ms）、capture Ensure 487ms（内含 Cell）、持久性等待 237ms；backgroundBaseline 总计 241ms。源码中 RequestBaseline 在同一个 capture 循环运行，SnapshotStreamPages 在 writeMu 内编码并提交快照，因此“后台发起”不代表不会阻塞第一笔写。上述时间是嵌套/重叠观测，不把二者总计重复相加，也不能把另一次 2.5s 的全部余量归因于快照。

本次完成同步全节点发现和已覆盖日志冗余下载的确定性修复，未实现后台快照与增量提交解耦。进一步降低冷重建延迟需要分别优化 owner 元数据往返和基线快照调度；快照解耦涉及一致水位、cursor reset、失败重试与跨 epoch 完整性，不能靠延迟启动快照或提前响应绕过。真实 COS 单次点查也观测到约 817ms 波动，当前结果不支持承诺全部冷首写低于 1s。

后续 ADR-294 已实施后台快照解耦；本记录中的“未实现”描述的是 ADR-293 当时状态，最新验证见 [后台快照后续记录](detached-snapshot-2026-10-25.md)。
