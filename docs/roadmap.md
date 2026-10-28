# 路线图与剩余工作

_最后更新：2026-10-28_

历史路线图（2026-09，P0–P5 退出标准）已归档到 [roadmap-2026-09](archive/roadmap-2026-09.md)。P0–P5 已交付；本文件只记录当前状态和剩余项，不再重复历史阶段。

## 当前状态

- 计算层：`cellhive-workerd` fork（ADR-264）；CellHive 通过 `workerLoader`、bindings 与 capnp 集成，不在本仓改 workerd。
- 状态层：SQLite + 对象存储权威，owner/epoch/lease、RPO=0、fleet 复制已上线。
- 生产：SG Helm revision 79、USW 63（源码 `49133b1`），见 [release-2026-10-28](release-notes.md)。
- 完整功能与兼容边界以 [compatibility-matrix](compatibility-matrix.md)、[bindings](bindings.md)、[known-issues](known-issues.md) 为准。

## 剩余工作（按风险排序）

1. **冷恢复延迟**：HK COS 真实冷 GET 仍在 1.1–3.1s 区间；控制 cell 冷鉴权、owner/claim 等待与 COS 尾延迟未完全分段消除。需要同区 fleet、公开入口与不同 spool 规模下的复测。
2. **fleet 写/读长尾**：binding/lease/resource-fence 刷新导致周期性 58–150ms 长尾；退出期 lease watchdog 抢断 handoff 尚未修复。
3. **跨主机与多 AZ 故障**：k3d A→B→A / A-B-C 已通过；真实跨主机 RTT、COS 跨 AZ、并发 epoch fence 与备份恢复仍缺生产验收。
4. **对象存储矩阵**：COS/OSS 需逐桶通过 native-append 与条件删除；固定 MinIO 不能作为 owner/lease 权威桶。其他 provider 未验证。
5. **Durable Objects**：滚动更新后偶发连接/地址恢复问题与 recovery pointer CAS 告警仍需根因与自动恢复验证；真实 COS 故障窗口未演练。
6. **Workflow / Queue 边界**：Workflow 仍不支持跨 Worker、step 历史/progress；Queue 仍有跨批顺序与部分 retry/DLQ 边界。
7. **Hyperdrive 剩余项**：per-resource proxy/direct override、多副本配置分发、`pg` CJS shim 内置、ProxySQL 代理 HA 演练。
8. **可观测性与文档**：Tail 超过 10 分钟 token 的无缝续接未验收；英中文文档镜像需持续同步，历史数据恢复未做全量审计。

## 验证原则

- 设计变更先更新 `docs/decisions.md`，状态同步到 `docs/known-issues.md`。
- 未在真实环境验证的能力标记为 Partial/未验证，不以本地推演代替验收。
