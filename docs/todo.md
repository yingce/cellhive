# TODO / 暂停项

_最后更新：2026-11-10_

## Hyperdrive（暂停，ADR-255/256）

已完成：direct/proxy 开关、协议端点改写、logical DB、动态+静态代理 e2e、真实 `pg`/`postgres.js` 驱动验证、MySQL direct + ProxySQL proxy e2e（ADR-261）、ProxySQL 自动同步/生成凭据/健康检查（ADR-262/263）。

剩余项：

1. **per-resource proxy/direct override**：目前是全局 `CELLHIVE_HYPERDRIVE_MODE`。
2. **多副本配置分发**：ETag/long-poll/watch + 每 Pod 独立渲染，或 controller + ConfigMap。
3. **`pg` CJS shim 内置到 bundler**：当前由租户按文档手工打包。
4. **同 Pod / per-node ProxySQL**：只有在 HPA 副本少、延迟敏感时再评估。
5. **代理 HA 故障演练**：多副本 ProxySQL 挂一个后的行为。

## 其他

当前剩余工作（冷恢复延迟、fleet 长尾、跨主机/多 AZ、对象存储矩阵、DO/Workflow/Queue 边界等）统一记录在 [roadmap.md](./roadmap.md) 和 [known-issues.md](./known-issues.md)。已完成项不再在本文件重复。

## 存储性能后续评估

以下从旧数据路径计划提取，只保留仍有评估价值的方向；不是当前已确认缺陷，也不继承旧计划的未勾选状态或发布承诺。当前实测见 [存储性能报告](reports/2026-11-10-storage-performance.md)。

- 同区、隔离负载的 fleet 性能对照：完整阶段 trace、历史记录规模效应、spool/fsync 次数与 p99；不能从混合验收负载推断历史性能回退或提升。
- 如逐文件 fsync 仍是实测瓶颈，再评估 spool 持久化批处理；未设计/验证前不改变 ACK 条件或引入追加日志格式。
- 如空闲 CPU 或首写延迟仍不达标，复核全部写入口通知、兜底轮询及任务延迟；旧计划的 2ms/25ms 数字不是当前配置契约。
- 跨 cell bundle 仅为未采纳的探索方向，不是已授权实现的新对象格式。
- 大 cell 继续使用已定原生 LTX level/epoch 设计；旧 manifest/双格式在线兼容方案已放弃。100 epoch 深链专项按用户决定暂缓。
- R2 不可变旧正文的安全在线 GC 尚未实现，需先定义读者与预签名 URL 保留边界；不能直接清除历史版本。
