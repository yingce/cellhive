# TODO / 暂停项

_最后更新：2026-10-28_

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
