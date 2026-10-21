# TODO / 暂停项

_最后更新：2026-10-21_

## Hyperdrive（暂停，ADR-255/256）

已完成：direct/proxy 开关、协议端点改写、logical DB、动态+静态代理 e2e、真实 `pg`/`postgres.js` 驱动验证、MySQL direct + ProxySQL proxy e2e（ADR-261）。

暂停后待办：

1. ✅ ProxySQL create/delete 自动同步、30s reconcile、多 admin fanout（ADR-262）；生成式客户端凭据（ADR-263）；MySQL/PostgreSQL dynamic e2e 通过。
2. ✅ 绑定级生成凭据（ADR-263）；origin 轮换由 reconcile 处理。
3. **proxy 健康检查/fail-closed**：endpoint 不可达时快速失败，而不是只检查非空。
4. **per-resource proxy/direct override**：目前是全局 `CELLHIVE_HYPERDRIVE_MODE`。
5. **多副本配置分发**：ETag/long-poll/watch + 每 Pod 独立渲染，或 controller + ConfigMap。
6. **`pg` CJS shim 内置到 bundler**：当前由租户按文档手工打包。
7. **同 Pod / per-node ProxySQL**：只有在 HPA 副本少、延迟敏感时再评估。
8. **代理 HA 故障演练**：多副本 ProxySQL 挂一个后的行为。

## 其他待解决

- ✅ R2 `writeHttpMetadata` 在 queue/workflow 路径的 per-call facade（ADR-259）。
- ✅ Workflow 长时间 sleep、事件超时、durable retry e2e（ADR-257）。
- ✅ Queue 多节点 retry/DLQ/failover（ADR-259）；批量超时已有单测；并发 smoke 已通过（ADR-260）。
- ✅ 多节点 static 路由表按设计不全局同步（每节点独立 hot set）。
- ✅ Workflow/Queue/Bindings e2e 入仓：`make e2e`（KV/D1/R2、workflow、queue 单/多节点/并发）；MySQL auto 为 `make e2e-mysql`；本机 Hyperdrive profile 已加。
