# 模块文档

本目录按**模块**拆分设计说明：每个模块给出一句话作用、关键接口、**环境变量表（变量 / 默认 / 作用）**、必须守住的不变量、源码位置与测试锚点。配置的**完整清单**与解析规则见 [`../configuration.md`](../configuration.md)（权威来源是代码）。

## 模块导航

| 模块 | 作用 | 文档 |
|---|---|---|
| `cell-agent` | 固定集群的状态与控制节点：拥有并复制 KV/D1/Queue/Workflow/Cron 的 cell（每 cell 一份 SQLite），维护 owner/epoch 租约，派发计时器，托管控制面，并是**唯一长期持有对象存储凭据**的进程。 | [`cell-agent.md`](./cell-agent.md) |
| `user-runtime` | 公开入口 `:8081` + 内部特权派发 `:8088`：按 host/版本解析路由，用 `workerLoader` 动态加载不可变 bundle，注入 binding facade，执行租户的 `fetch`/`scheduled`/`queue`；租户出网仅公网。 | [`user-runtime.md`](./user-runtime.md) |
| `do-runtime` | 用固定 **host actor + facets** 承载原生 Durable Objects（每对象 SQLite、alarm shim、可休眠 WebSocket），分布式弹性；只持工作副本，不是权威。可选 `do-supervisor` 提供**输出门**，确认响应的耐久性（RPO=0）。 | [`do-runtime.md`](./do-runtime.md) |
| `bindings` | 把每种 Cloudflare binding 映射到平台资源与 host adapter，并定义同步/异步边界与拒绝项：KV、D1、R2、Queue、Cron、Workflows、Assets、Vars/Secrets、AI（BYO）、Hyperdrive、Vectorize、Service。 | [`bindings.md`](./bindings.md) |
| `timers-and-dispatch` | 统一定时器抽象：cron、queue 延迟/重试、DO alarm、workflow sleep/timeout、KV 过期；每个 owner 派发自己的 cell，单 fleet waker 兜"owner 已死/无主"的项。 | [`timers-and-dispatch.md`](./timers-and-dispatch.md) |
| `storage-and-replication` | 对象存储是权威：bucket 条件写选 owner，LTX 捕获/复制、快照/compaction、按页冷恢复（paged VFS）、GC。本地盘只是工作副本/缓存。 | [`storage-and-replication.md`](./storage-and-replication.md) |
| `control-plane` | 应用/版本/路由/域名/资源/密钥/审计与发布流水线；单控制库（app 是表里的 `ns`），版本不可变、发布原子，资源先登记再 deploy。 | [`control-plane.md`](./control-plane.md) |
| `observability` | 指标、租户日志 tail、OpenTelemetry OTLP traces 与 logs 导出。 | [`observability.md`](./observability.md) |
| `cli-and-packaging` | `cellhive` CLI（部署/运维/资源/队列/向量/工作流）、Go+esbuild 打包与 `wrangler` 配置兼容；`cli/` 是 Bun+Miniflare 的 **dev-only** 工具。 | [`cli-and-packaging.md`](./cli-and-packaging.md) |

## 阅读路径

- 想理解整体：[`architecture.md`](../architecture.md) → [`cell-protocol.md`](../cell-protocol.md) → 本目录 [`cell-agent`](./cell-agent.md) / [`storage-and-replication`](./storage-and-replication.md)。
- 想部署运维：[`deployment.md`](../deployment.md) → [`configuration.md`](../configuration.md) → [`operations.md`](../operations.md)。
- 想写 Worker：[`bindings`](./bindings.md) → [`compatibility-matrix.md`](../compatibility-matrix.md) → [`wrangler-compat.md`](../wrangler-compat.md)。
- 想改代码：[`../contributing.md`](../contributing.md)（按改动类型的阅读路径与测试锚点）。

_最后更新：2026-09-19_
