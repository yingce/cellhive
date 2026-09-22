# 模块：do-runtime / do-supervisor（Durable Objects）

用固定 **host actor + facets** 承载原生 Durable Objects（每对象 SQLite、alarm shim、可休眠 WebSocket），分布式弹性；只持工作副本，不是权威。可选 `do-supervisor` 提供**输出门**，确认响应的耐久性（RPO=0）。

> 配置权威来源见 [`../configuration.md`](../configuration.md) 与代码；下表是本模块相关子集。
## 关键接口

do-runtime `:8788`：`/v1/do/invoke`、`/v1/do/connect`（WS）、`/v1/do/{claim,renew,drain,abort,delete,restart,objects}`、`/v1/internal/do/alarm/upsert`、`/ready`。do-supervisor `:18901`：`/metrics`。

## 配置（环境变量）

| 变量 | 默认 | 作用 |
|---|---|---|
| `CELLHIVE_CELL_URL` / `CELLHIVE_ROOT_KEY` / `CELLHIVE_TENANT_OUTBOUND` / `CELLHIVE_AI_URL` / `CELLHIVE_AI_KEY` | 同上 | 与 user-runtime 相同 |
| `CELLHIVE_DATA_DIR` | `./.cellhive/data` | DO SQLite 盘 = `<DATA_DIR>/do` |
| `CELLHIVE_DO_ADDR` | `*:8788` | workerd 监听 |
| `CELLHIVE_DO_ADVERTISE` | `http://127.0.0.1:8788` | 广告地址（owner 转发目标） |
| `CELLHIVE_DO_GATE_URL` | 空 | 输出门基址；空=不过门 |
| `CELLHIVE_DO_LEASE` | `30s` | DO owner 租约（Go duration，转为整秒给 host actor） |
| `CELLHIVE_DO_NODE` | 主机名 | 节点 id |
| `CELLHIVE_DO_OBJECT_INDEX` | `false` | 对象注册表持久化到桶（`true`/`1`） |
| `CELLHIVE_DO_PREVENT_EVICTION` | `true` | resident/evictable，须恰好 `true`/`false` |
| `CELLHIVE_DO_RUNTIME_JS` | `workerd/do-runtime` | host actor JS |
| `CELLHIVE_ESBUILD` | 自动查找 | esbuild 路径 |
| `CELLHIVE_RUNTIME_DIR` | `$TMPDIR/cellhive` | 运行目录（子目录 `do-runtime/`） |
| `CELLHIVE_WORKERD` | 自动查找 | workerd 路径（优先 pin `1.20260916.1`） |


> 解析规则（字符串/布尔/时长/字节/列表）见 [`../configuration.md`](../configuration.md#解析规则)。

## 关键不变量

- facet class 必须来自 `workerLoader.getDurableObjectClass()`。
- alarm 必须 shim（stock workerd 对 SQLite facet 不实现原生 alarm）。
- WS 迁移/重启以 **1012** 关闭，客户端重连（不做 resume）。
- **门确认前不回 ack**。
- facet 的 tenant env 遵循 ADR-185：只有用户 vars 与用户命名 binding stub，零平台键；secret 尚未注入 runtime env。动态 loaded DO facet 的 Tail Worker 同样被 pin `1.20260916.1` 拒绝（`provided value is not of type 'Fetcher'`），故 tenant `console.*` 平台采集关闭。
- facet WorkerCode（64 MiB）与估算 env（1016 KiB）在 `workerLoader.get()` 前复核；跨 DO 边界丢失自定义 Error 字段时，仅解析并重新校验平台规范 LimitError，再返回 code/actual/max。

## 源码位置

`cmd/do-runtime/`、`cmd/do-supervisor/`；`internal/doruntime/`、`internal/dosupervisor/`；`workerd/do-runtime/{host.js,cellhive-do.js,bindings-wrapper.js}`；`workerd/platform/budget.js`。

## 测试锚点

`make js-test`（真实 workerd）、`internal/doruntime`。

## 相关文档

[`durable-objects.md`](../durable-objects.md)、[`workerd-integration.md`](../workerd-integration.md)

_最后更新：2026-09-22_
