# 模块：bindings 与兼容面

把每种 Cloudflare binding 映射到平台资源与 host adapter，并定义同步/异步边界与拒绝项：KV、D1、R2、Queue、Cron、Workflows、Assets、Vars/Secrets、AI（BYO）、Hyperdrive、Vectorize、Service。

> 配置权威来源见 [`../configuration.md`](../configuration.md) 与代码；下表是本模块相关子集。
## 关键接口

cell-agent 上的 `/v1/kv/*`、`/v1/d1/*`、`/v1/r2/*`、`/v1/queue/*`、`/v1/workflow/*`、`/v1/vectorize/*`、`/v1/service/run`、`/v1/internal/hyperdrive`。

## 配置（环境变量）

| 变量 | 默认 | 作用 |
|---|---|---|
| `CELLHIVE_AI_URL` / `_KEY` | 空 | BYO AI 端点；空=不注入 |
| `CELLHIVE_BINDING_CACHE` | `1s` (0 关) | binding 声明缓存 |
| `CELLHIVE_NS_RATE` | 空（关） | 每 ns 写准入 `rps[/burst]`（缺省 burst=rps） |


> 解析规则（字符串/布尔/时长/字节/列表）见 [`../configuration.md`](../configuration.md#解析规则)。

## 关键不变量

- binding 名 = 已登记资源名（无自动 provisioning）。
- **平台 env 键已压到最小**：租户 env 里的平台键只剩 **`CH_PLATFORM`**（平台桥：workflow step op 表 + 日志上送，身份绑 props）与 DO worker 的 **`CH_DO_CONNECT`**（仅 DO WebSocket 的 cluster-only 传输）；绑定名清单（R2/DO）经模块作用域常量 `__cellhivePlatform.*` 注入，不占 env 名。**保留命名空间**：`CH_*`/`CELL_*`/`__cellhive*` 与平台身份键（`PLATFORM`/`LOG_NS`/`LOG_WORKER`/`LOG_TOKEN`/`WF_*`）仍保留，用户的 var/secret/binding 名落在其中会被部署期拒绝（`reserved_env_name`），避免静默覆盖（ADR-184）。
- 每个 binding 一个 scoped token（HMAC）。
- 读**强一致**（转发 owner）。
- 对 CF 拒绝项**显式拒绝**（Cache API、Sessions、SSE-C…），未知字段/flag 部署期拒绝。
- KV 超出 CF API 的能力走**原子存在性条件写**（ADR-182，平台侧 "onlyIf"）：`put?if_exists=absent|present` 与写入同事务原子判定（失败 `200 {applied:false}`，非错误；无关 key 不影响；过期行计为 absent）；`POST /v1/kv/incr?by=`（默认 1，可负）读-加-写单事务原子，非整数 `400 not_integer`。

## 源码位置

`internal/{kv,d1,r2,queue,workflow,vectorize}`（在 cellstore 之上）；`workerd/platform/{bindings.js,facades.js,bindings-wrapper.js}`；`internal/server/*_binding*.go`。

## 测试锚点

`make js-test`、`internal/server`、`make cli-test`（wrangler 映射）。

## 相关文档

[`bindings.md`](../bindings.md)、[`compatibility-matrix.md`](../compatibility-matrix.md)、[`wrangler-compat.md`](../wrangler-compat.md)

_最后更新：2026-09-19_
