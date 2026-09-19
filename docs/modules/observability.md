# 模块：可观测性（指标 / 日志 / 追踪）

指标、租户日志 tail、OpenTelemetry OTLP traces 与 logs 导出。

> 配置权威来源见 [`../configuration.md`](../configuration.md) 与代码；下表是本模块相关子集。
## 关键接口

`/metrics`、`/v1/control/logs`（admin）、`/v1/internal/logs`（log 角色）、`/v1/internal/telemetry/spans`、`/v1/control/logs/subscribe`。

## 配置（环境变量）

| 变量 | 默认 | 作用 |
|---|---|---|
| `CELLHIVE_LOG_BUFFER` | `1000:200` | 日志尾缓冲 `<entries>:<workers>`（`cellhive tail --worker`） |
| `CELLHIVE_OTLP_ENDPOINT` | 空=关 | OpenTelemetry OTLP/HTTP 导出端点（如 `http://collector:4318` 或 OpenObserve `http://openobserve:5080/api/default`；完整 `/v1/traces` URL 也接受）。空 = 完全关闭、零开销（ADR-167；见 [`tracing.md`](../tracing.md)） |
| `CELLHIVE_OTLP_HEADERS` | 空 | OTLP 导出请求头，`k=v,k2=v2`（鉴权用） |
| `CELLHIVE_OTLP_LOGS` | `off` | OTLP logs 导出：`off`=关；`tail`=只有 `cellhive tail` 订阅的 worker 导出；`all`=全部（ADR-172；需 `CELLHIVE_OTLP_ENDPOINT`） |
| `CELLHIVE_SERVICE_NAME` | `cellhive-cell-agent` | OTLP Resource 的 `service.name`（user-runtime 里入口采样用同一变量） |
| `CELLHIVE_TRACES_SAMPLE_RATIO` | `0.01` | 入口新 trace 的头部采样比例（上游 `traceparent` 的 sampled 位优先） |


> 解析规则（字符串/布尔/时长/字节/列表）见 [`../configuration.md`](../configuration.md#解析规则)。

## 关键不变量

- 追踪是旁路、best-effort，不影响请求；无端点=零开销；导出失败/丢弃不阻塞请求。
- 租户日志行带 `trace_id`/`span_id`（请求内触发时）；resource 带 `service.name`/`service.instance.id`(=节点)/`cellhive.node_id`。
- 日志 ring 有界、非持久、单节点；`tail` 订阅经 lease 广播到活节点。
- 新增指标先登记名称与数据源（`observability.md`）。

## 源码位置

`internal/{telemetry,logbuf,nodelog}`；`internal/server/*`（metrics/spans/logs 端点）；`workerd/platform/{telemetry.js,log-tail.js}`。

## 测试锚点

`internal/telemetry`、`internal/server`、`internal/userruntime`（real workerd）。

## 相关文档

[`observability.md`](../observability.md)、[`tracing.md`](../tracing.md)

_最后更新：2026-09-19_
