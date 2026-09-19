# 模块：timers 与派发

统一定时器抽象：cron、queue 延迟/重试、DO alarm、workflow sleep/timeout、KV 过期；每个 owner 派发自己的 cell，单 fleet waker 兜"owner 已死/无主"的项。

> 配置权威来源见 [`../configuration.md`](../configuration.md) 与代码；下表是本模块相关子集。
## 关键接口

timer 行存于所属 cell SQLite；`POST /v1/internal/kv/expire`、`POST /v1/internal/do/alarm/upsert`、`POST /v1/timers/dispatch`（到 user-runtime）。

## 配置（环境变量）

| 变量 | 默认 | 作用 |
|---|---|---|
| `CELLHIVE_CRON_INTERVAL` | `30s` (0 关) | cron→timer 物化 |
| `CELLHIVE_DRAIN_TTL` | `30s` | drain token 有效期（关停等待 = 同值） |
| `CELLHIVE_QUEUE_BATCH` / `_LEASE` | `0`（store 默认） | 单批消息数 / 可见性租约 |
| `CELLHIVE_QUEUE_INTERVAL` | `1s` (0 关) | queue 消费 |
| `CELLHIVE_QUEUE_RETRY_DELAY` | `30s` | 失败批次的重投延迟（重试上限/DLQ 由 per-consumer 配置） |
| `CELLHIVE_TIMER_BATCH` / `_FIRED_TTL` | `256` / `24h` | 单 pass 到期上限 / fired 标记保留 |
| `CELLHIVE_TIMER_INTERVAL` | `1s` (0 关) | 到期计时派发 |
| `CELLHIVE_WAKER_BACKOFF_MAX` | `1m` | 连续错误时的退避封顶（base=interval，`interval·2^fails`） |
| `CELLHIVE_WAKER_BATCH` / `_FIRED_TTL` | `256` / `24h` | 单 pass 上限 / fired 标记保留 |
| `CELLHIVE_WAKER_INTERVAL` | `5s` (0 关) | fleet waker（TTL 自动取 2×） |
| `CELLHIVE_WAKE_REPAIR_BATCH` | `256` | 单 pass 检查的本地 cell 数（轮转窗口，几趟覆盖全部） |
| `CELLHIVE_WAKE_REPAIR_INTERVAL` | `5m` (0 关) | 本地 wake 索引修复扫描（ADR-177）；启动先跑一次 |


> 解析规则（字符串/布尔/时长/字节/列表）见 [`../configuration.md`](../configuration.md#解析规则)。

## 关键不变量

- due 记录在 cell SQLite（**权威**），bucket `wake/` 只是**发现索引**。
- **索引不落后于 timer**（先发布再提交；失败 fail-closed）。
- 派发 at-least-once，成功才标 fired（TTL 去重）。
- cron 分钟对齐、best-effort、不补跑。

## 源码位置

`internal/{timer,dispatch,cron,waker,wake,queue}`；`cmd/cell-agent/main.go`（runner/waker 循环）。

## 测试锚点

`internal/timer`、`internal/dispatch`、`internal/cron`、`internal/wake`。

## 相关文档

[`timers-and-dispatch.md`](../timers-and-dispatch.md)

_最后更新：2026-09-19_
