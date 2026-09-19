# 计时器与派发

## 统一定时器抽象

所有定时事件统一为：

```
timer = { dueAt, kind, scope, token }
kind ∈ { do-alarm, cron, queue-delay, queue-retry, workflow-sleep, workflow-timeout }
```

- **存储**：due 记录落在**各自所属 cell 的 SQLite**（时间索引/时间桶）；**不建 bucket 计时索引**；
- **去重**：`token = hash(scope, kind, dueAt, occurrence)`；cell SQLite 记 `fired:<token>`（TTL）；**已 fired 跳过**（ADR-033）。

## 触发：两层

| 层 | 谁 | 覆盖 | bucket |
|---|---|---|---|
| **本地 due** | 每个 `cell-agent` 为所拥有的计时器 cell 维护本地最早到期结构 | owner 健在的项 | 0 |
| **单 fleet waker** | bucket 租约选出的一个 leader | owner 已死/失联的项 | 接管时点查 owner 记录 |

- **派发寻址**：发给 `user-runtime` 的**逻辑服务名**（mesh），任一健康副本；**不需要节点表**；
- **前提**：含待触发计时器的 cell 必须 **resident/pinned**。

## 各类语义

| kind | 语义 | 去重 |
|---|---|---|
| **cron** | 分钟对齐、**best-effort**、**不补跑** | 按 slot 唯一键单次 |
| **queue-delay / retry** | 至少一次；延迟/重试/DLQ | queue cell 的 claim/ack |
| **workflow-sleep / timeout** | 绝对时间；至少一次 | 按 run token/step 状态 |
| **do-alarm** | 见下 | 以 actor SQLite alarm 行为准 |

## queue 派发

1. 生产者写消息到 queue cell；
2. cell-agent 的本地 due 循环按 consumer 配置取批（`max_batch_size` 等），调 `user-runtime.queue()`；
3. 结果 → ack / retry / DLQ；延迟消息进 delayed 索引；
4. 重复派发由 queue cell 的 claim 抑制。

## cron 派发

> **状态（2026-09-15）**：**全链路已实现**——调度器（ADR-076：`internal/cron`，5 字段 UTC，物化 slot 为 `KindCron` 定时器，`CELLHIVE_CRON_INTERVAL` 默认 30s）→ 派发（ADR-070：`Projection.CronTargets()` → `cronEnricher` 补 worker/bundle → user-runtime `POST /v1/timers/dispatch` → `scheduled(event, env, ctx)`，真实 workerd e2e）。scope 约定 `<ns>/__cron__/<worker>`、occurrence=slot、token 去重、best-effort 不补跑、每节点可跑。

- cron 配置存 `__cron__` cell；owner 的 cell-agent 计算下一次触发时间；
- 分钟 slot 唯一键保证**每 slot 单次**；错过不补跑。

## DO alarm

> **状态（2026-09-15）**：**平台 shim 已实现**（ADR-079）——facet 原生 alarm 不可用，改用 `cellhive-do.js` shim（set/get/deleteAlarm 落对象 storage 保留键）+ host 上报 cell-agent + 统一 `KindDOAlarm` timer + `DoAlarmDispatcher` → do-runtime `kind:"alarm"` 调 `alarm()`。以下为原设计（supervisor 只读 `_cf_ALARM`）保留作对照。

- alarm 行在 actor SQLite（随复制持久）；
- **supervisor 只读 workerd alarm 表**提取 due → 上报 cell-agent（`alarm_upsert/delete`）；
- cell-agent 在 **waker cell** 建 due 索引；
- waker 对"owner 死 + 到期"项 → **定向 takeover** 到可用 do-runtime，新 owner 读 alarm 派发；
- **带 alarm 的 DO 优先常驻**。

## 为什么没有独立 scheduler 服务

计时/派发统一在 `cell-agent`（ADR-009）：与 cell 归属一致（owner 派发自己的 cell），避免中心派发点与全局扫描；waker 只兜底死 owner。

## 定稿（ADR-152）

**Queue（消费者）**
- 轮询 `CELLHIVE_QUEUE_INTERVAL`（默认 1s，0=关）；单批大小 `CELLHIVE_QUEUE_BATCH`（0=store 默认）；可见性租约 `CELLHIVE_QUEUE_LEASE`（0=store 默认）。
- 失败重投延迟 `CELLHIVE_QUEUE_RETRY_DELAY`（默认 **30s**，batch 级 Retry）；重试上限与 DLQ（`dead_letter_queue`）由 **per-consumer 配置**（`max_retries`/`dead_letter_queue`/`max_batch_size`/`max_batch_timeout_seconds`/`max_concurrency`，ADR-072/112）决定，超限进 DLQ。
- 语义：at-least-once（claim→dispatch→ack/retry；整个 bat​ch 失败即整批重投），只有 owner 消费（`OwnerGate`），每次 store 变更走捕获证明（ADR-119）。

**Timer（派发）**
- 轮询 `CELLHIVE_TIMER_INTERVAL`（默认 1s，0=关）；单 pass 到期上限 `CELLHIVE_TIMER_BATCH`（默认 **256**）；fired 标记保留 `CELLHIVE_TIMER_FIRED_TTL`（默认 **24h**，防重复派发）。
- 派发 at-least-once：成功后才标记 fired；崩溃/失败留待下轮。

**Waker（单 fleet wake + 死节点恢复）**
- 轮询 `CELLHIVE_WAKER_INTERVAL`（默认 5s，0=关）；租约 TTL = **2×interval**；单 pass `CELLHIVE_WAKER_BATCH`（默认 256）；fired TTL `CELLHIVE_WAKER_FIRED_TTL`（默认 24h）。
- **退避**：连续错误时循环延迟按 `interval·2^fails` 增长，封顶 `CELLHIVE_WAKER_BACKOFF_MAX`（默认 **1m**）；成功后立即复位。空闲（0 派发）不触发退避。

**timer cell 的 resident/pinned 策略（决定）**
- **不做 pinning**：timer cell 是**普通可驱逐 cell**，由统一的 `CELLHIVE_MAX_RESIDENT_CELLS`/`CELLHIVE_CELL_IDLE`/盘预算管理；正确性不依赖驻留——到期项存在 cell（权威）与桶 `wake/` 索引里，waker 用 `wake` 索引按 scope 定位（不 List 全量），需要时按需重开。
- 容量：随 `CELLHIVE_CELLS_PER_NODE` 与磁盘预算；无单独的 timer 配额。

_最后更新：2026-09-17_
