# 计时器与派发

## 调度正确性修订（2026-10-15）

- Cron occurrence 对新任务包含分钟 slot 和完整表达式；同分钟不同表达式独立触发，重复物化同一表达式仍去重。旧分钟 occurrence 继续可读。`scheduled.noRetry()` 在租户 wrapper 中构造，通过普通数据返回派发结果，禁止依赖跨 isolate 的闭包状态。
- Workflow 的临时 claim 拒绝不能视为成功消费；初次启动必须具有持久化重试入口。等待事件和 sleep 统一可靠 park，事件与 park 交错不能丢唤醒；终态/暂停与暂时繁忙须区分。
- DO alarm 必须可靠交接到 cell-agent 定时器，不能在上报未确认时静默确认设置成功；须覆盖对象写入与索引建立之间的崩溃恢复。派发携带 alarm 身份，陈旧/已取消任务不得执行业务 handler；成功消费旧任务不得清除 handler 新设置的 alarm，失败保留可重试身份。
- 验收：真实 pinned workerd 覆盖 scheduled/noRetry 与 DO alarm；Docker 完整链路覆盖双 Cron、Workflow 并发排队/事件唤醒、alarm 重试/重设及待触发 alarm 跨 runtime 重启。命令与结果见 [验收记录](archive/timer-correctness-2026-10-15.md)。

实现细节：Workflow `workflow-run` outbox 与实例/事件同事务提交，claim 同事务建立 lease 过期恢复任务；wait park 原子释放租约并检查已到达事件。DO `setAlarm()` 在 `blockConcurrencyWhile` 中经对象绑定 capability **先建立 timer，再写 alarm ID/时间**；失败不确认。触发时 active 原子移入 pending，成功消费 pending、失败保留供重试；新设置的 active 不被旧任务清除。准备过程崩溃留下的陈旧 timer 由 generation 检查安全跳过。

注意：DO 重试仍为 runner 轮询重投，不承诺 Cloudflare 原生指数退避/重试上限；普通 JS step 的外部副作用与 DO alarm handler 均需按至少一次执行设计。旧版本 task 格式采用惰性读取兼容；升级不能混用忽略新 kind/generation 的旧 dispatcher。

## 统一定时器抽象

所有定时事件统一为：

```
timer = { dueAt, kind, scope, token }
kind ∈ { do-alarm, cron, queue-delay, queue-retry, workflow-sleep, workflow-timeout, workflow-run, workflow-schedule, kv-expire }
```

- **存储**：due 记录落在**各自所属 cell 的 SQLite**（权威）；桶 `wake/` 只索引每个 timer scope 最早到期时间，供故障恢复发现，不承载逐任务权威状态；
- **去重**：`token = hash(scope, kind, dueAt, occurrence)`；cell SQLite 记 `fired:<token>`（TTL）；**已 fired 跳过**（ADR-033）。

## 触发：两层

| 层 | 谁 | 覆盖 | bucket |
|---|---|---|---|
| **本地 due** | 每个 `cell-agent` 为所拥有的计时器 cell 维护本地最早到期结构 | owner 健在的项 | 0 |
| **单 fleet waker** | bucket 租约选出的一个 leader | owner 已死/失联的项 | 接管时点查 owner 记录 |

- **派发寻址**：发给 `user-runtime` 的**逻辑服务名**（mesh），任一健康副本；**不需要节点表**；
- **驻留**：不要求 resident/pinned；按需重开与 wake 索引恢复见下方 ADR-152 定稿。

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

> **状态（2026-10-19）**：调度器（ADR-076：`internal/cron`，5 字段 UTC，物化 slot 为 `KindCron` 定时器，`CELLHIVE_CRON_INTERVAL` 默认 30s）→ 派发（ADR-070：`Projection.CronTargets()` → `cronEnricher` 补 worker/bundle → user-runtime `POST /v1/timers/dispatch` → `scheduled(event, env, ctx)`）。scope 约定 `<ns>/__cron__/<worker>`；新 occurrence 包含 slot 和表达式。ADR-226 修复双节点各自派发同一 slot：只有当前 cell owner session 可物化、派发和写入 fired/retry，写入等待捕获证明。故障注入复验见兼容验收记录。

- cron 配置存 `__cron__` cell；owner 的 cell-agent 计算下一次触发时间；
- 新 occurrence 包含分钟 slot + 表达式，保证**每表达式每 slot 去重**；错过不补跑。派发成功但 fired 尚未持久化的崩溃仍可能重投。
- Timer Runner 和 fleet waker 在打开本地 SQLite 前确认或认领 owner；接管后从权威链恢复。没有过期 fired 行时，轮询不执行清理写事务。
- 同一 Scheduler 进程对已成功持久确认的 `(scope, cron, UTC slot)` 只物化一次；新进程或新 owner 可按同一 token 安全重试，避免每秒产生无意义的 LTX 段。

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
- 失败重投优先使用 per-consumer `retry_delay`（秒），未配置时使用 `CELLHIVE_QUEUE_RETRY_DELAY`（默认 **30s**）；显式 `retry({delaySeconds})` 优先于两者。`max_batch_timeout_seconds` 从最早可见消息起算，满批立即派发。`max_retries` 省略或旧值 0 时按首次投递后 3 次重试，超限进 `dead_letter_queue`；无 DLQ 时确认并记录丢弃（ADR-216）。
- 语义：at-least-once（claim→dispatch→ack/retry；整个 bat​ch 失败即整批重投），只有 owner 消费（`OwnerGate`），每次 store 变更走捕获证明（ADR-119）。

**Timer（派发）**
- 轮询 `CELLHIVE_TIMER_INTERVAL`（默认 1s，0=关）；单 pass 到期上限 `CELLHIVE_TIMER_BATCH`（默认 **256**）；fired 标记保留 `CELLHIVE_TIMER_FIRED_TTL`（默认 **24h**，防重复派发）。
- 派发 at-least-once：成功后才标记 fired；崩溃/失败留待下轮。
- 失败 cron token 的下一次尝试时间与次数写入 timer cell SQLite，按 5 秒起、最多 5 分钟的指数退避；原 slot/due_ms 不变，同 token 重复物化不清除退避，其他 timer 类型保持原重试语义（ADR-211）。

**Waker（单 fleet wake + 死节点恢复）**
- 轮询 `CELLHIVE_WAKER_INTERVAL`（默认 5s，0=关）；租约 TTL = **2×interval**；单 pass `CELLHIVE_WAKER_BATCH`（默认 256）；fired TTL `CELLHIVE_WAKER_FIRED_TTL`（默认 24h）。
- **退避**：连续错误时循环延迟按 `interval·2^fails` 增长，封顶 `CELLHIVE_WAKER_BACKOFF_MAX`（默认 **1m**）；成功后立即复位。空闲（0 派发）不触发退避。

**timer cell 的 resident/pinned 策略（决定）**
- **不做 pinning**：timer cell 是**普通可驱逐 cell**，由统一的 `CELLHIVE_MAX_RESIDENT_CELLS`/`CELLHIVE_CELL_IDLE`/盘预算管理；正确性不依赖驻留——到期项存在 cell（权威）与桶 `wake/` 索引里，waker 用 `wake` 索引按 scope 定位（不 List 全量），需要时按需重开。
- 容量：随 `CELLHIVE_CELLS_PER_NODE` 与磁盘预算；无单独的 timer 配额。

_最后更新：2026-09-17_
