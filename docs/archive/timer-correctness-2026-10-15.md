# Cron / Workflow / DO alarm 正确性验收（2026-10-15）

## 实现

1. Cron occurrence 持久化 `{slot,cron}`，同分钟不同表达式独立派发，旧分钟格式可读；timer upsert 不复活 fired token。
2. scheduled wrapper 在租户 isolate 构造 `noRetry()`；普通数据返回结果，不依赖跨 isolate 闭包状态。
3. Workflow busy claim 保留 timer，并发只计有效 running lease。create/batch、event、resume/restart 与 `workflow-run` outbox 同事务；claim 同事务建立 lease-expiry 恢复任务。
4. wait/sleep 原子 park、释放 run token、建立 deadline timer；wait 检查竞态到达的匹配事件，有则排队，无关事件不延长 deadline。
5. DO alarm 通过 props 中对象身份绑定的 `AlarmScheduler` service capability 先持久化 timer，再在 `blockConcurrencyWhile` 中写时间与随机 alarm ID。准备不删除旧 generation；派发携带 generation/due。active→pending 同 native storage transaction；成功消费 pending，失败保留 retryCount；新 active 不受旧 pending 消费影响。

## 失败复现与回归

观察旧实现失败后再修复：

- `TestSchedulerKeepsCoincidentTriggersDistinct`：两条表达式只有一条任务。
- `TestUserRuntimeDispatchesTimerToTenantHandler`：noRetry 抛错仍返回 500。
- `TestWorkflowBusyClaimKeepsWakePending`：暂拒 claim 返回 nil。
- `TestWorkflowWaitReleasesRunLease`：等待后仍为 running。
- `TestQueuedInstancesDoNotBlockEachOther`：两个 queued 在 limit=1 时均无法 claim。
- `TestCreatePersistsDispatchWake`：创建后没有持久化启动任务。
- `TestDoRuntimeAlarmConsumedAndStaleDeliveryIgnored`：成功触发后仍保留旧 alarm 时间。
- 扩展 `TestSchedulerMaterializesAndDedups`：下次扫描复活 fired slot。

修复后上述测试通过；另补充：

- `TestWaitParkAndEventCannotLoseWake`、`TestWaitParkUnrelatedEventPreservesDeadline`
- `TestWorkflowOutboxRetriesBusyRunAfterReopen`
- `TestDOAlarmPrepareDoesNotErasePreviousGeneration`
- `TestDoRuntimeAlarmHandoffFailsClosed`、`TestDoRuntimeAlarmRetryAndRearm`
- `TestDoRuntimeDeleteAlarmCancelsPendingRetry`：先复现取消后仍调用失败 handler，修复为清除 active 与 pending 后通过。
- `TestCronDispatchKeepsPersistedExpression`

## 门禁命令

Go SQLite tags 统一为 `sqlite_fts5 sqlite_dbstat sqlite_math_functions sqlite_column_metadata sqlite_preupdate_hook`。已执行：

```bash
go test -tags 'sqlite_fts5 sqlite_dbstat sqlite_math_functions sqlite_column_metadata sqlite_preupdate_hook' ./... -count=1 -p 1
go test -race -tags 'sqlite_fts5 sqlite_dbstat sqlite_math_functions sqlite_column_metadata sqlite_preupdate_hook' ./internal/timer ./internal/cron ./internal/workflow ./internal/dispatch ./internal/server ./cmd/cell-agent -count=1
make js-test
go vet -tags 'sqlite_fts5 sqlite_dbstat sqlite_math_functions sqlite_column_metadata sqlite_preupdate_hook' ./...
go build -tags 'sqlite_fts5 sqlite_dbstat sqlite_math_functions sqlite_column_metadata sqlite_preupdate_hook' ./...
bash scripts/runtime-baseline-e2e.sh
git diff --check
```

全量 Go、相关 race、JS/runtime、vet/build 与 diff 检查通过。`make js-test` 与 Docker 并行时曾在 userruntime 输出 PASS 后遇到 `WaitDelay expired before I/O complete`；独立执行相同命令通过，未将失败运行计为通过。

## Docker 完整链路

工具：stock `workerd 2026-09-16`，`esbuild 0.28.2`。隔离 Compose project/volumes，真实 cell-agent + user-runtime + gated do-runtime。

- 同分钟 `* * * * *`、`*/1 * * * *` 均写入执行凭证；前者 noRetry 后抛错，无重复 slot 凭证。
- Workflow definition 并发上限为 1，batch-a/b 均完成。
- 无 timeout 的事件等待显示客户端 `waiting`，发送事件后恢复并完成，payload 正确。
- DO alarm 第一次故意失败；重试成功并设新 alarm；新 alarm 到期，计数为 3、getAlarm 为 null。
- 再设置 alarm，重启 gated runtime；恢复后计数为 4、getAlarm 为 null。
- 既有 KV、Queue/DLQ、assets、DO SQL/公开 WebSocket 重启持久性、rollback/delete、边界检查继续通过。

最终输出：`RUNTIME-BASELINE-E2E: PASS`。

## 边界

外部副作用仍至少一次；DO 重试节奏/上限非 CF native 策略；`transactionSync` 内 alarm 不支持。旧分钟 Cron/数字 alarm 读取兼容，但不能混用忽略 `workflow-run` 或 generation 的旧 dispatcher。本次是本地 Docker/pinned workerd 验收，不代表云多节点验收。
