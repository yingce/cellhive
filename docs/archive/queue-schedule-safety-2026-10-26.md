# Queue 与 Workflow schedule 安全修复（2026-10-26，ADR-300）

## 复现与修复

- 原 Queue HTTP dispatcher 忽略读取和 JSON 解码错误。真实 httptest HTTP 服务发送不完整 Content-Length、无效 JSON、或 result 超过 1 MiB 且 retry 位于尾部时，原 Runner 将一条待重试消息删除（acked=1, remaining=0）；完整 retry 响应则 remaining=1。
- 修复后完整 ok=true envelope 才能确认；显式 204 保留无正文确认。空 200、null、缺少/false ok、尾随垃圾、截断、超限响应均走失败重试。既有 MaxRetries/DLQ 政策不变：无 DLQ 且耗尽重试仍会丢弃并告警，不宣称无限保留。
- 原 Workflow 两条同分钟匹配的 schedule 返回 matched=2，但 SQLite 只有一个 timer。现在首条匹配表达式（字典序）保留旧分钟标识，其他使用分钟与表达式 SHA-256。旧分钟已经 fired 时仅补其他任务；重建 Scheduler、重排规则、再次 Pass 不重放已完成任务。
- 原 Runner 在慢队列未返回时无法派发另一队列。现在最多 8 个队列任务并发，保留每队列批次并发与持久门；重复队列引用合并。测试验证两队列独立进入派发、8 路上限和取消停止后续领取。仍等待本轮全部任务结束，不承诺无限慢消费者下的低延迟。

## 验证

- Queue/Cron/Timer `go test -race ... -count=3` 通过，见 [race.txt](queue-schedule-safety-2026-10-26/race.txt)。包含真实 HTTP + SQLite 回归、显式 200/204 成功、异常响应保留、schedule 兼容及调度并发边界。
- `make build test vet` 全部通过（exit 0），见 [checks.txt](queue-schedule-safety-2026-10-26/checks.txt)。
- 所有 Go 命令使用项目要求的 SQLite feature tags。
- 本次未执行 workerd + HK COS 集群 E2E，HTTP 回归不能替代端到端持久恢复验收。
- 本次未提交、推送或部署；生产状态未改变。

## 兼容边界

同一分钟修改 Workflow schedule 表达式集合可能改变首条映射，不保证跨配置修改逐表达式去重。滚动升级沿用一个旧分钟任务，其余表达式新增独立任务；不补跑历史错过的分钟。Queue 旧的空 HTTP 200 测试桩须改为 204 或有效成功 envelope；生产 user-runtime 已返回 ok=true envelope。
