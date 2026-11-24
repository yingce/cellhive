# Redis review 修复验收（2026-11-24）

基线：main `1e321752`。本报告仅描述本地修复，不代表提交、镜像发布或生产切换。

## 修复边界

1. Cron 从原始 scheduledTime 计算下一 occurrence 的 cutoff；慢执行跨过 cutoff 后失败直接终止该轮重试，退避仍从失败完成时刻开始。
2. Nudge 使用独立、惰性初始化的合并通知通道；不等待 Pass 的 I/O 锁。Pass/Start 消费通知后重查计划版本，不丢弃当前 slot 成功记录。
3. 混合前缀分页合并 COS 原生物理页与 Redis 有界范围，以两侧已检查边界的较小值限制游标推进。保留空页、删除后续扫、对象大小/ETag/时间；只对没有 PagedLister 的兼容后端保留全量 List fallback。
4. 纯 Redis CommonPrefixes 在有界 Lua 循环中跳过整个已发现子树，无新增索引、schema 或迁移。
5. Outbox 每分钟刷新 owned 候选快照，每轮最多取 64 个通知和 64 个扫描候选；分别轮转，去重后由最多 4 个 worker 共享 3 秒预算。事务写路径通知 relay（失败的耐久等待也可能留下已提交 revision）；未处理/失败项保留。旧快照候选入队前检查本地维护登记，避免驱逐后仅凭旧快照重新开库。该登记仅是调度提示，原权威/耐久门禁仍执行。

## 回归证据

以下回归先在旧实现失败、再在对应修复后通过：

- `TestCronFailureAfterNextSlotDoesNotRetry`：60 秒 occurrence，121 秒首次失败；禁止 125 秒重试，且不留下一次 retry deadline。
- `TestNudgeDoesNotWaitForDiscovery`：发现 I/O 阻塞时 Nudge 返回，且正在运行的 Pass 结束后通知仍触发刷新。
- `TestMixedPrefixUsesBoundedPages`：5 个对象、每页 2 个；旧实现 3 次全量 List + 5 次 Stat，新实现 3 次原生分页 + 0 次全量 List/Stat。
- `TestRedisPrefixesSkipLargeSubtrees`：同一目录 3000 个键，目录查询的 Redis 往返和回复条目不随子树键数增加。统计限定本客户端，避免其他测试共享 Redis 的干扰。
- `TestScheduleOutboxSlowCellDoesNotBlockReadyCell`：一个发布阻塞，另一个 ready cell 仍在同轮完成发布。
- `TestScheduleOutboxCachedOwnedScopeDoesNotReopen`：驱逐后去除本地登记，旧快照不能重新打开该 cell。

其他边界：空物理页与游标、两端混合分页及删除、分页元数据、目录查询跨 Lua 批次和无子目录键、快照更新间隔内新 cell 的事务通知及发布失败重试。原有 stale ACK、源耐久、非 owner 拒绝和驱逐门禁测试保留。

## 验证状态

`make build vet` 通过；coordination、scheduleindex、cron、timer、owner、server 六个包的 `go test -race` 通过（包含独立 Redis 集成）。新增 outbox 回归的单独 race 检查通过。全量 `make test`（`go test -p 1 ./...`）通过。`git diff --check` 通过。首次全量/race 运行因新增故障测试未配置 logger 而失败，修正测试夹具后完整重跑通过。Redis 测试使用本机独立 `redis:7.2-alpine` 容器，不连接生产；该环境不验证托管 Redis 主从回退、跨机故障或线上性能收益。

## 第二轮复审修复

- 修正共享预算耗尽后的公平性：pending/owned 游标仅越过实际尝试的候选（owned 中已跳过的失效候选仍可推进），未执行项在后续轮次获得执行机会。仍保持最多 128 个候选、4 个 worker 和 3 秒整轮预算。
- ACK 使用不发送调度通知的内部捕获写入口；owner/lease、capture、耐久证明与精确 revision ACK 均不变。业务写仍在成功或失败时通知，不通过删除 pending 来压制 ACK，避免误删并发业务通知。
- 新增 `TestScheduleOutboxSaturatedWorkersDoNotStarveReadyCell`：四个发布持续阻塞，正常第五项应在后续轮次发布。旧实现运行 9 秒失败。
- 加强 `TestScheduleOutboxCachedOwnedScopeDoesNotReopen`：取消人工删除 ACK 通知，直接验证 ACK 后淘汰不会重新打开数据库。旧实现运行 2.5 秒失败。
- 修复后全部 `TestScheduleOutbox` 回归通过；server/owner race 通过（scheduleoutbox 包无独立测试，相关测试位于 server），make build vet 通过。首次 make test 在已有 TestKVCaptureReplicatesAndRestores 的 TempDir RemoveAll 清理阶段出现 directory not empty，其余包通过；完整重跑 make test 通过，保留首次清理失败记录。本轮未启动 Redis 服务或进行线上验证。
