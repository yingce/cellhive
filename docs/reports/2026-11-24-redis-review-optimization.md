# Redis review 后续优化（2026-11-24）

范围：shared schedule hint 重试/消费、自愈和 coordination ListPage 元数据读取。本地实现，未提交、未部署。保留工作区其他 telemetry/cron observability 改动；本报告不为它们提供单独验收。

## 行为

- hint 失败次数按 member 存在新增 failures hash，1/2/4/8/16/32/60 秒退避并封顶，不按次数丢弃。新发布重置失败次数；claim 内并发发布的 deadline 优先于旧失败重试。
- claim 返回真实 member/payload。非法 JSON、缺失 payload、payload 身份与 member 不一致时，按 token/revision ACK 清理；新发布不被旧 ACK 删除。
- handler 失败后继续其他候选；每轮最多 32 项且共享 3 秒 handler 预算。Redis claim/收尾仍受客户端超时限制；这不是整个 consume 的严格 3 秒上限。预算耗尽保留未处理项。
- ListPage 每批最多 128 个键，Lua 返回 HSTRLEN/revision，避免正文传输；generation guard、缺失记录跳过、分页边界与混合后端游标保持原语义。

## 验证

隔离 Docker Redis 7.2 与 Kvrocks，各自仅绑定本机独立端口，不访问既有 Redis 或业务数据。新增回归覆盖退避封顶/重置、并发新通知、旧 token、损坏与缺失 payload、错误身份、空 member、失败后继续消费、共享 handler 预算、元数据大小/ETag/缺失/generation guard。

分页用相同 257 条测试记录验证：旧源码为一次 list + 每键一次 Get，共 258 次 EVAL；新实现通过客户端 hook 断言 4 次 EVAL（一次 list + 三批元数据）。这是调用次数验证，不是线上延迟、吞吐或成本压测。

验证命令遵循 Makefile SQLite feature tags 与 SQLITE_ENABLE_FILESTAT。首次直接 go test 因遗漏 tags 被编译门禁拒绝，已纠正执行参数。最终 make test、make build、make vet、git diff --check 均通过；Redis/Kvrocks 两包定向回归通过，Redis 两包 race 通过。全量测试运行期间追加的共享预算测试又在 Redis race 与 Kvrocks 定向检查中通过。

## 边界

不改变 owner/epoch/lease、业务 ACK 或托管 Redis 主从回退限制。未改 outbox 排序锁、启动重试、epoch=0 清理或目录合并策略。新增 failures 为辅助状态，无需迁移权威数据；新旧程序混跑时旧消费者仍使用旧重试策略，且旧 ACK 不清理新增失败计数，完整新策略与清理行为以参与者升级完成为前提。退避状态不构成耐久业务数据。
