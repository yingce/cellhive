# 定时任务、Queue 与 Workflow 回归

2026-11-10，针对本地恢复实例修复后的版本进行隔离测试。使用 `/tmp/cellhive-multiclass-fix/bin`、现有 `/tmp/cellhive-workerd-lifecycle-20261110`；Docker 测试使用 `cellhive:multiclass-fix-20261110`。测试独立端口、临时 FS bucket 和数据目录，不启用现有业务实例的后台调度。本轮不是 COS/OSS provider 复验。

## 已完成

- `queue.sh`：真实消费、R2 metadata、重试耗尽进入 DLQ。
- `queue-concurrency.sh`：100 条消息，4 个并发 batch、每批 10 条；记录数 100，未完成数 0，实测最大同时处理 40 条。
- `queue-multinode.sh`：双 agent，共享对象存储；原 owner 被杀后继续消费，最终 8 条；DLQ 验证通过。
- `workflow.sh`：sleep 唤醒、waitForEvent 超时、前两次失败后第三次成功、R2 metadata。
- `workflow-recovery.sh`：旧 step/finish 回调 409；16 个并发等待消费 16 个不同事件；持久化 consume 后丢失响应并杀掉 runtime/agent，新本地目录从对象存储冷恢复，事件结果 42，其他两组结果保持正确。
- `examples.sh` 选择 cron/do-alarm/queues/workflows：4/4 通过，Cron 为真实分钟触发，DO alarm 触发后可读回 fires。
- `go test -count=1 -p 1` 对 internal/cron、timer、queue、waker、wake 全部通过，使用项目 SQLite build tags，GOMAXPROCS=4。

首次 examples 缺少 esbuild，设置已有 esbuild 可执行路径后重跑通过。首次 Docker Cron harness 缺少构建上下文，改用已有修复镜像并设置 SKIP_BUILD 后执行；未编译 workerd。这两次前置失败不作为功能通过证据。

## Cron 多节点接管

`cf-tail-multinode-e2e.sh` 启用 CRON_FAILOVER，通过。正常运行记录 3 个互不重复的成功时间槽；停掉实际 owner cf-tail-b 后，agent-a 完成下一 UTC 分钟任务。旧节点恢复后核验该槽 attempts=1、successes=1、logical_ids=1。原生 Wrangler 从另一节点读取 Fetch/Queue/Cron/Workflow Tail 也通过。此测试证明本次故障场景没有重复成功，不意味着对任意业务副作用承诺 exactly-once。

证据目录：`/tmp/cellhive-background-e2e-20261110/`，包括各项日志和五项核心 E2E 的 results.json。

## 真实 HK COS 补测

用户要求后切换到 `cellhive-e2e-hk-1376795072`，本机计算 + 真实香港 COS 公网 endpoint，`durability=bucket`；未重编译 workerd。新增 `hk-review.py --suite background`，复用既有隔离/串行锁/清理逻辑，覆盖 Queue 三项、Workflow 两项及四个实际 examples。命名空间前缀 `hk-review-1794317067-e3f86c`，证据 `/tmp/cellhive-background-cos-20261110/`。

注意：共享测试桶仍有以前运行遗留的 Workflow scope，后台日志对这些旧 scope 报 continuation 缺少 predecessor。它们不属于本轮 namespace；本轮不删除或修复它们，也不把当前用例通过表述为整个桶历史数据均可恢复。

COS 已完成：Queue 消费/R2 metadata/重试/DLQ、100 条并发消息（count=100，missing_end=0，max_concurrent=20）、双节点 owner 被杀后恢复消费至 8 条；Workflow sleep/timeout/第三次重试成功/R2 metadata；响应丢失断电后，使用新本地目录从 COS 恢复全部三组结果，事件输出 42。五组均通过且各自 app 清理 HTTP 200。

COS examples 最终 4/4：Cron 29.78s、DO alarm 3.05s、Queue 6.33s、Workflow 4.29s（耗时包括部署和等待，不是请求延迟基准）。第六组 app 清理 HTTP 200。六组真实 COS E2E 均通过。Cron owner 故障接管只在前述 FS/Docker 场景验证，本轮 COS 只覆盖正常 Cron 触发；不要混称 COS Cron failover 已验收。

复跑：设置 `E2E_BIN`、`CELLHIVE_ESBUILD` 后执行 `python3 scripts/e2e/hk-review.py --credentials /opt/.secrets/s3 --suite background --port-base 27100 --workerd /tmp/cellhive-workerd-lifecycle-20261110 --artifacts-dir /tmp/cellhive-background-cos-20261110`。该脚本语法检查与相关文件 diff whitespace 检查通过。
