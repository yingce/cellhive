# GZ Redis 协调与 fleet 发布

2026-11-24，仅 GZ。最终版本为下述 revision 7；初次发布源码 `e7577f8c3b5834bd6c73ec962c7d5204cd169f60`，
独立分支 `release/gz-redis-fleet-20261124` 已推送；原工作分支与未提交改动保留。
从干净 commit archive 构建 agent-only 镜像，保留线上 runtime/workerd。

镜像：`sgccr.ccs.tencentyun.com/vwork/cellhive:20261124-gz-redis-e7577f8c`。
固定 digest：`sha256:9ac1e0f9a9e04fc8afbf885dbace831afa653d80f98dcb72bb6b355783f44727`。

## 实际切换

- Revision 4：增加独立 Redis StatefulSet、20 GiB CBS PVC、agent/迁移任务专用网络访问规则和 Secret。
- Revision 5：关闭 HPA，按 user-runtime → do-runtime → agent 顺序停写；全部旧 Pod 退出后备份 70 条协调对象，确认桶尚无 Redis binding。
- 离线初始化将 owner/owner-gen、node lease、fleet 选主和 wake 记录迁入 Redis；初始化成功后才恢复业务。
- Revision 6：恢复两 agent、两 gated DO runtime、两 user-runtime 和 HPA，agent 显式设置 fleet。
- 停写开始至全部业务 Pod Ready 为 228 秒（以 Pod 条件时间计算，不是连续外部可用性探测）。

Redis 为单实例、固定镜像 digest、AOF `appendfsync always`、`noeviction`，没有自动副本提升。
业务 SQLite/LTX、恢复日志继续使用 COS。未使用此前提供的托管 Redis，也未删除该托管实例。
SG/USW 未切换。本机双 agent fleet 保持运行。

## 已完成验收

- 切换前 20 条 KV 全部可读；切换后新增 20 条 KV 写入和读取通过。
- 真实 workerd：Queue 消费与显式重试（1/2 attempts）、Cron 自然分钟执行、DO Alarm 均通过。
- 两个 agent 实际出现 `cellhive_commit_total{mode="fleet"}`，不是仅配置了 fleet。
- Redis 中可读取两节点 CPU、RSS、内存上限、resident/dormant/owned cell 数量。
- agent 运行镜像 digest、协调 Secret 引用、fleet 参数与 HPA 恢复均已读回确认。
- 初次发布及 revision 7 修复后各十分钟持续观察通过；最终 11 次采样中 7 个业务/Redis 容器持续 Ready，重启计数未增加，KV 探针持续成功，Cron 时间持续推进。

证据目录：`/var/tmp/cellhive-gz-redis-release-20261124/`，包括
`init-success.log`、`live-before.log`、`live-after.log`、`live-result.json`、
`observations.json`、`node-loads.json`、`cutover-duration.json`、`gz/before.json` 和 `gz/after.json`。
Secret 与协调快照在该目录的 `private/`，不能提交或公开。

## 限制与观察

迁移 Job 首次启动连接 Redis 被拒绝；加入带认证的连接就绪检查后成功。
最终版本两个 agent 首次启动也遇到 Redis connection refused，各自动重启一次后恢复；保留该重启计数，不以删除 Pod 隐藏。
启动尚无 peer 时出现一次 spool 确认；DO 恢复指针 CAS contention 时按原协议等待 bucket 证明。
不宣称每次写入都获得 follower 确认。
非控制 owner 的 agent 每分钟记录 `purge: retained / caller is not the owner`；
代码在读取/执行 purge 前验证控制权并退出，本次保留该安全拒绝日志，未调整日志级别。

Vector ARMS 出口 DNS 失败在发布前已存在（最早检索到 04:02 UTC），仍导致遥测超时/丢弃；
未在本次发布中修改其出口。业务验证不依赖该遥测链路，也不宣称日志零告警。

本次没有生产故障注入。Redis 节点失联须先隔离旧进程，不能强删 Pod/强挂卷形成两个写者。
丢卷/回退备份需停写恢复，不允许回退 COS 或启动不知道 binding 的旧 agent。
单实例协调存储会限制可用性，不等于跨 AZ 自动高可用。

## Revision 7：修复发布验收发现的 outbox 读放大

首轮采样中，协调 GET 已迁至 Redis，但 outbox relay 把 native DO 的 owned scope
当作 agent 本地 SQLite，每秒尝试 hydration，导致新的 LTX GET/List。
旧版回归测试能稳定复现；修复只检查已有本地文件，保留 evicted 本地 outbox 重放，
不省略 owner/fencing/耐久证明。补充 shared_schedule 来源归因。

修复源码：`d5a0d38f5e7092c3ff111914ed3ce3663bfad1cb`，已推送同一发布分支。
最终镜像：`sgccr.ccs.tencentyun.com/vwork/cellhive:20261124-gz-redis-d5a0d38f`，
digest `sha256:125ce13ae9864a74c59b9d35c663582088c75fd30a6ba1a87208eff85ff47bb0`。
仅顺序滚动两个 agent，不重新迁移权威、不修改 runtime 镜像。

验证：outbox 专项及 race、server/queue/timer/cell-agent 相关包全部通过。
旧版本新增回归测试按预期失败。生产重新发消息与重试、Alarm、新一轮 Cron 和迁移前 KV 读取均已通过。
最终十分钟稳定性观察通过（`observations-final.json`），业务复验见 `fix-live-result.json`。两个 agent 首次连接 Redis 各重启一次，
本报告保留该事实，不能把最终 Ready 解释为启动从未出错。

## COS 请求量采样

双 agent 合计，各窗口 180 秒，单位次/秒：

| 窗口 | GET | LIST |
| --- | ---: | ---: |
| 切换前 | 5.215 | 0.161 |
| revision 6（发现 outbox 缺陷） | 6.164 | 16.071 |
| revision 7（含业务验收） | 0.399 | 0.388 |
| revision 7（稳定观察） | 0.215 | 0.242 |

稳定观察 GET 比切换前低约 95.9%；GET+LIST 从 5.376 降至 0.457 次/秒，约低 91.5%。
LIST 比原窗口略高，不能宣称所有扫描均已消除。稳定窗口 GET 来源为 recovery/node_log/list
0.187 和 epoch_gc/ltx/direct 0.028 次/秒；owner/lease/control_owner 协调读取未再出现。
这里是运行指标的窗口比较，业务流量未固定，包含保留 canary 的 Cron 与探针，
不是相同负载基准或实际账单降幅保证。证据：`gz/fixed.json`、`gz/fixed-observation.json`。

最终 Chart 同步检查通过；部署 diff 仅 NetworkPolicy generation 元数据变化，无 spec 差异。
两个 agent 的实际 imageID 与最终 digest 一致；Redis AOF 写入状态 ok、无 delayed fsync。
迁移 Job 与临时探测 Pod 已清理，canary namespace 保留。
