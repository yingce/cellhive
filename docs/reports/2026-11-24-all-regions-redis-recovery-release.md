# SG / USW / GZ Redis coordination 与 recovery discovery 发布

状态：三地区已部署并完成业务与稳定性验收。

源码 `5ba2cee39f6e76f2bb7e9f4556a2b44cf4e70f79`，独立发布分支
`release/redis-recovery-all-20261124` 已推送。使用干净 commit archive 构建 agent-only 镜像，
保留既有 runtime/workerd，原工作分支及未提交改动保留。

镜像 tag：`20261124-redis-recovery-5ba2cee3`。
最终 digest：`sha256:deb75d2a88d052d21b619c93beed5e76ce92b6def2dde98117979f52fab89cdb`。

## 范围与切换

| 区域 | 最终 Helm revision | 切换 | 备份物理对象 | 停写至最后业务 Pod Ready |
| --- | ---: | --- | ---: | ---: |
| GZ | 9 | Redis v1 → v2，补齐恢复索引 | 112，另有 Redis RDB | 151 秒 |
| SG | 150 | COS → 独立持久 Redis v2 | 1,208 | 190 秒 |
| USW | 142 | COS → 独立持久 Redis v2 | 710 | 224 秒 |

时间由停写命令至最后一个业务 Pod 的 Ready 条件时间计算，不是连续外部 HTTP 可用性探测。
逐区停 user-runtime → DO runtime → agent，确认全部旧写者退出，备份协调与 node-log
（包括 append authority 的物理分段），离线初始化成功后恢复双 agent fleet、两个 DO runtime、
两个 user-runtime 及 HPA。使用运维 Helm wrapper，未绕过 context/preflight 校验。

各区域各自一个 Redis StatefulSet 和独立 20 GiB CBS PVC；AOF yes、appendfsync always、
noeviction，配置及 AOF write status 已读回。没有跨区 Redis、没有自动副本提升，也未采用此前 GZ 托管 Redis。
业务 SQLite/LTX 和 node-log 正文仍在 COS；Redis 承接协调权威、shared 到期索引和 pending-session 发现。
新绑定 v2 拒绝旧程序重新启动；不支持回退 COS 或滚动混用旧写者。

## 验证

每区迁移前写入 20 条 KV；切换后读回旧数据、新写 20 条并读回，真实 workerd Queue/重试、
Cron 和 Alarm 三地区均通过。运行镜像 digest、fleet 参数、Redis Secret 引用、索引 ready marker 和实际 fleet
提交计数逐区验证。三地区各 601–603 秒、每分钟一次共 11 次观察全部通过：七个业务/Redis 容器 Ready，
重启计数不增加，KV 探针成功，Cron 时间推进。

本地发布前全量 Go test、build/vet、相关 race、Redis/object 双进程故障恢复测试通过；
没有在生产执行故障注入。本次不修改 epoch GC 的周期或删除条件。

## 已知边界

- GZ 两个 agent 启动初次连接 Redis 被拒绝，各自动重启一次；SG/USW 初始启动均零重启。保留计数，不用删除 Pod 隐藏。
- DO follower recovery coordinate 竞争时仍可等待 COS 耐久证明，不宣称每次提交都由 fleet follower 确认。
- SG `vwork-105/__cron__/pageverra-api`、`vwork-96/__cron__/diagnostic-log-upload` 的 timer 失败在切换前已经存在；未修改租户代码。
- GZ 原有 Vector 遥测出口超时仍存在。发布结果不等于日志零告警。
- 单实例 Redis 不提供自动跨 AZ 高可用；节点失联要先隔离旧进程，不可强删 Pod/强挂卷形成两个写者。

证据：`/var/tmp/cellhive-all-redis-20261124/` 各区域目录下的迁移日志、业务结果、
`observations-final.json`、`finalcheck.json`、`before.json`、`after-long.json`。
三地区最终 Helm diff 仅 NetworkPolicy generation 元数据不同，无 spec 漂移；Chart 同步检查通过。
`private/` 内含 Secret/协调快照，禁止提交或公开。迁移 Job 已清理；测试 namespace 保留用于复验。

## COS 操作采样

双 agent 合计，单位次/秒；发布前 120 秒、发布后 360 秒，后者覆盖 GC 周期。

| 区域 | GET 前 | GET 后 | LIST 前 | LIST 后 | 发布后 LIST 来源 |
| --- | ---: | ---: | ---: | ---: | --- |
| GZ | 0.233 | 0.025 | 0.374 | 0.136 | epoch_gc |
| SG | 16.974 | 0.139 | 1.405 | 0.750 | epoch_gc |
| USW | 7.996 | 0.030 | 0.232 | 0.161 | epoch_gc |

三地区后窗口均无 Recovery/node-log GET 或 LIST，亦无 shared_schedule 来源读取。
GZ/USW 剩余 GET 来自 epoch_gc，SG 另有少量 assets 读取。两个短窗口（GZ/SG）曾恰好零 GET/LIST，
不能据此宣称持续零访问。工作负载未固定，包含 canary Cron 和在线业务；这些是运行指标，不能直接作为账单降幅保证。

验收快照中的实际 fleet 提交计数（两个 agent）分别为 GZ 221/85、SG 738/599、USW 122/139，
各区 pending-session 索引均为两个当前 session。发布后观察中没有新增重启；保留 GZ 启动各一次重启事实。
SG/USW 初始化时短暂的 control owner/连接检查告警，以及测试部署触发的 control version changed 提示，
后续未持续；没有修改既有权限/耐久检查来消除告警。
