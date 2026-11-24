# 本地 Compose Redis 协调部署

后续更新：本机已按用户要求改为双 agent fleet，见文末。下述单 agent 为首次迁移阶段。

2026-11-24：按用户要求暂停 GZ 发布，仅部署本机常驻 `cellhive-migration-test`。
未提交、未推送代码；SG/USW/GZ 均未切换 Redis 权威。

## 配置和数据

- 基础 Compose 增加可选 Redis；本地 overlay 自动启用 Redis 和 agent 单参数 URL。
- Redis 7.2、独立持久卷、AOF `appendfsync always`、`noeviction`，不映射宿主端口。
- 保留原 MinIO、三个业务卷、root key、8081/8082 端口及 gated do-runtime。
- 恢复正常派发地址 `http://user-runtime:8088`，移除旧测试配置中关闭 Cron/Timer/Queue/waker 的零周期，使用代码默认值。
- 本机有效配置：`deploy/compose/docker-compose.active.local.json`，权限 0600、Git 忽略；不能公开，其中含本机凭据。
- 启停命令见 [Compose 操作说明](../../deploy/compose/README.md)。

## 切换与镜像

停止三个服务及 MinIO 后完整备份原数据，随后恢复 MinIO，执行一次
`cell-agent coordination-init --offline`，再启动新版本。未清卷、未绕过 generation/binding。

本机镜像 `cellhive:local-redis-20261124`：
`sha256:81f718f5ca3826afd9cb65dc9fe8bb0c982cca220a6e7d2ceb6f69236c8b8d8d`。
基于原本机镜像保留兼容 workerd，替换四个 Go 二进制和当前 workerd 集成文件。
工作区源码快照、二进制校验、私有合并配置和验证日志位于
`/var/tmp/cellhive-local-redis-20261124/`。
原数据备份位于 `/opt/backups/cellhive-local-redis-20261124/data-before.tar`，附 SHA-256。
这些是本地构建证据，不是干净发布 commit 或生产镜像。

## 实际验证

- 基础及本地 overlay Compose 校验通过；确认 Redis URL 仅注入 agent，Redis 无宿主端口。
- 切换前写入 20 条 KV；切换后全部读取成功，再写入并读取 20 条新值。
- 真实 user-runtime/workerd：Queue 消费 1 次、显式 retry 后第 2 次成功；Cron 自然分钟执行；DO Alarm 执行成功。
- 提交延迟 Queue/Alarm 后停止业务服务、重启 Redis，再启动业务服务：authority generation 不变，延迟任务完成，KV 再次读写通过。
- 最终 cell-agent、user-runtime、do-runtime、Redis、MinIO 五容器 healthy；三个业务容器自动重启计数均为 0，镜像 ID 一致。

日志：`init.log`、`up-scheduling.log`、`kv-after.log`、`scheduling-enabled.log`、
`live-result.json`、`restart.log`、`restart-result.json`。

## 边界

这是单机、单 agent、真实 Redis 和 MinIO 的本地验收，不是跨机故障域或生产成本对比。
重启过程出现一次 Alarm 派发失败，随后正常重试完成；原本机数据已有
`vwork-1/jobs: control: not found` 的 Queue 配置告警，未删除既有配置掩盖问题。
仍有 epoch fold deferred 等维护告警，不宣称日志零告警。
测试命名空间 `local-redis-20261124` 保留用于观察。
Redis 丢卷、回滚备份、自动副本提升不在本轮验证范围；不能因此切回旧 COS 权威。

## 后续：双 agent fleet

增加 `cell-agent-b` 和独立卷，两个 agent 显式设置 `CELLHIVE_DURABILITY=fleet`，
共享原 Redis/MinIO。各自 peer URL 使用可互访的容器 DNS。原 agent 数据卷保留，
没有重新初始化 Redis authority；镜像保持不变。

旧 KV 读取、新 KV 写入读取通过；实测两节点合计 fleet commit 计数从 142 增至 181。
加入节点启动时出现 1 次无 peer 的 bucket-spool，随后两节点均产生 fleet 确认。
证据：`/var/tmp/cellhive-local-redis-20261124/fleet-result.json`。
这是同机独立卷双 agent，不是跨宿主机高可用证明。
