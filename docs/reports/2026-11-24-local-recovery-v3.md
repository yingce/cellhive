# 本机 Redis recovery authority v3 部署

2026-11-24 本机 cellhive-migration-test 已部署当前未提交源码快照；未提交、未推送、未修改云上区域。源快照及哈希位于 /var/tmp/cellhive-local-v3-20261124/，四个 Go 二进制在固定快照内由 make build 构建；现有 workerd fork 二进制 SHA-256 前后相同。

镜像 cellhive:local-recovery-v3-20261124，ID sha256:ebaa5c7db50c04e3b2d88b66ad274b22682043eea8808e8dfb068e3053531f07。两个 agent、user-runtime、do-runtime 实际镜像与二进制校验均匹配。

## 迁移

停止四个业务容器及 Redis/MinIO，对所有既有数据卷和 MinIO bind 目录离线备份。备份 /opt/backups/cellhive-local-v3-20261124/data-before.tar 及 SHA-256，目录私有。保留原 root key、端口、卷名、双 agent fleet、Redis AOF always/noeviction。有效配置仍为 deploy/compose/docker-compose.active.local.json（私有且 Git 忽略），只更换四服务镜像及两个 agent 的 GC interval=1h、initial=1m。

先启动 Redis/MinIO，运行新镜像 coordination-recovery-init --offline，读回 binding 最新帧为 v3/active，Redis generation 相符。启动后读取 Redis 有 4 个 session 正文与 15 个 DO recovery pointer（包含新会话）。六容器全部 healthy、自动重启数均为 0。

## 验证

- 迁移前验证 20 条历史 KV 并写入迁移 canary；迁移后全部保留。
- 迁移后 20 条新 KV 写读成功；真实 fleet commit 计数增长。
- 真实 Worker Queue 首次消费、失败重试第 2 次成功、DO Alarm、新 Cron tick 均通过。
- 审计 COS/MinIO 历史 node-log/DO pointer 共 15 个对象，新 agent 启动后零更新；新恢复元数据走 Redis。
- 四业务容器内二进制 SHA-256 与快照构建一致；workerd fork 未更换。

启动阶段 runtime 尚未就绪导致 Cron DNS/dial 失败，已走重试且新 Cron tick 验证通过。既有 queue vwork-1/jobs: control: not found 告警仍存在，未清理无关历史配置。该部署为同机双 agent、真实 Redis/MinIO，不代表跨机器容灾测试。

证据目录内包括 source.tar.gz、source.sha256、binaries.sha256、migrate.log、migration-verified.json、health.json、verification.json、cos-metadata-audit.json。私有 compose-before.json 含凭据，禁止公开。旧备份是离线灾备快照；v3 已有新写入，不能直接切回旧镜像或回滚 Redis。
