# Redis 调度与 invocation 日志优化三区发布（2026-11-24）

源码 `8fa81f58`，本地部署记录 `2b8b5c3d`，main 已推送。SG、USW 常规升级；GZ 经停写备份和 recovery authority v3 离线迁移后升级。发布源为 `/opt/cellhive-helm`，使用区域 wrapper；同时更新运行镜像与实际 Vector ConfigMap。

## 版本与产物

- SG：Helm 153 → 154。
- USW：Helm 145 → 146。
- GZ：Helm 11 → 12（维护）→ 13（恢复业务）。
- SG/USW 镜像：`20261124-redis-logging-8fa81f58`，digest `sha256:a260a46b307ed55d14f98ad44487494acb94e26efe0c51318f658196ac6f17af`。
- GZ 镜像：`20261124-redis-logging-gz-8fa81f58`，digest `sha256:3fe0ff6b3c911884ae945fee92e82a8c590c7f694c8e51425408596fd3e2879b`。

GZ 原镜像预拉出现仓库连接重置与 COS TLS handshake timeout，停写前已解决：改用 GZ 已有生产基础镜像打包相同四个 Go 二进制与同一提交 JS，减少历史层，两个节点完成预拉后才迁移。两种镜像的业务二进制、JS 和 workerd 内容相同；每区已回读 agent/user-runtime/do-runtime 二进制与 JS SHA-256，并核实所有业务 Pod 的实际 imageID。workerd fork SHA-256 保持 `afd36efa583587fd8fe1574507006f4bf3324693f82ce1398a33920d5a1b3a5f`。

Helm chart 同步移除已废弃的 CronMaxAttempts 配置；本次发布包含主分支已验证的 Cron 重试预算与 occurrence cutoff 修订。未改变托管 Redis 异步主从回退的风险边界。

## GZ 离线迁移

先停 HPA 和全部 runtime/agent 写者，确认旧写者退出，再备份 9 个 Redis 顶层键与 125 个 COS 协调/恢复元数据物理对象；私有备份包含值、类型、TTL 和 SHA-256。顶层键数量不是 cell 数量。

新镜像执行 `coordination-recovery-init --offline` 成功；generation 保持不变，ready marker 有效，15 个 session 正文进入 Redis，迁移时 DO pointer 为 0。恢复原副本数与 HPA。从停止命令到所有业务副本 Ready 为 73 秒，不是连续外部 HTTP 不可用时长测量。迁移后核对 30 个旧 COS node-log/DO recovery 物理对象，无新增、变化或删除。v3 已接受新写，不能直接回退旧 v2 镜像或 Redis 快照。

## 实际业务和日志验收

每区均通过：20 条历史 KV、5 条新 KV 写读、Queue 首次消费、显式重试后第二次成功、DO Alarm、新 Cron tick。稳定后各追加一次 KV 写读，双 agent 合计 fleet commit 计数增长。HTTP 探针经 Kubernetes port-forward 访问真实 user-runtime，不包含公网入口 TLS 验收。使用既有独立 rollout 测试 namespace，新增键与 DO ID 带本次提交前缀；没有调用业务修复接口。

每区独立 `review-logs` 测试 worker 分别抛异常和返回 HTTP 503。SLS `vwork/request` 中实际收到六条对应记录：`handler` 与 `handler_response` 各三区一条，`error_origin=unknown`，错误摘要中的探针凭据和 URL 已脱敏。已核对每区实际 Vector ConfigMap 与提交一致，使用 Vector 0.58.0-alpine。此证据覆盖 runtime → Vector → SLS，不只是 fixture 测试。

三区各完成 600 秒以上、11 次检查，共 33 次：每区 6 个业务 Pod 与 Vector 全程 Ready，自动重启为 0；真实 Worker 探针每次 HTTP 200，Cron 时间持续推进。最后 agent 日志抽查无 ERROR；仍有下述 WARN，不把零 ERROR 当成所有业务成功。Vector 未出现日志导出错误，GZ 仅记录启动时 metrics 源无消费者的配置警告。

## 告警与结论边界

SG 的 `pageverra-api`、`diagnostic-log-upload` Cron 失败仍存在。发布前同窗口样本已存在 `event_type=cron/outcome=failed/status=0`，并伴随 `scheduled/ok/status=0` 包装层记录；新版本将失败正确归一为 500。不能直接把 500 数量增加解释为新增业务故障，也不能把这些 handler 失败直接归责用户。

滚动／恢复启动阶段出现 owner unowned、短时无可用 peer 的 spool 警告；非 owner purge retained 与部分 epoch fold deferred 亦有记录。本次未修复历史 Cron 或 fold 问题，不声称零告警、冷恢复延迟已修复或业务吞吐提升。未测试托管 Redis 主从回退故障模型。

GZ 私有迁移备份另已持久保存至 `/opt/backups/cellhive-gz-review-8fa81f58/`（目录 0700、文件 0600），附 SHA-256 清单。

发布证据在 `/tmp/cellhive-production-8fa81f58/`：各区 business、observations、artifact-proof、SLS 校验、GZ backup/redis-verified/ready/cos-recovery-audit。目录含私有备份，不得公开整个目录。备份 relay、镜像预拉 Pod 与已完成迁移 Job 已清理；保留专用 rollout 测试资源与证据供复查。
