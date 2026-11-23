# SG Queue 与 epoch Fold 发布

用户授权将 USW 已验证的版本部署至 SG。本次复用源码 `9dcacebc` 的干净 archive 发布镜像，不重新构建或混入工作区文件。

## 发布

- SG Helm revision **136→137**，经 `cellhive-helm sg preflight/diff/upgrade` 完成；两个 cell-agent 顺序替换。
- digest：`sha256:fd4b214c75d8ebcf34f3dc65e7be05fccb59d922180f92706f4bfb3ac05b51ac`，与 USW revision 133 一致。
- 发布前 SG 实际配置已为 `reclaim / all-readers-validated`；本次不改变 authority 协议、存储目录或桶配置。
- SG values 原有未提交差异仅为已部署的镜像更新，先备份 diff 与文件，再替换目标 digest；其他配置不改。HPA 原运行 3 个 user-runtime，Helm render 显示配置副本数 2，滚动后初期仍为 3 个，04:29:02 UTC 因 All metrics below target 正常缩容至 2 个。
- 由于 SG 源版本较早，本次同时带入此前已在 USW 验证的 epoch Fold、只读 continuation 与重开维护登记修复。

## 验证

- 独立 namespace `queue-fix-sg-20261123`，业务名 q1/q1-dlq，物理 ID queue_1/queue_2。
- 原生 Worker 发送、消费副作用、ACK 清空、失败重试进入 DLQ、源队列清空均通过。
- 真实 COS 列举确认只有 queue_1/queue_2，无业务名错误路径。
- 原生 DO SQL seed=137、increment=138、read=138 通过。
- 初期 7 个服务 Pod 全部 Ready、零重启；HPA 正常缩容后为 6 个，均 Ready、零重启，实际 imageID 均匹配目标 digest。
- 通过端口转发和 Host 路由验证服务内部链路，不代表公网 DNS/TLS 验证。
- 发布后观察 601 秒，11 次定时采样中 9 次通过；第 4、5 分钟因 HPA 删除 port-forward 目标 Pod 而失败，重连后立即复查及后续 5 次定时探针均通过。所有采样中的实际服务 Pod 全部 Ready、零重启，imageID 正确。

## 日志边界

滚动期间控制面连接短暂失败，控制 cell 与尚未驻留 cell 的 Fold 安全延期；不会将这些记录解释为数据已丢失或强行跳过水位保护。

`vwork-105/pageverra-api` Cron 返回 500，日志定位 `tenant.js:67740`；读取当前部署 version 19 的 bundle 后确认是 scheduled 聚合任务失败时主动抛出 `A Pageverra scheduled operation did not complete`。聚合任务为 processPushBatch、pv_summary_cleanup、pv_cleanup_support；日志含升级前已累计重试的任务，但具体失败子任务尚未确认。本次未修改租户代码或任务，不宣称所有业务 Cron 均正常。

本机证据目录 `/var/tmp/cellhive-sg-queue-20261123/`，包括发布前配置备份、预检、diff、rollout、Queue/DO/COS 结果、Pod 快照与日志。完整 Queue 修复设计及已有 spool-only 故障窗口见 [Queue 验证报告](2026-11-23-queue-scope-durability.md)。USW 未再次改动。

HPA 缩容删除了本机 port-forward 绑定的 user-runtime Pod，观察第 4、5 分钟分别记录连接关闭和 connection refused。集群事件明确是正常缩容；重建 port-forward 后立即复查 Queue 副作用、主队列深度 0、DLQ 深度 1 均通过。保留原失败采样，不将本机探针断连描述为服务请求已成功。
