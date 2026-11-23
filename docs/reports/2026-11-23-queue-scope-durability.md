# Queue 资源寻址与 schema 持久化修复

生产资源 `jobs` 登记为 `vwork-1/__queue__/queue_1`，原消费者却直接访问 `jobs`。Queue 重开又执行无效 schema 事务、增加未捕获 TXID，使后台 continuation 因本地水位超前而安全拒绝。

## 修改

- 消费者和 DLQ 从资源登记解析物理 scope，owner、claim、ACK、retry、删除代次采用相同 ID；handler 的 `batch.queue` 保留业务名。无效登记仅阻止对应消费者，不猜测路径。
- 生产者沿用 binding 授权已经解析的 scope，贯穿 admission、owner 和数据操作；转发保留原 URL，目标节点重新鉴权。管理端支持登记业务名或物理 ID，歧义拒绝。
- schema 完整时只读检查，不增加 TXID；实际初始化/升级经过 capture 和持久化确认，同 cell 串行，确认失败可重试。
- 不删除、迁移或重写旧错误路径的数据，不放宽 continuation 的水位校验。

## 验证

2026-11-23 本地证据目录 `/var/tmp/cellhive-queue-fix-20261123/`：

- 修复前重开 TXID 3→4，名称解析得到错误业务名；修复后回归通过。
- `make test`、`make build`、`make vet` 通过；Queue/server/cell-agent 相关 race 测试通过。
- 并发 schema 升级仅增加一个 TXID；从 LTX 恢复后 schema 索引存在且 SQLite integrity_check 通过。
- 原生 Worker E2E：业务名 q1/q1-dlq，物理 ID queue_1/queue_2；发送、消费副作用、ACK、重试、DLQ 通过，未创建业务名数据库。
- 双节点 E2E：启动 spool 上传后强杀首节点，第二节点接管，消费计数 5→8。
- 本地容器镜像 `cellhive:local-20261123-queue` 已部署；独立 namespace `queue-fix-local-20261123` 的真实收发、ACK 清空、重试进入 DLQ 通过。
- USW 旧 `jobs.db` 两个副本复制到本机后只读检查：messages=0、TXID=2；这只是副本检查，不代表所有历史桶对象均已审计。线上未删除、迁移或修改 TXID。

## 已有故障边界

双节点测试若在启动 spool 上传前强杀首节点，会因控制 cell 缺少 TXID 3 而出现 `have 2 next 4`。上一版发布二进制也复现：启动时未发现 follower，已有 bucket-spool 策略只确认本地 fsync；其他节点不能读取这个尚未上传的副本。测试明确等待启动 spool 完成后再验证 Queue 接管，与已有 session 进程测试前提一致。本批未改 spool durability 策略，因此不能把本次通过描述为任意时间强杀/磁盘丢失均满足跨节点恢复。

## 发布状态

- 本地已部署并验证；本地原来关闭的 queue runner 已启用为 1s，dispatch 指向 user-runtime 内部端口。重启后只读请求保持主队列深度 0、DLQ 深度 1。后台为主队列 e3 与 DLQ e2 建立 continuation 并完成 Fold。
- 源码 `9dcacebc` 已推送，镜像从干净提交 archive 构建。
- USW 已经 wrapper 升级至 Helm revision **133**（原 132），preflight/diff 通过，diff 仅镜像更新。
- 镜像 digest：`sha256:fd4b214c75d8ebcf34f3dc65e7be05fccb59d922180f92706f4bfb3ac05b51ac`。
- 六个服务 Pod 实际 imageID 一致，全部 Ready、零重启；workerd 使用上一版相同二进制。
- 独立 namespace `queue-fix-usw-20261123` 原生 Worker 收发、ACK、重试、DLQ 通过；真实 COS 只存在 `queue_1`、`queue_2`，无 q1/q1-dlq 错误路径。
- 原业务 jobs/jobs-dlq 只读 stats 解析至 queue_1/queue_1-dlq，深度均为 0；此前测试的 20 个 KV 值与原生 DO 计数 133 保留。
- 使用 Host 请求到 user-runtime 的服务端口验证；不等同于公网 DNS/TLS 验收。
- SG 未部署，运维仓 SG 原有改动保留。发布后连续观察 604 秒，11 次 Queue 读探针全部通过，六个服务 Pod 全程 Ready、零重启、imageID 匹配。

观察日志中未见服务 ERROR。滚动替换时两次 Queue 控制面读取短暂失败，随后恢复；另有 4 次 COS 租约发布超时重试，剩余租约约 4.8 秒以上，未导致 session 切换或重启。一次 Fold 延期来自控制 cell 的本地水位领先，不是旧 jobs；未持续重复。测试预期的一次死信日志和非 owner purge 保留记录不作为异常消费处理。
