# Redis pending-session discovery 验证

最新状态：已提交独立发布分支并部署 SG/USW/GZ，见 [三地区发布记录](2026-11-24-all-regions-redis-recovery-release.md)。以下为发布前本地验收；本地常驻 Compose 未在本次生产发布中更新，原工作分支未提交改动保留。

## 实现边界

- Redis 模式在 COS node-log 创建前登记 pending-session hint，登记失败不能 Open 成功。已有 open session 的重试也不得绕过登记。
- 普通 Recovery 及启动恢复从 Redis 发现候选；活跃 session 的发现不读 COS 正文、不 List COS。开始实际恢复时仍重新读取 COS 正文和当前 lease，恢复数据/状态 CAS 沿用原协议。
- COS sealed 成功后移除索引；删除失败会返回错误，后续权威 Get 可补做删除。COS 缺失不移除候选，避免迟到创建被漏发现。
- 启动对拟使用的 session ID 做精确 COS Get，已封存的旧 ID 即使不在索引中也拒绝复用。
- 诊断全历史 List 保留对象存储语义，Redis 内部索引键不泄露到 Bucket.List。
- 初次离线初始化会补齐索引；现有 Redis 集群需停写执行 `coordination-recovery-init --offline`，绑定升级 v2。新程序拒绝 v1/未完成索引；旧程序拒绝 v2。未改变 owner epoch 或恢复数据。

## 验证记录

使用独立本机 Redis 7.2 容器，AOF always；FS bucket 及真实 HTTP follower。不是生产 COS/Kvrocks 故障注入证明。

- 单元/集成：登记失败阻止 COS 创建；登记后创建失败保留候选；重启 manager 后零 COS GET/LIST 发现；封存后索引删除失败可恢复；open 重试/坐标不匹配；缺 marker、generation 丢失、Redis 不可用拒绝继续；历史迁移与重复执行；损坏记录阻止激活；分页超过 256 条；Lua 部分写失败后的重试修复目录；私有索引键隔离。
- `TestRedisDiscoveryRecoversFollowerWithoutNodeLogList`：真实 Redis+HTTP follower，活跃发现无 node-log COS 读取；失效 session 从 follower 恢复 LTX 并封存、移除索引；启动拒绝复用 sealed ID；全程无 node-log COS List。
- Redis 和 object 两种模式真实双进程 `TestSessionRecoveryProcesses`：地址变化、恢复中断后重启、空替换盘拒绝、原 follower 盘恢复、80 条已确认数据、失权旧 owner SIGCONT 后退出、新 session 恢复均通过。
- nodelog/recovery/coordination 相关 race 通过；make build/vet 通过。全量 `make test` 通过（开启真实 Redis coordination 集成测试）；最后的 Lua 重试补强另经 coordination/recovery race 回归通过。

证据日志在 `/var/tmp/cellhive-recovery-index-*.log`，包括 `process-final.log`、`object-process.log`、`integration.log`、`race-final.log`、`full-test.log`、`build-vet-final.log`。

## 剩余边界

Recovery 轮询周期未变，改为 Redis 候选发现；这不是故障即时恢复承诺。登记后、正文创建前崩溃会留下额外候选，不能用正文暂不存在作为删除依据。历史 sealed COS 正文继续保留。

Epoch GC 尚未修改：当前仍首次 1 分钟、随后每 5 分钟遍历 owned scopes。后续应在 epoch 切换/Fold 完成时持久登记候选，由到期队列触发重试；不能只在切换瞬间执行一次，因为恢复链与一小时上传宽限期可能尚未允许删除。物理删除仍需恢复链、owner/session、退休下界和精确对象集合证明。
