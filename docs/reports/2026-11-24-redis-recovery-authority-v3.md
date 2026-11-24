# Redis recovery authority v3 本地验收

状态：本地实现与验收通过，未提交、未部署；线上仍是 v2。保留原工作区其他未提交改动。

## 改动

- Redis 选路加入 node-logs/ 正文及 cells/.../do-recovery.json；session 状态/封存水位和 DO 指针走现有 Lua CAS、revision 和 generation guard，无 TTL、无 COS 回退。
- binding v3 启动门禁；coordination-init 导入新元数据；coordination-recovery-init 支持 v1/v2 停写升级、loading 中断重试。active v3 再运行不从 COS 重放旧记录，避免覆盖已封存 session 或更新的指针。旧 COS 副本不自动删除。
- epoch GC 默认周期 1h，首次 1m；仍为每个 agent 一个 goroutine 顺序遍历本机 OwnedScopes。保留既有安全条件和宽限期；未改事件驱动。
- LTX/资源数据、R2、retired 标记及低频 fleet binding 仍在桶中。

## 已执行验证

隔离临时 Redis 7.2 容器，测试不连接生产 Redis；对象存储为本机临时 FSBucket，跨进程测试为同机独立 agent 进程，非跨机器容灾证明。

- `make test` 全量 Go 测试通过（设置 CELLHIVE_TEST_COORDINATION_URL，真实 Redis 集成路径运行）。缺外部依赖的其他既有用例仍按各自 skip 条件，不等于全部生产 E2E。
- `make build`、`make vet` 通过。
- coordination/nodelog/recovery race 通过。
- session 生命周期测试禁用 Base，覆盖创建、recovering、SealCovered、sealed 不重开；session/DO 指针 CAS 冲突、ABA、generation 丢失均拒绝。
- v2 离线迁移包含旧 session 和 DO 指针；腐坏记录使迁移失败且拒绝启动，修复后重试通过；active v3 重复迁移不会覆盖 Redis 更新。
- 真实 follower 恢复测试断言 session 元数据零 COS GET/LIST，已确认 LTX 数据实际恢复。
- DO 服务测试覆盖 object/Redis 两种后端，Redis 使用拒绝 COS 恢复元数据 GET/create/CAS 的后端；覆盖释放 capture owner 后恢复、A-B-A 指针与旧 owner 拒绝。
- `scripts/e2e/session-recovery.sh` 分别运行 object 与 Redis 模式并通过：地址变化、中断恢复、空替代盘拒绝、80 条已确认值恢复、两旧 session 封存；SIGSTOP 超 TTL 后接管，SIGCONT 旧 owner 退出，重启新 session 可读已确认值。

首次测试缺少 SQLite tags 被构建守卫拒绝；改用 Makefile 构建参数。首轮全量中旧测试要求 COS 正文点查，与 v3 契约相反；更新为零 COS 正文点查并重跑全量通过。

日志：/var/tmp/cellhive-recovery-v3-{tests,focused,race,build,vet,process-redis,process-object}.log。

## 发布边界

尚未执行线上停写/迁移/镜像发布。上线必须备份并停止旧参与者，执行 v3 离线迁移后启动新版本，不能混跑 v2/v3。托管 Redis 异步提升导致的状态回退风险同样影响恢复元数据；本次不宣称该故障模式 RPO=0。
