# Redis authority implementation plan

Goal: 可选 Redis/Kvrocks 权威协调，移走日常 owner/lease/wake 桶请求。
Architecture: internal/coordination 通过白名单路由元数据，保留原数据桶能力；离线初始化先绑定后迁移，再激活；现有安全协议不变。
Tech Stack: Go、现有 go-redis、Lua、隔离容器。

- [x] internal/coordination：实现 Lua CAS 与 generation 门、原子目录及 owner/session 集合；测试并发、ABA、丢失 gate。
- [x] internal/coordination：实现 Bucket 路由与可选流式/分页能力转发；只读基础桶元数据迁移、持久绑定与启动拒绝。
- [x] internal/config、cmd/cell-agent：配置与离线命令接入；默认模式不创建 Redis 客户端，Redis 模式失败不降级。
- [x] internal/scheduleindex：重复投递版本化，旧领取 ACK 不清除新通知；真实后端验证。
- [x] internal/lease、cmd/cell-agent：节点总 owner/resident/dormant 与实际进程负载采样。
- [x] 对 owner/lease/wake 跑真实 Redis/Kvrocks 集成及 race，全量 test/build/vet；同步 decisions/known-issues/报告。

执行在 feat/redis-coordination；不提交、不推送、不部署。直接在当前会话执行，不创建子代理。
