# Redis recovery authority v3

用户已批准 session 状态和 DO 恢复指针迁 Redis CAS；epoch GC 周期改为一小时。本次保留每节点一个 goroutine 遍历 OwnedScopes 的模型，首次检查仍为一分钟，不实现事件驱动 GC。实现和验证中，未部署。

## 权威边界

Redis 模式将 node-logs/ 正文（含 sealed tombstone、CoveredSeq）及 cells/.../do-recovery.json 纳入 generation-guarded CAS；恢复 hint 仍先登记再创建正文，sealed 正文成功发布后移除 hint。正文没有 TTL，重试不得重新打开 sealed session。DO 指针仍保留 epoch 检查和替换前恢复旧 capture session 的规则。对象模式不变。LTX、静态资源、R2、retired 标记和 fleet/coordination.json 仍在桶中。

## 离线迁移

binding v3 是新启动门禁，旧 v1/v2 程序拒绝 v3；新程序拒绝尚未升级的 v1/v2。停止所有写者后运行 coordination-recovery-init，先将 binding 标为 v3/loading，再关闭 ready marker，复制 COS session 正文及 DO 指针并重建发现索引，最后发布 ready marker 与 active binding。中断后可重试 loading 迁移。已 active 的 v3 禁止从旧 COS 重放（命令只验证 ready），保留旧 COS 文件作为历史。初次 coordination-init 同样导入这些记录。不得在旧进程存活时执行迁移。

Redis 故障不回退 COS。现有托管 Redis 异步提升回退风险同样适用于这些恢复元数据，不宣称故障切换 RPO=0；恢复坐标丢失时必须隔离旧写者并人工恢复。

## 验证

真实隔离 Redis 下验证 session 生命周期零 COS 正文读写、CAS 冲突/ABA 与 generation guard、sealed 不重开、DO 指针 CAS、v2 离线导入/失败重试/重复执行不覆盖 Redis 新状态，以及配置默认一小时。运行 recovery/server/owner 等相关回归和 build/vet。
