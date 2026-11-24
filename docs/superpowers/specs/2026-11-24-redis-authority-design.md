# Redis/Kvrocks 协调权威后端

状态：已部署 SG / USW / GZ；版本与验收见三地区发布记录。此文修订此前 shared 仅辅助索引的限制。

## 契约

显式 `CELLHIVE_COORDINATION_BACKEND=redis://host:6379/0`（或 `rediss://...`） 时，owner、owner-gen、nodes、fleet 的 drain/waker 选主记录、wake 索引为 Redis/Kvrocks 权威。默认 object 模式保留原行为。Redis 读写失败不回落到桶。现有 owner/session/epoch、节点租约安全余量、永久 self-fence、接管恢复、退役校验与 RPO=0 ACK 门保持。

采用限定元数据路由的 adapter，底层数据桶仍提供 streaming、分页和原生条件操作。不得把 LTX、node-log、R2 业务元数据等可靠数据迁成可丢弃缓存。所有原有参与者必须使用同一后端；旧二进制禁止与迁移后的集群共存。

Redis Lua 原子实现 compare/create/update/delete；普通 claim 在一次 Lua 内更新代次高水位并发布 owner；独占维护/资源清理使用单次 Lua 预留代次，再 CAS 发布，保留未知结果回收语义，全部走 Redis。revision 使用随机不可复用 token，不在 Lua 中把 uint64 epoch 转为浮点数。记录无 TTL，租约由正文 expiry 判断。按 node/session 维护 owner 索引，包含休眠 cell；epoch 与退役历史不得因租约到期删除。

## 初始化与故障

离线迁移时停止所有 agent/runtime 和后台写者，使用 `cell-agent coordination-init --offline`。在桶保存永久后端绑定，然后复制既有协调记录（保留 epoch、退役正文），最后激活 Redis generation 与桶绑定。中断可重跑；完成后的命令不得覆盖活跃状态。普通启动必须校验绑定及 Redis generation；Redis 空库不能自动重新授权。对象模式遇到绑定也拒绝启动。

不支持透明回滚至旧 Redis 副本/旧备份，不承诺异步主从故障提升安全。部署必须禁止会丢已确认写的自动提升，配置持久化和 noeviction。Redis 状态丢失须保持停写，完成旧 session 隔离和数据链恢复后另行重建；本批不提供自动灾难重建。

## 调度与负载

wake 的持久写改走 Redis，保留先持久状态再发布的顺序及低频对账。共享 ZSET 提示继续以 cell 权威状态核对执行，修复同一提示在领取期间再次产生而被旧 ACK 删除的问题。Cron/Queue/Alarm/清理使用同一 Redis owner 权威；任务业务结果和安全 GC 条件仍保存在 cell/复制协议。调度加速不承诺 exactly-once，也不消除数据恢复、真实清理的桶访问。

节点统计区分总 owner、resident 和 dormant；采集进程 CPU/RSS 与内存余量，标注不可用数据，避免将未采样值当真实零。自动均衡策略不在本次实施范围。

## 验收

真实隔离 Redis 与 Kvrocks：并发 CAS 单胜者、ABA、迟到释放、owner 接管与旧 session 拒绝、权威丢失拒绝服务、迁移中断重试、零桶元数据点查、owner 休眠计数；任务领取重投递/重复通知/旧 ACK；流式与分页数据路径不退化。全量 go test/build/vet 与关键包 race。报告必须区分本地通过、未验收与未部署。

配置收敛修订：Redis URL 同时启用 shared 调度；独立调度开关和 URL/region 环境项移除，namespace 从 bucket 配置派生。旧环境项和无效 URL 报错；已激活旧 namespace 绑定不自动迁移。见 docs/configuration.md 与 decisions.md 2026-11-24 单入口决策。

2026-11-24 recovery discovery 扩展（已部署三地区）：Redis 新版绑定升级为 v2，增加 pending-session 索引；COS 正文仍是恢复权威。现有 v1 集群须停写执行 `coordination-recovery-init --offline`。见 [扩展设计](2026-11-24-redis-recovery-discovery-design.md)。
