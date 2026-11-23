# 显式调度模式实施计划

1. 配置与兼容。默认 cluster 保留已有单机与多机行为，shared 要求显式端点和区域标识。节点增减、重启和分区不能改变模式。新增依赖仅用于 Redis 协议连接、认证和 TLS。
2. 到期投影。复用 Queue 和 timer 已有的到期时间，保留原有本机登记和 COS 索引。投影走有界后台队列；网络延迟不得进入权威写入与 ACK 路径。
3. 到期领取。使用两端支持的 ZADD NX、EVAL、ZSET 和 HASH 命令。领取 token 仅用于移除提示，业务交给已有 owner 和 runner。Redis 超时、清库、重启或版本回退由旧发现兜底。
4. 验证。配置测试、过期 token、重复发布和断连；隔离 Redis 与 Kvrocks 实例共用一组回归。上线前以真实双节点和权威数据验证重建、接管、业务延迟与 COS 成本。
5. 节点负载与 owner 路由提示单独评估和灰度，避免在调度模式切换中混入写权或路由证明变化。生产发布与关闭旧扫描分别决策。

## 本轮实现与验收

- 启动时读取 `CELLHIVE_SCHEDULE_MODE`。默认 `cluster`，显式 `shared` 需配置 `CELLHIVE_SCHEDULE_REDIS_URL` 与 `CELLHIVE_SCHEDULE_REGION`。每区先用一个有序集合，沿用现有 runner 派发。Cron 继续按当前 UTC 分钟生成 timer。
- Redis 7.2.17 和 Kvrocks 2.17.0 上验证了领取、可见性超时重领和旧 token 拒绝。两个后端分别运行同一套双 agent Queue 用例，各消费 5 条，死信队列 1 条，停掉一个 agent 后总消费数达到 8。此用例覆盖共享索引与原有发现同时开启时的接管。
- 默认模式配置不可用的 Redis 地址，timer 仍正常触发。共享模式在两个后端分别观察到 timer 提示入索引、到期清除和业务触发，在各自隔离实例中另做清库试验：持久 timer 已进入到期集合、尚未触发时执行 FLUSHDB，确认数据库为空，两端对应 timer 均在原 agent 上留下 fired 记录。停止后端后新建 timer 仍能本地触发。此用例保留了原进程和磁盘。
- 下一步在相同任务量、两档空闲 cell 数（相差十倍）下对照 cluster/shared，分别记录 GET、HEAD、LIST、写请求、传输字节和最老待办迟到，单列共享提示新增的 owner 点查。覆盖首次预约中断且无后续外部请求、双进程冷启动、索引清空、无 peer 降级 ACK 与回退旧版。完成这些验收后再逐项延长对应扫描周期。节点负载、owner 路由缓存和生产发布另行安排。
