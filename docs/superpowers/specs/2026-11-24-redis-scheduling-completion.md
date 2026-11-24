# Redis 调度补齐（本地实施，未部署）

沿用已批准的 Redis 权威后端，补齐 Queue/Cron/Alarm/清理的调度与验收。

1. timer/timer_retry/messages 的 INSERT/UPDATE/DELETE 在同一 SQLite 事务更新 schedule_outbox revision；迁移已有数据也产生 pending。发送成功前不得清除 pending；按 revision 条件确认，旧确认不覆盖新变更。outbox 与业务数据一起走 capture，不建立第二套消息权威。
2. 后台 relay 在当前 owner、资源 gate、节点租约和耐久证明下读取 pending、同步发布 Redis hint，再通过 capture 确认。启动检查本机已有 scope，驻留与休眠淘汰候选均可重试；保留既有全域恢复发现。Redis 失败不丢 pending、不回落 COS 授权。低频重投 clean deadline 可重建丢失的辅助 ZSET；Redis 权威数据丢失仍按既有停写契约处理。
3. 共享调度按 kind/scope 去重，Lua 保存当前 payload/revision 和 claim token。领取期间新通知不可被旧 ACK 清除；陈旧提示只能触发当前持久状态核对。timer 改期/取消以 SQLite 为准，不通过不带版本的共享删除授权取消业务任务。
4. Cron 投影下一计划 deadline，并接收共享 nudge；本地计划解析、revision 检查和每分钟配置对账保留。控制面清理/purge 投影到期 deadline，接收者只唤醒原循环，执行仍校验持久任务租约和 GC 证明。
5. 真实 workerd + user-runtime + do-runtime + cell-agent + Redis/Kvrocks 验证 Queue 消费/重试、自然分钟 Cron、Alarm 改期/取消/失败重投。另测 relay 失败重试、旧 ACK、休眠/重启与所有权约束。保持现有完整回归。

不承诺外部副作用 exactly-once，不取消低频全域恢复；自动均衡及 Redis 权威灾难重建仍不在本批范围。禁止生产部署、提交或推送。
