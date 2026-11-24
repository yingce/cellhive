# 2026-11-24 三地区托管 Redis 切换验收

GZ、SG、USW 已从集群内 Redis 切换到各自区域同 VPC 的腾讯云托管 Redis 7.0 标准主从实例。业务代码与镜像未变，agent 继续使用 fleet durability；owner/lease/wake、recovery index 和 shared scheduler 索引均在云 Redis。

## 计费与实例

SG / USW 复用刚创建的按量实例，已转为包月 1 个月，未重复购买。已核对已完成包月订单，实付分别为 ¥36.63 / 月、¥40.70 / 月，合计 ¥77.33 / 月；两台均 1 GiB，自动续费已开启（2026-11-24 按用户要求设置并回读确认），到期日 2026-12-24。初始按量购买订单显示 SG ¥0.10、USW ¥0.11，已转包月，无持续按量实例。包月订单状态均为 4（交易成功），RealTotalCost 与报价一致。GZ 复用原有包月实例。

单可用区创建被云 API 拒绝（需白名单），实际为各区域内跨可用区的一主一副本；无公网地址，安全组仅允许集群子网 TCP 6379，主节点读写、noeviction。

| 区域 | 云实例 | Helm revision | 停写到全副本 Ready | 持续观察 | 验收 |
|---|---|---:|---:|---|---|
| GZ | `crs-9rvf2tb9` | 10 | 81s | 601s / 11 samples | PASS |
| SG | `crs-mzcbijjd` | 151 | 72s | 601s / 11 samples | PASS |
| USW | `crs-g2ufqrh2` | 143 | 94s | 603s / 11 samples | PASS |

停写到 Ready 从删除 HPA/开始缩容计算，非连续 HTTP 探针测得的精确不可用时间。

## 迁移及验证

- 三地区顺序停 HPA、user-runtime、DO runtime、全部 agent；确认没有业务写者后再导出。
- 每地区完整复制 DB0 全部 9 个 Redis 键：6 个协调键和 3 个 shared scheduler 键，包含 string/hash/zset。三个源实例都没有其他非空数据库；此次实际键均无 Redis TTL。导入前云库为空，源/目标类型、二进制内容、分数、过期时间逐项相等，快照摘要存证。generation/revision/epoch 原样保留，未重新初始化 bucket binding。
- 每地区验证迁移前 20 个 KV 值、新写读 20 个 KV 值、真实 Worker Queue 首次处理/失败重试、Cron、原生 DO Alarm。另检查上一轮已有 canary 的 20 个 KV、Queue/重试状态、Alarm 状态，并确认旧 Cron 的 scheduledTime 继续推进。
- 每地区 11 次、跨度至少 600 秒的观测：6 个业务 Pod Ready、重启计数不增加、KV 可读、Queue 状态正确、Cron 时间推进。实际 agent 镜像与 fleet env 核对通过；generation 未变、recovery index version=1，云 Redis 无 eviction。
- 集群内 Redis StatefulSet/Service 已移除，原 20 GiB PVC 保留；离线备份与原 Secret/values 存在受限目录。临时迁移 relay Pod 与本次临时 canary 应用已在验收后清理，未删除此前已有的 canary。
- 启动窗口存在短暂 owner unowned / peer connection refused 和 fleet peer 尚未就绪告警；SG 既有 vwork-105 / vwork-96 定时任务失败仍存在。不得将它们描述为此次迁移修复或全平台零错误。

## 发布与安全边界

agent 镜像：`sgccr.ccs.tencentyun.com/vwork/cellhive@sha256:deb75d2a88d052d21b619c93beed5e76ce92b6def2dde98117979f52fab89cdb`，代码发布提交 `5ba2cee39f6e76f2bb7e9f4556a2b44cf4e70f79`。本次未构建/部署工作区未提交代码。操作通过区域 cellhive-helm wrapper，未变更业务镜像。

托管 Redis 异步复制与自动提升可能丢失已确认的协调写，当前 generation guard 不能检测同 generation 回退；Lua CAS 不解决这一点。用户选择该托管部署取舍，故不能宣称跨托管故障切换的协调 RPO=0 或 fencing 安全已证明。本次未进行主从强制切换/灾难恢复测试。异常须停写并隔离旧 session 后人工恢复，不自动回退 COS。

旧 PVC 仅作离线保留，目标已接受新业务写后不能直接恢复旧 Redis/旧 Secret 或执行 Helm rollback；必须再次停写、迁移最新权威数据并校验。Redis 故障不回退 COS；epoch GC 周期未在本次更改。

操作源配置：`/opt/cellhive-helm/environments/{gz,sg,usw}/values.yaml`，三处 `coordinationRedis.enabled=false`；agent 仍读取 `cellhive-coordination` Secret。密码仅存 Kubernetes Secret 与 `/opt/.secrets`，不得提交。

证据目录：`/var/tmp/cellhive-cloud-redis-20261124/`，其中 `private/` 含敏感备份，禁止公开。本报告与设计/运维文档修改保留在当前工作区，未额外提交或推送。

自动续费补充：GZ 原已开启；SG / USW 已设置 AutoRenewFlag=1，三个区域均回读确认。未执行即时续费购买，当前到期日仍为 2026-12-24。证据为操作目录的 autorenew-verified.json。
