# SG / USW Redis 恢复权威 v3 发布与性能观察

2026-11-24，按用户要求提交推送本地代码并发布 SG、USW；GZ 保持原诊断版本。两区发布、业务验证及各十分钟稳定性观察已完成。

## 源码与发布边界

- 分支 `feat/redis-coordination`，源码提交 `7df788d3` 已推送；工作区全部本批改动已纳入，未合并 main。
- 镜像由干净 commit archive 构建，仅替换 agent；保留已验收 runtime/workerd。
- Tag：`20261124-recovery-v3-7df788d3`。
- Digest：`sha256:e36e74d356f99f2f134469abd8af6cc6aa7fc8e3b8629c00601f7840a50c086c`。
- Redis v3 将 session 正文/状态/sealed CoveredSeq、DO recovery pointer 迁至 Redis CAS；LTX 业务数据仍在 COS。
- epoch GC 默认间隔 1h、首次 1m，每 agent 一个 goroutine 遍历 OwnedScopes。两区线上无覆盖该默认值的环境配置。
- `CELLHIVE_TRACE_LOG_UNTIL=2026-11-24T15:49:37Z`，北京时间 23:49:37 自动关闭额外 span 日志。
- 两区最终 Helm values 相对发布前仅 agent digest 与 trace 截止时间变化。迁移期间暂时关闭 HPA、将业务副本数置零，恢复后回读验证。

## 发布前验证

`make test`、`make build`、`make vet` 通过；telemetry/nodelog/recovery/coordination race 通过。隔离临时 Redis 上 coordination/recovery/server race 通过。object 与 Redis 两种后端的真实双进程 session 恢复测试均通过，包含 80 条已确认数据、停进程超 TTL 接管与旧进程恢复后拒绝继续写。临时 Redis 已清理。常规全量测试未设置 Redis URL，Redis 集成另有独立命令验证，不将 skip 当作通过。

迁移前两区验证 20 条历史 KV，另写入并读回 20 条 KV。十次连续 Worker `/status` 请求均 HTTP 200；通过本机 Kubernetes port-forward 访问，两区网络基线不同，不能把客户端耗时直接当作服务端处理耗时。

## 离线迁移

先停止 HPA、user-runtime、DO runtime、全部 agent，确认旧写者全部退出。随后导出完整 Redis 键（保留类型/二进制值/绝对过期时间）并备份 COS 协调/session/DO 元数据物理对象，保存 SHA-256。通过 Helm wrapper 部署维护状态、运行 `coordination-recovery-init --offline`，验证 generation 不变与 recovery ready marker，再恢复原副本数及 HPA。

- SG：COS 1,218 个物理对象，Redis 9 个顶层键；迁入 59 个 session 正文，旧 DO pointer 为 0。Helm 151 → 152（维护）→ 153（恢复）；停写命令至全部业务 Pod Ready 85 秒。
- USW：COS 720 个物理对象，Redis 9 个顶层键；迁入 56 个 session 正文，旧 DO pointer 为 0。Helm 143 → 144（维护）→ 145（恢复）；停写命令至全部业务 Pod Ready 141 秒。

以上 Redis 键为聚合 hash/zset 等顶层键，不是业务记录数。停写至 Ready 不是连续外部 HTTP 不可用时长测量。v3 接受新写后不能直接回退 v2 或旧 Redis 备份。

## 已捕获的冷恢复证据

SG 真实业务 `vwork-114/__kv__/DRAMA`：agent `http.server` 1406.98ms，trace `5014310c38e7ee88fe57049be9be43ae`。对应后台打开 trace `d299c6fdf78cd720e5a627d472d816cb`：`cell.open_uncached` 1361.32ms，`cell.restore_paged_probe` 1270.45ms，记录 101 次 `object.RangedGet`、3 次 CommonPrefixes、2 次 ListPage。这里是调用次数，不能当作 101 个不同文件。

SG 后台 Cron `vwork-77/__cron__/scheduled-job`：打开 1966.83ms，其中两次恢复槽位等待分别 491.70ms、422.33ms，完整恢复 881.61ms；198 次完整 GET。该后台打开时长不能当作用户 HTTP 响应时长。

Redis 元数据迁移不会自动消除 LTX 恢复 I/O；当前仍观察到秒级冷打开。本次未修改恢复选链/seek/合并策略，不能宣称解决原 GZ 5.25 秒请求。

## 证据

`/var/tmp/cellhive-sg-usw-v3-20261124/` 保存源码 SHA、测试/构建日志、镜像 digest、各区发布 diff、迁移 Job 日志、业务探针、Pod/镜像回读、COS 指标与 span。`private/` 含 Secret/备份，只留本机，禁止提交或公开。

两区上线后的旧 KV、新写入、使用新 key 的 Queue 首次消费/重试、新 DO Alarm completed 增量、新 Cron scheduledTime 推进全部通过。迁移后 COS 恢复元数据核对：SG 134 个、USW 117 个历史物理对象，无新增、无 ETag 变更、无删除。

USW 真实业务 `vwork-74/__kv__/SCHEDULED_RUNS` 的 agent 请求 1960.75ms（trace `38fee246ab4a76afaab9c4d1165ac757`），其中 `cell.capture_preflight` 1934.54ms，包含等待打开 1768.32ms。另一个后台 Cron scope `vwork-74/__cron__/scheduled-job` 打开 2394.00ms、完整恢复 2285.88ms，实际 199 次完整 GET；两者不是同一个 cell，不混合归因。

## 探针延迟

同一已存在测试 Worker `/status`，发布前/后各 10 次连续请求（经本机 Kubernetes port-forward）：

| 区域 | 发布前中位数 | 发布后中位数 | 发布后首请求 | 状态 |
| --- | ---: | ---: | ---: | --- |
| SG | 115.27ms | 110.93ms | 182.20ms | 全部 HTTP 200 |
| USW | 454.95ms | 473.96ms | 550.34ms | 全部 HTTP 200 |

这些小样本包含跨区域网络与 Kubernetes 转发，首请求也可能已由后台恢复预热；不称作严格冷启动基准，不计算生产 p95，不据此宣称提升或退化。

## 运行边界

启动时出现过 owner 尚未取得、peer 尚未就绪、写入暂落 spool 的告警；不计作持续健康问题，但保留原始日志。SG timer 派发失败仍集中在发布前相同的 `vwork-105/__cron__/pageverra-api` 与 `vwork-96/__cron__/diagnostic-log-upload`；不是零告警发布。USW 有 `epoch fold deferred: continuation waits for resident cell` 延后记录。

上线后 Redis 回读：SG 61 个 session 正文、2 个 DO pointer；USW 58 个 session 正文、2 个 DO pointer，generation 保持不变。两区最终 Helm diff 为空，迁移 Job 和临时 relay Pod 已删除；原 runtime 镜像未更换。

## 最终观察结果

两区各 600 秒、11 次健康检查全部通过，每区六个业务 Pod Ready 且重启数为零；11 次持续 Worker 探针全部 HTTP 200，Cron 时间持续推进。实际 fleet 提交计数：SG 两 agent 为 375 / 927，USW 为 234 / 26。镜像 digest、原 runtime 镜像和 values 范围已回读确认。

双 agent 合计 COS 访问速率，单位次/秒：

| 区域 | 发布前 120s GET / LIST | 发布后 600s GET / LIST | 后续稳定 300s GET / LIST |
| --- | ---: | ---: | ---: |
| SG | 0.158 / 0.964 | 7.037 / 0.630 | 0.033 / 0.000 |
| USW | 0.000 / 0.000 | 0.038 / 0.215 | 0.000 / 0.000 |

SG 发布后首个窗口 GET 中 epoch_fold 占 6.291 次/秒，后续 300 秒已降至 0；剩余 GET 0.033 次/秒来自静态资源。USW 后续窗口 GET/LIST 均为 0。没有 recovery/node-log 来源的 COS GET/LIST。前后负载和 GC/fold 阶段不同，不能直接换算性能/账单降幅；一小时 GC 尚未覆盖完整周期，不承诺持续零 LIST。

综上：v3 迁移和减少恢复元数据 COS 访问已在两区验证；冷恢复秒级延迟仍有实证，尚未修复。下一步应针对实际选中的恢复链与分页目录读取优化，不能用测试 cell 的文件数量替代真实业务 cell 的证据。
