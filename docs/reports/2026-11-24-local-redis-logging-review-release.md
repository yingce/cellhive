# Redis 与 invocation 日志优化本机部署

源码提交 `8fa81f58`，本地 main；未推送、未部署云端。部署对象为既有 `cellhive-migration-test` 双 agent Redis/MinIO Compose 栈，保留原端口、凭据、数据卷和 v3 binding，无迁移或重新初始化。

镜像 `cellhive:local-8fa81f58`，实际 ID `sha256:9f57e446e5d3e8f208590031a5b2421734912f5f928a78bce2feb7a135ef7931`。从提交归档编译四个 Go 二进制，叠加到原本机基础镜像并复制该提交 workerd JS；未修改 workerd fork。fork SHA-256 为 `afd36efa583587fd8fe1574507006f4bf3324693f82ce1398a33920d5a1b3a5f`，前后相同。初次 Docker 构建因裸 image ID 和 bin ignore 失败，修正本地打包配置后成功；业务 runtime 有短暂停机。

## 验证

- 两个 agent、user-runtime、do-runtime 全部 healthy、重启次数 0；实际镜像 ID 相同，四个服务的二进制与归档构建 SHA-256 相符。
- 20 条历史 KV 读回，新 5 条 KV 写读通过。
- Queue 首次消费成功，显式请求重试后第 2 次成功；DO Alarm 与新的 Cron tick 通过。
- 独立测试应用 `review-8fa81f58` 的真实函数异常返回 500，落盘 `failure_stage=handler`；主动返回 503 落盘 `handler_response`。两者 `error_origin=unknown`，不把用户函数中的异常直接归责用户。
- 上述两条实际日志分别通过两份 Vector 配置，共 4 项归一化检查；凭据与 URL 文本脱敏通过。
- 本机未配置 OTLP 接收端或独立 CellHive Vector，实际日志保留在 runtime spool。Vector 是隔离容器重放验证，不代表线上采集链路已经更新。

部署前组合测试见 `/tmp/cellhive-combined-validation.md`；部署证据与私有配置备份位于 `/tmp/cellhive-local-8fa81f58/`，不得公开含凭据的 compose-before.json。保留独立本地测试应用和带提交前缀的测试记录用于复查。

## 观察与边界

启动时出现一次 owner unowned 的 Cron/Queue 警告，此后真实 Cron/Queue 验证通过；非 owner agent 的 purge retained 和旧 queue 的 control not found 告警仍存在。启动时另有一次 epoch fold deferred（maintenance cannot resume durable chain）；本次未归因或修复，不将功能验证当作 fold 全路径验收。无自动重启。

本机同宿主双 agent 不代表跨机器容灾；未做托管 Redis 主从故障切换或线上性能验收。
