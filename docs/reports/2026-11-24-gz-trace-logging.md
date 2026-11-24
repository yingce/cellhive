# GZ trace 日志诊断发布与观测

2026-11-24：为排查 App 115 `kv-cache-counter` / `vwork-115/__kv__/CACHE` 冷访问延迟，发布有时限的 agent span JSON 日志。原请求 trace `760acba4bcc9826d01ca86c64a87987e` 在 SLS 中函数耗时 5250ms、网关 5257ms；无 ARMS 查询权限，不能补取当时完整 span。

## 发布

- 独立分支 `fix/gz-trace-log-20261124`，基于线上 `5ba2cee3`，提交 `e99985a5` 已推送。
- GZ Helm revision 11；仅 agent 诊断镜像与日志截止时间变更；runtime 镜像不变，线上 Redis coordination 仍为 v2。
- Agent 镜像：`sgccr.ccs.tencentyun.com/vwork/cellhive@sha256:b6031f996e6d3a998f04eac44feff55a70d72bf26ca210d9ad8980341a9bd9dd`。
- `CELLHIVE_TRACE_LOG_UNTIL=2026-11-24T14:57:10Z`，北京时间 22:57:10 自动停止诊断记录。
- 两个 agent Ready、0 次重启。全量 `make test`、`make vet`、telemetry race 测试通过。

日志包含 trace/span/parent ID、名称、耗时和白名单属性，不记录请求体、凭据或完整 URL。未采样请求使用 RecordOnly，不提高 ARMS 采样率。异步有界队列满时丢弃诊断日志。新增对象存储读取/列举、local validation 和 epoch chain span。

## 实测

发布后首个探针 trace `464de86013cf45bda3d1344bcf92058e`，HTTP 200，客户端总耗时 **1583.282ms**，捕获 47 个请求 span。

| 阶段 | 耗时 |
| --- | ---: |
| `cell.wait_existing_open` | 718.694ms |
| `cell.capture_preflight` | 374.622ms |
| `cell.durability_proof` | 315.047ms |

同 scope、同时间区间的后台打开 trace `86a73c7ebf8fedb1b68f7ed17c255185`：`cell.open_uncached` 718.96ms，其中 `cell.restore_paged_probe` 294.78ms、`cell.restore_full` 423.76ms。恢复探测、完整恢复、首次写入前检查中可见重复 epoch/LTX 列举；完整恢复并行获取多个 L0 文件和 L9。父子 span 不重复累加。

随后热访问 trace `ab2c6fd9f961439b8257a8d5ba05b6b2`，HTTP 200，客户端总耗时 **179.846ms**，捕获 26 个 span。最慢 agent `http.server` 12.966ms，两次 `cell.durability_proof` 分别 8.930ms、7.843ms，`cell.capture_preflight` 降至 0.019ms / 0.010ms。

这些数据证明诊断日志生效，并定位到新样本的冷恢复及首次写入开销；不能据此认定原 5250ms 请求的全部耗时来源。当前发布是诊断增强，尚未发布延迟优化。Worker prepare 尚无独立细分 span，客户端总耗时不等同 agent span 总和。

## 证据

私有证据目录：`/var/tmp/cellhive-gz-slow-20261124-084121/`。包括原请求 SLS、agent 日志、测试/构建/发布日志、部署摘要以及上述两个请求的 span JSON。目录中的响应正文仅用于排查，不纳入仓库。
