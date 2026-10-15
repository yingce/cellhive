# 原生 KV / Queue 语义补齐

状态：第一轮实现完成（2026-10-15）。

- 原生 KV `expiration_ttl` 支持正整数秒（含 1–59），拒绝溢出；Cloudflare 管理兼容 API 继续要求 TTL >=60 秒。
- 原生 `incr` 增加始终为十进制字符串的 `value_i64`，支持整个有符号 i64 域；原 `value` 数字字段保留兼容，JS 调用方必须用 `BigInt(value_i64)` 获取无损结果。
- Queue 幂等记录独立于 messages，按每队列、首次成功发送起 24 小时保留。重复发送不续期；同键同指纹返回原 ID，同键不同指纹冲突；到期可重用键，即使旧消息仍存活。ack/purge 不删除幂等凭证。
- 逻辑指纹由 body 精确字节、规范化 content type、规范化 JSON metadata 组成，经 SHA-256；对象键顺序/空白不影响 metadata 相等性，JSON 数字不转 float64；delay 不属于消息身份，重复请求不更改第一次的投递时间。空 metadata 与 JSON null 等价。
- DLQ 移动与 operator replay 保留 body/type/metadata/原业务幂等键，使用源消息 ID 作为独立转移幂等凭证；允许显式重放已消费消息，但同一次转移重试不会重复入队。跨 owner 转移只能经内部鉴权端点。
- 本轮不修改 max_batch_timeout 凑批行为或逐 Worker CPU/内存硬隔离。

## 验证

回归先观察原实现失败：原生 TTL=1 被拒绝；i64 响应没有无损字段；ack 后重复发送生成新 ID；相同逻辑 metadata 因空白/键序/投递延迟变化冲突。

新增/扩展测试：`TestKVTTLAndMetadataE2E`（真实一秒 TTL 到期）、`TestKVCasAndIncrE2E`（大于 JS 安全范围及 i64 两端）、Cloudflare 单条/批量 TTL 下限、`TestIdempotencySurvivesAckFor24Hours`（包括 store 重开和边界过期）、`TestExpiredIdempotencyKeyCanCoexistWithOldMessage`、`TestIdempotencyCanonicalMetadataAndTransportDelay`、`TestIdempotencyMigratesLiveMessages`、`TestTransferPreservesMetadataAndHasIndependentReceipt`、`TestRemoteQueueTransferPreservesMetadata`（真实双 HTTP server、内部鉴权、消费后转移重试去重）。

通过：全部 SQLite tags 下 `go test ./... -count=1 -p 1`（显式设置 pinned `CELLHIVE_WORKERD`）、`go test -race ./internal/queue ./internal/server -count=1`、相关包 `go vet`、`git diff --check`。首次未显式选择 pinned workerd 的全量测试曾在 userruntime 输出 PASS 后发生 WaitDelay I/O 未关闭；该次不记为通过，显式 pinned 重跑全量通过。Docker `scripts/runtime-baseline-e2e.sh` 最终通过（`RUNTIME-BASELINE-E2E: PASS`），新增真实链路覆盖 Worker 原生 1 秒 TTL 到期、`sendBatch` 同幂等键返回同一 ID、消息实际消费、metadata（含大整数的字符串表达）保留。E2E 还暴露并修复了 `QueueProducer.sendBatch` 对 JSON 字符串做 `new Uint8Array(string)` 导致空消息的 JS 编码 bug。`incr` 仍只在 Go HTTP 契约测试中验证，因为当前 Worker KV facade 没有 incr 方法。

迁移限制：仅能回填升级时仍存在的消息；升级前已消费并删除的历史消息不能追溯建立去重凭证。禁止与仍依赖 messages 唯一索引、不了解 transfer receipt 的旧 writer 混用。
