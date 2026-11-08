# 模块：对象存储与复制

对象存储是权威：bucket 条件写选 owner，LTX 捕获/复制、快照/compaction、按页冷恢复（paged VFS）、GC。本地盘只是工作副本/缓存。

> 配置权威来源见 [`../configuration.md`](../configuration.md) 与代码；下表是本模块相关子集。
## 关键接口

bucket 抽象（FS / S3 兼容）；`POST /v1/internal/commit(_binary)`；peer `POST /v1/peer/append_batch` 与 HTTP 101 流 `/v1/peer/stream`。

## 配置（环境变量）

| 变量 | 默认 | 作用 |
|---|---|---|
| `CELLHIVE_CAPTURE_GROUPCOMMIT` | `0`→2ms | WAL 合并窗口 |
| `CELLHIVE_CAPTURE_PIPELINE` | `0`→8ms | 提交延迟阈值，超过则多 chunk 在途 |
| `CELLHIVE_CAPTURE` | 启用 | `off`/`0`/`false` 关闭捕获=写仅本地无复制证明 |
| `CELLHIVE_CELL_WAL_CHECKPOINT` | `64MiB` | 捕获 cell WAL 截断阈值，`0` 关闭 |
| `CELLHIVE_UPLOAD_SHARDS` | `0`→NumCPU(≤8) | 并行桶上传通道 |


> 解析规则（字符串/布尔/时长/字节/列表）见 [`../configuration.md`](../configuration.md#解析规则)。

## 关键不变量

- 条件写是唯一 owner 仲裁（无共识服务）。
- 接管控先恢复死 owner 的 open log。
- 本地盘永不作为唯一副本；未上传的已 ack 数据不可删。
- 热路径禁 List。
- `OfflineQuiescent=false`（生产默认）时，compaction 只做有界的元数据 listing 观测并报告 epoch、对象数/字节数及 covered/deferred/unknown 分类；不会读取 payload，也不会删除对象。无法从 listing 得到大小时字节数标记为 unknown，超出观测上限的对象同样只计入 unknown。
- 历史 epoch 的物理 GC 仍需要外部停止并排空 writers/readers 后显式维护窗口；在线 compaction 不删除历史对象，因此 deferred storage growth 和对象存储费用需要容量监控与离线维护计划。
- ADR-309 增加显式 admin/CLI Safe GC、默认保留两代 L1、backlog/lag 指标，以及先写 deleting fence 的幂等清理；生产默认仍 DEFERRED。
- ADR-308 定义了可选的 Safe 删除协议：reader 写带 lease 的 pin，退休前写不可变 seal 并等待 pin 排空，逐 key 写 tombstone 且宽限期后才能删除；成功退休的 epoch 写 `gc-retired`，恢复与水位扫描跳过它。默认仍是 DEFERRED，生产后台不得启用 Safe。

## 源码位置

`internal/{bucket,objectstore,ltx,replica,compaction,pagedvfs,capture,upload,peer,nodelog,recovery,restore,walscan?}`；`cmd/{s3probe,s3init,restoreverify,recoververify}`。

## 测试锚点

`make rpo-test`、`make s3-test`、`internal/{ltx,replica,compaction,pagedvfs}`。

## 相关文档

[`storage-and-s3.md`](../storage-and-s3.md)、[`cell-protocol.md`](../cell-protocol.md)、[`benchmarks.md`](../benchmarks.md)

_最后更新：2026-09-19_
