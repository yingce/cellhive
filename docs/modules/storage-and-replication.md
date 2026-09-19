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

## 源码位置

`internal/{bucket,objectstore,ltx,replica,compaction,pagedvfs,capture,upload,peer,nodelog,recovery,restore,walscan?}`；`cmd/{s3probe,s3init,restoreverify,recoververify}`。

## 测试锚点

`make rpo-test`、`make s3-test`、`internal/{ltx,replica,compaction,pagedvfs}`。

## 相关文档

[`storage-and-s3.md`](../storage-and-s3.md)、[`cell-protocol.md`](../cell-protocol.md)、[`benchmarks.md`](../benchmarks.md)

_最后更新：2026-09-19_
