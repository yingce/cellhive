# Sealed follower recovery 合批（2026-10-27，本地未发布）

## 改动

此前trace确认：owner异步上传已合批，但接管时sealed follower tail逐段Append，造成119个恢复对象、206个逻辑段中93段重复。此次只修改sealed session恢复路径，连续同scope/epoch/txid的delta按最多64段或1MiB合批。切换scope/epoch、txid不连续、快照/marker即flush；非delta保留独立对象名，不改变恢复基线发现。单段超过1MiB仍单独上传，不拆分已编码段。

每页全部上传成功（或权威删除fence明确替代）才checkpoint；失败不封存、不reclaim。重试走既有immutable对象校验；不跳过尚未独立证明已持久化的数据。legacy Held与active DO live-tail不变。对象格式沿用既有AppendBatch，无新存储格式、无新增bucket List。

这减少重复内容的物理对象数，不消除重复内容本身。恢复仍下载后按Header.ID去重；旧的已上传单段对象不会自动被这次改动重写。后续compaction仍负责收敛历史对象。

## 测试

- `TestRecoverSessionTailAcrossPages` 修复前失败：300段产生300对象，目标<=8；修复后完整恢复300段，包含owner预先发布的重叠batch。
- `TestRecoveryBatchBoundaries` 覆盖字节上限、scope/epoch隔离、txid gap、快照及marker独立对象。
- `TestRecoveryBatchFailureRetainsPending` 覆盖上传失败保留pending。
- `TestRecoveryBatchFailureDoesNotCheckpointOrReclaim`：100段，第一批64段成功、第二批失败，checkpoint仍0且未sealed；重试100段完整恢复，最终2对象、checkpoint100且sealed。
- recovery race三轮通过，replica race通过；make build/vet通过。完整make test退出0（所有Go包）。

## 真实HK COS E2E

使用与trace基线相同的 `hk-review.py --suite batch-recovery --port-base 28600 --trace-endpoint http://127.0.0.1:28480`。本机双agent、真实COS、100次不同key的fleet确认，强杀owner，接管校验100个值；survivor drain后删除整个测试cells目录，重启后测首GET并校验100个值。包括冷控制cell，不等价于只淘汰KV。

| 指标 | 改前trace第2轮 | 改后第1轮 |
|---|---:|---:|
| KV下载对象数 | 119 | 22 |
| KV对象下载 | 582.686ms | 138.183ms |
| KV cold open | 711.815ms | 255.735ms |
| SQLite文件应用 | 4.701ms | 6.176ms |
| 逻辑段解码/重复 | 206/93 | 210/96 |
| 冷GET总耗时 | 2948.081ms | 1100.331ms |

第1轮强杀时仍有5项spool；接管及冷恢复均100/100正确，app清理HTTP200。冷GET trace `c62a9bed0f28ffa208f4883d9202869d`；KV恢复 `81a323d39a07f3b719b19120f58bac98`。

对象数量是trace下载池实际对象数，不是逻辑段计数。旧测试warmup与本轮有少量不同，故最终逻辑段数不同。总耗时同时受控制cell冷启动与COS波动影响：不能将全部2.95s→1.10s差额归因合批，更不能声称已保证<1s。

第2轮同样通过：强杀时22项spool，接管和冷恢复100/100正确，app清理HTTP200；下载18对象79.925ms，KV cold open200.037ms，SQLite应用5.091ms。解码206段/重复96段。完整冷GET1609.341ms，binding查询473.722ms；owner查询之后、KV打开之前另有约880ms未细分的区间，因此本次不宣称解释了所有剩余耗时。

第2轮请求trace `7d8c243f55884f9ea8c4dff52cf9d3d0`；KV恢复 `70772b264ecaa6d52c4ac8fd9fd07a98`。测试二进制SHA256 `4434604f540af0013c82d1102659b26f907f40003f24b2b1016f65a6afc6f664`。

两轮下载对象数为18–22（改前119–120）；主要归因是同一sealed tail不再逐段创建对象。逻辑去重计数仍高，说明未靠丢弃数据实现对象减少。

未提交、未推送、未部署。控制cell冷启动、重复epoch发现及COS尾延迟仍待后续处理。
