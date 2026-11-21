# Epoch GC 与当前代抬 base（2026-11-20）

状态：2026-11-21 已落地。下面按现行代码更正最初草案里已经不对的步骤。

## 目标

1. 旧 epoch GC 扫完整代，不用统计接口的 5 万对象上限当跳过理由。
2. `FoldBaseline` 把恢复链收进**当前** epoch（L9 + 有界 L3），之后 Safe GC 才能删前代。

不做：同盘重启保留 epoch；不经抬 base 就按覆盖范围删旧代。

## 文件

| 文件 | 结果 |
|--|--|
| `internal/replica/epoch_gc_native.go` | 分页扫完全部对象；只删扫描时记下的 key |
| `internal/compaction/fold.go` | `FoldBaseline` |
| `internal/compaction/fold_test.go`、`fold_s3_integration_test.go` | 崩溃中途、重叠裁剪、大 L9 |
| `cmd/cell-agent/main.go` | 交权前与空闲驱逐调用 `FoldBaseline`，不置 `IdleDeleteOK` |

## FoldBaseline

1. 占 compaction 槽，并在读输入和每次发布前跑 `Verify`。
2. 当前 epoch **单独就能恢复**才跳过（`already compacted`）。仅仅「已经有一个 L9」不够：L3 发到一半崩溃后必须能续上。
3. 用 `RestoreChainKeys` 取链。没有 `min==1` 的快照则跳过。优先用 `min==1` 的 L9。
4. 快照之后的文件先丢掉 `max<=snap.max` 的，第一段重叠输入裁到 `snap.max+1`，再按 `MaxInputBytes` 切块。交权/空闲路径固定 256MiB，不使用 `CELLHIVE_EPOCH_SNAPSHOT_MAX`（该值默认 1GiB，只限制显式整库 L9）。
5. **从最新一块开始发布 L3，L9 最后落地。** 已有单对象 L9 且不在当前代时直接复制，复制前先查 spool 空间，不受 80MiB 逻辑库上限约束。否则压缩合成。
6. 调用方另跑 `RetireEpochs`。Fold 自己不删前代。

## 旧 epoch GC

- 链必须来自恢复规划。当前代不是链尾、或仍有 pin，则不删。
- 上一代表留。更老的代要过宽限（生产 1 小时，按该代最新对象的上传时间）。
- 任一对象 `LastModifiedMs<=0`，或出现非原生 LTX key，则 `nativeOnly=false`，**整代保留**。
- 删除只用这次扫描得到的 key 列表（`deleteEpochGCKeys`）。扫描之后新出现的对象不会被这次删除带走。

## 触发

`BeforeRelease` 与 `runL9OnEvict` 调 `FoldBaseline`。不在每次 epoch 切换时搬。交权 Fold 有独立时间片，不够就跳过，失败也不阻止 release。

## 同 epoch 删除

见设计说明里的 `IdleDeleteOK`。生产调用不设置它。
