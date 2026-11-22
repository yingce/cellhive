# Design: L0 合批上传 + L0→L1 + 换代 L9/L2 + 空闲/交权抬 base

日期：2026-11-20  
状态：已按 2026-11-21 代码修订。下面是现行实现，不是最初草案。

## 目标

降低小对象与无效 compaction 调用。单写者和 epoch fence 不变。没有 follower 时，写确认改为本地 spool fsync，对象最多晚一个合批周期进桶。

## 决策摘要

### 1. 换代抬 base

- **触发**：优雅交权前（`BeforeRelease`）与空闲驱逐（`L9_ON_EVICT` → `FoldBaseline`）。owner 和 epoch 都不变。空闲路径在没有 L9 时，用 owner 的未收 L0 文件累计和 `CELLHIVE_COMPACTION_MIN_OBJECTS` 比较，不列桶；交权前不看这个个数。再平衡和磁盘压力也先 Fold，但不在 claim 锁里做：cell 被打开或所有权已变就不交还；仍关闭才用最多 10 秒删 owner，这次不再 Fold。关机交权仍在锁里 Fold。
- **发布顺序**：当前代还不能单独恢复时，把快照之后的增量切成有界 L2，**先发最新块，最后再复制或合成基线**。基线在逻辑库和输入字节都不超过 `CELLHIVE_EPOCH_SNAPSHOT_MAX` 时是 L9；超过则同一段收到 L2，不合成新 L9。已有 L9 仍整份拷贝。当前代已经自包含则跳过，原因 `already compacted`。Fold 不发布 L3。
- **重叠**：前代增量如果跨过快照水位（例如 L9 覆盖到 tx 5，下一段从 tx 3 开始），第一段裁到 `snap.max+1`，完全落在快照里的输入丢掉。不裁就发布会让恢复链在 L9 落地前断开。
- **预算**：关机交权的 Fold 最多用 5 分钟，并给随后的 owner release 留 2 秒。剩余时间不够就跳过。Fold 失败也不挡住 release。再平衡和磁盘压力的 Fold 用同一个 5 分钟上限，但在锁外；随后的删除最多 10 秒。
- **不做**：普通同 session claim、每次裸 `epoch+1` 强制搬 L9。纯复制已有 L9 不受 1GiB 上限约束。显式整库快照和换代时新合成的 L9，逻辑库上限都是 `CELLHIVE_EPOCH_SNAPSHOT_MAX`（默认 1GiB），输入字节也用同一上限。显式快照超过就跳过。换代合成超过就改收到 L2。第一次 capture 全页快照另有 `CELLHIVE_CAPTURE_SNAPSHOT_MAX`（默认 8GiB），超过是失败。交权/空闲 Fold 的 L2 分块固定 256MiB，不跟随 1GiB。
- 崩溃接管后链仍长时，新 owner 可在随后的空闲或交权再 Fold。

### 2. L0 合批后再上传

- 桶上传 batcher：段数上限与收 L1 的文件个数共用 `CELLHIVE_COMPACTION_MIN_OBJECTS`（默认 256），`CELLHIVE_UPLOAD_INTERVAL` 默认 30 秒，字节上限固定 1MiB。三者满足任一即刷。`CELLHIVE_UPLOAD_MAX_SEGMENTS` 已删除。
- 这不是「最后一次写入后再空闲 30 秒」。到点、满段或满 1MiB 都会刷。
- 单机，以及双机里没有备机确认的写，先把段 fsync 到本地 spool 再返回。它们和已经 peer-ack 的后台上传进同一个队列，按 30 秒、256 段或 1MiB 合成一次 PUT。spool 写不下去才立即上传这一段。
- follower 之间的 fsync 确认仍然等本批落盘，不把那段等待改成 30 秒。

### 3. Level 压缩

- 有 spool 时写应答只等本地 fsync。L0 在随后的合批进桶。压缩仍在这次上传成功之后。
- 这一批 L0 刷完后只检查这个 cell，只收 L1。门槛仍是未覆盖文件满 256 个，或这些文件的存放字节满 64MiB。每次 L0 上传把光标放到已上传的最大事务号，每次 L1 把光标放到这批 L1 的最大事务号，然后只重算事务号还在 L1 光标之后的文件数和字节。完全落在 L1 光标里的重叠 L0 不再计入。这个判断不列桶。没有扫描周期，也不按存放时间晋升。后台不收 L2、L3。
- 选中的是整份 LTX。64MiB 只决定要不要开始。一批最多 256 个对象，合成一个输出，不把一个已合并的对象按字节拆开。一次上传会连续收，直到不再满足条件，或这一轮没有发出新对象，最多 32 批。32 批后仍超过门槛，或这一轮失败且所有权未变，延迟后再排入这个 cell；失败从 1 秒退避到 1 分钟。进程退出或所有权换走则不重试。仍不列桶。`CELLHIVE_COMPACTION_INTERVAL` 与 `CELLHIVE_COMPACTION_TIMED` 已删除。
- 交权和空闲 Fold 仍按 256MiB 切 L2，不走这套计数。新 L9 另看 1GiB 上限。

## 同 epoch 删除

`PruneIdleEpoch`、`PruneSupersededL9`、`PruneCoveredBelowL9` 只有 `Options.IdleDeleteOK == true` 才删对象。Fold 和空闲驱逐不置这个位，所以自动删旧对象仍然关闭。开启前必须同时有排空保护和「没有读者还引用旧对象」的证明；现在没有这条证明，不能在生产打开。

旧 epoch GC 与此无关：它只删已经不再被恢复链需要、并且上传时间已知、过了宽限期的旧代对象，而且只删扫描时记下的那份 key。任一对象没有上传时间，或出现非 LTX key，整代保留。

## 非目标

- 同 epoch 被覆盖对象的在线删除（仍关闭）。
- Redis / wake 主存迁移。
- 每次 epoch+1 强制 Fold。
- `--prune-l9` CLI。

## 验收

- `cmd/cell-agent/compaction_cascade_test.go`：255 个对象且不足 64MiB 不升；满 64MiB 的一块升 L1；调度不再调用 L2。
- `internal/compaction/promote_test.go`：L1 为 256 个对象或 64MiB。
- `internal/compaction/fold_test.go`：每块发布后仍可恢复；重叠前代被裁剪；复制 L9 前先查盘。
- `internal/compaction/prune_*_test.go`：未置 `IdleDeleteOK` 时删除数为 0。
