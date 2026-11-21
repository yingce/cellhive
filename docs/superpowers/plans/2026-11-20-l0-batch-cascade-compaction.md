# L0 batch + cascade compaction

状态：2026-11-21 只在 L0 进桶后按 256 个对象或 64MiB 收 L1。定时压缩已删除。

设计：`docs/superpowers/specs/2026-11-20-l0-batch-cascade-fold-design.md`

## 已落地

| 文件 | 结果 |
|--|--|
| `internal/config/config.go` | 上传段数与 L1 文件数共用 `CELLHIVE_COMPACTION_MIN_OBJECTS`（默认 256）。`CELLHIVE_UPLOAD_INTERVAL` 默认 30s |
| `cmd/cell-agent/main.go` | L0 进桶后用 owner 累计判断 L1：256 个文件或 64MiB。空闲 Fold 的文件数门槛用同一累计。没有定时模式 |
| `cmd/cell-agent/compaction_cascade_test.go` | 不足 256 个且不足 64MiB 不升 L1；后台不调用 L2 |

个数或字节门槛在调用 `CompactLevel` 之前判断。一批只收整份 LTX，最多 256 个，不按字节切开。够了就连续收，直到不再满足或没有新对象发出，最多 32 批。32 批后仍超过门槛，或本轮失败且所有权未变，延迟后再排入；失败退避到 1 分钟。进程退出或所有权换走不重试。显式整库 L9 和换代新合成的 L9 都看 `CELLHIVE_EPOCH_SNAPSHOT_MAX`（默认 1GiB）。显式快照超过就跳过。换代合成超过就改收到 L2。交权/空闲 Fold 复制已有 L9，增量按 256MiB 切成 L2，不发布 L3。

## 不要再按原任务做

- 不要恢复 30 秒 / 5 分钟 / 1 小时的 level 时钟，也不要按分钟去试 L2、L3。
- 后台不发布 L2、L3。L2 只在换代 Fold 里出现。
- 1MiB 字节上限是 `NewSharded` 的固定参数，没有单独的环境变量。
