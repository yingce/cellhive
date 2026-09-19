# 线格式与协议定义

本文件定义跨进程/跨节点的数据格式与接口。所有整数为**大端**，所有字符串为 UTF-8。`proto_version` 当前为 `v1`（ADR-034）。

## 1. owner 记录（bucket `cells/<scope>/owner.json`）

```json
{ "node": "n1", "role": "cell-agent|do-runtime", "session": "s1",
  "epoch": 1, "expiry": 1730000000000, "address": "10.0.0.1:7001",
  "proto_version": "v1" }
```

- `expiry`（unix ms）**总是 ≤ 节点 lease expiry**；
- 获取：`If-None-Match:*`（冷）或 `If-Match:<etag>`（接管/续约）。

## 2. 节点 lease（bucket `nodes/<node>.json`）

```json
{ "node": "n1", "session": "s1", "advertise": "10.0.0.1:7001",
  "expiry": 1730000000000, "proto_version": "v1",
  "load": { "owned_cells": 12, "resident_cells": 12, "rss_bytes": 123,
            "cpu_percent_x100": 500, "pressured": false, "memory_headroom": 900000,
            "shed_cells": 0, "restoring": 0, "sampled_ms": 1730000000000 } }
```

## 3. node-log（bucket `node-logs/<node>/<session>.json`）

```json
{ "node": "n1", "session": "s1", "epoch": 1,
  "followers": ["n2"], "status": "open|recovering|sealed" }
```

## 4. LTX 段（`cells/<scope>/ltx/e<epoch>/<seq>.ltx`）

用于复制 SQLite 提交。P0 spike 使用**帧流 + 段头**；生产可对齐 `superfly/ltx`。

段头（固定 44 字节）：

| 偏移 | 长度 | 字段 |
|---|---|---|
| 0 | 4 | magic `"LTX1"` |
| 4 | 1 | `version`（段格式版本） |
| 5 | 1 | `kind`（0=delta, 1=snapshot, 2=link） |
| 6 | 2 | flags（保留；仅 `kind=snapshot` 解释：bit15=分页，bit0..14=part index） |
| 8 | 8 | `epoch` |
| 16 | 8 | `start_txid` |
| 24 | 8 | `end_txid` |
| 32 | 8 | payload length |
| 40 | 4 | CRC32C(payload) |

- 段文件名 `<start_txid>-<end_txid>.ltx`（或 `<txid>.snapshot`）；
- `kind=2 link` 用于 **snapshot→delta 衔接**（checkpoint 对齐）；
- 上传：`Put`（临时 key → 原子 rename），**热路径不 List**。

`kind=delta` 的 SQLite WAL payload（`WAL1`）：

| 字段 | 编码 |
|---|---|
| magic | 4 bytes `"WAL1"` |
| page_size | u32 BE |
| transaction_count | u32 BE |
| 每个 transaction | frame_count u32 BE |
| 每个 frame | page_no u32 BE + db_size u32 BE + `page_size` bytes |

每个 transaction 的最后一帧 `db_size != 0`（SQLite WAL commit marker）。LTX header 的 `start_txid..end_txid` 覆盖 payload 中按序排列的 transactions。

SQL capture 生产路径改用 **`WAL2` page-map**：一个 capture chunk 内按 page number 只保留**最后一次写入**，payload 只含最终 page image，不保留中间事务版本。

| 字段 | 编码 |
|---|---|
| magic | 4 bytes `"WAL2"` |
| page_size | u32 BE |
| commit | u32 BE（最终 DB page count）|
| tx_count | u32 BE（该 chunk 覆盖的应用事务数）|
| page_count | u32 BE |
| 每个 page | page_no u32 BE + `page_size` bytes（按 page_no 升序、唯一）|

**`WAL3`（page-map v2，ADR-160）**：snapshot/compaction 现在写 `WAL3`——每个 page frame 先用 **LZ4 block** 压缩（压不小则存 raw），并把**每页的 frame 位置**放进 payload 头部索引，使 paged 读取能一次 ranged read 直接取到并解压单页（不必扫 payload）：

| 字段 | 编码 |
|---|---|
| magic | 4 bytes `"WAL3"` |
| page_size / commit / tx_count / page_count | u32 BE ×4 |
| reserved | u32 BE |
| 索引（每页 12B） | page_no u32 BE \| off u32 BE（payload 内偏移）\| stored u32 BE（低 31 位=帧长，bit31=1 表示 LZ4）|
| frames | 每页一个（raw `page_size` 或 LZ4 block，解压后恒为 `page_size`）|

`WAL2`（v1，固定帧、无压缩）仍可被解码（向后兼容）；`PageLocs`/`FrameData` 统一处理两种。

**快照分页（大 cell）**：一个完整快照（页 `1..commit`）可拆成多个 `kind=snapshot` 段，**共享同一 txid watermark**，每段 payload 仍是 `WAL2` 且带相同的 `commit`，页范围互不重叠、按页号升序。分片信息编码在 **LTX header 的 `flags`**（仅对 snapshot 解释）：

- bit15 = 分页标记；bit0..14 = 0-based part index；
- 段名 `<end_txid>.p<part>.snapshot`（放得下的单段快照仍是 `<end_txid>.snapshot`，`flags=0`，向后兼容）。

restore 取**最大 watermark** 的快照，合并其所有分片后再按 txid 套用 delta。每段预算默认 `ltx.DefaultSnapshotPartBytes`（8 MiB），须小于 cell-agent `maxSegmentBytes`（64 MiB）。

**L1 compaction（大 cell 接管优化）**：`cells/<scope>/ltx/e<epoch>/L1/` 存放折叠后的快照分片，`L1/manifest.json` 指向当前 L1：

| 字段 | 说明 |
|---|---|
| `min_txid` / `max_txid` | 该 L1 覆盖的 txid 区间 |
| `objects` | L1 快照分片对象 key（按 part 顺序） |
| `commit` / `page_size` | 折叠后快照的最终页数 / 页大小 |

接管 restore：有 manifest 时只读其 `objects` + `txid > max_txid` 的 L0 delta；否则读整条 L0 链。L0 段暂不删除（`Bucket` 无 Delete 接口），由 manifest 指针取代。

`WAL1` 保留用于需要 transaction 边界的场景与兼容解码。delta payload 目前仍不压缩（很小；见 known-issues）。

**L1 页索引（`L1/index.bin`，ADR-160）**：`CID2` 在 v1 的页号表后追加**每页 12B 的 frame 位置**（page_no u32 \| off u32 \| stored u32，bit31=LZ4），cold start 只取需要的页；`CIDX`（v1，仅页号）仍可读。

## 5. bundle manifest（对象存储 `bundles/sha256/<aa>/<hash>` 内）

```json
{ "proto_version": "v1", "sha256": "<bundle-hash>",
  "entry": "index.js",
  "modules": [ { "name": "index.js", "type": "esm", "sha256": "...", "size": 123 } ],
  "compatibility_date": "2026-04-24", "compatibility_flags": [],
  "assets_ref": "<hash>", "bindings_ref": "<control-cell-key>" }
```

- 模块加载前逐项校验 `sha256`；
- 版本指针：`deploys/<ns>/<worker>/<version>.json` → `{ bundle_sha256, bindings_ref, created_at }`。

## 6. 路由投影（user-runtime 拉取）

```json
{ "projection_version": 42, "generated_ms": 1730000000000,
  "routes": [ { "host": "demo.workers.local", "path_prefix": "/hello-jsonc/",
                "ns": "demo", "worker": "hello-jsonc", "active_version": 3 } ],
  "reserved_ns": ["__system__", "__platform__"] }
```

- 纯拉取 + 5–10s TTL（ADR-031）；`projection_version` 单调递增。

## 7. Go↔Go 内部调用（HTTP，`:7001`）

**内部无 gRPC**（ADR-136）；下述逻辑操作都是 REST/HTTP 端点（LTX 段为长度前缀二进制帧）：

| 逻辑操作 | 端点 | 用途 |
|---|---|---|
| append | `POST /v1/peer/append`、`/v1/peer/append_batch` | owner→follower 复制（LTX 字节帧） |
| tail | `GET /v1/peer/tail` | 恢复时拉取 follower 保留段 |
| seal | `POST /v1/peer/seal` | recovery：seal follower |
| acquire / release | `POST /v1/internal/{claim,release}` | 归属与优雅 handoff |
| resolve | `GET /v1/internal/resolve` | owner 解析 |
| restore | `GET /v1/internal/blob`（分页 ranged） | 冷激活/接管下发快照+delta |

详细协议与帧格式见 [`networking.md`](./networking.md) 与 ADR-042。

## 8. REST（workerd JS → cell-agent，`:7001`；与对外同一接口，ADR-030/031）

`/v1/kv/*`、`/v1/d1/*`、`/v1/queue/*`、`/v1/workflow/*`、`/v1/cron/*`；请求头带 `x-cellhive-internal-token` + `x-cellhive-scope`（scope 声明）。

SQL capture 内部热路径：

```text
POST /v1/internal/commit_binary?scope=<scope>&epoch=<epoch>
Content-Type: application/octet-stream
X-Cellhive-Followers: <comma-separated peer URLs>
Body: raw LTX segment (max 64MiB)
```

响应沿用 `{ "mode": "fleet", "acked_by": "..." }`；JSON/base64 `/v1/internal/commit` 保留兼容。

## 9. 错误码

| code | 含义 |
|---|---|
| `invalid_scope` | scope 格式错误 |
| `unauthorized` | 内部 token 缺失/错误 |
| `owner_live` | 其他节点持有 live lease（附 owner） |
| `claim_race` | 条件写竞争失败（重解析） |
| `epoch_mismatch` | epoch 已变（被接管） |
| `result_unknown` | 非幂等操作结果未知（**不重放**） |
| `storage_unavailable` | 对象存储不可用 |
| `overloaded` | 过载（P1 admission） |

_最后更新：2026-09-14_
