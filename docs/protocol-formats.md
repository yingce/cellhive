# 线格式与协议定义

本文件定义跨进程/跨节点的数据格式与接口。所有整数为**大端**，所有字符串为 UTF-8。`proto_version` 当前为 `v1`（ADR-034）。

## 1. owner 记录（bucket `cells/<scope>/own.json`）

```json
{ "node": "n1", "role": "cell-agent|do-runtime", "session": "s1",
  "epoch": 1, "expiry": 1730000000000, "address": "10.0.0.1:7001",
  "admin_address": "10.0.0.1:8082", "proto_version": "v1" }
```

- `expiry`（unix ms）**总是 ≤ 节点 lease expiry**；
- 获取：`If-None-Match:*`（冷）或 `If-Match:<etag>`（接管/续约）。
- `own.json` 是逻辑 owner key。COS/OSS 通过 native append 的位置条件及有界 generation 实现条件写；S3/MinIO 控制记录通过条件 PUT/tombstone 适配。物理 owner 编码不等同于普通 JSON PUT，详见 [存储适配](storage-and-s3.md)。旧 `owner.json` 布局不在线兼容。

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

## 4. 原生 LTX（`cells/<scope>/ltx/e<epoch>/<level-4hex>/<min>-<max>.ltx`）

唯一存储格式是 celld/Litestream 原生 LTX，与 `/opt/celld` 的固定参考版本 `90b4301` 对齐。无格式开关、旧格式兼容或独立 manifest/index。旧数据迁移使用 SQLite export/import。

Header 固定 100 字节，多字节整数为大端序：

| 偏移 | 长度 | 字段 |
|---|---|---|
| 0 | 4 | magic `LTX1` |
| 4 | 4 | flags（当前 writer 使用标准 NoChecksum 位；文件 CRC64 仍校验） |
| 8 | 4 | page size |
| 12 | 4 | commit（最终数据库页数） |
| 16 | 8 | min TXID |
| 24 | 8 | max TXID |
| 32 | 8 | timestamp |
| 40 | 8 | pre-apply checksum |
| 48 | 8 | WAL offset |
| 56 | 8 | WAL size |
| 64 | 4 | WAL salt 1 |
| 68 | 4 | WAL salt 2 |
| 72 | 8 | node ID |
| 80 | 20 | reserved |

Header 不包含 epoch 或 kind。epoch 来自对象目录；**min TXID=1 就是快照**，其余为增量。快照必须覆盖最终数据库全部页面（SQLite pending-byte lock page 除外）；增量仅保存范围内每个被修改页的最终图像，并按 commit 处理截断。

文件顺序为：100 字节 header → 原生 page frames → 6 字节零 page header → 内嵌 page index → 8 字节 index length → 16 字节 trailer。页帧按页号排序，使用原生 LZ4 编码；索引以 varint 编码页号、文件内 offset 和帧长度，以零结束。trailer 包含 post-apply checksum 与文件 CRC64。不再包裹 WAL1/WAL2/WAL3、CIDX/CID2 或私有 44 字节段头。

对象名为固定 16 位小写十六进制 `<min>-<max>.ltx`，只扫描实际 level `0000`、`0001`、`0002`、`0003`、`0009`。普通 PUT/multipart 发布整个文件，允许同名幂等重传；快照先在本地 spool 完成并 fsync 后发布。不产生 snapshot-started/done、epoch-cut、seal、pin、tombstone 或自定义 fence 文件。

续链在新 epoch 发布 `[cut+1,cut+1]` 的零页原生 LTX，保留真实 page size/commit 与 WAL 坐标；首个应用增量从 cut+2 开始。恢复按最新 epoch 倒推，只接受恰好衔接 cut 的前驱，并裁剪越过该 cut 的晚到前驱尾部。

压缩按独立的 30 秒、5 分钟、1 小时 UTC 边界晋升 L0→L1→L2→L3，只合并未覆盖范围。L9 是完整链快照。压缩为 additive，不删除源对象；回收遵循原生 epoch GC，使用 celld 自带的 stream 级 `ltx/retired.json`，见 [存储与 GC](storage-and-s3.md)。

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

_最后更新：2026-09-19_
