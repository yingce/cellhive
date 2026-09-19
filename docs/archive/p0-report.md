# P0 验证报告

_生成：2026-09-14 · 工具：`cmd/cellbench`（进程内 httptest + 文件系统 bucket）、`cmd/realbench`（真实多进程 + 真实 TCP）、`cmd/s3init`（S3 初始化/探针）_

## ⚠️ 指标真实性说明

- **`cmd/cellbench` 的早期数字不是"真实"指标**：in-process httptest + 文件系统 bucket（无真实网络、无真实对象存储）。
- 本轮补充了**真实测量**（见下节"真实测量"）：
  - **真实多进程 + 真实 TCP**：owner/follower 为两个独立 `cell-agent` 进程，走 loopback HTTP（`cmd/realbench`）。
  - **真实 S3 兼容对象存储**：MinIO（`RELEASE.2025-04-22`，docker，loopback）作为 bucket，`S3Bucket` 走真实 S3 协议（条件写/range/presign）。
- **仍需注意**：MinIO 在本机 loopback，**云 S3 区域延迟约 ~90ms**（见 `cell-protocol.md` §6），本页 S3 数字不代表云上跨区/同区真实延迟。

## 真实测量（本轮）

### 启动探针（真实 S3）

3 个 `cell-agent`（owner/follower/bucket）以 `CELLHIVE_BUCKET=s3://cellhive` 启动，均输出 `storage diagnose ok` → **真实 S3 条件创建/CAS/ranged read 通过**；`s3init` presign 成功。

### 真实多进程 + 真实 TCP（FS bucket）

| 姿态 | c | n | failures | p50 | p99 | rps |
|---|---|---|---|---|---|---|
| fleet（follower fsync） | 1 | 1000 | 0 | 1.23ms | 2.05ms | 777 |
| fleet | 32 | 2976 | 0 | 28.2ms | 34.1ms | 1121 |
| bucket（单节点 FS） | 1 | 1000 | 0 | 0.24ms | 0.41ms | 3881 |
| bucket（单节点 FS） | 32 | 2976 | 0 | 3.8ms | 6.3ms | 8215 |

### fleet follower group commit（优化后，2 节点，数据在 `/opt`=vdb）

初版 follower **每段单独 fsync**（`Spool.Append`：建文件 + 文件 fsync + 目录 fsync，且全程持互斥锁）→ 吞吐被 fsync 串行化**封顶 ~1.1k RPS**（c=32/64 都 ~1.1k）。改为**每 `(scope,epoch)` 单 append-log `segments.log` + 一批一次 fsync**（`Spool.AppendBatch`，follower 侧复用 `internal/upload` 合批器）后：

| c | 优化前 p50 / RPS | 优化后 p50 / RPS |
|---|---|---|
| 1 | 1.26ms / 754 | 1.57ms / 590 |
| 8 | 7.52ms / 1,035 | 3.14ms / 2,252 |
| 32 | 28.2ms / 1,111 | 5.38ms / 5,026 |
| 64 | 57.5ms / 1,109 | 7.47ms / **7,685** |
| 128 | — | 11.6ms / **10,155** |
| 256 | — | 23.2ms / 10,445 |
| 512 | — | 45.7ms / **10,881** |

**结论**：**~1.1k → ~10.9k RPS（~10x）**，`failures=0`、`mode=fleet`；c≥128 后饱和——瓶颈从 fsync 转为 **owner↔follower 每笔一次 HTTP 往返**（非磁盘）。c=1 略升（合批窗口 + 换盘）属正常。

清理根盘（`/dev/vda2` 97%→83%）后，数据放 `/tmp`（vda2）复测：c=128 **12.3k**、c=256 **13.6k**、c=512 **13.8k RPS**。

### fleet 传输层 group commit（synthetic LTX，借鉴 celld，2 节点）

上一步的瓶颈是 owner **每笔一次 HTTP POST/响应**。借鉴 celld（`CELLD_LOG_TRANSPORT=stream` + `CELLD_LOG_PIPELINE=4` + `CELLD_LOG_GROUP_COMMIT_MS=1`）改为 **owner 侧帧化批量**：并发 commit 合并为**一帧多段**、**每个 follower set 一次 POST**（`ShipBatcher` → `/v1/peer/append_batch`），follower 侧 `Spool.AppendBatch` 一次 fsync。

| c | p50 | p99 | RPS |
|---|---|---|---|
| 1 | 1.17ms | 2.95ms | 790 |
| 64 | 2.76ms | 7.80ms | 21,181 |
| 128 | 3.79ms | 15.1ms | 29,567 |
| 256 | 5.79ms | 16.4ms | 38,170 |
| 512 | 10.5ms | 30.6ms | 41,866 |
| 1024 | 19.0ms | 65.0ms | **45,040** |
| 2048 | 39.5ms | 125ms | 43,917 |

**结论**：**~13.8k → ~45k RPS（再 3.3x；累计 ~1.1k → ~45k，~40x）**，`failures=0`、`mode=fleet`，c≥1024 饱和。此处只说明 **synthetic tiny-LTX 复制层**与同机 `bucket async`（~44.9k）持平；它不是 SQL TPS。c=1 的 1.17ms 仍是 follower fsync 地板。

### fleet 持久流（synthetic LTX；借鉴 celld HTTP 101，不是 SSE）

实现 `POST /v1/peer/stream`：HTTP 认证后返回 **101 Upgrade**，切换为自定义双向二进制协议；owner 持续写 batch frame，follower 每批 fsync 后回 1-byte ack。每 follower 建 **4 条 scope-hashed lane**（同 scope 恒在一 lane 保序），每 lane 最多 **4 批在途**；流失败自动回退 `/v1/peer/append_batch`。`Spool` 同时由全局锁改为 per-`(scope,epoch)` 目录锁。

同等总负载（总 c=1024、总 n=102,400，2 节点、`/tmp`）：

| 口径 | p50 | p99 | RPS | 相对 HTTP batch |
|---|---|---|---|---|
| HTTP batch 基线，单 scope | 19.0ms | 65.0ms | 45,040 | — |
| **101 stream，单 scope** | **18.4ms** | **62.7ms** | **47,082** | **+4.5%** |
| 101 stream，4 scope | 18.8–19.1ms | 58.5–60.9ms | 45,645 | +1.3% |
| 101 stream，8 scope | 19.6–21.2ms | **48.0–52.0ms** | 44,568 | -1.0% |

**结论**：持久流不是新的数量级提升；连接池下 HTTP batch 的协议成本已经很低。它主要把**多 scope p99 从 ~63–65ms 压到 ~48–52ms**，吞吐仍被本机 8 核 CPU + 单盘 fsync 组合封顶在 **~45–47k RPS**。数量级提升来自前两层 group commit（每段 fsync → follower 合批 → owner 帧化合批）；后续重点是 **hedge、跨主机同 AZ、真实 NVMe**，而非继续优化 loopback 建连。

### 真实 Go SQLite SQL → WAL → LTX → fleet → follower fsync

`cmd/sqlbench` 执行真实 hot-row UPSERT；数据写与 `cell_meta.txid` 在**同一 SQLite transaction**，WAL cursor 按 commit frame 保留事务边界，payload 为真实 page number + page bytes（`WAL1`），请求等待 covering LTX 的 fleet ack 后才成功。`wal_autocheckpoint=0`，避免测量中 WAL 被自动截断。

测试环境：2 个真实 `cell-agent`、HTTP 101 peer stream、follower 文件 fsync、`/tmp` ext4、64-byte value；每档 fresh DB/scope，failures=0。

| c / n | SQL commit p50 / p99 | gate wait p50 / p99 | 端到端 p50 / p99 | durable TPS | tx/batch |
|---|---|---|---|---|---|
| 1 / 500 | 0.23 / 0.67ms | 2.93 / 6.40ms | 3.16 / 6.81ms | 308 | 1.0 |
| 8 / 800 | 0.71 / 2.10ms | 4.84 / 9.28ms | 5.65 / 9.79ms | 1,399 | 6.9 |
| 32 / 3200 | 2.14 / 7.97ms | 10.9 / 24.4ms | 13.4 / 27.4ms | 2,280 | 27.4 |
| 64 / 6400 | 3.72 / 22.0ms | 18.2 / 77.7ms | 23.4 / 109ms | 2,537 | 52.5 |
| 128 / 12800 | 6.48 / 19.9ms | 29.0 / 86.9ms | 36.9 / 87.8ms | 3,314 | 105.8 |
| 256 / 25600 | 12.3 / 53.1ms | 55.0 / 158ms | 69.4 / 178ms | 3,321 | 208.1 |
| 512 / 51200 | 24.4 / 111ms | 107 / 394ms | 137 / 395ms | 3,451 | 412.9 |
| 1024 / 102400 | 47.6 / 220ms | 213 / 548ms | 270 / 639ms | **3,673** | 812.7 |

每个 SQL transaction 平均约 **2.0 WAL frames**。实际甜点约 c=64–128：**2.5–3.3k TPS、p50 23–37ms**；继续增并发只把 capture 合批放大（最高 812.7 tx/batch），吞吐缓慢升至 **3.67k TPS**，但 p50 恶化到 270ms。c≤128 的 SQL commit 明显小于 gate；高并发瓶颈转为 `Cursor.Poll` **每次全量读取不断增长的 WAL** + 大 WAL1 payload 的 JSON/base64 owner commit，而不是 peer stream 本身。

首轮曾测得峰值 917 TPS，原因是 `OpenAt` 未对齐生产 `Store.Open`，误用 SQLite `synchronous=FULL`；修为 `NORMAL` 后重测得到上表。c=128 首次还暴露 `/commit` JSON 固定 1MiB 截断（`unexpected EOF`），现仅对受鉴权 commit 端点放宽到 64MiB LTX 的 base64 上限，其他 JSON 端点仍为 1MiB。

**口径限制**：这是 **Go SQLite SQL→fleet benchmark**，包含 SQL commit、WAL 文件读取/page copy、LTX 编码、owner、101 stream、follower fsync 与 output gate；不包含 workerd/V8、actor 文件发现 supervisor、checkpoint snapshot/link reconciliation，因此不能称 workerd DO TPS。

### SQL fleet 性能优化（借鉴 celld 增量 capture / ticket）

优化项：

- `wal.Cursor` 长期持有 WAL fd，`ReadAt` 只读 header + 未消费 tail；不再每 poll 全量 `os.ReadFile`。
- 新增 raw LTX `POST /v1/internal/commit_binary`，去掉 JSON/base64；原 JSON endpoint 保持兼容。
- capture 限制 128 transactions / 1MiB WAL1 payload，采用 2ms group-commit wait。
- 对齐 celld `req_seq/synced_seq` 思路：`sqlcapture.Writer` 在 SQL commit 顺序内分配内存 durability ticket，LTX chain 持久化 txid；不再为了每次应用事务额外更新 `cell_meta` page。旧 `PutTx` 保留兼容。
- binary SQL LTX 已经是预合批段，owner 使用 `ShipNow`，避免重复进入 tiny-LTX 合批队列。

同一 2 节点环境、64-byte hot-row UPSERT：

| c | 优化前 TPS / p99 | 优化后 TPS / p99 | WAL bytes/tx | tx/batch |
|---|---|---|---|---|
| 32 | 2,280 / 27.4ms | **6,337 / 12.9ms** | 4,123 | 29.6 |
| 64 | 2,537 / 109ms | **7,973 / 18.8ms** | 4,121 | 39.4 |
| 128 | 3,314 / 87.8ms | **12,227 / 18.4ms** | 4,121 | 63.8 |
| 256 | 3,321 / 178ms | **15,782 / 28.2ms** | 4,120 | 81.1 |
| 512 | 3,451 / 395ms | 9,846 / 439ms | 4,120 | 90.1 |

**目标达成**：c=128 和 c=256 均超过 **10k TPS 且 p99 <100ms**；推荐工作区间 c=128–256。c=512 已过载，capture batch p99 升至 76ms、gate p99 434ms，不应作为稳态配置。

纯 SQL 对照：旧 `PutTx`（应用页 + txid meta 页）约 15.2k TPS；单页 raw UPSERT 约 54.5k TPS。移除 hot-path meta 页后 WAL 从约 **8.25KB/tx、2 frames/tx** 降到 **4.12KB/tx、1 frame/tx**，是超过 10k 的决定性改动。增量 WAL + binary commit 把此前 O(n²) IO/编码问题消除；`wal_bytes_read` 现稳定等于新增 WAL 量。

仍未实现 celld 的受控 checkpoint/truncate：当前 benchmark 关闭 autocheckpoint；必须等 snapshot/link apply 完整后才能安全引入。

### SQL capture page-map 去重（对齐 celld `WalReader::page_map`）

celld 的 capture 不是逐事务搬运全部 WAL page，而是在一个 capture range 内按 **page number 取最后一次写入**（`collect_wal_pages` → `HashMap<u32, i64>`），同一 hot page 只编码一次，payload 里放最终 page image。我们的 `WAL1` 之前保留每个事务的每个 frame，hot-row 场景下 4KiB 页被重复传输几十次。

新增 `WAL2` page-map payload（`ltx.PageMapFromTransactions` / `EncodeWALPageMap` / `DecodeWALPageMap`），capture 每个 chunk 先做 last-write-per-page 去重再编码；txid range 仍覆盖该 chunk 的应用事务数，barrier 语义不变。

| c | ADR-044 TPS / p99 | page-map TPS / p99 | pages/tx | LTX bytes |
|---|---|---|---|---|
| 32 | 6,337 / 12.9ms | **8,242 / 5.05ms** | 0.032 | 0.86MB |
| 64 | 7,973 / 18.8ms | **14,671 / 7.74ms** | 0.024 | 1.27MB |
| 128 | 12,227 / 18.4ms | **20,452 / 10.78ms** | 0.019 | 2.00MB |
| 256 | 15,782 / 28.2ms | **19,214 / 18.6ms** | 0.020 | 4.36MB |
| 512 | 9,846 / 439ms | **19,194 / 35.2ms** | 0.020 | 8.34MB |

**效果**：hot-row 下 `ltx_binary_bytes` 从约 105MB 降到约 2MB（**~50x**），c=128 从 12.2k 升到 **20.4k TPS**，p99 从 18.4ms 降到 **10.8ms**；相对 ADR-043 基线（3.3k / 87.8ms）为 **~6.2x TPS、~8x p99**。`pages_per_transaction ≈ 0.02` 说明约每 50 个事务只产生 1 个去重页。c=128 为甜点，c≥256 吞吐持平但延迟上升。

WAL 读取仍是 4120 B/tx（WAL 文件本身必须读），但发送/编码/fsync 的数据量降了约 50x；剩余成本为 SQL commit、本地 WAL 读取与 page 去重拷贝。

### 真实 workerd 复验（A1/A3/A4；goal 剩余项）

- **A1 checkpoint 对齐（I-03）**：workerd 2026-06-15 的 DO SQLite 默认 **1000 页 autocheckpoint**。持续写入下 `walscan` 观测到每 ~1000 写 **salt1 递增**、`total_frames` 重置为 1000、WAL 文件 **reuse 同尺寸 ~4,120,032B（非 TRUNCATE shrink）**。我们的 `wal.Cursor` 以 **salt 轮换**为 `Checkpoint=true` 信号并重定基 → 与 workerd 对齐；stock workerd 无法关闭 autocheckpoint，因此逐 actor 捕获每 ~1000 写产生一次快照边界（ADR-049 已处理）。
- **A3 `globalOutbound` 公网-only**：outbound `network.allow=["public"]` 时租户 `fetch 127.0.0.1:7001` 被 **`connect() blocked by restrictPeers()`** 拒绝（500）→ 隔离可强制、不改 workerd；生产应 `["public"]`，cell-agent 走 service binding/HMAC。
- **A4 兼容表**：workerd 2026-06-15 支持 `compatibilityDate` 上限 **2026-06-22**；未知 `compatibilityFlags` 硬报错 `No such compatibility flag: <x>`，已知 flag（`nodejs_compat`）通过。
- **A2 workerd alarm（◐）**：alarm 表为 `metadata.sqlite` 的 `_cf_ALARM(actor_id TEXT PRIMARY KEY, scheduled_time INTEGER) WITHOUT ROWID`，按 due 提取 SQL = `SELECT actor_id FROM _cf_ALARM WHERE scheduled_time <= ?`；探针未观测到持久化行（时序待查），回退：waker 询问 live supervisor。

### 真实 workerd DO 输出门 spike（ADR-051）

`cmd/cell-supervisor`（capture actor `-wal` → LTX → cell-agent，证明后响应）+ `workerd/spikes/p0/gate/`（DO 写后 `await env.GATE.fetch("/sync")`，即输出门），**不改 workerd**。真实 workerd 2026-06-15 + 双 cell-agent。用 `cmd/gatebench`（Go keep-alive 并发客户端，替代逐请求 `curl`）每级 `-d 4s`、warm 300：

**A. workerd DO + 输出门（`{"n":..,"proof":"fleet"}`）**

| c | p50 | p90 | p99 | req/s |
|---|---|---|---|---|
| 1 | 3.16ms | 4.31ms | 5.42ms | **301** |
| 4 | 8.64ms | 10.48ms | 12.67ms | 463 |
| 16 | 35.21ms | 39.69ms | 45.84ms | 469 |
| 64 | 132.74ms | 151.78ms | 160.01ms | **526** |

**B. 对照：workerd DO only（`workerd/spikes/p0/gate/plain.js`，无门）**

| c | p50 | p90 | p99 | req/s |
|---|---|---|---|---|
| 1 | 1.11ms | 1.29ms | 1.93ms | **860** |
| 4 | 3.34ms | 7.05ms | 15.22ms | **988** |
| 16 | 15.56ms | 17.62ms | 35.98ms | 873 |
| 64 | 71.66ms | 77.70ms | 115.95ms | 853 |

- `failures=0`，输出门请求全部含 `proof=fleet`。
- **结论**：单 actor 服务端上限——裸 workerd DO ≈ **860–990 req/s**；加输出门后 ≈ **500–530 req/s**（门在关键路径增加 ~2ms/req：supervisor WAL 捕获 + owner→follower fleet 证明）。因 workerd DO 单线程 + supervisor 每 actor 串行，**TPS 不随 c 增长而 p50 线性增长**（排队）；延迟敏感场景 c=1–4（p99 5–13ms）最佳，吞吐优先 c≥16。
- 早前逐请求 `curl` 的 280 req/s 与并发倒退（88/14）是**客户端进程模型**造成，已由本表修正。
- **数据准确性**：从 follower 复制链（502 段）恢复 actor DB → `t.n=520`（20 预热 + 500）与源一致，`integrity_check=ok`。
- **提升路径**：多 actor 分片（每 actor 独立 supervisor/scope）可近线性叠加；生产宜把 capture 放进 cell-agent 进程内或用持久流（ADR-047）。

### 多 DO、非 DO、cell-agent SQLite 接口（补充测量）

**A. 多 DO（单 workerd，`GET /aN` 路由到 N 个 actor，`cmd/gatebench` c=32×N，5s）**

| N actor | 输出门 TPS (p50) | 裸 DO 对照 TPS (p50) |
|---|---|---|
| 1 | 553 (60.1ms) | 855 (35.5ms) |
| 2 | 738 (83.2ms) | 849 (71.7ms) |
| 4 | 752 (157.6ms) | 811 (153.0ms) |
| 8 | 790 (294.7ms) | 794 (312.0ms) |

- **裸 workerd DO 在 1→8 个 actor 间完全不平移（~800–855）**：瓶颈是 **单个 workerd 进程的提交路径（每写一次 fsync/落盘）**，不是 actor 串行、也不是输出门或 cell-agent。
- 输出门在 N=1 多花 ~35%，但 N≥4 后与裸 DO 收敛到同一 ~800 墙——门开销被 workerd 上限掩盖。
- **结论：多 DO 在同一 workerd 内不扩展**；要扩展需更多 workerd 进程，或降低每请求 fsync（一次请求批量多写/显式事务）。ADR-051 的"多 actor 分片近线性"假设被实测否定（至少单进程内）。

**A2. 为什么单 workerd 卡在 ~850——不是 fsync"每条写"，而是"每个请求"**

stock workerd 对 **一个 DO 事件（请求）内所有 `sql.exec` 写做自动原子合并**（一次提交/一次 fsync）。实测（`sep` = k 次独立 `exec`，c=8）：

| 每请求写数 k | req/s | writes/s |
|---|---|---|
| 1 | 928 | 928 |
| 4 | 970 | 3,880 |
| 16 | 897 | 14,352 |
| 64 | 820 | 52,480 |
| 256 | 611 | **156,416** |

- `req/s` 几乎恒定（**~900 = 每秒提交数 = fsync 上限**），`writes/s` ∝ k。多语句 `exec("a;b;c")` 等价（k=256 达 196k writes/s）。
- **`sql.exec("BEGIN")` 被 stock workerd 拒绝**（"use state.storage.transaction()/transactionSync()"）；原子性用 `transactionSync`。
- 所以降低 fsync 的操作手段（不改 workerd）：**把更多逻辑写塞进同一个请求/事件**，而不是改 PRAGMA。我们的输出门基准是"1 写/请求"→ 被 ~900 提交/s 封顶；客户端侧批量化即可按 k 倍提升。

**A3. 批量化 + 输出门（框架层组提交验证）**

`workerd/spikes/p0/gate/batch.js`：一个请求内做 k 次写（workerd 合并为 1 次提交），然后**对该批只做一次门证明**。c=8，4s：

| 每请求写数 k | 门 req/s | 门 writes/s | 无门 req/s | 无门 writes/s |
|---|---|---|---|---|
| 1 | 531 | 531 | 924 | 924 |
| 16 | 460 | 7,360 | 880 | 14,080 |
| 64 | 466 | 29,824 | 793 | 50,752 |
| 256 | 430 | **110,080** | 591 | 151,296 |

- **门的成本被批量摊销**：一次门证明覆盖整批，writes/s 随 k 线性上升；k=256 达 **110k writes/s**（对比"1 写/请求"的 531 → **~207×**）。
- **数据准确性**：gated 100 请求 × k=4 = 400 写 → 源计数增 400；从 follower 复制链（7021 段）恢复 → `t.n` **完全一致**、`integrity_check=ok`。**批量化不破坏 RPO=0 门语义**。
- 结论：把"每请求写数"提上去即可绕开 ~900 提交/s 墙——这是**请求边界/API 形态**问题，不是 SQL 写法问题；框架层组提交（一次门覆盖一批）可行且已验证。

**A4. 平台侧组提交 wrapper（`workerd/wrapper/groupcommit.js`）**

把上面的机制做成可复用模块：租户只声明 `apply(op)`，wrapper 缓冲并发 `submit(op)`，一个事件内应用整批 + 一次门证明。1 op/请求、gated、`windowMs=3`（`workerd/spikes/p0/gate/gc.js`）：

| c | req/s | p50 | p99 |
|---|---|---|---|
| 1 | 160 | 6.18ms | 7.89ms |
| 8 | 936 | 8.44ms | 15.71ms |
| 32 | 2,501 | 12.07ms | 41.07ms |
| 64 | 3,196 | 18.69ms | 83.12ms |
| 128 | **3,637** | 32.30ms | 106.12ms |

- 同一负载不用 wrapper 是 ~530 req/s → **~6.9×**；`/stats`：`avg_batch≈23`、`maxObserved=128`、`commits==batches`。
- 数据准确性：200 次顺序 gated op → 从 follower 链（2500 段）恢复 `t.n` 精确一致、integrity ok。
- 权衡：c=1 时单请求也要等 `windowMs`（160 < 531）→ 生产用**自适应窗口**；每批一次门 RTT 在关键路径。见 `workerd/wrapper/README.md`。

**B. 非 DO（`cmd/sqlbench`，Go SQLite → LTX → fleet，c=128，n=30000）**

| 模式 | TPS | p50 | p99 | 组提交 |
|---|---|---|---|---|
| `-durable=true`（完整 fleet 证明） | **28,939** | 4.31ms | 9.51ms | 84.5 tx/批 |
| `-durable=false`（纯本地 SQL） | **43,693** | 2.81ms | 4.89ms | — |

同一 durability 路径，非 DO 的 Go 侧因**组提交 + 流水线**做到 ~29k TPS，是 workerd 单进程 ~850 的 **34×**。

**C. cell-agent 自身 SQLite 接口（HTTP KV，`cellstore`）**

初测（**优化前**，`Store.Open` 每请求 `sql.Open`+`migrate`(4 DDL)+`Close`，无缓存）：PUT c=32 2,978 rps、PUT c=1 274 rps、GET c=32 6,291 rps。

**优化（ADR-052）**：`Store.Cell` 按 scope 缓存长生命周期 `*Cell`。复测：

| 操作 | c | before | after | 提升 |
|---|---|---|---|---|
| PUT | 1 | 274 (2.86→3.53ms) | **3,050** (0.30ms) | **11×** |
| PUT | 32 | 2,978 (p99 106ms) | **5,442** (p50 4.09ms, p99 26ms) | 1.8× |
| PUT | 128 | — | 5,224 (16.9ms) | — |
| GET | 32 | 6,291 (p99 37ms) | **12,864** (p50 1.41ms, p99 15ms) | 2.0× |
| GET | 128 | — | 11,808 (8.3ms) | — |

- 结论：瓶颈确为**每请求 open/migrate/close**，非 SQLite 写入；缓存后 PUT 上限 ~5.2k、GET ~12k rps，p99 大幅下降。（见 ADR-052）

### SQL 写路径与网络开销（ADR-046）

**prepared statement**：`sqlcapture.Writer` 之前每次 `ExecContext` 都用 SQL 字符串，SQLite 每次重新 prepare。改为 `Writer` 持有一个 `*sql.Stmt`、在事务内用 `tx.StmtContext` 复用。隔离 benchmark：`BenchmarkRawUpsert` 16.2µs（~62k TPS）→ `BenchmarkRawUpsertPrepared` **9.4µs（~107k TPS）**。端到端 loopback c=128：20.4k → **24.8k TPS**，p99 10.8ms → 9.3ms。

**网络延迟注入**：新增 `CELLHIVE_PEER_LATENCY_MS`（单向），`peer.LatencyTransport` 在每次 replication 的请求与 ack 各注入该延迟（总 RTT ≈ 2×）。用于在单机模拟同 AZ / 跨区网络，无需两台物理机。

2 节点、c=128、hot-row UPSERT（failures=0）：

| one-way 延迟 | RTT≈ | SQL commit p50 | gate wait p50 | 端到端 p50 / p99 | TPS |
|---|---|---|---|---|---|
| 0（loopback） | ~0 | 1.72ms | 3.14ms | **4.92 / 9.30ms** | **24,785** |
| 1ms | 2ms | 1.34ms | 6.75ms | 8.27 / 13.0ms | 14,769 |
| 5ms | 10ms | 1.44ms | 24.2ms | 25.7 / 31.2ms | 5,617 |
| 20ms | 40ms | 1.47ms | 84.7ms | 86.1 / 96.6ms | 1,799 |

**结论**：`PEER_LATENCY=0` 时网络不是瓶颈（gate 3.1ms）；延迟升高后 **gate 完全被 RTT 主导**，因为当前每个 hot scope 只有 **1 条 lane、1 个批次在途**——每批必须等一个完整 RTT。这与 celld 的 `CELLD_LOG_PIPELINE`（每 lane 最多 8 帧在途）是最后一处未对齐的差异。下一步若面向跨 AZ/跨区，应实现**单 lane 内多批在途（pipelining）**，预计在高 RTT 下接近 K×。当前 RPO=0 语义与顺序不变。

### fleet pipelining（ADR-047）与实测边界

`ShipBatcher` 改为「每 shard 一个累加器 + 一个有序发送器」，发送器最多 **K 批在途**；`rawStream` 增加 `sendAsync`：帧在 `writeMu` 下**同步写出**（保序），ack 通过 future 异步回收。`StreamTransport`/`LatencyTransport` 实现 `AppendBatchAsync`，`CELLHIVE_PEER_PIPELINE` 控制 K（默认 4）。因此同一 lane 上多批在途时，帧仍按 dispatch 顺序到达 follower（单读者按序 apply），只有 ack 等待被流水线化。

synthetic LTX 路径（走 `Ship` 合批器）实测，c=128、fail=0：

| one-way | pipeline=1 | pipeline=8 |
|---|---|---|
| 0（loopback） | 2,105 rps / p50 59.7ms | 2,226 rps / p50 56.8ms |
| 5ms（RTT 10ms） | 153 rps / p50 833ms | **334 rps / p50 384ms（2.2x）** |

**但 SQL→fleet 单 scope 路径没有改善**（5ms：5,773 → 5,759；20ms：1,798 → 1,749）。原因是结构性的：SQL capture 走 `ShipNow`（预合批段直接同步提交），**capture loop 一次只允许 1 个 LTX 在途**——它在 `Commit` 上等完整 ack 才进行下一次 poll。瓶颈在 capture→owner 这一跳，不在 owner→follower 的合批器。

**下一步（明确设计）**：要让 SQL 路径也吃到 K×，需要 **capture 级流水线**：
1. `sqlcapture` 改为 `CommitAsync`：按 txid 顺序**连续发出**多个 chunk，不逐个等 ack；
2. owner 侧对 `(scope)` 做**有序交接**（reorder/handoff by `start_txid`）：只写「下一个期望 txid」，其余缓冲，帧写完后立即释放交接并**在锁外等 ack**；
3. barrier 只在**连续** ack 到达时推进，保证 RPO=0 与顺序。

风险点是这一跳位于 durability 关键路径，需要先补 reorder 单测与 kill 恢复验证，不能蛮改。

**已验证的安全杠杆：加大 capture chunk 上限**。把 `maxCaptureTransactions` 从 128 提到 **512**（WAL2 page-map 去重后 hot-page chunk 仍很小，1MiB payload 上限仍在）。在单批在途的限制下吞吐 ≈ chunk/RTT，因此该参数直接决定高 RTT 上限：

| one-way | RTT≈ | c | 端到端 p50 / p99 | TPS | tx/batch |
|---|---|---|---|---|---|
| 0（loopback） | ~0 | 128 | 4.8 / 9.0ms | 25,833 | 71 |
| 0 | ~0 | 512 | 17.9 / 25.8ms | **28,532** | 77 |
| 1ms | 2ms | 512 | 17.2 / 29.2ms | **29,314** | 164 |
| 5ms | 10ms | 512 | 29.1 / 33.5ms | 18,114 | 267 |
| 5ms | 10ms | 1024 | 43.3 / 68.6ms | 23,994 | 368 |
| 20ms | 40ms | 512 | 90.3 / 92.9ms | 5,873 | 267 |
| 20ms | 40ms | 1024 | 133 / 142ms | **8,576** | 388 |

全部 `failures=0`。相对 chunk=128：20ms/c=1024 从 2,765 → **8,576 TPS（3.1x）**；loopback c=512 从 19,194 → 28,532（1.49x）。代价是单 chunk 覆盖更多事务、延迟上升，因此应结合目标 RTT 选择 chunk 上限。

### capture 级有序 pipelining（ADR-048）

上面仍有一处结构限制：capture 走 `ShipNow`（同步），**一次只允许 1 个 LTX 在途**，所以高 RTT 下吞吐 ≈ chunk/RTT。本次实现 celld 式 capture 流水线：

1. `sqlcapture` 改为 `CommitAsync`：按 txid 顺序连续发出多个 chunk，不逐个等 ack；
2. owner 新增 `POST /v1/internal/commit_binary?pipelined=1&base=<txid>`：`orderedDispatcher` 按 `start_txid` 做**有序交接**——只写「下一个期望 txid」的帧，其余缓冲；帧写完后**在锁外等 ack**，因此写序严格、ack 等待重叠；
3. capture 的 barrier 只在**连续** ack 到达时推进（`drain`），乱序 ack 不越过未证明的 chunk，RPO=0 不变；
4. `LatencyTransport.AppendBatchAsync` 改为「立即写帧、只在 ack 上注入 RTT」，否则 dispatcher 持锁调用会把发送串行化；
5. capture 采用**粘性自适应**：先串行，首个提交观测到 ≥8ms 才一次性切到 pipeline=4 并用该 chunk 的 `start_txid` 作基线的 `base`——本机 loopback 保持零开销快路径。

2 节点、hot-row UPSERT、`failures=0`：

| one-way | c | 串行 TPS | **pipelined TPS** | 变化 |
|---|---|---|---|---|
| 0（loopback） | 128 | 25,833 | **26,409** | 持平 |
| 0 | 512 | 28,532 | **28,922** | 持平 |
| 5ms（RTT 10ms） | 1024 | 23,994 | **29,289** | **+22%** |
| 20ms（RTT 40ms） | 1024 | 8,576 | **13,287** | **+55%** |

**正确性测试**：`TestDrainAdvancesOnlyContiguousAcks`（乱序 ack 不越过 head）、`TestDrainPropagatesChunkError`、`TestOrderedDispatcherDispatchesInTxidOrder`（乱序到达仍按 txid 写帧）、`TestOrderedDispatcherFailsScopeOnGapTimeout`（缺口超时而非永久阻塞）、`TestCommitBinaryPipelinedFleet`（端到端）。全量 `go test ./...` 与关键包 `-race` 通过。

**修复过程中发现的两个真实缺陷**：
- owner 曾用「第一个到达的 chunk」初始化排序基线，并发下较小 txid 被永久缓冲 → 改为显式 `base`；
- capture 的 `poll` 在「无新事务」时提前返回、跳过 `drain`，导致已到达的 ack 被搁置 → 死锁；改为 poll 开头先 `drain`。

### snapshot + 恢复 apply + 受控 checkpoint（ADR-049）

- **快照**：`cellstore.SnapshotPages` 在 checkpoint 回填后**直接读 DB 文件**得到全页镜像。**不能用 `VACUUM INTO`**（会重排页号，使后续 WAL delta 套错页 → 恢复出 malformed DB，已实测踩坑）。
- **capture**：`Capture.Snapshot(ctx)`（静默期调用）先 `CheckpointTruncate` 再发全页 `KindSnapshot`（复用 WAL2 page-map），快照后 `cursor.Reset()`、`nextTxID=watermark`、barrier 推进。检测到外部 checkpoint 时 `poll` 也会发快照。
- **apply**：`internal/restore.ApplyFile` 取「最后 snapshot + 其后 delta（按 txid）」逐页写真实 SQLite + fsync，要求 snapshot 覆盖全部页，写前清 `-wal/-shm`。
- **checkpoint**：`CheckpointTruncate` 在调用方静默时执行（短 busy_timeout=500ms 防卡顿），不再长期持有读锁。

**数据准确性校验（端到端）**：2 节点、`sqlbench -unique` 写 5,000 个唯一 key（value=`%064d`）→ `fail=0`、源库 `integrity=ok`；从 **follower 的复制链**（108 段）用 `cmd/restoreverify` 恢复 → **5000/5000 行、0 缺失、0 错值、`integrity_check=ok`**，与源库一致。✅

**测试**：`TestApplyFileSnapshotRoundTrip`、`TestApplyFileAppliesDelta`、`TestCaptureEmitsSnapshotOnCheckpoint`、`TestCaptureChainRestores`、`TestContinuousWritesWithCheckpointRestore`（静默期显式 checkpoint），全绿含 `-race`。

**未完成（诚实边界）**：**连续写入下的自动截断撤回**。曾用「暂停 writer + TRUNCATE + 水位」，但在 `database/sql` 连接池（长期读锁占 1 连接）与 writer 锁的交互下发生连接饥饿死锁（`Put` 持 `w.mu` 等连接，读锁占另一连接）。已去掉长期读锁（改为调用方静默期显式 checkpoint），但**完整的连续写入安全截断**仍需 celld 式接管时序（长读事务 pin WAL → PASSIVE 回填 → 确认 capture 消费到 WAL 末端 → 释放 → TRUNCATE → 重获）与更谨慎的连接管理；在此之前 WAL 仍随写入增长，需运维在静默期调用 `Snapshot`/`Checkpoint`。

### 连续写入下安全 WAL checkpoint（ADR-050）

补齐了最后一块：capture 在 WAL 超阈值且无在途 chunk 时自动截断——`writer.WithPaused` 暂停写者 → 短 `busy_timeout` 的 `wal_checkpoint(TRUNCATE)` → 暂停内读取 committed 水位 → 恢复写者 → 读（截断后稳定的）DB 文件发全页快照 → `cursor.Reset()`、`nextTxID/watermark` 推进。暂停后新提交进新 WAL，成为 txid>watermark 的 delta，**没有任何 ticket 在持久证明前被释放**。

- **连接/锁修复**：`MaxOpenConns=3`，且**不再长期持 SQLite 读锁**（早期在 `Capture.New` 长期 `AcquireReadLock` 导致 `Put` 持 `w.mu` 等连接、读锁占另一连接 → 连接饥饿死锁）。
- **数据准确性**：持续写入 + 512KiB 自动 checkpoint 写 5000 唯一 key → `fail=0`；从 follower 复制链（154 段）恢复 **5000/5000 行、0 缺失、0 错值、integrity_check=ok**。
- **性能**：loopback c=128 无 ckpt **29,602 TPS / p50 4.19ms / p99 9.06ms**；ckpt=4MiB 22,237 TPS；c=512 ckpt=4MiB 26,196；20ms 单向 c=1024 ckpt=4MiB **16,274 TPS / p99 98.7ms**。checkpoint 的代价是写者短暂停顿，阈值越大越少。

### 真实 S3（MinIO loopback）

| 姿态 | c | n | failures | p50 | p99 | rps |
|---|---|---|---|---|---|---|
| bucket（每次 commit 等 S3 PUT） | 1 | 200 | 0 | 2.53ms | 7.96ms | 357 |
| bucket | 16 | 800 | 0 | 22.2ms | 27.4ms | 719 |
| fleet（ack=follower fsync，S3 异步） | 1 | 1000 | 0 | 2.13ms | 3.61ms | 438 |
| fleet | 32 | 2976 | 0 | 32.0ms | 39.6ms | 988 |

**读法**：S3 仅 loopback，故 bucket c=1 ~2.5ms；**云 S3 同区预计 ~90ms、跨区 ~600ms**，与本页不同。fleet 的 ack 由 follower fsync 决定，S3 上传在后台（group commit，`put_per_ack≈0.09`）。

### 对象存储供应商实测（`cmd/s3probe`）

| 供应商 | 条件创建 | reject-create | CAS 更新 | reject-stale | ranged read | 判定 |
|---|---|---|---|---|---|---|
| MinIO（本地 docker，RELEASE.2025-04-22） | ✅ | ✅ | ✅ | ✅ | ✅ | **合格** |
| **Bunny Storage（LA，`la-s3.storage.bunnycdn.com`）** | ✅ | ✅ | ✅ | ✅ | ✅ | **合格**（实测支持条件写，官方文档未列） |

**结论**：**Bunny Storage（LA zone `vwork-la`）实测四项条件写 + ranged read 全部通过 → 合格**；**MinIO 合格**。后续真实云测试统一使用 **Bunny LA**。

### 真实云对象存储（Bunny Storage，洛杉矶，跨太平洋）

cell-agent 用 `CELLHIVE_BUCKET=s3://vwork-la` + `https://la-s3.storage.bunnycdn.com`（region `la`）启动：**`storage diagnose ok`**（真实云上条件创建/CAS/ranged read 通过；启动耗时 11–45s，跨太平洋波动大）。

| 姿态 | c | n | failures | p50 | p99 | mean | rps |
|---|---|---|---|---|---|---|---|
| **fleet（owner 缓存后；ack=follower fsync，S3 异步）** | 1 | 200 | 0 | **1.26ms** | 3.19ms | 2.16ms | 459 |
| **fleet** | 8 | 400 | 0 | 2.52ms | 383.6ms | 10.7ms | 570 |
| bucket（单节点，等云 S3 PUT；首轮） | 1 | 20 | 0 | 186.7ms | 10542ms | 2391ms | 0.42 |
| bucket（单节点，等云 S3 PUT；后轮） | 1 | 13 | **2** | **8259ms** | 15982ms | 8534ms | 0.08 |

**读法/发现**：
1. **fleet 已达标**：加 owner/epoch 缓存（`owner.ResolveCached`，1s TTL）后，远程 S3 下 fleet c=1 p50 **1.26ms**（此前 ~1s）——ack 由 follower fsync 决定，**S3 完全不在 ack 路径**。✅
2. ⚠️ **bucket 姿态不可用**：单节点等云 S3 PUT，p50 从 187ms 到 **8.3s** 剧烈波动，p99 5–16s，且出现 **failures=2**（超时）→ 远程/跨洋对象存储**不能放在写 ack 路径**，印证设计（fleet 把 S3 放后台）。
3. Bunny LA **支持条件写**（实测四项全过）。
4. 注意：**若同桶存在其他节点 lease，"bucket" 会被 `liveFollowers` 自动发现成 fleet**；测纯 bucket 需单节点/独立桶。
5. 尾延迟：启动 diagnose 11–45s、bucket 偶发超时 → 跨洋对象存储**尾延迟不可靠**。

### 单节点 commit 模式（`CELLHIVE_DURABILITY` / `CELLHIVE_BUCKET_WAIT`）

单节点无 follower，证明退化为对象存储。配置两轴：**`CELLHIVE_DURABILITY`**（`auto`|`fleet`|`bucket`，选证明方式）+ **`CELLHIVE_BUCKET_WAIT`**（bucket 姿态下是否等上传）。

**两种 bucket 子模式都以"块"提交**（同一 `(scope,epoch)` 的一批写攒成一个对象，块名 txid 区间、有序）；区别是 ack 时机：

- `batch`（默认，`BUCKET_WAIT=true` 或 `DURABILITY=fleet` 降级）：攒块，**等块提交完成**再 ack（RPO=0；单并发即等价逐笔 PUT）。
- `async`（`BUCKET_WAIT=false`）：**入队即 ack**，块在后台**串行异步提交**（RPO>0）。

> `fleet` 是 RPO=0 要求：无 peer 时**退化为等 bucket**（并告警），不静默 ack；`fleet` + `BUCKET_WAIT=false` 启动即拒绝。`CELLHIVE_COMMIT_MODE=async` 为兼容别名。已移除独立的 `sync` 模式。

**本机单节点，并发阶梯（每档 n=c×100，fail=0）**：

FS bucket（c=64，交错 3 轮）：

| c=64 | batch p50 / p99 / RPS | async p50 / p99 / RPS |
|---|---|---|
| 轮 1 | 1.90ms / 8.41ms / 26,726 | **0.88ms** / 9.75ms / **37,253** |
| 轮 2 | 1.57ms / 12.68ms / 29,250 | **0.91ms** / 8.93ms / **38,527** |
| 轮 3 | 1.66ms / 9.76ms / 29,761 | **0.89ms** / 7.64ms / **40,842** |
| 中位 | 1.66ms / 9.76ms / ~29.2k | **0.89ms** / 8.93ms / **~38.5k** |

MinIO（loopback :9110，c=64，交错 4 轮；**本机噪声很大**）：

| c=64 | batch RPS（4 轮） | 中位 | async RPS（4 轮） | 中位 |
|---|---|---|---|---|
| RPS | 33,718 / 13,839 / 14,408 / 14,677 | ~14.6k | 20,553 / 15,734 / 15,624 / 15,168 | **~15.7k** |
| p50 | 1.53 / 3.93 / 3.83 / 3.72 ms | 3.78ms | 2.36 / 3.31 / 3.09 / 3.27 ms | **3.19ms** |

> 单档 n=c×100 的早期 c=1/8/32 数据方向一致（async 不劣势），但因噪声未逐档复跑。

**Bunny LA（跨洋），c=8，n=32**：

| 模式 | p50 | p99 | RPS | failures |
|---|---|---|---|---|
| async | 0.42ms | 1408ms | 22.7 | 0 |
| batch | 5376ms | 24145ms | 0.19 | **15** |

**结论**：
- **`async` 一般 ≥ `batch`**（ack 不等上传）：FS 上 async 稳定快 **~1.3x**、p50 **约一半**（0.89 vs 1.66ms）。
- **MinIO loopback 上两者基本打平**，且**本机噪声极大**——同一 `batch` 配置单轮 RPS 在 **11.7k–33.7k** 间跳动（±3x）。因此**单轮排名不可信**；此前记录的"`batch` 在 c≥8 反超 `async`（30.2k vs 22.8k）"是**噪声离群**，已更正。
- `batch` 的价值是 **RPO=0 + 天然背压**（门控限制在途写数，避免高并发下 CPU 过订阅），**不是比 `async` 快**。故**默认 `batch`**（要持久性），追求吞吐且可容忍丢窗口时用 `async`。
- p50 极低（0.1–1.7ms）是**合批摊薄**的结果（一次 PUT 覆盖多笔 ack），不代表单次 PUT 延迟。
- **跨洋桶**：`batch`（RPO=0）不可用（尾延迟数秒、有超时）；只有 `async`（RPO>0）能低 p50，但上传队列积压。
- 压测注意：块对象 **create-once + txid 区间命名**，同一 `(scope,epoch)` 复用 txid 区间会命中同名对象而 412（本次排查即此因）；每档并发需用独立 scope。

## 结论摘要

| 项 | 结果 | 判定 |
|---|---|---|
| WAL 捕获 + checkpoint 检测 | 通过（`internal/wal`） | ✅（替身写入者；真实 workerd 待 P0.7） |
| LTX 编解码 / 复制 / epoch restore / batch 切分 | 通过 | ✅ |
| 双节点 fleet：quorum-1 fsync ack | 通过 | ✅ |
| 单节点 bucket 降级 | 通过（0 failures） | ✅ |
| node-log + recovery（已 ack 未上传 → 从 follower 收齐 → 落 bucket） | 通过（`internal/recovery`） | ✅ RPO=0（模拟） |
| 热路径禁止 List | ✅（`list` 仅在 5s 缓存样本刷新时 +1） | ✅ |
| **每 ack PUT 数 ≪ 1（group commit）** | ✅ **fleet `put_per_ack ≈ 0.09`** | ✅（batch 上传器，见 `internal/upload`） |
| 真实 S3 条件写 / ranged read / presign | ✅ 已验证（MinIO + **Bunny LA**） | `s3probe` / `s3init` + 启动 `diagnose ok` |
| 真实云对象存储延迟（Bunny LA，跨太平洋） | ✅ 已测 | fleet（S3 后台）p50 1.26ms；bucket（S3 在 ack 路径）p50 0.19–8.3s、偶发超时 |
| owner/epoch 缓存（消除每笔 S3 GET） | ✅ 已实现并验证 | `owner.ResolveCached`；远程 S3 下 fleet 从 ~1s → 1.26ms |
| workerd 真实 actor WAL 可观察性 | ✅ 已验证（P0.7） | `walscan` 解析真实 `-wal` |

## 基准数据（failures=0）

### c=1

| 姿态 | n | p50 | p99 | mean | rps | PUT/ack |
|---|---|---|---|---|---|---|
| fleet | 500 | 1.03ms | 1.66ms | 1.07ms | 927 | **0.11** |
| bucket | 500 | 0.074ms | 0.168ms | 0.078ms | 11759 | 1.00 |

### c=32

| 姿态 | n | p50 | p99 | mean | rps | PUT/ack |
|---|---|---|---|---|---|---|
| fleet | 2976 | 28.6ms | 35.5ms | 28.6ms | 1112 | **0.09** |
| bucket | 2976 | 3.03ms | 5.86ms | 2.93ms | 10275 | 1.00 |

其他：restore 300 段 2.6ms / 500 段 5.2ms；`list` 全程 2 次（缓存样本）；`conditional_create` 3–4（2 次 claim + node-log）。

## 发现与调整项

1. **fleet ack = follower fsync**（设计如此）。初版 follower **每段 2 次 fsync**（文件 + 目录）→ c=32/64 吞吐封顶 **~1.1k RPS**。已改为**单 append-log + 批量一次 fsync**（见上），**~10.9k RPS（~10x）**；c≥128 饱和于 HTTP 往返。✅ 仍待真实 NVMe/跨主机复测。
2. **bucket 姿态在 FS 上不具代表性**（p50 0.07–3ms，无网络、无 fsync）；**真实 S3 约 90ms**，必须用真实 S3/R2 复测。
3. **group commit 已实现**（`internal/upload` 批量上传 + 有界队列溢出时同步回退），fleet `put_per_ack` 从 ~1 降到 **~0.09**。✅
4. **热路径 List 已消除**：节点样本按 5s 缓存（`lease.SampleCached`）。
5. **node-log 每会话一次**：修复 per-commit 条件写浪费（`conditional_create` 1489 → 3–4）。
6. bucket 姿态为 **1 PUT/ack**（设计：等 bucket 证明）；不属于 group commit 缺口。

## 未达/待办

- **真实 S3/R2**：延迟、条件写、presigned URL（P0.6/P0.7）。
- **workerd 集成**（P0.7）：见下。

## P0.7 workerd 验证（已完成）

环境：本地 workerd `2026-06-15`（`@cloudflare/workerd-linux-64`），最小配置 `workerd/spikes/p0/config.capnp`，示例 DO `workerd/spikes/p0/worker.js`。

结论：
- 真实 workerd 跑通**原生 DO + SQLite + localDisk**；`curl` 连续自增 `{"n":0/1/2}`。
- actor 落盘：`do/p0-counter/<hash>.sqlite`(+`-wal`/`-shm`) + **共享 `metadata.sqlite`**（多文件布局，M-04 确认）。
- `cmd/walscan` 只读解析真实 `-wal` 成功：actor `page_size=4096, total_frames=5, commits=4`；metadata `total_frames=2, commits=1`。
- ✅ **workerd actor SQLite 为 WAL 模式，且可被外部进程只读打开**——P0.2 的两个核心假设在真实 workerd 上成立。
- ✅ **`workerLoader` 动态加载**：`workerd/spikes/p0/loader.capnp` + `workerd/spikes/p0/host.js` 用 `env.LOADER.get()` 加载租户模块（`modules` 为 record、`mainModule` 为 `*.js`），完成真实 **KV put/get 往返**到 cell-agent（`put:200`、`get:hello-world`）。
- 待补：`globalOutbound` **公网-only 隔离**强制；真实 workerd 的 checkpoint 过程对齐。

_最后更新：2026-09-14_
