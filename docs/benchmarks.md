# 基准回归（benchmarks）

本页记录可复现的基准命令与结果，用于回归对照。**非生产硬件**；对象存储分 **FS（本地文件）** 与 **S3（本地 MinIO）** 两档。部分历史直连基准不含真实 workerd（DO 相关数字见 `docs/archive/p0-report.md`）；**标注 stock workerd 的结果是切换到 cellhive-workerd fork 前的历史基线**，当前默认镜像使用 fork，数字不可直接跨版本比较。原始输出归档在 `docs/archive/bench/`。

历史 MinIO 基准只证明当时的条件写与数据路径性能，不证明当前权威桶所需的条件删除契约；固定版 MinIO 的 stale `DeleteObject If-Match` 会被忽略，现行 `cellhive diagnose` 因此拒绝启动。`kvbench` 数字直连 cell-agent，不包含 user-runtime/workerd、资源删除栅栏或真实 COS 往返（见 `testing.md`、ADR-235）。

## 环境

- Go：`/usr/local/go`（`go version` 见运行输出）。
- 对象存储：FS bucket（`-bucket-dir`）或本地 MinIO（`quay.io/minio/minio:RELEASE.2025-02-18T16-25-55Z`，`make s3-test`）。
- cell-agent：本机单进程，`CELLHIVE_DURABILITY=auto` + `BucketWait=true`（bucket 等待，RPO=0）；FS 档 `realbench` 为 bucket 模式。
- 机器：单机 loopback（无真实跨主机 RTT）。

## 命令

```bash
make build
# 自包含（进程内 owner+follower + FS bucket）
./bin/cellbench -n 3000 -c 16

# 起 cell-agent（FS bucket），再跑 HTTP 路径基准
CELLHIVE_NODE_ID=bench CELLHIVE_BUCKET_DIR=/tmp/bench/bucket ./bin/cell-agent &
./bin/realbench -owner http://127.0.0.1:7001 -n 1000 -c 1
./bin/sqlbench  -owner http://127.0.0.1:7001 -n 1000 -c 8

# S3 档：MinIO 起桶后以 s3:// 后端跑同一基准（见 make s3-test 起桶方式）
CELLHIVE_BUCKET=s3://cellhive AWS_ENDPOINT_URL=http://127.0.0.1:9000 \
  AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin CELLHIVE_S3_PATH_STYLE=true ./bin/cell-agent &
./bin/sqlbench -owner http://127.0.0.1:7001 -n 500 -c 8
```

> `kvbench` 需 scoped token（`-scope-token`）；`gatebench` 需 workerd + do-supervisor 输出门 URL（见 `docs/archive/p0-report.md` 的 P0 门基准）。

## 结果

### cellbench（进程内 owner+follower + FS bucket）

| 并发 | fleet p50 | fleet p99 | fleet rps | put/ack |
|---|---|---|---|---|
| c=1 | 0.93 ms | 2.22 ms | 979 | ≈0.02 |
| c=16 | 13.84 ms | 16.80 ms | 1141 | ≈0.08 |
| c=64 | 49.45 ms | 60.10 ms | 1268 | — |

（fleet 模式：等 follower fsync；`restore` 200 段 ≈ 1.9 ms；failures=0。）

### realbench → cell-agent（commit 往返，bucket 模式）

| 后端 | 并发 | p50 | p99 | rps | failures |
|---|---|---|---|---|---|
| FS | c=1 | 0.22 ms | 0.33 ms | 3930 | 0 |
| FS | c=64 | 1.31 ms | 15.44 ms | **30807** | 0 |
| S3 / MinIO | c=1 | 2.00 ms | 2.73 ms | 476 | 0 |
| S3 / MinIO | c=64 | 6.59 ms | 39.75 ms | **7821** | 0 |

### sqlbench → cell-agent（SQL→WAL→LTX→bucket 端到端，`db_integrity=ok`）

**吞吐随并发线性上升**（延迟受限；TPS ≈ 并发 / E2E 延迟）：

| 后端 | 并发 | TPS | E2E p50 | E2E p99 | tx/batch |
|---|---|---|---|---|---|
| FS | c=8 | 2758 | 2.87 ms | 3.57 ms | 7.9 |
| FS | c=32 | 10841 | 2.88 ms | 3.79 ms | 32.0 |
| **FS** | **c=128** | **30749** | 3.72 ms | 7.07 ms | 61.5 |
| S3 / MinIO | c=8 | 1558 | 5.06 ms | 6.23 ms | 7.8 |
| S3 / MinIO | c=32 | 1256 | 5.57 ms | 13.95 ms | 32.0 |
| S3 / MinIO | c=64 | 10915 | 5.53 ms | 10.53 ms | 54.5 |

（S3 的 E2E 比 FS 高 ~2 ms，源于每批 bucket 提交等待；`failures=0`、`integrity=ok`。）

### backend-A 捕获开销（kvbench → cell-agent，单机 FS bucket，ADR-092）

`CELLHIVE_CAPTURE=off` 与默认（捕获开，`CELLHIVE_DURABILITY=bucket`，RPO=0）对比；`kvbench -d 15s`，`fail=0`。原始输出：`docs/archive/bench/backend-a-capture.txt`。

| 配置 | 路径 | 并发 | rps | p50 | p99 |
|---|---|---|---|---|---|
| capture ON（RPO=0） | KV put | c=1 | 335 | 2.76 ms | 3.88 ms |
| capture ON（RPO=0） | KV put | c=4 | 1157 | 3.55 ms | 6.31 ms |
| capture ON（RPO=0） | D1 insert | c=1 | 330 | 2.79 ms | 4.00 ms |
| capture ON（RPO=0） | D1 insert | c=4 | 1141 | 3.60 ms | 6.11 ms |
| capture OFF（本地） | KV put | c=4 | 5542 | 0.34 ms | 8.77 ms |
| capture OFF（本地） | D1 insert | c=4 | 5061 | 0.37 ms | 8.74 ms |

- **捕获开销是延迟，不是天花板**：c=1 时 capture ON 341 rps vs OFF 3144 rps（p50 2.7ms vs 0.31ms）；加并发后两者到达**同一写上限 ~5.5–6k rps/单 cell**。c=4 的 1.1k 只是延迟受限（rps≈c/latency），**不是上限**。
- **单 cell 写上限 ~5.5–6k rps**：KV put 峰值 c=32 5500 rps、D1 5158 rps（capture ON）；capture OFF 峰值 c=8 6136 rps。**读**可达 ~30k rps（c=128）。peak 时 cell-agent CPU 写 ~2.1/8 核、读 ~4.7/8 核 → **非 CPU/线程瓶颈**，而是单 cell SQLite 单写者（每写一个事务+提交）串行。
- **多 cell 只在本地写（capture OFF）叠加**：4 个独立 cell 并行（各 c=8，capture OFF）聚合 **13.9k rps**；但 capture ON（RPO=0）时多 cell **不叠加**——瓶颈是对象存储 sink（见下节）。
- `CELLHIVE_BUCKET_WAIT=false`（bucket-async）下捕获 committer **拒绝** ack（`commit was not durable (mode "bucket-async")`）——设计上不提供 RPO>0 的捕获 ack。
- 冷恢复/一致性（非基准）：1000 acked KV 写 → `kill -9` owner → 空盘新节点接管 → 读回 1000/1000（p50 0.27 ms）。

### 单 cell vs 双 cell（capture ON，LTX→bucket，RPO=0）

脚本：`scripts/bench-capture-cells.sh`（capture ON、`CELLHIVE_DURABILITY=bucket`、`kvbench` 单进程按 ns round-robin；双 cell = 两个独立 namespace/cell，相同总并发）。原始输出：`docs/archive/bench/single-vs-dual-capture-sharded.txt`（FS）、`single-vs-dual-capture-s3.txt`（S3/本地 MinIO）。

**FS bucket（LTX 已验证落桶 + 可恢复）**

| cell 数 | c | rps | p50 | p99 |
|---|---|---|---|---|
| 1 | 1 | 342 | 2.73 ms | 3.88 ms |
| 1 | 16 | 4612 | 2.96 ms | 11.31 ms |
| 1 | 32 | 5395 | 4.81 ms | 23.49 ms |
| 1 | 64 | 5393 | 10.27 ms | 33.37 ms |
| 2 | 1 | 323 | 2.93 ms | 4.30 ms |
| 2 | 16 | 4979 | 2.80 ms | 12.18 ms |
| 2 | 32 | **7955** | 3.35 ms | 20.64 ms |
| 2 | 64 | 6578 | 7.77 ms | 35.44 ms |
| 2 | 128 | 6509 | 15.64 ms | 72.20 ms |

**S3 / 本地 MinIO**

| cell 数 | c | rps | p50 |
|---|---|---|---|
| 1 | 64 | 2711 | 22.15 ms |
| 1 | 128 | 2398 | 46.35 ms |
| 2 | 64 | 2217 | 25.47 ms |
| 2 | 128 | 2439 | 49.40 ms |

结论：
- **双 cell 不是近线性**：FS 上峰值 1.47x（c=32），更高并发反降；S3/MinIO 上双 cell **无增益**（对象存储已是瓶颈）。
- **原因是对象存储 sink 串行/同盘**：本地 FS bucket（`internal/bucket/fsbucket.go`）用**单个全局 mutex** 包住 `Put/CAS/List`（+`os.WriteFile`），所有 cell 的提交串行；MinIO 单节点也在同一块盘上。上传层已分片（`upload.NewSharded`，默认 NumCPU 上限 8、按 scope 哈希、保序），但 sink 本身并行度不足时分片无益。
- **LTX 落桶已验证**：FS 运行后 `restoreverify -bucket -scope ... -epoch 1` 输出 `integrity:"ok"`、keys 计数正确（单 cell 1000 / 双 cell 各 500）。
- **要真正多 cell 扩展**需：真实分布式对象存储（不同硬件）+ 降低提交频率 + sink 并行。单机同盘下多 cell 收益有限。
### 写路径优化（ADR-093，单机 FS bucket）

**批量 KV 写已移除**：`env.KV` 无批量 API，该场景不存在（见 ADR-093 注记）。

**binding 缓存 A/B（capture OFF，单 cell）**：cache off 6156/5985/5722 rps vs cache 1000ms 6045/5911/5458（c=8/32/128）→ **无增益**（binding 查询不是瓶颈），保留但不宣称收益。

**SQLite 裸基线**：autocommit UPSERT 12.4µs(~80k/s)、cellstore `PutTx` 58.8µs(~17k/s)、批 100/txn 10.2µs/key(~98k/s)。

原始输出：`docs/archive/bench/optimization-2026.txt`。


### 单键写优化（ADR-094，单机 FS bucket）

**capture OFF，单 cell 单键 put**：加 `Cell.writeMu`（Go 侧串行化写事务，避免 SQLITE_BUSY 重试）后：

| 并发 | 之前 rps | 之后 rps | 之后 p50 |
|---|---|---|---|
| c=8 | 6156 | 7481 | 1.23 ms |
| c=16 | 5561 | **9637** | 1.61 ms |
| c=32 | 5985 | 8679 | 3.28 ms |
| c=128 | 5722 | 9334 | 13.39 ms |

两客户端各 c=16：之前 1980+1934=3914（塌），之后 4563+4561=**9124**（稳）。

**capture ON（RPO=0）单 cell 单键 put**：c=16 4616、c=32 5052、c=128 4512；两客户端 4681 → **桶证明是上限**。

**group-commit 窗口生效**（ticker 跟随窗口，c=128）：窗口 2/5/10/20ms → 段内事务 **8.6/21.3/41.2/89.3**，吞吐 ~5–6k 持平 → 合并不是瓶颈。

原始输出：`docs/archive/bench/optimization-single-key.txt`。


### 两节点 fleet（同机双进程，ADR-094 语境）

`scripts/bench-fleet.sh`：两个 cell-agent 进程共享同一 bucket，`CELLHIVE_DURABILITY=auto` → 有 peer 即走 **fleet（peer fsync）**，桶上传异步。原始：`docs/archive/bench/fleet-2node.txt`。

| 并发 | rps | p50 | 对照：单节点 bucket（同 cell） |
|---|---|---|---|
| c=1 | 241 | 4.44 ms | 322 / 2.79 ms |
| c=16 | 3524 | 4.26 ms | 4616 |
| c=32 | 4215 | 7.45 ms | 5052 |

- **fleet 路径已确认**：follower 的 `peer-spool/<ns>/__kv__/default/e1/segments.log` 有数据，桶里有 727 个异步 `.batch` 对象。
- **但同机 fleet 不更快**（甚至略慢）：两个进程抢同一块盘/CPU，peer fsync 与桶写是同一块盘，loopback 的桶本来就快。"fleet 显著快于单节点桶"只在**跨主机/跨网络**场景成立；要复现需**第二台主机**（当前环境缺，C 类 blocker）。
- 结论：fleet 是为**远桶/高 RTT** 设计的；本地/近桶时单节点桶已够快。

### 写成本分解与 txid 镜像（ADR-095）

| 层 | 每写 | 上限 |
|---|---|---|
| 裸 autocommit UPSERT | 12.3µs | ~81k |
| 显式事务 + UPSERT（无 txid） | 16.4µs | ~61k |
| cellstore `PutTx`（txid 记账）改前 | 57.6µs | ~17k |
| cellstore `PutTx` 改后（txid 内存镜像） | **40.4µs** | ~25k |
| 进程内 handler（binding 缓存）改前/改后 | 91 / **74.6µs** | ~13k |

真实 HTTP 单 cell 单键 put（capture OFF）：c=32 **8679 → 9784 rps**（+13%），c=16 ~9.8k。capture ON 仍 ~4.6k（桶证明限制）。原始：`docs/archive/bench/write-cost-decomposition.txt`。


### KV / D1 读写（单 cell，单机 8 核，FS bucket，ADR-095/098 后）

`kvbench -mode put|get|d1|d1query -ns capA -d 8s`（`fail=0`）。原始：`docs/archive/bench/kv-d1-readwrite.txt`。

| capture | 操作 | c=16 | c=64 | c=128 | p50(c=64) |
|---|---|---|---|---|---|
| OFF | KV put | 11173 | 12149 | 11713 | 5.11 ms |
| OFF | KV get | 25667 | 29529 | 30317 | 1.71 ms |
| OFF | D1 insert | 10763 | 11929 | 10935 | 5.24 ms |
| OFF | D1 query | 24502 | 28373 | 29225 | 1.79 ms |
| ON (RPO=0) | KV put | 5099 | 9475 | 11308 | 5.84 ms |
| ON (RPO=0) | KV get | 25355 | 30011 | 30471 | 1.68 ms |
| ON (RPO=0) | D1 insert | 4992 | 8563 | 7746 | 6.88 ms |
| ON (RPO=0) | D1 query | 21193 | 26006 | 26652 | 2.02 ms |

- **读 ~30k/s（KV get / D1 query）**，与 capture 开关基本无关（读不付耐久证明）；KV 读 ON/OFF 持平，D1 query ON 略低（≈26.6k，**因为 D1 query 也走了 `capturedWrite`**）。
- **写 ~12k/s（OFF）/~11k/s（KV, ON）**；D1 写 ON ~8.5k（有 p99 尾延迟）。相比 ADR-094 时代的 ~5k，writeMu + txid 内存镜像 + binding 缓存把单键写翻了一倍多。
- **D1 只读已跳过捕获（ADR-100）**：SELECT/EXPLAIN/VALUES 不再走 `capturedWrite`（单测 `TestD1ReadSkipsCapture` 证明不再触发 Ensure/owner 检查），PRAGMA/WITH 可能写，仍捕获。**复测 D1 query（capture ON）≈26.0k→26.5k，无可测提升**——说明 ON/OFF 的那点差距主要来自捕获/上传的**环境争用**（CPU/磁盘），不是每请求的证明开销。改动仍保留（少一次 owner 解析、语义更干净）。


### KV / D1 读写复测（ADR-159 CGo 驱动 + ADR-160 LZ4 WAL3，同一方法）

**方法完全相同**（单 cell `capA`、单机 8 核、FS bucket、`kvbench -mode put|get|d1|d1query -c 1/16/64/128 -d 8s`，`fail=0`）；唯一变量是代码：SQLite 由 `modernc`（纯 Go）换成 **`mattn/go-sqlite3`（CGo，ADR-159）**，L1/page-map 变为 **LZ4 压缩的 `WAL3` + `CID2` 索引（ADR-160）**。原始：`docs/archive/bench/kv-d1-readwrite-cgo.txt`。

| capture | 操作 | c=16（旧→新） | c=64（旧→新） | c=128（旧→新） | p50(c=64) 旧→新 |
|---|---|---|---|---|---|
| OFF | KV put | 11173 → **12368** (+11%) | 12149 → **15733** (+29%) | 11713 → **13011** (+11%) | 5.11 → **3.98 ms** |
| OFF | KV get | 25667 → **29770** (+16%) | 29529 → **36394** (+23%) | 30317 → **38436** (+27%) | 1.71 → **1.40 ms** |
| OFF | D1 insert | 10763 → **12237** (+14%) | 11929 → **12738** (+7%) | 10935 → **13440** (+23%) | 5.24 → **4.06 ms** |
| OFF | D1 query | 24502 → **27641** (+13%) | 28373 → **33435** (+18%) | 29225 → **34487** (+18%) | 1.79 → **1.53 ms** |
| ON (RPO=0) | KV put | 5099 → 4623 (−9%) | 9475 → **9336** (−1%) | 11308 → 11173 (−1%) | 5.84 → **6.15 ms** |
| ON (RPO=0) | KV get | 25355 → **31314** (+23%) | 30011 → **38098** (+27%) | 30471 → **38696** (+27%) | 1.68 → **1.34 ms** |
| ON (RPO=0) | D1 insert | 4992 → 3739 (−25%) | 8563 → 8171 (−5%) | 7746 → **9302** (+20%) | 6.88 → **6.87 ms** |
| ON (RPO=0) | D1 query | 21193 → **27215** (+28%) | 26006 → **32958** (+27%) | 26652 → **34413** (+29%) | 2.02 → **1.55 ms** |

- **读（KV get / D1 query）普遍 +13% ~ +29%**，p50(c=64) 从 1.7ms/1.8ms 降到 1.4ms/1.5ms → CGo 驱动（真实 C SQLite）在读路径更快；capture 开关对读基本无影响（读不付耐久证明）。
- **本地写（capture OFF）**：KV put +11~29%、D1 insert +7~23%，p50(c=64) 5.1→4.0ms / 5.2→4.1ms。
- **capture ON（RPO=0）写 ≈ 持平**（±10% 波动）：这条路径的上限是**桶证明**（每次 ack 等对象存储），不是 SQLite/驱动；ADR-160 的压缩只作用于 snapshot/L1（delta 仍是 `WAL1` 未压缩），所以对逐笔写吞吐无直接影响。
- 全程 `fail=0`；本基准的 cell 常驻本地，**不涉及 paged VFS**（压缩收益体现在桶字节数与冷启动，见 ADR-160 章节）。

### 单 vs 双 cell 复测（ADR-159/160，同一脚本/参数）

脚本与参数同 `scripts/bench-capture-cells.sh`（`CONCS="16 32 64 128" DUR=10s MODE=put BUCKET_MODE=fs`）。原始：`docs/archive/bench/single-vs-dual-capture-cgo-on-repeats.txt`（ON，两次重复，compression off/on 各一次）、`single-vs-dual-capture-cgo-off.txt`（OFF）、以及第一次 ON 采样 `single-vs-dual-capture-cgo-on.txt`（**慢态**样本，见下）。

**capture OFF（本地）**

| cell | c=16 | c=32 | c=64 | c=128 |
|---|---|---|---|---|
| 1 | 12409 | 15251 | 15531 | 15442 |
| 2 | **16103** | **20038** | **24339** | **24828** |

旧（2026-09-16）单 cell 11.1–12.2k、双 cell 14.6–19.2k → 本次单 cell +10~27%、双 cell +10~38%（CGo 驱动 + 噪声），双/单 ≈ **1.6x**。

**capture ON（durability=bucket，RPO=0）** — 取两次重复（compression off / on，同一 box 状态）

| cell | c=16 | c=32 | c=64 | c=128 |
|---|---|---|---|---|
| 1 | 5161–5189 | 9760–10031 | 12336–12338 | 13278–13295 |
| 2 | 4760–4908 | 8503–8602 | 15310–15445 | 16322–16773 |

对照旧值（单 5161/10044/10685/10636、双 4884/9841/14045/16612）：c≥64 **持平到显著更好**（双 cell c=128 +0~+29%），双/单 ≈ **1.55x**（与旧的 1.55–1.57x 一致）。第一次 ON 采样（单 cell c=32 8283、双 c=64 12002）与压缩 A/B 的 off 组同区间，但相邻重复（同一状态）回到 9760–10031 / 15310–15445 → 判定为**共享机器上的 run 间噪声（±10~20%）**，非代码回归。

### S3 / MinIO 档（ADR-159/160）

原始：`docs/archive/bench/kv-d1-readwrite-s3-cgo.txt`、`single-vs-dual-capture-s3-cgo.txt`。

**KV/D1（单 cell，capture ON = RPO=0 over MinIO；capture OFF = 写不落桶）**

| capture | 操作 | c=1 | c=16 | c=64 | c=128 |
|---|---|---|---|---|---|
| OFF | KV put | 4030 | 12394 | **16015** | 14324 |
| OFF | KV get | 5318 | 29395 | 35830 | **38136** |
| OFF | D1 insert | 3862 | 12334 | 15410 | 13554 |
| OFF | D1 query | 4836 | 27350 | 33156 | **33947** |
| ON | KV put | 146 | 1877 | 3383 | **3781** |
| ON | KV get | 4639 | 30968 | 33899 | **37738** |
| ON | D1 insert | 140 | 1592 | 2915 | **3636** |
| ON | D1 query | 4448 | 26931 | 31579 | **33585** |

- **capture OFF 与 FS 档几乎一致**（写路径不逐笔落桶；桶只在 compaction/GC 时用）→ 说明 ADR-159 的驱动收益在两种后端都拿到。
- **capture ON 的写 ≈ FS 的 1/4**（每笔 ack 等一次 MinIO 往返，p50 8–32ms）；**读与 FS 相同**（读不付耐久证明）。这正是"写上限=桶证明"的直接证据。

**单 vs 双 cell（capture ON，`CONCS="1 16 32 64 128" DUR=10s`）**：单 144/1892/2716/3439/3812、双 146/1836/2764/3434/3871 rps；旧（2026-09-16）单 148/1423/1811/2711/2398、双 140/1229/1773/2217/2439 → **c≥16 提升 +33~59%**，p50(c=128) 46.35→32.08 ms（−31%）；双 cell 在单节点 MinIO 上仍**无增益**（对象存储是瓶颈）。

### LZ4 压缩开关 A/B（ADR-161）

`CELLHIVE_LTX_COMPRESSION=off`（`WAL2` 固定帧）vs `on`（`WAL3` + LZ4），capture ON，FS bucket。

| 场景 | c=16 | c=32 | c=64 | c=128 |
|---|---|---|---|---|
| 单 cell，off（rep1/rep2） | 5165/5062 | 10062/10024 | 12447/12300 | — |
| 单 cell，on（rep1/rep2） | 5161/5065 | 10020/10064 | 12347/12356 | — |
| 双 cell，off | 4760 | 8503 | 15445 | 16773 |
| 双 cell，on | 4908 | 8602 | 15310 | 16322 |

**结论**：同一 box 状态下 on≈off（**1–3% 以内**，p50 也相同）→ LZ4 编码**不在写 ack 关键路径**、对吞吐无可测代价；而收益是桶对象 **3.7× 更小**（见 ADR-160）。因此默认保持压缩，`CELLHIVE_LTX_COMPRESSION=false` 只是 CPU 受限/要确定性时的逃生门，不是性能默认。

### 单 vs 双 cell（当前代码，含 writeMu/txid 镜像/D1 只读跳过捕获；单机 FS bucket）

`scripts/bench-capture-cells.sh`（`kvbench` 单进程按 ns round-robin；双 cell = 两个独立 namespace）。原始：`docs/archive/bench/cells2-on.txt`、`cells2-off.txt`。

**capture OFF（本地）**

| cell | c=16 | c=32 | c=64 | c=128 |
|---|---|---|---|---|
| 1 | 11134 | 12233 | 12063 | 11808 |
| 2 | 14628 | 18525 | 17610 | **19165** |

**capture ON（durability=bucket，RPO=0，同步等桶）**

| cell | c=16 | c=32 | c=64 | c=128 |
|---|---|---|---|---|
| 1 | 5161 | 10044 | 10685 | 10636 |
| 2 | 4884 | 9841 | 14045 | **16612** |

- 双 cell 相对单 cell ≈ **1.55–1.57x**（两侧峰值），优于早前的 1.44–1.47x；仍**不是 2x**，因为同机共享 CPU/磁盘与一个桶（capture ON 还叠加 FS 桶的全局锁）。
- 单 cell 绝对值已提升：OFF ~12.2k、ON ~10.6k（早前 ON ~5k）。LTX 落桶 + 恢复校验（`integrity ok`，keys 1000/500）通过。


### LTX 碎片与自动合并

`capture` 每个合并批产生一个 LTX 对象；上层 `upload` 的 `AppendBatch` 会把多个段**打包进一个对象**（`.batch`），`compaction` 再把 L0 链 fold 成 L1。

| 配置 | 2 万写后的对象数 | 说明 |
|---|---|---|
| 关闭 compaction | **1336** 个 L0（0 L1） | ~15 写/对象（窗口 2ms、c=64） |
| 开启 compaction（默认 30s / ≥64 段或 64MB） | **3** 个（0 L0 + 3 L1） | `L1/{manifest.json,<maxTx>.snapshot,index.bin}` |

- **有自动合并**：cell-agent `runCompactionLoop` 周期扫描 **owned** scope，`>=MinSegments(64)` 或 `>=MinBytes(64MB)` 时 fold L0→L1 + GC 旧对象（fold 前校验 owner/epoch）；恢复只读 L1 + 更新的 L0（有界）。原始：`docs/archive/bench/ltx-fragmentation.txt`。
- 碎片峰值 ≈ 两次 compaction 之间的对象数 ≈ **PUT 速率 × compaction 周期**。远端桶建议 `CELLHIVE_COMPACTION_INTERVAL=5–10s`、并配合窗口/分片控制 PUT 速率。


### 已试过、**无收益、已回退**的微优化

- `Store.Path` 去掉每请求 `os.MkdirAll`（路径 memoize + 仅在打开时 mkdir）。
- KV `{"ok":true}` 用预置字节写，省一次 marshal。
- 结果：进程内 handler 74.6→71.3µs（−4%，httptest 噪声内），真实 HTTP 单 cell 无变化（put c=64 11853→10974，get ~30k）。→ **已回退**，不保留。结论：HTTP/鉴权/binding 那 ~34µs 是**分散**的（HMAC、query 解析、分配、net/http、loopback），小改拿不动。

### 快速路径 A/B 延迟（真 workerd，ADR-106）

stub 在 router/service 跳上注入 3ms；worker 内 `performance.now()` 计时（8 次取 p50）：

| 调用 | 快路径 OFF | 快路径 ON | 节省 |
|---|---|---|---|
| DO invoke 首调（经 router） | 4.00 ms | 4.00 ms | —（首次学习） |
| DO invoke 稳态（直连 owner） | 3.00 ms | **0.00 ms** | ~3 ms |
| service RPC（原生同实例） | 4.00 ms | **0.00 ms** | ~4 ms |

loopback + 注入延迟，验证"跳被移除"的机制；生产节省 = 真实往返（跨主机为 RTT，需第二台主机）。

### user-runtime prepared-worker 热路径 A/B（真 workerd，2026-09-24）

固定 stock workerd `1.20260916.1`；同一个 Go HTTP control/KV stub；public loader → workerLoader → tenant isolate 全链路；`/get` 还经过 KV WorkerEntrypoint → `PLATFORM.fetch`。每条路径预热 100 次，再以 c=64 跑 12s。基线只替换为优化前镜像中的 `loader.js`，其余代码/依赖相同。

| 路径 | 基线 RPS | prepared-worker + 未采样快路径 | 变化 | 基线 p50 | 优化后 p50 | 失败（基线→优化） |
|---|---:|---:|---:|---:|---:|---:|
| `/ping`（无 binding） | 2,204 | **4,077** | **+85.0%** | 23.90 ms | **2.83 ms** | 12→0 |
| `/get`（KV read） | 1,593 | **2,288** | **+43.6%** | 34.99 ms | **24.09 ms** | 0→0 |

实现按 `namespace/worker@version-bundle_sha` 缓存纯数据 load plan（bundle、binding spec、模块模板和预算校验），并 single-flight 首次准备。workerd capability 是 request-scoped：`LOADER.get()`、`OUTBOUND`、binding/tail stub 与 `getEntrypoint()` **不能跨请求缓存**，否则真实 workerd 报 `Cannot perform I/O on behalf of a different request`；这些仍在当前请求获取。版本号变化即得到新 plan，同 bundle 的 vars/bindings-only 发布不会复用旧 env。未采样请求保留 ADR-146 traceparent 传播，但跳过 span 构造/flush。

限制：优化后 `/ping` p99 为 523 ms，存在尾部尖峰；4,077 RPS 仍低于近期 5k 目标。下一步应 profile loader→tenant 调度与连接生命周期，不能把这次结果解释为已完成 worker 性能优化。

### KV 自然并发读取合批（真 workerd，ADR-191，2026-09-24）

同一探针与 c=64，预热 100 次后每路径跑 8s、连续 2 轮。`/get` 做 1 次 `KV.get`；`/batch` 在租户 isolate 内 `Promise.all` 3 次 `KV.get`。后端 stub 按 batch 请求中的 key 数返回同序结果。

| 轮次 | `/get` RPS / p50 / p99 | `/batch` RPS / p50 / p99 | `/batch` 每租户请求的后端请求数 |
|---|---|---|---:|
| 1 | 2,008 / 27.75ms / 113.18ms | 1,912 / 28.37ms / 165.09ms | **1** |
| 2 | 1,784 / 29.59ms / 155.71ms | 1,610 / 32.32ms / 162.79ms | **1** |

run 1 的累计 `kv_requests=46,443`，精确等于 `/local`、`/get`、`/batch` 的成功租户请求数（14,777+16,066+15,300）加三条路径各 100 次预热；所以 `/batch` 内 3 次读取确实只产生 1 次 cell-agent HTTP 请求，而不是靠响应时间推断。三读 p50 仅比单读高 0.63–2.73ms，避免了三次独立 JSRPC/HTTP 成本。该优化不解决 loader 调度尾部：同次 run 2 的 `/ping` p99 仍为 466ms。

### native Tail 捕获开关 A/B（真 workerd，ADR-192，2026-09-24）

固定 stock workerd `1.20260916.1`、同一 public loader → workerLoader → tenant ping、c=64、预热 200 次、每轮 8s。默认 `CELLHIVE_TENANT_LOG_CAPTURE=on` 三轮为 **4,311 / 4,625 / 4,549 RPS**；`off` 的最终实现三轮为 **4,728 / 5,244 / 4,816 RPS**，配对提升约 **5.9%–13.4%**。关闭只省 native Tail 事件采集/派发，租户请求、bindings、trace 与 header 隔离不变，但平台不再捕获该 runtime 的租户 `console.*` 与未捕获异常。更早一次 5.7k–6.6k 结果受宿主调度噪声影响，不作为承诺。

同时验证并回退了三个无效/不安全方向：`WorkerStub` 没有直接 `fetch()`，必须使用 `getEntrypoint()`；无 binding env 快路径降到 3.9k–4.1k RPS；完全跳过 Request/header/trace 重建虽有约 5%–10% 上界收益，但会破坏任意 `x-cellhive-*` 清理与 trace 传播。

### user-runtime CPU / 阶段 / 副本 proof（真 workerd，2026-09-24）

固定 stock workerd `1.20260916.1`、外部压测器、Tail off、trace ratio 0、简单 `pong` Worker、c=64。CFS quota 1C 三轮中位 **4,827 RPS**，无 quota 的 cpuset 单逻辑 CPU 为 **5,021 RPS**，无 quota 四逻辑 CPU 为 **5,034 RPS**；quota 每个 8s 窗口仅 throttled 0.26–0.27 CPU 秒，且无 quota 时 p99 仍为 109–124ms。故 CFS quota 只解释约 3%–4% 损失，100ms 级尾延迟不是 quota 根因；单 runtime 加逻辑 CPU 也没有纵向收益。

一次性阶段截点在 c=64 下得到 route/env/plan/stub **10.7k–11.4k RPS**、完成 `prepareRequest` 后 **7.3k RPS**、完整 tenant fetch **4.6k RPS**。完整 no-copy 诊断为 5.5k–5.8k RPS，但破坏任意 `x-cellhive-*` 清理与 ADR-146 traceparent 传播，不可采用。保留语义的单 Request clone 与低分配 header scan 分别降至中位 3.7k RPS，均已回退；真实 workerd 还证明入站 Request headers immutable，不能原地修改。

固定 CPU 4–7（本机仅 **2 个物理核 / 4 个 SMT 逻辑 CPU**）、12s、三轮：

| 布局 | RPS（三轮） | 中位 RPS | p99 中位 | failures |
|---|---:|---:|---:|---:|
| 1 runtime × 4 logical CPU | 4,455 / 4,469 / 4,391 | **4,455** | **113.9ms** | 0 |
| 2 runtimes × 2 logical CPU | 5,796 / 5,893 / 7,382 | **5,893** | **86.7ms** | 0 |
| 4 runtimes × 1 logical CPU | 9,042 / 8,707 / 7,258 | **8,707** | **100.6ms** | 0 |

四副本相对单副本中位吞吐 **1.95×**，接近两个物理核的容量上限，证明正确方向是 runtime 多副本而非单进程加核。原定 `4×1C ≥18k RPS、p99 ≤50ms` 需要至少四个无 SMT 争用的物理核环境验证；本机拓扑不具备该验收条件，不能宣称达标。

## 结论

- **基线一致性**：FS `c=128` sqlbench **30.7k TPS**，与历史 durable ~28.9k 一致（历史上万级是**高并发**下的稳态，不是单请求延迟）。**并发低则 TPS 低**（c=8 → ~2.8k），属延迟受限的正常缩放。
- **FS → S3 的差**主要落在 **bucket 提交等待**（realbench p50 0.22→2.0 ms；sqlbench E2E p50 2.9→5.1 ms），即 RPO=0 的代价；本机 MinIO 仍非生产云对象存储尾延迟。
- **写合并有效**：sqlbench `transactions_per_batch ≈ 7.4–7.9`，`put_per_ack` 远小于 1；capture 批内合并显著摊薄提交。
- **数据准确**：所有档 `db_integrity=ok`、`failures=0`。
- **workerd/DO**：本页已重跑 user-runtime（prepared plan、Tail、CPU/阶段/副本）基准；历史 DO 进程级上限与门控数字（≈800 req/s 裸 DO、k 写合并提升）仍见 `docs/archive/p0-report.md`，本轮未重跑 DO。
- **局限**：单机 loopback、无跨主机 RTT；MinIO 本地；未压真实磁盘/云对象存储。跨主机/云数字需环境（C 类 blocker）。

### 可观测性开销（ADR-178，`-benchmem`）

命令：`go test <tags> -run '^$' -bench 'BenchmarkLogIngest|BenchmarkTraceIDs|BenchmarkExportLogOff' -benchmem ./internal/server/ ./internal/telemetry/`（本机 8 核）。

| benchmark | ns/op | allocs/op |
|---|---|---|
| `BenchmarkTraceIDs`（W3C traceparent → trace/span id） | **50** | **0** |
| `BenchmarkExportLogOff`（OTLP 日志关闭时，每行） | **6.4** | **0** |
| `BenchmarkLogIngestPlain`（2 行/请求，无 traceparent） | 6138 | 41 |
| `BenchmarkLogIngestTraceparent`（2 行/请求，带 traceparent） | 6363 | 42 |

含义：**默认（`CELLHIVE_OTLP_ENDPOINT` 空）零开销**——trace 中间件在 `!telemetry.Enabled()` 时直接放行（`internal/server/server.go:333`），`ExportLog` 先查 mode 再早退；打开后新增的每行成本约 **0.1 µs**，且只在**异步日志路径**（非请求热路径）。trace 导出默认头部采样 `0.01`。

## 本机双 agent + 双 runtime 端到端标准（2026-10-20）

运行 `CELLHIVE_WORKERD_BIN=<pinned-workerd> CELLHIVE_ESBUILD_BIN=<esbuild> scripts/bench-runtime-dual-local.sh`。脚本使用独立临时文件桶、2 个 cell-agent、2 个 user-runtime 和轮询入口；先由 agent1 写 KV A、agent2 写 KV B，再检查两个数据库分属不同节点。`/ping`（仅返回 pong）、`/fast`（分配 64 KiB）、KV `/read` 和 `/write` 默认各运行 30 秒，非 200 响应计失败；`CELLHIVE_BENCH_SPECS` 可选择路径和并发。结果与宿主负载、workerd 版本一起记录；本机共享主机上的两轮读吞吐为 752.7/933.9 RPS，不能视为容量上限或性能修复的 A/B 结论。原始结果见 `docs/archive/bench/resource-fence-2026-10-20.md`。

纯 workerd 对照使用 `CELLHIVE_WORKERD_BIN=<pinned-workerd> scripts/bench-workerd-bare.sh`，以 `wrk` 直接访问 workerd HTTP socket，不启动任何 Go 服务。相同 `/ping` Worker 的单实例为 13993.77 RPS，双实例合计 26640.23 RPS；`/fast` 单实例 11392.98 RPS、双实例合计 17917.72 RPS。原始结果、连接数和尾延迟见 `docs/archive/bench/bare-workerd-2026-10-20.md`。这不是完整 CellHive 路径的同压测器 A/B，不能直接把吞吐比当作某个组件的开销。

公共 fetch 移除入口侧第二层响应流包装后，同机双 agent/单 runtime 的 `/ping`（`wrk -t4 -c64 -d20s`，直连 workerd）在关闭 Tail 时从 1638.64 增至 2234.42 RPS，开启本地 Tail ring 时从 1483.97 增至 1807.71 RPS。均为共享宿主诊断，不是稳定容量结论；流式版本切换与公开 WebSocket 已用固定版 workerd 验证。原始输出及方法见 `docs/archive/bench/tail-batching-2026-10-20.md`。

### user-runtime 小响应缓冲 A/B（ADR-237，2026-10-20）

固定 stock workerd `1.20260916.1`，本机单 cell-agent + 单 user-runtime（public `:18281`），Worker 带一个 KV binding；`wrk -t4 -c64 -d25s -H 'Host: bench.cellhive.test'` 直连 workerd public socket，每轮重启 runtime 并预热 5s，base/优化版交替（另含反向顺序控制漂移）。共享宿主诊断，只能作同会话配对比较，不是容量承诺。

| 路径 | 基线 RPS（中位） | 小响应缓冲后 RPS（中位） | 变化 | p50 | p99 |
|---|---:|---:|---:|---|---|
| `/ping`（`text/plain`，4B） | 1,944（5 轮 1787–2087） | **2,341**（1958–2577） | **+20%**（5/5 轮更快，+3.5%–26%） | 4.7ms → 3.4ms | 1.0–1.7s，两侧持平 |
| `/json`（`application/json`，146B） | 1,815（2 轮） | **1,919** | +8% | 8.0ms → 8.0ms | 1.0–1.1s → 1.5–1.6s |

原始输出、诊断变体（HWM/tee/bytestream/每请求定时器）与方法见 `docs/archive/bench/user-runtime-body-buffer-2026-10-20.md`。

流式与边界行为：`/stream`（`text/event-stream`）首块约 1ms、100 块约 1.0s，仍逐块可见；256KiB `text/plain` 超过 128KiB 缓冲上限后回退为受生命周期跟踪的流，字节数一致；204/500 状态与 `content-type` 不变；`make js-test` 通过。p99 的 1s 级尖峰在改动前的基线、甚至裸 workerd `/ping`（p99 223–726ms）上就存在，本次未改变，也不把它归因于本改动。

### user-runtime 动态 Tail 挂载 A/B（ADR-238，2026-10-20）

单 cell-agent + 单**动态** user-runtime（公共 `:18281`），`CELLHIVE_TENANT_LOG_CAPTURE=on`、trace ratio 0.01；`wrk -t4 -c64 -d15s -H 'Host: bench.cellhive.test'`。通过内部订阅端点 `POST /v1/internal/logs/subscribe`（ttl 20s）模拟“有人正在 tail”：

| 阶段 | RPS | p50 | p99 | wrk timeout |
|---|---:|---:|---:|---:|
| 无订阅（动态门控关闭 Tail） | 3,060 / 3,215 | 3.7–5.0ms | 1.1–1.2s | 2 / 0 |
| 订阅中（切到 tail 变体） | 2,586 | 3.7ms | 1.76s | 51 |
| 订阅过期（切回 no-tail） | 3,038 | 3.6ms | 1.32s | 4 |

对照：同一动态路径在改动前（Tail 常开）为 2.7–2.8k；静态托管原型 Tail 关 13k、Tail 开 7.9–8.3k（见 `docs/archive/bench/static-hosting-prototype-2026-10-20.md`）。动态 loader 的单实例地板上限仍在 ~3.4k，本项收益是“无人看时不再付 Tail 成本”（约 +12%~+18%），静态托管则需在渲染期消费同一端点（后续工作）。共享宿主诊断，只作同会话比较。

### Go ingress 静态/动态混合 e2e（ADR-240/245/246，2026-10-20）

单 cell-agent + 单 user-runtime：ingress `:18300` → 动态 loader `:18281`；热集提升后静态 generation `:19500`。被测 worker 为 `loadbench`（带 KV binding，静态代包含平台服务与 service binding）。

- 提升前 100 个预热请求全部走动态（`dynamic_total=100`）；
- 热集自动提升后，路由表写入 `bench.cellhive.test → http://127.0.0.1:19500`；后续 164 个请求 `requests_total` 增加而 `dynamic_total` 保持 100，确认全部走静态 socket；
- 静态路径 `wrk -t4 -c64 -d15s`：**11,099 / 10,154 RPS**（load 2.2 / 3.7），p50 6.3 / 6.8ms，p99 165 / 186ms，3 个 socket 错误；
- 同机动态 loader 对照 ~3.0–3.2k（Tail 关），即 Go ingress + 静态代在本例中约 **3.3–3.6×** 于动态路径，接近静态直连口径。
- `/kv` 返回 404 是租户路由本身未实现（响应来自静态 worker），不是平台 502；静态代内的 KV binding 已通过 `TestBoundWorkerRunsAsStaticGeneration` 验证 `env.KV` 存在。
- 共享宿主诊断，load 2.2–3.7，只作同会话比较。

### 静态最小入口 vs wrapper（ADR-249，2026-10-20）

同一 `loadbench` 静态代（KV binding），同一宿主、`wrk -t4 -c64 -d10s`：

| 入口 | RPS | workerd CPU | CPU/请求 |
|---|---:|---:|---:|
| 完整 wrapper | 7,133–7,161 | 1.05 cores | ~146µs |
| **minimal（auto，本次默认）** | **14,162–14,295** | 1.02 cores | **~72µs** |
| 之后混合 e2e（load 较高） | ingress 静态 **9,781** / 静态直连 9,157 / 动态 2,788 | — | — |

Tail 订阅开启后 ingress 静态回落到 5,852（Tail 仍是主要成本）；无 Tail 时静态约为动态路径的 3.5×。

### Tail 不挂静态快路径（ADR-250，2026-10-20）

同一 `loadbench` 静态代（KV binding，auto minimal），`wrk -t4 -c64 -d10s`：

| 状态 | RPS | 说明 |
|---|---:|---|
| ingress → 静态（无 Tail） | **12,565** | 静态快速路径 |
| 静态直连（无 Tail） | 13,581 | 接近裸 workerd |
| 动态 loader 直连 | 2,841 | 对照 |
| 有 tail 订阅时 | 该 host 回落动态（~2.8k），Tail 事件正常；订阅过期后恢复静态 | 不拖累其他静态 worker |

### wrapper plain-fetch 入口（ADR-251，2026-10-20）

同一 `loadbench` 静态代，强制完整 wrapper（`CELLHIVE_STATIC_MINIMAL=0`），`wrk -t4 -c64 -d10s`：

| 入口 | RPS | CPU/请求 |
|---|---:|---:|
| 旧 `new CellHiveHost()` 每请求 | ~7,133–7,161 | ~146µs |
| **`handleFetch` plain 入口** | **9,602–12,388** | **83–102µs** |
| minimal（auto，可选） | ~14,162–14,295 | ~72µs |

混合 e2e（wrapper 入口）：ingress 静态 **11,376** / 静态直连 11,800 / 动态 3,010；功能 21/21。

### 全部 platform binding e2e（2026-10-20）

真实 cell-agent + user-runtime + ingress + staticgen + do-runtime，动态与静态各一遍：**32/32 通过**（含 Queue consumer、Workflow create→complete、DO 存储跨 do-runtime 重启、Hyperdrive 真实 Postgres SQL）。

| binding | 动态 | 静态 |
|---|---|---|
| KV（put/get/getWithMetadata/list/delete） | ✅ | ✅ |
| D1（prepare/bind/run/first/all） | ✅ | ✅ |
| Drizzle ORM（drizzle-orm/d1 insert/select） | ✅ | ✅ |
| R2（put/get/head/list/delete + writeHttpMetadata） | ✅ | ✅ |
| Queue producer send + consumer | ✅ | ✅ |
| Service binding fetch | ✅ | ✅ |
| AI run（本地 OpenAI-compatible fake） | ✅ | ✅ |
| DO fetch + storage 跨重启 | ✅ | ✅ |
| Workflow create→complete | ✅ | ✅ |
| Vectorize insert/query | ✅ | ✅ |
| Hyperdrive connectionString + 真实 Postgres SQL | ✅ | ✅ |

注：Hyperdrive 用最小 PostgreSQL v3 协议客户端验证真实查询；DO/Workflow/Vectorize 走本地 harness 资源。

### 双节点 DO/Vectorize e2e（2026-10-21）

两个 cell-agent 共享 bucket + 稳定 cell 代理 + 各自 do-runtime/user-runtime：

| 场景 | 结果 |
|---|---|
| n1 `/do` | `do-1` |
| n2 `/do`（同一 DO id） | `do-2` |
| kill n1 + do1 后 n2 `/do` | `do-3`（DO owner/存储恢复） |
| n1/n2 `/vec` | 同一 Vectorize 结果 |
| kill n1 后 n2 `/vec` | 等 control lease TTL（~10s）后正常 |

注：tenant `CELL_URL` 必须指向稳定入口；控制面 owner 切换受 lease TTL 约束。

### Workflow 长等待/超时/retry e2e（2026-10-21）

| 场景 | 结果 |
|---|---|
| `step.sleep('3 seconds')` | waiting → complete，`{"slept":true}` |
| `step.waitForEvent(timeout='2 seconds')` | waiting → errored，`workflow waitForEvent timed out` |
| `step.do(retries.limit=5)` 前两次失败 | 第 3 次成功，complete，`{"attempts":3}` |

### Queue 多节点 retry/DLQ e2e（2026-10-21）

| 场景 | 结果 |
|---|---|
| q1 正常消息 ×5 | consumed=5 |
| 一条消息 `retry()` 超限 | DLQ q1-dlq depth=1 |
| 强杀 n1，lease 后 n2 接管，再发 3 条 | consumed=8 |

仓库脚本：`make e2e-workflow`、`make e2e-queue`、`make e2e-queue-multinode`、`make e2e-queue-concurrency`。

### R2 metadata handler 覆盖（2026-10-21）

| 路径 | `writeHttpMetadata` |
|---|---|
| queue handler | `text/x-queue` |
| workflow handler | `text/x-workflow` |

### Queue 并发消费 smoke（2026-10-21）

| 参数 | 结果 |
|---|---|
| 100 条消息，batch=10，concurrency=4 | count=100，missing_end=0，max concurrent handlers=40 |
| 并发生产 20 条 + 顺序 80 条 | parallel send_ms≈32ms；全部 100 条消费 |


### KV / D1 cell-agent 接口基准（2026-10-21）

直接打 cell-agent 内部 HTTP（`x-cellhive-scope-token`），单节点、本地 FS bucket、SQLite；不是 worker/user-runtime 端到端。

```text
wrk -t4 -c32 -d10s
```

| 接口 | RPS | p50 | p99 |
|---|---:|---:|---:|
| KV GET `/v1/kv/get` | ~36.8k | 0.75ms | 3.7ms |
| KV PUT `/v1/kv/put` | ~9.3k | 3.2ms | 34.9ms |
| D1 SELECT `/v1/d1/query` | ~46.6k | 0.59ms | 2.6ms |
| D1 INSERT `/v1/d1/exec` | ~7.9k | 3.5ms | 66ms |

说明：

- KV PUT 固定写同一个 key，存在同一 cell 的写竞争；D1 INSERT 表持续增长且走 capture proof，p99 偏高。
- 这是内部 API 上限参考，不含 workerLoader、binding facade、网络/TLS。
- 未启用多副本；单 agent 写吞吐受 SQLite/FS bucket 限制。
- 最终有效轮次无 non-2xx；此前 D1 的 non-2xx 是请求体未用 JSON 的测试脚本错误，不是接口问题。

### Worker JS KV 端到端基准（2026-10-21）

单节点、动态 loader（未启用 static generation），worker 只做 `env.KV.get` / `env.KV.put`，经 user-runtime 公开入口 + Host 路由。

```text
wrk -t2 -c8 -d10s
```

| worker 路径 | RPS | p50 | p99 |
|---|---:|---:|---:|
| `/ping`（无 binding） | ~3.3k | 1.55ms | 216ms |
| `/get`（KV get） | ~1.9k | 3.74ms | 75ms |
| `/put`（KV put） | ~1.2k | 6.38ms | 82ms |

`-t4 -c32` 复测 RPS 接近（ping ~3.4k、get ~2.0k、put ~1.6k），但 p99 明显变差（ping ~871ms、get ~94ms、put ~109ms），说明该路径在高并发下排队/尾延迟明显。

对比 cell-agent 内部 API：

| 路径 | RPS 比例 |
|---|---:|
| KV GET | worker ~5% of direct (~36.8k) |
| KV PUT | worker ~13% of direct (~9.3k) |

结论：

- 当前动态 loader 路径下，worker JS 的 KV 吞吐主要受 runtime/loader 和 binding RPC 限制，而不是 cell-agent 的 KV 存储；
- 写路径更差（p50 6.4ms、1.2k RPS）；
- 这是单节点动态路径基线，不代表启用 static generation 后的上限。

### Worker JS KV：动态 vs static generation（2026-10-21）

同一 worker，仅 `env.KV.get` / `env.KV.put`；先经 ingress warm hot set，等待 `staticgen: generation 1 published` 后打 ingress static 端口。

`wrk -t2 -c8 -d10s`：

| 路径 | 动态 RPS | static RPS | 动态 p50 | static p50 |
|---|---:|---:|---:|---:|
| `/ping` | ~3.3k | **~13.6k** | 1.55ms | 0.52ms |
| KV GET | ~1.9k | **~3.8k** | 3.74ms | 1.93ms |
| KV PUT | ~1.2k | **~1.65k** | 6.38ms | 4.61ms |

`wrk -t2 -c32 -d10s`：

| 路径 | 动态 RPS | static RPS |
|---|---:|---:|
| `/ping` | ~3.4k | **~16.3k** |
| KV GET | ~2.0k | **~4.2k** |
| KV PUT | ~1.6k | **~2.5k** |

结论：

- static generation 把空 handler 吞吐提高约 **4–5×**，KV GET 约 **2.2×**，KV PUT 约 **2×**；
- KV 路径仍远低于 cell-agent 直连（GET ~36.8k、PUT ~9.3k），剩余瓶颈是 binding RPC/写路径，不是 loader；
- static 下 p99 仍高（GET ~128–187ms、PUT ~114–128ms），写路径和 cell compaction 的尾延迟需继续观察；
- 本测试是单节点、本地 FS bucket、无多副本。

### KV 慢的原因定位：per-call binding RPC，不是 HTTP 协议（2026-10-21）

static worker 单 key GET ~4.3k RPS；同一 worker 的 `getBatch(10 keys)` 为 ~3.38k requests/s，折算 **~33.8k keys/s**，接近 cell-agent 直连 `get` 的 ~36.8k。

| 路径 | 结果 |
|---|---:|
| cell-agent 直连 KV GET（c32） | ~36.8k RPS |
| static worker KV GET（c32） | ~4.3k RPS |
| static worker KV `getBatch(10)`（c32） | ~3.38k req/s → **~33.8k keys/s** |
| direct KV GET（单连接 c1） | ~6.6k RPS |
| static worker `/ping`（c32） | ~16.2k RPS |

结论：

- 主要开销是 **每次 KV 调用都经过 worker → platform `KV` WorkerEntrypoint → `PLATFORM.fetch` → cell-agent** 的 RPC/服务跳转，以及每请求的鉴权/序列化；
- 不是 cell-agent 存储慢：批量 10 key 时，每 key 成本被摊薄，接近直连；
- 不是 HTTP 协议本身：直连同样走 HTTP/1.1 能达到 36.8k；换自定义协议不会自动消除 workerd 内的 binding/service hop 和 token 边界；
- p99 仍高（单 key static GET ~163ms，getBatch ~87ms），写路径和 compaction/排队仍需继续看。

建议：

1. 应用层优先用 `getBatch`/批量读取；CellHive 的 `getBatch` 是扩展，纯 CF 兼容代码不能直接用。
2. 若必须单 key 高频，优化方向是 **减少 binding RPC 跳数**（例如同 isolate 平台直连、连接复用/批量协议），而不是换掉 HTTP。
3. 写路径没有批量 put；考虑应用侧合并/异步写，或评估专门的数据接口。

### MySQL Hyperdrive e2e（2026-10-21）

- MySQL 8.0 + ProxySQL 3.0.11；
- direct：`mysql://` 原样下发，`mysql2` 查询返回 `{"ok":1,"db":"app"}`；
- proxy：origin 故意不可达 `127.0.0.1:13307`，binding 改写至 ProxySQL `6033`，查询仍返回 `{"ok":1,"db":"app"}`；
- ProxySQL `stats_mysql_connection_pool`：backend `127.0.0.1:13306`，`Queries` 增长。
- PostgreSQL 17 + ProxySQL 3.0.11：自动生成 `pgsql_servers`，worker 查询返回 `{"rows":[{"ok":1,"db":"postgres"}]}`。
- `mysql2` 必须传 `disableEval: true`，否则 workerd 拒绝 `new Function`。

### PostgreSQL + ProxySQL static generation（2026-10-21）

- PostgreSQL 17 + ProxySQL 3.0.11；
- CellHive 自动创建 `pgsql_servers`；
- dynamic 查询：`{"rows":[{"ok":1,"db":"postgres"}]}`；
- warm hot set 后 static table 指向 `127.0.0.1:19900`，static 查询同样返回 `{"rows":[{"ok":1,"db":"postgres"}]}`；
- 说明 ProxySQL proxy 模式在 dynamic/static 两条 worker 路径都可用。
