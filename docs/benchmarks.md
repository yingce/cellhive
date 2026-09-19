# 基准回归（benchmarks）

本页记录可复现的基准命令与结果，用于回归对照。**非生产硬件**；对象存储分 **FS（本地文件）** 与 **S3（本地 MinIO）** 两档；未含真实 workerd（DO 相关数字见 `docs/archive/p0-report.md`）。原始输出归档在 `docs/archive/bench/`。

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

## 结论

- **基线一致性**：FS `c=128` sqlbench **30.7k TPS**，与历史 durable ~28.9k 一致（历史上万级是**高并发**下的稳态，不是单请求延迟）。**并发低则 TPS 低**（c=8 → ~2.8k），属延迟受限的正常缩放。
- **FS → S3 的差**主要落在 **bucket 提交等待**（realbench p50 0.22→2.0 ms；sqlbench E2E p50 2.9→5.1 ms），即 RPO=0 的代价；本机 MinIO 仍非生产云对象存储尾延迟。
- **写合并有效**：sqlbench `transactions_per_batch ≈ 7.4–7.9`，`put_per_ack` 远小于 1；capture 批内合并显著摊薄提交。
- **数据准确**：所有档 `db_integrity=ok`、`failures=0`。
- **workerd/DO**：进程级上限与门控数字（≈800 req/s 裸 DO、k 写合并提升）见 `docs/archive/p0-report.md`；本页未重跑 workerd 基准。
- **局限**：单机 loopback、无跨主机 RTT；MinIO 本地；未压真实磁盘/云对象存储。跨主机/云数字需环境（C 类 blocker）。
