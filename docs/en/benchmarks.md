# Baseline Regression (benchmarks)

This page records reproducible benchmark commands and results for regression comparison. **Non-production hardware**; object storage is split into **FS (local files)** and **S3 (local MinIO)** tiers; does not include real workerd (for DO-related numbers, see `docs/archive/p0-report.md`). Raw outputs are archived under `docs/archive/bench/`.

## Environment

- Go: `/usr/local/go` (`go version` is shown in the run output).
- Object storage: FS bucket (`-bucket-dir`) or local MinIO (`quay.io/minio/minio:RELEASE.2025-02-18T16-25-55Z`, `make s3-test`).
- cell-agent: local single process, `CELLHIVE_DURABILITY=auto` + `BucketWait=true` (bucket wait, RPO=0); the FS tier `realbench` runs in bucket mode.
- Machine: single-machine loopback (no real cross-host RTT).

## Commands

```bash
make build
# Self-contained (in-process owner+follower + FS bucket)
./bin/cellbench -n 3000 -c 16

# Start cell-agent (FS bucket), then run HTTP-path benchmarks
CELLHIVE_NODE_ID=bench CELLHIVE_BUCKET_DIR=/tmp/bench/bucket ./bin/cell-agent &
./bin/realbench -owner http://127.0.0.1:7001 -n 1000 -c 1
./bin/sqlbench  -owner http://127.0.0.1:7001 -n 1000 -c 8

# S3 tier: after starting MinIO and creating the bucket, run the same benchmarks with the s3:// backend (see make s3-test for the bucket startup method)
CELLHIVE_BUCKET=s3://cellhive AWS_ENDPOINT_URL=http://127.0.0.1:9000 \
  AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin CELLHIVE_S3_PATH_STYLE=true ./bin/cell-agent &
./bin/sqlbench -owner http://127.0.0.1:7001 -n 500 -c 8
```

> `kvbench` requires a scoped token (`-scope-token`); `gatebench` requires workerd + the gate URL emitted by do-supervisor (see the P0 gate benchmarks in `docs/archive/p0-report.md`).

## Results

### cellbench (in-process owner+follower + FS bucket)

| concurrency | fleet p50 | fleet p99 | fleet rps | put/ack |
|---|---|---|---|---|
| c=1 | 0.93 ms | 2.22 ms | 979 | ≈0.02 |
| c=16 | 13.84 ms | 16.80 ms | 1141 | ≈0.08 |
| c=64 | 49.45 ms | 60.10 ms | 1268 | — |

(fleet mode: waits for follower fsync; `restore` 200 segments ≈ 1.9 ms; failures=0.)

### realbench → cell-agent (commit round trip, bucket mode)

| backend | concurrency | p50 | p99 | rps | failures |
|---|---|---|---|---|---|
| FS | c=1 | 0.22 ms | 0.33 ms | 3930 | 0 |
| FS | c=64 | 1.31 ms | 15.44 ms | **30807** | 0 |
| S3 / MinIO | c=1 | 2.00 ms | 2.73 ms | 476 | 0 |
| S3 / MinIO | c=64 | 6.59 ms | 39.75 ms | **7821** | 0 |

### sqlbench → cell-agent (SQL→WAL→LTX→bucket end to end, `db_integrity=ok`)

**Throughput increases linearly with concurrency** (latency-bound; TPS ≈ concurrency / E2E latency):

| backend | concurrency | TPS | E2E p50 | E2E p99 | tx/batch |
|---|---|---|---|---|---|
| FS | c=8 | 2758 | 2.87 ms | 3.57 ms | 7.9 |
| FS | c=32 | 10841 | 2.88 ms | 3.79 ms | 32.0 |
| **FS** | **c=128** | **30749** | 3.72 ms | 7.07 ms | 61.5 |
| S3 / MinIO | c=8 | 1558 | 5.06 ms | 6.23 ms | 7.8 |
| S3 / MinIO | c=32 | 1256 | 5.57 ms | 13.95 ms | 32.0 |
| S3 / MinIO | c=64 | 10915 | 5.53 ms | 10.53 ms | 54.5 |

(S3 E2E is ~2 ms higher than FS, caused by waiting for the bucket commit for each batch; `failures=0`, `integrity=ok`.)

### backend-A Capture Overhead (kvbench → cell-agent, single-machine FS bucket, ADR-092)

Comparison between `CELLHIVE_CAPTURE=off` and the default (capture enabled, `CELLHIVE_DURABILITY=bucket`, RPO=0); `kvbench -d 15s`, `fail=0`. Raw output: `docs/archive/bench/backend-a-capture.txt`.

| configuration | path | concurrency | rps | p50 | p99 |
|---|---|---|---|---|---|
| capture ON (RPO=0) | KV put | c=1 | 335 | 2.76 ms | 3.88 ms |
| capture ON (RPO=0) | KV put | c=4 | 1157 | 3.55 ms | 6.31 ms |
| capture ON (RPO=0) | D1 insert | c=1 | 330 | 2.79 ms | 4.00 ms |
| capture ON (RPO=0) | D1 insert | c=4 | 1141 | 3.60 ms | 6.11 ms |
| capture OFF (local) | KV put | c=4 | 5542 | 0.34 ms | 8.77 ms |
| capture OFF (local) | D1 insert | c=4 | 5061 | 0.37 ms | 8.74 ms |

- **Capture overhead is latency, not the ceiling**: at c=1, capture ON is 341 rps vs OFF 3144 rps (p50 2.7ms vs 0.31ms); after increasing concurrency, both reach the **same write ceiling of ~5.5–6k rps/single cell**. The 1.1k at c=4 is only latency-bound (rps≈c/latency), **not the ceiling**.
- **Single-cell write ceiling is ~5.5–6k rps**: KV put peaks at c=32 with 5500 rps, D1 at 5158 rps (capture ON); capture OFF peaks at c=8 with 6136 rps. **Reads** can reach ~30k rps (c=128). At peak, cell-agent CPU is ~2.1/8 cores for writes and ~4.7/8 cores for reads → **not a CPU/thread bottleneck**, but serialization by the single-cell SQLite single writer (one transaction + commit per write).
- **Multiple cells only add up for local writes (capture OFF)**: 4 independent cells in parallel (each c=8, capture OFF) aggregate to **13.9k rps**; but with capture ON (RPO=0), multiple cells **do not add up**—the bottleneck is the object-storage sink (see the next section).
- With `CELLHIVE_BUCKET_WAIT=false` (bucket-async), the capture committer **rejects** ack (`commit was not durable (mode "bucket-async")`)—by design, it does not provide capture ack for RPO>0.
- Cold recovery/consistency (not a benchmark): 1000 acked KV writes → `kill -9` owner → new node takes over with an empty disk → reads back 1000/1000 (p50 0.27 ms).

### Single Cell vs Dual Cell (capture ON, LTX→bucket, RPO=0)

Script: `scripts/bench-capture-cells.sh` (capture ON, `CELLHIVE_DURABILITY=bucket`, `kvbench` single process round-robins by ns; dual cell = two independent namespaces/cells, same total concurrency). Raw outputs: `docs/archive/bench/single-vs-dual-capture-sharded.txt` (FS), `single-vs-dual-capture-s3.txt` (S3/local MinIO).

**FS bucket (LTX landing in bucket and recoverability verified)**

| cell count | c | rps | p50 | p99 |
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

**S3 / local MinIO**

| cell count | c | rps | p50 |
|---|---|---|---|
| 1 | 64 | 2711 | 22.15 ms |
| 1 | 128 | 2398 | 46.35 ms |
| 2 | 64 | 2217 | 25.47 ms |
| 2 | 128 | 2439 | 49.40 ms |

Conclusions:
- **Dual cell is not near-linear**: on FS, the peak is 1.47x (c=32), and drops at higher concurrency; on S3/MinIO, dual cell has **no gain** (object storage is already the bottleneck).
- **The cause is serialized/same-disk object-storage sink**: the local FS bucket (`internal/bucket/fsbucket.go`) wraps `Put/CAS/List` with **a single global mutex** (+`os.WriteFile`), serializing commits from all cells; single-node MinIO also runs on the same disk. The upload layer is already sharded (`upload.NewSharded`, default NumCPU capped at 8, hashed by scope, order-preserving), but sharding does not help when the sink itself lacks parallelism.
- **LTX landing in the bucket has been verified**: after an FS run, `restoreverify -bucket -scope ... -epoch 1` outputs `integrity:"ok"` and the correct key counts (single cell 1000 / dual cell 500 each).
- **True multi-cell scaling requires**: real distributed object storage (different hardware) + lower commit frequency + sink parallelism. On a single machine with the same disk, multi-cell gains are limited.
### Write-Path Optimization (ADR-093, single-machine FS bucket)

**Bulk KV writes have been removed**: `env.KV` has no bulk API, so this scenario does not exist (see the ADR-093 note).

**binding cache A/B (capture OFF, single cell)**: cache off 6156/5985/5722 rps vs cache 1000ms 6045/5911/5458 (c=8/32/128) → **no gain** (binding lookup is not the bottleneck), retained but no benefit is claimed.

**SQLite bare baseline**: autocommit UPSERT 12.4µs(~80k/s), cellstore `PutTx` 58.8µs(~17k/s), batch 100/txn 10.2µs/key(~98k/s).

Raw output: `docs/archive/bench/optimization-2026.txt`.


### Single-Key Write Optimization (ADR-094, single-machine FS bucket)

**capture OFF, single-cell single-key put**: after adding `Cell.writeMu` (serializing write transactions on the Go side to avoid SQLITE_BUSY retries):

| concurrency | before rps | after rps | after p50 |
|---|---|---|---|
| c=8 | 6156 | 7481 | 1.23 ms |
| c=16 | 5561 | **9637** | 1.61 ms |
| c=32 | 5985 | 8679 | 3.28 ms |
| c=128 | 5722 | 9334 | 13.39 ms |

Two clients each at c=16: before 1980+1934=3914 (collapse), after 4563+4561=**9124** (stable).

**capture ON (RPO=0) single-cell single-key put**: c=16 4616, c=32 5052, c=128 4512; two clients 4681 → **bucket proof is the ceiling**.

**group-commit window is effective** (ticker follows the window, c=128): window 2/5/10/20ms → transactions per segment **8.6/21.3/41.2/89.3**, throughput stays ~5–6k → coalescing is not the bottleneck.

Raw output: `docs/archive/bench/optimization-single-key.txt`.


### Two-Node Fleet (dual processes on the same machine, ADR-094 context)

`scripts/bench-fleet.sh`: two cell-agent processes share the same bucket, `CELLHIVE_DURABILITY=auto` → with a peer present, it uses **fleet (peer fsync)**, and bucket upload is asynchronous. Raw: `docs/archive/bench/fleet-2node.txt`.

| concurrency | rps | p50 | comparison: single-node bucket (same cell) |
|---|---|---|---|
| c=1 | 241 | 4.44 ms | 322 / 2.79 ms |
| c=16 | 3524 | 4.26 ms | 4616 |
| c=32 | 4215 | 7.45 ms | 5052 |

- **fleet path confirmed**: the follower's `peer-spool/<ns>/__kv__/default/e1/segments.log` contains data, and the bucket has 727 asynchronous `.batch` objects.
- **But same-machine fleet is not faster** (even slightly slower): the two processes contend for the same disk/CPU, peer fsync and bucket writes use the same disk, and the loopback bucket is already fast. "fleet is significantly faster than a single-node bucket" only holds in **cross-host/cross-network** scenarios; reproducing it requires **a second machine** (missing in the current environment, class-C blocker).
- Conclusion: fleet is designed for **remote buckets/high RTT**; for local/near buckets, a single-node bucket is already fast enough.

### Write Cost Breakdown and txid Mirror (ADR-095)

| layer | per write | ceiling |
|---|---|---|
| bare autocommit UPSERT | 12.3µs | ~81k |
| explicit transaction + UPSERT (no txid) | 16.4µs | ~61k |
| cellstore `PutTx` (txid accounting) before | 57.6µs | ~17k |
| cellstore `PutTx` after (txid in-memory mirror) | **40.4µs** | ~25k |
| in-process handler (binding cache) before/after | 91 / **74.6µs** | ~13k |

Real HTTP single-cell single-key put (capture OFF): c=32 **8679 → 9784 rps** (+13%), c=16 ~9.8k. capture ON remains ~4.6k (limited by bucket proof). Raw: `docs/archive/bench/write-cost-decomposition.txt`.


### KV / D1 Reads and Writes (single cell, single-machine 8 cores, FS bucket, after ADR-095/098)

`kvbench -mode put|get|d1|d1query -ns capA -d 8s` (`fail=0`). Raw: `docs/archive/bench/kv-d1-readwrite.txt`.

| capture | operation | c=16 | c=64 | c=128 | p50(c=64) |
|---|---|---|---|---|---|
| OFF | KV put | 11173 | 12149 | 11713 | 5.11 ms |
| OFF | KV get | 25667 | 29529 | 30317 | 1.71 ms |
| OFF | D1 insert | 10763 | 11929 | 10935 | 5.24 ms |
| OFF | D1 query | 24502 | 28373 | 29225 | 1.79 ms |
| ON (RPO=0) | KV put | 5099 | 9475 | 11308 | 5.84 ms |
| ON (RPO=0) | KV get | 25355 | 30011 | 30471 | 1.68 ms |
| ON (RPO=0) | D1 insert | 4992 | 8563 | 7746 | 6.88 ms |
| ON (RPO=0) | D1 query | 21193 | 26006 | 26652 | 2.02 ms |

- **Reads ~30k/s (KV get / D1 query)**, basically independent of the capture switch (reads do not pay for durability proof); KV reads are flat ON/OFF, while D1 query ON is slightly lower (≈26.6k, **because D1 query also went through `capturedWrite`**).
- **Writes ~12k/s (OFF) / ~11k/s (KV, ON)**; D1 writes ON ~8.5k (with p99 tail latency). Compared with ~5k in the ADR-094 era, writeMu + in-memory txid mirror + binding cache more than doubled single-key write throughput.
- **D1 read-only now skips capture (ADR-100)**: SELECT/EXPLAIN/VALUES no longer go through `capturedWrite` (unit test `TestD1ReadSkipsCapture` proves Ensure/owner checks are no longer triggered); PRAGMA/WITH may write, so they are still captured. **Retest of D1 query (capture ON) ≈26.0k→26.5k, with no measurable improvement**—this shows the small ON/OFF gap mainly comes from **environment contention** (CPU/disk) caused by capture/upload, not per-request proof overhead. The change is still kept (one fewer owner resolution, cleaner semantics).


### KV / D1 read/write retest (ADR-159 CGo driver + ADR-160 LZ4 WAL3, same method)

**The method is exactly the same** (single cell `capA`, single 8-core machine, FS bucket, `kvbench -mode put|get|d1|d1query -c 1/16/64/128 -d 8s`, `fail=0`); the only variable is the code: SQLite changed from `modernc` (pure Go) to **`mattn/go-sqlite3` (CGo, ADR-159)**, and L1/page-map changed to **LZ4-compressed `WAL3` + `CID2` index (ADR-160)**. Raw data: `docs/archive/bench/kv-d1-readwrite-cgo.txt`.

| capture | Operation | c=16 (old→new) | c=64 (old→new) | c=128 (old→new) | p50(c=64) old→new |
|---|---|---|---|---|---|
| OFF | KV put | 11173 → **12368** (+11%) | 12149 → **15733** (+29%) | 11713 → **13011** (+11%) | 5.11 → **3.98 ms** |
| OFF | KV get | 25667 → **29770** (+16%) | 29529 → **36394** (+23%) | 30317 → **38436** (+27%) | 1.71 → **1.40 ms** |
| OFF | D1 insert | 10763 → **12237** (+14%) | 11929 → **12738** (+7%) | 10935 → **13440** (+23%) | 5.24 → **4.06 ms** |
| OFF | D1 query | 24502 → **27641** (+13%) | 28373 → **33435** (+18%) | 29225 → **34487** (+18%) | 1.79 → **1.53 ms** |
| ON (RPO=0) | KV put | 5099 → 4623 (−9%) | 9475 → **9336** (−1%) | 11308 → 11173 (−1%) | 5.84 → **6.15 ms** |
| ON (RPO=0) | KV get | 25355 → **31314** (+23%) | 30011 → **38098** (+27%) | 30471 → **38696** (+27%) | 1.68 → **1.34 ms** |
| ON (RPO=0) | D1 insert | 4992 → 3739 (−25%) | 8563 → 8171 (−5%) | 7746 → **9302** (+20%) | 6.88 → **6.87 ms** |
| ON (RPO=0) | D1 query | 21193 → **27215** (+28%) | 26006 → **32958** (+27%) | 26652 → **34413** (+29%) | 2.02 → **1.55 ms** |

- **Reads (KV get / D1 query) are generally +13% to +29%**, with p50(c=64) dropping from 1.7ms/1.8ms to 1.4ms/1.5ms → the CGo driver (real C SQLite) is faster on the read path; the capture switch has basically no impact on reads (reads do not pay for durability proof).
- **Local writes (capture OFF)**: KV put +11~29%, D1 insert +7~23%, p50(c=64) 5.1→4.0ms / 5.2→4.1ms.
- **Writes with capture ON (RPO=0) are roughly flat** (±10% variance): the upper bound of this path is **bucket proof** (each ack waits for object storage), not SQLite/driver; ADR-160 compression only applies to snapshot/L1 (delta is still uncompressed `WAL1`), so it has no direct impact on per-write throughput.
- `fail=0` throughout; the cell in this benchmark is resident locally, so **paged VFS is not involved** (compression benefits show up in bucket bytes and cold start; see the ADR-160 section).

### Single vs dual cell retest (ADR-159/160, same script/parameters)

Script and parameters are the same as `scripts/bench-capture-cells.sh` (`CONCS="16 32 64 128" DUR=10s MODE=put BUCKET_MODE=fs`). Raw data: `docs/archive/bench/single-vs-dual-capture-cgo-on-repeats.txt` (ON, two repeats, compression off/on once each), `single-vs-dual-capture-cgo-off.txt` (OFF), and the first ON sample `single-vs-dual-capture-cgo-on.txt` (**slow-state** sample; see below).

**capture OFF (local)**

| cell | c=16 | c=32 | c=64 | c=128 |
|---|---|---|---|---|
| 1 | 12409 | 15251 | 15531 | 15442 |
| 2 | **16103** | **20038** | **24339** | **24828** |

Old (2026-09-16) single cell 11.1–12.2k, dual cell 14.6–19.2k → this run single cell +10~27%, dual cell +10~38% (CGo driver + noise), dual/single ≈ **1.6x**.

**capture ON (durability=bucket, RPO=0)** — take the two repeats (compression off / on, same box state)

| cell | c=16 | c=32 | c=64 | c=128 |
|---|---|---|---|---|
| 1 | 5161–5189 | 9760–10031 | 12336–12338 | 13278–13295 |
| 2 | 4760–4908 | 8503–8602 | 15310–15445 | 16322–16773 |

Compared with old values (single 5161/10044/10685/10636, dual 4884/9841/14045/16612): at c≥64, **flat to significantly better** (dual cell c=128 +0~+29%), dual/single ≈ **1.55x** (consistent with the old 1.55–1.57x). The first ON sample (single cell c=32 8283, dual c=64 12002) was in the same range as the off group of the compression A/B, but the adjacent repeat (same state) returned to 9760–10031 / 15310–15445 → judged to be **run-to-run noise on a shared machine (±10~20%)**, not a code regression.

### S3 / MinIO profile (ADR-159/160)

Raw data: `docs/archive/bench/kv-d1-readwrite-s3-cgo.txt`, `single-vs-dual-capture-s3-cgo.txt`.

**KV/D1 (single cell, capture ON = RPO=0 over MinIO; capture OFF = writes do not persist to bucket)**

| capture | Operation | c=1 | c=16 | c=64 | c=128 |
|---|---|---|---|---|---|
| OFF | KV put | 4030 | 12394 | **16015** | 14324 |
| OFF | KV get | 5318 | 29395 | 35830 | **38136** |
| OFF | D1 insert | 3862 | 12334 | 15410 | 13554 |
| OFF | D1 query | 4836 | 27350 | 33156 | **33947** |
| ON | KV put | 146 | 1877 | 3383 | **3781** |
| ON | KV get | 4639 | 30968 | 33899 | **37738** |
| ON | D1 insert | 140 | 1592 | 2915 | **3636** |
| ON | D1 query | 4448 | 26931 | 31579 | **33585** |

- **capture OFF is almost identical to the FS profile** (write path does not persist each write to the bucket; the bucket is only used during compaction/GC) → this shows ADR-159 driver gains are obtained on both backends.
- **Writes with capture ON are ≈1/4 of FS** (each ack waits for one MinIO round trip, p50 8–32ms); **reads are the same as FS** (reads do not pay for durability proof). This is direct evidence that "write ceiling = bucket proof".

**Single vs dual cell (capture ON, `CONCS="1 16 32 64 128" DUR=10s`)**: single 144/1892/2716/3439/3812, dual 146/1836/2764/3434/3871 rps; old (2026-09-16) single 148/1423/1811/2711/2398, dual 140/1229/1773/2217/2439 → **c≥16 improved by +33~59%**, p50(c=128) 46.35→32.08 ms (−31%); dual cell still has **no gain** on single-node MinIO (object storage is the bottleneck).

### LZ4 compression switch A/B (ADR-161)

`CELLHIVE_LTX_COMPRESSION=off` (`WAL2` fixed frames) vs `on` (`WAL3` + LZ4), capture ON, FS bucket.

| Scenario | c=16 | c=32 | c=64 | c=128 |
|---|---|---|---|---|
| Single cell, off (rep1/rep2) | 5165/5062 | 10062/10024 | 12447/12300 | — |
| Single cell, on (rep1/rep2) | 5161/5065 | 10020/10064 | 12347/12356 | — |
| Dual cell, off | 4760 | 8503 | 15445 | 16773 |
| Dual cell, on | 4908 | 8602 | 15310 | 16322 |

**Conclusion**: under the same box state, on≈off (**within 1–3%**, p50 also the same) → LZ4 encoding is **not on the write ack critical path** and has no measurable throughput cost; the benefit is that bucket objects are **3.7× smaller** (see ADR-160). Therefore compression remains enabled by default, and `CELLHIVE_LTX_COMPRESSION=false` is only an escape hatch for CPU-bound/deterministic cases, not the performance default.

### Single vs dual cell (current code, including writeMu/txid mirror/D1 read-only capture skip; single-machine FS bucket)

`scripts/bench-capture-cells.sh` (`kvbench` single process round-robins by ns; dual cell = two independent namespaces). Raw data: `docs/archive/bench/cells2-on.txt`, `cells2-off.txt`.

**capture OFF (local)**

| cell | c=16 | c=32 | c=64 | c=128 |
|---|---|---|---|---|
| 1 | 11134 | 12233 | 12063 | 11808 |
| 2 | 14628 | 18525 | 17610 | **19165** |

**capture ON (durability=bucket, RPO=0, synchronously waits for bucket)**

| cell | c=16 | c=32 | c=64 | c=128 |
|---|---|---|---|---|
| 1 | 5161 | 10044 | 10685 | 10636 |
| 2 | 4884 | 9841 | 14045 | **16612** |

- Dual cell vs single cell ≈ **1.55–1.57x** (peak values on both sides), better than the earlier 1.44–1.47x; still **not 2x**, because the same machine shares CPU/disk and one bucket (capture ON also adds the global lock of the FS bucket).
- Absolute single-cell values have improved: OFF ~12.2k, ON ~10.6k (earlier ON ~5k). LTX bucket persistence + recovery verification (`integrity ok`, keys 1000/500) pass.


### LTX fragmentation and automatic compaction

`capture` produces one LTX object per merged batch; the upper-level `upload` `AppendBatch` **packs multiple segments into one object** (`.batch`), and `compaction` then folds the L0 chain into L1.

| Configuration | Object count after 20k writes | Notes |
|---|---|---|
| Compaction disabled | **1336** L0 objects (0 L1) | ~15 writes/object (2ms window, c=64) |
| Compaction enabled (default 30s / ≥64 segments or 64MB) | **3** objects (0 L0 + 3 L1) | `L1/{manifest.json,<maxTx>.snapshot,index.bin}` |

- **Automatic compaction exists**: the cell-agent `runCompactionLoop` periodically scans **owned** scopes, and when `>=MinSegments(64)` or `>=MinBytes(64MB)`, it folds L0→L1 + GCs old objects (verifies owner/epoch before folding); recovery reads only L1 + the updated L0 (bounded). Raw data: `docs/archive/bench/ltx-fragmentation.txt`.
- Peak fragmentation ≈ object count between two compactions ≈ **PUT rate × compaction interval**. For remote buckets, recommended `CELLHIVE_COMPACTION_INTERVAL=5–10s`, together with window/sharding controls on PUT rate.


### Micro-optimizations tried, with **no benefit, rolled back**

- Removed per-request `os.MkdirAll` from `Store.Path` (path memoization + mkdir only on open).
- KV `{"ok":true}` uses prebuilt bytes, saving one marshal.
- Result: in-process handler 74.6→71.3µs (−4%, within httptest noise), no change for real HTTP single cell (put c=64 11853→10974, get ~30k). → **Rolled back**, not kept. Conclusion: the ~34µs from HTTP/auth/binding is **distributed** (HMAC, query parsing, allocation, net/http, loopback), and small changes cannot move it.

### Fast path A/B latency (real workerd, ADR-106)

The stub injects 3ms on router/service hops; timing via `performance.now()` inside worker (p50 from 8 runs):

| Call | Fast path OFF | Fast path ON | Savings |
|---|---|---|---|
| DO invoke first call (via router) | 4.00 ms | 4.00 ms | — (first learning) |
| DO invoke steady state (direct to owner) | 3.00 ms | **0.00 ms** | ~3 ms |
| service RPC (native same instance) | 4.00 ms | **0.00 ms** | ~4 ms |

loopback + injected latency validates the mechanism that "the hop is removed"; production savings = real round trip (RTT for cross-host, requires a second host).

## Conclusion

- **Baseline consistency**: FS `c=128` sqlbench **30.7k TPS**, consistent with historical durable ~28.9k (historical tens-of-thousands level is steady state under **high concurrency**, not single-request latency). **TPS is low when concurrency is low** (c=8 → ~2.8k), which is normal latency-limited scaling.
- **The FS → S3 gap** mainly lies in **bucket commit wait** (realbench p50 0.22→2.0 ms; sqlbench E2E p50 2.9→5.1 ms), i.e. the cost of RPO=0; local MinIO still does not represent production cloud object storage tail latency.
- **Write coalescing is effective**: sqlbench `transactions_per_batch ≈ 7.4–7.9`, `put_per_ack` is far below 1; in-capture batching significantly amortizes commits.
- **Data is accurate**: all profiles have `db_integrity=ok`, `failures=0`.
- **workerd/DO**: process-level ceilings and gating numbers (≈800 req/s bare DO, k write coalescing improvement) are in `docs/archive/p0-report.md`; workerd benchmarks were not rerun on this page.
- **Limitations**: Single-node loopback, no cross-host RTT; local MinIO; did not stress real disks/cloud object storage. Cross-host/cloud numbers require the environment (Class C blocker).