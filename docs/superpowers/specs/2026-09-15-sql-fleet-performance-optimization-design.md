# SQL Fleet Performance Optimization Design

## Goal

Raise the Go SQLite SQL→WAL→LTX→fleet benchmark from its practical c=64–128 range of 2.5–3.3k durable TPS to **at least 10k durable TPS with p99 below 100ms** on the same two-node loopback test host.

## Root Cause

The peer proof layer already sustains 45–47k synthetic LTX RPS. The SQL benchmark is slower because:

1. `wal.Cursor.Poll` calls `os.ReadFile` and reparses the complete, ever-growing WAL on every notification/tick. At c=1024 this repeatedly transfers tens of gigabytes to process 102,400 transactions.
2. Captured LTX is base64-encoded into JSON for `/v1/internal/commit`, adding 33% wire expansion and several full-buffer copies.
3. Unbounded capture batches reach hundreds of transactions, raising gate p99 even when they improve throughput.

## Reference Technique

The durability proof does not reread the complete WAL on every capture. The LTX database keeps the WAL file open, stores the previous WAL offset/size, and uses exact positioned reads for the new WAL tail; it also caches the last L0 position/header to avoid repeated directory and file scans.

Capture uses event-driven dirty notifications with a slow fallback tick, and durability tickets let concurrent writes ride one capture without crediting writes committed after capture began.

The capture connection owns checkpointing: it disables automatic checkpointing, holds a dedicated long-running read transaction to prevent unsafe WAL reset, and runs a bounded checkpoint/truncate policy itself (default truncate threshold 128 pages, with safeguards based on database size and capture continuity).

The fleet peer path uses compact binary append/entry frames and a length-prefixed upgraded stream. The follower drains frames already delivered by the socket and group-commits compatible frames before acknowledging them. The public durability contract gates responses until bucket or fleet proof covers the write.

## Scope

### Included

- Incremental positioned WAL reads using a held file descriptor.
- Binary owner commit endpoint carrying raw LTX bytes.
- Bounded capture segments: at most 128 transactions and at most 1 MiB of WAL1 binary payload.
- WAL bytes-read and capture batch timing metrics in `sqlbench`.
- Fresh two-node performance sweep at c=32/64/128/256.

### Deferred

- Controlled checkpoint/truncate and snapshot/link generation. CellHive cannot safely truncate until snapshot/link application is implemented.
- Custom SQLite VFS/hooks or CGO.
- workerd actor-file supervisor integration.
- Adaptive peer hedge.

## Incremental WAL Cursor

`wal.Cursor` will retain:

- an open `*os.File` for the WAL;
- the current salt and page/frame sizes;
- `frameIdx`, the first unconsumed frame;
- cumulative bytes read for diagnostics.

Each poll performs:

1. `Stat` the held fd.
2. `ReadAt` exactly the 32-byte WAL header.
3. Detect salt rotation or shrink below the cursor offset; close/reopen/reset and report `Checkpoint=true`.
4. Compute complete frames after `frameIdx` from file size.
5. `ReadAt` only those new frame bytes.
6. Advance `frameIdx` through the last complete commit marker; leave incomplete trailing frames for the next poll.

The cursor retries once with a reopened fd on `os.ErrClosed`, missing file, or short-read caused by WAL replacement. `Read(path)` remains the cold diagnostic full-file API.

Expose:

```go
func (c *Cursor) BytesRead() uint64
func (c *Cursor) Close() error
```

The performance test must prove bytes read is O(new WAL bytes), not O(WAL size × poll count).

## Binary Commit Endpoint

Add:

```text
POST /v1/internal/commit_binary?scope=<scope>&epoch=<epoch>
Content-Type: application/octet-stream
X-Cellhive-Followers: comma-separated internal peer URLs
Body: raw LTX segment
```

The endpoint uses existing internal-token authentication, caps the body at `maxSegmentBytes`, validates LTX/epoch, and calls the same fleet/bucket decision function as JSON `/commit`. The response remains the small existing JSON proof object.

`sqlcapture.HTTPCommitter` uses binary commit by default. The JSON endpoint remains for compatibility and `realbench` until separately migrated.

If the binary endpoint is unavailable (404/405), the benchmark fails explicitly; it does not silently revert to JSON because that would mix performance paths.

## Capture Batch Bounds

After one WAL poll, capture splits transactions into ordered LTX segments. A segment ends before either limit would be exceeded:

- 128 transactions;
- 1 MiB encoded WAL1 payload.

A single transaction larger than 1 MiB is allowed as its own segment up to `maxSegmentBytes`; this prevents deadlock for valid large SQL writes.

Segments commit sequentially for one scope. After each fleet acknowledgement, the barrier advances to that segment's `EndTxID`, releasing only covered SQL waiters. This bounds p99 without weakening ordering or RPO=0.

## Metrics

Extend `sqlcapture.Stats` and `sqlbench` output with:

- `wal_bytes_read`;
- `wal_bytes_per_transaction`;
- `ltx_binary_bytes`;
- `capture_batch_p50_ms` / `capture_batch_p99_ms`;
- existing transactions/batch and frames/transaction.

## Tests

1. Cursor incremental-read test: after a baseline poll, one new transaction reads approximately one WAL tail, not the complete existing WAL.
2. Cursor replacement/checkpoint test: held fd is reopened and checkpoint remains detectable.
3. Binary commit endpoint test: raw LTX reaches fleet follower spool and JSON commit remains compatible.
4. Capture split test: 300 small transactions produce 3 ordered segments (128/128/44), with barrier advancement after each acknowledgement.
5. Oversized single transaction test: emitted alone without an empty segment.
6. End-to-end SQL smoke: 100 transactions, c=4, failures=0, follower payload decodes.
7. Full/race verification on WAL, LTX, capture, server and peer packages.

## Acceptance

- failures=0 at c=32/64/128/256.
- At least one tested concurrency reaches **10k TPS**.
- The same point has **end-to-end p99 <100ms**.
- `wal_bytes_read` scales near actual new WAL bytes and does not multiply by capture poll count.
- Results remain labelled Go SQLite SQL→fleet, not workerd DO TPS.
