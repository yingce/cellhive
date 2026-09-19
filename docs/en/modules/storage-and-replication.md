# Module: Object Storage and Replication

Object storage is authoritative: bucket conditional writes elect the owner, LTX capture/replication, snapshots/compaction, paged cold restore (paged VFS), and GC. The local disk is only a working copy/cache.

> For the authoritative configuration source, see [`../configuration.md`](../configuration.md) and the code; the table below is the module-related subset.
## Key Interfaces

Bucket abstraction (FS / S3 compatible); `POST /v1/internal/commit(_binary)`; peer `POST /v1/peer/append_batch` and HTTP 101 stream `/v1/peer/stream`.

## Configuration (Environment Variables)

| Variable | Default | Purpose |
|---|---|---|
| `CELLHIVE_CAPTURE_GROUPCOMMIT` | `0`→2ms | WAL coalescing window |
| `CELLHIVE_CAPTURE_PIPELINE` | `0`→8ms | Commit latency threshold; if exceeded, multiple chunks are in flight |
| `CELLHIVE_CAPTURE` | enabled | `off`/`0`/`false` disable capture = writes are local-only with no replication proof |
| `CELLHIVE_CELL_WAL_CHECKPOINT` | `64MiB` | Captured cell WAL truncation threshold; `0` disables it |
| `CELLHIVE_UPLOAD_SHARDS` | `0`→NumCPU(≤8) | Parallel bucket upload channels |


> For parsing rules (strings/booleans/durations/bytes/lists), see [`../configuration.md`](../configuration.md#parsing-rules).

## Key Invariants

- Conditional writes are the sole owner arbitration mechanism (no consensus service).
- Takeover control first restores the dead owner’s open log.
- The local disk is never the only replica; acknowledged data that has not been uploaded must not be deleted.
- No List operations on the hot path.

## Source Locations

`internal/{bucket,objectstore,ltx,replica,compaction,pagedvfs,capture,upload,peer,nodelog,recovery,restore,walscan?}`; `cmd/{s3probe,s3init,restoreverify,recoververify}`.

## Test Anchors

`make rpo-test`, `make s3-test`, `internal/{ltx,replica,compaction,pagedvfs}`.

## Related Documents

[`storage-and-s3.md`](../storage-and-s3.md), [`cell-protocol.md`](../cell-protocol.md), [`benchmarks.md`](../benchmarks.md)

_Last updated: 2026-09-19_