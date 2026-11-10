# Object Storage

The current, unshipped worktree follows native LTX from pinned celld `90b4301`. Compaction is additive: publish target-level files and retain sources. The old `Options.GC`, L1-generation retention, seals/pins/tombstones and separate manifest/index APIs are removed.

Physical reclamation uses native epoch GC: validate the recovery chain and current owner; retain required epochs and the immediate predecessor; delete only older epochs beyond the one-hour upload-age margin. Publish upstream `ltx/retired.json` (`retired_below`) before deletion so interruptions can resume. Unknown objects/timestamps, truncated listings and missing predecessors fail closed. Online callers hold a resident cell request/eviction gate, require complete materialization, and revalidate ownership after scanning and before each epoch deletion. Otherwise nothing is deleted. Explicit offline maintenance requires externally quiescent writers/readers; individual deletions also validate native path/header/CRC. The one-hour margin is not an active-epoch L0/L1 TTL. Bundle/assets GC is unchanged.

Paged activation reads embedded native indexes and requires every final image page except the lock page. The cut stays immutable; a missing object does not permit switching to a newer cut. Size-threshold selection reads the final 100-byte header and uses page size × commit, without a separate index or zero-L0 restriction. Compaction reads only uncovered inputs. Independent level schedules are 30 seconds, five minutes and one hour; per-scope timeout defaults to five minutes.

All native LTX uploads (byte deltas, continuation seeds, spools) use native forbid-overwrite PUT on COS/OSS, not AppendObject. Control records retain their separate append-based conditional-write protocol. LTX objects carry standard `litestream-timestamp` metadata derived from the header in UTC RFC3339Nano; multipart sets it at initiation without a sidecar.

Native codecs, restored page writes, and snapshot spool uploads support streaming. Spools are fully CRC-validated before atomic publication through owned Objects; retries compare at most 1MiB per ranged read. Regenerated snapshots may differ only in Timestamp and its resulting file CRC, after independent validation of the existing file and exact comparison of every other byte; the first object and metadata are retained. Deltas require byte identity. COS uses 5MiB multipart uploads with forbid-overwrite at completion and abort on failure/cancellation; S3/OSS support streamed single-object conditional creation. Multipart state creates no additional visible files. L1/L2/L3/L9 compaction reads native HTTP/file streams and writes a local temporary spool, then validates CRC and publishes through streaming upload. It closes inputs and removes the spool on failure. Page indexes still grow with page count; ordinary Get/RestoreChain bodies remain buffered, so memory is not constant across the entire system. COS/OSS and S3 default to a 512MiB object-read limit and reject oversized reads. Strict multi-epoch crash and real COS recovery acceptance is recorded below; a full same-topology memory/throughput comparison with celld remains outstanding. See [storage details](../storage-and-s3.md).

## Hard Requirements (All Required)

1. **Atomic conditional create** (S3 `If-None-Match: *`; native AppendObject at position 0 or current length after a tombstone);
2. **Conditional overwrite** (S3 `If-Match: <etag>`; native append of a new frame at the current length);
3. **read-after-write consistency**;
4. **ranged read** (returning the requested byte range with correct bytes);
5. **presigned URL / short-lived scoped credentials** (for direct bundle/assets reads, ADR-030);
6. **Atomic conditional delete** (S3 `DeleteObject` + `If-Match: <etag>`; native append of a tombstone at the current length): owner/lease release depends on it (ADR-134/189).

**Startup probe**: Each node first runs provider-specific preflight. For COS, it reads `X-Cos-Bucket-Az-Type` from `HEAD Bucket` and explicitly rejects `MAZ`, because Tencent COS does not support APPEND Object on multi-AZ buckets. It then verifies conditional create, duplicate-create rejection, CAS, stale-CAS rejection, ranged reads, stale conditional-delete rejection, and current conditional delete. Any failure terminates startup. `cellhive diagnose` repeats the same **write-based** probe on a running node; it is not read-only. This probe does not replace independent concurrency/linearizability or presigned-URL qualification.

## Providers

| Provider / interface | Authority status | Notes |
|---|---|---|
| Local filesystem (empty `CELLHIVE_BUCKET`) | Local single-node development | Not a cloud multi-node authority |
| S3 API (`s3://<bucket>`) | Verify each instance | "S3-compatible" alone does not guarantee atomic owner CAS and delete |
| Pinned local MinIO (`RELEASE.2025-02-18T16-25-55Z`) | ❌ | 2026-09-22: create/CAS/range passed but stale `DeleteObject If-Match` returned success; the complete integration probe fails |
| Native OSS (`oss://`) | Tested Beijing bucket passed; verify each other bucket | Isolated concurrent claim/CAS/delete and startup diagnose passed. Same-host process `SIGKILL` followed by fresh-directory takeover: 5,000/5,000 writes ACKed at 192 clients; 6,470 ACKed values recovered exactly (missing=0, wrong=0), SQLite integrity=ok. |
| Native COS (`cos://`) | Tested Hong Kong `cell-1376795072` bucket passed; verify each other bucket | Isolated concurrent claim/CAS/delete and startup diagnose passed. Same-host process `SIGKILL` followed by fresh-directory takeover: 5,000/5,000 writes ACKed at 192 clients; 6,488 ACKed values recovered exactly (missing=0, wrong=0), SQLite integrity=ok. Earlier `vwork-hk-1376795072` returned 405 for AppendObject and remains unqualified. |
| COS/OSS S3 endpoints tested on 2026-09-22 | ❌ for those configurations | COS accepted duplicate `If-None-Match:*` PUT; OSS returned `NotImplemented` on the first PUT. Do not generalize to untested accounts/endpoints |

## Bucket Namespaces (Current Single Bucket)

The implementation currently configures one `CELLHIVE_BUCKET` with one endpoint/region/credential set. Reserved prefixes isolate the following subsystems inside that bucket and cannot yet be split into separate buckets:

| Prefix | Content |
|---|---|
| `cells/`, `nodes/`, `node-logs/`, `fleet/` | cell replication, owner/lease records, node logs, and fleet control records |
| `bundles/`, `deploys/` | content-addressed Worker bundles and version pointers |
| `assets/` | static assets |
| `r2/` | tenant R2 virtual buckets |

Independent `state/code/assets/r2/backup` bucket roles and per-role credentials are not implemented. There is also no independent backup/archive write path.

## Key Layout (Example)

```
cells/<ns>/<class>/<id>/own.json
cells/<scope>/ltx/e<epoch>/<level-4hex>/<min>-<max>.ltx
cells/<scope>/ltx/e<epoch>/0009/<min>-<max>.ltx
nodes/<node>.json
node-logs/<node>/<session>.json
fleet/capacity-v1.json
fleet/drain-token.json
bundles/sha256/<aa>/<hash>
deploys/<ns>/<worker>/<version>.json
assets/<ns>/<worker>/<token>/<path>
r2/<ns>/<bucket_name>/<object-key>
```

**Owner physical layout (current canonical format)**: `cells/<scope>/own.json` is the only owner object. COS/OSS applies conditional create, CAS, and conditional delete directly to that key; no `owner.json.w<window>` or other owner sidecar is produced.

## Credentials

- **Long-lived credentials are only in `cell-agent`**;
- `do-runtime`/`user-runtime` **do not hold** long-lived credentials;
- Bundle/assets reads: a runtime first asks cell-agent for a short-lived read-only presigned URL, then reads bytes directly from object storage; local-filesystem or unsupported-presign backends fall back to the authenticated internal point-read path without giving the runtime long-lived credentials (ADR-030);
- The admin backend never touches bucket credentials (only via cell-agent).

## Lifecycle and GC

- **Retained versions**: bundles are deduplicated by content addressing; version deletion goes through lifecycle (reference counting/`deploys` pointer cleanup);
- **LTX**: background **compaction → L1** for L0 segments (`min_txids`/`min_mb`); large cell **paging** (sparse files on demand);
- **Snapshots**: L9 snapshots are used for takeover/large databases; generated when checkpoints are aligned;
- **Cleanup**: worker/app deletion uses soft delete + `purges` jobs (ADR-131); **purge only takes effect for entities that are still soft-deleted**, and deploy/create cancels the job (ADR-135); each cell-agent performs idempotent deletion of local replicas and bucket objects in bounded, paginated passes (ADR-142/169);
- **Enumeration**: operations commands only, with bounded pagination; **LIST is prohibited on hot paths** (ADR-266 has one exception: when a single authority logical key misses in the current window, one bounded lookup of that key prefix recovers the previous/last long-idle segment; this is not a bucket-wide LIST).

## Performance

- Buckets should be in the **same region/nearby**; in fleet mode, S3 uploads are **batched in the background and not on the write ack path**;
- The number of object PUTs per ack should be much less than 1 (group commit);
- Only single-node degradation waits for the bucket per record (~90ms).

## To Be Refined

- Compatibility and TTL of presigned URLs across providers;
- Lifecycle rules and cost model;
- Independent bucket roles and credentials, plus the backup snapshot/archive strategy;
- Other cloud stores/accounts: atomic write/delete semantics, concurrent races, and presigned reads (Class C environment).

_Last updated: 2026-09-25_

### Native protocol acceptance audit (2026-11-09)

The reference is pinned celld 90b43017241f81189453d326d05948f388b34652. Current Go/Rust header/page interoperability, native delta WAL coordinates, snapshot recognition, levels 0/1/2/3/9, UTC 30s/5m/1h schedules, exact-cut epoch continuation, additive compaction, and real COS level promotion have direct source/test evidence. The strict 100k three-owner-crash run passed, including cold verification with the standby stopped and no fallback URLs. See the Chinese audit table for individual evidence and limits.

NoChecksum is the standard flag used by reference celld ordinary sync and replica_compactor. Its separate handoff snapshot_to_writer also supports rolling post-apply snapshot checksums; CellHive reads that native variant, while its current writer uses NoChecksum. File CRC64 is always retained. Do not claim every celld writer uses NoChecksum.

Actual HTTPStore/direct replica DO paths now use streaming ProvenWatermark, HEAD/Range with size and ETag, and chain=1 for cross-epoch page recovery. Generic Restore/RestoreChain and external SegmentSource fallbacks can still buffer bodies; index memory scales with page count.

Final make test/build/vet passed. The latest binary recovered 100/100 values after owner SIGKILL and after local SQLite removal on HK COS; cold first GET was 1.035300s (/tmp/native-do-proof-final-batch.log). The strict 100k run used the earlier snapshot-clock-fixed binary; the new HTTP/proof paths separately passed real COS 8.48MB cross-epoch recovery with identical SHA (/tmp/native-cos-http-stream-proof.log). These are distinct binary acceptance runs.

The previously requested general cell SQLite export/import product entry point is still outstanding, separate from these six protocol-alignment requirements. The successful one-off control-database migration does not establish completion of that feature.


### Large-cell resource budgets (2026-11-09)

Native LTX format is unchanged. Adjacent-level rounds select at most256 inputs/64MiB; one indivisible large source runs alone. OptionalL9 checks the latest native header before payload reads (80MiB generic default, existing256MiB policy on owner release); over-budget logical size or full-chain inputs skip without partial publication or predecessor deletion.

Maintenance, native proof, restore/index construction and file upload have process concurrency limits. Long plans use bounded versioned ranges; materialization caches at most8MiB decoded pages with1MiB encoded requests, preserving source/ETag and sparse range boundaries. Production DO paging errors do not fall back to whole bodies. CRC remains mandatory; buffered disk IO and chunked index comparison reduce overhead. Final page-index memory still scales with page count.

A disposable64MiB/256-entry local disk LTX cache contains only newly successful publications and requires remote size/ETag equality before reuse; it is neither authority nor remote metadata. See the [10GiB acceptance report](../reports/2026-11-09-large-cell-storage.md). Mandatory capture retains a configurable default8GiB snapshot limit; storage-level acceptance does not imply default capture accepts10GiB.

Cache review: only existing snapshot/compaction file spools populate the local cache after successful new publication. Ordinary byte deltas do not add cache filesystem writes; a dedicated regression enforces this hot-path boundary.
