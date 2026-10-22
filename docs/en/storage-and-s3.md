# Object Storage

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
cells/<ns>/<class>/<id>/owner.json
cells/<scope>/ltx/e<epoch>/<segment>
cells/<scope>/snapshot/<txid>
nodes/<node>.json
node-logs/<node>/<session>.json
fleet/capacity-v1.json
fleet/drain-token.json
bundles/sha256/<aa>/<hash>
deploys/<ns>/<worker>/<version>.json
assets/<ns>/<worker>/<token>/<path>
r2/<ns>/<bucket_name>/<object-key>
```

**Provider authority physical layout (ADR-266)**: on `cos://` / `oss://`, logical authority keys (`owner.json`, `owner-gen`, `nodes/<node>.json`, `node-logs/...`, `fleet/waker.json`) map to time-segmented `.w<window>` objects, e.g. `cells/<scope>/owner.json.w<window>`. The API keeps the logical key; the provider console shows the segment keys. Each segment is a `CHL2` frame log read from its tail, and old segments are physically deleted at rollover; normally only the current and previous windows exist.

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