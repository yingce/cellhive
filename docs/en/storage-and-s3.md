# Object Storage

## Hard Requirements (All Required)

1. **Conditional create** (`If-None-Match: *`);
2. **Conditional overwrite** (`If-Match: <etag>`, CAS);
3. **read-after-write consistency**;
4. **ranged read** (returning the requested byte range with correct bytes);
5. **presigned URL / short-lived scoped credentials** (for direct bundle/assets reads, ADR-030);
6. **Conditional delete** (`DeleteObject` + `If-Match: <etag>`): conditional release of owner/lease depends on it (ADR-134); compatible storage that ignores this header will **degrade to unconditional delete**, a known residual risk.

**Startup probe**: On each node startup, perform 4 conditional writes + one ranged read verification; stop if requirements are not met. Operators use `cellhive diagnose` (read-only) — covering conditional create/reject-create/CAS/reject-stale/ranged read/**conditional delete** (stale etag must be rejected + correct etag must delete + object must disappear, ADR-135).

## Providers

| Provider | Conditional Writes | Notes |
|---|---|---|
| S3-compatible object storage | ✅ | Must support conditional create/conditional overwrite/read-after-write/ranged read; confirm with the `cellhive diagnose` probe before deployment |
| **Local S3-compatible (MinIO Community Edition)** | ✅ | **Tested and passed in this repository** (RELEASE.2025-04-22, four conditional writes + ranged read); not officially certified for production; avoid `RELEASE.2025-09-06T17-38-46Z` (conditional create anomaly can cause the initial deploy to fail); run `diagnose` to confirm other versions |
| Storage that does not support the required conditional writes | ❌ | Stop if the startup probe fails |

## Bucket Roles (Default Single Bucket + Prefix, Splittable)

| Role | Default | Content | Splittable into Separate Bucket |
|---|---|---|---|
| `state` | primary bucket | cell replication, owner/lease, nodes, node-logs, drain-token | ✅ |
| `code` | primary bucket `bundles/`, `deploys/` | Worker bundle (content-addressed) + version pointer | ✅ |
| `assets` | primary bucket `assets/` | static assets (can be attached to CDN) | ✅ |
| `r2` | primary bucket `r2/` | tenant R2 virtual buckets | ✅ |
| `backup` | none | optional archive | ✅ |

Configuration: each role can specify its own `bucket / endpoint / region / credentials / prefix`; if unset, it falls back to the primary bucket + prefix.

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

## Credentials

- **Long-lived credentials are only in `cell-agent`**;
- `do-runtime`/`user-runtime` **do not hold** long-lived credentials;
- bundle/assets reads: cell-agent issues **short-lived read-only** presigned/scoped credentials by `(ns, worker, version)` (ADR-030);
- The admin backend never touches bucket credentials (only via cell-agent).

## Lifecycle and GC

- **Retained versions**: bundles are deduplicated by content addressing; version deletion goes through lifecycle (reference counting/`deploys` pointer cleanup);
- **LTX**: background **compaction → L1** for L0 segments (`min_txids`/`min_mb`); large cell **paging** (sparse files on demand);
- **Snapshots**: L9 snapshots are used for takeover/large databases; generated when checkpoints are aligned;
- **Cleanup**: worker/app deletion uses soft delete + `purges` jobs (ADR-131); **purge only takes effect for entities that are still soft-deleted**, and deploy/create cancels the job (ADR-135); physical SQLite files are retained until explicit cleanup; the cross-node cell-data cleanup hook is still not closed-loop;
- **Enumeration**: operations commands only, with bounded pagination; **LIST is prohibited on hot paths**.

## Performance

- Buckets should be in the **same region/nearby**; in fleet mode, S3 uploads are **batched in the background and not on the write ack path**;
- The number of object PUTs per ack should be much less than 1 (group commit);
- Only single-node degradation waits for the bucket per record (~90ms).

## To Be Refined

- Compatibility and TTL of presigned URLs across providers;
- Lifecycle rules and cost model;
- Snapshot/archive strategy for the backup role;
- Empirical testing of cloud conditional writes/conditional deletes (Class C environments, see [`testing.md`](testing.md)).

_Last updated: 2026-09-19_