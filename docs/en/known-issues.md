# Current issues, limitations and acceptance

_Updated 2026-11-10. This describes the migration worktree, not a deployed release or a live production inventory._

The chronological issue log is preserved in [historical records](../archive/known-issues-history-2026-11-10.en.md). Historical pending statements are not current tasks, and archiving does not automatically close unverified issues. The [Chinese current page](../known-issues.md) has matching status; contracts remain in [decisions](decisions.md).

## Verified fixes

- Local E2E 15/15, COS matrix 15/15 and examples 26/26 passed, along with Compose/Kubernetes fleet, OSS/MinIO, SQLite transfer and cold recovery. Rolling updates included transient HTTP 500 responses recovered by retry; this is not zero-error availability. See [coverage and limitations](../reports/2026-11-10-full-e2e-examples.md).
- Native DO data uses the verified shard-owner epoch instead of fixed epoch 1. Delayed former owners cannot overwrite successors. Ordinary LTX uploads do not rely on COS forbid-overwrite or additional publication files. See [publication fixes](../reports/2026-11-10-object-publication-fixes.md).
- COS assets now use same-key native append at position zero with a 16-byte length header. Empty/nonempty concurrent creation, 64MiB uploads and public static asset E2E passed; [failure and repair evidence](../reports/2026-11-10-cos-assets-atomic-create.md) is retained.
- Default supervisor namespace, Workflow binding alias, R2 HEAD, cold-route races and the WebSocket example were fixed and retested.

## Remaining boundaries

1. These changes are not deployed. Historical release versions are not current live observations.
2. Old LTX/fixed-e1/R2 sidecar layouts cannot be mixed with the new readers. Migrate databases through SQLite export/import. Raw COS assets require an empty destination or offline cleanup and reupload; redeploying identical content reuses the old token.
3. COS assets use authenticated agent reads instead of direct physical-object presigned URLs. Explicit logical Content-Length preserves the runtime small-file cache; initial reads depend on the agent. No performance improvement is claimed.
4. R2 immutable old bodies remain to protect readers and signed URLs. Safe online version GC is not implemented; physical usage can exceed logical usage.
5. S3/MinIO control-record CAS tombstones do not make raw conditional DELETE safe. Unsupported physical conditional deletion must not fall back to unconditional deletion. COS/OSS use native append authority; other buckets need their own probes. See [provider acceptance](../reports/2026-11-10-oss-minio-acceptance.md).
6. Local compute with real cloud storage does not prove production same-region or multi-host/multi-AZ behavior. The 100-epoch depth test is deferred. The latest full matrix did not repeat every optional large-DO combination; [historical native 10GiB evidence](../reports/2026-11-09-final-acceptance.md) remains available.
7. Product compatibility and Hyperdrive/performance follow-ups remain in [TODO](../todo.md), [bindings](bindings.md) and [compatibility matrix](compatibility-matrix.md).

See also [storage performance](../reports/2026-11-10-storage-performance.md) and [native DO import isolation](../reports/2026-11-10-native-do-import-exclusion.md).
