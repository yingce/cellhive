# OSS / MinIO actual-provider acceptance, 2026-11-10

Initial findings below are historical; the follow-up section records the fixes and successful reruns.

## Initial probe, before approved changes

All CellHive binaries were freshly built from `/opt/cellhive-worktrees/celld-ltx-migration`, using the five SQLite feature tags. Production source was not modified. Artifacts: `/tmp/cellhive-oss-minio-acceptance/`. This is provider-focused acceptance, not full fleet E2E acceptance.

## OSS Beijing: llmfs-bj

- Native append, startup storage diagnosis, provider list: PASS.
- CHA3 authority: PASS, including concurrent single-winner create, rollover, stale CAS rejection, generation retirement, delete/recreate, cold authority reads and recovery after bootstrap/seal/genesis/descriptor publication interruptions.
- Native LTX snapshot upload, full SQLite restore (500 rows), compaction, page-fetcher materialization (500 rows): PASS. No-op compaction baseline payload GET=0.
- Tests used unique object keys. Existing COS CHA3 and S3 replica test bodies were adapted via Go overlays in the artifact directory to use the production OSS provider; they are not permanent additional repository tests.
- An old opt-in `TestProviderResourceFence` failed because it still requests removed `resource-fence.json` behavior. This does not establish an OSS provider defect; the failure remains recorded in `oss-contract.log`.
- SQLite HTTP import/export E2E: BLOCKED at agent startup. Existing `nodes/chchaos454797c2-oss-a.json` fails current authority decoding (`invalid authority frame length`). No old bucket records were deleted. The same credentials can read/write objects but cannot create a bucket: `oss:PutBucket` returned 403. A clean authorized test bucket or explicit scoped legacy-test cleanup is needed before claiming full E2E acceptance.

Evidence: `oss-authority.log`, `oss-replica.log`, `oss-transfer.log`, `/tmp/cellhive-e2e.RXo28M/transfer.log`.

## Local MinIO Docker

| Image | Reported version | Result |
|---|---|---|
| `bitnamilegacy/minio:2025.7.23` | DEVELOPMENT.2025-07-23T15-54-02Z, commit 7ced9663e6a791fef9dc6be798ff24cda9c730ac | Authority contract FAIL; replication/restore PASS; startup correctly rejected |
| `pgsty/minio:latest` pinned to digest below | RELEASE.2026-08-04T00-00-00Z, commit d88f46ccee345a9c2fabe2d221d9a9e56bc11aec | Same result |

Image digests:
- `sha256:8935e75fa5d11295c17171e4aa49efe390a1193cd7f12e4d21b92af9ffef09d7`
- `sha256:b6bfe7239bfc83fb90d31612d9704d86039dd714f7904b3f1ad68f211e602372`

Both servers ran in isolated containers bound to localhost (29900 / 29920). `scripts/s3-integration.sh` ran against each endpoint. Conditional create, CAS and ranged read passed, but stale conditional DeleteObject returned success instead of ErrPrecondition. The production cell-agent refused both endpoints with `bucket owner authority: conditional delete (reject-stale): expected precondition, got <nil>`.

Both passed the isolated LTX restore/compaction/page-materialization test with 500 rows. This does not make them safe owner authority stores. HTTP import/export and owner-takeover E2E were not bypassed past the failed startup gate.

Official MinIO Quay and Docker Hub image pulls failed; the two successfully pulled images above were used instead. No source-built MinIO was used. Containers were stopped after testing; evidence and images were retained.

Evidence: `minio-contract.log`, `minio-new-contract.log`, `minio-startup.log`, `minio-new-startup.log`.


## Follow-up: approved conditional PUT adapter and successful reruns

User explicitly approved a safe conditional PUT logical-delete adapter for MinIO. The production `s3://` factory now returns `S3ControlBucket`. Owner/owner-gen, node leases, node-log coordination, fleet election/drain and wake records use their existing keys. A CHC1 frame has a 21-byte header (magic, deleted flag, random revision) and the original payload. Every write changes the revision, including identical-value writes. Conditional DELETE is an atomic If-Match PUT tombstone; recreation CASes that tombstone. No read-then-unconditional-delete fallback exists. Control roots are not physically deleted. LTX/bundle/asset bytes, paths and streaming are unchanged; COS/OSS retain their native append adapter.

Bounds checks reject a live control record before publication if its encoded size would exceed the read cap. Logical Get/Stat/range/stream/list paths hide tombstones. A physically nonempty but logically empty page retains its continuation cursor. Presigned reads of enveloped control records are disallowed. Unsupported non-control conditional deletion returns ErrNotSupported: this does not claim arbitrary physical-GC support on MinIO. Tombstones consume one retained object per deleted control key, and wake history can add listing work; no automatic physical tombstone reclamation is introduced. Do not apply lifecycle deletion rules to authority records. Existing raw S3 control records require offline migration or an empty test bucket, never mixed old/new writers.

### Additional bug reproduced and fixed

The first MinIO resource delete/recreate test returned 503, which the old script misleadingly labeled as data reappearing. The response was XMinioInvalidObjectName. A released owner retains its epoch/retirement lineage but has empty node/session. ClaimWithReset incorrectly invoked predecessor recovery for that empty identity, requesting `node-logs//.json`. The fix skips predecessor recovery only when both node and session are empty; local reset, epoch increment, retirement lineage and CAS remain enforced. The regression test failed before the fix and passed afterward. Actual dead predecessor recovery is still exercised by SIGKILL tests.

### OSS test-bucket maintenance

The existing llmfs-bj object credentials worked; only bucket creation was forbidden. The obsolete test node records, old `cells/__platform__/__control__/main/` test control database and old fleet drain/waker objects were archived locally before exact-key removal. Old node frame CRCs and expired leases were checked; backups were compared against a fresh read before deletion. No whole-bucket purge was performed. Backups and inventories are under `/tmp/cellhive-oss-minio-acceptance/old-{node,platform,fleet}-backups/` and `cleanup-*.log`. This resets old test control metadata; it is not an in-place production migration or compatibility reader.

### Successful acceptance

- Both Docker MinIO images listed above: production control adapter concurrency, same-value/recreate ABA, stale CAS/delete rejection, tombstone filtering/cursors and startup Diagnose PASS. Native raw DELETE remains unsafe; that historical failure is preserved. `scripts/s3-integration.sh` now exercises the production control contract, explicitly skips the separate raw DELETE contract in that mode, and pins the tested pgsty image digest.
- OSS native append/CHA3 concurrent create, rollover, retirement, four publication-fault recovery stages, list and removed-sidecar rejection PASS (`oss-final-contract.log`). The shared CHA3 test is now permanent rather than an OSS-only overlay.
- OSS production-provider LTX restore/compaction/page-materialization: 500 rows PASS, no-op baseline payload GET=0 (`oss-data-final.log`).
- Both providers: SQLite export, integrity/value checks, import into new cell, duplicate import rejection, post-import write, local-cache removal/cold recovery, re-export, plain SQLite BLOB/index migration PASS (`oss-transfer-fixed.log`, `minio-transfer-final.log`).
- Both providers: 100 acknowledged writes, owner SIGKILL, peer takeover and cache-wiped cold restore: 100/100 PASS (`oss-batch-fixed.log`, `minio-batch-final.log`). Observed first cold request: OSS 2.844537 s, local MinIO final run 0.080173 s. Different network topologies; these are not comparative throughput benchmarks.
- Both providers: forwarding, auth rejection, resource delete/recreate and old-data absence PASS after the released-owner fix (`oss-routing-repaired.log`, `minio-routing-repaired.log`).
- MinIO: Workflow sleep, timeout, retries and R2 metadata PASS; stale callbacks rejected with 409; 16 concurrent waits consume distinct events; SIGKILL runtime+agent after durable consume before response then restart from fresh local directory restores all results including 42 (`minio-workflow-final.log`).
- Final full Go run: 77 packages PASS, zero failing packages, exit 0. Focused bucket/owner race tests and full vet PASS. Evidence: `final-full-go.log`, `final-race.log`, `final-vet.log`. Conditional provider/native runtime tests remain opt-in; the real provider gates are listed separately above. Final source/binary hashes are in `final-sha256.json`.

No workerd rebuild, commit, deployment or 10 GiB / 100-epoch benchmark. Current CellHive changes are exclusively in the migration worktree. E2E processes were cleaned up; test Docker containers are stopped after acceptance, with logs/images retained.
