# Local clai D1 incident — 2026-11-15

## Scope and evidence

This report concerns Docker project `cellhive-migration-test`, not SG/USW Kubernetes. Its S3 endpoint is local RustFS, not Tencent COS. The user authorized a maintenance window and a backed-up, offline database recovery.

Active control registration and workbench version 1140 bind `DB_WORKBENCH` to `platform/__d1__/DB_WORKBENCH`. `cai-workbench` is a distinct registration. Both current local files initially contained only universal cell tables; those tables do not prove D1-to-DO misrouting.

The historical pre-RustFS volume contains business data but is stale. It was not used for recovery. SQLite backup API copies of that database and the current empty database passed integrity_check.

RustFS epoch 57 contains a complete snapshot ending at 0x19038f and 137 subsequent objects. An isolated reconstruction with `restore.IndexChain` and `restore.ApplyFile` reaches TXID 1639454 (0x19041e), matching the last identified successful commit at 2026-11-14T15:04:11.617Z. Its integrity_check passes. Counts: user=3, session_index=31, task=119, runner_machine=4, asset=32, d1_migrations=16. This is the latest located durable chain, not a proof about every attempted/unacknowledged request.

Backups, chain object hashes, reconstructed DB and restricted deployment configs are under `/var/backups/cellhive/d1-incident-20261115-q92m7c6e`. Config files contain credentials and must not be published. Original live DB was retained after clean service stop, WAL/SHM absence verified, and the reconstructed file installed offline with preserved ownership and fsync. No bucket object was deleted. Live authenticated D1 reads subsequently confirmed the business tables and counts. Public root/health returned 200; user login acceptance is separate.

## Reproducible empty-file defect

`PendingTimerMin` used the ordinary create-capable SQLite DSN. If a repair scanner retains a path after owner invalidation removes its file, this purported read creates an empty database. `Store.Cell` sees the existing file and skips cold hydration. A new regression first failed because the absent file was recreated; changing the scanner to `mode=ro` makes it pass. Independent review also found the inherited `_journal_mode=WAL` could make a read-only scan of a DELETE-journal database fail. A new DELETE/WAL test reproduced the readonly-write failure; the scanner now uses a dedicated escaped file URI with mode=ro and busy timeout, without forcing journal changes. Additional coverage verifies scanner→Store.Cell actually hydrates a business sentinel, an open uncheckpointed WAL timer remains visible, and no schema/journal mutation occurs. immutable=1 is deliberately not used.

Incident logs show capture stopping at 15:04:12.249Z followed by wake-index repair at 15:04:12.927Z, consistent with this race; no per-file trace proves the precise historical interleaving. The fix is local and is NOT in the deployed old image. Temporary runtime mitigation disables `CELLHIVE_WAKE_REPAIR_INTERVAL` (0s). Normal timer dispatch remains, but automatic repair of missing wake hints is suspended until the patch is released.

## Lease/storage instability

RustFS logged a 492-byte node lease PutObject still pending in its store layer after 5000ms at 15:52:27.737Z, and another CAS at 3933ms. The agent then expired/fenced at 15:52:28.689Z. Basic container-network health probes were sub-millisecond; these are object-store processing stalls, not proven DNS/TCP failures. Capacity scanner timed out after 512560 files/15034ms; filesystem 90% full. These are leads, not proof of a specific disk/lock root cause.

Runtime lease TTL increased from default 10s to 60s as a reversible mitigation. Expiry fencing remains enabled; failover can take longer. This is not a storage root-cause fix. Current compose configuration is the restricted `compose-recovery-guarded.json` in the incident directory; do not accidentally roll back to the prior file without preserving post-recovery writes.

## Post-recovery checks

After the guarded agent replacement at 2026-11-14T16:19:40Z, restart count remained zero through the >5 minute observation; logs contained no lease publish failure or permanent fence and durable workbench TXIDs advanced beyond 1640107. A second real authenticated D1 query after this restart returned users=3/tasks=119. Public curl root and health returned HTTP200 with expected workbench health body. The Python default User-Agent monitoring requests were rejected by Cloudflare with HTTP403/error1010; they must NOT be represented as a successful continuous public health test. A subsequent request using the curl User-Agent returned 200. User login remains unverified by the agent.

Full regression: cellstore 3.072s, server 46.316s, cmd/cell-agent 2.615s; all passed. No patched binary released.

## Remaining acceptance

- Continuous lease/public-request observation after final mitigation and no new restarts.
- User-confirmed login and business access.
- Independent review/release of scanner fix, then re-enable wake repair.
- RustFS store latency root cause and remediation; do not claim health200 or longer TTL proves it solved.

## Follow-up verification and tooling deviation

The follow-up workspace changes add fail-closed capture history lookups and readiness-triggered public worker retirement. The timer read-only URI also required `OmitHost: true` to preserve relative-path support. Final uncached `make test` and all six `make e2e` suites passed in private network/PID namespaces with cleared credentials and explicit paths under `/tmp/kilo/timer-verify.Buxtxn`. No patch was committed or deployed. Local activation trust and internal dispatch lifecycle/admission remain separate unresolved design work.

The first verification runner incorrectly expanded its output path to `/bin`, which resolves to `/usr/bin` on this host. It wrote 14 build outputs at 2026-11-15 01:15:49 through 01:16:07 +0800: `cell-agent`, `cellhive`, `user-runtime`, `do-runtime`, `cellbench`, `realbench`, `sqlbench`, `s3init`, `s3probe`, `walscan`, `gatebench`, `kvbench`, `restoreverify`, and `recoververify`. Prior contents/existence were not recorded, so overwrite impact cannot be established. These files were not deleted or restored speculatively. The deviation was disclosed; subsequent verification used checked literal temporary paths. This must not be described as an entirely filesystem-isolated operation.
