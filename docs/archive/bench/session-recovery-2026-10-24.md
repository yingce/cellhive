# Session recovery: persistent identity and restart gate

Date: 2026-10-24. Reference: celld checkout `90b4301`; ADR-285.

## Problem and red tests

The old node-log stored follower URLs. After a Pod IP changed, recovery called the old address. The automatic runner skipped an entire node if its new process had a live lease, leaving old sessions open/recovering. A seal request to a missing session could create an empty seal and return final=0.

Regression tests failed before the corresponding fixes:

- `TestRunnerRecoversOldSessionOfLiveNode`: old session remained open.
- `TestRecoveryFollowsIdentityAfterAddressChange`: connection refused at old URL.
- `TestPeerRejectsWrongPersistentIdentity`: wrong persistent disk returned HTTP 200/final=0.
- `TestIdentifiedSealRejectsMissingLog`: absent retained log accepted as empty proof.

## Implementation

- v3 node-log stores original URLs plus each follower's node/instance identity. Prepare/fsync an empty follower session before publishing the recovery coordinates; missing log is never inferred to be an empty tail.
- Bind HTTP requests and persistent stream handshakes to the expected node/instance, requiring matching response headers. Reusing a URL for another disk fails; a new address for the original identity can continue the same ensemble using cached membership (no per-write bucket lookup).
- Recovery point-reads `nodes/<node>/recovery/<instance>` or, when absent, the node lease. The recovery endpoint is not membership or liveness. Both the persisted identity and the receiving server must match.
- Under the data-directory flock, start a recovery-only listener and publish the recovery address. Recover old local sessions before publishing the normal lease and serving business/new appends. Failed recovery retries with 1–30s backoff and keeps readiness false. Health remains available so a large recovery is not killed by the startup probe.
- The independent recovery worker shares waker election but does not block timer dispatch. It checks sessions individually, with bounded attempts and 1–60s backoff. Current live session and uncertain lease reads fail closed.
- After each uploaded tail page, CAS `recovered_seq`. Verify contiguous sequence through final and only then CAS covered_seq/sealed. Terminal sealed state cannot be moved back to recovering. Spool reclamation follows durable seal.
- Headless Service publishes not-ready addresses so simultaneous boots can reach retained disks; business requests to a recovering Pod return 503.

## Verification

All completed with exit 0:

- `make test`
- `make vet`
- `make build`
- `go test -race -tags 'sqlite_fts5 sqlite_dbstat sqlite_math_functions sqlite_column_metadata sqlite_preupdate_hook' ./internal/recovery ./internal/nodelog ./internal/lease ./internal/peer ./internal/server ./cmd/cell-agent`
- `make helm-lint` and `make k8s-render`
- `bash scripts/e2e/session-recovery.sh`
- `OWNER_E2E_SECONDS=4 bash scripts/e2e/owner-lease.sh`

The real-process session E2E uses two separately launched cell-agent binaries, independent data directories, real HTTP KV requests, and an isolated shared FS bucket. It confirms both owners create v3 fleet session logs, then SIGKILLs both. After stopping writers, it restores only the two test cells' bucket LTX directories to their pre-write baseline and removes local cell databases/upload spools. This deliberately simulates the acknowledged-but-not-uploaded window; it does not claim to measure the natural upload race or real COS behavior. Follower spools and node-log records remain intact.

It restarts A at a different address with B offline, observes 503 readiness, kills A during recovery, then restarts it again. B is temporarily replaced by a same-name process with an empty data directory. The replacement publishes its distinct instance, but A remains not ready and its old session unsealed. After returning B's original disk at a new address, both nodes become ready. All 80 final KV values are read back exactly and both old session records have durable coverage and sealed status.

Final expanded run: **8.34s, 80/80 values correct**. Separate real-HTTP checkpoint test fails tail at sequence 256/300, creates a fresh recovery manager, uploads exactly the remaining 44 fragments, and seals at 300. HTTP and stream tests both reject wrong target identities without adding fragments.

Existing owner/restart E2E also passed: first write after same-directory restart 23.233ms, cold hydrate first write 16.140ms, SQLite integrity_check=ok. These are local FS observations, not SG latency promises.

An earlier broad run overlapped test-source changes (including an independent OTLP change in the shared workspace) and failed compilation/tests. The completed full run and race run above were repeated after the source stabilized; the failed intermediate run is not counted as acceptance.

## Compatibility and production boundary

No production writes, deployment, or SG historical-session repair were performed by this change. No commits were made for this task.

`NodeLeaseLiveness` changes to `node-lease-fence-v2-recovery-v3`. Stop old-version nodes before starting this version; do not roll one new node into a live old fleet. Old recovery writers interpret unknown node-log protocols as legacy Held recovery, so a compatibility fence is necessary. Startup refuses live incompatible peers.

v1/v2 records are readable but retain their original URLs. They do not acquire guessed identities or automatic old-IP aliases. Before upgrading SG, independently verify the original follower/PVC and the entire retained tail for session `1792144743099984471`, restore its recovery reachability under a reviewed mapping, and complete durable recovery. The last known head (22395) and retained log existence alone do not prove all contents. Do not delete the spool or manually mark the record sealed. Until this prerequisite is satisfied, the new startup gate may correctly keep the affected node unavailable.

Production COS recovery performance, real Kubernetes simultaneous restart, and DNS propagation timing remain deployment acceptance work; the local tests prove protocol behavior over HTTP and real processes, not production resolution or storage latency.
