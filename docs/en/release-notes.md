# Release Notes

_Last updated: 2026-11-01_

Current production version and recent releases. Full release records
(image, Helm revisions, acceptance evidence, residual warnings) live in
`docs/archive/release-*.md` (Chinese source).

## 2026-10-31 — recovery/retention hardening and dispatch hotfix

- `20261031-02` (source `7be1102`, digest `sha256:ee6df63604fb0e0c9551037a39a43511b4b7a26440cb6cf62b423260e39745ae`, SG rev **85** / USW **69**): atomic restore, paged fail-closed, workflow prune/retention batching, objgc pre-delete recheck, owner epoch floor and node-lease touch, queue ack batching/claim index, R2 Stat/multipart metadata, cached muxes, projection singleflight, KV TTL fast path, loader resident scan, bounded host maps and indexed HostView.
- `20261031-03` (source `c9ba0fd`, digest `sha256:f3a7cf6b1e515e8a94e205f9dc89fa70f2d9d615b7e28d536e885d07b6022159`, SG rev **86** / USW **70**): invocation persistence is best-effort after a successful handler; a finish-write 503 no longer turns `scheduled`/queue/workflow success into a retryable 500.
- `20261031-01` (source `bd2c5d7`, digest `sha256:b437c8ce1aa59bcb70133dde34fa0bfc9c270bbfc386872d65286695564d2ea2`, SG rev **84** / USW **68**): `CellHiveLifecycle` status/retire control RPCs are filtered from tenant Tail/function errors.
- This does **not** fix the `vwork-40/vlog-daily-report` tenant cron: its handler still throws `Feishu send failed with HTTP 200`; that needs a tenant-side response check or `controller.noRetry()`.
- Evidence: [release-2026-10-31-03](../archive/release-2026-10-31-03.md), [release-2026-10-31](../archive/release-2026-10-31.md).

## 2026-10-30 — runtime retirement and serialization hardening

- `20261030-01` (source `951ee79`, SG rev **82** / USW **66**) and `20261030-02` (source `251725a`, SG rev **83** / USW **67**): tighter upload spool/eviction/staticgen/session locking, dead-code cleanup, and the runtime retirement plus ownership-recovery race fixes.
- Acceptance: both regions on new digests, Ready, `restartCount=0`.

## 2026-10-28 — pinned KV resource-id fix and cleanup

- First rollout: source `49133b1`, image tag `49133b1`, SG rev **79** / USW **63**.
- Same-day cleanup image: source `b8e01fa`, tag `20261028-01` (digest `sha256:627b935015f0c28cfdce6960ae7575cfc1ad541b7f7696d49350d203d260c8a2`), SG rev **80** / USW **64**; local Compose updated too. All workload Pods/containers Ready, `restartCount=0`.
- Fixes: the loader signs `b.id` for pinned KV bindings instead of the binding key (ADR-302); `scopedtoken` rejects delegated/non-KV claims carrying `Resource`. The cleanup commit removes dead code and refreshes docs.
- Acceptance: per region and locally, 20 direct KV PUT/GET pairs plus 10 Worker binding PUT/GET pairs with `name=KV,id=main`; temporary app deletion returns 200.
- Details: [release-2026-10-28](archive/release-2026-10-28.md).

## 2026-10-27 — COS recovery, bindings and queue hardening

- Source `48216e7`; image digest `sha256:c2865514fcee9f12f8491038fd69791dab9ec157da60d57385fd59b86263bd0d`.
- SG Helm revision **71**, USW **56**.
- Includes COS empty-read observation, sealed follower recovery batching, COS background upload batching, pinned-version authorization refresh, Queue acknowledgement/isolation, and Workflow event/transaction fencing.
- Evidence and residual warnings: [release-2026-10-27](archive/release-2026-10-27.md).

## Earlier

- The 2026-09 “unreleased (main)” feature list is archived at [release-notes-2026-09-main](archive/release-notes-2026-09-main.en.md) for history only; those features have since shipped.
- Point fixes are recorded under `docs/archive/` by date.
