# Release Notes

_Last updated: 2026-10-28_

Current production version and the two most recent releases. Full release records
(image, Helm revisions, acceptance evidence, residual warnings) live in
`docs/archive/release-*.md`.

## 2026-10-28 — pinned KV resource-id fix

- Source `49133b1`; image `sgccr.ccs.tencentyun.com/vwork/cellhive@sha256:c65a26b74296ecd78a127bc0a12fc8c03938dd7485da677f559716786d5175eb` (tag `49133b1`).
- SG Helm revision **79**, USW **63**; every workload Pod runs the new imageID, Ready, `restartCount=0`.
- Fixes: the loader signs `b.id` for pinned KV bindings instead of the binding key (ADR-302); `scopedtoken` rejects delegated/non-KV claims carrying `Resource`.
- Acceptance: per region, 20 direct KV PUT/GET pairs plus 10 Worker binding PUT/GET pairs with `name=KV,id=main`; temporary app deletion returns 200.
- Details: [release-2026-10-28](archive/release-2026-10-28.md).

## 2026-10-27 — COS recovery, bindings and queue hardening

- Source `48216e7`; image digest `sha256:c2865514fcee9f12f8491038fd69791dab9ec157da60d57385fd59b86263bd0d`.
- SG Helm revision **71**, USW **56**.
- Includes COS empty-read observation, sealed follower recovery batching, COS background upload batching, pinned-version authorization refresh, Queue acknowledgement/isolation, and Workflow event/transaction fencing.
- Evidence and residual warnings: [release-2026-10-27](archive/release-2026-10-27.md).

## Earlier

- The 2026-09 “unreleased (main)” feature list is archived at [release-notes-2026-09-main](archive/release-notes-2026-09-main.en.md) for history only; those features have since shipped.
- Point fixes are recorded under `docs/archive/` by date.
