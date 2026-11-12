# SG authority reclamation deployment — 2026-11-13

SG deployed the same agent image validated in USW:
`sha256:e9f8511a7a45077ea57c832740deab8f08d71c0b48a6ea9580e6dc7c29f36cd8`.

- Helm 134: validate mode, both actual agent images/modes/Ready verified before enabling deletion.
- Helm 135: reclaim mode plus all-readers-validated acknowledgement; both agents Ready, zero container restarts. Runtime images unchanged.
- Bucket node inventory found exactly two live SG cell-agent nodes; namespace has no Jobs/CronJobs.
- bucket/config/lease full package race tests passed before deployment.
- Isolated SG COS probe passed: validate retains g0, reclaim deletes g1, cached validate reader sees g2, stale token rejected. Unique probe keys cleaned synchronously. Reused probe prints a historical USW label, but credentials, bucket and endpoint were explicitly set to SG.
- Startup log checks showed reclaim storage diagnosis success and no lease publish failures or fencing. This is short observation, not long-duration acceptance.
- Final Helm diff empty. No public counter function invoked. The attempted public /healthz path returned 404 and was not counted as a health test; Pod /readyz probes are passing.

Remaining boundaries: historical backlog and failed-delete retry sweeps are not enabled; permanent roots still grow. Safe rollback is validate mode with this or a newer compatible binary, never legacy or an old binary after reclamation.

Safe validate values: `/var/backups/cellhive/sg-gn-20261113/values-validate.yaml`.
Evidence: `/tmp/cellhive-sg-gn-{regression,validate-upgrade,reclaim-upgrade,final-diff}.log`.
