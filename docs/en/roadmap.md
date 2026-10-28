# Roadmap and remaining work

_Last updated: 2026-10-28_

The historical roadmap (2026-09, P0–P5 exit criteria) is archived at
[roadmap-2026-09](archive/roadmap-2026-09.en.md). P0–P5 are delivered; this page
tracks current status and remaining work only.

## Current status

- Compute: the `cellhive-workerd` fork (ADR-264); CellHive integrates through
  `workerLoader`, bindings and capnp and does not patch workerd in this repo.
- State: SQLite plus object storage are authoritative; owner/epoch/lease,
  RPO=0 and fleet replication are live.
- Production: SG Helm revision 79, USW 63 (source `49133b1`); see
  [release notes](release-notes.md).
- Functional and compatibility boundaries: [compatibility matrix](compatibility-matrix.md),
  [bindings](bindings.md), [known issues](known-issues.md).

## Remaining work (by risk)

1. **Cold-recovery latency**: real HK COS cold GET is still ~1.1–3.1s. Control
   cell cold authorization, owner/claim waits and COS tail latency are not fully
   separated. Re-measure in-region fleet, public entry and different spool sizes.
2. **Fleet read/write tail**: binding/lease/resource-fence refreshes still cause
   periodic 58–150ms tails; the SIGTERM lease-watchdog handoff race is unfixed.
3. **Cross-host / multi-AZ failure**: k3d A→B→A and A-B-C pass; real cross-host
   RTT, cross-AZ COS, concurrent epoch fences and backup restore lack production
   acceptance.
4. **Object-store matrix**: COS/OSS must pass native append and conditional
   delete per bucket; the pinned MinIO image is not an owner/lease authority.
   Other providers are unverified.
5. **Durable Objects**: rare connection/address recovery after rolling updates
   and recovery-pointer CAS warnings need root cause and automatic-recovery
   verification; no real-COS failure-window drill.
6. **Workflow / Queue boundaries**: Workflows still lack cross-Worker support and
   step history/progress; Queue still has cross-batch ordering and some retry/DLQ
   boundaries.
7. **Hyperdrive remainder**: per-resource proxy/direct override, multi-replica
   config distribution, built-in `pg` CJS shim, and ProxySQL HA drills.
8. **Observability and docs**: Tail token renewal beyond 10 minutes is unverified;
   English/Chinese mirrors need continuous sync and historical data has not been
   fully audited.

## Verification principles

- Design changes update `docs/decisions.md` first and `docs/known-issues.md` for
  status.
- Anything not verified in a real environment stays Partial/unverified; local
  reasoning is not acceptance.
