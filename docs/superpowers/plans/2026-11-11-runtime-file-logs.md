# Runtime file logging implementation plan

> Superseded transport: the unshipped Vector sidecar design was replaced by Go OTLP forwarding. Historical tests below cover the earlier reader; current acceptance is in `2026-11-11-runtime-go-export.md`.

**Goal:** Move user-runtime/DO invocation and console log export off per-request HTTP onto bounded asynchronous local files consumed by Vector.

**Architecture:** Trusted host JS emits framed JSON over the existing child output pipe. A process-random host-only token prevents tenant console output from forging attributed records. Go drains pipes into a bounded non-waiting queue, strips framing, and writes rotated NDJSON outside generation directories. Vector tails files with saved offsets and batches remote delivery. Trace spans remain OTLP, but DO schedules flush via waitUntil rather than awaiting it on the response path. Business durability gates remain synchronous.

**Tech stack:** Go standard library, shared workerd platform JS, Vector 0.58.

## Constraints
- Main work in /opt/cellhive; no workerd fork changes required.
- Existing dirty changes preserved. No production deployment or implicit commit.
- Best-effort logs: bounded memory/disk, count dropped entries, never wait for collector/disk on the child pipe reader.
- Native Tail ring remains queryable; file-exported records must not be exported twice by polling.
- Do not trust tenant-supplied namespace or frame markers without the host-only token.
- Logs use files; traces retain existing protocol. DO checkpoint/replication is unaffected.

## Tasks
- [x] Implement and test asynchronous spool: fragmented/oversized lines, tenant spoofing, queue pressure, disk errors, rotation/restart, bounded shutdown.
- [x] Integrate user-runtime (adaptive and regular), do-runtime and do-supervisor output; structured invocation and trusted Tail export; async DO spans.
- [x] Add Vector file pipeline/example deployment with file/offset volume, rotation and disk limits; preserve current log schema and trace pipeline.
- [x] Run unit/race/JS/integration tests, real workerd + Vector ingestion, stopped/restarted Vector, slow/failing sink, rotation and concurrency; update decisions/known issues/report.

Completed locally; evidence and limits: docs/reports/2026-11-11-runtime-file-logs.md. No production deployment or commit.
