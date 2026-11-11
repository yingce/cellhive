# Runtime Go telemetry forwarding implementation plan

**Goal:** Replace the unshipped per-Pod Vector readers with Go-owned bounded disk buffering and batch OTLP export to the existing CellHive Vector service.

**Architecture:** Trusted host log/span frames enter the existing supervisor pipe/spool. One background exporter per spool reads complete records, converts them to OTLP protobuf, sends bounded HTTP batches, and atomically persists its acknowledged file/offset. Cell-agent continues using its Go OTLP exporter. No VWork resources are changed.

**Tech Stack:** Go standard library and existing OTLP protobuf dependencies; existing workerd host JS and Helm chart.

## Boundaries

- Business durability gates remain unchanged; telemetry is best effort and cannot block request admission.
- Disk retains at most eight 16 MiB files; overwriting unconsumed telemetry under pressure is permitted and counted. Pod deletion loses emptyDir logs.
- At-least-once retry within retention can duplicate records after partial delivery or a crash before cursor persistence.
- One HTTP request at a time, bounded response/body timeout, exponential retry, cancellation on shutdown.
- Trusted span frames preserve original IDs and sampled trace relationships. Legacy embedding without a spool retains the existing cell-agent path.
- No deployment or commit in this task. VWork Vector remains untouched.

## Tasks

- [x] Implement bounded OTLP reader/exporter, persisted cursor, failure accounting and lifecycle integration; test outage, restart, partial lines, response stalls, rotation, OTLP partial rejection.
- [x] Route host spans through the existing trusted local pipe and test fallback behavior.
- [x] Replace sidecar Helm wiring with Go spool/export configuration; synchronize only reviewed chart files and remove native port6000 receiver.
- [x] Update current docs and deployment script endpoint; run Go/race/JS checks and real Vector acceptance with outages.

Acceptance: `docs/reports/2026-11-11-runtime-go-export.md`. Local only; no deployment.
