# Runtime file logs and Vector local acceptance — 2026-11-11

> Superseded transport: the unshipped Vector sidecar design was replaced by Go OTLP forwarding. Historical tests below cover the earlier reader; current acceptance is in `2026-11-11-runtime-go-export.md`.

Status: implemented locally; not committed or deployed to production.

## Behavior

Production user-runtime, do-runtime and do-supervisor launchers create a shared
per-parent asynchronous spool. Trusted host invocation/Tail records traverse the
existing child output pipe with a random host-only token. The parent validates
and strips the token; unframed output is a runtime diagnostic, never an attributed
tenant log. Queue: 256 entries, maximum 64 KiB encoded line; full queues drop
telemetry instead of blocking the pipe reader. Background file writes/syncs are
outside the request path. Maximum 8 x 16 MiB per stream; old files are pruned
before creating the next file, including after restart. Rotation failure cannot
create an unbounded number of files. Writer counters are in *-stats.json.

Tail remains queryable. File-exported events are marked to prevent a second OTLP
export during agent polling. User HTTP summaries retain request IDs, status,
method/path and trace IDs. Trusted native Tail supplies console, exceptions and
non-HTTP invocation summaries. All-mode file capture is independent of agent OTLP
mode. Tenant bindings do not receive the framing token.

DO had awaited span export before returning. It now schedules spans and best-effort
stats with waitUntil, with bounded transport deadlines; SQLite/capture/checkpoint
and output-gate durability remain synchronous. Logs use files; trace spans keep
OTLP. This change does not rewrite the native workerd fork.

## Real acceptance

- Isolated local Docker runtime, same optimized workerd built for the earlier 502
  correction, 2 CPU / 4 GiB, adaptive pool maximum 3 slots.
- Vector image: timberio/vector:0.58.0-alpine. File reader -> native Vector protocol
  -> collector using the checked-in vwork.log.v1 remap -> local JSON test sink.
- Native DO integration: span collector intentionally never responds; sampled DO
  returns HTTP200 before releasing collector, and trusted DO console reaches file.
- With file reader stopped, 32 concurrent Go clients invoke an empty Hello handler
  (no KV bindings). 27174 requests / 15 seconds, zero failures,
  1810.5 success RPS, P95 27.739 ms. This is a local availability
  regression, not a production throughput comparison; the earlier fixture had KV bindings.
- Reader restart, collector outage/recovery and saved-offset reader restart:
  90 unique invocation IDs + 90 console canaries delivered; no duplicates in these
  graceful restart scenarios. Crash delivery remains at-least-once, not exactly-once.
- With reader stopped, file rotation crossed 16 MiB; after restart all 240 invocation
  IDs and 240 console canaries arrived. Files increased from 3 to 4, every file <=16 MiB.
- Writer snapshot after the test: {"Written": 28164, "Dropped": 0, "Errors": 0}.

Initial pre-correction canaries showed console capture still gated by remote
OTLP mode despite file mode. The file-mode all policy was corrected, and subsequent
console, outage and rotation tests above used the corrected JS. Earlier evidence
is retained, not counted as console acceptance.

## Automated checks

Spool race tests cover split lines, forged framing, oversize lines, bounded memory,
blocked writer, failed disk open, retention errors, graceful restart and bounded
shutdown. All 167 platform/user/DO JS tests pass. Relevant Go integration suites,
vet and Helm checks are recorded with the final task result. The new DO test runs
against the actual fork executable, not a mocked JS context.

## Operational boundary

The Helm chart adds a Vector sidecar only when telemetry/fileLogs are enabled.
Logs and Vector offset/disk-buffer directories are emptyDir: container restart
preserves them, Pod deletion does not. File retention and bounded buffers can lose
logs during long outages; no log RPO=0 claim. Vector only forwards kind=log records;
unattributed runtime diagnostics remain locally inspectable. The native receiver
port6000 must be published with the runtime/sidecar release. No production image,
Helm release or bucket was changed during acceptance.

Evidence: /var/backups/cellhive/local-502-diagnosis-20261111/file-logs
Unit/integration logs: /tmp/filelogs-*.log and /tmp/do-filelogs-e2e.log.

Final checks: all 167 JS tests; Go cmd/user-runtime, cmd/do-runtime, cmd/do-supervisor, internal/doruntime, internal/userruntime, internal/userruntimepool, internal/staticgen, internal/workerdpool, internal/server and internal/logbuf passed. Spool race tests passed. Targeted go vet, Vector0.58 config validation, Helm lint/render and operational chart mirror check passed. Full repository E2E/disaster recovery was not rerun for this logging change.
