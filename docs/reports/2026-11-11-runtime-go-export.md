# Runtime Go telemetry forwarding acceptance

Local implementation and acceptance only. No commit or production deployment.
This supersedes the earlier unshipped Vector-sidecar transport.

## Result

- user-runtime and DO trusted log/span frames enter the Go-owned bounded spool.
- Go reads complete lines, batches OTLP protobuf, consumes bounded responses,
  retries with backoff, and atomically saves acknowledged file/offset cursors.
- No per-Pod Vector process. CellHive Vector receives OTLP on4318, transforms
  logs to vwork.log.v1, and forwards traces. Cell-agent uses its existing Go OTLP
  exporter. The operations deployment script selects CellHive Vector again;
  this changes the next CellHive deployment, not the current running Pods.
- VWork Vector and its workloads were not changed.

## Validation

- Go spool race tests: framing/spoof rejection, queue pressure, retention limits,
  partial line completion, file rotation, outage/restart cursor replay,
  stalled response body with nonblocking writes/shutdown, OTLP partial rejection.
- Real Vector0.58.0 Docker: three successful runs each delivered100 individually
  validated logs; namespace/app, worker, request IDs, status and trace/span IDs
  verified. The three test runs intentionally reuse the same100 fixture IDs.
- Real Vector trace forwarding to a local OTLP receiver preserved trace ID,
  span parent ID and span name. Native traces remain in OTLP format.
- Real Vector stopped before export, restarted during retry:101 written records,
  zero dropped, one export error; all100 logs and the trace delivered.
- Native workerd DO: response200 while downstream trace receiver is hung;
  trusted tenant console persisted. Uses the existing optimized fork binary.
-168 JS tests passed. Go packages passed: cmd/user-runtime, cmd/do-runtime,
  cmd/do-supervisor, internal/doruntime, userruntime, userruntimepool, staticgen,
  workerdpool, server, logbuf. SQLite feature tags supplied for these suites.
- Helm lint/render: only the main container in each runtime Pod, no Vector
  sidecar. Chart mirror check, shell syntax, spool vet and whitespace checks pass.

Real acceptance caught and corrected `use_otlp_decoding.logs=true`: Vector
preserved an entire resourceLogs envelope, whereas the remap expects individual
Vector log events. Setting logs=false splits batches and preserves attributes.
traces=true remains necessary for native OTLP forwarding. Initial failed-decoder
output is retained and excluded from the300 valid log records.

The first manual outage harness expired at8seconds before the collector restart
and subsequent backoff retry. The coordinated rerun uses a30second acceptance
window and restarted the collector after2seconds; it passed in3.01seconds.

## Limits

Telemetry remains best effort:256-entry queue,64KiB line bound,eight16MiB files.
Retention may evict unsent files; EvictedFiles counts evicted files, not records.
Dropped counts known rejected records. EmptyDir survives container restart,
not Pod deletion. Crash-before-cursor and mixed-signal partial batch delivery can
replay acknowledged records. No exactly-once or audit-grade guarantee.
Startup spool configuration/open failures still fail startup; runtime write or
network failures do not block business requests. Trace/log transport changes
never bypass the DO persistence/output gate.

No new production performance claim or full storage/disaster-recovery E2E claim
is made for this change. SLS/ARMS cloud delivery was not exercised; real Vector
used local sinks and a local OTLP trace receiver.

## Evidence

`/var/backups/cellhive/local-502-diagnosis-20261111/go-export/` contains Vector
config, raw output, spools/cursors, and acceptance.json. Test logs are
`/tmp/go-export-{go-all,all-js,race,do,real-outage-fixed}.log`.
