# Observability

## Metrics (Prometheus text; exposed independently by each service)

### cell-agent `/metrics` (**current code state**, aligned with ADR-157)

| Metric | Type | Description |
|---|---|---|
| `cellhive_requests_total` | counter | Number of internal/data-plane HTTP requests |
| `cellhive_claims_total` | counter | Number of owner claims |
| `cellhive_append_total` | counter | Number of segment appends |
| `cellhive_segments_read_total` | counter | Number of segment reads |
| `cellhive_cellstore_open_cells` | gauge | Currently open cells (SQLite handles) |
| `cellhive_resident_cells` | gauge | Resident cells (currently = open cells) |
| `cellhive_owned_cells` | gauge | Number of owner scopes held by this node |
| `cellhive_cellstore_evicted_total` / `cellhive_cellstore_sweeps_total` | counter | Evictions/sweeps |
| `cellhive_cellstore_disk_files` / `cellhive_cellstore_disk_bytes` | gauge | Number and bytes of local cell files |
| `cellhive_bucket_ops_total{op}` | counter | Bucket operations (`put`/`get`/`list`/`conditional_create`/`cas`/`delete`) |
| `cellhive_list_calls_total` | counter | **Should be 0 (List is prohibited on hot paths)**; non-zero indicates a diagnostic/ops path is listing buckets |
| `cellhive_paged_cells` | gauge | Number of currently open cells going through the paged VFS (ADR-160) |
| `cellhive_paged_faults_total` / `_runs_total` | counter | Number of page faults / data-fetch calls (window or single page) |
| `cellhive_paged_prefetch_hits_total` | counter | Faults served directly from the child prefetch cache without another fetch |
| `cellhive_paged_hydrated_pages` / `_total_pages` | gauge | Number of materialized pages / total pages in the cut (background backfill progress) |
| `cellhive_upload_batches_total` / `_segments_total` / `_dropped_total` / `_deferred_total` / `_replayed_total` / `_spool` | counter/gauge | Upload batches/spool (ADR-143) |
| `cellhive_upload_spool_file_syncs_total` / `_dir_syncs_total` / `_sync_seconds_total` | counter | Spool power-loss-safe fsync counts and duration (directory sync is memoized, ADR-171) |
| `cellhive_binding_calls_total{kind,outcome}` | counter | Binding endpoint calls (`outcome=ok|denied|error`; `denied`=401/403/429) (ADR-165) |
| `cellhive_durability_proof_seconds` | histogram | Write-path `Capture.Wait` durability proof latency (buckets 5ms/25ms/100ms/500ms/1s/5s) (ADR-165) |
| `cellhive_owner_epoch_changes_total{role}` | counter | Number of epoch (generation) increments (ADR-165) |
| `cellhive_takeover_total{outcome}` | counter | Takeover: `success` (took over an expired external owner record)/`failed` (CAS race lost)/`blocked` (blocked by an active external lease) (ADR-165) |
| `cellhive_route_projection_version` | gauge | Control revision for which the route projection has been built (ADR-165) |
| `cellhive_peer_hedge_fired_total` / `_won_total` | counter | Number of fleet replication hedge replicas issued / acknowledgements won (ADR-165) |
| `cellhive_replication_bytes_total{kind}` | counter | Fleet replication bytes: `shipped` (replica bytes actually sent from owner to follower)/`received` (bytes persisted by this node as follower) (ADR-166) |
| `cellhive_waker_fires_total{kind,outcome}` | counter | Timer dispatch results (`outcome=ok|failed`; kind=cron/queue_delay/workflow_sleep/do_alarm/…), counted for both waker and single-scope runner (ADR-166) |

### do-supervisor `/metrics` (ADR-166)

The Go supervisor process of do-runtime exposes metrics on `-listen` (default `:18901`):

| Metric | Type | Description |
|---|---|---|
| `cellhive_do_wal_captured_bytes_total` | counter | Input bytes captured by the output gate (SQLite + sidecar file sizes, accumulated synchronously) |
| `cellhive_do_restore_seconds` | summary | Cold restore (`RestoreAll`/`RestoreObject`) duration `_sum`/`_count` |
| `cellhive_do_output_gate_timeouts_total` | counter | Number of `/sync-all` output gate failures (capture/proof errors) |
| `cellhive_do_alarms_fired_total{outcome}` | counter | Tenant `alarm()` execution results (increments reported by the host actor) |
| `cellhive_do_ws_sessions` | gauge | Number of DO WebSocket sessions (host actor reports `opens-closes` increments; supervisor accumulates and clamps to 0 when the host restarts) |

The host actor (`workerd/do-runtime/host.js`) reports increments after each invoke via best-effort `POST <GATE_URL>/internal/do/stats`; this is a process-level approximation (host restarts reset it to zero, and the next reported negative increment clamps it back to 0).

### Per-domain stats (ops, non-Prometheus; ADR-157)

`GET /v1/<kind>/stats` (`kv`/`d1`/`queue`/`r2`/`workflow`/`hyperdrive`/`do`) returns **per-resource** read-only metadata (page/file bytes, expiry indexes, table lists, queue backlog and lag, bounded R2 approximations, instance estimates). Its division of responsibility with metrics: **metrics = node-level time series and alerts; stats = per-resource point-in-time inventory** (not sampled, not aggregated, on demand). Fields and costs are documented in `docs/bindings.md` (Vectorize's `/v1/vectorize/stats` also includes `ann` model information).

### Planned (not yet implemented)

None: the "planned" metrics previously listed in `docs/observability.md` have all been implemented (ADR-165 server side; ADR-166 replication/waker + do-supervisor). Before adding new metrics, register the name and data source here first.

## Logs

- Structured JSON (Go `log/slog`; structured stdout on the workerd side);
- Required fields: `ts`, `level`, `service`, `node`, `request_id` (when available), `ns`/`worker`/`scope` (when available), `event`;
- **Do not** log: secrets in plaintext, internal token, raw bucket response bodies;
- Key events: `owner_acquired`, `epoch_bumped`, `takeover_started/finished`, `drain_started/finished`, `waker_fire`, `do_restore_started/finished`, `output_gate_timeout`.

### Tenant logs: in-memory tail + optional OTLP export (ADR-172)

- Tenant `console.*` is collected by `workerd/platform/log-tail.js` → `POST /v1/internal/logs` → cell-agent's **bounded in-memory ring** (`CELLHIVE_LOG_BUFFER=<entries>:<workers>`, default `1000:200`) → `cellhive tail --worker <ns>/<worker>` polls `GET /v1/control/logs`. The ring is **non-persistent and single-node**; when busy, it drops the oldest entries.
- **Optional OTLP/HTTP logs export** (standard protocol; changing backends only requires changing environment variables; shares endpoint/headers/resource with traces, path `/v1/logs`): `CELLHIVE_OTLP_LOGS=off|tail|all` (default **off**).
  - `all`: export every entry (centralized collection; high volume).
  - `tail`: **export only when there is an active subscription for `(ns,worker)`**—each `cellhive tail --worker` poll POSTs `/v1/control/logs/subscribe` (TTL 60s, auto-renewed), and export stops within ≤60s after tail exits.
  - Export is out-of-band, best-effort, and batched in the background (`sdk/log` BatchProcessor, 2s); failures do not affect requests and do not block.
  - **Fleet broadcast**: `cellhive tail` still connects to only one cell-agent; after that agent receives the subscription, it **broadcasts it to other live nodes** (internal endpoint `POST /v1/internal/logs/subscribe`, same TTL; at most once every 25s per worker). The node list comes from the lease (`Advertise`), and broadcast is best-effort—a temporarily unreachable node may miss one broadcast, and the next renewal (tail polls every ≤1s) will fill it in within ≤25s.

## Tracing (OTLP, ADR-167)

See [`tracing.md`](tracing.md) for details (configuration, span catalog, **OpenObserve**/Collector/Tempo/Jaeger integration, troubleshooting).

Overview: aligned with standard **OpenTelemetry OTLP/HTTP**: CellHive does not store traces; it only exports spans to the OTLP backend you configure, and **changing backends only requires changing `CELLHIVE_OTLP_ENDPOINT`/`CELLHIVE_OTLP_HEADERS`**.

- **Enablement/sampling**: empty `CELLHIVE_OTLP_ENDPOINT` = disabled (no spans, no overhead); otherwise, new incoming trace heads are sampled according to `CELLHIVE_TRACES_SAMPLE_RATIO`, and the sampled bit of an upstream `traceparent` takes precedence (`ParentBased`).
- **Go spans** (cell-agent): `http.server` (per request, `http.route`/`http.status_code`; binding endpoints add `cellhive.namespace`/`cellhive.binding` once the scope resolves, matching the ADR-179 metric dimension), `cell.durability_proof` (`Capture.Wait`), `peer.append` (each replica sent to a follower, with scope attributes). do-supervisor gate/restore also uses the same exporter.
- **JS spans** (workerd, platform worker holding internal token): loader `http.server` entry span, and do-runtime host `do.invoke` and `do.gate`. JS cannot run the OTel SDK, so it records spans best-effort by `POST`ing them to cell-agent's `/v1/internal/telemetry/spans`, and Go exports them centrally.
- **Propagation**: W3C `traceparent` is generated/propagated at ingress → tenant handler → props-bound binding calls (the loader sets `traceparent` in its own isolate; ADR-167 fixed the remaining issue from ADR-146 where "props-bound facades did not carry trace") → DO invoke spec.
- **Remaining gaps**: tenant isolate `facades.js` calls (non-props-bound fallback path) have no internal token and do not report spans; `/v1/do/connect`/abort and compaction/upload background loops have no independent spans; JS span timestamps have millisecond precision.

## Multi-tenancy and external querying (push logs/traces, pull metrics internally)

- **Push logs and traces**: nodes only push to an **internal OTLP endpoint** (`CELLHIVE_OTLP_ENDPOINT` / `_HEADERS`; `CELLHIVE_OTLP_LOGS=off|tail|all`). The platform exposes no query surface — storage, retention, query and **tenant auth** belong to the backend (OpenObserve/Tempo/Collector).
- **Unified resource attributes**: every record carries `service.name`, `service.instance.id` (= node id), `cellhive.namespace`, `cellhive.worker`; tenant log lines emitted inside a traced request carry `trace_id`/`span_id` (`logbuf.Entry` + per-line `traceparent` from `log-tail.js`, parsed by cell-agent) so backends can jump **metrics → trace → logs**.
- **The only tenant-facing hop is the backend**: map each namespace to a backend **org/stream** (e.g. OpenObserve `logs-<ns>`) and give the tenant a read-only user for exactly that scope; do **not** rely on the `cellhive.namespace` attribute alone for row-level isolation (most backends cannot filter by arbitrary attributes).
- **Reference pipeline**: [`../../deploy/observability/otel-collector.yaml`](../../deploy/observability/otel-collector.yaml) (OTLP in → redact/route/tail-sample → OpenObserve) plus [`../../deploy/observability/README.md`](../../deploy/observability/README.md); compose with `--profile observability` (OpenObserve is in the `tracing` profile).
- **Reproducible smoke**: `bash scripts/otlp-collector-smoke.sh` starts a real `otel/opentelemetry-collector-contrib` plus cell-agent + user-runtime + a worker that `console.log`s and writes KV, then asserts the collector received spans (with `cellhive.namespace`), a log record carrying the caller `trace_id`, and an `ns`-labelled binding counter on `/metrics`.
- **Keep metrics internal**: `/metrics` is **unauthenticated**; do not expose it publicly. To fold metrics into OTLP, use the collector's `prometheus` receiver instead of exposing the raw endpoint.
- **Delivery semantics**: OTLP export is **best-effort with bounded in-memory batches**; a backend outage drops telemetry rather than blocking requests. Audit-grade retention needs a durable buffer (file exporter) in front.

## Alerting recommendations (ops)

- `cellhive_takeover_total{outcome="failed"}` increasing (takeover contention/failure);
- `cellhive_durability_proof_seconds` p99 above threshold: `histogram_quantile(0.99, sum(rate(cellhive_durability_proof_seconds_bucket[5m])) by (le))`;
- Abnormal increase in `cellhive_binding_calls_total{outcome="denied"}` (auth/quota issues);
- Node lease expiration / sustained `pressured=true`;
- `cellhive_do_output_gate_timeouts_total` increasing (DO output gate capture/proof failure);
- `cellhive_do_restore_seconds` p99 (`rate(_sum)/rate(_count)`) continuously increasing;
- `cellhive_list_calls_total` > 0 (regression of the hot-path List prohibition).

_Last updated: 2026-09-19_