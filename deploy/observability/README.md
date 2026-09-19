# Observability pipeline (reference)

CellHive **pushes** logs and traces over OTLP/HTTP; metrics are exposed in
Prometheus text on each process. This directory holds a reference pipeline that
keeps the platform's query surface off the public network and lets a backend own
storage, retention, query and **tenant-facing auth**.

```
CellHive nodes ──OTLP (internal, platform creds)──► otel-collector ──redact/route/tail-sample──► OpenObserve (org/stream per tenant)
                                                            └──────────────────────────────────► ops Prometheus (metrics, internal scrape only)
Tenants ──(OpenObserve users/roles, or a thin query proxy)──► read only their own org/stream
```

- `otel-collector.yaml` — Collector config: OTLP in, redaction, optional
  namespace routing, tail sampling, OTLP out to OpenObserve; an optional
  `prometheus` receiver for turning CellHive's `/metrics` into OTLP.
- CellHive side (set on **every** node; endpoint empty = fully disabled):
  - `CELLHIVE_OTLP_ENDPOINT=http://otel-collector:4318`
  - `CELLHIVE_OTLP_HEADERS=authorization=Bearer <internal-token>` (optional)
  - `CELLHIVE_OTLP_LOGS=off|tail|all` (default `off`)
  - `CELLHIVE_TRACES_SAMPLE_RATIO=0.01` (new traces; upstream sampled flag wins)
- Compose: the `tracing` profile starts OpenObserve; add the collector with
  `--profile observability` (see `deploy/compose/docker-compose.yml`).

## Multi-tenant model (which hop authenticates)

1. **Node → Collector**: the platform's own credential (or none on a private
   network). Tenants never see this endpoint or its token.
2. **Collector → backend**: the backend credential lives only in the Collector.
3. **Tenant → backend (query)**: this is the only tenant-facing hop, and it is
   authenticated by the **backend**: map each namespace to an OpenObserve
   **organization or stream** (e.g. `logs-<ns>`) and give the tenant a
   read-only user for exactly that scope. A backend that cannot filter by an
   arbitrary attribute row-by-row must use this per-org/stream mapping; do not
   rely on a `cellhive.namespace` attribute alone for isolation.

Every record carries the resource attributes `service.name`,
`service.instance.id` (the node id), and per-record `cellhive.namespace` /
`cellhive.worker` (spans and logs). Tenant log lines also carry `trace_id` /
`span_id` when emitted inside a traced request, so backends can jump
metrics → trace → logs.

## Metrics

Keep `/metrics` **internal** (Prometheus pull). It is unauthenticated, so do not
expose it publicly. To include metrics in the OTLP pipeline, enable the
collector's `prometheus` receiver instead of exposing the endpoint.

## Notes and boundaries

- OTLP export is **best-effort** (bounded in-memory batches): a backend outage
  drops telemetry rather than blocking requests. For audit-grade retention add a
  durable buffer (file exporter) in front of the backend.
- `CELLHIVE_OTLP_LOGS=tail` exports a worker's lines only while a
  `cellhive tail --worker` subscription is active (TTL, fleet-broadcast) — the
  cheapest way to give a tenant logs on demand without exporting everything.
- Tail sampling keeps all errors and slow requests, so low head-sample ratios
  are safe for correctness investigation.
