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


## Optional Vector pipeline

`vector.toml` is a Vector 0.58 reference pipeline for **OTLP logs/traces +
Prometheus metrics**:

```
CellHive ──OTLP/HTTP──► Vector :4318 ──► ARMS OTLP (traces/metrics)
CellHive /metrics ◄──prometheus_scrape──┘
CellHive logs ────────────────► SLS Kafka (topic/logstore)
```

- CellHive config: `CELLHIVE_OTLP_ENDPOINT=http://vector:4318`,
  `CELLHIVE_OTLP_LOGS=all` (or `tail`), `CELLHIVE_TENANT_LOG_CAPTURE=on`.
  CellHive has **one** OTLP endpoint shared by traces and logs; pointing it at
  Vector sends both. Keep traces direct to ARMS only if you do not route logs
  through Vector, or add a separate log-endpoint configuration in CellHive.
- Metrics are **scraped**, not OTLP-pushed. Keep `/metrics` internal.
- ARMS accepts traces at the existing `/api/otlp/traces` URL; the separate
  metrics URL is configured independently. Do not point logs at the ARMS trace
  endpoint (it does not accept OTLP logs).
- SLS has no native Vector sink; use its Kafka upload protocol. The Kafka
  bootstrap endpoint is `<project>.<region>.log.aliyuncs.com:10011` (VPC) or
  `:10012` (public), SASL/SSL username is the SLS project, and password is
  `<AccessKeyId>#<AccessKeySecret>`. Append `.json` to the logstore topic to
  parse JSON fields.
- The CellHive logs are **remapped to `vwork.log.v1`** before the SLS Kafka
  sink, so the existing `request.json` logstore can be reused:
  - `source_service = "cellhive"`
  - `category = function_request | queue_operation | cron_dispatch` for
    invocation records, `user_console` for console lines
  - `gateway_access` remains VWork's gateway category
  - `app_id` is derived from a `vwork-<digits>` namespace; `function_name` is
    the CellHive worker. If namespaces do not follow that convention, provide
    an explicit mapping instead of relying on this derivation.
  - `event_id` is deterministic from namespace/worker/request/category/span/
    message/observed time. If VWork's Wrangler tail relay already writes the
    same invocation, choose one path or dedupe by `event_id`.
- Vector 0.57+ disables `${ENV}` interpolation by default; start with
  `VECTOR_DANGEROUSLY_ALLOW_ENV_VAR_INTERPOLATION=true`.
- Compose: `docker compose --profile vector up vector` (do not also enable the
  `observability` profile; both bind 4317/4318).

The config is validated with `vector validate` against Vector 0.58. The
`cellhive_otlp.metrics` port is intentionally unconsumed today because CellHive
does not push OTLP metrics; metrics come from `prometheus_scrape`.

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

## Validated end-to-end

`bash scripts/openobserve-e2e.sh` (repo root) starts OpenObserve + this
collector config + cell-agent + user-runtime + a probe worker and asserts:

- logs land in OpenObserve stream `default logs` with `cellhive_namespace=obs`
  and the caller's `trace_id` preserved;
- a slow (>1s) request survives the reference `tail_sampling` keep-slow policy,
  so the whole trace (loader `http.server`, cell-agent `http.server`,
  `cell.durability_proof`) lands in `default traces` with
  `cellhive_namespace`/`cellhive_binding` attributes.

Notes learned from the live run:

- OpenObserve **flattens attribute dots** for SQL: query `cellhive.namespace`
  as `cellhive_namespace`. Traces are searched with
  `POST /api/<org>/_search?type=traces`; the span name column is
  `operation_name`.
- The `attributes/redact` processor only accepts exact keys for `delete` on
  current Collector versions (no `key_regex`); the config lists the credential
  headers explicitly. A `key_regex` there makes the Collector fail to start.
- The reference `tail_sampling` keeps errors, >1s traces and a 1% baseline, so
  fast, successful traces are intentionally dropped. Raise `baseline` or drop
  the processor when you want every trace.

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
