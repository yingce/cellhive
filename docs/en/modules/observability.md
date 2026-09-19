# Module: Observability (Metrics / Logs / Tracing)

Metrics, tenant log tailing, and OpenTelemetry OTLP traces and logs export.

> The authoritative configuration sources are [`../configuration.md`](../configuration.md) and the code; the table below is the relevant subset for this module.
## Key Interfaces

`/metrics`, `/v1/control/logs` (admin), `/v1/internal/logs` (log role), `/v1/internal/telemetry/spans`, `/v1/control/logs/subscribe`.

## Configuration (Environment Variables)

| Variable | Default | Purpose |
|---|---|---|
| `CELLHIVE_LOG_BUFFER` | `1000:200` | Log tail buffer `<entries>:<workers>` (`cellhive tail --worker`) |
| `CELLHIVE_OTLP_ENDPOINT` | empty=off | OpenTelemetry OTLP/HTTP export endpoint (such as `http://collector:4318` or OpenObserve `http://openobserve:5080/api/default`; a full `/v1/traces` URL is also accepted). Empty = fully disabled, zero overhead (ADR-167; see [`tracing.md`](../tracing.md)) |
| `CELLHIVE_OTLP_HEADERS` | empty | OTLP export request headers, `k=v,k2=v2` (for authentication) |
| `CELLHIVE_OTLP_LOGS` | `off` | OTLP logs export: `off`=off; `tail`=export only workers subscribed via `cellhive tail`; `all`=all (ADR-172; requires `CELLHIVE_OTLP_ENDPOINT`) |
| `CELLHIVE_SERVICE_NAME` | `cellhive-cell-agent` | `service.name` in the OTLP Resource (the same variable is used for entry sampling in user-runtime) |
| `CELLHIVE_TRACES_SAMPLE_RATIO` | `0.01` | Header sampling ratio for new entry traces (the sampled bit in upstream `traceparent` takes precedence) |


> For parsing rules (strings/booleans/durations/bytes/lists), see [`../configuration.md`](../configuration.md#parsing-rules).

## Key Invariants

- Tracing is out-of-band and best-effort and does not affect requests; no endpoint = zero overhead; export failures/drops never block requests.
- Tenant log lines carry `trace_id`/`span_id` (when emitted inside a traced request); the resource carries `service.name`/`service.instance.id` (= node) /`cellhive.node_id`.
- The log ring is bounded, non-persistent, and single-node; `tail` subscriptions are broadcast to active nodes via leases.
- Register the metric name and data source (`observability.md`) before adding a new metric.

## Source Locations

`internal/{telemetry,logbuf,nodelog}`; `internal/server/*` (metrics/spans/logs endpoints); `workerd/platform/{telemetry.js,log-tail.js}`.

## Test Anchors

`internal/telemetry`, `internal/server`, `internal/userruntime` (real workerd).

## Related Documentation

[`observability.md`](../observability.md), [`tracing.md`](../tracing.md)

_Last updated: 2026-09-19_