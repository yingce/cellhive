# Tracing (OpenTelemetry OTLP)

CellHive exports distributed traces using **standard OpenTelemetry OTLP/HTTP** (ADR-167). CellHive **does not store traces itself**: it only sends spans to the OTLP backend you configure (OpenObserve / OTel Collector / Tempo / Jaeger / cloud). **Switching backends only requires changing environment variables**.

- Implementation: `internal/telemetry` (official `go.opentelemetry.io/otel` SDK + `otlptracehttp`); workerd side `workerd/platform/telemetry.js`.
- **Disabled** by default (`CELLHIVE_OTLP_ENDPOINT` empty = no spans, no extra overhead).

```
cell-agent (Go) ──┐
do-supervisor(Go)─┼── OTLP/HTTP (:4318 /v1/traces or /api/<org>/v1/traces) ──▶ backend
loader/host (JS) ─┘        ▲
   └── POST /v1/internal/telemetry/spans ── cell-agent unified export
```

## 1. Configuration

| Variable | Default | Description |
|---|---|---|
| `CELLHIVE_OTLP_ENDPOINT` | empty (off) | OTLP/HTTP endpoint. It can be a base URL (automatically appends `/v1/traces`) or a **complete** `/v1/traces` URL. `http://` is automatically treated as insecure |
| `CELLHIVE_OTLP_HEADERS` | empty | Export request headers, `k=v,k2=v2` (values may contain spaces; split on the first `=`) |
| `CELLHIVE_TRACES_SAMPLE_RATIO` | `0.01` | Head sampling ratio for new entry traces; the sampled bit from upstream `traceparent` takes precedence (`ParentBased`) |
| `CELLHIVE_SERVICE_NAME` | `cellhive-cell-agent` | OTLP Resource `service.name` (the user-runtime entry sampling also uses this ratio) |

It is effective when the startup log shows `otel export enabled endpoint=… ratio=…`.

## 2. Which spans are sampled

| Component | span | Key attributes |
|---|---|---|
| cell-agent | `http.server` (per request) | `http.route`, `http.status_code`, `cellhive.namespace/kind/name/scope` |
| cell-agent | `cell.durability_proof` | scope (time spent on `Capture.Wait` durability proof) |
| cell-agent | `peer.append` | scope (each time a replica is sent to a follower) |
| do-supervisor | gate / restore | — |
| workerd loader | `http.server` (entry) | `http.method`, `cellhive.namespace/worker` |
| do-runtime host | `do.invoke` / `do.gate` | `cellhive.namespace/worker/class/do_kind` |

**Propagation and correlation** (within one trace):

```
client ──traceparent──▶ loader http.server ──header──▶ cell-agent http.server ──ctx──▶ cell.durability_proof
                              │
                              └─(props-bound binding call inside worker)──▶ cell-agent http.server
                              │
                              └─(DO facade with traceparent)──▶ cell-agent(do_proxy) ──spec──▶ host do.invoke
                                                                                              └─▶ host do.gate ──header──▶ supervisor do.gate
```

- **Can be correlated**: worker→cell-agent binding calls, worker→DO (`do.invoke`), DO→`do.gate`→supervisor `do.gate`, `cell.durability_proof` (all in the same trace, with correct DO/gate parent-child relationships—asserted by real workerd tests).
- **Break point 1 (binding calls inside DO)**: `this.env.KV/D1/...` (props-bound entrypoint) inside a facet **does not carry** the DO's traceparent, and will form a separate trace. Reason: workerd gives the host actor, facet, and platform entrypoint independent isolate `globalThis` instances, and the CF-shaped binding API has no "pass parameters per request" entry point (user-runtime can correlate because the loader is that isolate). Test: `internal/doruntime TestDoRuntimeBindingInsideDO` (currently asserts empty).
- **Break point 2 (peer replication)**: `peer.append` runs on the capture pipeline (derived from `context.Background()` + group commit merging multiple requests) and is **not** a child span of any request trace; it forms an independent trace per "batch".
- The fallback path in the tenant isolate's `facades.js` does not report JS spans, but its `pfetch` still carries `traceparent`, so cell-agent's server span can still attach to that worker's trace.
- Sampling is **entry head sampling**: unsampled traces are only propagated and do not produce spans.

## 3. Integrating with OpenObserve (key points)

OpenObserve OTLP endpoints (official documentation):

- Self-hosted: `http(s)://<host>:5080/api/<org_name>/v1/traces` (`<org_name>` defaults to `default`)
- Cloud: `https://api.openobserve.ai/api/<org_name>/v1/traces`
- Authentication: `Authorization: Basic base64(userid:password)`

### 3.1 One-click example (compose profile)

The repository includes a `tracing` profile:

```bash
# 1) Generate the Basic header (default account root@example.com / Complexpass#123)
export CELLHIVE_OTLP_HEADERS="authorization=Basic $(printf %s 'root@example.com:Complexpass#123' | base64 -w0)"
# 2) Point to the self-hosted OpenObserve OTLP endpoint
export CELLHIVE_OTLP_ENDPOINT=http://openobserve:5080/api/default
export CELLHIVE_TRACES_SAMPLE_RATIO=1        # Use full sampling first; reduce it after verification
export CELLHIVE_ROOT_KEY=$(openssl rand -base64 32)

docker compose -f deploy/compose/docker-compose.yml --profile tracing up --build
```

Notes:
- `CELLHIVE_OTLP_ENDPOINT` only needs to be written up to `/api/default` (the exporter automatically appends `/v1/traces`); writing the full `…/api/default/v1/traces` is also accepted.
- The value of `CELLHIVE_OTLP_HEADERS` may contain spaces (`Basic ` followed by base64); separate multiple headers with commas.
- OpenObserve also needs port `5080`; compose already maps it. The UI defaults to `http://localhost:5080`; log in with `ZO_ROOT_USER_EMAIL/PASSWORD` (see compose for defaults).

### 3.2 Existing OpenObserve (outside compose)

```bash
export CELLHIVE_OTLP_ENDPOINT=http://<openobserve-host>:5080/api/default
export CELLHIVE_OTLP_HEADERS="authorization=Basic $(printf %s '<user>:<pass>' | base64 -w0)"
export CELLHIVE_TRACES_SAMPLE_RATIO=0.1
# Restart cell-agent / user-runtime / do-supervisor (with these environment variables)
```

Cloud users: `CELLHIVE_OTLP_ENDPOINT=https://api.openobserve.ai/api/default` (HTTPS automatically uses TLS).

### 3.3 Verification (tested)

1. Visit a tenant route (or any `/v1/...`/`/healthz` request) to generate traffic;
2. In OpenObserve UI → **Traces**, you can see traces with `service_name=<CELLHIVE_SERVICE_NAME>`;
3. Or query the API directly (the stream name for traces is `default`):

```bash
NOW=$(date +%s%6N); START=$((NOW-3600000000))
curl -s -u '<user>:<pass>' \
  "http://<host>:5080/api/default/default/traces/latest?start_time=$START&end_time=$NOW&from=0&size=10"
# → {"total":N,"hits":[{"trace_id":"...","first_event":{"service_name":"cellhive-cell-agent","operation_name":"http.server"},...}]}
```

> Note: `POST /api/<org>/_search` is the **logs** search API. Using it to query traces returns `Search stream not found`; for traces, use `…/{stream}/traces/latest` (or the UI).

4. If there is no data: confirm the startup log has `otel export enabled endpoint=… ratio=…`, `CELLHIVE_TRACES_SAMPLE_RATIO > 0`, the Basic header is correct (`printf %s` should not include a newline), and port `5080` is reachable over the network; on the OpenObserve side, check the container logs for `POST /api/<org>/v1/traces 200` (UA `OTel OTLP Exporter Go/…`).

> Only **traces** are exported; OpenObserve logs/metrics endpoints (`/api/<org>/v1/logs|metrics`) are not currently used by CellHive (logs use `cellhive tail`; metrics use Prometheus `/metrics`).

## 4. Other backends

**OTel Collector** (recommended for production, convenient for fan-out to multiple backends):

```yaml
receivers: { otlp: { protocols: { http: { endpoint: 0.0.0.0:4318 } } } }
exporters:  { otlphttp/openobserve: { endpoint: http://openobserve:5080/api/default, headers: { authorization: "Basic ${BASIC}" } } }
service:    { pipelines: { traces: { receivers: [otlp], exporters: [otlphttp/openobserve] } } }
```
Point CellHive to `CELLHIVE_OTLP_ENDPOINT=http://collector:4318` (no headers).

**Tempo**: `CELLHIVE_OTLP_ENDPOINT=http://tempo:4318` (no authentication required).
**Jaeger v2**: `CELLHIVE_OTLP_ENDPOINT=http://jaeger:4318`.
**Cloud OTLP**: use the cloud provider's OTLP endpoint + `x-otlp-api-key`/`authorization` header.

## 5. Boundaries and remaining gaps

- See the two correlation break points in §2: **props-bound binding calls inside DO** (new trace), **peer.append** (independent per-batch trace).
- JS only covers **platform workers** (loader entry, do host invoke/gate); the fallback path in the tenant isolate's `facades.js` has no internal token and does not report JS spans (but server spans can still attach); DO `/v1/do/connect`·abort and compaction/upload background loops have no independent spans.
- JS span timestamps have millisecond precision (`Date.now()*1e6`).
- Sampling is entry head sampling; when `CELLHIVE_TRACES_SAMPLE_RATIO=0`, new traces are not sampled (an upstream explicit `-01` will still be sampled).
- When the endpoint is empty, everything is no-op; export is background batch processing (BatchSpanProcessor, 5s timeout + bounded dropping) and does not block the request path.
- **Logs (optional)**: `CELLHIVE_OTLP_LOGS=off|tail|all` exports OTLP **logs** (`/v1/logs`) using the same endpoint/headers; `tail` exports only while `cellhive tail` is subscribed, and the subscription is **broadcast to all live nodes** (ADR-173). See [`observability.md`](observability.md) §Logs.

## Related

- Metrics/logs/alerts: [`observability.md`](observability.md)
- All environment variables: [`configuration.md`](configuration.md)
- Decisions: [`decisions.md`](decisions.md) ADR-167, ADR-146 (`traceparent` propagation)

_Last updated: 2026-09-19_