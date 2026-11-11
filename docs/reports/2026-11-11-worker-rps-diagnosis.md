# Local Worker throughput diagnosis — 2026-11-11

Diagnostic results only; no runtime performance implementation was changed or deployed in this investigation. Current source: `/opt/cellhive`, HEAD `a745121` plus existing uncommitted runtime changes. This is not an old-version regression comparison.

## Method

Same optimized cellhive-workerd fork binary, localhost HTTP, persistent connections, one load generator process with 32 goroutines and GOMAXPROCS=4. Each case runs twice for 10 seconds. Tests run sequentially on an 8-CPU shared host without dedicated CPU pinning; numbers are local observations, not production capacity guarantees. No TLS/Traefik or WAN. The CellHive setup uses two cell-agents and an isolated local filesystem bucket, not COS. Hello requests do not perform KV reads.

CellHive variants use the same kv-performance fixture `/perf-ready` (returns `ok`), with and without a KV binding. Native and minimal dynamic fixtures return the same body. Dynamic tests retain loader.get per request and use the actual lifecycle.js managedModules implementation. The untracked diagnostic changes only `return life.response(res)` to `return res` inside the generated managed wrapper; this is unsafe as a general production substitution and was confined to /tmp.

Invocation logging and Tail are independently controlled. Trace sampling remains 0.01 in CellHive cases. Vector is a real isolated decoder/remapper with blackhole output; this does not measure remote durable telemetry ingestion.

## Results

| Case | Round 1 / round 2 RPS | Failures |
|---|---:|---:|
| native | 14120 / 13819 | 0 |
| dynamic-bare | 5485 / 5611 | 0 |
| dynamic-managed | 3565 / 3532 | 0 |
| dynamic-untracked | 5139 / 4953 | 0 |
| off-nobinding.test | 3092 / 2977 | 0 |
| invocation-nobinding.test | 2519 / 2588 | 0 |
| tail-nobinding.test | 2318 / 2255 | 0 |
| all-nobinding.test | 2019 / 1986 | 0 |
| all-e2eperf.test | 1960 / 1943 | 0 |

The additional off/invocation/tail binding variants and percentiles are retained in raw JSONL. All accepted throughput cases returned HTTP 200 with the expected body and zero failures. `excluded-startup-failure.jsonl` is explicitly excluded: a temporary diagnostic fixture syntax error left its port unavailable; the fixture was corrected before the accepted untracked run.

## Findings

1. Native static Hello reaches about 14k RPS. Workerd executing an empty handler is not inherently limited to 2k on this host.
2. Minimal loader -> dynamic tenant fetch reaches about 5.55k. Cross-isolate request dispatch has substantial overhead even without CellHive routing, logging or bindings. This experiment does not distinguish loader lookup from all native dispatch/context machinery.
3. Adding current managed lifecycle lowers the minimal dynamic case to about 3.55k. Bypassing only response tracking in the isolated diagnostic recovers about 5.05k, identifying response-body handling as a major removable cost if equivalent native lifetime protection can be retained.
4. Full CellHive with logs and Tail off reaches about 3.03k without bindings and 2.91k with an unused KV binding. Routing, trace/header processing and host lifecycle add further cost; these have not been individually apportioned.
5. Invocation only is about 2.55k; Tail only about 2.29k; both about 2.00k (no-binding cases). Moving telemetry network I/O into Go removes network blocking, but JS event creation, serialization and Tail dispatch still cost CPU. The deltas are not additive CPU percentages.
6. Perf sampling of the full no-log CellHive process shows request/context execution, allocation, stream-reader and Response work. Inclusive call-stack percentages overlap and must not be summed or described as wall-clock stage percentages. Captured 2,341 samples with no lost samples; see off-flat.txt/off-children.txt and off.perf. Profiled throughput was separately recorded and excluded from ordinary results.

## Optimization priorities

- Preserve response lifetime correctness while avoiding JS read/rebuild for memory-backed small responses. Investigate native completion/lifetime support in the fork; validate streaming cancellation, SSE, WebSocket, waitUntil and version retirement before adoption.
- Reduce dynamic fetch dispatch/context overhead with a minimal native benchmark before changing loader caching. Request-scoped capabilities must not be cached across request contexts.
- Reduce redundant invocation/Tail work and event allocations while preserving required tenant console capture and request records. Turning off all logging is a diagnostic control, not the proposed fix.
- Then isolate request header copying, trace IDs and routing work. There is currently no evidence to attribute this Hello bottleneck to KV network or per-request owner reads.

Evidence: `/var/backups/cellhive/worker-rps-diagnosis-20261111`. No code fix, commit or deployment is claimed by this report.

## Follow-up: static Worker with Tail

Same optimized fork, localhost, keep-alive, 32 load-generator goroutines, two 10-second rounds per case, sequential. Static Hello and Tail services share one workerd process. No dynamic loader, routing, managed lifecycle wrapper, invocation logger, KV request, or network telemetry exporter.

| Static case | RPS rounds | P95 ms rounds |
|---|---:|---:|
| off | 14095 / 14481 | 2.542 / 2.535 |
| noop | 10002 / 10354 | 3.669 / 3.445 |
| cellhive | 6772 / 6541 | 5.635 / 5.461 |

`noop` attaches a real native Tail handler that only increments an event count. `cellhive` uses the current TenantTail.prototype.tail implementation with fixed trusted Worker identity and a diagnostic FILE_LOG_TOKEN: event normalization/export preparation and bounded ring storage execute. The Hello handler emits no console logs or exceptions; public fetch summary export is suppressed by existing Tail logic, so this case does not measure Go spool or Vector delivery. It measures an actively attached Tail, not an unsubscribed `tail` configuration.

625,564 measured requests passed with expected body and zero failures. Tail counters exactly match measured requests plus one warm-up for each enabled case (203,628 noop; 133,912 CellHive), and workerd stderr is empty. Native Tail dispatch alone reduces throughput in this test; CellHive event processing adds further cost, but static plus actual Tail still achieves 6.5–6.8k RPS. No performance code changed. Evidence: `/var/backups/cellhive/static-worker-tail-20261111`.
