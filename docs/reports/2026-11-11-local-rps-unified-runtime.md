# Local RPS after runtime / telemetry changes

Local working tree at base `a745121`, including uncommitted changes. No production rollout or matched old-version comparison.

Topology: one host (8 visible CPUs, shared with existing services), two cell-agent processes, one user-runtime with one observed workerd child, local FS bucket. No production/COS data used. Go server processes and the Go load generator use GOMAXPROCS=4; no CPU container quota on these processes. Current optimized fork binary, no rebuild.

Endpoints: runtime `http://127.0.0.1:29802` (Host `e2eperf.test`), agent `http://127.0.0.1:29800`, peer `:29804`. Persistent HTTP connections, closed-loop concurrent Go goroutines; each measured phase10seconds after warmup. KV1024-byte values over32 keys, checked response content. Direct requests reuse one valid scoped JWT; no key derivation per request. Hello uses `/perf-ready` on the same KV-bound Worker fixture (not a separate binding-free Worker).

Logs: invocation logging on, Go spool / OTLP export enabled to real Vector0.58.0 on localhost:19878. Actual vwork.log.v1 remap executes; final sink is blackhole. No SLS/cloud-backend latency in these figures. Trace sampling default1%.

| Path | Operation | Concurrency | Successes | RPS | P50 ms | P95 ms | P99 ms |
|---|---|---:|---:|---:|---:|---:|---:|
| worker | hello | 1 | 17162 | 1710.0 | 0.517 | 0.672 | 0.861 |
| worker | get | 1 | 6625 | 662.5 | 1.354 | 1.862 | 2.347 |
| worker | put | 1 | 1301 | 130.0 | 7.319 | 8.700 | 9.825 |
| worker | hello | 8 | 19463 | 1945.7 | 3.641 | 4.948 | 7.143 |
| worker | get | 8 | 7935 | 793.0 | 8.955 | 12.799 | 31.772 |
| worker | put | 8 | 5929 | 592.3 | 11.436 | 16.185 | 73.645 |
| worker | hello | 32 | 19802 | 1978.4 | 14.294 | 18.513 | 60.585 |
| worker | get | 32 | 8933 | 891.6 | 31.707 | 53.619 | 142.489 |
| worker | put | 32 | 10222 | 1020.9 | 28.161 | 55.279 | 98.741 |
| direct | get | 32 | 167062 | 16698.8 | 1.611 | 4.534 | 6.423 |
| direct | put | 32 | 43130 | 4310.6 | 6.363 | 11.182 | 17.769 |

Total 307564 successful measured requests, zero failures. Actual fleet commit counter increased by 5458 during measurement. Writer stats:98331 written,0 dropped,0 errors,0 evicted files at the end of load. Sampled workerd FD peak63, one child PID; no unhealthy/EMFILE observed. Sampling covered part of the run, not its entire lifetime.

This is a short throughput measurement, not endurance acceptance. Worker vs direct KV has a large gap; no causal attribution or regression percentage without profiling and an equivalent before/after baseline. Earlier2CPU Docker/other fixtures are not directly comparable.

Evidence: `/var/backups/cellhive/local-rps-unified-20261111`. Full disposable run retained at `/tmp/cellhive-e2e.QBe1pB`. Test runtimes/agents and the isolated Vector container are cleaned up; existing services untouched.
