# Roadmap

Goal: proceed with **minimum closed loop first**, validate the two fundamental assumptions first, then fill in layer by layer.

## Two Fundamental Assumptions (Risks Throughout)

1. **Observability of DO/WAL in stock workerd**: actor SQLite is WAL, can be externally read-only, checkpointing can be aligned, and the DO file layout can be replicated.
2. **Correctness of the self-developed cell replication protocol**: owner conditional writes, epoch fence, ensemble, node-log recovery, RPO=0.

If assumption 1 fails → fall back to FUSE (self-managed) or periodic snapshots (lower RPO).

---

## P0 — Feasibility Spike (Do First)

**Deliverables**
- Minimal Go `cell-agent`: single-cell SQLite + bucket conditional-write owner/lease + epoch + `diagnose`;
- Minimal workerd host adapter: `workerLoader` loads one sample worker, bindings go through cell-agent;
- **WAL capture spike**: supervisor reads workerd actor SQLite in read-only mode, captures and reports;
- Minimal closed loop for a two-node fleet (owner + 1 follower ensemble).

**Exit Criteria (Performance Gates)**
- Conclusion on workerd WAL/checkpoint/file-layout validation;
- `kill -9` takeover with RPO=0, ≤5s;
- Two-node write p50/p99 ≤ 30/100ms; single-node degraded mode ~90ms;
- Cross-node "capture → cell-agent → proof" p50/p99;
- Hot path LIST=0; number of ack PUTs ≪ 1;
- Bucket provider conditional writes/presigned pass.

## P1 — Complete cell-agent + Security/Routing

**Deliverables**
- Complete cell replication: LTX/snapshots/compaction/paging;
- **node-log + recovery** (complete implementation);
- Lease/takeover/handoff/drain token;
- Timers (unified timer abstraction) + dispatch + single waker;
- Owner resolution library + endpoints;
- Control-plane skeleton (apps/versions/routes/secrets, sharded by app);
- **security.md implemented**: tenant network isolation, scope declarations;
- **routing.md implemented**: routing projection (pull-only, 5–10s).

**Exit Criteria**: stable single-/dual-node fault injection; closed loop for control-plane release/rollback; multi-tenant isolation validated.

## P2 — Bindings + Wrangler Compatibility + Assets

**Deliverables**
- Binding facade for KV/D1/R2/Queue/Cron + `cell-agent` API: ✅ (KV/D1/R2/Queue ADR-063; Cron ADR-070/076 + deploy validation `invalid_cron`); **Workflows ✅ Partial (ADR-086)**.
- Go+esbuild packaging + jsonc/toml parsing + strict rejection + contract tests;
- **Complete asset pipeline** (upload/versioning/_headers/_redirects/worker-first/ETag): ✅ read side (ADR-069/071) + **write side** (Go CLI `asset put`/`bundle put`/`deploy --assets-*` + `/v1/control/asset|bundle`; ADR-062).
- CLI: deploy/dev/tail/secret + resource create commands;
- `cellhive dev` (built-in file storage + single node + hot reload).

**Exit Criteria**: CF compatibility suite passes; framework/Vite prebuilt artifacts can be deployed.

## P3 — Complete DO

**Deliverables**
- do-runtime (native facet + supervisor): **facet ✅ (ADR-077); supervisor/capture/gates ✅ (ADR-083)**;
- DO claim (cell-agent driven) + cold activation **paged lazy loading**: ✅ claim (ADR-078/080) + cold activation/recovery/on-demand paging/per-object cold start (ADR-084);
- **alarm**: ✅ functionality (ADR-079, mechanism changed to shim + unified timer/waker);
- **WebSocket CF compatibility**: ✅ connection+1012 (ADR-080) + **cross-node forwarding/1012 passthrough (ADR-084)**;
- in-flight migration `result_unknown`: ✅ (ADR-080);
- **output gate + RPO=0 cross-node**: ✅ gate (ADR-083) + cross-node cold activation/takeover (ADR-084, object granularity); ◐ real multi-host/cloud validation not done (Class C environment).

**P3 Closure (2026-09-15 All Complete) ✅**
- HTTPCommitter proof relaxed to `fleet`/`bucket`/`bucket-batch` (reject `bucket-async`) ✅;
- **WS cross-node forwarding** ✅ (`proxyConnect`, 1012 passthrough);
- **`transferred_classes` within same worker** ✅ (= rename alias; cross-worker `script_name` rejected);
- **Runtime VFS lazy reads** ✅ implemented on the cell-agent side (ADR-160, `internal/pagedvfs`, enabled by default); not feasible on the DO side (ADR-085), substitute = per-object/per-page materialization on cold start ✅;
- `refreshFacets` incremental cache ✅; `deleteAll()` shim (KV+SQL) ✅.
- Remaining work is only **Class C environment validation** (real cross-host RTT, cloud object-storage conditional writes, multi-host takeover), blocked by lack of environment and recorded as such.

**Exit Criteria**: DO compatibility suite (synchronous SQL/transactions/alarm/WS/migration/eviction): ✅ **`TestDOCompatSuite` unified entry point, all 19 subtests PASS** (ADR-084).

## P4 — Multi-Tenancy + Scaling

**Deliverables**
- Complete control plane (audit, release logs, idempotency): ✅ audit (ADR-062) + release log `Releases` + idempotent deploy `idempotency_key` (ADR-087);
- Full CLI command surface; admin console: ✅ Go CLI covers all admin endpoints (+`status`/`capacity`/`releases`/`workflow create`); admin console = `GET /admin` single-page console (stdlib, no dependencies) + admin JSON API (ADR-036);
- autoscaler (read-node lease signals): ✅ `internal/autoscaler.Advisor` + `/v1/control/capacity` (**signals**; actual orchestration belongs to external systems, Class C environment);
- **Quota/admission (ADR-035)**: ✅ per-namespace token bucket + 429 on write endpoints (ADR-087);
- publish/rollback: ✅ (ADR-060, already available).

**Exit Criteria**: multi-tenant end-to-end; scale-out/scale-in and migration with no data loss.

## P5 — Operations and Hardening

**Deliverables**
- `diagnose`, observability, upgrade/rollback procedures: ✅ enhanced `/v1/diagnose` (proto/admission/capacity/counters) + `/metrics`;
- Protocol versioning implemented (reader-before-writer): ✅ `cell.SupportedProtoVersion` + claim handshake fail-closed (ADR-088);
- Chaos testing, load testing, provider matrix regression: ✅ recovery chaos tests (ADR-057/066) + `cmd/*bench` load tests + `TestVendorMatrixContract` (ADR-088); real multi-host/cloud matrix requires an environment (Class C);
- Documentation completion and release: ✅ glossary exists + `docs/release-notes.md`.

## Ongoing Items

- Track pinned workerd / Wrangler semantic baseline upgrades (explicit action + contract tests);
- Unverified items in `known-issues.md` converge along with P0/P1.

_Last updated: 2026-09-16_