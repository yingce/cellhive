# Test Strategy

## Layering

| Layer | Scope | Method |
|---|---|---|
| Unit | cell protocol logic, owner/epoch, timer deduplication, route parsing | Go unit tests |
| Contract | Wrangler semantic baseline, internal REST/binding schema | Fixed baseline/contract tests |
| Integration | Real workerd + cell-agent + object storage (MinIO/local stub) | Bring up stack with docker-compose |
| End-to-end | wrangler project → deploy → fetch/binding/DO/cron/queue | Compatibility suite |
| Fault injection | Crash/partition/takeover/checkpoint | Targeted scripts |
| Performance | P0 performance gates | Benchmark scripts (wrk/hey + custom) |

## Compatibility Suite (one per binding)

- API behavior examples for each CF binding (KV/D1/R2/Queue/Workflows/Cron/DO/ASSETS/service binding);
- Byte-level deployment of a `wrangler` project → run;
- Edge cases: large values/large results, caps, error codes, pagination/cursors.

## Replication and Consistency Tests (Core)

- Steady-state writes on two nodes: p50/p99, group commit batching ratio;
- **`kill -9` owner**: RPO=0, takeover ≤5s;
- **Sequential crashes of two nodes**: node-log/recovery collects all segments not yet uploaded;
- **Partition**: old owner is fenced and cannot acknowledge writes;
- **checkpoint truncation**: detection + snapshot alignment + delta continuity;
- **bucket vendor matrix**: conditional writes + ranged reads + presigned URL.

## DO-specific

- Synchronous SQL semantics; transactions; `blockConcurrencyWhile`;
- Cold activation/takeover (paginated lazy loading);
- alarm: local trigger + targeted takeover by dead owner waker;
- WS: deploy/migrate 1012, client reconnect, handler restart (CF-compatible);
- in-flight migration → `result_unknown`.

## Performance Gates (P0, see cell-protocol.md §13)

- Two-node write p50/p99, single-node degraded mode, cross-region;
- **Cross-node capture→cell-agent→proof** p50/p99;
- Cold recovery time (100 MiB);
- PUTs per ack, conditional write conflict rate, number of objects read during takeover, hot-path LIST=0.

## Upgrade/Rollback Tests

- reader-before-writer rolling upgrade;
- Rollback with version skew ≤1;
- Explicit migration drill for incompatible changes.

## Acceptance Matrix (Exit Criteria → Evidence)

| Stage exit criteria | Evidence |
|---|---|
| P0 performance gates (two-node write/capture→proof/cold recovery/hot-path LIST=0) | `make perf-test` (`CELLHIVE_PERF_GATE=1`), `make rpo-test`, [`benchmarks.md`](benchmarks.md) (raw output archived under `docs/archive/bench/`) |
| P1 single-/two-node fault injection stability | `internal/recovery`, `internal/owner`, `internal/drain`, `internal/peer` unit tests + `make rpo-test` (`kill -9` takeover, sequential crashes, partition fence) |
| P1 control-plane release/rollback closed loop | `internal/control TestDeployIdempotencyAndReleases`, CLI e2e (`app create`→`deploy`→`releases`), `TestPromoteRollbackInvalidatesBindingCache` |
| P1/P4 multi-tenant isolation | `internal/server TestAdminNamespaceAuthorization`, `TestScopedCredentialAuthorizationMatrix`, `TestScopedCredentialForwarding` |
| P2 CF compatibility suite | `internal/wrangler`+`internal/wranglercompat` (configuration surface), `internal/userruntime` real workerd e2e (bindings/assets/workflow/service) |
| P2 prebuilt framework artifacts are deployable | ASSETS pipeline (`_headers`/`_redirects`/SPA fallback) + `wrangler.jsonc` subset parsing; per-framework adaptation cases still need refinement |
| P3 DO compatibility suite | `TestDOCompatSuite` unified entry point (19 subtests, see below) |
| P4 scale-out/scale-in and migration without data loss | `internal/autoscaler`, `internal/rebalance`, drain tests + `make rpo-test`; **multi-machine orchestration is a Class C environment** |
| P4 multi-tenant end-to-end | `cmd/cellhive TestCLIEndToEndWithWorkerCode` (CLI→control plane→real workerd→facade→cell) |
| P5 protocol versioning / diagnostics / vendor matrix | `cell.SupportedProtoVersion` + claim handshake fail-closed, `/v1/diagnose`, `TestVendorMatrixContract`, `make s3-test` |

## Class C Environments: Local Substitutes (Covered) and Residuals (Not Covered)

**Local substitutes validated** (most recent run all PASS):

| Target | Local substitute | Evidence (command / test) |
|---|---|---|
| Object storage conditional write/conditional delete | Local MinIO (`make s3-test`, skip if docker is unavailable) | `TestS3BucketIntegration` (create/reject/CAS/reject-stale/ranged/**ListPage**), `TestDiagnoseDetectsConditionalDeleteIgnored`, `cellhive diagnose` |
| Multi-process crash / takeover / RPO=0 | Two-process real crate (`make rpo-test`) | `RPO-ZERO: PASS (1200 acked keys all present and exact)`, restoreverify `integrity:ok` |
| Cross-node RTT / peer pipeline | Synthetic latency injection | `internal/peer TestLatencyTransportInjectsRoundTripDelay` (`CELLHIVE_PEER_LATENCY` round trip 2×) |
| Slow/rotating/faulty OIDC IdP | `httptest` mock IdP | `internal/auth TestJWTBearerRS256`, `TestJWTWithoutExpiryRejected`, `TestJWKSServesStaleDuringRefresh` |
| owner timing/partition simulation | Deterministic model | `internal/owner` (`sim_test.go`) all pass |
| Cloud vendor behavior contract | Contract test | `internal/wranglercompat TestVendorMatrixContract` |

**Residuals that cannot be validated locally (environment required)**:

| Item | Reason |
|---|---|
| Conditional writes, `If-Match` semantics, and `ListPage` latency of real cloud object storage | No cloud credentials; when S3-compatible implementations ignore `If-Match`, conditional delete degrades (`cellhive diagnose` can self-check) |
| Real cross-host (second host) RTT, cross-AZ placement, multi-machine chaos, and takeover deadlines | Single-machine/simulation cannot represent real networks and failure domains |
| Real OIDC IdP (real signatures/JWKS operational rotation/network failures) | Mock only; real IdP not integrated |
| Real scale-out/scale-in orchestration (external orchestrator/cloud driver) | autoscaler only emits signals; core does not include a cloud driver |
| HTTPS edge + certificates/wildcards | Edge is statically configured by operations (ADR-132); repository does not include certificates |

## Queue per-message ack/retry/delay (ADR-155)
- Unit tests: `internal/queue TestSendDelayVisibility`, `TestRunnerPerMessageAckRetry` (explicit ack + explicit retry + implicit ack for the rest), `TestRunnerRetryDelay` (invisible during delay → visible after), `TestRunnerRetryExhaustedGoesToDLQ`.
- JS e2e: `internal/userruntime TestUserRuntimeDispatchInjectsBindings` asserts response `ack`/`retry{duration}` and `typeof batch[0].ack/retry === "function"`.
- Real compose: `ack()` → acked=1; `retry(delay 5)` → retried=1 and not acked before t+6, redelivered with attempts=2 at t+8; `send(delay 6)` → not consumed before t+6, consumed at t+8.

## Full-stack dispatch e2e (ADR-154)
- Real compose `--profile rpo0`: fetch+KV+DO, queue (2 messages in one batch, no retry), cron (`cron_last="* * * * *"`), gate facets, in-bucket KV/Queue/Timer/DO segments, cell-agent 0 dispatch failures.
- Regression: `internal/dispatch TestHTTPDispatcherSendsNamespace`, `internal/userruntime TestUserRuntimeDispatchInjectsBindings` (CF MessageBatch shape assertion).

## Compatibility Matrix Closure (ADR-153)
- `internal/bundler TestBuildExternalizesPlatformModules` (`cloudflare:*` always externalized; `node:*` only with nodejs_compat, otherwise bundling fails).
- `internal/wranglercompat TestKnownFlagsMatchDevCLI` (Go list ↔ `cli/src/validate.ts` exact per-item consistency), `TestPinPairsWithCompatibilityDate` (pin ↔ compatibility upper bound).
- `internal/wrangler TestFrameworkPrebuiltLayouts` (OpenNext/SvelteKit/Astro layouts + bundling).
- D1 sessions: real workerd e2e asserts `withSession()` reports "not supported".

## dispatch / waker (ADR-152)
- `internal/waker TestBackoffDelay` (exponential backoff to cap, disabled, base=0 fallback).
- `internal/config TestDispatchDefaults` (timer 256/24h, waker 256/24h/1m, queue 0/0/30s + overrides take effect).

## scaling / multi-AZ (ADR-151)
- `internal/autoscaler TestAdvisorCooldown` (action change within window → `hold/cooldown`, recovers after window; Cooldown=0 old behavior).
- `internal/server TestSelectFollowersPrefersOtherAZ` (prefer cross-AZ / no AZ use order / only same AZ fallback).

## Root Key Source (ADR-150)

- Unit test: `internal/config TestLoadRootKeyFromFile` (trim, env takes precedence, missing file fails closed + Validate rejects, ALLOW_INSECURE fallback).
- Container: `-v /run/secrets` + `CELLHIVE_ROOT_KEY_FILE` → `/readyz` ready and `cellhive creds internal` matches env mode; startup fails if file is missing.

## CLI Compatibility (ADR-148)

- Go: `internal/server TestDeployDryRun` (valid/invalid dry-run produces no version); `cmd/cellhive TestTranslateWranglerCompat` (`--dry-run`/`--var`/`--secrets-file` pass-through + compat subcommand forwarding), `TestCmdCompatRejections` (structured rejection messages).
- Container smoke: `deploy --dry-run` (valid + invalid bundle), `--var` (deploy response includes vars), `--secrets-file` (secret list includes key), `triggers list`/`versions list`, `d1 execute` rejected.

## secrets Management (ADR-147)

- Unit tests: `internal/control TestSecretDeleteAndList` (list excludes ciphertext, 404 after delete, cross-worker isolation, idempotent), `internal/server TestSecretDeleteAndListEndpoints` (HTTP list does not leak value/`wrapped_dek`, audit `secret.delete`).
- Container CLI: `secret put×2 → list → delete → list` + audit.

## trace Propagation (ADR-146)

- Real workerd: `internal/userruntime TestUserRuntimeTraceContextPropagation` (tenant sees generated traceparent; preserves client-provided value verbatim).
- Go: `internal/server TestServiceFetchPropagatesTraceparent`, `TestDOInvokePropagatesTraceparent` (outbound body carries caller traceparent).

## R2 list Pagination/Sizing (ADR-145)

- Unit tests: `internal/bucket TestFSListPage` (exclusive cursor/ordered/last page/out of range), `internal/r2 TestR2ListPaging` (2+2+1, sizing, bucket isolation), `internal/server TestR2ListCursorPaging` (HTTP `truncated`+`cursor`).
- Real MinIO: `TestS3BucketIntegration` in `make s3-test` includes `ListPage` assertion (`StartAfter` pagination a..e).

## service binding ACL / Cross-ns (ADR-144)

- Unit tests: `internal/control TestServiceACL`, `internal/server TestServiceBindingACLDeployGate` (same ns passes / cross ns without authorization 400 `service_binding_denied` / 200 after authorization / 400 again after revocation), `TestServiceFetchCrossNamespaceACL` (runtime 403 → 200).
- Real workerd: `internal/userruntime TestUserRuntimeCrossNamespaceServiceBinding` (`ns=acme&target_ns=team&worker=api`).

## async Upload Persistence (ADR-143)

- Unit tests: `internal/upload TestSpoolDefersFailedUploadAndReplays` (failure→deferred, recovery→replayed), `TestSpoolReplaysAfterRestart` (replay after restart), `TestSpoolFullCountsSpoolError`, `TestSpoolRoundTrip` (encode/decode/order/bad files skipped).
- Container: `/metrics` has metrics such as `cellhive_upload_spool`, `/data/state/upload-spool` created.

## purge Data Plane (ADR-142)

- Unit tests: `internal/purge` (app-level, worker-level only deletes its own, `MaxDeletes` in rounds, invalid ns/worker), `internal/cellstore TestForgetPrefixDropsMatchingScopes`, `internal/control TestRunPurgeLoopResumableHook`.
- Container end-to-end: delete `acme/api` → `cells/acme/__do__/api~*` and `assets/acme/api/*` deleted, `api2~`/`__kv__`/`cells/other` retained, audit `purge.done`; delete app → `cells/acme/*`+`assets/acme/*` all deleted.

## One-command Gate (ADR-141)

OTLP/collector end-to-end: `bash scripts/otlp-collector-smoke.sh` (needs docker plus the `otel/opentelemetry-collector-contrib` image; starts a real collector + cell-agent + user-runtime + a probe worker and asserts spans/logs/`ns` metrics, ADR-179).
Backend end-to-end: `bash scripts/openobserve-e2e.sh` (needs docker plus an `openobserve` image; real OpenObserve behind the repo's reference collector config, asserting logs/traces land in OO and are queryable by ns/trace_id).

`make ci` (`scripts/ci.sh`) chains all checks: gofmt, `go vet`, `go test`, `make build`, `js-test`, `cli-test`, `perf-test`, `rpo-test`, `s3-test`, `docker-build`, `compose-config`, `runtime-baseline-e2e`, `k8s-render`, and `helm-lint`; missing tools are skipped (`REQUIRE_ALL=1` turns them into hard failures). Current acceptance on 2026-09-22 is **GATE: PASS 14/14**; runtime-baseline E2E ends with `RUNTIME-BASELINE-E2E: PASS` and has no actual skipped checks.

### ADR-186 Runtime Baseline Acceptance

- Focused: `go test ./internal/runtimeenv ./internal/workerdcompat ./internal/workerbudget ./internal/wranglercompat -count=1 -v`, `make js-test`, and `make cli-test`.
- Real binary: `CELLHIVE_WORKERD="$(command -v workerd)" bash scripts/workerd-compat-probe.sh`.
- Final gate: `bash scripts/runtime-baseline-e2e.sh` ended with `RUNTIME-BASELINE-E2E: PASS`, followed by `GATE: PASS` from `REQUIRE_ALL=1 bash scripts/ci.sh`. Any actual `SKIP`/`SKIPPED` or missing tool remains a failure (`skipped 0` in TAP means zero skipped tests).
- The E2E must verify image versions, in-image esbuild source packaging, visibility of user-defined `CELL_URL`/`CELL_TOKEN` while host canaries stay absent, fetch/KV, gated-DO count continuity after restart, capnp/final-WorkerCode secret scans, and near-limit code/env probes.
## compose Stack Startup Smoke (C, ADR-140/153)

`CELLHIVE_ROOT_KEY=<32B> CELLHIVE_DO_RUNTIMES=do-runtime-gated:8788 docker compose -f deploy/compose/docker-compose.yml --profile rpo0 up -d`:

- `ps`: `cell-agent Up (healthy)`, `user-runtime`, `do-runtime-gated` (+ gate-free `do-runtime` idle) are all Up;
- `cell-agent /readyz` = `{"status":"ready"}`; user-runtime `:8081` is reachable (`/`→404 is normal);
- Gate `GET :18901/status` is available;
- Runtime-baseline runs `cellhive app create` plus a TypeScript source deploy inside the image. A Bun client reaches an ephemeral loopback publication of public `user-runtime :8081` and completes a real WebSocket 101 plus duplex messages. The shared `Counter` DO persists HTTP `1` → WS `2` → gated do-runtime restart → WS `3` → HTTP `4`, with authoritative bucket LTX produced.

- The same isolated Compose E2E also checks versioned static assets from an in-image source deployment, Queue consumption and DLQ/operator replay, minute-aligned Cron, Workflow `step.do` → `sleep` → resumed result and `createBatch`. A second release becomes visible on the public route, `rollback` restores the previous one, and deleting the worker yields a 404. Actual run on 2026-09-22: `PATH=/usr/local/go/bin:/root/.bun/bin:$PATH bash scripts/runtime-baseline-e2e.sh` ended with `RUNTIME-BASELINE-E2E: PASS`.

## Deployment Artifact Validation (ADR-139)

Not part of `go test`; run manually/in CI:

- `make docker-build` — image build (including pinned workerd);
- `make compose-config` — `docker compose config` syntax/variable validation;
- `make k8s-render` — `kubectl kustomize deploy/k8s` (client-side render, no cluster required);
- Historical ADR-139 evidence: the container then reported workerd `2026-06-15` and the services listened. The ADR-186 image must read back `2026-09-16` and esbuild `0.28.2`, then be reaccepted through the runtime-baseline E2E; the historical evidence cannot be reused.

## DO Output Gate Deployment (ADR-140)

- Unit tests: `internal/doruntime TestRenewLoopPostsWithToken`/`TestRenewLoopReportsErrors`/`TestDrain` (renewal with internal token, failures visible, drain hits the right endpoint).
- Container end-to-end (manual, see ADR-140): bring up `do-runtime-gated` → gate `/status` available, renewal failures 0, SIGTERM drain normal; `/v1/do/invoke` returns `count:2..5` and writes to bucket `cells/workerd/__do__/<host>.sqlite/ltx/e1/*.ltx`.

## Package-Level Coverage

`go test ./...` covers **54/68** packages. The **14** packages without standalone unit tests are:

- Service entrypoints `cmd/user-runtime`, `cmd/do-supervisor`: covered by integration/e2e (`internal/userruntime`, `internal/doruntime`, `cmd/cellhive` e2e) (`cmd/cell-agent`, `cmd/do-runtime`, and `cmd/cellhive` themselves have unit tests);
- Load-test/tools `cmd/{cellbench,realbench,sqlbench,kvbench,gatebench,s3init,s3probe,walscan,recoververify,restoreverify,cell-supervisor}`: one-off CLIs, triggered by `make s3-test`/`make perf-test`;
- `internal/workerdbin`: binary discovery, indirectly covered by `make js-test` (real workerd).

## To Be Refined

- CI pipeline and sharding (current entrypoints: `make test` / `js-test` / `cli-test` / `perf-test` / `rpo-test` / `s3-test`);
- Reproducible environment for performance benchmarks (fixed machine type/kernel/workerd pin);
- Chaos injection tooling and scenario library (currently relies on `internal/owner` simulation + `make rpo-test`).

## S3 Integration Test (Local MinIO/rustfs; Complete Contract Currently Fails)

One command is enough (no cloud credentials required): `make s3-test` (= `bash scripts/s3-integration.sh`). If `CELLHIVE_S3_TEST_ENDPOINT` is not set, the script automatically starts a MinIO container (`S3_IMAGE` can be changed to rustfs), then cleans it up after the run; it can also point to an existing S3 endpoint.

Coverage: `s3init` (bucket + presign), `s3probe` (four conditional-write checks and ranged read, **not conditional delete**), `TestS3BucketIntegration` (now also rejects stale conditional delete and checks successful current-version deletion), and `TestS3ReplicationRestoreChain`. On 2026-09-22 `GO=/usr/local/go/bin/go bash scripts/s3-integration.sh` passed writes/range/replication but **failed because the pinned MinIO returned success for stale `DeleteObject If-Match`**. This image cannot be the authority; a successful `s3probe` alone is insufficient.

```bash
make s3-test
# Or specify an existing endpoint:
CELLHIVE_S3_TEST_ENDPOINT=http://127.0.0.1:9000 make s3-test
```

- `TestS3BucketIntegration`: create/reject-create/CAS/reject-stale/range/presign/**conditional delete**. The pinned MinIO fails the stale delete assertion.
- `TestS3ReplicationRestoreChain`: snapshot→Restore→ApplyFile + Compact→PageFetcher (ranged)→Materialize.
- An isolated Go package test skips without `CELLHIVE_S3_TEST_ENDPOINT`; `make s3-test` starts MinIO and runs the probe, and does not skip.

## DO Compatibility Suite (ADR-083/084)

Real workerd e2e (`internal/doruntime`), progressively summarizing all aspects of the P3 exit criteria:

| Aspect | Test |
|---|---|
| Synchronous SQL / transactions | `TestDOCompatSuiteSyncSQLAndTransactions` (`ctx.storage.sql` + `transactionSync` + persistence) |
| alarm | `TestDoRuntimeAlarmShim` (set/get/delete + dispatch + reschedule) |
| WebSocket 1012 | `TestDoRuntimeWebSocketAbort` |
| Version restart / migrations | `TestDoRuntimeVersionRestart`, `TestDoRuntimeClassRenameKeepsStorage` |
| eviction config | `TestRenderResidencyAndDisk` (resident/evictable) |
| owner forwarding / result_unknown | `TestDoRuntimeOwnerForwardAndResultUnknown` |
| Cross-node cold activation | `TestDoRuntimeCrossNodeColdActivation` |
| Per-object cold start | `TestDoRuntimePerObjectColdStart` (only restores `storage_id/class/name` → state continues) |
| WS cross-node forwarding | `TestDoRuntimeWebSocketCrossNodeForward` (B proxies to owner A; abort→`CLOSE:1012` passthrough) |
| Public entry→tenant→DO WebSocket | `TestTenantDoWebSocketPassesThroughPublicLoader` (real workerd: public 101 + duplex frames + zero tenant platform keys); runtime-baseline Docker connects before and after a gated do-runtime restart |
| Bindings inside DO | `TestDoRuntimeBindingInsideDO` (`env.KV` round trip inside DO) |
| deleteAll | `TestDoRuntimeDeleteAllCaptured` (KV), `TestDoRuntimeDeleteAllSQLCaptured` (SQL + empty after cold start) |
| migrations transfer | `wranglercompat` same worker `{from,to}` accepted, `script_name` rejected |
| Process-level takeover | `TestDoRuntimeTakeoverAfterCrash` |
| Output gate | `TestDoRuntimeOutputGate` |
| Capture/restore | `internal/dosupervisor`: `TestCaptureRestoreRoundTrip`, `TestParseFacets`, `TestPagedRestoreUsesRangedReads`, `TestObjectScopesAndRestoreObject` |

**Unified entrypoint**: `go test ./internal/doruntime/ -run TestDOCompatSuite -v` (all 19 subtests PASS).

Object storage primitives: `objectstore.Objects` (`TestObjectsPrefixScopeAndSafety`: prefix normalization, traversal/absolute/out-of-bounds rejection, round trip, List); reserved-prefix ownership guards (`TestReservedPrefixGuard`, `TestReservedPrefixesDisjointAndOwned`).

Hot path: `TestSyncAllSkipsUnchangedBlobs` (steady-state 0 PUT), `TestRefreshFacetsPicksUpNewFacet` (incremental cache invalidation); commit proof `TestHTTPCommitterProofModes` (fleet/bucket/bucket-batch pass, bucket-async rejected).

P4 (ADR-087): `internal/admission` (`TestLimiterBurstAndRefill`/`TestDisabledLimiter`), `internal/server TestAdmissionRateLimit`, `internal/control TestDeployIdempotencyAndReleases`, `internal/autoscaler TestAdvisorScalesUpForLoadAndPressure`.

RPC entrypoint bindings (ADR-090):
- DO host: `TestDoRuntimeBindingInsideDO` (KV, this.env + parameters), `TestDoRuntimeD1R2QueueInsideDO` (D1/R2/Queue), `TestDoRuntimeDoInsideDO` (DO→DO), `TestDoRuntimeWorkflowInsideDO` (Workflow).
- fetch host: `TestUserRuntimePublicLoaderBuildsBindingFacades` (KV), `TestUserRuntimePublicLoaderD1R2Queue` (D1/R2/Queue), `TestUserRuntimeDurableObjectBinding` (DO), `TestUserRuntimeRunsWorkflow` (Workflow), `TestUserRuntimeServiceBinding` (Service fetch), `TestUserRuntimeServiceBindingRPC` (Service RPC).
- cell-agent handler: `TestServiceFetchProxy` (service.fetch scope validation/projection/dispatch).

Workflows (ADR-086): `internal/workflow` unit tests, `internal/server TestWorkflowEndpoints`, `internal/dispatch TestWorkflowDispatcherRunAndSleepTimer`, real workerd `TestUserRuntimeRunsWorkflow` (step memoization + sleep redispatch), `wranglercompat TestWorkflowBindingRequiresClassName`.

Restore without bucket credentials: `TestRestoreViaAgentNoBucketCreds` (`internal/dosupervisor`) + `TestInternalBlobRoundTrip` (`internal/server`, `/v1/internal/blob` auth/prefix/traversal).
Agent path on-demand paging: `TestAgentPagedRestoreUsesRangedReads` (`internal/dosupervisor`) + `TestReadSegmentRange` (`internal/server`, Range 206).

Already implemented: `transferred_classes` (same-worker alias, ADR-107 boundary), WS cross-node forwarding (`TestDoRuntimeWebSocketCrossNodeForward`, ADR-084).

Object GC: `TestGCAdminEndpoints` (server: bundle/assets two-phase mark→delete, reference retention), `internal/objgc` units, `internal/artifacts` List/Delete, `internal/control` BundleRefs/AssetRefs/GCMark.
Queue concurrency: `TestRunnerMaxConcurrency` (concurrency limit + default non-regression), `TestConsumeSpecs`.
R2 multipart: `TestR2Multipart` (store: parts invisible/etag fail-closed/ordered assembly/abort), `TestUserRuntimePublicLoaderD1R2Queue` (real workerd: four facade endpoints + assembled read-back).
Local disk: `TestHasShedTarget` (only active nodes with headroom enter pressure state), `TestSelectDiskEvictionsLazy` (only considered over budget, only considered files are selected), `TestCovers` (L1 manifest coverage decision), `TestOverloadedRefusesClaims` (under pressure readyz/claim/write-claim return 503, recover after release, ADR-123), `TestPlanDiskEviction` (budget/LRU/owned protection), `TestDiskFilesAndForget` (ledger/idempotent delete, ADR-122).

Write-path audit: `TestTimerUpsertForwardsToOwner` (timer upsert forwarding/claiming, ADR-121).

Read forwarding/retry: `TestReadForwardingAndRetry` (non-owner reads are forwarded and have no local replica; stale owner 409→retry exactly once; dial failure→502; ADR-120).

Queue ownership/capture: `TestOwnerGate` (only owner consumes, takeover ≤1s after release), `TestRunnerFilterSkipsQueues`, `TestRunnerCommitWrapsMutations` (ADR-119).

Route revocation/throttling: `TestUserRuntimeRoutingScale` (real workerd: Host scan throttled to ≤8 upstream queries; under 10s host TTL, polling `/v1/control/routes` ETag makes revocation take effect in <5s, ADR-116).

Route reads: `TestControlHostAndWorkerEndpoints` (host/worker endpoints 200/304/404, pointer trimming, revision invalidation), `TestUserRuntimeHostCacheGovernance` (real workerd: negative cache/singleflight/TTL revalidation/failure backoff/stale-on-error/deleted and newly added host, ADR-115).

dev CLI (`make cli-test`, Bun): `cli/test/assets.test.ts` (assets mapping unit test), `cli/test/dev-assets-e2e.test.ts` (real Miniflare: `_headers`/`_redirects` 302/`not_found_handling` 404/`run_worker_first` path/worker fallback).

isolate/env cache: `TestUserRuntimeBindingOnlyRedeployTakesEffect` (real workerd: same `bundle_sha`, new version number, changed `MODE` must take effect; verified to fail with fallback id, ADR-126).
Internal dispatch version key (ADR-127): `internal/userruntime TestUserRuntimeServiceDispatchVersionRefreshesEnv` (internal `/v1/services/run` same sha, version 1→2, changed vars must take effect) and `internal/doruntime TestDoRuntimeSameShaRedeployRefreshesFacet` (DO facet with same sha and changed version must rebuild env) — both have been verified to fail with the “fallback fix”.
Local connection reuse (ADR-130): `internal/doruntime TestDoRuntimeHoldsConnectionAcrossRequests` (real workerd: tenant DO holds loopback TCP connection; two invokes reuse it → server-side accepts==1; verified to fail with “reconnect per request”).
Route mount semantics (ADR-131): `TestUserRuntimeRouteStripPrefix` (real workerd: after mounting `/api`, worker sees `/users`, POST body/query retained, `path=''` does not strip, `/apix` does not match `/api`) and `internal/server TestRouteMountSemantics` (projection/host view includes mount path).
ADR-134 review fix batch: authorization `TestScopedCredentialAuthorizationMatrix` (cross-ns 403 for rollback/asset/logs/deploy/secret), `TestScopedCredentialForwarding` (authorization before forwarding + apps filtered after retrieval), `TestPromoteRollbackInvalidatesBindingCache`; RPO watermark `internal/cellcapture TestWaitBlocksUntilCommitted` + `internal/server TestCapturedStoresAdvanceCellTxID` (rolling back control.tx will fail); loading `TestUserRuntimeColdLookupFailureRecovers`, `TestUserRuntimeNodejsCompatFlagApplied` (rolling back the fixes will fail both), `TestWorkerEnvVersionSHAAndCompat`; ADR-135 legacy fixes: `internal/control TestPurgeSkipsResurrectedWorker/App` (rolling back Deploy cleanup will fail), `internal/server TestCaptureCommitEpochFence`, `internal/doruntime TestDoRuntimeConnectMatchesInvokeIdentity`, `internal/cellstore TestDeleteNamespaceDrainsAndDrops`, `internal/cellcapture TestEnsureDoesNotBlockOtherScopes` (rolling back to taking snapshots under the lock will fail), `internal/auth TestJWKSServesStaleDuringRefresh`/`TestJWTWithoutExpiryRejected`, `cmd/cellhive TestCLISubcommandArity`.
Storage `internal/bucket TestDiagnoseDetectsConditionalDeleteIgnored` (simulates a bucket that ignores If-Match; diagnose must name the failure explicitly) and the CLI e2e `diagnose()` call, `internal/cellstore TestForgetWaitsForInFlightRequest` (rollback will fail)/`TestForgetWithVerifyRefuses`, `TestValidateRejectsInsecureFleetSecrets`, millisecond fix in `internal/autoscaler`, `internal/wrangler` hyperdrive is not rejected.
ADR-138 (wrangler aliases): `cmd/cellhive TestTranslateWrangler` (deploy auto-discovery/`--name`/`--env`, mappings for `delete`/`versions list`/`secret`/`tail`, errors for unsupported flags and commands), `TestTranslateWranglerExplicitConfig` (`-c` takes precedence; explicit error when config and bundle are both missing), `TestDiscoverWranglerConfig` (jsonc takes precedence).
ADR-137 (configuration simplification): `cmd/cell-agent TestDerivedSecretKeyDecodes` (the derived secrets-root must be decodable by `control.ParseRootKey` into 32B—rolling back to RawURL will fail), `cmd/cellhive TestCmdCreds`; `make rpo-test` end-to-end under root-derived credentials **PASS** (all 1200 acked keys present and exact). `internal/config TestDeriveCredentials` (determinism/domain separation/different roots differ), `TestLoadRootKey`, `TestFromEnvDerivesRoleCredentials`, `TestFromEnvStorageNames` (`AWS_*`), `TestFromEnvDurationKnobs`, `TestFromEnvMergedKnobs`, `TestValidateRootKey`, `TestLegacyEnvWarnings` (old variable names must warn); `cmd/do-runtime TestDoLeaseSeconds`/`TestRuntimeRoot`; `cmd/cellhive` e2e switched to shared `CELLHIVE_ROOT_KEY`.
ADR-136 (internal protocol/wiring): `internal/config TestFromEnvAdvertiseDefault` (ADVERTISE defaults to `127.0.0.1:7001`), `cmd/cell-agent TestNewLogBufferIsWired` (buffer non-nil + round trip + source guard `Logs: newLogBuffer(cfg)`; removing the wiring fails), `cmd/do-runtime TestEnvTrue` (`DO_OBJECT_INDEX` has the same semantics as cell-agent).
Control-plane schema v2 / domains / authorization (ADR-131): `internal/control` (soft delete + purge job, `hosts` ownership coexists with pending, `bindings` derived table), `internal/server TestBuiltinDomainOnDeploy` (deploy creates the hosts row and route for `<ns>-<worker>.cell.base`), `TestCustomDomainRegistration` (unregistered 404, routes immediately after registration, 404 after `domain rm`), `TestHostOwnershipConflict` (pending can take over, verified returns 409, built-in domains cannot be claimed), `TestAdminNamespaceAuthorization` (JWT `cellhive_ns` authorization, lists filtered by ns, platform-level requires `*`, audit records `sub`), `TestSoftDeleteApp` (soft delete stops routing immediately + purge cleans up).
Hyperdrive resources (ADR-129): `internal/control TestResourceConfigSealed` (ciphertext does not contain plaintext; refuses without root key), `internal/server TestHyperdriveResourceAndBinding` (register → compatibility gate passes, unregistered still rejected, inline URL compatibility, internal endpoint, spec injection), `internal/userruntime TestUserRuntimeHyperdriveResolvesFromPlatform` (real workerd: env receives the resolved URL rather than the id; binding omitted when resolution fails), CLI e2e adds `resource create --connection-string` + `deploy --hyperdrive NAME=NAME`.
Env for non-fetch handlers (ADR-128): `TestUserRuntimeDispatchInjectsBindings` (queue/scheduled dispatch injects bindings+vars, bindings request includes `version`, fail open when spec fails) and `internal/server TestInternalBindingsVersionPin` (`version=1` gets v1 env, omitted gets active, unknown version returns empty spec, invalid parameter 400)—the former has already been verified to fail with the “rollback fix”.

Hyperdrive: `internal/wrangler` mapping cases + `TestUserRuntimePublicLoaderD1R2Queue` (real workerd: tenant reads `env.HYPERDRIVE.{connectionString,host,port,user,database}`, ADR-125).

wrangler mapping: `internal/wrangler` (jsonc comments/trailing commas, bindings/consumer/cron/assets/migrations mapping, env inheritance, toml rejection, rejected sections, ADR-124); CLI e2e adds a second deployment with `--config` (releases=2 + vars effective).

CLI end-to-end (`cmd/cellhive` `TestCLIEndToEndWithWorkerCode`): real CLI (`app create` / `resource create --scope` / `deploy --bundle-sha --kv --route`) → in-process cell-agent control plane → **real workerd loader** → tenant worker code → `env.KV` facade → KV cell; asserts tenant response `kv:v1`, the CLI-written route/version appears in the projection, and the KV cell contains authoritative data. Automatically skipped when workerd is missing; covered by `make test`.

ADR-156 (vwork operations interface): `internal/control TestResourceRevoke` (reference report/idempotency/tombstone takes effect immediately and re-registration unblocks), `internal/server TestResourceDeleteEndpoint` (`resource_in_use` 409 → `force=1` 200 → disappears from list → after revocation, deploy declaring that binding is rejected by the compatibility gate → 404 → audit `resource.delete`), `internal/queue TestStatusCounts` (`depth/visible/leased`; delayed messages invisible, leases not counted in visible), `internal/server TestQueueStatusAndReplayDLQ` (dead-letter depth → `replayed:2` → main queue depth/visible=2, DLQ cleared → no DLQ 400 `no_dead_letter_queue`), `internal/userruntime TestUserRuntimePublicLoaderBuildsBindingFacades` (real workerd: after projection fetch, `/ready` 200 + `cell:"ok"`; wrong internal token `/drain` 401; after correct token, `/ready` 503 `draining`), `internal/doruntime TestDoRuntimeReadyAndDrainProbe` (real workerd: `/ready` 200, wrong token `/v1/do/drain` 401, after drain `/ready` 503 `draining`). Deployment artifacts: `make compose-config`/`make k8s-render`/`make helm-lint` (user-runtime and do-runtime probes switched to `/ready`).

ADR-157 (per-domain resources + stats): `internal/cellstore TestDiskStatsAndKVStats` (page/file fields, new stores must have the `kv_expires` index, `expired`/`next_expiry_ms`, dbstat estimates and `estimate_note`, `?exact` semantics, prefix scope) and `TestKVExpiryIndexUsed` (`EXPLAIN QUERY PLAN` must contain `kv_expires`—rolling back the fix will fail); `internal/d1 TestD1Stats` (internal tables excluded, dbstat details for `?tables=1`, row-count estimates) and `TestD1StatsLargeCellSkipsDetail` (must skip over-threshold cells instead of becoming slow); `internal/queue TestStatusCounts` (adds `oldest_visible_ms`/`max_attempts` + `DiskStats`); `internal/r2 TestStatsBoundedListing` (total/prefix/`truncated`+cursor/`multipart_*` counts/staging area invisible to user List/invalid bucket names); `internal/workflow TestStats` (estimated vs `?exact=1` status breakdown); `internal/server TestPerKindResourceEndpoints` (per-domain CRUD, server fills scope, body/path kind conflict 400, 409+`force`, **equivalent to `/v1/control/resource*` aliases**), `TestPerKindStats` (real-data assertions for seven stats types + old `/v1/control/queue/status` remains flat), `TestStatsFailClosedAndScoped` (unknown/revoked 404, JWT cross-ns 403), `TestMetricsBucketAndCellGauges` (`cellhive_list_calls_total`/`cellhive_bucket_ops_total{op="list"}`/`cellhive_resident_cells` must appear); `cmd/cellhive` e2e adds per-domain CLI for `kv namespace|queue` (create/list/stats/delete).

ADR-160 (paged cold start): `internal/pagedvfs` (`TestPagedOpenFaultsPagesOnDemand`: point reads fault only a few pages and after `HydrateAll` the file is byte-for-byte equal to the source; `TestPagedWritesMarkHydrated`: writes/checkpoint are not overwritten by the old cut; `TestBackgroundHydrateFillsTheFile`: rate-limited background hydration fills to full size and is byte-for-byte identical), `internal/cellstore TestPagedCellServesKVAndSnapshots` (KV point reads correct + `SnapshotPages` hydrates first + writes usable + Close releases), `internal/compaction TestPagedCellAgentEndToEnd` (real path: real cell → bucket snapshot → Compact emits L1 index → PageFetcher → cold cellstore paged open; measured 8/771 pages faulted, 8 ranged reads, whole object 3158B vs image 3158016B; **20 ranged reads for full table scan** due to window prefetch), `internal/ltx TestPageMapV2Compression`/`TestPageMapV1StillReadable` (`WAL3` compressed frames + v1 compatibility; benchmark `BenchmarkFrameDecode` 1.1GB/s), `internal/compaction TestL1SnapshotCompression` (L1 payload 4.4%), `TestCompactFoldsLegacyV1AndGCsCompressedL1` (**v1 WAL2 baseline + compressed L1 + GC**: old L1/folded L0 deleted, index/manifest retained, paged reads from compressed L1 and full DB restore remain correct), `internal/pagedvfs TestChildPrefetchIsConcurrent` (peak=4 workers, 6 scattered children 45.7ms), `TestRunPrefetchReducesBucketReads` (336-page full scan = 6 window reads), `TestParseChildren`/`TestFaultPolicyChildAloneAndWindow` (internal-page child fetched as a single page, sequential child hits the prefetch cache, windowing only starts for sequential non-child), `TestHydrateUsesRuns` (batch hydrate), `internal/cellstore TestPagedCellReopenAfterClose` (**regression**: after process restart, sparse cache is re-registered via marker bits; data is correct, not zero pages/corruption), `internal/server TestMetricsPagedStats`, `internal/cellstore TestDiskUsageCountsAllocatedBytes`/`TestDiskEvictionUsesAllocatedBytes` (sparse files are measured and evicted by `st_blocks`).

Cold recovery boundary: `internal/replica TestColdHydrateIsWholeObjectNotPaged` (**measured**: cell-agent hydrate path = 1 whole-object download 4198464B, **0 ranged reads**; paging primitives can materialize only some pages; ranged-read path is covered by `internal/compaction TestPageFetcherOnDemandFromL1Index`)—changing back to "paged hydrate" must explicitly update this test.

ADR-161 (compression switch): `internal/ltx TestPageMapCompressionDisabledWritesV1` (when `CELLHIVE_LTX_COMPRESSION=false`, outputs `WAL2` fixed frames + fixed off layout, `PageLocs`/`DecodeWALPageMap` round-trip correctly).
ADR-167 (OTLP tracing): `internal/telemetry TestExportsSpanAndRemoteSpan`/`TestSamplerHonorsRemoteFlag`/`TestDisabledIsNoop` (in-process OTLP receiver decodes protobuf and verifies export/parent-child/sampling/disabled no-op), `internal/userruntime TestUserRuntimeTraceExport` (real workerd: loader `http.server` span sent to `/v1/internal/telemetry/spans`), `internal/doruntime TestDoRuntimeSpanExport` (real workerd: `do.invoke` span), `internal/dosupervisor TestGateEmitsSpan` (`do.gate` span exported, and trace/parent span continue the caller’s `traceparent`), `internal/doruntime TestDoRuntimeBindingInsideDO` (asserts the known boundary that props-bound binding calls inside DO currently do not carry traceparent), `TestUserRuntimeTraceContextPropagation` (props-bound binding now carries traceparent).
ADR-166 (replication/timer/do metrics): `internal/timer TestFireStatsAggregates`/`TestDispatchDueObservesFireOutcomes`, `internal/dosupervisor TestSupervisorMetricsAndStats` (`/internal/do/stats` increments + `/metrics` rendering + ws clamp-to-zero), `internal/doruntime TestDoRuntimeMetricsReportedToSupervisor` (real workerd: host reports increments after alarm), `internal/server TestMetricsObservability` (replication/waker lines).
ADR-165 (observability metrics): `internal/server TestMetricsObservability` (`cellhive_binding_calls_total{kind,outcome}`, `cellhive_durability_proof_seconds` buckets/sum/count, `cellhive_route_projection_version`, owner/takeover and hedge lines), `internal/owner TestClaimStatsTakeoverAndEpoch` (fresh/blocked/takeover success + epoch bumps), `TestClaimAsStats`, `internal/peer TestShipBatcherHedgeStats` (fired/won).
ADR-164 (peer adaptive hedge + idempotent spool): `internal/peer TestSpoolIdempotentPerSequence` (repeated append of the same segment is a no-op; identity set is rebuilt from the log after restart), `TestShipBatcherHedgeSkipsSlowBackupWhenPrimaryFast` (if primary acks in time, the second follower is not contacted), `TestShipBatcherHedgeFiresOnSlowPrimary` (send a second copy on timeout; first arrival wins), `TestShipBatcherHedgeOffSingleCopy` (`=0` only sends to primary; sequential failover when primary fails), `TestShipBatcherHedgeAllFail`, `TestShipBatcherHedgeAsync` (async stream path: primary writes synchronously, hedge sent afterward), `TestShipBatcherAdaptiveHedgeWait` (4× recent slowest, 250ms floor, backstop ceiling).
ADR-173 (log subscription fleet broadcast): `internal/server TestLogSubscribeFleetFanout` (admin subscription broadcasts an internal subscription to another live node; internal endpoint takes effect locally).
ADR-175 (compatibility closure): `internal/dispatch TestDoAlarmDispatcherAddsScheme` (do-runtime address without scheme), `internal/server TestDOAlarmOccurrenceCarriesIdentity` (occurrence carries storage_class/storage_id, `|` safe); DoAlarmDispatcherRoutesAlarms asserts that alarm body replays storage identity. Real gated stack matrix: alarm/wsecho/r2, etc. all PASS.
ADR-174 (example compatibility): `internal/userruntime TestUserRuntimeKVPutStreamAndBytes` (KV `put` ReadableStream/Uint8Array stored as-is), `internal/server TestKVValidationLimits` (key≤512B/metadata≤1KiB/ttl≥60s/expiration in the future)
ADR-177 (wake index fix): `internal/timer TestUpsertFailsClosedWhenIndexDown` (timer does not commit when index is unavailable), `TestSyncIndexRepairsMissingEntry`, `TestStoreSyncsWakeIndex` (publish before commit); `internal/cellstore TestLocalScopesAndPendingTimerMin` (local scan + read-only read of earliest due)., `internal/doruntime TestDoRuntimeClassicDOAndStatus` (legacy DO class wrapper + 426 status and `x-cellhive-do-app` passthrough + legacy state persistence), `internal/server TestDOProxyForwardsTenantResponse` (cell-agent passes through 426/content-type/marker).
ADR-172 (OTLP log export): `internal/telemetry TestLogExportModes` (`off` does not export, `all` exports all, `tail` does not export before subscription/exports after subscription/stops after TTL expiry), `TestLogSubscribeTTL`; `internal/server TestLogSubscribeEnablesExport` (`POST /v1/control/logs/subscribe` + `LogSubscribed`).
ADR-171 (spool power-loss safety): `internal/upload TestSpoolDurableAppendSyncs` (fsync file on every append, memoized directory fsync, Load readable), `TestBatcherStatsIncludeSpoolDurability` (`/metrics` exposes fsync count).
ADR-170 (DO concurrency capture + ordered sweep): `internal/server TestOrderedDispatcherDoSweepsIdleScope` (Do triggers eviction of idle scope), `internal/dosupervisor TestSyncAllPipelinesAcrossFiles` (6 files/4 concurrency: max in-flight≥2, total duration<serial, failure returns error); `-race`.
ADR-169 (purge paginated deletion): `internal/objectstore TestObjectsListPage` (cursor pagination/prefix boundary/reserved prefix rejection), `internal/purge TestPurgePaginatesAcrossBudget` (budget 10 / 25 keys drained across multiple rounds).
ADR-168 (R2 list include/delimiter): `internal/r2 TestListPageDelimited` (delimiter coalescing, prefix narrowing, limit/truncated+cursor), `internal/server TestR2ListDelimiterAndInclude` (`include` backfills sidecar metadata on demand, `delimitedPrefixes`, unknown include 400), `internal/userruntime TestUserRuntimeR2ObjectFidelityAndD1Meta` (real workerd: `list({delimiter,include})` passthrough + returns `delimitedPrefixes`/object metadata).
ADR-163 (R2/D1 contract fidelity): `internal/r2 TestR2MetadataStatAndChecksums` (metadata sidecar round trip/`Stat`/checksum validation/delete clears sidecar), `internal/d1 TestLastInsertID` (`last_row_id` for exec/query/batch), `internal/userruntime TestUserRuntimeR2ObjectFidelityAndD1Meta` (real workerd: R2ObjectBody fields + `text()` + `head()` + list `truncated/cursor` + put metadata headers + D1 `meta.last_row_id`/`D1_ERROR`).
ADR-162 (DO RPC): `internal/doruntime TestDoRuntimeRPCDispatch` (real workerd: native JSRPC for regular tenant methods, tagged Map/Date round trip, tagged Map parameters, shared references/cycles, handler error 500, missing method 404, reserved/`__ch*` methods 400, non-serializable result 500), `TestDoRuntimeDurableObjectToDurableObjectRPC` (call sibling DO from inside a facet via injected facade), `internal/userruntime TestUserRuntimeDurableObjectRPC` (real user-runtime: `getByName` → tagged round trip → structured Error `code`/`message`), `internal/server TestDOProxyRPCPassthrough` (rpc bytes passthrough such as `1e21`, request/rpc mutual exclusion, kind validation, 8 MiB over-limit 400).
ADR-159 (CGo + vec1): `internal/vectorize TestVec1CellReplicatesThroughLTX` (**LTX replication end-to-end**: vec1 index + ANN model → cellcapture snapshot/delta → `restore.ApplyFile` restores to new cellstore → query results/model/delete/metadata are consistent, writable after restore), `TestVec1ExtensionRegistered` (`vec1_info()` works and vec1 vtab can be created even without `.so`; proves static registration succeeded), store unit tests switched to vec1 backend (insert-only/upsert replacement, cosine/euclidean score mapping, namespace pushdown, 9 metadata filters, limit and config immutability, dot-product rejection, `BuildANN`/`DropANN`), `BenchmarkQueryVec1` (20k×256 flat ≈ 6.3ms; ANN ≈ 0.22ms reproduced by `cellhive vectorize rebuild`); `internal/server TestVectorizeANNEndpoints` (rebuild/stats ann/drop-ann/dot-product 400); driver regression `go test ./...` (all packages) + `scripts/ci.sh` (tags passed through by Makefile/ci.sh, S3 replication/restore chain retested).

ADR-158 (Vectorize): `internal/vectorize` (store unit tests: `insert` does not overwrite existing id / `upsert` full replacement, three score types and ordering for cosine/euclidean/dot-product, namespace, 9 filter operations including nested dot paths and implicit AND, dimension/metadata/id limits, `topK` clamping, metadata index catalog and 10-entry limit, `describe`/`listVectors`/`stats`/`queryById`; `TestTopKWindowOrdering` (middle insertion into a partially filled window must shift and must not leave empty slots—a real bug caught by container smoke), `BenchmarkQueryExactScan` provides latency table); `internal/server TestVectorizeEndpoints` (missing config rejected → create → `/v1/vectorize/stats` → binding-side insert/query/filter/queryById/get/list/describe/dimension 400/delete → metadata index CRUD → after revocation 403 `binding_not_registered` → cross-ns scoped token 403); `internal/userruntime TestUserRuntimeVectorizeBinding` (**real workerd**: all facade methods, `ns=acme&index=docs` addressing, JS-minted token can be verified by Go, response shape matches CF); `internal/wranglercompat TestVendorMatrixContract` (vectorize in support matrix + lookup registration with `index_name`); CLI `cli/test/bindings-parity.test.ts` (dev reports explicit error for vectorize and is no longer a platform rejection item).

_Last updated: 2026-09-19_
