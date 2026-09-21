# Tenant Env User-Owned Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove every platform-owned key and naming restriction from tenant Worker `env` while preserving workflow durability and, when supported by pinned workerd, platform log tailing.

**Architecture:** User vars and user-named binding stubs are the only values materialized in loaded Worker env. Workflow transport moves from `env.CH_PLATFORM` to a props-bound capability passed as a native JSRPC argument. Logging moves to stock workerd Tail Workers after a fixed-version test; if unsupported, platform log capture is removed instead of restoring an env key.

**Tech Stack:** Go, JavaScript ES modules, stock workerd `1.20260615.1`, Cap'n Proto, native JSRPC, Go `testing`/`httptest`.

## Global Constraints

- Do not fork or modify workerd.
- Tenant `env`, including `import { env } from "cloudflare:workers"`, contains no platform-owned key.
- Users may define `CH_*`, `CELL_*`, `__cellhive*`, `PLATFORM`, `LOG_*`, and `WF_*` names.
- Tenant outbound remains public-only; platform URLs, credentials, and private fetchers stay outside tenant code.
- Workflow identity is bound by the trusted host and cannot be overridden by tenant parameters.
- Logging is best-effort and cannot fail a tenant request.
- If dynamic Tail Workers fail on pinned workerd, disable platform log capture and document it.
- Follow TDD: observe each intended test failure before changing production code.
- Update `docs/` before or together with behavior changes.

---

## File Structure

- `docs/decisions.md`: ADR-185, superseding ADR-184's remaining `CH_PLATFORM` and reserved-name conclusion.
- `internal/wranglercompat/{wranglercompat.go,wranglercompat_test.go}`: remove deploy-time platform name reservation.
- `internal/server/{control.go,control_test.go}`: remove secret-name reservation.
- `workerd/user-runtime/{loader,internal,workflow-wrapper,queue-wrapper}.js`: zero platform env keys and JSRPC workflow capability.
- `workerd/do-runtime/host.js`: zero platform env keys in facet env.
- `workerd/platform/bindings.js`: retain the fixed-op workflow capability without injecting it into env.
- `workerd/platform/log-tail.js`: remove the old env-based logging path.
- `workerd/platform/tail.js`: create only if the pinned dynamic-tail test passes.
- `internal/{userruntime,doruntime}`: real workerd acceptance and rendered tail service when supported.
- Current docs and maintained English translations: reflect exact final behavior.

---

### Task 1: Record ADR-185 and remove platform name reservation

**Files:**
- Modify: `docs/decisions.md`
- Modify: `internal/wranglercompat/wranglercompat_test.go`
- Modify: `internal/wranglercompat/wranglercompat.go`
- Modify: `internal/server/control_test.go`
- Modify: `internal/server/control.go`

**Interfaces:**
- Consumes: `wranglercompat.Validate(Input) Result`; `POST /v1/control/secret`.
- Produces: all formerly reserved names are valid user names; `ReservedEnvName` is removed.

- [ ] **Step 1: Add ADR-185**

Document zero platform env keys, user ownership of all names, workflow capability arguments, native-tail preference, and the no-tail fallback. Mark only the conflicting parts of ADR-184 as superseded.

- [ ] **Step 2: Write the deploy acceptance test**

Replace `TestReservedEnvNamesRejected` with `TestFormerPlatformEnvNamesAllowed`. Use vars/bindings named `CH_PLATFORM`, `CELL_URL`, `__cellhive_test`, `PLATFORM`, `LOG_TOKEN`, and `WF_ID`, then assert:

```go
for _, f := range append(res.Errors, res.Warnings...) {
    if f.Code == "reserved_env_name" {
        t.Fatalf("former platform name rejected: %+v", f)
    }
}
```

- [ ] **Step 3: Verify RED**

Run: `go test ./internal/wranglercompat -run TestFormerPlatformEnvNamesAllowed -count=1`

Expected: FAIL because current validation emits `reserved_env_name`.

- [ ] **Step 4: Write the secret acceptance test**

Add `TestSecretFormerPlatformNameAllowed`: write a base64 value under key `CH_PLATFORM`, read it back, and assert HTTP 200 plus exact decoded value.

- [ ] **Step 5: Verify RED**

Run: `go test ./internal/server -run TestSecretFormerPlatformNameAllowed -count=1`

Expected: FAIL with HTTP 400 / `reserved_env_name`.

- [ ] **Step 6: Implement the minimum policy removal**

Delete `ReservedEnvPrefixes`, `ReservedEnvNames`, and `ReservedEnvName`; remove their binding/var validation and secret-put checks. Preserve unrelated schema and supported-binding validation.

- [ ] **Step 7: Verify GREEN**

```bash
go test ./internal/wranglercompat -count=1
go test ./internal/server -run 'Test(ControlAdminDeployAndSecretRoundTrip|SecretFormerPlatformNameAllowed)' -count=1
```

- [ ] **Step 8: Commit**

```bash
git add docs/decisions.md internal/wranglercompat internal/server/control.go internal/server/control_test.go
git commit -m "feat(env): make tenant env names user-owned"
```

---

### Task 2: Remove `CH_PLATFORM` and pass workflow transport over JSRPC

**Files:**
- Modify: `internal/userruntime/userruntime_test.go`
- Modify: `internal/doruntime/compat_test.go`
- Modify: `workerd/user-runtime/loader.js`
- Modify: `workerd/user-runtime/internal.js`
- Modify: `workerd/user-runtime/workflow-wrapper.js`
- Modify: `workerd/do-runtime/host.js`
- Modify: `workerd/platform/bindings.js`

**Interfaces:**
- Consumes: `ctx.exports.PlatformBridge({props})`.
- Produces: `CellHiveWorkflow.handleRun(className, eventJson, workflowBridge)`; tenant env contains only vars/bindings.

- [ ] **Step 1: Strengthen env purity tests**

Update `TestTenantEnvHasNoPlatformCredentials` to compare handler env and imported `cloudflare:workers` env. Declare user var `CH_PLATFORM=user-value`; assert it survives and `CELL_URL`, `CELL_TOKEN`, and undeclared `PLATFORM` are absent. Add equivalent constructor-param and `this.env` assertions for DO.

- [ ] **Step 2: Verify RED**

```bash
go test ./internal/userruntime -run TestTenantEnvHasNoPlatformCredentials -count=1
go test ./internal/doruntime -run TestDoRuntimeBindingInsideDO -count=1
```

Expected: FAIL because the platform overwrites `CH_PLATFORM`.

- [ ] **Step 3: Rewrite the workflow scoping test**

Make `TestWorkflowStepsStubIsScoped` run a real workflow step without reading `env.CH_PLATFORM`. Include forged identity fields in the event payload and assert the cell-agent request still contains dispatcher-bound namespace/workflow/id/run.

- [ ] **Step 4: Verify workflow RED**

Run: `go test ./internal/userruntime -run TestWorkflowStepsStubIsScoped -count=1`

Expected: FAIL until the dispatcher passes the capability argument.

- [ ] **Step 5: Remove every platform env write**

In both `tenantEnv()` functions and `buildFacetEnv()`, remove reserved-name collision loops and `out.CH_PLATFORM` / `env.CH_PLATFORM`. Preserve user vars, binding stubs, and unknown-binding diagnostics.

- [ ] **Step 6: Pass the trusted capability**

In the workflow dispatcher construct:

```js
const bridge = ctx.exports.PlatformBridge({ props: {
  ns: body.namespace,
  worker: body.worker,
  workflow: body.workflow,
  id: body.id,
  run: body.run_token || "",
} });
```

Call:

```js
stub.getEntrypoint("CellHiveWorkflow").handleRun(className, eventJson, bridge)
```

Change `handleRun(className, eventJson, bridge)` and `makeStep(bridge, instanceId)` so fixed-op calls close over the argument, not `this.env.CH_PLATFORM`.

- [ ] **Step 7: Verify GREEN**

```bash
go test ./internal/userruntime -run 'Test(TenantEnvHasNoPlatformCredentials|WorkflowStepsStubIsScoped|UserRuntimeRunsWorkflow|UserRuntimeWorkflow)' -count=1
go test ./internal/doruntime -run 'TestDoRuntime(BindingInsideDO|WorkflowInsideDO)' -count=1
```

- [ ] **Step 8: Commit**

```bash
git add workerd/user-runtime workerd/do-runtime/host.js workerd/platform/bindings.js internal/userruntime/userruntime_test.go internal/doruntime/compat_test.go
git commit -m "refactor(env): move workflow transport out of tenant env"
```

---

### Task 3: Prove native Tail Workers or remove platform log capture

**Files:**
- Modify: `internal/userruntime/userruntime_test.go`
- Modify: `internal/doruntime/compat_test.go`
- Modify: `internal/userruntime/userruntime.go`
- Modify: `internal/doruntime/doruntime.go`
- Modify: `workerd/user-runtime/{loader,internal,queue-wrapper,workflow-wrapper}.js`
- Modify: `workerd/do-runtime/host.js`
- Delete: `workerd/platform/log-tail.js`
- Create on spike success: `workerd/platform/tail.js`

**Interfaces:**
- Consumes: dynamic Worker `tails: [{name: "tenant-tail"}]`; Tail Worker `tail(events)`.
- Produces on success: trusted log batches to `/v1/internal/logs`; on failure: explicit no-platform-tail behavior.

- [ ] **Step 1: Write the fixed-version spike test**

Add `TestWorkerLoaderNativeTail`. A dynamically loaded worker logs `native-tail-marker`; its Worker definition has `tails: [{name: "tenant-tail"}]`; the test asserts the trusted tail service receives the marker and a stable script identifier.

- [ ] **Step 2: Verify initial RED**

Run: `go test ./internal/userruntime -run TestWorkerLoaderNativeTail -count=1 -v`

Expected: FAIL because no tail service is wired.

- [ ] **Step 3A: On supported pinned workerd, implement native tails**

Create `workerd/platform/tail.js`. Its `tail(events)` derives namespace/worker from the platform-issued loaded Worker id, ignores tenant content for identity, normalizes console records, and sends through its own trusted `CELL_URL`, `LOG_TOKEN`, and `PLATFORM` bindings. Add the service to rendered Cap'n Proto and point loaded Worker `tails` at it. Add no tail binding to tenant env.

- [ ] **Step 3B: On rejected pinned workerd, implement the approved fallback**

Remove `log-tail.js` imports/source injection, `LOG_TAIL_SRC`, `__cellhiveLogFlush`, and `PlatformBridge.logSend`. Rename log tests to `TestUserRuntimeLogTailUnavailable` / `TestDoRuntimeLogTailUnavailable`; assert tenant requests succeed and no platform log entry appears.

- [ ] **Step 4: Remove the legacy path in either branch**

Delete every loaded-worker read of `env.CH_PLATFORM`, old console patch hooks, obsolete `PlatformBridge.logSend`, and `workerd/platform/log-tail.js`.

- [ ] **Step 5: Verify the selected branch GREEN**

Supported branch:

```bash
go test ./internal/userruntime -run 'Test(WorkerLoaderNativeTail|UserRuntimeLogTail)' -count=1 -v
go test ./internal/doruntime -run TestDoRuntimeLogTail -count=1 -v
```

Fallback branch:

```bash
go test ./internal/userruntime -run 'Test(TenantEnvHasNoPlatformCredentials|UserRuntimeLogTailUnavailable)' -count=1 -v
go test ./internal/doruntime -run TestDoRuntimeLogTailUnavailable -count=1 -v
```

- [ ] **Step 6: Commit**

```bash
git add internal/userruntime internal/doruntime workerd
git commit -m "refactor(logging): remove log transport from tenant env"
```

---

### Task 4: Add exhaustive env-surface regression coverage

**Files:**
- Modify: `internal/userruntime/userruntime_test.go`
- Modify: `internal/doruntime/compat_test.go`
- Modify as failures require: `workerd/user-runtime/*.js`, `workerd/do-runtime/host.js`

**Interfaces:**
- Consumes: zero-system-key runtime.
- Produces: exact-key assertions across all execution surfaces.

- [ ] **Step 1: Add the user-name matrix**

Use:

```go
[]string{"CH_PLATFORM", "CELL_URL", "CELL_TOKEN", "PLATFORM", "LOG_NS", "LOG_WORKER", "LOG_TOKEN", "WF_NS", "WF_NAME", "WF_ID", "WF_RUN_TOKEN", "__cellhive_test"}
```

Tenant code returns entries for those names; assert every declared value is unchanged.

- [ ] **Step 2: Cover every execution surface**

Exercise public fetch, queue, scheduled, service RPC, workflow constructor env, DO constructor env, and DO `this.env`. Compare keys against the exact declared set, not only a denylist.

- [ ] **Step 3: Observe remaining RED paths**

```bash
go test ./internal/userruntime -run 'Test.*Env.*UserOwned' -count=1
go test ./internal/doruntime -run 'Test.*Env.*UserOwned' -count=1
```

- [ ] **Step 4: Correct only dirty workerLoader env definitions**

Remove any leftover platform write/collision logic. Do not merely filter handler arguments while leaving imported env or DO env dirty.

- [ ] **Step 5: Verify GREEN and commit**

```bash
go test ./internal/userruntime -run 'Test.*Env.*UserOwned' -count=1
go test ./internal/doruntime -run 'Test.*Env.*UserOwned' -count=1
git add internal/userruntime/userruntime_test.go internal/doruntime/compat_test.go workerd
git commit -m "test(env): cover user-owned env across runtime paths"
```

---

### Task 5: Synchronize docs and run full acceptance

**Files:**
- Modify: `docs/{configuration,workerd-integration,compatibility-matrix,known-issues,release-notes}.md`
- Modify: `docs/modules/{bindings,user-runtime,do-runtime}.md`
- Modify maintained matching files under `docs/en/`.

**Interfaces:**
- Consumes: final Task 3 logging result.
- Produces: code-accurate current docs and final verification evidence.

- [ ] **Step 1: Remove stale current claims**

Search for `CH_PLATFORM`, `reserved_env_name`, “保留 env”, `PlatformBridge`, and `vars < namespace secrets < worker secrets`. Update current docs; leave historical ADR text intact with ADR-185 supersession notes.

- [ ] **Step 2: Document the measured logging boundary**

If native tails passed, record Tail Worker routing and pinned-version evidence. If rejected, mark platform tenant tail unsupported and record the exact error/boundary in `known-issues.md`.

- [ ] **Step 3: Check stale references**

```bash
rg -n 'env\.CH_PLATFORM|reserved_env_name|租户 env.*CH_PLATFORM|tenant env.*CH_PLATFORM' docs workerd internal
git diff --check
```

Expected: only intentional historical/absence assertions remain.

- [ ] **Step 4: Run full validation**

```bash
make fmt
make js-test
make test
make vet
make build
```

Expected: every command exits 0; skip/timeout/partial output is not PASS.

- [ ] **Step 5: Commit and read back Git state**

```bash
git status --short
git diff --check
git add docs workerd internal
git commit -m "docs(env): document zero platform keys in tenant env"
git status --short
```

Expected: clean worktree and all commits present on the current branch.

