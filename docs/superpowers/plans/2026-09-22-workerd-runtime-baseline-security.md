# Workerd Runtime Baseline Security Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Establish a fixed, auditable stock-workerd runtime baseline that keeps platform credentials out of tenant WorkerCode, ships pinned workerd/esbuild binaries, derives compatibility validation from the pinned upstream release, and rejects oversized final WorkerCode or workerLoader env before activation.

**Architecture:** Keep platform configuration in host-worker bindings populated with Cap'n Proto `fromEnvironment`; construct a strict child-process environment for both direct runtimes and the DO supervisor. Use a checked-in compatibility manifest generated from the exact upstream workerd source, and a focused Go `workerbudget` package plus a matching JS runtime guard with shared golden vectors. Validate deployments before the control transaction, then revalidate immediately before every dynamic `workerLoader.get()`.

**Tech Stack:** Go 1.27, JavaScript ES modules, Cap'n Proto workerd configuration, stock workerd `1.20260916.1`, esbuild `0.28.2`, Bun + Miniflare, Docker Compose, Go `testing` and real-workerd integration tests.

## Global Constraints

- Do not fork or patch workerd; use only stock `workerLoader`, bindings and Cap'n Proto configuration.
- Preserve cell + bucket authority, owner/epoch fencing, RPO=0 output gates, and the no-independent-Gateway architecture.
- Keep production control/services in Go; JS remains limited to `workerd/`, and Bun + Miniflare remains development-only.
- The exact production pins are workerd `1.20260916.1` and esbuild `0.28.2`; no ranges, `latest`, or implicit fallback.
- The final WorkerCode hard limit is `64 * 1024 * 1024` bytes.
- The workerLoader env platform limit is `1024 * 1024` bytes with `8 * 1024` bytes reserved headroom, giving a CellHive limit of `1016 * 1024` bytes.
- Tenant env remains entirely user-owned: zero platform keys and zero reserved names, including `CELL_URL`, `CELL_TOKEN`, `CH_*`, `CELL_*`, and `__cellhive*`.
- Platform URLs/tokens must not appear in tenant env, final dynamic module bytes, rendered Cap'n Proto files, process arguments, or logs.
- Bucket hot paths must not use List; do not add Redis/Valkey, NATS, etcd, APISIX, a second JS engine, or another state authority.
- Follow TDD: observe every new test fail for the intended reason before changing production code.

## File and Interface Map

- `internal/runtimeenv/runtimeenv.go`: strict `KEY=value` construction shared by both runtime launch paths.
- `internal/workerbudget/budget.go`: Go authority for code/env estimates and typed limit errors.
- `internal/workerdcompat/manifest.json`: generated compatibility metadata for the exact workerd pin.
- `internal/workerdcompat/manifest.go`: manifest loader and lookup API used by Go validation.
- `cmd/workerd-compat-gen/main.go`: deterministic generator from an explicit upstream checkout.
- `workerd/platform/budget.js`: runtime-side recheck using the same constants and golden vectors.
- `scripts/workerd-compat-probe.sh`: real-binary date/flag verification.
- `scripts/runtime-baseline-e2e.sh`: image/Compose/env/KV/DO/restart acceptance without implicit skips.

---

### Task 1: Record the Runtime Baseline ADR Before Code Changes

**Files:**
- Modify: `docs/decisions.md`
- Modify: `docs/workerd-integration.md`
- Modify: `docs/known-issues.md`
- Modify: `docs/en/decisions.md`
- Modify: `docs/en/workerd-integration.md`
- Create: `internal/wranglercompat/runtime_baseline_docs_test.go`

**Interfaces:**
- Consumes: approved design `docs/superpowers/specs/2026-09-22-workerd-runtime-baseline-security-design.md`.
- Produces: ADR-186 as the authoritative contract for Tasks 2–11.

- [ ] **Step 1: Add a documentation contract test that initially fails**

Add `internal/wranglercompat/runtime_baseline_docs_test.go`:

```go
package wranglercompat

import (
    "os"
    "strings"
    "testing"
)

func TestRuntimeBaselineADRIsCurrent(t *testing.T) {
    b, err := os.ReadFile("../../docs/decisions.md")
    if err != nil { t.Fatal(err) }
    s := string(b)
    for _, want := range []string{
        "ADR-186", "1.20260916.1", "esbuild 0.28.2",
        "fromEnvironment", "64 MiB", "1016 KiB",
    } {
        if !strings.Contains(s, want) { t.Errorf("decisions missing %q", want) }
    }
}
```

- [ ] **Step 2: Run the contract test and verify the expected failure**

Run: `go test ./internal/wranglercompat -run TestRuntimeBaselineADRIsCurrent -count=1 -v`

Expected: FAIL because ADR-186 is absent.

- [ ] **Step 3: Add ADR-186 and synchronized current-state wording**

ADR-186 must state the exact pins and limits, host-only `fromEnvironment`, recursive final-WorkerCode secret scanning, control-plane preflight plus runtime recheck, generated compatibility manifest, reader-before-writer upgrade, and that Tail remains disabled until its later dedicated phase. Mark implementation status as “设计批准，实施中” until Task 11.

- [ ] **Step 4: Run the contract test and documentation consistency searches**

Run:

```bash
go test ./internal/wranglercompat -run TestRuntimeBaselineADRIsCurrent -count=1 -v
rg -n "1\.20260615\.1|2026-06-22" docs --glob '!archive/**'
```

Expected: test PASS; old values remain only where explicitly labeled historical evidence.

- [ ] **Step 5: Commit the design contract**

```bash
git add docs/decisions.md docs/workerd-integration.md docs/known-issues.md docs/en/decisions.md docs/en/workerd-integration.md internal/wranglercompat/runtime_baseline_docs_test.go
git commit -m "docs: define workerd runtime baseline contract"
```

### Task 2: Remove Platform URL and Token from Final WorkerCode

**Files:**
- Modify: `workerd/user-runtime/loader.js`
- Modify: `workerd/do-runtime/host.js`
- Modify: `internal/userruntime/userruntime_test.go`
- Modify: `internal/doruntime/compat_test.go`

**Interfaces:**
- Produces: `platformConsts(spec)` containing only frozen non-secret binding-name metadata.
- Security invariant: recursive serialization of every object sent to `workerLoader.get()` contains neither canary URL nor canary token.

- [ ] **Step 1: Add failing source and real-runtime regression tests**

In both runtime packages add a static scan and retain the existing real env tests:

```go
func TestDynamicWorkerCodeContainsNoPlatformCredential(t *testing.T) {
    b, err := os.ReadFile("../../workerd/user-runtime/loader.js")
    if err != nil { t.Fatal(err) }
    s := string(b)
    if strings.Contains(s, "cellUrl: ${JSON.stringify(env.CELL_URL") ||
       strings.Contains(s, "cellToken: ${JSON.stringify(env.CELL_TOKEN") {
        t.Fatal("platform URL/token are rendered into final WorkerCode")
    }
}
```

Extend the loader test harness to record `JSON.stringify(workerCode)` before returning the stub and assert the canaries `https://platform-secret.invalid` and `token-canary-7f6e` do not occur in env, module names, text modules, binary modules, compatibility fields, or mainModule.

- [ ] **Step 2: Run tests and verify they fail on `platformConsts()`**

Run:

```bash
go test ./internal/userruntime -run 'TestDynamicWorkerCodeContainsNoPlatformCredential|TestTenantEnvHasNoPlatformCredentials' -count=1 -v
go test ./internal/doruntime -run 'TestDynamicWorkerCodeContainsNoPlatformCredential|TestDoRuntimeEnvUserOwned' -count=1 -v
```

Expected: new WorkerCode test FAIL due to `cellUrl`/`cellToken`; existing env tests remain PASS.

- [ ] **Step 3: Make generated wrapper metadata non-secret**

Replace `platformConsts(env, spec)` with:

```js
function platformConsts(spec) {
  const names = (kind) => Object.entries(spec || {})
    .filter(([, b]) => b && b.kind === kind)
    .map(([name]) => name);
  return `;const __cellhivePlatform = Object.freeze({ r2Bindings: ${JSON.stringify(names("r2"))}, doBindings: ${JSON.stringify(names("do"))} });\n`;
}
```

Update all callers and remove any equivalent URL/token rendering from `host.js`.

- [ ] **Step 4: Run targeted tests and JS syntax checks**

Run:

```bash
node --check workerd/user-runtime/loader.js
node --check workerd/do-runtime/host.js
go test ./internal/userruntime ./internal/doruntime -run 'WorkerCodeContainsNoPlatformCredential|EnvUserOwned|TenantEnvHasNoPlatformCredentials' -count=1 -p 1 -v
```

Expected: PASS.

- [ ] **Step 5: Commit credential removal**

```bash
git add workerd/user-runtime/loader.js workerd/do-runtime/host.js internal/userruntime/userruntime_test.go internal/doruntime/compat_test.go
git commit -m "fix(runtime): keep platform credentials out of WorkerCode"
```

### Task 3: Move Host Bindings to `fromEnvironment` for Direct and Supervised Runtimes

**Files:**
- Create: `internal/runtimeenv/runtimeenv.go`
- Create: `internal/runtimeenv/runtimeenv_test.go`
- Modify: `internal/userruntime/userruntime.go`
- Modify: `internal/userruntime/userruntime_test.go`
- Modify: `internal/doruntime/doruntime.go`
- Modify: `internal/doruntime/doruntime_test.go`
- Modify: `cmd/user-runtime/main.go`
- Modify: `cmd/do-runtime/main.go`
- Modify: `cmd/do-supervisor/main.go`

**Interfaces:**
- Produces: `runtimeenv.Build(values map[string]string) ([]string, error)`, sorted and validated.
- Produces: `userruntime.WorkerdEnv(Config) ([]string, error)` and `doruntime.WorkerdEnv(Config) ([]string, error)`.
- Changes: `Run(ctx, workerdBin, capnpPath string, childEnv []string) error` for both runtimes.

- [ ] **Step 1: Add failing strict-environment and rendered-secret tests**

```go
func TestBuildIsSortedAndRejectsInvalidNames(t *testing.T) {
    got, err := Build(map[string]string{"B": "2", "A": "1"})
    if err != nil { t.Fatal(err) }
    if !slices.Equal([]string{"A=1", "B=2"}, got) { t.Fatalf("env=%q", got) }
    if _, err := Build(map[string]string{"BAD=NAME": "x"}); err == nil { t.Fatal("wanted invalid-name error") }
}
```

Change `TestRenderSubstitutesConfig` to assert the canary values are absent and these bindings occur:

```text
(name = "CELL_URL", fromEnvironment = "CELLHIVE_HOST_CELL_URL")
(name = "CELL_TOKEN", fromEnvironment = "CELLHIVE_HOST_CELL_TOKEN")
```

- [ ] **Step 2: Run tests and verify rendered values fail the new assertion**

Run: `go test ./internal/runtimeenv ./internal/userruntime ./internal/doruntime -run 'BuildIsSorted|Render.*Environment|RenderSubstitutesConfig' -count=1 -p 1 -v`

Expected: FAIL because `runtimeenv` is absent and capnp still embeds text values.

- [ ] **Step 3: Implement strict child env and `fromEnvironment` bindings**

`runtimeenv.Build` accepts only `[A-Z_][A-Z0-9_]*`, rejects NUL, sorts keys, and returns a fresh list. Convert host-only URL/token/scope/dispatch/ticket/AI keys to `fromEnvironment`; keep ordinary numeric/topology settings rendered when they are not credentials.

`Run` must use exactly the supplied environment:

```go
cmd := exec.CommandContext(ctx, workerdBin, "serve", "--experimental", capnpPath)
cmd.Env = append([]string(nil), childEnv...)
cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
return cmd.Run()
```

- [ ] **Step 4: Wire all launch owners, including `do-supervisor`**

`cmd/user-runtime` and direct `cmd/do-runtime` retain the constructed `Config`, call `WorkerdEnv(cfg)`, and pass it to `Run`. `cmd/do-supervisor` constructs the same host values from its authoritative flags/env and derived credentials, then calls `doruntime.Run(..., doruntime.WorkerdEnv(cfg))`; it must not rely on environment changes made by the `do-runtime -render-only` subprocess.

- [ ] **Step 5: Prove missing env fails and present env succeeds on real workerd**

Add one minimal real-workerd test that starts the rendered config with an empty child env and expects a configuration/startup error, then with `WorkerdEnv(cfg)` and expects `/healthz` or `/status` success.

Run:

```bash
go test ./internal/runtimeenv ./internal/userruntime ./internal/doruntime ./cmd/do-supervisor -count=1 -p 1 -v
```

Expected: PASS with the pinned workerd available; no secret value appears in generated `.capnp`.

- [ ] **Step 6: Commit host env isolation**

```bash
git add internal/runtimeenv internal/userruntime internal/doruntime cmd/user-runtime cmd/do-runtime cmd/do-supervisor
git commit -m "feat(runtime): bind host secrets from child environment"
```

### Task 4: Upgrade the Exact Workerd Pin and Generate Compatibility Metadata

**Files:**
- Create: `cmd/workerd-compat-gen/main.go`
- Create: `cmd/workerd-compat-gen/main_test.go`
- Create: `internal/workerdcompat/manifest.json`
- Create: `internal/workerdcompat/manifest.go`
- Create: `internal/workerdcompat/manifest_test.go`
- Create: `scripts/workerd-compat-probe.sh`
- Modify: `internal/workerdbin/workerdbin.go`
- Modify: `internal/wranglercompat/wranglercompat.go`
- Modify: `internal/wranglercompat/wranglercompat_test.go`
- Modify: `cli/src/validate.ts`

**Interfaces:**
- Produces: `workerdcompat.Manifest() Data`, `Allowed(flag string) bool`, `MaxCompatibilityDate() string`.
- Manifest fields: `workerd_version`, `upstream_revision`, `source_sha256`, `max_compatibility_date`, `flags[]` with `name`, `experimental`, `cellhive_allowed`.

- [ ] **Step 1: Add failing pin and manifest tests**

```go
func TestManifestMatchesPinnedWorkerd(t *testing.T) {
    m := Manifest()
    if m.WorkerdVersion != workerdbin.PinnedVersion { t.Fatalf("manifest=%s pin=%s", m.WorkerdVersion, workerdbin.PinnedVersion) }
    if m.WorkerdVersion != "1.20260916.1" { t.Fatalf("unexpected pin %s", m.WorkerdVersion) }
    if m.SourceSHA256 == "" || m.UpstreamRevision == "" { t.Fatal("manifest lacks source identity") }
}
```

Generator fixture test must prove stable alphabetical output and distinguish experimental enable flags from stable flags.

- [ ] **Step 2: Run tests and verify they fail on the old pin/missing package**

Run: `go test ./cmd/workerd-compat-gen ./internal/workerdcompat ./internal/wranglercompat -count=1 -v`

Expected: FAIL because packages/manifest are absent and the pin is `1.20260615.1`.

- [ ] **Step 3: Implement deterministic generation from an explicit upstream checkout**

The generator command is:

```bash
go run ./cmd/workerd-compat-gen \
  -source "$WORKERD_SRC" \
  -version 1.20260916.1 \
  -revision "$(git -C "$WORKERD_SRC" rev-parse HEAD)" \
  -out internal/workerdcompat/manifest.json
```

It reads the known upstream compatibility definition file, hashes its exact bytes, sorts flags, writes indented JSON, and never performs network access. The checked-in manifest must be generated from the workerd revision backing npm `1.20260916.1`.

- [ ] **Step 4: Replace hand-maintained Go validation with manifest consumption**

Keep `wranglercompat.KnownCompatibilityFlags` as a generated compatibility facade if callers need the map, but build it from `workerdcompat.Manifest().Flags`. Experimental or explicitly unsupported flags remain fail-closed.

- [ ] **Step 5: Generate the Bun validator data from the same manifest**

Add a `//go:generate` or generator output `cli/src/workerd-compat.generated.ts` exporting exact readonly constants; `validate.ts` imports it instead of declaring `KNOWN_COMPAT_FLAGS` manually. The existing Go/CLI equality test must compare both consumers to `manifest.json`.

- [ ] **Step 6: Add real binary probes**

`scripts/workerd-compat-probe.sh` builds temporary configs and verifies the manifest maximum date loads, the next calendar day is rejected as too new, every `cellhive_allowed` flag loads, and `cellhive_unknown_flag_probe` is rejected. The script exits nonzero on a missing binary or any mismatch; it never reports SKIP.

- [ ] **Step 7: Run generator, unit tests, and real probe**

Run:

```bash
go test ./cmd/workerd-compat-gen ./internal/workerdcompat ./internal/wranglercompat -count=1 -v
CELLHIVE_WORKERD="$(command -v workerd)" bash scripts/workerd-compat-probe.sh
```

Expected: PASS and binary version `2026-09-16`.

- [ ] **Step 8: Commit pin and compatibility authority**

```bash
git add cmd/workerd-compat-gen internal/workerdcompat internal/workerdbin internal/wranglercompat cli/src/validate.ts cli/src/workerd-compat.generated.ts scripts/workerd-compat-probe.sh
git commit -m "feat(runtime): generate compatibility rules for pinned workerd"
```

### Task 5: Align Miniflare with the Production Workerd Pin

**Files:**
- Modify: `cli/package.json`
- Modify: `cli/bun.lock`
- Modify: `cli/src/index.test.ts`
- Modify: `cli/src/validate.test.ts`

**Interfaces:**
- Produces: exact Miniflare/workerd pair with no transitive version drift.
- Candidate: `miniflare@5.20260916.0-alpha` with override `workerd@1.20260916.1`.

- [ ] **Step 1: Add a failing installed-version test**

The Bun test reads the resolved package metadata and asserts:

```ts
expect(pkg.dependencies.miniflare).toBe("5.20260916.0-alpha");
expect(pkg.overrides.workerd).toBe("1.20260916.1");
```

It also starts the existing Miniflare smoke and runs module fetch plus KV/D1/R2 operations.

- [ ] **Step 2: Run the test and observe old-version failure**

Run: `cd cli && bun test`

Expected: FAIL on current `4.20260616.0` / `1.20260615.1` assertions.

- [ ] **Step 3: Update exact dependencies and lockfile**

Run: `cd cli && bun add --exact miniflare@5.20260916.0-alpha && bun install`

Set `overrides.workerd` exactly to `1.20260916.1`; inspect the lockfile to prove no second workerd version is resolved.

- [ ] **Step 4: Run all CLI tests and resolved-version audit**

Run:

```bash
cd cli && bun test
cd cli && bun pm ls | rg 'miniflare|workerd'
```

Expected: all tests PASS, exactly the approved pair is listed. If the alpha cannot pass the existing smoke, stop this phase and record the blocker; do not retain a cross-date pair.

- [ ] **Step 5: Commit the dev-runtime alignment**

```bash
git add cli/package.json cli/bun.lock cli/src
git commit -m "chore(cli): align miniflare with pinned workerd"
```

### Task 6: Ship a Pinned esbuild in the Production Image

**Files:**
- Modify: `deploy/Dockerfile`
- Create: `deploy/dockerfile_test.go`
- Create: `third_party/workerd/LICENSE`
- Create: `third_party/esbuild/LICENSE`
- Create: `THIRD_PARTY_NOTICES.md`
- Modify: `docs/deployment.md`

**Interfaces:**
- Produces: `/usr/local/bin/esbuild` at `0.28.2` and `CELLHIVE_ESBUILD=/usr/local/bin/esbuild`.

- [ ] **Step 1: Add a failing Dockerfile contract test**

```go
func TestDockerfilePinsRuntimeTools(t *testing.T) {
    b, _ := os.ReadFile("Dockerfile")
    s := string(b)
    for _, want := range []string{
        "ARG WORKERD_VERSION=1.20260916.1",
        "ARG ESBUILD_VERSION=0.28.2",
        "CELLHIVE_ESBUILD=/usr/local/bin/esbuild",
        "esbuild --version",
    } {
        if !strings.Contains(s, want) { t.Errorf("Dockerfile missing %q", want) }
    }
}
```

- [ ] **Step 2: Run and verify failure**

Run: `go test ./deploy -run TestDockerfilePinsRuntimeTools -count=1 -v`

Expected: FAIL because esbuild and new workerd pin are missing.

- [ ] **Step 3: Add a pinned esbuild extraction stage**

Download the exact platform npm tarball, verify a checked-in SHA-512/integrity value or exact registry integrity before extraction, copy only the native binary, and execute `/out/esbuild --version`. Do the same integrity verification for workerd rather than trusting only HTTPS and a versioned URL.

Copy the official workerd Apache-2.0 license and esbuild MIT license into `/usr/share/licenses/cellhive/{workerd,esbuild}/LICENSE`. `THIRD_PARTY_NOTICES.md` records the exact packaged versions, upstream URLs and license identifiers. The image contract test must assert both paths exist in the final stage.

- [ ] **Step 4: Build and inspect the real image**

Run:

```bash
docker build -f deploy/Dockerfile -t cellhive:runtime-baseline .
docker run --rm --entrypoint /usr/local/bin/workerd cellhive:runtime-baseline --version
docker run --rm --entrypoint /usr/local/bin/esbuild cellhive:runtime-baseline --version
```

Expected: `2026-09-16` and `0.28.2` respectively.

- [ ] **Step 5: Commit container tooling**

```bash
git add deploy/Dockerfile deploy/dockerfile_test.go docs/deployment.md third_party THIRD_PARTY_NOTICES.md
git commit -m "build: ship pinned workerd and esbuild binaries"
```

### Task 7: Implement Shared WorkerCode and Env Budget Semantics

**Files:**
- Create: `internal/workerbudget/budget.go`
- Create: `internal/workerbudget/budget_test.go`
- Create: `internal/workerbudget/testdata/vectors.json`
- Create: `workerd/platform/budget.js`
- Create: `workerd/platform/budget.test.mjs`

**Interfaces:**
- Produces: `EstimateCode(CodeInput) int64`, `CheckCode(CodeInput) error`.
- Produces: `EstimateEnv(any) (int64, error)`, `CheckEnv(any) error`.
- Typed error: `LimitError{Code, Actual, Max int64}` with codes `worker_code_too_large` and `worker_env_too_large`.

- [ ] **Step 1: Write Go boundary tests first**

```go
func TestCheckCodeBoundary(t *testing.T) {
    for _, tc := range []struct{ n int64; ok bool }{{CodeMaxBytes-1,true},{CodeMaxBytes,true},{CodeMaxBytes+1,false}} {
        err := CheckCode(CodeInput{ModuleBytes: tc.n})
        if (err == nil) != tc.ok { t.Fatalf("n=%d err=%v", tc.n, err) }
    }
}

func TestEstimateEnvChargesTwoByteStrings(t *testing.T) {
    ascii, _ := EstimateEnv(map[string]any{"k":"aaaa"})
    unicode, _ := EstimateEnv(map[string]any{"k":"中文中文"})
    if unicode <= ascii { t.Fatalf("unicode=%d ascii=%d", unicode, ascii) }
}
```

- [ ] **Step 2: Run and verify missing-package failure**

Run: `go test ./internal/workerbudget -count=1 -v`

Expected: FAIL because the package is absent.

- [ ] **Step 3: Implement deterministic Go estimators**

`EstimateCode` sums every module name byte, text/binary payload byte, main module byte, generated wrapper byte, and fixed injected source byte exactly once. `EstimateEnv` JSON-encodes the complete env, adds the V8 two-byte penalty for every non-Latin-1 key/value string, rejects unsupported/cyclic values, and uses the exact constants in Global Constraints.

- [ ] **Step 4: Create cross-language golden vectors**

Vectors must include empty env, ASCII, Chinese, emoji, long keys, many binding-shaped objects, code at limit −1/limit/limit +1, binary modules, and long module names. JS tests load the same JSON and assert byte-for-byte equality with recorded Go results.

- [ ] **Step 5: Implement the JS runtime guard and run both suites**

Run:

```bash
go test ./internal/workerbudget -count=1 -v
node --test workerd/platform/budget.test.mjs
```

Expected: PASS with identical estimates.

- [ ] **Step 6: Commit budget primitives**

```bash
git add internal/workerbudget workerd/platform/budget.js workerd/platform/budget.test.mjs
git commit -m "feat(runtime): define WorkerCode and env budgets"
```

### Task 8: Enforce Budgets Before the Control Transaction

**Files:**
- Modify: `internal/server/control.go`
- Modify: `internal/server/control_test.go`
- Modify: `internal/server/server.go`
- Modify: `internal/artifacts/artifacts.go`
- Modify: `internal/artifacts/artifacts_test.go`

**Interfaces:**
- Consumes: `workerbudget.CheckCode`, `workerbudget.CheckEnv`, stored bundle bytes, normalized `control.DeploySpec`.
- Produces: stable HTTP errors carrying `worker_code_too_large` or `worker_env_too_large`, `actual_bytes`, and `max_bytes` without source/secrets.

- [ ] **Step 1: Add failing HTTP deployment tests**

Create a bundle just over the effective final-code limit and an env just over `EnvMaxBytes`; POST each to `/v1/control/deploy`. Assert HTTP 413 for `worker_code_too_large`, HTTP 400 for `worker_env_too_large`, stable error code, actual/max fields, and no active version created. Add exact-limit acceptance tests.

- [ ] **Step 2: Run and verify deploy currently accepts oversized input**

Run: `go test ./internal/server -run 'TestDeployRejectsFinalWorkerCodeOverBudget|TestDeployRejectsWorkerEnvOverBudget' -count=1 -v`

Expected: FAIL because deployment succeeds or returns no budget code.

- [ ] **Step 3: Add bundle stat/limited-read support without List**

Use point `GetBundle`/`Stat` only. Do not add bucket List. Read at most the maximum plus one byte when the object store supports bounded reads; otherwise reject after `Stat` before allocating an oversized buffer.

- [ ] **Step 4: Validate before `Control.Deploy`**

In `handleDeploy`, after compatibility and resource validation but before `s.Control.Deploy`, load/measure the immutable bundle, add fixed/dynamic runtime injection costs, construct the exact current tenant env shape, and call both checks. Dry-run executes identical checks. Map `LimitError` to the stable response without exposing payloads.

- [ ] **Step 5: Prove transactional non-activation and run server tests**

Run:

```bash
go test ./internal/artifacts ./internal/server ./internal/control -count=1 -v
```

Expected: PASS; rejected deployments leave version count and active pointer unchanged.

- [ ] **Step 6: Commit control-plane enforcement**

```bash
git add internal/server internal/artifacts
git commit -m "feat(control): reject oversized dynamic workers before deploy"
```

### Task 9: Recheck Budgets Immediately Before Every `workerLoader.get()`

**Files:**
- Modify: `internal/userruntime/userruntime.go`
- Modify: `internal/doruntime/doruntime.go`
- Modify: `workerd/user-runtime/loader.js`
- Modify: `workerd/user-runtime/internal.js`
- Modify: `workerd/do-runtime/host.js`
- Modify: `internal/userruntime/userruntime_test.go`
- Modify: `internal/doruntime/compat_test.go`

**Interfaces:**
- Consumes: `workerd/platform/budget.js` embedded as `budget.js` or `BUDGET_SRC` in each trusted host.
- Runtime behavior: old/corrupt/bypassed versions fail closed before `workerLoader.get()` and emit a bounded error code.

- [ ] **Step 1: Add failing bypass/corrupt-data tests**

Mock the control responses directly (bypassing deploy), return an over-budget bundle/env, invoke public fetch, internal dispatch, service load, and DO facet load, and assert `worker_code_too_large` / `worker_env_too_large`; assert the loader callback was never invoked.

- [ ] **Step 2: Run and verify callbacks are currently invoked**

Run: `go test ./internal/userruntime ./internal/doruntime -run 'RuntimeRejects.*OverBudget' -count=1 -p 1 -v`

Expected: FAIL because current loaders have no budget guard.

- [ ] **Step 3: Embed and call the runtime budget module**

Add `budget.js` to trusted host module lists. Build the complete WorkerCode object first, call `assertWorkerCodeBudget(workerCode)` and `assertWorkerEnvBudget(workerCode.env)`, then pass the already-validated object to `workerLoader.get()`. Apply the same helper to public, internal, service, workflow, queue/scheduled and DO paths rather than duplicating formulas.

- [ ] **Step 4: Add bounded metrics/errors without tenant data**

Count failures by `{kind="code|env",surface="user|do"}` only. Error bodies include code/actual/max, not module source, env keys, namespace values, URLs, or tokens.

- [ ] **Step 5: Run JS syntax and all real-runtime package tests**

Run:

```bash
node --check workerd/platform/budget.js
node --check workerd/user-runtime/loader.js
node --check workerd/user-runtime/internal.js
node --check workerd/do-runtime/host.js
go test ./internal/userruntime ./internal/doruntime -count=1 -p 1 -v
```

Expected: PASS.

- [ ] **Step 6: Commit runtime defense-in-depth**

```bash
git add internal/userruntime internal/doruntime workerd/user-runtime workerd/do-runtime workerd/platform/budget.js
git commit -m "feat(runtime): enforce dynamic load budgets at execution"
```

### Task 10: Complete Documentation, Migration, Rollback, and License Audit

**Files:**
- Modify: `docs/decisions.md`
- Modify: `docs/workerd-integration.md`
- Modify: `docs/configuration.md`
- Modify: `docs/compatibility-matrix.md`
- Modify: `docs/known-issues.md`
- Modify: `docs/testing.md`
- Modify: `docs/release-notes.md`
- Modify: `docs/modules/user-runtime.md`
- Modify: `docs/modules/do-runtime.md`
- Modify: `docs/modules/cli-and-packaging.md`
- Modify: corresponding `docs/en/**`
- Modify: `docs/acknowledgements.md`
- Modify: `docs/en/acknowledgements.md`
- Modify: `THIRD_PARTY_NOTICES.md`

**Interfaces:**
- Produces: current-state documentation with exact commands and honest Tail/secret/multi-module boundaries.

- [ ] **Step 1: Run stale-reference audit and save the exact hit list**

Run:

```bash
rg -n "1\.20260615\.1|2026-06-22|动态.*Tail|缺少 esbuild|text = \"\{\{\.Cell(Token|URL)\}\}\"" docs deploy internal workerd cli --glob '!docs/archive/**'
```

Expected: hits requiring current-state updates are present.

- [ ] **Step 2: Update ADR-186 from “实施中” to the evidenced implementation state**

Document exact pins, manifest generation/probe command, budget formulas and errors, direct/supervised `fromEnvironment` launch, migration/rollback rules, Miniflare pair, and the fact that Tail remains a later phase even though its upgraded-pin spike is rerun.

- [ ] **Step 3: Audit WDL-derived code and notices**

Compare every new generator/budget source file against WDL commit `dc70da6cc04acee7d31d80fc0caf8f323bbacf21`. This plan requires an independent implementation: no WDL source is copied or substantially adapted. Record WDL as a design-method reference in both acknowledgements files and state in `THIRD_PARTY_NOTICES.md` that WDL code is not shipped. If the audit contradicts that statement, stop before this commit and replace the affected implementation with an independent one; do not silently rely on missing NOTICE text.

- [ ] **Step 4: Run bilingual and stale-reference checks**

Run:

```bash
go test ./internal/wranglercompat -run 'Docs|Compatibility' -count=1 -v
rg -n "1\.20260615\.1|2026-06-22" docs --glob '!archive/**'
git diff --check
```

Expected: only explicitly historical old-pin references remain; current Chinese/English docs agree.

- [ ] **Step 5: Commit documentation and notices**

```bash
git add docs THIRD_PARTY_NOTICES.md
git commit -m "docs: publish runtime baseline migration and verification"
```

### Task 11: Run Real Docker E2E and the Full No-Skip Gate

**Files:**
- Create: `scripts/runtime-baseline-e2e.sh`
- Modify: `scripts/ci.sh`
- Modify: `docs/testing.md`

**Interfaces:**
- Produces: one repeatable acceptance script for image versions, in-container bundling, env isolation, KV, gated DO, restart persistence and credential scans.

- [ ] **Step 1: Add the E2E script to CI in required mode and observe initial failure**

Add a `runtime-baseline-e2e` gate that is optional in ordinary local CI but mandatory under `REQUIRE_ALL=1`. Initially run:

```bash
REQUIRE_ALL=1 bash scripts/ci.sh
```

Expected: FAIL because `scripts/runtime-baseline-e2e.sh` or its acceptance output is absent.

- [ ] **Step 2: Implement isolated Docker acceptance with trap cleanup**

The script must use a unique Compose project name and temporary directories, install a trap before starting containers, and perform these exact assertions:

1. image reports workerd `2026-09-16` and esbuild `0.28.2`;
2. in-image `cellhive deploy` bundles source with esbuild rather than uploading prebuilt JS;
3. worker-defined `CELL_URL=user-cell-url` and `CELL_TOKEN=user-cell-token` are visible while host canaries are absent;
4. real fetch and KV put/get succeed;
5. gated DO increments, bucket durability proof exists, DO runtime is restarted, and the next increment continues;
6. rendered capnp and captured final WorkerCode contain neither host canary;
7. compatibility probe and near-limit code/env probes pass;
8. no relevant test line contains `SKIP`, `SKIPPED`, or an unexecuted marker.

- [ ] **Step 3: Run focused unit and real-workerd suites before the expensive gate**

Run:

```bash
go test ./internal/runtimeenv ./internal/workerdcompat ./internal/workerbudget ./internal/wranglercompat -count=1 -v
make js-test
make cli-test
```

Expected: PASS with no relevant skip.

- [ ] **Step 4: Run the real Docker E2E**

Run: `bash scripts/runtime-baseline-e2e.sh`

Expected terminal line: `RUNTIME-BASELINE-E2E: PASS`; exit code 0.

- [ ] **Step 5: Run the complete official gate with skips forbidden**

Run: `REQUIRE_ALL=1 bash scripts/ci.sh`

Expected: every official gate PASS, including Docker build/Compose and the new runtime baseline E2E; final exit code 0.

- [ ] **Step 6: Audit the first-phase completion evidence**

Run:

```bash
git status --short --branch
git log --oneline --decorate -15
rg -n "CELL_URL.*JSON.stringify|CELL_TOKEN.*JSON.stringify" workerd
rg -n "text = .*Cell(URL|Token)|1\.20260615\.1" internal deploy cli docs --glob '!docs/archive/**'
docker image inspect cellhive:runtime-baseline --format '{{json .RepoDigests}} {{.Id}}'
```

Classify every approved-spec requirement as complete/contradicted/unverified. Do not call the phase complete on an empty search alone; cite the corresponding tests and live requests.

- [ ] **Step 7: Commit the acceptance harness and verified status**

```bash
git add scripts/runtime-baseline-e2e.sh scripts/ci.sh docs/testing.md docs/decisions.md docs/release-notes.md
git commit -m "test: accept pinned workerd runtime baseline end to end"
```

## Phase Boundary

After Task 11, re-audit the approved first-stage spec. If every item is proven, write and approve the next independent specification for dynamic loader governance. Do not mark the overall Goal complete: loader lifecycle, DO mid-flight fencing/restart generation, native Tail/OTLP, and compatible multi-module artifacts remain mandatory subsequent phases.
