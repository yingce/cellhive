# `cellhive dev` (Local Development Mode)

> Status: **M1/M2 implemented** (`cli/` Bun dev + `internal/wranglercompat` server-side interception, 2026-09-15); for a summary of key points, see [`known-issues.md`](known-issues.md) M-08; remaining items are listed in that section.
> Related: [`wrangler-compat.md`](wrangler-compat.md) (bundling/config/assets), [`bindings.md`](bindings.md) (binding mapping), [`control-plane.md`](control-plane.md) (deploy and server-side interception), [`routing.md`](routing.md), [`deployment.md`](deployment.md), [`observability.md`](observability.md).

## 0. One-Sentence Design

**dev = Bun CLI + Miniflare**: `cellhive dev` is a tool written in Bun. Internally it uses **Miniflare** to start a real workerd plus simulated bindings. **No Go backend process runs on the dev machine**; **production** remains Go workerd + cell-agent (unchanged). User code stays compatible by "aligning with the Cloudflare contract on both sides"; platform features (versions/routes/secrets/deploy/scope/RPO=0) are not in dev and go through the real platform.

## 1. Goals and Non-Goals

**Goals (tenant DX)**
- **Zero external dependencies**: No Docker / cloud bucket / MinIO / Traefik / DNS configuration required; a single `cellhive dev` command starts everything.
- **Standard CF shape**: Tenant code and `wrangler.jsonc` are consistent with CF/`wrangler dev`; KV/D1/R2/Queue/DO/`vars`/`secrets`/assets are available (built into Miniflare).
- **Hot reload / inspector / persist**: Use Miniflare capabilities directly; do not build our own.
- **Zero user-code changes**: The same code can run on CF, `wrangler dev`, `cellhive dev`, and after `cellhive deploy` to the platform (provided the contract is aligned; see §10).

**Non-Goals**
- dev **does not validate the platform**: it does not run cell-agent / control plane / replication / owner / lease / scope token / RPO=0 output gates (this is the core trade-off versus "real-stack dev"; the platform is validated through contract tests + real environments).
- Multi-node HA / drain / handoff / fleet; Traefik / TLS / OIDC / multi-tenant network boundaries.
- Production scale and performance gates (see [`p0-report.md`](../archive/p0-report.md)).

## 2. Process Model

```
cellhive dev  (Bun, one process, zero Go)
└── Miniflare (npm, in-process)
      ├── workerd subprocess (started by Miniflare; pinned to the platform version, see §3)
      ├── Simulated bindings: KV/D1/R2/Queue/DO/Cache/Service/Assets (local implementations)
      ├── Hot-reload watch + inspector + persist
      └── Local HTTP service + routing (host/path)
```

- **Bun hosts Miniflare (M1 verified ✅)**: `bun 1.4.0` fully starts Miniflare (using Bun's Node compatibility layer to successfully spawn the workerd subprocess) and serves KV/D1/R2/`vars` **as an HTTP dev server**, with cold start around ~150ms. There are no blocking issues in `require`/startup/hot paths.
- **Other CLI commands** (`deploy/promote/secret/routes`) are already HTTP clients that call the cell-agent admin API (`:8082`) and are decoupled from dev.
- **No Go**: `cellhive dev` does not require / spawn any cell-agent, supervisor, or FSBucket code.

### 2.1 Why Miniflare Instead of wrangler (Clarifying "Is Miniflare Obsolete?")

- **Miniflare is not obsolete; it is wrangler's engine**: `wrangler@4.x` dependencies directly include `"miniflare"` (+ `workerd`), and the published package includes `miniflare-dist/`; both belong to the `cloudflare/workers-sdk` monorepo. CF documentation also states that "most users use Wrangler", while the Miniflare API targets **advanced/programmatic** use cases—which is exactly the scenario where we write our own CLI.
- What makes people think it is "obsolete" is the **standalone CLI from Miniflare 2.x** (after v3 was rewritten with workerd in 2023, it is no longer the primary recommendation); **v3/v4 are libraries**, shared by `wrangler dev`, `@cloudflare/vite-plugin`, and `vitest-pool-workers`.
- **Trade-off**: Embed Miniflare directly → use the same engine as wrangler, control config translation and workerd pinning ourselves, and keep dependencies light; call `wrangler dev` → heavy dependency (CLI, `engines: node>=22`), configuration/bundling semantics are determined by it, and embedding is difficult. **We choose the former** (this ADR).
- Both **must pin workerd** (wrangler's `workerd` also goes through catalog/resolution), so the pinning cost is not saved by choosing wrangler.

### 2.2 "What Should We Use" — Runtime vs Tooling Layer (Including CF Official Status)

**There is only one runtime engine: Miniflare (= workerd)**. `wrangler`, `createTestHarness`, `@cloudflare/vite-plugin`, and `vitest-pool-workers` are **all built on top of it**. So the dispute is not about the engine, but about the "dev server / tooling layer":

| Option | Description | Cost | Trade-off |
|---|---|---|---|
| **Embed Miniflare directly** (chosen by this ADR) | Use the engine itself + our own config translation/bundling | Need to implement wrangler config semantics and bundling ourselves (the bulk of the work; starting workerd is the simplest part, already measured at ~150ms) | ✅ Consistent with our own CLI + Bun + Go bundler; support surface has been narrowed by ADR-014, so translation work is manageable |
| **`@cloudflare/vite-plugin`** | **CF's currently officially recommended programmatic dev server** (with Vite `createServer()`); still uses Miniflare/workerd underneath | Introduces Vite as the dev build system, overlapping with our Go bundler/own CLI | Preferred fallback (if config fidelity cost is too high) |
| **`wrangler dev`** | Most complete CF semantics | Heavy (CLI, `engines: node>=22`), bundling/config semantics are determined by it, difficult to embed | Only as a `--wrangler` pass-through escape hatch |

**Key fact (CF documentation, 2026-07-27)**: `unstable_startWorker` / `unstable_dev` **are deprecated**; CF now recommends—use `createTestHarness()` for tests (wraps Miniflare and can directly read wrangler config), and use the **Cloudflare Vite plugin** for programmatic dev servers. Therefore our "fallback" should point to the Vite plugin and **must not** depend on deprecated wrangler programmatic APIs.

**Conclusion**: CellHive continues to "embed Miniflare directly"—the **cost center is config translation + bundling** (which we need to build in P2 anyway for the Go+esbuild bundler), not the engine. Only if the fidelity of our own implementation becomes unacceptable do we fall back to the Vite plugin to reuse CF's config/bundling.

## 3. Runtime Version Pin (Important)

| Component | Version | Description |
|---|---|---|
| Platform pinned workerd | **1.20260615.1** | Production/contract baseline (compatibility-date upper bound 2026-06-22, already tested) |
| **Selected Miniflare** | **4.20260616.0** | Same period as the platform workerd; its bundled workerd = `1.20260616.1` |
| Miniflare 4.20260714.0 (previously tried) | workerd `1.20260714.1` | ❌ Internal control worker hardcodes `2026-07-08`, incompatible with the pinned version |

- **Pinning rule (corrected, tested on 2026-09-15)**: The **Miniflare version must be aligned with the platform pinned workerd from the same period**, then override its own `workerd` dependency to the pinned version.
  - ❗ **Do not** forcibly override the workerd of a "newer Miniflare" (such as 4.20260714.0) to an older pinned version — Miniflare's internal control worker (`MINIFLARE_DEV_CONTROL`) **hardcodes** `compatibilityDate` (4.20260714.0 = `2026-07-08`), while pinned workerd `1.20260615.1` only supports up to `2026-06-22`, so startup fails immediately: `This Worker requires compatibility date "2026-07-08", but the newest date supported by this server binary is "2026-06-22"`.
  - The previous spike that "only did `require`/`dispatchFetch` without a port" would **miss** this issue (the control service is only instantiated in dev server mode)—now corrected.
- Implementation: `cli/package.json` depends on `miniflare@4.20260616.0` + `overrides: { "workerd": "1.20260615.1" }` (both Bun/pnpm support `overrides`) → installed result is `miniflare@4.20260616.0` + `workerd@1.20260615.1` (already tested).
- **Verified ✅**: With this combination, `bun` starts the dev server, and KV/D1/R2/`vars` all pass over HTTP (see §14 M1 for details).
- The workerd binary comes from Miniflare's `workerd` dependency (`@cloudflare/workerd-linux-64`); no system installation is required.

## 4. Config Translation (wrangler → Miniflare)

| wrangler | `cellhive dev` action |
|---|---|
| `name` | Miniflare worker name + ns/worker for dev routing |
| `main` | Miniflare `scriptPath` (or the entry after esbuild bundling; see below) |
| `compatibility_date` / `compatibility_flags` | Pass to Miniflare (and preflight in the CLI: ≤ platform upper bound, flags known; see §8) |
| `vars` | Miniflare `bindings` (text/json) |
| `kv_namespaces` / `d1_databases` / `r2_buckets` / `queues` | Map to Miniflare bindings with the same names (local implementations) |
| `durable_objects` + `migrations` | Map to Miniflare DO + SQLite storage (only `created/sqlite`; reject rename/delete/transfer, ADR-014) |
| `assets` | Map `directory`/`binding` (including `[site] bucket`) to Miniflare assets (static serving). **Limitation: worker+assets interop (asset miss fallback to worker / `env.ASSETS.fetch` / `run_worker_first`) is not supported**; dev warns `assets_worker_interop` (requires wrangler's internal asset-router wiring) |
| `vars`/`secrets` declarations | Pass `vars` directly; read `secrets` from `.dev.vars` and expose them as Miniflare secret bindings |
| `env.<name>` | Selected by `--env`; each environment has an independent namespace/persistence directory |
| Unsupported fields | **Explicitly reject** (same validation as ADR-014; see §8) |

- **Bundling**: Two paths—(a) Default: Miniflare/wrangler semantics (`.js` loaded directly; `.ts` built with `Bun.build`); (b) `--strict-build`: call the **platform Go+esbuild bundler** (`internal/bundler` + `cellhive bundle build <entry> --out f`, ADR-005), so dev and deploy use the same artifact. Requires the `cellhive` Go binary (`make build` produces `bin/cellhive`; `CELLHIVE_BIN` can specify it); if not found, error and print guidance. **Implemented and verified ✅**.
- `tsconfig`/`rules`/`no_bundle`/`find_additional_modules`/`base_dir`/`minify`/`keep_names`/`define` are handled according to wrangler-compat.md; pass through to Miniflare directly where possible.
- **Miniflare 5 assets routing boundary (ADR-114)**: the built-in router incorrectly couples not-found handling and user-worker fallback through `has_user_worker`. The upgraded dev CLI uses one entry worker in the same Miniflare instance, with service bindings that orchestrate the native asset service and the user worker in production order. It adds no listener, process, credential, or state, remains development-only, and must never enter the production ingress or become a gateway. The Miniflare 5 upgrade remains in progress until the complete assets and hot-reload smoke passes.

## 5. Data Directory and Persistence

- `./.cellhive-dev/` (`--data-dir` can change it; `--clean` clears it):
  - `miniflare/`: Miniflare `persist` root (local on-disk storage for KV/D1/R2/DO);
  - `logs/`: Aggregated CLI logs;
  - `dev.json`: CLI-side state (ns/worker/env, last URL).
- Use Miniflare's `persist` (file backend); **do not build our own storage**; `--persist` semantics match `wrangler dev --persist`.
- `.gitignore` ignores `.cellhive-dev/` by default.

## 6. Hot Reload

- **Use Miniflare/wrangler watch directly**: module graph, `wrangler` config, `.dev.vars`, assets; save triggers rebuild.
- Compilation error → print diagnostics and keep the previous version serving (Miniflare behavior); the CLI adds "rebuilding/failed" hints.
- The CLI only performs **extra** actions when needed (for example, when `--strict-build` uses the platform bundler to rebuild).

## 7. Binding Support Surface and Platform Differences (Key)

**Binding plugins exposed by Miniflare 4.20260714 in testing** (`*_PLUGIN_NAME` exports):
`kv, d1, r2, queues, do, cache, service(core), assets(worker-loader), workflows, ai, ai-search, vectorize, hyperdrive, images, media, browser-rendering, email, dispatch-namespace, analytics-engine, pipelines, secrets-store, ratelimit, mtls, stream, vpc-networks, vpc-services, websearch, flagship, agent-memory, artifacts, version-metadata, hello-world`.

**This shows Miniflare "knows" far more than our platform** (still rejected: `ai_search/dispatch_namespaces/secrets_store/send_email/browser/images/containers/flagship/pipelines/vpc/placement/site`; `vectorize`/`hyperdrive` are already supported by ADR-158/129). Therefore:

| Binding | Miniflare | Our Platform | Result |
|---|---|---|---|
| KV / D1 / R2 / Queue / DO / Cache / Service / Vars / Secrets / Assets | ✅ Local implementation | ✅ Supported | Consistent |
| **`env.IMAGES`** | **Has `images` plugin + local `imagedelivery` worker path**; tested `images:{binding:"IMAGES"}` can be constructed and the worker **starts without credentials** (→ local implementation) | **❌ Rejected** (ADR-014) | **dev will actually "work", deploy is blocked** (the most dangerous class; must be covered by §8) |
| `env.AI` / `env.BROWSER` (browser-rendering) | Has plugins; **likely proxies to Cloudflare / requires account credentials** (`browser-rendering` has a localhost control path, needs spike confirmation) | ❌ Rejected (this phase) | dev may report "login/credentials required" |
| `vectorize` | Only has configuration schema (workerd does not have this service; no local simulation) | ✅ Platform supported (ADR-158), **dev explicitly rejects** (`DEV_UNSUPPORTED_BINDINGS`) | Use `cellhive vectorize ...` against the real namespace |
| `hyperdrive` | Has plugin (`localConnectionString`) | ✅ Supported (ADR-129) | dev uses Miniflare `hyperdrives` |
| `send_email` / `secrets_store` / `dispatch_namespaces` / `containers` … | Some have plugins | ❌ Rejected | Explicitly rejected |

**Conclusion**:
1. When using Miniflare for dev, **users may "use" bindings locally that the platform does not support** (most typically `env.IMAGES`).
2. Therefore, the **§8 deploy server-side capability/compatibility interception** must be the fallback, and **CLI/dev must warn early** (same validation library).
3. "Miniflare recognizes it" ≠ "it really runs locally": AI/Browser and similar bindings most likely require CF credentials; when encountered in dev, provide a clear "platform unsupported / credentials required" prompt instead of an ambiguous success.

## 8. deploy Server-Side Capability/Compatibility Interception (New, Authoritative)

**Principle**: The CLI only provides fast feedback; **the server side is authoritative** (to prevent bypassing and different CLI versions). Put the validation logic in a shared library (Go), used by both the `deploy` endpoint and CLI preflight.

The server validates the following in `POST /v1/control/deploy` (and bundle upload):
1. **bundle**: `bundle_sha` exists in object storage (content-addressed object exists), and `assets` references exist.
2. **Compatibility date**: `compatibility_date` ≤ the platform-supported upper bound (known **2026-06-22**, empirically verified); report an error and provide the upper bound if exceeded.
3. **compatibility_flags**: Must be within the known set of the pinned workerd (unknown flags report an error like `No such compatibility flag`; known flags such as `nodejs_compat` are allowed).
4. **Bindings**: The type of each binding must be in the **support matrix**; otherwise reject it (`images/browser-rendering/send_email/ai_search/dispatch_namespaces/secrets_store/containers/...`). Bindings supported by the platform but not simulatable in dev (currently only `vectorize`) are **explicitly errored** on dev startup.
5. **Resource references**: Resources referenced by bindings (KV/D1/R2/Queue…) must already be registered in the control plane (automatic provisioning is rejected, ADR-014); otherwise the error suggests the corresponding `cellhive <kind> create`.
6. **DO lifecycle**: Only `created`/`sqlite` (including `new_classes/new_sqlite_classes`); rename/delete/transfer/`script_name` are rejected.
7. **Unknown fields**: Explicitly report errors (field path + reason + "whether support is planned").

Error shape: `{error, message, field_path?}`, with stable error codes (such as `unsupported_binding`, `compat_date_too_new`, `unknown_flag`, `binding_unregistered`).

**CLI/dev reuse**: `cellhive dev` and `cellhive deploy` first run the same validation locally → report "this binding is not supported by the platform" before startup/submission. This way, even if Miniflare allows it, users receive a clear warning **during dev**, avoiding "dev green, deploy rejected".

## 9. Contract Differential Testing (Miniflare as Oracle)

- Goal: Make it **verifiable** that "user code behaves consistently in dev (Miniflare) and prod (us)".
- Method: Fix a set of **golden requests/responses** (fields, error codes, and boundaries for KV/D1/R2/Queue/DO), send them respectively to **Miniflare** and the **platform cell-agent**, and diff the results. Miniflare's responses serve as the **CF semantic baseline** (following ADR-014's "pinned Wrangler semantic baseline" approach).
- Differences are divided into two categories:
  - We **deviate from CF** (bug) → fix our facade/endpoint;
  - **Intentional differences** (such as RPO=0 write latency, scope/quota) → record them in the §12 differences table, and require user code not to depend on them.

## 10. User Code Compatibility (Tenants Do Not Need to Care About the Backend, but Fields Must Align)

"Backend invisibility" holds only if the **contract is faithful**. The current facade (ADR-063 `workerd/platform/facades.js`) has known gaps relative to CF and must converge in P2 (together with §9 differential testing):

| Aspect | CF | Current State | To Add |
|---|---|---|---|
| KV `list()` | `keys:[{name,expiration?,metadata?}]` | ✅ Includes expiration/metadata (ADR-098, `with_metadata`) | None |
| KV `get()` | Optional metadata | ✅ `getWithMetadata` + `x-cellhive-kv-metadata` (ADR-098) | None |
| R2 `get()` | `R2ObjectBody{key,version,size,etag,httpEtag,uploaded,httpMetadata,customMetadata,checksums,...}` + `text/json/arrayBuffer/blob` | ✅ All fields + methods (`bodyUsed` exception, see `known-issues`) | None |
| R2 `put()/head()/list()` | `R2Object` / `{objects,truncated,cursor}` | ✅ All fields; `put(key,value,{httpMetadata,customMetadata,md5,sha256})` stores metadata sidecar (validates md5/sha256) | None (per-object metadata in list: see known-issues) |
| D1 `run()/batch()` | `meta:{changes,last_row_id,changed_db,duration,rows_read,rows_written}` | ✅ `changes/last_row_id/changed_db/duration` (`rows_read/written` are approximate) | Engine-level row counts |
| Error shape | `D1_ERROR`/`KVError`, etc. | ✅ `err.name` (`D1_ERROR`/`KVError`/`R2Error`/`QueueError`) + `err.code` (platform code) | None |

- After completion, **the same user code** can be ported among CF / `wrangler dev` / `cellhive dev` / `cellhive deploy` (platform).
- This is **platform engineering responsibility** (contract fidelity), not tenant responsibility.

## 11. CLI (Bun)

```
cellhive dev [projectDir] [flags]
  --namespace <ns>        Defaults to wrangler name
  --port <n>              Local port (default 8787)
  --data-dir <path>       Defaults to ./.cellhive-dev (Miniflare persist root inside it)
  --env <name>            Select wrangler env.<name>
  --var KEY=VALUE         Override vars (repeatable)
  --clean                 Clear data and rebuild persistence according to config
  --no-hot-reload         Disable watch
  --strict-build          Use the platform Go bundler to produce the bundle (otherwise use wrangler/Miniflare esbuild)
  --inspector-port <n>    workerd inspector
  --log-level <lvl>
```

- Example output:
```
cellhive dev — DEV (Bun + Miniflare; single worker, local simulation)
worker: api    namespace: acme    env: default
bindings: KV(kv) DB(d1) BUCKET(r2) QUEUE(queue)
url:      http://localhost:8787/        (host form: http://api.acme.localhost:8787/)
workerd:  1.20260615.1 (pinned; Miniflare bundled version overridden)
persist:  ./.cellhive-dev/miniflare
warning:  binding "IMAGES" is not supported by the CellHive platform; deploy will be rejected.
```

## 12. Differences from Production (Must Be Explicit)

| Dimension | dev (Bun+Miniflare) | Production (Go) |
|---|---|---|
| Binding implementation | Miniflare local simulation | cell-agent (bucket conditional writes + replication) |
| Persistence | Local files (Miniflare persist) | Object storage + cell SQLite |
| Write acknowledgement | Immediate (local) | **RPO=0 output gate** (waits for proof; real latency exists) |
| Support surface | Broad (including bindings rejected by the platform) | Support matrix (strict rejection) |
| Platform features | None (versions/routes/secrets/deploy/scope are not in dev) | All |
| Runtime | Miniflare-bundled workerd (pinned to the same version as prod) | pinned workerd |
| Topology | Single worker | Multi-tenant + multiple replicas |
| Secrets | `.dev.vars` | Control-plane envelope ciphertext + external root key |

- The startup banner explicitly states "DEV / local simulation", and **warns** when detecting bindings not supported by the platform (§8).

## 13. Failure Modes

| Symptom | Handling |
|---|---|
| Bun unavailable | Report an error and prompt to install Bun (or publish a `bun build --compile` single-file CLI) |
| Miniflare cannot start on Bun (Node compatibility layer) | **Spike has passed** (Miniflare works normally since Bun 1.4.0); if a future Bun version regresses, fallback: run Miniflare with system Node (still zero Go), or `--platform` (connect to a real local cell-agent) |
| Miniflare workerd version ≠ pinned | Override according to §3; if override is impossible, warn about drift in the banner + run contract tests on pinned |
| Platform-unsupported binding (such as `IMAGES`) | dev warning; `cellhive deploy` server-side rejection (§8), error includes field path |
| `compatibility_date` exceeds upper bound | CLI preflight + server-side rejection (≤ 2026-06-22) |
| Port occupied | Clear error + occupying PID |

## 14. Implementation Plan (Bun Project; Do Not Touch the Go Platform)

- Add `cli/` (Bun/TS project, replacing/running in parallel with `cmd/cellhive`):
  - `cli/src/dev.ts`: translate wrangler → Miniflare options, start Miniflare, print banner/URL, watch prompts;
  - `cli/src/config.ts`: jsonc/toml parsing + **client-side call/replica** of the shared validation library;
  - `cli/src/deploy.ts`, etc.: HTTP client calls admin API;
  - `cli/package.json`: depends on `miniflare` (+ optional `wrangler`), **pin workerd to 1.20260615.1**; can use `bun build --compile` to produce a single file.
- **Do not do**: Do not implement local startup for the Go backend; do not replicate cell-agent in dev.
- **Need to add** (Go side, for §8/§9): a shared **compatibility validation library** (`internal/wranglercompat`) for CLI preflight and the `deploy` endpoint to share; and a golden set for binding contract differential testing.
- **Milestones**:
  - **M1 (implemented and verified on 2026-09-15 ✅, partial)**: `cli/` Bun project created; `cellhive dev` reads `wrangler.jsonc/.json/.toml` (subset) → maps to Miniflare (KV/D1/R2/DO/`vars`) → starts dev server; preflight (`compat_date_too_new`/`unknown_flag` rejection, `images`/`ai` and other `unsupported_binding` warnings); `--port/--data-dir/--env/--var`. **Verified**: `bun run src/index.ts dev <dir>` starts the service, `/`,`/kv`,`/d1`,`/r2` pass over HTTP; bad configs are correctly rejected/warned. **Added**: hot reload, Queue end-to-end, `rules`→`modulesRules`, `--clean`, assets (static serving). **Added**: `--strict-build` (`internal/bundler` Go+esbuild + `cellhive bundle build`). **Added**: worker+assets interop + `_headers`/`_redirects`/`not_found_handling` (ADR-114, `make cli-test`); **To add**: Go-side config parsing (currently parsed by CLI, `html_handling` not parsed), local behavior for `ai`/`browser` not tested;
  - **M2 (implemented and verified on 2026-09-15 ✅)**: Go `internal/wranglercompat` shared validation library (binding support matrix/`compat_date` upper bound/flags/resources registered/unknown fields, stable error codes + unit tests); `POST /v1/control/deploy` server-side interception wired up (`deploy_rejected` + findings, including **bundle existence** point lookup); CLI preflight aligned with the same codes (`compat_date_too_new`/`unknown_flag`/`unsupported_binding`). Tests: `internal/wranglercompat` + `internal/server` interception cases pass.
  - M3 §9 contract differential testing + §10 facade gap closure;
  - M4 `--strict-build`, `--platform` fallback, error experience polish.

## 15. Acceptance Criteria

1. No dependencies other than Bun, **no Go process**: `cellhive dev` starts the service in a project containing `wrangler.jsonc` and prints the URL.
2. `curl` examples for KV/D1/R2/Queue/DO work; after stopping and restarting, `persist` data is retained (`--clean` resets it).
3. Edit `main` → Miniflare hot reload takes effect; syntax errors do not crash it, and the previous version is retained.
4. When using a binding **not supported** by the platform (such as `IMAGES`): dev **warns**; `cellhive deploy` is **rejected by the server side** and returns a stable error code + field path.
5. `compatibility_date` after 2026-06-22 / unknown flag: CLI preflight + server-side rejection.
6. dev workerd version = platform pinned (override effective), or the banner clearly states drift.
7. The §9 differential test set passes on Miniflare and the platform cell-agent (or all differences are classified as intentional differences in §12).

## 16. Not Done / Follow-Up

- Hosting AI/Browser on Miniflare outside `--sim` (requires CF credentials) — explicitly reject and prompt.
- Direct compatibility layer for `wrangler dev` (if the user has wrangler installed, `cellhive dev --wrangler` passthrough).
- **Fallback evaluation**: If the cost of self-developed wrangler config translation/bundling is unacceptable, evaluate switching to **`@cloudflare/vite-plugin`** (CF official programmatic dev server; **do not** use the deprecated `unstable_startWorker`/`unstable_dev`).
- ~~Complete Bun+Miniflare startup spike~~ **Completed (2026-09-15)**: Bun 1.4.0 + Miniflare 4.20260714.0 + pinned workerd 1.20260615.1 starts service, KV/D1/R2 pass; results have been backfilled into §2/§3/§7/§13. Remaining to test: actual behavior of local Miniflare `images`/`ai`/`browser` (whether credentials are required).
- CI gate paired with §9 differential testing (contract tests go into `docs/testing.md`).

_Last updated: 2026-09-15_
