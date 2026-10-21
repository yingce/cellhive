# Wrangler / Workers Compatibility

Goal: preserve the Workers programming model. Official Wrangler can use `wrangler.jsonc`/`wrangler.toml`; the Go `cellhive deploy --config` currently parses JSONC/JSON, bundles locally, and publishes to CellHive **without touching Cloudflare**.

## Three Key Measures

1. **Configuration parsing**: the Go CLI parses `wrangler.jsonc`/`.json`, applies environment overrides and compatibility validation, then maps to platform metadata; official Wrangler and the Bun dev CLI can read TOML;
2. **Bundling**: the Go CLI embeds the **esbuild Go SDK** (esbuild itself is Go), bundles in-process, and has **no Node dependency**;
3. **CLI surface** (actual): `deploy` (`--config <wrangler.jsonc>` maps main/bindings/vars/crons/assets/migrations, or use explicit flags), `bundle build` (Go+esbuild packaging), `app`/`resource` (explicit resource registration, **no auto-provisioning**), `versions`/`deployments`/`promote`/`rollback`, `queues consumer|purge`, `workflows trigger|instances`, `vectorize`, `route`, `secret`, `worker delete`, `tail`, `status`/`capacity`/`diagnose`/`audit`, `gc bundles|assets`, and `asset put`. `dev` lives in the Bun CLI (`cli/`).

> This proposal is a **Wrangler-compatible subset**: unknown/unsupported fields are strictly rejected, and a compatibility matrix (see the [`compatibility-matrix.md`](compatibility-matrix.md) plan) and contract tests are maintained.

## Bundling (Go + esbuild, no Node)

- Use the esbuild Go API (`api.Build`) to bundle in-process: `format=esm`, `platform=neutral`, `conditions=workerd,worker,browser`, handling `cloudflare:*`/`node:*` externals, `define`, `keep_names`, `minify`, `sourcemap`, and `tsconfig`;
- Map `rules` to esbuild loaders: `Text` / `Data` / `CompiledWasm`;
- `no_bundle`: skip bundling, and collect modules according to `rules` + `find_additional_modules` + `base_dir`;
- The artifact format is defined by CellHive (module manifest + assets); matching Wrangler's output format is not a goal;
- Users may still optionally bring framework/Vite prebuilt artifacts (preferentially supported; see below).

**Cost**: requires continuously tracking the evolution of Wrangler's **semantics** (configuration defaults, inheritance rules, edge cases), relying on the compatibility matrix + contract tests + strict rejection as the fallback.

## Configuration Parsing

- Format: **both jsonc + toml are supported** (`cellhive dev`); **`cellhive deploy --config` only parses jsonc/json** (no third-party TOML library introduced; toml users can convert to jsonc or use dev);
- Environment: `env.<name>` → independent app/namespace per environment; preserve the semantics of "bindings are not inheritable" and top-level-only fields;
- Validation: at least `name`, `main`, and `compatibility_date` are required (`main` may be omitted for assets-only); provide clear error codes.

## Support Matrix (Summary)

| Category | Fields | Handling |
|---|---|---|
| Core | `name` `main` `compatibility_date` `compatibility_flags` `tsconfig` `rules` `build` `no_bundle` `find_additional_modules` `base_dir` `preserve_file_names` `minify` `keep_names` `define` | Parse and preserve; `build.command` is executed locally only, not on the server |
| Bindings | `vars` `secrets`(declarations) `kv_namespaces` `d1_databases` `r2_buckets` `queues.producers/consumers` `workflows` `services` `durable_objects.bindings` `ai` `hyperdrive` `assets` `triggers.crons` | ✅ Map to binding/cell (hyperdrive: the origin URL is stored as a registered resource (envelope-encrypted), `id` is resolved by resource name; the platform does not pool connections, ADR-125/129; service `service` may be `ns/worker`, cross-ns access is authorized by the target-side `service-acl`, ADR-144) |
| DO lifecycle | New `exports` (declarative) + old `migrations` (imperative) | ✅ `created`/`sqlite` (including `new_classes`/`new_sqlite_classes`), `renamed_classes`, `deleted_classes`, and same-worker `transferred_classes {from,to}`; **reject** cross-worker transfer using `script_name` |
| Routing | `workers_dev` `route`/`routes` `custom_domain` `preview_urls` | `workers_dev` → `<ns>.<domain>/<worker>/`; custom domains/routes may optionally be mapped to Traefik host routing (ns/worker resolved by user-runtime), otherwise rejected; `preview_urls` rejected |
| Observability/limits | `observability` `logpush` `limits` `tail_consumers` | Positive `limits.cpu_ms` and `limits.subrequests` are saved with the version; `cpu_ms` is not yet enforced (ADR-213). Other fields are partially mapped or rejected. |
| Environment | `env.<name>` | Independent app/namespace per environment |
| Auto-provisioning | No automatic resource creation by id | **Rejected**; create production resources explicitly with Wrangler resource commands, then reference their returned IDs in configuration; deploy never implicitly creates them |
| Unsupported | `ai_search*` `dispatch_namespaces` `secrets_store_secrets` `send_email` `browser` `images` `containers` `flagship` `pipelines` `vpc` `placement` `site` | Explicitly rejected + error code |
| `vectorize` | **Supported** (ADR-158): `vectorize[{binding,index_name}]` → registered resource (kind `vectorize`, configured with dims/metric); `cellhive wrangler vectorize ...` forwards to native CellHive commands. The official Wrangler API supports V2 `create/list/get/delete/info`; other official Vectorize data commands are not yet aligned. Native `--deprecated-v1` is accepted with V2 semantics, without a V1 compatibility mode. |

## `cellhive wrangler` Prefix (ADR-138)

`cellhive wrangler <command>` is a wrangler-style **thin alias + argument translation**: command names are mapped to the native CLI. When `deploy` has no `-c/--config`, it **automatically discovers** `wrangler.jsonc`/`wrangler.json` in the current directory. Unsupported wrangler flags return actionable alternative suggestions.

| wrangler usage | Maps to |
|---|---|
| `wrangler deploy --namespace <ns> [-c file] [--env e] [--name w] [other native flags]` | `cellhive deploy <ns> <w> --config <file> …` (`worker` defaults to the configuration `name`) |
| `wrangler delete <ns> <worker>` | `cellhive worker delete` |
| `wrangler versions list` / `wrangler deployments list` | `cellhive versions list` / `cellhive deployments list` (the latter reads durable deployment events) |
| `wrangler deployments status` | `cellhive deployments status <ns> <worker>` (reads the current release) |
| `wrangler queues consumer add/list/remove` / `queues purge` | `cellhive queues consumer add/list/remove <ns> <queue> [worker]` / `cellhive queues purge <ns> <queue>` |
| `wrangler workflows trigger` / `workflows instances ...` | `cellhive workflows trigger <ns> <workflow>` / `cellhive workflows instances ... <ns> <workflow> [id]` |
| `wrangler rollback` / `wrangler promote` | Commands with the same names |
| `wrangler secret put\|get\|delete\|list` | `cellhive secret ...` (including `bulk`) |
| `wrangler tail` | `cellhive tail` |
| `wrangler d1 <verb> ...` | `cellhive d1 create\|list\|delete\|stats <ns> [name]`; data plane supports `execute\|query\|export <ns> <db> ...` and `migrations create\|list\|apply` |
| `wrangler r2 bucket <verb> ...` | `cellhive r2 create\|list\|delete\|stats <ns> [name]`; data plane supports `object put\|get\|delete\|list` |
| `wrangler kv namespace\|key ...` | `cellhive kv ...`; `key put\|get\|delete\|list <ns> <binding> [key] [value]` uses a scoped data-plane token; `bulk put\|delete` is also supported |
| `wrangler triggers deploy` | Prompt to use `cellhive deploy --config wrangler.jsonc` (`triggers.crons` are applied atomically with the version) |

- **Namespace**: CellHive has no account concept, so `--namespace <ns>` (= tenant) is required;
- **`deploy` flags**: `--dry-run` (server-side preflight without persisting state), `--var NAME=VALUE`, and `--secrets-file <JSON>` are supported and passed through; all other unsupported flags error directly and provide alternatives (for example, `--minify`→`build.minify`, `--compatibility-date`→`compatibility_date`);
- **Compatible command groups**: `versions`/`deployments`/`triggers`/`workflows`/`queues`/`types`/`init` are forwarded to native implementations or **structurally rejected + alternative explanation** (ADR-148); `d1`/`r2`/`kv` are forwarded to native per-domain commands, including the common data-plane commands above; `vectorize`/`hyperdrive` prefixes are passed through to native implementations;
- **Table-driven coverage of known commands**: `translateWrangler` has a built-in "mapping or alternative" table for known wrangler commands (`wranglerReject`), and **no known command will fall into `unknown command`**; only unknown names report unknown. Legacy `kv:bulk` maps to `kv bulk`. Explicitly unsupported (with alternatives): `dev`→`cellhive dev`, `pages`→deploy a Worker with `assets`, `dispatch`/`containers`/`pubsub`/`mtls-certificate`/`cert` → unsupported, `login`/`logout`→no CF account (use `cellhive creds`/`status`); stock `whoami` works against the compatibility API, `check`→`cellhive deploy --dry-run`, `docs`→repository `docs/`, `telemetry`→no telemetry, `unstable_*`→internal commands;
- **Subcommand-level alternatives**: `versions upload`→`cellhive deploy` (atomic release), `versions view`→`cellhive versions view`, `triggers deploy`→`cellhive deploy --config` (crons are applied atomically with the version). Native commands now cover `queues consumer list\|add\|remove`, `queues purge`, `workflows trigger`, and `workflows instances list\|describe\|pause\|resume\|terminate\|restart\|send-event\|delete`; `types` and `init` map to their native implementations.

**Wrangler 4.133.0 / 4.151.3 acceptance**: stock `whoami`, `deploy`, `deployments list/status`, `versions list`, `queues consumer add/list`, `queues purge`, `workflows trigger`, `workflows instances list/describe`, and `vectorize create/list/get/info` ran against an isolated admin server and were checked against durable control, Queue, and Workflow state. Deployment list reads events committed in the activation transaction; stock Wrangler status reads the latest event. Workflow describe includes persisted completed steps and outputs only; full attempt logs, step timing, and in-progress steps are not stored. Listing supports page, status, and RFC3339 date filters; `restart --from` is unsupported. 4.151.3 also calls `GET /workers/workers/{name}` after publish; that endpoint is now implemented. In that acceptance run, Queue consumer `retry_delay` was rejected, and purge uses Queue owner/capture writes. Vectorize management covers V2 create/list/get/delete/info; dot-product metric remains unsupported. Stock Hyperdrive management now supports create/list/get/update/delete: the id is the CellHive resource name, origin is sealed, and managed `hd_*` credentials are reused; `caching`/`mtls` are rejected because the platform does not implement cache/mTLS/connection-pool semantics. `whoami` uses the newly implemented `/user/tokens/verify`. This acceptance does not mean every listed lifecycle command was independently tested with stock Wrangler.

2026-10-19 update: Queue consumer `retry_delay` is wired, `max_batch_timeout` is active, and omitted `max_retries` defaults to three attempts. The preceding paragraph records earlier Wrangler acceptance; the new behavior still needs stock Wrangler and fault E2E revalidation (ADR-216).

**D1 SQL migrations**: `cellhive d1 migrations create\|list\|apply <ns> <db> [message] [--directory migrations]` shares the migration table with official Wrangler 4.133.0 remote migrations. Wrangler `migrations_table` and native CLI `--table` can select the same custom table. `list` shows pending files; `list --applied` shows applied records. Each file and its record commit in one transaction; errors roll back the file. Standard migrations submitted by both commands store a SHA-256 digest. The native CLI rechecks all files and rejects changed content under an existing name. Digest calculation ignores surrounding SQL whitespace to match Wrangler submissions. Wrangler skips already applied files, so rerunning it after changing one locally does not submit the change for detection. Official Wrangler `d1 execute --remote --command/--file` and `d1 migrations list/apply --remote` passed a real request-level comparison. File uploads are limited to 8 MiB and execute idempotently by SHA-256. Upload sessions live on one admin instance for 10 minutes; load balancing must keep init/upload/ingest on that instance, and a command can be rerun after a node exits. Local Wrangler D1 remains managed by its Miniflare instance rather than production CellHive.

**Additional CLI parity**: Wrangler 4.133.0 `d1 export --remote`, `kv bulk put/delete --remote`, `secret bulk`, and `versions view` are supported by equivalent native CellHive commands. D1 exports are capped at 32 MiB and omit nested transaction markers so `d1 execute --file` can restore them. Export download links live on one admin instance for one hour. KV bulk validates the full request first, then writes keys one by one; a later failure can leave earlier keys applied. Secret bulk is one control-cell transaction (1-100 entries); JSON `null` deletes a key.

**Type generation**: `cellhive types [worker-configuration.d.ts] [--config wrangler.jsonc] [--env name] [--env-interface Env] [--check]` generates an `Env` interface for configured vars, bindings, and assets. Vars use `string` and do not expose values. Full runtime declarations still come from `@cloudflare/workers-types` or official `wrangler types`. The official command is local and does not call CellHive. The Go CLI currently parses JSONC/JSON only; official Wrangler can handle `wrangler.toml`.

**Project initialization**: `cellhive init <worker-name> [--yes]` creates `wrangler.jsonc` and `src/index.js` without installing dependencies or overwriting an existing directory; `cellhive wrangler init` works too. Official `wrangler init` continues to run its own local wizard and needs no platform API.

**KV/R2 data plane**: official Wrangler 4.133.0 `kv key put/get/list/delete --remote` and `r2 object put/get/delete --remote` passed real request-level comparisons. Equivalent operations are also available through `cellhive kv key ...` and `cellhive r2 object ...` (the CellHive form includes `<namespace>`).
- **Official Wrangler compatibility**: the admin listener accepts Wrangler 4.133.0 with `CLOUDFLARE_API_BASE_URL=<CellHive admin>/client/v4`, `CLOUDFLARE_API_TOKEN=<CellHive admin token>`, and `CLOUDFLARE_ACCOUNT_ID=<namespace>`; Cloudflare's account ID maps to a CellHive namespace. Worker publish supports both the `content/v2`/`versions` paths and the legacy `PUT /workers/scripts/{script_name}` path used when migrations/DO/containers make Wrangler choose legacy upload; the `migrations` metadata accepts both the array form and `{new_tag, steps}`; `versions upload` returns `metadata.has_preview`; account discovery, Worker delete, selected-version deploy, deployment events and versions, KV/D1/R2/Queues resource create/list/delete, Workflow CRUD and instance operations, Worker version list/get, Cron schedules, Worker secrets, routes, short-lived tail sessions with WebSocket log streaming, and the `assets-upload-session` JWT single or multipart bulk asset upload flow use Cloudflare-shaped endpoints. Workflow `concurrency.limit`, `schedules`, and `default_retention` are persisted and enforced; `limits` remains explicitly rejected. Production resources are explicitly created first and deploy references their IDs; deploy does not implicitly provision resources. `wrangler deploy` reports success, but its `Current Version ID` display may still be `null` (the real version is visible via `versions list`). Complex multi-module uploads and unlisted Cloudflare APIs remain outside this compatibility surface.

## `compatibility_flags` Authority (ADR-153/186)

The platform follows pinned workerd (`internal/workerdbin.PinnedVersion`, currently `1.20260916.1`) and validates at deploy time; unknown flags and flags marked experimental upstream fail closed. The complete list is no longer handwritten in documentation or validators. `cmd/workerd-compat-gen` reads `compatibility-date.capnp` from a fixed upstream revision, emits `internal/workerdcompat/manifest.json`, then renders `cli/src/workerd-compat.generated.ts` for Bun. The Go control plane consumes the manifest directly, and contract tests compare both consumers item by item.

The compatibility ceiling `2026-09-23` is bound to this pin/manifest. `scripts/workerd-compat-probe.sh` verifies the maximum date, rejection of the following day, every allowed flag, and unknown-flag rejection against the real binary. Acceptance for **framework prebuilt artifacts** (OpenNext/SvelteKit/Astro) is covered item by item in `internal/wrangler TestFrameworkPrebuiltLayouts`.

## Rejection Policy

- Unknown/unsupported fields **error explicitly** and are not silently ignored (to avoid failing only at runtime);
- Error messages include the field path, reason, and "whether support is planned";
- Maintain `compatibility-matrix.md`, recording Supported / Partial / Rejected across the two dimensions of runtime + Wrangler configuration;
- Run **offline contract tests** against a fixed Wrangler semantic baseline.

## Routing

- `workers_dev` (default true) → namespace service path `<ns>.<platform-domain>/<worker>/`;
- Custom domains / `routes` / `custom_domain`: **optionally** mapped to Traefik host routing, then `user-runtime` resolves ns/worker; explicitly rejected when unavailable;
- `preview_urls`: unsupported (rejected).
- Entry = Traefik (TLS/host routing) + `user-runtime` (Worker routing/version resolution/header cleaning); **no separate gateway** (ADR-017).

## Assets (Complete Pipeline Filled In This Phase)

CF static-assets semantics must be covered:

- Static directory upload + **content hash + version binding** (rollback flips the asset URL);
- `_headers` / `_redirects` (including validation of rule count limits);
- `not_found_handling` (such as SPA fallback);
- `run_worker_first` (route to Worker before assets);
- `.assetsignore`;
- Edge asset service: ETag / `If-None-Match` 304, `cache-control`;
- Asset service in local `cellhive dev`;
- Alignment with asset expectations of SSR frameworks (Next/OpenNext `.open-next/assets`, SvelteKit, Astro).

## Framework / Vite Prebuilt Artifacts (Preferentially Supported)

Next/OpenNext, SvelteKit, Astro, Nuxt(nitro), Qwik, SolidStart, and similar frameworks usually provide their own build, producing a Worker + configuration + assets. In this path, we mainly perform **configuration parsing + module/asset collection + upload**, do not participate in bundling, and have the lowest cost; it should be supported first.

## Local Development

- Provide built-in `cellhive dev`: **Bun CLI + Miniflare** (real workerd + locally simulated bindings), requiring no Docker/cloud bucket, and zero Go processes on the dev machine (ADR-065);
- Version pin: Miniflare's built-in workerd is overridden to the platform-pinned version;
- Preview/environment selection aligns with `env.*`;
- deploy has **server-side feature/compatibility interception** (support matrix; rejects `images/ai/browser/...`), and dev/CLI warns early;
- For the complete design (process model/version pinning/contract diffing/user-code compatibility/CLI/differences/acceptance), see [`dev-mode.md`](dev-mode.md).

## Pinning and Contract Tests

- Pin: **workerd version** + **Wrangler semantic baseline version** (as a reference; its runtime is not introduced);
- Contract tests: validate configuration normalization, binding mapping, rejection behavior, and artifact shape against the fixed baseline;
- Upgrading is an explicit action and must run the contract tests.

## Security / Privacy

- Native `cellhive deploy` does not call the Cloudflare API. Stock `wrangler deploy` can target the CellHive admin listener through `CLOUDFLARE_API_BASE_URL`; users should disable Wrangler telemetry themselves, for example with `WRANGLER_SEND_METRICS=false`;
- The production CellHive CLI does not embed or launch Wrangler. Pinned Wrangler 4.133.0 is used for compatibility tests or invoked directly by users;
- The default path (Go+esbuild) produces no external telemetry.

## Capability Boundaries with the cf Runtime

- SSR: inherits the workerd ecosystem, but the platform must **enable `nodejs_compat` (including v2)** and complete the asset pipeline;
- `nodejs_compat` is enabled by default when the compatibility date ≥ 2026-08-04; workers that need to disable it must set both `no_nodejs_compat` and `no_nodejs_compat_v2`;
- Unsupported: Python Workers, Cache API, Browser Rendering, Email Workers, etc. (consistent with the workerd exposed surface); Vectorize/Hyperdrive are implemented by the platform itself (ADR-158/129).

_Last updated: 2026-09-18_

## assets Routing Configuration (ADR-071)

Supports `assets.directory`, `assets.binding`, `assets.not_found_handling` (`none`/`404-page`/`single-page-application`), and `assets.run_worker_first` (bool or path array). Stored with the immutable version at deploy time and applied by the loader (real workerd e2e). The legacy `[site] bucket` is still mapped to directory.

## DO migrations Support Matrix (ADR-081)

| Migration | Support | Notes |
|---|---|---|
| `tag` / `new_classes` / `new_sqlite_classes` | ✅ | Class creation (we load dynamically; declarations are only used for validation) |
| `renamed_classes` | ✅ | Record `codeClass→storageClass` alias; renaming **preserves data** (ADR-082) |
| `deleted_classes` | ✅ | Mark deleted + physically reclaim facet storage via `/v1/do/delete` |
| `transferred_classes` (same worker `{from,to}`) | ✅ | Equivalent to rename: hand over the storage identity of `from` to `to` (reuse the ADR-082 alias registry; fail-closed if `to` already exists) |
| `transferred_classes` with `script_name` (cross-worker) | ❌ `invalid_migration` | Cross-worker transfer is unsupported: objects are named by worker/storage_id, and cannot be safely moved across workers |
| Other fields | ❌ `unknown_migration_field` | Explicitly rejected |
| Shape errors | ❌ `invalid_migration` | For example, `{from}` missing `to` |

Code changes (same class name, new bundle) **do not require a migration**: they take effect via facet-level lazy restart (`doStorageId` ensures storage remains unchanged).
