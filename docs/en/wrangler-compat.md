# Wrangler / Workers Compatibility

Goal: allow users to keep using standard `wrangler.jsonc`/`wrangler.toml` and the Workers programming model, while `cellhive deploy` performs local packaging and publishes to the CellHive platform, **without ever touching Cloudflare**.

## Three Key Measures

1. **Configuration parsing**: parse `wrangler.jsonc`/`.json`/`.toml` + environment inheritance + schema validation, and map them to platform metadata;
2. **Bundling**: the Go CLI embeds the **esbuild Go SDK** (esbuild itself is Go), bundles in-process, and has **no Node dependency**;
3. **CLI surface** (actual): `deploy` (`--config <wrangler.jsonc>` can directly map main/bindings/vars/crons/assets/migrations, or use explicit flags), `bundle build` (Go+esbuild bundling), `app`/`resource` (explicit resource registration, **no auto-provisioning**), `promote`/`rollback`/`releases`, `route`, `secret put|get`, `worker delete`, `workflow create`, `tail`, `status`/`capacity`/`diagnose`/`audit`/`routes`, `gc bundles|assets`, `asset put`. [`dev` is in the Bun CLI (`cli/`), not this binary.]

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
| DO lifecycle | New `exports` (declarative) + old `migrations` (imperative) | ✅ Only `created`/`sqlite` (and `new_classes`/`new_sqlite_classes`); **reject** rename/delete/transfer and `script_name` |
| Routing | `workers_dev` `route`/`routes` `custom_domain` `preview_urls` | `workers_dev` → `<ns>.<domain>/<worker>/`; custom domains/routes may optionally be mapped to Traefik host routing (ns/worker resolved by user-runtime), otherwise rejected; `preview_urls` rejected |
| Observability/limits | `observability` `logpush` `limits` `tail_consumers` | Partially mapped or rejected |
| Environment | `env.<name>` | Independent app/namespace per environment |
| Auto-provisioning | No automatic resource creation by id | **Rejected**; resources are explicitly managed by the control plane |
| Unsupported | `ai_search*` `dispatch_namespaces` `secrets_store_secrets` `send_email` `browser` `images` `containers` `flagship` `pipelines` `vpc` `placement` `site` | Explicitly rejected + error code |
| `vectorize` | **Supported** (ADR-158): `vectorize[{binding,index_name}]` → registered resource (kind `vectorize`, configured with dims/metric); CLI `wrangler vectorize create|list|delete|get|info|insert|upsert|query|get-vectors|delete-vectors|list-vectors|create-metadata-index|list-metadata-index|delete-metadata-index` prefix pass-through; `--deprecated-v1` is accepted but has V2 semantics (no V1 compatibility mode) |

## `cellhive wrangler` Prefix (ADR-138)

`cellhive wrangler <command>` is a wrangler-style **thin alias + argument translation**: command names are mapped to the native CLI. When `deploy` has no `-c/--config`, it **automatically discovers** `wrangler.jsonc`/`wrangler.json` in the current directory. Unsupported wrangler flags return actionable alternative suggestions.

| wrangler usage | Maps to |
|---|---|
| `wrangler deploy --namespace <ns> [-c file] [--env e] [--name w] [other native flags]` | `cellhive deploy <ns> <w> --config <file> …` (`worker` defaults to the configuration `name`) |
| `wrangler delete <ns> <worker>` | `cellhive worker delete` |
| `wrangler versions list` / `wrangler deployments list` | `cellhive releases` |
| `wrangler rollback` / `wrangler promote` | Commands with the same names |
| `wrangler secret put\|get\|delete\|list` | `cellhive secret ...` |
| `wrangler tail` | `cellhive tail` |
| `wrangler d1 <verb> ...` | `cellhive d1 create\|list\|delete\|stats <ns> [name]` (data-plane verbs such as `execute` are structurally rejected → use the Worker's D1 binding) |
| `wrangler r2 bucket <verb> ...` | `cellhive r2 create\|list\|delete\|stats <ns> [name]` (`object` and other data-plane verbs are structurally rejected → use the R2 binding) |
| `wrangler kv namespace\|key ...` | `cellhive kv ...` (namespace verbs use the native path; `key` data-plane operations are structurally rejected → use the KV binding) |
| `wrangler triggers deploy` | Prompt to use `cellhive deploy --config wrangler.jsonc` (`triggers.crons` are applied atomically with the version) |

- **Namespace**: CellHive has no account concept, so `--namespace <ns>` (= tenant) is required;
- **`deploy` flags**: `--dry-run` (server-side preflight without persisting state), `--var NAME=VALUE`, and `--secrets-file <JSON>` are supported and passed through; all other unsupported flags error directly and provide alternatives (for example, `--minify`→`build.minify`, `--compatibility-date`→`compatibility_date`);
- **Compatible command groups**: `versions`/`deployments`/`triggers`/`workflows`/`queues`/`types`/`init` are forwarded to native implementations or **structurally rejected + alternative explanation** (ADR-148); `d1`/`r2`/`kv` are forwarded to **native per-domain commands** (ADR-157, only data-plane verbs are rejected); `vectorize`/`hyperdrive` prefixes are passed through to native implementations;
- **Table-driven coverage of known commands**: `translateWrangler` has a built-in "mapping or alternative" table for known wrangler commands (`wranglerReject`), and **no known command will fall into `unknown command`**; only unknown names report unknown. Explicitly unsupported (with alternatives): `dev`→`cellhive dev`, `pages`→deploy a Worker with `assets`, `dispatch`/`containers`/`pubsub`/`mtls-certificate`/`cert` → unsupported, `login`/`logout`/`whoami`→no CF account (use `cellhive creds`/`status`), `check`→`cellhive deploy --dry-run`, `docs`→repository `docs/`, `telemetry`→no telemetry, `kv:bulk`→data plane (use KV binding), `unstable_*`→internal commands;
- **Subcommand-level alternatives**: `versions upload`→`cellhive deploy` (atomic release), `versions view`→`cellhive releases`, `triggers deploy`→`cellhive deploy --config` (crons are applied atomically with the version), `queues consumer`→`queues.consumers` + `deploy` (use `queue status` for backlog), `workflows status\|describe\|trigger`→`env.WF` (instances are runtime entities), `d1 execute`/`r2 object`/`kv key`→Worker binding + `cellhive dev`, `types`/`init`→unsupported (write your own `Env` interface / create your own wrangler.jsonc);
- **True wrangler compatibility (alternative, not implemented)**: testing shows wrangler 4.133.0 reads `CLOUDFLARE_API_BASE_URL` (old name `CF_API_BASE_URL`) and the SDK's `CLOUDFLARE_BASE_URL`, so a subset of the CF API could be implemented to let it run unchanged; the cost is tracking CF API drift + matching the `content/v2` multipart upload format. This is an independent spike and outside the scope of this item.

## Supported `compatibility_flags` List (ADR-153)

Track pinned workerd (`internal/workerdbin.PinnedVersion`, currently `1.20260615.1`), validate at deploy time (`unknown_flag` fails closed), and keep the Go validator and dev CLI list mirrored and enforced by tests:

`nodejs_compat`, `nodejs_compat_v2`, `nodejs_compat_populate_process_env`, `no_handle_cross_request_promise_resolution`, `global_fetch_strictly_public`, `disable_fetch_stream_teeing`, `streams_enable_constructors`, `transformstream_enable_standard_constructor`, `export_commonjs_default`, `export_commonjs_namespace`, `disable_nodejs_process_v2`, `enable_ctx_exports`, `deployment_id_header`, `require_custom_ports_development`.

The compatibility upper bound `2026-06-22` is bound to this pin (`TestPinPairsWithCompatibilityDate`). Acceptance for **framework prebuilt artifacts** (OpenNext/SvelteKit/Astro) is covered item by item in `internal/wrangler TestFrameworkPrebuiltLayouts`.

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

- **Never call the Cloudflare API** and do not perform a real `wrangler deploy`;
- If an "optional high-fidelity path" is provided in the future (calling the locally pinned wrangler), `send_metrics` and `dependencies_instrumentation` must be forcibly disabled;
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