# workerd Integration

## Principles

**Use stock workerd, without modifying a single line of C++**. Use it only through official public surfaces:

- `workerLoader` (dynamically load tenant bundles; the process requires `--experimental`);
- capnp configuration (services / sockets / bindings / `durableObjectNamespaces` / localDisk / network);
- bindings (`globalOutbound`, service, network, DO namespace, etc.);
- process switches and compatibility configuration.

## Version Pin

- **The workerd binary is pinned to an exact version**; the valid set of `compatibility_date`/`flags` is maintained with that version (a table);
- Loaded bundles must satisfy the maximum `compatibility_date` supported by that workerd;
- Upstream `$experimental` tenant flags are **not enabled**; the default policy for `nodejs_compat`/`nodejs_compat_v2` is determined by date;
- Contract tests are fixed to the pinned version.

ADR-186 fixes the implemented code baseline at stock workerd `1.20260916.1`, esbuild `0.28.2`, and Miniflare `5.20260916.0-alpha`, with maximum compatibility date `2026-09-23`. The manifest is generated from upstream revision `adda2635656d09e541b0feeea796da9d2a8bc10e`; the Go control plane and Bun CLI consume the same generated authority, which is cross-checked against the real binary. See [`testing.md`](testing.md) for the final Docker Compose and `REQUIRE_ALL=1` acceptance status.

Generation and verification commands (`$WORKERD_UPSTREAM` must be a local checkout at that revision):

```bash
go run ./cmd/workerd-compat-gen \
  -source "$WORKERD_UPSTREAM" \
  -version 1.20260916.1 \
  -revision adda2635656d09e541b0feeea796da9d2a8bc10e \
  -out internal/workerdcompat/manifest.json \
  -ts-out cli/src/workerd-compat.generated.ts
CELLHIVE_WORKERD="$(command -v workerd)" bash scripts/workerd-compat-probe.sh
```

## Services and Configuration

| Service | workerd Role | Configuration Notes |
|---|---|---|
| `user-runtime` | Tenant loader | `workerLoader`; sockets `:8081` (public loader) + `:8088` (internal privileged dispatch); tenant loaded worker **public Internet outbound only** |
| `do-runtime` | DO host actor + facet | `durableObjectNamespaces` + localDisk; `workerLoader` loads the same bundle and `getDurableObjectClass()`; supervisor acts as PID1 |
| (Optional) `system-runtime` | Platform-internal worker | Private + public Internet outbound (if platform-side workerd workers are enabled) |

## Loading Flow

```
worker id = <ns>:<worker>:<version>   (immutable)
user-runtime loader:
  1. Obtain worker id from the routing projection
  2. workerLoader.get(id, () => fetchBundle(id))
       · bundle is fetched from object storage by content address (SHA-256) (scoped read-only credentials, ADR-030)
  3. Generate wrapper (JS layer):
       · wrap tenant module exports (fetch/scheduled/queue/alarm/RPC)
       · construct tenant env from user-declared vars and user-named binding stubs only; zero platform keys (ADR-185)
       · preserve built-in modules such as `cloudflare:workers`; shim modules such as `cloudflare:workflows`
  4. Invoke handler
```

- **Env budget (ADR-186, implemented)**: 1 MiB upstream limit with 8 KiB headroom, giving a **1016 KiB** CellHive limit, including V8 two-byte string cost. Rejections use `worker_env_too_large` and expose only `actual_bytes`/`max_bytes`.
- **WorkerCode budget (ADR-186, implemented)**: the final form passed to `workerLoader` (tenant modules, wrapper and platform-injected modules) is limited to **64 MiB**. The control plane rejects before its transaction/active-pointer change, and the runtime rechecks through the single shared `checkedWorkerGet()` path. Rejections use `worker_code_too_large`.
- **Host secrets (ADR-186, implemented)**: platform URLs/tokens enter trusted host bindings only through Cap'n Proto `fromEnvironment`. user-runtime, do-runtime, and do-supervisor construct an explicit workerd child environment rather than inheriting the parent environment. Secrets must not appear in rendered capnp, final WorkerCode, tenant env, arguments, or logs.
- **Secret boundary**: secrets can be encrypted, stored, and managed, but are not yet injected into the runtime env; they are not an env source above. A future injection path must retain zero platform keys and no reserved names.
- **Workflow / logging boundary**: a trusted internal host creates a dispatcher-bound `WorkflowBridgeTarget extends RpcTarget` and passes it as a JSRPC parameter across `workerLoader` to the wrapper. It is neither a `ServiceStub` nor tenant env. The dynamic-Tail spike was rerun on pin `1.20260916.1` and remains rejected (`provided value is not of type 'Fetcher'`), so platform capture of tenant `console.*` is disabled and must not fall back to env transport.
- **Reserved module prefixes**: platform-generated module names use reserved prefixes (such as `__cellhive-`); tenants must not occupy them.

## host adapter and Networking

- Generate a **binding-scoped facade** for each binding, with immutable props (`ns` + binding type/id);
- The facade calls `cell-agent` (`:7001` REST/JSON), carrying a **scope declaration** (ADR-029);
- **The tenant loaded worker's `globalOutbound` = public Internet only**, excluding RFC1918/cell-agent;
- Platform code (loader/host adapter/DO host actor) uses a separate private network binding.

## Limits

- `limits`: per-worker `cpu_ms`/`subrequests` (workerd config); V8 heap limit (process/isolate level);
- Not supported: Python Workers, Cache API, Browser Rendering, Email Workers, and Analytics Engine. See the compatibility matrix for the CellHive-supported Vectorize and Hyperdrive subsets;
- Compatibility with all historical `compatibility_date` behavior is not guaranteed—it is subject to the set of flags we support.

## P0 Validation Items (Together with DO/WAL)

1. Whether actor SQLite is WAL and can be opened read-only by an external process;
2. Checkpoint/truncation can be detected and aligned;
3. Actor file layout (shared metadata + per actor);
4. `globalOutbound` public Internet restriction configuration takes effect;
5. `workerLoader` load/eviction behavior works correctly under the current pinned version;
6. env budget validation matches reality.

## To Be Refined

- Specific capnp configuration snippets (user-runtime / do-runtime);
- Wrapper generation details and reserved module name prefixes;
- Compatibility flag table (following the pinned workerd).

## Upgrade and Rollback

1. Deploy the new reader/runtime and prove it can read old artifacts and DO working copies before allowing the control plane to accept new dates/flags (reader-before-writer).
2. Upgrade the workerd binary, `internal/workerdcompat/manifest.json`, `cli/src/workerd-compat.generated.ts`, and the Miniflare pair as one unit; never replace only one of them.
3. Before rollback, enumerate active versions and validate their dates/flags with the target old pin. If any active version needs a date/flag unknown to that pin, reject rollback and first promote a compatible version.
4. Rolling back esbuild affects future builds only; existing content-addressed bundles are not rewritten. This phase has no schema, cell/owner/epoch, or RPO=0 protocol migration.

_Last updated: 2026-09-22_
