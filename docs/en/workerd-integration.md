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

- **env budget**: workerd serialized env limit (about 1 MiB); the control plane validates it on deploy/secret changes (including V8 double-byte overhead).
- **Secret boundary**: secrets can be encrypted, stored, and managed, but are not yet injected into the runtime env; they are not an env source above. A future injection path must retain zero platform keys and no reserved names.
- **Workflow / logging boundary**: a trusted internal host creates a dispatcher-bound `WorkflowBridgeTarget extends RpcTarget` and passes it as a JSRPC parameter across `workerLoader` to the wrapper. It is neither a `ServiceStub` nor tenant env. Native Tail Workers for dynamically loaded Workers are rejected on pin `1.20260615.1` (`provided value is not of type 'Fetcher'`), so platform capture of tenant `console.*` is disabled and must not fall back to env transport.
- **Reserved module prefixes**: platform-generated module names use reserved prefixes (such as `__cellhive-`); tenants must not occupy them.

## host adapter and Networking

- Generate a **binding-scoped facade** for each binding, with immutable props (`ns` + binding type/id);
- The facade calls `cell-agent` (`:7001` REST/JSON), carrying a **scope declaration** (ADR-029);
- **The tenant loaded worker's `globalOutbound` = public Internet only**, excluding RFC1918/cell-agent;
- Platform code (loader/host adapter/DO host actor) uses a separate private network binding.

## Limits

- `limits`: per-worker `cpu_ms`/`subrequests` (workerd config); V8 heap limit (process/isolate level);
- Not supported: Python Workers, Cache API, Vectorize, Hyperdrive, Browser Rendering, Email Workers, Analytics Engine (consistent with the workerd exposed surface);
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

_Last updated: 2026-09-22_
