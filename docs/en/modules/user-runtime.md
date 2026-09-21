# Module: user-runtime (Entry Loader and Tenant Execution)

Public entry `:8081` + internal privileged dispatch `:8088`: resolves routes by host/version, dynamically loads immutable bundles with `workerLoader`, injects the binding facade, and executes the tenant's `fetch`/`scheduled`/`queue`; tenant outbound access is public-network only.

> See [`../configuration.md`](../configuration.md) and the code for the authoritative configuration source; the table below is the subset relevant to this module.
## Key Interfaces

`:8081` public (Host→worker; `/ready`, `/healthz`); `:8088` internal (`/v1/timers/dispatch`, queue dispatch, `/drain`).

## Configuration (Environment Variables)

| Variable | Default | Purpose |
|---|---|---|
| `CELLHIVE_AI_URL` / `CELLHIVE_AI_KEY` | empty | BYO AI endpoint and key; empty = not injected |
| `CELLHIVE_CELL_URL` | `http://127.0.0.1:7001` | cell-agent REST |
| `CELLHIVE_DO_DIRECT` | empty = enabled | `0` disables owner-hint direct access |
| `CELLHIVE_ESBUILD` | auto-discovered | esbuild path |
| `CELLHIVE_FACADES_JS` | `workerd/platform/facades.js` | facade source |
| `CELLHIVE_ROOT_KEY` | required | Derives internal/scope/dispatch/log credentials |
| `CELLHIVE_RUNTIME_DIR` | `$TMPDIR/cellhive` | Runtime directory (subdirectory `user-runtime/`) |
| `CELLHIVE_SERVICE_NATIVE` | empty = enabled | `0` disables native service RPC |
| `CELLHIVE_TENANT_OUTBOUND` | empty→`public` | Outbound category `public`/`private`/`local` |
| `CELLHIVE_USER_RUNTIME_INTERNAL_PORT` | `8088` | Internal dispatch |
| `CELLHIVE_USER_RUNTIME_JS` | `workerd/user-runtime` | loader JS |
| `CELLHIVE_USER_RUNTIME_PORT` | `8081` | Public entry |
| `CELLHIVE_WORKERD` | auto-discovered | workerd path (prefers pinned `1.20260615.1`) |


> For parsing rules (strings/booleans/durations/bytes/lists), see [`../configuration.md`](../configuration.md#parsing-rules).

## Key Invariants

- Tenant `globalOutbound` = **public-only**; binding stubs keep their transport in the trusted platform host, not in tenant env.
- The env patch (`bindings-wrapper.js` / `queue-wrapper.js`) keeps `this.env` consistent with the constructor parameter `env`.
- **Does not hold bucket credentials**; only obtains the binding facade and scoped tokens.
- Workflow callbacks cross `workerLoader` as a dispatcher-bound `WorkflowBridgeTarget extends RpcTarget` JSRPC argument, never as a `ServiceStub` or tenant-env value. Pin `1.20260615.1` rejects Tail Workers for dynamically loaded Workers (`provided value is not of type 'Fetcher'`), so platform capture of tenant `console.*` is disabled.

## Source Locations

`cmd/user-runtime/`; `internal/userruntime/`; `workerd/user-runtime/{loader.js,internal.js,queue-wrapper.js,workflow-wrapper.js,cellhive-workflow.js}`; `workerd/platform/{facades.js,bindings.js,bindings-wrapper.js,telemetry.js,rpc-codec.js}`.

## Test Anchors

`make js-test` (real workerd), `internal/userruntime`.

## Related Documents

[`routing.md`](../routing.md), [`bindings.md`](../bindings.md), [`workerd-integration.md`](../workerd-integration.md)

_Last updated: 2026-09-22_
