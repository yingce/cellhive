# Module: Bindings and Compatibility Surface

Map each type of Cloudflare binding to platform resources and host adapters, and define sync/async boundaries and rejected items: KV, D1, R2, Queue, Cron, Workflows, Assets, Vars/Secrets, AI (BYO), Hyperdrive, Vectorize, Service.

> The authoritative source for configuration is [`../configuration.md`](../configuration.md) and the code; the table below is the relevant subset for this module.
## Key Interfaces

`/v1/kv/*`, `/v1/d1/*`, `/v1/r2/*`, `/v1/queue/*`, `/v1/workflow/*`, `/v1/vectorize/*`, `/v1/service/run`, and `/v1/internal/hyperdrive` on cell-agent.

## Configuration (Environment Variables)

| Variable | Default | Purpose |
|---|---|---|
| `CELLHIVE_AI_URL` / `_KEY` | empty | BYO AI endpoint; empty = not injected |
| `CELLHIVE_BINDING_CACHE` | `1s` (0 disables) | binding declaration cache |
| `CELLHIVE_NS_RATE` | empty (off) | per-ns write admission `rps[/burst]` (default burst=rps) |


> Parsing rules (strings/booleans/durations/bytes/lists) are in [`../configuration.md`](../configuration.md#parsing-rules).

## Key Invariants

- binding name = registered resource name (no automatic provisioning).
- **Tenant env is entirely user-owned**: Tenant Worker and DO-facet env contains only user-declared vars, current managed secrets, and user-named binding stubs, with **zero** platform keys. Secrets are envelope-encrypted in the control cell and decrypted for runtime injection; a same-named secret overrides a var. `CH_*`, `CELL_*`, `__cellhive*`, `PLATFORM`, `LOG_*`, and `WF_*` are user names (ADR-185). DO WebSocket upgrades and calls use platform-side `fetch(request)`/`rpcObject(...)` stubs, with no tenant-visible private transport. A trusted internal host passes its dispatcher-bound `WorkflowBridgeTarget extends RpcTarget` as a JSRPC argument to the wrapper; it is neither a cross-loader `ServiceStub` nor env.
- One scoped token per binding (HMAC).
- Reads are **strongly consistent** (forwarded to owner).
- Cloudflare rejected items are **explicitly rejected** (Cache API, Sessions, SSE-C, etc.); unknown fields/flags are rejected at deployment time.

## Source Locations

`internal/{kv,d1,r2,queue,workflow,vectorize}` (on top of cellstore); `workerd/platform/{bindings.js,facades.js,bindings-wrapper.js}`; `internal/server/*_binding*.go`.

## Test Anchors

`make js-test`, `internal/server`, `make cli-test` (wrangler mapping).

## Related Documentation

[`bindings.md`](../bindings.md), [`compatibility-matrix.md`](../compatibility-matrix.md), [`wrangler-compat.md`](../wrangler-compat.md)

_Last updated: 2026-09-22_
