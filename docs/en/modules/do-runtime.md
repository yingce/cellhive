# Module: do-runtime / do-supervisor (Durable Objects)

Use a fixed **host actor + facets** to host native Durable Objects (per-object SQLite, alarm shim, hibernatable WebSocket), with distributed elasticity; it only holds working copies and is not authoritative. Optional `do-supervisor` provides an **output gate** to confirm response durability (RPO=0).

> See [`../configuration.md`](../configuration.md) and the code for the authoritative configuration source; the table below is the relevant subset for this module.
## Key Interfaces

do-runtime `:8788`: `/v1/do/invoke`, `/v1/do/connect` (WS), `/v1/do/{claim,renew,drain,abort,delete,restart,objects}`, `/v1/internal/do/alarm/upsert`, `/ready`. do-supervisor `:18901`: `/metrics`.

## Configuration (Environment Variables)

| Variable | Default | Purpose |
|---|---|---|
| `CELLHIVE_CELL_URL` / `CELLHIVE_ROOT_KEY` / `CELLHIVE_TENANT_OUTBOUND` / `CELLHIVE_AI_URL` / `CELLHIVE_AI_KEY` | Same as above | Same as user-runtime |
| `CELLHIVE_DATA_DIR` | `./.cellhive/data` | DO SQLite disk = `<DATA_DIR>/do` |
| `CELLHIVE_DO_ADDR` | `*:8788` | workerd listener |
| `CELLHIVE_DO_ADVERTISE` | `http://127.0.0.1:8788` | Advertised address (owner forwarding target) |
| `CELLHIVE_DO_GATE_URL` | empty | Output gate base URL; empty = bypass gate |
| `CELLHIVE_DO_LEASE` | `30s` | DO owner lease (Go duration, converted to whole seconds for the host actor) |
| `CELLHIVE_DO_NODE` | hostname | Node id |
| `CELLHIVE_DO_OBJECT_INDEX` | `false` | Persist object registry to bucket (`true`/`1`) |
| `CELLHIVE_DO_PREVENT_EVICTION` | `true` | resident/evictable, must be exactly `true`/`false` |
| `CELLHIVE_DO_RUNTIME_JS` | `workerd/do-runtime` | host actor JS |
| `CELLHIVE_ESBUILD` | auto-detect | esbuild path |
| `CELLHIVE_RUNTIME_DIR` | `$TMPDIR/cellhive` | Runtime directory (subdirectory `do-runtime/`) |
| `CELLHIVE_WORKERD` | auto-detect | workerd path (prefer pinned `1.20260916.1`) |


> For parsing rules (strings/booleans/durations/bytes/lists), see [`../configuration.md`](../configuration.md#parsing-rules).

## Key Invariants

- facet class must come from `workerLoader.getDurableObjectClass()`.
- alarm must be shimmed (stock workerd does not implement native alarm for SQLite facets).
- WS migration/restart closes with **1012**, and clients reconnect (no resume).
- **Do not return ack before gate confirmation**.
- The facet tenant env follows ADR-185: user vars and user-named binding stubs only, with zero platform keys; secrets are not yet injected into runtime env. Pin `1.20260916.1` also rejects a Tail Worker for dynamic loaded DO facets (`provided value is not of type 'Fetcher'`), so platform capture of tenant `console.*` is disabled.
- Facet WorkerCode (64 MiB) and estimated env (1016 KiB) are rechecked before `workerLoader.get()`. When a DO boundary drops custom Error fields, only the platform's canonical LimitError string is parsed and revalidated before returning code/actual/max.

## Source Locations

`cmd/do-runtime/`, `cmd/do-supervisor/`; `internal/doruntime/`, `internal/dosupervisor/`; `workerd/do-runtime/{host.js,cellhive-do.js,bindings-wrapper.js}`; `workerd/platform/budget.js`.

## Test Anchors

`make js-test` (real workerd), `internal/doruntime`.

## Related Documents

[`durable-objects.md`](../durable-objects.md), [`workerd-integration.md`](../workerd-integration.md)

_Last updated: 2026-09-22_
