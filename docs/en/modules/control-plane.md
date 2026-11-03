# Module: Control Plane and Release

> ADR-304 is **implemented, local acceptance passed, not deployed**. Bearer HS256 JWT, default audience `cellhive`, no static-header/OIDC compatibility. Explicit `ns=["*"]` permits global operational reads/GC/full metrics; issuer CRUD requires bootstrap admin. CF only on `:8082`, never `:7001/:8083`.

Applications/versions/routes/domains/resources/secrets/audit and release pipelines; single control database (`app` is the `ns` in the table), immutable versions, atomic releases, resources are registered before deploy.

> For the authoritative configuration source, see [`../configuration.md`](../configuration.md) and the code; the table below is the subset relevant to this module.
## Key Interfaces

`/v1/control/*` on admin `:8082` (apps, deploy, promote, rollback, routes, domain, resource, secret, audit, releases, capacity, gc, worker, queue, workflow).

## Configuration (Environment Variables)

| Variable | Default | Purpose |
|---|---|---|
| `CELLHIVE_TOKEN` | empty | CLI Bearer JWT, refresh before expiry; see configuration for input precedence |
| `CELLHIVE_AUDIT_RETENTION` | `720h` (0 permanent) | Audit retention |
| `CELLHIVE_AUTO_CREATE_APP` | `true` | Automatically create app on first deploy |
| `CELLHIVE_BASE_DOMAIN` | empty | Built-in domain `<ns>-<worker>.<base>`; empty=disabled |
| `CELLHIVE_DISPATCH_URL` | empty | Timer/queue/cron/workflow dispatch target — must be the **user-runtime internal privileged endpoint** (`http://user-runtime:8088`). If empty, the dispatch loop does not start (set by default in compose/k8s/Helm) |
| `CELLHIVE_DO_EAGER_RESTART` | `false` | Eagerly restart DO on deploy |
| `CELLHIVE_NS_RATE` | empty (off) | Per-ns write admission `rps[/burst]` (default burst=rps) |
| `CELLHIVE_WORKFLOW_RETENTION` | `0` permanent | Prune terminal instances |


> For parsing rules (strings/booleans/durations/bytes/lists), see [`../configuration.md`](../configuration.md#parsing-rules).

## Key Invariants

- ADR-304 implemented, local acceptance including DO ticket/full baseline passed, not deployed. Explicit `ns=["*"]` permits global operational reads/GC/full metrics, not issuer CRUD (bootstrap admin only). Current issuer authority reads are uncached; create returns keys once. Durable `bundle_grants` binds namespace/SHA; native namespace uploads require explicit namespace, CF grants likewise. `:7001/metrics` is the private unauthenticated exception; `:8082/v1/control/metrics` authenticates/filters.
- Issuers live in the existing control cell, `kid=issuer/<immutable-id>`, with namespace ceiling and max TTL (<=86400 seconds), no actions/key_version. Same-name recreation gets a new ID; missing/disabled/soft-deleted records reject. Reads are authoritative, optionally uncached; caches last at most 5 seconds. Bootstrap admin JWT registry creation is not a missing-business-record bypass.
- Automatic provisioning is **rejected**: bindings must reference registered resources.
- Domain registration grants authorization (no DNS validation, ADR-133); host must be unique.
- Versions are immutable; rollback/promote are atomic switches.
- Control-plane writes also go through `capturedWrite` (RPO=0); deploy batches migrations + auto CreateApp + the version commit into one proof (ADR-271).

## Source Locations

`internal/control/`; `internal/server/control.go`; `cmd/cellhive/` (full CLI command surface).

## Test Anchors

`internal/control`, `internal/server`, `cmd/cellhive` e2e.

## Related Documentation

[`control-plane.md`](../control-plane.md), [`wrangler-compat.md`](../wrangler-compat.md), [`security.md`](../security.md)

_Last updated: 2026-09-19_
