# Module: Control Plane and Release

> ADR-304 **stateless amendment is in implementation and acceptance, not deployed**; earlier stateful tests do not validate it. Bearer HS256 JWT, audience `cellhive`, no old headers/OIDC. Namespace JWTs grant all scoped business; `issuer/*` with explicit `ns=["*"]` grants global business/operations/GC/metrics. `token/admin` retains global bootstrap; issuer CRUD is removed. CF only on `:8082`.

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

- Durable `bundle_grants` binds namespace/SHA; native uploads require explicit namespace, CF grants likewise. `:7001/metrics` is the private unauthenticated exception; `:8082/v1/control/metrics` authenticates/filters. ADR-302 pinned KV, secret encryption and DO tickets bounded by30 seconds and parent exp remain unchanged.
- Stateless `kid=issuer/<pattern>` carries exact/prefix `*`/`*` ceilings, fixed maximum TTL86400 seconds. No actions/key_version, issuer/namespaces-registry authorization, owner/persistence checks. Exactly32-hex legacy IDs reject without fallback. Old table data may remain inert without destructive migration; no instant issuer revocation, only exp/root rotation. Leaked keys require root rotation.
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
