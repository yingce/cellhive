# Routing and Version Resolution

## Entry Point

There is no standalone gateway. The north-south entry point = **ops-provided edge proxy (TLS + coarse host-based routing)** + **user-runtime loader (platform code)**.

```
Public Internet ──TLS──▶ Edge proxy (Traefik/nginx/cloud LB)─┬─▶ user-runtime :8081 (tenant)
                                                             └─▶ cell-agent :8082 (admin/CLI)
```

- Edge proxy: TLS termination and coarse host → backend routing; **the platform does not push edge configuration** (ADR-132). Ops configures it statically (including the built-in domain wildcard `<base>`);
- user-runtime loader: **Worker-level routing + version resolution + header scrubbing + request-id + reserved namespace interception** (before tenant modules execute), and performs host gating (unregistered hosts return 404).

## Route Projection

**Route projection** = the mapping maintained by the control plane:

```
host / path  →  (namespace, worker)  →  active version
```

- Stored in the control cell (sharded by app); exposed for reads by `cell-agent` (`:8082` admin plane; internal reads may also go through `:7001`);
- **Distribution method: pure pull + TTL** (ADR-031, no push); host cache uses negative caching/singleflight/TTL revalidation/failure backoff (ADR-115). Revocation relies on polling `/v1/control/routes` with ETag/revision, and **revocation takes effect in <5s** (ADR-116);
- During the stale window, individual replicas may still serve the **old immutable version**, and requests can still complete normally (no correctness issue).

## Request Path (normal fetch)

```
Client → Traefik(TLS, host) → user-runtime :8081
  loader:
    1. host/path → (ns, worker)          # route projection
    2. (ns, worker) → active version      # route projection
    3. Intercept reserved namespaces (__system__/__platform__, etc.)
    4. Strip/overwrite trusted headers forged by the client; inject request-id
    5. Construct worker id = <ns>:<worker>:<version>
    6. workerLoader loads the immutable bundle by id → execute fetch(env, ctx)
```

## Three Sources of Version Resolution (do not conflate)

| Resolution | Answers | Performed by |
|---|---|---|
| **Public route version** | Which code should this host/path use? | user-runtime loader (reads projection) |
| **Binding frozen version** | Which target version does the service binding point to? | user-runtime (reads the frozen version from caller metadata) |
| **Dispatch version** | Which version should cron/queue/workflow use? | cell-agent (active or frozen) |

## Host Forms

| Type | Example | Backend |
|---|---|---|
| Built-in worker domain | `<ns>-<worker>.<CELLHIVE_BASE_DOMAIN>` | user-runtime |
| Custom domain / `routes` | Any host (+ optional path prefix) | user-runtime |
| admin/CLI | admin host (or separate domain name) | cell-agent :8082 |

- **Built-in domains**: During `deploy`/`promote`, the system creates `hosts(kind='builtin')` + `routes(host,'',ns,worker)`; the `<base>` wildcard and certificates are configured by the edge/ops side (ADR-131/132/133);
- **Custom domain = registration is authorization** (ADR-133): After `cellhive domain add <host> <ns> <worker>`, it **takes effect immediately**, with no DNS/CNAME/TXT validation; if host ownership conflicts, already-registered (verified) hosts return 409, and claiming the built-in domain space is rejected;
- **Mount prefixes are always stripped**: When a route includes a placeholder `(host,path)`, the path seen by the worker = the request path with the mount prefix removed; `path=''`/`'/'` means the entire host; matching is by **segment boundary** (`/api` does not match `/apix`, ADR-131);
- `workers_dev` → namespace path (see [`wrangler-compat.md`](wrangler-compat.md)); `preview_urls` are not supported.

## Reserved Namespaces

Platform-internal namespaces (such as `__system__`/`__platform__`) are **not externally routable**; the loader intercepts and rejects them during the resolution phase.

## Implemented

- Projection data format and cache: `internal/control` (`hosts/routes` tables) + `internal/server` (`/v1/control/routes`, ETag/revision) + loader cache (ADR-115/116);
- Pull endpoints and authentication: internal token (`:7001`/`:8082`);
- Custom domain binding: `cellhive domain add|ls|rm` → `POST/DELETE /v1/control/domain`, `GET /v1/control/domains` (ADR-131/133);
- Edge: **no push-based configuration** (ADR-132); statically configured by ops.

_Last updated: 2026-09-17_