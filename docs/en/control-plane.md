# Control Plane

## Positioning and Form

- **Single management backend** (ADR-036): does not distinguish between administrators and tenants; tenant identity (ns/app) is passed in as request parameters; used by trusted operators; **not exposed to tenants/the public Internet**.
- **Merged into `cell-agent`** (ADR-012): single Go binary; the control plane listens on `:8082` (via an operator-provided reverse proxy/cloud LB), with separate listeners and separate authentication from the data plane `:7001` (Go↔Go and JS↔cell share :7001; no gRPC plane, ADR-136).
- secrets: `POST /v1/control/secret` (put), `GET /v1/control/secret` (get), `DELETE /v1/control/secret`, `GET /v1/control/secrets` (only lists key/update time); CLI `cellhive secret put|get|delete|list` (ADR-147).
- **Per-domain resource management (ADR-157, recommended)**: `POST|GET|DELETE /v1/<kind>/resources` (`kind=kv|d1|queue|r2|workflow|hyperdrive`), kind is fixed by the path, `scope` defaults to `<ns>/__<kind>__/<name>`; `GET /v1/<kind>/stats` provides read-only operational statistics (see `docs/bindings.md`); queue dead-letter replay is `POST /v1/queue/dead-letters/replay`. Corresponding CLI: `cellhive kv namespace|d1|r2 bucket|queue|workflows|hyperdrive create|list|delete|stats`. The following `/v1/control/*` forms are retained as **compatibility aliases** (same handler).
- Resource revocation (alias): `DELETE /v1/control/resource?namespace=&kind=&name=[&force=1]` (when a referenced worker still declares this binding, returns 409 `resource_in_use` + `referenced_by`; `force=1` forces it; **does not delete the data cell**, registers a tombstone so resolution immediately fails closed, and `resource create` re-enables it); CLI `cellhive resource delete|rm|revoke <ns> <kind> <name> [--force]` (ADR-156).
- Queue operations: `GET /v1/control/queue/status?namespace=&queue=` (`depth`/`visible`/`leased` + dead-letter queue name and depth), `POST /v1/control/queue/replay-dlq?namespace=&queue=&limit=` (re-enqueue dead letters to the main queue, at-least-once); CLI `cellhive queue status|replay-dlq` (ADR-156).
- **Metadata** is stored in a **single** control cell (`__platform__/__control__/main`, relational tables; ADR-117), with app as the `ns` column.

## Managed Objects

| Object | Description |
|---|---|
| Application / namespace | Tenant boundary |
| Worker | Name (Wrangler `name`) |
| Immutable version | A version number is assigned on each deploy; promote/rollback switches the pointer |
| Route | host pattern / path / custom domain → generates route projection |
| Binding metadata | This Worker's KV/D1/R2/Queue/Workflow/DO bindings; physical id is frozen at deploy time |
| Secret | Envelope ciphertext + external root key |
| vars | Environment variables |
| Audit | Control write operation log |

## Deployment Pipeline

```
cellhive deploy
  1. Go + esbuild local build → content-addressed bundle (SHA-256) + assets
  2. Upload bundle/assets to object storage
  3. Parse Wrangler configuration, validate and freeze binding metadata (reference existing resources)
  4. Assign immutable version number
  5. Atomic commit (WATCH/transaction): write version metadata + switch route active version
  6. user-runtime pulls the new projection within TTL and takes effect (ADR-031)
```

## Resource Lifecycle (Reject Automatic Provisioning)

Wrangler's "automatic resource creation without id" is **rejected** (wrangler-compat.md): resources must be explicitly created by the control plane. The CLI must provide:

| Command (example) | Purpose |
|---|---|
| `cellhive app create <ns>` | Create application/namespace |
| `cellhive resource create <ns> kv <name>` | Register KV namespace (= one `__kv__` cell) |
| `cellhive resource create <ns> d1 <name>` | Register D1 database cell |
| `cellhive resource create <ns> queue <name>` | Register queue cell |
| `cellhive resource create <ns> r2 <name>` | Register R2 virtual bucket |
| `cellhive resource create <ns> workflow <name>` | Register workflow definition |
| `cellhive resource create <ns> hyperdrive <name> --connection-string <url>` | Register Hyperdrive origin (envelope-encrypted, ADR-129) |
| `cellhive vectorize create <ns> <index> --dimensions N --metric cosine\|euclidean\|dot-product` | Register Vectorize index (configuration is envelope-encrypted; `resource create <ns> vectorize <index>` + body `config` is equivalent, ADR-158) |
| `cellhive resource delete <ns> <kind> <name> [--force]` | Revoke resource registration (tombstone takes effect immediately; data cell is retained; ADR-156); per-domain equivalent form `cellhive <kind> delete <ns> <name> [--force]` (ADR-157) |
| `cellhive <kind> create\|list\|stats` | Per-domain registration and operational statistics: `kv namespace` / `d1` / `r2 bucket` / `queue` / `workflows` / `hyperdrive` (`stats`/`info` output, see `docs/bindings.md`, ADR-157) |
| `cellhive queue status <ns> <queue>` / `queue replay-dlq <ns> <queue> [--limit]` | Queue backlog/dead-letter visibility and dead-letter replay (ADR-156) |
| `cellhive secret put <ns> <worker> <KEY>` | Write secret (envelope encryption) |
| `cellhive deploy / promote / rollback / releases / domain / tail` | Release and operations |

- deploy references a non-existent resource → **explicit error and suggestion of the corresponding create command**;
- Resource creation = register scope in the control plane (the cell is lazily activated on first access).

## Credentials and Token Issuance CLI (ADR-074/137/181)

| Command | Effect |
|---|---|
| `cellhive creds [role]` | Print the role credentials derived from `CELLHIVE_ROOT_KEY` (`peer`/`internal`/`dispatch`/`log`/`admin`/`scope`/`do-ticket`/`secret-key`) |
| `cellhive creds issuer <name>` | Print a delegated issuer key (`HKDF(SCOPE_SECRET,"issuer/"+name)`) to hand to a trusted entry (e.g. vwork); it holds **no root** |
| `cellhive token --ns <ns> --kind <kind> --name <name\|glob> [--iss <name>] [--ttl 5m] [--key <b64>]` | **Single issuance entry point**: platform scope key by default (no expiry); `--iss` uses the delegated issuer key and **requires `--ttl>0`**; `--key` signs with a given key (delegated self-sign, no root). `kind`/`name` accept `*`/`pre*` |

- Binding endpoints accept only `x-cellhive-scope-token`: platform tokens (empty `iss`) use the registered-binding ACL; delegated tokens (non-empty `iss`) require an expiry and authorize by ns/scope (ADR-181).
- The delegated key is **not narrowed per ns** (it can sign any ns); isolation is the delegate's responsibility. Stop-loss on leak = rotate root/`SCOPE_SECRET`. See `docs/security.md`.

## Deploy Server-Side Feature/Compatibility Interception (ADR-065)

The CLI is only for **fast feedback**; `deploy` validation treats the **server side as authoritative** (prevents bypass and different CLI versions). Validation logic is placed in a **shared library**, used by both the `deploy` endpoint and CLI preflight.

`POST /v1/control/deploy` (including bundle upload) validation:

1. **bundle**: the content-addressed object for `bundle_sha` exists; `assets` references exist.
2. **Compatibility date**: `compatibility_date` ≤ platform-supported upper bound (known **2026-09-23**, verified); if exceeded, return an error and provide the upper bound.
3. **compatibility_flags**: must be in the known set of the pinned workerd (unknown flag returns an error).
4. **Bindings**: type must be in the **support matrix**; otherwise reject (`images/browser-rendering/send_email/ai_search/dispatch_namespaces/secrets_store/containers/...`; `vectorize`/`hyperdrive` are supported, and the resource reference for vectorize = `index_name`, ADR-158).
5. **Resource references**: KV/D1/R2/Queue/… pointed to by a binding must have been registered by the control plane (**reject automatic provisioning**, ADR-014); otherwise suggest the corresponding `cellhive <kind> create`.
6. **DO lifecycle**: only `created`/`sqlite` (including `new_classes/new_sqlite_classes`); rename/delete/transfer/`script_name` are rejected.
7. **Unknown fields**: explicitly reject (field path + reason + "whether support is planned").

Error shape `{error, message, field_path?}`, stable error codes: `unsupported_binding`, `compat_date_too_new`, `unknown_flag`, `binding_unregistered`, `unknown_field`.

> Bindings allowed by `cellhive dev` (Bun+Miniflare, ADR-065) but not supported by the platform are blocked here; dev/CLI use the same validation library to warn early, avoiding "dev passes, deploy rejected".

> **Status (2026-09-15)**: implemented. Validation logic is in `internal/wranglercompat` (with additional unit tests); `POST /v1/control/deploy` returns `deploy_rejected` + `findings` (field path/error code); resource registration uses `control.HasBinding`, and bundle existence uses point lookup in object storage (`artifactStore().GetBundle`; a miss is `missing_bundle`). Tests are in `internal/server/control_test.go`.

## Control Plane Request Routing

- Reverse proxy/cloud LB load-balances admin traffic to **any cell-agent replica**;
- Non-owner replicas use the owner resolution library to **internally forward write operations to the owner of the control cell** (internal HTTP, internal token; ADR-118);
- Read operations may be locally cached; deploy/rollback ordering is guaranteed by the control cell owner (single writer);
- Control cell owner death → standard takeover flow; during the brief unavailability window, admin writes return `503`.

## Authentication and Identity

- Single operations credential (bootstrap/admin token); OIDC can be integrated later;
- Tenant identity is only a parameter; the platform has **no tenant account/session system**;
- Runtime binding isolation (ADR-029) is independent of this.

## To Be Refined

- CLI command set and parameters (detailed mapping with Wrangler input);
- Edge certificate gating is operator-managed (the platform does not distribute it, ADR-132); domain validation has been removed (ADR-133, registration implies authorization).


## Configuration (ADR-131)

| Variable | Default | Purpose |
|---|---|---|
| `CELLHIVE_BASE_DOMAIN` | empty | Built-in worker domain `<ns>-<worker>.<base>`; empty = disable built-in domain. On the edge side, configure wildcard and forwarding for `*.<base>` yourself (the platform does not distribute edge configuration, ADR-132) |
| `CELLHIVE_AUTO_CREATE_APP` | `1` | Idempotently create missing ns during deploy (one-command app run) |
| `CELLHIVE_ADMIN_JWT` (CLI) | empty | Use OIDC/JWT (Bearer) instead of a static admin token; the JWT's `cellhive_ns` determines manageable ns, `*` = platform-level |

CLI: `cellhive domain add|verify|ls|rm`; `route add` requires the host to be registered and (for custom domains) verified.

## Schema v2 (control plane cell: `__platform__/__control__/main`)

> Design finalized on 2026-09-17 (ADR-131; domain validation has been removed per ADR-133). Behavioral implementation (soft delete/purge/loader changes) has landed;
> the table structure in this section is authoritative. Data cells (`<ns>/<class>/<id>`) are not part of this schema.

### Tenants and Versions
```sql
CREATE TABLE IF NOT EXISTS apps (
  ns         TEXT PRIMARY KEY,
  created_ms INTEGER NOT NULL,
  deleted_ms INTEGER NOT NULL DEFAULT 0            -- 软删（purge 异步、列表过滤）
);
CREATE TABLE IF NOT EXISTS workers (
  ns         TEXT NOT NULL,
  name       TEXT NOT NULL,
  active     INTEGER NOT NULL DEFAULT 0,
  previous   INTEGER NOT NULL DEFAULT 0,
  storage_id TEXT NOT NULL DEFAULT '',             -- DO 存储身份（跨版本稳定）
  host_label TEXT NOT NULL DEFAULT '',             -- 内置域 label（唯一）
  created_ms INTEGER NOT NULL DEFAULT 0,
  deleted_ms INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (ns, name)
);
CREATE UNIQUE INDEX IF NOT EXISTS workers_host_label ON workers(host_label) WHERE host_label <> '';

CREATE TABLE IF NOT EXISTS versions (             -- 不可变快照（JSON 为真值）
  ns TEXT NOT NULL, worker TEXT NOT NULL, number INTEGER NOT NULL,
  bundle_sha TEXT NOT NULL, assets_sha TEXT NOT NULL DEFAULT '',
  storage_id TEXT NOT NULL DEFAULT '', session_policy TEXT NOT NULL DEFAULT '',
  bindings TEXT NOT NULL DEFAULT '[]', vars TEXT NOT NULL DEFAULT '{}',
  consumers TEXT NOT NULL DEFAULT '[]', crons TEXT NOT NULL DEFAULT '[]', assets TEXT,
  compat_date TEXT NOT NULL DEFAULT '', compat_flags TEXT NOT NULL DEFAULT '[]',
  created_ms INTEGER NOT NULL, actor TEXT NOT NULL DEFAULT '',
  PRIMARY KEY (ns, worker, number)
);
CREATE INDEX IF NOT EXISTS versions_assets ON versions(assets_sha) WHERE assets_sha <> '';

-- 规范化绑定（派生自 versions.bindings）：HasBinding / 服务 pin / GC 走索引
CREATE TABLE IF NOT EXISTS bindings (
  ns TEXT NOT NULL, worker TEXT NOT NULL, number INTEGER NOT NULL, name TEXT NOT NULL,
  type TEXT NOT NULL, id TEXT NOT NULL DEFAULT '', class_name TEXT NOT NULL DEFAULT '',
  entrypoint TEXT NOT NULL DEFAULT '', pin_sha TEXT NOT NULL DEFAULT '',
  PRIMARY KEY (ns, worker, number, name)
);
CREATE INDEX IF NOT EXISTS bindings_lookup ON bindings(ns, type, name);
CREATE INDEX IF NOT EXISTS bindings_pin    ON bindings(ns, id, pin_sha);

CREATE TABLE IF NOT EXISTS version_bundle_refs (
  ns TEXT NOT NULL, worker TEXT NOT NULL, number INTEGER NOT NULL, sha TEXT NOT NULL,
  PRIMARY KEY (ns, worker, number, sha)
);
CREATE INDEX IF NOT EXISTS bundle_refs_sha ON version_bundle_refs(sha);
```
### Domains and Routes (Traditional `(host,path)` + CDN-Style Domain Verification)
```sql
CREATE TABLE IF NOT EXISTS hosts (                 -- Domain ownership (registration authorizes, ADR-133)
  host       TEXT PRIMARY KEY,                     -- normalized (lowercase/remove port/remove trailing dot)
  ns         TEXT NOT NULL,
  worker     TEXT NOT NULL DEFAULT '',             -- builtin: target worker; custom: ''
  kind       TEXT NOT NULL DEFAULT 'custom',       -- builtin|custom
  created_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS hosts_ns ON hosts(ns);

CREATE TABLE IF NOT EXISTS routes (                -- traditional (host,path) → worker (path is the mount point)
  host TEXT NOT NULL, path TEXT NOT NULL DEFAULT '', ns TEXT NOT NULL, worker TEXT NOT NULL,
  PRIMARY KEY (host, path)
);
CREATE INDEX IF NOT EXISTS routes_ns        ON routes(ns, host, path);
CREATE INDEX IF NOT EXISTS routes_ns_worker ON routes(ns, worker);
```
Rules:
1. **Built-in domains** `<ns>-<worker>.cell.internal` (`host_label` is unique) are generated by the system during deploy/promote:
   `hosts(kind='builtin')` + `routes(host, '', ns, worker)`; users cannot write them.
2. **Custom domains** take effect upon registration (ADR-133: no DNS verification; the credential that can manage the ns is the authority for its domains; already registered by another ns → 409);
   already `verified` by another ns → 409; only `pending` → coexistence is allowed, and the first to pass verification wins.
3. Before writing `routes`, the corresponding `hosts` row must exist and `hosts.ns = request ns` (use
   `ON CONFLICT(host,path) DO UPDATE … WHERE ns=excluded.ns` as a concurrency fallback).
4. When deleting a worker, clean up the corresponding rows via `routes_ns_worker` / `hosts(ns)`.
5. **Prefix matching follows path segment boundaries**: `/api` matches `/api` and `/api/x`, but not `/apix`; `path=''` or `'/'` means the entire host.
6. **Route = mount point (always stripped)**: after the loader selects a route, it strips the route path prefix from the request path **once** (query/Host unchanged). Both assets and workers see the stripped path; an empty result after stripping is treated as `/`. For example, after `route add app.test /api api`, the worker receives `/users` for `/api/users`.

### Resources / Secrets / DO Classes / Locks / GC / purge
```sql
CREATE TABLE IF NOT EXISTS resources (
  ns TEXT NOT NULL, kind TEXT NOT NULL, name TEXT NOT NULL,
  scope TEXT NOT NULL DEFAULT '', created_ms INTEGER NOT NULL,
  cfg_dek BLOB, cfg_nonce BLOB, cfg_ct BLOB,       -- sealed configuration (hyperdrive origin, etc.)
  PRIMARY KEY (ns, kind, name)
);
CREATE INDEX IF NOT EXISTS resources_kind ON resources(kind, ns, name);

CREATE TABLE IF NOT EXISTS secrets (
  ns TEXT NOT NULL, worker TEXT NOT NULL, key TEXT NOT NULL,
  wrapped_dek BLOB, value_nonce BLOB, ciphertext BLOB, updated_ms INTEGER NOT NULL,
  PRIMARY KEY (ns, worker, key)
);

CREATE TABLE IF NOT EXISTS do_classes (
  ns TEXT NOT NULL, worker TEXT NOT NULL, code_class TEXT NOT NULL,
  storage_class TEXT NOT NULL DEFAULT '', deleted INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (ns, worker, code_class)
);

CREATE TABLE IF NOT EXISTS delete_locks (
  ns TEXT NOT NULL, worker TEXT NOT NULL, actor TEXT NOT NULL DEFAULT '',
  at_ms INTEGER NOT NULL, exp_ms INTEGER NOT NULL, PRIMARY KEY (ns, worker)
);
CREATE TABLE IF NOT EXISTS deploy_idempotency (
  ns TEXT NOT NULL, worker TEXT NOT NULL, key TEXT NOT NULL, version INTEGER NOT NULL,
  created_ms INTEGER NOT NULL DEFAULT 0, PRIMARY KEY (ns, worker, key)
);
CREATE TABLE IF NOT EXISTS gc_marks (
  kind TEXT NOT NULL, id TEXT NOT NULL, marked_ms INTEGER NOT NULL, PRIMARY KEY (kind, id)
);

CREATE TABLE IF NOT EXISTS purges (                -- deletion jobs (idempotent, retryable)
  ns TEXT NOT NULL, worker TEXT NOT NULL DEFAULT '',   -- '' = entire ns
  state TEXT NOT NULL DEFAULT 'pending',               -- pending|running|done|failed
  requested_ms INTEGER NOT NULL, updated_ms INTEGER NOT NULL,
  attempts INTEGER NOT NULL DEFAULT 0, last_error TEXT NOT NULL DEFAULT '',
  PRIMARY KEY (ns, worker)
);
```

### Audit and rev
```sql
CREATE TABLE IF NOT EXISTS audit (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ns TEXT NOT NULL, at_ms INTEGER NOT NULL,
  actor TEXT NOT NULL DEFAULT '',          -- JWT sub / static-admin
  actor_kind TEXT NOT NULL DEFAULT '',     -- user|service|static|system
  on_behalf_of TEXT NOT NULL DEFAULT '',
  request_id TEXT NOT NULL DEFAULT '',
  action TEXT NOT NULL, target TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS audit_ns    ON audit(ns, at_ms);
CREATE INDEX IF NOT EXISTS audit_actor ON audit(actor, at_ms);

CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT NOT NULL);   -- 'rev'
```

### Migration
Implemented: `schema` → `migrateColumns` (column-by-column `ALTER TABLE … ADD COLUMN`, reading `PRAGMA table_info` to skip existing columns) → `schemaV2` (new tables and indexes).
Therefore, **existing cells can be upgraded in place** without rebuilding; `bindings` are written by deploy, and binding queries from older versions fall back to the `versions.bindings` JSON.

### Split Preparation (Out of Scope for This Iteration)
When splitting control, create a separate `__platform__/__dispatch__/main` (owned by cell-agent) to carry the dispatch index
(queue/cron/workflow/DO class → worker+version+bundle), so that cell-agent does not read control-plane tables;
it is **not created** for now.

_Last updated: 2026-09-17_