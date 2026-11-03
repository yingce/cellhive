# Security Model

> ADR-304 **implemented, local acceptance passed, not deployed**. Go/JS/CLI/vwork, DO ticket hardening and the full runtime baseline passed local acceptance. Production cutover is not complete; legacy implementation is not a compatibility promise.

## Trust Boundaries

| Component | Boundary |
|---|---|
| Edge proxy (operations) | Only performs TLS and coarse host-based routing; **does not perform business authorization**; the platform does not push edge configuration (ADR-132) |
| `user-runtime` | Entry point + execution of tenant code (cross-namespace service binding requires target ns authorization, ADR-144); tenant loaded worker has **public-Internet-only egress**; platform loader runs before tenant modules |
| `do-runtime` | Private network only; **does not hold object storage credentials** |
| `cell-agent` | `:7001` internal data/resolution is private; `:8082` admin/CF API uses Bearer HS256 JWT. CF routes are not mounted on `:7001` or `:8083`; native internal control mirrors admit only designated internal identities. Internal JWT keys are purpose-separated (ADR-304) |
| Object storage | Long-lived credentials exist **only in cell-agent**; bundle/assets reads use short-lived scoped credentials (ADR-030) |

## Multi-Tenant Isolation (ADR-029)

1. **Network isolation (primary defense)**: In user-runtime capnp, the tenant loaded worker's `globalOutbound` = **public-Internet-only network service** (can explicitly add `private`/`local` via `CELLHIVE_TENANT_OUTBOUND`, ADR-130), by default **excluding RFC1918 / cell-agent addresses**. Tenant Workers **cannot** directly access internal `:7001/:8082`.
2. **Tokens do not enter tenant env**: The internal token exists only **between the host adapter (platform code) and cell-agent**; tenants only receive a binding facade, and cannot obtain the token/backend address.
3. **Scope declaration validation**: When the host adapter calls cell-agent, it declares `(ns, binding type/id)`; cell-agent validates it and rejects any overreach. The binding itself is uniquely bound to a specific cell by immutable props in the adapter.
4. **Defense in depth**: Short-lived binding JWTs authorize exact namespace/kind/name. Platform-pinned fields retain ADR-302 version authorization and resource fences, not current-active remapping. Trusted hosts refresh tokens; broad internal tokens and signing keys never enter tenant loaded workers.

> Secrets: Control-plane cells store **envelope ciphertext**, and the root key stays outside the cell (env/KMS). When a runtime execution view is read, cell-agent decrypts and injects secrets into Worker/DO `env`; plaintext enters only that load envelope and workerd env, never the bundle, rendered configuration, arguments, or logs. A same-named secret overrides a var.

## Admin Backend (ADR-036/131)

- Single admin backend (no separate Web UI, = admin API), with tenant identity (ns/app) carried by the request;
- Ordinary HTTP accepts only `Authorization: Bearer <JWT>`, fixed HS256. No static credentials, legacy admin/scope headers or OIDC/JWKS fallback. Vwork's own login is unchanged.
- Namespace JWTs authorize all business operations in scope, including deployment, CF resource data, secrets, logs and namespace-filtered monitoring. No actions or metrics-reader role. When permitted by the issuer ceiling, a token explicitly granting `ns=["*"]` also permits full-platform operational reads and GC, including full `/metrics` on the admin listener; exact or prefix namespace grants do not. **Issuer registry create, read, list, update and delete remain bootstrap-admin-JWT-only, including for wildcard issuers**.
- Operators restrict the admin host's exposure; TLS/JWT business access is separate from private internal listeners;
- All control write operations keep **audit logs** (recording actor/sub).

## Network Policy

| Source → Target | Allowed |
|---|---|
| Public Internet → Traefik → user-runtime:8081 | ✅ |
| Public Internet/CLI → Traefik → cell-agent:8082 | ✅ (after authentication) |
| user-runtime → cell-agent:7001 | ✅ private network |
| cell-agent → user-runtime:8088 | ✅ private network + **dispatch role token** (ADR-075, defaults to 401 if missing) |
| cell-agent ↔ cell-agent:7001 | ✅ private network |
| do-runtime → cell-agent:7001 | ✅ private network |
| Any → cell-agent:7001 (public Internet) | ❌ |
| Tenant Worker → private network (any) | ❌ |

## Quotas and Admission (ADR-035)

- Per-namespace write-rate limiting is implemented; cell-agent returns `503 overloaded` when overloaded;
- Per-DO WAL limits, consumption-node `pressured`/`shed_cells` backpressure, and queue DLQ limits are implemented;
- Per-worker CPU/subrequest and V8 heap enforcement are not implemented; only deploy-time WorkerCode/env budgets exist today.

## Internal Role Tokens (ADR-075)

- `/v1/peer/*` uses the `peer` token; `/v1/internal/*`+`/v1/control/routes`+`/v1/diagnose` use the `internal` token; user-runtime `/v1/queues|timers/dispatch` uses the `dispatch` token.
- **Bearer JWT only, no static fallback**. `internal`/`peer`/`dispatch`/`log` have separate derived keys; verified kid constrains purpose without a redundant role claim. Give callers only required JWTs; trusted local refresh/signing components receive only required purpose keys, not a fleet-wide copy of root.
- **Secure defaults**: `Validate()` rejects missing/dev root keys unless `CELLHIVE_ALLOW_INSECURE_DEFAULTS=1` is set (otherwise the admin plane could be accessed with known credentials, ADR-134/137).
- **Honest note**: Without PKI, role isolation = independent secrets, not cryptographic identity; mTLS/internal CA is a stronger future option (not implemented).

## JWT and Persisted Issuers (ADR-304, In Progress)

- Required header: `alg=HS256, typ=JWT, kid=<key-id>`. Common claims: `ver=1,type,sub,aud,iat,exp`; default audience `cellhive`, Unix seconds. Reject unknown algorithms/types, ambiguous headers, invalid times, audience or signatures.
- A `namespace` token has a nonempty `ns` string array, supporting exact names, trailing `*` prefixes and standalone `*` only. Issuer ceiling, token scope and request scope narrow successively; exceeding the ceiling rejects the whole token rather than clipping it. Lists, logs, metrics and cross-resource references obey the same ceiling.
- A `binding` token has an exact `ns` string and exact `kind/name`; it cannot deploy or manage issuers. Pinned `worker/version/resource` fields are platform-only. Namespace JWT binding calls still name the target explicitly and perform registry resolution and resource fencing.
- One vwork issuer key may sign the `vwork-*` prefix, a single App namespace, or restricted binding JWTs; two keys are not required. No `actions` or `key_version`. Signing keys never travel with requests; possessing a JWT does not permit minting another.
- Issuer metadata lives in `__platform__/__control__/main` SQLite: random nonreusable ID, display name, namespace ceiling, enabled, maximum TTL (`0 < TTL <= 86400` seconds), creation/update/deletion times, never plaintext keys. `kid=issuer/<immutable-id>`; same-name recreation gets a fresh ID and cannot revive old tokens.
- Verification requires an authoritative active record (initial implementation may be uncached). Missing, disabled, soft-deleted, over-TTL or out-of-scope issuers fail closed. Deletion is irreversible; create/disable/delete are captured control-cell writes, acknowledged only after durability confirmation. Non-owner local SQLite is not authority.
- If positive caching is used, authorization lasts at most 5 seconds from the start of the authoritative read. Errors never extend it; expired lookup failures reject. Local management writes invalidate cache and prevent stale in-flight refill. Do not claim instant global revocation.
- JWT key = `HKDF-SHA256(decoded_root_key, nil, "cellhive/jwt/v1/" + kid, 32)`. Retain the old `ScopeSecret` derivation for Hyperdrive and other non-HTTP credentials only, never JWTs.
- Bootstrap signs a short-lived admin JWT with root-derived `token/admin` for registry creation/recovery; the key itself is not a Bearer credential. This system identity is not a deletable business issuer; **missing business issuer records never have a bypass**.
- Owner forwarding retains original authorization and revalidates at the destination. CF bridges cannot substitute unrestricted credentials and lose namespace ceilings. Authoritative issuer reads use separate internal authentication to avoid recursion.
- Assets/Tail sessions use purpose-separated keys, cannot expand parent authority and inherit issuer revocation constraints. Stock Wrangler Tail's real session query token remains a protocol exception, not an ordinary user-header fallback.
- **Explicit ADR-178 exception**: private `:7001/metrics` retains unauthenticated full-platform pull, accessible only to trusted collectors through private networking and firewall/NetworkPolicy controls, never public ingress or tenant egress. No JWT refresher, proxy or extra service is needed. Business `/v1/control/metrics` requires JWT authentication and namespace filtering; platform or unattributable series must not leak to restricted tenants. Full `/metrics` on the admin listener requires admin or an explicit `*` namespace JWT. Long-running authenticated API clients still refresh JWTs; the private metrics pull exception does not.

## Acceptance Status

ADR-304 local acceptance passed; see [testing](testing.md) and [known issues](known-issues.md) for evidence. Not deployed; production cutover is not complete. New DO grants recheck the parent and execution metadata is signed; already-issued tickets have an at-most-30-second revocation bound, not instant global revocation.

## Root Key Sources (ADR-150)

- `CELLHIVE_ROOT_KEY` (inline) → `CELLHIVE_ROOT_KEY_FILE` (mounted file, Docker `_FILE`/K8s secret volume convention) → use dev root only under `CELLHIVE_ALLOW_INSECURE_DEFAULTS`; fails closed if the file is unreadable/empty.
- **Cloud KMS (Vault/AWS KMS/Aliyun KMS) is not integrated**: the two options above are the seam for "obtaining root key bytes"; integrating KMS only requires implementing one decrypt/fetch step at this seam; the current environment has no cloud credentials, recorded as residual risk.

## mTLS / Internal CA (Decision: not for now, ADR-150)

- Internal roles rely on independent purpose keys and private networking; ADR-304 migrates transport credentials to short-lived JWTs. `:7001` stays private; `:8082` is available only through a controlled admin entry.
- **Why mTLS is not implemented**: It requires a full CA/issuance/rotation/trust-distribution system (the current fixed cluster has no PKI), and network isolation + independent secrets are already the primary defense; introducing mTLS brings marginal benefit while significantly increasing operational surface area.
- Re-evaluate for cross-trust-domain deployments or compliance requiring certificate identity; transport certificate validation can supplement Bearer JWTs.

_Last updated: 2026-09-25_
