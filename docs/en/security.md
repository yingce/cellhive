# Security Model

> ADR-304 **stateless issuer amendment is in implementation and acceptance, not deployed**. Earlier stateful Go/JS/CLI/vwork, DO ticket and runtime baseline results do not validate this amendment. No legacy credential compatibility is promised.

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
- Namespace JWTs authorize all scoped business operations including deployment, CF data, secrets, logs and monitoring, without actions or metrics-reader. `issuer/*` with explicit `ns=["*"]` permits all global business operations, including operational reads, GC and full admin `/metrics`; exact/prefix grants do not. `token/admin` retains global bootstrap authority; issuer CRUD is removed.
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

## JWT and Stateless Issuers (ADR-304 Amendment, In Progress)

- Required header: `alg=HS256, typ=JWT, kid=<key-id>`. Common claims: `ver=1,type,sub,aud,iat,exp`; default audience `cellhive`, Unix seconds. Reject unknown algorithms/types, ambiguous headers, invalid times, audience or signatures.
- A `namespace` token has a nonempty `ns` string array, supporting exact names, trailing `*` prefixes and standalone `*` only. Issuer ceiling, token scope and request scope narrow successively; exceeding the ceiling rejects the whole token rather than clipping it. Lists, logs, metrics and cross-resource references obey the same ceiling.
- A `binding` token has an exact `ns` string and exact `kind/name`; it cannot deploy. Pinned `worker/version/resource` fields are platform-only. Namespace JWT binding calls still name the target explicitly and perform resource-registry resolution and resource fencing.
- One vwork issuer key may sign the `vwork-*` prefix, a single App namespace, or restricted binding JWTs; two keys are not required. No `actions` or `key_version`. Signing keys never travel with requests; possessing a JWT does not permit minting another.
- `kid=issuer/<pattern>` carries its namespace ceiling: exact, trailing `*` prefix or `*`. Fixed `0 < exp-iat <= 86400` seconds; reject expired or over-ceiling JWTs in full. Reject legacy issuer IDs of exactly 32 hexadecimal characters, without fallback.
- No issuer CRUD, active record, owner lookup, authorization cache or persistence check; the namespaces registry does not authorize issuers either. Historical issuer table data may remain inert without destructive migration. Resource registries and lifecycle fences remain.
- No instant per-issuer revocation: issued JWTs expire at exp; a leaked key can keep minting, requiring root rotation and redistribution of derived keys for containment.
- JWT key = `HKDF-SHA256(decoded_root_key, nil, "cellhive/jwt/v1/" + kid, 32)`. Retain the old `ScopeSecret` derivation for Hyperdrive and other non-HTTP credentials only, never JWTs.
- Bootstrap signs short-lived global admin JWTs with root-derived `token/admin`, without issuer records; keys are not Bearer credentials. `cellhive creds issuer 'vwork-*'` locally derives a raw 32-byte key encoded as unpadded base64url; trusted signers use `cellhive token --issuer 'vwork-*' --key "$key" --ns vwork-42 --ttl 5m`.
- Owner forwarding retains original authorization and revalidates statelessly at the destination. CF bridges cannot replace it with unrestricted credentials. Issuer authentication does not look up owners.
- Assets/Tail sessions use purpose-separated keys, statelessly bind the parent issuer pattern, target namespace and parent expiry, and cannot widen authority or outlive parent exp. Tail checks expiry every 500ms, not revoked records; assets recheck after reading upload bodies. Stock Wrangler Tail query tokens remain a protocol exception, not ordinary header fallback.
- **Explicit ADR-178 exception**: private `:7001/metrics` retains unauthenticated full-platform pull, accessible only to trusted collectors through private networking and firewall/NetworkPolicy controls, never public ingress or tenant egress. No JWT refresher, proxy or extra service is needed. Business `/v1/control/metrics` requires JWT authentication and namespace filtering; platform or unattributable series must not leak to restricted tenants. Full `/metrics` on the admin listener requires admin or an explicit `*` namespace JWT. Long-running authenticated API clients still refresh JWTs; the private metrics pull exception does not.

## Acceptance Status

The ADR-304 stateless amendment is in implementation and acceptance; see [testing](testing.md) and [known issues](known-issues.md). Earlier stateful tests do not validate it; not deployed. DO grants still revalidate parents statelessly and sign execution metadata; tickets last at most30 seconds and never outlive parent expiry, without instant cancellation. Secret encryption and ADR-302 pinned KV authorization are unchanged.

## Root Key Sources (ADR-150)

- `CELLHIVE_ROOT_KEY` (inline) → `CELLHIVE_ROOT_KEY_FILE` (mounted file, Docker `_FILE`/K8s secret volume convention) → use dev root only under `CELLHIVE_ALLOW_INSECURE_DEFAULTS`; fails closed if the file is unreadable/empty.
- **Cloud KMS (Vault/AWS KMS/Aliyun KMS) is not integrated**: the two options above are the seam for "obtaining root key bytes"; integrating KMS only requires implementing one decrypt/fetch step at this seam; the current environment has no cloud credentials, recorded as residual risk.

## mTLS / Internal CA (Decision: not for now, ADR-150)

- Internal roles rely on independent purpose keys and private networking; ADR-304 migrates transport credentials to short-lived JWTs. `:7001` stays private; `:8082` is available only through a controlled admin entry.
- **Why mTLS is not implemented**: It requires a full CA/issuance/rotation/trust-distribution system (the current fixed cluster has no PKI), and network isolation + independent secrets are already the primary defense; introducing mTLS brings marginal benefit while significantly increasing operational surface area.
- Re-evaluate for cross-trust-domain deployments or compliance requiring certificate identity; transport certificate validation can supplement Bearer JWTs.

_Last updated: 2026-09-25_
