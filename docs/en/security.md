# Security Model

## Trust Boundaries

| Component | Boundary |
|---|---|
| Edge proxy (operations) | Only performs TLS and coarse host-based routing; **does not perform business authorization**; the platform does not push edge configuration (ADR-132) |
| `user-runtime` | Entry point + execution of tenant code (cross-namespace service binding requires target ns authorization, ADR-144); tenant loaded worker has **public-Internet-only egress**; platform loader runs before tenant modules |
| `do-runtime` | Private network only; **does not hold object storage credentials** |
| `cell-agent` | `:7001` (REST, shared by Go↔Go and workerd bindings) internal data/resolution is **private-network-only**; `:8082` admin is accessed via edge + credentials (static token or OIDC JWT); data-plane paths cannot reach control-plane handlers. Internal calls are authorized by **role tokens** (`peer`/`internal`/`dispatch`, derived from `CELLHIVE_ROOT_KEY`, ADR-075/137) |
| Object storage | Long-lived credentials exist **only in cell-agent**; bundle/assets reads use short-lived scoped credentials (ADR-030) |

## Multi-Tenant Isolation (ADR-029)

1. **Network isolation (primary defense)**: In user-runtime capnp, the tenant loaded worker's `globalOutbound` = **public-Internet-only network service** (can explicitly add `private`/`local` via `CELLHIVE_TENANT_OUTBOUND`, ADR-130), by default **excluding RFC1918 / cell-agent addresses**. Tenant Workers **cannot** directly access internal `:7001/:8082`.
2. **Tokens do not enter tenant env**: The internal token exists only **between the host adapter (platform code) and cell-agent**; tenants only receive a binding facade, and cannot obtain the token/backend address.
3. **Scope declaration validation**: When the host adapter calls cell-agent, it declares `(ns, binding type/id)`; cell-agent validates it and rejects any overreach. The binding itself is uniquely bound to a specific cell by immutable props in the adapter.
4. **Defense in depth**: Per-binding signed scoped token (namespace + binding type/id), so even if isolation is bypassed, only that binding can be accessed (**ADR-074**). **Computed locally by the platform, no expiration**: revocation relies on `HasBinding` validation on every request + `SCOPE_SECRET` rotation; the **broadly privileged internal token does not enter the tenant loaded worker**, and the facade only carries a scoped token.

> Secrets: Control-plane cells store **envelope ciphertext**; the root key is outside the cell (env/KMS); it is decrypted inside cell-agent during loading and injected into `env`, with plaintext only entering the load envelope + workerd env.

## Admin Backend (ADR-036/131)

- Single admin backend (no separate Web UI, = admin API), with tenant identity (ns/app) carried by the request;
- Two authentication modes:
  - **Static operations credential** `CELLHIVE_ADMIN_TOKEN` (`x-cellhive-admin-token`);
  - **OIDC/JWT** (ADR-131): After `CELLHIVE_OIDC_JWKS_URL` is configured, the Bearer JWT must have a valid signature and **must contain `exp`** (tokens without `exp` are rejected directly, ADR-135); the `cellhive_ns` claim restricts the caller to a namespace, and platform-level operations require `*`; list/read endpoints are filtered by ns, and overreach returns 403; JWKS refresh uses singleflight + cached keys for in-flight validation (slow IdP does not block validation).
- **Never exposed to tenants or the public Internet**: reachable only via admin host + private network;
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

## Quotas and Admission (P1, ADR-035)

- Per-namespace write rate limiting; cell-agent returns `503 overloaded` when overloaded;
- Per-DO WAL limit; worker `limits` (cpu/subrequests) + V8 heap limit;
- `pressured`/`shed_cells` in consumption node leases provide backpressure;
- Queue DLQ limit.

## Internal Role Tokens (ADR-075)

- `/v1/peer/*` uses the `peer` token; `/v1/internal/*`+`/v1/control/routes`+`/v1/diagnose` use the `internal` token; user-runtime `/v1/queues|timers/dispatch` uses the `dispatch` token.
- **Single credential, no fallback**; comparisons are constant-time and fail-closed. All role credentials are derived from a **single `CELLHIVE_ROOT_KEY` via HKDF** (domain separation, ADR-137); production configures only one secret; `CELLHIVE_ADMIN_TOKEN` can optionally provide an independent override.
- **Secure defaults**: `Validate()` rejects missing/dev root keys unless `CELLHIVE_ALLOW_INSECURE_DEFAULTS=1` is set (otherwise the admin plane could be accessed with known credentials, ADR-134/137).
- **Honest note**: Without PKI, role isolation = independent secrets, not cryptographic identity; mTLS/internal CA is a stronger future option (not implemented).

## Scoped Token Scope and Delegated Issuance (ADR-074/181)

- Binding endpoints accept only `x-cellhive-scope-token`; claims `{ns,kind,name,iss?,exp_ms?}`.
- **`ns` is always exact** (isolation boundary); `kind`/`name` support segment-level globs (`*` any, `pre*` prefix), **anchored per segment** (`acme` does not match `acmex`).
- **Platform tokens** (empty `iss`): the loader signs locally with `SCOPE_SECRET`; `HasBinding` is validated on every request.
- **Delegated tokens** (non-empty `iss`): a trusted tenant platform signs with a derived issuer key (`HKDF(SCOPE_SECRET,"issuer/"+iss)`; print with `cellhive creds issuer <name>`, or mint directly with `cellhive token ... --iss <name> --ttl 5m`); an **expiry is mandatory** and authorization is by ns/scope (no registered-binding requirement).
- **Honest boundary**: the delegated key is **not narrowed per ns** (it can sign any ns) — that is the "entry manages many namespaces, may delegate all of them" design. The platform only enforces **request ns == token ns**; cross-ns / per-user isolation is the delegate's responsibility. A leaked issuer key therefore exposes every ns it may sign for; stop-loss is rotating `SCOPE_SECRET`/root (issuer versions/allowlist are not implemented).

## Implemented

- capnp `globalOutbound`/network: `workerd/user-runtime/userruntime.go` rendering + `CELLHIVE_TENANT_OUTBOUND` (ADR-130);
- Control-plane authentication middleware (role tokens + OIDC/JWT) and audit logs (including actor/on_behalf_of/request_id, ADR-131);
- Scoped token revocation: `HasBinding` (platform tokens) on every request + `SCOPE_SECRET` rotation (ADR-074); delegated tokens rely on a mandatory short `exp` + issuer-key rotation (ADR-181).

## Root Key Sources (ADR-150)

- `CELLHIVE_ROOT_KEY` (inline) → `CELLHIVE_ROOT_KEY_FILE` (mounted file, Docker `_FILE`/K8s secret volume convention) → use dev root only under `CELLHIVE_ALLOW_INSECURE_DEFAULTS`; fails closed if the file is unreadable/empty.
- **Cloud KMS (Vault/AWS KMS/Aliyun KMS) is not integrated**: the two options above are the seam for "obtaining root key bytes"; integrating KMS only requires implementing one decrypt/fetch step at this seam; the current environment has no cloud credentials, recorded as residual risk.

## mTLS / Internal CA (Decision: not for now, ADR-150)

- Current state: internal role isolation = **independently derived secrets (HMAC/constant-time comparison) + private-network isolation** (ADR-075/137); `cell-agent :7001/:8082` are not exposed externally.
- **Why mTLS is not implemented**: It requires a full CA/issuance/rotation/trust-distribution system (the current fixed cluster has no PKI), and network isolation + independent secrets are already the primary defense; introducing mTLS brings marginal benefit while significantly increasing operational surface area.
- When to re-evaluate: deployments across trust domains (cell-agent interoperating with untrusted networks), or compliance requirements for "cryptographic identity"; the interface layer (`token` header) is already abstracted, so certificate records can be added then.

_Last updated: 2026-09-17_