# Module: CLI, Packaging, and wrangler Compatibility

`cellhive` CLI (deployment/operations/resources/queues/vectors/workflows), Go+esbuild packaging, and `wrangler` configuration compatibility; `cli/` is a Bun+Miniflare **dev-only** tool.

> The authoritative sources for configuration are [`../configuration.md`](../configuration.md) and the code; the table below is the subset relevant to this module.
## Key Interfaces

CLI command surface (`deploy --config`, `bundle build/put`, `resource`, `queue`, `vectorize`, `domain/route`, `creds`, `token`, `tail`…); packaging via `internal/bundler` (esbuild).

**Credentials and scoped-token issuance (ADR-181)**:
- `cellhive creds [role]`: print the 8 root-derived role credentials.
- `cellhive creds issuer <name>`: print a delegated issuer key (`HKDF(SCOPE_SECRET,"issuer/"+name)`) to hand to a trusted entry (holds no root).
- `cellhive token --ns <ns> --kind <kind> --name <name|glob> [--iss <name>] [--ttl 5m] [--key <b64>]`: single issuance entry point (platform key / delegated issuer key / a given key); delegated tokens require `--ttl>0`; `kind`/`name` accept `*`/`pre*`.

## Configuration (Environment Variables)

| Variable | Default | Purpose |
|---|---|---|
| `CELLHIVE_ADMIN_JWT` | empty | If set, switches to `Bearer` (ns-level authorization) |
| `CELLHIVE_ADMIN_URL` | `http://127.0.0.1:8082` | admin endpoint |
| `CELLHIVE_BIN` | automatic | packager used by dev `--strict-build` |
| `CELLHIVE_CONTROL_URL` | `http://127.0.0.1:7001` | internal endpoint |
| `CELLHIVE_ESBUILD` | auto-discovered | esbuild path |
| `CELLHIVE_PERF_GATE` | empty | Set by `make perf-test` to enable performance gate assertions |
| `CELLHIVE_ROOT_KEY` | required | Derives admin (`CELLHIVE_ADMIN_TOKEN` can override) and internal tokens |
| `CELLHIVE_S3_TEST_ACCESS` / `_SECRET` / `_BUCKET` | `minioadmin`/`minioadmin`/`cellhive` | S3 integration test credentials |
| `CELLHIVE_S3_TEST_ENDPOINT` | empty | Points to an existing S3; if empty, `make s3-test` attempts to start MinIO, and skips if docker is unavailable |
| `CELLHIVE_WORKERD` | auto-discovered | workerd path (prefers pinned `1.20260916.1`) |
| `TEST_BYTES` | — | Used by `internal/config` unit tests |


> For parsing rules (strings/booleans/durations/bytes/lists), see [`../configuration.md`](../configuration.md#parsing-rules).

## Key Invariants

- Production packaging is **Go + esbuild, no Node**.
- The dev CLI is not part of production artifacts, and Miniflare/workerd must stay in sync with the pinned version.
- The current exact dev pair is Miniflare `5.20260916.0-alpha` with workerd override `1.20260916.1`.
- Unknown wrangler fields/flags or overly new compatibility dates are **rejected at deployment time**. Go and Bun both consume generated authority from `internal/workerdcompat/manifest.json` (rendered as `workerd-compat.generated.ts` for Bun), rather than maintaining a second handwritten flag table.
- Final WorkerCode 64 MiB and env 1016 KiB budgets are enforced before packaging/dynamic loading; an over-budget release creates no version and does not move the active pointer.

## Source Locations

`cmd/cellhive/`, `cmd/workerd-compat-gen/`; `internal/{bundler,wrangler,wranglercompat,workerdbin,workerdcompat,workerbudget}`; `cli/`; `scripts/workerd-compat-probe.sh`.

## Test Anchors

`make cli-test`, `internal/bundler`, `internal/wrangler`, `internal/wranglercompat`; `cmd/cellhive TestMintScopeToken` (issuance), `internal/scopedtoken` (glob/issuer), `internal/server TestScopeAuthDelegated`.

## Related Documentation

[`wrangler-compat.md`](../wrangler-compat.md), [`dev-mode.md`](../dev-mode.md), [`security.md`](../security.md) (scoped-token scope and delegation), [`control-plane.md`](../control-plane.md) (issuance CLI)

_Last updated: 2026-09-20_
