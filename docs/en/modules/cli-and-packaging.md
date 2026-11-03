# Module: CLI, Packaging, and wrangler Compatibility

`cellhive` CLI (deployment/operations/resources/queues/vectors/workflows), Go+esbuild packaging, and `wrangler` configuration compatibility; `cli/` is a Bun+Miniflare **dev-only** tool.

> The authoritative sources for configuration are [`../configuration.md`](../configuration.md) and the code; the table below is the subset relevant to this module.
## Key Interfaces

CLI command surface (`deploy --config`, `bundle build/put`, `resource`, `d1`/`kv`/`r2` data commands, `versions`/`deployments`, `queues consumer|purge`, `workflows trigger|instances`, `vectorize`, `domain/route`, `creds`, `token`, `tail`, and others); packaging uses `internal/bundler` (esbuild). `cellhive wrangler <command>` translates supported Wrangler-style arguments to these native commands; it is not the official Wrangler API client.

Stock Wrangler 4.133.0 can reach the compatible admin API with `CLOUDFLARE_API_BASE_URL=<CellHive admin URL>/client/v4`, `CLOUDFLARE_API_TOKEN=<admin token>`, and `CLOUDFLARE_ACCOUNT_ID=<namespace>`. Resources must be created explicitly before deploy. See the [Wrangler guide](../wrangler-compat.md) for tested commands and remaining boundaries. The Hyperdrive runtime binding works, but its official Wrangler management API is unavailable.

**Credentials and scoped-token issuance (ADR-181)**:
- `cellhive creds [role]`: print the 8 root-derived role credentials.
- ADR-304 stateless amendment is in implementation and acceptance; old tests do not validate it. `cellhive creds issuer 'vwork-*'` locally derives a raw32-byte unpadded base64url key using `HKDF-SHA256(decoded_root_key,nil,"cellhive/jwt/v1/issuer/vwork-*",32)` for trusted signers without root; no issuer CRUD or registry dependency.
- `cellhive token --issuer 'vwork-*' --key "$key" --ns vwork-42 --ttl 5m` signs namespace JWTs; add `--kind kv --name CACHE` for exact binding JWTs. TTL defaults to5m, fixed maximum86400 seconds. The kid pattern is exact/prefix `*`/`*`; exactly32-hex legacy IDs reject without fallback, no old `--iss`/scope-token compatibility.

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

`make cli-test`, `internal/bundler`, `internal/wrangler`, `internal/wranglercompat`, `cmd/cellhive TestTranslateWrangler`, `internal/server TestStockWrangler` (real Wrangler requires `CELLHIVE_WRANGLER_BIN`). Historical issuance tests `TestMintScopeToken`, `internal/scopedtoken` and `TestScopeAuthDelegated` do not validate the ADR-304 stateless amendment; current gates are in [test strategy](../testing.md).

## Related Documentation

[`wrangler-compat.md`](../wrangler-compat.md), [`dev-mode.md`](../dev-mode.md), [`security.md`](../security.md) (scoped-token scope and delegation), [`control-plane.md`](../control-plane.md) (issuance CLI)

_Last updated: 2026-10-15_
