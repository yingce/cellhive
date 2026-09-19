# Module: CLI, Packaging, and wrangler Compatibility

`cellhive` CLI (deployment/operations/resources/queues/vectors/workflows), Go+esbuild packaging, and `wrangler` configuration compatibility; `cli/` is a Bun+Miniflare **dev-only** tool.

> The authoritative sources for configuration are [`../configuration.md`](../configuration.md) and the code; the table below is the subset relevant to this module.
## Key Interfaces

CLI command surface (`deploy --config`, `bundle build/put`, `resource`, `queue`, `vectorize`, `domain/route`, `creds`, `tail`…); packaging via `internal/bundler` (esbuild).

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
| `CELLHIVE_WORKERD` | auto-discovered | workerd path (prefers pinned `1.20260615.1`) |
| `TEST_BYTES` | — | Used by `internal/config` unit tests |


> For parsing rules (strings/booleans/durations/bytes/lists), see [`../configuration.md`](../configuration.md#parsing-rules).

## Key Invariants

- Production packaging is **Go + esbuild, no Node**.
- The dev CLI is not part of production artifacts, and Miniflare/workerd must stay in sync with the pinned version.
- Unknown wrangler fields/flags or overly new compatibility dates are **rejected at deployment time**.

## Source Locations

`cmd/cellhive/`; `internal/{bundler,wrangler,wranglercompat,workerdbin}`; `cli/`.

## Test Anchors

`make cli-test`, `internal/bundler`, `internal/wrangler`, `internal/wranglercompat`.

## Related Documentation

[`wrangler-compat.md`](../wrangler-compat.md), [`dev-mode.md`](../dev-mode.md)

_Last updated: 2026-09-19_