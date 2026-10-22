---
name: cellhive
description: "Use when a request concerns CellHive, stock workerd, Cloudflare Workers compatibility, Wrangler, local dev, static assets, bindings, deployment, routing, CellHive operations, or choosing a runnable example/scenario."
---

# CellHive AI Router

## Online source of truth

Read the current catalog before implementation:

- Human router: `https://cellhive.example.com/ai`
- Machine catalog: `https://cellhive.example.com/ai/catalog.json`
- CLI reference: `https://cellhive.example.com/cli.md`
- Runnable examples: `https://cellhive.example.com/examples/README.md` (English default; `README.zh.md` for Chinese)

The catalog is routing metadata, not a substitute for live inspection. Match the request against `id`, `keywords`, and `status`; follow its `reference` and `verification` fields. Do not invent unsupported Cloudflare behavior.

## Mandatory context gate

1. Inspect `wrangler.jsonc`, `wrangler.json`, or `wrangler.toml`.
2. Inspect `.cellhive/project.json` and identify the profile/namespace without reading secrets into output.
3. Run `cellhive version` and `cellhive auth test` when the task is remote or operational.
4. For local work, use `cellhive dev` or `wrangler dev`; do not claim production state from Miniflare.
5. For deployment, run a dry-run or deploy command and capture its result; never claim success from generated files alone.

## Route by intent

| Intent | Read next | First action |
|---|---|---|
| Examples/scenarios/starter templates | `/examples/README.md`, `/examples/README.zh.md`, `examples.scenarios` | open the feature and composite-scenario tables; inspect the chosen `index.js` + `wrangler.jsonc`; register resources before deploy |
| React/Vite/static/SSR/Assets | `/docs/static-sites.md`, `runtime.assets` | inspect `assets.directory`, `main`, `binding`, and build output |
| Wrangler compatibility/config | `/docs/wrangler-compat.md`, `cli.wrangler` | inspect config and compatibility date/flags |
| Local development | `/docs/dev-mode.md`, `cli.dev` | run `cellhive dev <dir>` or `wrangler dev` |
| Deploy/release/rollback | `/docs/cli.md`, `cli.deploy` | run `cellhive auth test`, then dry-run/deploy |
| KV/D1/R2/Queue/Workflow | `/docs/bindings.md`, `runtime.bindings` | verify resource registration before deploy |
| Service binding/worker RPC | `/docs/bindings.md`, `runtime.service-rpc` | deploy target first; check same/cross namespace ACL |
| Durable Objects | `/docs/compatibility-matrix.md`, `runtime.durable-objects` | check migration/class/WS/RPC boundary |
| Domain/path routing | `/docs/routing.md`, `ops.routing` | inspect domain and route projection |
| GC/cleanup/incident | `/docs/operations.md`, `ops.storage-gc` | inspect current references/status before mutation |

For "show me an example", "how do I build X", or "what combination should I use", resolve through the examples README first, then return to the capability reference for boundaries.

## Examples and scenario routing

Start with [`/examples/README.md`](https://cellhive.example.com/examples/README.md) (English default) or [`README.zh.md`](https://cellhive.example.com/examples/README.zh.md) (Chinese companion).

- The **feature table** maps each capability (KV/D1/R2/Queue/Cron/DO/Workflow/assets/AI/Hyperdrive/Vectorize/...) to an example directory, a typical use case, required resources, and a quick check.
- The **composite scenarios** table maps product-level requests (SaaS API, upload pipeline, realtime rooms, existing database, RAG, scheduled ETL, internal services, static console + API) to a recommended combination.
- Inspect the selected `index.js` and `wrangler.jsonc` before copying code. Examples are demos, not production templates.
- Local: `cd cli && bun run src/index.ts dev ../examples/<name>` (Miniflare; no cell-agent).
- Production: register resources first, then `cellhive deploy <namespace> - --config examples/<name>/wrangler.jsonc`; examples do not auto-provision.
- Never copy secrets or real credentials from examples into generated code or output.

## Safety boundaries

- Never put `CELLHIVE_ROOT_KEY`, admin tokens, JWTs, or S3 credentials in project files, Skill output, docs, or generated Worker code.
- Never assume `wrangler` compatibility means full Cloudflare parity; use the catalog status (`supported`, `partial`, `rejected`).
- Never use `cellhive dev` as evidence that a real cell-agent/object-store/deployment path works.
- Do not create resources implicitly when the compatibility docs require explicit registration.
- Do not change or fork workerd.
- For destructive operations (delete, purge, revoke, rollback), inspect first and require explicit user intent.

## Verification contract

Report the exact command and result. Suitable evidence includes:

- `make test`, `make cli-test`, `make vet`;
- real workerd tests in `internal/userruntime`/`internal/doruntime`;
- `cellhive deploy --dry-run` or a real deploy response;
- a local example run (`bun run src/index.ts dev ../examples/<name>`) or a production deploy of an example config;
- HTTP response from the configured route;
- `cell-agent`/user-runtime readiness and projection output;
- object/resource/version inspection after a change.

If a capability is `partial` or `rejected`, say so and link the boundary. If a required online URL is unavailable, use the checked-in `docs/ai/catalog.json` and local docs as fallback, then report the fallback.
