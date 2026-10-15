# CellHive Examples

The default examples route is English. The complete runnable index and code navigation are maintained in the [Chinese companion](README.zh.md) for now.

Each directory is a small Cloudflare Workers project with `index.js` and `wrangler.jsonc`. Examples cover KV, D1, R2, Queues, Cron, Durable Objects, Workflows, service bindings, assets, AI, Hyperdrive, and Vectorize.

## Run an example locally

```bash
cd cli && bun install
bun run src/index.ts dev ../examples/kv
```

The development CLI uses Miniflare and does not need a cell-agent. For the production path, create the resources first and deploy with the CellHive CLI; CellHive does not implicitly provision resources.

## Directory map

`hello` · `webapi` · `body` · `kv` · `d1` · `r2` · `queues` · `cron` · `do-counter` · `do-alarm` · `do-rpc` · `do-websocket` · `router` · `workflows` · `services` · `worker-rpc` · `assets` · `vars-secrets` · `ai` · `hyperdrive` · `vectorize` · `async`

See the [English documentation home](../docs/README.md), [module documentation](../docs/modules/README.md), and [Wrangler compatibility guide](../docs/en/wrangler-compat.md).

_Last updated: 2026-10-15_
