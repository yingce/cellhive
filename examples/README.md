# CellHive Examples

The default examples route is English. The scenario-oriented feature table is below; the Chinese companion also has the code navigation map.

Most directories are a single Worker project (`index.js` + `wrangler.jsonc`); `assets` and `react-static` are assets-only, and `services` / `worker-rpc` contain multiple Worker subdirectories. Examples cover KV, D1, R2, Queues, Cron, Durable Objects, Workflows, service bindings, assets, AI, Hyperdrive, and Vectorize.

## Run an example locally

```bash
cd cli && bun install
bun run src/index.ts dev ../examples/kv
```

The development CLI uses Miniflare and does not need a cell-agent. For the production path, create the resources first and deploy with the CellHive CLI; CellHive does not implicitly provision resources.

## Feature table

| Feature | Example | Typical use case | Create first | Quick check |
|---|---|---|---|---|
| Minimal Worker / host + path routing | [`hello`](./hello) | Health check, host routing, hello world | — | `GET /` |
| Web APIs / request bodies | [`webapi`](./webapi), [`body`](./body) | JSON APIs, query/header parsing, text/binary uploads, streaming responses | — | `GET /?a=1`; `POST /` (JSON or binary) |
| KV | [`kv`](./kv) | Config, sessions, small objects, TTL, metadata | `kv KV` | `PUT /k`, `GET /k`, `DELETE /k` |
| D1 | [`d1`](./d1) | Relational data, SQL queries, user/business tables | `d1 DB` | `POST /` insert; `GET /` query |
| R2 | [`r2`](./r2) | Image/file uploads, object storage, downloads, metadata | `r2 FILES` | `PUT /k`, `HEAD /k`, `GET /k`, `GET /` |
| Queues | [`queues`](./queues) | Async jobs, load smoothing, background work, retries/DLQ | `queue JOBS`, `queue DLQ` | `GET /?n=1`; inspect consumer logs |
| Cron | [`cron`](./cron) | Scheduled cleanup, reports, checks | — | Wait for `scheduled` (example runs every minute) |
| Durable Object + SQLite | [`do-counter`](./do-counter) | Per-room/user serialized state, counters, sessions | — | `GET /?name=a` |
| DO alarm | [`do-alarm`](./do-alarm) | Delayed work, reminders, object-level alarms | — | `GET /arm`, wait 2s, then `GET /` |
| DO RPC | [`do-rpc`](./do-rpc) | Object-oriented method calls, typed DO APIs | — | `POST /`, `GET /` |
| DO WebSocket | [`do-websocket`](./do-websocket) | Realtime collaboration, chat, games, hibernating sockets | — | WebSocket upgrade `/` (plain request returns 426) |
| DO object routing | [`router`](./router) | Multi-room/multi-tenant object routing | — | `GET /roomA` |
| Workflows | [`workflows`](./workflows) | Multi-step long jobs, `step.do`/`step.sleep`, status lookup | `workflow create report-builder` | `GET /create?seed=1` -> `GET /status?id=ID` |
| Service binding | [`services`](./services) | Internal API split, caller -> api | Deploy `services/api`, then `services/caller` | Call caller's fetch |
| Worker-to-worker RPC | [`worker-rpc`](./worker-rpc) | Native JSRPC, structured-clone boundaries | Deploy `worker-rpc/api`, then `worker-rpc/caller` | Call caller's RPC |
| Static assets | [`assets`](./assets), [`react-static`](./react-static) | Static site, React/Vite build, SPA fallback, `_headers`/`_redirects` | — | `GET /` (asset); `GET /api` (Worker) |
| Vars / Secrets | [`vars-secrets`](./vars-secrets) | Config, API tokens, secret injection | `cellhive secret put ...` for secrets | `GET /` |
| AI (bring your own endpoint) | [`ai`](./ai) | Call your own model/gateway, add AI features | Set `CELLHIVE_AI_URL`/`CELLHIVE_AI_KEY` | `GET /?prompt=hi` |
| Hyperdrive | [`hyperdrive`](./hyperdrive) | Connect an existing PostgreSQL/MySQL database without migrating to D1 | `hyperdrive HYDR` | `GET /` |
| Vectorize | [`vectorize`](./vectorize) | Semantic search, RAG, similarity queries | `vectorize VEC --dimensions 3 --metric cosine` | `GET /seed`, `GET /?v=1,0,0` |
| Async execution | [`async`](./async) | `waitUntil`, background writes, non-blocking responses | — | `GET /` |

## Composite scenarios

| Scenario | Recommended combination | Notes |
|---|---|---|
| SaaS API / multi-tenant backend | `hello`/`webapi` + `d1` + `kv` + `vars-secrets` | D1 for business data, KV for config/sessions, Worker REST API; split resources by tenant |
| Upload and processing | `r2` + `queues` + `workflows` | Upload to R2, smooth with Queue, run long tasks in Workflows |
| Realtime collaboration / rooms | `do-websocket` + `do-counter` + `router` | One DO per room, serialized state + WebSocket; use gated DO for production RPO=0 |
| Existing database | `hyperdrive` + `webapi` | Connect existing PostgreSQL/MySQL through Hyperdrive; no D1 migration |
| RAG / semantic search | `ai` + `vectorize` + `r2` | Documents in R2, vectors in Vectorize, generation through AI |
| Scheduled ETL / reports | `cron` + `workflows` + `d1`/`r2` | Cron triggers, Workflow orchestrates steps, results in D1/R2 |
| Internal service split | `services` + `worker-rpc` | Service bindings for discovery/calls, RPC for structured objects |
| Static console + API | `assets` + API Worker | Static pages and API on one host; SPA fallback to `index.html` |

## Choosing a starting point

- **Just validating the platform**: start with `hello`, `kv`, `d1`;
- **Building a business API**: `webapi` + `d1` + `vars-secrets`; add `r2` for files;
- **Async/long-running work**: `queues` + `workflows`, plus `cron` for schedules;
- **Realtime/stateful work**: `do-counter`, `do-websocket`, `router`;
- **Existing database**: `hyperdrive`; do not migrate just for the example;
- **AI application**: `ai` + `vectorize`, with documents in `r2`.

See the [English documentation home](../docs/README.md), [module documentation](../docs/modules/README.md), and [Wrangler compatibility guide](../docs/en/wrangler-compat.md).

_Last updated: 2026-10-15_
