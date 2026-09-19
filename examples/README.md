# CellHive 示例

每个目录是一个可运行的 Cloudflare Workers 小项目（`index.js` + `wrangler.jsonc`），覆盖一项平台能力。示例既是**用法文档**，也是**兼容性对拍**：都能通过 CellHive 的生产部署链路，也能用开发 CLI（Miniflare）本地跑。

> 示例是演示，不是生产产物。配置里的 `compatibility_date` 取 `2026-01-01`（≤ 平台上限）。

## 运行方式

### A. 本地（最简单，开发 CLI 自动 provision 绑定）

```bash
cd cli && bun install
# 在仓库根运行，<dir> 指向某个示例目录
bun run src/index.ts dev ../examples/kv
# 例：curl -X PUT --data hi http://127.0.0.1:8787/k && curl http://127.0.0.1:8787/k
```

开发 CLI 用 Miniflare 模拟 KV/D1/R2/Queue/DO/vars/secrets/assets，**无需 cell-agent**。

### B. 生产链路（真实 cell-agent + workerd）

CellHive **不做自动 provisioning**：binding 名必须等于已登记的资源名，先建资源再 deploy。

```bash
export CELLHIVE_ADMIN_URL=http://127.0.0.1:8082
export CELLHIVE_CONTROL_URL=http://127.0.0.1:7001
export CELLHIVE_ROOT_KEY=<你的 root key>

cellhive app create demo

# 按需创建资源（名字 = 示例里的 binding 名）
cellhive resource create demo kv KV
cellhive resource create demo d1 DB
cellhive resource create demo r2 FILES
cellhive resource create demo queue JOBS
cellhive resource create demo queue DLQ
cellhive workflow create demo report-builder
cellhive vectorize create demo VEC --dimensions 3 --metric cosine
cellhive resource create demo hyperdrive HYDR --connection-string 'postgres://user:pass@host:5432/db'

# 部署（worker 名取自 wrangler.jsonc 的 name）
cellhive deploy demo - --config examples/kv/wrangler.jsonc
cellhive domain add demo kv.example.test
cellhive route add demo kv.example.test kv
```

## 示例索引

| 目录 | 演示 | 需要先创建的资源 | 触发 / 验证 |
|---|---|---|---|
| [`hello`](./hello) | 最小 `fetch` + 路由 | — | `GET /` |
| [`webapi`](./webapi) | URL/Headers/TextEncoder/atob/crypto | — | `GET /?a=1` |
| [`body`](./body) | 请求/响应体（json/text/arrayBuffer/流） | — | `POST /`（JSON 或二进制） |
| [`kv`](./kv) | KV put/get/delete/list + TTL + metadata | `kv KV` | `PUT /k`、`GET /k` |
| [`d1`](./d1) | SQL 建表/参数化插入/查询/`meta.last_row_id` | `d1 DB` | `POST /`、`GET /` |
| [`r2`](./r2) | R2 put/get/head/list/delete + metadata | `r2 FILES` | `PUT /k`、`GET /k`、`GET /` |
| [`queues`](./queues) | 生产者 send + 消费者 batch + DLQ | `queue JOBS`、`queue DLQ` | `GET /?n=1` |
| [`cron`](./cron) | `scheduled` + `triggers.crons` | — | 每分钟触发（看日志） |
| [`do-counter`](./do-counter) | 经典 DO 类 + SQLite storage | — | `GET /?name=a` |
| [`do-alarm`](./do-alarm) | DO alarm（shim + 统一 timer） | — | `GET /arm` 后等 2s 再 `GET /` |
| [`do-rpc`](./do-rpc) | DO RPC（`extends DurableObject`） | — | `POST /`、`GET /` |
| [`do-websocket`](./do-websocket) | 可休眠 WebSocket echo | — | WS 升级 `/`（非升级返回 426） |
| [`router`](./router) | Worker → 多 DO 对象路由 | — | `GET /roomA` |
| [`workflows`](./workflows) | Workflow `step.do`/`step.sleep` + 生命周期 | `workflow create report-builder` | `GET /create?seed=1` → `/status?id=ID` |
| [`services`](./services) | service binding（fetch + 入口类 RPC） | — | 先部署 `services/api`，再部署 `services/caller` |
| [`worker-rpc`](./worker-rpc) | worker↔worker 原生 JSRPC（结构化克隆） | — | 先部署 `worker-rpc/api`，再部署 `worker-rpc/caller` |
| [`assets`](./assets) | 静态资产 + `_headers`/`_redirects`/SPA + Worker 回退 | — | `GET /`（资产）、`GET /api`（Worker） |
| [`vars-secrets`](./vars-secrets) | `vars` 与 secret | — | `GET /`；secret：`cellhive secret put demo vars-secrets TOKEN` |
| [`ai`](./ai) | BYO AI 端点（`env.AI.run`） | 平台设 `CELLHIVE_AI_URL/_KEY` | `GET /?prompt=hi` |
| [`hyperdrive`](./hyperdrive) | Hyperdrive 连接串绑定 | `hyperdrive HYDR` | `GET /` |
| [`vectorize`](./vectorize) | Vectorize `insert`/`query` | `vectorize VEC --dimensions 3 --metric cosine` | `GET /seed`、`GET /?v=1,0,0` |
| [`async`](./async) | 异步 DO 存储 + `setTimeout` + `waitUntil` | — | `GET /` |

## 代码导航（功能 → 示例 → 源码 → 文档）

| 功能 | 示例 | 主要源码 | 文档 |
|---|---|---|---|
| Workers `fetch` / 路由 / Web APIs / body | `hello`、`webapi`、`body` | `internal/userruntime/`、`workerd/user-runtime/loader.js` | [`routing.md`](../docs/routing.md)、[`modules/user-runtime.md`](../docs/modules/user-runtime.md) |
| KV | `kv` | `internal/cellstore/`、`internal/server/`（`/v1/kv/*`）、`workerd/platform/bindings.js` | [`bindings.md`](../docs/bindings.md)、[`modules/bindings.md`](../docs/modules/bindings.md) |
| D1 | `d1` | `internal/d1/`、`internal/server/`（`/v1/d1/*`） | 同上 |
| R2 | `r2` | `internal/r2/`、`internal/server/r2.go` | 同上 |
| Queues | `queues` | `internal/queue/`、`internal/dispatch/`、`workerd/user-runtime/queue-wrapper.js` | [`timers-and-dispatch.md`](../docs/timers-and-dispatch.md) |
| Cron | `cron` | `internal/cron/`、`internal/timer/` | 同上 |
| Durable Objects（storage/alarm/WS/路由） | `do-counter`、`do-alarm`、`do-websocket`、`router`、`async` | `internal/doruntime/`、`workerd/do-runtime/{host.js,cellhive-do.js}` | [`durable-objects.md`](../docs/durable-objects.md)、[`modules/do-runtime.md`](../docs/modules/do-runtime.md) |
| DO RPC | `do-rpc` | `internal/doruntime/`、`workerd/platform/rpc-codec.js`、`workerd/platform/facades.js` | [`bindings.md`](../docs/bindings.md)（DO RPC） |
| Workflows | `workflows` | `internal/workflow/` | [`bindings.md`](../docs/bindings.md)（Workflows） |
| Service binding / worker↔worker RPC | `services`、`worker-rpc` | `internal/userruntime/loader.js`（`setServiceLoader`）、`workerd/platform/bindings.js` | [`bindings.md`](../docs/bindings.md)（Service） |
| 静态资产 | `assets` | `internal/userruntime/`（loader 资产管道）、`internal/wrangler/wrangler.go` | [`wrangler-compat.md`](../docs/wrangler-compat.md) |
| Vars / Secrets | `vars-secrets` | `internal/control/`（secret 信封加密）、`internal/server/` | [`security.md`](../docs/security.md) |
| AI（BYO） | `ai` | `workerd/platform/bindings.js`、`internal/userruntime/` | [`bindings.md`](../docs/bindings.md)（AI） |
| Hyperdrive | `hyperdrive` | `internal/control/`（资源连接串）、`workerd/user-runtime/loader.js` | [`bindings.md`](../docs/bindings.md)（Hyperdrive） |
| Vectorize | `vectorize` | `internal/vectorize/` | [`bindings.md`](../docs/bindings.md)（Vectorize） |
| 复制 / RPO=0（无独立示例） | — | `internal/{ltx,replica,capture,peer,recovery}` | [`cell-protocol.md`](../docs/cell-protocol.md)、[`modules/storage-and-replication.md`](../docs/modules/storage-and-replication.md) |

## 相关文档

- 文档首页 / 阅读路径：[`docs/README.md`](../docs/README.md)
- 模块说明（含每个模块的环境变量表）：[`docs/modules/`](../docs/modules/README.md)
- 兼容矩阵（Supported / Partial / Rejected）：[`docs/compatibility-matrix.md`](../docs/compatibility-matrix.md)
- 贡献者阅读路径：[`docs/contributing.md`](../docs/contributing.md)

_最后更新：2026-09-19_
