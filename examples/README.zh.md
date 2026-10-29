# CellHive 示例

大多数目录是单 Worker 小项目（`index.js` + `wrangler.jsonc`）；`assets` 与 `react-static` 是 assets-only，`services` 与 `worker-rpc` 包含多个 Worker 子目录。示例既是**用法文档**，也是**兼容性对拍**：都能通过 CellHive 的生产部署链路，也能用开发 CLI（Miniflare）本地跑。`services` 展示 service binding 的 fetch + 入口类 RPC；`worker-rpc` 专门展示 native JSRPC 的结构化克隆边界。

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

## 功能列表

| 功能 | 示例 | 典型场景 | 需要先创建 | 快速验证 |
|---|---|---|---|---|
| 最小 Worker / host + path 路由 | [`hello`](./hello) | 健康检查、host 路由、Hello World | — | `GET /` |
| Web API / 请求响应体 | [`webapi`](./webapi)、[`body`](./body) | JSON API、query/header 解析、文本/二进制上传、流式响应 | — | `GET /?a=1`；`POST /`（JSON 或二进制） |
| KV | [`kv`](./kv) | 配置、会话、小对象、TTL、metadata | `kv KV` | `PUT /k`、`GET /k`、`DELETE /k` |
| D1 | [`d1`](./d1) | 关系型数据、SQL 查询、用户/业务表 | `d1 DB` | `POST /` 插入；`GET /` 查询 |
| R2 | [`r2`](./r2) | 图片/文件上传、对象存储、下载、metadata | `r2 FILES` | `PUT /k`、`HEAD /k`、`GET /k`、`GET /` |
| Queues | [`queues`](./queues) | 异步任务、削峰、后台处理、重试/DLQ | `queue JOBS`、`queue DLQ` | `GET /?n=1`，查看 consumer 日志 |
| Cron | [`cron`](./cron) | 定时清理、报表、巡检 | — | 等待 `scheduled` 触发（示例为每分钟） |
| Durable Object + SQLite | [`do-counter`](./do-counter) | 按房间/用户串行状态、计数器、会话 | — | `GET /?name=a` |
| DO alarm | [`do-alarm`](./do-alarm) | 延迟任务、定时提醒、对象级 alarm | — | `GET /arm`，等 2s 再 `GET /` |
| DO RPC | [`do-rpc`](./do-rpc) | 面向对象方法调用、强类型 DO API | — | `POST /`、`GET /` |
| DO WebSocket | [`do-websocket`](./do-websocket) | 实时协作、聊天、游戏、可休眠连接 | — | WebSocket 升级 `/`（普通请求返回 426） |
| DO 对象路由 | [`router`](./router) | 多房间/多租户对象路由 | — | `GET /roomA` |
| Workflows | [`workflows`](./workflows) | 多步骤长任务、`step.do`/`step.sleep`、状态查询 | `workflow create report-builder` | `GET /create?seed=1` → `GET /status?id=ID` |
| Service binding | [`services`](./services) | 内部 API 拆分、caller → api 调用 | 先部署 `services/api`，再部署 `services/caller` | 调用 caller 的 fetch |
| Worker ↔ Worker RPC | [`worker-rpc`](./worker-rpc) | 原生 JSRPC、结构化克隆边界 | 先部署 `worker-rpc/api`，再部署 `worker-rpc/caller` | 调用 caller 的 RPC |
| 静态资产 | [`assets`](./assets)、[`react-static`](./react-static) | 静态站点、React/Vite 构建、SPA 回退、`_headers`/`_redirects` | — | `GET /`（资产）；`GET /api`（Worker） |
| Astro SSR | [`astro`](./astro) | 通过 `@astrojs/cloudflare` 服务端渲染的 Astro | — | `GET /`（每请求 HTML）；`GET /about` |
| Vars / Secrets | [`vars-secrets`](./vars-secrets) | 普通配置、API token、密钥注入 | secret 用 `cellhive secret put ...` | `GET /` |
| AI（BYO 端点） | [`ai`](./ai) | 调用自有模型/网关、AI 功能 | 平台配置 `CELLHIVE_AI_URL`/`CELLHIVE_AI_KEY` | `GET /?prompt=hi` |
| Hyperdrive | [`hyperdrive`](./hyperdrive) | 连接现有 PostgreSQL/MySQL，不迁移数据库 | `hyperdrive HYDR` | `GET /` |
| Vectorize | [`vectorize`](./vectorize) | 语义检索、RAG、相似度查询 | `vectorize VEC --dimensions 3 --metric cosine` | `GET /seed`、`GET /?v=1,0,0` |
| 异步执行 | [`async`](./async) | `waitUntil`、后台写、非阻塞响应 | — | `GET /` |

## 框架注意事项

- **Astro SSR** 使用 `@astrojs/cloudflare`，并设置 `imageService: "passthrough"`。默认 `compile` 会把 sharp/Node 内置模块带进 Worker bundle，CellHive 打包会失败。adapter 12 把静态资产写在 `_worker.js` 旁边，所以示例的 build 会把除 `_worker.js`/`_routes.json` 外的内容复制到 `dist/client`；不要把 `assets.directory` 指向 `dist`。详见 [`astro/README.md`](./astro/README.md)。

## 组合场景

| 场景 | 推荐组合 | 说明 |
|---|---|---|
| SaaS API / 多租户后端 | `hello`/`webapi` + `d1` + `kv` + `vars-secrets` | D1 存业务数据，KV 存配置/会话，Worker 暴露 REST；按租户拆 namespace/资源 |
| 文件上传与处理 | `r2` + `queues` + `workflows` | 上传进 R2，Queue 削峰，Workflow 做转码/审核等长任务 |
| 实时协作 / 房间 | `do-websocket` + `do-counter` + `router` | 每个房间一个 DO，串行状态 + WebSocket；需要弹性时走 DO gated 生产模式 |
| 接入已有数据库 | `hyperdrive` + `webapi` | Worker 通过 Hyperdrive 连接现有 PostgreSQL/MySQL，无需迁移到 D1 |
| RAG / 语义搜索 | `ai` + `vectorize` + `r2` | R2 存文档，Vectorize 存向量，AI 做生成；适合知识库/客服 |
| 定时 ETL / 报表 | `cron` + `workflows` + `d1`/`r2` | Cron 触发，Workflow 编排多步骤，结果落 D1/R2 |
| 内部服务拆分 | `services` + `worker-rpc` | Service binding 做服务发现/调用，RPC 传递结构化对象 |
| 静态控制台 + API | `assets` + API Worker | 静态页面与 API 同域部署，SPA 回退到 `index.html` |

## 选择建议

- **只是想验证平台**：从 `hello`、`kv`、`d1` 开始；
- **做业务 API**：`webapi` + `d1` + `vars-secrets`，文件再加 `r2`；
- **做异步/长任务**：`queues` + `workflows`，定时用 `cron`；
- **做实时/有状态**：`do-counter`、`do-websocket`、`router`；
- **接已有数据库**：`hyperdrive`，不要为了示例迁移数据库；
- **做 AI 应用**：`ai` + `vectorize`，文档存 `r2`。

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

- 文档首页 / 阅读路径：[`docs/README.zh.md`](../docs/README.zh.md)
- 模块说明（含每个模块的环境变量表）：[`docs/modules/`](../docs/modules/README.zh.md)
- 兼容矩阵（Supported / Partial / Rejected）：[`docs/compatibility-matrix.md`](../docs/compatibility-matrix.md)
- 贡献者阅读路径：[`docs/contributing.md`](../docs/contributing.md)

_最后更新：2026-10-15_
