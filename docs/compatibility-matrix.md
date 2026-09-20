# 兼容矩阵

状态：**Supported**（普通应用可用）· **Partial**（有明确边界）· **Rejected**（部署/配置期显式拒绝）· **Internal**（平台面，非租户 API）。

## Cloudflare 运行时面

| 面 | 状态 | 说明 |
|---|---|---|
| ES module Workers / `fetch` | Supported | `workerLoader` 动态加载不可变版本 |
| WebSocket | Supported | user-runtime 代理；DO 上迁移/部署时 1012，客户端重连（CF 兼容）；**`env.DO.get().fetch(升级请求)` 经 `/v1/do/connect` 直连 owner（`{owner,ticket}` 查询 + ticket 仅本 shard，ADR-175）** |
| KV | Supported | 强一致（读也转发给 owner，ADR-120；cell SQLite）；无全局边缘复制；**`put` 接受 string/ArrayBuffer/ArrayBufferView/Blob/ReadableStream（ADR-174）**；**写入校验：key≤512B、metadata≤1KiB、expiration 须在未来、value≤25MiB（ADR-176）；expirationTtl 接受任意正整数（ADR-183 放开 CF 的 ≥60s 下限——读路径惰性过期秒级生效，清扫按最近到期武装）** |
| D1 | Supported | SQL + `batch`/`exec`；单写者；**Sessions/bookmarks 显式拒绝**（`withSession()`/`session()` 抛错，无读复制，ADR-153）；`run()/batch()` 的 `meta` 含 `last_row_id`/`changed_db`，错误映射 `D1_ERROR`（ADR-163） |
| R2 | Supported | S3 兼容 + **presign（`createPresignedUrl`）** + **multipart（ADR-113）** + **cursor 分页 list（ADR-145）**；**R2Object 全字段 + `put` http/custom metadata/checksums + `head()`（ADR-163）**；**list 支持 `include`（按需回填逐对象 metadata）与 `delimiter`/`delimitedPrefixes`（ADR-168）**；**`R2ObjectBody.body` + `writeHttpMetadata`（ADR-174/175：本地 Proxy 包装，metadata 写入调用方 Headers）**；无 SSE-C/jurisdiction、无 object versioning/conditional put |
| Queues | Partial | 至少一次 + DLQ；`max_concurrency` **支持**（ADR-112，批次并发上限）；`contentType=v8` 拒绝 |
| Cron | Supported | 分钟对齐、best-effort、不补跑 |
| Durable Objects | Partial | 原生 facet + 同步 SQL；**DO 内 bindings ✅（ADR-090）**；仅同 worker class；部署默认**惰性重启**（可 `do_eager_restart`/`session_policy`）；WS 迁移/重启 1012；**DO RPC ✅（ADR-162）**：`env.NS.get(id|name).method(...)`，tagged JSON + host→facet 原生 JSRPC（≤8 MiB；无函数/stub/流）；**legacy（未 `extends DurableObject`）类由 facet 包装支持（ADR-174）**；**DO `fetch` 的非 2xx 原样返回（状态/content-type，ADR-174）** |
| Workflows | Partial | 自研引擎；支持 create/get/status/lifecycle/delete/list + `step.do`（含 `retries{limit,delay,backoff}`）/`sleep`/`waitForEvent` + `sendEvent`；不支持跨 worker；`locationHint` 接受但忽略 |
| ASSETS | Supported | 对象存储 + 版本化；完整资产管道 |
| AI | Partial（可选） | **BYO OpenAI 兼容端点**（`env.AI.run`，`CELLHIVE_AI_URL/_KEY`）；无平台托管目录、无 workers-ai 目录模型 |
| Service / Platform bindings | Supported | 版本冻结 + ACL；**worker↔worker RPC = 同实例原生 JSRPC（ADR-102）**；**DO RPC = owner 路由 JSON+tagged（ADR-162）** |
| Vars / Secrets | Supported | secrets 信封加密 |
| `nodejs_compat` | Supported | 随 workerd；需开启 |
| Cache API | Rejected | 无边缘缓存 |
| Vectorize / AI Search / Browser / Email / Analytics Engine | Rejected | workerd 未提供（Hyperdrive 已支持，见下） |
| Python Workers | Rejected | 不支持 |

## Wrangler 配置面

| 配置 | 状态 |
|---|---|
| `name` / `main` / `compatibility_date` / `compatibility_flags` | Supported |
| `vars` / `secrets`(声明) | Supported |
| `kv_namespaces` / `d1_databases` / `r2_buckets` | Supported |
| `queues.producers` / `queues.consumers` | Supported（含 `max_concurrency`，ADR-112） |
| `workflows` / `triggers.crons` | Supported |
| `services` | Supported |
| `assets`（完整管道） | Supported |
| `durable_objects` / `migrations` / `exports` | Supported（同 worker）：`tag`/`new_classes`/`new_sqlite_classes`/`renamed_classes`/`deleted_classes`/`transferred_classes`（**跨 worker transfer（`script_name`）拒绝**）；`enable_ctx_exports` 标志支持 `ctx.exports` |
| `workers_dev` / `routes` / `custom_domain` | Partial：`workers_dev`→命名空间路径；自定义域可选；`preview_urls` 拒绝 |
| `env.<name>` | Supported（每环境独立 app/ns） |
| 自动 provisioning（无 id 创建） | Rejected（须显式 create） |
| **Hyperdrive** | Supported | 绑定给 `connectionString`（+ host/port/user/password/database）；origin URL 作为**注册资源信封加密**存储（`resource create --connection-string`），`--hyperdrive NAME=<resource>` / `wrangler.jsonc hyperdrive[{binding,id}]` 按名解析（ADR-129）；**平台不做连接池**：本地复用 = 在**自己的 Durable Object** 里持有 `cloudflare:sockets` 连接（DO 生命周期内复用；普通 worker 无法跨请求复用，ADR-130 有实证）；共享池需自备 pgbouncer 式协议终止代理；私有网段源库受租户 public-only 出网限制；dev 走 Miniflare `hyperdrives`（`localConnectionString`，ADR-125/129）|
| **Vectorize** | Supported（ADR-158/159）：注册资源（`--dimensions`/`--metric`，不可变）+ facade `env.<BINDING>`（`insert/upsert/query/queryById/getByIds/deleteByIds/describe`）；存储/检索用 SQLite 官方 **vec1**（默认 flat 精确，`vectorize rebuild` 建 IVFADC/OPQ ANN）；**`dot-product` 不支持**（仅 cosine/euclidean）；metadata 过滤为后置过滤；资源 = `index_name`；dev 不支持（Miniflare 无实现，明确报错） |
| `ai_search*` / `dispatch_namespaces` / `secrets_store_secrets` / `send_email` / `browser` / `images` / `containers` / `flagship` / `pipelines` / `vpc` / `placement` / `site` | Rejected |
| 未知字段 | Rejected（显式报错） |

## 打包

- Go + esbuild（无 Node）；产物自定格式（module manifest + assets），不追求与 Wrangler 输出一致；
- 以**某个确切的 Wrangler 版本作为语义基线**做离线契约测试；
- 优先支持框架/Vite 预构建产物（Next/OpenNext、SvelteKit、Astro 等）。

## 定稿（ADR-153）

- **`compatibility_flags` 精确列表**：见 `internal/wranglercompat.KnownCompatibilityFlags`（与 `cli/src/validate.ts` 的 `KNOWN_COMPAT_FLAGS` 镜像）；未知 flag 部署期 `unknown_flag` 失败关闭。`TestKnownFlagsMatchDevCLI` 保证两侧一致；`TestPinPairsWithCompatibilityDate` 把 workerd pin（`1.20260615.1`）与兼容上限（`2026-06-22`）绑成一个决定。
- **框架预构建产物验收**：`internal/wrangler TestFrameworkPrebuiltLayouts` 覆盖 OpenNext（`.open-next/worker.js` + `.open-next/assets`）、SvelteKit（`.svelte-kit/cloudflare/_worker.js` + 同目录资产）、Astro（`dist/_worker.js/index.js` + `dist/client`）：配置映射 + `bundler.Build` 打包。
- **不做的项**：见"Rejected"行的原因说明（Cache API/浏览器渲染/Email/Python 等）；Vectorize 已支持（ADR-158/159）。

_最后更新：2026-09-19_
