# 兼容矩阵

状态：**Supported**（普通应用可用）· **Partial**（有明确边界）· **Rejected**（部署/配置期显式拒绝）· **Internal**（平台面，非租户 API）。

## Cloudflare 运行时面

| 面 | 状态 | 说明 |
|---|---|---|
| ES module Workers / `fetch` | Supported | `workerLoader` 动态加载不可变版本 |
| WebSocket | Supported | user-runtime 代理；DO 上迁移/部署时 1012，客户端重连（CF 兼容）；**`env.DO.get().fetch(升级请求)` 经 `/v1/do/connect` 直连 owner（`{owner,ticket}` 查询 + ticket 仅本 shard，ADR-175）** |
| KV | Supported | 强一致（读也转发给 owner，ADR-120；cell SQLite）；无全局边缘复制；**`put` 接受 string/ArrayBuffer/ArrayBufferView/Blob/ReadableStream（ADR-174）**；`get`/`getWithMetadata` 支持字符串类型和 `{type:"text"|"json"|"arrayBuffer"}` 形态；`cacheTtl` 被接受但不提供边缘缓存语义；**写入校验：key≤512B、metadata≤1KiB、expiration 须在未来、expirationTtl≥60s、value≤25MiB（ADR-176）** |
| Wrangler Cloudflare API 资源与命令 | Partial | 原版 Wrangler 4.133.0 的 KV/D1/R2/Queues `create/list/delete`、Worker secrets/routes、`deployments list/status`、`versions list`、Queue consumer add/list/purge、Vectorize V2 create/list/get/delete/info 已提供对应管理 API；资源先显式创建，deploy 不自动 provisioning。Hyperdrive 运行时 binding 可用，但原版 Wrangler Hyperdrive 管理 API 未开放；Vectorize 其余原版数据命令未对齐。`cellhive wrangler` 只是原生 CLI 命令翻译，范围见 [Wrangler 指南](wrangler-compat.md)。 |
| Wrangler Assets / Workflows API | Partial | Assets upload session + JWT 单文件或 multipart bulk 上传、Workflow create/list/get/delete 与 trigger/instances list/describe 已映射；`concurrency.limit`、`schedules`、`default_retention` 已持久化并执行；`limits` 与实例 `restart --from` 仍不支持。 |
| D1 | Supported | SQL + `prepare`/`bind`/`all`/`first`/`run`/`raw`/`batch`/`exec`；`prepare()`/`bind()` 同步返回本地语句对象，ORM 可将其传给 `batch()`；`ArrayBuffer`/TypedArray 按 BLOB 绑定与回读；单写者；**Sessions/bookmarks 与事务显式拒绝**（`withSession()`/`session()` 抛错，`BEGIN` 不支持，无读复制，ADR-153）；`meta` 含 `changes`/`last_row_id`/`changed_db`，错误映射 `D1_ERROR`（ADR-163） |
| R2 | Supported | S3 兼容 + **presign（`createPresignedUrl`）** + **multipart（ADR-113）** + **cursor 分页 list（ADR-145）**；**R2Object 全字段 + `put` http/custom metadata/checksums + `head()`（ADR-163）**；**list 支持 `include`（按需回填逐对象 metadata）与 `delimiter`/`delimitedPrefixes`（ADR-168）**；**`R2ObjectBody.body` + `writeHttpMetadata`（ADR-174/175）**；`onlyIf` 的 exists/etagMatches/etagDoesNotMatch 已支持；无 SSE-C/jurisdiction、无 object versioning |
| Queues | Partial | `send()`/`sendBatch()`、原子批量、`json`/`text`/`bytes`/`v8`、128 KiB 限制、metadata、原始 timestamp、delay 和幂等冲突已实现；仍有 `max_batch_timeout`、跨批顺序和部分 retry/DLQ 边界待收口。 |
| Cron | Supported | UTC 五字段数字 cron（支持列表/范围/步长，Vixie DOM/DOW OR）；分钟对齐、best-effort、不补跑；调度器默认每 30 秒扫描，故不保证恰好一次或准点触发 |
| Durable Objects | Partial | 原生 facet + 同步 SQL；**DO 内 bindings ✅（ADR-090）**；仅同 worker class；部署默认**惰性重启**（可 `do_eager_restart`/`session_policy`）；WS 迁移/重启 1012；**DO RPC ✅（ADR-162）**：`env.NS.get(id|name).method(...)`，tagged JSON + host→facet 原生 JSRPC（≤8 MiB；无函数/stub/流）；**legacy（未 `extends DurableObject`）类由 facet 包装支持（ADR-174）**；**DO `fetch` 的非 2xx 原样返回（状态/content-type，ADR-174）** |
| Workflows | Partial | 自研引擎；支持 create/createBatch/get/status/lifecycle/delete/list + `step.do`（含 `retries{limit,delay,backoff}`）/`sleep`/`waitForEvent` + `sendEvent`；create/createBatch payload 总量 ≤1 MiB、batch 1..100。不支持跨 worker；管理 API 的 describe 可读取已持久化的完成步骤与输出，缺逐次尝试日志、步骤计时和运行中步骤；无 progress 回调。`locationHint` 接受但忽略；duration 字符串、Date/time sleepUntil、wait 超时错误、step.do timeout、创建时 worker/bundle/version pin 已支持；sensitive 与部分 lifecycle 选项仍为 Partial。 |
| ASSETS | Supported | 对象存储 + 版本化；完整资产管道 |
| AI | Partial（可选） | **BYO OpenAI 兼容端点**（`env.AI.run`，`CELLHIVE_AI_URL/_KEY`）；无平台托管目录、无 workers-ai 目录模型 |
| Service / Platform bindings | Supported | 版本冻结 + ACL；**worker↔worker RPC = 同实例原生 JSRPC（ADR-102）**；**DO RPC = owner 路由 JSON+tagged（ADR-162）** |
| Vars / Secrets | Supported | vars、managed secrets 与用户命名 binding stub 注入 tenant env；tenant env 为零平台键、无保留前缀（ADR-185）。secrets 在 control cell 信封加密，运行时由 cell-agent 解密注入 Worker/DO；同名 secret 覆盖 var |
| 租户 `console.*` 平台采集 | Supported | 默认由 host-bound 原生 Tail `Fetcher` 捕获 Worker/service target/DO facet 的 console 与异常，写入 ring/可选 OTLP；身份与 log 凭据不进入 tenant env（ADR-185）。可按 runtime 设 `CELLHIVE_TENANT_LOG_CAPTURE=off` 关闭并省去 Tail 开销（ADR-192） |
| `nodejs_compat` | Supported | 随 workerd；需开启 |
| Cache API | Rejected | 无边缘缓存 |
| AI Search / Browser / Email / Analytics Engine | Rejected | workerd 未提供；Vectorize 由 CellHive 的 SQLite vec1 facade 提供（见下，ADR-158/159），Hyperdrive 已支持 |
| Python Workers | Rejected | 不支持 |

### Queue / Workflow 的调用边界

以下是租户运行时的行为；`queues.producers/consumers` 和 `workflows` 配置能被解析，不等于对应的 Cloudflare API 全部兼容。

| 调用/配置 | CellHive 当前行为 | 迁移时的处理 |
|---|---|---|
| Queue `send()` 的消息格式 | 已使用 `json`/`text`/`bytes`/`v8` 枚举；v8 使用 CellHive 的 tagged structured encoding，消费者按枚举还原。 |
| Queue `sendBatch()` | 已实现 1..100 条、单事务原子入队、输入顺序返回 ID；重复幂等键同内容复用 ID，不同内容返回冲突。 |
| Queue 大小/延迟 | body 上限 128 KiB；`delaySeconds` 严格校验 0..86400。 |
| Queue 消费配置 | `max_batch_size`、`max_concurrency` 生效；`max_batch_timeout`、跨批顺序和 `max_retries=0` 的 CF 精确语义仍待收口。 |
| Queue 消息元数据 | 保存并传递原始入队 timestamp 和 metadata；批次间不保证顺序。 |
| Workflow `step.sleep/sleepUntil` | 已支持数字、常见时长字符串和 `Date`/时间字符串。 |
| Workflow `step.waitForEvent` | 数字和常见时长字符串可用；超时抛 `WorkflowTimeoutError`；事件载荷仍是 CellHive 保存的 JSON 事件对象。 |
| Workflow 步骤/实例选项 | `step.do` timeout 已进入执行路径；`sensitive`、`restart({from})`、`terminate(options)`、`subscribe()`、step 历史和进度流仍待收口。 |
| Workflow 状态/版本 | 租户 `status()` 把内部 `sleeping` 映射为 `waiting`，非 ASCII 输出按 UTF-8 解码；实例保存创建时 worker/bundle/version，恢复/重派发使用该目标。 | |

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
| **Hyperdrive** | Supported（运行时配置）；绑定给 `connectionString`（+ host/port/user/password/database）；origin URL 作为**注册资源信封加密**存储（`resource create --connection-string`），`--hyperdrive NAME=<resource>` / `wrangler.jsonc hyperdrive[{binding,id}]` 按名解析（ADR-129）；**平台不做连接池**；原版 Wrangler 管理 API 未开放。dev 走 Miniflare `hyperdrives`（`localConnectionString`）。 |
| **Vectorize** | Supported（运行时配置，ADR-158/159）；注册资源（`--dimensions`/`--metric`，不可变）+ facade `env.<BINDING>`（`insert/upsert/query/queryById/getByIds/deleteByIds/describe`）；SQLite **vec1** 默认 flat 精确，可用 `vectorize rebuild` 建 ANN；**`dot-product` 不支持**；metadata 后置过滤；资源 = `index_name`；dev 不支持（Miniflare 无实现，明确报错）。原版 Wrangler 命令范围见上表。 |
| `ai_search*` / `dispatch_namespaces` / `secrets_store_secrets` / `send_email` / `browser` / `images` / `containers` / `flagship` / `pipelines` / `vpc` / `placement` / `site` | Rejected |
| 未知字段 | Rejected（显式报错） |

## 打包

- Go + esbuild（无 Node）；产物自定格式（module manifest + assets），不追求与 Wrangler 输出一致；
- 以**某个确切的 Wrangler 版本作为语义基线**做离线契约测试；
- 优先支持框架/Vite 预构建产物（Next/OpenNext、SvelteKit、Astro 等）。

## 定稿（ADR-153）

- **`compatibility_flags` 精确列表**：权威是从固定上游 revision 生成的 `internal/workerdcompat/manifest.json`；Go 的 `KnownCompatibilityFlags` 与 Bun 的 `workerd-compat.generated.ts` 都消费该清单。未知/experimental flag 部署期失败关闭；契约测试和真实二进制 probe 把 workerd pin `1.20260916.1`、兼容上限 `2026-09-23` 与允许列表绑成一个决定。
- **框架预构建产物验收**：`internal/wrangler TestFrameworkPrebuiltLayouts` 覆盖 OpenNext（`.open-next/worker.js` + `.open-next/assets`）、SvelteKit（`.svelte-kit/cloudflare/_worker.js` + 同目录资产）、Astro（`dist/_worker.js/index.js` + `dist/client`）：配置映射 + `bundler.Build` 打包。
- **不做的项**：见"Rejected"行的原因说明（Cache API/浏览器渲染/Email/Python 等）；Vectorize 已支持（ADR-158/159）。

_最后更新：2026-09-23_
