# Wrangler / Workers 兼容

目标：让用户沿用 Workers 编程模型；原版 Wrangler 可使用 `wrangler.jsonc`/`wrangler.toml`，Go 版 `cellhive deploy --config` 当前解析 JSONC/JSON，本地打包并发布到 CellHive 平台，**绝不触碰 Cloudflare**。

## 三板斧

1. **配置解析**：Go CLI 解析 `wrangler.jsonc`/`.json`，应用环境覆盖与兼容校验，再映射到平台元数据；原版 Wrangler 和 Bun dev CLI 可读取 TOML；
2. **打包**：Go CLI 内嵌 **esbuild Go SDK**（esbuild 本身是 Go），进程内打包，**无 Node 依赖**；
3. **CLI 面**（实际）：`deploy`（`--config <wrangler.jsonc>` 映射 main/bindings/vars/crons/assets/migrations，或用显式 flags）、`bundle build`（Go+esbuild 打包）、`app`/`resource`（资源显式登记，**无 auto-provisioning**）、`versions`/`deployments`/`promote`/`rollback`、`queues consumer|purge`、`workflows trigger|instances`、`vectorize`、`route`、`secret`、`worker delete`、`tail`、`status`/`capacity`/`diagnose`/`audit`、`gc bundles|assets`、`asset put`。`dev` 在 Bun CLI（`cli/`）。

> 本方案为 **Wrangler 兼容子集**：严格拒绝未知/不支持字段，并维护兼容矩阵（见 [`compatibility-matrix.md`](./compatibility-matrix.md) 规划）与契约测试。

## 打包（Go + esbuild，无 Node）

- 使用 esbuild Go API（`api.Build`）在进程内打包：`format=esm`、`platform=neutral`、`conditions=workerd,worker,browser`，处理 `cloudflare:*`/`node:*` externals、`define`、`keep_names`、`minify`、`sourcemap`、`tsconfig`；
- `rules` 映射到 esbuild loader：`Text` / `Data` / `CompiledWasm`；
- `no_bundle`：跳过打包，按 `rules` + `find_additional_modules` + `base_dir` 收集模块；
- 产物格式由 CellHive 自定（module manifest + assets），不追求与 Wrangler 输出格式一致；
- 用户仍可选择性地自带框架/Vite 预构建产物（优先支持，见下）。

**代价**：需长期跟随 Wrangler 的**语义**演进（配置默认值、继承规则、边界 case），靠兼容矩阵 + 契约测试 + 严格拒绝兜底。

## 配置解析

- 格式：**jsonc + toml 都支持**（`cellhive dev`）；**`cellhive deploy --config` 只解析 jsonc/json**（不引第三方 TOML 库；toml 用户可转 jsonc 或用 dev）；
- 环境：`env.<name>` → 每环境独立 app/namespace；保留"绑定不可继承"、top-level-only 的语义；
- 校验：至少 `name`、`main`、`compatibility_date` 必需（assets-only 可省 `main`）；给出明确错误码。

## 支持矩阵（概要）

| 类别 | 字段 | 处理 |
|---|---|---|
| 核心 | `name` `main` `compatibility_date` `compatibility_flags` `rules` `build` `no_bundle` `find_additional_modules` `base_dir` `preserve_file_names` `minify` `keep_names` `define` | 解析并保留；`build.command` 仅在本地执行，服务端不跑。`tsconfig` 字段可解析，但显式自定义路径尚未传给 esbuild；当前只使用入口目录旁的 `tsconfig.json` |
| 绑定 | `vars` `secrets`(声明) `kv_namespaces` `d1_databases` `r2_buckets` `queues.producers/consumers` `workflows` `services` `durable_objects.bindings` `ai` `hyperdrive` `assets` `triggers.crons` | ✅ 映射到 binding/cell（hyperdrive：origin URL 存为注册资源（信封加密），`id` 按资源名解析；平台不池化，ADR-125/129；service 的 `service` 可为 `ns/worker`，跨 ns 由目标侧 `service-acl` 授权，ADR-144）|
| DO 生命周期 | 新 `exports`（声明式）+ 旧 `migrations`（命令式） | ✅ `created`/`sqlite`（及 `new_classes`/`new_sqlite_classes`）、`renamed_classes`、`deleted_classes`、同 worker `transferred_classes {from,to}`；**拒绝**带 `script_name` 的跨 worker transfer |
| 路由 | `workers_dev` `route`/`routes` `custom_domain` `preview_urls` | `workers_dev` → `<ns>.<domain>/<worker>/`；自定义域/routes 可选映射到 Traefik host 路由（由 user-runtime 解析 ns/worker），否则拒绝；`preview_urls` 拒绝 |
| 观测/限制 | `observability` `logpush` `limits` `tail_consumers` | 部分映射或拒绝 |
| 环境 | `env.<name>` | 每环境独立 app/namespace |
| 自动 provisioning | 无 id 自动创建资源 | **拒绝**；生产资源先通过 Wrangler 资源命令显式创建，再把返回 ID 写入配置；部署不会隐式创建资源 |
| 不支持 | `ai_search*` `dispatch_namespaces` `secrets_store_secrets` `send_email` `browser` `images` `containers` `flagship` `pipelines` `vpc` `placement` `site` | 显式拒绝 + 错误码 |
| `vectorize` | **支持**（ADR-158）：`vectorize[{binding,index_name}]` → 注册资源（kind `vectorize`，配置 dims/metric）；`cellhive wrangler vectorize ...` 前缀透传原生命令；原版 Wrangler API 已提供 V2 `create/list/get/delete/info`，其余原版 Vectorize 数据面命令尚未对齐。原生 `--deprecated-v1` 接受但按 V2 处理，无 V1 兼容模式 |

## `cellhive wrangler` 前缀（ADR-138）

`cellhive wrangler <命令>` 是 wrangler 风格的**薄别名 + 参数翻译**：命令名映射到原生 CLI，`deploy` 无 `-c/--config` 时**自动发现**当前目录的 `wrangler.jsonc`/`wrangler.json`，不支持的 wrangler flag 给出可执行的替代提示。

| wrangler 用法 | 映射到 |
|---|---|
| `wrangler deploy --namespace <ns> [-c file] [--env e] [--name w] [其它原生 flag]` | `cellhive deploy <ns> <w> --config <file> …`（`worker` 缺省取配置 `name`） |
| `wrangler delete <ns> <worker>` | `cellhive worker delete` |
| `wrangler versions list` / `wrangler deployments list` | `cellhive versions list` / `cellhive deployments list`（后者读取持久部署事件） |
| `wrangler deployments status` | `cellhive deployments status <ns> <worker>`（读取当前 release） |
| `wrangler queues consumer add/list/remove` / `queues purge` | `cellhive queues consumer add/list/remove <ns> <queue> [worker]` / `cellhive queues purge <ns> <queue>` |
| `wrangler workflows trigger` / `workflows instances ...` | `cellhive workflows trigger <ns> <workflow>` / `cellhive workflows instances ... <ns> <workflow> [id]` |
| `wrangler rollback` / `wrangler promote` | 同名命令 |
| `wrangler secret put\|get\|delete\|list` | `cellhive secret ...`（含 `bulk`） |
| `wrangler tail` | `cellhive tail` |
| `wrangler d1 <verb> ...` | `cellhive d1 create\|list\|delete\|stats <ns> [name]`；数据面支持 `execute\|query\|export <ns> <db> ...` 与 `migrations create\|list\|apply` |
| `wrangler r2 bucket <verb> ...` | `cellhive r2 create\|list\|delete\|stats <ns> [name]`；数据面支持 `object put\|get\|delete\|list` |
| `wrangler kv namespace\|key ...` | `cellhive kv ...`；`key put\|get\|delete\|list <ns> <binding> [key] [value]` 通过 scoped token 访问数据面；另支持 `bulk put\|delete` |
| `wrangler triggers deploy` | 提示用 `cellhive deploy --config wrangler.jsonc`（`triggers.crons` 原子随版本应用） |

- **命名空间**：CellHive 无账号概念，`--namespace <ns>`（= 租户）必填；
- **`deploy` flag**：`--dry-run`（服务端预检不落状态）、`--var NAME=VALUE`、`--secrets-file <JSON>` 已支持并透传；其余不支持的 flag 直接报错并给替代（如 `--minify`→`build.minify`、`--compatibility-date`→`compatibility_date`）；
- **兼容命令组**：`versions`/`deployments`/`triggers`/`workflows`/`queues`/`types`/`init` 转发到原生实现或**结构化拒绝 + 替代说明**（ADR-148）；`d1`/`r2`/`kv` 转发到原生每域命令，已支持上述常用数据面命令；`vectorize`/`hyperdrive` 前缀透传到原生实现；
- **表驱动的已知命令覆盖**：`translateWrangler` 内置已知 wrangler 命令的"映射或替代"表（`wranglerReject`），**任何已知命令都不会落入 `unknown command`**；未知名字才报 unknown。旧式 `kv:bulk` 映射到 `kv bulk`。明确不支持（带替代）：`dev`→`cellhive dev`、`pages`→用 `assets` 部署 Worker、`dispatch`/`containers`/`pubsub`/`mtls-certificate`/`cert` → 不支持、`login`/`logout`/`whoami`→无 CF 账号（用 `cellhive creds`/`status`）、`check`→`cellhive deploy --dry-run`、`docs`→仓库 `docs/`、`telemetry`→无遥测、`unstable_*`→内部命令；
- **子命令级替代**：`versions upload`→`cellhive deploy`（原子发布）、`versions view`→`cellhive versions view`、`triggers deploy`→`cellhive deploy --config`（crons 随版本原子应用）；`queues consumer list\|add\|remove` 和 `queues purge`、`workflows trigger` 与 `workflows instances list\|describe\|pause\|resume\|terminate\|restart\|send-event\|delete` 均有原生命令；`types` 可用 `cellhive types` 生成配置 Env 声明，`init` 可用 `cellhive init` 创建最小 Worker；

**Wrangler 4.133.0 验收**：隔离管理员服务上的真实 CLI 测试覆盖 `deployments list/status`、`versions list`、`queues consumer add/list`、`queues purge`、`workflows trigger`、`workflows instances list/describe`、`vectorize create/list/get/info`，并检查持久控制、Queue、Workflow 状态。`deployments list` 返回事务内提交的部署事件；`status` 取当前最新事件。Workflow `describe` 仅展示已持久化的完成步骤与输出，没有完整尝试日志、步骤计时或运行中步骤。列表支持页码、状态和 RFC3339 时间过滤；`restart --from` 不支持。Queue consumer `retry_delay` 不支持且显式报错；purge 通过 Queue owner/capture 路径写入。Vectorize 管理仅覆盖 V2 的 create/list/get/delete/info；点积 metric 不支持。Hyperdrive 原版管理命令仍未开放，因为其缓存/mTLS/连接池等语义与当前原生加密 origin URL 资源不同。上述验收不代表每个列出的生命周期命令都已用原版 Wrangler 独立跑过。

**D1 SQL migrations**：`cellhive d1 migrations create\|list\|apply <ns> <db> [message] [--directory migrations]` 与原版 Wrangler 4.133.0 的远端迁移共用迁移表；Wrangler `migrations_table` 与原生 CLI `--table` 可选择同一个自定义表。`list` 显示待执行文件，`list --applied` 显示已执行记录。每个文件的 SQL 与记录同事务提交，失败整文件回滚。两套命令提交的标准迁移均另存 SHA-256；原生 CLI 每次重查文件，可拒绝同名内容变更；摘要计算忽略 SQL 文件首尾空白，以匹配 Wrangler 提交行为。Wrangler CLI 会跳过已应用文件，改写本地同名文件后重跑不会向服务端提交，因而无法检测该变更。原版 Wrangler 的 `d1 execute --remote --command/--file`、`d1 migrations list/apply --remote` 已真实对拍；`--file` 上传限 8 MiB，按文件 SHA-256 幂等执行。上传 session 保存在单个管理员实例内，10 分钟过期；负载均衡需保持该三段请求落在同一实例，中途节点退出可重跑命令。Wrangler 本地 D1 仍由其 Miniflare 管理，不连接生产 CellHive。

**新增命令对齐**：原版 Wrangler 4.133.0 的 `d1 export --remote`、`kv bulk put/delete --remote`、`secret bulk`、`versions view` 均有对应的 CellHive 原生命令。D1 导出上限 32 MiB，省略内层事务标记，可由 `d1 execute --file` 恢复；下载链接保存在单管理员实例一小时。KV bulk 先校验整批，再逐 key 写入，后续失败可能留下前面已写的 key。Secret bulk 在控制 cell 内一次事务提交（1-100 项），JSON `null` 删除对应 Secret。

**类型生成**：`cellhive types [worker-configuration.d.ts] [--config wrangler.jsonc] [--env name] [--env-interface Env] [--check]` 生成配置里的 vars、bindings、assets 的 `Env` 接口；变量声明为 `string`，不写入值。完整运行时声明仍使用 `@cloudflare/workers-types` 或原版 `wrangler types`。原版命令是本地操作，不连接 CellHive；原生 Go CLI 当前只解析 JSONC/JSON，`wrangler.toml` 可由原版命令处理。

**项目初始化**：`cellhive init <worker-name> [--yes]` 创建 `wrangler.jsonc` 与 `src/index.js`，不安装依赖，也不覆盖现有目录；`cellhive wrangler init` 同样可用。原版 `wrangler init` 继续执行它自己的本地向导，不需要平台 API。

**KV/R2 数据面**：原版 Wrangler 4.133.0 的 `kv key put/get/list/delete --remote` 与 `r2 object put/get/delete --remote` 已真实对拍；等效功能也通过 `cellhive kv key ...`、`cellhive r2 object ...` 提供（CellHive 命令额外传 `<namespace>`）。
- **原版 Wrangler API 兼容**：管理员端提供 Cloudflare API 形状的 `PUT/POST /client/v4/accounts/:account_id/workers/scripts/:script_name/{content,versions}`，并接受原版 Wrangler 的 Bearer token 与 `content/v2` multipart（`metadata` + 主模块）。`account_id` 映射为 CellHive namespace；上传后转为 CellHive bundle 并创建版本。`/services/:name`、`/versions` 和 `/deployments` 已提供真实查询/激活语义。使用 `CLOUDFLARE_API_BASE_URL=<CellHive admin>/client/v4`、`CLOUDFLARE_API_TOKEN=<CellHive admin token>`、`CLOUDFLARE_ACCOUNT_ID=<namespace>`。
- **原版 Wrangler API 资源语义**：管理员兼容端点同时提供 KV/D1/R2/Queues 的 create/list/delete（分别映射到 `storage/kv/namespaces`、`d1/database`、`r2/buckets`、`queues`），以及 Workflow 的 create/list/get/delete。Worker 版本 list/get、Cron schedules、secret get 也已覆盖。Workflow 的 `concurrency.limit`、`schedules`、`default_retention.success_retention/error_retention` 会持久化并执行；`limits` 仍显式拒绝。Assets 使用 Wrangler 的 `assets-upload-session` + JWT 单文件或 multipart bulk 上传协议，上传内容进入版本资产 token。资源端点只登记 CellHive 资源，不改变“资源先创建、部署引用 ID”的控制面约束；绑定 ID 与资源登记名均可用于部署校验。Secrets 和 Workers routes 也提供 Cloudflare 形状的 CRUD。
- **兼容边界**：复杂多模块上传、官方 Wrangler 的本地 dev/login 及未列出的 Cloudflare 专有资源 API 不属于当前兼容端点。账号发现、Worker 删除、版本查询与按版本发布已提供；tail 有短期 session 和 WebSocket 日志推送。Workflow/Queue/Vectorize/Hyperdrive 的命令级边界见上文和兼容矩阵。

## `compatibility_flags` authority（ADR-153/186）

跟随 pinned workerd（`internal/workerdbin.PinnedVersion`，当前 `1.20260916.1`），部署期验证；未知 flag 与上游标记为 experimental 的 flag 均失败关闭。完整列表不再手写在文档或校验器中：`cmd/workerd-compat-gen` 从固定 revision 的 `compatibility-date.capnp` 生成 `internal/workerdcompat/manifest.json`，再生成 Bun 使用的 `cli/src/workerd-compat.generated.ts`。Go 控制面直接消费 manifest；契约测试逐项比对两个 consumer。

兼容上限 `2026-09-23` 与该 pin/manifest 绑定，并由 `scripts/workerd-compat-probe.sh` 对真实二进制验证最大日期、下一日拒绝、全部允许 flag 与未知 flag 拒绝。**框架预构建产物**（OpenNext/SvelteKit/Astro）逐项验收见 `internal/wrangler TestFrameworkPrebuiltLayouts`。

## 拒绝策略

- 未知/不支持字段**显式报错**，不静默忽略（避免运行时才崩）；
- 错误信息包含字段路径、原因、以及"是否计划支持"；
- 维护 `compatibility-matrix.md`，按运行时 + Wrangler 配置两个维度记录 Supported / Partial / Rejected；
- 对固定的 Wrangler 语义基线做**离线契约测试**。

## 路由

- `workers_dev`（默认 true）→ 命名空间服务路径 `<ns>.<platform-domain>/<worker>/`；
- 自定义域 / `routes` / `custom_domain`：**可选**映射到 Traefik 的 host 路由，再由 `user-runtime` 解析 ns/worker；不可用时显式拒绝；
- `preview_urls`：不支持（拒绝）。
- 入口 = Traefik（TLS/host 分流）+ `user-runtime`（Worker 路由/版本解析/清头）；**不设独立 gateway**（ADR-017）。

## Assets（本期补齐完整管道）

CF static-assets 语义需覆盖：

- 静态目录上传 + **内容哈希 + 版本绑定**（rollback 翻转资产 URL）；
- `_headers` / `_redirects`（含规则数量上限校验）；
- `not_found_handling`（如 SPA 回退）；
- `run_worker_first`（先 Worker 后资产的路由）；
- `.assetsignore`；
- 边缘资产服务：ETag / `If-None-Match` 304、`cache-control`；
- 本地 `cellhive dev` 的资产服务；
- 与 SSR 框架（Next/OpenNext 的 `.open-next/assets`、SvelteKit、Astro）的资产期望对齐。

## 框架 / Vite 预构建产物（优先支持）

Next/OpenNext、SvelteKit、Astro、Nuxt(nitro)、Qwik、SolidStart 等通常自带构建，产出 Worker + 配置 + 资产。此路径下我们主要做**配置解析 + 模块/资产收集 + 上传**，不参与打包，成本最低，应优先支持。

## 本地开发

- 提供自带 `cellhive dev`：**Bun CLI + Miniflare**（真实 workerd + 本地模拟绑定），不需要 Docker/云 bucket，dev 机器零 Go 进程（ADR-065）；
- 版本 pin：Miniflare 自带 workerd override 到平台 pinned 版本；
- 预览/环境选择与 `env.*` 对齐；
- deploy 有**服务端功能/兼容性拦截**（支持矩阵；`images/ai/browser/...` 拒绝），dev/CLI 提前警告；
- 完整设计（进程模型/版本 pin/契约对拍/用户代码兼容/CLI/差异/验收）见 [`dev-mode.md`](./dev-mode.md)。

## Pin 与契约测试

- Pin：**workerd 版本** + **Wrangler 语义基线版本**（作为参考，不引入其运行时）；
- 契约测试：对固定基线校验配置归一化、绑定映射、拒绝行为、产物形状；
- 升级是显式动作，必须跑契约测试。

## 安全 / 隐私

- `cellhive deploy` 不调用 Cloudflare API；原版 `wrangler deploy` 可以通过 `CLOUDFLARE_API_BASE_URL` 指向 CellHive 管理端。用户须自行关闭 Wrangler 遥测（如 `WRANGLER_SEND_METRICS=false`）；
- 当前生产 CLI 不内嵌或代启动 Wrangler；固定 Wrangler 4.133.0 仅用于兼容测试与用户自行调用；
- 默认路径（Go+esbuild）不产生任何对外遥测。

## 与 cf 运行时的能力边界

- SSR：继承 workerd 生态，但平台必须**开启 `nodejs_compat`（含 v2）**并补齐资产管道；
- `nodejs_compat` 默认在 compatibility date ≥ 2026-08-04 时启用；需要关闭的 worker 须同时设 `no_nodejs_compat` 与 `no_nodejs_compat_v2`；
- 不支持：Python Workers、Cache API、Browser Rendering、Email Workers 等（与 workerd 暴露面一致）；Vectorize/Hyperdrive 由平台自实现（ADR-158/129）。

_最后更新：2026-09-18_

## assets 路由配置（ADR-071）

支持 `assets.directory`、`assets.binding`、`assets.not_found_handling`（`none`/`404-page`/`single-page-application`）、`assets.run_worker_first`（bool 或路径数组）。deploy 时随不可变版本存储并由 loader 应用（真实 workerd e2e）。旧 `[site] bucket` 仍映射为 directory。

## DO migrations 支持矩阵（ADR-081）

| 迁移 | 支持 | 说明 |
|---|---|---|
| `tag` / `new_classes` / `new_sqlite_classes` | ✅ | 类创建（我们动态加载，声明仅作校验） |
| `renamed_classes` | ✅ | 记录 `codeClass→storageClass` 别名，改名**保数据**（ADR-082） |
| `deleted_classes` | ✅ | 标记删除 + `/v1/do/delete` 物理回收 facet 存储 |
| `transferred_classes`（同 worker `{from,to}`） | ✅ | 等价于 rename：把 `from` 的存储身份交给 `to`（复用 ADR-082 别名注册表；`to` 已存在则 fail-closed） |
| `transferred_classes` 带 `script_name`（跨 worker） | ❌ `invalid_migration` | 跨 worker 转移不支持：对象按 worker/storage_id 命名，跨 worker 无法安全搬运 |
| 其它字段 | ❌ `unknown_migration_field` | 显式拒绝 |
| 形状错误 | ❌ `invalid_migration` | 如 `{from}` 缺 `to` |

代码变更（同名 class、新 bundle）**不需要 migration**：由 facet 级惰性重启生效（`doStorageId` 保证存储不变）。
