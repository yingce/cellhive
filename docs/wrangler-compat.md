# Wrangler / Workers 兼容

目标：让用户沿用标准 `wrangler.jsonc`/`wrangler.toml` 与 Workers 编程模型，`cellhive deploy` 完成本地打包并发布到 CellHive 平台，**绝不触碰 Cloudflare**。

## 三板斧

1. **配置解析**：解析 `wrangler.jsonc`/`.json`/`.toml` + 环境继承 + schema 校验，映射到平台元数据；
2. **打包**：Go CLI 内嵌 **esbuild Go SDK**（esbuild 本身是 Go），进程内打包，**无 Node 依赖**；
3. **CLI 面**（实际）：`deploy`（`--config <wrangler.jsonc>` 可直接映射 main/bindings/vars/crons/assets/migrations，或用显式 flags）、`bundle build`（Go+esbuild 打包）、`app`/`resource`（资源显式登记，**无 auto-provisioning**）、`promote`/`rollback`/`releases`、`route`、`secret put|get`、`worker delete`、`workflow create`、`tail`、`status`/`capacity`/`diagnose`/`audit`/`routes`、`gc bundles|assets`、`asset put`。〔`dev` 在 Bun CLI（`cli/`），不是这个二进制。〕

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
| 核心 | `name` `main` `compatibility_date` `compatibility_flags` `tsconfig` `rules` `build` `no_bundle` `find_additional_modules` `base_dir` `preserve_file_names` `minify` `keep_names` `define` | 解析并保留；`build.command` 仅在本地执行，服务端不跑 |
| 绑定 | `vars` `secrets`(声明) `kv_namespaces` `d1_databases` `r2_buckets` `queues.producers/consumers` `workflows` `services` `durable_objects.bindings` `ai` `hyperdrive` `assets` `triggers.crons` | ✅ 映射到 binding/cell（hyperdrive：origin URL 存为注册资源（信封加密），`id` 按资源名解析；平台不池化，ADR-125/129；service 的 `service` 可为 `ns/worker`，跨 ns 由目标侧 `service-acl` 授权，ADR-144）|
| DO 生命周期 | 新 `exports`（声明式）+ 旧 `migrations`（命令式） | ✅ 仅 `created`/`sqlite`（及 `new_classes`/`new_sqlite_classes`）；**拒绝** rename/delete/transfer、`script_name` |
| 路由 | `workers_dev` `route`/`routes` `custom_domain` `preview_urls` | `workers_dev` → `<ns>.<domain>/<worker>/`；自定义域/routes 可选映射到 Traefik host 路由（由 user-runtime 解析 ns/worker），否则拒绝；`preview_urls` 拒绝 |
| 观测/限制 | `observability` `logpush` `limits` `tail_consumers` | 部分映射或拒绝 |
| 环境 | `env.<name>` | 每环境独立 app/namespace |
| 自动 provisioning | 无 id 自动创建资源 | **拒绝**；资源由控制面显式管理 |
| 不支持 | `ai_search*` `dispatch_namespaces` `secrets_store_secrets` `send_email` `browser` `images` `containers` `flagship` `pipelines` `vpc` `placement` `site` | 显式拒绝 + 错误码 |
| `vectorize` | **支持**（ADR-158）：`vectorize[{binding,index_name}]` → 注册资源（kind `vectorize`，配置 dims/metric）；CLI `wrangler vectorize create|list|delete|get|info|insert|upsert|query|get-vectors|delete-vectors|list-vectors|create-metadata-index|list-metadata-index|delete-metadata-index` 前缀透传；`--deprecated-v1` 接受但语义同 V2（无 V1 兼容模式） |

## `cellhive wrangler` 前缀（ADR-138）

`cellhive wrangler <命令>` 是 wrangler 风格的**薄别名 + 参数翻译**：命令名映射到原生 CLI，`deploy` 无 `-c/--config` 时**自动发现**当前目录的 `wrangler.jsonc`/`wrangler.json`，不支持的 wrangler flag 给出可执行的替代提示。

| wrangler 用法 | 映射到 |
|---|---|
| `wrangler deploy --namespace <ns> [-c file] [--env e] [--name w] [其它原生 flag]` | `cellhive deploy <ns> <w> --config <file> …`（`worker` 缺省取配置 `name`） |
| `wrangler delete <ns> <worker>` | `cellhive worker delete` |
| `wrangler versions list` / `wrangler deployments list` | `cellhive releases` |
| `wrangler rollback` / `wrangler promote` | 同名命令 |
| `wrangler secret put\|get\|delete\|list` | `cellhive secret ...` |
| `wrangler tail` | `cellhive tail` |
| `wrangler d1 <verb> ...` | `cellhive d1 create\|list\|delete\|stats <ns> [name]`（数据面 verb 如 `execute` 结构化拒绝 → 用 worker 的 D1 绑定） |
| `wrangler r2 bucket <verb> ...` | `cellhive r2 create\|list\|delete\|stats <ns> [name]`（`object` 等数据面 verb 结构化拒绝 → 用 R2 绑定） |
| `wrangler kv namespace\|key ...` | `cellhive kv ...`（namespace verbs 走原生；`key` 数据面结构化拒绝 → 用 KV 绑定） |
| `wrangler triggers deploy` | 提示用 `cellhive deploy --config wrangler.jsonc`（`triggers.crons` 原子随版本应用） |

- **命名空间**：CellHive 无账号概念，`--namespace <ns>`（= 租户）必填；
- **`deploy` flag**：`--dry-run`（服务端预检不落状态）、`--var NAME=VALUE`、`--secrets-file <JSON>` 已支持并透传；其余不支持的 flag 直接报错并给替代（如 `--minify`→`build.minify`、`--compatibility-date`→`compatibility_date`）；
- **兼容命令组**：`versions`/`deployments`/`triggers`/`workflows`/`queues`/`types`/`init` 转发到原生实现或**结构化拒绝 + 替代说明**（ADR-148）；`d1`/`r2`/`kv` 转发到**原生每域命令**（ADR-157，仅数据面 verb 拒绝）；`vectorize`/`hyperdrive` 前缀透传到原生实现；
- **表驱动的已知命令覆盖**：`translateWrangler` 内置已知 wrangler 命令的"映射或替代"表（`wranglerReject`），**任何已知命令都不会落入 `unknown command`**；未知名字才报 unknown。明确不支持（带替代）：`dev`→`cellhive dev`、`pages`→用 `assets` 部署 Worker、`dispatch`/`containers`/`pubsub`/`mtls-certificate`/`cert` → 不支持、`login`/`logout`/`whoami`→无 CF 账号（用 `cellhive creds`/`status`）、`check`→`cellhive deploy --dry-run`、`docs`→仓库 `docs/`、`telemetry`→无遥测、`kv:bulk`→数据面（用 KV 绑定）、`unstable_*`→内部命令；
- **子命令级替代**：`versions upload`→`cellhive deploy`（原子发布）、`versions view`→`cellhive releases`、`triggers deploy`→`cellhive deploy --config`（crons 随版本原子应用）、`queues consumer`→`queues.consumers` + `deploy`（积压看 `queue status`）、`workflows status\|describe\|trigger`→`env.WF`（实例是运行期的）、`d1 execute`/`r2 object`/`kv key`→Worker 绑定 + `cellhive dev`、`types`/`init`→不支持（自己写 `Env` 接口 / 自建 wrangler.jsonc）；
- **真 wrangler 兼容（备选，未做）**：实测 wrangler 4.133.0 读 `CLOUDFLARE_API_BASE_URL`（旧名 `CF_API_BASE_URL`）与 SDK 的 `CLOUDFLARE_BASE_URL`，故可实现 CF API 子集让其原样运行；代价是跟随 CF API 漂移 + `content/v2` multipart 上传格式对拍，作为独立 spike，不在本条范围。

## `compatibility_flags` 支持列表（ADR-153）

跟随 pinned workerd（`internal/workerdbin.PinnedVersion`，当前 `1.20260615.1`），部署期验证（`unknown_flag` 失败关闭），Go 校验器与 dev CLI 列表镜像并由测试强制一致：

`nodejs_compat`、`nodejs_compat_v2`、`nodejs_compat_populate_process_env`、`no_handle_cross_request_promise_resolution`、`global_fetch_strictly_public`、`disable_fetch_stream_teeing`、`streams_enable_constructors`、`transformstream_enable_standard_constructor`、`export_commonjs_default`、`export_commonjs_namespace`、`disable_nodejs_process_v2`、`enable_ctx_exports`、`deployment_id_header`、`require_custom_ports_development`。

兼容上限 `2026-06-22` 与该 pin 绑定（`TestPinPairsWithCompatibilityDate`）。**框架预构建产物**（OpenNext/SvelteKit/Astro）逐项验收见 `internal/wrangler TestFrameworkPrebuiltLayouts`。

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

- **绝不调用 Cloudflare API**，不做真实 `wrangler deploy`；
- 若未来提供"可选高保真路径"（调用本机 pinned wrangler），必须强制关闭 `send_metrics` 与 `dependencies_instrumentation`；
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
