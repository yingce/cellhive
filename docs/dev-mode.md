# `cellhive dev`（本地开发模式）

> 状态：**M1/M2 已实现**（`cli/` Bun dev + `internal/wranglercompat` 服务端拦截，2026-09-15）；要点摘要见 [`known-issues.md`](./known-issues.md) M-08，剩余见该节。
> 相关：[`wrangler-compat.md`](./wrangler-compat.md)（打包/配置/资产）、[`bindings.md`](./bindings.md)（binding 映射）、[`control-plane.md`](./control-plane.md)（deploy 与服务端拦截）、[`routing.md`](./routing.md)、[`deployment.md`](./deployment.md)、[`observability.md`](./observability.md)。

## 0. 一句话设计

**dev = Bun CLI + Miniflare**：`cellhive dev` 是 Bun 写的工具，内部用 **Miniflare** 起真实 workerd + 模拟绑定，**dev 机器上不跑任何 Go 后端进程**；**生产**仍是 Go workerd + cell-agent（不变）。用户代码靠"两边都对齐 Cloudflare 契约"保持兼容；平台特性（版本/路由/密钥/deploy/scope/RPO=0）不在 dev，走真实平台。

## 1. 目标与非目标

**目标（租户 DX）**
- **零外部依赖**：不需要 Docker / 云 bucket / MinIO / Traefik / DNS 配置；`cellhive dev` 一条命令起来。
- **标准 CF 形态**：租户代码与 `wrangler.jsonc` 与 CF/`wrangler dev` 一致；KV/D1/R2/Queue/DO/`vars`/`secrets`/assets 可用（Miniflare 内建）。
- **热重载 / inspector / persist**：直接用 Miniflare 的能力，不自造。
- **用户代码零改动**：同一份代码在 CF、`wrangler dev`、`cellhive dev`、`cellhive deploy` 到平台都能跑（前提：契约对齐，见 §10）。

**非目标**
- dev **不验证平台**：不跑 cell-agent / 控制面 / 复制 / owner / lease / scope token / RPO=0 输出门（这是与"真实栈 dev"的核心取舍；平台用契约测试 + 真实环境验证）。
- 多节点 HA / drain / handoff / fleet；Traefik / TLS / OIDC / 多租户网络边界。
- 生产规模与性能门（见 [`p0-report.md`](./archive/p0-report.md)）。

## 2. 进程模型

```
cellhive dev  (Bun, 一个进程，零 Go)
└── Miniflare (npm, 进程内)
      ├── workerd 子进程（Miniflare 自带启动；pin 到平台版本，见 §3）
      ├── 模拟绑定：KV/D1/R2/Queue/DO/Cache/Service/Assets（本地实现）
      ├── 热重载 watch + inspector + persist
      └── 本地 HTTP 服务 + 路由（host/path）
```

- **Bun 承载 Miniflare（M1 已验证 ✅）**：`bun 1.4.0` 完整启动 Miniflare（用 Bun 的 Node 兼容层成功 spawn workerd 子进程）并**以 HTTP dev server 形态**服务 KV/D1/R2/`vars`，冷启动 ~150ms。`require`/启动/热路径均无阻塞问题。
- **CLI 其余命令**（`deploy/promote/secret/routes`）本来就是 HTTP 客户端，调 cell-agent admin API（`:8082`），与 dev 无耦合。
- **无 Go**：`cellhive dev` 不 require / spawn 任何 cell-agent、supervisor、FSBucket 代码。

### 2.1 为什么用 Miniflare 而不是 wrangler（澄清"Miniflare 是否过时"）

- **Miniflare 没过时，它就是 wrangler 的引擎**：`wrangler@4.x` 的 dependencies 直接包含 `"miniflare"`（+ `workerd`），发行包里带 `miniflare-dist/`；两者同属 `cloudflare/workers-sdk` monorepo。CF 文档也写明"多数用户用 Wrangler"，Miniflare API 面向 **advanced/programmatic** 用例——这正是我们自己写 CLI 的场景。
- 让人误以为"过时"的是 **Miniflare 2.x 的独立 CLI**（2023 年 v3 用 workerd 重写后不再主推）；**v3/v4 是库**，被 `wrangler dev`、`@cloudflare/vite-plugin`、`vitest-pool-workers` 共同使用。
- **取舍**：直接嵌 Miniflare → 用 wrangler 同一个引擎、可自控配置翻译与 workerd pin、依赖轻；调 `wrangler dev` → 依赖重（CLI，`engines: node>=22`）、配置/打包语义由它定、难嵌入。**我们选前者**（本 ADR）。
- 二者**都需 pin workerd**（wrangler 的 `workerd` 也走 catalog/resolution），所以 pin 成本不因选 wrangler 而省。

### 2.2 「应该用什么」——运行时 vs 工具层（含 CF 官方现状）

**运行时引擎只有一个：Miniflare（= workerd）**。`wrangler`、`createTestHarness`、`@cloudflare/vite-plugin`、`vitest-pool-workers` **全部构建在它之上**。所以争议不在引擎，而在"dev server / 工具层"：

| 方案 | 说明 | 代价 | 取舍 |
|---|---|---|---|
| **直接嵌 Miniflare**（本 ADR 选择） | 用引擎本体 + 我们自己的配置翻译/打包 | 需自研 wrangler 配置语义与打包（工作量大头；起 workerd 最简单，已实测 ~150ms） | ✅ 与自有 CLI + Bun + Go 打包器一致；支持面已被 ADR-014 砍窄，翻译量可控 |
| **`@cloudflare/vite-plugin`** | **CF 现官方推荐的 programmatic dev server**（配 Vite `createServer()`）；仍底层用 Miniflare/workerd | 引入 Vite 作为 dev 构建系统，与我们的 Go 打包器/自有 CLI 重叠 | 回退首选（若配置保真成本过高） |
| **`wrangler dev`** | 最完整的 CF 语义 | 重（CLI，`engines: node>=22`）、打包/配置语义由其决定、难嵌入 | 仅作 `--wrangler` 透传后路 |

**关键事实（CF 文档，2026-07-27）**：`unstable_startWorker` / `unstable_dev` **已废弃**；CF 改为推荐——测试用 `createTestHarness()`（wraps Miniflare，可直接读 wrangler 配置），programmatic dev server 用 **Cloudflare Vite plugin**。因此我们的"后路"应指向 Vite plugin，**不要**依赖已废弃的 wrangler programmatic API。

**结论**：CellHive 继续"直接嵌 Miniflare"——**成本重心是配置翻译 + 打包**（我们本来就要在 P2 做 Go+esbuild 打包器），而不是引擎；只有当自研保真不可接受时，才回退到 Vite plugin 复用 CF 的配置/打包。

## 3. 运行时版本 pin（重要）

| 组件 | 版本 | 说明 |
|---|---|---|
| 平台 pinned workerd | **1.20260615.1** | 生产/契约基线（兼容日期上限 2026-06-22，已实测） |
| **选用的 Miniflare** | **4.20260616.0** | 与平台 workerd 同期；其自带 workerd = `1.20260616.1` |
| Miniflare 4.20260714.0（曾试用） | workerd `1.20260714.1` | ❌ 内部 control worker 硬编码 `2026-07-08`，与 pinned 不兼容 |

- **pin 规则（已修正，2026-09-15 实测）**：**Miniflare 版本必须与平台 pinned workerd 同期对齐**，然后把它自己的 `workerd` 依赖 override 到 pinned 版本。
  - ❗ **不能**把"更新的 Miniflare"（如 4.20260714.0）的 workerd 强行 override 到旧的 pinned —— Miniflare 内部 control worker（`MINIFLARE_DEV_CONTROL`）**硬编码** `compatibilityDate`（4.20260714.0 = `2026-07-08`），pinned workerd `1.20260615.1` 只支持到 `2026-06-22`，启动直接失败：`This Worker requires compatibility date "2026-07-08", but the newest date supported by this server binary is "2026-06-22"`。
  - 之前"只做 `require`/`dispatchFetch` 不带端口"的 spike 会**漏掉**这个问题（control 服务在 dev server 模式下才实例化）——已修正。
- 实现：`cli/package.json` 依赖 `miniflare@4.20260616.0` + `overrides: { "workerd": "1.20260615.1" }`（Bun/pnpm 均支持 `overrides`）→ 装出来 `miniflare@4.20260616.0` + `workerd@1.20260615.1`（已实测）。
- **已验证 ✅**：该组合下 `bun` 起 dev server，KV/D1/R2/`vars` 经 HTTP 全通（详见 §14 M1）。
- workerd 二进制来自 Miniflare 的 `workerd` 依赖（`@cloudflare/workerd-linux-64`），无需系统安装。

## 4. 配置翻译（wrangler → Miniflare）

| wrangler | `cellhive dev` 动作 |
|---|---|
| `name` | Miniflare worker 名 + dev 路由的 ns/worker |
| `main` | Miniflare `scriptPath`（或 esbuild 打包后入口，见下） |
| `compatibility_date` / `compatibility_flags` | 传给 Miniflare（并在 CLI 预检 ≤ 平台上限、flag 已知，见 §8） |
| `vars` | Miniflare `bindings`（text/json） |
| `kv_namespaces` / `d1_databases` / `r2_buckets` / `queues` | 映射到 Miniflare 同名 binding（local 实现） |
| `durable_objects` + `migrations` | 映射到 Miniflare DO + SQLite 存储（仅 `created/sqlite`；rename/delete/transfer 拒绝，ADR-014） |
| `assets` | 映射 `directory`/`binding`（含 `[site] bucket`）到 Miniflare assets（静态服务）。**限制：worker+assets interop（asset miss 回退 worker / `env.ASSETS.fetch` / `run_worker_first`）未支持**，dev 会警告 `assets_worker_interop`（需 wrangler 的 asset-router 内部接线） |
| `vars`/`secrets` 声明 | `vars` 直传；`secrets` 从 `.dev.vars` 读取并作为 Miniflare secret bindings |
| `env.<name>` | `--env` 选择；每环境独立 namespace/持久化目录 |
| 不支持的字段 | **显式拒绝**（与 ADR-014 同一校验，见 §8） |

- **打包**：两条路——(a) 默认：Miniflare/wrangler 语义（`.js` 直载；`.ts` 用 `Bun.build`）；(b) `--strict-build`：调用**平台 Go+esbuild 打包器**（`internal/bundler` + `cellhive bundle build <entry> --out f`，ADR-005），dev 与 deploy 同产物。需要 `cellhive` Go 二进制（`make build` 产出 `bin/cellhive`；`CELLHIVE_BIN` 可指定），未找到时报错并提示。**已实现并验证 ✅**。
- `tsconfig`/`rules`/`no_bundle`/`find_additional_modules`/`base_dir`/`minify`/`keep_names`/`define` 按 wrangler-compat.md 处理，能直传 Miniflare 就直传。
- **Miniflare 5 assets 路由边界（ADR-114）**：内建 router 把 not-found handling 与 user-worker fallback 错误耦合到 `has_user_worker`。升级后的 dev CLI 在同一 Miniflare 实例内使用一个入口 worker，通过 service binding 编排原生 asset service 与用户 worker，复现生产顺序；它无额外 listener/进程/凭据/状态，仅属开发工具，绝不进入生产入口或成为 gateway。完整 assets 与 hot-reload smoke 通过前，Miniflare 5 升级状态为实施中。

## 5. 数据目录与持久化

- `./.cellhive-dev/`（`--data-dir` 可改；`--clean` 清空）：
  - `miniflare/`：Miniflare `persist` 根（KV/D1/R2/DO 的本地落盘）；
  - `logs/`：CLI 汇总日志；
  - `dev.json`：CLI 侧态（ns/worker/env、上次 URL）。
- 用 Miniflare 的 `persist`（文件后端），**不自造存储**；`--persist` 语义与 `wrangler dev --persist` 一致。
- `.gitignore` 默认忽略 `.cellhive-dev/`。

## 6. 热重载

- **直接用 Miniflare/wrangler 的 watch**：模块图、`wrangler` 配置、`.dev.vars`、assets；保存即重建。
- 编译错误 → 打印诊断、保留上一版继续服务（Miniflare 行为）；CLI 附加"重新构建中/失败"提示。
- CLI 只在需要时做**额外**动作（如 `--strict-build` 时用平台打包器重建）。

## 7. 绑定支持面与平台差异（关键）

**Miniflare 4.20260714 实测暴露的绑定插件**（`*_PLUGIN_NAME` 导出）：
`kv, d1, r2, queues, do, cache, service(core), assets(worker-loader), workflows, ai, ai-search, vectorize, hyperdrive, images, media, browser-rendering, email, dispatch-namespace, analytics-engine, pipelines, secrets-store, ratelimit, mtls, stream, vpc-networks, vpc-services, websearch, flagship, agent-memory, artifacts, version-metadata, hello-world`。

**这说明 Miniflare 的"认识面"远宽于我们平台**（仍拒绝：`ai_search/dispatch_namespaces/secrets_store/send_email/browser/images/containers/flagship/pipelines/vpc/placement/site`；`vectorize`/`hyperdrive` 已由 ADR-158/129 支持）。因此：

| 绑定 | Miniflare | 我们平台 | 结果 |
|---|---|---|---|
| KV / D1 / R2 / Queue / DO / Cache / Service / Vars / Secrets / Assets | ✅ 本地实现 | ✅ 支持 | 一致 |
| **`env.IMAGES`** | **有 `images` 插件 + 本地 `imagedelivery` worker 路径**；实测 `images:{binding:"IMAGES"}` 可构造且 worker **无凭据即可启动**（→ 本地实现） | **❌ 拒绝**（ADR-014） | **dev 会真的"能用"、deploy 被拦**（最危险的一类，必须靠 §8 兜） |
| `env.AI` / `env.BROWSER`（browser-rendering） | 有插件；**很可能是代理到 Cloudflare / 需账户凭据**（`browser-rendering` 有 localhost 控制路径，需 spike 确认） | ❌ 拒绝（本期） | dev 可能报"需要登录/凭据" |
| `vectorize` | 只有配置 schema（workerd 无该服务，无本地模拟） | ✅ 平台支持（ADR-158），**dev 明确拒绝**（`DEV_UNSUPPORTED_BINDINGS`） | 用 `cellhive vectorize ...` 打真实命名空间 |
| `hyperdrive` | 有插件（`localConnectionString`） | ✅ 支持（ADR-129） | dev 走 Miniflare `hyperdrives` |
| `send_email` / `secrets_store` / `dispatch_namespaces` / `containers` … | 部分有插件 | ❌ 拒绝 | 明确拒绝 |

**结论**：
1. 用 Miniflare 做 dev，**用户可能在本地"用上"平台不支持的绑定**（最典型 `env.IMAGES`）。
2. 因此必须靠 **§8 的 deploy 服务端功能/兼容性拦截**兜住，并让 **CLI/dev 提前警告**（同一校验库）。
3. "Miniflare 认识" ≠ "本地真跑"：AI/Browser 等大概率要 CF 凭据；dev 遇到时应给出"平台不支持 / 需凭据"的明确提示，而不是含糊成功。

## 8. deploy 服务端功能/兼容性拦截（新增，权威）

**原则**：CLI 只是快速反馈；**服务端是权威**（防绕过、防不同 CLI 版本）。校验逻辑放在一个共享库（Go），`deploy` 端点与 CLI preflight 共用。

服务端在 `POST /v1/control/deploy`（及 bundle 上传）校验：
1. **bundle**：`bundle_sha` 在对象存储存在（内容寻址对象存在），`assets` 引用存在。
2. **兼容日期**：`compatibility_date` ≤ 平台支持上限（已知 **2026-06-22**，实测）；超限报错并给出上限。
3. **compatibility_flags**：必须在 pinned workerd 的已知集内（未知 flag 报 `No such compatibility flag` 类错误；`nodejs_compat` 等已知放行）。
4. **绑定**：每个 binding 的 type 必须在**支持矩阵**内；不在则拒绝（`images/browser-rendering/send_email/ai_search/dispatch_namespaces/secrets_store/containers/...`）。平台支持但 dev 无法模拟的（当前仅 `vectorize`）在 dev 启动时**明确报错**。
5. **资源引用**：binding 指向的资源（KV/D1/R2/Queue…）必须已在控制面登记（拒绝自动 provisioning，ADR-014）；否则错误提示对应 `cellhive <kind> create`。
6. **DO 生命周期**：仅 `created`/`sqlite`（含 `new_classes/new_sqlite_classes`）；rename/delete/transfer/`script_name` 拒绝。
7. **未知字段**：显式报错（字段路径 + 原因 + "是否计划支持"）。

错误形状：`{error, message, field_path?}`，稳定 error code（如 `unsupported_binding`、`compat_date_too_new`、`unknown_flag`、`binding_unregistered`）。

**CLI/dev 复用**：`cellhive dev`、`cellhive deploy` 在本地先跑同一校验 → 启动/提交前就报"该绑定平台不支持"。这样即使用 Miniflare 允许，用户也会**在 dev 阶段**收到明确警告，避免"dev 绿、deploy 拒"。

## 9. 契约对拍（Miniflare 作为 oracle）

- 目标：让"用户代码在 dev(Miniflare) 与 prod(我们) 行为一致"**可验证**。
- 方法：固定一组**golden 请求/响应**（KV/D1/R2/Queue/DO 的字段、错误码、边界），分别打到 **Miniflare** 与 **平台 cell-agent**，diff。Miniflare 的响应作为 **CF 语义基线**（沿用 ADR-014 的"pinned Wrangler 语义基线"思路）。
- 差异分两类：
  - 我们**偏离 CF**（bug）→ 修我们的 facade/端点；
  - **有意差异**（如 RPO=0 写延迟、scope/quota）→ 记入 §12 差异表，并要求用户代码不依赖。

## 10. 用户代码兼容性（租户不用关心后端，但字段要对齐）

"后端不可见"成立的前提是**契约保真**。当前 facade（ADR-063 `workerd/platform/facades.js`）相对 CF 的已知缺口，需在 P2 收敛（与 §9 对拍一起做）：

| 面 | CF | 我们当前 | 待补 |
|---|---|---|---|
| KV `list()` | `keys:[{name,expiration?,metadata?}]` | ✅ 含 expiration/metadata（ADR-098，`with_metadata`） | 无 |
| KV `get()` | 可选 metadata | ✅ `getWithMetadata` + `x-cellhive-kv-metadata`（ADR-098） | 无 |
| R2 `get()` | `R2ObjectBody{key,version,size,etag,httpEtag,uploaded,httpMetadata,customMetadata,checksums,...}` + `text/json/arrayBuffer/blob` | ✅ 全字段 + 方法（`bodyUsed` 例外，见 `known-issues`） | 无 |
| R2 `put()/head()/list()` | `R2Object` / `{objects,truncated,cursor}` | ✅ 全字段；`put(key,value,{httpMetadata,customMetadata,md5,sha256})` 存 metadata sidecar（校验 md5/sha256） | 无（list 逐对象 metadata 见 known-issues） |
| D1 `run()/batch()` | `meta:{changes,last_row_id,changed_db,duration,rows_read,rows_written}` | ✅ `changes/last_row_id/changed_db/duration`（`rows_read/written` 为近似） | 引擎级行计数 |
| 错误形状 | `D1_ERROR`/`KVError` 等 | ✅ `err.name`（`D1_ERROR`/`KVError`/`R2Error`/`QueueError`）+ `err.code`（平台码） | 无 |

- 补齐后，**同一份用户代码**可在 CF / `wrangler dev` / `cellhive dev` / `cellhive deploy`(平台) 之间移植。
- 这是**平台工程责任**（契约保真），不是租户责任。

## 11. CLI（Bun）

```
cellhive dev [projectDir] [flags]
  --namespace <ns>        默认取 wrangler name
  --port <n>              本地端口（默认 8787）
  --data-dir <path>       默认 ./.cellhive-dev（Miniflare persist 根在其中）
  --env <name>            选择 wrangler env.<name>
  --var KEY=VALUE         覆盖 vars（可重复）
  --clean                 清空数据并按配置重建持久化
  --no-hot-reload         关闭 watch
  --strict-build          用平台 Go 打包器产出 bundle（否则用 wrangler/Miniflare 的 esbuild）
  --inspector-port <n>    workerd inspector
  --log-level <lvl>
```

- 输出示例：
```
cellhive dev — DEV (Bun + Miniflare; single worker, local simulation)
worker: api    namespace: acme    env: default
bindings: KV(kv) DB(d1) BUCKET(r2) QUEUE(queue)
url:      http://localhost:8787/        (host form: http://api.acme.localhost:8787/)
workerd:  1.20260615.1 (pinned; Miniflare bundled version overridden)
persist:  ./.cellhive-dev/miniflare
warning:  binding "IMAGES" is not supported by the CellHive platform; deploy will be rejected.
```

## 12. 与生产的差异（必须显式）

| 维度 | dev（Bun+Miniflare） | 生产（Go） |
|---|---|---|
| 绑定实现 | Miniflare 本地模拟 | cell-agent（bucket 条件写 + 复制） |
| 持久化 | 本地文件（Miniflare persist） | 对象存储 + cell SQLite |
| 写确认 | 即时（本地） | **RPO=0 输出门**（等证明；有真实延迟） |
| 支持面 | 宽（含平台拒绝的绑定） | 支持矩阵（严格拒绝） |
| 平台特性 | 无（版本/路由/密钥/deploy/scope 不在 dev） | 全部 |
| 运行时 | Miniflare 自带 workerd（pin 后与 prod 同版本） | pinned workerd |
| 拓扑 | 单 worker | 多租户 + 多副本 |
| 密钥 | `.dev.vars` | 控制面信封密文 + 外部根密钥 |

- 启动横幅明示"DEV / local simulation"，并在检测到平台不支持的绑定时**警告**（§8）。

## 13. 失败模式

| 现象 | 处理 |
|---|---|
| Bun 不可用 | 报错并提示安装 Bun（或发布 `bun build --compile` 单文件 CLI） |
| Bun 上 Miniflare 起不来（Node 兼容层） | **已 spike 通过**（Bun 1.4.0 起 Miniflare 正常）；若未来 Bun 版本回归，回退：用系统 Node 跑 Miniflare（仍零 Go），或 `--platform`（连本机真实 cell-agent） |
| Miniflare workerd 版本 ≠ pinned | 按 §3 override；若无法 override，横幅警告漂移 + 契约测试在 pinned 上跑 |
| 平台不支持的绑定（如 `IMAGES`） | dev 警告；`cellhive deploy` 服务端拒绝（§8），错误含字段路径 |
| `compatibility_date` 超上限 | CLI 预检 + 服务端拒绝（≤ 2026-06-22） |
| 端口占用 | 明确报错 + 占用 PID |

## 14. 实现计划（Bun 工程；不碰 Go 平台）

- 新增 `cli/`（Bun/TS 工程，替代/并行于 `cmd/cellhive`）：
  - `cli/src/dev.ts`：翻译 wrangler → Miniflare options、起 Miniflare、打印横幅/URL、watch 提示；
  - `cli/src/config.ts`：jsonc/toml 解析 + 共享校验库的**客户端调用/复刻**；
  - `cli/src/deploy.ts` 等：HTTP 客户端调 admin API；
  - `cli/package.json`：依赖 `miniflare`（+ 可选 `wrangler`），**pin workerd 到 1.20260615.1**；可用 `bun build --compile` 出单文件。
- **不做**：不实现 Go 后端本地启动；不在 dev 复刻 cell-agent。
- **需要补**（Go 侧，供 §8/§9）：一个共享的**兼容性校验库**（`internal/wranglercompat`），CLI preflight 与 `deploy` 端点共用；以及 binding 契约对拍的 golden 集。
- **里程碑**：
  - **M1（已实现并验证 2026-09-15 ✅，部分）**：`cli/` 已建 Bun 工程；`cellhive dev` 读 `wrangler.jsonc/.json/.toml`（子集）→ 映射 Miniflare（KV/D1/R2/DO/`vars`）→ 起 dev server；preflight（`compat_date_too_new`/`unknown_flag` 拒绝，`images`/`ai` 等 `unsupported_binding` 警告）；`--port/--data-dir/--env/--var`。**已验证**：`bun run src/index.ts dev <dir>` 起服务，`/`,`/kv`,`/d1`,`/r2` 经 HTTP 通过；坏配置正确拒绝/警告。**已补**：热重载、Queue 端到端、`rules`→`modulesRules`、`--clean`、assets（静态服务）。**已补**：`--strict-build`（`internal/bundler` Go+esbuild + `cellhive bundle build`）。**已补**：worker+assets interop + `_headers`/`_redirects`/`not_found_handling`（ADR-114，`make cli-test`）；**待补**：配置的 Go 侧解析（现由 CLI 解析，`html_handling` 未解析）、`ai`/`browser` 本地行为未测；
  - **M2（已实现并验证 2026-09-15 ✅）**：Go `internal/wranglercompat` 共享校验库（绑定支持矩阵/`compat_date` 上限/flags/资源已登记/未知字段，稳定错误码 + 单元测试）；`POST /v1/control/deploy` 服务端拦截已接线（`deploy_rejected` + findings，含 **bundle 存在性** 点查）；CLI 预检与同一套码对齐（`compat_date_too_new`/`unknown_flag`/`unsupported_binding`）。测试：`internal/wranglercompat` + `internal/server` 拦截用例通过。
  - M3 §9 契约对拍 + §10 facade 缺口补齐；
  - M4 `--strict-build`、`--platform` 回退、错误体验打磨。

## 15. 验收标准

1. 无 Bun 之外依赖、**无 Go 进程**：`cellhive dev` 在含 `wrangler.jsonc` 的项目起服务并打印 URL。
2. `curl` 示例 KV/D1/R2/Queue/DO 可用；停止再起，`persist` 数据保留（`--clean` 重置）。
3. 编辑 `main` → Miniflare 热重载生效；语法错误不崩、保留上一版。
4. 使用平台**不支持**的绑定（如 `IMAGES`）：dev **警告**；`cellhive deploy` 由**服务端拒绝**并给出稳定 error code + 字段路径。
5. `compatibility_date` 超 2026-06-22 / 未知 flag：CLI 预检 + 服务端拒绝。
6. dev 用 workerd 版本 = 平台 pinned（override 生效）或横幅明确漂移。
7. §9 对拍集在 Miniflare 与平台 cell-agent 上通过（或差异全部归入 §12 有意差异）。

## 16. 未做 / 后续

- 用 Miniflare 承载 `--sim` 之外的 AI/Browser（需 CF 凭据）——明确拒绝并提示。
- `wrangler dev` 直接兼容层（若用户已装 wrangler，`cellhive dev --wrangler` 透传）。
- **回退评估**：若自研 wrangler 配置翻译/打包成本不可接受，评估改用 **`@cloudflare/vite-plugin`**（CF 官方 programmatic dev server；**不要**用已废弃的 `unstable_startWorker`/`unstable_dev`）。
- ~~Bun+Miniflare 完整启动 spike~~ **已完成（2026-09-15）**：Bun 1.4.0 + Miniflare 4.20260714.0 + pinned workerd 1.20260615.1 起服务、KV/D1/R2 通过；结果已回填 §2/§3/§7/§13。剩余待测：Miniflare 本地 `images`/`ai`/`browser` 的实际行为（是否需凭据）。
- 与 §9 对拍配套的 CI 门（契约测试进 `docs/testing.md`）。

_最后更新：2026-09-15_
