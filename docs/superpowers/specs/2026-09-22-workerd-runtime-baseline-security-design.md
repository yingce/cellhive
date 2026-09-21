# stock workerd 运行时基线与安全加固

日期：2026-09-22

## 1. 背景与总目标分解

CellHive 将吸收 WDL 在 stock workerd 动态运行时上的成熟做法，但不改变自身架构：计算层继续使用未修改的 stock workerd，状态权威仍是 cell + bucket，RPO=0 仍由 owner/epoch/output gate 保证，不增加独立 Gateway、Redis/Valkey、第二套 JS 引擎或非 Go 控制面。

该总目标横跨运行时基线、动态加载治理、DO ownership 和 Tail/多模块四个可独立验收的子系统。为避免一次变更同时改变安全边界、生命周期和可观测性，本规格只定义第一阶段“运行时基线与安全”。后续阶段分别另写规格、计划和验收：

1. 动态 loader 治理：安全 wrapper、invocation-scoped env/context、冷加载边界、历史 isolate 主动淘汰；
2. DO ownership 加固：mid-flight lease fence 与持久 restart generation；
3. 原生 Tail 和多模块 artifact：有界 best-effort Tail、OTLP 接线、兼容的多模块产物。

第一阶段完成后，后续阶段拥有一个固定、可审计且容器内可用的 workerd/esbuild 基线，并且平台秘密已经从动态 WorkerCode 中清除。

## 2. 本阶段目标

本阶段交付以下不可分割的运行时基线：

- `CELL_URL`、`CELL_TOKEN` 及其他平台 URL/凭据不再进入动态租户或 DO facet 的最终 WorkerCode；
- workerd 宿主 bindings 中的平台配置通过 Cap'n Proto `fromEnvironment` 取得，不再把秘密渲染进临时 `.capnp`、命令行或日志；
- 生产镜像同时包含固定版本的 stock workerd 和外部 esbuild，两个版本都在构建及运行验收中校验；
- workerd 固定升级到 `1.20260916.1`，兼容日期和 flag 清单从该固定版本的上游源自动生成并由真实二进制验证；
- 控制面和运行时共同执行最终 WorkerCode 64 MiB 预算与 workerLoader env 预算；
- 用户可见 env 继续遵守 ADR-185：只有用户声明的 vars 和用户命名的 binding，平台键为 0，任何名称都不因平台保留而被拒绝。

## 3. 当前问题

### 3.1 平台秘密进入最终 WorkerCode

`workerd/user-runtime/loader.js` 的 `platformConsts()` 当前把 `env.CELL_URL` 和 `env.CELL_TOKEN` 序列化到 `wrapper.js` 源码。即使租户模块通常不能直接 import wrapper，这仍把平台秘密放进了动态 `WorkerCode.modules`，扩大了意外泄露、调试转储、错误信息和未来模块重写缺陷的影响面。

现有 wrapper 实际只需要 R2/DO binding 名称等非秘密元数据；平台传输已经由宿主侧 entrypoint stub 承担。因此 URL/token 注入既不必要，也违反“平台秘密永不进入租户 WorkerCode”的边界。

### 3.2 宿主配置靠字符串渲染

`internal/userruntime` 和 `internal/doruntime` 由 Go 模板生成 workerd 配置，当前注释及实现假设 workerd 不能从进程环境建立 binding。目标版本支持 `fromEnvironment`。继续渲染秘密会让它们进入临时配置文件，并使配置快照、报错和诊断更难安全处理。

### 3.3 镜像缺少 esbuild

生产约定是 Go 调用外部 esbuild，不引入 Node 或第二套 JS 引擎。但当前 `deploy/Dockerfile` 只复制 workerd 和 Go 二进制，未提供 `esbuild`，导致镜像内正常部署/打包路径不可用。

### 3.4 pin 与兼容清单靠人工同步

当前 pin 是 `1.20260615.1`，兼容日期上限为 `2026-06-22`，Go 与 Bun CLI 各维护一份 flag 表。测试只证明两份人工表相同，并未证明它们与固定版本的 workerd 上游定义相同。

### 3.5 动态加载没有前置预算

stock workerd 对最终 `WorkerCode` 和序列化 env 有硬上限。当前控制面主要校验上传 bundle，没有按“用户代码 + 平台 wrapper + 注入模块”的最终形态校验 64 MiB，也没有对完整动态 env 预留序列化余量。超限会延迟到请求冷加载时失败。

## 4. 已选方案

采用“固定版本、单一生成源、控制面前置拒绝、运行时防御复核”的兼容演进方案。

未选择以下方案：

- 不一次性合并 loader 生命周期、DO fencing 和 Tail；这些变化的失败域不同，无法用同一回滚开关安全处理。
- 不长期维护 loader v1/v2 双轨；它会产生两套安全边界和兼容行为。
- 不从运行中的二进制猜测全部 flag；二进制错误探针适合验证，不适合作为完整清单的唯一来源。完整清单来自与 pin 对应的上游源，真实二进制负责交叉验证。
- 不因为预算估算可能保守就推迟校验；控制面保守拒绝优于线上冷加载失败。

## 5. 设计

### 5.1 设计契约先行

实施第一步新增 ADR（顺延当前编号）并同步以下现行文档及英文镜像：

- `docs/decisions.md`；
- `docs/workerd-integration.md`；
- `docs/configuration.md`；
- `docs/compatibility-matrix.md`；
- `docs/known-issues.md`；
- `docs/modules/{user-runtime,do-runtime,cli-and-packaging}.md`；
- 对应 `docs/en/` 文件；
- `docs/testing.md` 与 `docs/release-notes.md`。

文档必须区分“设计决定”“代码已实现”和“真实环境已验证”。在测试通过前不得把新 pin、Tail 能力或容器部署路径写成已完成。

### 5.2 平台秘密只存在于宿主 env

user-runtime 和 do-runtime 的 Cap'n Proto 配置把平台字符串 binding 改为 `fromEnvironment`：

- Go 进程只负责准备非秘密的静态/结构配置与子进程环境；
- workerd 启动后将环境变量读入宿主 Worker binding；
- 动态 loaded Worker 只收到 `tenantEnv()` / `buildFacetEnv()` 构造的用户 env；
- 临时 `.capnp` 文件、进程参数、错误信息和正常日志不得包含秘密值。

workerd 子进程环境从空列表构造，只加入 Cap'n Proto `fromEnvironment` 明确引用的键和已记录的非秘密运行参数；不继承父进程中的任意其他变量，也不把秘密复制为额外别名。宿主仍可获得 cell-agent URL、角色 token、私网 service binding 和 loader；这些能力不得进入动态 Worker env、模块源码或用户可访问的 global。

配置渲染测试使用唯一 canary secret，扫描完整渲染结果、命令参数和动态模块源码；任何命中都失败。测试日志本身只报告键名和命中位置，不打印值。

### 5.3 最终 WorkerCode 中平台秘密为 0

删除 `platformConsts()` 中的 `cellUrl` 与 `cellToken` 字段。生成 wrapper 只允许包含运行所需的非秘密结构元数据，例如用户 binding 名称列表。

这一约束覆盖全部动态执行路径：

- user-runtime public fetch；
- queue/scheduled/service/workflow 内部派发；
- do-runtime 动态 facet；
- 后续多模块 artifact 与 Tail wrapper。

测试不能只检查 `Object.keys(env)`。它必须捕获传给 `workerLoader.get()` 的最终对象，递归检查：

- `env` 的键和值；
- `mainModule`；
- 所有 `modules` 名称、文本和二进制内容；
- compatibility 配置及其他可序列化字段。

canary URL/token 均不得出现。非秘密 binding 名称可以出现，但必须来自用户/版本配置，不得携带平台地址或身份。

### 5.4 固定 workerd 与 esbuild

#### workerd

- 唯一生产 pin 改为 `1.20260916.1`；
- `internal/workerdbin.PinnedVersion` 是 Go 侧权威值；Docker build arg、CLI override、文档和测试必须与它逐项一致；
- 镜像构建下载精确包并校验固定完整性，运行 `/usr/local/bin/workerd --version`；
- 禁止 `latest`、版本范围和运行时自动升级。

升级采用 reader-before-writer：新二进制必须先证明能读取和执行现有 artifact、DO working copy 与协议，再允许控制面接受只在新 pin 上合法的日期/flag。回滚只允许回到仍能读取当前 artifact/协议的上一固定 pin。

#### esbuild

- 生产镜像加入独立 esbuild 获取阶段，固定为 `0.28.2`；
- 只复制原生 `esbuild` 二进制到运行镜像，不携带 Node/npm；
- 下载物必须有固定版本与完整性校验；构建阶段执行 `esbuild --version`；
- 运行镜像设置 `CELLHIVE_ESBUILD=/usr/local/bin/esbuild`；
- 容器验收必须在镜像内走一次真实 `cellhive deploy` 打包路径，不能仅检查文件存在。

#### dev CLI

同日可获取的 Miniflare 是 `5.20260916.0-alpha`，不能仅因日期相同就直接采用。候选组合必须分别跑：Bun 单测、Miniflare 启动、KV/D1/R2 和 module worker smoke、与生产 pin 的版本读回。

选择规则：

1. 采用与目标 workerd 同期且所有 smoke 通过的精确 Miniflare 版本；当前候选为 `5.20260916.0-alpha`，是否采用由真实 smoke 决定而不是版本名决定；
2. 不允许用 2026-06 的 Miniflare 搭配 2026-09 的 workerd 作为完成态，也不允许去掉 override 后使用 Miniflare 自带的其他 workerd；
3. 若同期候选不通过，第一阶段不得宣告完成，文档记录 CLI 阻塞，不能静默版本漂移。

### 5.5 compatibility 日期与 flag 自动生成

新增一个 Go 生成器，输入是与 `1.20260916.1` 对应的 workerd 上游源码中的 compatibility 定义文件，输出一个带以下元数据的 checked-in manifest：

- workerd npm 版本；
- 对应上游 revision/tag；
- 支持的 compatibility flag 名称及稳定/实验属性；
- 生成器输入 SHA-256；
- 已验证的 compatibility date 上限。

Go 控制面和 Bun dev CLI 都从这一 manifest 生成或读取清单，不再手写两份列表。生成器不在生产请求路径联网；更新 pin 时由维护命令显式获取/指定上游 checkout，生成结果纳入 code review。

离线一致性测试保证：

- manifest 版本等于 `workerdbin.PinnedVersion`；
- Go 与 CLI 消费的清单完全一致；
- 生成器对固定 fixture 可重复、排序稳定；
- manifest 输入哈希与 vendored/下载后上游定义相同；
- 实验 flag 默认拒绝，除非 ADR 明确允许且真实测试覆盖。

真实 workerd 探针保证：

- manifest 中 CellHive 允许的每个 flag 都能通过配置加载；
- 一个随机未知 flag 被 workerd 拒绝；
- `MaxCompatibilityDate` 由目标二进制的未来日期错误边界探测得到，并与 manifest 相同；
- 旧日期、当前上限和超限日期分别得到预期结果。

这些探针是 pin 更新的硬门禁。不能用“列表编译通过”代替真实二进制验证。

### 5.6 最终 WorkerCode 预算

定义平台常量：

- `WorkerCodeMaxBytes = 64 * 1024 * 1024`；
- 计量对象是交给 `workerLoader` 的最终 WorkerCode，不只是上传 bundle。

预算包括：

- 所有用户 text/ES modules 和 binary/data modules；
- 生成 wrapper；
- 平台注入的 facade、RPC codec 和其他模块；
- 模块名及生成代码中确定性的附加字节。

Go 控制面在创建不可变版本、切换 active pointer 之前按一份显式预算清单计算最终预算；JS 运行时冷加载按相同清单复核。两端共享 checked-in 测试向量，覆盖固定注入模块、wrapper 动态片段和模块名开销，防止公式漂移。超限返回稳定错误 `worker_code_too_large`，包含实际字节和上限，不包含源码。运行时复核同时防止旧数据、损坏数据或绕过控制面写入；失败返回明确 4xx/5xx 分类并计数，绝不截断代码。

边界测试至少覆盖 `limit-1`、`limit`、`limit+1`，以及“小用户 bundle + 大平台注入”和“多模块名/二进制模块”估算。若估算与真实 workerd 存在偏差，必须使平台更保守，并记录固定 headroom；不能允许平台接受后由 workerd 拒绝。

### 5.7 workerLoader env 预算

上游硬上限按 `1 MiB` 建模，平台预留 `8 KiB`：

- `WorkerEnvUpstreamMaxBytes = 1024 * 1024`；
- `WorkerEnvHeadroomBytes = 8 * 1024`；
- `WorkerEnvMaxBytes = 1016 * 1024`。

估算完整动态 env：用户 vars、当前用户命名 binding stub 的可序列化 props，以及未来接入后用户 secrets。平台秘密和系统键本就不应在该 env 中，不能以预算为由重新加入。

字符串估算使用 UTF-8 JSON 字节数，并对包含非 Latin-1 字符的 V8 双字节表示补罚；对象键和值都计入。控制面在部署和任何会改变实际 binding props 的操作上复算，超限返回 `worker_env_too_large`。当前 secrets 尚未进入 runtime env，因此 secret put 不虚构计费；未来接入 secrets 时，secret 变更必须在同一事务边界内先复算再生效。运行时在 workerLoader 前复核，失败关闭，不删除字段、不截断值。

边界测试覆盖 ASCII、中文/emoji、长键名、大量小 binding、`limit-1/limit/limit+1`，并通过真实 workerd 构造接近边界的 env 验证估算不会接受必然失败的配置。

### 5.8 错误处理与可观测性

稳定错误分类：

| 场景 | 错误码 | 行为 |
|---|---|---|
| 最终 WorkerCode 超限 | `worker_code_too_large` | 部署拒绝；运行时旧数据 fail-closed |
| 动态 env 超限 | `worker_env_too_large` | 部署/secret/binding 变更拒绝；运行时 fail-closed |
| compatibility date 超限 | `future_compatibility_date`（保留现有 API 码则记录映射） | 部署拒绝 |
| 未知/实验 flag | `unknown_flag` / `experimental_flag_unsupported` | 部署拒绝 |
| pin/manifest 不一致 | 启动或 CI 错误 | 不构建发布物 |
| 缺少/错误 esbuild | 明确打包错误 | 不创建版本 |

新增低基数指标仅按 outcome 分类，不带 namespace/worker 之外的无界源码或 secret 数据。日志可记录版本、预算值和配置键名，不记录 token、URL query、secret 值或 Worker 源码。

## 6. 数据流

### 6.1 启动

1. Go 服务读取平台配置；
2. 渲染不含秘密值的 capnp；
3. 以受控子进程环境启动固定 workerd；
4. workerd 通过 `fromEnvironment` 建立宿主 binding；
5. 健康检查确认 pin、配置加载和必要宿主 binding 可用。

### 6.2 部署

1. 控制面解析 bundle 和 worker 配置；
2. 依据生成 manifest 校验 compatibility date/flags；
3. 构造与运行时一致的最终 WorkerCode 形状并计算 64 MiB 预算；
4. 构造完整租户 env 估算并执行 1 MiB−8 KiB 预算；
5. 仅在全部通过后写不可变版本并切换 active pointer。

任何失败都不能留下半发布 active version。沿用现有 cell 事务实现原子发布，不新增旁路状态存储。

### 6.3 请求冷加载

1. loader 读取 immutable bundle/env；
2. 使用共享预算逻辑复核；
3. 生成不含平台秘密的 wrapper；
4. 调用 `workerLoader.get()`；
5. 租户只看到用户 env，平台 stub 在宿主侧执行。

冷加载 deadline、重试、响应限额、指标与 isolate 淘汰属于下一阶段，本阶段只提供预算和秘密隔离基础。

## 7. 安全不变量

- 不修改或 fork workerd。
- 平台 URL/token 不进入动态 WorkerCode、租户 env、租户 global、临时 capnp、进程参数或日志。
- `Object.keys(env)` 和 `import { env } from "cloudflare:workers"` 都只看见用户声明名称。
- 用户仍可使用 `CELL_URL`、`CELL_TOKEN`、`CH_*`、`CELL_*`、`__cellhive*` 等名称；同名用户值不得被宿主值覆盖。
- 租户 outbound 保持 public-only；宿主私网能力不传给租户。
- 超限、未知 flag、pin 漂移和秘密配置缺失一律 fail-closed。
- bucket 热路径不增加 List；本阶段不改变 cell/owner/epoch/RPO=0 协议。

## 8. 测试与验收

实施按 TDD：每个新行为先增加会失败的测试，记录失败原因，再修改生产代码。

### 8.1 单元与契约测试

- capnp 渲染产物不含 canary secret，且使用 `fromEnvironment`；
- 捕获 user-runtime/do-runtime 最终 WorkerCode，递归证明 canary URL/token 不存在；
- 所有入口继续证明平台 env 键为 0，用户同名键可用；
- WorkerCode/env 预算边界、Unicode 与错误码；
- flag 生成器 fixture、manifest pin/hash、一源多消费者一致性；
- Dockerfile、CLI override、Go pin 和文档 pin 的静态一致性。

### 8.2 真实 stock workerd

- `1.20260916.1` 版本读回；
- `fromEnvironment` 宿主 binding 实际可读，缺失必需值时启动失败；
- compatibility date 上限、允许 flag、未知 flag、实验 flag 探针；
- user-runtime env/KV/D1/R2/Queue/Workflow/Service；
- do-runtime SQL、RPC、binding、WebSocket、alarm、冷启动和输出门；
- 现有动态 Tail 失败证据重新执行并记录，但 Tail 恢复不属于本阶段完成条件。

### 8.3 Docker Compose 真实请求

构建单镜像并起 `rpo0` profile，至少验证：

- 容器内 `workerd --version` 与 `esbuild --version`；
- 容器内用 esbuild 打包并部署一个读取用户 env、执行 KV 与 DO 的 worker；
- 用户自定义 `CELL_URL`/`CELL_TOKEN` 可见且等于用户值，宿主真实值不可见；
- fetch、KV、DO 连续写、DO runtime 重启后计数延续；
- 输出门有持久证明，bucket 中存在预期 cell 段；
- 渲染文件和采集到的最终 WorkerCode 不含平台 canary；
- 相关用例无隐式 `SKIP`。

### 8.4 完整门禁

- `REQUIRE_ALL=1 bash scripts/ci.sh`；
- Docker 可用时所有 13 个官方 gate 必须 PASS，不能把缺工具或镜像下载失败算作通过；
- 与本阶段相关的测试逐项列出执行命令、退出码和结果；
- 最终记录 `git status`、提交范围、镜像版本和未验证的外部环境项。

## 9. 升级与回滚

- 先在测试/本地 Compose 同时验证新 pin 读取旧 artifact；再切控制面接受上限；
- schema、artifact 和协议在本阶段不做破坏性变化，因此应用回滚不需数据迁移；
- 一旦部署了只被新 pin 接受的 compatibility date/flag，回滚到旧 pin 前必须先证明所有 active version 均可被旧 pin 加载；否则拒绝回滚并给出清单；
- esbuild 回滚只影响未来构建，不重写已有内容寻址 bundle；
- manifest 与二进制必须成对回滚，禁止单独回滚清单。

## 10. WDL 借鉴边界与许可证

本设计参考 WDL 固定提交 `dc70da6cc04acee7d31d80fc0caf8f323bbacf21` 的以下方法：

- Cap'n Proto `fromEnvironment` 宿主 binding；
- 最终 WorkerCode 预算；
- 1 MiB env 上限及 8 KiB headroom；
- 从固定 workerd 上游源提取 compatibility flag；
- 镜像内固定二进制并执行版本读回。

只借鉴方法与边界，不复制 WDL 的 Redis/Valkey、Gateway、Rust 控制面、EFS/local-disk 权威或系统 runtime 架构。若实施中复制或实质改编 Apache-2.0 源码，必须在同一提交补齐源文件头、LICENSE/NOTICE 和第三方归属；若独立实现，则在文档保留方法来源，不制造不必要的运行时依赖。

## 11. 非目标

- 本阶段不实现 loader cold-load deadline/retry/body limit/single-flight/主动 isolate eviction；
- 不实现 DO mid-flight lease renewal/abort 或 restart generation 持久化；
- 不恢复 Tail Worker，不把日志 transport 放回租户 env；
- 不改变 bundle 为多模块 artifact；但预算器必须为下一阶段保留多模块输入能力；
- 不补做用户 secret 的运行时版本化/热更新；其未来接入必须经过本规格 env 预算并遵守零平台键；
- 不新增 gateway、Redis/Valkey、NATS、etcd、APISIX 或 bucket 热路径 List。

## 12. 完成定义

仅当以下证据同时成立，第一阶段才完成：

1. 设计 ADR 和中英文文档与实现一致；
2. 最终 WorkerCode、租户 env、capnp、进程参数和日志均无平台秘密；
3. 固定 workerd/esbuild 在生产镜像内可执行，CLI 版本组合无静默漂移；
4. compatibility manifest 由固定上游源生成并经真实目标二进制验证；
5. WorkerCode/env 边界在控制面和运行时都 fail-closed；
6. 单元、真实 workerd、Docker Compose 请求和 `REQUIRE_ALL=1` 官方门禁通过，无相关隐式 SKIP；
7. 许可证/NOTICE、迁移/回滚说明、Git 状态和外部未验证项可审计。

满足本阶段不代表总 Goal 完成。总 Goal 必须继续完成后续三个独立规格及其实现验收。
