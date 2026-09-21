# Tenant env 完全由用户拥有

日期：2026-09-21

## 目标

租户 Worker 可见的 `env` 命名空间完全属于用户。平台不向租户 `env` 注入任何系统键，也不保留任何名称或前缀。

最终契约：

- 租户 `env` 仅由用户声明的数据组成：当前为版本 `vars` 和用户命名的 binding stub；现有 secrets 运行时注入缺口单独处理，但 secret 名称同样完全属于用户。
- 平台系统键数量为 0；删除当前唯一的 `CH_PLATFORM`。
- 用户可以使用 `CH_*`、`CELL_*`、`__cellhive*`、`PLATFORM`、`LOG_*`、`WF_*` 等任意合法名称。
- 控制面不再返回 `reserved_env_name`，secret 写入也不因这些名称被拒绝。
- 平台传输、身份、凭据和日志设施不得借用户 `env` 搭便车。
- 用户 binding 仍按用户指定的 binding 名进入 `env`；它是用户配置，不是平台保留键。

本设计不改变 binding 的权限边界、租户 public-only 出网、stock workerd 约束或 RPO=0 语义。

## 当前问题

当前 `tenantEnv()` / `buildFacetEnv()` 在用户 vars 和 binding stub 之外额外写入 `CH_PLATFORM`。这个 props-bound `PlatformBridge` 同时承载：

1. Workflow step 对 cell-agent 的受限回调；
2. 租户 console 日志向平台 log ring 的上送。

虽然 `CH_PLATFORM` 不直接暴露内部 token，但它仍占用了用户命名空间，并且可由租户代码直接调用。为避免名称冲突，控制面又进一步保留了 `CH_*`、`CELL_*`、`__cellhive*` 和若干历史精确名称。这与“用户 env 完全由用户拥有”的目标冲突。

## 设计

### 1. 租户 env 构造

user-runtime 的公开 fetch、内部 queue/scheduled/service/workflow 路径，以及 do-runtime facet 使用同一个契约：

1. 从版本环境取得用户 vars；未来补齐 secrets 注入时也必须遵守本设计的零平台键契约；
2. 按用户声明名称加入 binding stub；
3. 不加入任何平台键。

平台继续在可信宿主 Worker 自己的 env 中持有 `CELL_URL`、角色凭据、private outbound、loader 等 binding。这些宿主 env 不属于租户 Worker，不能复制到 loaded Worker 的 env。

现有 wrapper 模块作用域里的平台常量可保留，但必须满足：租户模块无法 import、枚举或读取它们；其中不得新增用户可调用的通用私网传输。

### 2. Workflow：能力作为调用参数传递

Workflow 不再从 `env.CH_PLATFORM` 取得 step transport。

可信的 user-runtime internal host 在派发 workflow 时：

1. 用 `ctx.exports.PlatformBridge({props})` 创建身份已绑定的 capability；
2. props 固定 `namespace`、`worker`、`workflow`、`id` 和 `run`；
3. 通过原生 JSRPC，把 capability 作为 `CellHiveWorkflow.handleRun()` 的参数传给 workflow wrapper；
4. wrapper 用闭包构造传给租户 `run(event, step)` 的 `step` 对象。

capability 不进入租户 `env`，也不直接交给租户 workflow 类。租户只拿到 Cloudflare 形状的 `step` 对象。`PlatformBridge` 继续使用固定 op 表，拒绝原始路径、身份覆盖和跨租户参数。

JSRPC capability 参数必须在 pinned workerd `1.20260615.1` 上做真实测试，覆盖 step get/put、sleep、waitForEvent、finish、错误完成以及身份伪造拒绝。

### 3. 日志：使用 stock workerd Tail Worker

移除 loaded Worker 内依赖 `env.CH_PLATFORM` 的 console monkey patch 和 `log-tail.js` 上送链路。改用 stock workerd 的原生 Tail Worker：

1. user-runtime 和 do-runtime 各配置可信 tail service；
2. 动态 loaded Worker 的 `tails` 指向该 service；
3. tail service 自己持有平台日志凭据和 private transport；
4. tail event 中的平台 worker 标识映射回 `namespace/worker/version`，然后写入现有 cell-agent log ring；
5. 日志失败保持 best-effort，不影响租户请求。

实施前先在 pinned workerd 上做最小真实 spike，验证 `workerLoader` 动态 Worker 配置支持 `tails`、公开 fetch/内部派发/DO facet 的 console 事件均能抵达 tail service，并确认事件中有稳定、不可由租户伪造的 loaded Worker 标识。

如果 pinned workerd 不支持动态 Worker 的 Tail Worker，回退策略是：删除平台日志捕获并在兼容矩阵明确标为暂不支持。不得重新向租户 env 注入日志 sink、token、URL 或其他系统键。Workflow 的 JSRPC 参数方案不受该回退影响。

### 4. 取消保留 env 名称

删除控制面的平台 env 名称注册表和以下校验：

- deploy 对 vars 名称的 `reserved_env_name`；
- deploy 对 binding 名称的 `reserved_env_name`；
- secret put 对 secret 名称的 `reserved_env_name`；
- user-runtime / do-runtime 对历史平台名称的运行时碰撞告警。

普通配置规则仍保留，例如名称格式、重复用户 binding、配置 schema 和不支持的 binding 类型。若用户 var、secret、binding 使用同名，应由统一的用户 env 冲突规则决定，不能再以“平台保留名”处理。

### 5. Secrets 边界

当前 secret 已能加密存储和管理，但运行时注入链路与文档存在不一致。本次不顺带设计 secret 版本化、热更新和 workerLoader 缓存失效；该缺口单独修复并同步文档。

本次立即取消 secret 名称的系统保留限制；用户可使用任意合法自定义名称。后续补齐 secret 注入时不得重新引入平台保留名称。

## 安全不变量

- 租户枚举 `Object.keys(env)` 时只能看到用户声明的名称。
- `import { env } from "cloudflare:workers"` 也不能看到平台键；不能只在 handler 参数上做过滤。
- 租户不能取得 cell-agent URL、internal/log/scope token、private service binding 或任意通用平台 fetcher。
- Workflow capability 的身份只能由可信 host 绑定，租户参数不能覆盖。
- Tail Worker 身份不能取自租户可控 header、日志正文或 env 值。
- 租户 `globalOutbound` 保持 public-only。
- 不修改或 fork workerd。

## 失败处理

- Workflow capability 缺失或 RPC 失败时，该 workflow attempt 明确失败，不能静默跳过 durable step。
- 日志 tail 初始化或发送失败时仅丢日志并计入平台侧错误指标，不影响租户响应。
- 未知 binding kind 继续 fail loudly；不能因为移除平台键而静默丢 binding。
- 若 Tail Worker spike 不通过，按文档化回退关闭平台日志功能，不回退到 env 注入。

## 测试与验收

### 租户 env

- fetch、queue、scheduled、service RPC、workflow、DO constructor `env` 和 `this.env` 均断言不存在任何未声明键。
- 租户分别声明 `CH_PLATFORM`、`CELL_URL`、`PLATFORM`、`LOG_TOKEN`、`WF_ID`、`__cellhive_test`，部署成功且值/binding 可正常读取。
- `import { env } from "cloudflare:workers"` 与 handler 参数看到相同的纯用户 env。
- 控制面 deploy、secret put 不再产生 `reserved_env_name`。

### Workflow

- 在没有 `CH_PLATFORM` 的情况下，真实 workerd workflow step durable round trip 通过。
- 非法 op、原始路径、伪造 namespace/workflow/id/run 均被拒绝或忽略。
- sleep、waitForEvent、retry、finish/error 行为保持现有契约。

### 日志

- pinned workerd spike 先证明动态 `tails` 能力；只有实测通过后才保留平台 tail 功能。
- fetch、queue、workflow 和 DO 日志能按正确 namespace/worker 进入现有 ring。
- 租户伪造内容不能改变日志归属。
- tail 服务不可用时请求仍成功。

### 回归

- `make js-test`
- `make test`
- `make vet`
- `make build`
- 文档与 `docs/decisions.md`、`docs/configuration.md`、`docs/modules/*`、`docs/compatibility-matrix.md`、`docs/known-issues.md` 同步。

## 非目标

- 不改变用户 binding API 或资源模型。
- 不把平台 transport 暴露为另一个用户可见对象、全局变量或特殊 symbol。
- 不新增第二套 JS 引擎、gateway 或消息系统。
- 不为日志开放公网内部端点。
- 不把仅过滤 handler 参数误认为隔离；模块导入 env 与 DO env 也必须满足同一契约。
