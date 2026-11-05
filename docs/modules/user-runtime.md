# 模块：user-runtime（入口 loader 与租户执行）

公开入口 `:8081` + 内部特权派发 `:8088`：按 host/版本解析路由，用 `workerLoader` 动态加载不可变 bundle，注入 binding facade，执行租户的 `fetch`/`scheduled`/`queue`；租户出网仅公网。

> 配置权威来源见 [`../configuration.md`](../configuration.md) 与代码；下表是本模块相关子集。
## 关键接口

`:8081` 公开（Host→worker；`/ready`、`/healthz`）；`:8088` 内部（`/v1/timers/dispatch`、queue 派发、`/drain`）。

## 配置（环境变量）

| 变量 | 默认 | 作用 |
|---|---|---|
| `CELLHIVE_AI_URL` / `CELLHIVE_AI_KEY` | 空 | BYO AI 端点与密钥；空=不注入 |
| `CELLHIVE_CELL_URL` | `http://127.0.0.1:7001` | cell-agent REST |
| `CELLHIVE_DO_DIRECT` | 空=启用 | `0` 关闭 owner-hint 直达 |
| `CELLHIVE_ESBUILD` | 自动查找 | esbuild 路径 |
| `CELLHIVE_FACADES_JS` | `workerd/platform/facades.js` | facade 源 |
| `CELLHIVE_ROOT_KEY` | 必填 | 派生 internal/scope/dispatch/log 凭据 |
| `CELLHIVE_RUNTIME_DIR` | `$TMPDIR/cellhive` | 运行目录（子目录 `user-runtime/`） |
| `CELLHIVE_SERVICE_NATIVE` | 空=启用 | `0` 关闭原生 service RPC |
| `CELLHIVE_TENANT_OUTBOUND` | 空→`public` | 出网类别 `public`/`private`/`local` |
| `CELLHIVE_USER_RUNTIME_INTERNAL_PORT` | `8088` | 内部派发 |
| `CELLHIVE_USER_RUNTIME_JS` | `workerd/user-runtime` | loader JS |
| `CELLHIVE_USER_RUNTIME_PORT` | `8081` | 公开入口 |
| `CELLHIVE_WORKERD` | 自动查找 | workerd 路径（优先 pin `1.20260916.1`） |


> 解析规则（字符串/布尔/时长/字节/列表）见 [`../configuration.md`](../configuration.md#解析规则)。

## 关键不变量

- 租户 `globalOutbound` = **public-only**；capability binding 是平台侧 entrypoint stub（`:7001` 传输在平台 worker，ADR-184），租户 env 无 `PLATFORM`/`CELL_URL`。
- env patch（`bindings-wrapper.js` / `queue-wrapper.js`）让 `this.env` 与构造形参 `env` 一致。
- **不持桶凭据**；只拿 binding facade 与 scoped token。
- Workflow 回调通过 dispatcher-bound `WorkflowBridgeTarget extends RpcTarget` 作为 JSRPC 参数越过 `workerLoader`；不使用 `ServiceStub`，也不进入 tenant env。默认由 host-bound 原生 Tail Fetcher 捕获租户 Worker 与 service target 的 `console.*`/异常，日志身份和凭据留在可信宿主；`CELLHIVE_TENANT_LOG_CAPTURE=off` 可按 runtime 关闭。
- 最终 WorkerCode（64 MiB）和估算 workerLoader env（1016 KiB）在每次动态加载前经共享 `workerd/platform/budget.js` 复核；控制面检查不能替代运行时防御。

## 当前工作树边界（2026-11-04，未部署）

公共 loader 合并退休 sweep，并重新查询 pinned Worker 的 live WebSocket 状态，避免 socket 关闭后永久保留 pinned 标记；这不是所有 isolate/长连接已可回收的保证。源码缓存使用 65MiB 计量预算，public/internal 的条数上限分别为 2000/256；internal 原始与重写源码共享预算。有效源码超过缓存预算时仍加载，只是不缓存；最终 WorkerCode/env 校验及独立 runtime capacity 准入继续存在，缓存预算不能成为新的部署大小限制。

DO response 的 JS hops 可传递 stream，但 Go DO proxy 仍有 8.25MiB 缓冲限制，不能宣称端到端流式 DO fetch。整合后的真实 pinned workerd/长连接/大源码回归与压力验收仍需单独确认，见 [当前问题](../known-issues.md)。

## 源码位置

`cmd/user-runtime/`；`internal/userruntime/`；`workerd/user-runtime/{loader.js,internal.js,queue-wrapper.js,workflow-wrapper.js,cellhive-workflow.js}`；`workerd/platform/{facades.js,bindings.js,bindings-wrapper.js,telemetry.js,rpc-codec.js,budget.js}`。

## 测试锚点

`make js-test`（真实 workerd）、`internal/userruntime`。

## 相关文档

[`routing.md`](../routing.md)、[`bindings.md`](../bindings.md)、[`workerd-integration.md`](../workerd-integration.md)

_最后更新：2026-09-22_
