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
| `CELLHIVE_WORKERD` | 自动查找 | workerd 路径（优先 pin `1.20260615.1`） |


> 解析规则（字符串/布尔/时长/字节/列表）见 [`../configuration.md`](../configuration.md#解析规则)。

## 关键不变量

- 租户 `globalOutbound` = **public-only**；capability binding 是平台侧 entrypoint stub（`:7001` 传输在平台 worker，ADR-184），租户 env 无 `PLATFORM`/`CELL_URL`。
- env patch（`bindings-wrapper.js` / `queue-wrapper.js`）让 `this.env` 与构造形参 `env` 一致。
- **不持桶凭据**；只拿 binding facade 与 scoped token。

## 源码位置

`cmd/user-runtime/`；`internal/userruntime/`；`workerd/user-runtime/{loader.js,internal.js,queue-wrapper.js,workflow-wrapper.js,cellhive-workflow.js}`；`workerd/platform/{facades.js,bindings.js,bindings-wrapper.js,log-tail.js,telemetry.js,rpc-codec.js}`。

## 测试锚点

`make js-test`（真实 workerd）、`internal/userruntime`。

## 相关文档

[`routing.md`](../routing.md)、[`bindings.md`](../bindings.md)、[`workerd-integration.md`](../workerd-integration.md)

_最后更新：2026-09-19_
