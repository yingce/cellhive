# 模块：控制面与发布

> ADR-304 **无状态修订实施与验收中，未部署**，旧有状态测试不证明本次通过。Bearer HS256 JWT，audience `cellhive`，无旧 header/OIDC。namespace JWT 覆盖全部授权业务，`issuer/*` 且显式 `ns=["*"]` 覆盖全局业务/运维/GC/指标；`token/admin` 保留全局 bootstrap，issuer CRUD 移除。CF 仅 `:8082`。

应用/版本/路由/域名/资源/密钥/审计与发布流水线；单控制库（app 是表里的 `ns`），版本不可变、发布原子，资源先登记再 deploy。

> 配置权威来源见 [`../configuration.md`](../configuration.md) 与代码；下表是本模块相关子集。
## 关键接口

admin `:8082` 上的 `/v1/control/*`（apps、deploy、promote、rollback、routes、domain、resource、secret、audit、releases、capacity、gc、worker、queue、workflow）。

## 配置（环境变量）

| 变量 | 默认 | 作用 |
|---|---|---|
| `CELLHIVE_TOKEN` | 空 | CLI Bearer JWT，需到期刷新；输入优先级见配置页 |
| `CELLHIVE_AUDIT_RETENTION` | `720h` (0 永久) | 审计保留 |
| `CELLHIVE_AUTO_CREATE_APP` | `true` | deploy 首次自动建 app |
| `CELLHIVE_BASE_DOMAIN` | 空 | 内置域 `<ns>-<worker>.<base>`；空=禁用 |
| `CELLHIVE_DISPATCH_URL` | 空 | 计时/queue/cron/workflow 派发目标 —— 必须是 **user-runtime 内部特权端点**（`http://user-runtime:8088`）。空则派发循环不启动（compose/k8s/Helm 默认已设） |
| `CELLHIVE_DO_EAGER_RESTART` | `false` | deploy 急切重启 DO |
| `CELLHIVE_NS_RATE` | 空（关） | 每 ns 写准入 `rps[/burst]`（缺省 burst=rps） |
| `CELLHIVE_WORKFLOW_RETENTION` | `0` 永久 | 终态实例修剪 |


> 解析规则（字符串/布尔/时长/字节/列表）见 [`../configuration.md`](../configuration.md#解析规则)。

## 关键不变量

- `bundle_grants` 持久化 namespace/SHA 授权，namespace 原生上传须显式 namespace，CF 同样授予 grant。`:7001/metrics` 私网免鉴权例外；`:8082/v1/control/metrics` 鉴权过滤。ADR-302 固定 KV、secret 加密与 DO 最长30秒且不超过父 exp 的票据边界不变。
- 无状态 `kid=issuer/<pattern>`，精确/前缀 `*`/`*` 上限，固定最大 TTL86400秒；无 actions/key_version、issuer/namespaces 注册表授权、owner/持久化检查。恰好32位十六进制旧 ID 拒绝且无回退。旧表可惰性保留，无破坏性迁移；无即时 issuer 撤销，只有 exp/root轮换，泄露 key 需轮换 root 止损。
- 自动 provisioning **拒绝**：binding 必须引用已登记资源。
- 域名登记即授权（无 DNS 校验，ADR-133）；host 唯一。
- 版本不可变；rollback/promote 是原子切换。
- 控制面写也走 `capturedWrite`（RPO=0）；deploy 的 migrations + 自动 CreateApp + 版本提交合并为一次 proof（ADR-271）。

## 源码位置

`internal/control/`；`internal/server/control.go`；`cmd/cellhive/`（CLI 全命令面）。

## 测试锚点

`internal/control`、`internal/server`、`cmd/cellhive` e2e。

## 相关文档

[`control-plane.md`](../control-plane.md)、[`wrangler-compat.md`](../wrangler-compat.md)、[`security.md`](../security.md)

_最后更新：2026-09-19_
