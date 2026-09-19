# 模块：控制面与发布

应用/版本/路由/域名/资源/密钥/审计与发布流水线；单控制库（app 是表里的 `ns`），版本不可变、发布原子，资源先登记再 deploy。

> 配置权威来源见 [`../configuration.md`](../configuration.md) 与代码；下表是本模块相关子集。
## 关键接口

admin `:8082` 上的 `/v1/control/*`（apps、deploy、promote、rollback、routes、domain、resource、secret、audit、releases、capacity、gc、worker、queue、workflow）。

## 配置（环境变量）

| 变量 | 默认 | 作用 |
|---|---|---|
| `CELLHIVE_ADMIN_JWT` | 空 | 有值则改用 `Bearer`（ns 级授权） |
| `CELLHIVE_ADMIN_TOKEN` | 空→派生 | 可选：单独覆盖控制面 admin 凭据 |
| `CELLHIVE_AUDIT_RETENTION` | `720h` (0 永久) | 审计保留 |
| `CELLHIVE_AUTO_CREATE_APP` | `true` | deploy 首次自动建 app |
| `CELLHIVE_BASE_DOMAIN` | 空 | 内置域 `<ns>-<worker>.<base>`；空=禁用 |
| `CELLHIVE_DISPATCH_URL` | 空 | 计时/queue/cron/workflow 派发目标 —— 必须是 **user-runtime 内部特权端点**（`http://user-runtime:8088`）。空则派发循环不启动（compose/k8s/Helm 默认已设） |
| `CELLHIVE_DO_EAGER_RESTART` | `false` | deploy 急切重启 DO |
| `CELLHIVE_NS_RATE` | 空（关） | 每 ns 写准入 `rps[/burst]`（缺省 burst=rps） |
| `CELLHIVE_OIDC_ISSUER` / `_AUDIENCE` | 空 | JWT iss/aud 校验（JWT 必须含 `exp`） |
| `CELLHIVE_OIDC_JWKS_URL` | 空 | 设置后 admin 可额外用已验证 JWT bearer（ADR-036/131） |
| `CELLHIVE_WORKFLOW_RETENTION` | `0` 永久 | 终态实例修剪 |


> 解析规则（字符串/布尔/时长/字节/列表）见 [`../configuration.md`](../configuration.md#解析规则)。

## 关键不变量

- 自动 provisioning **拒绝**：binding 必须引用已登记资源。
- 域名登记即授权（无 DNS 校验，ADR-133）；host 唯一。
- 版本不可变；rollback/promote 是原子切换。
- 控制面写也走 `capturedWrite`（RPO=0）。

## 源码位置

`internal/control/`；`internal/server/control.go`；`cmd/cellhive/`（CLI 全命令面）。

## 测试锚点

`internal/control`、`internal/server`、`cmd/cellhive` e2e。

## 相关文档

[`control-plane.md`](../control-plane.md)、[`wrangler-compat.md`](../wrangler-compat.md)、[`security.md`](../security.md)

_最后更新：2026-09-19_
