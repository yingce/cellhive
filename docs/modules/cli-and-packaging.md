# 模块：CLI、打包与 wrangler 兼容

`cellhive` CLI（部署/运维/资源/队列/向量/工作流）、Go+esbuild 打包与 `wrangler` 配置兼容；`cli/` 是 Bun+Miniflare 的 **dev-only** 工具。

> 配置权威来源见 [`../configuration.md`](../configuration.md) 与代码；下表是本模块相关子集。
## 关键接口

CLI 命令面（`deploy --config`、`bundle build/put`、`resource`、`queue`、`vectorize`、`domain/route`、`creds`、`token`、`tail`…）；打包经 `internal/bundler`（esbuild）。

**凭据与 scoped token 签发（ADR-181）**：
- `cellhive creds [role]`：打印 root 派生的 8 个角色凭据。
- `cellhive creds issuer <name>`：打印委派签发方的 issuer key（`HKDF(SCOPE_SECRET,"issuer/"+name)`），交给可信入口（不持 root）。
- `cellhive token --ns <ns> --kind <kind> --name <name|glob> [--iss <name>] [--ttl 5m] [--key <b64>]`：统一签发 scoped token（平台 key / 委派 issuer key / 直接给定 key）；委派令牌强制 `--ttl>0`；`kind`/`name` 支持 `*`/`pre*`。

## 配置（环境变量）

| 变量 | 默认 | 作用 |
|---|---|---|
| `CELLHIVE_ADMIN_JWT` | 空 | 有值则改用 `Bearer`（ns 级授权） |
| `CELLHIVE_ADMIN_URL` | `http://127.0.0.1:8082` | admin 端点 |
| `CELLHIVE_BIN` | 自动 | dev `--strict-build` 用的打包器 |
| `CELLHIVE_CONTROL_URL` | `http://127.0.0.1:7001` | 内部端点 |
| `CELLHIVE_ESBUILD` | 自动查找 | esbuild 路径 |
| `CELLHIVE_PERF_GATE` | 空 | `make perf-test` 设置，启用性能门断言 |
| `CELLHIVE_ROOT_KEY` | 必填 | 派生 admin（`CELLHIVE_ADMIN_TOKEN` 可覆盖）与 internal 令牌 |
| `CELLHIVE_S3_TEST_ACCESS` / `_SECRET` / `_BUCKET` | `minioadmin`/`minioadmin`/`cellhive` | S3 集成测试凭据 |
| `CELLHIVE_S3_TEST_ENDPOINT` | 空 | 指向既有 S3；空则 `make s3-test` 尝试起 MinIO，docker 不可用则 skip |
| `CELLHIVE_WORKERD` | 自动查找 | workerd 路径（优先 pin `1.20260615.1`） |
| `TEST_BYTES` | — | `internal/config` 单测用 |


> 解析规则（字符串/布尔/时长/字节/列表）见 [`../configuration.md`](../configuration.md#解析规则)。

## 关键不变量

- 生产打包 **Go + esbuild，无 Node**。
- dev CLI 不属生产产物，且 Miniflare/workerd 必须与 pinned 版本同期。
- 未知 wrangler 字段/flag/过新兼容日期**部署期拒绝**。

## 源码位置

`cmd/cellhive/`；`internal/{bundler,wrangler,wranglercompat,workerdbin}`；`cli/`。

## 测试锚点

`make cli-test`、`internal/bundler`、`internal/wrangler`、`internal/wranglercompat`；`cmd/cellhive TestMintScopeToken`（签发）、`internal/scopedtoken`（glob/issuer）、`internal/server TestScopeAuthDelegated`。

## 相关文档

[`wrangler-compat.md`](../wrangler-compat.md)、[`dev-mode.md`](../dev-mode.md)、[`security.md`](../security.md)（scoped token 范围与委派）、[`control-plane.md`](../control-plane.md)（签发 CLI）

_最后更新：2026-09-20_
