# 静态网站与 React 部署

ADR-206 的 CF-API + assets binding 部分已于 2026-11-01 实现；完整 React E2E 仍待补。目标为 Cloudflare Workers Static Assets 标准配置与行为兼容；以真实测试结果更新本页。

官方契约：
- https://developers.cloudflare.com/workers/static-assets/binding/
- https://developers.cloudflare.com/workers/static-assets/routing/worker-script/
- https://developers.cloudflare.com/workers/static-assets/routing/single-page-application/
- https://developers.cloudflare.com/workers/static-assets/routing/advanced/html-handling/

React/Vite 在本地或 CI 构建，上传 `dist`；构建时变量和 runtime vars 分离。可直接参考 `examples/react-static/`。SSR 必须使用 workerd 适配器。

## 当前实现

- Wrangler 4 的 CF-API metadata 支持 `{"type":"assets","name":...}` binding；4.133 可能省略 `assets.config.binding`，服务端采用 `bindings` 里的资产绑定名。有 `assets.jwt/config` 时注入版本资产 token，assets-only 部署会使用固定 404 Worker，重复部署可继承 active 版本的资产 token。
- Wrangler 接入走 `CELLHIVE_ADMIN_ADDR`（默认 `:8082`）上的 `/client/v4/**`；`CLOUDFLARE_API_TOKEN` 使用无状态 `kid=issuer/<pattern>` 的 HS256 namespace JWT（`type=namespace`、`ns` 数组、`aud=cellhive`），或可信运维的短期全局 bootstrap admin JWT。服务端校验 pattern 上限、固定最大 TTL86400秒、有效期及 `account_id`，不查询 issuer/namespaces 注册表授权；`/accounts` 仅列出允许的 namespace。恰好32位十六进制旧 issuer ID 拒绝且无回退；无静态 admin token、OIDC 或旧 claims。assets 会话绑定父 pattern/namespace/exp，不扩权或延寿，无即时 issuer 撤销。ADR-304 无状态修订实施与验收中，未部署，旧有状态测试不证明本次通过。
- CF-API 错误码区分：真实 multipart 失败为 `invalid_multipart`；未知 binding 为 `unsupported_binding`；assets 缺配置/名字不一致为 `invalid_binding`；超限为 `payload_too_large`。
- 已用 Wrangler 4.133.0 真实 CLI 验证 Worker+assets、重复部署、assets-only 和租户 JWT 部署（`TestReviewWranglerAssetsRealCLI`，需要 `CELLHIVE_TEST_WRANGLER_CLI`）。

- assets-only 配置会由 CLI 上传固定 404 Worker bundle，因而仍可复用不可变版本、发布、回滚和路由模型。
- `assets.binding` 会注入用户指定名称的 `Fetcher`，其身份固定在当前版本的 namespace/worker/token；不能访问其他租户或版本。
- `.assetsignore` 支持常用 gitignore 形式的路径、目录、`*`/`?` 和 `!` 重新包含规则；`.assetsignore` 本身不上传。
- 同一 `(namespace, worker, assets token, path)` 只能写入一次；重试相同字节成功，写入不同字节失败。
- COS/OSS 的 `assets/` 对象使用原生 PutObject 禁止覆盖选项执行原子创建，包含零字节文件；冲突交由 artifact 层读取并比较内容。不得使用先读后普通 Put，也不得对空文件使用 position=0 append，因为这两种方式无法保证首次写入后不可覆盖。控制面 authority 与 LTX 路径保持原有 append/fence 协议。

完整 React 验收应检查：`/`、深层 history 路由、hash 资源、懒加载 chunk、API worker-first、版本回滚和 `.assetsignore` 文件不可访问。
