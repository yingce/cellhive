# 静态网站与 React 部署

ADR-206 实施中。目标为 Cloudflare Workers Static Assets 标准配置与行为兼容；以真实测试结果更新本页。

官方契约：
- https://developers.cloudflare.com/workers/static-assets/binding/
- https://developers.cloudflare.com/workers/static-assets/routing/worker-script/
- https://developers.cloudflare.com/workers/static-assets/routing/single-page-application/
- https://developers.cloudflare.com/workers/static-assets/routing/advanced/html-handling/

React/Vite 在本地或 CI 构建，上传 `dist`；构建时变量和 runtime vars 分离。可直接参考 `examples/react-static/`。SSR 必须使用 workerd 适配器。

## 当前实现

- assets-only 配置会由 CLI 上传固定 404 Worker bundle，因而仍可复用不可变版本、发布、回滚和路由模型。
- `assets.binding` 会注入用户指定名称的 `Fetcher`，其身份固定在当前版本的 namespace/worker/token；不能访问其他租户或版本。
- `.assetsignore` 支持常用 gitignore 形式的路径、目录、`*`/`?` 和 `!` 重新包含规则；`.assetsignore` 本身不上传。
- 同一 `(namespace, worker, assets token, path)` 只能写入一次；重试相同字节成功，写入不同字节失败。

完整 React 验收应检查：`/`、深层 history 路由、hash 资源、懒加载 chunk、API worker-first、版本回滚和 `.assetsignore` 文件不可访问。
