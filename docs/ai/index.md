# CellHive AI 文档路由

这是给 AI agent 和开发者使用的在线文档入口。能力事实以 [`catalog.json`](catalog.json) 为准；Skill 只负责意图识别、边界、安全检查和验证流程，不复制完整 API 表。

## 路由顺序

1. 识别用户目标：本地开发、部署、静态站、SSR、binding、路由、运维或故障排查；
2. 查 [`catalog.json`](catalog.json)，匹配 `id`、`keywords`、`status`；
3. 打开对应的 canonical reference；
4. 检查当前项目的 `wrangler.jsonc`、`.cellhive/project.json`、profile 和运行时状态；
5. 只在有命令输出或 HTTP/测试证据时声明完成。

## 常用入口

| 目标 | 文档 |
|---|---|
| 登录、profile、namespace、多 Worker、部署 | [`docs/cli.md`](../cli.md) |
| Wrangler 配置与兼容边界 | [`docs/wrangler-compat.md`](../wrangler-compat.md) |
| 本地 Miniflare/workerd 开发 | [`docs/dev-mode.md`](../dev-mode.md) |
| React/Vite 静态站、Assets、SPA | [`docs/static-sites.md`](../static-sites.md) |
| KV/D1/R2/Queue/DO/Workflow/Service | [`docs/bindings.md`](../bindings.md) |
| Workerd/CF 能力矩阵 | [`docs/compatibility-matrix.md`](../compatibility-matrix.md) |
| 域名和路径路由 | [`docs/routing.md`](../routing.md) |
| 部署和对象存储 | [`docs/deployment.md`](../deployment.md)、[`docs/storage-and-s3.md`](../storage-and-s3.md) |

## 明确边界

- CellHive 使用 stock workerd，不修改或 fork workerd；
- `cellhive dev` 是 Bun + Miniflare，本地开发不连接真实 cell-agent；
- `cellhive deploy` 负责 Worker bundle/assets 发布，但不替框架执行 `npm install` 或 `npm run build`；
- 资源通常必须先通过 `cellhive resource`/对应资源命令显式登记，部署不自动 provisioning；
- `Cache API`、Browser、Email、Python Workers 等能力按矩阵显式拒绝；
- 兼容 CF 不等于证明目标 namespace、资源或域名已经存在，先做 live inspection。
