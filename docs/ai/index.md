# CellHive AI 文档路由

这是给 AI agent 和开发者使用的在线文档入口。能力事实以 [`catalog.json`](catalog.json) 为准；Skill 只负责意图识别、边界、安全检查和验证流程，不复制完整 API 表。

## 路由顺序

1. 识别用户目标：本地开发、部署、静态站、SSR、binding、路由、运维、故障排查，或"要一个示例/组合场景"；
2. 查 [`catalog.json`](catalog.json)，匹配 `id`、`keywords`、`status`；
3. 打开对应的 canonical reference；
4. 如果用户要示例、模板或场景组合，先打开 [`examples/README.md`](../../examples/README.md)（中文：[`README.zh.md`](../../examples/README.zh.md)），按功能表/组合场景选择目录；
5. 检查当前项目的 `wrangler.jsonc`、`.cellhive/project.json`、profile 和运行时状态；
6. 只在有命令输出或 HTTP/测试证据时声明完成。

## 常用入口

| 目标 | 文档 |
|---|---|
| 示例、模板、组合场景、可运行代码 | [`examples/README.md`](../../examples/README.md)、[`examples/README.zh.md`](../../examples/README.zh.md) |
| 登录、profile、namespace、多 Worker、部署 | [`docs/cli.md`](../cli.md) |
| Wrangler 配置与兼容边界 | [`docs/wrangler-compat.md`](../wrangler-compat.md) |
| 本地 Miniflare/workerd 开发 | [`docs/dev-mode.md`](../dev-mode.md) |
| React/Vite 静态站、Assets、SPA | [`docs/static-sites.md`](../static-sites.md) |
| KV/D1/R2/Queue/DO/Workflow/Service | [`docs/bindings.md`](../bindings.md) |
| Workerd/CF 能力矩阵 | [`docs/compatibility-matrix.md`](../compatibility-matrix.md) |
| 域名和路径路由 | [`docs/routing.md`](../routing.md) |
| 部署和对象存储 | [`docs/deployment.md`](../deployment.md)、[`docs/storage-and-s3.md`](../storage-and-s3.md) |

## 示例与场景

- **单能力示例**看 [`examples/README.md`](../../examples/README.md) 的功能表（KV/D1/R2/Queue/Cron/DO/Workflow/assets/AI/Hyperdrive/Vectorize…）；
- **组合场景**看同一 README 的组合场景表（SaaS API、上传处理、实时房间、已有数据库、RAG、定时 ETL、内部服务、静态站 + API）；
- 本地运行：`cd cli && bun run src/index.ts dev ../examples/<name>`；
- 生产路径：先登记资源，再 `cellhive deploy <ns> - --config examples/<name>/wrangler.jsonc`；示例不自动 provisioning；
- 示例是演示，不是生产模板；复制前先读对应 `index.js`/`wrangler.jsonc`，不要把示例中的占位密钥当真。

## 明确边界

- CellHive 使用 [cellhive-workerd](https://github.com/yingce/cellhive-workerd) fork（ADR-264）；fork 改动只在 fork 仓，本仓通过 loader/bindings/capnp 集成；
- `cellhive dev` 是 Bun + Miniflare，本地开发不连接真实 cell-agent；
- `cellhive deploy` 负责 Worker bundle/assets 发布，但不替框架执行 `npm install` 或 `npm run build`；
- 资源通常必须先通过 `cellhive resource`/对应资源命令显式登记，部署不自动 provisioning；
- `Cache API`、Browser、Email、Python Workers 等能力按矩阵显式拒绝；
- 兼容 CF 不等于证明目标 namespace、资源或域名已经存在，先做 live inspection。
