# CellHive CLI 使用

## 目标

CellHive CLI 使用 Wrangler 配置，但通过 CellHive Admin API 发布。开发者可以把项目归属和凭据分开保存：

- 项目目录 `.cellhive/project.json`：namespace、profile、多 Worker 配置；可提交 Git；
- 用户配置 `~/.config/cellhive/config.json`：Admin URL、token/JWT；权限 `0600`，不提交项目。

## 登录 profile

```bash
cellhive login \
  --profile staging \
  --url https://cellhive.example.com \
  --token "$CELLHIVE_DEPLOY_TOKEN"
```

也可以使用 JWT：

```bash
cellhive login --profile production \
  --url https://cellhive.example.com \
  --jwt "$CELLHIVE_ADMIN_JWT"
```

检查 profile：

```bash
cellhive auth list
cellhive auth current
cellhive auth test
cellhive auth logout staging
```

环境变量覆盖：

```bash
CELLHIVE_CLI_CONFIG=/path/to/config.json
CELLHIVE_PROFILE=production
```

命令行环境变量优先于 profile；`CELLHIVE_ADMIN_TOKEN`/`CELLHIVE_ADMIN_JWT` 优先于 profile；项目中的 `.cellhive/project.json` 不得保存秘密。

## 初始化项目

```bash
cellhive project init --namespace acme --profile staging
```

生成：

```json
{
  "profile": "staging",
  "namespace": "acme"
}
```

多 Worker 项目可以扩展为：

```json
{
  "profile": "staging",
  "namespace": "acme",
  "workers": {
    "web": "apps/web/wrangler.jsonc",
    "api": "apps/api/wrangler.jsonc"
  }
}
```

部署：

```bash
cellhive wrangler deploy
cellhive wrangler deploy --name api
cellhive wrangler deploy --config apps/web/wrangler.jsonc
```

优先级：

```text
显式 --namespace / --config
  > .cellhive/project.json
  > 缺少配置时报错
```

## 开发与部署

本地开发：

```bash
npx wrangler dev
```

或使用 CellHive 的 Bun/Miniflare 开发 CLI：

```bash
cellhive dev .
```

构建和发布：

```bash
npm run build
cellhive wrangler deploy
```

CellHive 不会替框架执行 `npm install` 或 `npm run build`；React/Vite、SSR 框架的构建仍由项目脚本或 CI 负责。

## 使用官方 Wrangler

CellHive CLI 可以输出官方 Wrangler 所需的标准环境变量：

```bash
eval "$(cellhive env)"
npx wrangler deploy
```

输出映射为：

```text
CLOUDFLARE_API_BASE_URL → CellHive /client/v4 API
CLOUDFLARE_API_TOKEN    → profile admin token/JWT
CLOUDFLARE_ACCOUNT_ID   → CellHive namespace
```

`cellhive env` 只打印 shell exports，不启动子进程，也不会修改当前 shell；不要把输出写入项目文件或 CI 日志。

## 安全边界

- `CELLHIVE_ROOT_KEY` 是平台根密钥，不应放入项目配置；
- CI 推荐使用独立的 namespace-scoped admin token 或 JWT；
- profile 文件权限为 `0600`；
- `wrangler.jsonc` 中的 `account_id`/项目 namespace 是非敏感标识；
- `cellhive wrangler` 不访问 Cloudflare，而是访问配置的 CellHive Admin API。
