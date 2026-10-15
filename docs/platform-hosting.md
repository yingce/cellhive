# CellHive 平台前端托管

平台文档、AI Skill 和未来管理平台前端统一作为 CellHive Worker 发布。当前控制面 API 仍由 `cell-agent` 提供，前端不持有任何平台 root/internal/bucket 凭据。

## 推荐拓扑

```text
公网
 ├── docs.example.com      → user-runtime → platform/docs
 ├── admin.example.com     → cell-agent:8082（CLI/Wrangler 控制面）
 ├── admin-ui.example.com  → user-runtime → platform/admin（未来）
 └── app.example.com       → user-runtime → tenant Worker

私网
 └── cell-agent:8082    控制面 API（Admin token/OIDC，非租户通用出口）
```

## 文档与 Skill 发布

源文件：

```text
docs/ai/
skills/
```

构建发布目录：

```bash
scripts/build-ai-docs-site.sh
```

然后发布：

```bash
cellhive project init --namespace platform --profile production
cellhive wrangler deploy --name docs --config docs-site/wrangler.jsonc
cellhive domain add platform docs.example.com
cellhive route add platform docs.example.com docs
```

发布产物必须包含：

```text
/docs/ai/catalog.json
/docs/ai/manifest.json
/skills/manifest.json
/skills/cellhive/manifest.json
/skills/cellhive/SKILL.md
```

这些路径是公开只读资源；Admin token、JWT、root key、S3 凭据不得进入静态产物。CLI 的 `CELLHIVE_ADMIN_URL` 指向 `admin.example.com` 控制面；Skill 的 `CELLHIVE_SKILL_BASE_URL` 指向 `docs.example.com` 文档 Worker。文档 Worker 没有 `/docs` 挂载前缀，因此构建产物中的 `ai/`、`skills/` 和参考 Markdown 文件直接对应站点根路径。

## 管理平台前端

未来管理平台前端构建为 `platform/admin` Worker：

```text
platform-admin/dist/
platform-admin/wrangler.jsonc
```

前端通过配置的 Admin API URL 调用控制面，并使用浏览器安全的 OIDC/session 方案。前端发布不改变 cell-agent 的私网鉴权边界；所有 namespace、resource、deploy、rollback、audit 写操作仍由 cell-agent 授权和持久化。

## 版本与回滚

```text
platform/docs v1 → assets token A
platform/docs v2 → assets token B
                         ↓
                    active pointer
```

文档或管理前端发布失败时，active version 不切换；发布成功后可以使用：

```bash
cellhive rollback platform docs
cellhive rollback platform admin
```

未被任何版本引用的 bundle/assets 由 CellHive GC 清理。
