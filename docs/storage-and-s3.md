# 对象存储

## 硬要求（全部必需）

1. **条件创建**（`If-None-Match: *`）；
2. **条件覆盖**（`If-Match: <etag>`，CAS）；
3. **read-after-write 一致性**；
4. **ranged read**（返回请求的字节范围且字节正确）；
5. **presigned URL / 短期 scoped 凭据**（供 bundle/assets 直连读取，ADR-030）；
6. **条件删除**（`DeleteObject` + `If-Match: <etag>`）：owner/lease 的条件释放依赖它（ADR-134）；忽略该头的兼容存储会**退化为无条件删除**，属已知残余。

**启动探针**：每节点启动执行 4 次条件写 + 一次 ranged read 校验；不满足则停止。运维用 `cellhive diagnose`（只读）——覆盖条件创建/reject-create/CAS/reject-stale/ranged read/**条件删除**（stale etag 必须被拒 + 正确 etag 必须删除 + 对象必须消失，ADR-135）。

## 供应商

| 供应商 | 条件写 | 备注 |
|---|---|---|
| S3 兼容对象存储 | ✅ | 需支持条件创建/条件覆盖/read-after-write/ranged read；部署前用 `cellhive diagnose` 探针确认 |
| **本地 S3 兼容（MinIO 社区版）** | ✅ | **本仓实测通过**（RELEASE.2025-04-22，四条件写 + ranged read）；官方未认证生产；避开 `RELEASE.2025-09-06T17-38-46Z`（条件创建异常会导致首次 deploy 失败）；其他版本跑 `diagnose` 确认 |
| 不支持所需条件写的存储 | ❌ | 启动探针不通过则停止 |

## 桶角色（默认单桶 + 前缀，可拆）

| 角色 | 默认 | 内容 | 可拆独立桶 |
|---|---|---|---|
| `state` | 主桶 | cell 复制、owner/lease、nodes、node-logs、drain-token | ✅ |
| `code` | 主桶 `bundles/`、`deploys/` | Worker bundle（内容寻址）+ 版本指针 | ✅ |
| `assets` | 主桶 `assets/` | 静态资产（可挂 CDN） | ✅ |
| `r2` | 主桶 `r2/` | 租户 R2 虚拟桶 | ✅ |
| `backup` | 无 | 可选归档 | ✅ |

配置：每个角色可各自指定 `bucket / endpoint / region / credentials / prefix`；不填则回落主桶 + 前缀。

## Key 布局（示例）

```
cells/<ns>/<class>/<id>/owner.json
cells/<scope>/ltx/e<epoch>/<segment>
cells/<scope>/snapshot/<txid>
nodes/<node>.json
node-logs/<node>/<session>.json
fleet/capacity-v1.json
fleet/drain-token.json
bundles/sha256/<aa>/<hash>
deploys/<ns>/<worker>/<version>.json
assets/<ns>/<worker>/<token>/<path>
r2/<ns>/<bucket_name>/<object-key>
```

## 凭据

- **长期凭据只在 `cell-agent`**；
- `do-runtime`/`user-runtime` **不持**长期凭据；
- bundle/assets 读取：cell-agent 按 `(ns, worker, version)` 签发**短期只读** presigned/scoped 凭据（ADR-030）；
- 管理后台绝不接触桶凭据（只经 cell-agent）。

## 生命周期与 GC

- **保留版本**：bundle 按内容寻址去重；版本删除走 lifecycle（引用计数/`deploys` 指针清理）；
- **LTX**：L0 段后台 **compaction → L1**（`min_txids`/`min_mb`）；大 cell **paging**（稀疏文件按需）；
- **快照**：接管/大库使用 L9 快照；checkpoint 对齐时产生；
- **清理**：worker/app 删除走软删 + `purges` 作业（ADR-131）；**purge 只对仍处软删状态的实体生效**，deploy/create 会取消作业（ADR-135）；物理 SQLite 文件保留至显式清理；跨节点 cell-data 清理 hook 仍未闭环；
- **枚举**：仅运维命令，有界分页；**热路径禁止 LIST**。

## 性能

- 桶**同区/就近**；fleet 模式下 S3 **后台合批上传，不在写 ack 路径**；
- 每 ack 的对象 PUT 数应远小于 1（group commit）；
- 单节点降级才逐笔等 bucket（~90ms）。

## 待细化

- 各供应商 presigned URL 的兼容性与 TTL；
- lifecycle 规则与成本模型；
- backup 角色的快照/归档策略；
- 云端条件写/条件删除实测（C 类环境，见 [`testing.md`](./testing.md)）。

_最后更新：2026-09-19_
