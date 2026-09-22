# 对象存储

## 硬要求（全部必需）

1. **原子条件创建**（S3 `If-None-Match: *`；原生 AppendObject provider 为位置 0 或 tombstone 后的当前长度）；
2. **条件覆盖**（S3 `If-Match: <etag>`；原生追加 provider 为当前长度位置追加新帧）；
3. **read-after-write 一致性**；
4. **ranged read**（返回请求的字节范围且字节正确）；
5. **presigned URL / 短期 scoped 凭据**（供 bundle/assets 直连读取，ADR-030）；
6. **原子条件删除**（S3 `DeleteObject` + `If-Match: <etag>`；原生追加 provider 为当前长度位置追加 tombstone）：owner/lease 条件释放依赖它（ADR-134/189）；忽略条件的存储不能作状态权威。

**启动探针**：每节点启动先做 provider 预检；COS 读取 `HEAD Bucket` 的 `X-Cos-Bucket-Az-Type`，值为 `MAZ` 时明确拒绝（腾讯云不支持在多 AZ 桶使用 APPEND Object）。随后执行条件创建、重复创建拒绝、CAS、旧版本 CAS 拒绝、ranged read、旧版本条件删除拒绝及当前版本条件删除；任何一项失败均退出，不开始服务。运维用 `cellhive diagnose` 在已启动节点上再次运行同一套**写入式**探针（ADR-135），并非只读操作。启动探针不代替并发/线性化或 presigned URL 的独立验证。

## 供应商

| 供应商 / 接入方式 | 状态权威 | 备注 |
|---|---|---|
| 文件系统（`CELLHIVE_BUCKET` 为空） | ✅（单节点开发） | 仅本地单进程开发；不作云端多节点权威 |
| S3 API（`s3://<bucket>`） | 逐实例验证 | 必须满足本页全部原子语义；不能从“S3 兼容”推断任意供应商可用 |
| 本地 MinIO（`scripts/s3-integration.sh` 固定镜像 `RELEASE.2025-02-18T16-25-55Z`） | ❌ | 2026-09-22 实测条件创建/CAS/ranged read 通过，但 stale `DeleteObject If-Match` 返回成功；不满足 owner/lease 条件释放，启动探针应拒绝。不能把先前仅测试四条件写的结果视为权威桶验收 |
| 原生 OSS（`oss://`） | OSS 北京测试桶通过；其它桶逐实例验证 | 隔离键并发 claim/CAS/删除与启动诊断通过；同宿主进程 `SIGKILL`→新目录接管，192 并发 5000 次写全 ACK，冷恢复核对 6470 个已 ACK 值，missing=0/wrong=0，SQLite integrity=ok。 |
| 原生 COS（`cos://`） | COS 香港 `cell-1376795072` 测试桶通过；其它桶逐实例验证 | 隔离键并发 claim/CAS/删除与启动诊断通过；同宿主进程 `SIGKILL`→新目录接管，192 并发 5000 次写全 ACK，冷恢复核对 6488 个已 ACK 值，missing=0/wrong=0，SQLite integrity=ok。原 `vwork-hk-1376795072` 桶的 AppendObject 返回 405，仍不合格。 |
| COS / OSS 的 S3 API | ❌（实测配置） | 2026-09-22 对可用桶以独立 `itest/` 键探测：COS 对重复 `If-None-Match:*` PUT 返回成功，OSS 对首个该 PUT 返回 `NotImplemented`；均不满足 owner 选举。未对其它账户/端点作普遍结论 |

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
- bundle/assets 读取：runtime 先向 cell-agent 的内部鉴权端点申请短期只读 presigned URL，再直连对象存储；本地 FS 或后端不支持 presign 时回退内部鉴权点查，不持长期桶凭据（ADR-030）；
- 管理后台绝不接触桶凭据（只经 cell-agent）。

## 生命周期与 GC

- **保留版本**：bundle 按内容寻址去重；版本删除走 lifecycle（引用计数/`deploys` 指针清理）；
- **LTX**：L0 段后台 **compaction → L1**（`min_txids`/`min_mb`）；大 cell **paging**（稀疏文件按需）；
- **快照**：接管/大库使用 L9 快照；checkpoint 对齐时产生；
- **清理**：worker/app 删除走软删 + `purges` 作业（ADR-131）；**purge 只对仍处软删状态的实体生效**，deploy/create 会取消作业（ADR-135）；数据侧 hook 已闭环：各 cell-agent 幂等清理本地副本与桶对象，并以有界分页分轮续跑（ADR-142/169）；
- **枚举**：仅运维命令，有界分页；**热路径禁止 LIST**。

## 性能

- 桶**同区/就近**；fleet 模式下 S3 **后台合批上传，不在写 ack 路径**；
- 每 ack 的对象 PUT 数应远小于 1（group commit）；
- 单节点降级才逐笔等 bucket（~90ms）。

## 待细化

- 各供应商 presigned URL 的兼容性与 TTL；
- lifecycle 规则与成本模型；
- backup 角色的快照/归档策略；
- 云端对象存储其它供应商及不同账户的原子条件语义、presigned URL 和并发竞争测试（C 类环境；COS/OSS 上述实测不等于全部云供应商）。

_最后更新：2026-09-22_
