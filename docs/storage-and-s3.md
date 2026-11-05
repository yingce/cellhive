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

## 桶命名空间（当前单桶）

当前实现只配置一个 `CELLHIVE_BUCKET`（及同一组 endpoint/region/credentials）；以下保留前缀在同一 bucket 内隔离，尚不能拆成独立桶：

| 前缀 | 内容 |
|---|---|
| `cells/`、`nodes/`、`node-logs/`、`fleet/` | cell 复制、owner/lease、节点日志与控制记录 |
| `bundles/`、`deploys/` | Worker bundle（内容寻址）与版本指针 |
| `assets/` | 静态资产 |
| `r2/` | 租户 R2 虚拟桶 |

独立 `state/code/assets/r2/backup` 桶角色与各角色凭据配置尚未实现；备份/归档也没有独立写入路径。

## Key 布局（示例）

```
cells/<ns>/<class>/<id>/owner.json
cells/<scope>/ltx/e<epoch>/<segment>
cells/<scope>/snapshot/<txid>
nodes/<node>.json
node-logs/<node>/<session>.json
wake/due/<UTC minute>/<scope>.json
wake/ptr/<scope>.json
fleet/capacity-v1.json
fleet/drain-token.json
bundles/sha256/<aa>/<hash>
deploys/<ns>/<worker>/<version>.json
assets/<ns>/<worker>/<token>/<path>
r2/<ns>/<bucket_name>/<object-key>
```

**provider 权威对象的物理布局（ADR-266）**：`cos://` / `oss://` 上，逻辑权威 key（上表的 `owner.json`、`owner-gen`、`nodes/<node>.json`、`node-logs/...`、`fleet/waker.json`）实际映射为按时间窗分段的 `.w<window>` 对象，例如 `cells/<scope>/owner.json.w<window>`。API 仍使用逻辑 key；对象存储控制台看到的是分段 key。每个段是 `CHL2` 帧流，读取只取 tail frame；旧段在 rollover 的 deep GC 中物理删除，每个逻辑 key 常态只保留当前与上一窗口。详见 ADR-266。

## 凭据

- **长期凭据只在 `cell-agent`**；
- `do-runtime`/`user-runtime` **不持**长期凭据；
- bundle/assets 读取：runtime 先向 cell-agent 的内部鉴权端点申请短期只读 presigned URL，再直连对象存储；本地 FS 或后端不支持 presign 时回退内部鉴权点查，不持长期桶凭据（ADR-030）；
- 管理后台绝不接触桶凭据（只经 cell-agent）。

## 生命周期与 GC

**ADR-307 优先于下文早期 GC 描述（契约先行，实施与验收中，未部署）**：压实继续发布，自动物理复制删除默认 **DEFERRED**。`Options.GC` 仅表示意图；删除 covered L0、旧 L1 或 `RetireEpochs` payload 必须显式设置 `OfflineQuiescent bool=true`（默认 `false`），由调用者在外部停止并排空所有 writers 和 readers，并在整个删除期间保持静默。生产后台不得启用，interval/timeout 不能绕过；无 owner 或 nil `Verify` 不是许可。离线模式逐次 Delete 前执行配置的 `Verify`，观察到丢失即停止，但检查与删除并非跨对象原子 fence；可选 payload ETag 也不能证明 owner 安全。

所有模式保留 marker/link/不可变 epoch-cut/snapshot-started/done/未知名称。manifest/index 完整覆盖证明仍必需，不能仅以水位判断；不可变 cut、cut-before-prune、大 marker 持久化-before-Serve 修复仍在实施与验收。`ReportDeferredGC`/`GCSkipped` 确定性报告延后，不把未删算成功；不新增 pending-deletion registry。代价是桶增长与历史 List 开销；完整在线 GC 所需 generation descriptor、单调 epoch reference、reader pinning 尚未解决。此边界仅针对复制物理 GC，bundle/assets 运维 GC 不变。

- **保留版本**：bundle 按内容寻址去重；版本删除走 lifecycle（引用计数/`deploys` 指针清理）；
- **LTX**：L0 段后台 **compaction → L1**（`min_txids`/`min_mb`）；大 cell **paging**（稀疏文件按需）；
- **快照**：接管/大库使用 L9 快照；checkpoint 对齐时产生；
- **清理**：worker/app 删除走软删 + `purges` 作业（ADR-131）；**purge 只对仍处软删状态的实体生效**，deploy/create 会取消作业（ADR-135）；数据侧 hook 已闭环：各 cell-agent 幂等清理本地副本与桶对象，并以有界分页分轮续跑（ADR-142/169）；
- **枚举**：仅运维命令，有界分页；**热路径禁止 LIST**（ADR-266 的唯一例外是：某个权威逻辑 key 当前窗口点查 miss 时，对**该单 key 前缀**做一次有界查找，用于找回上一窗口或长时间未写的最后一段；这不是 bucket 级 LIST）。

## 性能

### 当前工作树的预检与读取边界（2026-11-04，未部署）

- PageFetcher 仅跳过严格 canonical snapshot/part 且水位不高于页索引的对象；未知、混合、snapshot batch 保留解码。`NewPageFetcherMinBytes` 只在零 L0 且索引 image 小于阈值时提前回退整库恢复；任何 L0 都可能增加解码后的 commit，须照常构建并比较。
- compaction 仅在名称/大小能证明低于阈值时省正文读取；旧 L1 和完全覆盖的 L0 不作为新增触发量。快捷 covered-L0 GC 要求持久 index 水位不低于 manifest，随后复核 owner/epoch；缺失/落后 index 延后，读错误失败关闭。未知、混合、跨水位对象不由此快捷路径删除，也不重写 manifest/index。
- `CELLHIVE_COMPACTION_TIMEOUT=5m` 按 scope 独立计时；串行扫描仍可能被多个慢 scope 累积拖延。这不消除整库恢复的 segment 缓冲、旧 epoch GC 扫描或所有后台/Range 超时缺口。
- 原生 COS/OSS 与 S3 默认读取上限 512MiB（S3 的 Go option `MaxReadBytes` 可覆盖），不是总恢复内存预算。超限返回错误而非截断数据；provider 默认只有连接/TLS/响应头期限，body 依赖调用 context。peer 30s、supervisor HTTPStore 60s 是总请求期限，慢链路仍可能失败。

ADR-306 增量已通过本地审查与测试，详见 [当前问题](known-issues.md)；后续 epoch/lineage 修复按 ADR-307 实施与验收中，仍是发布门禁。上述水位/owner 检查不是在线删除授权，物理 GC 必须遵循本页 ADR-307 默认延后边界。本地增量接受不能作为完整恢复流式化、全部 GC 安全或已部署的证据。

- 桶**同区/就近**；fleet 模式下 S3 **后台合批上传，不在写 ack 路径**；
- 每 ack 的对象 PUT 数应远小于 1（group commit）；
- 单节点降级才逐笔等 bucket（~90ms）。

## 待细化

- 各供应商 presigned URL 的兼容性与 TTL；
- lifecycle 规则与成本模型；
- 独立桶角色、凭据与 backup 快照/归档策略；
- 云端对象存储其它供应商及不同账户的原子条件语义、presigned URL 和并发竞争测试（C 类环境；COS/OSS 上述实测不等于全部云供应商）。

_最后更新：2026-09-25_
