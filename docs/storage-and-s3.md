# 对象存储

## 硬要求（全部必需）

1. **原子条件创建**（S3 `If-None-Match: *`；原生 AppendObject provider 为位置 0 或 tombstone 后的当前长度）；
2. **条件覆盖**（S3 `If-Match: <etag>`；原生追加 provider 为当前长度位置追加新帧）；
3. **read-after-write 一致性**；
4. **ranged read**（返回请求的字节范围且字节正确）；
5. **presigned URL / 短期 scoped 凭据**（供 bundle/assets 直连读取，ADR-030）；
6. **原子条件删除**（s3:// 控制记录为 `PutObject If-Match` tombstone；原始 S3 物理删除仍需 `DeleteObject If-Match`；原生追加 provider 为当前长度位置追加 tombstone）：owner/lease 条件释放依赖它（ADR-134/189）；忽略条件的存储不能作状态权威。

**启动探针**：每节点启动先做 provider 预检；COS 读取 `HEAD Bucket` 的 `X-Cos-Bucket-Az-Type`，值为 `MAZ` 时明确拒绝（腾讯云不支持在多 AZ 桶使用 APPEND Object）。随后执行条件创建、重复创建拒绝、CAS、旧版本 CAS 拒绝、ranged read、旧版本条件删除拒绝及当前版本条件删除；任何一项失败均退出，不开始服务。运维用 `cellhive diagnose` 在已启动节点上再次运行同一套**写入式**探针（ADR-135），并非只读操作。启动探针不代替并发/线性化或 presigned URL 的独立验证。

## 供应商

| 供应商 / 接入方式 | 状态权威 | 备注 |
|---|---|---|
| 文件系统（`CELLHIVE_BUCKET` 为空） | ✅（单节点开发） | 仅本地单进程开发；不作云端多节点权威 |
| S3 API（`s3://<bucket>`） | 逐实例验证 | 必须满足本页全部原子语义；不能从“S3 兼容”推断任意供应商可用 |
| 本地 MinIO Docker | 条件 PUT 适配实测通过 | 原始 DELETE If-Match 不安全；当前 s3:// 通过原 key 的 CAS tombstone 保护控制记录，LTX 保持原生。`scripts/s3-integration.sh` 使用固定 pgsty 镜像 digest。 |
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
cells/<ns>/<class>/<id>/own.json
cells/<scope>/ltx/e<epoch>/<level-4hex>/<min>-<max>.ltx
cells/<scope>/ltx/e<epoch>/0009/<min>-<max>.ltx
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

**owner 物理布局**：逻辑 key 为 `cells/<scope>/own.json`。COS/OSS 使用已有 CHA3 根及 `.gN` 条件追加段；s3:// 控制记录在原 key 写 CHC1 帧（magic、删除位、16 字节随机 revision、原 payload），每次写入 revision 更新。条件删除写 tombstone，重建用 tombstone ETag CAS，根不物理删除。没有新增 sidecar；LTX 格式和路径不变。旧 s3:// 控制格式不得混跑，需离线迁移/空测试桶。

s3:// 的非控制对象不提供未经验证的条件 DELETE，返回 ErrNotSupported。资源 retirement 清理只对持久退休下界覆盖的旧 epoch 对象物理删除，successor 使用新 epoch；这与初始 purge 的隔离规则相同，不放宽普通 mutable 对象的删除条件。控制列表过滤 tombstone，普通 LTX 扫描不逐对象 GET。逻辑 tombstone 仍占一个对象，不能由普通 lifecycle 规则删除控制前缀，否则会失去删除重建的排他边界。

## 凭据

- **长期凭据只在 `cell-agent`**；
- `do-runtime`/`user-runtime` **不持**长期凭据；
- bundle/assets 读取：runtime 先向 cell-agent 的内部鉴权端点申请短期只读 presigned URL，再直连对象存储；本地 FS 或后端不支持 presign 时回退内部鉴权点查，不持长期桶凭据（ADR-030）；
- 管理后台绝不接触桶凭据（只经 cell-agent）。

## 生命周期与 GC

当前工作树以固定 celld `90b4301` 为原生 LTX 契约（未部署）。压缩只新增目标 level 文件，保留源 LTX；已移除旧 `Options.GC`、L1 generation 保留参数、seal/pin/tombstone、独立 manifest/index。恢复从原生文件范围及内嵌索引规划，不生成完成标记。

物理复制回收由 epoch GC 执行：验证当前恢复链与当前 owner，保留链需要的 epoch 及直接前驱；只删除超过一小时 upload-age 窗口的更旧 epoch。先发布 celld 原生 `ltx/retired.json` 的 `retired_below`，再删除；中断可重试。未知对象、未知时间、截断的 listing 或缺失的前驱保守保留/报错。在线调用必须持有 resident cell 请求/淘汰 gate，确认分页已完全 materialize，并在扫描后及删除每个 epoch 前重新验证 owner。没有这些条件则不删除。显式离线维护的 `OfflineQuiescent=true` 要求外部保持 writers/readers 静默；单对象删除也验证原生路径、header 与 CRC。

一小时窗口不是活跃 epoch 的 L0/L1 TTL；活跃链可能长期保留源文件。该行为与上述固定 celld 的 additive compaction 一致。bundle/assets 运维 GC 不受此复制协议变更影响。

- **保留版本**：bundle 按内容寻址去重；版本删除走 lifecycle（引用计数/`deploys` 指针清理）；
- **LTX**：L0 段后台 **compaction → L1**（`min_txids`/`min_mb`）；大 cell **paging**（稀疏文件按需）；
- **快照**：接管/大库使用 L9 快照；checkpoint 对齐时产生；
- **清理**：worker/app 删除走软删 + `purges` 作业（ADR-131）；**purge 只对仍处软删状态的实体生效**，deploy/create 会取消作业（ADR-135）；数据侧 hook 已闭环：各 cell-agent 幂等清理本地副本与桶对象，并以有界分页分轮续跑（ADR-142/169）；
- **枚举**：仅运维命令，有界分页；**热路径禁止 LIST**（ADR-266 的唯一例外是：某个权威逻辑 key 当前窗口点查 miss 时，对**该单 key 前缀**做一次有界查找，用于找回上一窗口或长时间未写的最后一段；这不是 bucket 级 LIST）。

## 性能

### 当前工作树的读取边界（2026-11-09，未部署）

- PageFetcher 按 native level/range 选择覆盖链，读取内嵌索引，最终 commit 范围必须每页都有来源（lock page 除外）。激活后 cut 不变；旧文件消失时报错，不能切换更新 cut 混合页面。
- `NewPageFetcherMinBytes` 先读最终文件的 100 字节 header，按最终 page size × commit 判断；低于阈值可回退整库读取，无需独立 index 或“零 L0”限制。
- 压缩只读选中的未覆盖 source range，已覆盖 L0 与已有目标 level 正文不下载。独立 level 周期为 30 秒、5 分钟、1 小时；每 scope 超时默认 5 分钟。
- 所有原生 LTX（字节增量、续链 seed、spool）在 COS/OSS 统一使用 forbid-overwrite PUT；不再使用 AppendObject。AppendObject 仍用于独立的控制记录条件写。LTX 对象携带从 header 导出的标准 `litestream-timestamp` metadata（UTC RFC3339Nano）；multipart 在初始化时设置，不生成 sidecar。
- 原生 codec、恢复页写入及快照 spool 支持流式处理；完整校验后通过普通 PUT/multipart 发布，不依赖 If-None-Match/forbid-overwrite 或重传后的全量下载比较。COS 超过5MiB进入 multipart，默认32MiB/片；更大对象按10000片限制自适应增加片大小。失败时 abort 未完成上传。L1/L2/L3/L9 compaction 使用原生流输入和本地 spool，页索引内存仍随页数增长；普通 Get/RestoreChain 为缓冲接口。默认单对象 Get 上限512MiB，不能等同于流式上传上限。当前验收见下方报告链接。

- 桶**同区/就近**；fleet 模式下 S3 **后台合批上传，不在写 ack 路径**；
- 每 ack 的对象 PUT 数应远小于 1（group commit）；
- 单节点降级才逐笔等 bucket（~90ms）。

## 待细化

- 各供应商 presigned URL 的兼容性与 TTL；
- lifecycle 规则与成本模型；
- 独立桶角色、凭据与 backup 快照/归档策略；
- 云端对象存储其它供应商及不同账户的原子条件语义、presigned URL 和并发竞争测试（C 类环境；COS/OSS 上述实测不等于全部云供应商）。

_最后更新：2026-09-25_

### 原生协议验收审计（2026-11-09）

参考版本固定为 celld 90b43017241f81189453d326d05948f388b34652。以下审计区分协议证据和完整产品路径，不能将单个测试的通过扩大到所有路径。

| 要求 | 当前实现及证据 | 尚未完成的验证/工作 |
| --- | --- | --- |
| 原生 header/payload | internal/ltx/native*.go；当前 Go 与实际 Rust Compactor 双向互读，逐字段、逐页一致；CRC/LZ4/index/trailer 回归通过 | 严格 100k 故障验收通过，最新二进制另通过双节点恢复 |
| 快照识别 | NativeHeader.IsSnapshot 仅判断 MinTXID==1；完整页面覆盖、截断再扩展、损坏 CRC 回归；不依赖独立快照标记 | 无额外私有格式路径可豁免 |
| 原生增量 | sqlcapture/DO producer 直接编码 SQLite 页；原始 WAL offset/size/salts 和 Timestamp；600 次重复页更新跨 chunk 验证 | 严格 100k 三次 owner 崩溃切换通过 |
| level | 仅 0/1/2/3/9；L1/L2/L3 独立 UTC 30秒/5分钟/1小时；L9 全链快照 | 本轮真实 COS 已逐级验证；不是一小时墙钟长跑 |
| epoch 续链 | 零页 seed 为 cut+1；旧 epoch 必须恰好接到后继 cut；目录外部携带 epoch；原生 retired.json GC | 已停止备用节点并禁止 URL 回退；冷恢复 100000/100000 一致 |
| compaction | 与参考 Compactor 同样逐输入页缓冲、选择最后版本并按 commit 截断；additive 保留源对象；流式输入/spool 输出 | 整体性能不宣称等同于 celld；不同宿主和 provider 请求路径需分开测量 |

NoChecksum 是参考 celld 普通 sync 和 replica_compactor 使用的标准位，不是 CellHive 私有格式。celld 的独立 handoff snapshot_to_writer 还支持带 rolling post-apply checksum 的快照；CellHive decoder 能读该原生变体，当前 writer 使用 NoChecksum。此区别不能混写成“参考 celld 所有写入路径均禁用数据库校验和”；文件 CRC64 始终保留。

DO 的实际 HTTPStore 与直接 replica 路径现通过 ProvenWatermark 流式证明续链；HEAD/Range 保留大小与 ETag，chain=1 返回跨 epoch 原生恢复计划，PageFetcher 按页恢复。通用 Restore/RestoreChain 及外部 SegmentSource 回退仍可缓冲正文，页索引内存仍随页数增长。

最终验证：make test、make build、make vet 通过。最新二进制在指定 HK COS 双节点 SIGKILL 与清空本地 SQLite 后均恢复 100/100，冷首读 1.035300 秒（/tmp/native-do-proof-final-batch.log）。严格 100k/三次崩溃验收使用此前快照时钟修复版本；新增 HTTP/证明路径另经真实 COS 8.48MB 跨 epoch 恢复校验 SHA 一致（/tmp/native-cos-http-stream-proof.log），不混称同一二进制测试。

此前要求的通用 cell SQLite export/import 产品入口仍未交付，独立于本次六项协议对齐；既有一次性控制库迁移的成功不能代替通用导出/导入功能完成。


### 大 cell 资源预算（2026-11-09）

原生 LTX 格式保持不变。相邻 level 每轮最多256输入、64MiB；不可拆的超大文件单独推进。可选L9在下载正文前检查最新原生header的逻辑大小：通用默认80MiB，owner释放传入现有256MiB配置；超过逻辑大小或完整链输入预算时跳过，保留原恢复链，不产生部分快照，也不授权删除前驱。

维护/恢复/文件上传各有进程并发限制；长链用短Range代替长期占用所有远端连接。大文件正文流式化，索引仍随页数增长。完整物化最多缓存8MiB解码页面，每个Range不超过8MiB编码数据，并严格限制在相同源对象/ETag及请求页范围内。生产DO分页失败直接返回错误，不退回整段缓冲。原生文件CRC继续完整验证；编码、校验使用缓冲IO，索引尾部比较不再复制完整索引。

本地LTX缓存为可淘汰的64MiB/256项磁盘缓存，仅保存成功的新对象；复用前必须点查远端大小和ETag。它不是权威数据或远端marker，不缓存owner/水位。完整实现、10GiB实库测试边界和指标见[大cell验收报告](reports/2026-11-09-large-cell-storage.md)。默认强制capture快照上限仍为8GiB，可通过CELLHIVE_CAPTURE_SNAPSHOT_MAX调整；存储测试不能替代默认capture配置验收。

大cell缓存复核：仅已有文件的快照/压缩输出在成功新建后填充本地LTX缓存，普通字节增量不额外写缓存文件，避免确认写入增加小文件IO；有专门回归约束。

2026-11-09 通用 cell SQLite 迁移实施契约：管理员 GET /v1/control/cell/export?scope=ns/class/id 返回一致的完整 SQLite 文件；POST /v1/control/cell/import?scope=... 接收 SQLite 文件，仅接受无本地激活、无历史 LTX 的新目标，已有数据返回冲突。鉴权始终使用 scope 内命名空间，不能由其他 query 覆盖；二进制 body 不经过 JSON 探测。导入在上传前 integrity_check，流式编码原生 LTX，持有本地独占 admission gate 并在发布前后核验 owner/epoch，成功后从对象存储重新打开。最大导入64GiB，数据经过磁盘 spool、不整体读入内存。旧库应先导出 SQLite 后导入新目标；不新增 COS 标记文件、不改变原生格式。实施与验收中。

SQLite 文件迁移命令（验收中）：

```bash
cellhive cell export app/__kv__/SOURCE --output source.sqlite
cellhive cell import app/__kv__/TARGET source.sqlite
```

使用持有该 cell 的节点 admin URL（`CELLHIVE_ADMIN_URL`）；目标无 owner 时由接收节点声明。导出文件必须不存在，CLI 用临时文件及独占发布避免半文件和误覆盖。导入目标必须是全新的 cell；已经激活或拥有 LTX 的目标返回409。导入成功意味着完整原生快照 LTX（L0）已发布，并再次确认 owner/session/epoch 和资源删除 fence；后续读从对象存储恢复。命名空间授权以 scope 为准。操作并发上限2；输入与LTX均写本地临时文件，逐页编码，无新增远端文件类型。


上线前适配修订（2026-11-10）：S3 的 `r2meta/` 也使用原 key CHC1 条件 PUT 删除，覆盖写能安全清除先前 metadata。没有条件 DELETE 的 S3 使用 `wake/due/<minute>/<scope>/<192-bit-revision>.json` 不可变提示，ConditionalCreate 发布，每次调度使用新 revision；完成或过期提示可物理回收，不遗留随事件数量增长的 due tombstone。可变 `wake/ptr/` 仍保持 CAS tombstone；原生条件删除 provider 的 due 布局不变。此修订针对空目标或离线迁移，不自动转换旧 raw r2meta，也不在线回收此前生成的旧格式 due tombstone。没有增加任何 LTX marker 或改变 LTX 字节。

原生 DO SQLite 迁移（2026-11-10）：使用 DO 数据 scope（用户类 + object-addressed ID），不能把 shard owner scope 当作业务数据库。离线停止源 runtime 后导出；DO export 以原生 LTX 为准，不读取 agent 通用 CellStore 的 resident。导入新对象后，native 首次打开按对象 scope 恢复，无需为迁移制造 host manifest 或新的对象存储 marker。同 shard 及全新 runtime 目录恢复的真实 MinIO/OSS 验收见 reports/2026-11-10-native-do-transfer.md。

原生 DO 导入并发隔离（2026-11-10）：由 control owner 的当前投影验证 storage_id，解析既有 storage class/object ID 并独占对应 __do__ shard owner，之后才读取上传 body。任一运行中的同 shard DO 都会使导入返回409；导入期间该 shard 暂不可激活。普通 Claim、后台 rebalance/idle release 不可提前释放该占用，runtime claim/renew/release 不可借同名 node 绕过。成功响应前释放 shard，取消/校验失败也有界清理；对象存储错误时失败关闭，不返回成功。__do__ host/owner scope 不接受业务 SQLite 导入。原生 DO 的 capture 和独占 import 固定已验证 shard owner epoch 作为数据 epoch；新导入快照从 TXID 1..1 起步，接收 agent 的 data-owner 只验证节点权限。导入失败不承诺回滚不可变对象，重试不会覆盖已有数据。没有新增对象存储 marker。验收见 reports/2026-11-10-native-do-import-exclusion.md。

DO import 转发与释放恢复补充：非 control owner 节点使用流式 admin 转发，只有此导入路径采用30分钟总超时，保留客户端取消，且不自动重放。独占 shard 释放失败后进入进程内后台队列，每5秒触发一个有界清理 pass；每项仅尝试原 scope/epoch 的条件释放，接管后不删除 successor。单 pass 最多32项、15秒，每次释放最多2秒；不承诺每个任务5秒内完成。存储持续不可用时保持失败关闭，恢复后自动重试，进程退出后沿用节点租约接管，无新增桶文件。

后台清理也覆盖独占 owner 领取时的丢响应：实际条件写报错后登记本次候选 epoch，即使该写已在存储提交，也能在后台按精确身份释放。领取本身仍返回失败，不继续上传；若旧候选已被新 owner 取代，清理不会删除新 owner。

2026-11-10 完整 provider 验收修订：R2 sidecar 统一以 `r2meta/<ns>/<bucket>/<sha256(key)>` 命名，避免任意用户 key 与 native authority 代际后缀冲突。COS/OSS 使用既有 native append authority 的条件逻辑删除；S3 保持 CHC1 条件 PUT tombstone。原生 provider 在正文变更前预检 metadata 容量（默认64KiB，含1024字节生成字段预算）。旧 sidecar 仅离线迁移，不在线兼容；正文、LTX 格式不变。

COS/OSS R2 正文条件替换明确失败关闭：r2/ 的 CAS 不下沉到不具备已验证原子条件 PUT 的 S3 兼容端点；返回 ErrNotSupported、不改正文。普通覆盖写及 native append metadata CAS 不受影响。S3/MinIO 的条件替换继续使用已验证的条件 PUT。

## R2 原子发布（当前实现）

用户 R2 onlyIf 不再依赖 COS/OSS raw body CAS。正文写入 `r2/.versions/<ns>/<bucket>/<key-hash>/<random>` 后，以既有 `r2meta/<ns>/<bucket>/<key-hash>` 权威条件写发布正文引用和 metadata；条件判断与发布使用同一 authority token。已提交的正文永不原地覆盖。`r2/<ns>/<bucket>/<key>` 是非权威 listing 索引；List 必须读取发布记录过滤未提交/删除对象，不能采用索引对象的长度/ETag。GET/HEAD/Range 使用同一个 Snapshot，Presign 固定版本。

旧正文暂不在线删除，避免破坏已开始读取及签名 URL；这一点增加物理占用，逻辑 usage 不计保留版本。旧 raw R2/sidecar 布局不在线兼容。内部 namespace `.versions`、`.mpu` 拒绝所有业务读写。LTX 字节、目录没有变化；LTX 使用下述普通上传语义，不要求不可变条件创建。

## LTX 普通上传与幂等覆盖（2026-11-10 修订）

按用户确认及 celld 源码，LTX 的 byte/spool 路径使用普通 PUT/multipart，不附 If-None-Match 或 forbid-overwrite，不新增发布协调文件。同名重传由 epoch/TXID 路径、单写者捕获和压缩约束保证语义；副本读取固定 ETag，版本变化失败关闭。上传前仍校验完整 LTX，完整对象成功后才能推进持久水位。续链 seed 初始化仍验证已有内容的角色，不能用空 seed 替换增量。owner 和 R2 publication 的 CAS、静态 assets 的独立不可变契约不受影响。本修订替代此前 LTX 必须条件创建、快照保留首次时间戳的条款；普通重传可更新对象 ETag/时间戳，不承诺保留首次物理版本。


2026-11-10 后续修复替代固定e1阻塞状态：原生DO使用已验证shard owner epoch作为LTX数据epoch，独占导入固定相同代际，恢复沿既有原生链查找；data-owner仅作为接收节点权限fence。失主导入、迟到旧上传不覆盖接管者、真实COS fleet owner1→2→3分页恢复及SQLite导出/导入/冷重启通过。旧固定e1部署不可混跑，离线迁移，不新增防覆盖记录。详见[对象发布报告](reports/2026-11-10-object-publication-fixes.md)。
