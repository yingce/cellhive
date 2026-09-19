# 发布说明

## 未发布（main）

本版把路线图 P3–P5 的剩余项收口，全部以真实运行/测试为证据。

### 租户日志可选 OTLP 导出（ADR-172）
- `CELLHIVE_OTLP_LOGS=off|tail|all`（默认 `off`）：用与 traces 相同的 OTLP 端点/headers 导出 logs（`/v1/logs`）；`tail` 只在 `cellhive tail --worker` 的 TTL 订阅有效期间导出，退出后 ≤60s 停。
- 导出是旁路、best-effort、后台批量，不影响请求；内存 ring 与 `cellhive tail` 行为不变。
- **fleet 广播（ADR-173）**：订阅会经 lease 节点名单广播到其它 cell-agent（内部端点 + 节流），因此 `tail` 门控覆盖任意节点服务的同名 worker（无需切 `all`）。

### wake 索引修复（ADR-177）
- timer 的 wake 索引改为**提交前先发布**（索引只会领先、不会落后）；索引写失败则 timer 不提交（fail-closed）。
- 节点注册/认领 timer scope 时重建索引；新增有界轮转的本地修复扫描（`CELLHIVE_WAKE_REPAIR_INTERVAL=5m`、`_BATCH=256`，0 关），修复崩溃/桶写失败造成的缺条目。

### KV 写入校验对齐 Cloudflare（ADR-176）
- `put` 现在拒绝：key >512B、metadata >1KiB、`expirationTtl` <60s、`expiration` 在过去（分别 `key_too_large`/`metadata_too_large`/`bad_expiration`）；value ≤25MiB 不变。此前这些会被放行（比 CF 宽松）。

### 兼容性收口（ADR-175）
- **DO WebSocket**：`env.DO.get(id).fetch(升级请求)` 现在经 `/v1/do/connect`（owner lookup + ticket）直连 owner，`wsecho` 示例的 WS echo 可用。
- **R2 `writeHttpMetadata`**：本地 Proxy 包装，metadata 写入调用方 `Headers`（content-type 正常）。
- **DO alarm**：dispatch 补全 `storage_id`/`storage_class`（此前 alarm 会落到与 fetch 不同的 facet，`fires` 永远 0）并修无 scheme 地址导致的每 tick URL 解析失败。

### 示例兼容性修复（ADR-174）
- **KV `put`** 接受 `ReadableStream`/`ArrayBuffer`/`ArrayBufferView`/`Blob`（此前把请求体 JSON 成 `"{}"`）。
- **legacy DO 类**（不 `extends DurableObject`、只有 `fetch/alarm/webSocket*`）现在会被包装成 facet，counter/router/body/async/alarm 等示例可直接跑。
- **DO `fetch` 非 2xx** 原样返回状态与 content-type（不再变 500）；cell-agent 透传。
- **R2ObjectBody** 补 `body` 与 `writeHttpMetadata`（后者受 RPC 限制，见 known-issues）。
- 复测示例套件 + Hono 4.13.8（`node_modules` 打包、多路径、`--path` 挂载、KV binding）：除已登记缺口（wsecho 的 DO WS、alarm 需 gate）与策略拒绝（compat 日期/未登记字段）外全部通过。

### `cellhive wrangler` 命令/参数兼容收口
- `translateWrangler` 改为**表驱动**：已知 wrangler 命令要么映射到原生，要么给出可执行替代（`wranglerReject`），任何已知命令都不再落入 `unknown command`；子命令级替代补齐（`triggers deploy`/`queues consumer`/`workflows status|describe|trigger` 等）。
- `test`：`TestWranglerKnownCommandsHaveOutcomes`（覆盖表）、`TestWranglerRejectionsAreActionable`。

### `cellhive wrangler` 映射修复
- `wrangler d1|r2|kv` 前缀之前落到 `unknown command`；现转发到原生每域命令（`d1`/`r2`/`kv`，ADR-157），只有数据面 verb（`d1 execute`/`r2 object`/`kv key`）返回结构化拒绝（ADR-148）。
- `wrangler triggers deploy` 明确提示用 `cellhive deploy --config`（crons 随版本原子应用）；未知命令的提示补全兼容组。

### upload spool 断电安全（ADR-171）
- 异步上传 spool 的 append 改为原子写：tmp fsync → rename → 目录 fsync（叶子+父，记忆化，每进程一次）；断电不再丢已 spool 的段。
- `/metrics` 新增 `cellhive_upload_spool_file_syncs_total`/`_dir_syncs_total`/`_sync_seconds_total`。

### DO 输出门并发捕获 + ordered sweep（ADR-170）
- `dosupervisor.SyncAll` 对多个 facet 文件**有界并发**提交（`Concurrency` 默认 8），跨 scope 重叠持久化证明 RTT；同文件内仍按 txid 串行，门语义不变。
- `orderedDispatcher.sweep` 接进 `Do`：永久 txid 缺口导致的空闲 scope 会被主动淘汰并释放缓冲调用方。

### purge 游标分页删除（ADR-169）
- `objectstore.Objects.ListPage` + `bucket.PagedLister`：purge 数据侧不再物化整段键列表（S3 `StartAfter`、内存有界），边删边推进、预算用尽下一轮续跑。

### R2 list：include 与 delimiter（ADR-168）
- `bucket.list({include:["httpMetadata","customMetadata"]})` 按 CF 语义**按需**回填逐对象 metadata（每对象一次 sidecar 读；不请求则零成本）；未知 include 值 400。
- `bucket.list({delimiter:"/"})` 返回 `delimitedPrefixes`（公共前缀归并；扫描上限 10000，超出给 `truncated`+`cursor` 续扫）。

### 标准 OpenTelemetry 追踪（ADR-167）
- 新增 OTLP/HTTP 导出（官方 `go.opentelemetry.io/otel` SDK）：`CELLHIVE_OTLP_ENDPOINT`（空=关、零开销）、`CELLHIVE_OTLP_HEADERS`、`CELLHIVE_TRACES_SAMPLE_RATIO`（默认 0.01，`ParentBased` 尊重上游 sampled）。换后端只改端点，CellHive 不自存 trace。
- span：cell-agent `http.server`/`cell.durability_proof`/`peer.append`；do-supervisor gate/restore；workerd 侧 loader `http.server`、do host `do.invoke`/`do.gate`（JS 经 `/v1/internal/telemetry/spans` 汇总，`workerd/platform/telemetry.js`）。
- 修复 ADR-146 残余：props-bound binding 调用现在携带 `traceparent`。
- 对接指南见 `docs/tracing.md`（含 OpenObserve 的 compose `tracing` profile 与 Basic 鉴权示例）。

### 可观测性指标补齐 batch 2（ADR-166）
- cell-agent `/metrics` 新增 `cellhive_replication_bytes_total{kind=shipped|received}` 与 `cellhive_waker_fires_total{kind,outcome}`。
- do-supervisor（`-listen`，默认 `:18901`）新增 `GET /metrics`：`cellhive_do_wal_captured_bytes_total`、`cellhive_do_restore_seconds`（summary）、`cellhive_do_output_gate_timeouts_total`、`cellhive_do_alarms_fired_total{outcome}`、`cellhive_do_ws_sessions`；host actor 每次 invoke 后 best-effort 上报增量。
- `docs/observability.md` 的"计划中"指标清单已全部落地（无遗留）。

### 可观测性指标补齐（ADR-165）
- `/metrics` 新增：`cellhive_binding_calls_total{kind,outcome}`（ok/denied/error）、`cellhive_durability_proof_seconds`（写路径持久化证明延迟直方图）、`cellhive_owner_epoch_changes_total{role}`、`cellhive_takeover_total{outcome=success|failed|blocked}`、`cellhive_route_projection_version`、`cellhive_peer_hedge_fired_total`/`_won_total`。
- `docs/observability.md` 的告警建议改用真实存在的指标；仍待实现项（replication_bytes、waker_fires、do_*）继续明确标注为"计划中"。

### peer 自适应 hedge（ADR-164）
- fleet 复制先写 primary follower，超过等待（自适应 = `max(250ms, 4×最近最慢 append)`，上限 `CELLHIVE_PEER_HEDGE_MAX_MS`）再向下一 follower 发**第二份**，取先到的 ack；`CELLHIVE_PEER_HEDGE_MS` 默认 `adaptive`，`0`=只发 primary（单份，primary 失败才 failover），`>0`=固定 ms。
- **前置：spool 按 sequence 幂等**。`Spool.AppendBatch` 现在按 `ltx.Header.ID()`（epoch/kind/txid 区间/CRC）去重，重复/重试/重连/hedge 副本都是 no-op；否则恢复会因同 txid 重复而报"non-contiguous chain"。
- 主帧仍在 sender goroutine 内**同步写**（保持 lane 批序），hedge 副本才延迟发送；恢复路径按 `rank+StartTxID` 排序，故迟到副本的乱序到达可容忍。
- 测试：`internal/peer TestSpoolIdempotentPerSequence`、`TestShipBatcherHedgeSkipsSlowBackupWhenPrimaryFast`、`TestShipBatcherHedgeFiresOnSlowPrimary`、`TestShipBatcherHedgeOffSingleCopy`、`TestShipBatcherHedgeAllFail`、`TestShipBatcherHedgeAsync`、`TestShipBatcherAdaptiveHedgeWait`、`internal/config TestPeerHedgeMSEnv`。

### R2/D1 契约保真（ADR-163）
- **R2**：`get()` 返回完整 `R2ObjectBody`（`key/size/etag/httpEtag/version/uploaded/httpMetadata/customMetadata/checksums` + `text/json/arrayBuffer/blob`）；`put(key,value,{httpMetadata,customMetadata,md5,sha256})` 存 metadata sidecar 并校验 checksum；新增 `head()`；`list()` 透传 `truncated/cursor` 且对象带 `httpEtag/version`。实现要点：workerd RPC 会把**可枚举的函数属性序列化成可调用 stub**，所以对象既有数据字段又有正文方法。
- **D1**：`run()/batch()` 的 `meta` 增加 `last_row_id`（来自 SQLite `LastInsertId`）与 `changed_db`；绑定错误按 CF 形状抛出（`err.name = D1_ERROR/KVError/R2Error/QueueError` + `err.code`=平台码）。
- **后端**：`bucket.Statter`（S3 `HeadObject`，FS 回退读体）供 `head()`/range 全长；`r2meta/` 登记为保留前缀（`objectstore`，owner `r2`）。
- 测试：`internal/r2 TestR2MetadataStatAndChecksums`、`internal/d1 TestLastInsertID`、`internal/userruntime TestUserRuntimeR2ObjectFidelityAndD1Meta`（真 workerd 端到端）。

### DO 调用支持 RPC（ADR-162）
- `env.NS.get(id)` / `env.NS.getByName(id)` 现在返回方法可调用的 stub：`await env.ROOM.getByName("r1").addMessage("u1", "hi")`。
- **实现**：`kind:"rpc"` tagged JSON 信封复用 `/v1/do/invoke`（owner-hint/ticket/`409/5xx 不重放` 语义与 fetch 相同）；host 用**原生 JSRPC** 调 facet 方法，因此 Map/Date/ArrayBuffer 与共享引用/环无损；Go 侧 `json.RawMessage` 字节透传。
- **保真/限制**：tagged 编解码支持 `undefined`、`-0/NaN/±Inf`、bigint、Date、RegExp、Map、Set、ArrayBuffer、TypedArray/DataView、Error（含 cause）、URL、URLSearchParams、共享引用/环；不支持函数/符号/Promise/弱集合/流/`RpcTarget`/stub；类实例降级普通对象；args 与结果各 ≤ **8 MiB**（原路由体限 1 MiB 已相应放宽）；方法名保留 `fetch`/`alarm`/原型内部名/`__ch*`。
- **测试**：真 workerd 的 `internal/doruntime` / `internal/userruntime` e2e + `internal/server` 透传/限额；详见 `docs/testing.md`。

### cell-agent 按页冷启动（ADR-160）
- 新增 fault-in SQLite VFS `internal/pagedvfs`（`?vfs=cellhive-paged`）：主库是**按 pinned cut 的稀疏文件**，`xRead` 缺页时**同步**从复制链取该页并写入本地；`xWrite`/`xTruncate` 维护 hydration 位图；fault 失败 fail-closed。
- 页源 = `replica.PageFetcher`（L1 `index.bin` 按页 ranged 读 + 更新的 L0 尾巴）；链 < `CELLHIVE_PAGED_MIN_BYTES`（默认 256MiB）或未 compaction → 仍整克隆。
- 实测（真链路测试）：冷点读 **8 次页 fault / 771 页、8 次 ranged 读、整对象读 3,158B（镜像 3,158,016B）≈ 1000× 少**；`SnapshotPages` 前强制 `HydrateAll`，保证 LTX 快照不含稀疏零页。
- 开关：`CELLHIVE_PAGED_RESTORE`（默认开）、`CELLHIVE_PAGED_MIN_BYTES`、`CELLHIVE_PAGED_HYDRATE_MBPS`（默认 16，0=保持稀疏）、`CELLHIVE_PAGED_WINDOW_PAGES`（默认 64）。
- **窗口预取**：`replica.PageFetcher.ReadRun` 一次 ranged read 连取同 L1 对象内的相邻页（256KiB 预算），pagedvfs 缓存窗口 → 全扫 336 页只需 6 次桶读（此前每页一次）；`xTruncate` 回调丢弃陈旧窗口页。
- **稀疏感知磁盘口径**：`DiskFile.AllocBytes`（`st_blocks*512`）进入 `DiskUsage`/`/metrics` 与磁盘驱逐预算，paged cell 不再因 apparent size 误触发反压。
- **b-tree 感知 fault 策略**（内部页 child 单页取，避免 overflow 链被整段拉取；顺序 child 按 `SCAN_AHEAD`=64 预取到内存缓存，连续 child 合并成一次读）、`HydrateAll`/后台补齐批量取数、`/metrics` 六个 `cellhive_paged_*` 指标。
- **L1 逐帧 LZ4 压缩**（`WAL3` page-map v2 + `CID2` 页索引）：L1 3.16MB → 140KB（合成）/ 315KB → 86KB（真栈），页解码 1.1GB/s；`WAL2`/`CIDX` 向后兼容。
- **child 并发预取**：`CELLHIVE_PAGED_PREFETCH_WORKERS`（默认 4），散列 child 预取从 ~90ms 降到 45.7ms。
- **GC 兼容**：LTX L0/L1 GC 基于 manifest key + txid 水位，与编码格式无关；混入旧 `WAL2` 基线也能折叠并回收，`index.bin`/`manifest.json` 为同 key 覆盖写不进删除集。真栈连续折叠后 `L1/` 只留 1 个压缩快照+索引+manifest、L0 计数 0；compaction 日志新增 `deleted_l1`/`deleted_l0`/`stored_bytes`。
- **修复真 bug**：稀疏 paged 缓存跨进程重启/驱逐后被 base VFS 打开会读到零页（实测 `database disk image is malformed`）。新增 `<cell>.db.paged` 标记位：有标记即"缓存≠数据库"，打开时按 cut 复用缓存并重新注册（不匹配或无可页源则丢弃并从桶重建），全量物化后删标记。

### LTX 压缩开关（ADR-161）
- 新增 `CELLHIVE_LTX_COMPRESSION`（默认 `true`）：LTX page-map 用 LZ4（`WAL3`）；设为 `false` 退回 `WAL2` 固定帧（不压缩）。解码器/`PageLocs`/GC 对两种格式都兼容，`WAL2` 与 `WAL3` 可混链折叠回收。
- **性能复核（2026-09-18，同旧方法）**：
  - KV/D1 读写（单 cell，单机 8 核，FS bucket）：读 **+13~29%**、本地写 **+7~29%**（CGo 驱动），capture-ON 写 ≈ 持平（写上限=桶证明）；p50(c=64) put 5.11→3.98ms、get 1.71→1.40ms。原始：`docs/archive/bench/kv-d1-readwrite-cgo.txt`。
  - 单/双 cell（capture ON，FS）：双/单 ≈1.55x（与旧一致）；S3/MinIO 档 c≥16 **+33~59%**，双 cell 在单节点 MinIO 上仍无增益；capture OFF 与 FS 档一致（写不逐笔落桶）。
  - **压缩 A/B**：capture-ON 下 `CELLHIVE_LTX_COMPRESSION` on vs off 吞吐差异 **1~3% 以内**（同一 box 状态重复运行）→ 编码不在 ack 关键路径，无可见代价；桶对象仍小 3.7×。原始：`docs/archive/bench/ltx-compression-ab.txt`、`single-vs-dual-capture-cgo-on-repeats.txt`。

### Vectorize 迁到 SQLite 官方 vec1（ADR-159）
- **SQLite 驱动换 CGo**：`modernc.org/sqlite` → `mattn/go-sqlite3`（同为 SQLite 3.53.4，**文件/WAL 格式不变**），构建带 `sqlite_fts5/sqlite_dbstat/...` tags + `#error` 守卫；Dockerfile 装 gcc、`CGO_ENABLED=1`。
- **官方 ANN 扩展 `vec1` 静态编入**（`sqlite3_auto_extension`，不需要 `.so`）：IVFADC+OPQ、AVX2/NEON。
- **性能**：20k×256/K=10 —— 旧 Go KNN ≈48ms → **flat 精确 ≈6.3ms** → `vectorize rebuild` 建 ANN 后 **≈0.22ms**。
- **API 不变**：facade/端点/CLI 形状不变；新增 `cellhive vectorize rebuild|drop-ann` 与 `POST /v1/vectorize/rebuild`、`DELETE /v1/vectorize/ann`；`describe/stats` 多了 `ann` 字段；旧 ADR-158 cell 自动迁移。
- **差异**：`dot-product` 不支持（仅 cos/l2）；metadata 过滤为后置过滤；`dev` 仍不支持 vectorize。

### Vectorize（ADR-158）
- **Cloudflare 兼容的向量索引**：注册资源（`--dimensions` 1..1536 / `--metric cosine|euclidean|dot-product`，配置信封加密且不可变）+ facade `env.<BINDING>`：`insert/upsert/query/queryById/getByIds/deleteByIds/describe`（+ `listVectors`）。
- **score/filter/namespace 与 CF 对齐**：cosine=相似度（大者近）、euclidean=距离（小者近）、dot-product=内积；`$eq/$ne/$in/$nin/$lt/$lte/$gt/$gte` + 嵌套路径 + 隐式 AND；`topK` 与 `returnMetadata` 限额同 CF。
- **诚实实现**：SQLite 无向量类型/ANN（FTS5 只是全文，C 扩展在纯 Go 驱动里无法加载；workerd 也没有 vectorize 绑定类型），故以 **cell 内 float32 + Go 精确 KNN** 实现；默认上限 100k 向量，实测 20k×256≈47ms、5k×1536≈50ms。
- **CLI**：`cellhive vectorize create|list|delete|info|stats|insert|upsert|query|get-vectors|delete-vectors|list-vectors|create-metadata-index|list-metadata-index|delete-metadata-index`（数据面用根密钥现场铸造 scoped token）；`wrangler vectorize` 前缀透传；`--config` 的 `vectorize[{binding,index_name}]` 直接可用（资源=index_name）。
- **dev 明确不支持**：Miniflare 无本地实现，`cellhive dev` 遇到 vectorize 报错退出（不再假装可用）。

### 每域资源管理与 stats（ADR-157）
- **每域登记**：`POST|GET|DELETE /v1/<kind>/resources`（kv/d1/queue/r2/workflow/hyperdrive），scope 服务端缺省；队列死信重放 `POST /v1/queue/dead-letters/replay`；`/v1/control/*` 旧路径保留为兼容别名。
- **每域 stats**（metadata-only，读不放大）：`GET /v1/<kind>/stats` — 页/文件/WAL 字节、`kv_expires` 索引与过期、D1 表清单（`?tables=1` 的 dbstat 明细）、队列积压+滞后、R2 有界近似、workflow 实例估计、hyperdrive 登记、DO 索引聚合。
- **修复**：新库缺失 `kv_expires` 部分索引（TTL 清 sweeper 整表扫的真 bug）。
- **指标**：`/metrics` 新增 `cellhive_bucket_ops_total{op}`、`cellhive_list_calls_total`、`cellhive_owned_cells`、`cellhive_resident_cells`；`observability.md` 与代码对齐并标出计划指标。
- **CLI**：`cellhive kv namespace|d1|r2 bucket|queue|workflows|hyperdrive create|list|delete|stats`（`resource *` 仍为通用入口）。

### vwork 运维接口（ADR-156）
- **资源吊销**：`DELETE /v1/control/resource`（409 `resource_in_use` + `referenced_by`，`--force` 强制）+ `revoked_resources` 墓碑（立即 fail-closed，重新登记解禁）+ CLI `resource delete`。
- **队列运维**：`GET /v1/control/queue/status`（depth/visible/leased + DLQ 深度）、`POST /v1/control/queue/replay-dlq`（死信重投回主队列）+ CLI `queue status|replay-dlq`。
- **就绪/排空探针**：user-runtime `/ready` + `/drain`（SIGTERM 自动排空，3s 宽限）、do-runtime `/ready`；compose/k8s/Helm 探针改用 `/ready`。

### P3 — DO 完整（收口）
- HTTPCommitter 持久性证明放宽为 `fleet`/`bucket`/`bucket-batch`（拒绝 `bucket-async`）。
- **WebSocket 跨节点转发**：`proxyConnect` 代理到 owner，1012 透传（`TestDoRuntimeWebSocketCrossNodeForward`）。
- **`transferred_classes`**：同 worker `{from,to}` = rename 别名；跨 worker `script_name` 拒绝。
- 运行期 VFS 懒读：边界定稿（ADR-085），替代=冷启动按对象/按页 materialize。
- `refreshFacets` 增量缓存；`deleteAll()` shim（KV+SQL）。
- `TestDOCompatSuite`：15 子测试。

### P2 — 绑定与资产
- cron 部署期校验（`invalid_cron`）。
- 资产写侧（CLI `asset put`/`bundle put`/`deploy --assets-*`）。
- **Workflows Partial（ADR-086）**：自研引擎（`__workflow__` cell、记忆化 `step.do`、`sleep` via timer、facade `env.WF`、cell-agent API、`cellhive workflow create`）。

### P4 — 多租户 + 伸缩（ADR-087）
- 发布日志 `Releases` + 幂等部署 `idempotency_key`。
- Admission：每命名空间写限流（`CELLHIVE_NS_RATE=rps[/burst]`）→ 429。
- Autoscaler **信号**：`internal/autoscaler` + `/v1/control/capacity`。
- CLI 命令面补全（`status`/`capacity`/`releases`/`workflow create`）。

### P5 — 运维与加固（ADR-088）
- 协议版本化 reader-before-writer（`cell.SupportedProtoVersion`，claim fail-closed）。
- `/v1/diagnose` 增强（proto/admission/capacity/计数）。
- 供应商矩阵回归守卫 `TestVendorMatrixContract`；压测 `cmd/*bench`；恢复混沌测试。


### 追加（收尾五项）
- **ServiceBinding RPC（ADR-090）**：`env.SVC.<method>()` 经 Proxy 转发到目标 worker 的命名 entrypoint；`fetch()` 保留。
- **Workflows 子集（ADR-086）**：`pause/resume/terminate/restart`、实例 `list`、`step.do` `retry/backoff`、`step.waitForEvent`；facade `env.WF.get(id)` 为 CF 形态实例对象。
- **S3 供应商验证（ADR-091/testing）**：`make s3-test` 一条命令跑本地 MinIO 的条件写/CAS/range/presign + 复制恢复链（`CELLHIVE_S3_TEST_ENDPOINT` gated）。云端对象存储仍缺凭据→ blocker。
- **基准回归（docs/benchmarks.md）**：cellbench / realbench / sqlbench 的 p50/p99（FS 与 S3 两档），原始输出归档 `docs/archive/bench/`。
- **CLI（ADR-091）**：`app list`、`resource list`、`tail`（审计跟随）、`deploy --assets-dir`（版本级 assets token）。

### 控制面、域名与运行时（ADR-127~133）
- **内部派发版本键（ADR-127/128）**：内部派发 body 带 `version`；queue/scheduled/workflow 派发注入 bindings（`GET /v1/internal/worker/bindings`，失败开放）；内部 loader id 与公开入口格式统一。
- **Hyperdrive（ADR-129）**：origin URL 作为注册资源**信封加密**存储，`/v1/internal/hyperdrive` 按名解析；CLI `resource create --connection-string`；平台不做连接池。
- **本地连接复用（ADR-130）**：实测普通 worker 不能跨请求复用 socket → 复用靠在自己的 DO 里持有 `cloudflare:sockets` 连接。
- **控制面 schema v2 + 域名/路由 + 多租户鉴权（ADR-131）**：软删 + purge 作业、`bindings` 派生表、`hosts`、JWT `cellhive_ns` 授权、列表分页、`domain`/JWT CLI；自定义域挂载前缀**总是剥离**、段边界匹配。
- **移除边缘配置下发（ADR-132）**：删 `GET /v1/internal/traefik` 与 `CELLHIVE_ADMIN_HOST`/`_ADMIN_BACKEND_URL`；边缘由运维静态配置，平台只保证 loader 级 host 门控。
- **域名不做 DNS 校验（ADR-133）**：登记即授权（`verify_state` 字段与验证循环删除），内置域仅 `<ns>-<worker>.<base>`。

### review 修复批次（ADR-134/135）
- **ADR-134**：授权**先于**转发（否则换 internal token 转发可绕过 ns 校验）；所有被捕获的写必须走 `Cell.Tx`（RPO=0 水位）；`compatibility_date`/`compatibility_flags` 真正生效（`nodejs_compat` e2e 可证伪）；`Bucket.ConditionalDelete` + owner 条件释放；`Forget` 排空 + `ForgetWithVerify`；WAL 自动截断；上传有界重试；`ParseScope` 拒绝 `..`；`r2.List` 用 `SizeLister`。
- **ADR-135**：**purge vs 重新部署**（Deploy 复活 worker/app 并取消 purge 作业；purge 仅对仍软删实体生效）；**捕获提交 epoch 栅栏**；DO WS `connect` 身份与 invoke 一致（含 storage_class 租约键）；`DeleteNamespace` 走 `forget` 排空；`cellcapture.Ensure` 快照移出全局锁；JWKS 刷新移出锁 + 单飞 + 在途用缓存密钥、JWT 缺 `exp` 拒绝；`internal.js` bundle 缓存 256 + 单飞；`_headers` 按请求路径匹配、log-tail 挂 `ctx.waitUntil`、R2 range 无 length 省略 end；`cellhive diagnose` 增加**条件删除探测**；CLI 子命令 arity 校验。

### 内部协议与接线修正（ADR-136）
- **取消从未实现的 gRPC 平面**：内部 Go↔Go 一直走 HTTP（LTX 为长度前缀二进制帧 + HTTP 101 持久流，ADR-042）；删除 `CELLHIVE_GRPC_ADDR`/`:7000` 与 `Config.GRPCAddr`。
- **`CELLHIVE_ADVERTISE` 默认改为 `127.0.0.1:7001`**（原 `:7000` 无监听者，owner 转发默认指向死端口）。
- **删除 `CELLHIVE_CELL_AGENTS`/`Config.CellAgents`**：`ownerclient.New` 种子发现无调用者（库只保留 `Hint`/`OwnerURL`）。
- **接线日志尾缓冲**：`cmd/cell-agent` 现在构造 `logbuf.New(cfg.LogBufferEntries, cfg.LogBufferWorkers)` → `/v1/internal/logs` 与 `cellhive tail --worker` 在生产可用（此前恒 503）。
- **统一 `CELLHIVE_DO_OBJECT_INDEX` 解析**（`true`/`1`），消除 cell-agent 与 do-runtime 不一致。

### 配置面简化（ADR-137）
- **单一根密钥**：`CELLHIVE_ROOT_KEY` 经 HKDF 派生 peer/internal/dispatch/log/admin/scope/do-ticket/secrets-root；删除 8 个独立 secret 变量，`CELLHIVE_ADMIN_TOKEN` 仅作可选覆盖。
- **标准 AWS 桶凭据名**：`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`/`AWS_ENDPOINT_URL`/`AWS_REGION`。
- **工具与脚本跟进**：`cellhive creds [role]` 打印 root 派生凭据；bench/probe 工具（kvbench/realbench/sqlbench/recoververify/*-supervisor）默认令牌改为 root 派生；`make rpo-test` 全流程（跨进程 + 工具）改用 root。
- **时长统一 Go 语法**（`PEER_LATENCY`/`BINDING_CACHE`/`CELL_IDLE`/`CAPTURE_*`/`DO_LEASE`）；目录派生（`CELLHIVE_RUNTIME_DIR`、spool、DO 盘）；合并 `CELLHIVE_LOG_BUFFER`、`CELLHIVE_NS_RATE`，派生 `WAKER_TTL`/`DRAIN_WAIT`；删除 `CELLHIVE_COMMIT_MODE`。合计 99 → 85 个 env。

### wrangler 风格用法（ADR-138）
- `cellhive wrangler <命令>`：`deploy`（自动发现 `wrangler.jsonc`、`--namespace`/`--name`/`--env` 翻译）、`delete`→`worker delete`、`versions|deployments list`→`releases`、`secret`/`tail`/`rollback`/`promote`；不支持的 wrangler flag/命令给出明确替代或"不支持"。
- 官方 wrangler 改 `CLOUDFLARE_API_BASE_URL` 直连（CF API 子集）作为备选，未实现。

### 可部署产物（ADR-139）
- 单镜像（5 个二进制 + **pinned workerd 1.20260615.1** + `workerd/` JS，glibc 基础镜像），`deploy/entrypoint.sh` 按短名分发服务；`deploy/compose/docker-compose.yml`（cell-agent + user-runtime + do-runtime，`s3`/`edge` profiles，只需 `CELLHIVE_ROOT_KEY`）；`deploy/k8s/` kustomize manifests（StatefulSet/Deployment/HPA/ConfigMap/Secret 示例）；Makefile `docker-build`/`compose-config`/`compose-up`/`k8s-render`。

### DO 输出门部署（ADR-140）
- `do-runtime -render-only` + `do-supervisor` 接管 workerd 生命周期（续租/drain）；compose profile `rpo0`、k8s `overlays/rpo0`；容器端到端验证 DO 调用经门并落桶。

### 部署加固与门禁（ADR-141）
- 非 root 运行（UID/GID 65532 + fsGroup）、ServiceAccount/PDB/NetworkPolicy/startup 探针；Helm chart（`deploy/helm/cellhive`，`doRuntime.gate` 切换 RPO=0）；`scripts/ci.sh`/`make ci` 一键门禁 + GitHub Actions（go/js/cli/deploy 四个 job）。最近一次 `GATE: PASS 13/13`。

### purge 闭环（ADR-142）
- `RunPurgeLoop` 的数据侧 hook 已接线：worker 删除清该 worker 的 DO 存储与 assets，app 删除清整个 ns；每个 cell-agent 执行（跨节点本地副本 drain + 桶删除幂等），可续跑（有界删除/轮）。修 FS 桶按段内前缀无法列键的可移植性 bug。

### async 上传持久化（ADR-143）
- 写前 spool（`<DATA_DIR>/upload-spool`）+ 启动/周期重放：失败从 `dropped` 改为 `deferred`（进程重启不丢）；`/metrics` 暴露 `cellhive_upload_{dropped,deferred,replayed}_total` 与 `upload_spool`。

### service binding ACL（ADR-144）
- 目标侧 `service_acls` 允许列表 + 部署期 `service_binding_denied` 拒绝 + 运行期再校验；支持 `ns/worker` 跨命名空间 service binding（`target_ns` 协议，caller scope token 不变）；CLI `cellhive service-acl add|ls|rm`。

### R2 list 分页（ADR-145）
- `bucket.PagedLister` + `r2.ListPage`：S3 用 `StartAfter`+`MaxKeys`、FS 有界选择，不再物化整个前缀；`/v1/r2/list` 与 facade 返回 `truncated`/`cursor`（R2 语义）。

### trace context 传播（ADR-146）
- W3C `traceparent`：入口生成/透传 → 租户 handler → facades → cell-agent 的 service/DO 出向；无 OTLP/采样（残余见 known-issues）。

### secrets 管理（ADR-147）
- `DELETE /v1/control/secret` + `GET /v1/control/secrets`（只列 key）+ CLI `cellhive secret delete|list` + 审计 `secret.delete`。

### CLI 兼容（ADR-148）
- 服务端 `dry_run` + `deploy --dry-run/--var/--secrets-file`；`versions`/`deployments`/`triggers`/`workflows`/`queues`/`types`/`init`/`d1`/`kv`/`r2` 兼容组（映射或结构化拒绝 + 替代）；`cellhive wrangler` 透传这三个 flag 并转发兼容组。

### C 类环境（ADR-149）
- 本地替代全部 PASS：MinIO 条件写/删除 + ListPage、`make rpo-test` 多进程 RPO、peer 延迟注入、mock OIDC(JWKS/JWT)、owner 仿真、供应商契约；真实云/第二主机/真实 IdP/编排/证书列为残余。

### 根密钥来源与 mTLS 决定（ADR-150）
- `CELLHIVE_ROOT_KEY_FILE`（Docker/K8s secret 挂载）+ env 内联两种来源，失败关闭；云 KMS 留接缝未实现；mTLS/内部 CA 决定暂不做（理由与触发条件记录在 security.md）。

### scaling 与多 AZ（ADR-151）
- autoscaler 冷却 `CELLHIVE_AUTOSCALE_COOLDOWN`（默认 5m，防抖）；`CELLHIVE_PLACEMENT_AZ` + follower 跨 AZ 优先（单 AZ 故障不带走全部副本）；rebalance 间隔/批大小定稿。

### dispatch 定稿（ADR-152）
- 可配队列 batch/lease/重投延迟、timer/waker 批大小与 fired TTL、waker 指数退避（封顶 1m）；timer cell 明确不做 pinning（可驱逐 + 桶 wake 索引定位）。

### 兼容矩阵收口（ADR-153）
- **修 bug**：平台 bundler 现在默认外置 `cloudflare:*`（`node:*` 仅 nodejs_compat），OpenNext 等框架预构建产物可正常 `deploy --config`。
- `compatibility_flags` 精确列表与 dev CLI 镜像并由测试强制；pin `1.20260615.1` ↔ 兼容上限 `2026-06-22` 绑定；OpenNext/SvelteKit/Astro 布局验收；D1 sessions/bookmarks 显式拒绝。

### dispatch 缺陷修复（ADR-154）
- 修：timer body 缺 `namespace`（cron/scheduled 一直 400）、部署产物未配 `CELLHIVE_DISPATCH_URL`（队列/定时循环静默不启动）、`event.cron` 为空、queue 批次非 CF 形状（`.messages`/`.queue`/`ackAll`/`retryAll`）且跨 isolate 丢属性/丢 trace。
- 全栈 e2e 通过：fetch+KV+DO、queue 批量消费、cron 触发，cell-agent 0 dispatch 失败。

### Queue 逐消息语义（ADR-155）
- `message.ack()` / `message.retry({delaySeconds})` / `batch.retryAll({delaySeconds})`（未处理隐式 ack、超限进 DLQ）+ 延迟生产 `send(body,{delaySeconds})` + `message.body` 按 content-type 类型化；真实全栈 e2e 验证（retry 5s / delay 6s 均符合时序）。

### 已知边界
- C 类环境验证缺环境（云端对象存储条件写与条件删除、真实跨主机 RTT、多主机混沌/接管、真实扩缩容编排）——如实记录，不伪造。
- S3 兼容存储忽略 `If-Match` 时条件删除退化为无条件删除（owner/lease fence 依赖它）；`cellhive diagnose` 含自检探测。
- async 上传只有有界重试（3 次退避）+ `dropped` 计数告警，**无持久化重试队列**。
- 跨节点 purge 的 cell-data/桶清理 hook 仍未闭环（ADR-131 遗留）。
- Workflows Partial（不支持 pause/resume/terminate/restart、retry 配置、waitForEvent、delete/locationHint/跨 worker）。
- 运行期 SQLite VFS 懒读不可行于 stock workerd（ADR-085）。

**验证**：`gofmt` / `go vet` / `go test ./...` 全绿；`make build`。
