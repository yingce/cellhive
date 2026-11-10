> Historical record archived on 2026-11-10. Pending/resolved statements below describe their original dates, not current status. See [current issues](../known-issues.md). Inline paths retain their original document context.

# 评审问题与发布状态（Known Issues / Review Closure）

## 2026-11-10 COS assets 并发不可变创建：后续修复验收

后续修复：COS assets 已改为原 key 上带 16 字节长度头的 native append(position=0)，不增加协调文件、不改 LTX。真实 COS 10 轮非空/空文件竞争、64MiB 上传读回及静态资源公开访问 E2E 已通过。COS assets 通过 agent 点读回退，旧 raw assets 需在空目标重传（或停服清理后重传），不可混跑；未部署。见[修复报告](../reports/2026-11-10-cos-assets-atomic-create.md)。以下保留修复前失败证据。

真实 COS 桶 `cellhive-e2e-hk-1376795072` 的 `TestCOSAssetImmutableCreateLive` 两次失败：8 个不同内容并发创建同 key，分别 4 个和 7 个返回成功。本轮全量 E2E 复验再次失败，8 个写入者有 2 个返回成功（`/tmp/cellhive-e2e-examples-20261110/cos-assets-detail.log`）。当前 adapter 发送 `x-cos-forbid-overwrite: true`；顺序重复创建被拒绝不能证明并发唯一创建。后续 LTX 已按用户决策改为普通上传并完成数据 epoch 隔离，不再依赖此条件创建路径；assets 仍依赖唯一创建，必须完成根因与影响排查后再发布。COS/OSS/MinIO 的 10 万条强杀、在线/冷恢复内容校验已通过，但不抵消此契约失败。详见 [本轮验收报告](../reports/2026-11-10-full-worktree-acceptance.md)。

## 2026-11-09 worktree 审查

当前改动与本轮测试范围见 [worktree 审查报告](../reports/2026-11-09-worktree-review.md)。已修复互操作样本被忽略和 E2E owner 路径陈旧问题。新 fork 已通过 16MiB 原生 DO 分页本地/COS 验收、完整 runtime/supervisor 回归及 2000 DO 压力用例，10GiB 原生 DO 与真实 fleet 分页 owner 切换仍未覆盖；COS multipart 已改为按对象大小向上取整扩大分片，移除固定 5MiB 导致的约 48.83GiB 代码限制；超过该大小的真实 COS 上传仍未实测。以下旧 ADR 记录按各自日期保留，不代表当前格式仍使用旧 marker 协议。

## 2026-11-08 ADR-311 canonical LTX migration (current)

当前唯一生产格式与 celld 对齐：owner 为 `cells/<scope>/own.json`；LTX 为 `cells/<scope>/ltx/e<epoch>/<level-4hex>/<min>-<max>.ltx`。读取和扫描只接受 canonical level 目录，按 level 使用 continuation/offset；旧 `owner.json`、`L1/`、十进制、canonical `.ltx` 和 snapshot-started/done 不再读取或生成。完整快照通过本地 spool fsync 后一次性发布；COS contract、64MiB 分页恢复和 COS cold E2E 已实测，数据校验 `missing=0 wrong=0`。

## 2026-11-05 ADR-309 Safe GC 运营化与清理原子性（实施与验收中，未部署）

新增显式 admin/CLI Safe GC（owner、admin、scope 校验，dry-run）、`KeepL1Generations` 默认保留两代 L1、backlog/lag/L1 代数/deferred/retained epoch 指标，以及 cleanup 的 deleting fence + 幂等续跑契约。生产默认仍 DEFERRED，Safe 只能显式开启；真实 HK COS 100k 测试暴露的 cleanup 502 半删（e1/e2 全删、e3 半删）由本 ADR 修复。验收未完成前不得宣称线上 GC 已安全。

## 2026-11-05 ADR-308 恢复下界与安全删除协议（实施与验收中，未部署）

恢复链新增下界校验：marker cut 必须由最终 watermark 达到，cut 跨 epoch 单调不减；加载 epoch 前先应用后继 cut，manifest 高于 cut 时不得用于恢复或授权 prune；无 L1 的完整快照用 `restore.IndexChain` 验证页覆盖；退休 epoch 写 `gc-retired` 并由恢复/水位扫描跳过。读端在 `ErrNotFound` 时按 manifest/epoch 变化重试，否则 fail closed。

安全删除新增 reader pin（持久化 lease + 续租 + 过期）、epoch seal（阻止新 pin 后等待排空）、per-key tombstone 与宽限期、`RetireOptions.Safe`。默认仍为 DEFERRED，生产后台不得启用 Safe 或 OfflineQuiescent；桶增长与历史 List 成本继续保留。验收尚未完成，不能宣称在线 GC 已投产。

## 2026-11-04 复制 GC 正确性边界（ADR-307，契约先行，实施与验收中，未部署）

最终 epoch 审查不能证明跨对象 owner 检查/删除原子性，也不能证明 pinned readers 或 lineage cut 安全。保守决策：compaction 继续发布，covered L0/旧 L1/`RetireEpochs` payload 自动物理删除默认 DEFERRED；`Options.GC` 不足以授权。显式 Go option `OfflineQuiescent bool` 默认 `false`，只有调用者外部停止并排空 writers **和 readers** 且全过程保持静默才可设 `true`。生产后台永不启用，interval 不能绕过；无 owner、nil `Verify` 不是许可。离线逐次 Delete 前配置的 `Verify` 只停止观察到的丢失，不是原子 fence；可选 payload ETag 不能证明 owner。

所有模式保留 marker/link/epoch-cut/snapshot-started/done/未知名称；`ReportDeferredGC`/`GCSkipped` 确定性报告延后，不伪报删除成功。不可变 epoch-cut、manifest/index 完整证明、cut-before-prune、大 marker 持久化-before-Serve 修复仍在实施与验收，不能沿用此前“未修改”描述，也不能宣称已全部通过。无 pending-deletion registry，bundle/assets 运维 GC 不变。桶增长与历史 List 开销是明确代价；完整在线 GC 的 generation descriptor、单调 epoch reference 与 reader pinning 尚未解决，仍不得宣称线上 GC 安全。下节 ADR-306 验收不覆盖本次修订，无部署声明。

## 2026-11-04 性能与资源边界批次（增量本地验收通过，未提交、未部署）

本节记录当前工作树，不是发布记录。初始审计中的可疑热点、源码确认的重复读取/无界保留，以及确定性回归确认的缺陷须分开；不能把所有候选统称为 P0 或宣称全部修复。决策与限额见 ADR-306、[配置](../configuration.md)、[存储](../storage-and-s3.md) 和 [可观测性](../observability.md)。

- 分页探测跳过页索引水位已覆盖的严格 canonical snapshot/part；未知、混合或 snapshot batch 仍解码。`MinBytes` 的索引提前退出仅允许 **零 L0 对象**，不能从压缩后的 L0 字节推断 SQLite image 大小。
- compaction 预检可避免确定不需要的正文读取；仅对名称可证明完全覆盖的 L0 做清理，先要求持久页索引 `Watermark >= manifest.MaxTxID` 并复核 owner。缺索引/落后索引延后 GC，读取失败不删除；未知/混合/跨水位对象不走该快捷 GC。每 scope 默认 5 分钟 deadline，不是整个后台系统的超时修复。
- 原生 COS/OSS 与 S3 默认单对象读取上限 512 MiB；supervisor blob/segment 为 64 MiB；blob 写入超限明确 413、不截断后成功。默认 peer HTTP 总超时 30 秒，supervisor HTTPStore 60 秒，慢链路的大 body 可能被中断；provider body 仍依赖调用 context。
- 控制投影批量查询减少逐 worker 查询，列表复用投影；派发回调共享 5 秒 TTL/single-flight。所有 mutator 已改用 `projectionStrict`，不继承 stale 配置；owner 原子 read-fence guard 与 `cacheEpoch` 防止 trim 后 `0==0` 误判，相关增量回归已本地通过。
- invocation 清理改为每 namespace 首次写、随后 1 分钟或 256 次写触发；每条清理查询最多 1000 行，7 天/10000 条为软保留目标，旧记录写入可强制触发。timer 增加 `(kind, occurrence)` / `(due_ms, token)` 索引；撤回了省略提交后 `NextDue` 对账的优化，不能描述为最终单查询路径。
- Go logbuf 的 entries/events 各有每 worker 1 MiB 预算，记录截断/拒绝/淘汰仅影响可观测数据，不表示业务失败。JS response 流式转发只覆盖 workerd hops；Go DO proxy 仍缓冲至 8.25 MiB。DO `seen` 为 10000 项 CountLRU，原生 facet/version 身份仍保留 Map，不能为内存回收丢弃身份。bundle 源码缓存预算 65 MiB；超预算有效源码旁路缓存继续加载，不新增缓存准入拒绝。

**增量审查结论：ACCEPT**（reviewer `ses_e5926bc28`）：四个本批引入的阻塞项已修复并本地验证，包括恢复 native facet/version Maps、恢复 timer 提交后 fresh `syncIndex`、owner 原子 fence/`cacheEpoch`、全部 mutator 的 strict projection。timer 保留 pre-Put 崩溃安全顺序与新索引；失败 Delete 与并发 publish 的既有窄窗口仍可能存在，不是本批新引入或已修复的问题。facet Maps 的无界保留也是既有边界，相应内存优化已撤回。

**最终本地验证**：Node **141/141**、全量 Go、vet、timer/owner race、focused server 及真实 pinned user-runtime/do-runtime 测试通过。这是本批最终结果，不是较早的 134 项 Node 结果；本次文档校正未重新运行这些测试。仅接受该增量批次，不表示 GA、生产验收或所有问题完成。

**仍为发布门禁/残余**：六项 epoch/lineage 协议缺陷的后续修订按 ADR-307 实施与验收中，仍阻止发布；上文早期水位/owner GC 检查不构成在线删除许可。完整恢复内存/流式化、旧 epoch GC 成本、Range/后台读取超时覆盖及慢链路风险保留。有效源码按 UTF-16 权重超过 65MiB 时旁路缓存而不拒绝，反复加载的 cache thrash 未解决；Go DO sharding hop 仍缓冲至 8.25MiB。无提交、无部署。

## 2026-11-03 ADR-304 授权重设计（无状态修订实施与验收中，未部署）

统一 Bearer HS256 JWT，默认 audience `cellhive`，无静态 header/OIDC 兼容。单个 vwork issuer key 签 namespace 与受限 binding，无 actions/key_version；namespace `ns` 为精确/前缀 `*`/`*` 数组，binding 为精确 `ns` 字符串与 kind/name，固定字段仅平台签发。

无状态 `kid=issuer/<pattern>`，支持精确/前缀 `*`/`*` 上限与固定最大 TTL86400秒；恰好32位十六进制旧 issuer ID 拒绝、无回退。无 CRUD、active record、owner 点查、持久化检查或 namespaces 注册表授权；历史 issuer 表可保留惰性数据，不做破坏性迁移。key 使用 `HKDF-SHA256(decoded_root_key,nil,"cellhive/jwt/v1/"+kid,32)`；旧 ScopeSecret 仅保留给 Hyperdrive 非 HTTP 凭据，secret 加密不变。无即时单 issuer 撤销，JWT 靠 exp，泄露 key 需轮换 root 并重分发才能止损。

namespace JWT 包含全部业务、CF 数据和过滤后的监控，平台 admin 独立。CF 仅8082，不在7001/8083，转发保留授权。真实 Wrangler Tail 会话 query 是协议例外，不是普通用户 header 回退；私网全量 metrics scrape 可保留明确例外。root/key 限定分发，内部角色用途隔离，长期组件须刷新 JWT。

**历史有状态版本验收，不证明本次无状态修订通过**：metadata/durability 修复后的最终 build/vet/test 全通过；111项 JS 与真实 runtime、DO 收紧后的完整相关 race/测试、当时源码的完整 runtime baseline 均通过。原版 Wrangler4.133.0 assets/control/vectorize 3/3通过（18.332秒，无跳过）；VWork TS1706通过/19跳过，Rust369通过/1忽略，Docker3/3通过无跳过。证据见 [测试](../testing.md)，不是本次文档任务重跑，未部署，不代表生产切换完成。

**本次待验收边界**：DO grant 无状态重验 parent，执行 metadata 签名；票据最长30秒且不超过父 exp，保留 ADR-302 固定 KV 授权。assets/Tail 无状态绑定父 pattern/namespace/有效期，Tail 每500ms检查到期，不查撤销记录。`issuer/*` 且显式 `ns=["*"]` 允许全部全局业务及运维读/GC/指标，`token/admin` 保留全局 bootstrap。`:7001/metrics` 私网免鉴权例外、`:8082/v1/control/metrics` 鉴权过滤不变。`bundle_grants` 持久化 namespace/SHA 授权不变；已知 SHA 不代表可部署。旧 OIDC/静态/有状态 issuer 记录不是兼容承诺。

本文件记录系统评审发现的全部问题，以及**每条问题的解决决议**。原始分级见文末附录。设计文档（architecture / cell-protocol / durable-objects / networking / decisions）已按此同步。

Static Assets 标准部署收口（ADR-206）实施中：assets-only、生产 ASSETS binding、忽略规则、不可变上传、HTML/SPA 路由与入口配置需以 `static-sites.md` 验收记录为准。

_最后更新：2026-11-01_

## 2026-10-31 / 11-01 评审修复与 dispatch 回归（已发布 SG rev85/86，USW 69/70）

本轮 review 修复随 `20261031-02`（源码 `7be1102`）和 `20261031-03`（源码 `c9ba0fd`）发布：

- 数据/恢复：`restore.ApplyFile` 改为 temp+fsync+rename 原子落盘；paged cache 对 dirty/无 source 情况 fail closed 并传播探测错误；workflow `Prune` 单事务、retention 分批/索引；objgc 删除前复核引用；owner 在 `owner-gen` 缺失时保持 epoch 高于被替换记录，并增加 node-lease segment Touch；invocation 只清理被 prune 记录的 membership。
- 热路径：queue ack 批量化 + claim 排序索引；R2 conditional PUT 用 HEAD/Stat、multipart marker 保留到 part 清完；server mux 缓存、projection singleflight、KV TTL 锁外快速路径；loader 仅在新版本出现时扫描 residents；ingress/hybrid host map 有界；segmented 窗口回看 + HostView 索引。
- control：`resource_cleanups` due 索引、completed job 7 天保留期、app/worker purge 连带清理 `deployment_events`。
- 验证：`make test`、`make vet`、`make js-test`、受影响包 race 和全量 in-repo e2e 通过；两区 Pod 新 digest、Ready、`restartCount=0`。详见 [release-2026-10-31-03](release-2026-10-31-03.md) 和 [release-2026-10-31](release-2026-10-31.md)。
- 残余：objgc recheck→Delete 微秒窗口、legacy scope 缺 `owner-gen` 时仍可能从 1 开始、waker `Due` 的逐 key Get、DO envelope 二次 JSON 编解码。
- CF-API（历史验收记录）：Wrangler 4 的 `assets` binding 已支持（4.133 可能省略 `assets.config.binding`，采用 bindings 里的名字；含 assets-only、重复部署继承 active asset token）；Wrangler 4.133 真实 CLI 的 Worker+assets、重复部署、assets-only、租户 JWT 部署均已通过；错误码区分 `unsupported_binding`/`invalid_binding`/`invalid_multipart`。当时独立 CF 监听与 `cellhive_ns` 属于旧契约；现行 ADR-304 无状态修订要求仅 admin `:8082`、pattern issuer HS256 namespace JWT（`ns` 数组）或 bootstrap admin JWT，无静态/OIDC 回退。见顶部状态，不证明本次修订通过或生产切换完成。
- `vwork-40/vlog-daily-report` 的 cron 500 是租户 `sendFeishu` 错误（`Feishu send failed with HTTP 200`），不是平台 dispatch 修复项；需租户修响应解析或调用 `controller.noRetry()`。ADR-305 的 `CELLHIVE_CRON_MAX_ATTEMPTS` 只负责终止无限重试，不能替代修复 webhook 或 `noRetry()`。

## 2026-10-27 COS并发创建空读（已随 48216e7 发布：SG rev71 / USW rev56）

此前COS并发创建偶发 `append authority: empty object`；HTTP证据确认首次Append仍在途时GET可返回200/length=0/appendable。现仅对成功空读最多5次观察（5/10/20/40ms退避），不重试写、不视为空缺，持续空/损坏仍拒绝。修复前100轮3失败；修复后300轮全通过，仍实际遇到10次空响应且正确处理。COS契约6/6、真实双agent强杀和清缓存恢复100/100、全Go测试及相关race通过。详见 [修复验收](cos-empty-read-fix-2026-10-27.md)。原生COS裸S3条件创建不兼容仍是能力边界，生产继续依赖native-append adapter。

## 2026-10-27 sealed follower恢复合批（已随 48216e7 发布：SG rev71 / USW rev56）

sealed session恢复改为同scope/epoch连续delta最多64段/1MiB合批，快照/marker独立，整页持久化后才checkpoint。失败重试、删除fence、分页及重叠batch回归通过。HK两轮强杀/清缓存恢复100/100正确；KV下载对象119→18–22，KV cold open712ms→200–256ms。完整冷GET仍1.10/1.61s；控制cell冷鉴权、owner/claim未细分等待及COS尾延迟未解决。旧对象不自动重写，legacy/active DO恢复不变。详见 [恢复合批验收](recovery-batching-2026-10-27.md)。

## 2026-10-27 冷恢复trace（诊断记录，非发布项）

两轮HK真实COS强杀/清缓存后100/100数据正确，冷GET为2.78/2.95s。确认两个独立成本：冷binding等待控制cell恢复（第2轮1.60s等待）；KV下载119对象耗583ms，206段中93段重复，来自follower接管逐段补传与owner batch重叠。SQLite应用仅约5ms；第1轮单COS GET达到1.01s。恢复合批、重复元数据查询与控制cell冷启动仍待优化，详见 [冷恢复trace](cold-restore-trace-2026-10-27.md)。

## 2026-10-27 COS 后台合批（已随 48216e7 发布：SG rev71 / USW rev56）

上传周期20ms→200ms，只扩大无需同步等待COS的写入合批窗口；同步确认继续立即唤醒，follower fsync保留2ms。修复等待项可能错过wake的竞态。HK双agent对照成功上传批数213→114，平均每批4.19→7.78段；热owner PUT中位约5.5ms。强杀时尚有4项spool，100次fleet确认写在接管及清空缓存后均完整读回。冷GET仍1815ms，未解决全部冷恢复/转发长尾。见 [合批验收](cos-batching-2026-10-27.md)。


## 2026-10-27 完整压力 trace 补充（长尾未消除）

153,960请求全量trace确认：759ms转发PUT有660ms在binding刷新，owner实际处理6.4ms；447ms Worker GET有439ms在版本授权刷新，同期后台control owner COS点查达到1s超时。后台预热仍会因授权有效期耗尽而阻塞前台，前次短测不能外推。另有Worker外层耗时未完整关联的trace缺口，以及退出watchdog抢断handoff再次发生。详见 [完整trace诊断](kv-full-trace-2026-10-27.md)。

## 2026-10-27 固定版本授权后台续期（已随 48216e7 发布：SG rev71 / USW rev56）

固定 KV token 不再查询当前 active binding；独立验证版本与撤销状态，默认1s授权有效期由有界后台任务提前刷新，空闲60s移出。失效/刷新失败不延长授权，冷请求与容量不足仍可能同步校验。1920次HK双agent正式请求零失败，采集到的请求内版本授权刷新只有首次加载一次，其余78次均后台。此优化不消除直接HTTP旧token长尾和后台COS查询。见 [验收记录](version-authorization-2026-10-27.md)。

## 2026-10-27 Worker KV 固定映射（ADR-302；已随 48216e7 引入，resource-id 修复随 49133b1 发布：SG rev79 / USW rev63）

Worker KV token 携带平台签名的 worker/version/resource；解析实际 cell 使用版本固定 ID，避免同名 binding 改动把旧版本重定向到新资源。Go binding spec 与 user-runtime loader 同步。registered-binding ACL、资源删除 fence、owner 校验仍保留，所以没有消除 1s 授权刷新。D1/R2/Queue 原本使用运行时资源名，其他 binding 策略不变。发布必须先升级全部 cell-agent 再升级运行时；混合旧 agent 不能保证固定映射。验收见 [固定映射记录](binding-pinned-2026-10-27.md)。

## 2026-10-27 双 agent 长尾与退出竞态（部分优化）

HK COS 请求级 trace 确认：fleet 确认正常时，binding 过期刷新、control owner 强制失效查询及 resource fence GET 仍阻塞请求，产生 58–150ms 长尾；此前 400–744ms 极值尚未完成归因。另在 SIGTERM 后观察到 lease watchdog 抢断 owner handoff；进程退出不能作为 session 正常封存证明。诊断、证据与验证边界见 [长尾诊断](hk-tail-diagnosis-2026-10-27.md)。已在本地合并 binding 刷新并复用远端 control 路由，保留本地权威检查；HK 双 agent 1152 请求无错误，但仍有约 120ms 长尾。binding 刷新部分已随 49133b1 发布；退出竞态仍未修复。见 [第一阶段记录](binding-refresh-2026-10-27.md)。


## 2026-10-26 内部 binding 鉴权结构整理

`scopeAuth` 拆分为凭据验证、binding 授权、资源生命周期保护三个步骤。只调整代码职责，保留原有校验顺序、错误响应、撤销策略及请求保护生命周期；未调整缓存与 token 格式，不作为性能优化验收。`internal/server`、`internal/scopedtoken` 全量单测、鉴权/接管/恢复相关 race 回归及 `make build vet` 均通过。已随 49133b1 发布（SG rev79 / USW rev63）。

## 2026-10-26 Queue 响应安全与调度隔离（ADR-300）

已修复 Queue 在 HTTP 200 响应截断、无效 JSON 或超过读取上限时误 ACK 的问题；失败响应进入原有重试/DLQ 策略，重试耗尽且未配置 DLQ 仍按原有策略丢弃告警。不同队列单轮最多 8 路并发，每队列原有并发限制不变；所有任务结束才启动下一轮，因此这是有界隔离，不保证任意数量慢队列下的调度延迟。

Workflow 同分钟多表达式不再合并为一条任务，首条按字典序确定并保留旧分钟标识，其余带表达式 hash；同分钟变更表达式集合的跨配置去重不作保证。本地修复与测试见 [验收记录](queue-schedule-safety-2026-10-26.md)，已随 48216e7 发布（SG rev71 / USW rev56）。

## 2026-10-26 Workflow 事务栅栏与事件恢复（ADR-299）

本地已修复旧 run 回调跨 restart 写入新执行、事件消费与步骤保存之间中断导致漏事件，以及并发消费重复领取同一事件。回调写入在事务内校验 run token，事件消费与步骤结果同事务提交；暂停/终止/错误终态同步清理租约。无 schema 迁移，仍等待原有持久证明；升级前已消费但没有步骤结果的事件不能自动重建。

HK COS 验收还发现提前唤醒已记忆 sleep 时没有释放新 run lease，已补原 deadline 再 park；删除重建同 scope 的 schema 缓存失效问题也已修复。回归和发布状态以 [验收记录](workflow-atomic-recovery-2026-10-26.md) 为准；已随 `dd614b4` 发布 SG revision69、USW revision54 及本地；三处 Workflow 事件完成、KV/DO 数据回读通过，DO recovery pointer 既有告警仍保留。

## 2026-10-26 P0：后台快照与增量批次边界不一致（ADR-298）

SG invocation链故障已从现场副本复现：snapshot3771跨delta3770..3772，恢复过早剪掉e22基线。完整保留链可恢复，1887条invocation、SQLite integrity正常；修复批次切分、跨epoch严格回退、compaction修复路径及GC前验证。现场副本compaction前后恢复数据库SHA一致；已发布SG revision68、USW revision53及本地。SG实际compaction/GC后的单个L1冷恢复成功，1900条invocation中原1887条逐行一致；实现和发布状态见[P0记录](p0-snapshot-boundary-2026-10-26.md)。不能将本scope验证扩展为所有历史scope均安全。


## 2026-10-26 ADR-296/297 已发布与残余告警

peer stream 超时/并发ACK及runtime日志配置读取超时已随 `0e7c67d` 发布至SG revision67、USW revision52和本地；新镜像地址切换E2E、发布前后KV/DO持久化检查通过，USW DO探针网络故障自动恢复。两地仍观察到DO recovery pointer CAS竞争后的bucket回退；SG `vwork-77/__invocations__/main` 持续compaction报 non-contiguous chain（expected3772/got3770），该scope冷恢复尚未验证。另记录滚动期cron500/invocation502及USW已删除resource的waker告警。不能据此宣称所有数据恢复/性能问题均已解决；详见[发布与未解决项](do-address-recovery-2026-10-26.md)。

## 2026-10-25 SG KV 转发热缓存、连接复用与残余尾延迟

转发节点此前在成功转发后直接返回，遗漏热访问登记，导致 binding/资源 fence 缓存过期后仍同步查询。现已让成功的转发 PUT/GET/get-batch（含正常 GET 404）进入原有有界热访问刷新集合；鉴权、删除 fence、owner lease 和 RPO=0 确认规则保持不变。拒绝/失败请求不激活刷新。发布前后证据见 [SG 分批测试记录](sg-kv-performance-2026-10-25.md)。owner 直连约390ms样本中仍有约270ms在持久化span之外，具体分段尚未定位；本修复不宣称消除全部400ms尾延迟。

首次发布后长时间并发GET暴露另一问题：默认转发transport每目标仅保留2条idle连接，SG agent-1累积28,261个TIME_WAIT，触发 `dial tcp ... cannot assign requested address` / HTTP502。已将默认转发client复用并扩大idle pool至256/128，定向HTTP测试同样5轮×8并发的建连数从32降至8；线上长测和最终发布状态见同一验收记录。

SG revision64分段观测已确认另一条热写尖峰：HTTP555.8ms中SQLite仅0.111ms，bucket提交等待553.1ms；选择时follower缓存租约已过期约2.57s，`no_eligible_follower`触发`bucket-batch`。前序提交又使后续请求达到604ms。ADR-295已增加独立已知节点点查刷新（默认2秒、单次最多1秒、最多8并发），慢全量扫描不得覆盖较新点查；本地单元、双节点真实HTTP及进程恢复E2E通过，SG revision66已验收，最终69,299次计量写入无失败/无新增桶回退，owner/转发p99为26.78/30.85ms（仍有复制尾延迟）；不允许延长过期租约或取消RPO=0回退。该结论不解释所有历史proof之外的耗时。实施和最终结果见 [独立租约刷新验收](known-follower-refresh-2026-10-26.md)。 USW revision51及本地也已部署同镜像并通过KV/DO发布前后验收；USW滚动期间DO的cell_unreachable未自动恢复，agent就绪后额外重建DO Pod恢复，根因待查，详见同一记录。

## 2026-10-25 后台快照阻塞写入（ADR-294）

marker 恢复后的后台基线已改为 SQLite backup 固定副本，释放原库写锁后上传，不占用增量 capture 循环、不截断源 WAL、不提前推进确认水位。上传失败/取消保留旧链；退出等待后台任务，大小预算覆盖 WAL 页，启动时清理强杀遗留副本。首次无恢复链和 WAL 截断后的快照仍同步；本地复制仍需短暂写锁和额外磁盘。全量检查、相关 race 与真实 HK R2/D1/Queue 回归通过；最终 HK 冷重建 1660ms、重启首写 885ms，140 个确认键正确。冷重建仍未过 1s 门槛，见 [验收记录](detached-snapshot-2026-10-25.md)。

## 2026-10-25 真实 HK COS 冷恢复性能验收未通过

前测发现的重启 8.7s / 冷重建 9.2s 已定位为首次 NodeLease 同步扫描 nodes/ 的阻塞。ADR-293 改为目标节点点查、异步 fleet 采样，并在下载前过滤 manifest 已覆盖的 L0。最终无插桩真实 HK COS 测试（本机计算，durability=bucket）重启首写 974ms、冷重建 2318ms；接管请求 1673ms、故障到成功 11909ms。140 个已确认键无缺失/内容错误，SQLite integrity 正常；**冷重建仍未通过原 1s 性能门槛**。后台历史节点扫描成本仍存在，未测 HK 同区计算、跨机器 fleet 或长期生产负载。见 [本次修复与证据](cold-recovery-2026-10-25.md) 和 [前测](hk-review-2026-10-25.md)。

## 2026-10-25 跨模块审查修复（ADR-292）

F1–F9 的 Queue 并发分配、adaptive router 解锁错误、冷恢复池分类、R2 旧 metadata、D1 二次方累计、等待取消、JWKS 刷新放大、busy map 残留及 CI SQLite tags 已实施修复。新增回归包含真实 HTTP 无 deadline 的交互请求隔离，以及两个 R2 Store 并发时旧清理不能删除新 metadata。验证与待发布状态以 [修复验收记录](project-review-fixes-2026-10-25.md) 为准。

R2 双对象发布仍非原子：迟到 sidecar 发布和同内容并发写未解决，错误响应可能已经更新数据；已知不支持条件删除的 provider 对存在旧 sidecar 的覆盖会提前拒绝。D1 32 MiB 是保留载荷预算，不等于进程 RSS。Queue 上界是单队列并发，不是全节点资源配额。后续 Queue 批量事务、公平调度、DO 重连/续租容量、SQLite RSS/FD 准入和 Cron 补跑属于独立阶段，尚未实现。

## 2026-10-25 失租后迟到续租复活（ADR-291）

原 Publish 无条件 Put 且可恢复 Healthy，导致原租期已过的进程重新恢复服务，或迟到写覆盖替代 session。改为不可逆 fence、ETag 条件续租、独立退出 watchdog；新 session 走既有恢复。初始回归已复现过期后重发、挂起续租返回、覆盖替代 session 三个问题。全量测试、lease/owner/recovery/server/queue/cell-agent race、vet/build，以及真实双进程 SIGSTOP 超期→peer 恢复接管→SIGCONT 旧进程退出→新 session 恢复均通过。见 [验证及发布记录](lease-fence-2026-10-25.md)。代码 `520b61b` 已部署本地、SG revision 59、USW revision 48。

## 2026-10-25 owner 路由周期性点查（ADR-290）

改为本机连续安全租约窗口快速路径、远端观察租约期限缓存和同 cell 并发解析合并；不再以固定 1s 刷新普通请求 owner。热 KV 后台预热也复用路由解析。首次访问、期限已过、显式失效仍可能等待对象存储；此优化不消除其他资源 fence、绑定、持久性等待。全量测试、owner/lease/queue/server race、vet/build 和真实 HTTP 双入口/交接回归通过；同机文件桶加 owner GET 延迟的对照见 [验证记录](bench/owner-route-cache-2026-10-25.md)。已随 `520b61b` 部署本地、SG、USW，持久化冒烟验收通过；未宣称生产 COS 或跨机器 fleet 稳态性能。

## 2026-10-25 USW DO 滚动更新后连接未恢复（待定位）

agent 地址变化后，一个 DO Pod 持续 ready 503/cell_unreachable；同容器直连 agent readyz 返回 200。保留 PVC 重建该 Pod 后恢复，原 DO 数据与 HTTP 验收通过。疑似 workerd 连接或地址缓存未刷新，尚无根因证据，不能视为已修复。见 [发布记录](lease-fence-2026-10-25.md)。

## 2026-10-25 DO capture 重启 fence（ADR-289）

SG 发布验收发现 capture epoch 固定 1 与 cell-agent 接管后 owner epoch 增长冲突，DO 恢复后持续 gate_500/epoch_mismatch。现分离 LTX epoch 和 capture owner epoch，同时验证 DO shard fence；回归覆盖 agent epoch 增长后的真实 HTTP commit、旧 capture/shard fence 拒绝及普通 cell fence 保留。另修复失败捕获后 cursor 已推进而遗漏未确认 WAL 的问题：保留原始片段直到确认，真实 SQLite 恢复测试覆盖提交前拒绝、确认丢失后新写入。部署状态见发布记录。

## 2026-10-25 DO HTTP 与完整清空（ADR-288）

发布前双节点验收另发现：DO invoke 的绑定检查读取 follower 本地 Projection，造成新 Worker 持续 403。现按当前 control owner 点查 active Worker，读取失败返回 503；只读查询可转发，DO 调用不因此重放。双节点回归覆盖合法 class 成功、非法 class 拒绝。

本地修复：deleteAll 原先误读 Map 的 cursor，300 个键只删 256 个；DO fetch 在共享 binding 与 host 多次 text()，破坏非 UTF-8 请求/响应；host、cell-agent、binding 丢失租户响应头；Go/内部 fetch 自动跟随 302。现改用 base64 请求 envelope、字节响应及多值响应头，保留状态/Location/Set-Cookie，过滤内部和逐跳头；重复分页直到清空，并清除 alarm shim 元数据。新请求字段要求暂停/排空 DO 流量，升级全部 do-runtime 和调用方后恢复；do-runtime 自身也可调用其它 DO，不能仅靠先滚动 do-runtime 保证安全。已随 004401c 部署本地、SG revision 58、USW revision 47。

续租增加本地 HTTP 5s deadline、单 scope fetch/body 2s deadline，单个失败继续处理其余租约，迟到失败不会删除更新的 claim。仍按 scope 串行续租，不宣称解决大量 shard 或过短租约下的容量问题。HTTP 仍经过缓冲和既有大小限制，非流式传输。验证范围和 user-runtime 合并审查见 [记录](runtime-http-review-2026-10-25.md)。

## 2026-10-25 user-runtime 旧版本/实例缓存（ADR-287）

本地修复：指定 version 读取失败回退到其它缓存版本；显式历史版本误填 active 缓存；旧查询 404 覆盖新 env；删除清理后重建复用 version/SHA 导致旧代码或旧变量/绑定 resident 命中；暖 host 的挂起刷新绕过原 5s 兜底。host pointer 传播 storage_id，缓存和 resident 使用代际身份；无代际旧指针重新查询并核验，错误不降级为另一版本。host 刷新使用 5s AbortController/deadline 及 generation/epoch 双重结果守卫。有效在途旧版本可完成；路由 SWR 与投影 stale-on-error 可见性边界未改变。已随 004401c 部署本地、SG revision 58、USW revision 47；不得把代码级复现直接归因到请求 `53900423061812ab6983f683207f7742`。

pool 路由/专属进程传播并校验 storage_id，实际反向代理测试覆盖 Connection 头不能删除可信身份头；内部 bindings 端点返回 version/bundle_sha/storage_id，Queue/Timer/Service/Workflow adapter 不再永久缓存无代际版本，加载实例带代际。旧 metadata 响应不带身份或不可用时的 fail-open 兼容语义未整体移除，SHA-only service 仍不是完整环境 pin；这些边界及投影读取快照一致性、bundle 读后哈希验证不属于本次完整解决范围。

## 2026-10-24 异常接管与驻留回收（ADR-286）

本地已修复并验证：请求入口忽略失效错误；queue/timer 在本地清理前发布 owner；TryInvalidate 校验失败泄漏句柄；最旧项被 Pin/busy 保护时误驱逐未超时热 cell。claim 按 scope 串行化，前任恢复与本地清理成功后才发布 owner，失败保持可重试。sealed tail 使用服务端校验游标，连续分页实际读取量接近日志大小；重启/重试/截断/替换文件检查保留。

全量测试、owner/peer/queue/cellstore/server race、vet/build、真实双进程换地址和中断恢复 E2E 已通过；尚未部署。驻留上限仍是软限制，capture 默认2ms轮询随 resident 写入 cell 增长；2核建议32–64起步，完整数据和验证边界见 [记录](bench/recovery-residency-2026-10-24.md)。


## 2026-10-24 热 KV 读写的周期性 COS 等待（ADR-283）

已打开 cell 的连续热 GET 约 0.3–0.4ms，但 binding、owner 和资源删除 fence 的 1 秒缓存失效后会在请求路径同步读取控制面/COS；本机到香港 COS 测试桶每隔 1.15 秒的 8 次 GET 曾为 44–185ms。分段诊断还发现本机 owner 被滞后的 node-lease 快照判为不存活时，GET 会同步点读 `nodes/<self>.json`，仅路由判活就耗 24–25ms。ADR-283 在有限活跃 KV 集合中提前刷新原有缓存，并以本机已成功发布且健康的 lease 直接判断本机 owner；未发布或不健康时失败关闭，失效边界和请求准入检查不变。最终版真实 COS 的 60 次写后 GET p50/p95/p99 为 0.60/0.76/1.03ms、最大 29.99ms；300 次空闲连续 GET p99 0.45ms；8 次间隔 1.15 秒 GET 均为 0.55–0.62ms。跨地域 COS 忙时提前刷新仍可能失败或晚于失效时间，此前复测曾见 20–61ms 残余尖峰；**长尾尚未证明完全消除**。首次读与冷写不在此次热缓存优化范围。SG 已做短时同 Pod 写后 GET 复验，公开入口和长时间 GET 尾延迟仍待测；详见 [对照记录](bench/kv-ttl-wake-2026-10-24.md)。

PUT 共用上述 binding、资源 fence、owner 路由，原先只有 GET 能维持提前刷新：间隔 1.15 秒的纯 PUT 在最近 GET 停止后退回同步检查，分段诊断约 25–30ms binding、12–16ms fence、23–31ms gate，加上约 50ms 写入与 bucket 持久化证明。现成功 PUT 也维持活跃 scope；本机到香港 COS 的三轮间隔 PUT 各 12 次中，两轮全部约 43–66ms，另一轮 11 次约 49–66ms、一次 611ms。**PUT 仍须同步等待 RPO=0 证明**；诊断确认部分 80–181ms 连续写的增加量全部在证明等待中。611ms 单次尖峰未被同请求阶段日志覆盖，不能将所有尾延迟归因或称已解决；SG 短时同 Pod fleet 复验见下文，公开入口和长期尾延迟仍待验证。细节见同一[对照记录](bench/kv-ttl-wake-2026-10-24.md)。

ADR-284 又去掉了 COS 不可变 LTX Append 成功后为取 ETag 做的常规 HEAD：测试桶实测响应 ETag 与 HEAD 15/15 相同，缺失响应头时仍回退 HEAD。独立顺序 A/B 的普通 PUT p50 从 49.95ms 到 44.19ms，TTL PUT p50 从 48.78ms 到 46.57ms；不同轮 COS 波动不可归因于单项变更。优化后普通 PUT 仍有 544ms 尖峰，跨地域 bucket 模式的长尾与冷请求仍未解决。

扩大到 120 对写入的临时分段诊断确认，数百毫秒尖峰主要落在 RPO=0 提交前的 COS 资源 fence 权威点读（最高观察到 778ms）或原生 COS Append（例如 412–472ms）；不是 SQLite 或 TTL 唤醒扫描。该 bucket 模式不能安全跳过 fresh fence 或先于 COS 持久化应答。需在 SG 同区 fleet 模式确认 follower fsync 正常承担热写证明、bucket 回退的频率和原因，并分别测同 Pod 与公开入口。

SG 已在 2026-10-24 部署 `ac5ad43`（Helm revision 53）。独立 namespace 的 Pod 本地真实 HTTP 校验通过 10 次普通 PUT、10 次 TTL PUT 与逐次 GET；首写 752ms、次写 146ms，之后 18 次热 PUT p50 9.84ms、p95 11.44ms、最大 13.14ms。该窗口 `fleet` 提交 +22、`bucket-batch` +0，`live_followers=1`，产品 App 删除返回 200。通过 `kubectl port-forward` 的另一轮 20 次 PUT/GET 也全部正确，但其延迟包含隧道开销，不能用来判断同 Pod 性能；该轮记录到 fleet +17、bucket-batch +4，计数包括控制面提交，需继续追踪 bucket 回退。随后对原先的 `vwork-24`/`TEST` 同路径复测，连续热 TTL PUT 11 次中位数 9.41ms（旧版 6 次为 190.7–299.9ms），但首次冷请求 2.440 秒；间隔 1.15 秒的 8 次 TTL PUT 中又出现 159.59/108.33ms 两次慢写，并有 bucket-batch +1。**热 TTL 性能有明显提升，但冷请求、周期性长尾和公开入口仍未解决。**滚动后 cell-agent-1 曾有一次对旧 follower IP `10.0.16.195` seal 超时的 recovery WARN；恢复过程失败关闭，旧 session 是否能最终封存尚未确认，需单独跟踪。

## 2026-10-24 KV TTL 同步重设 timer 的写入延迟（ADR-282）

SG 部署前同 Pod 的小值 KV put 对照中，不带 TTL 多次约 9–11ms（偶有 60–164ms），带 `expiration_ttl=300` 为 191–300ms；本地 FS 各 10 次的中位数分别为 4.2/5.4ms。写入持久化证明的平均值约 24ms，不能解释大部分 TTL 耗时。ADR-282 已实现复用已持久化、尚未到期且足够早的 `kv-expire` timer，避免每次 TTL put 都同步改写 COS `wake/` 索引；首次和更早到期仍要同步发布。本地修复后普通/TTL 中位数为 3.85/4.11ms，真实进程 1 秒/3 秒 TTL 物理清理和五场景 FS 冷写 E2E 均通过。真实 COS 测试桶（本机跨地域、bucket 模式）旧版普通/TTL 中位数 56.0/257.2ms、修复版 65.7/47.0ms；2 秒/6 秒 TTL 读回和到期均正确。SG 已部署并完成上述短时同 Pod 普通/TTL PUT 复验；公开入口延迟仍未测，见 [TTL wake 对照记录](bench/kv-ttl-wake-2026-10-24.md)。

## 2026-10-24 SG owner 判活分歧（ADR-280）

SG revision 50 后观察到同一 control owner 在两个 cell-agent 上分别显示 `expired=false` 与 `expired=true`，后者的资源清理循环持续收到 `owner: live owner exists`。定位为旧节点租约快照的负向判定未向桶点读确认；修复与 SG 复验记录见 ADR-280。此问题与 Cron 租户派发 `status 400` 分开处理。

## 2026-10-24 invocation owner 换手时的 503

已转发的 invocation 写请求可能在旧 owner 的 capture 预检前遇到 owner 换手。旧节点先前返回 503 `not_owner`，入口节点按 ADR-120 不重试 5xx，调用记录写入失败。已把**写入前**失权的转发请求改为 409 `not_owner`，让入口节点刷新 owner 并最多重试一次；写入后或结果未知的失败仍不可重放。`TestInvocationForwardedCaptureHandoff` 三节点 HTTP 回归在修复前稳定复现 503，修复后验证调用记录只写入新 owner、旧 owner 未执行写入，并通过 `-race`。本地 `make test`、`make vet`、`make build` 均通过。线上 503 与该路径的因果关系仍需部署后以响应正文和 owner/epoch 日志确认；Cron 同时出现的 timer owner 拒绝需独立追踪。

## 2026-10-24 COS 冷写性能与 L1 恢复（ADR-276/277）

KV/Queue 每次 LTX 提交 fresh 读取资源删除栅栏；旧 `resource-fence.json` 缺失时分段 authority 会执行一次 `List`。已改为首次迁移、随后稳定点查的 `resource-fence.v2`，并用节点租约协议标识阻止新旧栅栏写者混跑；app purge 保留新旧栅栏。90 秒 COS 稳态的 put p99 从旧版 1092.9ms 降至 434.7ms，但仍高于 200ms 目标。全局 List 从 769 次降至 353 次，按前缀跟踪主要是后台 `nodes/` 租约枚举读到历史测试节点和 probe 对象，不能把这些 List 归因于新的栅栏写热路径。后台枚举和最大 3282.9ms 的偶发写延迟仍待处理。

首次 COS 复测还暴露出更严重的 RPO 违例：compaction 删除全部 L0 后，冷恢复先过滤 `L1/manifest.json`，把仅有 L1 的 epoch 跳过；重启后 80 个已确认 key 全部读成 404。ADR-277 已修复，并在同参数 COS 五场景复测中读回全部 140 个最终 key（missing=0、wrong=0，SQLite `integrity_check=ok`）。复测的同节点重启首写 2346ms、跨节点接管后 50 次 put p99 633.6ms、删除本地目录冷恢复首写 3374ms，仍未达到各自设计目标。精确数据和测试边界见 [冷写栅栏 COS 实测](bench/cold-write-fence-v2-2026-10-24.md)。Cron 使用独立的 `__cron__` cell 和定时派发，不直接读取 KV/Queue 的资源删除栅栏；共享节点租约、COS 和控制面拥塞可能间接影响派发。

随后 FS 多 cell 接管又发现初始快照的 started 与 done 标记可分别写在 txid 0 和 1，导致已确认的 filler cell 在新 owner 上报 `restore: no snapshot in chain`。ADR-278 已让初始快照两种标记使用实际快照水位，started 写失败则拒绝写入；该缺陷的 FS/COS 完整复验结果见同一实测记录。Cron 的独立 Docker runtime e2e 使用 pinned workerd，在 `cell-agent` SIGKILL/重启后确认两个表达式都生成重启之后的新分钟 slot 且无重复；整体 `RUNTIME-BASELINE-E2E: PASS`。这只能证明恢复后继续派发，不代表停机覆盖整个分钟时会补发未物化的 slot；当前设计仍为 best-effort、不补跑。celld 则在保留的 cron cell 中持久化 alarm，重启时处理逾期 alarm，两者语义不同。

ADR-278 后 180 秒真实 COS 五场景复验：稳态 put p99 511.8ms、GET p99 65.3ms、epoch 变化 0；重启首写 3874ms，冷恢复首写 4183ms；最终 140 个已确认 key missing/wrong=0，SQLite `integrity_check=ok`。测试脚本用 10 秒宽松阈值跑完完整性检查并报告 PASS，**设计性能门仍未通过**。重启/冷恢复多次列举与读取，以及 COS 写尾延迟仍待优化；详见上述实测记录。

ADR-279 让后台节点租约列举复用物理列表中的最新分段，真实 COS 定向测试验证 3 个历史节点仅 1 次 List。同参数顺序复验的稳态全局 List 从 1243 降到 421，put p99 为 450.7ms，GET p99 为 56.1ms；重启首写仍为 3893ms，冷恢复首写 2868ms。两次完整性均通过，但写 p99、重启和冷恢复目标仍未达标。停止前 compaction 取消导致近千个 segment 留待重启恢复，是当前可观察的冷路径慢因之一。

## 2026-10-23 评审加固：WAL / snapshot / handoff / DO / R2 / paged

本轮把评审确认的 critical/high 与可安全落地的 medium 项落到代码和回归测试：

- WAL 读取校验 header/frame checksum；`Cursor` 检测 WAL 被删除重建后重开 inode；torn tail 按 transient 重试，不把坏帧当 commit。
- `cellstore.SnapshotConsistent` 在 `writeMu` 内完成 checkpoint + txid + 页镜像，`sqlcapture` 快照不再出现“水位领先镜像”。
- 优雅 handoff 先 `uploader.Flush`、再 `nodeLog.Seal`、最后释放 owner；失败保留 owner 与 open node-log 交给 predecessor recovery。
- Queue `OwnerGateWithReset` 在 unowned/foreign claim 后 `store.Forget`，避免旧本地 SQLite 重放；waker claim 后把 scope 注册进本地 timer runner。
- DO 租户 facet 前剥离 `x-cellhive-*` 平台凭据；`/v1/do/invoke` 校验 namespace + active worker 的 DO binding/class；WebSocket 与 service fallback 透传租户 headers。
- R2 loader/compat 用 `bucket_name` 解析；Cloudflare R2 PUT 超 64 MiB 返回 413，不再静默截断。
- Fleet follower 采样 TTL 现在不超过 `LeaseTTL/2`，避免 3s lease 下把 live peer 当过期、fleet 静默退化为 bucket。
- Paged cache marker 记录 cut txid 与 clean 标志；source cut 早于本地 marker 时 fail closed，不 reuse/truncate；checkpoint/evict/close 时把本地 txid 写回 marker。进程在 paged cell 打开期间崩溃会留下 dirty marker，下次打开直接 `ErrPagedCacheUnusable`，不会从可能落后的 bucket 重建而丢本地已 ack 页。v2/v3 旧 marker 没有 clean 标志，也按 dirty 处理；升级后需在确认 bucket 覆盖后清理旧 paged 缓存。新增 paged 驱逐/崩溃回归：Sweep 驱逐后重开可读回数据；子进程真实 `SIGKILL` 后 dirty marker 保留，下一次打开返回 `ErrPagedCacheUnusable`，不会从落后 cut 静默重建。真实 PVC 强杀仍建议在发布前补一轮。
- Upload spool replay 改为 512 条有界窗口；损坏条目隔离；decodeLimit 拒绝尾部 JSON 并返回 413；nodelog 拒绝重开 sealed session；restore IndexChain 校验覆盖/重复页；Restore 在 manifest 变更时重试；补登记 `node-logs/`、`deploys/` 保留前缀。

验证：`make test`、`go vet`、`go test -race`（server/cellstore/sqlcapture/wal/upload/timer/queue/userruntimepool/workerdpool/cell-agent）、`make js-test`、bindings/workflow/queue/queue-multinode/queue-concurrency e2e 均通过。高并发 32 writer + `SIGKILL` + 删除本地 DB + 接管：919 个 acked key missing=0/wrong=0；官方 RPO kill：900 个 acked key missing=0/wrong=0。剩余风险：paged 崩溃窗口的本地 txid 可能停留在上次 close，真实 COS/OSS 跨 AZ fleet 故障与 Docker/k3d 套件仍需发布前验收。

## 2026-10-22 资源名 / scope 的可寻址性校验（ADR-269）

创建资源曾只校验 `kind`/`name` 非空，`name: vwork-74/CHTEST` 这类带 `/` 的名字会 200 入库，但默认 scope `<ns>/__<kind>__/vwork-74/CHTEST` 不是合法 cell scope：KV 数据调用返回 `invalid cell scope`（5xx）、`/v1/kv/stats` 500 `resource_scope_invalid`、Cloudflare 兼容 `keys` 在 `%2F` 编码下 503、未编码时 404，且 `DELETE` 因 `ResourceDataScope` 解析失败 500——登记既不可用也不可删。现已在 `control.CreateResourceWithConfig` 单一入口校验：`name` 必须是单个合法 cell 组件（拒绝 `/`、`\`、`.`/`..` 与含 `..` 的组件）；`scope` 只能省略、给可解析的完整 `<ns>/__<kind>__/<id>`，或给裸 cell id；跨 ns scope 仍由数据面拒绝（ADR-097）。新增 control/server 回归覆盖非法 name/scope 拒绝且不落行、合法历史形态继续可用，以及 Cloudflare/原生创建路径同样 400。存量非法登记现在也可吊销（跳过不存在的数据清理作业，返回 `cleanup:null`），但不会被自动修复；Cloudflare 兼容 URL 的路径段建议使用 `[A-Za-z0-9._-]`。

## 2026-10-22 Cloudflare 兼容资源注册表的跨节点读写（ADR-268）

生产 `GET /client/v4/accounts/vwork-74/storage/kv/namespaces/CHTEST/keys` 返回 `kv resource "CHTEST" not found: control: not found`，同一 namespace 发布时又报 `no registered kv resource named CHTEST`。复现与定位：Cloudflare 兼容层的注册表操作直接读写接入节点本地的 control cell SQLite——`cloudflareBindingCall` 的 `Control.GetResource` 只查本地副本，`cloudflareResourceCreate/List` 还把注册行写到本地副本。admin 流量被负载均衡到任意 cell-agent 副本后，非 control owner 的本地副本为空/过期时，已注册 namespace 被读成不存在；`wrangler kv namespace create` 落在非 owner 时注册行只写进非权威副本并返回 200，owner 永远看不到它，于是 owner 侧的发布校验（wranglercompat）与 `/v1/kv/stats` 都报未注册。现已把注册表读改为经 control owner 的 `bindingInfo` 解析（owner 不可达 5xx 失败关闭）、把 KV/D1/R2/Queue 与 Workflow/Vectorize 的注册写改为先 `forwardOrClaim(control.Scope())` 再做 `capturedWrite`，并把列表/get 管理读（含 worker routes、secret 列表/读取）`forwardRead` 到 control owner、worker route 写带 `capturedWrite`；大 body 数据面不整包转发。双节点共享桶回归覆盖 keys/list/get、follower create 的权威性与 stats 可见性、发布不再误报未注册、route 写读落到 owner，`go test ./internal/server -run 'TestCloudflare|TestStats|TestControl'` 通过。剩余：真实多副本 + COS 的生产故障演练尚未执行，`bindingInfo` 1s 绑定缓存意味着 follower 上刚创建的 namespace 最多 1s 后可见。

## 2026-10-19 捕获失败后重新建立持久证明（ADR-232）

首次快照提交失败时，旧 `cellcapture.Ensure` 只记警告仍启动捕获；运行中的 LTX 提交失败后，下一次 `Ensure` 也会复用已停止的实例，发布重试持续得不到持久证明。本地代码现让快照失败直接拒绝写入，并在失败实例上先用完整快照重建证明，再允许后续写入；无快照配置保持失败关闭。进程内故障注入确认部署首次返回错误，同幂等键重试只产生一个版本和一条 deployment event，隔离 FS 对象桶恢复也只读到该版本。首次错误后本地事务可能已提交，调用方必须读回对账；真实 COS 持久确认失败与跨 AZ 接管仍需验收。

## 2026-10-19 强杀恢复后的 CLI 发布误报（ADR-231）

本地完整基线两次在 `cell-agent` 被 `SIGKILL`、`/readyz` 刚返回后立即发布 v4 时误报 KV、Queue、Workflow 均未注册。保留失败现场后，同一资源列表完整可读、同一发布命令成功创建 v4，说明旧 CLI 把暂时失败的控制资源读取静默变为空列表。现已改为读取失败明确报错，基线脚本等待资源读回后再发布；最终完整复跑 `RUNTIME-BASELINE-E2E: PASS`。`/readyz` 仅代表监听器就绪，不承诺所有 namespace 的 control cell 已恢复。

## 2026-10-19 Wrangler 重发布与跨节点版本配置（ADR-227/228）

隔离双节点复验发现，第二次原版 Wrangler 上传的 multipart 元数据省略 `triggers.crons`，兼容层曾把省略当成清空，active 版本失去 Cron 和短时 Queue consumer；Cron 后续分钟因此停止。另一个问题是 user-runtime 指向非 control owner 的 B 时，B 的内部版本配置端点从本地过期副本读取，找不到新版本便返回关闭 console/invocation 的策略：业务继续执行，但 Tail 和调用记录缺失。本地代码已改为上传省略时继承当前触发器、显式空列表才清除 Cron；专用 schedules PUT 等待控制写持久证明，内部版本配置先转发到 control owner。相关定向回归与隔离双节点 E2E 已通过；真实 COS 跨 AZ 故障仍待验收。

## 2026-10-19 双节点 Cron 重复派发（ADR-226）

隔离双 cell-agent E2E 在无故障时发现同一 UTC slot 有两条成功调用记录，相隔约 0.85 秒。旧 Scheduler、Timer Runner 与 waker 各自读写本地 SQLite，未先判 owner，也未等待 timer 状态的捕获证明。现已在本地代码中接入 owner session 判定、过期 owner 认领后恢复、fired/retry 捕获确认，并避免空闲轮询产生清理写事务。固定版本双节点 E2E 读回无故障阶段 3 个唯一成功 slot，A 停止后 B 对目标 slot 只有 1 次成功执行，A 返回后未重发。业务执行成功至 fired 持久确认之间的崩溃重投仍属至少一次边界。

## 2026-10-19 OTLP 日志与 trace 关联（ADR-222）

2026-10-24 本地增强（ADR-215）：持久调用成功/失败的 OTLP 完成日志增加整数 `cellhive.started_ms` / `cellhive.finished_ms` / `cellhive.duration_ms`，时间戳及 request ID 语义不变。只导出正值起止时间；缺失或倒序不生成 duration，普通 console/Tail 不补造时间区间。仅本地 Go/OTLP protobuf 回归验证，未部署、未新增真实后端验收。调用记录没有可信单一父 span，因此未增加 `parent_span_id`；Queue producer links 与 Workflow 父链边界不变。

真实 OpenObserve 验收发现 Worker 的请求和 traceparent 已正确进入租户，但 Tail 事件内部 `traceparent` 在 Go JSON 解码时被忽略，导致 OTLP 日志只有 request ID、没有 trace ID。现已保留入站关联值，公共 Wrangler Tail JSON 仍不展示内部字段；有效 ID 同时作为 OTLP 原生 trace/span ID 和可索引的 `cellhive.trace_id` / `cellhive.span_id` 属性输出。真实 Collector→OpenObserve 查询确认 HTTP fetch 的日志与 8 个 trace span 同 ID，也分别读回 Queue/Cron/Workflow 完成记录的 request ID 和 trace ID，见 `cf-compatibility-acceptance.md`。Queue 生产者上下文按消息持久关联，消费调用保持独立 trace 并通过 OTel links 关联；Workflow 创建请求到异步尝试的持久父链仍未实现。

## 2026-10-19 Wrangler 多节点发布与 Tail（ADR-221）

原版 Wrangler 上传落在非 control owner 的 cell-agent 时，旧桥接把 Cloudflare URL 留给原生部署转发，owner 的内部路由返回 404。现已让上传先按 control owner 转发，再由 owner 做 app/Workflow 预留和持久化发布；版本及部署读回也按 owner 转发。隔离 Compose 的双 cell-agent、共享 FS bucket、真实 user-runtime 验证了从非 owner B 发布，B 上的原版 Wrangler Tail 读取 A 执行的 fetch、Queue、Cron、Workflow 事件；100 秒会话无重连，主动重启 B 后 Wrangler 也完成重连。异步原生 Trace 只有 RPC 方法；现由带实例密钥的可信派发包装层补齐 Queue/Cron 的 CF 载荷，并在隔离双节点的原版 Wrangler 与原始 WebSocket 中验证（ADR-230）。资源注册表读写已按 ADR-268 纳入 control owner 路由；真实 COS 跨 AZ 故障和真实多副本演练仍需验收，见 `cf-compatibility-acceptance.md`。

## 2026-10-19 DO follower 确认接管（ADR-212）

本地代码改为带 DO shard owner 身份的写入先记录持久恢复指针，再以 follower fsync 证明打开输出门；接管先按 host manifest 逐文件补传 follower 尾部，活跃 cell-agent 的共享 session 不封存。缺少 follower 时写入回退到等 COS；恢复失败时拒绝 DO 激活。协议测试覆盖捕获 owner 记录释放后的恢复、其他 cell 不受阻断、缺失或损坏的 follower 日志和 A→B→A 单调指针。独立 k3d 的双 cell-agent 真实 workerd A→B→A 返回 `1→2→3`；确定性阻断 FS bucket 的 LTX 上传，确认 follower-only 写入 `4` 未落桶后强杀 A，B 接管返回 `5`，滚动请求最终返回 `126`，脚本退出 0。三副本 DO runtime 另做 A→B→C：B 的第三次写入由 follower 确认且未落桶，强杀 B 后首次接管的 C 返回 `4`；独立脚本模式退出 0。生产仍是旧镜像，真实 COS 的故障窗口尚未验收。活 session 按 scope、follower 重启和封存尾部改为流式读，避免一次载入约 20Gi 日志；冷接管仍需顺序扫描共享日志，生产发布前须测量 SG/USW 现有 spool 规模下的恢复时间。

## 2026-10-23 D1 binding 的 `binding` 名（例如 `DB`）曾被 loader 当成 database id，导致 `wrangler d1 create` 返回的资源名、`database_id` 都正确时，Worker 仍打开空的 `DB` cell 并报 `no such table`。现在 D1 deploy validation 优先用 `database_id`（API `id`）解析资源，loader 也用该 id 生成 scoped token 和 `/v1/d1/...?db=`；binding 名只保留为 env key。新增 `scripts/e2e/bindings-d1-id.sh`（binding=`DB`、database_id=`store-inspection-...`，预建表后 Worker 查询）回归。

2026-10-22 资源删除栅栏修复后还发现：follower 的 admin `DELETE /v1/control/resource` 被 owner 转发到 owner 的 REST 端口，但该端口只注册了 `POST`，返回 405。现 admin 转发改走 owner 记录中的 `admin_address`，保留 admin 凭证并由 owner 重新 `adminAuth`；REST mux 不再补 admin 路由，旧记录缺字段时按地址 host + admin 端口兜底。`ProviderBucket.ConditionalCreate` 曾不认 `resource-fence.json` 返回 `bucket: not supported`，cleanup job 10 次失败后不再自动重试、同名资源被 `ErrCleanupPending` 永久挡住。现栅栏走 append authority（ADR-270）：COS/OSS 用 `AppendObject(position)` 原子 create/CAS，旧 plain JSON 栅栏首次 CAS 时迁移；历史 10 次失败 job 用 cleanup retry 端点显式重置后继续。

2026-10-18 SG 高频日志与 fleet session（ADR-210）

SG 同一 `cell-agent` 服务的多个 cell epoch 不同；旧版 node-log 和服务端 session 缓存把首个 epoch 错当成整个进程 session 的不变量，导致其他 epoch 的写入每次退回 bucket 并打印 WARN。修复后仅 follower 集合保持不可变，fragment 的 scope/epoch 仍由 follower 日志逐条记录和恢复。cron 重复 500 另有租户 handler 错误：`vwork-24/kv-cache-counter` 报 `VWORK runtime context is invalid`，`vwork-40/vlog-automation` 报 `SUPABASE_URL is required`；失败 timer 改为持久化指数退避（ADR-211），但租户错误本身仍须修正。2026-10-24 SG 另发现 `vwork-24/kv-cache-counter` 旧 slot 在活跃版本 `Crons=[]` 后仍重试，派发缺少 worker/bundle 而返回 400；ADR-281 的代码修复使已物化 slot 可解析活跃 worker，发布后仍须观察其 handler 结果，不能将 400 消失视为执行成功。COS/OSS S3 兼容数据面按需校验响应 checksum，避免 AWS SDK 对每个不带可选 checksum 的成功 GET 打 WARN；TLS 和应用层 LTX 校验仍保留。

## 2026-10-18 DO 接管持久链（ADR-208）

SG 的 `cell-agent-0` PVC 曾被一个约 20GB 的旧 peer session `fragments.log` 占满，导致 queue/cron 和上传报磁盘满。2026-10-18 经授权把该 PVC 在线扩到 40Gi，容器内显示剩余约 20G；旧文件未删除（可能包含尚未覆盖到 bucket 的已确认数据）。该 session 的回收和增长原因仍需单独核查，当前扩容仅解决容量阻塞。

旧版 supervisor 在 DO 接管后把捕获序号重置为 0，可能覆盖同 epoch 的 LTX 段；全目录输出门还可能重发旧 PVC 上已迁走 shard 的状态。fleet 确认早于 bucket 上传时，旧版 DO 冷恢复还可能读到落后的 bucket。修复版首次捕获先读取并校验持久链水位，新段从更高序号继续；请求输出门仅捕获当前 host；同名不同内容的段写入被拒绝；DO 提交等待 bucket 确认。COS/OSS 的新 LTX 段以原生 AppendObject(position=0) 原子创建。已被旧版覆盖的数据不能由升级自动找回，需按持久副本冷恢复结果确认。单元回归和单节点 k3d 双 Pod/保留 PVC 的 A→B→A 通过（`1→2→3`，滚动 120 次写后 `124`）；SG、USW 真实 COS 桶原子 LTX 探针及线上 Worker→DO 连续写入 `1→2` 通过。生产集群跨主机 COS A→B→A 接管尚未触发验证。

## 2026-10-15 资源运维与调用审计（ADR-205）

2026-10-20 性能复核：资源删除栅栏曾使每个 KV Worker 请求重复点查权威桶约 5 次，其中多次在同 cell 世代锁内等待 COS。ADR-235 去重后常态为 2 次可并发点查；同机访问 USW E2E COS 的 A/B 中 KV 读从 3.7 提升到 76.7 RPS，写从 3.2 到 5.6 RPS；双 agent 新版读/写 74.2/13.0 RPS（均为跨地域客户端，不能外推为区域内上限）。写入仍要完成 RPO=0 的持久证明；若无合格 fleet follower 会等待 COS。区域内双 agent 的新版本吞吐尚未重测，见 `archive/bench/resource-fence-2026-10-20.md`。

2026-10-20 Worker `/ping` 单 runtime 热路径复核：原版 Tail 开启约 973 RPS；保留原生 Tail 挂载但不处理事件约 1,723 RPS，完全关闭约 1,721 RPS；64 条/5ms 有界合批约 1,441 RPS；256 条/3s 异步合批约 1,622 RPS（308 个 timeout）。改为 runtime 本地 ring 且查询时才转换事件后约 1,715 RPS（314 个 timeout），热路径只保留原生 Tail 投递和有界入队。静默调用仍可由 Tail 读取，ring 溢出/重启仍可丢实时事件。共享宿主测试含 socket timeout，只能用于定位，不能作容量承诺。早先双 runtime 的“合批从 1.73k 到 3.37k”因临时脚本误关 Tail 而无效，详见 `archive/bench/tail-batching-2026-10-20.md`。

DLQ 已新增只读 keyset 预览及按 ID 批量幂等 ack（owner 路由/capture）；预览不 claim、不改 lease/attempts/visibility。KV/Queue 删除已有持久化 cleanup job、查询、自动退避和手动重试；物理 scope owner 清理本地 SQLite/WAL 和权威 LTX，保留小型 generation/fence 元数据，防止异步上传恢复旧数据。独立 DLQ 不删除，仅清理来源可证明属于主 Queue 的消息；旧来源缺失消息需人工 preview/ack 后 retry，否则作失败报告。共享物理 scope 当前拒绝破坏性删除。旧 sealed follower logs 仍按 session 回收，不逐资源立即清除历史副本。

Queue/Cron 调用 UUID、逻辑重试关联、结果/异常/console 日志已独立持久化，可按 namespace/worker/time/id 查询。retention 7 天/10,000 次每 namespace，console 截断有标记；进程在完成报告前崩溃留 running 状态。native Tail `off` 不关闭调用记录。所有 replica/consumer/recovery 节点需同步升级后才开放删除。

复核问题已修复并重新验收：本地 incarnation marker + 世代准入阻止离线/迟到请求回退，retirement GC 义务跨 namespace 清理保留，已 claim 旧批次不能跨重建执行；目的地事务验证 source_generation，历史 DLQ 与 transfer-only 目的地均入清理快照。混合批次部分重试按消息 lineage/前驱关联，返回值和 ack/retry 明细已持久化。最终全量/race/JS/vet/build 和 Docker 启动 E2E 均通过，见 `archive/resource-operations-invocations-2026-10-15.md`。

## 2026-10-15 KV / Queue 原生语义第一轮

- 原生 KV `expiration_ttl` 已支持 1 秒起；Cloudflare 兼容入口仍执行 60 秒下限。
- `incr` 已增加无损 `value_i64` 十进制字符串；旧 `value` 字段保留兼容。
- Queue 幂等 ledger 已从消息行分离，ack 后仍保留 24 小时；metadata 指纹使用 SHA-256 和 UseNumber JSON 解码；DLQ/transfer 保留 metadata 和业务 idempotency key，并通过独立 transfer receipt 去重。
- `max_batch_timeout` 与逐 Worker CPU/内存硬隔离仍未在本轮实现，需单独确认 VWork 合同。

## 2026-10-15 Wrangler 命令兼容边界

Workflow 实例详情当前只持久化完成步骤的结果，不记录 Cloudflare 的逐次尝试、步骤开始结束时间及运行中步骤；Wrangler `instances describe` 只展示可证明的完成步骤。`restart --from`、跨 Worker Workflow 仍不支持。Queue consumer `retry_delay`、`max_batch_timeout` 与有限重试已接入（ADR-216）；固定版本 Docker E2E 已对领取中的消息强杀 cell-agent、重启并发布新版本，确认 lease 到期后按新 active 版本重新投递。跨节点故障与长时压力仍待验收；purge 为持久 owner 写入。Vectorize 原版管理只覆盖 V2 create/list/get/delete/info，点积 metric 不支持。Hyperdrive 原版管理命令现已支持 create/list/get/update/delete（id=资源名，origin 加密，managed `hd_*`），但 `caching`/`mtls` 明确拒绝；不承诺 Cloudflare 缓存、mTLS 或连接池语义。详见 `wrangler-compat.md`、ADR-202/203。

## 2026-10-15 调度正确性修复

Cron 同分钟不同表达式独立物化；`noRetry()` 返回租户执行结果，避免跨 isolate 闭包失效。Workflow busy claim 不消费 timer，create/event/resume/restart 有事务性 `workflow-run` outbox；等待事件原子 park/释放租约，防事件竞态丢唤醒。DO alarm 采用先持久化 timer 的可靠交接、generation 校验、active/pending 消费与重试；上报失败不静默成功。真实 workerd 专项、Go/race 与 Docker 完整调度链路验证见 `timers-and-dispatch.md` 和 `testing.md`。剩余边界：DO 重试不是 CF 原生指数退避/次数上限；外部副作用仍至少一次；`transactionSync` 内 alarm 不支持。旧数字 alarm 惰性兼容，新旧 dispatcher 不能混用忽略 `workflow-run`/alarm generation 的版本。

---

## 决议一览

| ID | 主题 | 决议状态 |
|---|---|---|
| C-01 | DO owner claim flow | ✅ 定稿（durable-objects.md） |
| C-02 | bundle/assets 读取路径 | ✅ 已实现：runtime 向 cell-agent 申请短期只读 presigned URL 后直连对象存储；本地 FS/后端不支持 presign 时回退内部鉴权点查（ADR-030） |
| C-03 | node-log / recovery | ✅ **完整实现**（ADR-057/066：node-log 在 fleet ack 前建立并绑定不可变 ensemble；follower 按 owner session 保存连续 fragment；接管前 seal/tail 无缺口恢复并记录 `covered_seq`；失败时拒绝接管；节点死亡自动编排已接线。2026-09-23 真实双进程 E2E：写入 200 key → `SIGKILL` owner → 删除本地目录 → follower 接管，`restoreverify` integrity=ok/keys=200，live read missing=0/wrong=0） |
| C-04 | DO 冷激活 / restore | ✅ 定稿（durable-objects.md） |
| C-05 | DO alarm 恢复 | ✅ 定稿（见下） |
| C-06 | 路由投影下发 | ✅ 定稿：**纯拉取 + 5–10s TTL（无 push）**（ADR-031） |
| C-07 | in-flight DO 请求迁移 | ✅ 定稿（durable-objects.md） |
| I-01 | 租户 binding 鉴权 | ✅ 已实现（ADR-061：scope 声明 + scoped token；KV 端点强制） |
| I-03 | WAL checkpoint/截断 | ✅ 定稿（见下） |
| I-04 | DO WebSocket 重连 | ✅ 定稿（见下） |
| I-05 | Timer 派发去重 | ✅ 定稿（见下） |
| I-07 | scope-hash 路由热点/失败转移 | ✅ 定稿：best-effort affinity（见下） |
| I-09 | 多租户隔离安全 | ✅ 定稿（见下 + security） |
| M-01 | MinIO 版本说明 | ✅ 定稿 |
| M-02 | :7000 协议复用 | ✅ 定稿：**:7001 单一内部 REST**（Go↔Go 与 JS↔cell 共用）；**gRPC 平面已取消**（ADR-136） |
| M-03 | owner expiry vs node lease TTL | ✅ 定稿：expiry ≤ node lease TTL |
| M-04 | workerd 多文件 actor 存储 | ✅ 已确认（P0.7）：`metadata.sqlite` + 每 actor `<hash>.sqlite`(+wal/shm)；见下 |
| M-05 | 静态资产服务路径 | ✅ 定稿：对象存储/CDN 直连（与 C-02 一致） |
| M-06 | 内部 vs 对外 API | ✅ 定稿：同一 REST 接口、仅鉴权不同 |
| M-07 | drain token 协议 | ✅ **已实现**（ADR-057：`internal/drain`，条件创建/CAS/TTL/Release；cell-agent 优雅关停用） |
| M-08 | cellhive dev | ✅ 定稿（见下） |
| M-09 | cell 协议版本化 | ✅ 定稿（见下） |
| M-10 | 限流/配额/admission | ✅ 定稿；**已实现 admission**（ADR-087），剩余项见下 |

---

## C-05 DO alarm 恢复（定稿）

1. alarm 行随 actor SQLite 复制到 cell-agent（持久）。
2. **supervisor 只读 workerd 的 alarm 表，提取到期时间**，上报 cell-agent：`alarm_upsert(scope, dueAt, token)` / `alarm_delete(scope, token)`。
3. cell-agent 在 **waker cell** 维护 due 索引（按时间分桶）。
4. waker 发现 "owner 已死 + 到期" 的项，**定向触发该 DO 的 takeover**（claim + restore）到可用 do-runtime，由新 owner 读 alarm 行派发；派发成功后原子推进/删除索引项。
5. **带 alarm 的 DO 优先常驻**（放置策略/`DO_PREVENT_EVICTION` 类似），减少冷激活。
6. 若无法从 alarm 表可靠提取（P0 验证 schema），回退：waker 定期向 live supervisor 询问各自最早到期 alarm，仅对 owner 已死者做 takeover。

## I-01 租户 binding 鉴权（定稿）

- **主防线是网络隔离**（见 I-09）：租户 Worker **无法**访问内网 cell-agent :7001。
- 平台角色 token **只存在于 host adapter（平台代码）与 cell-agent 之间**，**绝不注入租户 env**；租户只拿到 binding facade。
- host adapter 为每个 binding 生成资源范围 token；cell-agent 校验签名、namespace、binding kind/name 与登记 ACL，缺省或越权即拒（ADR-074/181）。
- ADR-304 要求无状态 kid pattern 上限、固定最大86400秒、到期/root轮换，无 issuer 注册表或即时撤销；修订实施与验收中，见 [`security.md`](../security.md)。
- 管理与业务 binding 统一 Bearer HS256 JWT，namespace 业务权限与平台 admin 分离，无静态/OIDC 回退。

## I-03 WAL checkpoint/截断（定稿）

- supervisor 轮询 `(salt, frame)`；salt 变化或 frame 归零 = 发生 checkpoint。
- 检测到 → **暂停 delta 流** → 用 **SQLite Online Backup API**（只读连接）取一致快照 → 上传为 **L9 快照**（临时 key + 原子提交）并记 txid → 从新 salt/frame 0 续 delta，并写 **snapshot→delta 衔接标记**。
- cell-agent 校验快照完整性与衔接连续性。
- P0 必测：**活跃写入期间触发 checkpoint** 的检测与对齐。

## I-04 DO WebSocket 语义（定稿，CF 兼容）

- **部署 / promote / 迁移都重启 DO**：旧 owner 以 **1012** 关闭 WS；**客户端重连**；新 owner 重新构造，**handler 重启**（`webSocketOpen` 重新触发）。
- **不做 resume**：平台不保证跨重启会话连续；DO 会话状态须落存储。
- host adapter 仅负责：检测断连 → 重新解析 owner → 让客户端重连。
- 与 Cloudflare 行为一致（ADR-032）。

## I-05 Timer 派发去重（定稿）

- 去重在 **cell 侧**：timer 带 `token = hash(scope, kind, dueAt, occurrence)`；cell SQLite 记 `fired:<token>`（TTL）。
- **已 fired 的 token 跳过**；cron 按 slot 唯一键单次触发；DO alarm 以 actor SQLite 的 alarm 行为准，派发成功后原子推进/删除 due 索引。
- user-runtime **不做去重**（无状态）。

## I-07 scope-hash 路由（定稿）

- 哈希仅为 **best-effort affinity（性能提示）**，非硬约束。
- 首选 cell-agent 不可用 → **换下一个可用节点**；不同节点上传的段凭 **epoch prefix** 合并，无歧义；owner 记录是权威。
- 可选一致哈希环以降低成员变化抖动。

## I-09 多租户隔离（定稿）

- workerd capnp：租户 loaded worker 的 `globalOutbound` = **公网 only network service**（不含 RFC1918 / cell-agent 地址）。
- **host adapter 的内网 binding 不暴露给租户**。
- worker CPU time 与 V8 heap per-worker 强制上限**尚未实现**；subrequest 上限已由动态 Worker 的 invocation quota 执行并有真实 workerd 测试。当前另有 deploy-time WorkerCode/env 预算、per-namespace 写准入与 cell-agent 过载拒绝。详见 [`security.md`](../security.md)。
- **动态加载总内存保护仍在收口（2026-10-13）**：先前真实 pinned workerd 在 2 GiB 容器中，451 个不同 1 MiB Worker 或 575 个同 SHA 不同版本触发 OOMKilled；189 个 10 MiB 静态资源也触发 OOM。当前公共 user-runtime 已加入版本退休、活动生命周期、冷加载并发/容量准入、源码/plan/env/asset 字节预算和大 asset 流式返回；同样 2 GiB/1000 个 1 MiB 合成 Worker 压测不再 OOM，但仍观测约 840 MiB cgroup、workerd abort 日志，尚未作为生产容量承诺。DO runtime、内部 dispatch、长连接/RPC、FD 限制和更长时 churn 仍需独立验证。证据与复现入口见 [`runtime-memory-2026-10-13.md`](bench/runtime-memory-2026-10-13.md) 与 [`workerd-loader-lifecycle-2026-10-13.md`](bench/workerd-loader-lifecycle-2026-10-13.md)。

## M-02 端口划分（定稿）

| 端口 | 协议 | 用途 |
|---|---|---|
| `:7001` | **REST/JSON + 二进制帧** | 内部全部：Go↔Go（peer LTX、DO WAL/claim/restore、owner 转发）与 workerd(JS) bindings 共用；LTX 热路径是长度前缀二进制帧走 HTTP 101 持久流（ADR-042） |
| `:8082` | REST/JSON | 对外 admin/控制面（经 Traefik） |

## M-06 内部/对外 API（定稿）

**同一 REST 接口、仅鉴权不同**：内部走 `:7001`（平台 token + scope 声明），对外走 Traefik→`:8082`（租户 token）。ADR-013 的"对外后置"= 尚未对外开放，**不是另一套 API**。

## M-08 cellhive dev（定稿；完整设计见 dev-mode.md）

状态：**M1/M2 已实现**（`cli/` Bun dev + `internal/wranglercompat` 服务端拦截，2026-09-15，见下）；剩余见下。要点：

- **dev = Bun CLI + Miniflare**：Miniflare 起真实 workerd + 本地模拟绑定；**dev 机器零 Go 后端进程**（ADR-065）。生产仍 Go workerd + cell-agent。
- **版本 pin**：`miniflare@5.20260916.0-alpha` 的 workerd 精确 override 到平台 pinned `1.20260916.1`；lockfile/安装版本测试拒绝漂移。
- **零外部依赖**：无需 Docker/MinIO/Traefik；数据在 `./.cellhive-dev/`（Miniflare persist）。
- **标准 CF 形态**：`wrangler.jsonc` + KV/D1/R2/Queue/DO/`vars`/`secrets`/assets；热重载/inspector/persist 用 Miniflare 自带。
- **契约对拍**：Miniflare 作为 CF 语义 oracle，golden 对拍约束我们 Go 侧行为（ADR-014 思路）。
- **deploy 服务端功能/兼容性拦截（ADR-065）**：服务端权威校验（bundle/日期/flag/绑定矩阵/资源已登记/DO 生命周期/未知字段）；**Miniflare 支持的 `images/ai/browser/vectorize/...` 平台拒绝** → CLI/dev 用同一校验库提前警告。
- **用户代码兼容是平台责任**：补齐 facade 缺口（KV list metadata、R2 `R2Object` 字段、D1 `meta.last_row_id`、错误形状）。
- **M1 已实现并验证（2026-09-15）✅**：`cli/`（Bun）已建；`cellhive dev` 读 wrangler 配置子集 → 起 Miniflare dev server，**KV/D1/R2/`vars` 经 HTTP 全通**；preflight 正确拒绝 `compat_date_too_new`/`unknown_flag` 并对 `images`/`ai` 等给 `unsupported_binding` 警告。
- **pin 规则（重要修正）**：Miniflare 与平台 pinned workerd 必须同期精确对齐。当前采用 **`miniflare@5.20260916.0-alpha` + `overrides.workerd=1.20260916.1`**，module/KV/D1/R2 与版本读回已实测。
- **M2 已实现并验证（2026-09-15）✅**：Go `internal/wranglercompat`（绑定矩阵/`compat_date`/flags/资源登记/未知字段 + 稳定码 + 单元测试）；`/v1/control/deploy` 服务端拦截接线（`deploy_rejected` + findings + bundle 存在性点查）；CLI 预检与同一套码对齐；`internal/server` 拦截用例通过。
- `images:{binding:"IMAGES"}` 本地可构造且**无凭据**启动 → dev 会真的"能用"平台拒绝的 `env.IMAGES`（正是服务端拦截要兜的）。剩余待测：Miniflare 本地 `ai`/`browser` 的实际行为（是否需 CF 凭据）。
- dev：热重载/Queue/`rules`/assets/`--strict-build` 已实现。Miniflare 5 内建 router 的 `has_user_worker` 无法同时满足 not-found handling 与 worker fallback；ADR-114 的同实例、无额外 listener 的 dev-only service-binding router 已实现，`_headers`/`_redirects`/404-page/SPA/worker fallback/`run_worker_first`/binding/hot reload 均真实通过。

详见 [`dev-mode.md`](../dev-mode.md)。

## M-09 协议版本化（定稿）

- owner record 增 `proto_version`；**LTX 段头带版本字节**。
- 升级遵循 **reader-before-writer**；**回滚安全：读写版本差 ≤1**；不兼容变更走显式迁移流程。

## M-10 限流/配额/Admission（admission 已实现，ADR-087）

- per-namespace 写速率限制；cell-agent 过载返回 `503 overloaded`。
- **单 DO WAL 大小上限**；worker `limits`（cpu/mem）；消费 `pressured`/`shed_cells` 做 backpressure。
- queue DLQ 上限。
- 多租户生产前必须。**现状（2026-09-16）：已实现 admission**（`internal/admission`：per-namespace token bucket + 写端点 429，ADR-087）。**剩余**：单 DO WAL 大小上限、worker `limits`（cpu/mem）、`pressured`/`shed_cells` backpressure 消费、queue DLQ 上限（DLQ 本身已做，ADR-072）。

## M-04 workerd 多文件 actor 存储（✅ 已确认）

P0 先枚举 workerd 实际文件布局（共享 metadata + 每 actor SQLite）；初步按整体复制 + 目录级快照保证一致；WAL 以每个 SQLite 为单位。

---

## 未验证 / 待实现（设计已定，需 P0 或实现验证）

以下**不是悬而未决的设计问题**，而是"设计已定、但结论依赖实测或尚未实现"：

### P0 必须实测（决定可行性/性能门）

| 项 | 状态 | 证据 / 若失败的回退 |
|---|---|---|
| workerd actor SQLite 是否 WAL 模式、可被外部只读打开 | ✅ 已验证 | P0.7 历史实测：`walscan` 解析真实 workerd actor `-wal`（workerd 2026-06-15） |
| workerd actor 文件布局（共享 metadata + 每 actor，M-04） | ✅ 已确认 | P0.7：`metadata.sqlite` + `<hash>.sqlite`(+wal/shm) |
| LTX 编解码 / 复制 / epoch restore | ✅ 已验证 | P0.3：`internal/ltx`、`internal/replica` 测试 |
| 双节点 quorum-1 fsync ack + bucket 降级 | ✅ 已验证 | P0.4：`internal/peer`、`internal/server` 测试 |
| node-log + recovery（已 ack 未上传 → 收齐落桶） | ✅ 已验证（模拟） | P0.5：`internal/recovery` 测试 |
| 每 ack PUT 数 ≪ 1（group commit） | ✅ 达成（fleet ≈0.09） | P0.6：`internal/upload` + `cmd/cellbench` |
| 热路径禁止 List | ✅ 达标 | P0.6：节点样本 5s 缓存；`list` 仅随刷新 |
| checkpoint/截断检测与对齐（I-03） | ✅ 历史实测（workerd 2026-06-15）：DO SQLite 默认 **1000 页 autocheckpoint**；每次 checkpoint **salt1 递增（salt 轮换）**、`total_frames` 重置为 1000、WAL 文件 **reuse 同尺寸 ~4.12MB（非 TRUNCATE shrink）**。我们的 `wal.Cursor` 按 salt 轮换报告 `Checkpoint=true` 并重定基 → 与 workerd 对齐 | 实测：每 ~1000 写触发一次；`walscan` 观测 salt 序列 `...889→890→891...`、size 恒 4120032。**含义**：stock workerd 无法关闭 autocheckpoint，因此逐 actor 捕获会每 ~1000 写产生一次快照边界（ADR-049 已处理）；回退：周期快照 |
| 从 workerd alarm 表可靠提取 due（C-05） | ✅ 历史实测（workerd 2026-06-15）：alarm 存 per-namespace `metadata.sqlite` 的 **`_cf_ALARM(actor_id TEXT PRIMARY KEY, scheduled_time INTEGER) WITHOUT ROWID`**；`scheduled_time` 为 **Unix 纳秒**（`Date.now()+Δms` 存为 ns；实测行 `actor_id=862b…a8112, scheduled_time=1789453754754000000`）；按 due 提取 = `SELECT actor_id FROM _cf_ALARM WHERE scheduled_time <= <now_ns>` | 触发：真实 DO `storage.setAlarm(Date.now()+Δ)`；actor DB `<hash>.sqlite` 与 metadata 同目录。回退：waker 询问 live supervisor |
| 真实对象存储条件语义 + presigned | ✅ 原生 OSS 北京测试桶与原生 COS 香港测试桶通过完整启动诊断；固定 MinIO 因条件删除不合格 | `internal/bucket TestProviderCloudDiagnose` / `TestNativeAppendCloud`；其它桶仍须逐实例探测 |
| **fleet commit 每笔做一次 S3 GET（owner epoch）** | ✅ 已修 | `owner.ResolveCached`（1s TTL，claim/release 失效）；远程 S3 下 fleet c=1 p50 **1.26ms** |
| **follower spool 每段单独 fsync（fleet ack 吞吐封顶 ~1.1k RPS）** | ✅ 已修 | 改单 append-log `segments.log` + 批量一次 fsync（`Spool.AppendBatch` + follower 合批器）；2 节点 fleet **~1.1k → ~13.8k → ~45k RPS（帧化批量再 3.3x；与单节点 bucket async 持平）** |
| 持久 peer 流 / pipeline | ✅ 已实现（ADR-042） | HTTP 101 自定义二进制流；4 scope-hashed lane × 4 批在途；单 scope 45.0k→47.1k（+4.5%），8 scope p99 ~63–65ms→48–52ms；吞吐瓶颈已转为本机 CPU/单盘 |
| 真实 SQL→WAL→LTX→fleet TPS | ✅ Go SQLite 全链路（ADR-043~050）；✅ **真实 workerd DO 输出门端到端**（ADR-051，`cmd/cell-supervisor` + `workerd/spikes/p0/gate/` + `cmd/gatebench`） | Go SQLite：loopback c=128 29.6k TPS/p99 9.1ms。workerd **进程级**上限：裸 DO ≈**800–855 req/s**，加输出门 N=1 553 / N≥4≈790；**多 DO 在单 workerd 内不平移**（1→8 actor 均 ~800，fsync 约束）。非 DO `sqlbench`：**durable 28.9k / 纯本地 43.7k TPS**（c=128）。cell-agent KV（ADR-052 缓存 cell 后）：PUT ~**5.4k** / GET ~**12.9k** rps（c=32）。恢复 actor DB `t.n` 精确一致、integrity ok |
| SQL capture 性能优化 | ✅ ADR-044/045/046：增量 WAL + raw binary commit + 内存 ticket + WAL2 page-map + prepared statement | loopback c=128 **24.8k TPS / p99 9.3ms**；纯 SQL 上限 ~107k（prepared）。c≥256 吞吐持平、延迟上升 |
| snapshot + 恢复 apply | ✅ ADR-049：checkpoint 后读 DB 文件全页快照 + `restore.ApplyFile` 逐页重建 | **数据准确性端到端校验通过**：5,000 唯一 key 从 follower 复制链恢复 → 5000/5000 行、0 缺失/错值、integrity=ok。◐ 受控 checkpoint 需静默期调用 |
| 连续写入下的自动 WAL 截断 | ✅ ADR-050：pause writer → 短 TRUNCATE → 水位 → DB 文件快照 → 新 WAL 续 delta | 5000 唯一 key 持续写入 + 自动 ckpt → 链恢复 5000/5000、0 缺失/错值、integrity ok；loopback c=128 ckpt=4MiB 22.2k TPS（vs 29.6k 无 ckpt）|
| fleet 跨网络 RTT | ✅ ADR-048：capture 级有序 pipelining 已实现（自适应，loopback 走串行） | 20ms 单向 c=1024 **8.6k → 13.3k TPS（+55%）**；5ms c=1024 24.0k → 29.3k（+22%）；loopback 持平。受控 checkpoint 仍待 snapshot/link apply |
| 单节点 commit 模式 | ✅ 已实现：`CELLHIVE_DURABILITY`=`auto`\|`fleet`\|`bucket` + `CELLHIVE_BUCKET_WAIT`（默认 true）。`fleet` 无 peer 时退化为等 bucket 并告警，不静默 ack；`fleet`+`WAIT=false` 启动拒绝 | FS c=64（3 轮）：async 中位 ~38.5k / batch ~29.2k RPS（async ~1.3x）；**MinIO loopback 噪声 ±3x，两者基本打平**，单轮排名不可信 |
| 云对象存储尾延迟 | ⚠️ 实测：远端对象存储单节点 batch p50 可达数百 ms–秒级、偶发超时；async 低 p50 但 RPO>0 | 桶提交仅后台；上 ≥2 节点走 fleet；同区/就近 |
| **S3 权威桶条件语义** | ❌ 2026-09-22 `make s3-test` 固定 MinIO：`s3probe` 四条件写 + range 通过、复制恢复链通过，但新增 `TestS3BucketIntegration` 检出 stale `DeleteObject If-Match` 未拒绝（返回 nil） | 完整权威契约失败；不能用该 MinIO 镜像上线。需支持目标 CAS、条件删除的对象存储并逐实例验证；不可仅凭旧四项通过推断可用 |
| 跨节点"捕获→cell-agent→证明" p50/p99 | ✅ 真实多进程 + 真实 TCP（loopback）已测；◐ 云/跨主机未测 | 评估 FUSE 或调整放置 |
| do-runtime 冷激活/恢复时间 | ✅ 功能已由真实 workerd 冷激活/接管测试覆盖；◐ 云端大对象与跨主机时延未测 | `TestDoRuntimeCrossNodeColdActivation`、`TestDoRuntimeTakeoverAfterCrash`；生产容量仍需环境压测 |
| 兼容日期/flag 表与 pinned workerd | ✅ 已实测（workerd 2026-09-16）：支持 `compatibilityDate` 上限 **2026-09-23**（更新会硬报错"newest date supported ... 2026-09-23"）；未知 `compatibilityFlags` 硬报错 `No such compatibility flag: <x>`，已知 flag（如 `nodejs_compat`）通过配置校验 | 探测方式：临时 capnp 设未来日期/伪 flag + `workerd serve`。含义：pinned workerd 的 flag 集必须在加载时逐项验证，不能盲信 wrangler 默认 |
| `workerLoader` 加载租户模块 + 真实 KV 往返 | ✅ 已验证 | P0.7：`workerd/spikes/p0/`，`put:200` / `get:hello-world` |
| `globalOutbound` 公网-only 隔离强制 | ✅ 历史实测（workerd 2026-06-15）：outbound `network.allow=["public"]` 时，租户 `fetch("http://127.0.0.1:7001")` 被拒：`connect() blocked by restrictPeers()`（host 返回 500）。**隔离可强制、无需改 workerd** | P0.7 loader 默认 `allow=["public","private"]` 以便连本地 cell-agent；生产应设 `["public"]`，cell-agent 走 service binding/HMAC 而非公网 fetch（**已实现：ADR-073**——租户 outbound public-only，facade 经 `PLATFORM` service binding 出网，真实 workerd e2e） |
| snapshot watermark 与镜像一致性 | ✅ **已修复**（ADR-055）：`poll` 检测 checkpoint 时未回填就快照 → 把空/旧镜像标成高 watermark → 静默丢 1..N 事务。修复为 `rebaseline`（暂停写者+TRUNCATE 回填+读 W0 后再读页）。回归 `TestRebaselineSnapshotReflectsCommittedWrites`；E2E full restore `integrity=ok` |
| 冷启动按需拉取（桶） | ✅ ADR-056 + GC：`compaction` 写 LTX payload page map（页→对象偏移），`replica.PageFetcher` 用 `RangedGet` 单页取数（L0 覆盖优先）；`restoreverify -bucket` 实测 full `integrity=ok`、`-pages 1-2` 稀疏 hole 正确 |
| P1 cell 复制完整（LTX/快照/compaction/paging） | ✅ 快照分页、compaction 内核+编排、txid 连续性、**稀疏按需 paging**（ADR-055/056）、**L0/L1 GC**（ADR-056）均已完成（详见文末「P1 复制缺口清单」） | 快照分页：`ltx.EncodeSnapshotParts`（flags 分片、`<min>-<max>.ltx`）+ `restore` 合并 + capture/supervisor 接入。compaction：`restore.Compact` + `internal/compaction.Compact`（阈值 `CELLHIVE_COMPACTION_MIN_SEGMENTS`=64 / `_MIN_BYTES`=64MiB）+ `replica` L1 前缀/manifest + `Restore` 优先 L1 + cell-agent 后台循环（`CELLHIVE_COMPACTION_INTERVAL`=30s，写前 Verify 复核 owner/epoch）。**已补（ADR-055/056）**：稀疏文件按需 paging（`replica.PageFetcher` + LTX payload page map ranged-get）、L0/L1 旧对象 GC。注：**cell-agent 侧运行期 paged VFS 已实现（ADR-160，`internal/pagedvfs`，默认开；链 ≥ `CELLHIVE_PAGED_MIN_BYTES` 才分页，小链整克隆）**，KV/D1/Queue/Workflow/Vectorize cell 的冷恢复按页加载；do-supervisor 与 `restoreverify` 亦用同一 paging 原语。**E2E**：双 cell-agent + `COMPACTION_INTERVAL=1s`，sqlbench 5000 事务后 159 个 L0 段 → 1 个 L1 对象 + manifest（min=32/max=5000），restore 加 txid 连续性 fail-closed |
| DO 能力（facets / preventEviction，pinned 2026-06-15） | ✅ 已实测：`ctx.facets` **存在**（host-actor 托管多 facet 可行；`class` 必须来自 `workerLoader.getDurableObjectClass()`，裸类/普通 namespace 绑定报 TypeError）；capnp `preventEviction = true` **可解析并启动**（resident vs evictable 两变体可行） | 见 **ADR-053**；实现属 **P3**（roadmap）。 |
| DO 写合并 / 减少 fsync（stock workerd） | ✅ 历史实测（workerd 2026-06-15）：**一个 DO 事件（请求）内的所有 `sql.exec` 写被 workerd 自动合并成一次原子提交**（错误信息原文 "Durable Objects' automatic atomic write coalescing"）。故 `req/s` 固定 ~900–970，而 `writes/s` 随"每请求写数 k"线性增长：k=1→928、k=4→3.9k、k=16→14.4k、k=64→52k、k=256→**196k** writes/s。`sql.exec("BEGIN")` 被**拒绝**（要求改用 `state.storage.transaction()` / `transactionSync()`）；多语句 `exec("a;b;c")` 可行但不改变提交次数 | 减少 fsync 的唯一 stock 手段 = **提高每请求写数**（批量化），不改 workerd（ADR-001）；`BEGIN` 不可用 → 原子性用 `transactionSync`。**已验证**：带输出门时一次门覆盖整批，k=256 达 **110k writes/s**（对比 1 写/请求的 531，~207×），且恢复 `t.n` 精确一致。平台侧 wrapper `workerd/wrapper/groupcommit.js` 已实现（1 op/请求 → 3,637 req/s，~6.9×，avg_batch≈23）|

### 已实现（P0 代码，历史清单）

`internal/`：`config`、`cell`、`bucket`（FS + 诊断 + 计数 + **S3Bucket**）、`owner`、`lease`、`server`、`cellstore`（SQLite）、`wal`、`ltx`、`replica`、`peer`（spool/transport）、`nodelog`、`recovery`、`upload`。`cmd/`：`cell-agent`、`cellhive`、`cellbench`、`realbench`、`s3init`、`s3probe`、`walscan`。见 [`p0-report.md`](p0-report.md)。

### 待实现文档

无（`docs/` 现行文档已建，含 `tracing.md`；后续随实现补 `routing`/`security` 等的细节）。

### 已确认的推荐（用户交互确认，2026-09-14）

- **ADR-030**：bundle/assets 已实现**短期 presigned URL 直连对象存储**；本地 FS 或不支持 presign 时回退 cell-agent 内部鉴权点查。
- **ADR-031**：路由投影**纯拉取 + 5–10s TTL（无 push）**。
- **ADR-028**：端口划分 `:7001` 内部 REST（Go↔Go 与 JS↔cell 共用；`:7000` gRPC 平面已取消，ADR-136）/ `:8082` admin。
- **ADR-035**：限流/配额/admission **P1 实现**，保守默认 + 可配。
- **ADR-036**：管理后台**不区分管理员与租户**，租户身份作为请求参数传入。
- **ADR-032（A）**：DO WebSocket = **CF 兼容**（部署/迁移都重启 DO、断连、客户端重连、handler 重启，不做 resume）。
- **ADR-029（B）**：binding 鉴权 = **网络隔离 + scope 声明**（scoped token 留 P1）。
- **ADR-037（C/D/E）**：**node-log/recovery 完整实现**；**DO claim 由 cell-agent 驱动**；冷激活/接管用**分页懒加载**。

### 评审范围说明

本清单来自**一轮系统性评审**。设计仍建立在若干**未证假设**（stock workerd 的 DO/WAL 可观察性、自研 cell 复制协议的正确性）之上；实现阶段仍可能有新的缺口。**"已定稿"= 设计决议已记录，不等于验证通过或已实现。**

---

## 附录：文档规划（均已创建，见 `docs/`）

| 文档 | 覆盖 | 优先级 |
|---|---|---|
| `routing.md` | 路由投影、版本解析、自定义域 | P1 |
| `security.md` | 租户隔离、binding 鉴权、network policy | P1 |
| `control-plane.md` | CLI 资源管理、部署流水线、身份模型 | P1 |
| `bindings.md` | 每种 CF binding 的 cell 映射 | P1 |
| `timers-and-dispatch.md` | timer 抽象、去重、cron/queue/workflow | P2 |
| `scaling-and-ha.md` | autoscaler、handoff、balancing | P2 |
| `deployment.md` | K8s/Compose/systemd 示例 | P2 |
| `compatibility-matrix.md` | CF/Wrangler 兼容矩阵 | P2 |
| `testing.md` | 测试策略、故障注入 | P2 |
| `glossary.md` | 术语表 | P3 |

## P1 复制缺口清单（LTX / 快照 / compaction / paging）

对照 roadmap P1「cell 复制完整」与 ADR-043~054 的现状盘点：

| 缺口 | 状态 |
|---|---|
| LTX 段格式（WAL1/WAL2、batch、link） | ✅ 已有（ADR-043~045） |
| 快照（KindSnapshot 全页镜像） | ✅ 已有（ADR-049） |
| **快照分页**（大库拆段，> maxSegmentBytes 可快照） | ✅ ADR-054（`ltx.EncodeSnapshotParts`） |
| **compaction 折叠内核**（chain → 单快照） | ✅ ADR-054（`restore.Compact`） |
| **compaction 编排**（L1 前缀 + manifest + 后台触发 + 接管优先读 L1） | ✅ ADR-054（`internal/compaction` + cell-agent 循环） |
| txid 连续性 fail-closed（防“静默过期”还原） | ✅ ADR-054 |
| **稀疏文件按需 paging**（冷启动不整库下载） | ✅ **原语已交付**（ADR-055/056；`internal/replica TestColdHydrateIsWholeObjectNotPaged` 实测 cell-agent hydrate 是整对象下载 + 0 ranged 读）：`restore.IndexChain`/`SparseFile`、`replica.PageFetcher` + LTX payload page map ranged-get、`restoreverify -bucket`；**GC ✅**（ADR-056）。**消费方**：do-supervisor（DO 文件）、`restoreverify`，以及 **cell-agent 的 cell 冷恢复（ADR-160，`CELLHIVE_PAGED_RESTORE` 默认开；链 ≥ `CELLHIVE_PAGED_MIN_BYTES`（默认 256MiB）走 fault-in VFS 按页加载，小链整克隆，后台按 `CELLHIVE_PAGED_HYDRATE_MBPS` 补齐）**，稳态读本地 |
| L0/L1 旧对象 GC | ✅ ADR-056（`compaction.Options.GC`：删被 manifest 取代的旧 L1 + 已折叠 L0；cell-agent 默认开）。E2E 折叠后 `ltx/e1/` 仅剩 L1（L0=0） |
| drain token + 优雅 handoff（§7/M-07） | ✅ ADR-057（`internal/drain`；SIGTERM→drain→seal→release owners→release token；draining 拒新 claim） |
| node-log/recovery 完整化（§6/C-03） | ✅ **完整**：ADR-057（`RecoverNode`、sealed fast path、`cmd/recoververify`）+ **ADR-066 自动编排**（waker leader-only 每轮 pass 调 `recovery.Runner.Pass`：`nodelog.Nodes` 枚举、lease 判活、死亡节点 `RecoverNode`；幂等、fail-safe）。测试：`internal/recovery/runner_test.go`、`internal/waker`、`internal/nodelog` |
| 混合粒度段去重（recovery batch vs 单段） | ✅ ADR-057（`ltx.Header.ID` + Restore/compaction/PageFetcher 去重） |
| 统一定时器 + 本地派发（timers-and-dispatch） | ✅ ADR-058（`internal/timer`：cell SQLite timers/fired、token 去重、runner at-least-once、`/v1/internal/timer/upsert`）；E2E 恰好一次 + dedup |
| 单 fleet waker（死 owner 兜底） | ✅ ADR-058（`internal/waker`：`fleet/waker.json` 选举 + TTL；leader 只派发 owner 过期 scope）；E2E 唯一 leader、kill 后 TTL 内接管 |
| owner 解析库 + 非 owner 转发（ADR-003） | ✅ ADR-059：`internal/ownerclient`（resolve+TTL 缓存+forward+409 重试）；服务端非 owner 转发（commit/commit_binary/append）+ 环防护 fail-closed；`requireOwnerEpoch` 增"本地须为 owner"。E2E 向非 owner 提交 → 转发 → fleet（failures=0） |
| 控制面骨架（应用/版本/路由/密钥） | ✅ ADR-060：`internal/control`（按 app 分片 control cell、不可变版本、单事务 deploy/切指针、信封加密 secret、audit、路由投影 ETag） |
| admin 分监听 + 数据面隔离 | ✅ ADR-060：admin `:8082`（`CELLHIVE_ADMIN_TOKEN`）是操作者入口；控制写同时在**内部监听**（internal token）提供，供非 owner 节点转发给 owner（ADR-118）|
| routing 投影（纯拉取） | ✅ ADR-060：`GET /v1/control/routes` + 稳定 ETag + `if-none-match` 304（ADR-031） |
| security：scope 声明校验 / per-load scoped token | ✅ **ADR-061**：`internal/scopedtoken`（HMAC scope token）+ `control.HasBinding` + `scopeAuth` 强制 KV 端点；`RequireScope` 默认 true。其余 binding 落地时套同一中间件 |
| bundle/assets 内容寻址 + 读取路径 | ✅ ADR-062（`internal/artifacts`：`bundles/sha256/<aa>/<sha>` 去重、`assets/<ns>/<worker>/<token>/<path>`、internal presign/流式读取；CLI `bundle/asset put`、`deploy --bundle`） |
| 边缘配置下发 | ❌ **已移除（ADR-132）**：平台不提供外部代理功能；边缘由运维静态配置 |
| 审计保留/格式 | ✅ ADR-062（`AuditQuery` limit/since、`PruneAudit`、每小时后台 prune、`CELLHIVE_AUDIT_RETENTION` 默认 720h） |
| OIDC/JWT admin 鉴权 | ✅ ADR-062（`internal/auth`：StaticToken + JWTBearer(RS256 JWKS/HS256) + Any 回退；`CELLHIVE_OIDC_*`） |
| D1 binding（SQL API） | ✅ ADR-063（`internal/d1`：query/exec/batch 单事务、`?` 参数、`/v1/d1/*` + scope kind=d1） |
| R2 binding（对象 API） | ✅ ADR-063（`internal/r2`：`r2/<ns>/<bucket>/<key>`、PUT/GET(+Range)/DELETE/list；scope kind=r2） |
| Queue binding（生产者 + 消费者） | ✅ ADR-063（`internal/queue`：send+delay+幂等、claim/lease、ack、retry）；✅ **消费者派发 ADR-067**（`Version.Consumers` + `Projection.QueueTargets()` → `queue.Runner` Claim → POST `/v1/queues/dispatch` → **user-runtime workerd 加载 bundle 调 `queue()`** → Ack/Retry；`internal/userruntime` 对真实 workerd e2e 通过） |
| KV delete/list | ✅ ADR-063（`/v1/kv/delete`、`/v1/kv/list` 有界游标） |
| binding facade（host adapter） | ✅ ADR-063（`workerd/platform/facades.js`）；关键：loader env 不可含函数 → 平台注入 wrapper 模块（源码字符串 + 文本 bindings），facade 在 loaded worker 内构造 |
| user-runtime 公开入口（路由/版本/加载） | ✅ ADR-068（`workerd/user-runtime/loader.js`：投影拉取 TTL 5s、Host 路由、活跃 version bundle 加载、facade env、剥离 `x-cellhive-*`；真实 workerd e2e） |
| user-runtime 内部派发（queue/scheduled） | ✅ ADR-067（`internal.js` + wrapper `CellHiveHost` RPC；真实 workerd e2e） |
| 静态资产服务（loader 侧） | ✅ ADR-069 + **ADR-071**（`_redirects`/`_headers`/ETag/304/index/回退 worker + `not_found_handling`(SPA/404-page) + `run_worker_first`(bool/paths)；真实 workerd e2e 4 用例） |
| Cron → `scheduled()` 派发 | ✅ ADR-070（`Version.Crons` + `Projection.CronTargets()` + cell-agent `cronEnricher` + user-runtime `/v1/timers/dispatch` → `handleScheduled`；真实 workerd e2e）；解析/评估/调度在校验与运行期均已实现（`internal/cron` `Parse`/`Matches` + `Scheduler.Pass`，部署期 `ValidateCrons` → `invalid_cron`；CLI `triggers list`） |
| KV/Cron/R2 的 Cloudflare 语义边界 | ⚠️ KV 的 `{type}` 读取形态已对齐，`cacheTtl` 不产生边缘缓存；Cron 只接受 UTC 五字段数字表达式，当前分钟 best-effort 调度且不补跑；R2 `onlyIf` 条件读写不支持并显式抛错。详见 `docs/compatibility-matrix.md`。 |
| Workflows/Cron binding | ✅ **ADR-086**（Workflows：`__workflow__` cell + `env.WF` + `step.do/sleep` + 生命周期/事件）+ **ADR-070/076**（Cron：`Version.Crons` 投影 + 调度器物化 slot + `scheduled()` 派发） |

## 可观测性（ADR-146 后）

- **tracing**：`traceparent` 传播已实现（入口/wrapper/facades/cell-agent service+DO），但**没有 OTLP 导出、没有采样**；props-bound facades（KV/D1/R2/Queue，loader isolate）拿不到每请求上下文，其调用不带租户 trace；后台 queue/timer 派发默认新 trace。

## 部署产物（ADR-139 后）

- **Terraform 未做**（orchestration 由 Helm chart + kustomize base/overlay 提供，`make ci` 离线校验）。
- **云 KMS 未接**（根密钥可用 env 或文件，ADR-150）；**mTLS/内部 CA 有意不做**（独立派生 secret + 私网隔离为主防线；触发条件见 security.md）。
- **Secret 历史版本迁移**：ADR-204 升级前的 Secret 变更未按版本保存，无法追溯旧值；首次迁移以升级时的当前值初始化既有版本快照。升级后的 Secret 更新、发布和回滚按版本快照执行。

## purge / 上传 / WAL / r2.List（ADR-134 后更新）

- **删除/purge**：软删 + `purges` 作业 + `RunPurgeLoop` 已落地（ADR-131），并且 purge 只对**仍处于软删状态**的实体生效、deploy/create 会取消作业（ADR-135）；**数据侧 hook 已接线（ADR-142）**：worker 删除清该 worker 的 DO 段与 assets、app 删除清整个 ns 的 `cells/`+`assets/`，每个 cell-agent 都执行（跨节点本地副本 drain + 桶删除幂等），hook 可续跑（有界删除/轮，`done=false` 保留 pending）。**已补（ADR-169）**：数据侧改为 `objectstore.Objects.ListPage` + `bucket.PagedLister` 游标分页删除（S3 `StartAfter`；内存有界）；`Purger.delete` 边删边推进、预算用尽下一轮续跑。**残余**：每轮从目标前缀起点重扫（未持久化跨轮 cursor，冷路径）；FS dev 每页仍走树（内存有界）。
- **async bucket 上传**：已有**持久化重试队列**（ADR-143）：写前 spool（`<DATA_DIR>/upload-spool`）+ 启动/周期重放，失败计 `deferred`（不再丢，只有落盘失败+上传失败才 `dropped`），`/metrics` 暴露 spool/deferred/dropped。**已补（ADR-171）**：spool 原子写（tmp fsync → rename → 每次叶目录 fsync；父目录创建项按进程记忆化），成功 `Remove` 后也 fsync 叶目录，保证 publish 与完成删除均断电持久。
- **对象存储状态权威兼容性**：S3 `ConditionalDelete` 依赖 `DeleteObject If-Match`；忽略该头会无条件删除新 owner/lease，故启动探针失败时 `cell-agent` 直接退出。`CELLHIVE_BUCKET` 接受空值（本地）、`s3://`，以及原生追加式 `oss://` / `cos://`；其余 scheme 拒绝。原生 provider 以 `AppendObject(position)` 和 tombstone 帧提供可变权威对象的 CAS/条件删除，不以 `HEAD+PUT/DELETE` 假装原子。2026-09-22 OSS 北京桶及 COS 香港 `cell-1376795072` 桶的隔离键并发 claim、CAS/条件删除、启动诊断通过；同宿主双进程共享真实云桶的 `SIGKILL`/新目录接管与高并发数据精确核对亦通过（见 ADR-189）。原 COS 香港 `vwork-hk-1376795072` 桶 AppendObject 返回 405，仍不能作为权威桶。COS/OSS 的普通 S3 API 在所测桶不满足权威**条件 PUT CAS**：COS 接受 `If-Match`/`If-None-Match` 但忽略（2026-10-22 复测），OSS 返回 `NotImplemented`；COS 的 `DeleteObject If-Match` 可用，OSS 的会被忽略（ADR-188）。其他供应商/桶仍须分别验收；跨主机分区及长时混沌未测。
- **权威对象无限追加（ADR-266 已修）**：provider 上 `owner.json`/`owner-gen`/`nodes/*.json`/`node-logs/*`/`fleet/waker.json` 原先是永久 append-only 单对象；默认 10s lease、每 TTL/3 续约，单 cell owner 日志约 4.4MB/天、节点 lease 约 9.7MB/天、waker 约 2MB/天，且 `Get`/`CAS` 全量读历史。现固定按 1h 窗口分段为 `.w<window>`（不新增环境变量），`CHL2` tail 读取，token 带 window+instance+length，rollover 建新段并 GC 旧段，单段硬上限固定 8MiB 失败关闭；legacy CHL1 大对象在第一次写时迁移并物理删除。单测覆盖轮转/GC/legacy 迁移/长空闲恢复/硬上限/并发创建；真实 COS/OSS 的 suffix-range 与混合版本边界仍需实桶验收（混合版本不安全，须全量替换）。
- **`r2.List`**：已 cursor 分页 + 尺寸化（ADR-145）：`bucket.PagedLister`（S3 `StartAfter`+`MaxKeys`、FS 有界选择），`/v1/r2/list` 返回 `truncated`/`cursor`，facade 透传。**已补（ADR-168）**：`include=httpMetadata,customMetadata`（显式才逐对象读 sidecar）与 `delimiter`/`delimitedPrefixes`（扫描上限 10000，超出 `truncated`+cursor）。**残余**：FS 后端算 etag 仍读对象体（dev 后端）；仍未实现 object versioning/SSE-C。

## 删除/清理（ADR-131/142，已实现）

删除是**两阶段**的：请求路径只做控制面软删（撤销 + 审计），数据清理由幂等、可重跑、可分页的 purge 循环完成：

1. `DELETE /v1/control/worker|app` 在同一事务里软删并 `enqueuePurgeTx` 写 `purges(ns, worker, state, requested_ms, attempts, last_error)`；worker/app 立刻不可路由。
2. cell-agent 的 `RunPurgeLoop`（默认 5s tick、每轮 `PendingPurges` 最多 20）调用 `Purger.Run(ns, worker)`：`MaxDeletes`（默认 500）**有界**地清桶前缀 `cells/<ns>/`、`assets/<ns>/`；`worker==""`（app 级）清**全部资源 + DO 存储**，否则只清该 worker 的 **DO 存储（`cells/<ns>/__do__/…`）与其 assets**（KV/D1/R2/Queue 资源跨 worker 存活，同 CF）。未清完返回 `done=false`，job 保留下一 tick 续跑；失败 `FailPurge` 记 attempts/last_error 后重试；完成 `FinishPurge` 删除 job + 审计。
3. 重新 deploy 会在同一事务里**取消 pending purge**，且 purge 通过时会跳过已被 `resurrected` 的 worker/app（`TestPurgeSkipsResurrectedWorker`/`App`）。
4. 本地副本由各 owner 处置（`LocalCells.DeleteNamespace`/`ForgetPrefix`）。

测试：`internal/purge TestAppPurgeDropsNamespaceData`/`TestWorkerPurgeDropsOnlyThatWorker`、`internal/control TestPurgeSkipsResurrectedWorker`/`App`/`TestRunPurgeLoopResumableHook`、`internal/server TestWorkerDeletePurgesAssets`。

---

## P5 运维/加固（ADR-088）

- 协议版本化（reader-before-writer）、诊断增强、矩阵回归守卫、压测/混沌工具均已有（ADR-088；恢复混沌见 ADR-057/066）。**边界**：真实多主机混沌、云供应商矩阵回归需环境（C 类）。
- **对象 GC（ADR-110/111）**：`internal/objgc` 两阶段标记 + 宽限期，回收未被任何版本引用的 **bundle** 与 **assets 版本**；admin `POST /v1/control/gc/{bundles,assets}` / CLI `cellhive gc bundles|assets`；cell-agent 后台循环（bundle+assets）默认关。

## P4 多租户/伸缩（ADR-087）

- 发布日志/幂等部署/Admission/Autoscaler **信号** 已实现并单测（ADR-087）。
- **边界**：autoscaler 只给信号，实际增删节点属外部编排（无 orchestrator/云驱动）→ C 类环境验证；admin 后台=admin API（无 Web UI）。

## P2 Queue 兼容边界（Partial）

Queue 生产/消费主链路已实现；`sendBatch()`、`json/text/bytes/v8`、128 KiB 上限、metadata、原始 timestamp、delay 校验和幂等冲突已补齐。剩余差异是跨批顺序以及部分 retry/DLQ 边界；`max_batch_timeout` 已接入并做真实服务回归，继续按 [`compatibility-matrix.md`](../compatibility-matrix.md#queue--workflow-的调用边界) 验收。

## P2 Workflows 绑定（ADR-086，Partial）

**已实现（真实 workerd e2e）**：`internal/workflow`（`__workflow__` cell：instances/steps/events，step 记忆化）；facade `env.WF.create/createBatch/get/sendEvent`；cell-agent API（租户 create/create-batch/get/event，内部 step/sleep/finish）；user-runtime `/v1/workflows/run` + `cellhive-workflow.js` base + `step.do/sleep`（shim：改写 `cloudflare:workers`，平台自构造，因 workerd 的 `WorkflowEntrypoint` 引擎外不可构造）；sleep 复用统一 timer（`KindWorkflowSleep`）；CLI `workflow create`；`wranglercompat` 要求 `class_name`。create/createBatch 请求总 payload 上限 1 MiB，batch 为 1..100 个实例。

**Partial 边界（未做）**：跨 worker 实例；`locationHint` 被接受但**忽略**（best-effort 放置提示，无效果）；step 历史列举与 progress 回调；实例恢复/重派发使用当前活跃 worker 版本，未固定创建版本。
**调用形态差异**：`sleep/sleepUntil` 仅数字毫秒、`waitForEvent.timeout` 仅数字秒数且超时返回 `undefined`；`step.do timeout/sensitive`、`restart({from})`、`terminate(options)` 未生效。详见 [`compatibility-matrix.md`](../compatibility-matrix.md#queue--workflow-的调用边界)。
**已实现**：`pause`/`resume`/`terminate`/`restart`（`internal/server/workflow.go` + facade）、`waitForEvent`（`handleWorkflowWait` + `env.WF.sendEvent`）、实例列举（`handleWorkflowList` + `env.WF.list`）、`instance.delete()`、每步 `retries{limit,delay,backoff}`、`createBatch` 与 payload 预算。

## backend-A 捕获与冷恢复（ADR-092）

原先 KV/D1/Queue/Workflow 只写本地 cellstore、**未捕获**。现已全量接线：`internal/cellcapture` + `commitSegmentCore`；`capturedWrite` 覆盖 **KV/D1/Queue/Workflow/timer/control**。并补齐：
- **冷恢复**：`cellstore.Store.Hydrate` + `replica.LatestEpoch` — 新 owner 首次打开 cell 本地无文件时从桶 `replica.Restore` + `restore.ApplyFile`（此前只有 do-supervisor 会恢复；cell-agent 会空盘起并 `binding_not_registered`）。
- **owner 续约**：cell-agent 按 TTL/3 续约 `OwnedScopes`（此前不续约，租约过期即失所有权、捕获停、写 503）。
- **D1 txid 对齐**：`cellstore.Cell.Tx` 让 D1 写与 KV 一样在同一事务推进 `cell_meta.txid`（此前 D1 不推进 → `Wait` 立即返回 → acked 但未持久化）。

测试：`internal/cellcapture`、`TestKVCaptureReplicatesAndRestores`、`TestKVCaptureConsistencyN`、`TestD1CaptureConsistency`、`TestLatestEpoch`、`TestHydrateOnFirstOpen`；live：kill owner → 接管读回全部 acked key。

## 基准环境限制（非缺陷，需知）

- 本地 **FS bucket 用单个全局 mutex**（`internal/bucket/fsbucket.go`）串行化 `Put/CAS/List`，且与 SQLite 同盘 → **capture ON 时多 cell 不叠加**（FS 单 ~5.4k、双 ~8k 峰值≈1.47x；本地 MinIO 双 cell 无增益）。生产用真实分布式对象存储 + 分盘才可能近线性。
- 上传层已按 scope 分片（`upload.NewSharded`，保序），但 sink 并行度不足时分片无益。
- 单 cell 写上限受"每写一次提交证明往返"限制；吞吐受单写者与"每写一次证明"限制（峰值 CPU 仅 ~2/8 核）。

## P3 DO 持久性与跨节点缺口清单（ADR-083/084）

| 项 | 状态 |
|---|---|
| facet 发现（`.facets`） | ✅ 已实测解析（ADR-084）：magic `57efb0c55bcecdc4` + `[flag][len][name]`；条目 i↔`<hosthash>.<i+1>.sqlite`（双 facet probe 验证）。`ParseFacets`/`Supervisor.Facets()`/status/manifest 均含 |
| 路径化 WAL 捕获（任意 SQLite 路径） | ✅ `internal/wal` cursor + `snapshot`/`delta`（ADR-083） |
| 输出门（RPO=0） | ✅ `POST /internal/do/gate`；失败/超时 `result_unknown`（ADR-083，真实 workerd） |
| 跨节点冷激活 + 按需分页 | ✅ `RestoreAll` + `CompactAll` + `PageFetcher.Materialize`；双 runtime e2e + `TestPagedRestoreUsesRangedReads`（ADR-084） |
| 进程级接管恢复 | ✅ `TestDoRuntimeTakeoverAfterCrash`：A 崩溃（`exec.CommandContext` → SIGKILL workerd）→ 租约过期 → B 从 bucket 恢复接管（计数 1→2） |
| WebSocket 跨节点转发 | ✅ `proxyConnect`（host.js 代理到 owner + 1012 透传）；`TestDoRuntimeWebSocketCrossNodeForward`（B→A，abort→`CLOSE:1012`） |
| `deleteAll()` | ✅ **shim（ADR-079 扩展）**：stock workerd 对 SQLite-backed facet 调 `deleteAll()` 抛内部错误（`expected parent == kj::none`）；`cellhive-do.js` 改为清空全部 KV + `DROP` 用户 SQL 表（保留 `_cf_*`/`sqlite_*`）+ 清 alarm。`TestDoRuntimeDeleteAllCaptured`、`TestDoRuntimeDeleteAllSQLCaptured`（含冷启动恢复后为空） |
| 运行期 SQLite VFS 懒读 | ✅ **cell-agent 侧已实现（ADR-160）**：`internal/pagedvfs` fault-in VFS（稀疏 cut + hydration 位图 + 同步 fault + IOERR fail-closed）+ `replica.PageFetcher` 页源；`CELLHIVE_PAGED_RESTORE`（默认开）/`_MIN_BYTES`（默认 256MiB，小于则整克隆）/`_HYDRATE_MBPS`（默认 16，0=保持稀疏）；`SnapshotPages` 前强制 `HydrateAll`；**b-tree 感知 fault 策略**（内部页 child 单页取、顺序 child 按 `scanAhead`=64 预取到内存缓存、连续 child 合并成 run；顺序访问才开窗）；**缓存标记位** `<cell>.db.paged`（有标记=稀疏缓存必须经 paged VFS；cut 不匹配/无页源则丢弃重建；全量 hydrate 或后台补齐完成即删标记——修复了"重启/驱逐后用 base VFS 打开稀疏文件读到零页"的 bug）；**批量 hydrate**（`ReadRun`）；**LZ4 逐帧压缩**（`WAL3` page-map v2 + `CID2` 索引；L1 3.1MB→140KB/真栈 315KB→86KB；解码 1.1GB/s）；**child 并发预取**（`CELLHIVE_PAGED_PREFETCH_WORKERS`=4）；**`/metrics`** 六个 `cellhive_paged_*` 指标；**稀疏感知磁盘口径**（`AllocBytes`/`DiskBytes`）。`WAL2`/`CIDX` 向后兼容可读；delta（`WAL1`）不压缩（逐笔小段收益低）；LZ4 编码单线程 ~47MB/s（3MB 快照 ~70ms，冷路径）；**GC 与压缩无关**（manifest key + txid 水位；混格式链可折叠回收，真栈 L0 归零），但 `MinBytes` 现在按压缩后字节计（更贴近 IO 预算），`EncodeSnapshotParts` 分片预算仍按 v1 估算（分片偏小，安全）。实测：点读 8/771 页、全扫 336 页 = 6 窗口读；真栈冷请求 123ms、`cells=1 faults=72 runs=9 prefetch_hits=3 hydrated=12/77`。⛔ **DO 侧边界（ADR-085，待 FUSE）**：不可实现于 stock workerd（需 FUSE 或改 workerd）；替代=冷启动按对象/按页 materialize（`RestoreObject` + `PageFetcher.Materialize` range 读）。**修订（ADR-159/160）**：cell-agent 侧已实现运行期 paged VFS（`internal/pagedvfs`，ADR-160）；捕获/快照前强制 `HydrateAll`，避免零页静默损坏 |
| DO 内 bindings | ✅ **已修（ADR-089）**：facet `this.env` 注入 worker bindings（KV/D1/R2/Queue/Workflow/DO）；`TestDoRuntimeBindingInsideDO`。边界：经 `this.env`（构造参数 `env` 为 host env） |
| DO 兼容套件 | ✅ `TestDOCompatSuite` 统一入口，19 真实 workerd 子测试（sync SQL/事务、alarm、WS 1012、migrations、注册/删除、转发/`result_unknown`、门、冷激活、接管、resident/evictable） |
| 无桶凭据恢复 | ✅ `HTTPStore` 经 cell-agent `/v1/internal/segments`+`/segment`(Range 206)+`/blob`；`TestRestoreViaAgentNoBucketCreds`、`TestAgentPagedRestoreUsesRangedReads`、`TestInternalBlobRoundTrip`、`TestReadSegmentRange` |
| gate 热路径 blob 写 | ✅ **已优化**：manifest/sidecar **内容哈希去重**（`blobUnchanged`），稳态零桶 PUT（`TestSyncAllSkipsUnchangedBlobs`）；`refreshFacets` 改为**增量缓存**（目录 mtime 发现新增 + `.facets` (mtime,size) 检测内容变化，稳态仅几次 stat，不再每请求全目录 walk；`TestRefreshFacetsPicksUpNewFacet`） |
| HTTPCommitter 证明过窄 | ✅ **已修**：提交证明接受 `fleet`/`bucket`/`bucket-batch`（RPO=0），拒绝 `bucket-async`（RPO>0）与未知；`TestHTTPCommitterProofModes`。纯 bucket 部署不再 gate 失败 |
| **对象键 `storage_id/<class>/<objectName>`** | ✅ **已解（ADR-084，spike 反转）**：hosthash **可从 DO 内部确定得到**——`this.ctx.id.toString()` = `<hosthash>`，`this.ctx.id.name` = hostId（含 storage_id）。host.js 双上报（router: host_id→storage_id/class；actor: host_id→host_hash）→ supervisor `/internal/do/bind`；facet 文件按对象 scope `workerd/<class>/<base64url(storage_id/class/objectName)>` 复制。`TestObjectScopesAndRestoreObject` + `TestDoRuntimePerObjectColdStart` 验证 |
| 按对象粒度恢复 | ✅ `Supervisor.RestoreObject`（facet 文件 + host actor + `.facets`，分页读）；`TestDoRuntimePerObjectColdStart` 只恢复该对象 → 状态延续 |

| Vectorize 的两种查询模式（ADR-159） | ✅ **默认 flat 精确**（20k×256/K=10 ≈ 6.3ms，比旧 Go KNN 快 ~8×）；`vectorize rebuild` 建 **vec1 IVFADC/OPQ** ANN 后 ≈ 0.22ms（≈210×），但 ANN 是近似（需代表性训练数据并调 `nprobe`/`codesize`；官方 1M×128d recall@10 ≈ 0.92）。训练是显式算子动作，OPQ 训练较慢（本机 20k 约数秒，nthread≤16） |
| Vectorize 的 dot-product 与过滤语义（ADR-159） | ⚠️ **边界**：`dot-product` 指标不支持（vec1 只有 l2/cos，创建时报 `vectorize_bad_config`）；namespace 下推到索引，其余 metadata 过滤是**后置过滤**（4× 过采样，极端过滤可能少于 topK；CF 是先过滤）；vec1 无 partition key（namespace 借用 metadata 列） |
| vec1 v0.7 的 rowid UPDATE 段错误（ADR-159） | ✅ **已规避**：对 `vec` 的 rowid-targeted `UPDATE` 会让 v0.7 崩溃（store 单测发现），因此 upsert = `DELETE` + `INSERT`（同一事务、语义等价）；升级 vec1 后可再评估 |
| Vectorize 的 CF 差异（ADR-158） | ⚠️ **超集/边界**：metadata index **不强制**（CF 要先建 metadata index 才能过滤；我们允许直接过滤完整 metadata，且未模拟 indexed string 的 64B 截断）；`describe`/`insert` 返回 `{mutationId,count,ids}`（CF 只保证 `mutationId`）；`--deprecated-v1` 无 V1 语义（按 V2 处理）；批量上限 1000；`listVectors` 是扩展（CF 仅 wrangler） |
| L1 快照分片预算保守（ADR-160） | ✅ **有意接受，不做**：`EncodeSnapshotParts` 按 v1「每页 4KiB」估算 → 分片偏小（安全）。注意**分片本身是 CellHive 的 HTTP `maxSegmentBytes` 传输约束**（LTX 是"一文件一对象、无分片概念"，对象数量靠 compaction level 控）；按实际压缩帧字节精确分片只在"大且高压缩"的快照上少几个对象，收益小、不值得加复杂度 |
| 租户 console 日志（ADR-185） | ✅ **已实现**：pinned stock `workerd 1.20260916.1` 接受动态 Worker 的真实 `Fetcher`：宿主用 `ctx.exports.TenantTail({props})` 固定 `namespace/worker/version`，捕获 Worker、service target 与 DO facet 的 `console.*`/未捕获异常并写入 ring/可选 OTLP。旧失败只因误传 capnp 风格 `{name:"tenant-tail"}`。log token/URL/private transport 仅在可信宿主，不进入 tenant env；投递仍是 best-effort。 |
| Wrangler Tail 与结构化 OTLP（ADR-214/215/217/225） | ⚠️ **部分验收**：调用事件保留 outcome、exceptions、版本和 `event_type`；OTLP 导出原始参数、关联 ID，且无 console 的调用仍有一条记录。版本化 Worker 日志策略已通过真实 workerd 的同 App 双 Worker console 隔离及 Queue/Cron 调用记录关闭测试；真实 Collector→OpenObserve 已查询到 HTTP、Queue、Cron、Workflow 的关联记录。原版 Wrangler 4.133.0 已在双 cell-agent 上订阅另一节点的四类事件，长连接无意外重连；主动重启订阅节点后 Wrangler 重连并收到新日志。Queue/Cron 的 CF 载荷已由可信派发包装层补齐并通过双节点原版 Wrangler/原始 WebSocket 验收；真实 COS 跨 AZ 故障仍未验收。 |
| workerd 运行时基线（ADR-186） | ✅ **第一阶段已验收**：pin `1.20260916.1`、esbuild `0.28.2`、宿主 `fromEnvironment`、生成式 compatibility manifest、最终 WorkerCode 64 MiB 与 env 1016 KiB 双层预算均已落地；隔离 Docker E2E 验证源码打包、env 隔离、KV、gated DO 桶持久化与重启续数，`REQUIRE_ALL=1` 为 **GATE: PASS 14/14**。原生 Tail 已通过真实 Fetcher 接线；兼容多模块 artifact 仍是独立阶段。 |
| OTLP 追踪覆盖（ADR-167） | ⚠️ **边界**：**DO 内 `this.env.<binding>`（props-bound）调用不带 DO traceparent、自成一条 trace**（workerd 三个 isolate 各自 globalThis + CF 绑定 API 无按请求参数；`TestDoRuntimeBindingInsideDO` 断言当前为空）；**`peer.append` 是捕获流水线按批的独立 trace**（group commit 合并多请求，无法归属单一 trace）；租户 isolate `facades.js` 回退路径无 JS span（服务端 span 仍能挂上）；DO `/v1/do/connect`·abort、compaction/upload 后台循环无 span；JS span 毫秒精度；入口头部采样；端点空时 no-op |
| peer 复制冗余（ADR-164） | ⚠️ **默认语义**：adaptive hedge 下常态是 **owner 本地 + 1 个 follower**（慢时才补第 2 份），`CELLHIVE_PEER_HEDGE_MS=0` 永远单份；要"始终 2 份 follower"需显式接受常态双发（当前不提供该模式）。单节点故障可恢复（owner 死→从 follower spool `/v1/peer/held` 恢复；follower 死→owner 本地段后台上传），同时坏 owner+follower 才丢；一致性不受影响（单写者/顺序/epoch/quorum-1 不变） |
| R2 对象字段与 metadata（ADR-163） | ⚠️ **边界**：`get()` 的 `R2ObjectBody` 数据字段与 `text/json/arrayBuffer/blob` 都可用（workerd RPC 按值序列化数据、把函数序列化成可调用 stub；binding 侧方法需跨 RPC，本地 facade 方法保持 non-enumerable；JSON.stringify 会跳过函数）；`bodyUsed` 在 props-bound RPC 路径读取后**不翻转**（快照值）；`list()` 默认只回 `key/size/etag/httpEtag/version`，**不逐对象回填** http/custom metadata/uploaded/checksums；显式 `list({include:["httpMetadata","customMetadata"]})` 才按需逐对象读 sidecar（ADR-168）；`get()` 多一次 metadata sidecar 读；`put` 的 metadata 存 `r2meta/<ns>/<bucket>/<key>` sidecar（已登记保留前缀），删除对象会一并删 sidecar；`md5/sha256` 提供时校验 |
| R2 `writeHttpMetadata`（ADR-174→ADR-175 已修） | ✅ **已解决**：`loader.js`/`buildFacetEnv` 经模块作用域常量 `__cellhivePlatform.r2Bindings` 传 R2 绑定名（不占 env 名），`queue-wrapper`/`bindings-wrapper` 对每个 R2 binding 套本地 Proxy（`facades.js wrapR2Metadata`），`writeHttpMetadata` 在租户 isolate 改调用方 `Headers`，content-type 正常；仅 `get()` 被代理，其余方法透传 |
| KV 大值（ADR-176 记录） | ⚠️ **缺口**：value ≤25MiB 目前**全量内联**在 cell 的 SQLite，并随 LTX 捕获/复制走一遍（对齐做法是 >1MiB 转 fleet bucket blob `v2:e<epoch>:<digest>`，同一个 alarm 里做 GC；实测 1MiB 是延迟交叉点）。大值 `put` 的代价随值大小线性上涨；正确性不受影响。对齐需要"小值内联/大值 blob + epoch 引用 + GC"的完整链路 |
| wake 索引每轮全量扫描（ADR-267 已修） | ✅ **已解决**：`wake/due/<minute>/<scope>.json` + `wake/ptr/<scope>.json`，waker 用 `CommonPrefixes("wake/due/")` 只打开到期分钟；leader 每 10 分钟有界清理孤儿 entry；启动 `Migrate` 一次迁移旧 `wake/<scope>.json`；`Due` 只扫新布局。对象数每 scope 2 个，不再随 timer 数量做全量 Get。剩余：`CommonPrefixes` 在未知 backend 回退全量 List（正确但慢） |
| wake 索引落后于 timer（ADR-177 已修） | ✅ **已解决**：`Store.Upsert` 先发布索引再提交（索引不落后，失败则 fail-closed）；`registerTimerScope`/认领时 `SyncIndex` 重建；`CELLHIVE_WAKE_REPAIR_INTERVAL` 有界轮转修复本地 cell（只读扫描，稀疏 paged 未挂载跳过）。剩余：修复只覆盖本节点本地 cell（死节点盘不可达，靠新 owner 认领时重建） |
| DO stub 上的 WebSocket 升级（ADR-174→ADR-175 已修） | ✅ **已解决（ADR-184 后无需租户传输）**：带 DO binding 的公开 loader→`CellHiveHost.fetch(request)` 与租户 facade→平台侧 `DurableObjectNamespace.fetch(request)` 两个 workerLoader 边界都保持 fetch 形 RPC；无 binding Worker 按 ADR-193 直载默认 fetch，仍为 fetch-shaped RPC。后者给 Request 加 `DO_ID_HEADER`，再经 cell-agent scoped lookup 取 `{owner,ticket}` 并直连 owner do-runtime。`TestTenantDoWebSocketPassesThroughPublicLoader` 验证外部客户端 101 + 双向帧，`TestTenantDoWebSocketCrossesRpc` 验证 tenant 内部消费 socket；Docker gated runtime 验证重启前后公开 WS 与持久计数。`CH_DO_CONNECT`/`CELLHIVE_CAP_WS` 保持删除。 |
| DO RPC 的值与上限（ADR-162） | ⚠️ **边界**：支持 `undefined`/`-0`/NaN/±Inf/bigint/Date/RegExp/Map/Set/ArrayBuffer/TypedArray/DataView/Error(含 cause)/URL/URLSearchParams + 共享引用/环；**不支持** function/Symbol/Promise/弱集合/流/`RpcTarget`/DO stub；类实例降级普通对象；args 与结果各 ≤ **8 MiB**（tagged 编码后，二进制 base64 约 ×4/3）；方法名必须标识符且禁 `fetch`/`alarm`/`constructor`/`__proto__`/`then`/`toString` 等与 `__ch*`；跨进程无 traceparent（仅 requestId） |
| SQLite 现在是 CGo + 构建 tags（ADR-159） | ⚠️ **必须知道**：`make build/test`（或 `-tags "sqlite_fts5 sqlite_dbstat sqlite_math_functions sqlite_column_metadata sqlite_preupdate_hook"`）不可省——漏加会在 `internal/cellstore` 触发 `#error` 守卫；二进制含 AVX2 代码（`-march=x86-64-v3`），非 AVX2 CPU 用 `-tags cellhive_vec1_portable`（标量，较慢），arm64 走 NEON |
| Vectorize 绑定面的鉴权层级（ADR-158） | ✅ **已对齐**：`/v1/vectorize/*` 绑定端点与 KV/D1/R2/Queue 一样**只需 scoped token**（不带 internal token）；写操作 `forwardOrClaim`、读 `forwardRead`。容器 smoke 曾发现误包 `s.auth` 导致 401，已修 + 回归测试 |
| Vectorize 在 dev 不可用（ADR-158） | ⚠️ **边界**：Miniflare 只解析 `vectorize` 绑定（workerd 无该服务），`cellhive dev` 遇到它会明确报错退出；本地调试用 `cellhive vectorize` 打真实命名空间 |
| R2 stats 是估算用量或诊断性近似（ADR-157/234） | ⚠️ **有意**：默认返回启用后持久跟踪的对象数/字节数，不回填旧对象，故障或并发可造成漂移，不用于计费；`?scan=1`/`prefix`/`limit` 才走有界 List（默认 1000/上限 10000），大桶给 `truncated`+`cursor`，并计入 `cellhive_list_calls_total` |
| Wrangler D1/KV/R2 数据面（ADR-197） | ⚠️ **固定版本边界**：原版 Wrangler 4.133.0 的远端 D1 query/file import/migrations、KV key、R2 object 已真实验收；自定义 `migrations_table` 可由 Wrangler 配置或原生 CLI `--table` 指定。D1 文件导入限 8 MiB，上传 session 是单管理员实例内存态（需同实例路由）；R2 jurisdiction/storage class 显式拒绝。生产资源仍须先登记。 |
| Wrangler 批量命令与 D1 export（ADR-198） | ⚠️ **已验收的边界**：D1 导出限 32 MiB，虚拟表拒绝，下载链接为单管理员实例内存态（一小时）；KV bulk 逐 key 提交，后续错误可能留下前面已写入的 key；Secret bulk 最多 100 项并在控制 cell 内原子提交。 |
| 原生 `types`（ADR-199） | ⚠️ **生成范围**：只按 JSONC/JSON 配置生成 Env 接口，变量统一为 `string`；完整运行时类型和 TOML 类型生成仍由原版 Wrangler 提供。 |
| 原生 `init`（ADR-200） | ⚠️ **模板范围**：只生成可部署的最小 JS Worker；原版 Wrangler 的交互式向导、框架模板和 Dashboard 下载由原版命令处理。 |
| 行数只能是估计（ADR-157） | ⚠️ **SQLite 无行数元数据**：默认给 dbstat 叶页 `ncell` 估计（KV `rows_estimate`、D1 每表、workflow `instances_estimate`）；精确计数须 `?exact=1`（`count(*)`，KV 走最小索引/index-only，仍是 O(rows)）。不提供"永远精确且永远便宜"的选项（`cell_meta` 计数器方案未采纳） |
| Workflow `by_status`/DO stats 的前提（ADR-157） | ⚠️ **边界**：workflow 状态分解只在 `?exact=1` 时给（未加 `instances(status)` 索引，避免写放大）；DO stats 只在对象索引开启（`CELLHIVE_DO_OBJECT_INDEX`，ADR-109）时有数据，否则 `available:false` |
| `resource delete` 语义边界（ADR-156） | ⚠️ **有意**：只吊销**登记**（墓碑 fail-closed）并**不删**数据 cell（KV 条目/队列消息/DO SQLite 仍在）——vwork 的"关停"不是"销毁"；彻底回收需删除对应 cell（当前无运维端点）。`replay-dlq` 为**至少一次**（主队列入队成功后 DLQ ack 失败会重复重放），且要求队列已被 producer binding 登记并声明了 `dead_letter_queue` |
| 裸 `do-runtime` SIGTERM drain（ADR-156） | ✅ **已解决**：信号先触发本地 `/v1/do/drain`，host actor 停新请求、等待 in-flight 并释放租约；drain 返回或 15s 超时后才取消 workerd。生产仍推荐 `do-runtime-gated`，因为 supervisor 还负责 WAL 捕获与输出门。 |
| Kubernetes 多 Pod 验收（ADR-194） | ⚠️ **已验证范围/边界**：真实单节点 k3d 中 2 个 gated Pod、owner 删除冷恢复与 RollingUpdate 期间 120 个逻辑写通过，计数精确 `1→2→123`；ownership 切换时观察到短暂 5xx，客户端重试后无丢写/重复提交，因此不等于物理请求零错误。真实跨主机 RTT、云对象存储、多主机混沌以及目标 NetworkPolicy/CNI 仍属 C 类环境验证。 |

| 入口 `/ready` 的覆盖范围（ADR-156） | ⚠️ **边界**：描述的是**入口/DO 宿主**的就绪（投影新鲜度、排空状态），不是租户 worker 内部健康；props-bound facades（KV/D1/R2/Queue 在 loader isolate）没有独立探针 |

**含义（2026-09-15 更新）**：P3.7/P3.8 的交付（facet 发现、对象键、按对象恢复、门、跨节点冷激活/接管、compat 套件）**均已实现并验证**。P3 各项均已实现/定稿（WS 跨节点转发、`transferred_classes` 同 worker、运行期 VFS 懒读见 ADR-160、`refreshFacets` 增量缓存）；仅剩 **C 类环境验证**（真实跨主机 RTT、云端对象存储、多主机混沌/接管）与跨 worker `transferred_classes`（有意拒绝）。

## 静态/动态混合托管（ADR-240 ~ ADR-248）

- 静态代只覆盖无 assets、非 path 挂载的 host；path 路由、assets 与 pattern host 继续走动态 loader。
- 静态代一个 workerd 进程承载该代全部静态 worker；部署/热集变化会重建整代，warm isolate 会丢失一次。
- 静态快路径不挂 Tail：有活跃 tail 订阅的 host 会回落动态（Tail 仍可用，但该 host 回到 ~3k）；订阅过期后自动回静态。跨 worker service binding 走 cell-agent HTTP，延迟高于动态同实例路径。
- 静态 service binding 需要 cell-agent 配置 `CELLHIVE_DISPATCH_URL`，否则返回 503 no_dispatch。
- 多节点 tenant 的 `CELL_URL` 必须指向稳定入口；控制面 owner 故障切换受 `CELLHIVE_LEASE_TTL`（默认 10s）限制。
- Hyperdrive 已用最小 PostgreSQL v3 协议客户端做真实查询；租户侧驱动/连接池仍需自行验证。
- Queue producer + consumer、并发批量、retry→DLQ 已 e2e；更长时间/大批量压测尚未覆盖。
- 热集/Plan 刷新默认 30s；部署后的短暂窗口内可能继续服务旧静态版本（ingress 静态失败会回落动态）。
- ingress 运维端点（`CELLHIVE_INGRESS_ADMIN_PORT`）无鉴权，应只暴露在私网。

## ADR-285 session 恢复本地修复与验证

v3 follower 身份、启动恢复门和按 session 退避重试已实现。真实双进程换地址重启、恢复中强杀、同名空盘拒绝后换回原盘通过，80 个已确认值全部读回；全量测试、vet/build 和相关包 race 检查通过。USW 已部署至 revision 42，19 个历史 session 完整恢复封存。SG 的旧 IP session `1792144743099984471` 已完整校验并持久恢复 22395 条、封存；但另两份历史日志在现有 PVC 上缺失，其余积压尚待恢复，SG 因此保持旧版、暂缓升级。新旧 recovery 协议禁止混跑，旧 URL 记录必须先恢复可达性并完整封存；否则新版保持恢复模式。验证与部署约束见 [本地记录](bench/session-recovery-2026-10-24.md) 和 [部署记录](bench/session-recovery-rollout-2026-10-24.md)。

- 2026-10-24 SG legacy migration: two explicitly authorized missing-tail sessions may be closed with audited `recovery_resolution`; this is accepted unknown historical loss, not RPO=0 recovery. TailPage byte-budget false truncation is fixed and covered by regression tests; see `cell-protocol.md` and `archive/bench/session-recovery-rollout-2026-10-24.md`.

- 2026-10-24 pinned control takeover fix: USW returned binding_not_registered after owner change because Pin suppressed stale-file invalidation (local txid 4516 vs bucket 4518). Ownership invalidation now bypasses residency Pin and refuses a busy stale cell. The old binary fails the retained-control-DB process E2E; the fixed binary restores registrations and all 80 KV values.

## 2026-10-26 稳定性复查与修复状态

ADR-296已在源码修复peer握手/Write取消失效、建连持全局锁、异步ACK无限占槽；增加5秒连接/写入/ACK停滞边界、按lane合并建连及共享失败，保留调用取消与整条lane退役的区别。DO readiness已增加限频分类诊断和恢复日志。真实HTTP桶回退→fleet恢复、workerd后端故障恢复、race和session恢复验证见 [稳定性与性能review及修复记录](stability-performance-review-2026-10-26.md)。后续ADR-297已复现并修复DO后台日志配置GET无超时导致共享HTTP客户端保留旧地址的问题，同样修复user-runtime两入口，见 [DO地址恢复实验](do-address-recovery-2026-10-26.md)。已发布（见上方 ADR-296/297 条目）；这证明了可导致USW同症状的机制，但不是历史事件唯一归因。headless业务入口包含未就绪节点及fleet上传队列满同步等桶仍为剩余边界。

- Wrangler 暂存版本（ADR-301，2026-10-26）：官方 versions upload、versions secret put/delete/bulk 和单版本 100% deployment 已实现暂存/激活分离；暂存要求已存在且有效的 namespace，拒绝 DO migrations。settings PATCH、JSON vars 和内嵌 secret_text binding 尚不支持，明确报错。Wrangler 4.133.0 普通 deploy 实测成功，但环境标签同步会警告且版本号输出为 null（内部数字版本 ID 与 CLI UUID 格式不一致）；以 versions API/active readback 为准。

## 2026-10-27 授权刷新及退出续租（ADR-303）

本地 control owner 的授权刷新重复 COS 点查已改为同 session 健康租约下的读前/读后证明；Release 并发及失败均失效。退出期间保持续租直至 drain/seal/release；旧 session Renew/Release 拒绝。节点发现增加有界并发，取消不发布残缺快照。相关测试/race/vet及真实双 agent HK COS 153960 次请求通过，两个进程均完成 handoff。已随 49133b1 发布。

剩余：远端 control owner route 到期仍需 COS 查询，真实转发 PUT 出现510ms尾延迟（owner处理18ms，binding等待369ms）；高并发Worker GET仍有168ms尾延迟。本次没有移除权威查询或放宽撤销/租约边界。见 [验证记录](control-owner-proof-2026-10-27.md)。

## 2026-10-27 全量 E2E 新发现及修复进度

本轮全量回归不能判为全绿：冷 cell 并发 Queue/send 与 control/deploy 可返回 `local_invalidation_failed` 503；并发 control 初始化还复现 `duplicate column name: compat_date` 400。DO 并发存储请求复测有 HTTP500（内部 `result_unknown/gate_500`），尚未确定 gate 内部原因，不能据此推断已确认数据丢失。官方 Wrangler 多节点第二次发布在 versions 上传阶段 `invalid_multipart`，阻塞 tail/reconnect/failover/reconciliation 后续断言。文件桶 restart/cold 首次成功约1.8s，超过脚本1s门槛，但该轮数据完整性全部通过。真实HK COS restart/cold首次成功分别约3.1s/4.3s，同样超时；最终140个校验key完整。冷写日志另有已删除历史Workflow scope的后台restore无snapshot警告，待定根因。详细证据和最终执行矩阵见 [全量E2E记录](all-e2e-2026-10-27.md)。本轮为诊断记录；相关修复随后续提交发布。

后续修复：control 迁移串行化、入请求门前原子失效/claim、Wrangler inherit binding、binding 冷路径主动接管；文件桶 restart/cold 已降至47/89ms并通过完整性校验。DO gate根因为并发读取WAL时瞬时checksum失败，捕获观察有界重读并保留checkpoint，持续损坏仍拒绝确认；两轮60秒压测12262/16900请求均无HTTP错误，真实损坏WAL回归及race通过。Wrangler全部6种模式通过，含重连/Cron接管/部署响应丢失与提交前后强杀。原10个失败入口9个已复测通过；HK COS默认配置仍为1.09/2.38秒，恢复并发16的对照仍未达1秒，不宣称性能门槛全部解决；生产默认配置未改。后续证据见 [修复记录](e2e-fixes-2026-10-27.md)。以上修复已随 48216e7 发布。

### 2026-10-27 release observation

COS/binding/recovery fixes deployed to SG revision 71, USW revision 56 and local; direct KV/TTL and Worker binding smoke tests passed on all three. Residual warnings remain recorded: SG deleted test app `ops-workflow299` restore chain; one local queue `database is closed`; USW rolling cron 500 and deleted-resource wake. Final regional agent 60-second window had no WARN/ERROR, but residual cleanup causes and individual cron retry outcomes remain unverified. See [release record](release-2026-10-27.md).

### 数据路径性能后续计划（2026-10-27，未实施）

源码确认compaction在阈值判定前下载基线/增量且字节阈值包含旧L1；fleet确认后上传Enqueue仍同步写本机spool，队列满时可同步COS；capture默认每cell 2ms轮询。实际线上贡献需按[优化实施计划](../todo.md#存储性能后续评估)建立基线后验证，不将候选优化描述为已修复。所有阶段保留连续durability屏障与真实HK COS双agent验收。

- compaction预检第一步已在隔离分支实现：空尾部/低于阈值不下载L1，旧L1及完全覆盖的L0不计入触发阈值。相关race与HK COS契约通过；仍读取L0正文，dirty队列及完整性能矩阵尚未实施；已完成部分已随 49133b1 发布。见[验证记录](compaction-preflight-2026-10-27.md)。

### 数据路径观测进度（2026-10-27）

已补 spool/Enqueue 分段累计观测，保持确认顺序。HK 双 agent 初步热采样153960请求无失败、无bucket回退，仍有follower入口686ms长尾，原因待trace定位。完整性能矩阵和观测开销门槛未验收；见 [基线记录](data-path-baseline-2026-10-27.md)。

### 双节点转发热写长尾已获trace证据（2026-10-27）

真实HK COS复现835ms PUT，其中794ms位于follower转发前resource fence读取，owner仅5.5ms；1秒fence缓存过期无singleflight，8个请求同时查询。普通入口2秒空闲停止预热，分批切换入口可重新暴露冷元数据。后续已在隔离分支修复KV纯转发前fence准入，并合并同scope查询，HK trace确认入口HTTP资源准入为0；随后定位owner路由COS查询导致567ms长尾：远端提示未复用续期租约、点查节点未登记known refresh。隔离分支已修复，最终trace热转发不再出现前台owner读取（首次冷访问除外）。Worker并发trace关联仍有缺口。见 [COS trace报告](data-path-cos-trace-2026-10-27.md)。

修复与验证：[KV转发/fence修复](kv-forward-fence-fix-2026-10-27.md)。全仓build/vet/test、replica/server三轮race、HK双agent153960热请求、强杀/冷恢复100/100、删除同名重建均通过；已随 49133b1 发布。

路由长尾后续修复与真实COS验证见 [闭环报告](kv-forward-route-fix-2026-10-27.md)。复用仅限转发提示，不能成为本机读写权限；遥测上报交给waitUntil。历史节点逐个串行解析导致首次发现延迟，已补最多8并发解析；节点发现仍随历史记录数量增长，测试就绪等待与稳定期分开统计。已随 49133b1 发布。

### 2026-11-08 原生 LTX 迁移验收仍未完成（ADR-312）

本轮补齐单文件流式快照（分页回调不能成为多个独立快照），恢复同名不可变对象冲突拒绝，修复 LZ4 尾部格式，使 Go 生成的快照/增量通过 celld Rust 解码，并保存 Rust 生成的反向固定样本。新建 SQLite cell 初始 TXID 改为 1，避免首笔增量被识别为快照；capture 不再单独钳制 baseline。此证据只覆盖 codec 和 capture 局部，不证明完整生产链路已迁移。旧 codec/fallback、epoch continuation、完整 level 调度/compaction 以及真实 COS 冷恢复仍须完成后重新验收；历史 COS 结果不可作为本次原生格式通过证据。

追加验证：全仓测试暴露 own.json 的普通 owner epoch 被误用作 resource generation，1 秒缓存刷新后可把正常 cell 误判为重建并使 TXID 回退。已改为只用 retirement job/epoch floor，补代际隔离回归，真实 HTTP KV 250 条写入→冷恢复完整性测试连续三轮通过。其余全仓失败仍涉及 epoch marker、compaction、DO/recovery 旧格式调用方；整体目标未完成。

续链扫描进展：`Restore`/`RestoreChain` 与 durable watermark 已共用原生 epoch/level 范围规划，不再通过 manifest 或下载正文识别快照/marker。补充错前驱跳过、晚到尾部裁剪、缺前驱拒绝、最新快照停止历史扫描、跨 level 选择和 names-only 水位回归。原生压缩范围重叠现在按 celld 允许并恢复其最新页；paged index 同步修正缩库 commit 与快照索引映射。restore/sqlcapture/cellstore 整包及 HTTP KV/D1 冷恢复测试通过。旧 MarkEpoch 写入端仍为无效反向范围，必须完成 cut+1 零页对象和本地票据统一推进后再验收 owner 切换；未跑本轮 COS，不宣称续链全链路完成。

原生 compaction 进展（2026-11-08）：已接入 L0→L1→L2→L3 相邻范围合并、UTC 对齐独立周期，以及 L9 显式/owner release 快照；正常压缩不再写 manifest/index。DO producer 快照和增量均直接写原生页，agent ReadSegment 不再重新包装旧格式，DO 冷恢复/接管/失败重试/损坏 WAL 回归通过。实际 celld Rust compactor 对照的 header/pages 一致。真实 HK COS 小数据链路（40 行、16 次更新、三级晋升、L9 冷恢复）通过，冷恢复连同校验约225ms；非 fleet、非大 cell、未测试 owner/epoch 切换，不替代剩余验收。[记录](bench/native-ltx-levels-2026-11-08.md)。原生分页恢复与 GC 仍待迁移，全仓测试尚未全部通过。


原生分页恢复进展（2026-11-08）：PageFetcher 已改为从原生恢复计划读取 LTX 尾部内嵌索引，以范围请求读取原生 LZ4 页帧，不再依赖外部 page index/manifest 或整库 fallback。跨 epoch 选页、截断、覆盖页中断 read-run、ETag 固定恢复视图已加回归；对象缺失不会重建为较新 cut，避免混读。内嵌索引长度/范围/顺序与页号均校验；范围读不声称验证整文件 CRC（与 celld paged 路径相同），完整读取仍校验 CRC。目前仍逐对象 Stat 获取 size/ETag，后续可复用列表元数据减少请求。ltx/restore/sqlcapture/cellstore/dosupervisor/objectstore 整包通过。真实指定 HK COS 64MiB 测试通过：1MiB 随机 blob 字节一致，查询 5 次范围读、2,118,006 字节，整项测试 8.53s（含建库、上传、压缩、清理，不是冷激活延迟）。epoch continuation 写入和本地 TXID 推进、GC、旧 codec 清理及全量验收仍未完成。


原生 epoch continuation 进展（2026-11-08）：生产 cell-agent 不再调用旧 MarkEpoch；改用 SQLite 实际 page_size/page_count/WAL salts 的零页 `[cut+1,cut+1]` 文件（WALOffset=32），发布成功后持久化本地计数并重建启动前 capture cursor/barrier，首笔业务增量从 cut+2 开始。上传成功但响应丢失时，仅对已存在的原生零页文件做只读证明后重试；真实缺失增量不可冒充续链。完整冷恢复在临时文件原子发布前采用已验证链水位，分页恢复在开放业务写入前采用页源水位，避免续链未修改 SQLite 元数据页导致 TXID 复用。对应恢复/失败发布/首次增量回归和 native race 通过。

分页恢复已删除远端 `gc/pins/*.json` 创建；PageFetcher 固定 cut，无远端续租或自动换链。HasCompleteSnapshot 同样删除 pin/manifest/index 证明路径，使用原生当前 epoch 独立链验证。在线物理 GC 当前仍 deferred；缺失对象读取报错，不能将去掉 pin 解读为在线删文件已获安全证明。真实指定 HK COS 测试：前驱57、续链58、首笔增量59，跨 epoch L1 压缩及完整/分页物化后41行字节与 integrity_check 通过，9–10 对象全部 .ltx（批次数不同）。测试是单 Go 进程+真实 COS，非 fleet owner lease 接管。旧 MarkEpoch API/测试、旧 codec/manifest/index 接口、物理 GC 策略和全仓验收仍未清理完成，不能宣称全面对齐。

原生完整性证明修订（2026-11-08）：`ValidateChain` 已直接流式解码原生 LTX，验证文件 CRC、页面尺寸和最终页面覆盖，不再仅凭 TXID 连续性证明完整快照。只保存页号集合，不保留数据库页面正文；截断后扩库必须重新提供被截断的页面。已补缺页但 CRC 正确的快照、真实零页续链、错误页尺寸、损坏 trailer、截断后页复活及当前 epoch 独立性回归。此修订加强删除前的证明，物理 GC 仍未完成。


原生恢复/上传收口（2026-11-08）：restore.IndexChain/ApplyFile/Compact 已只接受原生100字节header；按内嵌索引保留原始压缩帧位置，按需解压写入1MiB磁盘写窗口，不再转换WAL page-map或保留整库解压页集合。恢复选链仅使用MinTXID/MaxTXID，旧header epoch/marker/分页snapshot选择逻辑删除；epoch前驱裁剪继续由replica路径规划负责。旧内部marker测试替换为原生续链缺前驱回归，epoch裁剪回归保留在replica。restore.Compact现在无分片预算参数，输出单个原生索引快照。

Append/AppendL1及批量入口已走原生校验/合并，拒绝私有44字节格式。批量上传改用CompactNative，修复缩库后旧page超出Commit导致上传失败，并去重字节一致的重试，冲突内容仍拒绝。readNativePlan与AppendSnapshotFile校验不再为验证创建私有page-map。restore/ltx/cellcapture/cellstore/sqlcapture/dosupervisor整包和native race通过；真实指定COS续链冷恢复通过（41行、57→58→59，全对象.ltx）。provider仍缓冲对象正文，故尚不能声称全链路内存有界；旧codec在其他模块、旧GC/manifest接口和全仓测试失败仍待处理。


原生 follower/灾难恢复修订（2026-11-08）：follower spool 已直接验证原生LTX，epoch来自请求/session envelope，不再比较不存在的header epoch。非session spool内容去重改为原始文件SHA256，同范围不同内容必须保留到恢复阶段，由不可变对象冲突阻止误确认；seen集合只在文件fsync和目录sync成功后更新，避免失败重试误认已持久化。recoveryBatch使用原生header/CRC验证与MinTXID==1快照识别，不再转换私有page-map。peer/recovery/upload整包通过；新增同范围冲突不得seal/checkpoint、损坏文件不得ack、目录sync失败必须重新证明回归。

真实指定HK COS新增TestCOSNativeFollowerRecovery通过：两个实际HTTP follower spool先确认SQLite捕获数据，writer停止时COS尚无cell LTX，恢复从followers上传COS再冷恢复新SQLite，20行逐字节一致、TXID21和integrity_check通过；2个LTX对象，恢复及冷恢复校验约1.512s，总测试2.43s。测试是单Go进程内两个HTTP服务+真实COS，不代表多节点进程崩溃或owner lease接管验收。旧格式接口/GC与其他包旧测试仍未清理完成。


owner 删除代次修订（2026-11-08）：删除围栏合并进 own.json 后，Claim/ClaimAs 必须保留 RetirementJob/RetiredEpoch，普通 Release/ReleaseAs 对带删除历史的记录只清空持有者，不删除记录；ClaimAs 与 Claim 一样拒绝 Retired。修复更高 epoch owner 接管时丢失删除代次、导致离线节点接受旧本地 SQLite 的实际回归。普通 owner epoch 变化不改变资源 generation；没有新增 COS 对象。owner 整包、server 离线旧数据隔离与 KV/D1 捕获一致性测试通过；全量协议对齐验收仍未完成。


原生 HTTP/fleet 测试迁移（2026-11-09）：server 主要 commit/append/peer stream、DO recovery、resource cleanup 测试已用真实原生页文件，epoch 只在请求 envelope 中传递；大 JSON 请求使用不可压缩页保持大于1MiB的边界，range 断言比较原始压缩文件字节。binary commit/pipelined commit 改为 ValidateNative，慢提交日志只读原生 header，不再转换私有 page-map；新增旧格式必须入口400拒绝回归。相关测试通过。KV TXID偶发跳号已用调用栈确认来自首次owner激活后的异步timer建表事务；测试先初始化timer schema后仍严格断言每个应用写TXID连续，并保留250行逐字节冷恢复验证，重复20次通过。指定HK COS follower恢复重跑通过：20行、TXID21、2个LTX，恢复与冷校验662ms、总测试1.24s（单进程两个HTTP follower，非多节点故障验收）。server GC指标自动更新和旧GC测试对退休幂等性的断言仍待处理，不宣称全量通过。


原生 GC 诊断修订（2026-11-09）：BacklogStats 删除 manifest 与 retired/seal 对象读取，按原生0/1/2/3/9文件范围的并集判断低层覆盖；高层范围缺口不会被最大TXID掩盖。压缩时间使用高层对象LastModified，秒数单位修正；epoch数量报告实际列出的目录，不再推断私有退休标记。列表经owned objectstore校验。显式诊断与管理维护更新进程内指标快照，压缩与metrics scrape不触发额外广泛扫描。原生压缩前后统计、范围缺口/跨level并集/无metadata GET及server指标测试通过。更正待办：旧管理GC实现仍调用SealEpoch/MarkRetired/EnsureTombstone，其特殊文件路径必须整体替换；不能只调整旧测试的第二次退休计数就宣称符合无特殊文件要求。


快照标记接口删除（2026-11-09）：彻底移除 cellcapture 的 SnapshotStarted/SnapshotDone/ClearSnapshotStarted 回调、replica 同名空API和 skipPartialSnapshot 旧标记判定；sqlcapture 删除 SnapshotWithStart，RequestBaseline不再接受外部标记回调。仅保留内部onImageReady用于固定镜像边界确定后释放捕获锁，不产生远端对象。初始快照测试检查整个scope只含LTX，后台快照测试直接校验原生header与continuation后的精确水位，上传失败测试要求启动失败；旧started/done前置设置删除。指定COS的level/续链/冷恢复通过（57→58→59，41行，9个LTX）。回归同时暴露并修复capture失败竞态：barrier已返回fatal而Done尚未关闭时，Ensure必须取消并等待退出清理再重建，不得把失败实例当作warm。GC特殊文件路径、旧codec及全量验收仍待完成。


原生 epoch GC 实施（2026-11-09）：RetireEpochsWithOptions已整体替换为celld恢复链基线算法，使用已有epochChain而非“最新快照”猜测；必须看到当前owner epoch为最新链和最新目录，已记录退休基线高于本次基线时拒绝清理。默认一小时对象上传年龄、保护紧邻前一epoch，未知时间/截断列表/非原生对象保留。扫描后重新验证owner，再写与上游完全同名同字段的ltx/retired.json（retired_below），随后删除候选LTX；不创建每epoch seal/tombstone/gc-retired/pin文件。这个retired.json在固定celld 90b4301中确实存在，不是CellHive自定义扩展。

后台与管理GC使用AcquireResident持有本地请求门闩，不打开冷cell，分页恢复未完成则不扫描/不删除；完整扫描后再次检查本地就绪与精确owner epoch，删除中owner失效停止。默认不授权调用仍返回deferred；显式离线调用保留外部静默前提。纯判定、前一epoch保护、时间边界/时钟偏差、缺opener/旧视图/本地pinned、owner丢失前不得发布退休标记、重复清理和管理接口无私有sidecar测试通过。旧level GC辅助API、旧codec和全量验收仍待清理，尚不能宣称全部对齐或生产发布。


原生epoch GC验收补充（2026-11-09）：补充恢复计划可解析检查，当前epoch虽有快照但TXID中间缺口时不得发布退休标记；每轮最多扫描64个旧epoch，与celld一致。owner验证在扫描后及每个候选epoch删除前执行，不为每个LTX重复读取owner。server整包通过（46.039s），GC/驻留门闩相关race通过。真实指定HK COS：停止两个producer并完成分页物化后，以测试时钟推进保留窗口，epoch3发布原生L9，删除epoch1的6个LTX，保留epoch2，剩余5个对象只含LTX和上游retired.json；随后冷恢复41行逐字节一致且integrity_check通过。总测试6.39s；这是离线静默GC+真实COS验收，不代表在线多节点竞争/一小时真实等待验收。全仓make test仍在运行，其他旧接口/level GC测试待修齐。


退休epoch恢复覆盖（2026-11-09）：恢复sealed follower和旧Held收集在验证原生LTX后查询精确scope退休标记；已退休epoch不重新上传。持久化upload spool仅在重放路径查询标记并移除退休记录，实时Append/AppendBatch不增加标记GET。Covers先用原生链回答，覆盖不足时退休epoch视为全TXID已覆盖；未退休范围冲突依然拒绝。缓存按celld存在5分钟/不存在30秒，本地GC成功写标记后立即更新；并发/迟到读取不降低已知退休水位，读取错误不缓存为absence，scope隔离，缓存最多4096项。退休覆盖/缓存TTL/精确scope/读取失败重试、恢复不复活且活跃冲突不隐藏、spool清理回归及race通过。最新全仓测试完成，仍失败的包为compaction和replica（旧格式与旧GC契约测试）；其余包通过，整体目标仍未完成。


旧epoch marker接口删除（2026-11-09）：删除MarkEpoch及ErrEpochMarkerConflict，清除反向范围[cut+1,cut]生成和旧page-map转换；Go调用点归零。旧marker测试迁移为真实AppendContinuation：[cut+1,cut+1]零页、WALOffset32、真实page size/commit；并发同键不同尺寸必须仅一方成功，冲突统一包装bucket.ErrPrecondition；发布成功响应丢失后的Check/Append重试不增对象。原先跳过的旧marker/混合batch测试由原生前驱裁剪/缺失前驱/压缩续链/已覆盖L0不下载/未知对象保留测试替代，不提供旧格式兼容。

基础replica测试同步原生语义：批量输出单个原生合并文件并验证最终页；L1按范围优先无需manifest；重叠增量保留未覆盖后缀；单文件和批量混合按最长连续范围去重；Covers只由真实原生数据证明。资源删除围栏fixture改为原生页，保留晚到上传拒绝断言。目标仍未完成：其他旧manifest/index/GC辅助API、其余两包旧测试和完整验收继续清理。

原生分页恢复完整性补强（2026-11-09）：PageFetcher 在原生内嵌索引合并及截断裁剪后，验证最终 Commit 范围的每个数据页都有来源（排除 SQLite pending-byte lock page），缺页在激活前拒绝。按唯一页数 O(1) 检查，不遍历声明的巨大数据库，不增加对象存储请求。基线证明测试覆盖 0000/0001/0002/0003/0009 的 MinTXID=1、损坏、截断、范围不符和缺失对象；冷恢复及大小阈值测试迁移原生格式。相关原生测试和 race 通过；指定 COS 桶 64 MiB 分页恢复通过（8.07 秒，读取 1 MiB 查询数据使用 5 次范围读、2118004 字节）。该结果不是全量 fleet/灾难恢复验收；旧 GC 接口和其余旧测试清理仍未完成。

原生压缩/GC契约收敛（2026-11-09）：以固定 celld 90b4301 的 crates/ltx/src/replica_compactor.rs 为准，level 压缩是 additive：只发布目标原生 LTX，不删除源文件，不按 L1 generation 数清理，也不写 seal、reader pin、tombstone 或 gc-retired。物理回收由原生 epoch GC 执行；一小时是旧 epoch 对象 upload-age 安全窗口，不代表活跃 epoch 的 L0/L1 一小时后自动删除。此条替代此前同 epoch pin/tombstone 和 KeepL1Generations 的设计描述。原生退休证明仍是 celld 自带的 stream 级 retired.json。显式离线单对象删除只允许外部静默且原生路径与文件内容验证通过的 LTX；不得由 Safe 布尔值独立授权。相关旧 API/测试正在删除和替换，尚未宣称全量验收完成。

原生GC清理验证（2026-11-09）：已删除 gcproto.go 中 seal/pin/tombstone/gc-retired 全部实现、旧同 epoch 删除辅助函数及其并发工具；移除 ReaderPinTTL/DisableReaderPins/ReadOnly pin 配置。EpochRetired 只读原生退休缓存；显式离线删除改为原生路径/header/CRC完整验证，并支持全部五个level。删除中断后可从已发布retired.json继续回收，保留链始终可恢复；新增测试覆盖无静默拒绝、跨epoch拒绝、校验失败/owner丢失拒绝、重复删除、原生GC中断重试。DryRun与真实compaction共用原生范围计划，不读manifest或LTX正文。新增additive测试覆盖全部晋升级别后旧PageFetcher首次读仍保持原cut、新reader得到更新且只有.ltx对象。replica/server/dosupervisor全包、相关race、build/vet通过。指定COS真实续链57→58→59、41行冷恢复、旧epoch删除6个LTX并保留直接前驱，GC后再次恢复一致；总测试4.35秒。仍有旧compaction测试和manifest/index API需清理；不代表完整fleet验收。

独立LTX元数据与压缩契约清理（2026-11-09）：删除 Manifest/PutManifest/ReadManifest/ManifestName、PageIndex/PutIndex/ReadIndex/IndexName 及 CIDX/CID2 sidecar codec；移除 compaction 的 GC/Safe/OfflineQuiescent/KeepL1Generations/MaxPartBytes 等无效选项和虚假 deferred/deleted 结果字段。维护接口不再接受 keep_l1_generations，压缩发布前验证真实 owner；日志仅报告实际增量压缩输入/输出。移除已失效 CELLHIVE_LTX_COMPRESSION 配置，生产原生 LTX 无格式切换。旧格式兼容、同epoch删除、独立索引契约测试被原生字节完整恢复、稀疏materialize、范围去重、阈值、续链、旧cut保留和epochGC测试替代；replica/compaction完整包测试通过（清理最终选项后全仓重跑中）。protocol-formats、storage-and-s3、cell-protocol、configuration 中英文正文已同步，不再把44字节/WAL3描述为生产协议。最终全仓与COS验收结果待后续记录。

本轮最终检查（2026-11-09）：make test 全仓通过（Go允许复用未变更包缓存），make build、make vet通过；replica/compaction两包完整race重跑通过（4.045s/6.230s）。后台压缩测试改为验证两个原生L0/L1对象、源保留且无sidecar，不再要求已删除的gc-deferred日志。CLI去除--keep-l1，--grace默认3600秒并说明epoch对象upload-age；相关e2e脚本已同步。指定COS续链/GC/冷恢复再次通过（4.22秒，57→58→59，41行，旧epoch删除6对象后恢复一致）。完整fleet灾难恢复、SQLite导出导入及工具命令旧codec清理仍未完成；全仓单元/集成命令通过不等于该端到端验收完成。

原生工具与乱序上传修复（2026-11-09）：删除仅由P0 spike引用、非当前部署依赖的cmd/cell-supervisor及Makefile构建目标，历史gate目录标为不可运行的研究记录。cellbench/realbench改为真实原生512字节单页文件（synthetic transport benchmark，不是SQLite SQL吞吐），restoreverify只用ValidateNative。删除未使用的EncodeNative私有page-map转换入口及DecodeAny/SplitAny混合格式回退。发现并修复真实并发上传bug：到达顺序下一个scope的队列可能含TXID间隙，不能整体交给单文件原生compactor。replica.UploadBatch排序并分割为连续范围逐个不可变发布，AppendBatch仍严格要求连续；上传实时、spool重放、队列满同步回退共用该入口。缺失TXID仍使恢复失败；只有全部分组成功才确认队列项，失败保留spool供幂等重试；同范围内容冲突仍拒绝。新增测试覆盖乱序[5,1,4,2]与迟到[6,3]、部分发布失败重试、缺口不隐藏、冲突不隐藏。replica/upload/cellbench完整race通过；修复后的16请求×4并发fleet/bucket均零失败（仅smoke，不能作为性能对比）。COS两HTTP follower恢复20行通过1.84秒。用本轮Go生成输入/输出再次运行固定celld Rust Compactor，header及页面完全一致。最终全仓测试结果随后记录；完整fleet灾难恢复/导出导入/大文件有界内存仍未全部验收。

本轮收尾验证（2026-11-09）：上述乱序上传与工具清理后的 make test 全仓通过，make build/vet通过；COS follower恢复和固定Rust互操作通过。该结果仍不代替独立多进程fleet崩溃/切owner/SQLite导出导入验收。

原生快照分片残留清理（2026-11-09）：删除私有 EncodeSnapshotParts/EncodeSnapshotPart、SnapshotFlags/SnapshotPart 及对应旧格式测试；SnapshotStream 的批次仅是原始页面读取缓冲区，送入同一个原生 encoder，不产生独立对象。缓冲区页数按原始页面预算计算，移除44字节/WAL3开销；新增精确页边界测试。S3集成fixture改为原生完整快照，修正L1读次数探针及owned cells前缀清理；指定HK COS验证快照恢复、压缩、按页恢复通过，无新增压缩的L1 body GET=0。恢复启用大数据A→B→A测试，验证原生快照/增量、压缩、离线GC物理保留前一epoch与逐页恢复；逻辑retired_below不等于物理删除边界。
真实双agent batch-recovery本轮失败于初始化：共享控制cell cells/__platform__/__control__/main 中存在旧index.bin/manifest.json/epoch-cut；只读盘点104对象、536978字节。未添加旧格式兼容、未删除这些共享数据。须经数据库导出/导入迁移后重跑，不能把局部COS成功宣称为完整fleet验收。其他旧44字节codec残留、provider大文件有界内存及完整迁移验收仍未完成。

本轮最终验证（2026-11-09）：清理后的 make test 全仓通过（允许Go缓存），make build/vet通过；恢复启用的大数据owner切换测试race通过（19.456秒）。本轮独立双agent COS测试失败仍按上述记录，未将全仓测试通过当作完整fleet验收通过。

原生codec最终清理与控制数据库迁移（2026-11-09）：删除44字节Encode/Decode/Split、DecodeNative私有page-map转换、WAL1/WAL2/WAL3编解码器和开关，保留WAL捕获内存结构及只写原生格式的encoder。测试直接校验原生header/pages/CRC，旧格式拒绝仅保留固定字节fixture；删除无兼容要求的旧resource-fence迁移跳过测试。对齐celld WalReader::page_map，过滤超过最终commit页数的旧页，修复同批WAL数据库缩小时原生编码失败；回归先失败(commit=2却保留page9)后通过。增量批次大小估算改为原生header/frame/LZ4最坏开销/内嵌index/trailer，上限1MiB（单个事务不可拆除外）；不可压缩fixture修复前1053403字节超限，修复后通过。
HK测试桶控制cell迁移已完成：104个旧对象先完整备份（含ETag/SHA256），隔离旧版工具导出watermark86/89页SQLite，integrity_check=ok；导出文件与原生LTX恢复文件逐字节一致。共享桶测试锁内确认36个node lease均不存活、owner不存在、备份对象未变化后，先发布e34/L9原生快照，再删除103个旧LTX/辅助对象，保留owner-gen；COS恢复SQLite SHA256=aea1e731ebfdbf5dac926e59369f7b7430cae691819dd30f246d5baa884e2357，与导出一致。备份及一次性工具仅保存在/tmp/native-control-backup，旧codec没有加入当前生产代码。该迁移仅覆盖共享控制cell，不表示所有历史测试cell都迁移完。
双独立agent真实COS batch-recovery通过：确认fleet ack后写100条，SIGKILL owner，接管恢复100/100；清空接管节点本地SQLite后COS冷恢复100/100，首次GET1.077385秒；命名空间清理HTTP200。日志/tmp/native-migrated-batch-recovery.log，产物/tmp/hk-review-1794158724-4f060b-fg9gkqek。provider大文件有界内存、通用SQLite导出导入产品入口与完整100k epoch-stress验收仍待完成；不宣称全部goal完成。

本轮最终检查（2026-11-09）：删除旧codec并修正native增量字节预算后的 make test 全仓通过，make build/vet通过。ltx/sqlcapture/replica完整race在本轮codec清理及缩页修复后通过（1.180s/31.025s/25.569s；后续仅调整批次字节预算，其回归及全仓测试通过）。10万条真实COS epoch-stress已启动，尚未终态，不作通过声明。另修正S3集成测试清理前缀：Scope.Key本身含cells/，不能再拼接一次；测试使用owned Objects。

原生快照流式上传（2026-11-09，真实provider验收进行中）：新增bucket.StreamCreator与Objects.ConditionalCreateReader，FS临时文件完成后hard-link原子发布（.tmp不进入扫描）、S3 IfNoneMatch、COS/OSS原生forbid-overwrite。AppendSnapshotFile不再os.ReadFile，使用NativeDecoder完整CRC校验再上传；同范围重试按1MiB以内Range GET与spool逐字节比较并检查ETag稳定，复用既有resource fence后置校验/晚到写条件清理。FS Stat/Range改为流式hash避免整文件分配。COS阈值/分片均5MiB，与固定celld默认相同，最多10000片；最终complete禁止覆盖，失败或取消用独立30秒清理context abort。只产生原有canonical LTX对象，没有新marker/manifest文件。
单元测试覆盖初始/重试流式路径、冲突与损坏拒绝、原子可见性、尺寸不符/取消不发布、COS multipart成功/完成冲突/分片失败/取消/短源全部abort；bucket/objectstore/replica完整race通过2.761s/1.021s/32.404s。真实COS8MiB不可压缩SQLite multipart+同字节重试+按页冷恢复测试已加入，等待现有100k epoch-stress释放共享测试锁；目前不能把SDK wire测试当作真实provider验收。当前100k运行仍使用开跑时旧二进制（此前nativecodec清理/缩页/预算修复版本），未混入本轮流式上传版本。普通Get与部分compaction仍非完整有界内存；对象timestamp metadata与producer时间戳语义尚须继续对齐审计。

真实COS流式上传与恢复性能验收（2026-11-09）：TestCOSNativeStreamingSnapshotMultipart已真实通过（随机SQLite约8.48MB/2054页，原生分片上传、相同内容重试、禁止整文件GET、Range冷恢复、SQLite逐字节SHA256及integrity_check）。分阶段定位旧Materialize逐页GET瓶颈：上传0.393秒、重试1.250秒、完整恢复66.548秒；Materialize改为复用ReadRun，按来源key/ETag/相邻帧及稀疏范围边界截断，最多缓存1MiB解码页，并对SQLite lock page写零而不请求不存在的远端帧。回归先失败600次GET，再通过5次GET（含覆盖页）；稀疏恢复4页仅2次GET、未请求页面保持空洞。相同规格真实COS复测：上传0.375秒、重试0.738秒、恢复0.531秒、整项1.993秒，SHA256一致。两次随机内容不同，属同规格单次对比，不是统计吞吐基准；日志/tmp/native-cos-stream-phases.log和/tmp/native-cos-stream-optimized.log。
100k独立双agent真实COS epoch-stress终态PASS：单写线程100000确认写入909.8秒，三次SIGKILL A→B→A→B；在线及冷恢复各100000/100000值相同，cold首GET2.529947秒，清理HTTP200。运行使用本轮流式变更前的固定二进制。发现旧cold脚本可回退备用节点，因此额外直接读取冷恢复peer的SQLite对ledger验证100000/100000，integrity=ok，文件SHA256=bcc7234641207b625148752fc31042a61c4391bc4f11f1d97b9cf5a5ec50203a（/tmp/native-epoch-cold-sqlite-proof.log）。后续脚本已改为cold前停备用节点并禁止备用URL，语法检查通过，严格新脚本尚待重跑。证据/tmp/native-epoch-stress-current.log，/tmp/hk-review-1794158867-ea5bdf-3pa_wq66。不能把该串行故障注入用例的平均速率当作并发RPS。

本轮收尾（2026-11-09）：上述流式/multipart/Materialize合并读取最终状态的 make test 全仓通过、make build/vet通过；Materialize/原生PageFetcher/流式快照定向race通过5.229秒。未运行新严格版100k脚本；下一轮须继续compaction输入/输出缓冲和timestamp metadata/生产时间戳审计，不能以本轮上传及恢复性能结果代替整个goal完成。

压缩流式路径修订（2026-11-09）：L1/L2/L3/L9不再GetVersion/RestoreChain保留全部正文，也不再bytes.Buffer输出；owned Objects.OpenReader打开同一对象版本，最多16个并发开流请求，校验文件名范围与100字节header一致，再由原生compactor校验CRC/合并页面到临时spool，AppendLevelFile复用不可变上传与resource fence。失败关闭全部流、删除spool；provider禁止通过流接口绕过authority记录语义。没有新增COS对象类型，页索引仍随页数增长。四包race通过；真实指定HK COS约8.48MB/2054页，禁止buffered LTX GET/PUT，逐级L1/L2/L3/L9均成功（1.344/1.622/0.866/1.178秒），L9冷恢复0.639秒，SQLite SHA256一致、integrity_check=ok，整项7.532秒，日志/tmp/native-cos-compaction-stream.log。该单次结果不是吞吐基准，也不代表timestamp metadata和最终严格fleet验收已完成。

真实fleet发现并修复小spool上传生命周期错误（2026-11-09）：COS SDK单次PUT会关闭实现io.Closer的请求体，导致小于5MiB的os.File在成功上传或409冲突后无法Seek/ReadAt，出现app_create_failed。multipart使用内部分片buffer，因此此前大文件测试未覆盖。COS/OSS/S3单次流上传现在借用仅暴露Read/Seek的包装器，文件仍由调用方关闭，保留重试内容比对和上传后fence校验。新增成功/冲突两条回归均先复现file already closed，再通过修复；bucket race通过6.907秒。首次新fleet失败记录/tmp/native-stream-compaction-batch-recovery.log已清理测试namespace；修复版本正在重跑，不能把首次失败写成通过。

小spool修复最终验收（2026-11-09）：make test全仓、make build、make vet通过。真实HK COS双agent batch-recovery重跑通过：owner SIGKILL后100/100确认值恢复，清空本地SQLite后的COS冷恢复100/100精确一致，首GET1.835647秒，namespace清理HTTP200。日志/tmp/native-spool-fix-batch-recovery.log，工件/tmp/hk-review-1794161476-950fc0-d1skjv3j；该结果覆盖修复后的真实小文件上传及故障恢复，不代表最终严格100k多epoch脚本完成。

原生对象时间戳契约（2026-11-09）：与固定celld object_store.rs一致，原生LTX上传从100字节header读取Timestamp，负值钳为Unix epoch，写标准litestream-timestamp metadata，值为UTC RFC3339Nano（去尾零）；超出RFC3339年份范围拒绝。COS单次PUT和multipart initiation、OSS单次PUT、S3条件创建均写该属性，不额外发写请求、不新增对象文件，也不改LTX正文或不可变重试比较。bucket仍是通用字节存储，非原生正文和其他命名空间不添加此属性；replica保持原生CRC严格校验。生产capture时间戳与同watermark重复snapshot的重试身份仍待完成，不能用固定timestamp测试fixture宣称已完成生产语义。

同轮provider路径纠正：真实COS HEAD发现普通字节LTX仍走AppendObject，导致continuation缺少metadata；已改为与spool相同的原生forbid-overwrite PUT，禁止回退append。源level仍保持additive，旧格式不兼容。替换旧AppendETag/HEAD回退测试为原生create/冲突/缺能力拒绝测试。真实COS复测6个对象（L0、L1/L2/L3/L9、下一epoch零页seed）均有正确litestream-timestamp=2025-01-01T00:00:00.5Z；大文件multipart、相同内容重试、逐级压缩、SQLite冷恢复SHA/integrity全部通过，总9.717秒，日志/tmp/native-cos-metadata-put.log。生产端Timestamp仍待下一步实现和重试验证。

metadata/原生PUT最终验收（2026-11-09）：最终make test、make build、make vet通过，bucket race通过2.643秒。修复后真实HK COS双agent故障测试通过：100/100确认写入在owner SIGKILL后恢复，清空本地SQLite后的COS冷恢复100/100一致，首GET0.874078秒，清理HTTP200，日志/tmp/native-metadata-put-batch.log。尚未补齐生产capture时间戳和增量WAL坐标，也未运行最终严格100k脚本，goal继续。

增量header坐标实施契约（2026-11-09）：WAL cursor返回本次已提交帧的实际起始字节位置与长度（不含未提交尾部），capture分块按原始帧数量推进offset/size，在页去重和截断过滤前计算；原生增量header保留本代WAL salt。与celld db.rs的info.offset/sz/rd_salt语义一致。该修订不改变快照、压缩输出或续链seed的既有坐标语义；实施与验收中。

增量坐标与producer时间戳进展（2026-11-09）：WAL PollResult现返回已提交offset/size，SQL capture按chunk原始帧数推进，DO capture保留本次提交跨度；两者增量均填真实WAL salts和当前Unix毫秒Timestamp。DO快照与cellstore续链seed也填当前时间；DO失败保留原生字节，seed重试保留首次发布对象，新增测试验证时间改变仍不覆盖原字节。SQL capture快照时间戳仍未改，须先完成同watermark重复快照的幂等设计。600次重复页更新和独立baseline分块回归验证跨度精确覆盖实际WAL；损坏尾帧不进入跨度，修复后从上一已提交cut继续且不重复。四包race通过（sqlcapture35.497秒），补充最终坐标/seed定向race通过。

本轮最终验收：全仓make test通过（/tmp/native-coordinate-full-test.log），build/vet通过。指定HK COS新二进制双agent测试通过：owner SIGKILL后100/100确认数据恢复，删除本地SQLite后冷恢复100/100一致，首GET0.807389秒，清理HTTP200；证据/tmp/native-coordinate-batch.log。SQL capture快照Timestamp重试身份与最终严格多epoch验收仍待完成。

原生快照时间戳重试契约（2026-11-09）：SQL capture快照现按celld SnapshotReader填生成时Unix毫秒Timestamp，不消耗额外TXID。同一canonical快照范围重试仅允许生成Timestamp（header偏移32..40）及随之变化的最终file CRC不同；旧对象必须经NativeDecoder完整校验，其他header、页面、索引、trailer字节必须一致。成功重试不覆盖对象，不修改原ETag或首次timestamp metadata；真实数据/其他header不同、损坏旧CRC、增量时间戳不同仍拒绝。byte与spool路径均执行该规则，spool旧对象最多1MiB Range缓冲并检查ETag一致；普通首次上传无额外请求。该规则是同一快照重复生成的幂等性，不是旧格式兼容或额外marker。测试覆盖跨capture实例的两种提交接口、并发首次发布、页面/header冲突、旧CRC损坏及增量拒绝。

快照时间戳真实COS验收（2026-11-09）：约8.48MB/2054页快照以不同Timestamp重新编码，重试保持原ETag，6个LTX对象HEAD均保留首次标准metadata；随后L1/L2/L3/L9压缩及SQLite冷恢复逐字节SHA和integrity_check通过，总9.739秒，时间戳重试1.889秒，冷恢复0.655秒（/tmp/native-cos-snapshot-timestamp.log）。刷新固定celld 90b4301双向互读：当前Go生成含Timestamp/WAL坐标文件，Rust真实Compactor输出header/pages与Go完全一致；Go再读取本次Rust输出并逐页验证通过（/tmp/native-current-rust-interop.log、/tmp/native-current-reverse-interop.log）。最终严格100k多epoch测试已启动，尚未完成；不以该单次对象测试替代fleet验收。

大cell删除前证明修订（2026-11-09）：HasCompleteSnapshot改用OpenNativeFiles+ValidateChainReaders，逐页解码验证完整CRC、TXID续链、截断后页面覆盖，再证明当前epoch独立于前驱；不再readNativePlan/GetVersion保留整条链正文。页身份集合仍随数据库页数增长，所有输入流在成功/失败后关闭。新增测试禁止LTX完整GET并验证坏CRC拒绝、流无泄漏；真实COS proof用例已加入，等待当前严格100k测试释放共享锁后运行。当前运行二进制保持不变，不把其结果声称为本次证明实现的验收。

无桶凭据DO原生恢复修复（2026-11-09）：审计发现httpBucket无Stat使PageFetcher失败回退整段读取，Range丢失ETag，HTTP列表只含当前epoch。新增segment HEAD大小/ETag点查，Range透传ETag，chain=1返回原生planner选出的完整前驱链；HTTPStore通过1MiB以内Range构成StreamReader。新增ProvenWatermark按流完整验证CRC和页面覆盖，DO seedCaptureWatermark在真实HTTP/直接bucket路径使用该证明，不再保留全部LTX正文；仅外部旧SegmentSource接口保留兼容适配（不涉及旧对象格式）。测试禁止LTX完整GET，验证跨epoch分页读取/水位、HEAD无body、Range版本、旧列表和完整链列表语义。当前严格100k仍使用启动时二进制，本次DO/证明修订独立测试中。

严格100k与DO真实COS终态（2026-11-09）：固定快照时钟修复后二进制的10万单写线程测试已PASS，891.5秒完成全部确认写入；25026/50033/75042确认处三次SIGKILL，在线100000/100000核对，冷恢复先停止备用节点、删除活动节点SQLite且只请求冷节点，100000/100000核对，首GET2.081096秒，清理HTTP200。日志/tmp/native-final-strict-epoch-stress.log。该运行不包含运行期间新增的流式证明/HTTP适配修订，不能混称同一二进制验收。
新增修订的独立真实COS验收通过：8.48MB/2054页快照，流式HasCompleteSnapshot证明0.323秒；真实鉴权cell-agent Handler + 无桶凭据HTTPStore跨epoch水位cut41、分页恢复SHA一致，合计1.034秒；原生metadata/时间戳重试/逐级压缩/直接冷恢复亦通过，总13.879秒，日志/tmp/native-cos-http-stream-proof.log。首次HTTP测试因fixture仅设TokenInternal未设RootKey导致401，按实际DeriveCredentials修正fixture后通过，未放宽鉴权。

最终原生HTTP/证明版本验收（2026-11-09）：make test/build/vet通过；指定cellhive-e2e-hk-1376795072真实COS双节点owner SIGKILL与清空本地SQLite恢复均100/100，冷首读1.035300秒，清理HTTP200。证据/tmp/native-do-proof-full-test.log、/tmp/native-do-proof-build.log、/tmp/native-do-proof-vet.log、/tmp/native-do-proof-final-batch.log。六项协议审计见storage-and-s3.md；通用SQLite export/import产品入口仍待交付，不以本次协议验收代替。

2026-11-09 大 cell 资源预算实施：按 docs/archive/plans/2026-11-09-large-cell-storage.md 执行。原生 LTX 格式不变；增量分批、可选 L9 超预算跳过并保留链，生产分页恢复错误不得悄悄退回全量缓冲。10 GiB 真实 COS 验收尚未运行，不宣称完成。

2026-11-09 大 cell 优化实现与当前验收：见 docs/reports/2026-11-09-large-cell-storage.md。原生格式不变，压缩256输入/64MiB、可选L9正文前预算拒绝、进程维护/恢复/文件上传限流、64MiB本地LTX版本缓存、DO恢复错误不回退全量正文、LZ4查找表复用、磁盘缓冲和分块索引校验已实现。全量go test ./...、build/vet及相关race通过（/tmp/large-cell-verified-{test,build,vet}.log；/tmp/large-cell-latest-race.log）。原20ms压缩测试在共享磁盘负载下误失败，改为500ms取消隔离测试后连续10次通过；捕获一致性测试先完成真实owner激活及后台注册，保留精确TXID递增断言，连续20次通过。10GiB第一优化版跨epoch恢复约503秒，最终数据校验和8MiB读取批次版本复测尚在进行，不能宣称全部10GiB验收完成。

大cell缓存复核：仅已有文件的快照/压缩输出在成功新建后填充本地LTX缓存，普通字节增量不额外写缓存文件，避免确认写入增加小文件IO；有专门回归约束。

2026-11-09 大cell最终验收：指定HK COS的10GiB实库测试通过（SQLite10,751,451,136字节，LTX787,262,031字节），全库SHA、SQLite integrity、10,240行逐条校验一致，跨epoch恢复水位42。最终续链证明+索引+完整恢复107.672秒（上一轮502.646秒），总测试536.62秒，进程峰值RSS762,564KiB。L9只读取100字节header后按预算跳过。最新全量go test/build/vet与相关race通过，测试对象已清理。报告docs/reports/2026-11-09-large-cell-storage.md；不是部署fleet或100GB验收，默认capture8GiB配置与索引随规模增长的边界仍保留。

### 2026-11-09：大库冷激活优化实施中

10GiB 分页组件试验表明全链证明46.93s、索引1.26s、首条查询1.45s；这不是生产入口延迟。计划把在线索引激活与完整CRC审计分开，压缩页映射内存，并接入workerd DO按页读取。DO当前仍完整Materialize，尚未完成该优化或部署；见冷激活计划。

冷激活阶段验收更新：DO seed已改用固定页索引cut，完整CRC审计API保留。10GiB真实COS经过cellstore打开、TXID接续到首条核对成功0.934s；完整审计在查询后独立执行50.543s并通过。不是公开HTTP/fleet首响应验收。DO按页打开仍未实现，且其baseline/checkpoint快照直接读文件，必须连同capture一起调整才能安全使用稀疏文件；不能仅删Materialize。全量Go测试、vet及相关race通过。详见大库报告追加章节。

DO 按页冷启动实施进展（2026-11-09）：原生 VFS、supervisor 本地页服务及 continuation capture 已有代码，尚未通过新二进制的原生 DO/COS 验收。主库写入及截断现在有开始/结束计数和 generation 校验；snapshot 拒绝活动写入、跨代读取和失败视图。相关 Go 测试及 dosupervisor race 通过。workerd 完整构建仍在进行，不能以现有 Go 测试替代原生验收。后续 checkpoint 仍需要全量 hydrate 和内存快照，这一大库性能边界尚未消除。

2026-11-09 capture 一致性补充：主库 write/truncate 使用开始/结束计数和 generation 校验，失败 native I/O 不释放 capture 屏障。checkpoint 快照提交后，同一次 gate 必须重读并持久化当前 WAL，不能等待下一次请求。新增立即跨目录恢复回归：旧行为返回值 1（预期 42）失败，修复后返回 42 且 dosupervisor race 通过。原生 DO/COS 验收仍待新 workerd 构建完成。

2026-11-09 通用 cell SQLite 迁移实施契约：管理员 GET /v1/control/cell/export?scope=ns/class/id 返回一致的完整 SQLite 文件；POST /v1/control/cell/import?scope=... 接收 SQLite 文件，仅接受无本地激活、无历史 LTX 的新目标，已有数据返回冲突。鉴权始终使用 scope 内命名空间，不能由其他 query 覆盖；二进制 body 不经过 JSON 探测。导入在上传前 integrity_check，流式编码原生 LTX，持有本地独占 admission gate 并在发布前后核验 owner/epoch，成功后从对象存储重新打开。最大导入64GiB，数据经过磁盘 spool、不整体读入内存。旧库应先导出 SQLite 后导入新目标；不新增 COS 标记文件、不改变原生格式。实施与验收中。

2026-11-09 COS/fleet 验收更新：指定桶 cellhive-e2e-hk-1376795072、官方 workerd 1.20260916.1 的100k确认写入、3次owner强杀、清空本地库后100k全量验证通过；通用SQLite导出/导入及无CellHive元数据旧SQLite迁移真实COS通过，包括导入后写入、冷恢复、BLOB和索引保留、重复导入拒绝及测试命名空间清理。10GiB cellstore分页首条验证0.808s，随后全链审计49.709s通过；这不是原生workerd 10GiB验收。原生分页fork构建、全量恢复及完整性能报告仍在执行，goal保持active。证据见 docs/reports/2026-11-09-cos-fleet-acceptance.md。


2026-11-09 原生DO大库验收状态：10GiB试跑仅完成源写入/哈希，环境切换导致进程和临时日志丢失，冷恢复未验证。新增持久日志、分阶段计时和真实双agent/双runtime分页接管套件，仍待允许本地监听/COS网络的环境实跑。详见[验收记录](../reports/2026-11-09-cos-fleet-acceptance.md)最新一节。


### 2026-11-09 原生 DO 大库与 fleet 验收未闭环

真实10GiB DO首次快照暴露整库内存复制；已改为流式本地spool及HTTP提交，定向测试和16MiB真实COS通过，10GiB重测仍在运行。真实双agent/双runtime冷接管后的全BLOB扫描发生409，期间runtime续租HTTP超时；已加入有界顺序预取并通过定向回归，fleet重测尚未完成。任意长同步SQLite查询的续租隔离仍需验证；不得把上述状态描述为全量验收通过。证据见 `reports/2026-11-09-cos-fleet-acceptance.md`。

2026-11-09 更新：实际原生10GiB DO/COS重测已PASS，163840条BLOB全量SHA256及修改后独立恢复一致，9个原生LTX对象，无辅助标记。冷查询2.170s/92KiB；初次快照364.457s，整库独立恢复672.628s（约10.85GB编码对象）。有界快照内存修正已通过此大库验收；fleet扫描409修正仍在真实复测中。详见上述报告。

2026-11-09 fleet复测更新：真实双agent/双native-runtime两次强杀PASS，epoch1→2→3、确认值10→11→12、两次256条BLOB哈希一致，cleanupHTTP200。预取修正解决本次409，但第二次扫描期间仍有一次续租HTTP超时（发生在proof结束前），同步SQLite阻塞runtime续租接口的结构性问题未关闭；需把执行中租约续期移出workerd事件循环，并验证超过lease时长的查询及进程死亡后停止续租。不能把本次小库PASS当作任意长扫描通过。

2026-11-09 续租实施更新：Go supervisor现已独立续租确认的scope/node/epoch，子进程退出/释放/续租失败停止；1s TTL下2.8s原生缺页查询和竞争owner拒绝测试通过，关闭此机制的负对照复现409。128MiB单事务又暴露64MiB缓冲提交上限，已改为原生L0流式提交且同事务真实COS成功。后续长扫描触及cell-agent代理原有30s HTTP截止时间，保持该边界；>30s租约测试使用认证runtime直接请求，生产owner/fleet/gate仍为真实链路，正在验收。不能将其称为公共代理支持超过30s请求。

2026-11-09 原生续租边界验收关闭：真实128MiB fleet两次冷扫描54.056s/52.484s，超过不变的30s租约，2048条BLOB哈希均一致；两次仅杀workerd子进程后34s/32s接管，epoch1→2→3、确认值10→11→12，cleanupHTTP200。普通cell-agent代理路径16MiB复测也PASS（两次扫描5.258s/5.075s）。原生独立续租和大增量413修正已通过；公共DO代理自身30s请求截止时间仍保留。当前整体goal等待官方runtime功能/迁移/100k/性能批次的持久化证据刷新，见reports/2026-11-09-final-acceptance.md。

2026-11-09 最终验收证据刷新：官方workerd1.20260916.1的review/queue/multinode、workflow、routing、batch、SQLite导出导入与旧SQLite迁移全部通过。100k写入经历3次owner强杀，在线及清缓存冷恢复均100000/100000一致，首次冷GET2.642489s。双节点fleet性能36组/153960请求零错误，3轮Wrangler上传/激活及真实运行版本验证通过。实际10GiB原生DO、128MiB单事务与超过租约时长扫描的先前本轮证据已逐项复核；全量快照内存、独立续租和大增量413修正完成验收。性能首轮因端口占用未启动，改用临时端口范围外26700重跑完整通过，失败日志保留。报告及边界见[最终验收报告](../reports/2026-11-09-final-acceptance.md)。无提交、无生产部署。

### 2026-11-10 七项性能优化（实施中，尚未验收）

计划见 `archive/plans/2026-11-10-storage-performance.md`。COS multipart默认32MiB及动态10000片上限不变，改为最多4个在途缓冲/上传、源流单读者；完成列表按part number排序，失败取消并等待全部请求退出后abort，仅所有分片和长度校验通过才原子Complete。不得改变原生LTX格式、owner/epoch、鉴权TTL及resource fence边界。后续checkpoint安全续捕获、恢复seek、fork review和间隔访问延迟改动须分别有故障回归及真实COS验收；本条不是完成声明。

2026-11-10 checkpoint review: reproduced raw-main fallback snapshot superseding already acknowledged WAL when snapshot publication succeeds but its ACK is lost (independent restore1, expected2). Implemented bounded committed-WAL overlay snapshot; regression now restores2. Native checkpoint callback wiring, concurrent/crash validation, and real COS acceptance remain open; do not interpret this focused fix as full migration acceptance.

2026-11-10 checkpoint ambiguity recovery (implementation, acceptance pending): production supervisor requires native `do-paging-fail-stop/1` support and enables process fail-stop for poisoned paged activations. Lost checkpoint-begin/end responses, failed paging callbacks and failed native main-file mutations terminate the workerd child after the existing bounded callback deadline (30s preparation, 5s ordinary callback); no certificate is reset or retried. Supervisor waits for child exit before stopping independent owner renewal, skips graceful ownership release for that dead child, and lets the existing lease expire. Recovery requires a new activation and authoritative restore; sparse local guards remain intact. This intentionally sacrifices all activations in the affected child for safe bounded recovery, creates no COS objects or new persistent metadata, and is not production acceptance.

2026-11-10 critical owner authority regression: real COS bucket cellhive-e2e-hk-1376795072 ignores S3 PutObject If-None-Match and If-Match on own.json; repeated create and stale CAS both overwrite successfully (cos-owner-conditions.log). Nodes use append authority, so the existing nodes-only startup probe misses the distinct raw owner path. Startup diagnostics must test both nodes and cells/.../own.json conditional semantics and fail closed before serving. Queue multinode acceptance failed with node1 epoch1 writes and node2 epoch2 empty reads. Prior passing suites do not prove owner safety. LTX format remains unchanged; provider authority remedy pending resolution of COS versus strict plain owner format requirement.

2026-11-10 authorized COS owner exception: user explicitly chose native conditional append for owner authority. Provider own.json now uses the existing segmented append authority (position fencing, bounded tail reads, hourly windows and8MiB cap), shared with node authority. Physical owner segment naming/content differs from celld plain JSON; native LTX names/bytes and restore chain are unchanged. No snapshot/checkpoint marker is introduced. No compatibility with prior unsafe raw owner objects is claimed. Startup probes cover both authority key families.

2026-11-10 additional authority boundary regression: deterministic TestSegmentedRolloverRejectsInterveningOldWindowWrite fails: rollover copies from an old token into a new physical segment without consuming that token on the predecessor. An intervening acknowledged old-window CAS can be overwritten. This affects reused segmented authority, including node leases, and blocks final correctness acceptance even though current-window COS/Queue tests pass. Evidence authority-rollover-red.log. Native append provider remedy remains under development; no deployment.

2026-11-10 authority correction implementation update: replaced clock-window authority selection with CHA3 stable-root / sealed-generation append protocol. Initial creation consumes root position zero; rollover consumes the predecessor token before successor publication. Retired generations retain one-byte stubs to reject delayed helpers and old writes; online physical deletion of roots/stubs is forbidden. No old-format runtime compatibility was added. Root lifetime and generation limits are documented in the native-authority correction plan. Deterministic rollover/create races, publication-stage recovery, provider retirement-error mapping and bounded descriptor-cache races pass; full tagged Go test/build/vet pass. Real COS CHA3 rollover acceptance and new-binary functional acceptance remain pending. Earlier CHA2 functional successes do not close this issue.

2026-11-10 real COS CHA3 authority probe now PASS9.805s, including independent clock-skew creators, retired-object native rejection, delete/recreate and four post-commit interruption points. New-binary runtime functional and native workerd checkpoint acceptance are still pending; this does not establish full goal completion.

2026-11-10 supervisor hydration membership now uses sparse1024-page bitmap blocks with exact unique-page accounting; truncation floor and activation lifetime are unchanged. Dense10GiB bookkeeping allocation benchmark drops from about74.3MiB to0.45MiB; very sparse occupancy can allocate more, and these figures are not process RSS. Focused boundary/truncate-regrow race tests pass; full supervisor/native COS acceptance remains in progress.

2026-11-10 supervisor bitmap full-package race PASS256.652s (`paging-bitmap-race.log`); focused previously-unhydrated truncate/regrow and boundary race PASS1.083s. Independent source review found no blockers. Full tagged Go/build/vet refresh and native integration remain pending.

2026-11-10 fence CAS atomicity: deletion/recreation decisions and retired-epoch floor updates must derive from the same owner record and version used for conditional publication. A conflicting write requires rereading and reevaluating the decision; copying an earlier fence into a later owner read is forbidden. Deterministic interleavings reproduced stale reblocking and epoch-floor regression; repair and acceptance are in progress. No TTL or object format change.

2026-11-10 owner-mediated retirement plan: docs/archive/plans/2026-11-10-owner-mediated-retirement.md. Before eliminating idle resource-fence GETs, durable control intent must precede external retirement, owner admission must close and drain before exact-owner conditional retirement publication, and dead-owner cleanup must remain lease-fenced. Unreachable live owners cannot yield successful completed retirement; pending cleanup remains retryable. Preserve the existing one-second fence checks until all admission paths, authority transitions and fault tests establish the new protocol. No new COS objects or LTX fields; mixed old/new retirement writers are not supported by a future zero-network fast path. Implementation is staged and not accepted.

2026-11-10 critical suffix-read version regression: real COS concurrent native fence test returned success for all eight floor updates but recovered floor6. Deterministic HTTP regressions show TailGet combined older GET bytes/ETag with a newer HEAD length, creating an invalid authority CAS token. Authority payload, byte length and version must come from one response; fallback HEAD/range must compare versions and fail on mismatch. Partial responses without valid suffix Content-Range fail closed. Repair and real COS revalidation pending; earlier functional passes alone do not establish concurrent authority safety.

2026-11-10 retirement intent identity: the deletion callback must receive the exact cleanup job snapshot read under the control lifecycle lock before the durability proof. Re-reading the resource name after proof can substitute a later unproven cleanup incarnation. A deterministic test reproduces this substitution; snapshot binding is required together with releasing the lock before external callbacks.

2026-11-10 scoped node discovery: optional immediate-child logical listing filters nested recovery authorities before authority reads, collapses physical generations through the stable root, and hides tombstones. Recursive List remains unchanged; backends without the extension retain recursive discovery. Raw listing remains recursive, so this bounds authority reads rather than listed-key volume. Per-node Get/JSON errors in lease.Sample remain a separate startup fail-closed audit; no runtime/deployment acceptance is claimed.

2026-11-10 node discovery fail-closed follow-up: Sample returns no partial peers on any listed node read error except concurrent disappearance (ErrNotFound), or invalid JSON. This enforces the existing startup compatibility gate; cancellation remains fatal. No additional Node/session field validation is introduced.

2026-11-10 acceptance follow-up: fixed native workerd plus quiescence Go binaries passed actual10GiB COS full restore/hash audit (163840 BLOBs) and real fleet fail-stop/lease-expiry takeovers (epochs1/2/3, acknowledged values10/11). Optimized sequential materialization277.553s versus previously observed613.158s; original baseline raw log was accidentally overwritten, with extracted observations explicitly identified in the report. These binaries predate subsequent coordinator/removal changes. Capture failed-initialization readiness publication race is corrected and full cellcapture race passed2.176s; Drop-error local removal preservation passed cellstore race4.785s. A combined sqlcapture run exposed intermittent checkpoint failure (isolated5 and full3 repeats passed); concurrent checkpoint/header-read and ignored SQLite busy-result correction is in progress. Production owner-mediated cleanup, bounded paging lifetime reclamation and final performance acceptance remain incomplete. See docs/reports/2026-11-10-storage-performance.md.

2026-11-10 SQL capture checkpoint correction (targeted local race verification passed): explicit Capture.Checkpoint must serialize with the WAL poll boundary so TRUNCATE cannot race Stat/header ReadAt and terminate capture with EOF. Cell checkpoint helpers must inspect the SQLite wal_checkpoint result row and reject busy checkpoints, which Exec alone reports as successful SQL execution. Existing quiesced-writer requirement and snapshot rebaseline protocol remain unchanged; this is not deployment acceptance.

2026-11-10 cleanup integration follow-up: both production BlockResource call sites have been replaced by leased owner-mediated preparation. A real sealed-follower/nodelog recovery regression covers resource-retired and live scopes in one predecessor session; all live ACK pages survive, target retired pages remain absent, and seal/tail/session-CAS faults prevent takeover until successful retry. Five production delete/recreate cycles pass with gate count returning to zero and stale cleanup RPC rejected before touching the new incarnation. Focused coordinator/routing/owner tests pass; full server verification is running after converting ownerless/direct-seed test fixtures to explicit authority. Former-owner stale gate reclamation, local fast proof, and final real-COS acceptance remain outstanding.

2026-11-10 gate lifecycle follow-up: former-owner gate reclamation is implemented with fresh absent/foreign authority and local claim exclusion; same-process ownership, failed capture stop, and changing authority retain the gate. Bounded local pagination runs on control followers too. Focused combined race tests and3-repeat claim-exclusion/pagination checks pass. The previous integration binary passed real COS review, queue and multinode failover; latest reclamation code is in full verification. Local generation fast proof and final paging/performance acceptance remain incomplete.

2026-11-10 continuous lease evidence: local owner route and local control-read proofs now bind a node lease health revision. Renewals preserve it only across uninterrupted safe deadlines; Publish detects unobserved margin gaps and recovery creates a new revision. Actual expiry remains permanently fenced. Idle access can reuse the exact owner tuple across continuous publications, while local ownership transitions invalidate its cache revision. Foreign routes keep fixed deadlines. This does not enable resource-generation fast admission, change pinned authorization TTL, or remove resource-fence reads. Targeted race regressions passed; current full verification and real performance acceptance remain pending.

2026-11-10 request-bound resource proof integration (under verification): native KV/queue scope admission may derive generation from an exact retained local owner proof instead of refreshing the one-second fence cache. Validate before preparation, after taking the resource gate, and before response headers. Release evidence only after the handler finishes; cold/foreign paths keep authoritative checks. Retirement drains the same gate before publication. The node liveness compatibility identifier is now `node-lease-fence-v2-recovery-v3-retirement-v1`, rejecting live old unilateral-retirement writers; coordinated replacement is required, with no deployment performed. Pinned authorization TTL and background capture/queue/timer checks remain unchanged. No remote object or LTX-format changes. Integration/race/COS/performance acceptance is still pending.

2026-11-10 bounded native paging lifecycle implementation in progress: explicit native handle lifecycle, activation-bound callbacks, bounded resident index/bitmap and local immutable-cut spill are being added. Descriptor/transport retention and local state storage have finite admission quotas; no full idle database hydration or new COS markers. This source change is not represented by the fixed native binary and has no runtime acceptance yet.

### 2026-11-10 cleanup lease-loss audit (acceptance open)

The cleanup coordinator's before/after checks did not revalidate durable cleanup authority inside retirement CAS retries or individual deletes of an administrative purge batch. Explicit required validation callbacks are being added; tests must distinguish stopping subsequent operations from rejecting an acknowledgment after an in-flight operation completed. Epoch-bounded LTX deletion still protects successor epochs. The mutable wake pointer cleanup has a separate delayed-delete/takeover/recreation risk: current unconditional Delete can target a newer incarnation after authority loss. This risk is under investigation; per-operation lease checks alone cannot fence an already-issued unconditional delete. No completed-acceptance claim covers this edge case.

Wake deletion update: same-content recreation ABA now has a random publication revision in the existing wake JSON and captured-version conditional deletes across destructive paths. Deterministic red/green and real COS3-repeat tests pass. Resource cleanup checks authority after candidate reads; timer deletes/retired-scope Forget have authority validators. Full related race and independent review remain pending. This does not establish CAS ordering of delayed ordinary wake PUTs; that preexisting publication edge must not be confused with the verified conditional-delete behavior.

Wake provider boundary: verified COS/FS conditional deletion continues to reclaim stale hints. Providers returning ErrNotSupported (including current raw OSS path) defer only physical hint reclamation; they never fall back to unconditional Delete and do not fail an authoritative timer mutation solely for unavailable hint GC. Retained hints can cause extra cold scans on those providers and are not counted as removed. Authority-validator failures still reject deletion. No COS cleanup is disabled.

2026-11-10 bounded paging lifecycle local verification: new native lifecycle binary and all46 SQLite KJ cases pass; native FS small/lease/fail-stop and four real host abort/native-close/cache-removal/re-admit cycles pass. Supervisor reclaims descriptor, local cut/guard, file-state, claim cache and loader admission after final durable capture; late queued capture and failed-new-restore cleanup have targeted race regressions. Failed-admit cleanup is restricted to newly absent/disposable cache tuples and cannot delete an existing poisoned/uncaptured descriptor. Root Host metadata remains outside facet retirement. Final new-binary COS/10GiB retirement acceptance remains open; prior fixed-binary results do not prove it. Expanded native FS scheduling regressions now cover an old start-info callback resumed after successor admission without native open, and an issued synchronous open preventing host abort from overtaking its admission. Four retirements add zero cumulative hydration; the retained-bitmap gauge itself decreases when caches are reclaimed. Details: docs/reports/2026-11-10-sqlite-paging-review-followup.md.

2026-11-10 lifecycle COS gate update: actual10GiB new-native managed-lease run PASS733.371s, all163840 BLOB hashes/counters after independent full restore, four zero-hydration retirements and delayed-start/issued-open regressions. Final Go binaries also pass COS routing, SQLite transfer and native fleet A→B→A epoch1→2→3 recovery. Earlier lifecycle COS-pending statements are historical; final remaining whole-system/performance acceptance is still active. Evidence and precise boundaries: docs/reports/2026-11-10-storage-performance.md.

2026-11-10 final seven-objective acceptance closed: current-binary COS routing/SQLite transfer/queue/multinode/workflow lost-response recovery/batch/100000-write three-owner-switch online+cold audit/native fleet and actual10GiB all-hash/lifecycle checks PASS. Full Go/vet and46 native SQLite tests PASS with the documented test-only fixture delta. Final interleaved throughput528206requests0failures;30s worker-PUT follow-up22832requests0failures,mean−0.37% versus baseline. No broad sustained-RPS improvement is claimed. Multipart mean3.43x provider-only gain and idle-gap reduction are separate outcomes. Earlier pending acceptance entries are historical. Remaining design limits are unchanged: process-wide paging fail-stop, logical quotas rather than aggregate RSS guarantees, root Host metadata outside payload retirement, latest-epoch arbitrary overlap discovery, coordinated retirement-writer deployment and separate ordinary delayed wake PUT publication semantics. No deployment/commit. See docs/reports/2026-11-10-storage-performance.md.


2026-11-10 权威追加容量调整：按用户要求，将共享 SegmentedAuthority 默认单段上限从 8MiB 降为 1MiB，根文件上限从 128MiB 降为 16MiB。适用于 owner、owner-gen 及使用同一适配器的节点/fleet 权威记录；不是预分配空间。每次切段描述约 64 字节，根容量约 262144 次切段（扣除初始化帧）。保持条件追加、尾部读取、旧段占位和容量耗尽失败关闭语义；不删除或重置已有权威对象。旧对象若已超过新上限会被拒绝读取，部署前须核对容量。此变更不代表已部署或已通过真实 COS 验收。


2026-11-10 全量回归发现 COS 并发唤醒清理边界：ConditionalDelete 的 NoSuchKey/404 原先直接透传 SDK 错误，绕过 wake.deleteCandidate 对 bucket.ErrNotFound 的安全忽略，导致 Workflow sleep/event 返回500。统一映射为 ErrNotFound；412 仍是 ErrPrecondition，权限等其他错误继续传播，不降级无条件删除。HTTP适配器回归覆盖404/412/403；真实COS复验状态见 /tmp/cellhive-authority-small-acceptance/report.md。

2026-11-10 容量调整与NoSuchKey修复回归结果：修复版全量Go/vet及相关race、JS/CLI、真实COS条件写/轮转、Workflow故障恢复、导入导出、队列、多节点、批量恢复及原生DO fleet复验通过；容量版10万写入三次强杀在线/冷恢复均100000/100000一致。保留首次失败证据及条件跳过，不含本轮10GiB/100epoch或生产部署。见[回归报告](../reports/2026-11-10-authority-capacity-regression.md)。

2026-11-10：真实 MinIO 两个 Docker 版本忽略 DELETE If-Match，原始 S3Bucket 不满足 owner authority。已获用户确认实施基于条件 PUT 的控制记录逻辑删除适配；实现/验收中，不得以原生 LTX 恢复成功代替 owner 安全验收。OSS 旧测试桶的过期 CHL1 节点记录已安排备份及限定清理；不增加旧协议在线兼容。

2026-11-10 OSS/MinIO 阻塞处理：OSS 旧测试节点、旧测试控制库和 fleet 记录已备份后限定清理；真实 SQLite 导入导出、强杀接管及冷恢复通过。MinIO 的原始 DELETE 缺陷不变，但 s3:// 控制记录已用 CAS tombstone 安全适配并通过真实 E2E；不增加 LTX sidecar。修复删除重建时空前任身份被传入恢复器导致的 503，实际不是旧数据重现。限制：tombstone 保留、非控制条件物理 GC 不支持、旧 S3 控制格式不得混跑；详见 reports/2026-11-10-oss-minio-acceptance.md。

- 2026-11-10 上线前 review 四项已修复并回归：DO import/export owner 交接、S3 R2 metadata 覆盖写、retired LTX 清理、S3 新 wake hint 物理回收。77 包 Go、vet、相关 race 及 MinIO/OSS 指定 E2E 通过；原生 workerd DO 迁移未单独跑 E2E，生产未部署。旧 S3 raw r2meta / 旧 wake tombstone 不在线转换，目标应为空或显式离线迁移。详见 [验收报告](../reports/2026-11-10-prerelease-review-fixes.md)。

- 2026-11-10 DO 迁移补测：真实 workerd + MinIO/OSS 的 SQLite 导出、新对象导入、HTTP 读写、全新 runtime 本地目录恢复已通过；修复空 resident 导出、新 facet 首次恢复、同 shard 未激活 sibling gate 及首次恢复失败清理。此前“原生 workerd DO 迁移未测”已解除；生产未部署。见 [验收报告](../reports/2026-11-10-native-do-transfer.md)。

原生 DO import 并发激活修复：通过既有 shard owner 的独占权排除远端 workerd 激活，已覆盖 runtime-first/import-first、取消、失败释放、owner epoch 递增及真实 MinIO/OSS workerd 迁移回归。未部署；详情见 reports/2026-11-10-native-do-import-exclusion.md。持续存储故障期间清理仍失败关闭；失败的独占 owner 释放已进入进程内后台重试队列，存储恢复后按原 scope/epoch 自动清理，owner 已变化则放弃旧任务。进程退出后沿用节点租约接管，不依赖 embedded Expiry。

2026-11-10 跨节点 DO import 转发超时已修复：仅导入使用30分钟总超时，普通请求仍为30秒，共用连接池并传播取消、不重放导入。指定 COS 桶 cellhive-e2e-hk-1376795072 的双 agent + native workerd 迁移实测通过：从非 control owner 节点上传，暂停31秒后成功，完整 BLOB 哈希/修改后全新目录冷恢复/再导出一致。77包 Go、相关race、vet及独立review通过；未部署。见 reports/2026-11-10-native-do-import-exclusion.md 的转发与自动清理补充。

2026-11-10 独占 owner 领取丢响应遗漏已修复：此前条件创建/CAS已提交但响应丢失，会返回失败却不登记清理，导致node-lease guard长期占用。本次只在独占领取的实际条件写失败处分配既有后台释放任务，保留本次候选epoch；不返回可用guard、不重试领取。确定性提交后丢响应测试覆盖首次创建/过期owner接管及successor保护，修复前复现、修复后race通过；普通Claim不变。全量77包Go及vet通过，未部署。

上述领取丢响应修复的最新二进制已通过真实COS双节点native fleet：两次强杀A→B→A、epoch1→2→3、counter与全部256行BLOB哈希一致，namespace清理HTTP200。证据见 reports/2026-11-10-native-do-import-exclusion.md；未部署。

2026-11-10 全量发布前审查修订（验证中，未部署）：普通 cell 全库快照排除 SQLite locking page；continuation 与 DO retirement 显式关闭 WAL cursor；分页索引缩库时释放已删除 block 的驻留预算；资源清理保留原生 retired.json 退休下界并正确跨页；worker purge 与 namespace purge 一致保留 own.json/owner-gen 单调授权记录；同 scope transfer 等待可取消且不占全局执行额度。不新增对象存储格式。

2026-11-10 OSS R2 metadata 修订（验证中）：OSS 不支持原子条件 DELETE，因此 metadata 采用既有 native append authority 的条件逻辑删除与分段，不使用裸 DELETE。r2meta/<ns>/<bucket>/<sha256(key)> 固定命名防止用户对象名与 .gN 物理代际文件冲突；COS 使用相同 adapter，S3 仍走条件 PUT tombstone。LTX 与 r2 正文格式不变。旧 sidecar 不在线兼容，空目标桶或离线迁移；R2 正文 onlyIf 能力不因 metadata 修订扩大。

原生 R2 sidecar 容量在正文写入前预检：序列化请求 metadata 加 1024 字节生成字段预留不得超过 authority 配置的 valueMax（默认64KiB）。超限失败不更改原对象。正文与 sidecar 仍是两个对象，既有并发发布限制不变。

COS/OSS R2 正文条件替换明确失败关闭：r2/ 的 CAS 不下沉到不具备已验证原子条件 PUT 的 S3 兼容端点；返回 ErrNotSupported、不改正文。普通覆盖写及 native append metadata CAS 不受影响。S3/MinIO 的条件替换继续使用已验证的条件 PUT。

2026-11-10 R2 正文/metadata 原子发布修复：已改为不可变正文版本 + 既有 metadata authority 条件发布，并让读请求固定一次解析的 Snapshot；COS/OSS onlyIf 使用 authority 而非 raw body CAS。真实 COS/OSS/MinIO 的八写入者竞争、正文 metadata 配对、空对象、删除重建及旧版本读取通过；HTTP/race 与独立审查通过，完整应用 E2E 正在汇总。旧布局不兼容，旧正文保留增加物理占用，尚无安全在线版本 GC。COS LTX 发布竞态不因 R2 修复关闭。

2026-11-10 R2 原子发布验收完成：COS/OSS/MinIO 的当前二进制 review-fixes、queue、queue-multinode 全通过；77 包 Go、vet、相关 race 通过。详见 [对象发布修复报告](../reports/2026-11-10-object-publication-fixes.md)。COS LTX 原子创建尚未修复：multipart 同样多赢家；append 中断留下零长度 canonical 候选对象，未采用。额外协调状态与用户“不新增特殊文件”约束存在冲突，等待明确选择，未部署。

2026-11-10 普通LTX上传验证更新：不增加发布协调文件，撤回此前等待用户许可协调状态的条目。COS普通multipart/metadata/跨epoch完整SHA恢复与cell-transfer正常流程通过。但原生DO仍固定数据epoch1，迟到导入故障注入可覆盖接管者数据；普通覆盖的整体切换尚未验收，不可上线。必须先满足原生DO的数据epoch隔离前提，不能删除或放宽这两项失主导入测试。详见 reports/2026-11-10-object-publication-fixes.md（docs下相对位置以该文件实际路径为准）。

2026-11-10 后续修复替代固定e1阻塞状态：原生DO使用已验证shard owner epoch作为LTX数据epoch，独占导入固定相同代际，恢复沿既有原生链查找；data-owner仅作为接收节点权限fence。失主导入、迟到旧上传不覆盖接管者、真实COS fleet owner1→2→3分页恢复及SQLite导出/导入/冷重启通过。旧固定e1部署不可混跑，离线迁移，不新增防覆盖记录。详见[对象发布报告](../reports/2026-11-10-object-publication-fixes.md)。

2026-11-10 全量 examples 复验进行中：发现 Workflow binding alias 校验错误、D1 示例旧 database_id、静态模板日期超限和具名 service entrypoint 缺 fetch，正在修正复验。另发现冷路由查询与 routes revision 更新竞态导致瞬时 503、R2 示例 HEAD 500 和 WebSocket 示例失败；未关闭前不得宣称 examples 全通过。

2026-11-10 全量 examples 回归更新：26/26 已通过，包括 Queue 消费、Cron 触发、原生分页 WebSocket；上述示例/Workflow/R2 HEAD/冷路由竞态已修正。额外发现并修复默认 supervisor facet namespace 与 paging admission 不一致，默认 Compose 重启恢复及 Kubernetes fleet A→B→C 已通过。其他 COS 大库和独立 assets 条件创建探针仍在验收；不据此宣称全量外部测试全部通过。详见[本轮验收报告](../reports/2026-11-10-full-e2e-examples.md)。

2026-11-10 全量 E2E/examples 收尾：本地 15/15、COS 15/15、examples 26/26 通过；最终二进制 COS DO transfer/fleet、分页/完整冷恢复、10GiB 普通 cell 全文件与全行哈希、Compose/Kubernetes、OSS/MinIO 均通过。独立 COS assets 唯一创建再次失败（8 写入者 2 成功），仍是发布阻塞；未部署。完整证据与可选矩阵未测范围见[验收报告](../reports/2026-11-10-full-e2e-examples.md)。

2026-11-10 COS assets 并发创建阻塞已修复并验收：采用同 key 带非空头的原子追加，真实竞争、64MiB、公开静态资源 E2E、最终 77 包 Go/vet 及相关 race 通过。旧 raw assets 需在空目标重传或停服清理后重传；预签名直读改为 agent 回退，未部署。详见 [修复报告](../reports/2026-11-10-cos-assets-atomic-create.md)。
