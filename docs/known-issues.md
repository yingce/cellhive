# 当前问题、限制与验收状态

_更新：2026-11-15；历史验收按日期列出。当前数据安全修复已通过下述本地隔离验收，不代表已部署或当前线上状态。_

历史问题、逐轮修复与旧阶段清单已移到 [历史记录](archive/known-issues-history-2026-11-10.md)。其中“待实现/验证中”只描述当时状态，不应直接作为当前任务；归档也不等于把未核实事项自动关闭。设计契约见 [decisions](decisions.md)，产品边界见 [compatibility matrix](compatibility-matrix.md)。

## 2026-11-23 KV / Queue 退役巡检

本地将已完成记录移出分钟级 COS 复查。一轮历史遍历结束后冷却一小时，期间继续处理分页和错误。进程重启会重新遍历，积压和故障可能延长回收时间。现有协议尚不能自动证明所有跨节点在途 PUT 和可重放上传已排空，因此还不能永久退出。见[后台定时访问](operations.md#cell-agent-后台定时访问)。

## 2026-11-23 后台 purge 读取（本地验证）

原有每五秒检查 purge 权限的路径在双区 quiet 采样中分别产生 1.20 和 1.19 次 control owner GET/s。本地改为启动检查、删除事务通知和空闲分钟对账。缺失通知或 owner 转移后，持久待办可能多等一个对账周期。线上收益需发布后按来源复测。

## 2026-11-23 共享到期索引（本地验证）

`shared` 已接入 Queue 与持久 timer 的到期提示，默认 `cluster` 无需外部服务。原有 Queue 对账、持久 wake 和孤儿扫描仍按原频率执行。共享提示可能增加 owner 路由点查，生产 COS 净请求量和任务迟到仍待同负载对照。首次预约在持久索引写入中途崩溃的恢复也需单独验收。见[实施计划](superpowers/plans/2026-11-23-explicit-scheduling-mode.md)。

## 2026-11-17 前驱 epoch 精确 cut 的 seek（已部署本地与 USW，SG 未部署）

- 当后继给出精确 cut 时，已发现的高层连续链可作为下一层 seek 起点；只有补链恰好到 cut 才跳过早期对象，否则回退完整发现。最新 epoch（无 cut）仍完整发现，未改变 activation 水位门禁、LTX 正文验证或 GC。
- 回归先红：为补 21 个尾事务扫描 100023 个键、103 页；实现后 `internal/replica` 整包隔离测试通过（1.958s），包括历史跨界/缺口回退、最新 epoch 跨界尾和覆盖 oracle。
- 对本机 DB_WORKBENCH 原始 e68 对象只读验证 `upper=1827120`：发现 436ms，共 11 页/6254 个键，其中 L0 仅 21 个；新计划 77 段。下载校验并分别恢复新计划与事故保留的 29 段链，总计 583ms，两份 3883008 字节镜像逐字节一致，SHA-256 `72ee57e77b60a838b93981bb4ba3981a478ee40c3bdeadbbbeb18c83b4164b38`。证据 `/tmp/kilo/seek-fix-test/live-result.txt`。这是精确历史 cut 的验证，不证明 e68 作为最新 epoch 时全量发现已加速；未部署、未重启服务、未改桶对象。
- 后续本地部署：代码提交 `17ab586756`，镜像 `sha256:a6fc261fca490e16e215cee0011c569a70cf1f4bf7226679f39e4dd56feaba0b`；镜像额外保留此前已部署、未包含在该提交中的 D1 冷启动预算补丁（`internal/d1/d1.go`、`internal/server/server.go`），不是纯提交版。三个服务均 healthy。首次验收 SQL 误用复数表名失败；改用 `task`/`user` 后再次重启 agent，无 stats 预热首查 145ms 成功，bookmark 1827151、task=120、user=3、session_index=26。该次是保留本地库的冷开库，不是删库下载恢复；证据 `/tmp/kilo/seek-release-hijGqA/`。未改现有卷或 RustFS 服务，未部署 USW/SG。
- 随后按用户要求撤回预算叠加：当前本地镜像 `sha256:8336bc80e9608b74b1e53164f4488f6398485308df265a86564d76c4735d1f13` 从 `17ab586756` 干净归档构建，保留 seek，恢复开库与 SQL 共用 30 秒预算、普通 D1 转发超时。三个服务均 healthy，无预热首查 108ms、bookmark 1827151，业务计数仍为 120/3/26。证据 `/tmp/kilo/seek-budget-revert-dYtMH7/rollout.log`。未操作 TKE。
- USW 后续发布：Helm revision 126，仅更新两个 cell-agent，镜像 `sha256:1182c8c9da728d3fde6e78b47e12ca37c07c1d3a3a9c5dd31fa7f419fc34e96a`，使用 `17ab586756` 原始 D1 预算。两个 agent Ready、重启 0，实际二进制 SHA-256 均为 `2c4b42c18f343e4235d6ea579d14187c309f952f0d29e145f4e2b625af7cbd35`；user/DO runtime 与 Vector Pod UID 未变，SG 未操作。独立 namespace `seek-release-20261117-0626` 双向跨 agent KV 写读成功（键 TTL 3600 秒，资源保留）。滚动期间出现 connection refused、owner unowned 和 follower prepare 告警；06:26:15–06:26:51 UTC 采样仅见既有非 owner `purge: retained` 警告，不声称零错误切换或长期无故障。此次未在 USW 复造 17.9 万对象恢复性能场景。证据 `/tmp/kilo/usw-seek-release-OJni77/`。

## 2026-11-10 验证记录

- 本地 E2E 15/15、COS 矩阵 15/15、examples 26/26；Compose/Kubernetes fleet、OSS/MinIO、SQLite 导入导出及冷恢复通过。滚动期间仍有短暂 HTTP 500，经重试恢复；不能宣称零错误切换。详见 [全量报告及未测范围](reports/2026-11-10-full-e2e-examples.md)。
- 原生 DO 不再固定数据 epoch 1；使用已验证 shard owner epoch，迟到旧写者不会覆盖接管者的数据。普通 LTX 上传不依赖 COS forbid-overwrite，也不新增发布协调文件。详见 [对象发布修复](reports/2026-11-10-object-publication-fixes.md)。
- COS assets 原子创建已修复：同 key native append(position=0) 加 16 字节长度头，非空与空文件竞争、64MiB 及公开静态资源 E2E 通过。原先失败证据保留在 [assets 修复报告](reports/2026-11-10-cos-assets-atomic-create.md)。
- 默认 supervisor namespace、Workflow binding alias、R2 HEAD、冷路由竞态及 WebSocket 示例问题已修复并复验；不再作为当前未解决项。

- DO idle retirement 的 owner context 与关闭中 checkpoint 捕获已修复；本地恢复实例的原失败会话 events 复验 200，详见 [修复及剩余边界](reports/2026-11-10-do-retirement-owner-fix.md)。

## 2026-11-15 本机 D1 故障补充

- `PendingTimerMin` 的 SQLite 打开曾允许创建文件：扫描与 owner invalidation 交错时可能重建空库并抑制后续冷恢复。新增回归先失败，改为只读后整包测试通过；源码修复尚未部署。clai 本机业务库已从 RustFS epoch 57 链离线恢复到 TXID 1639454，临时禁用 wake repair 并延长节点租约，存储尾延迟根因和用户验收仍待闭环。范围、证据及配置边界见 [事故报告](reports/2026-11-15-local-d1-recovery.md)。
- capture 历史查询失败的快照 fallback 已在工作区改为 fail closed（未提交、未部署）：epoch、当前 epoch 水位、跨 epoch 水位查询错误均保留原始错误，不发布本地快照，失败初始化可重试。新增空库/落后本地库 6 个场景在旧实现均复现错误快照；修复后 cellcapture 整包及该回归 race 重复 10 次通过。此为独立数据安全加固，不证明本机事故曾触发该 fallback，也不等于已实现所有本地文件可信性校验。
- user-runtime 回收补充（工作区已实现、未提交、未部署）：公开 loader 的 `/ready` 与现有 `/healthz` 一样可调度 idle sweep；`/ready` 使用现有节流/单飞并通过请求 `waitUntil` 执行，不等待退休完成，不改变就绪检查结果。此修改仅覆盖公开 loader，不解决内部 dispatch 的容量、退休重试和 active workflow 生命周期，也不代表已证实或修复全部高 RSS 来源。
- 本批验证：timer 相对路径 URI 回归已修复（`OmitHost`）；最终代码在独立 network/PID 命名空间、空凭据环境和显式临时路径中运行完整 `make test`（禁用测试缓存）、`make js-test` 及 `make e2e` 六套均通过。公开 loader Node 测试 25/25、真实 workerd readiness-only 退休回归通过；主动 abort 仍有既有的 termination stderr。测试不代表已发布，也未复现 RustFS 本机故障全过程。
- 设计边界：单独的 capture fail-closed 不保护只读请求；后续已增加普通冷 activation 水位门禁，见下节，但仍不等于完整内容/身份验证或自动 repair。不得用“无业务表”判定空库，也不得在请求 gate 内阻塞式 invalidate。内部 dispatch 的无界 resident/退役失败丢追踪、TTL 可能中断 active workflow 仍待独立生命周期与 admission 设计，不以提前删除记录或强杀活动任务换取内存回收。

## 2026-11-15 截断修复契约

- 工作区已实现、未提交、未部署：跨节点转发不对合法上游响应静默裁剪为 1 MiB；保留上游状态和端到端响应头，正文完整流式转发。上游正文中途读取失败时中断响应，不能用正常 EOF 掩盖不完整内容；不扩大原接口的请求大小限制。
- Workflow 事件请求体上限保持 1 MiB，按实际读取字节数检查（不信任 Content-Length）；恰好上限完整接收，超过上限返回 HTTP 413，读取失败返回错误，不追加事件或触发执行。此修复不扩大事件配额，也不改变正常事件内容。
- 验证：真实双 Server 跨 owner 的 2 MiB KV、D1 SQL 导出、HTTP/1.1 上游中途断流，以及 Workflow 上限前/恰好上限/上限加一/2 MiB/读取失败均有红绿回归；隔离完整 `make test`（禁用缓存）、`make js-test`、`make e2e` 六套通过。转发定向 race 重复 10 次通过。HTTP/2 和 admin 大响应未单独做 wire 测试；启用 telemetry 时现有 trace wrapper 的非 defer span 收尾可能被响应 abort 跳过，属非阻断可观测性后续项，不影响中断向客户端传播。
- 截断批次未覆盖其余并发协议；随后用户授权的数据安全修复见下节，不能将该批旧 e2e 证据套用于后续更改。GC 默认策略不变。DO WebSocket 持久化输出门仅在 fork 文档记录 TODO，本轮不实施。

## 2026-11-15 数据安全修复契约（本地验证通过、未部署）

最终隔离验收位于 `/tmp/kilo/data-safety-acceptance-ceE1xi/`：完整 `make test`（禁用测试缓存）、`make js-test`、`make e2e` 六套、带 `pagedvfs_test` 的 pagedvfs/cellstore race 重复 3 次、`make build vet` 全部退出码 0，runner 输出 `ALL_VALIDATION_EXIT=0`。包含末轮 NUL 清单歧义和 GC 取消进度修复；此前未完整接线/旧 fixture/脚本出错的运行不计通过。独立复审指出的问题已分别补红绿回归；这不是实网 COS/OSS、多机掉电或升级部署验收，不能覆盖所有故障组合。下列实施契约已落地，兼容和运行限制仍适用。

- pagedvfs：以 cell-agent 持有数据目录独占锁为前提，后台/同步补页、前台 fault 的条件安装与 SQLite 主库写入、截断、注册失效必须共享本地原子同步域。远端取页在锁外，安装时重新验证注册身份和页面状态；不得在 C 锁内进入 Go 取页回调。旧注册或截断前的页不得覆盖新文件或复活已删除页；写入失败后不得发布 hydrated/complete。后台仅在文件 Sync 成功且当前注册完整时报告完成，失败必须保留 paged marker。本条不声称任意多进程共享 inode 安全，也不是 workerd fork 改动。
- Source 生命周期：实现 Close 的 source 必须有可稳定识别的非 nil 指针身份；同一个 source 可以由多个注册共享，最后一个注册的在途读取结束后才关闭，关闭期间拒绝再次注册。非指针/typed-nil 可关闭 source 明确拒绝，避免不可比较动态类型 panic；不实现 Close 的旧 source 行为保留。实际 fd 身份校验采用下述 FILESTAT 构建合同，不读取私有 `unixFile` 布局。
- paged marker：仅 ENOENT 可表示不存在；其他读取错误必须保留原错误并拒绝使用或丢弃本地数据库，不得据此删除文件或触发覆盖式恢复。marker 更新应使用同步临时文件、原子替换和父目录同步，明确传播失败；损坏 marker 与真实 I/O 错误不能混淆，但两者都不足以证明可以丢弃本地副本。格式损坏或存在非法 txid 的 marker 必须拒绝 activation 并保留文件，不再沿用“损坏即删缓存重建”的旧 fallback；缺少 clean 标志的历史格式继续按 dirty 拒绝。既有本地副本可能领先远端，不得靠忽略错误或“没有业务表”自动丢弃。
- 新增协议须分别说明跨节点资格、失效执行者、升级兼容和故障恢复限制；热请求不得新增全桶或全目录扫描。局部回归通过不等于整批验收，未部署的新协议仍需下述兼容与异常处理验证。
- R2 multipart v2 的 `r2/.mpu/v2/states/` 是平台保留控制记录，COS/OSS 必须走既有 native append authority，S3 control wrapper 走同样的控制记录分类；不得误用普通对象不受支持的 CAS 或把整个 `r2/` 解释为 authority。分片正文仍为普通对象。此接线必须有 provider 路由及旧 token 拒绝测试，本地 FSBucket 通过不能代替它。
- R2 multipart v2（已实现、未部署）：新上传使用旧实现拒绝的 `v2-` ID，状态和分片置于既有保留前缀 `r2/.mpu/v2/`。状态含随机 revision 与每分片在途 token，跨 Store 通过桶 CAS 登记资格和确认；不同分片可并行，同分片不可并行覆盖。只有分片写入及移除自身 token 的 active CAS 均成功才 ACK；不确定写入不释放资格。Complete 仅在无在途分片时独占转入 completing；调用最终对象发布后结果不明时保留该状态，不自动重发或超时接管。Abort/过期 GC 先 CAS 转不可复活的终态再清理已登记分片；迟到写最多留下未确认垃圾，终态记录保留并允许后续清理。旧纯时间戳上传不自动转换/删除，新端返回明确旧协议错误，旧端不支持新 ID；升级需统一路由至新实现，已有未完成旧上传需明确重新开始，不能声称混版本透明兼容。Create 不再同步扫描全桶；清理迁至有界分页后台，每次有页/时间预算，缺分页能力不回退全量 List。默认上传寿命保持不变，不能把停止清理当作修复。
- R2 GC 游标必须保存后端不透明 continuation，不能把返回的逻辑 key 当物理 after；空逻辑页带 continuation 合法，重复逻辑对象不能导致永久卡页。一个调度批次不能对每个状态都重新完整枚举 prefix。FS 开发后端如使用冷 GC 的稳定枚举快照，必须与恢复/权威查询的实时 List 隔离，不能引入影响数据恢复的新 TTL 缓存。状态编码须按 authority 实际 64 KiB payload 上限准入并预留最宽终态；分片号集合宜用固定上限 bitmap、只保存有界在途 token，避免每确认一片都重写不断增长的完整 map，不能缩小上传总分片上限来掩盖放大问题。在途名额用尽可明确拒绝重试，不得 ACK 未完成写入。
- 当前 R2 实施采用 1250-byte bitmap 保持最多 10000 个分片，最多 64 个同时在途 token；只限制并行资格，不缩减总分片数。Provider 的隐藏 generation/空逻辑页需按物理 continuation 前进。FS 冷 GC 使用独立稳定 key 快照，一轮只 Walk 一次，最多两个活跃快照、闲置超时清理且 pending 页可续期；不缓存正常 List 或恢复查询。首次快照仍有 O(N) 扫描和内存成本，不宣称硬常数 I/O；目标是消除逐状态反复扫描的平方放大。定向实现证据包括 257 状态跨 37 页仅访问 257 文件，以及 10000 分片累计 CAS JSON 约 36.5 MB；这些不是整批验收或真实云端性能结论。
- GC 分页错误也返回安全续传位置，后台必须保存该位置：只推进已明确成功或确认不存在的删除，失败/结果不明的当前分片仍留待重试。批次取消不能丢掉本批此前完成的工作，否则慢后端会在每次 deadline 后反复处理同一前缀。无新进度的错误保留原游标，错误不等于允许跳过待处理数据。
- metrics 磁盘统计只允许一个扫描刷新者，失败也保留短期缓存/重试间隔，避免并发 scrape 击穿重复全目录枚举；该近似缓存不参与删除安全证明。
- 本地 FSBucket 列举必须传播 context 取消，遍历和返回对象内容读取之间检查取消，避免后台预算过期仍继续全目录工作。该修复不改变分页排序或 ETag 语义；FSBucket 每页仍可能遍历完整 prefix，不能把有限返回条数描述为有限磁盘扫描，也不能据此推断 RustFS/COS 使用相同实现。
- DO `SyncHost` 扫描边界：当前生产 workerd 的 `cellhive-do-host` 固定布局下，逐次读取目标 host 的 `.facets` 与对应 SQLite/必要 sidecar 路径，不遍历其他 host，也不以 TTL/mtime 跳过新 facet 发现。一次同步复用同一份完整解析结果生成 manifest；损坏或不可读索引不得当空列表放行。非已知布局必须显式完整发现或报错，不能用不完整列表取得成功证明；兼容 fallback 和 `SyncAll` 仍可能扫描全目录，不宣称全部扫描已消除。任意自定义 sidecar 不属于该固定布局点查契约。
- 普通已有 SQLite 的首次 activation 增加只读水位门禁：在 opening 单飞内、迁移和 resident 发布之前读取合法的 `cell_meta.txid`，并验证权威恢复链水位。元数据缺失/无效、读取或权威查询失败、`local < durable` 均拒绝且保留 db/WAL/SHM，不做删除或自动抬高 txid。为保留现有本地领先提交/初始建链语义，合法 `local >= durable` 不被此门禁删除或降级；它只证明水位未落后，不证明相同 txid 内容一致或任意文件身份正确。新文件仍走既有恢复/建库，paged 文件服从独立 marker 契约；warm resident 不重复检查，冷验证只规划该 scope 的恢复链元数据，不新增业务热路径全桶 List。自动 repair 与完整内容身份验证不在此最小门禁的保证内。
- purge/资产回收采用持久独占 fence（已实现、未部署）：只有 control owner 可认领任务，必须取得认领记录的远端持久化证明后才允许物理删除。pending 可取消；running/uncertain 不可超时接管、复活或同 key 重用。任务的完成/失败按唯一 job/op/holder 校验；只有原执行者确认全部同步外部 I/O 已完成、没有未知结果，才能释放 fence。上传（包括相同 SHA）、自身 bundle/service pin/assets/StageOnly 发布、资源创建与重叠清理必须共同遵守同一持久事务域。未知删除结果保留阻塞且对外可诊断；在证明旧执行者和未决请求结束之前，不提供自动解锁，不能以一次 404 或重试成功代替证明。这收紧了历史自动跨进程续跑语义：有不确定结果时优先保留数据，而非自动恢复删除能力。现有物理 key 方案要求升级前排空旧清理进程和请求，不支持旧版本与新 fence 混跑。正常成功清理后仍可重建，不能以永久禁用 GC 代替协议修复。
- 删除完成后的引用保护：对象/token 的 deleted 记录不能随 running fence 一同消失，否则删除前已完成预查的旧 Deploy 可以在删除后发布悬空引用。bundle 仅在同 SHA 内容重新上传并取得成功 receipt 后解除 deleted；多文件 assets token 必须有显式 manifest+seal，按完整文件集合及内容校验成功后才允许复活发布，不能以单文件 Put 成功解除。采用增量兼容：未删除的旧 token 保留原行为；已删除 token 的旧逐文件接口可以写入重建草稿，但不能自行恢复可发布状态，需新 seal 流程（原生CLI/Cloudflare上传路径配套）。没有完整清单的旧客户端重用已删 token 时应返回明确需重新完成上传错误，不猜测完整性，也不通过热路径 List 推断清单。直接 DeleteWorker 的资产删除必须统一进入受保护清理路径，不能保留旁路。
- seal 的完整性必须有可验证承诺，不能只信请求声明“这是完整清单”：原生目录 token 校验排序 `path\0sha256\n` 的内容摘要；CF token 校验其规范化 manifest 摘要及完整路径集合，不能将不透明 Wrangler hash 冒充 SHA256。已建立的 token 清单/实际 SHA256 承诺在 GC/purge 后仍保留，复活不能缩减集合或改同 token 内容。缺少可信原清单且无法由 token 重建证明的旧已删除 token 不接受任意新子集清单；明确要求重新发布新 token，而不是放松验证。明确条件失败且无在途写入的上传可以按 op/holder 持久取消预约；超时、取消或未知写入仍不得借此解除 fence。
- 原生 manifest 的路径禁止 NUL，上传 key 与 token 计算须共同检查，防止将 `path\0sha256\n` 分隔符注入路径后产生不同文件集合的同一编码。合法本地文件名不存在 NUL，不改变既有正常 token 算法；不能仅依赖 FS 的路径拒绝来推断所有 provider 都拒绝。
- CF 旧 token 的安全重建：普通 session 继续使用规范化 manifest 的确定性摘要；仅当该旧 token 已被删除且缺少可信历史内容承诺时，服务端可签发带该摘要及随机后缀的新不透明 session token。客户端不必改文件内容，也不能借新上传解除旧 token 的 tombstone。新 token 必须仍绑定原 canonical manifest，首次完整 seal 后持久保存实际 SHA256 清单，后续复活按该承诺验证；正常已有承诺的版本不牺牲跨 session 去重。若接口约束不能支持后缀，必须明确失败，不回退允许未知旧 token 复活。

### 冻结复验及剩余扫描成本

另一次最终冻结复验位于 `/tmp/kilo/data-safety-final-vGFjix/frozen-*.log`：完整无缓存 `make test`、`make js-test`、六套 e2e、分页特殊 tags 的整包 race 重复 3 次及 `make build vet` 均退出 0。验证前后所有源码及构建文件 SHA256 清单一致，runner 输出 `SOURCE_UNCHANGED=1`、`ALL_VALIDATION_EXIT=0`。末轮 NUL 路径拒绝及取消后保存 GC 已确认进度均包含在这份冻结快照中。

修复重复扫描不等于消除所有冷路径遍历：FS 冷 GC 初次快照仍为 O(N) 枚举/内存与 O(N log N) 排序，极大目录可能超过首轮 deadline；普通 FSBucket 分页、wake repair 的发现、purge/资产候选发现及恢复链规划仍有枚举成本。全局 paged C 锁跨本地 I/O，慢磁盘可能阻塞其他 paged cell，尚未性能压测。RustFS 自身 capacity scan 未修改，不能把此前 512560 文件统计超时描述为本批已经消除。

### 最后接线与扫描边界

最终验收也包含生产 `cell-agent` 的 `pagedSource` 指针接线；接口方法仅由指针实现，避免可关闭源再次以值进入注册。专项测试先红后绿，之后重新执行完整 test/js-test/e2e、特殊分页 race 及 build/vet，均退出 0，日志位于 `/tmp/kilo/data-safety-acceptance-ceE1xi/`。

已消除的放大包括单 host gate 枚举其他 host、R2 Create 同步全局扫描、multipart GC 逐状态重复全目录扫描、metrics 并发刷新击穿及取消后重复清同一前缀。FS 冷快照首次仍有 O(N) 扫描/内存和 O(N log N) 排序；普通 FSBucket 分页、wake repair 发现、purge/资产候选发现和恢复链规划仍有冷路径枚举。极大首次快照超过 deadline 时仍可能重复初始化；全局 paged C 锁跨本地 I/O，慢磁盘影响尚未基准评估。RustFS 自身 capacity scan 未修改，本批不声称解决其 512560 文件统计超时，也不代表线上文件数已下降。

### 分页文件身份的受支持构建合同

前台实际 inode 校验采用 pinned go-sqlite3 的公开 `SQLITE_FCNTL_FILESTAT` 操作，受支持构建显式启用 `SQLITE_ENABLE_FILESTAT`（不启用 SQLITE_DEBUG）。读取当前文件对象返回的诊断 JSON 中实际 fd，在对象仍存活时立即 fstat 比较 dev/ino；只依赖固定依赖版本的 JSON 合同，不读取私有 `unixFile` 布局、不拥有或关闭底层 fd。缺少该能力、格式错误、fd 无效或身份不符均拒绝 paged 打开，不回退为路径 stat 猜测。Makefile、Docker 与隔离验证命令须一致启用该编译选项；直接 Go 构建需提供相同 CGO_CFLAGS。升级 SQLite 必须重跑这一合同测试；非 paged SQLite 行为不改变。此前标记的前台身份阻断只有在该接线及确定性回归完成后才可关闭。

namespace 本地删除也必须有完整生命周期门禁：删除开始后拒绝该 namespace 的新 request admission/Cell 打开，等待已开始 opening 完成后再枚举并逐 scope 排空、关闭和删除，最后才 RemoveAll；取消或关闭失败不能继续总目录删除。该本地门禁不替代 control owner 的持久 purge fence，也不声称跨节点互斥；其他 namespace 不受影响。不能只枚举当前 resident 而遗漏正在恢复的文件。

## 2026-11-15 非 owner 后台控制读取修复（本地验证通过，2026-11-16 已部署 USW）

USW `agent-1` 保留旧 control SQLite、而 `agent-0` 为有效 owner 时，后台 Queue/Cron 直接读取旧文件被水位门禁拒绝。本批统一调度 projection、跨 namespace queue registry、workflow schedules/concurrency 的读取来源：仅有效本地 owner/session 可以读取本地 control，其余通过有界、认证的内部只读请求访问当前 owner。读取前后核对 node/session/epoch；错误 receiver 拒绝，不互相递归转发，不为配置读取主动 claim/reset。失败不得回退旧 SQLite 或将 authority 故障解释为空列表/无限并发。已确认无配置可使用既有默认值。

复用现有 owner 判定、请求签名和本地 read fence；资源 generation 准入和 cleanup authority 校验保持不变，不增加第二套控制库复制机制。合并并发相同读取并做短暂错误退避，避免每秒重新走落后副本的冷 List。现有 5 秒 projection 缓存只用于调度视图，strict 消费配置不得用该缓存；取消不得返回过期缓存，已知 owner 变化应失效旧视图。移除无条件启动预打开，控制写入仍由现有 `ensureControlSchema` 在 capture 前初始化。验收需保留真实旧副本，覆盖 owner 更新、切换、请求失败、并发和后续准入，不能靠删缓存或关闭 LocalDurable 让测试通过。

相邻 maintenance 读取也不得继续打开非 owner 的 control：Workflow retention 的配置源走相同 reader，但实际 prune 仅在 workflow 数据 cell 的有效本地 owner/session 下，经资源准入与既有 CaptureWrite 执行；audit prune 仅由 control owner 经同样持久化路径执行。未持有数据 ownership 时跳过，不为清理主动接管，不因远程配置可读而允许改本地非 owner 副本。保留原保留时间和 SQL 条件，无主或 owner 不可用期间维护可以延迟；既有 resource cleanup bootstrap 负责正常无主接管，不在只读 reader 内引入新选主机制。

缓存中的 projection 与产生它的 authority generation 成对返回；初始未读和后续失败不能用同一个空 key 命中旧结果。Cron enrichment 每次经过共享 projection cache，不再叠加独立 5 秒 TTL。已知 authority 变化或失败会失效旧视图，但调度缓存仍是最多 5 秒的非授权视图；strict consumer/policy 查询不使用该缓存。内部读取接口仅允许调度所需操作/类型，不开放通用 SQL 或任意资源 secret 读取。

验收证据位于 `/tmp/kilo/control-owner-fix-jCg5FR/`。完整无缓存 `make test`、`make js-test`、六套 `make e2e`、`make vet` 通过，源码清单与最终实际运行的 cell-agent 二进制哈希已核对。定向 owner 变更/不同 session/无主不 claim/认证/单飞退避/取消/缓存交错/非 owner maintenance 保留副本回归 `-race -count=10` 通过。真实 RustFS 对照使用相同 85 秒观察预算：B 曾实际拥有 control，正常退出后 A 接管并更新 v2，B 保留原 data-dir 返回；旧发布二进制持续 `local 10 < durable 13` 且 Queue/Cron/Workflow 不执行，候选三者均执行 v2。两侧验收期间 A 的 node/session/epoch 不变，B 旧控制 DB 字节哈希不变。最初 fixture 的保留 namespace 导出拒绝和未等待 disowned 子进程退出不计为产品红灯；已改用测试进程正常退出和 pidfd 等待，没有放宽断言或线上处置。

维护写的末尾 fence 是结果校验而非 SQL rollback，持久化仍依赖既有 capture/epoch fence；本批未穷尽所有并发退役、掉电或真实 COS/OSS 故障。新增内部只读接口不改变存储格式，但新 reader 遇到旧 owner 尚无该接口时会报错退避，不能回退本地，因此发布需要考虑 owner/receiver 的更新顺序。

2026-11-16 USW 发布记录：Helm revision 125，仅更新两个 cell-agent，镜像 `sha256:639e0edc628eeb343c096cd95b598d4a9661810f2a8d9de1530c0354384234fa`，源码验收清单摘要 `4c694d36c4fb0b8dff112c46a15b86e36655be8527b8ed98aaacac12c794bb92`。两个 agent Ready、重启 0，user/DO runtime Pod UID 未变，SG 未操作。03:38:23–03:42:07 UTC 观察窗口内未再出现旧水位、Queue/Cron 或 retention 读取错误；非 owner 队列配置查询、隔离命名 KV 跨节点写读成功。滚动过渡的旧接口 404 已停止；非 owner `purge: retained / not the owner` 警告仍保留，不能宣称零警告或长期无故障。证据在 `/tmp/kilo/usw-control-release-WsWk2J/result.json`。

## 当前限制与上线边界

1. **发布范围**：本节历史本地验收不等于所有区域已上线；USW control-reader 的已知发布状态见上段。SG 未在本次发布中变更，其他修改和时点不能从单个 release 记录类推。
2. **离线迁移**：旧 LTX/固定 e1/旧 R2 sidecar 与新布局不混跑；数据库按 SQLite export/import 迁移。COS raw assets 需要空目标重传或停服清理后重传，仅重发同内容版本仍会命中旧 token。
3. **COS assets 读取**：不再直接预签名物理对象，走 authenticated agent 点读并返回逻辑 Content-Length，保留 runtime 小文件缓存。增加了首次读取对 agent 的依赖，未声称性能提升。
4. **R2 旧正文回收**：不可变版本保留以保护进行中的读取及签名 URL；安全在线版本 GC 尚未实现，物理占用可能大于逻辑 usage。见 [对象发布报告](reports/2026-11-10-object-publication-fixes.md)。
5. **provider 边界**：MinIO/S3 控制记录以条件 PUT tombstone 适配，不能据此声称原始条件 DELETE 可用；不支持的条件物理删除不可降级为无条件删除。COS/OSS 使用原生追加权威记录，其他桶仍需逐桶探针。见 [OSS/MinIO 报告](reports/2026-11-10-oss-minio-acceptance.md)。
6. **规模与环境边界**：本地计算访问真实云桶不等于生产同区/跨主机多 AZ 验收；滚动可用性、长尾需分开评价。100 epoch 深链专项暂缓；最新全量 E2E 未重跑全部可选大库组合，历史 10GiB 原生 DO 证据仍保留于 [大库验收](reports/2026-11-09-final-acceptance.md)。
7. **产品待办**：Hyperdrive、Workflow/Queue 兼容范围与后续性能评估见 [TODO](todo.md)、[bindings](bindings.md)、[compatibility matrix](compatibility-matrix.md)。不因存储验收通过而扩大产品能力声明。

8. **多 class DO 恢复映射已修复并在本地恢复实例验证**：host 按 storage_class 隔离，facet scope 不再依赖最后一次 Bind；错放的 SchedulerDO 数据已停机校正并逐字节验证。真实会话 events、SchedulerDO health/status、多 class 独立 epoch 冷恢复、idle 和导入导出均通过。见 [修复与验收边界](reports/2026-11-10-do-retirement-owner-fix.md)。

## 证据入口

- [全量 E2E/examples](reports/2026-11-10-full-e2e-examples.md)
- [COS assets 修复与专项复验](reports/2026-11-10-cos-assets-atomic-create.md)
- [存储与请求性能](reports/2026-11-10-storage-performance.md)
- [原生 DO 导入隔离](reports/2026-11-10-native-do-import-exclusion.md)
- [历史问题记录](archive/known-issues-history-2026-11-10.md)

- 2026-11-10 后续 DO capacity：并发冷打开重复 admit 覆写身份，导致 native 构造拒绝缓存。已加 per-facet in-flight admission 合并，26 项 JS 和真实并发冷读/idle 回归通过；指定会话 48/48 请求恢复 200。本地恢复实例已更新，详见 [报告](reports/2026-11-10-do-retirement-owner-fix.md)。

- 2026-11-10 驻留配置：普通 cell 默认软上限已由32调至128，空闲5m不变；Go 配置测试、Compose 校验、Helm 渲染通过。本地恢复实例已显式设128，metrics确认 max_open_cells=128、idle_ttl_seconds=300。未修改外部生产集群。

- DO 重接管 restore_500：整进程重启方案已撤回，源码改为目标 Host/shard 的 native detach 后清理与新 epoch 恢复。本地及真实 HK COS 同进程重新接管、跨节点写入后接回通过；尚未部署。见 [局部恢复验收](reports/2026-11-10-do-owner-recovery.md)。

- DO 版本变化且旧 WebSocket 常驻时返回 do_runtime_capacity：源码已改为单 facet 切换、关闭旧连接并等待 native detach；真实 fork 测试 16 并发切换、状态保留和无关连接存活通过，尚未部署。另修复 WebSocket-only facet 首次退出前映射未刷新导致的捕获 scope 错误。

- 2026-11-11 Hello 并发502：已在本地复现调用日志连接积累/子进程1024句柄耗尽，以及无日志时事件循环延迟处理健康连接。响应释放、I/O公平轮询及watchdog有界排空修复验收中，尚未部署。

2026-11-11 Runtime 502 本地验收：完整修复版 Hello 1/8/32 并发短测零失败，32 并发持续 120 秒 150150/150150 成功；KV 33553/33553 成功并校验内容。Hello 子进程句柄峰值79，未发生健康检查失败或重启。代码尚未部署；详见 [验收记录](reports/2026-11-11-runtime-502-fix.md)。

2026-11-11 遥测慢服务隔离（实施中）：调用日志与 spans 共用每 isolate 最多16个并发导出，无等待队列；超额遥测丢弃，不影响业务数据。导出覆盖响应头及响应体的3秒超时，响应体最多64KiB，正常读取释放连接、超限取消。业务响应和 resident release 不等待导出，保留 ctx.waitUntil 生命周期管理。需验证慢响应、挂起响应体、不可用及饱和时业务请求成功。

2026-11-11 冷加载 admission 修订：同一不可变 Worker 版本的 preparedInflight 等待者复用加载，不重复占用 coldLoads；保留16个不同冷加载的资源上限。避免32并发首次访问同一 Worker 时误报 cold-load capacity。

2026-11-11 遥测隔离/冷加载补充验收：真实 workerd 32 并发，首次冷访问及日志服务挂起响应头/响应体、断连、503、恢复六阶段，共 131118 次请求零失败；无 unhealthy/进程退出。相关单测30/30、loader 全套49/49通过。此处为本地验证，尚未部署。

2026-11-11 本地文件日志/Vector：user-runtime、DO共用有界异步spool，DO不再等待span/统计上报。真实Vector停机、重启、轮转补采集和native DO慢span测试通过；采集停机32并发27174次请求零失败。日志仍是best-effort，Pod删除/长时间积压可丢遥测。代码/Helm尚未部署，见 [验收记录](reports/2026-11-11-runtime-file-logs.md)。

2026-11-11 Go遥测转发修订（未部署）：撤回未上线的Vector sidecar，改为Go读取本地spool、批量OTLP直连CellHive Vector；Trace也走可信本地pipe。真实Vector验收发现native OTLP日志解码保留resourceLogs整批envelope，与现有逐条remap不匹配，已改logs解码为Vector事件格式；traces保持原生OTLP供后端转发。日志/Trace验收和故障边界见 [Go转发报告](reports/2026-11-11-runtime-go-export.md)。VWork资源未改。

2026-11-11 DO入口合并（验收中，未部署）：生产默认 `do-runtime` 一次启动即包含render和持久化supervisor，移除独立 `do-supervisor` 产物/启动脚本。原输出门、续租与退出顺序保持；隔离测试使用显式 `-gate=false`。README中英和部署入口同步，Service/PVC命名保持。

2026-11-11 DO统一入口本地验收：相关Go/native测试及导入导出、多class、idle重开、双agent双runtime分页接管E2E通过；两次crash后epoch依次1→2→3，数据保持。使用本地FS bucket和真实fork，不等于本次COS验收或生产上线。见 [统一入口报告](reports/2026-11-11-unified-do-runtime.md)。

- 2026-11-11 节点续租：已完成无截止时间的续租 I/O、丢失响应确认及节点换段清理阻塞的代码修复；本地 race、owner/server 回归、双进程 session-recovery E2E 通过。真实 COS 故障注入尚未进行。原租约过期仍停止服务。未部署；线上具体阻塞请求及恢复 fragments.log 缺失仍未定位。

- 存储 debug 日志需在 cell-agent 启动时设置 `CELLHIVE_LOG_LEVEL=debug`；本轮未在线开启。逐 HTTP 请求的 SDK 重试标识仅在 SDK 提供时记录；本地序号不是跨节点 trace ID。

2026-11-11 SG rollout exposed stale workerd HTTP-client addresses after headless agent Pod IP changes. Live user-runtime sockets continued SYN_SENT to retired 10.0.16.82:7001 while DNS returned current endpoints. Stable general/per-ordinal ClusterIP routing is being implemented; this alone does not prove every historical Capability deadline exceeded had this cause. USW also observed one lease_replaced restart after ambiguous renewal; that separate issue is not resolved by routing changes.

2026-11-11 部署更新：稳定 agent ClusterIP、统一 runtime、Go 日志出口已部署 SG 131 / USW 118；新 Pod 均 Ready、采样重启为零，SG 公开 KV canary 12/12 成功。旧 Pod IP 连接残留在新 runtime 网络采样中未再出现。上述独立的 USW lease_replaced 问题保持开放；本次不宣称全部历史超时已消除。

2026-11-11 USW lease_replaced 已定位并修复源码：超时后读到旧值过早清除 pending，下一次续租把自身延迟提交判为竞争者。现在保留并精确重试原条件操作；本地复现、race、真实 HK COS 4 项故障注入（含换段）及双进程恢复 E2E 通过。尚未部署，原始网络超时原因仍未确定。见 [验收记录](reports/2026-11-11-lease-delayed-commit.md)。

2026-11-11 发布更新：上述 pending 续租修复 `3e08b76` 已部署 USW 119 / SG 132，四个 agent Ready、零重启，采样未见续租/fencing 错误；SG KV 12/12 成功。仅更新 agent，原始网络超时原因仍未确定。

2026-11-11 gN 回收实验：新增显式 opt-in 的根前后验证、写确认和物理回收；默认保留占位对象，未部署。105 代正常换段只剩根＋当前段、并发 race 10 轮、真实 HK COS 旧段重建/拒绝旧权限/再次 GC 测试通过。warm Get 由 1 次变 3 次对象读取；旧进程不能混跑；根增长、生产启用与后台调度尚未解决。见 [实验报告](reports/2026-11-11-authority-reclamation-experiment.md)。

2026-11-11 管理请求转发修复：逐节点 stable Service 必须同时暴露 7001（peer/data）和 8082（admin）。遗漏 8082 导致非 owner 接到 releases 等管理查询时转发等待 30 秒后返回 502；直接访问 owner 正常。补齐 Service 端口，不修改鉴权、owner 协议或镜像。1/2/3 副本渲染回归覆盖两个端口。

2026-11-11 gN 实验第二轮：热读从初版 3 次对象读取降为 2 次（段读取后必须验证永久根）；冷读仍为 3 次，生产默认模式仍为 1 次。回收 API 改为显式游标分页，扫描候选/删除均有界，并保护非规范 g00 等名称；部分失败可重试。真实 HK COS 验证热读次数及回收安全。未部署，混合旧客户端、生产启用门禁、后台调度与永久根增长仍未解决，不能据此上线。详见实验报告。

2026-11-11 USW 回收启用准备：增加 legacy/validate/reclaim 三种模式与显式 ACK。validate 不删除对象，但读写验证与 reclaim 相同，可分两阶段滚动升级。运行时开关已实现，部署/验收未完成；ACK 只表示运维确认，不能自动阻止未知旧客户端。回退只能到 validate，不得退到 legacy。旧 backlog 补扫和永久根增长仍未解决。

2026-11-11 USW 已分阶段启用：Helm 121 全部 validate 后，Helm 122 全部 reclaim；两 agent 实际镜像/二进制和模式核对通过，真实 USW 独立 scope 回收探针通过。仅正常换代回收；历史 backlog/失败补扫与永久根增长仍未解决。回退必须保持 validate，不得回 legacy/旧二进制。SG 未启用。详见 [发布记录](reports/2026-11-11-usw-authority-reclamation.md)。

2026-11-13 当前部署状态：SG 已完成 Helm 134 validate → 135 reclaim 两阶段启用，与 USW 一致；两 agent 实际镜像、模式及 Ready 核对通过，SG 独立 COS 探针通过。随后观察到节点租约和 waker 的 g46 已删除、g47 正常写入；历史占位、失败补扫与永久根增长仍未解决。只能回退到支持根校验的 validate 模式。见 [SG 发布记录](reports/2026-11-13-sg-authority-reclamation.md)。

## 2026-11-22 换代后异步 Fold（实施中，未部署）

设计改为接管后后台收敛恢复链，取消同代 idle/pre-release 自动 Fold。同代仅 L0→L1，长期热 cell 的 L1 与冷恢复成本仍可增长。预算内完整 L9，超预算旧基线加 L2；旧读者不自动切换，GC 安全门保持。单元回归、真实桶/故障与部署验收状态另行补记。见 [设计](superpowers/specs/2026-11-22-epoch-fold-design.md)。

2026-11-22 本地实现验证：换代后台维护与完整快照策略已实现，8 个新增定向用例含 race 通过，make build 与相关包 vet 通过。make test 的 75 个包通过，但 WAL 测试导入环导致全量失败；完整 compaction race 的 GiB 用例超时。未提交、未部署，真实桶/多节点故障未验收。见 [验证报告](reports/2026-11-22-epoch-fold.md)。

2026-11-23 纯读接管修订：GET/List 不初始化 capture，原异步 Fold 会持续等待本代接链。后台现对已驻留 cell 安全初始化 continuation，限定预期 epoch、发布前复核 owner，复用 capture 初始化串行化；不冷开库、不回退完整 snapshot，保留 Fold 未接链保护。原测试手动预置本代对象不能证明纯读路径，已替换为真实 HTTP GET 接管、无后续业务写入的回归，并增加失败安全用例。本地验证、未部署；详见 [报告](reports/2026-11-22-epoch-fold.md)。

2026-11-23 空链 Fold 优化：恢复规划成功且链为空时休眠，只比较本进程的 L0 上传水位；水位变化、换代或重启后重新发现。扫描前记录水位，避免并发上传丢失唤醒；L1 覆盖 pending 不影响唤醒。读取失败和等待 continuation 仍退避重试，已自包含 epoch 不刷新。不增加通知队列或桶轮询。FS bucket 定向 race 验证空闲窗口 List 由 4 次降为 2 次，后续零新增；未做真实 COS 负载验证，未部署。详见 [报告](reports/2026-11-22-epoch-fold.md)。

2026-11-23 同 owner 重开维护登记修复（本地验证通过，未部署）：驱逐会 Forget 本地 OwnedScopes，但请求快路径确认同 node/session 后直接服务，未重新登记，导致 GC 与异步 Fold 漏掉重开的 cell。请求已验证 live owner 后恢复本地 advisory 登记，不新增桶 I/O、不改变 owner/epoch，不将其他 node/session 或退休记录加入本地集合；后台执行前仍重新校验 owner 和恢复链。回归覆盖真实请求驱逐重开及安全过滤，结果见 epoch Fold 验证报告。


2026-11-23 本地部署验收更新：当前未提交改动已部署本地 cell-agent/do-runtime/user-runtime，生产未部署。独立真实 MinIO 多节点验证通过纯读接管、GET/List/PUT 驱逐重开登记、固定切点并发尾部、L9/L2 两种预算路径、跨未完成中间代 Fold、Safe GC、SIGKILL 后已确认数据恢复、旧 epoch 拒绝、空链休眠/上传唤醒和阻塞 PUT 的 bucket-proof；实际 GiB 普通测试通过。独立 GC 仍会扫描空链，不能把 Fold 休眠解释为整个服务无桶扫描。既有 session 进程测试缺少 B 控制库文件，在新旧镜像都失败；全仓 WAL 测试导入环仍开放。默认一小时 GC 自动删除与真实 COS/生产负载尚未验收。完整证据及测试配置见 [报告](reports/2026-11-22-epoch-fold.md)。


2026-11-23 测试阻塞修订：WAL 集成测试改为外部测试包以解除导入环；session 进程测试显式构造缺少注册的旧控制库，并在故障注入前等待准备阶段 spool 上传，避免依赖非 owner 自动建库及误删启动阶段唯一持久副本。生产协议与部署不变。真实双进程恢复原有断言通过，相关用例 race 驱动下连续三次通过；详见 [报告](reports/2026-11-22-epoch-fold.md) 文末。此记录取代此前两项测试阻塞开放的状态。

2026-11-23 最终复验：上述测试修订后的 `make test`、`make vet` 全仓通过，独立目录 `make build` 与 `git diff --check` 通过。此前 WAL 导入环和 session 缺文件阻塞已解除；生产未部署。


2026-11-23 USW 发布：epoch Fold/纯读接链/重开维护登记/空链休眠/bucket-proof 批次 `351c23b7` 已发布 USW Helm revision 132。真实 KV、原生 DO 和 Worker 服务路由通过；TEST 只读重开后自动完成 e17 Fold，生产后台 GC 删除 e14/e15 共 630 个对象、保留 e16/e17，退休下界推进到 17。另有 `vwork-1/__queue__/jobs` epoch=29 因 local TXID=2、durable=1 安全拒绝 continuation，跨代维护仍延期，未修复或人工改数据。SG 未部署。详情见 [发布记录](reports/2026-11-23-usw-epoch-fold-release.md)。


### 2026-11-23 Queue 资源寻址与 schema 捕获修订（已部署本地与 USW）

消费者以资源登记的 `<ns>/__queue__/<storage-id>` 为唯一数据/owner/claim/ack/retry/删除代次身份；业务名称仅用于消费者配置匹配、日志和发送给 handler 的 `batch.queue`。DLQ 同样从本 namespace 的 queue 资源登记解析，不把业务名直接拼为存储路径；缺失或无效映射必须拒绝该消费者，不能退回名称建库。跨 namespace/class 的 scope 拒绝。

Queue 冷开/重开首先只读检查 schema；已完整迁移的库不执行空事务、不增加 TXID。实际 schema 初始化/升级经过正常 capture 和持久化确认，确认失败不得把该本地句柄标记为已就绪；同一 cell 初始化串行。后台 Fold 保留本地/持久水位相等与 owner 校验，不通过篡改 TXID 或强制快照消除未知差异。旧错误路径保留数据，先只读核查消息和引用，不自动迁移或删除。验收覆盖别名≠存储ID、handler 名称、死信、重开、失败重试、真实收发及接管。

- 真实 Worker 回归补充：Queue binding 授权已解析的 scope 必须贯穿发送、批量发送、claim、ACK/retry 与 admission/owner gate；转发保留原请求名称以便接收节点重新鉴权，禁止改写 URL 后造成 token 名称不匹配。

2026-11-23 Queue 修复本地验收通过：真实别名生产/消费/ACK/重试/DLQ、启动 spool 上传后的双节点接管、schema 升级恢复均通过，全量 Go test/build/vet 与相关 race 通过。已有启动 spool-only 窗口不能由另一节点恢复，旧二进制亦复现，本批未修改该策略。旧 jobs 副本只读检查为空，未删数据。USW 已发布 revision 133，604 秒/11 次采样通过，六个服务 Pod 全程 Ready、零重启。见 [验证报告](reports/2026-11-23-queue-scope-durability.md)。

2026-11-23 SG 发布：复用 USW 已验证的源码 `9dcacebc` 镜像，SG revision 136→137；原生 Queue 收发/ACK/重试/DLQ 与 DO SQL 写读通过。601 秒观察中服务 Pod 全部 Ready、零重启，HPA 3→2 缩容导致两次本机端口转发探针断连，重连后通过。另有 vwork-105/pageverra-api scheduled 聚合任务主动抛错、Cron 500 持续，具体子任务未确认，未修改租户代码。保留 authority/storage 配置，USW 未再次更改。见 [SG 发布记录](reports/2026-11-23-sg-queue-release.md)。

## 2026-11-23 Queue 发现与控制面 COS 请求（本地验证通过，未部署）

SG 约 172 秒采样记录两台 agent 合计 15099 次 GET 类操作；未观察到恢复文件或按需读页计数增长。已确认 Queue 每秒读取资源列表、成功结果未缓存，以及本地控制面读取重复 owner 检查的放大路径；指标尚不能将全部请求归因于此。按已批准方案加入 5 秒发现缓存和本地前后校验合并，安全边界及验证计划见 [实施计划](superpowers/plans/2026-11-23-control-read-cost.md)。上线请求降幅未验证。

本批 make test/build/vet 与相关 race 通过，计数与生产验证边界见 [报告](reports/2026-11-23-control-read-cost.md)。未提交、未部署，不能认为线上 88 次/秒已消除。

2026-11-23 SG 后续发布：源码 `0620be29`，Helm revision 138，仅滚动 cell-agent。真实 Queue 功能通过，但停用新增验收消费者后的 GET 类操作仍约 89.40/秒（发布前 89.79/秒），没有显著总量下降。COS 请求过多的问题尚未整体解决；见 [发布报告](reports/2026-11-23-sg-control-read-release.md)。USW 未更新。

- 2026-11-23：recovery 每 5 秒重复读取历史 node-log 放大 COS GET；已实现 60 秒有界发现缓存与 lease 变化即时刷新，保留同步恢复和权威校验；已发布 SG revision 139；稳定窗口 GET 从 82.85/s 降至 49.92/s（约 39.75%），剩余后台读取未消除。

- 2026-11-23：Queue、Cron、孤儿 Alarm 已在本地改为分钟级正常对账。Queue 每轮对账的工作量仍随已登记的消费者数和节点数增长，漏通知和孤儿唤醒可多等一个周期，再加 lease 到期、恢复和执行耗时。节点续租、恢复、授权刷新等仍会访问桶。生产 COS 降幅待部署验证，见[本地对比](reports/2026-11-23-low-frequency-dispatch.md)。

- 2026-11-23：资源清理分钟级对账与 recovery 同轮枚举复用在本地实施。漏通知的任务可能多等一个对账周期；长退役列表每轮仅推进一页。两区实际 COS 404 与总请求降幅待发布采样。

- 2026-11-24：已本地实现并验证 pinned KV 按需授权、sealed node-log 发现过滤与 discovery lease 有界观察复用；全量 test/build/vet 和相关 race 通过，已从独立发布分支部署 SG / USW / GZ；总量与剩余来源见 [发布验收](reports/2026-11-24-three-region-read-reuse-release.md)，不代表所有后台 GET 已消除。见 [设计与验证](reports/2026-11-24-celld-read-reuse.md)。

- 2026-11-24：剩余 GET 已完成 SG 限时 HTTP debug 与三地区来源采样；SG revision 146 已恢复普通日志。确认正常续租 4 GET + 1 Append、2 秒 peer 刷新、Recovery 5 秒选主与 Cron/wake 多阶段读取是主要开销。agent-1 日志存在轮转缺口，双节点 metrics 完整；无任务 span，不能称完整分布式 trace。本次未改请求逻辑，见 [诊断报告](reports/2026-11-24-get-trace.md)。

- 2026-11-24：peer/recovery 有界观察复用及 30 秒恢复维护周期已从独立源码 `892c9797` 部署 SG/USW/GZ，租约 TTL 仍为 10 秒；全量 test/build/vet、相关 race 和十分钟生产健康验证通过，见 [发布记录](reports/2026-11-24-peer-recovery-release.md)。将延后后台发现故障/新 session，显式接管校验与同步恢复保持；共享 fleet 扫描和 node-log/lease 合并尚未实施，不能宣称后台 GET 已整体解决。见 [设计](reports/2026-11-24-peer-recovery-read-reuse.md)。

## Redis/Kvrocks 权威 adapter（2026-11-24）

最新状态：本地 Compose 与 SG/USW/GZ 已切换 Redis；三地区同时启用 recovery index v2，见 [三地区发布记录](reports/2026-11-24-all-regions-redis-recovery-release.md)；本节此前“未部署生产”为切换前验收状态，见 [GZ 发布记录](reports/2026-11-24-gz-redis-fleet.md)。

新增可选协调权威后端；本地 Redis/Kvrocks 集成、双进程恢复、全量 test/build/vet 与关键 race 已通过，未部署生产。见 [验证报告](reports/2026-11-24-redis-authority.md)。与旧二进制不支持混跑；切换须离线迁移。Redis 元数据丢失不支持自动重建或透明切换至可能回退的副本。普通启动必须拒绝缺失 generation，不能把旧授权当不存在。业务数据与恢复日志仍在原存储。契约见 [设计](superpowers/specs/2026-11-24-redis-authority-design.md)。

## Redis 调度补齐（2026-11-24）

已补充事务 outbox、Cron/清理共享 deadline 和真实 workerd 验收脚本；待发布 outbox 暂缓驱逐，但不阻挡其他干净 cell。此前 Redis owner/KV 恢复演练不等于全部定时业务验收。见 [设计](superpowers/specs/2026-11-24-redis-scheduling-completion.md)，当前范围及验证结果见 [调度报告](reports/2026-11-24-redis-scheduling.md)，未部署生产。

- 2026-11-24 完整 E2E 新发现并本地修复启动 upload-spool 顺序：旧异步重放可能晚于接管/恢复，导致 SQLite 恢复旧桶切点后落后于上传尾部。改为恢复前严格同步重放，失败/损坏/历史 quarantine 阻断启动并保留文件。默认/object、Redis、Kvrocks 六套矩阵及故障验收进展见 [调度报告](reports/2026-11-24-redis-scheduling.md)。尚未部署生产。

- 2026-11-24 shared 即时接管已补齐：到期提示确认 owner 已死后走原恢复/CAS 门禁，失败提示条件重试；live owner 和 retired 防护保持。禁用分钟级扫描对本次验收的贡献（周期 10 分钟）后，Redis/Kvrocks 后端恢复并重启 agent 至 Queue/Alarm 完成分别约 3/2 秒；本地单次小规模结果，未部署生产，非生产 SLO。见 [调度报告](reports/2026-11-24-redis-scheduling.md)。

- 2026-11-24 配置已收敛为 `CELLHIVE_COORDINATION_BACKEND=redis://...` / `rediss://...` 自动启用共享调度，无独立 SCHEDULE_MODE。namespace 从 bucket 配置生成，旧实验 region 的 binding 不自动迁移；旧拆分环境项会报错而非静默忽略。Redis/Kvrocks 单参数业务调度、快速故障恢复、双进程恢复及默认 object Bindings 验收通过，详见 [协调报告](reports/2026-11-24-redis-authority.md)。未部署生产。

- 2026-11-24：已切换本机常驻 Compose，保留原 MinIO/数据卷；KV、Queue 重试、Cron、Alarm 和持久卷重启验证通过。当时 GZ 切换按用户要求暂停；后续已完成 GZ revision 7 发布，见下文。见 [本地部署记录](reports/2026-11-24-local-redis-compose.md)。
- 同日本机改为双 agent fleet，独立卷并验证实际 follower 确认；仍为同一宿主机，不能抵抗宿主机故障。无 peer 时按现有协议先落本地 spool。

- GZ 使用集群内单实例持久 Redis，AOF always/noeviction；未使用可能回退已确认版本的托管 Redis 副本提升。节点失联不得强删 Redis Pod/强挂卷。最终版本两个 agent 启动时连接 Redis 被拒绝，各重启一次后恢复；稳定性观察与剩余遥测告警见发布记录。

- GZ 首轮 Redis 采样发现 outbox 对 native DO scope 的重复 hydration，导致 other/LTX GET 和 List 增加。已在 revision 7 发布限定为已有 agent-local 数据库的修复；缺失本地数据库不由 outbox relay 触发恢复。最终生产采样见 GZ 发布记录。

- Redis recovery discovery 已部署 SG 150、USW 142、GZ 9；本地验证见 [记录](reports/2026-11-24-redis-recovery-discovery.md)，生产验收见上述三地区发布记录。普通恢复发现已不再周期扫描 COS node-logs。新版本要求离线补齐 pending-session 索引，不能直接滚动混用旧写者。epoch GC 当前仍使用周期检查，事件驱动方案尚未落地。


## 2026-11-24 托管 Redis 协调回退限制

用户选择采用腾讯云托管 Redis，GZ / SG / USW 已切换并完成常规验收；主从故障切换安全未验收。异步复制与自动主从提升可能丢失已确认的 owner/revision/lease/schedule 写；当前 generation 仅能拒绝空库或不匹配世代，不能检测同世代回退。Lua CAS 不解决此限制，不能宣称此故障模型下协调 RPO=0 或 fencing 安全已验证。异常必须停写并隔离旧 session 后人工恢复，不自动回退 COS 或历史 Redis。

### 2026-11-24 Redis recovery authority v3（本地、SG、USW 已部署）

Session 正文和 DO 恢复指针纳入 Redis CAS，需停写执行 v3 离线迁移。epoch GC 默认周期改为 1 小时，首次仍为 1 分钟，每节点集中遍历 OwnedScopes；事件驱动尚未实现。设计与兼容性边界见 [设计](superpowers/specs/2026-11-24-redis-recovery-authority-design.md)。

本地 v3 验收：全量 make test、build/vet、coordination/nodelog/recovery race、object/Redis 双进程恢复均通过，见 [本地验收](reports/2026-11-24-redis-recovery-authority-v3.md)。SG revision 153、USW revision 145 已离线升级 v3；GZ 仍为 v2。发布与性能观察见 [记录](reports/2026-11-24-sg-usw-recovery-v3-release.md)。


### 2026-11-24 GZ 限时 trace 日志诊断

新增 CELLHIVE_TRACE_LOG_UNTIL 限时 RecordOnly stdout span 日志，补充 object GET/LIST、local validation、epoch chain 阶段。无凭据/正文/完整 URL，原 sampled 位和 ARMS 采样不变。目的为定位 5–6 秒冷访问，目前未据此确认根因或修复延迟；不包含 Redis recovery v3 迁移。

### Cron retry celld parity（2026-11-24，本地实现与测试通过，未部署）

Cron 待重试状态已改为驻留内存，重启/驱逐/换 owner 丢弃该轮重试；这是用户选择的语义，不保证失败轮次最终送达。旧 durable retry 行升级时消费。普通未执行 occurrence 仍沿用现有调度器，本次不改变停机补跑。验收状态见对应 spec/plan。

## 2026-11-24 Redis review 修复（本地验证通过，未部署）

确认 Cron 跨周期失败后延后 cutoff、Nudge 被 Pass I/O 阻塞、Redis 混合前缀分页反复全量 List 三个问题。同步优化纯 Redis CommonPrefixes 与 outbox 候选扫描/整轮预算。上述问题已本地修复；新增边界回归、全量 make test、build/vet、coordination/scheduleindex/cron/timer/owner/server race 通过。Redis 使用独立本机实例，未验证托管主从回退或线上性能收益；生产状态不变。见 [修复报告](reports/2026-11-24-redis-review-fixes.md)。

第二轮复审发现并本地修复 outbox worker 全部阻塞时未执行候选饥饿，以及 ACK 自通知导致淘汰后重新开库的问题。两项回归均先在旧实现失败、修复后通过；本轮完整验证状态见上述修复报告，未部署。

## 2026-11-24 Redis review 后续优化（本地验证通过，未部署）

shared hint 失败保留重投递，采用按 member 的 1/2/4/8/16/32/60 秒有界退避；新发布重置失败计数，失败旧 claim 不得覆盖并发新通知的 deadline。每轮仍最多处理 32 项，共享 3 秒 handler 时间预算（Redis claim/收尾 I/O 另受客户端超时限制），handler 失败不终止其他候选；Redis 操作失败终止本轮。claim 返回真实 member 与 payload，损坏或缺失 payload 通过原 token/revision ACK 清理，并发新发布继续保留。新增失败计数 hash 与现有 hint 同生命周期，不改变权威或业务 ACK。

Redis ListPage 使用每批最多 128 个键的原子元数据读取，返回 HSTRLEN 与 revision，不下载正文；保留 generation guard、缺失跳过、混合物理页边界和游标语义。锁排序、启动策略、epoch 释放和混合目录枚举不在此批修改范围。测试使用隔离 Redis；不宣称托管故障切换安全或生产性能收益。

本地验收：隔离 Redis 7.2 / Kvrocks 定向回归、scheduleindex/coordination race、make test、make build、make vet 和 git diff --check 通过。详情见 [验证报告](reports/2026-11-24-redis-review-optimization.md)。

### 2026-11-24 后续本机部署

Redis review 后续优化与 invocation 日志诊断已随源码 `8fa81f58` 部署本机 Compose，未推送、未发布云端。无权威数据迁移；Vector 配置仅本地实际日志重放验证，本机未配置持续 OTLP 采集。功能验收及保留告警见 [部署记录](reports/2026-11-24-local-redis-logging-review-release.md)。
