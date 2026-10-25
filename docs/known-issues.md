# 设计决议（评审问题闭环）

本文件记录系统评审发现的全部问题，以及**每条问题的解决决议**。原始分级见文末附录。设计文档（architecture / cell-protocol / durable-objects / networking / decisions）已按此同步。

Static Assets 标准部署收口（ADR-206）实施中：assets-only、生产 ASSETS binding、忽略规则、不可变上传、HTML/SPA 路由与入口配置需以 `static-sites.md` 验收记录为准。

_最后更新：2026-10-26_

## 2026-10-25 SG KV 转发热缓存、连接复用与残余尾延迟

转发节点此前在成功转发后直接返回，遗漏热访问登记，导致 binding/资源 fence 缓存过期后仍同步查询。现已让成功的转发 PUT/GET/get-batch（含正常 GET 404）进入原有有界热访问刷新集合；鉴权、删除 fence、owner lease 和 RPO=0 确认规则保持不变。拒绝/失败请求不激活刷新。发布前后证据见 [SG 分批测试记录](archive/sg-kv-performance-2026-10-25.md)。owner 直连约390ms样本中仍有约270ms在持久化span之外，具体分段尚未定位；本修复不宣称消除全部400ms尾延迟。

首次发布后长时间并发GET暴露另一问题：默认转发transport每目标仅保留2条idle连接，SG agent-1累积28,261个TIME_WAIT，触发 `dial tcp ... cannot assign requested address` / HTTP502。已将默认转发client复用并扩大idle pool至256/128，定向HTTP测试同样5轮×8并发的建连数从32降至8；线上长测和最终发布状态见同一验收记录。

SG revision64分段观测已确认另一条热写尖峰：HTTP555.8ms中SQLite仅0.111ms，bucket提交等待553.1ms；选择时follower缓存租约已过期约2.57s，`no_eligible_follower`触发`bucket-batch`。前序提交又使后续请求达到604ms。ADR-295已增加独立已知节点点查刷新（默认2秒、单次最多1秒、最多8并发），慢全量扫描不得覆盖较新点查；本地单元、双节点真实HTTP及进程恢复E2E通过，SG revision66已验收，最终69,299次计量写入无失败/无新增桶回退，owner/转发p99为26.78/30.85ms（仍有复制尾延迟）；不允许延长过期租约或取消RPO=0回退。该结论不解释所有历史proof之外的耗时。实施和最终结果见 [独立租约刷新验收](archive/known-follower-refresh-2026-10-26.md)。 USW revision51及本地也已部署同镜像并通过KV/DO发布前后验收；USW滚动期间DO的cell_unreachable未自动恢复，agent就绪后额外重建DO Pod恢复，根因待查，详见同一记录。

## 2026-10-25 后台快照阻塞写入（ADR-294）

marker 恢复后的后台基线已改为 SQLite backup 固定副本，释放原库写锁后上传，不占用增量 capture 循环、不截断源 WAL、不提前推进确认水位。上传失败/取消保留旧链；退出等待后台任务，大小预算覆盖 WAL 页，启动时清理强杀遗留副本。首次无恢复链和 WAL 截断后的快照仍同步；本地复制仍需短暂写锁和额外磁盘。全量检查、相关 race 与真实 HK R2/D1/Queue 回归通过；最终 HK 冷重建 1660ms、重启首写 885ms，140 个确认键正确。冷重建仍未过 1s 门槛，见 [验收记录](archive/detached-snapshot-2026-10-25.md)。

## 2026-10-25 真实 HK COS 冷恢复性能验收未通过

前测发现的重启 8.7s / 冷重建 9.2s 已定位为首次 NodeLease 同步扫描 nodes/ 的阻塞。ADR-293 改为目标节点点查、异步 fleet 采样，并在下载前过滤 manifest 已覆盖的 L0。最终无插桩真实 HK COS 测试（本机计算，durability=bucket）重启首写 974ms、冷重建 2318ms；接管请求 1673ms、故障到成功 11909ms。140 个已确认键无缺失/内容错误，SQLite integrity 正常；**冷重建仍未通过原 1s 性能门槛**。后台历史节点扫描成本仍存在，未测 HK 同区计算、跨机器 fleet 或长期生产负载。见 [本次修复与证据](archive/cold-recovery-2026-10-25.md) 和 [前测](archive/hk-review-2026-10-25.md)。

## 2026-10-25 跨模块审查修复（ADR-292）

F1–F9 的 Queue 并发分配、adaptive router 解锁错误、冷恢复池分类、R2 旧 metadata、D1 二次方累计、等待取消、JWKS 刷新放大、busy map 残留及 CI SQLite tags 已实施修复。新增回归包含真实 HTTP 无 deadline 的交互请求隔离，以及两个 R2 Store 并发时旧清理不能删除新 metadata。验证与待发布状态以 [修复验收记录](archive/project-review-fixes-2026-10-25.md) 为准。

R2 双对象发布仍非原子：迟到 sidecar 发布和同内容并发写未解决，错误响应可能已经更新数据；已知不支持条件删除的 provider 对存在旧 sidecar 的覆盖会提前拒绝。D1 32 MiB 是保留载荷预算，不等于进程 RSS。Queue 上界是单队列并发，不是全节点资源配额。后续 Queue 批量事务、公平调度、DO 重连/续租容量、SQLite RSS/FD 准入和 Cron 补跑属于独立阶段，尚未实现。

## 2026-10-25 失租后迟到续租复活（ADR-291）

原 Publish 无条件 Put 且可恢复 Healthy，导致原租期已过的进程重新恢复服务，或迟到写覆盖替代 session。改为不可逆 fence、ETag 条件续租、独立退出 watchdog；新 session 走既有恢复。初始回归已复现过期后重发、挂起续租返回、覆盖替代 session 三个问题。全量测试、lease/owner/recovery/server/queue/cell-agent race、vet/build，以及真实双进程 SIGSTOP 超期→peer 恢复接管→SIGCONT 旧进程退出→新 session 恢复均通过。见 [验证及发布记录](archive/lease-fence-2026-10-25.md)。代码 `520b61b` 已部署本地、SG revision 59、USW revision 48。

## 2026-10-25 owner 路由周期性点查（ADR-290）

改为本机连续安全租约窗口快速路径、远端观察租约期限缓存和同 cell 并发解析合并；不再以固定 1s 刷新普通请求 owner。热 KV 后台预热也复用路由解析。首次访问、期限已过、显式失效仍可能等待对象存储；此优化不消除其他资源 fence、绑定、持久性等待。全量测试、owner/lease/queue/server race、vet/build 和真实 HTTP 双入口/交接回归通过；同机文件桶加 owner GET 延迟的对照见 [验证记录](archive/bench/owner-route-cache-2026-10-25.md)。已随 `520b61b` 部署本地、SG、USW，持久化冒烟验收通过；未宣称生产 COS 或跨机器 fleet 稳态性能。

## 2026-10-25 USW DO 滚动更新后连接未恢复（待定位）

agent 地址变化后，一个 DO Pod 持续 ready 503/cell_unreachable；同容器直连 agent readyz 返回 200。保留 PVC 重建该 Pod 后恢复，原 DO 数据与 HTTP 验收通过。疑似 workerd 连接或地址缓存未刷新，尚无根因证据，不能视为已修复。见 [发布记录](archive/lease-fence-2026-10-25.md)。

## 2026-10-25 DO capture 重启 fence（ADR-289）

SG 发布验收发现 capture epoch 固定 1 与 cell-agent 接管后 owner epoch 增长冲突，DO 恢复后持续 gate_500/epoch_mismatch。现分离 LTX epoch 和 capture owner epoch，同时验证 DO shard fence；回归覆盖 agent epoch 增长后的真实 HTTP commit、旧 capture/shard fence 拒绝及普通 cell fence 保留。另修复失败捕获后 cursor 已推进而遗漏未确认 WAL 的问题：保留原始片段直到确认，真实 SQLite 恢复测试覆盖提交前拒绝、确认丢失后新写入。部署状态见发布记录。

## 2026-10-25 DO HTTP 与完整清空（ADR-288）

发布前双节点验收另发现：DO invoke 的绑定检查读取 follower 本地 Projection，造成新 Worker 持续 403。现按当前 control owner 点查 active Worker，读取失败返回 503；只读查询可转发，DO 调用不因此重放。双节点回归覆盖合法 class 成功、非法 class 拒绝。

本地修复：deleteAll 原先误读 Map 的 cursor，300 个键只删 256 个；DO fetch 在共享 binding 与 host 多次 text()，破坏非 UTF-8 请求/响应；host、cell-agent、binding 丢失租户响应头；Go/内部 fetch 自动跟随 302。现改用 base64 请求 envelope、字节响应及多值响应头，保留状态/Location/Set-Cookie，过滤内部和逐跳头；重复分页直到清空，并清除 alarm shim 元数据。新请求字段要求暂停/排空 DO 流量，升级全部 do-runtime 和调用方后恢复；do-runtime 自身也可调用其它 DO，不能仅靠先滚动 do-runtime 保证安全。已随 004401c 部署本地、SG revision 58、USW revision 47。

续租增加本地 HTTP 5s deadline、单 scope fetch/body 2s deadline，单个失败继续处理其余租约，迟到失败不会删除更新的 claim。仍按 scope 串行续租，不宣称解决大量 shard 或过短租约下的容量问题。HTTP 仍经过缓冲和既有大小限制，非流式传输。验证范围和 user-runtime 合并审查见 [记录](archive/runtime-http-review-2026-10-25.md)。

## 2026-10-25 user-runtime 旧版本/实例缓存（ADR-287）

本地修复：指定 version 读取失败回退到其它缓存版本；显式历史版本误填 active 缓存；旧查询 404 覆盖新 env；删除清理后重建复用 version/SHA 导致旧代码或旧变量/绑定 resident 命中；暖 host 的挂起刷新绕过原 5s 兜底。host pointer 传播 storage_id，缓存和 resident 使用代际身份；无代际旧指针重新查询并核验，错误不降级为另一版本。host 刷新使用 5s AbortController/deadline 及 generation/epoch 双重结果守卫。有效在途旧版本可完成；路由 SWR 与投影 stale-on-error 可见性边界未改变。已随 004401c 部署本地、SG revision 58、USW revision 47；不得把代码级复现直接归因到请求 `53900423061812ab6983f683207f7742`。

pool 路由/专属进程传播并校验 storage_id，实际反向代理测试覆盖 Connection 头不能删除可信身份头；内部 bindings 端点返回 version/bundle_sha/storage_id，Queue/Timer/Service/Workflow adapter 不再永久缓存无代际版本，加载实例带代际。旧 metadata 响应不带身份或不可用时的 fail-open 兼容语义未整体移除，SHA-only service 仍不是完整环境 pin；这些边界及投影读取快照一致性、bundle 读后哈希验证不属于本次完整解决范围。

## 2026-10-24 异常接管与驻留回收（ADR-286）

本地已修复并验证：请求入口忽略失效错误；queue/timer 在本地清理前发布 owner；TryInvalidate 校验失败泄漏句柄；最旧项被 Pin/busy 保护时误驱逐未超时热 cell。claim 按 scope 串行化，前任恢复与本地清理成功后才发布 owner，失败保持可重试。sealed tail 使用服务端校验游标，连续分页实际读取量接近日志大小；重启/重试/截断/替换文件检查保留。

全量测试、owner/peer/queue/cellstore/server race、vet/build、真实双进程换地址和中断恢复 E2E 已通过；尚未部署。驻留上限仍是软限制，capture 默认2ms轮询随 resident 写入 cell 增长；2核建议32–64起步，完整数据和验证边界见 [记录](archive/bench/recovery-residency-2026-10-24.md)。


## 2026-10-24 热 KV 读写的周期性 COS 等待（ADR-283）

已打开 cell 的连续热 GET 约 0.3–0.4ms，但 binding、owner 和资源删除 fence 的 1 秒缓存失效后会在请求路径同步读取控制面/COS；本机到香港 COS 测试桶每隔 1.15 秒的 8 次 GET 曾为 44–185ms。分段诊断还发现本机 owner 被滞后的 node-lease 快照判为不存活时，GET 会同步点读 `nodes/<self>.json`，仅路由判活就耗 24–25ms。ADR-283 在有限活跃 KV 集合中提前刷新原有缓存，并以本机已成功发布且健康的 lease 直接判断本机 owner；未发布或不健康时失败关闭，失效边界和请求准入检查不变。最终版真实 COS 的 60 次写后 GET p50/p95/p99 为 0.60/0.76/1.03ms、最大 29.99ms；300 次空闲连续 GET p99 0.45ms；8 次间隔 1.15 秒 GET 均为 0.55–0.62ms。跨地域 COS 忙时提前刷新仍可能失败或晚于失效时间，此前复测曾见 20–61ms 残余尖峰；**长尾尚未证明完全消除**。首次读与冷写不在此次热缓存优化范围。SG 已做短时同 Pod 写后 GET 复验，公开入口和长时间 GET 尾延迟仍待测；详见 [对照记录](archive/bench/kv-ttl-wake-2026-10-24.md)。

PUT 共用上述 binding、资源 fence、owner 路由，原先只有 GET 能维持提前刷新：间隔 1.15 秒的纯 PUT 在最近 GET 停止后退回同步检查，分段诊断约 25–30ms binding、12–16ms fence、23–31ms gate，加上约 50ms 写入与 bucket 持久化证明。现成功 PUT 也维持活跃 scope；本机到香港 COS 的三轮间隔 PUT 各 12 次中，两轮全部约 43–66ms，另一轮 11 次约 49–66ms、一次 611ms。**PUT 仍须同步等待 RPO=0 证明**；诊断确认部分 80–181ms 连续写的增加量全部在证明等待中。611ms 单次尖峰未被同请求阶段日志覆盖，不能将所有尾延迟归因或称已解决；SG 短时同 Pod fleet 复验见下文，公开入口和长期尾延迟仍待验证。细节见同一[对照记录](archive/bench/kv-ttl-wake-2026-10-24.md)。

ADR-284 又去掉了 COS 不可变 LTX Append 成功后为取 ETag 做的常规 HEAD：测试桶实测响应 ETag 与 HEAD 15/15 相同，缺失响应头时仍回退 HEAD。独立顺序 A/B 的普通 PUT p50 从 49.95ms 到 44.19ms，TTL PUT p50 从 48.78ms 到 46.57ms；不同轮 COS 波动不可归因于单项变更。优化后普通 PUT 仍有 544ms 尖峰，跨地域 bucket 模式的长尾与冷请求仍未解决。

扩大到 120 对写入的临时分段诊断确认，数百毫秒尖峰主要落在 RPO=0 提交前的 COS 资源 fence 权威点读（最高观察到 778ms）或原生 COS Append（例如 412–472ms）；不是 SQLite 或 TTL 唤醒扫描。该 bucket 模式不能安全跳过 fresh fence 或先于 COS 持久化应答。需在 SG 同区 fleet 模式确认 follower fsync 正常承担热写证明、bucket 回退的频率和原因，并分别测同 Pod 与公开入口。

SG 已在 2026-10-24 部署 `ac5ad43`（Helm revision 53）。独立 namespace 的 Pod 本地真实 HTTP 校验通过 10 次普通 PUT、10 次 TTL PUT 与逐次 GET；首写 752ms、次写 146ms，之后 18 次热 PUT p50 9.84ms、p95 11.44ms、最大 13.14ms。该窗口 `fleet` 提交 +22、`bucket-batch` +0，`live_followers=1`，产品 App 删除返回 200。通过 `kubectl port-forward` 的另一轮 20 次 PUT/GET 也全部正确，但其延迟包含隧道开销，不能用来判断同 Pod 性能；该轮记录到 fleet +17、bucket-batch +4，计数包括控制面提交，需继续追踪 bucket 回退。随后对原先的 `vwork-24`/`TEST` 同路径复测，连续热 TTL PUT 11 次中位数 9.41ms（旧版 6 次为 190.7–299.9ms），但首次冷请求 2.440 秒；间隔 1.15 秒的 8 次 TTL PUT 中又出现 159.59/108.33ms 两次慢写，并有 bucket-batch +1。**热 TTL 性能有明显提升，但冷请求、周期性长尾和公开入口仍未解决。**滚动后 cell-agent-1 曾有一次对旧 follower IP `10.0.16.195` seal 超时的 recovery WARN；恢复过程失败关闭，旧 session 是否能最终封存尚未确认，需单独跟踪。

## 2026-10-24 KV TTL 同步重设 timer 的写入延迟（ADR-282）

SG 部署前同 Pod 的小值 KV put 对照中，不带 TTL 多次约 9–11ms（偶有 60–164ms），带 `expiration_ttl=300` 为 191–300ms；本地 FS 各 10 次的中位数分别为 4.2/5.4ms。写入持久化证明的平均值约 24ms，不能解释大部分 TTL 耗时。ADR-282 已实现复用已持久化、尚未到期且足够早的 `kv-expire` timer，避免每次 TTL put 都同步改写 COS `wake/` 索引；首次和更早到期仍要同步发布。本地修复后普通/TTL 中位数为 3.85/4.11ms，真实进程 1 秒/3 秒 TTL 物理清理和五场景 FS 冷写 E2E 均通过。真实 COS 测试桶（本机跨地域、bucket 模式）旧版普通/TTL 中位数 56.0/257.2ms、修复版 65.7/47.0ms；2 秒/6 秒 TTL 读回和到期均正确。SG 已部署并完成上述短时同 Pod 普通/TTL PUT 复验；公开入口延迟仍未测，见 [TTL wake 对照记录](archive/bench/kv-ttl-wake-2026-10-24.md)。

## 2026-10-24 SG owner 判活分歧（ADR-280）

SG revision 50 后观察到同一 control owner 在两个 cell-agent 上分别显示 `expired=false` 与 `expired=true`，后者的资源清理循环持续收到 `owner: live owner exists`。定位为旧节点租约快照的负向判定未向桶点读确认；修复与 SG 复验记录见 ADR-280。此问题与 Cron 租户派发 `status 400` 分开处理。

## 2026-10-24 invocation owner 换手时的 503

已转发的 invocation 写请求可能在旧 owner 的 capture 预检前遇到 owner 换手。旧节点先前返回 503 `not_owner`，入口节点按 ADR-120 不重试 5xx，调用记录写入失败。已把**写入前**失权的转发请求改为 409 `not_owner`，让入口节点刷新 owner 并最多重试一次；写入后或结果未知的失败仍不可重放。`TestInvocationForwardedCaptureHandoff` 三节点 HTTP 回归在修复前稳定复现 503，修复后验证调用记录只写入新 owner、旧 owner 未执行写入，并通过 `-race`。本地 `make test`、`make vet`、`make build` 均通过。线上 503 与该路径的因果关系仍需部署后以响应正文和 owner/epoch 日志确认；Cron 同时出现的 timer owner 拒绝需独立追踪。

## 2026-10-24 COS 冷写性能与 L1 恢复（ADR-276/277）

KV/Queue 每次 LTX 提交 fresh 读取资源删除栅栏；旧 `resource-fence.json` 缺失时分段 authority 会执行一次 `List`。已改为首次迁移、随后稳定点查的 `resource-fence.v2`，并用节点租约协议标识阻止新旧栅栏写者混跑；app purge 保留新旧栅栏。90 秒 COS 稳态的 put p99 从旧版 1092.9ms 降至 434.7ms，但仍高于 200ms 目标。全局 List 从 769 次降至 353 次，按前缀跟踪主要是后台 `nodes/` 租约枚举读到历史测试节点和 probe 对象，不能把这些 List 归因于新的栅栏写热路径。后台枚举和最大 3282.9ms 的偶发写延迟仍待处理。

首次 COS 复测还暴露出更严重的 RPO 违例：compaction 删除全部 L0 后，冷恢复先过滤 `L1/manifest.json`，把仅有 L1 的 epoch 跳过；重启后 80 个已确认 key 全部读成 404。ADR-277 已修复，并在同参数 COS 五场景复测中读回全部 140 个最终 key（missing=0、wrong=0，SQLite `integrity_check=ok`）。复测的同节点重启首写 2346ms、跨节点接管后 50 次 put p99 633.6ms、删除本地目录冷恢复首写 3374ms，仍未达到各自设计目标。精确数据和测试边界见 [冷写栅栏 COS 实测](archive/bench/cold-write-fence-v2-2026-10-24.md)。Cron 使用独立的 `__cron__` cell 和定时派发，不直接读取 KV/Queue 的资源删除栅栏；共享节点租约、COS 和控制面拥塞可能间接影响派发。

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
- 外部委派 issuer token 必须带过期时间；issuer key 目前不按 namespace 收窄，完整边界见 [`security.md`](./security.md)。
- 管理面支持静态 admin token 或 OIDC/JWT `cellhive_ns` 授权（ADR-131），与 binding 数据面隔离。

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
- worker CPU time 与 V8 heap per-worker 强制上限**尚未实现**；subrequest 上限已由动态 Worker 的 invocation quota 执行并有真实 workerd 测试。当前另有 deploy-time WorkerCode/env 预算、per-namespace 写准入与 cell-agent 过载拒绝。详见 [`security.md`](./security.md)。
- **动态加载总内存保护仍在收口（2026-10-13）**：先前真实 pinned workerd 在 2 GiB 容器中，451 个不同 1 MiB Worker 或 575 个同 SHA 不同版本触发 OOMKilled；189 个 10 MiB 静态资源也触发 OOM。当前公共 user-runtime 已加入版本退休、活动生命周期、冷加载并发/容量准入、源码/plan/env/asset 字节预算和大 asset 流式返回；同样 2 GiB/1000 个 1 MiB 合成 Worker 压测不再 OOM，但仍观测约 840 MiB cgroup、workerd abort 日志，尚未作为生产容量承诺。DO runtime、内部 dispatch、长连接/RPC、FD 限制和更长时 churn 仍需独立验证。证据与复现入口见 [`runtime-memory-2026-10-13.md`](./archive/bench/runtime-memory-2026-10-13.md) 与 [`workerd-loader-lifecycle-2026-10-13.md`](./archive/bench/workerd-loader-lifecycle-2026-10-13.md)。

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

详见 [`dev-mode.md`](./dev-mode.md)。

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
| 冷启动按需拉取（桶） | ✅ ADR-056 + GC：`compaction` 写 `L1/index.bin`（页→对象偏移），`replica.PageFetcher` 用 `RangedGet` 单页取数（L0 覆盖优先）；`restoreverify -bucket` 实测 full `integrity=ok`、`-pages 1-2` 稀疏 hole 正确 |
| P1 cell 复制完整（LTX/快照/compaction/paging） | ✅ 快照分页、compaction 内核+编排、txid 连续性、**稀疏按需 paging**（ADR-055/056）、**L0/L1 GC**（ADR-056）均已完成（详见文末「P1 复制缺口清单」） | 快照分页：`ltx.EncodeSnapshotParts`（flags 分片、`<txid>.p<part>.snapshot`）+ `restore` 合并 + capture/supervisor 接入。compaction：`restore.Compact` + `internal/compaction.Compact`（阈值 `CELLHIVE_COMPACTION_MIN_SEGMENTS`=64 / `_MIN_BYTES`=64MiB）+ `replica` L1 前缀/manifest + `Restore` 优先 L1 + cell-agent 后台循环（`CELLHIVE_COMPACTION_INTERVAL`=30s，写前 Verify 复核 owner/epoch）。**已补（ADR-055/056）**：稀疏文件按需 paging（`replica.PageFetcher` + `L1/index.bin` ranged-get）、L0/L1 旧对象 GC。注：**cell-agent 侧运行期 paged VFS 已实现（ADR-160，`internal/pagedvfs`，默认开；链 ≥ `CELLHIVE_PAGED_MIN_BYTES` 才分页，小链整克隆）**，KV/D1/Queue/Workflow/Vectorize cell 的冷恢复按页加载；do-supervisor 与 `restoreverify` 亦用同一 paging 原语。**E2E**：双 cell-agent + `COMPACTION_INTERVAL=1s`，sqlbench 5000 事务后 159 个 L0 段 → 1 个 L1 对象 + manifest（min=32/max=5000），restore 加 txid 连续性 fail-closed |
| DO 能力（facets / preventEviction，pinned 2026-06-15） | ✅ 已实测：`ctx.facets` **存在**（host-actor 托管多 facet 可行；`class` 必须来自 `workerLoader.getDurableObjectClass()`，裸类/普通 namespace 绑定报 TypeError）；capnp `preventEviction = true` **可解析并启动**（resident vs evictable 两变体可行） | 见 **ADR-053**；实现属 **P3**（roadmap）。 |
| DO 写合并 / 减少 fsync（stock workerd） | ✅ 历史实测（workerd 2026-06-15）：**一个 DO 事件（请求）内的所有 `sql.exec` 写被 workerd 自动合并成一次原子提交**（错误信息原文 "Durable Objects' automatic atomic write coalescing"）。故 `req/s` 固定 ~900–970，而 `writes/s` 随"每请求写数 k"线性增长：k=1→928、k=4→3.9k、k=16→14.4k、k=64→52k、k=256→**196k** writes/s。`sql.exec("BEGIN")` 被**拒绝**（要求改用 `state.storage.transaction()` / `transactionSync()`）；多语句 `exec("a;b;c")` 可行但不改变提交次数 | 减少 fsync 的唯一 stock 手段 = **提高每请求写数**（批量化），不改 workerd（ADR-001）；`BEGIN` 不可用 → 原子性用 `transactionSync`。**已验证**：带输出门时一次门覆盖整批，k=256 达 **110k writes/s**（对比 1 写/请求的 531，~207×），且恢复 `t.n` 精确一致。平台侧 wrapper `workerd/wrapper/groupcommit.js` 已实现（1 op/请求 → 3,637 req/s，~6.9×，avg_batch≈23）|

### 已实现（P0 代码，历史清单）

`internal/`：`config`、`cell`、`bucket`（FS + 诊断 + 计数 + **S3Bucket**）、`owner`、`lease`、`server`、`cellstore`（SQLite）、`wal`、`ltx`、`replica`、`peer`（spool/transport）、`nodelog`、`recovery`、`upload`。`cmd/`：`cell-agent`、`cellhive`、`cellbench`、`realbench`、`s3init`、`s3probe`、`walscan`。见 [`p0-report.md`](./archive/p0-report.md) 与 [`p0-tasks.md`](./archive/p0-tasks.md)。

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
| **稀疏文件按需 paging**（冷启动不整库下载） | ✅ **原语已交付**（ADR-055/056；`internal/replica TestColdHydrateIsWholeObjectNotPaged` 实测 cell-agent hydrate 是整对象下载 + 0 ranged 读）：`restore.IndexChain`/`SparseFile`、`replica.PageFetcher` + `L1/index.bin` ranged-get、`restoreverify -bucket`；**GC ✅**（ADR-056）。**消费方**：do-supervisor（DO 文件）、`restoreverify`，以及 **cell-agent 的 cell 冷恢复（ADR-160，`CELLHIVE_PAGED_RESTORE` 默认开；链 ≥ `CELLHIVE_PAGED_MIN_BYTES`（默认 256MiB）走 fault-in VFS 按页加载，小链整克隆，后台按 `CELLHIVE_PAGED_HYDRATE_MBPS` 补齐）**，稳态读本地 |
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

Queue 生产/消费主链路已实现；`sendBatch()`、`json/text/bytes/v8`、128 KiB 上限、metadata、原始 timestamp、delay 校验和幂等冲突已补齐。剩余差异是跨批顺序以及部分 retry/DLQ 边界；`max_batch_timeout` 已接入并做真实服务回归，继续按 [`compatibility-matrix.md`](./compatibility-matrix.md#queue--workflow-的调用边界) 验收。

## P2 Workflows 绑定（ADR-086，Partial）

**已实现（真实 workerd e2e）**：`internal/workflow`（`__workflow__` cell：instances/steps/events，step 记忆化）；facade `env.WF.create/createBatch/get/sendEvent`；cell-agent API（租户 create/create-batch/get/event，内部 step/sleep/finish）；user-runtime `/v1/workflows/run` + `cellhive-workflow.js` base + `step.do/sleep`（shim：改写 `cloudflare:workers`，平台自构造，因 workerd 的 `WorkflowEntrypoint` 引擎外不可构造）；sleep 复用统一 timer（`KindWorkflowSleep`）；CLI `workflow create`；`wranglercompat` 要求 `class_name`。create/createBatch 请求总 payload 上限 1 MiB，batch 为 1..100 个实例。

**Partial 边界（未做）**：跨 worker 实例；`locationHint` 被接受但**忽略**（best-effort 放置提示，无效果）；step 历史列举与 progress 回调；实例恢复/重派发使用当前活跃 worker 版本，未固定创建版本。
**调用形态差异**：`sleep/sleepUntil` 仅数字毫秒、`waitForEvent.timeout` 仅数字秒数且超时返回 `undefined`；`step.do timeout/sensitive`、`restart({from})`、`terminate(options)` 未生效。详见 [`compatibility-matrix.md`](./compatibility-matrix.md#queue--workflow-的调用边界)。
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

v3 follower 身份、启动恢复门和按 session 退避重试已实现。真实双进程换地址重启、恢复中强杀、同名空盘拒绝后换回原盘通过，80 个已确认值全部读回；全量测试、vet/build 和相关包 race 检查通过。USW 已部署至 revision 42，19 个历史 session 完整恢复封存。SG 的旧 IP session `1792144743099984471` 已完整校验并持久恢复 22395 条、封存；但另两份历史日志在现有 PVC 上缺失，其余积压尚待恢复，SG 因此保持旧版、暂缓升级。新旧 recovery 协议禁止混跑，旧 URL 记录必须先恢复可达性并完整封存；否则新版保持恢复模式。验证与部署约束见 [本地记录](archive/bench/session-recovery-2026-10-24.md) 和 [部署记录](archive/bench/session-recovery-rollout-2026-10-24.md)。

- 2026-10-24 SG legacy migration: two explicitly authorized missing-tail sessions may be closed with audited `recovery_resolution`; this is accepted unknown historical loss, not RPO=0 recovery. TailPage byte-budget false truncation is fixed and covered by regression tests; see `cell-protocol.md` and `archive/bench/session-recovery-rollout-2026-10-24.md`.

- 2026-10-24 pinned control takeover fix: USW returned binding_not_registered after owner change because Pin suppressed stale-file invalidation (local txid 4516 vs bucket 4518). Ownership invalidation now bypasses residency Pin and refuses a busy stale cell. The old binary fails the retained-control-DB process E2E; the fixed binary restores registrations and all 80 KV values.
