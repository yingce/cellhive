# CellHive 跨模块代码审查（2026-10-25）

基线：main / 213a012；业务代码发布提交 520b61b。以下保留修复前审查结论；后续状态见 [修复验收记录](project-review-fixes-2026-10-25.md)。原审查未修改业务代码或生产配置，诊断测试通过 Go overlay 注入，原始临时证据位于 `/tmp/cellhive-review-20261025/`。D1 性能对照也只替换测试构建中的文件。

本轮是全仓结构扫描加关键链路深入检查，不等于逐行审计全部代码。深入检查：cellstore 冷恢复/驱逐、owner 路由/lease、timer/cron/waker、queue、D1、R2、鉴权、Go adaptive runtime router、DO renew/readiness、capture/upload/recovery 与 CI。CLI、Vectorize、Hyperdrive、Workflow 解释执行和 workerd fork 未做完整专项审查。

## 优先修复

### F1 / P1：Queue 并发配置可以导致极大分配

位置：internal/control/store.go:1143；internal/server/cloudflare_api.go:984；internal/queue/runner.go:162-174。

配置只拒绝负数，没有上界。runner 在查看是否有可投递消息前，就 make([][]Message, 0, conc)。诊断测试实际 Deploy 接受 max_concurrency=1000000000；64-bit 下仅切片 backing array 就需要约 24 GB。没有执行危险分配或生产操作。风险需要具备相应部署/consumer 配置权限，不是匿名直接调用。

建议：控制 API 统一校验上限；runner 对存量配置再次钳制；加入节点与租户两级有限并发准入。不能仅依赖入口校验。

### F2 / P1：adaptive user-runtime 路由等待边界可以导致进程退出

位置：internal/userruntimepool/router.go:73-98。

等待同 host 的查询完成后，读取缓存随即 Unlock；如果条目已过期/消失，没有重新加锁就继续更新共享 map，最后再次 Unlock。缓存有效时提前 return，所以普通测试没有覆盖。

构造“等待完成，结果已过期”的状态，测试进程实际报 fatal error: sync: unlock of unlocked mutex，栈指向 router.go:98。这是进程级 fatal，不是可由 HTTP recover 恢复的普通 panic。较长调度停顿等情况可进入该分支，未测自然触发频率。

边界：仅 CELLHIVE_POOL_MODE=adaptive 路径；Helm 默认 mode 为空。本轮未查询生产环境变量，不声称当前 SG/USW 正在触发。

建议：用循环重新进入带锁的查询/flight 判定；不要在无锁状态沿用原临界区；补结果过期、错误缓存失效、等待取消的并发回归。

### F3 / P1：冷恢复后台/交互隔离在默认 timeout 配置下失效

位置：internal/cellstore/cellstore.go:179-219、269 附近、340-341；internal/config/config.go:634。

hydrateSlot 以 ctx 是否存在 Deadline 区分后台与请求。但调用前 hydrateContext 已给没有 deadline 的后台任务加上 HydrateTimeout（默认 30s），导致后台也全部进 interactive 池。

诊断：两池容量均 1，background Cell() 占用后，background slots=0 / interactive slots=1；另一个交互任务 100ms deadline 到期，仍拿不到名额。现有 TestInteractiveHydrateNotStarvedByBackground 没有设置 HydrateTimeout，漏掉了生产默认组合。

建议：在添加内部超时之前确定任务类型，或显式携带调度类别；保留真正独立的后台/交互池。验收必须使用生产默认 timeout。

### F4 / P2：R2 覆盖不清理旧 metadata，返回值可自相矛盾

位置：internal/r2/r2.go:181-184、207；internal/server/r2.go:151-166、356-366。

先 PutWithMeta 再不带 metadata 的 Put 时，新对象成功写入，但提前返回，没有清理旧 sidecar。GetMeta 不校验 sidecar 的 ETag 是否等于当前对象；r2ObjectJSON 还会用 sidecar 覆盖真实 Stat 的 size/etag。

诊断：新对象 16 字节，sidecar 仍为 5 字节，ETag 等于旧对象而不是新对象。并发写与 sidecar 写失败还存在双对象一致性设计问题，简单“无 metadata 就 Delete”不足以解决所有竞态。

建议：读取时校验对象与 metadata 的版本；写入采用与对象版本绑定的 metadata/manifest 策略，覆盖和删除正确失效；补不带 metadata 覆盖、并发覆盖、sidecar 写失败、相同内容不同 metadata 的用例。

### F5 / P2：D1 结果大小检查是 O(n²)

位置：internal/d1/d1.go:622、657。

每扫描一行都 totalResultBytes(out.Rows)，再次遍历之前所有行。MaxResultRows=100000；低字节数的大量行也消耗大量 CPU，形成与 SQLite 查询本身无关的延迟。

本地同一递归 SQL（每行短字符串）：
| 行数 | 原实现 | 累计计数临时对照 |
|---:|---:|---:|
| 5,000 | 85.7ms | 6.0ms |
| 10,000 | 271.3ms | 11.4ms |
| 20,000 | 937.3ms | 17.8ms |

单轮诊断、非并发、非真实 HTTP/云上压测；首次 1,000 行还包含开库成本。对照仅用于确认算法瓶颈，未作为完整修复验收。BLOB 转 []int 的内存膨胀和结果字节计量口径还应一起检查。

建议：累计计数，保持边界语义明确；增加行数扩展 benchmark、取消、大 BLOB/数字列和总响应大小测试。

### F6 / P2：同 cell 冷开等待不响应 context 取消

位置：internal/cellstore/cellstore.go:150-156、232-233。

Cell(ctx) 在其他 cold open/eviction 期间使用 cond.Wait，不观察 ctx.Done；BeginRequest 更没有 context 参数。调用方超时不能及时释放等待 goroutine。

诊断：已经取消的 ctx 调用 Cell，仍等待至少 100ms，直到首个 hydrate 被释放才返回。

建议：每 scope 可取消的 flight/done 等待；等待者取消不取消共享首个任务；BeginRequest 也支持 context。不能仅在 Wait 之前检查一次 ctx。

### F7 / P2：未知 JWT kid 绕过 JWKS 刷新间隔

位置：internal/auth/jwks.go:51-77。

TTL 判断只在 kid 已存在时成立；不存在的 kid 会触发刷新，singleflight 只合并同时发生的请求，不合并顺序请求。

诊断：TTL=1h，1 次有效 kid + 20 次相同未知 kid，产生 21 次 JWKS HTTP 请求。影响启用 JWKS 的部署，可能让无效 JWT 请求放大 IdP 负载与鉴权延迟，不代表签名校验被绕过。

建议：独立的全局刷新最小间隔、有限负缓存、错误退避，同时允许合理的密钥轮换刷新；不能无限缓存每个随机 kid。

### F8 / P2：请求 bookkeeping 不随 cell 释放

位置：internal/cellstore/cellstore.go:158-166。

busy[key] 从 1 减到 0 后没有 delete；MaxOpenCells/LRU 不清理该 map。进程访问不同 scope 越多，历史 key 留得越多。

诊断：10,000 个不同 key 的 BeginRequest/end 全部完成后仍有 10,000 条 busy entry。未测生产增长速率或内存占比。

建议：计数归零时删除；补大量不同 scope 的 churn 与取消回归。

### F9 / P2：GitHub Go CI 命令缺少强制 SQLite tags

位置：.github/workflows/ci.yml:17-18；internal/cellstore/require_tags.go:1-6。

GitHub job 直接 go vet ./...、go test ./...，没有 Makefile 的 SQLite tags。require_tags.go 明确用 C #error 拒绝该构建。脚本 scripts/ci.sh 有 tags，但 workflow 没有走脚本。

本机执行不带 tags 的 go test ./internal/cellstore -run '^$'，实际编译失败，错误为 CellHive needs SQLite feature tags。没有读取远端 Actions 状态。

建议：workflow 使用 make vet / make test，并对齐 production fork 的真实 runtime 验收；不要只依赖开发机 GOFLAGS。

## 模块优化建议（与已确认 bug 区分）

1. **Queue 批量提交**：runner.go:298 每条 ack 都走 Commit/持久证明。诊断 10 条消息的一批产生 11 次 Commit（1 claim + 10 ack）。可把同 queue、同 generation/claim 的 ack/retry 更新合并为事务和一次证明；DLQ 跨 cell 仍需保持来源幂等及恢复顺序，不能简单全合并。
2. **timer/Queue 的任务隔离**：timer Runner 按 scope 串行，DispatchDue 又按 timer 串行；HTTP timer dispatch 上限 15s。Queue 只在单 queue 内并行，队列之间等待整个 wave 结束。慢 handler 会拖延其他任务。建议有限 worker pool、scope 内顺序、跨 scope 公平调度，增加 queue delay/oldest due 观测。不是把现有所有 timer 改成无界 goroutine。
3. **DO renew 调度**：host.js renewAll 对 claims 串行，每个请求最多 2s；Go 外层总等待只有 5s、每 10s 调一次。大量 shard/慢 COS 可使靠后的 lease 续期延迟。需要有限并发、按到期时间排序、批量续租接口与超时取消传播；保留每个 shard 的 owner/epoch 校验。
4. **运行时连接恢复**：本次发布记录已有 USW cell_unreachable 的案例。当前 liveness 仅 TCP，不会检测“端口存活而控制连接永久失效”。先用 agent IP/DNS 变更的真实 E2E 定位连接恢复；不能直接把依赖 ready 当 liveness，避免 bucket/agent 短故障引发集体重启。
5. **SQLite 容量保护**：MaxOpenCells 是 30s sweep 的软上限，不是瞬时 admission。多 cell 冷访问仍可短时越限。建议实际 RSS/FD/恢复 bytes 加准入和滞回，保留 pinned/busy 正确性；不能把 2c 对应的句柄数视为固定安全容量。
6. **恢复与 wake 的批次预算**：waker 的节点恢复、cleanup、due 派发共用串行 pass；wake.Due 对所有历史到期 minute prefix 做 List+逐条 GET。大 backlog 会增加恢复和派发时延。建议分页、每轮预算、持久进度与公平重试；这是后台路径，不应误称每次 KV 请求都 List。
7. **Cron 产品语义**：当前 docs/timers-and-dispatch.md:40 明确 best-effort、不补跑。漏掉停机分钟符合现契约。若要“可靠业务调度”，应独立设计补跑窗口、misfire 策略、执行历史与告警；不能悄悄修改现有语义。
8. **测试组合**：把默认 timeout + 双并发池、缓存结果过期、单租户慢 handler、无 metadata 覆盖等组合纳入回归。现有单模块成功路径测试通过不代表这些组合安全。

## 验证与文件

- repro.log：cellstore 3 个问题、控制配置、D1 原实现、JWKS 的隔离诊断。
- router-repro.log：预期非零退出，实际 fatal mutex；不是“测试通过”。
- linear-r2.log：D1 临时累计计数对照、R2 覆盖问题。
- queue-repro.log：每批持久提交次数。
- ci-tags.log：缺 tags 的预期编译失败。
- baseline.log：8 个相关 package 的既有测试，-count=1。
- overlay.json / linear-overlay.json：可重复的构建替换映射。
- 本轮没有运行生产压力/故障注入，没有证明生产吞吐提高，也没有据此声称项目不存在其他灾难恢复问题。

建议顺序：F1/F2/F3 → F4/F6/F7/F8/F9 → F5 与 Queue 批量优化 → 调度公平性/DO 重连与 renew → Cron 补跑等产品能力。F5 成本较小，也可作为独立的低风险性能修复提前做。
