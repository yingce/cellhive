# 冷写延迟根治：节点租约所有权、种子化 capture、单次列举冷恢复、跨 epoch 续链

状态：设计稿（2026-10-23），待实现。实现方第一步须把本文的决策落成 `docs/decisions.md`
的 ADR-273 ~ ADR-275（编号以当时最后一条为准），并同步 `docs/known-issues.md`、
`docs/cell-protocol.md`（及 `docs/en/` 镜像）。本文与 ADR 冲突时以 ADR 为准。

参考实现：`/opt/celld`（`crates/logic/types.rs` OwnerRecord、`crates/ltx/src/replica.rs`
`seed_pos`、`crates/celld/ltx_repl.rs` `activate_with` / `evict` / `nonempty_epochs` /
`epoch_chain`、`crates/logic/restore.rs`）。

## 1. 问题与目标

现象：几 MiB 的 KV cell，`put` 偶发约 10s；读不受影响；COS 同区内网。

验收（每条都要有 COS e2e 实测证据）：

| # | 场景 | 指标 |
|---|---|---|
| A1 | 持续读写 ≥ 3 分钟（跨多个租约周期） | put p99 < 200ms；`epoch_changes` 增量 = 0；写热路径 List = 0 |
| A2 | LRU 淘汰后对同一 cell 写 | 首写 < 300ms；Ensure 0 次 List、0 次段 Get |
| A3 | 同 node id、同数据目录 SIGKILL 重启 | 重启到首写成功 < 2s（不等租约 TTL）；之后 p99 < 1s |
| A4 | 跨节点接管（owner 进程被杀） | 不可用窗口 ≤ `LEASE_TTL` + 恢复耗时；接管后首写起 p99 < 1s |
| A5 | 删除本地数据目录强制冷恢复（几 MiB） | 冷恢复 + 首写 < 1s；bucket List ≤ 3 次 |
| A6 | 以上全部 | 已 ack 值 missing = 0、wrong = 0；SQLite `integrity_check` = ok |

A4 的不可用窗口是协议固有的：不能证明对方已死，就必须等它的租约过期。本设计不承诺消除这段窗口，只承诺不会再因为续约或冷路径额外多出秒级延迟。

不变量（任何改动都不得破坏）：

- I1 单写者：同一 scope 同一时刻最多一个进程能确认（ack）写入。
- I2 epoch fence：被 fence 的旧 owner 写入的数据，绝不会进入恢复所选的 lineage。
- I3 RPO=0：已确认的写入在单节点故障后仍可恢复。
- I4 写热路径（请求 → 确认）不调用 bucket `List` / `CommonPrefixes`（AGENTS.md）。
- I5 恢复只选连续 lineage，遇到缺口失败关闭。

## 2. 根因（已读代码核实，行号以 HEAD b9ac06b 为准）

| # | 根因 | 位置 | 触发 10s 的方式 |
|---|---|---|---|
| R1 | owner 记录自带 `Expiry = now + LeaseTTL(10s)`，只有后台循环续约：每 `TTL/3` 串行遍历 `OwnedScopes()`，整轮共用一个 `now`，单次调用无超时；每次 `Renew` = `Resolve` + SegmentedAuthority `CAS`，约 5~8 次 COS 往返 | `cmd/cell-agent/main.go:165`、`internal/owner/owner.go:193` | 持有的 cell 一多，一轮续约就超过 TTL，正在读写的 cell 也会过期 |
| R2 | `OwnedScopes` 只增不减：只有 `Release` 会调 `forgetOwned`，淘汰或空闲关闭都不会 | `internal/owner/owner.go:245` | 节点运行越久，续约越慢，R1 越严重 |
| R3 | `Claim` 发现记录过期就一律 `nextEpoch`，即使过期的是本进程自己持有的同一 epoch | `internal/owner/owner.go:172` | epoch+1 → capture 重建 → 新 epoch 小库在写路径上整库快照；`watchOwner` → `OnLostOwner` → `Forget`（`FORGET_ON_LOSS` 默认 true）→ 冷 hydrate |
| R4 | 同 node id 重启后 session 变了：旧记录还活着时返回 `503 owner_session_mismatch`，`Claim` 返回 `ErrOwnerLive` | `internal/server/server.go:1072`、`owner.go:160` | 重启后最长 10s 不可写，客户端重试直到成功 |
| R5 | LRU 淘汰（`MAX_OPEN_CELLS=32`、`CELL_IDLE=5m`）时 `Drop` 停掉 capture，已知的 durable 位置随之丢失 | `internal/cellstore/cellstore.go:547`、`internal/cellcapture/cellcapture.go:377` | 下次 `Ensure` → `canResume` → `HasEpoch`（全量 List `/ltx/`）+ `DurableWatermark`（List + 下载解码每个 `.batch`），否则整库快照 |
| R6 | 冷 hydrate：`LatestEpoch` 全量 List、`RestoreChain`→`Epochs` 再全量 List；每个 epoch 依次 fence Get、manifest Get、List，再逐个串行 `ReadSegment` | `cmd/cell-agent/main.go:285`、`internal/replica/replica.go:483,580,847` | 对象数 × RTT；小库由于 R7 有几十个对象 |
| R7 | compaction 门槛是 64 段或 64MiB，小 cell 常年挂着最多 63 个 L0；epoch GC 每 5 分钟跑一次，保留上一个 epoch | `internal/config/config.go:595`、`internal/compaction/compaction.go:160` | 放大 R6 |
| R8 | paged 打开（≥32MiB 且有 L1）的新 epoch 小于 256MiB 时要整库快照，而 `SnapshotStream`/`SnapshotPages` 会先 `HydrateAll` | `internal/cellstore/cellstore.go:1580,1682`、`cellcapture.go:339` | paged 省下的读取，在首写时全部重新付出 |
| R9 | ~~`liveFollowers` 每次提交都同步 List `nodes/`~~ | 已由 b9ac06b 修复（stale-while-revalidate） | 不得回退 |

附带的文档问题：ADR-160 第 2 点写 `PAGED_MIN_BYTES` 默认 256MiB，第 7 点和 `config.go` 是 32MiB，以代码为准修正文档。

## 3. 阶段 1：所有权改为由节点租约判活（ADR-273）

### 3.1 模型（对齐 celld `OwnerRecord{node, epoch}`）

- `cells/<scope>/owner.json` 仍是单写者的权威 fence（只能通过条件创建、CAS、条件删除修改），但**存活与否不再看记录里的 `Expiry`**。owner 存活定义为：

  ```
  OwnerLive(o, now) := lease(o.Node).Live(now) && lease(o.Node).Session == o.Session
  ```

  这里的 `lease` 是 `nodes/<node>.json`（`internal/lease`），每个节点每 `TTL/3` 发布一次，每节点 O(1) 次。
- 节点租约的 session 和 owner 记录的 session 不一致（同一 node id 已重启）时，记录视为**已失效，可被接管**，但必须先完成前任 session 的 recovery（见 3.4）。
- 续约成本从 O(cells) 降到 O(1)：**删除 per-cell 续约循环**（`main.go:165`）。owner 记录只在 claim、takeover、release 时写入。

### 3.2 数据结构与格式

`internal/cell/owner.go`：

```go
type Owner struct {
	Node, Role, Session string
	Epoch               uint64
	// Expiry is legacy (proto <= current). New records write 0 and set Liveness.
	Expiry       int64  `json:"expiry"`
	Liveness     string `json:"liveness,omitempty"` // "node-lease" for ADR-273 records
	Address, AdminAddress, ProtoVersion string
}
```

- 新增 `cell.ProtoVersion` 升级（如 `"3"`）；新记录写 `Liveness: "node-lease"`、`Expiry: 0`。
- `Owner.Expired(now)` **删除**，改为一个需要租约视图的判定函数，避免调用方误用旧语义：

  ```go
  // internal/owner
  type LivenessView interface {
  	// NodeLease returns the cached lease of node; ok=false when unknown/absent.
  	NodeLease(ctx context.Context, node string) (lease.NodeLease, bool, error)
  }
  func (m *Manager) Live(ctx context.Context, o cell.Owner, now time.Time) (bool, error)
  ```

  legacy 记录（`Liveness == ""`）仍按 `Expiry` 判活，以便在线读取旧桶数据，见 3.7。
- DO owner（`ClaimAs`/`RenewAs`，`internal/owner/as.go`，role 为 do-runtime 的远端任务）**不在本次范围**：它的 node 不是 cell-agent，也没有 `nodes/` 租约，继续用 `Expiry` 语义（`Liveness == ""`），代码路径保持不变。实现时须用 role/Liveness 把两条路径分开，并补测试锁定 DO 行为不变。

### 3.3 Claim 状态机（`internal/owner/owner.go` `Claim`）

读当前记录 `cur, etag`，按顺序判定：

| 条件 | 动作 | epoch |
|---|---|---|
| 无记录（`ErrUnowned`） | `nextEpoch` + `ConditionalCreate` | 新 |
| `cur.Node == self && cur.Session == self.Session` | **原样返回 cur**（自己持有，不写桶）；`rememberOwned(cur.Epoch)` | 不变 |
| `OwnerLive(cur)`，且 node 或 session 不同 | `ErrOwnerLive` | — |
| 不 live，`cur.Node == self.Node`，session 不同（同 node 重启） | `BeforeTakeover(cur)`（恢复前任 session）→ `nextEpoch` → `CAS(etag)`，见 3.4 | 新 |
| 不 live，其他节点 | `BeforeTakeover(cur)` → `nextEpoch` → `CAS(etag)` | 新 |

要点：

- R3 由第 2 行消除：自己持有的记录**永远不需要续约，也不会自我过期**。只要本节点租约还活着，记录就一直有效。
- 本节点租约**发布失败**的情况：`lease.Manager` 记下最后一次发布成功的时间 `lastOK`。当 `now - lastOK > TTL - margin`（margin 默认 `TTL/5`，与 `SegmentedAuthority` 的时钟偏差保护一致）时，本节点**自我隔离（self-fence）**：拒绝新的确认（`capturedWrite*` 返回 `ErrNotOwner`，即 503），直到再次发布成功。这一点是 I1 的前提：别人判定"你的租约已过期"的那一刻，你必须已经停止确认写入。实现位置：`lease.Manager.Healthy(now) bool`，在 `Server.capturedWriteTxID` / `capturedWrite` / `CaptureWait` 的入口检查，同时在 `commitSegmentCore` 里检查（覆盖 capture 循环自身的提交）。
- 自我隔离恢复后，**epoch 不变、无需重新 claim**：期间别人如果接管，必然已 CAS 改写了 owner 记录，本节点下一次 `requireOwnerEpoch` 会失败。所以恢复之后的正确性由 owner 记录 CAS 加 epoch fence 保证。
- `ResolveCached` 的 1s 缓存保持不变；`forwardOrClaimMode` 中的 `o.Expired(now)` 全部替换为 `!m.Live(ctx, o, now)`，租约视图走 `SampleCached` 的内存快照（b9ac06b 的 stale-while-revalidate），不在热路径上发起 List。缓存里查不到某个 node 时，单独 `lease.Get(node)` 点查一次，并按 1s TTL 缓存。

### 3.4 同 node 重启的快速接管（R4）

当前：同 node 的新 session 必须等旧 owner 记录过期（最长 10s）。

新的启动顺序（`cmd/cell-agent/main.go`，对齐 celld 的 "boot order recovers this node's own predecessor before the lease installs"）：

1. 读 `nodes/<self>.json`，得到前任 `prevSession`（可能为空）。
2. 若 `prevSession != ""` 且 `!= cfg.SessionID`：同步执行 `takeoverRecovery.Recover(ctx, nodeLog, self, prevSession)`。node-log 不存在或已 sealed 时走 fast path，否则 seal followers、上传尾部、标记 sealed。**失败则启动失败**（fail closed，不发布新租约）。
3. 然后才 `lm.Publish` 新 session 的租约。从这一刻起，所有 `Node == self && Session == prevSession` 的 owner 记录都满足"不 live"（session 不匹配），按 3.3 第 4 行接管，无需等待，`BeforeTakeover` 只需确认 prevSession 已 sealed（幂等，fast path）。

安全论证：
- 前任 session 的进程必须确实已经不在。同一数据目录上不会有两个进程，用 `flock(<DataDir>/.lock)` 强制保证。**当前代码没有这把锁，必须新增**：启动时以 `LOCK_EX|LOCK_NB` 获取，失败就退出。
- 光有 flock 还不够：SIGKILL 后前任的租约最多还会"活着" TTL 这么久，而且无法从租约本身看出它是不是这个数据目录的进程。为此，节点租约新增字段 `Instance`，值是数据目录里持久化的随机 ID（`<DataDir>/instance-id`，首次启动时生成，fsync 后保存）。判定规则：
  - prev 租约的 `Instance == 本目录 instance-id`，且本进程已持有 flock：前任必然已退出，**立即继续**。这是 A3 的快速路径。
  - `Instance` 不同（同一 node id 在另一台机器或另一个目录上运行，属于配置错误），且 prev 租约仍 live：**拒绝启动**，并在日志里说明 node id 冲突；租约过期后才允许继续。最坏情况退回到等 TTL，不会出现双写者。
  - 旧版本发布的租约没有 `Instance`：按"不同"处理，即等 TTL。这样做是保守的。

### 3.5 owned 集合收缩（R2）

本阶段删除了 per-cell 续约，`OwnedScopes` 不再决定续约成本，但它仍被 compaction 循环、epoch GC、rebalance、磁盘压力和 `/metrics` 使用，所以仍要可收缩：

- 新增 `owner.Manager.Forget(s cell.Scope, epoch uint64)`：只删本地 `owned` 条目，不写桶。
- `cellstore.Store` 新增回调 `OnEvicted(scope)`；cell-agent 接到 `om.Forget`。被淘汰的 cell **不 Release owner 记录**（celld 也是保留 owner，由节点租约判活），这样再次打开时 3.3 第 2 行会原样返回，epoch 不变，capture 可以凭 hint 续链（阶段 2）。
- 被淘汰后、`owned` 里已没有该 scope 时，compaction/GC 循环不会再处理它。这是可以接受的：冷 cell 不写，链不会增长。下次打开时会重新 `rememberOwned`。

### 3.6 删除与改名清单

| 位置 | 变化 |
|---|---|
| `cmd/cell-agent/main.go:161-181` per-cell 续约 goroutine | 删除 |
| `owner.Manager.Renew` / HTTP `handleRenew` | 保留 API（外部 committer 和 DO 用）；对 `Liveness=="node-lease"` 的记录变成 no-op，返回 cur |
| `owner.Manager.newOwner` | 写 `Liveness:"node-lease"`、`Expiry:0` |
| 所有 `o.Expired(...)`（`grep -rn "\.Expired(" internal cmd`） | 改为 `om.Live(ctx, o, now)`；DO/legacy 路径用 `o.ExpiredLegacy(now)` |
| `cellcapture` 的 `Owner` 回调（`main.go:727`） | `o.Node == cfg.NodeID && o.Session == cfg.SessionID`，不再看 Expiry（自己的租约健康度由 3.3 的 self-fence 负责） |
| `lease.NodeLease` | 新增 `Instance string`；`lease.Manager` 新增 `lastOK`、`Healthy(now)` |
| `<DataDir>/.lock`、`<DataDir>/instance-id` | 新增 |
| `recovery.Runner.Pass`（ADR-066，按 lease 判活） | 改为同时比较 session：lease live 但 session 不同，也视为前任 session 已死，可以恢复 |

### 3.7 升级立场

新旧版本**不能在同一个桶上混跑**：旧节点按 `Expiry` 判活，会把新记录（`Expiry: 0`）当作已过期，立刻接管，破坏 I1。要求：

- 全量替换：先停掉所有旧节点（走 graceful handoff，会 Release 全部记录），再启动新节点。
- fail closed：新节点启动时 `Sample` 一次 `nodes/`，发现任何 live 租约的 `ProtoVersion` 低于新版本就**拒绝启动**；运行中如果在租约快照里发现旧版本节点，就停止确认写入并告警。
- 旧记录（`Liveness == ""`、`Expiry > 0`）可以读：按 Expiry 判活；过期后由新节点按正常 takeover 接管并改写为新格式。
- 在 `docs/known-issues.md` 里写明，措辞与 ADR-266 "混合版本不安全，须全量替换" 保持一致。

### 3.8 测试（必须新增）

`internal/owner`（用 FSBucket + 假的 `LivenessView`）：
1. `TestClaimOwnLiveRecordKeepsEpoch`：同 node 同 session 重复 Claim，epoch 不变，不写桶（计数 bucket 断言 CAS = 0）。
2. `TestClaimForeignLiveBlocked`：对方租约 live 且 session 一致，返回 `ErrOwnerLive`。
3. `TestClaimAfterNodeLeaseExpiredBumpsEpochAndRecovers`：对方租约过期，调用 BeforeTakeover，epoch+1。
4. `TestClaimSessionMismatchTakesOver`：租约 live 但 session 已变，可以接管，并先调用 BeforeTakeover。
5. `TestBeforeTakeoverFailureKeepsRecord`：recovery 失败，记录不变。
6. `TestLegacyExpiryRecordReadable`：旧格式记录按 Expiry 判活。
7. `TestDOClaimAsUnchanged`：DO 路径行为不变。

`internal/lease`：`TestHealthyFalseAfterPublishFailuresBeyondMargin`，`TestInstanceIDPersisted`。

`internal/server`：
8. `TestSelfFenceRejectsAcksWhenLeaseUnhealthy`：租约发布失败超过阈值时，写返回 503，capture 循环也不提交。
9. `TestForwardOrClaimUsesNodeLease`：owner 节点租约 live 时转发，过期时 claim。

`cmd/cell-agent`（或抽到 `internal/` 的启动逻辑）：
10. 同目录重启，Instance 相同：不等待 TTL，重新 claim 后 epoch+1，并读到前任已 sealed。
11. 不同 Instance 且前任租约 live：拒绝启动。
12. flock 已被占用：拒绝启动。

## 4. 阶段 2：种子化 capture 位置 + 单次列举冷恢复（ADR-274）

### 4.1 原则（对齐 celld `seed_pos`）

> owner 在 epoch fence 下本来就知道自己的持久位置；列举只用于"位置未知"的情况，并且只能出现在冷路径上，每次冷打开最多一次。

`cellcapture.Manager.Ensure` 的位置来源按优先级依次是：

1. **hint**（同 epoch 淘汰后重建）：零次 bucket 操作。
2. **restored**（本次冷打开刚 hydrate 完）：用 hydrate 得到的 `(epoch 链, txid)`，零次额外操作。
3. **fresh epoch**（刚 Claim 得到的新 epoch，桶里必然没有该 epoch 的数据）：直接走"新 epoch 开局"（小库整库快照，大库或 paged 的走阶段 3 的续链），**不调用 `HasEpoch`**。
4. **unknown**：执行一次 `ChainView` 列举（4.3），只在以上三种都不满足时使用。

### 4.2 resume hint（R5）

`internal/cellcapture/cellcapture.go`：

```go
type resumeHint struct {
	epoch   uint64
	durable uint64 // barrier.durable() at Drop time
	session string // owner session that produced it
}
// guarded by m.mu
hints map[string]resumeHint
```

- `Drop(ctx, scope)`（淘汰路径，持有 `openMu`，**不能打开 cell**）：在 `st.cap.Done()` 返回之后，读 `st.cap.Stats().DurableTxID`，非零就写入 `hints[key]`。
- 写入 hint 的前提：capture 停止前 barrier 已覆盖本地最新提交，否则本地 txid 会大于 durable，下次就用不上 hint。所以 `Drop` 在 cancel 之前**有界等待 drain**（`st.cap.Wait(ctx, committedWatermark)`，上限 `CELLHIVE_EVICT_DRAIN`，默认 2s；超时不写 hint，只打日志）。committed watermark 通过 `SetCommittedWatermark` 已有的回调读取，不打开 cell。
- `Ensure`：hint 存在、`h.epoch == epoch`、`h.session == 本进程 session`、`h.durable == baseline == local TxID` 时，`resume = true`，跳过 `canResume`，**消费并删除** hint。条件不满足时也删除 hint，然后走 4.1 的后续来源。
- 以下情况必须删除 hint：`watchOwner` 发现 owner 已变；`Store.Forget/TryForget/ForgetWithVerify/ForgetPrefix`（新增回调 `OnForget(scope)`）；本节点 self-fence（3.3）。
- 安全性：同 epoch 只有一个写者（I1），本进程在该 epoch 的链上已证明 durable 到 `h.durable`；本地 txid 等于它说明没有未捕获的尾部。所以续链不会留下缺口（I5）。

### 4.3 `replica.ChainView`：每次冷打开只列举一次（R6，对齐 celld `EpochChain`）

`internal/replica/chain_view.go`（新文件）：

```go
// ChainView is one cold open's immutable view of a scope's durable chain.
// It is built with exactly one CommonPrefixes plus one List per non-empty
// epoch at or above the newest complete baseline, and every later question
// (latest epoch, restore plan, durable watermark, has-epoch) is answered from it.
type ChainView struct {
	Scope   cell.Scope
	Epochs  []uint64                 // ascending, from CommonPrefixes(<scope>/ltx/, "/")
	Objects map[uint64][]ObjectRef   // per epoch: key, size, parsed span (if any)
	Manifest map[uint64]*Manifest
	Fence   resourceFence
}
type ObjectRef struct{ Key string; Size int64; Span segmentSpan; SpanKnown bool }

func (m *Manager) BuildChainView(ctx context.Context, s cell.Scope, maxEpoch uint64) (*ChainView, error)
func (v *ChainView) Latest() (uint64, bool)
func (v *ChainView) Has(epoch uint64) bool
func (v *ChainView) Plan() ([]ObjectRef, uint64, error)        // restore set + watermark, no I/O when all spans known
func (v *ChainView) DurableWatermark(epoch uint64) (uint64, error)
func (m *Manager) Fetch(ctx context.Context, refs []ObjectRef, parallel int) ([]Segment, error)
```

构建步骤：
1. 第一次请求 `CommonPrefixes("<scope>/ltx/", "/")` 得到 `e<N>/`。ProviderBucket 嵌入了 S3Bucket，走原生 delimiter；FSBucket 已实现。**不再全量 List `/ltx/`**。
2. 从最新的 epoch 往旧的走：每个 epoch `ListSizes(Prefix(s,e))` 一次（拿到 size，供 paged 阈值判断），manifest 存在时 Get 一次。只要遇到一个带 `.snapshot-done` 的完整快照，或一个 L1 manifest 基线，就停止往更老的 epoch 走（更老的 epoch 不在恢复集合里）。
3. fence 读一次，走现有的 1s 缓存。

调用方改造：

| 调用方 | 旧 | 新 |
|---|---|---|
| `store.Hydrate`（`main.go:285`） | `LatestEpoch` + `RestoreChain` | `v := BuildChainView` → `refs := v.Plan()` → `Fetch(refs, 16)` → `restore.ApplyFile` |
| `store.Paged`（`main.go:256`） | `LatestEpoch` + `NewPageFetcher` | 复用同一个 `v`（`Latest()`、size 求和）；没有 L1 index 时回退 Hydrate，并**复用同一个 v** |
| `cellcapture.Durable/DurableEpoch/HasEpoch` | 各自 List | 改为可选的 `View func(scope) *ChainView`：本次冷打开留下的 v（见 4.4）；没有 v 时才 Build |
| `replica.Epochs` / `LatestEpoch` | 全量 List | 内部改用 CommonPrefixes（诊断和 GC 路径也会受益） |

在 cellstore 和 capture 之间传递 v：`cellstore.Cell` 新增字段 `Restored *RestoreInfo{View *replica.ChainView; TxID uint64; Paged bool; Cut uint64}`，由 Hydrate/Paged hook 填写（hook 签名改为返回 `*RestoreInfo`）。`Ensure` 读取它作为 4.1 的第 2 个来源，读完就清空，不长期占用内存。

### 4.4 并发读取

`Fetch` 用 `errgroup` 加信号量，`parallel` 默认 16（`CELLHIVE_RESTORE_PARALLEL`）。按 refs 顺序返回结果，不改变 `restore.ApplyFile` 的输入语义。任何一个对象出错，整体失败（fail closed）。celld 用的是 64，cellhive 先取 16，避免 COS 限流；e2e 时再调。

### 4.5 `.batch` 名带 span（消除"为了知道 span 而下载"）

现状：`ltx.BatchName` 是 `<start>-<end>.batch`。`parseSegmentSpan` 能解析出 txid 范围，但**分不清里面是快照部件还是 delta**，所以 `DurableWatermark` 要下载每个 `.batch` 并解码（`replica.go:167`）。

新命名（`internal/ltx/ltx.go` `BatchName`）：
- 纯 delta：`<start>-<end>.d.batch`
- 含快照部件：`<end>.p<a>-p<b>.s.batch`（与现有部件命名并存，只是多了 `.s`）
- 混合：`<start>-<end>.m.batch`（仍需解码，但只在罕见的混合批次出现）

`parseSegmentSpan` 把 `.d.batch`、`.s.batch` 识别为 `SpanKnown`；旧名（没有 `.d`/`.s`/`.m`）设为 `SpanKnown=false`，`Plan()`/`DurableWatermark` 对它们才下载解码，保持向后兼容。`isSegmentKey` 不变（仍以 `.batch` 结尾）。

### 4.6 小 cell 的 compaction（R7）

- 新增阈值 `CELLHIVE_COMPACTION_MIN_OBJECTS`，默认 8：一个 epoch 的 L0 对象数达到它就触发，与现有的 MinSegments/MinBytes 是 OR 关系（`compaction.go:160` 现在按段数计数，`.batch` 内部的多个段只算一个对象，对象数才决定冷恢复的 RTT）。
- compaction 循环（`main.go:1085`）的间隔不变，30s。
- epoch GC：在新 epoch 首个完整基线（快照 done 或续链 marker 加对应的 L1）形成后，再保留上一个 epoch，这一点不变。阶段 3 会让续链的 epoch 依赖它的前任，见 5.4。

### 4.7 测试（必须新增）

复用 `internal/replica/cold_restore_test.go` 的 `countingBucket`，补上 `List`、`CommonPrefixes`、`ListSizes` 的计数，放到 `internal/bucket/buckettest`（新增）供各包共用。

`internal/cellcapture`（`continuity_test.go`）：
1. `TestEnsureAfterDropUsesHintNoBucketOps`：写 → Drop → 再 Ensure，List=0、段 Get=0、Snapshot=0，后续写可以正常恢复。
2. `TestHintDiscardedOnEpochChange` / `OnForget` / `OnSessionChange`。
3. `TestHintIgnoredWhenLocalAhead`：Drop 后本地又多一条未捕获的提交，走慢路径，链无缺口。
4. `TestDropDrainTimeoutSkipsHint`。
5. `TestEnsureFreshEpochSkipsHasEpoch`：用 Claim 返回的 fresh 标记，`HasEpoch` 调用次数为 0。
6. `TestEnsureUsesRestoredInfo`：hydrate 之后，Ensure 不再列举。

`internal/replica`：
7. `TestChainViewSingleListing`：3 个 epoch × 20 个对象，Build 时 CommonPrefixes=1，ListSizes ≤ 需要的 epoch 数，Plan 不发起 Get。
8. `TestChainViewStopsAtCompleteBaseline`：更老的 epoch 不会被 List。
9. `TestFetchParallelPreservesOrderAndFailsClosed`。
10. `TestBatchNameSpanRoundTrip` 加 `TestLegacyBatchStillDecoded`：新旧命名混在同一 epoch，watermark 正确。
11. 现有的 `TestDurableWatermark*`、跨 epoch restore 测试全部保持通过。

`internal/compaction`：12. `TestCompactTriggersOnObjectCount`。

`internal/cellstore`：13. `TestEvictionCallsOnEvictedAndKeepsOwner`；14. `TestForgetCallsOnForget`。

## 5. 阶段 3：跨 epoch 续链（marker），写路径上不再做整库快照（ADR-275）

### 5.1 现状与风险

- 新 epoch 默认开一个整库快照（ADR-272 P4）。正因为每个 epoch 都以快照开头，`restore.selectChain` 才可以**不区分 epoch** 地拼接 base 之后的所有 delta（`internal/restore/chain.go:64`）。
- 只要允许续链（新 epoch 不开快照），就必须回答一个问题：被 fence 的旧 owner 在 epoch N 里晚到的 delta（txid ≥ 新 epoch 起点），会不会被拼进恢复结果？现有规则下会，这破坏 I2。celld 用 epoch marker 解决这个问题（`ltx_repl.rs:2489` 的注释，以及 TLA `BreakNoMarker`）。

### 5.2 marker 段

- 新增 `ltx.KindMarker`（或复用 `KindLink`；它当前只用于排序 rank，没有写入方，实现时确认后二选一，并在 `docs/protocol-formats.md` 里登记）。
- marker 的 header 为 `{Kind: Marker, Epoch: e, StartTxID: cut+1, EndTxID: cut, Commit: pageCount}`，0 页，对象名 `<cut+1>.marker`，放在 `cells/<scope>/ltx/e<e>/` 下，用条件创建写入（If-None-Match）。
- 语义：**epoch e 从 txid cut+1 开始；所有 epoch < e、txid > cut 的对象一律作废。**
- 写入时机：新 epoch 的 capture `Start` 之前，同步等它 durable（bucket 写入，不走 fleet）。**marker durable 之前 cell 不服务**（`Ensure` 不返回）。

### 5.3 restore 规则（`internal/restore/chain.go` 加 `ChainView.Plan`）

在现有"选最新的完整快照，加连续 delta 后缀"之前，先做 epoch 截断：

```
lo(e) = marker(e).StartTxID        若 e 有 marker
      = 最早完整快照(e).EndTxID+1  若 e 以快照开头
      = 未定义                      否则（旧数据：e 只有 delta，按旧规则处理）
对每个对象 o ∈ epoch e'：
   若存在 e > e' 且 lo(e) 已定义，且 o.EndTxID ≥ lo(e)：丢弃 o
   （o 跨过 lo(e) 的部分已被更高 epoch 取代；跨界对象整体丢弃，
     要求 lo(e)-1 之前的前缀能由 e' 的其他对象或基线连续覆盖，否则失败关闭）
```

- 快照开头的 epoch 截断规则同样适用：顺带修掉现有"晚到的旧 epoch delta 与新 epoch delta 在同一 txid 冲突"的隐患（现在会报 non-contiguous 并回退到更老的基线，可能静默变旧）。
- marker 不包含页，恢复时它只提供 `lo` 和 `Commit`（页数，用于截断检查）。

### 5.4 新 epoch 开局策略（替换 `canResume` 中 `!exists` 那一支）

| 本次打开方式 | 开局 | 写路径上的 bucket 操作 |
|---|---|---|
| hint 命中（同 epoch） | 直接续，无 marker | 0 |
| paged 打开（`RestoreInfo.Paged`） | **marker(cut)**，然后续链 | 1 次条件 PUT |
| 全量 hydrate，库 ≤ `EPOCH_SNAPSHOT_MAX`，且 `ChainView` 显示链对象数 ≤ `CELLHIVE_EPOCH_CONTINUE_MAX_OBJECTS`（默认 32） | **marker(cut)**，然后续链；后台异步做快照 | 1 次条件 PUT |
| 其余情况（链太长、或无法判断） | 写路径上整库快照（现状），小库代价可接受 | 快照部件 |
| 本地没有任何数据（全新 cell） | 现状：空库快照或直接写 delta | 不变 |

- 后台快照（`cellcapture` 新增 `BackgroundBaseline(scope)`）：在 capture 循环里执行（需要暂停 writer 并 checkpoint，复用 `rebaseline`）。paged cell 要等后台 hydrate 完成后才做（`HydrateRate` 的 complete 回调），**绝不在请求路径上 `HydrateAll`**。
- `SnapshotStream`/`SnapshotPages` 中的 `HydrateAll` 保留，作为兜底的正确性守卫，但改为只允许从后台调用：请求路径上的调用会 panic（测试构建）或计数告警（生产构建）。
- 删除 `CELLHIVE_EPOCH_SNAPSHOT_MAX` 和"大库跨 epoch 续链"的特殊分支，统一由本表决定。配置项保留一个版本，标为 deprecated，然后删除（`config.go` 已有 deprecated 映射机制）。

### 5.5 epoch GC

`replica.RetireEpochs`（`replica.go:444`）现在的条件是"当前 epoch 有 `.snapshot-done`"。改为：当前 epoch 有**独立基线**（自己的 snapshot-done，或者 L1 manifest 且 `manifest.MinTxID == 1`）时，才删除更老的 epoch。只有 marker、还依赖前任的 epoch，**不触发 GC**。compaction 把"前任基线加本 epoch 的 delta"折叠成本 epoch 的 L1 之后，本 epoch 才算独立。这需要 compaction 支持读取跨 epoch 的输入，在 `compaction.Compact` 里改用 `ChainView.Plan()` 作为输入集合。

### 5.6 测试（必须新增）

`internal/restore` / `internal/replica`：
1. `TestMarkerTruncatesFencedOldEpochTail`：e1 有快照和 delta 1..10；e2 有 marker(lo=8) 和 delta 8..12；e1 晚到的 delta 8..9。恢复结果 = e1[1..7] + e2[8..12]，晚到的数据被丢弃。
2. `TestMarkerWithoutPredecessorPrefixFailsClosed`：lo=8，但 e1 只能连续覆盖到 5，恢复报错。
3. `TestSnapshotEpochAlsoTruncatesOlderTail`。
4. `TestLegacyDeltaOnlyEpochStillRestores`。
`internal/cellcapture`：
5. `TestPagedNewEpochWritesMarkerNoHydrateAll`：用计数 VFS 断言请求路径上 `HydrateAll` 调用次数为 0。
6. `TestCellNotServedBeforeMarkerDurable`：注入 marker PUT 失败，Ensure 返回错误。
7. `TestBackgroundBaselineAfterHydrateComplete`。
`internal/replica`：8. `TestRetireEpochsKeepsDependedPredecessor`；9. `TestRetireAfterCrossEpochCompaction`。
`internal/compaction`：10. 扩展 `TestPagedCellAgentEndToEnd`：接管 → marker → 写 → compaction → 冷恢复，结果逐字节一致。

## 6. 可观测性（先于其他改动落地，用于对比基线）

- 冷路径分段耗时日志，事件名 `cold_open`（对齐 celld 的 `restore_plan`）：`scope, epoch, mode(hint|restored|fresh|view), claim_us, view_us, fetch_us, apply_us, ensure_us, objects, bytes, lists, gets`。
- 新增指标：
  - `cellhive_cold_open_seconds{phase}` histogram
  - `cellhive_bucket_ops_total{op,path="hot|cold|bg"}`，在 ctx 上打 path 标签，由 `ProviderBucket` 统计
  - `cellhive_owner_self_fence_total`
  - `cellhive_capture_hint_total{result="hit|miss|stale"}`
  - `cellhive_epoch_marker_total`
- 热路径断言：测试构建中，ctx 被标为 hot 时若调用了 `List`/`CommonPrefixes`，直接 panic（`internal/bucket` 新增 `HotPathGuard`）。这样以后再加 List 时 CI 会立即暴露 I4 被破坏。

## 7. COS e2e（`scripts/e2e/cold-write-cos.sh`）

- 凭据：从 `CELLHIVE_COS_ENV_FILE`（`/opt/.secrets/s3`）读取 `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`，映射为 `COS_SECRET_ID`/`COS_SECRET_KEY`。**忽略并 unset** 文件中的 `AWS_BUCKET`/`AWS_ENDPOINT`（它们指向 `vwork-hk`，已知 AppendObject 返回 405）。任何输出都不能出现凭据值，读取凭据的那段不开 `set -x`。
- 桶：`CELLHIVE_BUCKET=cos://vwork-e2e-hk-1376795072`，`COS_ENDPOINT=https://vwork-e2e-hk-1376795072.cos.ap-hongkong.myqcloud.com`。2026-10-23 已用这组凭据通过 `TestCOSConditionalDelete`、`TestProviderResourceFence`、`TestProviderCloudDiagnose`。
- 隔离：每次运行使用唯一的 namespace（`e2e-cold-<ts>-<rand>`），只删除本次运行创建的 scope 前缀。
- 场景对应 §1 的 A1~A6。每个场景前后各抓一次 `/metrics`（`cellhive_bucket_ops_total`、`cellhive_owner_epoch_changes_total`、`cellhive_cold_open_seconds`），客户端逐条记录 put 延迟，最后全量回读核对 missing/wrong，并跑 `PRAGMA integrity_check`。
- 基线：用 `git worktree` 在改动前的 commit 上构建，跑同一个脚本，输出两列对比表。
- 输出：`docs/archive/bench/cold-write-cos-<date>.md`（只写数字和命令，不写凭据）。

## 8. 实施顺序与交付

| 步 | 内容 | 依赖 | 完成判据 |
|---|---|---|---|
| 0 | ADR-273~275（§6 可观测性并入 ADR-274） + known-issues/cell-protocol/protocol-formats 更新；修正 ADR-160 阈值 | — | 文档评审 |
| 1 | §6 可观测性 + HotPathGuard + e2e 脚本骨架；跑一次**基线** e2e | 0 | 基线数据入档 |
| 2 | §3 所有权（含 flock、instance-id、self-fence、启动恢复顺序） | 1 | §3.8 全绿；`make test`；`crash-e2e` 通过 |
| 3 | 对抗式 review：I1/I2 交错、时钟偏差、混合版本、SIGKILL 时机 | 2 | high/medium 清零 |
| 4 | §4 种子化 capture + ChainView + 并发 Fetch + batch 命名 + 小 cell compaction | 2 | §4.7 全绿；`deploy-e2e` 通过 |
| 5 | §5 marker 续链 + 后台基线 + GC | 4 | §5.6 全绿 |
| 6 | 对抗式 review：I3/I5、marker 截断、GC、paged | 5 | high/medium 清零 |
| 7 | §7 COS e2e 新旧对比 | 6 | §1 A1~A6 全部达标并入档 |

每一步单独提交，提交信息格式参照仓库现有风格（`fix(owner): ...`）。第 2 步和第 5 步改动了协议，必须附带 ADR 号。

## 9. 明确不做

- 不改 DO（do-runtime）owner 路径（`ClaimAs`/`RenewAs`）。
- 不引入 etcd/NATS 等外部协调组件（AGENTS.md）。
- 不承诺消除跨节点接管时最长 `LEASE_TTL` 的不可用窗口（A4）；如需缩短，单独评估 `LEASE_TTL` 下调对 COS 请求量的影响。
- 不在 workerd fork 中做任何修改。

## 10. 开放问题（实现前需确认）

1. marker 是新增 `KindMarker`，还是复用目前没有写入方的 `KindLink`？倾向新增，语义更清晰。
2. `EPOCH_CONTINUE_MAX_OBJECTS` 的默认值（32）需要在 e2e 中校准。
3. self-fence 的 margin（`TTL/5`）需要与 `SegmentedAuthority` 的时钟偏差保护值对齐，实现时读 `segmented.go` 的 `nearBoundary` 常量确认。
4. 3.5 中被淘汰的 cell 保留 owner 记录：会让 rebalance 看到的"负载"偏低。rebalance 默认关闭，可以接受；开启 rebalance 的部署需要注意。
