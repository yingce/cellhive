# Cell 协议

本文件定义 CellHive 状态层的核心协议：身份与寻址、单写者租约、代次围栏、复制与持久化证明、发现机制、bucket 约束。实现语言为 Go，复制实现参照 LiteFS/`superfly/ltx`。

## 1. 什么是 cell

**cell = 一个有名字、有独立 SQLite 数据库的状态单元。** 它是最小的一致性/故障/迁移单位，等价于一个 Durable Object。

```
scope = <namespace>/<class>/<id>
```

| class 示例 | 承载 |
|---|---|
| `__kv__` | 一个 KV namespace |
| `__d1__` | 一个 D1 数据库 |
| `__queue__` | 一个队列 |
| `__workflow__` | 一个 Workflow 定义实例集 |
| `__cron__` | cron 投影（计时器） |
| `<DOClass>` | 用户 Durable Object 类 |

**持久寻址**：`(class, id)` 稳定不变，运行时按 scope 解析 owner。对象未激活时仅存在于对象存储，成本近似为零。

## 2. 两种存储后端，一套协议

cell 协议只有一套（租约 / epoch / 复制 / RPO / 寻址 / 发现 / 诊断），但有两种 SQLite 属主：

| 后端 | 工作 SQLite 属主 | 用于 | 捕获方式 | owner 节点 |
|---|---|---|---|---|
| **A** | `cell-agent`（Go） | KV / D1 / Queue / Workflows / Cron | 自己控制事务与 WAL，精确捕获 | `cell-agent` 节点 |
| **B** | workerd（`do-runtime`） | Durable Object | do-runtime 的 supervisor 只读观察 WAL | `do-runtime` 节点 |

统一规则：**owner = 持有该 cell "工作 SQLite"的节点。** 后端 B 的 owner 记录由 `cell-agent` 协调并写入（指向 do-runtime 地址 + epoch）。后端 B 的差别与风险见 [`durable-objects.md`](./durable-objects.md)。

### A/B 协议一致性与差异

**一致（同一套 cell 协议）**：scope/身份、owner 记录条件写、epoch fence、LTX 复制与 `e<epoch>` 前缀、持久化证明与输出门（RPO=0）、epoch 链/快照恢复、接管/recovery、桶硬要求、禁止 LIST、计时器抽象。

**差异（后端 B）**：

| 维度 | A（普通 cell） | B（DO） |
|---|---|---|
| 工作 SQLite 属主 | `cell-agent` | workerd（do-runtime 本地） |
| 捕获方式 | 自己控制事务/WAL（可 hook、可调 checkpoint） | supervisor 外部只读观察 WAL |
| owner 节点 | `cell-agent` 节点 | `do-runtime` 节点（owner 记录含 `role`） |
| owner 记录写入者 | cell-agent 自己 | cell-agent 代 do-runtime 写 |
| LTX 上传者 | cell-agent 直接上传 | do-runtime 捕获 → 上报 cell-agent → cell-agent 上传 |
| 写 ack 路径 | cell-agent 内部 | 多一跳（捕获→cell-agent，可跨节点） |
| 存储形态 | 每 cell 一个 SQLite | workerd actor 存储多文件（共享 metadata + 每 actor） |
| 快照 | 精确可控 | 受 workerd checkpoint 行为约束 |
| FUSE 选项 | 不适用 | 可选（自管环境） |

## 3. owner 记录与单写者

每个 cell 在对象存储上有一个 owner 记录：

```
cells/<scope>/owner.json
{ "node": "<owner-node-id>", "role": "cell-agent|do-runtime",
  "session": "<session-id>", "epoch": 42,
  "expiry": 1730000000000, "address": "10.0.0.12:8788" }
```

**获取**：条件写。

- 无记录 → **条件创建**（`If-None-Match: *`）；
- 已有记录 → **CAS**（`If-Match: <etag>`）。

对象存储只接受其中一次写，因此两个节点不可能同时拥有同一个 cell。

**写入者**：后端 A 由 owner 的 `cell-agent` 自己写；后端 B 由 `cell-agent` 代写（do-runtime 不持桶凭据，claim 时把自己的 advertise 地址交给 cell-agent）。

**续租**：cell 的 owner 记录**不在热路径续租**，只在激活/接管/迁移时写；存活由节点 lease / 复制活动判定（见 §4）。

**自 fence**：节点在 lease 过期前无法续租且无法复制时，停止其名下所有 cell、拒绝未完成请求、退出（由外部 supervisor 重启）。fence **不写存储**：对端凭 lease 记录已判其死亡。

## 4. 节点 lease 与存活判定

### cell-agent 集群（固定，自注册）

```
nodes/<node>.json
{ "node": "...", "session": "...", "advertise": "10.0.0.12:7001",
  "expiry": ..., "format": ..., "load": { ... } }
```

- 每节点一条，按 TTL/3 续租；TTL 默认 10s（可配）。
- 节点 lease 是**存活证明**：节点失联 → 其名下所有 cell 可被接管。
- `load` 暴露伸缩信号：`owned_cells`、`placement_weight`、`resident_cells`、`host_websockets`、`rss_bytes`、`cpu_percent_x100`、`pressured`、`memory_headroom`、`shed_cells`、`restoring`、`sampled_ms`。
- **fleet capacity sample**：每节点每 5s 读一次共享快照；由一个节点刷新（读各 lease），其余读结果，避免 O(cells)。

### do-runtime（弹性，无自注册、无心跳）

- do-runtime **不持桶凭据**，因此不能像 cell-agent 那样写 `nodes/*`；
- 其存活由**复制活动隐式判定**：只要还在向 cell-agent 上报 WAL / 续约 owner，即视为存活；停止上报 → owner 租约到期 → 可接管；
- **接管候选**不靠心跳，走平台服务发现（K8s Endpoints / mesh）；详见 §4.1。

### 4.1 发现机制

| 目标 | 机制 |
|---|---|
| `cell-agent` 集群 | `nodes/*` bucket lease（自注册） |
| A 类 cell owner | bucket owner 记录 + 本地缓存（**点查，不 LIST**） |
| DO owner | owner 记录（指向 do-runtime 地址 + epoch），cell-agent 写 |
| `user-runtime` 派发 | 逻辑服务名（mesh），任意健康副本 |
| do-runtime 存活 | **复制活动隐式判活**（无专门心跳） |
| do-runtime 接管候选 | 平台服务发现；按需探测（选出后请其 claim） |

**调用方（workerd / supervisor）如何找 cell-agent**：用**逻辑服务名**（mesh / K8s Service），任一副本即可；非 owner 副本会按 owner 解析把请求**转发给 owner**，所以调用方无需知道具体机器。

**peer（cell-agent 之间）如何找具体节点**：

- 每节点启动/续租时写 `nodes/<node>.json`（bucket），含 **advertise 私网地址（或 pod DNS）、session、epoch、format、load**；
- 需要找 peer 时读这些 lease（**点查**）+ 共享 fleet sample（每 5s，由一个节点刷新，避免 O(n) 读）；
- ensemble 的 follower：owner 从**活 lease** 中选（排除自己，最多 2 个）；
- 接管/转发：用 owner 记录里的地址（DO 指向 do-runtime，A 类指向 cell-agent）；地址失效先按 lease TTL 判死再重选。

**地址内容与失败检测**：`advertise` 是内网可达地址；`session`/`epoch` 区分实例代次；**`expiry` 总是 ≤ 节点 lease TTL**（default 10s），以保证"节点 lease 过期 → owner 记录同步失效"语义一致；TTL/3 续租。节点 lease 是存活主证明；owner record 的 expiry 是快路径判断用，二者过期时间绑定。

**跨编排器**：bucket node lease 是**权威**；K8s headless Service / pod DNS 只提供**传输可达性**，不承担身份与 epoch。

**与 do-runtime 的区别**：do-runtime 不持桶凭据、不自注册，故其存活靠复制活动、候选靠平台服务发现（§4 上文）。

**非 K8s 环境（含 do→cell 端点发现）**：

- **默认无第三方注册中心**：用**可配置的 service name（可多个）**，由 **K8s / Docker Compose 内置 DNS** 解析——Compose 的 `<service>` 名返回所有副本 A 记录；K8s 用 Service（ClusterIP 或更推荐的 **headless** 返回 pod IP）。客户端（Go HTTP）解析全部 A 记录后 **round-robin / 失败重试**；对 DO 协调建议**按 scope 哈希选 cell-agent**，保证同一 DO 的 WAL 由同一 cell-agent 协调。裸机/VM 用**种子列表**或本地 DNS。
- **do-runtime → cell-agent（找集群入口）**：同样用上述 service name / 种子列表；do-runtime **不持桶凭据、不能**通过 `nodes/*` 自发现。需要动态/健康感知时才考虑 **Consul/Nomad**、给 do-runtime **只读且限定 `nodes/*` 的 scoped 凭据**、或由 cell-agent **反向下发**端点。
- **do-runtime 接管候选**：优先平台发现（K8s/Consul/Nomad）；无平台发现时 **do-runtime 注册/心跳到 cell-agent**，或配置列表。
- 核心 peer 发现（`nodes/*` lease）与 owner 记录仍基于 bucket，**不依赖 K8s**。do→cell 为内部直连（HTTP），**不经 Traefik**。

## 5. epoch fence

- 每次激活/接管/迁移：`epoch += 1`。
- 复制数据写入 `cells/<scope>/ltx/e<epoch>/...`。
- 丢失 owner 的节点即使继续写，其数据只落到**已废弃的 epoch 前缀**，恢复时只选取当前 lineage。

即：**epoch 是"写入有效性"的围栏**，配合 owner 记录上的代次校验，让旧 owner 的写入无害化。

## 6. 复制与 RPO=0

### 复制格式

- SQLite 提交事务被捕获为 **LTX**（事务日志段），上传到当前 epoch 前缀（普通 PUT）。
- 后端 A：`cell-agent` 直接捕获自己拥有的 SQLite（**ADR-092**：KV/D1/Queue/Workflow/Vectorize/timer/control 写路径经 `internal/cellcapture` 接 `sqlcapture` → LTX → fleet 证明，bucket 异步；冷恢复经 `cellstore.Hydrate` + `replica.LatestEpoch`；`/v1/internal/{append,commit}` 亦可供外部 committer）。
- 后端 B：`do-runtime` 的 supervisor 捕获 WAL 段，**上报给 cell-agent**，由 cell-agent 执行复制与证明（见 [`durable-objects.md`](./durable-objects.md)）。
- 快照：对大数据 cell，接管时先恢复一个整库快照（L9）以避免回放全部历史；否则物化 L0 链。
- **compaction**：后台把大量 L0 段折叠为 L1（`min_txids` / `min_mb` 触发），使接管只读少量对象而非成千上万。
- **paging**：超过阈值的 cell 采用稀疏文件 + 按需分页，冷启动不必整库下载。
- **checkpoint 对齐（I-03）**：后端 B 的 supervisor 轮询 `(salt, frame)`；salt 变化或 frame 归零 = 发生 checkpoint → 暂停 delta，用 **SQLite Online Backup API**（只读连接）取一致快照，上传为 **L9 快照**并记 txid，再从 frame 0 续 delta 并写 **snapshot→delta 衔接标记**。P0 必测"活跃写入期间 checkpoint"。

### 持久化证明（输出门）

写入在向调用方确认前，必须被证明已持久化：

| 模式（`CELLHIVE_DURABILITY`） | 证明 |
|---|---|
| **fleet**（优先；`auto` 有 live follower 时选它） | owner 把写入发给 1–2 个 follower（其他 `cell-agent` 节点），**至少 1 个 follower fsync 落盘即确认**；对象存储上传在确认之后异步进行。**无可用 peer 时不报错**，退化为"等 bucket"（见下），保证 RPO=0 |
| **bucket** | 只用对象存储：按块上传，等待该块完成（`CELLHIVE_BUCKET_WAIT=true`，同区 ~90ms）|

> `fleet`/`bucket` 是**持久性要求**，不是"必须有 peer"：`fleet` 无 peer 时退化为等 bucket（writes wait for the bucket），不静默 ack。

- owner 本地提交算 1 份；**再加 ≥1 个 follower 落盘**即构成证明，故 **2 个 cell-agent 节点即可**（owner + 1 follower）。
- owner 招募**最多 2 个 follower**（最多 3 副本）；只要还剩 1 个 follower 就继续 fleet 确认，不必退回逐笔等 bucket。
- follower 必须是**其他节点**（owner 从不把自己算作 follower）。
- **S3 不在写 ack 路径**（fleet 模式）：同区稳态写延迟约 ~25ms，由 follower fsync 决定。
- 多节点下 **bucket 上传是后台异步、可合批/周期进行**（LTX 段合并 / group commit），**不是逐笔 PUT**；bucket 仍是长期权威与恢复来源。
- 只有 **无可用 follower**（单节点，或 ensemble 不可用降级）时，ack 才退化为**等 bucket 块上传**（`bucket-batch`，同区 ~90ms）；显式 `BUCKET_WAIT=false` 才改为 ack-on-enqueue（RPO>0）。
- 后端 B（DO）：do-runtime 上报给 cell-agent 后，由该 cell-agent 作为 owner 向其他 cell-agent（follower）做同样的 ensemble 证明。
- 后端 B 的写 ack 路径为：workerd 提交 → supervisor 捕获 → **cell-agent（可能跨节点）** → 证明 → 输出门；跨节点内网延迟必须纳入 P0 性能门。
- 响应体流式输出时，逐块同样受输出门约束——调用方不会基于"可能丢失"的值行动。
- **RPO=0**：已确认（acknowledged）的写不会因单节点故障丢失。

### 单节点（bucket）模式（`CELLHIVE_DURABILITY` / `CELLHIVE_BUCKET_WAIT`）

单节点没有 follower，写确认退化为对象存储证明。配置分两个轴：

- **`CELLHIVE_DURABILITY`** = `auto`（默认）| `fleet` | `bucket`：选证明方式。
  - `auto`：有 live follower 走 fleet，否则走 bucket。
  - `bucket`：只用 bucket，不尝试 peer。
  - `fleet`：**要求 RPO=0**；有 peer 走 follower fsync，**无 peer 则退化为"等 bucket"并告警**（不静默 ack）。因此 `fleet` + `CELLHIVE_BUCKET_WAIT=false` 是矛盾配置，启动即拒绝。
- **`CELLHIVE_BUCKET_WAIT`** = `true`（默认）| `false`：bucket 姿态下**是否等该块上传完成**。（`fleet` 恒为等。）

bucket 提交**以"块"为单位**（同一 `(scope, epoch)` 的一批写攒成一个对象，块名用 txid 区间、有序）：

| bucket 子模式 | ack 时机 | RPO | 触发 |
|---|---|---|---|
| `batch`（默认） | 攒块；**等该块提交完成**再 ack（空闲立即 flush、并发自然合批） | 0 | `BUCKET_WAIT=true`，或 `DURABILITY=fleet` 降级 |
| `async` | **入队即 ack**；块在后台**串行异步提交**（有界队列，溢出回退同步） | **>0**（崩溃可能丢窗口） | `BUCKET_WAIT=false` |

- 两模式共用 `internal/upload` 合批器；`batch` 用 `EnqueueWait`，`async` 用 `Enqueue`。**未配置合批器**时退化为逐笔直接 PUT（`mode:"bucket"`）。
- **不再有独立 `sync` 模式**：`batch` 在单并发（每次立即 flush）下即等价于逐笔同步 PUT，且 RPO 相同，故并入 `batch`。
- **顺序保证**：合批器是**单循环**——`AppendBatch` 完成前不收下一批，因此同一 `(scope, epoch)` **不会有两个块并发上传**；块内按写入顺序打包，块名 `txid 区间` 递增；`Restore` 按 txid 重排。测试 `TestBatcherBlocksOrderedAndSerial` 覆盖（成块 `< N`、顺序 `0..N-1`、`maxInflight == 1`）。
- **约束**：块对象是 **create-once（If-None-Match）且按 txid 区间命名**，故同一 `(scope, epoch)` 的 txid 必须**单调递增、不可重用**；压测/重放若复用 txid 区间会命中同名对象而 412。
- **远程/跨洋对象存储**下 `bucket`（RPO=0）尾延迟数秒不可用；此时要么上 ≥2 节点走 fleet，要么显式接受 `async` 的 RPO>0。

### 接管

1. 检查前任 owner 的 node-log 记录；若 open/recovering，先 **recovery**：fence 记录、seal followers、上传其保留段、标记 sealed；
2. 沿 epoch 链恢复（快照起点或 predecessor 的衔接点）；
3. 领取 owner 记录（epoch+1）后开始服务。

大规模 dead node 可能让恢复持续数分钟；期间请求等待，按退避重试，超过预算才失败（可配）。

### node-log 与 recovery 协议（C-03 fix）

**node-log** 是每个 cell-agent 节点在 **fleet 模式**下维护的会话记录，用于保证 "follower fsync 已确认但 bucket 上传尚未完成" 的写入在节点崩溃后可被恢复。

**存储位置**：bucket `node-logs/<node>/<session>.json`，由本节点写。

**生命周期**：
- **open**：节点启动并收到第一次 fleet-mode 写 ack 时，条件创建。记录：`{node, session, epoch, followers, status="open"}`。
- **recovering**：另一节点看到 open 记录并开始执行 recovery 时设为 recovering（CAS）。
- **sealed**：recovery 完成后设为 sealed。节点优雅停止时也主动 seal。
- **失效**：节点重启后不使用旧 session；旧 log 留给 recovery 处理。

**Recovery 执行**（新 owner 接管前）：
1. 读 bucket `node-logs/<dead-node>/` 下所有 open/recovering 记录；
2. 对每个 open session，CAS 设为 recovering；
3. 联系 session 记录的 followers（地址来自其当时的 node lease）请求 seal（流式发送它们持有的未上传段）；
4. 将收到的段上传到对应的 `cells/<scope>/ltx/e<epoch>/`；
5. 确认完整后标记 session sealed；
6. 继续正常接管流程。

**fast path**：若节点优雅停止（SIGTERM 完成 handoff），它自己 seal log 并确保上传完成，接管直接跳 recovery 步骤。

**自动编排（ADR-066）**：上述 recovery 不再依赖人工触发。fleet waker 的 leader 每轮 pass 顺带执行 `recovery.Runner.Pass`：用 `nodelog.Nodes` 枚举有 node-log 的节点，**以 node lease 判活**（存活→跳过，绝不恢复；缺失/过期→视为死亡；lease 读取出错→跳过以保证 fail-safe），对死亡节点执行上面的 1–6。整个过程**幂等**（sealed 快速路径、open→recovering→sealed 单向），可与 timer 派发共用同一次 leader 选举。

**follower seal 不可达**：若某 follower 在 recovery 期间也死了，其保留段不可获取 → recovery 从 bucket 已上传部分恢复（可能丢失该 follower 确认后但未上传的最后一段）→ RPO 退化为"已传段"的精确点。这是 fleet 模式下双节点同时崩溃的极端情况，≥3 节点时有第二 follower 兜底。

## 7. 优雅 handoff

节点关停（SIGTERM）时：

1. 标记 draining，停止接受新 cell；
2. 逐批（如最多 32 个）对 cell：停止本地新路由 → 等待在途完成 → 关闭数据库并发布完整快照、校验可恢复 → 释放 owner 记录 → 请求一个兼容 peer 领取（peer 确认后保持 dormant）；
3. `cell-agent`：确认复制/快照完成后再退出；`do-runtime`：确认后再停 workerd。

同时关停由 **bucket drain token** 串行化，避免瞬时冲击存活节点。

**drain token 协议（M-07 fix）**：
- drain token 是 bucket 对象 `fleet/drain-token.json`，持有节点名 + session + expiry（默认 30s）；
- 关停时节点通过条件创建/CAS 抢 token；抢到才开始逐批迁移 cell；
- 节点优雅停止后删除 token（或让其过期），下一个节点可继续；
- 若持有节点崩溃，token 在 TTL 后自动过期，新节点可抢；
- 多节点并发关停（如滚动升级）时，至多一个节点处于活跃 drain 阶段；其余在等待队列中自旋重试（退避间隔 5–10s）。

## 8. bucket 硬要求

对象存储必须提供：

1. **条件创建**（不存在时写才成功）；
2. **条件覆盖**（读后被改则写失败）；
3. **read-after-write 一致性**；
4. **ranged read**（返回请求的字节范围）；
5. **条件删除**（旧版本不得删除新 owner/lease；S3 用 `If-Match`，原生追加式 provider 用当前位置的 tombstone）。

| 供应商 | 是否合格 |
|---|---|
| S3 API 对象存储 | 逐实例验证条件创建、目标 CAS、条件删除与 ranged read；不能仅凭协议名称判定合格 |
| 本仓固定 MinIO `RELEASE.2025-02-18T16-25-55Z` | ❌ 2026-09-22 实测忽略 stale `DeleteObject If-Match`；旧四条件写通过不等于 owner fencing 可用 |
| 实测 COS/OSS S3 端点 | ❌ 实测普通 S3 条件创建不成立；不能将其用于 owner/lease |
| 原生 `oss://` | OSS 北京测试桶的追加式并发 claim、owner 接管与启动探针通过；其它桶逐实例验收 |
| 原生 `cos://` | COS 香港 `cell-1376795072` 测试桶的追加式并发 claim、CAS/条件删除与启动探针通过；原 `vwork-hk-1376795072` 桶返回 405；其它桶逐实例验收 |

**启动探针**：每个节点运行条件创建、重复拒绝、CAS、旧版本拒绝、ranged read，以及旧版本条件删除拒绝 / 当前版本删除；任一失败即退出。运维的 `cellhive diagnose` 在已启动节点上再次执行该**写入式**探针，不是只读检查。详情见 `storage-and-s3.md` / ADR-188。

### 桶角色（拓扑）

| 角色 | 默认 | 内容 | 可拆为独立桶 |
|---|---|---|---|
| `state` | 主桶 | cell 复制数据、owner/lease、nodes | ✅ |
| `code` | 主桶 `bundles/`、`deploy/` | Worker bundle（内容寻址）、版本指针 | ✅ |
| `assets` | 主桶 `assets/` | 静态资产（可挂 CDN） | ✅ |
| `r2` | 主桶 `r2/` | 租户 R2 虚拟桶 | ✅ |
| `backup` | 无 | 可选归档 | ✅ |

**默认单桶 + 保留前缀；每个角色可覆盖为独立桶/凭据/生命周期。** 长期凭据只由 `cell-agent` 持有（ADR-023）。

## 9. 禁止扫描（bucket 交互预算）

- 热路径**只允许按 key 点查**，**禁止 LIST**。
- bucket 交互预算 = **O(节点数 + 激活/迁移次数）**，与 cell 数、请求数、计时器数解耦。
- 枚举（如列出某 class 的实例）只允许在运维命令中，且有界分页（如 ≤1000/页，游标续读）。

## 10. 计时器（统一 due 抽象）

所有定时事件统一为：

```
timer = { dueAt, kind, scope, token }
kind ∈ { do-alarm, cron, queue-delay, queue-retry, workflow-sleep, workflow-timeout }
```

- **存储**：due 记录落在**各自所属 cell 的 SQLite**（按时间建索引/时间桶）。**不建 bucket 计时索引。**
- **触发**：两层——
  - **本地 due**：每个 `cell-agent` 为其拥有的计时器 cell 维护本地最早到期结构；到点本地派发到目标（`do-runtime` / `user-runtime.scheduled()` / `user-runtime.queue()` / workflow 执行），零 bucket 交互；
  - **单 fleet waker**：bucket 租约选出的一个 leader，兜底 **owner 已死/失联**的到期项——先按 owner 记录接管相应 cell，再读其 SQLite 派发。
- **派发寻址**：cron/queue/workflow 统一发给 `user-runtime` 的**逻辑服务名**（mesh），任一健康副本，无需节点表；避免重复派发靠 **queue cell 的 claim/租约**，与目标副本无关。
- **前提**：含待触发计时器的 cell 必须 **resident/pinned**，否则无人 tick；owner 失效由 waker 接管激活。
- **保证**：at-least-once + `token/epoch` 去重；cron 分钟对齐、best-effort、**不补跑**；queue 至少一次 + DLQ；workflow sleep 使用绝对时间。

## 11. 失败语义

- 非幂等操作在 owner 传输失败后**不盲目重放**：返回 `result-unknown`，由调用方决定是否以稳定 operation id 重试。
- 陈旧 owner 返回专属控制错误时，调用方可**重新解析 owner 并重试一次**（仅当能证明 handler 未开始）。
- 跨版本遵循 reader-before-writer 滚动顺序。

### 协议版本化（M-09）

- **owner 记录**含 `proto_version`（如 `"v1"`）；**LTX 段头带版本字节**。
- 升级遵循 **reader-before-writer**：新版 reader 先上线（能读旧格式），旧 reader 全部下线后再启用新版 writer。
- **回滚安全规则：读写版本差 ≤ 1**；跨 ≥2 个版本的不兼容变更必须走显式迁移（离线/停机或双写），不做隐式兼容。

## 12. 节点间通信与延迟

### 12.1 通信内容

| 类型 | 方向 | 是否热路径 | 说明 |
|---|---|---|---|
| **LTX append / tail** | owner → follower | ✅ 热路径 | 流式追加 LTX 段；follower fsync 后 ack |
| ack | follower → owner | ✅ | 首个 ack 即构成证明 |
| seal / tail（recovery） | 恢复节点 ↔ followers | 冷路径 | 接管时收集前任未上传数据 |
| acquire / release | peer → peer | 冷路径 | 优雅 handoff、接管 |
| fleet sample | bucket（一节点刷新） | 否 | 非 peer 读，避免 O(n) |
| owner 解析/提示 | bucket + 缓存 | 否 | 非 peer |

### 12.2 传输选型

- 私网内**持久连接 + 多路复用**（HTTP/2 或自定义二进制帧流），**避免每笔写重新握手**；
- **控制通道与数据通道分离**，避免队头阻塞（大 LTX 段不阻塞 owner hint/acquire）；
- 私网明文 + fleet HMAC/内部 token（安全要求高时用 mTLS）；
- 二进制/长度前缀，**禁止 JSON** 上热路径。

**协议选型（内部统一 REST/HTTP，取消 gRPC 平面）**：

> **修订（ADR-136）**：本节原先规划 Go↔Go 用 gRPC（ADR-026/028）的 `:7000` 平面**从未实现**，且已明确**不再做**。现网内部全部走 HTTP：peer LTX 热路径是**长度前缀二进制帧 + HTTP 101 持久流**（ADR-042），其余为 REST/JSON。下表描述**当前实现**。

| 链路 | 协议 | 说明 |
|---|---|---|
| `cell-agent` ↔ `cell-agent`（peer RPC：acquire/release/seal/tail/recovery） | **REST/JSON（HTTP/1.1）** | 同一连接池；无 HTTP/2/gRPC |
| `cell-agent` → `cell-agent`（**LTX append/tail 热路径**） | **长度前缀二进制帧 + HTTP 101 持久流**，失败回退 `/append_batch` | 每 follower 多条 scope-hashed lane、每 lane 在途多批（ADR-042） |
| do-runtime supervisor（Go）→ `cell-agent`（WAL 上报/claim/restore） | **REST/JSON** | Go↔Go，同上 |
| `cell-agent` → `user-runtime` :8088（scheduled/queue/workflow 派发） | **REST/JSON** | 目标是 workerd(JS) |
| `user-runtime`（workerd JS）→ `cell-agent` :7001（bindings） | **REST/JSON** | workerd JS；见 ADR-013 |
| `user-runtime`（host adapter）→ owner `do-runtime` :8788（DO 调用/WS 代理） | **HTTP / JSRPC** | JS↔JS |
| do-runtime 内部 supervisor ↔ workerd | 本地（loopback/JSRPC） | 同 Pod；WAL 捕获是**文件读取** |
| 对外/租户 API | **REST/JSON** | 见 ADR-013 |

- **内部无 gRPC**；凡 workerd(JS) 或 Go 参与的内部链路都走 HTTP/REST/JSRPC。
- LTX 大段用**长度前缀二进制帧**流式分帧，避免单条超大消息造成队头阻塞。
- 若将来 HTTP 成为瓶颈，可选方案是**同接口下沉为原始 TCP 帧流**或用 Connect(Buf) 统一定义；默认不引入。

### 12.3 降低延迟（分层）

| 层 | 做法 |
|---|---|
| 网络 | **同区/同 AZ/同机架**；NVMe；避免跨 AZ/跨区；owner 与 follower 同 AZ 不同机（低延迟 + 故障独立） |
| 连接 | 长连接、连接池、keepalive、多路复用 |
| 批量 | ✅ **group commit**：owner 侧 `ShipBatcher`（1ms 窗口、≤256 段/批）把并发 commit 合成**一帧多段**一次 POST（`/v1/peer/append_batch`）；follower 侧 `Spool.AppendBatch` 一次 log 写 + 一次 fsync（ADR-040/041） |
| 冗余 | ✅ **自适应 hedge（ADR-164，默认）**：先写 primary（1 份），超过 `max(250ms, 4×最近最慢 append)`（上限 `CELLHIVE_PEER_HEDGE_MAX_MS`=2000）再向下一 follower 发**第二份**，取先到者（法定 1）；`CELLHIVE_PEER_HEDGE_MS=0` = 只发 primary（单份），primary 失败才顺序 failover。重复副本安全：spool **按 `ltx.Header.ID()` 幂等** |
| 协议 | ✅ 长度前缀二进制帧 + **HTTP 101 持久流**；每 follower 4 条 scope-hashed lane、每 lane 最多 4 批在途，失败回退 `/append_batch`（ADR-042）；❌ gRPC 平面**已取消**（ADR-136；内部全 HTTP） |
| 存储 | NVMe + batched fsync；避免写放大 |
| 读路径 | 只读本地，不碰 peer/bucket |
| 部署 | 节点亲和（同一失败域）；bucket 上传仅后台 |

### 12.4 计划采用

- ✅ **持久 peer 流 + group commit + 首个 ack + 自适应 hedge（ADR-164，默认；常态 1 份、慢时补第 2 份）**；
- owner 与 follower **同 AZ 亲和**；
- bucket 仅后台合批上传；
- 控制/数据通道分离；内部 token/HMAC。

## 13. P0 验证项（与性能门）

| 指标 | 门槛（建议） |
|---|---|
| cell-agent 双节点稳态写 p50 / p99 | ≤ 30ms / ≤ 100ms（同区 bucket） |
| 单节点降级写 p50 | ≤ 120ms（记录，非默认） |
| 跨区 bucket 写 p50 | 记录（预期 ~600ms） |
| **DO：捕获→cell-agent→证明（跨节点）p50 / p99** | 需单测并设门 |
| 冷启动 restore：100 MiB cell | p50 ≤ 5s，p99 ≤ 15s |
| `kill -9` 接管 | RPO=0，接管 ≤ 5s |
| 每 ack 的对象 PUT 数 | 高并发下显著 < 1（group commit 生效） |
| 条件写冲突/重试率 | < 1% |
| 大 DB 接管读取对象数 | 有 L1 compaction 时 ≤ ~50 |
| 热路径 LIST 次数 | 0 |
| **peer append 往返 p50 / p99** | 需单测并设门（同 AZ） |
| **hedge 命中 / 无效比例** | `/metrics` `cellhive_peer_hedge_fired_total`（发出副本）/`cellhive_peer_hedge_won_total`（副本赢得 ack）；等待用滚动窗口的最近最慢 append 推导（ADR-164/165） |
| **group commit 合批比** | 高并发下每 ack 的 append/fsync 次数显著 < 1 |

_最后更新：2026-09-19_
