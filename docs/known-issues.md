# 设计决议（评审问题闭环）

本文件记录系统评审发现的全部问题，以及**每条问题的解决决议**。原始分级见文末附录。设计文档（architecture / cell-protocol / durable-objects / networking / decisions）已按此同步。

_最后更新：2026-09-19_

---

## 决议一览

| ID | 主题 | 决议状态 |
|---|---|---|
| C-01 | DO owner claim flow | ✅ 定稿（durable-objects.md） |
| C-02 | bundle/assets 读取路径 | ✅ 定稿：短期 scoped 凭据直连对象存储（decisions.md 待定#1） |
| C-03 | node-log / recovery | ✅ **完整实现**（ADR-057：`RecoverNode` 列节点+fast path；优雅停止 seal；E2E kill -9 恢复 128 段、时延 0.04s、RPO=0/keys=1000。**ADR-066**：节点死亡自动编排已接线） |
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
- internal token **只存在于 host adapter（平台代码）与 cell-agent 之间**，**绝不注入租户 env**；租户只拿到 binding facade。
- cell-agent 校验 host adapter 注入的 **scope 声明**（`ns` + `binding 类型/id`）；缺省或越权即拒绝。
- 不新增 per-load token（可后续作为纵深防御补充）。
- **管理后台身份模型另见 ADR-036**：不区分管理员与租户，单一管理后台，租户身份作为请求参数传入；管理后台绝不暴露给租户/公网。
- 详见规划中的 `security.md`。

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
- 每 worker 配 `limits`（`cpu_ms`/`subrequests`）+ V8 heap 上限。
- 详见规划中的 `security.md`。

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
| 真实 S3 条件写 + presigned | ✅ 本地 S3 兼容（MinIO）实测；◐ 云端对象存储差异未测 | `cmd/s3probe` / `cmd/s3init`；云端按 `diagnose` 探针确认 |
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
| **S3 条件写/range 语义** | ✅ **可重复脚本 `make s3-test`（本地 MinIO）**：`s3probe` 四项全 OK（conditional create / reject-create / CAS / reject-stale）+ ranged read；`TestS3BucketIntegration`、**`TestS3ReplicationRestoreChain`**（snapshot→Restore→ApplyFile 500 行；Compact→PageFetcher **ranged**→Materialize 500 行）| 用本地 `minio`/`rustfs`：见 `docs/testing.md`「S3 集成测试」。剩余：云厂商/区域差异未测 |
| 跨节点"捕获→cell-agent→证明" p50/p99 | ✅ 真实多进程 + 真实 TCP（loopback）已测；◐ 云/跨主机未测 | 评估 FUSE 或调整放置 |
| do-runtime 冷激活/恢复时间 | ◐ 未验证（restore 500 段 5ms，非 workerd actor） | paging/常驻策略 |
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

- **ADR-030**：bundle/assets 用**短期 scoped 凭据直连对象存储**（不经 cell-agent 代理）。
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
| Workflows/Cron binding | ✅ **ADR-086**（Workflows：`__workflow__` cell + `env.WF` + `step.do/sleep` + 生命周期/事件）+ **ADR-070/076**（Cron：`Version.Crons` 投影 + 调度器物化 slot + `scheduled()` 派发） |

## 可观测性（ADR-146 后）

- **tracing**：`traceparent` 传播已实现（入口/wrapper/facades/cell-agent service+DO），但**没有 OTLP 导出、没有采样**；props-bound facades（KV/D1/R2/Queue，loader isolate）拿不到每请求上下文，其调用不带租户 trace；后台 queue/timer 派发默认新 trace。

## 部署产物（ADR-139 后）

- **Terraform 未做**（orchestration 由 Helm chart + kustomize base/overlay 提供，`make ci` 离线校验）。
- **云 KMS 未接**（根密钥可用 env 或文件，ADR-150）；**mTLS/内部 CA 有意不做**（独立派生 secret + 私网隔离为主防线；触发条件见 security.md）。

## purge / 上传 / WAL / r2.List（ADR-134 后更新）

- **删除/purge**：软删 + `purges` 作业 + `RunPurgeLoop` 已落地（ADR-131），并且 purge 只对**仍处于软删状态**的实体生效、deploy/create 会取消作业（ADR-135）；**数据侧 hook 已接线（ADR-142）**：worker 删除清该 worker 的 DO 段与 assets、app 删除清整个 ns 的 `cells/`+`assets/`，每个 cell-agent 都执行（跨节点本地副本 drain + 桶删除幂等），hook 可续跑（有界删除/轮，`done=false` 保留 pending）。**已补（ADR-169）**：数据侧改为 `objectstore.Objects.ListPage` + `bucket.PagedLister` 游标分页删除（S3 `StartAfter`；内存有界）；`Purger.delete` 边删边推进、预算用尽下一轮续跑。**残余**：每轮从目标前缀起点重扫（未持久化跨轮 cursor，冷路径）；FS dev 每页仍走树（内存有界）。
- **async bucket 上传**：已有**持久化重试队列**（ADR-143）：写前 spool（`<DATA_DIR>/upload-spool`）+ 启动/周期重放，失败计 `deferred`（不再丢，只有落盘失败+上传失败才 `dropped`），`/metrics` 暴露 spool/deferred/dropped。**已补（ADR-171）**：spool 改原子写（tmp fsync → rename → 目录 fsync，目录链记忆化），**断电安全**；`Remove` 不 fsync（replay 幂等，安全）。
- **S3 条件删除**：`ConditionalDelete` 依赖 `DeleteObject` 的 `If-Match`；忽略该头的兼容存储会退化为无条件删除（owner/lease fence 依赖它）。**自检方式**：`cellhive diagnose` 现在包含该探测（stale etag 必须被拒 + 正确 etag 必须删除 + 对象必须消失），失败信息会点名 "conditional delete (reject-stale)"；FS/本地桶已由单测覆盖，云端属 C 类环境待实测。
- **`r2.List`**：已 cursor 分页 + 尺寸化（ADR-145）：`bucket.PagedLister`（S3 `StartAfter`+`MaxKeys`、FS 有界选择），`/v1/r2/list` 返回 `truncated`/`cursor`，facade 透传。**已补（ADR-168）**：`include=httpMetadata,customMetadata`（显式才逐对象读 sidecar）与 `delimiter`/`delimitedPrefixes`（扫描上限 10000，超出 `truncated`+cursor）。**残余**：FS 后端算 etag 仍读对象体（dev 后端）；仍未实现 object versioning/SSE-C/conditional put。

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

## P2 Workflows 绑定（ADR-086，Partial）

**已实现（真实 workerd e2e）**：`internal/workflow`（`__workflow__` cell：instances/steps/events，step 记忆化）；facade `env.WF.create/get/sendEvent`；cell-agent API（租户 create/get/event，内部 step/sleep/finish）；user-runtime `/v1/workflows/run` + `cellhive-workflow.js` base + `step.do/sleep`（shim：改写 `cloudflare:workers`，平台自构造，因 workerd 的 `WorkflowEntrypoint` 引擎外不可构造）；sleep 复用统一 timer（`KindWorkflowSleep`）；CLI `workflow create`；`wranglercompat` 要求 `class_name`。

**Partial 边界（未做）**：跨 worker 实例；`locationHint` 被接受但**忽略**（best-effort 放置提示，无效果）。
**已实现**（曾误列为未做）：`pause`/`resume`/`terminate`/`restart`（`internal/server/workflow.go:126` + facade）、`waitForEvent`（`handleWorkflowWait` + `env.WF.sendEvent`）、实例列举（`handleWorkflowList` + `env.WF.list`）、`instance.delete()`（`Store.Delete` + `/v1/workflow/delete` + facade）、每步 `retries{limit,delay,backoff}`（`workerd/user-runtime/workflow-wrapper.js:74-152`，durable attempt）。

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
| DO 兼容套件 | ✅ `TestDOCompatSuite` 统一入口，16 真实 workerd 子测试（sync SQL/事务、alarm、WS 1012、migrations、注册/删除、转发/`result_unknown`、门、冷激活、接管、resident/evictable） |
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
| 租户 console 日志（ADR-185） | ⚠️ **暂不支持**：实测 pinned stock `workerd 1.20260916.1` 的动态 `workerLoader` 在 `tails: [{name:"tenant-tail"}]` 上拒绝 service designator：`TypeError: Incorrect type for array element 0: the provided value is not of type 'Fetcher'.`。因此已删除 `log-tail.js`/`PlatformBridge.logSend` 旧桥接；tenant 请求与 DO facet 仍正常执行，但 `console.*` 不写入平台 ring 或 OTLP。不得以 tenant env 的 token、URL 或 transport 恢复此路径；升级 pin 后必须重跑真实动态-tail spike。 |
| workerd 运行时基线（ADR-186） | ◐ **代码已实现，最终门禁待验收**：pin `1.20260916.1`、esbuild `0.28.2`、宿主 `fromEnvironment`、生成式 compatibility manifest、最终 WorkerCode 64 MiB 与 env 1016 KiB 双层预算均已落地，真实 user-runtime/do-runtime workerd 套件通过。尚待 Task 11 的隔离 Docker Compose E2E 与 `REQUIRE_ALL=1` 全门禁；在其通过前不得宣称阶段完成。Tail 在新 pin 上重跑仍不支持，且不以 tenant env 回退。 |
| OTLP 追踪覆盖（ADR-167） | ⚠️ **边界**：**DO 内 `this.env.<binding>`（props-bound）调用不带 DO traceparent、自成一条 trace**（workerd 三个 isolate 各自 globalThis + CF 绑定 API 无按请求参数；`TestDoRuntimeBindingInsideDO` 断言当前为空）；**`peer.append` 是捕获流水线按批的独立 trace**（group commit 合并多请求，无法归属单一 trace）；租户 isolate `facades.js` 回退路径无 JS span（服务端 span 仍能挂上）；DO `/v1/do/connect`·abort、compaction/upload 后台循环无 span；JS span 毫秒精度；入口头部采样；端点空时 no-op |
| peer 复制冗余（ADR-164） | ⚠️ **默认语义**：adaptive hedge 下常态是 **owner 本地 + 1 个 follower**（慢时才补第 2 份），`CELLHIVE_PEER_HEDGE_MS=0` 永远单份；要"始终 2 份 follower"需显式接受常态双发（当前不提供该模式）。单节点故障可恢复（owner 死→从 follower spool `/v1/peer/held` 恢复；follower 死→owner 本地段后台上传），同时坏 owner+follower 才丢；一致性不受影响（单写者/顺序/epoch/quorum-1 不变） |
| R2 对象字段与 metadata（ADR-163） | ⚠️ **边界**：`get()` 的 `R2ObjectBody` 数据字段与 `text/json/arrayBuffer/blob` 都可用（workerd RPC 按值序列化数据、把函数序列化成可调用 stub；方法设计为可枚举以跨 RPC，JSON.stringify 会跳过函数）；`bodyUsed` 读取后**不翻转**（快照值）；`list()` 默认只回 `key/size/etag/httpEtag/version`，**不逐对象回填** http/custom metadata/uploaded/checksums；显式 `list({include:["httpMetadata","customMetadata"]})` 才按需逐对象读 sidecar（ADR-168）；`get()` 多一次 metadata sidecar 读；`put` 的 metadata 存 `r2meta/<ns>/<bucket>/<key>` sidecar（已登记保留前缀），删除对象会一并删 sidecar；`md5/sha256` 提供时校验 |
| R2 `writeHttpMetadata`（ADR-174→ADR-175 已修） | ✅ **已解决**：`loader.js`/`buildFacetEnv` 经模块作用域常量 `__cellhivePlatform.r2Bindings` 传 R2 绑定名（不占 env 名），`queue-wrapper`/`bindings-wrapper` 对每个 R2 binding 套本地 Proxy（`facades.js wrapR2Metadata`），`writeHttpMetadata` 在租户 isolate 改调用方 `Headers`，content-type 正常；仅 `get()` 被代理，其余方法透传 |
| KV 大值（ADR-176 记录） | ⚠️ **缺口**：value ≤25MiB 目前**全量内联**在 cell 的 SQLite，并随 LTX 捕获/复制走一遍（对齐做法是 >1MiB 转 fleet bucket blob `v2:e<epoch>:<digest>`，同一个 alarm 里做 GC；实测 1MiB 是延迟交叉点）。大值 `put` 的代价随值大小线性上涨；正确性不受影响。对齐需要"小值内联/大值 blob + epoch 引用 + GC"的完整链路 |
| wake 索引落后于 timer（ADR-177 已修） | ✅ **已解决**：`Store.Upsert` 先发布索引再提交（索引不落后，失败则 fail-closed）；`registerTimerScope`/认领时 `SyncIndex` 重建；`CELLHIVE_WAKE_REPAIR_INTERVAL` 有界轮转修复本地 cell（只读扫描，稀疏 paged 未挂载跳过）。剩余：修复只覆盖本节点本地 cell（死节点盘不可达，靠新 owner 认领时重建） |
| DO stub 上的 WebSocket 升级（ADR-174→ADR-175 已修） | ✅ **已解决（ADR-184 后无需租户传输）**：租户 facade 给 Request 加 `DO_ID_HEADER` → 平台侧 `DurableObjectNamespace.fetch(request)`（fetch 形 RPC，101/WebSocket 可跨 RPC）→ cell-agent `GET /v1/do/connect`（scoped）取 `{owner,ticket}` → owner do-runtime `/v1/do/connect`（ticket 仅本 shard）；`TestTenantDoWebSocketCrossesRpc` 端到端覆盖，`wsecho` 示例 WS echo 通过。`CH_DO_CONNECT`/`CELLHIVE_CAP_WS` 已删除 |
| DO RPC 的值与上限（ADR-162） | ⚠️ **边界**：支持 `undefined`/`-0`/NaN/±Inf/bigint/Date/RegExp/Map/Set/ArrayBuffer/TypedArray/DataView/Error(含 cause)/URL/URLSearchParams + 共享引用/环；**不支持** function/Symbol/Promise/弱集合/流/`RpcTarget`/DO stub；类实例降级普通对象；args 与结果各 ≤ **8 MiB**（tagged 编码后，二进制 base64 约 ×4/3）；方法名必须标识符且禁 `fetch`/`alarm`/`constructor`/`__proto__`/`then`/`toString` 等与 `__ch*`；跨进程无 traceparent（仅 requestId） |
| SQLite 现在是 CGo + 构建 tags（ADR-159） | ⚠️ **必须知道**：`make build/test`（或 `-tags "sqlite_fts5 sqlite_dbstat sqlite_math_functions sqlite_column_metadata sqlite_preupdate_hook"`）不可省——漏加会在 `internal/cellstore` 触发 `#error` 守卫；二进制含 AVX2 代码（`-march=x86-64-v3`），非 AVX2 CPU 用 `-tags cellhive_vec1_portable`（标量，较慢），arm64 走 NEON |
| Vectorize 绑定面的鉴权层级（ADR-158） | ✅ **已对齐**：`/v1/vectorize/*` 绑定端点与 KV/D1/R2/Queue 一样**只需 scoped token**（不带 internal token）；写操作 `forwardOrClaim`、读 `forwardRead`。容器 smoke 曾发现误包 `s.auth` 导致 401，已修 + 回归测试 |
| Vectorize 在 dev 不可用（ADR-158） | ⚠️ **边界**：Miniflare 只解析 `vectorize` 绑定（workerd 无该服务），`cellhive dev` 遇到它会明确报错退出；本地调试用 `cellhive vectorize` 打真实命名空间 |
| R2 stats 是诊断性近似（ADR-157） | ⚠️ **有意**：R2 对象无本地索引，`/v1/r2/stats` 走**有界 List**（`limit` 默认 1000/上限 10000），大桶给 `truncated`+`cursor` 而非精确总数；它会计入 `cellhive_list_calls_total`（运维路径，允许但要有意识） |
| 行数只能是估计（ADR-157） | ⚠️ **SQLite 无行数元数据**：默认给 dbstat 叶页 `ncell` 估计（KV `rows_estimate`、D1 每表、workflow `instances_estimate`）；精确计数须 `?exact=1`（`count(*)`，KV 走最小索引/index-only，仍是 O(rows)）。不提供"永远精确且永远便宜"的选项（`cell_meta` 计数器方案未采纳） |
| Workflow `by_status`/DO stats 的前提（ADR-157） | ⚠️ **边界**：workflow 状态分解只在 `?exact=1` 时给（未加 `instances(status)` 索引，避免写放大）；DO stats 只在对象索引开启（`CELLHIVE_DO_OBJECT_INDEX`，ADR-109）时有数据，否则 `available:false` |
| `resource delete` 语义边界（ADR-156） | ⚠️ **有意**：只吊销**登记**（墓碑 fail-closed）并**不删**数据 cell（KV 条目/队列消息/DO SQLite 仍在）——vwork 的"关停"不是"销毁"；彻底回收需删除对应 cell（当前无运维端点）。`replay-dlq` 为**至少一次**（主队列入队成功后 DLQ ack 失败会重复重放），且要求队列已被 producer binding 登记并声明了 `dead_letter_queue` |
| `do-runtime` 非 gated 模式 SIGTERM 不 drain（ADR-156） | ⚠️ **边界**：仅 `do-runtime-gated`（do-supervisor，ADR-140）接管 drain；裸 `do-runtime` 依赖 readiness 摘除 + 租约过期 + 接管恢复（`TestDoRuntimeTakeoverAfterCrash`）。生产用 gated |
| 入口 `/ready` 的覆盖范围（ADR-156） | ⚠️ **边界**：描述的是**入口/DO 宿主**的就绪（投影新鲜度、排空状态），不是租户 worker 内部健康；props-bound facades（KV/D1/R2/Queue 在 loader isolate）没有独立探针 |

**含义（2026-09-15 更新）**：P3.7/P3.8 的交付（facet 发现、对象键、按对象恢复、门、跨节点冷激活/接管、compat 套件）**均已实现并验证**。P3 各项均已实现/定稿（WS 跨节点转发、`transferred_classes` 同 worker、运行期 VFS 懒读见 ADR-160、`refreshFacets` 增量缓存）；仅剩 **C 类环境验证**（真实跨主机 RTT、云端对象存储、多主机混沌/接管）与跨 worker `transferred_classes`（有意拒绝）。
