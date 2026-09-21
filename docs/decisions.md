# 决策记录（ADR）

本文件是 CellHive 设计过程中已锁定决策的**当前版本**。设计过程中多条 ADR 被修订（见文末"修订记录"）；此处保留编号、更新内容以保持文档一致。

状态图例：✅ 已定 · ⏳ 待 P0 实测确认 · 🕓 待定（尚未收口）。

---

## ADR-001 计算层使用 stock workerd，不修改 workerd ✅

- **决策**：计算层使用 Cloudflare 官方未修改的 workerd，仅通过 `workerLoader`、bindings、capnp 配置、进程开关（`--experimental`）使用它。
- **理由**：可跟上游修复；避免维护分叉。
- **代价**：DO 同步 SQL 无法落到进程外存储（见 ADR-006）；部分能力受 workerd 暴露面限制。

## ADR-002 状态模型：一切皆 cell，SQLite + 对象存储为权威 ✅

- **决策**：每个状态单元是一个有名字的 SQLite cell；对象存储（S3 兼容）为长期权威；不依赖 Valkey/kvrocks/NATS/etcd。
- **理由**：满足"数据全进 SQLite/S3"、去中心、可自托管。
- **代价**：写延迟受持久化证明约束；bucket 必须支持条件写。

## ADR-003 owner 解析 = Go 库 + 本地端点（不是独立服务） ✅

- **决策**：owner 解析（`scope → owner 地址 + epoch`：读取/条件写 owner 记录、节点发现、缓存）做成 **Go 库**，内嵌在 `cell-agent` 与 `do-runtime`；由 `cell-agent` 暴露内部解析端点供 workerd host adapter 调用。
- **理由**：解析无状态、逻辑轻；避免独立 `cell-router` 组件与一跳；workerd 无法内嵌 Go，故用端点。
- **代价**：`cell-agent` 多一个内部端点；调用方需按 owner 提示转发。

## ADR-004 自研 cell 服务，不复用/改造外部实现 ✅

- **决策**：自研 Go cell 服务；不 fork、不改造外部实现。
- **理由**：把执行引擎与状态层耦合在一起的一体化实现，与"workerd 计算 + 独立 cell 层"不匹配。
- **代价**：复制协议需自研并严格测试（最大工程风险）。

## ADR-005 统一语言 Go；打包调用外部 esbuild 二进制（无 Node） ✅

- **决策**：全栈 Go（cell-agent/supervisor/CLI/计时派发/控制逻辑）；CLI **调用外部 esbuild 二进制**（`exec`，见 `internal/bundler`）打包，不依赖 Node/不引入 esbuild Go SDK 依赖。打包时生成内容寻址 bundle（SHA-256）并上传到对象存储 `bundles/<sha256>`；部署记录只保存 bundle 哈希。
- **理由**：单工具链、单二进制；esbuild 本身是 Go；FUSE/复制有 LiteFS 先例。
- **代价**：打包需长期对齐 Wrangler **语义**（见 ADR-014）；外部 esbuild 版本与 Wrangler 的包装可能有细微差异，需测试覆盖；部署环境需提供 `esbuild` 可执行文件。

## ADR-006 DO = 原生 workerd facet + 每对象 lease + WAL→cell-agent + 输出门 ✅

- **决策**：DO 使用 workerd 原生 Durable Object facet（保留同步 SQL）；每对象一 owner/lease；**owner 记录指向 do-runtime 节点**，由 cell-agent 协调写入；SQLite 是 do-runtime 本地**工作副本**，其 WAL 段由 supervisor 捕获后**上报给 cell-agent**，由 cell-agent 执行复制与持久化证明；host adapter 输出门保证 RPO=0。
- **理由**：`ctx.storage.sql.exec()` 同步，跨进程存储不可行；原生 facet 保留 DO 语义；cell-agent 作为唯一持久化端点保证安全边界。
- **代价**：cell 侧只能观察 WAL（机制弱一档）；跨节点上报在写 ack 路径上（P0 验证）。

## ADR-007 接受 DO "结果等价、机制弱一档" ✅

- **决策**：DO 与 KV 在用户可见结果上等价（对象存储权威、可接管、RPO=0），但机制不同（workerd 拥有工作副本 vs cell-agent 拥有 SQLite）。
- **理由**：保留"stock workerd + 不改 workerd + DO 同步 SQL"三条约束的必然结果。
- **代价**：DO 复制比 KV 更脆弱，依赖 WAL/checkpoint 对齐与跨节点上报。

## ADR-008 FUSE 非默认；默认 WAL 捕获 → cell-agent ✅

- **决策**：默认由 do-runtime supervisor 捕获 WAL 并上报 cell-agent（可移植，适配托管/受限 K8s）；FUSE 卷作为自管环境的可选加速，不在 P0 关键路径。
- **理由**：FUSE 需 `/dev/fuse` + `SYS_ADMIN`，托管集群常禁用；workerd on FUSE 无先例。
- **代价**：需解析 WAL 与处理 checkpoint 截断；跨节点上报。

## ADR-009 定时器：各自 cell + 本地 due + 单 waker；无独立 scheduler ✅

- **决策**：DO alarm / cron / queue 延迟重试 / workflow sleep 统一为 timer；due 存各自 cell 的 SQLite；每节点本地 due 派发；单 fleet waker 兜底 owner 已死项；删除独立 `scheduler` 服务。派发到 `user-runtime` 用**逻辑服务名**（任一健康副本），重复派发靠 queue cell 的 claim/租约避免。
- **理由**：与去中心 cell 一致；避免全局扫描与中心派发点；bucket 零扫描。
- **代价**：`cell-agent` 变重（计时/派发/重试/DLQ），需清晰模块边界与指标；计时器 cell 需 resident/pinned。

## ADR-010 无 d1-runtime / kv-runtime；仅 do-runtime 需自研 PID1 supervisor ✅

- **决策**：D1/KV/Queue/Workflows/Cron 由 `cell-agent`（单 Go 进程，自处理 drain）承载，无独立 runtime；只有 `do-runtime` 需要自研 PID1 supervisor（workerd 拥有工作 SQLite，需保证"先 drain/flush 后停 workerd"）。WAL 捕获模块**并入 supervisor，不单独起 sidecar 程序**。
- **理由**：D1/KV 的 Worker API 是异步的，可走 cell-agent；只有 DO 的同步 SQL 需要 workerd 进程内。
- **代价**：无。

## ADR-011 部署：同集群；最小 2 个 cell-agent；S3 不在 ack 路径 ✅

- **决策**：各服务同一集群的不同工作负载；`cell-agent` 固定集群、**≥2 节点**（稳定身份）；`do-runtime` 弹性、**本地盘可临时**（可用 Deployment）；`user-runtime` Deployment + HPA；稳态写确认走 fleet ensemble（follower fsync），对象存储上传异步。
- **理由**：双节点把写延迟从 ~90ms 降到 ~25ms 并提供 RPO=0；单节点无法做 ensemble。
- **代价**：单节点降级模式下 DO 不可用（仅非交互式负载）。

## ADR-012 控制面并入 cell-agent（分监听/分鉴权） ✅

- **决策**：控制面（应用/版本/路由/身份/密钥/绑定元数据/发布回滚/审计）并入 `cell-agent` 单二进制；与数据面**分监听**（:8082 admin vs :7000 gRPC / :7001 REST 内部）、**分鉴权**；控制面元数据按 app 分片存 control cell；密钥模块隔离。
- **理由**：减少组件；控制面状态本就是 cell；与"cell-agent 唯一桶凭据"一致。
- **代价**：控制面与数据面同进程，爆炸半径变大，必须靠监听/鉴权隔离与模块边界压。

## ADR-013 持久化对外 API：内部 REST 先行，对外分类型 REST 后置 ✅

- **决策**：P0 只做内部 REST/JSON（binding ↔ cell-agent）；对外/租户的分类型 REST（D1=SQL、KV、Queue、Workflow）后置；**不提供**租户级"任意 scope 任意 SQL"入口（通用 SQL 仅管理员）；gRPC 后续可选。
- **理由**：核心风险在复制/租约/RPO，不在 API；过早对外会引入鉴权/配额/审计等非核心工作。
- **代价**：对外接口延后；内部接口需按"对外就绪"设计（scope 寻址、版本化、幂等 operation id）。

## ADR-014 Wrangler 兼容策略 ✅

- **决策**：jsonc + toml；严格拒绝未知/不支持字段；`workers_dev` 映射命名空间路径、自定义域/routes 可选映射到 Traefik host 路由、`preview_urls` 拒绝；DO 仅支持 `exports`(created/sqlite) 与 `migrations`(new_classes/new_sqlite_classes)，拒绝 rename/delete/transfer/script_name；**本期补齐完整 assets 管道**；打包用 Go+esbuild（ADR-005）；绝不调用 Cloudflare。
- **理由**：兼顾存量项目与可控实现；避免静默忽略导致运行时失败。
- **代价**：需长期跟随 Wrangler 语义并维护兼容矩阵/契约测试。

## ADR-015 不引入第二套 JS 引擎；SSR 依赖 workerd 生态 ✅

- **决策**：不引入第二套 JS 引擎；SSR 通过 stock workerd 的原生 Workers/Node 兼容实现，平台需开启 `nodejs_compat` 并补齐资产管道。
- **理由**：workerd 是 CF 生态的参考运行时，SSR 框架适配器均以其为目标。
- **代价**：平台需补 `nodejs_compat` 与静态资产能力；对 Python/Cache 等明确不支持（**Vectorize 已由 ADR-158 支持**）。

## ADR-016 复制参照 LiteFS + superfly/ltx；许可合规 ✅

- **决策**：Go 侧复制实现参照 LiteFS 与 `superfly/ltx`；借鉴/复用 Apache-2.0 代码时保留 LICENSE/NOTICE 与署名。
- **理由**：LiteFS 是 Go 的 FUSE+SQLite 复制生产先例，降低风险。
- **代价**：需自行实现或适配，不能直接搬现成的 Rust `ltx` 实现。

## ADR-017 不设独立 gateway；入口 = Traefik + user-runtime ✅

- **决策**：去掉独立 `gateway` 组件。TLS 与 host 分流（租户 vs admin）由运维 **Traefik** 承担；Worker 的 **host/path 路由、版本解析、可信头净化、request-id、保留命名空间拦截、WebSocket 代理**由 `user-runtime` 的 loader（平台代码，先于租户模块执行）承担。`cell-agent` 的内部接口（:7000 gRPC / :7001 REST）**不对外**，仅私网。
- **理由**：减少组件；HA 成本与保留 gateway 相当；DO 本就不对外可寻址，无需在入口解析 DO owner。
- **代价**：入口与租户执行同服务，信任边界变薄；每个 user-runtime 副本维护路由投影缓存。

## ADR-018 cell-agent 固定集群 + do-runtime 分布式弹性（可跨节点） ✅

- **决策**：`cell-agent` 是固定、有身份的集群（≥2，自注册 `nodes/*` lease）；`do-runtime` 是分布式弹性执行层，节点可增删、**本地盘可临时**；两者**可不同节点**，靠内网低延迟连接（同区/同 AZ）。do-runtime 的 DO 工作副本在本地，持久化经 cell-agent。
- **理由**：状态层需稳定身份与持久权威；执行层需弹性。
- **代价**：DO 写 ack 多一跳（捕获→cell-agent）；cell-agent 是所有持久化的汇聚点，需容量规划；临时盘导致冷激活需恢复。

## ADR-019 cell-agent = 状态 fleet，不执行用户代码 ✅

- **决策**：`cell-agent` 只做状态/cell/复制/租约/控制/派发，**不执行租户代码**；执行由 workerd（user-runtime / do-runtime）承担。
- **理由**：与"执行+状态一体"的实现区分；安全与职责分离。
- **代价**：多一层跨进程协调（相对一体化实现）。

## ADR-020 数据落位 ✅

- **决策**：Worker bundle 放**对象存储、内容寻址（SHA-256）+ 版本指针**（cell 只存引用）；secrets 存 control cell 的**信封密文**，**根密钥在 cell 之外**（环境/KMS）；控制元数据存 control cell 并**按 app 分片**；业务数据存 data cell；DO 数据为 do-runtime 本地工作副本 + 经 cell-agent 复制。
- **理由**：blob 不适合 SQLite；密钥必须信封加密；元数据小而结构化适合 cell。
- **代价**：需内容寻址与生命周期管理；密钥需 KMS/根密钥运维；控制 cell 分片。

## ADR-021 user-runtime 双端口 = 安全边界 ✅

- **决策**：`:8081` 对外（经 Traefik）承载普通 fetch；`:8088` **仅私网**承载平台特权派发（scheduled/queue/workflow run/notify）。端口/套接字隔离，公网流量永远到不了特权路径。
- **理由**：特权派发不能被公网触达（租户 Worker 可能定义同名路径）。
- **代价**：需两个监听与相应网络策略。

## ADR-022 对象存储角色化 ✅

- **决策**：默认**单个 bucket + 保留前缀**（`cells/ nodes/ fleet/ bundles/ deploy/ assets/ r2/`）；允许按角色覆盖为独立 bucket/凭据/生命周期：`state` / `code` / `assets` / `r2` / `backup`。
- **理由**：默认简单；按角色拆桶可获隔离、独立生命周期、CDN、独立凭据。
- **代价**：单桶默认下前缀**不是安全边界**，隔离靠鉴权层。

## ADR-023 cell-agent = 唯一长期桶凭据持有者 + 唯一持久化数据端点 ✅

- **决策**：只有 `cell-agent` 持长期对象存储凭据；`do-runtime`/`user-runtime` 不持。**DO 复制经 cell-agent**（do-runtime 上报 WAL，cell-agent 落盘与证明），字节不经 sidecar 直连。
- **理由**：安全边界最干净、协议唯一执行者；与"control 并入 cell-agent"一致。
- **代价**：cell-agent 成为所有持久化（含 DO WAL）的汇聚点与写 ack 一跳；需容量规划。

## ADR-024 ownership = 持有"工作 SQLite"的节点 ✅

- **决策**：owner 记录指向持有该 cell 工作 SQLite 的节点：A 类 = `cell-agent` 节点；B 类（DO）= `do-runtime` 节点。DO 的 owner 记录由 `cell-agent` 协调写入（含执行地址 + epoch）。
- **理由**："谁执行"（do-runtime）与"谁持久化"（cell-agent）分离，但归属唯一。
- **代价**：两套 owner 角色，协议需区分。

## ADR-025 do-runtime 无心跳；存活靠复制活动，候选靠平台发现 ✅

- **决策**：do-runtime 不写 bucket、不发专门心跳；其存活由**复制活动隐式判定**（持续上报 WAL/续约 owner 即存活）；owner 死后**接管候选**走**平台服务发现**（K8s Endpoints / mesh）或按需探测。
- **理由**：正常路由只认 owner 记录，不需要节点表；心跳是多余的。
- **代价**：依赖平台服务发现能力；idle 节点的存活判定需额外约定。

## ADR-026 内部协议：Go↔Go 用 gRPC，workerd JS / 对外用 REST/JSON ✅

> **修订注记（ADR-136）**：Go↔Go 的 gRPC 部分**未实现且已取消**；内部全部走 HTTP（LTX 为长度前缀二进制帧 + HTTP 101 持久流，ADR-042）。本条保留为历史记录。

- **决策**：`cell-agent` ↔ `cell-agent` 与 supervisor → `cell-agent` 用 **gRPC**（HTTP/2 + protobuf，bidi streaming）；**LTX append/tail 热路径**用 gRPC bidi 流承载**长度前缀的原始字节帧**（若 protobuf/流控成为瓶颈，底层可降为原始 TCP 帧，接口不变）；`user-runtime`（workerd JS）→ `cell-agent` 的 bindings 用 **REST/JSON**；对外/租户 API 用 REST/JSON（gRPC 后续可选，见 ADR-013）。
- **理由**：Go↔Go 适合强类型、多路复用、流式、易上 mTLS；workerd JS 不便使用 gRPC；对外保持 REST 兼容性。
- **代价**：内部存在两套协议面；LTX 大段需注意 HTTP/2 流控与分帧，避免队头阻塞。

## ADR-027 发现不绑定 K8s（bucket + 可替换服务发现） ✅

- **决策**：核心发现**不依赖 K8s，也不引入第三方注册中心（etcd/Consul 非必需）**——cell-agent peer 用 **bucket `nodes/*` lease**，owner 用 bucket owner 记录，DO 存活用**复制活动**。**默认用可配置的 service name（可多个）+ 平台内置 DNS** 作为 cell-agent 入口：K8s 用 Service（推荐 headless 返回 pod IP），Docker Compose 用 service 名（返回所有副本 A 记录）；客户端 round-robin / 失败重试，DO 协调按 **scope 哈希**选 cell-agent 以保证同一 DO 的 WAL 由同一 cell-agent 协调。裸机/VM 用**种子列表**或本地 DNS。Consul/Nomad 仅作为可选增强。
  - **do-runtime → cell-agent 入口**：同上 service name / 种子列表；do-runtime **不持桶凭据、不能自发现**。
  - do-runtime 接管候选：平台发现（K8s Endpoints / 可选 Consul / Nomad）；无平台发现时回退为 do-runtime 注册/心跳到 cell-agent，或配置列表。
  入口用独立 **Traefik**（或 HAProxy/nginx）；节点身份用静态主机名/IP + `advertise`。
- **理由**：只用 K8s/Compose 内置 DNS 即可跨两种环境；唯一第三方依赖是对象存储；可移植到 Docker Compose / systemd / Nomad / 裸机。
- **代价**：DNS 无健康检查，客户端须重试/多端点；do-runtime 需配置入口（或最小只读凭据/注册通道）。

## ADR-028 端口划分 ✅

> **修订注记（ADR-136）**：`:7000` gRPC 平面已取消；内部只有 `:7001`（含 Go↔Go）与 `:8082` admin。

- **决策**：`cell-agent` 分端口——`:7000` 内部 **gRPC**（Go↔Go：peer LTX、DO WAL/claim/restore）、`:7001` 内部 **REST/JSON**（workerd bindings）、`:8082` **admin**（经 Traefik）。`user-runtime`：`:8081` 公开 loader、`:8088` 内部特权派发。
- **理由**：协议、网络策略、TLS 边界清晰，避免同端口混协议。
- **代价**：多一个监听端口。

## ADR-029 租户隔离与 binding 鉴权 ✅

- **决策**：① 租户 loaded worker 的 `globalOutbound` = **公网 only**（不含 RFC1918/cell-agent）；② internal token **只存在于 host adapter，绝不进租户 env**；③ cell-agent 校验 host adapter 注入的 **scope 声明（ns + binding 类型/id）**，越权即拒。不引入 per-load token（可后续加强）。
- **理由**：网络隔离 + facade 隔离为主防线，避免共享 token 被滥用。
- **代价**：依赖 workerd capnp 的 outbound 配置正确；scope 声明需随绑定元数据一起冻结。

## ADR-030 bundle/assets 读取路径 ✅

- **决策**：cell-agent 按 `(ns, worker, version)` 签发**短期只读凭据/presigned URL**，`user-runtime` **直连对象存储**拉取 bundle；assets 由对象存储/CDN 直接服务。ADR-023 的"唯一持久化端点"仅约束**数据写路径**，不约束代码/资产读路径。
- **理由**：避免 cell-agent 成为代码/资产分发瓶颈。
- **代价**：需 bucket 支持 presigned URL；凭据签发与版本绑定需实现。

## ADR-031 路由投影下发（纯拉取 + 短 TTL） ✅

- **决策**：`user-runtime` **定期拉取**控制面路由投影，**TTL 默认 5–10s**（可配）；**不做 push**。promote/rollback 生效 ≤ TTL；期间个别副本可能仍服务**旧（不可变）版本**，可正常完成。
- **理由**：实现最简；不可变版本下陈旧窗口无正确性问题；push 仅缩短窗口，作为后续可选优化。
- **代价**：promote 生效有 ≤ TTL 延迟；若要近实时可后续增加 push 失效通知。

## ADR-032 DO alarm 与 WebSocket ✅

- **决策**：alarm——supervisor 只读 workerd alarm 表提取 due 并上报 cell-agent，cell-agent 在 waker cell 建 due 索引；waker 对"owner 死 + 到期"项**定向 takeover** 后派发；带 alarm 的 DO 优先常驻。**WS——CF 兼容**：部署/promote/迁移都**重启 DO**，旧 owner 以 **1012** 关闭，**客户端重连**，新 owner 重新构造、`webSocketOpen` 重新触发；**不做 resume**，会话状态须落存储。
- **理由**：与 Cloudflare 行为一致；实现简单。
- **代价**：alarm 依赖从 workerd 表提取（P0 验证 schema）；跨重启会话由应用自行持久化。

## ADR-037 DO claim 与恢复策略 ✅

- **决策**：① **owner claim 由 cell-agent 驱动**（resolve miss 时 cell-agent 选候选 do-runtime 并 `activate(scope)`，do-runtime 再 `claim`；自 claim 仅回退）；② 冷激活/接管采用**分页懒加载**（只恢复被访问的页，大库冷启动快）；③ node-log/recovery **完整实现**（fleet 模式崩溃后从 follower 收齐未上传段，保证 RPO=0）。
- **理由**：控制集中在固定集群；大库恢复体验好；RPO=0 是核心。
- **代价**：paging 与 node-log 实现复杂度较高。

## ADR-033 Timer 去重 ✅

- **决策**：去重在 **cell 侧**——timer 带 `token = hash(scope, kind, dueAt, occurrence)`，cell SQLite 记 `fired:<token>`（TTL）；已 fired 跳过；cron 按 slot 唯一键单次；DO alarm 以 actor SQLite 行为准，派发成功后原子推进/删除 due 索引。user-runtime 不去重。
- **理由**：无状态执行层不承担去重；cell 是权威。
- **代价**：cell SQLite 多一张 dedup 记录（有 TTL）。

## ADR-034 协议版本化 ✅

- **决策**：owner 记录含 `proto_version`；LTX 段头带版本字节；升级 **reader-before-writer**；**回滚安全：读写版本差 ≤1**；跨 ≥2 版本走显式迁移。
- **理由**：支持滚动升级与安全回滚。
- **代价**：每次不兼容变更需 reader/writer 两阶段发布。

## ADR-035 限流 / 配额 / Admission（P1） ✅

- **决策**：per-namespace 写速率限制；cell-agent 过载返回 `503 overloaded`；单 DO WAL 大小上限；worker `limits`（cpu/mem）；消费 `pressured`/`shed_cells` 做 backpressure；queue DLQ 上限。
- **理由**：防止单租户拖垮全体，尤其阻塞其他租户的 RPO 证明。
- **代价**：P1 工作量；MVP 可信单租户可暂缓。

## ADR-036 管理后台模型：不区分管理员与租户 ✅

- **决策**：平台提供**单一管理后台**（control plane），由受信操作方使用；**不提供租户账号 / 会话 / 权限体系**。租户身份（namespace / app）由调用方**作为请求参数传入**，平台据此执行操作。管理后台鉴权为**单一运维凭据**（bootstrap/admin token；可后续接 OIDC 做单点登录）。
- **理由**：简化身份模型；平台按"运营方代租户操作"模式运行。
- **代价**：无租户自助登录 / SSO / 细粒度权限；如未来需要多租户自助门户，需另行设计（非当前目标）。
- **安全约束**：管理后台**绝不暴露给租户或公网**（仅 admin host + 私网可达）；运行时的 **binding/namespace 隔离（ADR-029）与此独立，仍必须保持**，防止租户 Worker 越权访问数据。

## ADR-038 Go 技术栈与框架 ✅

- **决策**：
  - **HTTP**：内部/管理面用 **stdlib `net/http`**；不用 fasthttp/fiber。
  - **热路径（Go↔Go）**：**gRPC**（`google.golang.org/grpc`）+ **buf** 生成 protobuf（替代 protoc）。
  - **SQLite**：~~默认 `modernc.org/sqlite`（纯 Go，无 CGO）；需要自定义 VFS/性能瓶颈时切 `mattn/go-sqlite3`（CGO）~~ **已由 ADR-159 切换为 `mattn/go-sqlite3`（CGO + vec1 ANN）**；`internal/cellstore` 仍然隔离驱动（换回/再换只改一个包）。
  - **CLI**：**cobra**（后续）；**Metrics**：**`prometheus/client_golang`**；**日志**：stdlib **`log/slog`**；**测试**：stdlib **`testing`**。
- **理由**：内部 HTTP 非瓶颈（瓶颈是持久化证明与复制 I/O）；热路径用 gRPC/二进制帧；纯 Go SQLite 便于单二进制与交叉编译；依赖尽量少。
- **代价**：P0 的 peer 传输先为 HTTP（粗边），后续按 ADR-026 换 gRPC+buf；若需自定义 VFS 需引入 CGO。
- **备注**：`modernc` 与 `mattn` 的 **WAL 均可用**，但**自定义 VFS 不等价**（modernc 是 C→Go 转译）；**读 WAL 文件与驱动无关**。

---

## ADR-039 持久性姿态命名：`fleet` / `bucket` ✅

- **决策**：写确认姿态命名采用 **`fleet` | `bucket`**，配置为 `CELLHIVE_DURABILITY`（`auto`|`fleet`|`bucket`，默认 `auto`）+ `CELLHIVE_BUCKET_WAIT`（默认 `true`）。
  - `auto`：有 live follower 走 fleet，否则 bucket。
  - `bucket`：只用对象存储，不尝试 peer。
  - `fleet`：**RPO=0 要求**；有 peer 走 follower fsync，**无 peer 退化为"等 bucket"并告警**，不静默 ack。`fleet` + `BUCKET_WAIT=false` 启动即拒绝。
  - `CELLHIVE_BUCKET_WAIT=false`（等价旧 `CELLHIVE_COMMIT_MODE=async`）= 入队即 ack、后台块提交（RPO>0）。
- **理由**：此前用 `bucket` 指姿态、`batch/async` 指 ack 时机，把"姿态"与"是否等待"混在一个 `COMMIT_MODE` 里语义不清；拆成两个正交配置更清晰。
- **代价**：与原 `solo` 命名方案分叉（尚未落地，无迁移成本）；`fleet` 的 ack 语义是 **quorum-1**（≥1 follower fsync，非全 follower 持有），文档需显式标注。
- **备注**：`CELLHIVE_COMMIT_MODE` 保留为兼容别名（`async` → `BUCKET_WAIT=false`），显式 `CELLHIVE_BUCKET_WAIT` 优先。

---

## ADR-040 follower spool 用单 append-log + group commit fsync ✅

- **决策**：follower 的复制段不再"每段一个文件 + 文件/目录各一次 fsync"，改为**每 `(scope,epoch)` 一个 append-log `segments.log`**，段以 `[u32 len][bytes]` 分帧追加，**整批一次 fsync**；目录 fsync 仅在该 log 首次创建时一次。owner→follower 的 append 由 follower 侧复用 `internal/upload` 合批器合并（`handlePeerAppend` → `EnqueueWait`，**ack 等该批 fsync 完成**，保持 RPO=0 语义）。
- **理由**：初版 `Spool.Append` 每段 2 次 fsync（文件 + 目录）且全程持锁，fleet ack 吞吐被串行化**封顶 ~1.1k RPS**（并发无效）；append-log 把 fsync 降为 O(1)/批。
- **代价**：spool 布局由"每段一文件"改为"每 `(scope,epoch)` 一 log"；`Held()`/读取按帧解析，**尾部撕裂记录按 best-effort 截断**（崩溃安全）；P0 无旧数据，无迁移负担。
- **结果**：2 节点 fleet **~1.1k → ~13.8k RPS（~12x）**，`failures=0`；c≥128 饱和于 owner↔follower 的**每笔一次 HTTP 往返**，后续按 ADR-026 换 gRPC + 单请求多段。

---

## ADR-041 fleet 传输层 group commit：帧化批量 + 单批在途 ✅

- **决策**：owner→follower 不再"每笔一次 HTTP POST"，改为**按 follower set 合批**：`peer.ShipBatcher`（1ms 窗口、≤256 段/批）把并发 commit 合并成一帧多段（`[u32 len][bytes]`，`peer.EncodeFrames`），一次 POST 到新端点 `POST /v1/peer/append_batch`；follower 侧 `Spool.AppendBatch` **一次 fsync 整批**；仍为 **quorum-1**（首个 ack，非全 follower 持有，属有意取舍）。
- **理由**：ADR-040 后瓶颈是 owner 每笔一次 HTTP 往返（封顶 ~13.8k）；合批把 N 笔摊到 **1 次往返 + 1 次 fsync**；长连接持久流、在途多轮、1ms 群提交是既定方向。
- **代价**：引入 1ms 合批窗口（c=1 延迟略升 1.17ms）；批内任一段校验失败会使**整批失败**（回调退化为 bucket，方向安全）；**尚未实现** 持久流（HTTP 101）、在途多轮、hedge。**修订（ADR-164）**：持久流/在途多轮/自适应 hedge 均已实现。
- **结果**：2 节点 fleet **~13.8k → ~45k RPS（再 3.3x；累计 ~1.1k → ~45k，~40x）**，`failures=0`；c≥1024 饱和；已与同机单节点 `bucket async`（~44.9k）持平。
- **后续**：持久流 / 在途 4 轮已由 ADR-042 实现；hedge 仍待做；gRPC（ADR-026）替代 HTTP 粗边。

---

## ADR-042 fleet peer 使用 HTTP 101 持久二进制流与有序 lane ✅

- **决策**：实现 `POST /v1/peer/stream`，鉴权后 HTTP **101 Upgrade** 为自定义双向二进制流（不是 SSE）；owner 发送 scope/epoch + framed segments，follower `AppendBatch` fsync 后回 1-byte ack。每 follower 建 **4 条按 scope 哈希的 lane**，同 `(scope,epoch)` 固定 lane 保序，每 lane 最多 **4 批在途**；流建立/读写失败自动回退 ADR-041 的 `/v1/peer/append_batch`。
- **配套**：`Spool` 全局锁改为 per-directory mutex，不同 `(scope,epoch)` 可并行，同一 log 仍串行；`realbench` txid 改全局原子递增，消除并发 worker 重用 txid 的假冲突。
- **理由**：采用 signed peer HTTP POST + 101 升级为自定义二进制流、每 lane 多批在途；避免每批 HTTP header 往返并改善多 cell 尾延迟。
- **结果**：同等总 c=1024/n=102,400：单 scope **45.0k→47.1k RPS（+4.5%）**，p99 65.0→62.7ms；8 scope 吞吐 44.6k，但 p99 降到 **48–52ms**。持久流主要改善 tail，数量级吞吐收益来自 ADR-040/041 的两级 group commit。
- **代价**：当前仅支持内部 `http://` peer URL；升级失败会安全回退普通 batch HTTP；**修订（ADR-164）**：自适应 hedge 已实现（`CELLHIVE_PEER_HEDGE_MS`，默认自适应）。真实跨主机同 AZ / NVMe 仍需复测。

---

## ADR-043 SQL durability benchmark 使用原子 txid + WAL1 page payload + output barrier ✅

- **决策**：`Cell.PutTx` 将 UPSERT 与 `cell_meta.txid` 增量放在同一 SQLite transaction；`wal.Cursor` 按 `DBSize != 0` commit frame 输出 transaction boundaries；LTX delta payload 使用 `WAL1`（page size、transaction/frame count、page no、DB size、真实 page bytes）；`sqlcapture.Capture` 在 fleet ack 后才推进 durable txid 并释放 SQL waiter。
- **配置**：cellstore 设置 `wal_autocheckpoint=0`，测量中 checkpoint/salt rotation 明确失败，不静默跳帧。`cmd/sqlbench` 必须得到 `mode=fleet`，bucket fallback 算失败。
- **理由**：原 `realbench` 直接生成 payload=`"real"`，只能说明复制证明层性能，不能代表 SQL TPS；真实链路必须包含 SQL 单写者、WAL capture/page copy、LTX encoding 和 output gate。
- **结果**：2 节点、64-byte hot-row UPSERT（`synchronous=NORMAL`）：c=1/8/32/64/128 为 **308/1,399/2,280/2,537/3,314 TPS**；c=256/512/1024 为 3,321/3,451/**3,673 TPS**，但 c=1024 p50 达 270ms。甜点为 c=64–128（p50 23–37ms）。每事务约 2 WAL frames；高并发瓶颈为全量 WAL reread + JSON/base64 大批次。
- **协议修正**：`/v1/internal/commit` 原 1MiB JSON 上限会截断约 97 个事务的 base64 LTX；该受鉴权端点现按 64MiB LTX 的 base64 大小设专用上限，其他 JSON 仍为 1MiB。
- **边界**：这是 Go SQLite SQL→fleet benchmark，不包含 workerd/V8、actor-file supervisor 或 checkpoint snapshot/link apply；不得称 workerd DO TPS。
- **后续修订**：ADR-044 将 hot-path txid 改为内存 durability ticket；本 ADR 的 `PutTx` 保留为兼容 API，不再用于性能路径。

---

## ADR-044 SQL capture：增量 WAL、binary commit、内存 ticket ✅

- **决策**：`wal.Cursor` 持有 WAL fd 并以 `ReadAt` 只读未消费 tail，暴露 bytes-read；SQL capture 使用 raw LTX `/v1/internal/commit_binary`；每段最多 128 transactions / 1MiB WAL1 payload，2ms group-commit wait。
- **ticket**：应用事务不再更新 `cell_meta.txid`；`sqlcapture.Writer` 在串行 SQL commit 顺序内分配内存 durability ticket，capture 依 WAL transaction 顺序生成 LTX txid，fleet ack 后推进 barrier。持久 txid 来源是 LTX chain，而非应用数据库额外 page。旧 `Cell.PutTx` 留作兼容。
- **理由**：held WAL handle + exact offset tail reads、dirty notify、durability tickets 和 binary peer frames 是把逐事务开销压下来的关键；本机对照显示 raw single-page UPSERT 约 54.5k TPS，而每事务更新 meta txid 的 `PutTx` 仅约 15.2k TPS。
- **结果**：c=32/64/128/256 为 **6.34k/7.97k/12.23k/15.78k TPS**；c=128 p99 18.4ms、c=256 p99 28.2ms，达到 ≥10k TPS / p99<100ms。WAL 从 2 frames/~8.25KB 每事务降为 1 frame/~4.12KB。c=512 进入过载区（9.85k、p99 439ms）。
- **边界**：autocheckpoint 仍关闭；长期 read-lock + controlled checkpoint/truncate 需等 CellHive snapshot/link apply 完成后再引入。workerd supervisor 仍未接。

---

## ADR-045 SQL capture 采用 WAL2 page-map 去重 ✅

- **决策**：一个 capture chunk 内按 **page number 只保留最后一次写入**，编码为 `WAL2` payload（magic、page_size、commit、tx_count、page_count、`page_no + page bytes` 升序唯一）；不再逐事务传输全部 WAL frame。txid range 仍覆盖该 chunk 的应用事务数，barrier 与 RPO=0 语义不变。
- **理由**：对一个 chunk 的 page number 建 map、每页只取最终版本再编码，可消除冗余；hot-row 场景下同一 4KiB 页被几十个事务重写，逐 frame 传输造成约 50x 冗余。
- **结果**：`ltx_binary_bytes` 在 25,600 事务下从约 105MB 降至约 2MB（~50x）；c=64 14.7k TPS/p99 7.7ms，c=128 **20.4k TPS / p99 10.8ms**，c=256 19.2k/18.6ms。相对 ADR-043 基线（3.3k/87.8ms）为 ~6.2x TPS、~8x p99。`pages_per_transaction ≈ 0.02`。
- **代价**：payload 不再保留中间事务的页面版本，只能重建 chunk 末态（page image 复制本就不提供逻辑回放）；`WAL1` 保留用于需要 transaction 边界的场景。
- **后续**：受控 checkpoint/truncate 仍待 snapshot/link apply；多 cell 分片与 workerd 接入是下一步验证方向。

---

## ADR-046 SQL 写路径 prepared statement + peer 网络延迟注入基准 ✅

- **决策**：`sqlcapture.Writer` 持有一个 `*sql.Stmt`（UPSERT），事务内用 `tx.StmtContext` 复用，避免每次 `ExecContext` 重新 prepare。新增 `CELLHIVE_PEER_LATENCY_MS`（单向），`peer.LatencyTransport` 在每次 replication 请求与 ack 各注入该延迟（总 RTT≈2×），用于单机模拟同 AZ/跨区网络。
- **理由**：隔离 benchmark 显示 prepared 把单事务 16.2µs 降到 **9.4µs（~62k → ~107k TPS）**；端到端 loopback c=128 从 20.4k 升到 **24.8k TPS**。网络曲线需要可控 RTT 才能在单机复现，否则只能测 loopback。
- **网络实测**（2 节点 c=128，failures=0）：one-way 0/1/5/20ms 对应 **24,785 / 14,769 / 5,617 / 1,799 TPS**，端到端 p50 4.9/8.3/25.7/86.1ms。延迟升高后 gate 被 RTT 主导。
- **根因**：每个 hot scope 只有 1 条 lane 且同一时刻只有 1 个批次在途，每批都要等一个完整 RTT；应允许每 lane 多帧在途。
- **代价/边界**：延迟注入是每批发送前后 sleep 的近似，不是内核级 netem；不改变 RPO=0 与同 scope 顺序。
- **后续**：实现单 lane 多批在途（pipelining）以在高 RTT 下接近 K×；受控 checkpoint 仍待 snapshot/link apply。

---

## ADR-047 fleet 有序 pipelining：lane 内多批在途 ✅（SQL 路径待补）

- **决策**：`ShipBatcher` 拆分「累加器 + 有序发送器」；发送器对每个 shard 最多 **K 批在途**（`CELLHIVE_PEER_PIPELINE`，默认 4）。新增 `peer.AsyncTransport`，`rawStream.sendAsync` 在 `writeMu` 下**同步写出帧**（保证 lane 内顺序），ack 异步回收；`StreamTransport`/`LatencyTransport` 实现 `AppendBatchAsync`。
- **理由**：允许每 lane 多帧在途即可把 RTT 重叠；此前每批等完整 RTT，高延迟下吞吐 = batch/RTT。follower 是 lane 的单读者、按到达顺序 apply，因此只要**发送顺序**保持，流水线 acks 是安全的。
- **结果**：synthetic LTX 路径（`Ship` 合批器）c=128：one-way 5ms 时 pipeline=1 **153 rps / p50 833ms** → pipeline=8 **334 rps / p50 384ms（2.2x）**；loopback 2105→2226。
- **未覆盖**：SQL→fleet 单 scope 无改善（5ms 5773→5759；20ms 1798→1749）。原因是 capture 走 `ShipNow` 且**一次只允许 1 个 LTX 在途**，在 `Commit` 上等 ack 才继续 poll；瓶颈在 capture→owner 这一跳。
- **同批安全杠杆**：`maxCaptureTransactions` 128 → **512**（WAL2 page-map 后 chunk 仍小，1MiB 上限仍在）。单批在途时吞吐 ≈ chunk/RTT：20ms 单向 c=1024 从 2,765 → **8,576 TPS（3.1x）**；loopback c=512 从 19,194 → **28,532（1.49x）**。代价是 chunk 覆盖更多事务、延迟上升。
- **下一步设计（未实现，durability 关键路径，需先测）**：`sqlcapture.CommitAsync` 按 txid 顺序连续发出 chunk；owner 对 scope 做 **reorder/handoff（按 `start_txid`）**：只写下一个期望 txid，其余缓冲，帧写完后在锁外等 ack；barrier 仅在**连续** ack 时推进。必须补 reorder 单测与 kill 恢复验证。
  - **修订（ADR-048）**：该设计已实现（`CommitAsync`、owner `orderedDispatcher`、`LatencyTransport`、粘性自适应 pipeline），并有 reorder/gap/超时测试。**修订（ADR-170）**：补齐 `orderedDispatcher.sweep` 的生产调用与 DO/workerd（`dosupervisor`）跨文件并发提交。

---

## ADR-048 capture 级有序 pipelining ✅

- **决策**：
  1. `sqlcapture` 用 `CommitAsync` 按 txid 顺序连续发出多个 chunk（不逐个等 ack），barrier 仅在**连续** ack 时推进（`drain`），乱序 ack 不越过未证明的 chunk。
  2. owner 新增 `commit_binary?pipelined=1&base=<txid>`：`orderedDispatcher` 按 `start_txid` 有序交接——只写下一个期望 txid 的帧，其余缓冲；**帧写持 scope 锁、ack 等待在锁外**，因此写序严格而 RTT 重叠。
  3. `LatencyTransport.AppendBatchAsync` 改为「立即写帧、只在 ack 上注入 RTT」，否则 dispatcher 持锁调用会把发送串行化（这是首轮 pipelining 无效的真正原因）。
  4. capture 采用**粘性自适应**：先串行，首个提交 ≥8ms 才一次性切到 `capturePipeline=4`，并用该 chunk 的 `start_txid` 作 `base`；loopback 保持零开销快路径，且避免串行/流水线混用导致基线失配。
- **理由**：允许每 lane 多帧在途即可重叠 RTT；capture→owner 这一跳此前同步，吞吐 ≈ chunk/RTT。
- **结果**（2 节点 hot-row UPSERT，`failures=0`）：loopback c=128/512 与串行持平（26,409 / 28,922 TPS）；5ms 单向 c=1024 **23,994 → 29,289（+22%）**；20ms 单向 c=1024 **8,576 → 13,287（+55%）**。
- **测试**：`TestDrainAdvancesOnlyContiguousAcks`、`TestDrainPropagatesChunkError`、`TestOrderedDispatcherDispatchesInTxidOrder`、`TestOrderedDispatcherFailsScopeOnGapTimeout`、`TestCommitBinaryPipelinedFleet`；全量 `go test ./...` 与关键包 `-race` 通过。
- **修复的真实缺陷**：① orderer 用首个到达 chunk 初始化基线 → 改为显式 `base`；② capture `poll` 无新事务时提前返回跳过 `drain` → ack 搁置死锁 → 改为 poll 开头先 drain。
- **边界/后续**：仍未做受控 checkpoint/truncate（需 snapshot/link apply）；`orderedDispatcher` 对永久缺口只让调用方超时，未做 scope 淘汰（生产需补）；跨主机真实 RTT 与 workerd 接入待测。
  - **修订（ADR-049）**：受控 checkpoint + KindSnapshot + 恢复 apply 已实现。
  - **修订（ADR-170）**：`orderedDispatcher.sweep` 已在 `Do` 入口调用（空闲 scope 主动淘汰）；`dosupervisor.SyncAll` 改为跨文件有界并发提交（DO/workerd 接入），跨主机真实 RTT 仍是环境残余。

---

## ADR-049 受控 checkpoint + KindSnapshot + 恢复 apply ✅

- **决策**：
  1. `cellstore.Cell.SnapshotPages` 用 `VACUUM INTO` 产生一致全页镜像，作为 `KindSnapshot` 来源；
  2. capture 在 `wal.Cursor` 报告 checkpoint/截断时，先 `drain` 在途 chunk，再发一个全页 `KindSnapshot`（复用 WAL2 page-map），随后以新 WAL 世代继续 delta；快照不占 txid，barrier 语义不变；
  3. 受控 checkpoint 用显式 `Capture.Checkpoint(ctx)`（在静默期调用）：`cellstore.CheckpointTruncate` 释放读锁 → `wal_checkpoint(TRUNCATE)` → 重获读锁；capture 下一次 poll 观测到 salt 轮换并发快照。**不做自动截断**（见下）。
  4. `internal/restore.ApplyFile` 将「最后 snapshot + 其后 delta 按 txid」逐页写入真实 SQLite 文件并 `fsync`，要求 snapshot 覆盖全部页否则报错。
- **理由**：此前 checkpoint 被视为 fatal、WAL 无界增长、且没有真正的 apply → 恢复不可用。这是恢复正确性的最后一块。
- **结果**：端到端测试 capture→(deltas+snapshot+deltas)→`ApplyFile`→真实 SQLite，`integrity_check=ok`、数据完整；`TestApplyFile*`、`TestCaptureEmitsSnapshotOnCheckpoint`、`TestCaptureChainRestores`、`TestContinuousWritesWithCheckpointRestore` 通过（含 `-race`）。loopback c=128 压测无回退（25,331 TPS / p50 4.9ms / p99 8.6ms）。
- **关键修正**：快照最初用 `VACUUM INTO`，会**重排页号**，使其后的 WAL delta 套错页 → 恢复出 malformed DB。改为 checkpoint 回填后**直接读 DB 文件**（页号稳定，与 delta 一致）。
- **数据准确性校验**：2 节点 `sqlbench -unique` 写 5,000 唯一 key（`fail=0`，源库 integrity ok）；从 follower 复制链（108 段）用 `cmd/restoreverify` + `restore.ApplyFile` 恢复 → **5000/5000 行、0 缺失、0 错值、integrity_check=ok**。
- **代价/边界**：**自动截断未完成**。`Capture.Snapshot`/`Checkpoint` 需在**静默期**调用：尝试「暂停 writer + TRUNCATE + 水位」自动截断时，遇到 `database/sql` 连接池与 writer 锁的交互死锁（长期读锁占 1 连接，`Put` 持 `w.mu` 等连接）。已去掉长期读锁（避免饥饿），但连续写入下的安全截断仍需完整接管时序 + 更谨慎的连接管理；在此之前 WAL 随写入增长。`KindLink`/paged 未用。
- **后续**：两连接 read-lock 接管、跨主机真实 RTT、workerd DO 接入。

---

## ADR-050 连续写入下的安全 WAL checkpoint 接管 ✅

- **决策**：capture 在 WAL 超过阈值且无在途 chunk 时，通过 `SetCheckpointer` 钩子执行安全截断：
  1. `writer.WithPaused` 阻塞新的 `Put`（串行写者，paused 期间 committed 水位稳定）；
  2. `CheckpointTruncate`：短 `busy_timeout=500ms`、独立连接执行 `wal_checkpoint(TRUNCATE)`，**不长期持读锁**；
  3. 在 pause 内读取 `watermark = writer.Committed()`（TRUNCATE 已回填全部帧，DB 文件即当前态）；
  4. 恢复 writer；读取（截断后稳定的）DB 文件得到全页快照；
  5. 发 `KindSnapshot`（`StartTxID=EndTxID=watermark`）→ `cursor.Reset()` → `nextTxID=watermark` → barrier 推进到 watermark。
  pause 后新提交进入新 WAL，成为 txid > watermark 的 delta，因此**没有任何 ticket 在持久证明前被释放**。
- **连接/锁管理**：`MaxOpenConns=3`（writer + 读锁 + checkpoint/Conn 余量）；**不长期持有 SQLite 读锁**（早期在 `Capture.New` 里长期 `AcquireReadLock` 导致 `Put` 持 `w.mu` 等连接、读锁占另一连接 → 连接饥饿死锁，sqlbench 全挂）。
- **理由**：WAL 接管思路（回填 + 受控截断）能保证快照与 delta 对齐，同时要避免长期持读锁在 `database/sql` 池下的饥饿问题——用「pause + 短 TRUNCATE + 文件快照」达到同等正确性。
- **结果**（2 节点，`failures=0`，源库 `integrity=ok`）：
  - 持续写入 + 自动 checkpoint（512KiB）写 5000 唯一 key → 从 follower 复制链（154 段）恢复 **5000/5000 行、0 缺失、0 错值、integrity_check=ok**；
  - loopback c=128 无 checkpoint **29,602 TPS / p50 4.19ms / p99 9.06ms**；ckpt=4MiB 22,237 TPS（耗时 +约 25%）；
  - loopback c=512 ckpt=4MiB 26,196 TPS；20ms 单向 c=1024 ckpt=4MiB **16,274 TPS / p99 98.7ms**。
- **测试**：`TestContinuousWritesWithAutoCheckpointRestore`（持续写入 + 自动截断 + 链恢复逐 key 校验）、`TestContinuousWritesWithCheckpointRestore`、`TestCaptureChainRestores`、`TestApplyFile*`；全量 `go test ./...` 与关键包 `-race` 通过。
- **orderedDispatcher 缺口淘汰**：按 scope 记录 `lastUsed`，惰性清扫（TTL=1min）淘汰空闲 scope 并让被缓冲的请求以错误返回（`TestOrderedDispatcherEvictsIdleScope`），避免永久卡住与 map 无限增长。
- **边界**：checkpoint 期间 writer 短暂停顿（阈值越大越少）；跨主机真实 RTT 与 workerd DO 接入待测。

---

## ADR-051 workerd DO → WAL → LTX → cell-agent output gate 接入（不改 workerd）✅

> 状态：**设计 + 最小 spike 已实现并端到端验证**。不改 workerd（ADR-001）。

- **背景**：stock workerd 原生 DO + SQLite 已跑通（P0.7），actor `-wal` 可被外部只读解析（A1 实测）。缺的是"**响应的输出门**"——DO 的 `fetch()` 返回前，其 SQL 提交必须已被 cell-agent 证明持久（fleet follower fsync 或 bucket），否则调用方可能基于"可能丢失"的值行动。
- **约束**：不能改 workerd；不能用 FUSE 以外的方式拦截其 write/fsync（FUSE 方案见 durable-objects.md，代价大）。
- **方案对比**：
  1. **host binding `sync()`（推荐）**：由 `workerLoader`/`user-runtime` 注入一个 host 服务（如 `env.CELL_SYNC`），租户 DO 在执行写后、返回前调用 `await env.CELL_SYNC(scope, seq)`；host adapter 收到后：
     - supervisor 捕获 actor `-wal` 的已提交事务（`internal/wal`，salt 轮换即重定基并触发快照，A1 已验证）；
     - 生成 LTX（WAL2 page-map）并 `POST /v1/internal/commit_binary`（已有）；
     - `sync()` 在 **fleet/bucket 证明**后才 resolve → DO 响应才返回（输出门）。
     - 本质是把 `docs/durable-objects.md` 的 "host adapter 返回前调用 cell-agent.sync" 落地为 **binding 调用**，无需改 workerd。
  2. **FUSE 拦截**：workerd `localDisk` 放到 FUSE 卷，由 FUSE 层在 `fsync` 返回前完成本地落盘 + 复制（LiteFS 模式）；**免显式 sync**，但引入内核依赖与写放大，且 workerd checkpoint/salt 轮换需在 FUSE 层对齐（A1 实测 1000 页一次）。
- **验收标准**：真实 workerd DO + host binding；每个 DO 请求在 SQL 提交后调用 `sync()`，`sync()` 返回前 cell-agent 已完成 fleet 证明；测出**真实 DO SQL→fleet 端到端 p50/p99 与 TPS**；`kill -9` 后 RPO=0。
- **与"在引擎内做输出门"的差异**：我们不改 workerd，因此用 host binding 显式 `sync()`（或在 DO 内 `state.waitUntil` + binding）。这是 ADR-001 的必然代价。
- **代价/风险**：租户代码必须调用 `sync()`（或框架层自动注入）；`sync()` 增加一次跨进程往返；stock workerd 的 1000 页 autocheckpoint 使 actor 每 ~1000 写产生快照边界，需在 supervisor 侧正确处理（ADR-049）。
- **已实现 spike**（`cmd/cell-supervisor` + `workerd/spikes/p0/gate/`）：
  - workerd DO worker：`sql.exec(写)` → `await env.GATE.fetch("http://gate/sync")`（`GATE` 为 external service 绑定）→ 收到 `proof=fleet` 后才返回；
  - `cell-supervisor`：`/sync` 用 `internal/wal.Cursor` 捕获 actor `-wal`（salt 轮换→发全页 `KindSnapshot`，否则发 WAL2 page-map delta），经 `sqlcapture.HTTPCommitter` 提交 cell-agent `/v1/internal/commit_binary`，**证明返回后**才响应 200。
- **结果（真实 workerd 2026-06-15，双 cell-agent，`failures=0`；`cmd/gatebench` Go keep-alive 客户端，`-d 4s` warm 300）**：
  - 输出门生效：DO 响应含 `proof=fleet`；
  - **单 actor 服务端上限**：门路径 c=1 **301 req/s**（p50 3.16ms）→ 饱和 c=64 **526 req/s**（p50 132.74ms）；对照裸 workerd DO（`plain.js`，无门）c=1 **860 req/s**（p50 1.11ms）→ c=4 **988 req/s**。即输出门在关键路径增加 ~2ms/req（supervisor WAL 捕获 + owner→follower fleet 证明）。
  - TPS 不随并发增长（workerd DO 单线程 + supervisor 每 actor 串行），p50 随 c 线性上升；延迟优先 c=1–4（p99 5–13ms），吞吐优先 c≥16。早前逐请求 `curl` 的 280/88/14 是**客户端受限**，已修正。
  - **数据准确性**：从 follower 复制链（502 段）恢复 actor DB → `t.n=520`（20 预热+500）与源完全一致，`integrity_check=ok`。
- **多 DO 实测（单 workerd，`GET /aN`，`-actor-dir` 多 actor supervisor，c=32×N）**：输出门 TPS 553/738/752/790（N=1/2/4/8），裸 DO 对照 855/849/811/794。**裸 workerd DO 在 1→8 actor 间完全不平移** → 瓶颈是**单个 workerd 进程的提交路径（每写 fsync）**，非 actor 串行/门/cell-agent；门开销 N=1 时 ~35%，N≥4 被 workerd 上限掩盖。**"多 actor 分片近线性"假设被否定（单进程内）**：需更多 workerd 进程或降低每请求 fsync。
- **批量化验证（框架层组提交，`workerd/spikes/p0/gate/batch.js`）**：一个请求内做 k 次写（workerd 自动合并为 1 次提交），**对该批只做一次门证明**。c=8：门 writes/s 随 k 线性：k=1→531、16→7,360、64→29,824、256→**110,080**（对比"1 写/请求"的 531，~207×）；无门对照 924/14k/51k/151k。数据准确性：gated 400 写后从 follower 链（7021 段）恢复 `t.n` 完全一致、integrity ok → **批量化不破坏 RPO=0 门语义**。结论：绕开 ~900 提交/s 墙靠**提高每请求写数**（请求边界/API 形态），非 SQL 写法；门成本被批量摊销。
- **平台侧组提交 wrapper（`workerd/wrapper/groupcommit.js`）**：租户只声明 `apply(op)`；wrapper 缓冲并发 `submit(op)`，一个事件内应用整批 + **一次门证明**。实测 1 op/请求、gated、`windowMs=3`：req/s 160/936/2501/3196/**3637**（c=1/8/32/64/128），同一负载不攒批 ~530 → **~6.9×**；`avg_batch≈23`。数据准确性：200 次顺序 op 从 follower 链（2500 段）恢复 `t.n` 精确一致。权衡：c=1 单请求等 `windowMs`（→ 生产用自适应窗口）；每批一次门 RTT 在关键路径。见 `workerd/wrapper/README.md`。
- **结论/边界**：证明"**不改 workerd 也能做出真实 DO SQL→fleet 输出门**"；上限为 **workerd 进程级**（~850–990 提交/s，fsync 约束，**按事件合并写**）；批量化后 writes/s ∝ 每请求写数（已验证 110k）；非 DO 的 Go 组提交/流水线路径（ADR-044/048）可达 29k TPS；`cell-supervisor` 每 actor 串行；生产应把 capture 放进 cell-agent 进程内或用持久流（ADR-047）。

---

## ADR-052 cellstore per-scope cell 缓存（cell-agent SQLite 接口）✅

> 状态：**已实现并实测**。

- **背景**：cell-agent 的 KV 接口（`/v1/kv/put|get`）此前每请求 `Store.Open`：`sql.Open` 新建连接池 + `migrate`（4 条 DDL）+ `Close`，**无缓存**。实测 PUT c=1 仅 274 rps、c=32 ~3k rps——瓶颈是连接池/DDL 抖动而非 SQLite 写入。
- **决策**：`cellstore.Store` 增加按 scope（DB 路径）缓存的**长生命周期 `*Cell`**，经 `Store.Cell(ctx, sc)` 获取；`Open` 保留为一次性、调用方自持并 `Close` 的路径；新增 `Store.Close()` 统一关闭缓存。KV 处理器改用 `Store.Cell` 且不再 `Close`；cell-agent 退出时 `store.Close()`。`*sql.DB`/`Cell` 并发安全；`MaxOpenConns=3` 不变（1 写者 + 读锁 + headroom）。补 `TestCellCachesAndReopensAfterClose`、`TestCellConcurrentOpenAndPut`（含 `-race`）。
- **结果（`cmd/kvbench`，同一 owner，`failures=0`）**：

  | 操作 | c | before | after |
  |---|---|---|---|
  | PUT | 1 | 274 rps (p50 3.53ms) | **3,050 rps (p50 0.30ms)** |
  | PUT | 32 | 2,978 rps (p50 2.86ms, p99 106ms) | **5,442 rps (p50 4.09ms, p99 26ms)** |
  | GET | 32 | 6,291 rps (p50 2.80ms, p99 37ms) | **12,864 rps (p50 1.41ms, p99 15ms)** |

  - c=1 PUT 提升 **11×**；GET c=32 ~2×；p99 大幅下降（PUT 106→26ms，GET 37→15ms）；PUT 上限 ~5.2k、GET ~12k rps。
- **边界**：缓存以 scope 为键、**进程内**有效（owner 唯一即 OK）；海量不同 scope 的 KV 用法会让缓存无界——当前受租户 ns 数约束，如需要再加 LRU/上限。

---

## ADR-053 do-runtime host-actor + facets 与 supervisor 生命周期 ✅实现

> 状态：**设计已定；实现属 P3**（P0 已证 workerd WAL 可观察；本 ADR 固化调研结论，供 P3 直接落地）。

- **背景**：P3 交付 do-runtime（原生 facet + supervisor），需要一套 stock workerd 下的宿主 actor / supervisor 工程方案。
- **已实测（我们的 pinned workerd 2026-06-15）**：
  - ✅ `ctx.facets` **存在**（DO facet API 可用）；facet 的 `class` 必须是 `DurableObjectClass`（裸类/普通 namespace 绑定会报 TypeError），即必须来自 `workerLoader.getDurableObjectClass()`。
  - ✅ capnp `durableObjectNamespaces[...].preventEviction = true` **可解析并启动**。
- **决策**：
  1. **host actor + facets**：do-runtime 用一个 host DO 托管大量租户 facet（`ctx.facets.get(name, () => ({class, id}))`，`class` 来自 `workerLoader.getDurableObjectClass(className,{props})`）——一个稳定存储 shard / 一套原生 actor 栈；租户代码不改（`class` 由 loader 解析）。
  2. **`preventEviction` 两变体**：resident（`preventEviction=true`，避免空闲后重建 native actor/facet/SQLite 栈）与 evictable（省略），**共享同一 worker 定义，仅 namespace 选项分叉**；用编译期开关选变体。
  3. **每 class 固定 shard 数**（如 16）：`shard = hash(objectName) % N`，owner scope 含 shard → 把 DO 负载摊到多个 host actor，同时保持 owner 唯一。
  4. **supervisor 生命周期（Go 实现）**：PID1 spawn `workerd serve <config>`；并行 renew 任务，**renew 失败即 fail-closed 终止 child**；`SIGTERM/SIGINT` → 停 renew → drain（**只走 `127.0.0.1`**）成功则对 DO **SIGKILL**（避免 workerd post-SIGTERM 半死 listener 造成接管 504），失败则 SIGTERM 并保留 lease；watchdog 超时 SIGKILL；**时序自校验** drain < owner_ttl、shutdown > drain + buffer。
  5. **D1 read cache 思路**（后置）：单条 `SELECT`/`WITH` + 无写关键字 + 无 volatile 函数才缓存；**写即 `mutationVersion++` 全清**；TTL/条目/字节三重上限；token 防"读发起后被写"落旧值。
- **边界**：只借"进程/actor 层"的工程手法；owner/lease 用 CellHive 自己的 **bucket 条件写 + epoch fence + RPO=0 复制**（无共享盘），不依赖外部租约存储。

---

## ADR-054 快照分页（大 cell 快照拆段）✅

> 状态：**已实现并测试**（roadmap P1「快照/paging」）。

- **背景**：快照 = 整库全页镜像，此前编码为**单个** `KindSnapshot` 段。库大于 `maxSegmentBytes`（64 MiB，`internal/server`）时根本无法快照——P1 的硬缺口。
- **决策**：`ltx.EncodeSnapshotParts` 把一个快照拆成多个 `KindSnapshot` 段（共享 txid watermark、相同 `commit`、页范围不重叠且按页号升序），分片信息放 **LTX header `flags`**（bit15=分页、bit0..14=part index），段名 `<txid>.p<part>.snapshot`；放得下时仍写单段未分页形式（`flags=0`，向后兼容）。`restore.ApplyFile` 取最大 watermark 快照、合并其所有分片，再按 txid 套用 delta。预算 `ltx.DefaultSnapshotPartBytes`=8 MiB（< `maxSegmentBytes` 64 MiB，单段即单请求）。
- **落点**：`internal/sqlcapture` 的 `snapshotNow`、`cmd/cell-supervisor` 的 actor 快照均改用分页编码。
- **验收**：`internal/ltx/snapshot_parts_test.go`（单段未分页 / 分片预算·命名·页并集 / 预算过小 / 页不连续）；`internal/restore` 的 `TestApplyFilePagedSnapshotAndDelta`（分片快照 + delta → 行与 `integrity_check=ok`）。全量 `gofmt/build/vet/test` + `-race`（ltx/restore/sqlcapture）绿。
- **compaction 折叠原语（本次一并实现）**：`restore.Compact(chain, maxPartBytes)` 把一条 chain（snapshot + deltas）折叠为**单个**（必要时分页）快照，覆盖链中最高 txid，丢弃被覆盖的旧页版本；`restore` 抽出 `mergeChain` 供 `ApplyFile`/`Compact` 复用。测试 `TestCompactFoldsChain`（段数减少、watermark 正确、还原行与 integrity）。这是 L0→L1 的**折叠内核**。
- **compaction 编排（本次一并实现）**：新增 `internal/compaction.Compact`——按阈值（`MinSegments`/`MinBytes`，环境变量 `CELLHIVE_COMPACTION_MIN_SEGMENTS`(64)/`_MIN_BYTES`(64 MiB)）折叠当前 L1 基线 + 更新的 L0 delta 为新的 L1 快照，写 `cells/<scope>/ltx/e<epoch>/L1/` 对象与 `L1/manifest.json`（`min_txid`/`max_txid`/`objects`/`commit`/`page_size`）。`replica.Restore` **优先读 L1**：有 manifest 时只读其 `objects` + `txid > max_txid` 的 L0 delta，否则读整条 L0 链；`ListL0Segments` 排除 L1。cell-agent 起后台循环（`CELLHIVE_COMPACTION_INTERVAL`，默认 30s），对 `owner.Manager.OwnedScopes()` 逐个折叠，写前用 `Verify` 复核 node+epoch（失去租约则不写 manifest）。**未做**：L0/L1 旧对象的 GC（`Bucket` 接口暂无 Delete，由 manifest 指针取代）；**稀疏文件按需 paging**（冷启动不整库下载）。
- **真实 E2E（双 cell-agent，`CELLHIVE_COMPACTION_INTERVAL=1s`、`MIN_SEGMENTS=1`）**：`sqlbench` 写 5000 事务（`failures=0`、`integrity_check=ok`）后，后台把 **159 个 L0 段折叠为 1 个 L1 对象**（`5000.snapshot`）+ manifest（`min_txid=32`、`max_txid=5000`、`commit=5`）→ 接管只需读 1 个对象（目标 ≤ ~50，达成方向）。日志：`compacted scope=cpt/__kv__/main inputs=159 parts=1`。
- **连续性校验（正确性）**：`restore` 现要求 delta 自快照 watermark 起 **txid 连续**；有间隙（如已 ack 但对象未上传完）→ fail closed，避免折叠/还原出"静默过期"的库（`TestApplyFileRejectsTxIDGap`）。

---

## ADR-055 按需分页原语（页索引 + 稀疏 materialize）与 snapshot watermark 一致性修复 ✅

> 状态：**已实现并测试**。

- **背景**：roadmap P1「paging」要求「大 cell 采用稀疏文件 + 按需分页，冷启动不必整库下载」。此前 restore 只能整库 materialize（`ApplyFile` 把全部页读进内存再写整文件）。
- **决策（按需分页原语）**：
  - `ltx.PageMapNumbers` / `ltx.PageMapLookup`：只扫页号 / 只取单页，不整段解码拷贝。
  - `restore.IndexChain(segments)` → `PageIndex`：记录每页最终版本所在的 part（不物化页数据），套用与 `mergeChain` 相同的排序与 txid 连续性规则。
  - `PageIndex.Page(pgno)` 按需取单页；`PageIndex.Ranges()` 返回已定义页的连续区间；`PageIndex.SparseFile(dest, ranges)` 写出满长度、只填指定区间的**稀疏文件**（其余为 hole），内存有界。全区间填充与 `ApplyFile` **字节级一致**。
  - CLI：`restoreverify -pages "1-5,10"`（稀疏恢复）、`-list`（打印链头 kind/start/end/crc，用于诊断）。
- **正确性修复（snapshot watermark 必须与镜像一致）**：`poll` 检测到 checkpoint 时调用 `snapshotNow` 却**没有先 checkpoint 回填**，会把「writer 已 committed 到 N、但 DB 文件仍是旧镜像」的页标成 watermark N；随后 delta 从 N+1 续，导致 `1..N` 的事务被**静默丢失**（真实 E2E 复现：两个快照 payload 逐字节相同却分别标 watermark 0/16，full restore `integrity` 失败、`keys=0`）。修复：新增 `rebaseline`——经 `checkpointer`（暂停写者 + TRUNCATE 回填 + 读 W0）后再读页，保证 watermark 与镜像一致；`Snapshot()` 与 `maybeAutoCheckpoint` 复用统一的 `emitSnapshot`。
- **验收**：`internal/restore` 的 `TestPageIndexAndSparseFile` / `TestPageIndexRejectsGap` / `TestPageMapNumbersAndLookupRoundTrip`；`internal/sqlcapture` 的 `TestRebaselineSnapshotReflectsCommittedWrites`；真实 E2E（sqlbench 2000 事务）full restore `integrity=ok`，`restoreverify -pages "1-2"` → `filled=2`、满长度、hole 正确。全量 `gofmt/build/vet/test` + `-race` 绿；不改 workerd、无新依赖。
- **未做**：把 `PageIndex` 接到 **SQLite VFS / 冷启动按需拉取**消费者（真正实现"不整库下载"）；页索引对象（免于为建索引下载全部 part）。

---

## ADR-056 冷启动按需拉取（L1 页索引对象 + ranged-get PageFetcher）✅

> 状态：**已实现并测试**。

- **背景**：ADR-055 的 `PageIndex` 仍需把整条链的 payload 读进内存才能建索引；"冷启动不必整库下载"需要一个**持久化页索引** + **单页 ranged 读**。
- **决策**：
  - 新增 `replica.PageIndex`（对象 `L1/index.bin`，magic `CIDX`）：每个 L1 对象的 key + `page_size` + `commit` + **升序页号表**；由 `compaction.Compact` 在写完 L1 对象后写入（`manifest.json` 仍是提交点）。
  - `replica.NewPageFetcher(ctx, scope, epoch)`：读索引 + 读取更新的 L0 delta（未折叠、按构造很小，整段读）；`Page(ctx, pgno)`——L0 命中优先，否则按页号表二分算出该页在 L1 对象内的字节偏移（`ltx.HeaderSize + ltx.PageEntryOffset`），用 `Bucket.RangedGet` 取 `4+pageSize` 字节并校验页号；`Materialize(ctx, dest, ranges)` 复用 `restore.SparseFileFunc` 按需填充（未填为 hole），**内存只保留一页 + 小型 L0 集合**。
  - CLI：`restoreverify -bucket <dir> -scope .. -epoch .. [-pages ..]`。
- **实测（双 cell-agent + 后台 compaction，sqlbench 3000 事务）**：桶内出现 `L1/{3000.snapshot, index.bin, manifest.json}`；`-bucket` 全量按需恢复 → `filled=5, integrity=ok, keys=1`；`-pages "1-2"` → `filled=2`、满长度、尾部 hole。单测 `TestPageFetcherOnDemandFromL1Index`（L1 ranged-get + 更新 L0 覆盖 + 全量 == `ApplyFile` + 稀疏 hole）。
- **旧对象 GC（本次一并实现）**：`compaction.Options.GC` 在新 manifest 提交后删除被取代的对象——不再被 manifest 引用的旧 L1 对象，以及所有 segment 都已折叠（`EndTxID ≤ 新 watermark`）的 L0 对象（`replica.DeleteObject`）。cell-agent 后台默认 `GC: true`。测试 `TestCompactGarbageCollectsSupersededObjects`；真实 E2E（sqlbench 2000 事务、折叠 127 输入）后 `ltx/e1/` 下**只剩 `L1/{snapshot,index.bin,manifest.json}`（L0 计数 0）**，按需恢复仍 `integrity=ok`。
- **边界（未做）**：SQLite **运行期**懒读仍需 VFS（本 ADR 交付的是"按需取页 + 稀疏物化"，不是运行期 VFS）；页索引对象未做增量更新（每次 compaction 重写）。
- **消费方边界（重要）**：`PageIndex`/`PageFetcher`/`SparseFile` 的**消费方是 do-supervisor（DO facet/host 文件）与 `cmd/restoreverify`**；**cell-agent 自己的 cell（KV/D1/Queue/Workflow/Vectorize/control）打开路径不走它** —— `cmd/cell-agent` 的 `store.Hydrate` 是 `rep.LatestEpoch` → `replica.Restore`（整读 manifest 列出的 L1 对象 + 未折叠 L0 段）→ `restore.ApplyFile` **一次性写出整库**。即：KV/D1 冷恢复是"整 cell 物化（少量 L1 + L0 尾巴）"，不是"只拉工作集页"；逐页 ranged 传输目前只发生在 DO 侧（`Materialize(dest, nil)` 仍填满整库，只是按页取、内存有界）。

---

## ADR-057 drain token + 优雅 handoff + node-log/recovery 完整化 ✅

> 状态：**已实现并测试**（P1 第二项）。

- **背景**：cell-protocol §7 的 drain token（M-07）与优雅 handoff 未实现；node-log/recovery 是 P0 最小实现（ADR-037 C/D/E 要求完整化）。
- **决策**：
  - `internal/drain`：bucket `fleet/drain-token.json`（条件创建抢占、CAS 续期/过期抢占、TTL 30s、`Release` 删除、`Wait` 退避重试）；`Now` 可注入便于测试。
  - **优雅 handoff**（`cmd/cell-agent` + `server.SetDraining`）：收到 SIGTERM/SIGINT **立即置 draining**（`/v1/internal/claim` 返回 503 `draining`，停止接受新 cell）；HTTP 静默后 `performHandoff`——抢 drain token（串行化并发关停）→ seal 本节点 node-log（给下一个 owner 走 recovery fast path）→ 释放所有 owned scope 的 owner 记录 → 释放 token。
  - `recovery.RecoverNode`：列出死节点所有 open/recovering session（sealed 走 fast path），逐个 recovery（CAS recovering → 从 followers 收齐 Held 段 → 上传 bucket → sealed）。新增 CLI `cmd/recoververify`。
- **正确性修复（recovery 暴露的真实 bug）**：recovery 把 follower 的**单段**追加回桶，而 owner 之前是**成批**上传 → 同一逻辑段以两种粒度存在 → restore 报「duplicate page」/重复 delta（`mergeChain` 报 snapshot has duplicate page）。修复：`ltx.Header.ID()`（`epoch:kind:start:end:CRC`）作为段身份；`replica.Restore`、`compaction.Compact`、`PageFetcher` 按 ID 去重，保留一份逻辑副本。
- **验收（单测）**：drain token 独占/续期/TTL 抢占/非持有者 release no-op/`Wait` 重试；`server` draining 拒新 claim；`RecoverNode` 列节点 + sealed fast path + 二次 no-op；restore 去重（batch+单段同存）。全绿。
- **验收（真实 E2E）**：
  • 优雅 SIGTERM：node-log `open` → `sealed`；owner 记录释放（absent）；drain token 释放（absent）。
  • kill -9：node-log `open`、仅 25 个 L0 对象已上传；`recoververify` 从 follower 收齐 **128 段**（**时延 0.04s**，远低于接管 ≤5s 目标）并 sealed；从桶恢复 → `integrity=ok, keys=1000`（sqlbench 1000 个 unique key 全部恢复 = **RPO=0**）。
- **边界（未做）**：接管时**自动**触发 `RecoverNode` 的编排（当前是可调用完整路径 + CLI）；§7 步骤 2 的「逐批迁移 + peer dormant 领取」协调；do-runtime 侧 takeover。

---

## ADR-058 统一定时器抽象 + 本地派发 + 单 fleet waker ✅

> 状态：**已实现并测试**（P1 第三项；对照 docs/timers-and-dispatch.md、ADR-009/ADR-033）。

- **背景**：统一定时器/本地派发/单 fleet waker 此前不存在（无独立 scheduler 服务，ADR-009）。
- **决策**：
  - `internal/timer`：`Timer{DueAtMs,Kind,Scope,Occurrence,Token}`，`Kind ∈ {do-alarm,cron,queue-delay,queue-retry,workflow-sleep,workflow-timeout}`；`TokenFor = sha256(scope|kind|dueAt|occurrence)`（ADR-033 去重键）；`Store` 落在**所属 cell 的 SQLite**（复用 cellstore 缓存 cell）：`timers(due_ms 索引)` + `fired(TTL)`；`Upsert/Due/MarkFired/IsFired/Prune`；`Registry` 记录持有 timer 的 scope。
  - **本地 due 派发**：`timer.Runner` 轮询 registry 内 scope 的 `Due`（bounded batch），`DispatchDue` 成功后才 `MarkFired`，失败保留重试（**at-least-once**）；`Dispatcher` 接口 + `NoopDispatcher`；HTTP 实现 `internal/dispatch.HTTPDispatcher` POST 到 `user-runtime` 逻辑服务名（**无节点表**）。
  - **单 fleet waker**：`internal/waker`——bucket `fleet/waker.json` 条件创建/CAS + TTL 选举（同一时刻至多一个 leader）；leader 只对 **owner 过期** 的 timer scope（`owner.Manager.DeadScopes(class)`，冷路径 List，**不建 bucket 计时索引**）兜底派发；非 leader 不派发。
  - **接线**：server 加 `POST /v1/internal/timer/upsert`（写入所属 cell + 注册 scope）；cell-agent 按 `CELLHIVE_TIMER_INTERVAL` / `CELLHIVE_DISPATCH_URL` / `CELLHIVE_WAKER_INTERVAL` / `CELLHIVE_WAKER_TTL` 启动 runner 与 waker。
- **验收（单测）**：store（due 排序、去重 token、fired TTL、非法 kind、per-cell 隔离）；runner（恰好一次、失败重试、批量上限）；waker（唯一 leader、TTL 抢占、非 leader 不派发、failover）；`owner.DeadScopes`；server timer upsert；dispatch HTTP。
- **验收（真实 E2E）**：本地 runner 对 2 个到期 timer **恰好各派发 1 次**（sink 记录 2 条），同一身份 re-upsert 后仍为 2（fired 去重）；waker 唯一 leader=node-b，`kill -9` 后 TTL(2s) 内 **node-a 接管**。
- **边界（未做，留后续）**：cron 分数/时区、queue claim/ack/DLQ 具体语义、workflow sleep/timeout 与状态机对接、do-alarm 从 supervisor 提取 due（A2 表结构已有，派发接线留 P3）、timer cell 的 resident/pinned 策略、waker 退避参数细化。

---

## ADR-059 owner 解析库 + 端点 + 非 owner 转发 ✅

> 状态：**已实现并测试**（P1 第四项；ADR-003、docs/cell-protocol.md §4/§4.1）。

- **背景**：ADR-003 把 owner 解析定为 **Go 库 + 本地端点**；此前只有 `owner.Resolve*` 与 `GET /v1/internal/resolve`，缺**调用方库**与**非 owner 转发**（"任何副本可接受请求"）。
- **决策**：
  - `internal/ownerclient`：`Client{Seeds,Token,HTTP,TTL}`；`Resolve(scope)` 调 `GET /v1/internal/resolve`，返回 `Hint{Node,Role,Address,Epoch,Expiry}`；按 scope 做 TTL 缓存（stale 滞后 ≤TTL，写者仍靠 epoch 自 fence）；`OwnerURL`/`Invalidate`；`Forward` 把请求转发到 owner（内部 token + `x-cellhive-forwarded` 环防护），owner 返回 409 时失效缓存并**重试一次**。~~种子来自 `CELLHIVE_CELL_AGENTS`~~ —— **该 env 已删（ADR-136）**：`Client` 的种子构造无调用者，服务端只用 `Hint`/`OwnerURL`/`ForwardedHeader`，peer 列表改由 `nodes/*` lease 的 `PeerURL` 提供。
  - 服务端：`/v1/internal/resolve` 已返回 owner 地址/epoch；`requireOwnerEpoch` 增加"**本地须为 owner**"校验（否则 `owner.ErrNotOwner` → 409 `not_owner`）；`POST /v1/internal/commit`、`/v1/internal/commit_binary`（及 `/v1/internal/append`）在「本地非 owner、owner 未过期且含 Address」时**原样转发**给 owner（方法/路径/查询/body/内部 token）；已带环防护 header 的请求**不再转发**（fail closed）。
- **验收（单测）**：ownerclient（缓存命中/失效、无 owner、转发到 owner、409 失效重试一次）；server 转发（owner+非 owner 共享桶：向非 owner 提交 → 转发 → owner 落桶 200；环防护请求 → 409）。全绿。
- **验收（真实 E2E）**：双 cell-agent；claim 在 node-a；从 node-b `resolve` 返回 `owner.address=127.0.0.1:7001, epoch=1, expired=false`；`realbench -owner http://<node-b>`（**非 owner**）200 个提交 → **`mode=fleet, failures=0, p50 6.57ms, p99 10.25ms, rps≈1163`**（经转发由 owner 完成 fleet 证明）；带 `x-cellhive-forwarded` 的请求 → **409 not_owner**。
- **边界（未做）**：do-runtime/user-runtime 侧库接入（库已就绪）；WATCH 式解析快照（当前点查 + TTL 缓存）；转发大 body 的缓冲上限细化。

---

## ADR-060 控制面骨架（应用/版本/路由/密钥，按 app 分片）✅

> 状态：**已实现并测试**（P1 第五项；docs/control-plane.md、routing.md、security.md；ADR-012/029/030/031/036）。

- **背景**：控制面此前无实现（roadmap P1「控制面骨架 + routing/security 落地」）。
- **决策**：
  - `internal/control`：对象模型（App / Worker / **不可变 Version** / Route / Binding / Resource / Secret / Audit）存于**按 app 分片的 control cell**（`<ns>/__control__/main`，复用 cellstore SQLite；全局 app 注册在 `__platform__/__control__/main`）。`Deploy` 分配不可变版本号并**在同一 DB 事务内**写版本 + 切 active 指针；`Promote/Rollback` 切指针（记录 previous）；`CreateApp/CreateResource/PutRoute/DeleteRoute/Audit`。
  - **信封加密**（security.md）：随机 DEK（AES-GCM）加密值，根密钥（`CELLHIVE_SECRET_KEY`，base64/hex 32B，存于 cell 之外）包裹 DEK；只落 wrapped DEK + 密文；根密钥缺失 → secrets **fail closed**（`ErrNoEnvelope`）。
  - **路由投影**（ADR-031 纯拉取）：`Projection()` 汇总全部 app 的 routes + 每个 worker 的 active version；ETag 对**稳定内容**取哈希（排除 `at_ms`），无变化 → **304**。
  - **独立 admin 监听** `:8082`（`CELLHIVE_ADMIN_TOKEN`）：控制面**写端点只挂 admin mux**；数据面 `:7001` 仅暴露 `GET /v1/control/routes`（内部 token）供 user-runtime 拉取 → 控制写在数据面 **404**（security.md 分监听/分鉴权）。
  - CLI `cmd/cellhive`：`app create` / `resource create` / `deploy` / `promote` / `rollback` / `route add|rm` / `secret put|get` / `routes` / `audit`。
- **验收（单测）**：`internal/control`（信封往返/错误根钥、app+资源+保留 ns、deploy 不可变 + promote/rollback、投影 + ETag 随 promote 变化、secret 往返 + 无信封 fail-closed、audit）；`internal/server`（admin 无/错 token → 401、数据面控制写 → 404、投影 200 + `if-none-match` 304、deploy/secret 往返）。
- **验收（真实 E2E）**：单 cell-agent（admin :8082）→ CLI 建 app/资源 → deploy v1（带 route）/v2 → promote v2 → `routes` 投影显示 v2 → rollback → 投影变 v1（`sha1`）；`secret put`/`get` 往返 `hunter2`；无 token admin 写 **401**；`:7001` 控制写 **404**；条件拉取 **304**；audit **8 条**（首条 `app.create`）。
- **边界（未做）**：bundle/assets 上传与内容寻址存储（ADR-030 的 presign 读取已定）；Traefik provider 下发；审计日志持久化格式与保留；OIDC。

---

## ADR-061 binding 鉴权纵深防御：scope 声明 + per-load scoped token（ADR-029 第 3/4 层）✅

> 状态：**已实现并测试**（P1 第六项；ADR-029、docs/security.md、known-issues I-01）。

- **背景**：数据面此前仅「网络隔离 + 单一共享 internal token」——持有它的人可带任意 `ns` 读写任意租户 cell（**横向越权**，爆炸半径为全局）。
- **决策**：
  - `internal/scopedtoken`：HMAC-SHA256 的 scope token，claims `{ns, kind, name, exp_ms}`（`base64url(payload).base64url(sig)`）；`Mint`/`Verify`（拒绝篡改/过期/缺字段/错密钥）。
  - `control.HasBinding(ns, kind, name)`：ns 下存在 `Resource{kind,name}`，或某 worker 版本声明了该 binding —— 授权依据来自控制面冻结的绑定元数据。
  - 服务端 `scopeAuth` 中间件，仅作用于**租户 binding 数据端点** `/v1/kv/put|get`：有 `x-cellhive-scope-token` → `Verify`；无 token 且 `RequireScope` → 403 `scope_required`；校验 `kind=kv`、query `ns` 与 claims 一致、`HasBinding` 为真；解析出的 ns 传给 handler（**不再信任 query**）。**注：`RequireScope` 开关与 legacy query 分支已由 ADR-074/075 破坏性移除。**
  - `POST /v1/internal/scope-token`（内部 token）在加载期签发 token，供 host adapter 的 binding facade 使用（**不进租户 env**）。
  - 配置 `ScopeSecret`（`CELLHIVE_SCOPE_SECRET`，**必需**）。**注：`InternalToken`/`RequireScope` 已由 ADR-074/075 破坏性移除。**
- **验收（单测）**：`scopedtoken`（往返/篡改/过期/错钥/缺字段）；`control.HasBinding`（资源 / 版本绑定 / 未知 / 保留 ns）；`server` scopeAuth（无 token 403、有 token 200、错 ns 403、篡改 403、未注册 403、错 kind 403、legacy 关闭时 200）。
- **验收（真实 E2E）**：`REQUIRE_SCOPE=true` 的 cell-agent → app+资源 → mint token → 带 token KV put/get **200**（读回 `v`）；无 token / 错 ns / 篡改 / 未注册 binding → **403**；`REQUIRE_SCOPE=false` 的 agent → legacy query ns **200**。
- **边界（未做）**：scope 校验目前只覆盖 **KV**；D1/R2/Queue 等 binding 落地时套同一中间件；binding→cell 的精确映射（当前校验 binding 已声明，实际 cell 仍由 ns 派生）；token 撤销/轮换（靠短 TTL）；`workerd/spikes/p0/host.js` spike 里 token 仍置于 loaded env（生产应由 host adapter facade 附加）。

---

## ADR-062 bundle/assets 内容寻址 + Traefik 下发 + 审计保留 + OIDC/JWT admin（P1 收尾）✅

> 状态：**已实现并测试**（对照 ADR-030/031/036、storage-and-s3.md、routing.md、control-plane.md）。

- **bundle/assets 内容寻址（ADR-030）**：`internal/artifacts`——`PutBundle`（SHA-256 → `bundles/sha256/<aa>/<sha>`，`ConditionalCreate` 去重；重复上传 no-op）、`GetBundle`/`PresignBundle`；`PutAsset`（token=内容 SHA → `assets/<ns>/<worker>/<token>/<path>`）、`GetAsset`/`PresignAsset`；路径安全校验（拒绝 `..`/绝对/空）。端点：admin `POST /v1/control/bundle`、`POST /v1/control/asset`；internal `GET /v1/internal/bundle|bundle-url|asset|asset-url`（presign 走 `Bucket.PresignGet`；不支持则 501）。租户读路径用短期 presign，不经数据面（ADR-030）。
- **Traefik HTTP provider（routing.md）**：~~`GET /v1/internal/traefik` …~~ **已移除（ADR-132）**：平台不再下发边缘配置；边缘（反代/云 LB）由运维静态配置。
- **审计保留/格式（control-plane.md 待细化）**：`control.AuditQuery(ns, limit, sinceMs)`（取最近 limit 条、按 `since_ms` 过滤）、`control.PruneAudit(ns, beforeMs)`、`control.Apps()`；端点 `GET /v1/control/audit?namespace&limit&since_ms`；`CELLHIVE_AUDIT_RETENTION`（默认 720h）由 cell-agent **每小时**后台 prune。审计 JSON 即持久化格式（键 `audit/<016d-ts>-<target>`）。
- **OIDC/JWT admin 鉴权（ADR-036 后续）**：`internal/auth`——`Authenticator` 接口、`StaticToken`（默认，头 `x-cellhive-admin-token`）、`JWTBearer`（`Authorization: Bearer`；RS256 via JWKS 或 HS256；校验签名/`exp`/`iss`/`aud`）、`JWKS`（按 TTL 缓存、刷新失败回退 stale）、`Any`（JWT 或静态 token 任一通过）。`adminAuth` 改走 `Authenticator`（`server.Deps.AdminAuth`，nil 时用 Cfg.AdminToken）。配置 `CELLHIVE_OIDC_JWKS_URL`/`_ISSUER`/`_AUDIENCE`；未配置回落 StaticToken。
- **CLI**：`cellhive bundle put <file>`、`cellhive asset put <ns> <worker> <path> <file>`、`cellhive deploy --bundle <file>`（本地 SHA-256 + 上传 + deploy）。
- **验收（单测）**：artifacts（内容寻址/去重/资产地址/路径安全/stdlib SHA）、auth（StaticToken、RS256 JWT 通过 + 错 aud/过期/篡改/错 iss/伪造签名拒绝、`Any` 回退）、server（bundle/asset 上传+读取+路径安全、traefik 生成、scope/control 既有）。全量 `gofmt/build/vet/test`（**24 包**）+ `-race` 绿。
- **验收（真实 E2E）**：`cellhive bundle put` → sha 与本地一致、桶内 `bundles/sha256/72/<sha>` 存在、`GET /v1/internal/bundle` 字节一致；asset put → `assets/acme/api/<sha>/index.html`、internal 读回 `<html>index</html>`；`deploy --bundle --route` → version 1、sha 一致；`audit?limit=1` → 1 条；配置不可达 JWKS 时 **static token 200、garbage bearer 401**。（当时的 `GET /v1/internal/traefik` 断言已随 ADR-132 移除。）
- **边界（未做）**：assets 构建管道（wrangler/esbuild 产物枚举）；Traefik provider 的部署清单接线；OIDC key 轮换/撤销；scope 校验扩展到 D1/R2/Queue（binding 落地时套同一中间件）。

---

## ADR-063 binding facade + cell-agent API（KV/D1/R2/Queue）✅

> 状态：**已实现并测试**（P2 第一项；docs/bindings.md、ADR-029）。

- **D1**（`internal/d1`，cell `<ns>/__d1__/<db>`）：`Query`（单语句→Columns/Rows 或 affected）、`Exec`（affected）、`Batch`（多语句**单事务**、失败整体回滚）；`?` 参数绑定，JSON 数字整数值规范化为 int64；端点 `POST /v1/d1/query|exec|batch?ns&db`，scope kind=`d1`。
- **R2**（`internal/r2`，键 `r2/<ns>/<bucket>/<key>`）：`Put`/`Get`/`GetRange`/`Delete`/`List`（返回 key/size/etag，limit 有界）；端点 `PUT|GET|DELETE /v1/r2/object`（GET 支持 `Range`→206）、`GET /v1/r2/list`；scope kind=`r2`；键安全校验。
- **Queue**（`internal/queue`，cell `<ns>/__queue__/<name>`，表 `messages`）：`Send`（delay + **幂等键去重**）、`Claim`（可见性 + 租约 + attempts）、`Ack`、`Retry`（退避重入队）、`Depth`；端点 `POST /v1/queue/send|claim|ack|retry`；scope kind=`queue`。
- **KV 补全**：`DELETE /v1/kv/delete` 与 `GET /v1/kv/list`（`cellstore.Cell.List` 有界游标：prefix + after + limit）。
- **scope 鉴权泛化**：`scopeAuth(kind)` 工厂（kv/d1/r2/queue 复用同一中间件；校验 kind、ns、`HasBinding`）。
- **binding facade（host adapter，workerd JS）**：`workerd/platform/facades.js`——`makeKV/makeD1/makeR2/makeQueue/buildBindings`，把 CF 形态 API（KV get/put/delete/list；D1 prepare/bind/all/first/run/batch/exec；R2 put/get(range)/delete/list；Queue send）映射到上述端点，**闭包持有 scoped token**。
  - **关键约束（实测）**：`workerLoader` 的 env 必须可结构化克隆 → **不能把函数放进 env**（`DataCloneError`）。改为**平台注入 wrapper 模块**（`workerd/spikes/p0/host.js`）：loader 的 `modules` 传**源码字符串**（`worker.js`/`tenant.js`/`facades.js`，模块名须以 `.js` 结尾）与**纯文本 bindings**；`worker.js` wrapper 在 loaded worker 内 `buildBindings` 构造 facade，只把 facade 交给租户 → 租户 env 不含 internal token。
- **验收（单测）**：`d1`（create/insert/query、int64 规范化、batch 原子回滚、错误）、`r2`（put/get/range/delete/list、键安全）、`queue`（send/claim/lease/ack、retry+delay、幂等、depth）、`server`（KV/D1/R2/Queue 经 scope token 的集成 + 跨 kind token 403）；全量 `gofmt/build/vet/test`（**27 包**）+ `-race` 绿。
- **验收（真实 E2E）**：真实 workerd（`workerLoader`）+ cell-agent（RequireScope=true，注册 kv/d1/r2/queue 资源）→ 租户经 facade：KV `put`/`get` 往返、D1 `exec`+`prepare/bind/run/first` 往返 `{"d1":"dv"}`、R2 `put`/`get` 往返 + 桶内 `r2/p0/files/o.txt`、Queue `send` 返回 id。
- **边界（当时未做）**：Workflows/Cron binding；Queue 消费者派发；R2 multipart/presign；D1 migrations/sessions；service binding 版本冻结/ACL；facade wrapper 尚未通用化（当时在 P0 spike 内联）。
  - **修订（2026-09-19）**：除 D1 sessions（**显式拒绝**）外均已落地——Workflows（ADR-086）、Cron（ADR-070/076）、Queue 消费者派发（ADR-112/119）、R2 multipart/presign（ADR-113）、service binding 版本冻结/ACL（ADR-104/144）、通用 wrapper（ADR-090）。

---

## ADR-064 `cellhive dev` 本地开发模式 ✅实现

> 状态：**已实现（M1/M2）**（完整设计见 [`dev-mode.md`](./dev-mode.md)；实现与验证见 `known-issues.md` M-08 与 `release-notes.md`）。

- **决策**：
  1. **真实组件、只差拓扑**：dev = 单节点 `cell-agent`（**bucket 持久性**，本机 FS，RPO=0）+ 真实 workerd（`workerLoader`）+ 文件系统 bucket + 真实控制面/复制路径；**不做内存假实现**，避免"本地能跑线上崩"。
  2. **一个 `cellhive dev` 进程编排**：进程内跑 cell-agent（复用组装，控制面调用为本地调用），workerd user-runtime/do-runtime 为**子进程**（workerd 不可内嵌），加 watcher + dev front + 打包器。
  3. **控制面 seeding 是 dev-only 例外**：自动登记 wrangler 声明的资源（生产**拒绝**自动 provisioning，ADR-014）；重复启动幂等、不自动删资源。
  4. **热重载 = 重新打包 + 新不可变版本 + Promote**：复用生产 promote 语义；workerd 侧因加载 id 含版本号 → 新版本即**冷加载**，无需热替换 isolate；编译失败保留上一版继续服务。
  5. **binding 经 scope token + wrapper 模块注入**（ADR-029/ADR-063）：dev host 在加载期铸造 scoped token，loader `modules` 传源码字符串 + 纯文本 bindings，wrapper 在 loaded worker 内构造 facade（**internal token 不进租户 env**；env 不可含函数，ADR-063 实测）。
  6. **本机入口**：dev front `:8787` 做 host/path 路由（`<worker>.<ns>.localhost` 与 `/<ns>/<worker>/`），保留命名空间拦截/清头/request-id；不引入 Traefik。
  7. **数据目录** `./.cellhive-dev/`（bucket/cells/do/logs/dev.json；dev 专用随机 secret 根密钥，非生产），`--clean` 重建、`state.lock` 单实例。
  8. **明确差异横幅**：单节点、自动 provisioning/promote、无 Traefik/OIDC、DO 本地磁盘等，启动时打印。
- **理由**：DX 与"真实性"兼顾：免 Docker/云依赖快速起，但走与生产相同的接口与语义，才能让 dev 暴露真实问题（含 scope/复制路径）。
- **代价**：需抽出 `internal/agent.Run`（供进程内启动）、通用 wrapper 生成、esbuild 打包器（P2）；dev 与生产的差异需长期显式维护，避免"看起来一样实际不同"。

---

## ADR-065 `cellhive dev` = Bun CLI + Miniflare；deploy 服务端功能/兼容性拦截 ✅实现

> 状态：**M1 + M2 已实现并验证（2026-09-15）**（完整设计见 [`dev-mode.md`](./dev-mode.md)）。修订 ADR-005（"无 Node/JS"边界）与 ADR-064（原"真实 Go 栈 dev"方案）。
> 验证结论：`cli/` 的 `cellhive dev`（Bun + Miniflare）起真实 workerd，KV/D1/R2/`vars` 经 HTTP 全通；preflight 正确拒绝 `compat_date_too_new`/`unknown_flag` 并对 `images`/`ai` 等给出 `unsupported_binding` 警告。
> **重要修正（原 spike 结论有误）**：不能把 Miniflare 的 `workerd` 任意 override 到更旧版本——Miniflare 内部 control worker **硬编码** `compatibilityDate`（4.20260714.0 = `2026-07-08`），pinned workerd `1.20260615.1`（支持上限 2026-06-22）会拒绝启动。**正确做法见决策 3**。

- **决策**：
  1. **dev 运行时 = Bun CLI + Miniflare**：`cellhive dev` 用 Bun 写，内部用 Miniflare 起真实 workerd + 本地模拟绑定；**dev 机器上不跑任何 Go 后端进程**（不 require/spawn cell-agent）。生产 = Go workerd + cell-agent（**不变**）。
  2. **修订 ADR-005**：平台**服务**仍全 Go、单二进制；**dev 工具**允许 Bun/JS 承载（Miniflare；必要时 `@cloudflare/vite-plugin`）。生产产物不引入 JS 运行时。
  3. **版本 pin（2026-09-22 更新）**：**Miniflare 版本必须与平台 pinned workerd 同期对齐；不能把 Miniflare 的 `workerd` 依赖 override 到跨期版本**。当前精确组合为 **`miniflare@5.20260916.0-alpha` + `workerd@1.20260916.1`**，lockfile/安装版本测试与 module/KV/D1/R2 真实 smoke 已通过。历史 `4.20260714.0` → 旧 pin 的失败仍证明了该规则。
  4. **契约对拍（Miniflare 作为 CF oracle）**：固定 golden 请求/响应在 Miniflare 与平台 cell-agent 上 diff，差异要么修我们（偏离 CF=bug），要么记入"有意差异"（RPO=0 延迟/scope/quota）。沿用 ADR-014 的 pinned 基线思路。
  5. **dev 不覆盖平台特性**：versions/routes/secrets/deploy/scope/RPO=0 不在 dev → 走真实平台（`cellhive deploy` 是 HTTP 客户端）。
  6. **deploy 服务端功能/兼容性拦截（权威）**：`POST /v1/control/deploy` 服务端校验 bundle 存在、`compatibility_date` ≤ 支持上限（2026-06-22）、`compatibility_flags` 已知、绑定在支持矩阵内（**拒绝 `images/browser-rendering/send_email/ai_search/dispatch_namespaces/secrets_store/containers/...`**；`vectorize`/`hyperdrive` 已支持，见 ADR-158/129）、资源已登记、DO 生命周期合法、未知字段显式拒绝；CLI/dev **preflight 复用同一校验库**提前警告。理由：Miniflare 的绑定"认识面"远宽于平台（实测其插件含 images/ai/browser/vectorize/hyperdrive…），若只在 CLI 拦截会被绕过。
  7. **用户代码兼容是平台责任**：补齐 facade 字段缺口（KV list metadata/expiration、R2 `R2Object` 字段、D1 `meta.last_row_id`、错误形状），使用户代码在 CF/Miniflare/我们 dev/我们 prod 间可移植。
  8. **工具层选择**：`cellhive dev` **直接嵌 Miniflare**（= wrangler 内部同一引擎），不使用 `wrangler dev` CLI（重、语义旁落），也不使用**已废弃**的 `unstable_startWorker`/`unstable_dev`。若日后自研 wrangler 配置翻译/打包的保真成本不可接受，**回退首选 `@cloudflare/vite-plugin`**（CF 官方推荐的 programmatic dev server；配 Vite `createServer()`，底层仍是 Miniflare/workerd），而非 wrangler 的废弃 API。测试场景可用 `createTestHarness()`（wraps Miniflare，可直接读 wrangler 配置）复用配置解析。
- **理由**：用户明确"dev 不该管后端存储实现"；Miniflare 已提供成熟 DX（热重载/inspector/persist/绑定面），自造 dev 栈收益低。且 **Miniflare 即是 wrangler 的引擎**（`wrangler` 依赖 `miniflare`+`workerd`，同属 workers-sdk），"Miniflare 过时"是误解（过时的是 v2 独立 CLI；v3/v4 是库）。代价用"pin 版本 + 契约对拍 + 服务端拦截"兜住。
- **代价/风险**：引入 Bun + Miniflare（+其 workerd）为 dev 依赖（不进生产）；Bun 跑 Miniflare **已 spike 验证**（Bun 1.4.0 完整启动 Miniflare 并 spawn workerd，KV/D1/R2 通过，~150ms）；两套绑定实现需长期对齐（对拍闭环）；dev 与生产存在有意差异，需横幅/文档明示；**工作量重心是 wrangler 配置翻译 + 打包，而非引擎**。

---

## ADR-066 节点死亡自动 recovery 编排（leader-only / 幂等 / lease 判活）✅实现

> 状态：**已实现并验证（2026-09-15）**。补齐 roadmap P1「node-log + recovery 完整实现」的最后缺口（ADR-057 原本只有 `RecoverNode` 可调用路径，无自动触发）。

- **决策**：由 fleet waker（ADR-058 的单 leader 选举）在**每轮 leader pass 顺带执行** dead-node recovery；不再依赖人工 `cmd/recoververify`。
- **判活**：以 node lease（`nodes/<node>.json`，TTL）为准——**lease 存活则绝不恢复**；lease 缺失或过期 → 视为死亡；lease **读取出错 → 跳过**（fail-safe：宁可漏恢复，不误恢复存活节点）。
- **幂等**：`RecoverNode` 跳过 `sealed` 会话（优雅 handoff 已上传的快速路径），仅对 `open/recovering` 执行 fence→collect→seal；重复 pass 无副作用。
- **枚举**：新增 `nodelog.Nodes(ctx)`（Bucket `List("node-logs/")` 前缀），**冷路径**，仅 recovery/运维使用；热路径仍禁止 List（约束不变）。
- **集成**：`waker.Waker.RecoverNodes` 钩子在取得 leader 后调用（与 timer 派发同一 leader，**无需第二套选举**）；`cmd/cell-agent` 复用同一 `nodeLog`/`lease`/`replica`/`peer` 实例。
- **理由**：节点丢失后，已 ack（follower fsync）但未上传的段必须由新 owner 收齐再 seal，否则恢复可能"静默丢失已确认写"（RPO=0 的前提）。
- **代价/风险**：依赖 follower 可达（不可达则本轮跳过、下轮重试）；leader 承担冷路径 List（低频）；误恢复风险用"lease 判活 + 读取失败即跳过"兜住。

---

## ADR-067 Queue 消费者派发（cell-agent 轮询控制面 + user-runtime 内部派发）✅实现

> 状态：**已实现并验证（2026-09-15）**。补齐 P2「Queues 消费者派发」缺口。

- **决策（端到端）**：
  1. **消费者登记在控制面**：`Deploy` 接受 `consumers []string`（该 worker 消费的队列名），存于不可变 `Version.Consumers`；投影（ADR-031）随之暴露。CLI：`cellhive deploy ... --consumer <queue>`。
  2. **cell-agent**：`queue.Runner` 轮询集来自 `control.ResourcesByKind("queue")`（冷路径）；用 `Projection.QueueTargets()` 把队列解析到 **(worker, 活跃 bundle_sha)**；对每个队列 `Claim` → `Dispatch` → 成功 `Ack`、失败整批 `Retry`（**at-least-once**）。**worker 解析在 cell-agent（控制面数据在手）**，不留给 user-runtime。
  3. **user-runtime（新组件，`cmd/user-runtime` + `workerd/user-runtime/`）**：workerd 进程，公开 loader `:8081` + **内部特权派发 `:8088`**。`POST /v1/queues/dispatch`（body 含 `namespace/worker/bundle_sha/queue/messages`）从 cell-agent 取不可变 bundle（`/v1/internal/bundle?sha=`），经 **workerLoader** 加载租户模块，并用平台 wrapper（`WorkerEntrypoint` 子类 `CellHiveHost`）暴露 **`handleQueue` RPC**，从而调用租户的 `queue()` handler（workerLoader 默认只暴露 `fetch`）。租户只见到 facade（`facades.js`），看不到内部 token。
- **配置**：`CELLHIVE_QUEUE_INTERVAL`（默认 1s，0 关）；`CELLHIVE_DISPATCH_URL`（与 timer 共用）；`cmd/user-runtime` 用 `CELLHIVE_CELL_URL`/`CELLHIVE_TOKEN_INTERNAL`/`CELLHIVE_TOKEN_DISPATCH`/`CELLHIVE_SCOPE_SECRET`/`CELLHIVE_USER_RUNTIME_{PORT,INTERNAL_PORT,DATA,JS}`。
- **理由**：消费者是 Queues binding 的组成；复用既有 timer/dispatch 同构基础；workerLoader 不暴露非 fetch handler，故用平台 wrapper + RPC 桥接（已对 pinned workerd 2026-06-15 实测）。
- **验证**：`internal/userruntime` 端到端测试对**真实 workerd** 跑通——队列派发 → workerLoader 加载 bundle → 调用租户 `queue()` → 返回 `handled:1:hello`；cell-agent 侧 `queue.Runner`/`HTTPDispatcher`、`control` consumers→`QueueTargets` 均有单测。
- **代价/风险**：at-least-once（可能重复投递，消费者需幂等）；公开 loader 已由 **ADR-068** 实现；重试上限/死信（DLQ）属 M-10/P4；workerd 需 `--experimental`（workerLoader）。

---

## ADR-068 user-runtime 公开 loader（路由投影拉取 + 版本加载 + facade env）✅实现

> 状态：**已实现并验证（2026-09-15）**。补齐 user-runtime 公开入口（`:8081`），使 user-runtime 成为完整入口。

- **决策**：loader（workerd `:8081`）处理公网 fetch：
  1. **纯拉取**路由投影 `GET /v1/control/routes`（TTL 5s；请求失败时回退陈旧投影，符合 ADR-031）；
  2. 以 **Host** 匹配 route（host 精确 + 最长 `path` 前缀）→ worker；
  3. 取该 worker 活跃 version 的 `bundle_sha`，从 cell-agent `GET /v1/internal/bundle?sha=` 取**不可变 bundle**（按 sha 缓存）；
  4. 为 version 的 `kv/d1/r2/queue` bindings 用 `POST /v1/internal/scope-token` **mint scope token**（TTL 300s，按 `ns/kind/name` 缓存）拼 `SCOPE_SPEC`；`vars` 走 `VARS_JSON` 合并进租户 env；
  5. workerLoader 加载平台 wrapper（`CellHiveHost`），调用租户 `fetch`；**剥离 `x-cellhive-*` 平台头**；租户 `globalOutbound` = **public-only**（隔离，I-09）。
- **错误语义**：无 route → 404；投影不可用 → 503；bundle 失败 → 502；handler 异常 → 500。
- **理由**：入口无独立 gateway（Traefik 只做 TLS/host 分流），loader 必须在 workerd 内完成版本解析 + 加载执行，且与 dispatch 路径共用同一 wrapper/facade 机制。
- **验证**：`internal/userruntime` 对**真实 workerd** e2e——stub cell-agent 提供投影与 bundle，`Host: app.test` 命中路由加载租户，校验 `vars` 注入、平台头被剥离、未知 host 404；`queue` 派发 e2e（ADR-067）同套件。
- **代价/风险**：投影最多 ~5s 陈旧（ADR-031 有意）；scope token 缓存 300s；未做自定义域/路径改写/清头的完整白名单；**assets 尚未接入**（worker-first/assets 属后续）；多 worker/service binding 未做。

---

## ADR-069 user-runtime 资产管道（loaders 侧：index/type/ETag/_headers/_redirects/fallback）✅实现

> 状态：**已实现并验证（2026-09-15）**（对真实 workerd）。补齐 roadmap P2「完整资产管道」的读取/服务侧。

- **决策**：loader 解析出 worker 活跃 version 后，若 `assets_sha` 非空（资产版本 token），**静态资产优先于 worker**：
  1. **`_redirects`**：解析 `from to [status]`（默认 302），支持 `*` splat；`301/302` 返回带 `location` 的响应，`200` 为内部重写；
  2. 路径候选：`/`→`index.html`；无扩展名再试 `.html` 与 `/index.html`；
  3. **content-type** 按扩展名推断（html/js/css/json/svg/png/woff2/wasm…，未知→octet-stream）；
  4. **ETag**（对字节做 FNV-1a + 长度）与 **`If-None-Match`→304**；
  5. **`_headers`**：按路径块（`/*`、`/x/*`、精确）叠加响应头；
  6. 资产未命中 → **回退 worker fetch**；资产端点异常不拖垮动态路由（catch + 继续）。
- **读取路径**：经 cell-agent 内部 `GET /v1/internal/asset?ns=&worker=&token=&path=` 取字节（**不直连桶**，租户仍 public-only）；资产与 `_headers`/`_redirects` 按 `(ns,worker,token,path)` 缓存。
- **理由**：入口无独立 gateway，资产必须由 loader 服务；与版本化 bundle 同源（cell-agent 内部端点）。
- **验证**：`internal/userruntime` 对真实 workerd e2e——index+content-type、304、`_headers`（全局+路径）、`_redirects`(301)、未命中回退 worker。
- **非目标/后续**：`not_found_handling`（SPA/404 页）与 `run_worker_first` 的 **deploy 配置管线**未做；资产上传/版本化未改；CDN 直连未做；`_headers` 语义为子集（未做 `!` 移除/合并去重细节）。

---

## ADR-070 scheduled/timer 派发接入 user-runtime（cron → scheduled()）✅实现

> 状态：**已实现并验证（2026-09-15）**。与 ADR-067（queue→queue()）对称，补齐 timer→scheduled()。

- **决策**：
  1. **登记**：`Version.Crons []string`（worker 处理的 cron 表达式）；CLI `cellhive deploy ... --cron <expr>`；投影暴露，`Projection.CronTargets()` 提供 (ns,worker,cron,bundle)。
  2. **解析**：`KindCron` 定时器的 cell scope 约定为 `<ns>/__cron__/<worker>`；cell-agent 在派发前用 `cronEnricher`（投影缓存 5s）把 **worker + 活跃 bundle_sha** 填进 `timer.Timer`。
  3. **派发**：`dispatch.HTTPDispatcher` payload 增 `worker`/`bundle_sha`/`scheduled_time_ms`。
  4. **执行**：user-runtime internal `POST /v1/timers/dispatch` → workerLoader 加载 → wrapper `handleScheduled(event)` → 租户 `scheduled(event, env, ctx)`（event `{scheduledTime, cron}`）。
- **DO alarm / 其他 kind**：不填 worker → 不路由到 user-runtime（保持现状，DO alarm 属 do-runtime，P3）。
- **验证**：`internal/userruntime` 对真实 workerd e2e（scheduled() 被调用并返回 `scheduled:<cron>:<time>`）；`internal/dispatch` payload 单测；`internal/control` CronTargets 单测。
- **非目标/后续**：cron 的**创建/调度评估**（谁生成定时器）不属本 ADR；DO alarm 派发到 do-runtime 不属本 ADR。

---

## ADR-071 资产路由配置（run_worker_first / not_found_handling）✅实现

> 状态：**已实现并验证（2026-09-15，真实 workerd）**。补齐 ADR-069 的未做项。

- **数据模型**：`control.Version.Assets *AssetsConfig{ NotFoundHandling, RunWorkerFirst, RunWorkerFirstPaths }`；`DeploySpec.Assets` 随不可变版本存储；投影（Projection/Version）暴露。
- **取值**：`NotFoundHandling ∈ {"", "none", "404-page", "single-page-application"}`（空=`none`）；`RunWorkerFirst` bool；`RunWorkerFirstPaths []string`（支持 `*` 后缀前缀匹配）。
- **loader 语义**（GET/HEAD 且 `assets_sha` 非空）：
  - `run_worker_first`=true（或路径命中）→ **先 worker**；worker 返回 404 才回退资产；
  - 否则**先资产**（`_redirects` → 路径候选 → `_headers`/ETag）；
  - 资产未命中：`404-page`→服务 `/404.html`（**状态 404**）；`single-page-application`→回退 `/index.html`（200）；`none`→回退 worker。
- **管线**：`POST /v1/control/deploy` 接收 `assets`；CLI `cellhive deploy --assets-not-found ... --run-worker-first --run-worker-first-path <p>`。
- **验证**：`internal/userruntime` 对真实 workerd 4 个子用例（SPA / 404-page / worker-first / worker-first-paths）全 PASS。
- **代价/风险**：SPA 回退对**所有**非资产 GET 生效（含 404 资源请求，符合 CF SPA 语义）；`_headers` 仍为子集；`not_found_handling` 与 `_redirects` 的优先级按"redirects 先于资产、SPA 最后"实现。

---

## ADR-072 队列消费者配置 + 死信（DLQ）✅实现

> 状态：**已实现并验证（2026-09-15）**。升级 ADR-067 的消费者模型。

- **数据模型**：`Version.Consumers` 由 `[]string` 升级为 `[]Consumer{ Queue, MaxRetries, DeadLetterQueue, MaxBatchSize, MaxBatchTimeoutSeconds }`；`Projection.QueueTargets()` 携带 `Consumer`。
- **runner 语义**（`internal/queue.Runner`）：按消费者 `MaxBatchSize`（0→默认）`Claim`；派发失败时：
  - `MaxRetries > 0 && Attempts >= MaxRetries` → 若配置 `DeadLetterQueue` 则 `Send` 到同 ns 的 DLQ 并 `Ack` 原消息（日志 `queue message dead-lettered`）；否则 `Ack` 丢弃并告警（`queue message dropped ... no dead-letter queue`）；
  - 未达上限 → `Retry`（延迟）。
- **管线**：CLI `cellhive deploy --consumer <queue>[:maxRetries[:deadLetterQueue]]`（可重复）。
- **验证**：单测——达上限入 DLQ（原队列 depth 0 / DLQ depth 1）、无 DLQ 丢弃（depth 0）、未达上限保留重试（depth 1）；`control` consumer 配置投影单测。
- **兼容性**：`Version.Consumers` 结构变更；旧 JSON 中的字符串数组**不再兼容**（尚无生产数据，测试与 CLI 已同步；如需可加自定义 Unmarshal）。
- **非目标**：`MaxBatchTimeoutSeconds` 当前只透传不驱动 batching 窗口（轮询周期决定）；DLQ 本身不消费（可再登记为消费者）。

---

## ADR-073 租户 binding facade 经平台 service binding 出网 ✅实现

> 状态：**已实现并验证（2026-09-15，真实 workerd）**。补 I-09/ADR-029 的落地细节。

- **问题（本次测试暴露）**：租户 loaded worker 的 `globalOutbound` 必须 **public-only**（I-09 隔离），但 binding facade 在**租户 worker 内**执行、需访问内网 cell-agent；用全局 `fetch` 会被 `connect() blocked by restrictPeers()` 拒绝 → facade 实际不可用。
- **决策**：loader/internal 服务各提供一个指向 `private-outbound` 网络的 **service binding `PLATFORM`**，随 workerLoader 的 `env` 注入 loaded worker；wrapper 把 `fetcher: this.env.PLATFORM` 传入 `buildBindings`；`facades.js` 的 `pfetch(platform,url,opts)` **优先 `platform.fetcher.fetch`**，否则退回全局 `fetch`（兼容 p0 loader 的 private-outbound 场景）。
- **效果**：租户代码仍只有 public 出网；**仅平台 facade** 能经 PLATFORM 触达 cell-agent；内部 token 仍不进租户 env。
- **验证**：`TestUserRuntimePublicLoaderBuildsBindingFacades` 对真实 workerd 通过——loader mint scope token → 租户 `env.KV.get` → cell-agent KV 端点收到 `x-cellhive-scope-token` 与 `x-cellhive-internal-token`。
- **代价/风险**：capnp 多一个 binding；facades 必须走 `pfetch`（不要直接用全局 fetch）；network service 作为 binding 需用 `.fetch()`（不可当作函数调用）。

---

## ADR-074 内部绑定鉴权简化：本地 HMAC、无过期、资源绑定 ✅实现

> **修订注记（ADR-137）**：`SCOPE_SECRET` 不再单独配置，由 `CELLHIVE_ROOT_KEY` 派生（label `token/scope`）。

> 状态：**已实现并验证（2026-09-15，真实 workerd）**。修订 ADR-061（scoped token 的 mint/过期部分）与 ADR-029 第 4 层的落地细节。

- **背景**：原设计 = `internal token` + **cell-agent mint 的过期 scoped token** + `/v1/internal/scope-token`。三个问题：① token 在加载期固化进已缓存的 loaded worker，**300s 后 facade 403 且无法刷新**；② 广权限 `internal token` 被放进 loaded worker env；③ mint 端点不查 `HasBinding`，可越权签发。
- **决策**：
  1. **平台本地计算**：loader 用共享 `SCOPE_SECRET` 在加载期本地算 `token = base64url(canonicalJSON{ns,kind,name}) . base64url(HMAC-SHA256)`，**删除 `/v1/internal/scope-token` 与 mint 往返**。
  2. **无过期**：`Claims.ExpiresMs==0` 表示不过期（`Verify` 跳过过期判定）；**撤销改由** `HasBinding`（每请求点查，移除绑定即失效）+ **secret 轮换**（全量作废）。
  3. **绑定端点只认 scoped token**：kv/d1/r2/queue 端点去掉 `s.auth`，仅 `scopeAuth(kind)`（**恒强制**，缺失/非法即 403；`RequireScope` 开关已删除）；**非绑定内部端点（peer/dispatch/admin/control、`/v1/internal/*`）继续要求 internal token**。
  4. **internal token 不进 loaded worker**：`facades.js` 不再发 `x-cellhive-internal-token`；只有**算好的 per-binding token** 进 `SCOPE_SPEC`；secret 仅存 loader 与 cell-agent。
- **规范 JSON 契约**：签名字节 = 紧凑 JSON、字段序 `ns,kind,name,exp_ms`、**关闭 HTML 转义**（`SetEscapeHTML(false)`）；`ns/kind/name` 为标识符（无引号/控制字符）。平台 JS 端按同一规则拼装，**跨语言由 e2e 校验**：workerd 里 JS 本地算出的 token 被 Go `scopedtoken.Verify` 接受（`TestUserRuntimePublicLoaderBuildsBindingFacades`）。
- **效率**：热路径 = **1 次网络请求 + 1×HMAC + 1×`HasBinding` 点查**；mint 归零、无刷新、无惊群。
- **代价/风险**：① **放弃时间盒**（泄漏即长期有效，靠资源范围 + HasBinding + 轮换兜）；② **JS/Go 规范 JSON 强耦合**（有格式单测 + 跨语言 e2e 锁）；③ secret 分布在 loader 与 cell-agent（轮换需同版本部署）；④ 绑定端点不再有 internal token 兜底（依赖私网 + scoped token）。
- **非目标（当时）**：Tier-1 的 role-scoped internal token（后由 **ADR-075** 完成）；外部 Data API/API key（另立）。

---

## ADR-075 平台内部 Tier-1：按角色的平台令牌（+ 补 dispatch 入站鉴权）✅实现

> **修订注记（ADR-137）**：各角色令牌不再各自用环境变量配置，统一由 `CELLHIVE_ROOT_KEY` HKDF 派生（域分离）；`CELLHIVE_ADMIN_TOKEN` 可选覆盖。

> 状态：**已实现并验证（2026-09-15）**。ADR-074 只简化了绑定路径；本 ADR 收敛平台↔平台的共享凭据。
> **破坏性（无兼容，2026-09-15 修订）**：已删除 `CELLHIVE_INTERNAL_TOKEN` 与 `CELLHIVE_REQUIRE_SCOPE`；角色令牌为**唯一必需**凭据，无任何回退；绑定端点 `scopeAuth` 恒强制。

- **背景**：原先所有内部端点共用一个 `CELLHIVE_INTERNAL_TOKEN`（peer 复制、内部 API、派发、转发），任一进程被攻陷即拥有全部内部权限；且 user-runtime 的 `/v1/queues|timers/dispatch` **完全没有入站鉴权**。
- **决策**：
  1. **角色令牌（唯一凭据，无回退）**：`CELLHIVE_TOKEN_PEER` / `CELLHIVE_TOKEN_INTERNAL` / `CELLHIVE_TOKEN_DISPATCH` 各自独立，dev 默认 `dev-peer-token` / `dev-internal-token` / `dev-dispatch-token`；**任一角为空即该角全部 401**（不再有共享 `CELLHIVE_INTERNAL_TOKEN` 兜底）。
  2. **服务端**：`roleAuth(role)` + **常量时间比较**、fail-closed；`/v1/peer/*` → `peer`；`/v1/internal/*` + `/v1/control/routes` + `/v1/diagnose` → `internal`；绑定端点保持 `scopeAuth`（ADR-074）；admin 监听独立不变。
  3. **客户端按角色发 token**：peer 传输 → peer；timer/queue 派发 → dispatch；owner 转发/内部调用 → internal（转发头用 `Cfg.TokenInternal`）。
  4. **user-runtime 入站鉴权**：`internal.js` 对两个 dispatch 端点校验 `x-cellhive-internal-token == DISPATCH_TOKEN`（常量时间），否则 401；capnp internal 服务新增 `DISPATCH_TOKEN` 绑定。
- **诚实边界（重要）**：在没有 PKI/CA/mTLS 的前提下，**角色隔离 = 不同 secret**，不是密码学身份——被攻陷的进程若只拿到自己角色的 secret，就无法伪造其它角色；但平台无法证明"调用者确实是某角色的那个进程"（无 attestation）。**mTLS/内部 CA 是更强的后续步骤**（每实例证书 + SAN 角色）。
- **验证**：`TestRoleTokenIsolation`（peer↔internal 交叉调用 401，各自正确 token 通过）；`internal/userruntime` e2e（真实 workerd：无 token→401、正确 dispatch token→200，queue 与 timer 各一）；`TestRoleTokensRequired`（角色 token 为空/错误 → 401 fail-closed）；既有全部测试在断开回退后仍通过。
- **代价/风险**：配置面新增 3 个 token + `ScopeSecret`（**无回退**，生产必须显式配置；dev 有默认值）；轮换需同版本部署对应角色；破坏性移除 `CELLHIVE_INTERNAL_TOKEN`/`CELLHIVE_REQUIRE_SCOPE`（本项目未上线，无需迁移）。

---

## ADR-076 cron 调度器（生成 slot 定时器）✅实现

> 状态：**已实现并验证（2026-09-15）**。补齐 ADR-070 的前半段：`Version.Crons` → `scheduled()` 全链路闭环。

- **决策**：新增 `internal/cron`：
  1. **解析**：自研标准 **5 字段** cron（`min hour dom month dow`），支持 `*`/`a`/`a-b`/`a,b`/`*/n`/`a-b/n`；范围 min 0-59、hour 0-23、dom 1-31、month 1-12、dow 0-7（7=周日）；**UTC**；`dom` 与 `dow` 同时受限时按 vixie 取 **OR**。
  2. **物化**：`Scheduler.Pass` 把当前分钟（截断）与每个 `Projection.CronTargets()` 匹配，命中则向 `<ns>/__cron__/<worker>` cell `Upsert` 一条 `KindCron` 定时器：`dueAt`=slot 起始、`occurrence`=slot（unix 分钟）、scope 约定 `<ns>/__cron__/<worker>`；并 `Register(scope)` 让 timer runner 轮询。表达式解析**带缓存**，非法表达式仅告警跳过。
  3. **语义**：**幂等**（timer token 去重，重复 tick/多节点重复 upsert 只留一条）；**best-effort 不补跑**（错过的 slot 不回溯）；**每节点可跑**（幂等，无需选举）。
- **配置**：`CELLHIVE_CRON_INTERVAL`（默认 30s，0 关闭）。`cmd/cell-agent` 在控制面就绪后启动。
- **验证**：`internal/cron` 单测——解析/匹配（`*/5`、范围/列表/步长、dow 7=0、dom/dow OR、UTC）、非法表达式、**物化+去重**（同分钟重复 Pass 后 `Count==1`）、**handoff**（`Scheduler` 物化 → `timer.Runner` 派发到 fake dispatcher，scope/kind 正确）；`cmd/cell-agent` 编译通过、全量 32 包全绿。
- **边界**：UTC（不做 per-app 时区）；不做秒级/6 字段；不补跑；DO alarm→do-runtime 不在此列。

---

## ADR-077 do-runtime host-actor 骨架（原生 DO：facets + localDisk）✅实现

> 状态：**已实现并验证（2026-09-15，真实 workerd）**。P3（DO 完整）的第一块地基。

- **机制（实测）**：host actor（`class Host extends DurableObject`）用 `this.ctx.facets.get(actorId, () => ({ class, id }))` 运行租户 DO；`class` 来自 **`env.LOADER.get(id,getCode).getDurableObjectClass("<Class>")`**（注意 `getDurableObjectClass` 是 **WorkerStub** 的方法，不在 loader 上；`getEntrypoint` 同）。存储用 workerd **localDisk**：`durableObjectStorage = (localDisk = "<disk-service>")`，disk 服务 `disk = (path=…, writable=true)`，每类生成 `<uniqueKey>/<hash>.sqlite`(+wal/shm) 与 `metadata.sqlite`（与 P0.7 观察一致）。
- **实现**：
  - `workerd/do-runtime/host.js`：`POST /v1/do/invoke {namespace,worker,bundle_sha,class,id,request?}`（**internal 角色令牌**鉴权，常量时间）→ 按 `(ns,worker,class)` 取 host DO → host actor 拉取租户不可变 bundle（cell-agent `/v1/internal/bundle`）→ facet 调租户 DO 并回传响应；`/healthz`。租户 loaded worker `globalOutbound` = **public-only**（I-09），平台 fetchBundle 走 host 的 private outbound。
  - `internal/doruntime`：`Render`（capnp：`durableObjectNamespaces=[Host]`、`durableObjectStorage=(localDisk="do-disk")`、disk/public-network/private-outbound 服务、`HOST`/`LOADER`/`CELL_URL`/`CELL_TOKEN`/`OUTBOUND` 绑定）+ `Run`；`internal/workerdbin` 抽出 `FindWorkerd`（userruntime 共用）。
  - `cmd/do-runtime`：`CELLHIVE_CELL_URL`/`CELLHIVE_TOKEN_INTERNAL`/`CELLHIVE_DO_ADDR`(默认 `:8788`)/`CELLHIVE_DO_DISK`；Makefile 出 `bin/do-runtime`。
- **验证**：`internal/doruntime` 真实 workerd e2e——从 bundle 加载租户 DO 类并作为 facet 运行，**两次 invoke 返回 `tenant-do:1`/`tenant-do:2`（跨调用 SQLite 持久）**，无 token→401，磁盘生成 `.sqlite`。
- **边界（P3 后续，未做）**：Go supervisor / WAL 捕获与**输出门**（ADR-051）、DO **claim/冷激活与分页懒加载**、**alarm** 提取→waker、**WebSocket**、in-flight 迁移、跨节点复制/路由。本 ADR 只到"能原生跑租户 DO 且存储持久"；专用 `do` 角色令牌亦留后续（当前用 internal 角色）。

---

## ADR-078 DO 归属/栅栏/排空/驻留 ✅实现

> 状态：**已实现并验证（2026-09-15，真实 workerd）**。在 ADR-077 骨架上加单写者归属与稳定机制，用 **owner record + bucket 条件写**做 owner/lease。

- **设计取舍**：
  | 对照做法 | CellHive |
  |---|---|
  | 外部存储 owner lease + **monotonic generation** | `owner.Manager`（bucket 条件写）+ **`owner-gen` 单调计数器**（新增 `ClaimAs/RenewAs/ReleaseAs`）|
  | class 分 N 个 host actor shard；scope=storageId:class:shardN | 同：`hostId = ns/worker/class/shardN`、`shard = fnv1a(ns/worker/class/objectName) % 16`；scope=`<ns>/__do__/<worker>~<class>~shardN` |
  | pre-dispatch 校验 task/generation/lease + guard，必要时同 epoch renew，否则 fail-closed | 同：本地缓存 epoch/expiry，剩余 < `GUARD_MS(3s)` 先 renew，失败删本地并 `owner_unavailable` |
  | 本地 `127.0.0.1` drain/renew；**drain 后直接 kill workerd** | `cmd/do-runtime` 每 10s 本地 `/v1/do/renew`；SIGTERM → `/v1/do/drain` → 退出（kill workerd，不依赖 graceful window） |
  | `DO_PREVENT_EVICTION` 切 resident/evictable 两份配置，非法值启动失败 | `CELLHIVE_DO_PREVENT_EVICTION`（仅 `true`/`false`，默认 true；其它值 `Render` 报错）→ capnp `preventEviction` 有无 |
  | `getDurableObjectClass(name, { props })`；`facets.abort()` 绝不 delete | 同：传固定 `props{namespace,worker,class,shard}`（`facets.abort()` 备用） |
- **实现**：`internal/owner/as.go`（`ClaimAs/RenewAs/ReleaseAs` + `owner-gen` 单调栅栏）；`internal/server/do_owner.go` + 路由 `POST /v1/internal/do/{claim,renew,release}`（internal 角色令牌；claim 返回 epoch/expiry，`owner_live`→409、epoch 不符→409 `owner_epoch`）；`workerd/do-runtime/host.js`（shard、claim/guard/renew、`/v1/do/drain`、`/v1/do/renew`、props）；`internal/doruntime`（`NODE_ID`/`ADVERTISE` 绑定 + residency 模板）；`cmd/do-runtime`（renew 循环 + drain-on-shutdown + 环境校验）。
- **验证**：`internal/owner` 单测（epoch 递增、**释放后 epoch 不复用=2**、过期可抢占=3、错 epoch 拒绝）、`internal/server` DO owner 端点（claim/owner_live/renew epoch/release/错令牌 401）、`internal/doruntime` 真实 workerd e2e（首调 claim→`tenant-do:1`、复用 lease 再调→`:2` 且不重复 claim、外来 owner→409 `owner_unavailable`、drain→释放+后续 503、无令牌 401、resident/evictable capnp 选择正确）。
- **实测发现（重要）**：pinned workerd 2026-06-15 上 facet 的 `ctx.storage.setAlarm()` **抛** `alarms are not yet implemented for SQLite-backed Durable Objects` → **alarm 必须 shim**，单列 **P3.3**：shim `setAlarm/getAlarm/deleteAlarm` 写 DO SQLite + 上报现有统一 timer(`KindDOAlarm`)，waker 派发 alarm 到 do-runtime。
- **边界（未做，P3 后续）**：alarm shim（P3.3）；WebSocket/connect；in-flight 迁移 `result_unknown`；跨节点 owner forwarding/hint 缓存；专用 `do` 角色令牌（仍用 internal）；`owner-gen` 与 owner 写入非同一原子操作（并发抢占可能跳号，但**单调不复用**，安全）。

---

## ADR-079 DO alarm shim（facet 原生 alarm 不可用→平台 shim；统一 timer 派发）✅实现

> 状态：**已实现并验证（2026-09-15，真实 workerd）**。alarm 需要 shim；复用 CellHive 统一 timer + waker，**不引入外部工作流/租约存储**。

- **实测依据**：pinned workerd **2026-06-15** 报 `alarms are not yet implemented for SQLite-backed Durable Objects`；最新 **2026-09-15** 报 `Facets currently cannot set alarms` → **facet 原生 alarm 在任何版本都不可用**（我们因 ADR-053 必须用 facet 动态加载租户类）→ 必须平台 shim。
- **决策/实现**：
  1. **平台基类** `workerd/do-runtime/cellhive-do.js`：`class DurableObject extends <native>`，构造时把 `ctx.storage.setAlarm/getAlarm/deleteAlarm` shim 为写/读/删该 DO **自身 storage 的保留键 `__cellhive_alarm`**（KV 语义、随对象持久、单 alarm/对象、`setAlarm` 覆盖）； `Number.isFinite` 校验。暴露平台 RPC `__chAlarmState()`（`{dueMs|null}`）与 `__chRunAlarm()`（调租户 `alarm()`）。
  2. **注入方式**：do-runtime host 加载租户 bundle 时把说明符 `"cloudflare:workers"`/`'cloudflare:workers'` **改写为 `"cellhive-do.js"`**，并把该模块注入 workerLoader modules（其余经 `export *` 透传）。
  3. **上报**：host 在每次普通 invoke 与 `kind:"alarm"` invoke 之后读 `__chAlarmState()` 并 POST cell-agent `/v1/internal/do/alarm/upsert`（`due_ms<=0` = 清除；best-effort，下次再报）。
  4. **调度**：cell-agent 把 alarm 记为统一 `KindDOAlarm` 定时器（scope `ns/__timer__/do`、occurrence=`<worker>|<class>|<shard>|<id>`；先 `RemoveByOccurrence` 再 upsert 保证"单 alarm"），注册 scope 给 timer runner。`timer.Store.RemoveByOccurrence` 新增。
  5. **派发**：`internal/dispatch.DoAlarmDispatcher`（装饰器）对 `KindDOAlarm` 用 `owner.DOScope` resolve 出 do-runtime 的 `Address`，POST `<address>/v1/do/invoke {kind:"alarm"...}`（bundle 由 `Projection.ActiveBundle` 解析）；其它 kind 透传给 user-runtime dispatcher。`cmd/cell-agent` 接线；`cmd/do-runtime` 的 ADVERTISE 默认取本机地址。
- **语义**：**at-least-once**（timer 派发成功才标记 fired；上报 best-effort）。`getAlarm/deleteAlarm` 反映存储状态。
- **验证**：`internal/dispatch` 单测（DO alarm 路由到 do-runtime 且 body/token 正确；其它 kind 透传；resolve 失败报错）；`internal/server` 端点（upsert→timer 数 1；due_ms<=0→0；错令牌 401）；**真实 workerd e2e**（`TestDoRuntimeAlarmShim`）：`/arm`→上报正 due、`kind:"alarm"`→租户 `alarm()` 执行并在其中重排→上报更晚 due、`/disarm`→上报 0、`getAlarm()`→`null`。
- **CF 差异/已知边界（诚实）**：① 不实现 CF native alarm 的重试计数/指标；② **`transactionSync()` 内调用 setAlarm/deleteAlarm 不受支持**（sync 事务不能 flush 异步副作用）；③ 无 per-alarm lease 的重叠防护（依赖 timer 的 at-least-once 与派发成功才 fired）；④ shim 依赖"说明符改写"，对**非 bundle 化**的自定义模块加载形态未覆盖；⑤ `deleteAll()` 与 alarm 的交互未定义；⑥ WebSocket/in-flight/跨节点 forwarding 仍未做。

---

## ADR-080 DO 协议端到端：客户端绑定、放置/激活、owner 转发/hint、result_unknown、WebSocket ✅实现

> 状态：**已实现并验证（2026-09-15，真实 workerd）**。使租户代码能通过绑定真正调用 DO，并补齐跨节点归属/结果未知/WebSocket（CF 兼容重启）。

- **1. 客户端绑定 + 放置/激活（C-01）**
  - `cell.DOShard`：Go 侧 FNV-1a（`Math.imul` 等价）与 do-runtime JS **字节一致**，`shard = fnv1a(ns/worker/class/objectName) % 16`；有**跨语言向量单测**。
  - cell-agent `POST /v1/do/invoke`（**tenant-facing，`scopeAuth("do")`**）：按 shard 从 `CELLHIVE_DO_RUNTIMES` 确定性选一个 do-runtime 并转发（失败轮换下一个，全失败 503）。
  - 客户端：`facades.js` 增 `makeDO`（`env.DO.get(env.DO.idFromName(n)).fetch(...)`，文本 body）；`loader.bindingSpec` 支持 `do`（kind=do、class=`Binding.ID`、worker/`bundle_sha` 平台固定、token = 本地 HMAC `kind="do"`）。
  - 真实 workerd e2e：租户 `env.ROOM.get(...).fetch(...)` → 代理（断言 class/worker/bundle/id/body 与 `do` scope-token 验签）。
- **2. 跨节点 owner 转发 / hint**：`do/claim` 的 409 `owner_live` 带 **owner `address``**；do-runtime 冲突时**最多转发一次**（预派发安全，幂等/非幂等皆可）并缓存 **hint**（TTL 30s，命中不再 claim）。单测/真实 e2e：转发目标收到请求、hint 命中时 `claims==0`。
- **3. `result_unknown`**：**预派发**冲突 → `owner_unavailable`（安全可重试）；**转发 transport 失败**或**派发后失去归属** → **409 `result_unknown`**（结果未知，不得盲目重放）。e2e：owner address 不可达 → 409 `result_unknown`。
- **4. WebSocket（CF 兼容）**：`GET /v1/do/connect`（Upgrade）→ host actor facet 原样 `fetch`（101 保留）；hibernation API（`ctx.acceptWebSocket`）；`POST /v1/do/abort`（及 drain/重启路径）先 `__chCloseAll(1012)` 再 `facets.abort()` → 客户端收到 **1012**，`facets.delete()` 绝不使用（保 SQLite）。**真实 e2e（bun WS 客户端）**：echo 成功 → abort → `CLOSE:1012`。
- **验证**：`go test ./...` **34 包全绿**；`internal/userruntime`（DO binding）、`internal/doruntime`（转发/result_unknown/WS 1012）、`internal/server`（代理）、`internal/cell`（shard 跨语言向量）全 PASS；`make build` OK。
- **CF 差异/边界**：WS **跨节点转发未做**（owner_elsewhere → 503，文档）；`env.DO` 仅 `fetch`（无 RPC 方法）；DO 请求体为文本（非任意 structured clone）；`deleteAll()` 交互未定义。

---

## ADR-081 DO 存储生命周期：版本惰性重启、migrations 校验、显式 doStorageId ✅实现

> 状态：**已实现并验证（2026-09-15，真实 workerd）**。回答"class 代码变了怎么办 / 要不要 migration"。

- **1. 版本变更的 facet 级惰性重启**
  - `host.js`：bundle 缓存改 **per-version**（`Map<loaderId, source>`）；host actor 记 `facetVersion: Map<facetId, bundle_sha>`（facet id = `<class>/<objectName>`）。
  - dispatch（fetch/alarm/connect）时若**已构建版本 ≠ 当前 `bundle_sha`** → 仅对该 facet `facets.abort()`（**绝不 `facets.delete()`**，SQLite 保留）→ 下次 `get()` 用当前 bundle 重建。**未访问的对象不受影响**；其他类/租户/进程不受影响（facet 级）。
  - 语义澄清：DO 的"重启" = **丢弃内存实例、下次请求按当前代码重建**，storage 不动；不是"给运行中的对象切 bundle"。因为 live 实例的 class 无法热替换，部署要让新代码生效**必须**重启实例（CF 亦然）。
  - **真实 e2e** `TestDoRuntimeVersionRestart`：同一对象先 `sha1`→`v1:1`，再 `sha2`→**`v2:2`**（代码切换且**计数延续**），再 `sha1`→`v1:3`（storage 持续）。
- **2. DO migrations 校验（deploy 服务端，权威）**
  - `wranglercompat.ValidateMigrations`：**允许** `tag`/`new_classes`/`new_sqlite_classes`；**拒绝** `renamed_classes`/`deleted_classes`/`transferred_classes`（`unsupported_migration`，它们改变存储身份）；未知字段 → `unknown_migration_field`。
  - `deployReq.migrations` 在 deploy 时校验，失败 → 400 `deploy_rejected` + findings；CLI `cellhive deploy --migrations '<json>'`。
  - 单测：允许/拒绝/未知字段；server：带 `renamed_classes` 的 deploy 被拒且码正确。
- **3. 显式 `doStorageId`（稳定存储身份）**
  - `control.Store.DOStorageID(ns, worker)`：在 app cell 的 `do-storage/<worker>` **首次分配并冻结**（随机 `ds_…`，cell 事务内读改写），随 `Version.StorageID` 进入投影。
  - loader `do` binding spec 带 `storage_id`；facade 传到代理；`host.js` 的 hostId 改为 **`<storage_id>/shardN`**（不再含 worker/class），facet id = `<class>/<objectName>`。
  - 效果：**重部署（新 bundle）→ 存储身份不变、状态延续**；为将来 rename/transfer 迁移预留了显式 identity。
  - 单测：`DOStorageID` 幂等/唯一/跨版本稳定（v1/v2 同 id）；e2e（userruntime）：`do` 调用体带 `storage_id`。
- **验证**：`go test ./...` **34 包全绿**；`internal/doruntime`（含版本重启 + WS 1012）、`internal/control`、`internal/wranglercompat`、`internal/server`、`internal/userruntime` 全 PASS；`make build` OK。
- **边界（P3.6）**：Go supervisor/WAL 捕获与**输出门**（ADR-051）、**冷激活分页懒加载**（ADR-037/055/056）；class **rename/transfer 迁移仍拒绝**（故改名 = 新存储身份，不保数据）；do-runtime **进程级**事件（drain/崩溃）仍影响该节点全部对象。

---

## ADR-082 DO migrations v2：对象注册表、rename（别名保数据）、delete（标记+物理回收）、主动重启 ✅实现

> 状态：**已实现并验证（2026-09-15，真实 workerd）**。修订 ADR-081 的 migrations 矩阵（rename/delete 由拒绝改为支持；transfer 仍拒绝）。

- **背景（侦察）**：facet 存储是 `<hosthash>.<n>.sqlite` + `<hosthash>.facets`（facet 名→编号）+ `metadata.sqlite` → **枚举对象必须靠注册表**，它同时是 delete、主动重启、以及后续 supervisor/输出门与分页的前提。
- **1. 对象注册表**：do-runtime 主 worker 在每次 dispatch 时登记已见对象；`GET /v1/do/objects`（internal）返回；cell-agent `GET /v1/internal/do/objects` **扇出聚合**所有 do-runtime（冷路径，不新增持久化）。
- **2. rename（`renamed_classes`）**：控制面在 app cell 存**冻结别名 `codeClass → storageClass`**（`do-class/<worker>/<class>`；目标已存在则拒绝；源别名删除→名字复用从新存储开始）。投影暴露 `class_storage`；loader 的 `do` spec 带 `storage_class`；facade/代理透传；**host 用 `<storage_class>/<objectName>` 作为 facet 存储键，且 shard 也按 storage_class 计算** → 改名**不迁文件、数据保留**。e2e：class `A`→计数 1，改名 `A→B` 后 class `B` 仍为 **2**（延续），真正新类 `C` 从 **1** 开始。
- **3. delete（`deleted_classes`）**：控制面标记 `do-deleted/<worker>/<class>`；投影 `deleted_classes`；loader 标记绑定为删除态，facade 使用时报错；do-runtime `POST /v1/do/delete` → host `facets.delete(facetId)` **物理回收**（`facets.delete` 仅用于删除路径；正常重启仍只用 `abort`）。测试：delete 后再调用回到计数 **1**（存储已清）。
- **4. 主动重启**：do-runtime `POST /v1/do/restart {storage_id}` 对注册表内该 storage id 的对象 `facets.abort()`；cell-agent 在 deploy 成功后按 `CELLHIVE_DO_EAGER_RESTART`（默认 **false**=惰性）调用各 do-runtime。默认仍是 ADR-081 的按需惰性重启。
- **验证**：`go test ./...` **34 包全绿**；`internal/control`（别名/删除/投影）、`internal/doruntime`（rename 保数据、注册表+delete+restart）、`internal/server`（migrations 拦截、objects 聚合）、`internal/wranglercompat`（矩阵）全 PASS；`make build` OK。
- **migrations 矩阵（更新）**：`tag`/`new_classes`/`new_sqlite_classes`/`renamed_classes`/`deleted_classes` = ✅；`transferred_classes` = ❌ `unsupported_migration`；未知字段 = ❌ `unknown_migration_field`；形状错误 = ❌ `invalid_migration`。
- **边界（P3.7）**：注册表为**进程本地**（do-runtime 重启后重建；cell-agent 侧为扇出聚合、非持久化）；`transferred_classes` 仍拒绝；Go supervisor/WAL **输出门**（ADR-051）与**冷激活分页懒加载**（依赖本注册表 + facet 多文件布局）；进程级接管恢复、WS 跨节点转发。

---

## ADR-083 do-supervisor：facet 存储捕获 + 输出门（RPO=0 第一步）✅实现（部分）

> 状态：**已实现并验证（2026-09-15，真实 workerd）**：shard 粒度的存储捕获 + 输出门。**per-object 粒度、跨节点冷激活/分页、进程级接管仍属 P3.8（未做）**。

- **关键发现（决定做法）**：facet 的映射文件 `<hosthash>.facets` 是 workerd **内部二进制格式**（魔数 `57efb0c5…` + uint16 长度 + 名字），**未文档化且随版本可变** → **不解析它**。
- **决策**：以 **host actor（shard）目录为捕获/恢复/门控单位**，同步其下**所有 `*.sqlite`**（host actor + 各 facet + metadata）；scope 由相对路径派生。好处：避开内部格式，且与既有 owner/shard 归属对齐。代价：比 per-object 粗。
- **实现**：
  - `internal/dosupervisor`：遍历 data dir 的 `*.sqlite`，逐文件 `wal.Cursor` 轮询 + `cellstore.ReadDBPages` 全页快照 + `ltx` WAL2 分页增量，经 `sqlcapture.HTTPCommitter`（fleet/bucket proof）提交；`POST /sync-all`（每文件**基线快照** + 后续增量；**失败返回 500，门不得放行**）、`GET /status`。
  - `cmd/do-supervisor`（`-dir/-listen/-owner/-follower/-token/-scope-prefix`）。
  - **输出门**：`internal/doruntime` 增 `GateURL`（capnp `GATE_URL`）；`host.js` 在 facet 操作后、返回前 `await /sync-all`；失败/异常 → **409 `result_unknown`**（不静默 ack）。
- **验证**：`internal/dosupervisor` 单测（两个 `*.sqlite` 均被捕获：2 claims、≥2 commits；二次幂等）；**真实 workerd e2e `TestDoRuntimeOutputGate`**：门正常 → invoke 200 且 captured commits>0；门 500 → **409 `result_unknown`**。
- **边界（P3.8）**：~~**per-object 粒度**~~、~~**跨节点冷激活 + 分页懒加载**~~、~~**进程级接管恢复**~~、~~**DO 兼容套件**~~ —— **以上已由 ADR-084（19 项兼容套件）与 ADR-056/079/080 完成**；本 ADR 的 shard 粒度门控与捕获仍是基础。

---

## ADR-084 跨节点冷激活 + 对象寻址 + 按需分页 + 接管 ✅

> 状态：**已实现并验证（2026-09-15，真实 workerd；含按对象冷启动 e2e）**。

- **关键发现（解锁对象键）**：workerd host-actor 的文件名 `<hosthash>` **可从 DO 内部确定得到**——`this.ctx.id.toString()` 就是该 64-hex 前缀，`this.ctx.id.name` 就是传给 `idFromName()` 的 hostId（`<storage_id || ns/worker>/shardN`，含 storage_id）。实测 `/tmp/opencode/bind-probe`：actor 返回 `id=b6d269a5…`，磁盘即 `b6d269a5….sqlite` / `b6d269a5….1.sqlite` / `b6d269a5….facets`。因此 `{storage_id, class, objectName} ↔ <hosthash>.<n>.sqlite` **可推导，无需目录 diff、无竞态**。
- **绑定上报**：`host.js` 两类上报（join on `host_id`，best-effort，仅冷路径一次）——顶层 router 报 `{host_id, storage_id, class}`；`Host` actor 在 `fetch` 首行报 `{host_id, host_hash}`（`ctx.id`）。supervisor 新增 `POST /internal/do/bind` 合并。
- **对象键（A 已解）**：facet 文件 `<hosthash>.<n>.sqlite` 的复制 scope 改为 **对象寻址** `workerd/<class>/<base64url(storage_id/class/objectName)>`；host actor / metadata / sidecar 仍按可逆 relpath scope。manifest v2 记 `hosts`（host_id→binding）与 `objects`（`{rel, host_hash, n, name, storage_id, class, scope}`）。对象名来自 `.facets`（n↔name）。
- **按对象恢复（B 已解）**：`Supervisor.RestoreObject(ctx, outDir, storage_id, class, name)` 只恢复该对象的 facet 文件 + 同 host 的 host-actor sqlite + `.facets`（保证 workerd 以原编号重新附着），读页 `PageFetcher.Materialize`（每页 ranged 读）。`RestoreAll` 仍可用；`cmd/do-supervisor -restore-object storage_id/class/name`。
- **按需分页**：`CompactAll` 折叠出 page index；bucket 路径（`NewPageFetcher.Materialize`）与 agent 路径（`/v1/internal/segment` 支持 `Range` 206 + `HTTPStore.PageFetcher`）均逐页读。
- **无桶凭据恢复**：`HTTPStore` 经 cell-agent `/segments`+`/segment`(Range)+`/blob`；cell-agent 持唯一桶凭据。
- **统一对象存储原语**：新增 `objectstore.Objects`（`internal/objectstore`）——**带前缀白名单的复用型 bucket 视图**（`Get/Put/Delete/PresignGet/List`，key 必须在前缀下、拒绝穿越/绝对/反斜杠）。supervisor blob 句柄与其 handler 走它；bundle/asset（`internal/artifacts`）的读写/presign 也已改走它。
- **保留前缀注册表 + 属主守卫**：顶层前缀 `cells/`(replica)、`nodes/`(node-log)、`fleet/`(waker)、`bundles/`+`assets/`(artifacts)、`dosupervisor/`(do-supervisor) 由 `objectstore` 集中登记，各属一个子系统。保留前缀必须 `NewOwned(b, prefix, owner)` 由属主打开，泛用 `NewObjects` 直接拒绝（错误对象使所有操作失败）。**机制是白名单（默认拒绝）**：blob 只认 `dosupervisor/`，段端点只认 `cells/`（`replica.ValidateKey`），互不越界。测试：`TestReservedPrefixGuard`、`TestReservedPrefixesDisjointAndOwned`、`TestInternalBlobRoundTrip`（`cells/evil`→400）、`TestObjectsPrefixScopeAndSafety`。
- **跨节点冷激活/接管**：host 文件名跨进程确定性 → 恢复到原路径即重新附着；`cmd/do-supervisor [-workerd/-config]` 作 PID 1 spawn/监督 workerd；崩溃= `exec.CommandContext` SIGKILL workerd，owner 过期 → 新节点 claim + 恢复。
- **验证**：
  - 单测：`TestParseFacets`、`TestCaptureRestoreRoundTrip`、`TestPagedRestoreUsesRangedReads`、`TestObjectScopesAndRestoreObject`（对象 scope + 按对象恢复）、`TestRestoreViaAgentNoBucketCreds`、`TestAgentPagedRestoreUsesRangedReads`。
  - 真实 workerd：`TestDoRuntimeCrossNodeColdActivation`、`TestDoRuntimePerObjectColdStart`（A 计数 1 → 只恢复该对象到 B → 计数 2）、`TestDoRuntimeTakeoverAfterCrash`、`TestDOCompatSuite`（12 子测试）。
  - cell-agent：`TestInternalBlobRoundTrip`、`TestReadSegmentRange`。
- **WS 跨节点转发**：`proxyConnect` 把升级 socket 代理到 owner do-runtime（`WebSocketPair` 双向透传；1012 透传，不 resume）；`TestDoRuntimeWebSocketCrossNodeForward`。
- **边界**：hosthash 依赖 runtime 的 `uniqueKey` + pinned workerd 版本（当前固定 `cellhive-do-host` / 2026-06-15）；`.facets` 解析是 best-effort，失败回退 relpath scope。`transferred_classes` 未做；运行期 VFS 懒读在 **do-runtime（workerd）侧**不可行（ADR-085），在 **cell-agent 侧**已由 ADR-160 实现（`internal/pagedvfs`）。

---

## ADR-085 运行期 SQLite VFS 懒读：不可行边界与替代 ✅（边界定稿）

> 状态：**记录边界（不可实现于 stock workerd）**，非功能缺口。

- **需求**：DO 运行中按需 fault 页，而非冷启动整文件 materialize。
- **为什么不可行（**仅 DO/workerd 侧**）**：workerd 用**自己的 VFS 同步读**本地 SQLite 文件，而**同步 fault 在 DO 事件循环里不可控**（不能在 isolate 线程里阻塞等网络回源）。要"按需 fault"必须满足其一：
  1. 在 do-runtime 磁盘前挂一层**懒加载文件系统**（FUSE），页缺失时同步回源 —— 需引入 FUSE 库（违反"不新增外部依赖"），且同步 fault 在 DO 事件循环里不可控；
  2. workerd 暴露 VFS/懒读钩子 —— 违反"不改 stock workerd"。
  两者都被约束排除。半实现（稀疏文件 + 空洞零填充）会**静默损坏数据**（SQLite 读到 0 页），不可接受。
  > 注意：**"同步"本身不是 cell-agent 侧的障碍**——cell-agent 是普通 Go 进程（每 cell 单写者、可阻塞 I/O），同步回源不会卡事件循环；它的问题是实现与 p99/成本（见下方修订）。
- **已交付的替代（等价收益的降级）**：
  - **冷启动按对象 + 按页 materialize**：`RestoreObject` 只取该对象的 SQLite；`replica.PageFetcher.Materialize` **每页一次 ranged 读**（bucket 直连或经 cell-agent `/segment` Range），不整取 L1 对象、不整 shard、不整库。
  - 运行期读写落在本地盘（workerd 直接读写），**写**经 WAL 捕获复制、**读**本地无网络。
  - 因此网络成本集中在**冷启动一次**，且已按对象/页最小化；稳态运行无远程读。
- **后续可选（未做，需架构决策）**：若必须进一步降低超大对象的冷启动延迟，可评估 FUSE 懒读作为**可选部署组件**（显式引入依赖 + 独立 ADR），或在放置时优先把热对象常驻（`DO_PREVENT_EVICTION`，ADR-078）。
- **修订（ADR-159）**：SQLite 换成 CGo（mattn）后，**cell-agent 侧**的 cell 理论上可以用自定义 C VFS（`sqlite3_vfs_register`）做运行期懒读——不再是"驱动不可能"，而是**当时未实现——已由 ADR-160 实现**（`internal/pagedvfs`）；且无 DO 的同步/事件循环约束，阻塞回源是允许的。设计前提（供追溯）：
  1. **捕获/快照必须一起走 VFS**：`internal/wal` 与 `cellstore.ReadDBPages` 用 `os.ReadFile` 直接读文件，页未真正物化时会读到零页 → 把静默损坏写进 LTX；必须在 checkpoint/快照前强制物化，或让它们也用同一 VFS。
  2. **p99 成本**：写事务缺页会同步阻塞一次远端 ranged GET（点查，不违反禁 List），单写者下延迟直接进写路径；因此"懒读"适合**冷启动/工作集**，稳态仍应把页留在本地（cellstore LRU/驱逐）。
  3. **不完整实现 = 损坏**：任何"稀疏文件 + 补零"的取巧都不可接受（同 DO 侧）。
  **DO/workerd 侧不变**（同步 + 事件循环 → 仍需 FUSE 或改 workerd）。**cell-agent 侧已由 ADR-160 实现运行期 VFS 懒读**（按页冷启动）。
- **参照技术（paged VFS 模型）**：用 `sqlite3_vfs` 包装 base VFS——主库是**按 pinned cut 尺寸的稀疏文件**，`xOpen` 定尺寸、`xRead` 缺页时从 bucket 取该页 **run** 写入本地、`xWrite`/`xTruncate` 维护 **hydration set**、fault 同步在调用线程、失败→`SQLITE_IOERR_READ`；页图由 LTX 段页索引构建，读的是"one replica object 的 ranged read"（对应我们的 `replica.PageFetcher` + `L1/index.bin`）。门槛：链 ≥ 256MiB 才分页，小链整克隆；paged 文件**只当缓存**（不做 eviction/handoff 快照），paged epoch 用 marker 对象接续链，混合版本需 gate 保护；激活后按 16 MB/s 后台补齐。教训：run 必须有**解码后字节上限**（否则一个"run"可拉完整库）、内部分支页单独取（避免 overflow 链，`count(*)` 曾 fault 700MB）、连续扫描才窗口预取、关键 `IOERR/FULL/NOMEM/INTERRUPT` 销毁事务时**poison actor**（fail closed）。

---

## ADR-086 Workflows 自研引擎（Partial）✅

> 状态：**已实现并验证（真实 workerd e2e）**。支持子集见 `compatibility-matrix.md`（Partial）。

- **关键实测**：pinned workerd **导出** `WorkflowEntrypoint`，但**无法在其引擎外构造**（`new W(ctx,env)` 报 `constructor parameter 1 is not of type 'ExecutionContext'`，用真实 `WorkerEntrypoint.ctx` 亦然）→ 不能用原生 Workflows。
- **做法（shim）**：user-runtime 加载租户 bundle 做 workflow run 时把 `cloudflare:workers` 重写到 `cellhive-workflow.js`（提供可构造的 `WorkflowEntrypoint` base，其余 re-export），平台 wrapper 自行 `new cls(ctx, env)` 并调 `run(event, step)`——与 DO alarm/deleteAll shim 同法。
- **存储**：`internal/workflow`（`__workflow__` cell）：instances（status/output/error）、steps（按 `(instance, name)` **记忆化**）、events。
- **step API**：`step.do`（记忆化，result 经 JSON+base64 存 cell）、`step.sleep/sleepUntil`（写 wake_at → 复用统一 timer `KindWorkflowSleep` → 到点由 `WorkflowDispatcher` 重新派发；早于 wake 再次 `SleepSignal` 停住）。
- **cell-agent API**：租户 `POST /v1/workflow/create|event`、`GET /v1/workflow/get`（scopeAuth `workflow`）；内部 `GET/PUT /v1/internal/workflow/step`、`POST /v1/internal/workflow/sleep|finish`。**回调经 `PLATFORM` service binding**（不放宽租户 `globalOutbound`）。
- **控制面**：`cellhive workflow create <ns> <name>`（kind=workflow 资源）；`wranglercompat` 要求 workflow 绑定带 `class_name`（否则 `invalid_binding`）；`Projection.WorkflowTargets()` 解析 worker+class+bundle。
- **验证**：`internal/workflow` 单测；`internal/server TestWorkflowEndpoints`；`internal/dispatch TestWorkflowDispatcherRunAndSleepTimer`；**真实 workerd `TestUserRuntimeRunsWorkflow`**（`step.do` 记忆化 → 输出 `A:p:B`；`sleep` 首跑 parked → 重派发后 complete）；`wranglercompat TestWorkflowBindingRequiresClassName`。
- **Partial 边界**：`pause/resume/terminate/restart`、实例列举 **✅ 已实现**（`internal/workflow` status 迁移 + `Restart` 清记忆化步骤；cell-agent `/v1/workflow/{pause,resume,terminate,restart,list}`；facade `env.WF.get(id)` 返回 CF 形态实例对象 `status()/pause()/resume()/terminate()/restart()/sendEvent()`，另 `list()`）。验证：`TestLifecycleAndRestart`、`TestWorkflowEndpoints`（lifecycle/list/restart 清步骤）、`TestUserRuntimeWorkflowLifecycleFacade`（真实 workerd `wf:queued/paused`）。**`retry`/backoff ✅**：`step.do(name, {retries:{limit,delay,backoff}}, fn)`——`limit` 次重试，`delay`(秒)+`backoff`(constant/linear/exponential)，超过后抛出；验证 `TestUserRuntimeWorkflowStepRetries`（失败两次后第三次成功 → `ok:3`）。
- **`waitForEvent` ✅**：`step.waitForEvent(name,{type,timeout})`——`events` 增加 `consumed` 列 + `waits` 表；内部 `/v1/internal/workflow/event/consume` 与 `/wait`(GET/POST/DELETE)；事件到达经 `sendEvent`→重派发恢复，超时经 sleep timer 恢复并返回 `undefined`；验证 `TestUserRuntimeWorkflowWaitForEvent`（park→投递事件→`got:E`）。
- **生产级补齐（本次）**：
  - **run lease/generation（并发安全）**：实例增 `run_token/lease_until_ms/generation`；派发前 `ClaimRun`（有活租约/已暂停/终态 → 跳过）；所有 run 回调带 `run` 令牌并被 **fence**（陈旧 409）+ 顺带续租；parked/finish/pause/terminate 释放租约。验证 `TestRunLeaseAndFencing`、`TestWorkflowRunLeasePreventsDoubleDispatch`。
  - **协作式 pause/terminate**：每个 step 边界前查 `/v1/internal/workflow/state`——被 fence（409）或 `paused/terminated/complete` → `StopSignal` 停止推进（不 finish）。验证 `TestUserRuntimeWorkflowCooperativePause`（paused → `stopped`，steps/finished=0）。
  - **持久重试（跨崩溃）**：`step_attempts` 表存 `attempts/last_error`；失败不 inline sleep，而是记 attempt + 排 timer 重派发（park→wake 续跑同 attempt/backoff）；超 `limit` → errored。验证 `TestUserRuntimeWorkflowStepRetries`（跨重派发到 `ok:3`）。
  - **retention/TTL**：`CELLHIVE_WORKFLOW_RETENTION`（0=关）；cell-agent 周期 `Prune` 终态旧实例并级联 steps/attempts/events/waits。验证 `TestPruneTerminalOlderThanRetention`。
  - **NonRetryableError**：`cellhive-workflow.js` 导出；重写 `cloudflare:workflows` 导入；`step.do` 遇该错**不重试**。验证 `TestUserRuntimeWorkflowNonRetryable`（`errored`）。
- **仍未做（Partial 边界）**：`delete()`（实例删除 API）、`locationHint`、跨 worker、`createBatch`、step 历史列举/progress 回调、payload 字节预算（与 `compatibility-matrix.md` 一致；跨 worker 不做）。

---

## ADR-087 P4：发布日志 + 幂等部署 + Admission + Autoscaler ✅

- **发布日志**：`control.Store.Releases(ns, worker)` 列出该 worker 全部版本（newest first，含 `active`）；`GET /v1/control/releases`（admin）；CLI `cellhive releases`。
- **幂等部署**：`DeploySpec.IdempotencyKey`（`POST /v1/control/deploy` 的 `idempotency_key`）——同 key 重复部署返回首次创建的版本（`deploy-idem/<worker>/<key>`），不新分配；CLI `--idempotency-key`。测试 `TestDeployIdempotencyAndReleases`。
- **Admission（ADR-035 落地）**：`internal/admission` 每命名空间 token bucket（`CELLHIVE_NS_RPS`/`CELLHIVE_NS_BURST`，0=关）；`admit` 中间件包住**所有租户写端点**（kv put/delete、d1 query/exec/batch、r2 put/delete、queue send/claim/ack/retry、workflow create/event、do invoke），超额 → **429 `rate_limited`**；`Shed/Allowed` 计数供诊断。测试 `TestLimiterBurstAndRefill`、`TestDisabledLimiter`、`TestAdmissionRateLimit`。
- **Autoscaler（信号，非编排）**：`internal/autoscaler.Advisor` 读节点 lease（`lease.Sample` → 每节点 `Load{OwnedCells,Pressured,ShedCells}`），按 `CELLHIVE_CELLS_PER_NODE` 计算 `desired = ceil(owned/target)`，pressure/shed 至少 +1，`[MIN,MAX]` 夹取；输出 `{action: scale-up|scale-down|hold, reason}`。cell-agent 现在**真实上报** `OwnedCells/ResidentCells`（来自 `owner.Manager.OwnedScopes`）。`GET /v1/control/capacity`（admin）+ CLI `cellhive capacity`。测试 `TestAdvisorScalesUpForLoadAndPressure`。
  - **"驱动"落地**：`autoscaler.Actuator` 接口 + `Advisor.Run(interval, actuator)` 循环——`action != hold` 时调用 actuator。默认 `LogActuator` 只记日志；真实部署在 `cmd/cell-agent` 注入自己的 orchestrator（云 SDK 不进核心）。测试 `TestAdvisorRunAppliesPlan`。
  - **边界**：真正的增删节点属外部编排（本仓库无云驱动）；核心只提供信号 + 可插拔 actuator，不引入外部依赖。真实扩缩容验证需环境（C 类）。
- **CLI/admin**：CLI 增补 `status`/`capacity`/`releases`/`workflow create`，命令面覆盖全部 admin 端点；**admin 后台 = `GET /admin` 单页控制台**（stdlib 内联 HTML，调用 admin JSON API；ADR-036 单一控制面、无租户门户）——不引入前端依赖。

---

## ADR-088 P5：协议版本化（reader-before-writer）+ 诊断增强 + 回归守卫 ✅

- **协议版本**：`cell.ProtoVersion = "v1"` + `SupportedProtoVersions` / `SupportedProtoVersion(v)`（`""`=v1）。**reader-before-writer**：新二进制先容忍旧版本（缺 `proto_version` 视为 v1）再发布新写者；未知版本 `claim` **fail-closed 409 `protocol_unsupported`**。测试 `TestSupportedProtoVersion`、`TestProtocolVersionHandshake`。
- **诊断增强**：`/v1/diagnose` 现返回 `proto_version`、requests/claims/commits/segments、`admission{enabled,allowed,shed}`、`capacity`（autoscaler plan）。
- **回归守卫（供应商矩阵）**：`wranglercompat TestVendorMatrixContract` 固定支持的 binding kind 与 migration key 集合，防 `compatibility-matrix.md` 静默漂移。
- **混沌/压测**：节点死亡恢复已有 `internal/recovery`/`nodelog` 测试（`TestRunnerRecoversDeadNodeSkipsLiveAndSelf`、`TestRecoverUnuploadedAck` 等，ADR-057/066）；压测工具 `cmd/cellbench`/`kvbench`/`sqlbench`/`gatebench`/`realbench`/`multiscopebench`。真实多主机混沌/云矩阵回归需环境（C 类）。

---

## ADR-089 DO 内 bindings（CF parity）✅

- **问题**：DO facet 由 workerd 构造，`env` 是 host worker 的 env（`LOADER/HOST/CELL_URL/...`），**没有租户 bindings** → DO 内 `env.KV`/`env.DB`/另一个 DO 都是 `undefined`（与 CF 不一致）。
- **关键实测**：`workerLoader.getDurableObjectClass()` 返回的是**不可 `extends` 的句柄**（`Class extends value #<DurableObjectClass> is not a constructor`），所以**不能子类化注入**。
- **做法（shim 注入 env）**：
  1. `host.js #facet` 冷路径拉取 `GET /v1/internal/do/bindings?ns=&worker=`（cell-agent 由投影生成 `SCOPE_SPEC` + `VARS_JSON`，含 scoped token；`doBindingSpec` 与 user-runtime `loader.js bindingSpec` 对齐）；
  2. 把 `CELL_URL/CELL_TOKEN/PLATFORM(service)/SCOPE_SPEC/VARS_JSON` 作为**可克隆 env** 传给 `LOADER.get`，并把 `facades.js` 注入 facet 模块图；
  3. `cellhive-do.js` 的 `DurableObject` 基类 `mergeBindings(env)` 用 `buildBindings` 构造 facades，`super(ctx, merged)` 并设 `this.env = merged` → 租户 DO 的 `this.env.KV` 等可用。
- **边界**：绑定通过 `this.env` 暴露；用**构造函数参数** `env` 的 DO 仍看到 host env（workerd 构造限制）。cellhive-do.js 已设 `this.env`。
- **验证**：真实 workerd `TestDoRuntimeBindingInsideDO`（DO 内 `env.KV.put/get` 往返 → `kv:v1`）；DO 兼容套件 16 子测试。
- **性能**：bindings 每次 facet 创建拉取一次（冷路径），非每请求。

---

## ADR-090 Bindings 迁移为 RPC entrypoint env ✅ Phase 0（KV）

- **动机/目标**：ADR-089 只把 bindings 合并进 `this.env`，构造函数形参 `env` 仍是 host env。改为：**平台 worker 导出 `WorkerEntrypoint` 能力类，用 `ctx.exports.X({props})` 生成 props 绑定的 RPC stub，放进"被加载租户 worker 的 env"** → 租户 fetch 与其 DO facet **原生**拿到完整 env（`this.env` 与形参 `env` 都可）。
- **关键实测**：pinned workerd 支持 `ctx.exports.KV({props})`，且把该 stub 作为 `LOADER.get({env})` 的值传入后，DO 构造函数的形参 `env.KV` 与 `this.env.KV` **都可用**（probe：`param:yes this:KV[demo]:k`）。
- **实现（Phase 0，KV 试点）**：
  - `workerd/platform/bindings.js`：`KV extends WorkerEntrypoint`（get/put/delete/list，从 `this.ctx.props` 取 `ns/token`，经 `PLATFORM` service binding 调 cell-agent，仅带 `x-cellhive-scope-token`）+ `bindingStub(ctx, spec)`。
  - **do-runtime**：host worker 模块含 `bindings.js` 且 `export { KV }`；`#facet` 用 `buildFacetEnv(this.ctx, spec)`（vars + `bindingStub`）构造 facet env；`cellhive-do.js` 删除 `mergeBindings`（env 已完整）。
  - **user-runtime loader**：`tenantEnv(env, ctx, spec, vars)` 混合——已迁移 kinds 走 stub，其余进 `CH_SCOPE_SPEC` 由 wrapper 继续用 HTTP facade（**增量迁移，不回归**）；`loader.js` 默认 `fetch(req, env, ctx)`。
  - capnp：do-runtime/loader/internal worker 加 `bindings.js` 模块；Render 复制。
- **验证**：真实 workerd `TestDoRuntimeBindingInsideDO`（DO 内 `this.env.KV` + **构造函数形参 env.KV** 均可，KV 往返 `kv:v1`）；`TestUserRuntimePublicLoaderBuildsBindingFacades`（fetch 路径现走 KV entrypoint，scope token 仍由 loader 本地 HMAC 生成并被 Go 验证、不泄漏 internal token）；`go test ./...` 39/39 ok；DO 兼容套件 ok。
- **Phase 1 ✅（D1/R2/Queue）**：`bindings.js` 增 `D1Database`（`prepare()` 返回 **`RpcTarget`** 语句，支持 bind/all/first/run；`batch` 经 `statement.spec()` 取回 sql/params）、`R2Bucket`（`get()` 返回 `RpcTarget` body：text/json/arrayBuffer）、`QueueProducer`（send）。**实测**：pinned workerd 支持入口点返回 `RpcTarget`（probe `d1:[{"sql":"SELECT 1","n":2}]`）。验证：真实 workerd `TestDoRuntimeD1R2QueueInsideDO`（DO 内 env.DB/R2/QUEUE）+ `TestUserRuntimePublicLoaderD1R2Queue`（fetch 路径）。
- **Phase 2 ✅（DO/Workflow）**：CF 的 `idFromName` 是**同步**的，无法跨 RPC → 采用 **env-patch 机制**：生成 loaded worker 入口 `bindings-wrapper.js`，顶部 `import { env } from "cloudflare:workers"`，从 `CH_FACADE_SPEC` 用 `buildBindings` 造 local facades（DO namespace / Workflow），`Object.defineProperty` 补进 env，再 `export * from "tenant.js"`。**关键实测**：workerd 把**同一个 env 对象**交给 fetch 与其 DO 构造 → patch 后 `this.env.ROOM` 与**构造函数形参 `env.ROOM`** 都可见且 `idFromName` 同步可用（probe `param:true id:id:a1`）。验证：`TestDoRuntimeDoInsideDO`（DO→DO：`env.ROOM.idFromName` + `.get().fetch()` → `do:inner`；形参式 `param-do:yes`）、`TestDoRuntimeWorkflowInsideDO`（`env.WF.create` → `wf:w1`）。
- **Phase 3 ✅（统一）**：两宿主统一为"**patch importable env**"——do-runtime 用 `bindings-wrapper.js`，user-runtime 在 `queue-wrapper.js`/`workflow-wrapper.js` 顶部做同样 patch（`CH_FACADE_SPEC` → local facades）；`#bindings()` 直接返回 `this.env`。已退役 `cellhive-do.js` 的 `mergeBindings`。do-runtime `/v1/internal/do/bindings` 保留（返回 spec，冷路径）。
- **Phase 4 ✅**：6 种 binding 在两宿主的真实 workerd e2e 全覆盖（DO 宿主：`TestDoRuntimeBindingInsideDO`/`TestDoRuntimeD1R2QueueInsideDO`/`TestDoRuntimeDoInsideDO`/`TestDoRuntimeWorkflowInsideDO`；fetch 宿主：`TestUserRuntimePublicLoaderBuildsBindingFacades`/`TestUserRuntimePublicLoaderD1R2Queue`/`TestUserRuntimeDurableObjectBinding`/`TestUserRuntimeRunsWorkflow`）；DO 兼容套件 19 子测试；`docs/bindings.md`/`durable-objects.md`/`compatibility-matrix.md` 更新。**边界**：`ServiceBinding` 未对租户暴露；`workerd/spikes/p0/host.js`（P0 spike，非生产）仍用旧 SCOPE_SPEC 模式。
- **ServiceBinding ✅（含 RPC）**：`bindings.js` 的 `ServiceBinding` 是 `WorkerEntrypoint`，构造函数返回 **Proxy**：未知方法 → RPC 转发到目标 worker 的命名 entrypoint；`fetch()` 走 `/v1/service/fetch`。cell-agent `POST /v1/service/{fetch,run}`（`scopeAuth("service")`，投影解析目标 active bundle）→ user-runtime `/v1/services/{fetch,run}` → wrapper `CellHiveHost.callMethod(entrypoint, method, args)`（类 entrypoint 以 `(ctx, env)` 构造）。`control.Binding.Entrypoint` + `loader.js`/`doBindingSpec` 透传。验证：`TestUserRuntimeServiceBinding`（fetch→`svc:from-B:hi`）、`TestUserRuntimeServiceBindingRPC`（`env.SVC.greet("bob")`→`rpc:hi-bob`）、`TestServiceFetchProxy`（scope/投影/dispatch）。边界：RPC 参数/返回值经 JSON（非结构化克隆）。CLI `deploy` 已有 `--service NAME=TARGET`（含 entrypoint 需直接构造 body 或用扩展字段）。

---

## ADR-091 CLI 资源命令 + assets 版本 token + tail ✅

- **assets 版本 token**：`artifacts.PutAssetAt(ns,worker,token,path,data)` 允许**调用方指定版本 token**；`POST /v1/control/asset?...&token=<T>` 用该 token 存储，使一个版本的所有文件共享 `version.assets_sha`（loader 读路径按其 token 取 `assets/<ns>/<worker>/<T>/<path>`）。CLI `deploy --assets-dir <dir>` 走 `assetsDirToken`（对排序后的 `rel\0contentHash\n` 求 sha256，确定性）+ 逐文件上传 + 设 `assets_sha`。验证：`TestAssetVersionTokenRoundTrip`（共享 token 下两文件均可经 `/v1/internal/asset` 读、错 token 404）、`TestAssetsDirTokenDeterministic`。
- **列举**：admin `GET /v1/control/apps`、`GET /v1/control/resources?namespace=&kind=`（`control.Store.Resources`）；CLI `app list`、`resource list`。
- **tail**：`cellhive tail <namespace>` 轮询 `GET /v1/control/audit` 打印新增控制面审计事件（poll 语义，非逐请求日志；per-request 日志流未实现）。

---

## ADR-092 backend-A 捕获接线（cellstore → LTX → fleet → bucket）✅

- **问题**：KV/D1/Queue/Workflow 的请求 handler 直接写 cellstore（本地 SQLite），**没有任何捕获**（全仓只有 `cmd/sqlbench` 构造 `sqlcapture.Capture`）→ 这些 cell 只落本地盘、未进复制链，与 `cell-protocol.md` 声称的"backend A 自己捕获"不符，RPO=0 未达成。
- **做法**：
  1. **抽出提交核心** `Server.commitSegmentCore(ctx, scope, epoch, seg, followers, prebatched) (commitResult, error)`；HTTP `commitSegment` 与捕获 committer 共用，行为不变（`fleet`/`bucket`/`bucket-batch`/`bucket-async`）。
  2. **`internal/cellcapture.Manager`**：对 cellstore cell 惰性启动 `sqlcapture.Capture`（owner 栅栏 + epoch；epoch 变更/失去归属即停止并重定基）；`Ensure(scope)`（首捕获时发 baseline snapshot）、`Wait(scope, txid)`（Notify + 等 durable barrier）。
  3. **committer 适配** `Server.CaptureCommitter()`：调 `commitSegmentCore`，**只接受 RPO=0 证明**（`fleet`/`bucket`/`bucket-batch`），拒绝 `bucket-async`。
  4. **写路径接线**：`capturedWrite`（Ensure → 写 → 读 txid → Wait）。已接 **KV（put/delete）、D1（query/exec/batch）、Queue（send/ack/retry）、Workflow（create/event/lifecycle/finish/step/sleep/consume/wait/attempt）**；`cmd/cell-agent` 构造 Manager（owner 来自 `owner.Manager.Resolve`，epoch 参与捕获）。
- **证明语义（已确认）**：**fleet（peer fsync）** 为 ack 证明；bucket 上传**异步批量**（`Uploader.Enqueue`）。无可用 peer 时按 `CELLHIVE_DURABILITY`/`BucketWait` 回退 bucket 等待（`fleet` 姿态）或 async（RPO>0，明确记录）。
- **测试**：`internal/cellcapture`（捕获+Wait、owner 丢失停止、非 owner 拒绝）；`internal/server TestKVCaptureReplicatesAndRestores`（KV 写 → 捕获 → `replica.Restore` → `restore.ApplyFile` → 读回同值）。
- **timer/control 已接**：`/v1/internal/timer/upsert` 按请求 scope 捕获；控制面写（app create/delete、resource、deploy、promote、rollback、route put/delete、secret put、worker delete）按 cell scope（`<ns>/__control__/main`、registry `__platform__/__control__/main`）捕获。
- **一致性/准确性测试**：`TestKVCaptureConsistencyN`（250 唯一 key，冷恢复逐字节一致、无缺失/重复、`integrity_check=ok`）、`TestD1CaptureConsistency`（100 行，同上）；live e2e：1000 个 acked KV 写 → `kill -9` owner → 空盘新节点 claim 接管 → 读回全部 1000 key（RPO=0）。
- **性能**：`kvbench` capture 开/关（FS bucket，c=1/4，见 `docs/benchmarks.md`）；capture 开（`durability=bucket`，RPO=0）KV ≈1157 rps @c=4、p50 3.55ms；关（本地）5542 rps、p50 0.34ms。`CELLHIVE_BUCKET_WAIT=false`（bucket-async）被捕获 committer **拒绝**（宁可失败也不给 RPO>0 的 ack）。
- **修复（本 ADR 实现中暴露）**：
  1. **D1 txid 未推进**：D1 直接 `DB.ExecContext`，`cell_meta.txid` 不前进 → `capturedWrite` 读到陈旧 txid、`Wait` 立即返回（ack 早于持久化，RPO 破洞）。新增 `cellstore.Cell.Tx`，D1 的 query(mutation)/exec/batch 与 KV 写一样在同一事务内推进 txid。
  2. **`Delete` 两次提交**：先删再 bump 是两次 SQLite 提交，与捕获"每个 WAL 事务一个 txid"错位 → 改为单事务 `Tx`。
  3. **owner 租约不续**：cell-agent 从不续约 per-scope owner，`LeaseTTL` 后 owner 过期 → 捕获停止、owner-gated 写 503。新增按 `OwnedScopes` 的续约循环（TTL/3）。
  4. **冷恢复缺失**：cell-agent 从不从桶恢复 cellstore cell（只有 do-supervisor 会）→ 新 owner 空盘起、`binding_not_registered`。新增 `cellstore.Store.Hydrate`（首次打开且本地文件不存在时从桶 `replica.Restore` + `restore.ApplyFile`）+ `replica.LatestEpoch`（跨 epoch 找最新可用副本）。
- **剩余边界**：owner 变更时未证明尾段以最后一次 durable proof 为准；`Hydrate` 在 `Store.Cell` 的互斥锁内做桶 I/O（冷启动一次性）。

---

## ADR-093 cell-agent 写路径优化（binding 缓存 / KV 批写 / capture 调参）✅

- **背景（实测）**：单 cell capture ON（RPO=0）写 ~5.7–6.3k/s；SQLite 裸 ~80k/s、`PutTx` ~17k/s、批 100/txn ~98k/s → 瓶颈不在 SQLite，而在"每写一次 HTTP + 一个 SQLite 事务 + 一次 capture 证明"。
- **改动**：
  1. **binding 缓存**（`scopeAuth`）：`Control.HasBinding` 结果按 `ns|kind|name` 缓存（含负结果），TTL `CELLHIVE_BINDING_CACHE_MS`（默认 1000，0 关）；控制面 resource/deploy/delete worker/delete app 后 `invalidateBindings(ns)` 立即失效；表大小有上限。**实测无增益**（A/B 在噪声内，binding 查询不是瓶颈），保留为正确性/防御性优化并记录。
  2. ~~**KV 批写**~~（**已移除**）：曾加 `PutManyTx` + `POST /v1/kv/put-batch`（bench 用）。但 `env.KV` 没有批量 API，且该场景几乎不存在，故删除——不作为平台能力。
  3. **capture 调参**：`Capture.GroupCommitWait` / `PipelineThreshold` 可配（`CELLHIVE_CAPTURE_GROUPCOMMIT_MS`=0→2ms、`CELLHIVE_CAPTURE_PIPELINE_MS`=0→8ms），默认行为不变。
- **结果（单机 FS bucket，capture ON，RPO=0，c=32）**：单键 6298 keys/s → 批 10 = 31097 → 批 50 = 56963 → **批 100 = 66778 keys/s（10.6x）**；c=128 批 100 约 67009。原始输出：`docs/archive/bench/optimization-2026.txt`。
- **结论**：提升单 cell 写吞吐应减少提交次数，不是加线程/缓存；单 cell 单写者与"每写一次证明"是固有成本。binding 缓存、capture 窗口旋钮保留；KV 批写端点按需求移除（见上）。

---

## ADR-094 单键写路径优化（per-cell 写串行化 + 窗口生效）✅

- **背景（实测）**：不做客户端批写时，单 cell 单键写 ~6k/s，且**加客户端反而更慢**（两客户端 3914 < 单客户端 5561），CPU 仅 ~1.9/8 核 → 不是 CPU，是 **SQLite 单写者争用**（多连接并发写 → `SQLITE_BUSY` 重试风暴）。另发现 capture 的固定 1ms ticker 会盖住 group-commit 窗口，使窗口旋钮失效。
- **改动**：
  1. **`cellstore.Cell.writeMu`**：在 Go 侧串行化写事务（`Cell.Tx` 持锁），消除 BUSY 重试；读仍走原连接池。
  2. **capture ticker 跟随窗口**：`interval = max(1ms, GroupCommitWait)`，使 `CELLHIVE_CAPTURE_GROUPCOMMIT_MS` 真正控制合并（否则 1ms ticker 先把 WAL 抽干）。
- **结果（capture OFF，单 cell 单键 put）**：c=16 **5561 → 9637 rps**（1.7x）、c=128 5722→9334，且两客户端 c=16 = 9124（不再塌）。**capture ON（RPO=0）**：~4.6–5.1k，两客户端 4681 → **证明路径（桶提交）是当前上限**，writeMu 帮不到它。
- **窗口生效验证**（capture ON，c=128）：窗口 2/5/10/20ms → 段内事务 **8.6/21.3/41.2/89.3**，但吞吐 ~5–6k 持平 → 对 capture OFF 合并不是瓶颈；对 capture ON 瓶颈是 commit 速率。
- **结论**：无批写时，①先修本地写争用（writeMu）拿到 ~1.7x；②capture ON 的下一步是 **fleet（两节点）证明**提高 commit 速率，而不是调窗口。
- 原始输出：`docs/archive/bench/optimization-single-key.txt`。

---

## ADR-095 写路径成本分解 + txid 内存镜像 ✅

- **背景（实测分层，单机）**：裸 autocommit UPSERT 12.3µs(~81k/s) → 显式事务+UPSERT 16.4µs(~61k/s) → `PutTx` **57.6µs(~17k/s)**；即 **txid 记账（`UPDATE cell_meta` + `SELECT` 回读）≈41µs，比数据写本身还贵**。再叠加 handler（验签+binding ~21µs+mux+JSON）与 HTTP → ~9.6k。capture ON 再叠证明 → ~5k。这也解释了为何 80k/30k 不可比：80k 是单语句无记账；30k(sqlbench) 是**每请求批事务**（tx/batch 61）。
- **改动**：`Cell` 增加 `txid` 内存镜像（`writeMu` 保护，open 时从 `cell_meta` 载入）。`Tx` 只 `UPDATE cell_meta SET v=next`（不再 `SELECT` 回读）；`TxID()` 走镜像（不再每写查库）。
- **安全性**（针对"两个节点是否可能同时写同一个 SQLite"）：cell 是**独占所有权**（bucket CAS + epoch 栅栏），每个节点写自己本地的那份文件，接管时从桶恢复；`writeMu` 又保证进程内写串行。因此镜像安全。新增 `TestTxIDMirrorContinuesAcrossReopen`（重开从 cell_meta 续号）与 `TestTxIDsUniqueUnderConcurrency`（512 并发写，txid 唯一且无空洞）。**不变式**：一个进程独占一个 cell 文件；不要让两个 cell-agent 共用同一 `CELLHIVE_DATA_DIR`。
- **结果**：`PutTx` 57.6→**40.4µs（-30%）**；进程内 handler 91→**74.6µs（-18%）**；真实 HTTP 单 cell capture OFF 峰值 c=32 **8679→9784（+13%）**，~9.8k。capture ON 仍 ~4.6k（受桶证明限制）。
  - 试过 `UPDATE ... RETURNING v` 合并 UPDATE+SELECT：**更慢**（67µs），已回退。
- 原始：`docs/archive/bench/write-cost-decomposition.txt`；bench 见 `internal/cellstore/puttx_bench_test.go`、`internal/server/kvput_bench_test.go`。

---

## ADR-096 cell 句柄 LRU + 空闲驱逐 ✅

- **问题**：`cellstore.Store` 缓存每个打开过的 cell（`cells map`），只在 `Store.Close()`/`DeleteNamespace` 释放，**无上限**；fd/内存随访问过的 cell 数增长（缺少 resident/idle eviction 与上限）。
- **改动**：
  1. `Store.MaxOpenCells` / `Store.IdleTTL` / `Store.Drop`（默认 0/0 = 不驱逐，行为不回归）。`Store.Cell` 命中即 touch LRU；`Sweep(ctx)` 按 LRU 逐出「空闲超 TTL」或「超上限」的 cell；`Start(ctx)` 起后台 sweeper。
  2. **安全门（per-cell）**：`Store.BeginRequest(key)` 在驱逐进行中阻塞、否则 key 在途 +1，返回 `end()`；驱逐只选「自身 key 无在途」的 cell。key 就是**该 binding 映射到的 cell scope**：KV 固定 `ns/__kv__/default`（一个 namespace 一个 KV 库）、D1 `ns/__d1__/<db>`、queue `ns/__queue__/<name>`、workflow `ns/__workflow__/<name>`；取不到 id 时回退 `ns:<ns>`。server 用 `bindingScope`/`gateKey` 复用各 binding 的 scope 构造函数（避免与 handler 漂移）在 `scopeAuth` 打点；`/v1/internal/workflow/*` 的 `pinNSMW` 与 `/v1/internal/timer/upsert` 同样按 cell scope pin。
  3. **关前停 capture**：`cellcapture.Manager.Drop(ctx, scope)`（cancel + 等 `sqlcapture.Capture.Done()` 退出），由 `Store.Drop` 钩子调用；之后才 `Close`。
  4. 配置：`CELLHIVE_MAX_OPEN_CELLS`（0=不限）、`CELLHIVE_CELL_IDLE_S`（0=不驱逐，生产建议 600）。
- **语义**：驱逐**只释放本地句柄**，不释放所有权（owner 记录不变）；下次请求 `Store.Cell` 重开并重载 txid 镜像（ADR-095）。cell-agent 仍是 owner，数据/复制不受影响。
- **测试**：`TestEvictIdleAndReopenContinuesTxID`（驱逐后重开 txid 续号）、`TestSweepSkipsBusyKey`（在途不驱逐）、`TestSweepPerCellGate`（同 ns 两 cell：只跳过忙的那个）、`TestSweepEnforcesMaxOpenLRU`（超上限逐 LRU）、`TestDropRunsBeforeClose`、`TestDeleteNamespaceClearsLRU`、`TestStatsCountsEvictions`。
- **可观测**：`Store.Stats()` → `/metrics` 暴露 `cellhive_cellstore_open_cells`（gauge）、`cellhive_cellstore_evicted_total`、`cellhive_cellstore_sweeps_total`；`/v1/diagnose` 增加 `cellstore` 块。端到端验证：`CELLHIVE_CELL_IDLE_S=1` 下写完 `open_cells=3` → 空闲后 `open_cells=0 / evicted_total=3` → 再写 500/500 成功（自动重开）。
- **不变式**：一个进程独占一个 cell 文件；不要让两个 cell-agent 共用同一 `CELLHIVE_DATA_DIR`。

---

## ADR-097 KV 库必须有 id（不再写死 default）✅

- **背景**：KV 的 cell 被写死成 `ns/__kv__/default`（`kvCell`），一个 namespace 只能有一个 KV 库；`Resource.Scope` 虽然被强制要求却被忽略（D1/queue/workflow 用请求里的名字定位，KV 不用）。目标是：**KV 也必须由 binding 声明一个 id**，一个 ns 可有多个相互隔离的 KV 库；app-ns 仍是隔离边界，**不支持跨 app 共享**。
- **改动**：
  1. **control**：`Store.Binding(ctx, ns, kind, name) (id string, ok bool, err error)` —— 先查 `resource/<kind>/<name>`（用其 `Scope`，即资源声明的 cell scope 或 id），再扫 worker 版本的 `Bindings`（用 `Binding.ID`）；都没有 → `ok=false`。`HasBinding` 委托它。
  2. **server**：scope token 的 `Name` 放进请求 ctx；binding 缓存条目扩展为 `{ok,id,exp}`（`bindingInfo`；`bindingAllowed`/`bindingID` 共用；控制面写仍失效）。`kvScopeFor(ctx, ns, name)` 解析 KV cell：
     - 声明的 ref 若能 `cell.ParseScope` 成完整 scope（如 `acme/__kv__/main`）→ 直接用它，并**要求 `scope.Namespace == ns`**（否则拒绝，防止越出 app 边界）；
     - 否则当**裸 id** → `{ns, "__kv__", ref}`；
     - **id 必填**：缺失 → 400 `kv_id_required`；绑定未注册 → 403；**无 `default` 回退**。
     - 四个 KV handler（put/get/delete/list）改用 `kvScope`。
  3. **驱逐门**：`gateKey` 对 KV 用解析后的真实 cell（每个 KV 库一个门），D1/queue/workflow 仍按名字，取不到回退 `ns:<ns>`。
- **语义**：一个 binding 声明 = 一个 KV 库（cell）；同一 ns 内不同 KV 库互不影响（数据、owner、capture、驱逐都按 cell）。
- **破坏性变更**：不再有 `default` 回退。迁移方式：把该 KV 资源/绑定的 `scope` 设为 `default`（或它原本的 cell scope）即可复位到旧位置；否则数据会落到新 id 的库。
- **测试**：`TestBindingResolvesResourceScope`、`TestBindingFromWorkerVersion`（含"声明了但 id 为空"）、`TestKVScopeUsesBindingID`（裸 id / 完整 scope / 跨 ns 拒绝）；live 冒烟：`scope=sessions` → `cells/<ns>/__kv__/sessions.db`，`scope=<ns>/__kv__/other` → `other.db`，未注册 → 403。
- **后续（未做）**：按 key 哈希把**一个 KV 库分到 N 个 cell**（`<id>#<i>`），点操作 `hash(key)%N`，`list` 跨分片归并。

---

## ADR-098 KV TTL 与 metadata（过期清理复用 timer/waker 激活）✅

- **背景**：KV 没有 TTL/metadata：`kv` 表无过期列，端点和绑定都不支持 `expiration`/`expirationTtl`/`metadata`（`put(key,value)` 静默忽略选项）。
- **改动**：
  1. **cellstore**：`kv` 增加 `expires_ms INTEGER NOT NULL DEFAULT 0`（对已有库 `PRAGMA table_info` 检查后 `ALTER TABLE` 迁移 + 部分索引）；`PutOpts/PutTxOpts(...,expiresMs)`；`Get`/`List` 过滤 `expires_ms=0 OR >now`；`ListMeta` 返回 key/meta/expires；`NextExpiry`；`DeleteExpired(now)`（单事务删到期行，返回数量 + 下一个最小到期；无到期则不写）。
  2. **server**：KV put 解析 `expiration`(绝对秒)/`expiration_ttl`(相对秒)/`metadata`(base64)；写后 **arm** 该 cell 的 kv-expire timer（`OpenTimer` + `RemoveByOccurrence("kv-expire")` + `Upsert` + `Timers.Add`）；`GET /v1/kv/get` 返回 `x-cellhive-kv-metadata`；`GET /v1/kv/list?with_metadata=1` 返回 `{name,metadata,expiration}`；新增内部端点 `POST /v1/internal/kv/expire?scope=`：`capturedWrite` 里 `DeleteExpired` → 有下一个到期则重武装，否则删 timer 行 + `Timers.Remove`（防止把 cell 永久钉住、破坏 LRU 驱逐）。
  3. **timer/dispatch**：新增 `KindKVExpire`；`dispatch.KVExpireDispatcher`（解析 owner → POST 其 `/v1/internal/kv/expire`；非本 kind 透传 `Next`），在 cell-agent 接入链中。本地 `timer.Runner` 按注册表派发，owner 宕机由 fleet **waker** 用同一 Dispatcher 派发。
  4. **绑定**：`bindings.js`/`facades.js` 的 KV：`put(key,value,{expiration|expirationTtl,metadata})` 透传 query；`getWithMetadata(key)` 返回 `{value,metadata}`；`list({includeMetadata})` 返回带 metadata/expiration 的 key 对象。
- **语义拆分（关键）**：**读过滤保证正确性**（不依赖 timer：owner 宕机期间也不会读到过期值）；**timer 只负责回收存储**。删除是普通写 → 走 capture，RPO=0、副本一致。不设 TTL 永不过期。
- **验证**：`TestKVTTLAndMetadata`（cellstore）、`TestKVTTLAndMetadataE2E`（server：metadata 往返、过期读 404、内部端点删除）、`TestKVExpireDispatcherRoutesToOwner`（派发路径 + 透传）、**`TestUserRuntimeKVTTLAndMetadata`（真 workerd 走 facades.js：`put({expirationTtl,metadata})` / `getWithMetadata` / `list({includeMetadata})`，并断言请求带 `expiration_ttl=60` 与 base64 metadata）**；live：`CELLHIVE_TIMER_INTERVAL=200ms`，put ttl=1s → arm（next>0）→ 3s 后清理已由定时器完成（`deleted=0,next=0`，0 派发失败）。
- **注意**：过期派发需要 owner 记录里的 `Address` 可达 → `CELLHIVE_ADVERTISE` 必须指向内部 REST 监听。**默认已修为 `127.0.0.1:7001`**（原 `:7000` 无监听者，ADR-136）。`CELLHIVE_TIMER_INTERVAL=0`（关定时器）时 TTL 仍由读过滤保证正确，只是不回收。

---

## ADR-099 timer/waker 规模化：bucket 唤醒索引 + 到期才扫（修 alarm 失效）✅

- **问题（实测）**：
  1. **waker 每轮扫全 bucket**：`DeadScopes` → `owner.DeadScopes` 做 `B.List("cells/")` + 每个 owner.json 一次 `Resolve(Get)` → **O(全部 cell)** 的桶操作（10k cell = 每 5s ~10k List 结果 + ~10k Get）。
  2. **class 过滤 bug**：它过滤 `class == "__timers__"`，但 timer 实际存在 `__timer__`(DO alarm)/`__workflow__`/`__kv__` 等 cell → **永远匹配不到，dead-owner 的 timer/alarm 从不派发**。
  3. **本地 runner O(注册 scope)/秒**：`Runner.Pass` 每轮 `Open` 所有注册 scope（打开+查询）且 `Registry.Remove` 从不调用 → 常驻、并把 cell 钉住（破坏 LRU 驱逐）。
- **改动**：
  1. **唤醒索引** `internal/wake`：`wake/<scope>.json` 记录「有 pending timer 的 scope + 最早 due」（objectstore 注册 `wake/` 前缀，owner=waker）。`timer.Store.Index` 钩子在 `Upsert`/`RemoveByOccurrence`/`MarkFired` 后按 `NextDue` 同步（best-effort）。
  2. **waker 改读索引**：`DeadScopes` = `wake.Due(now)`（O(有 timer 的 scope)）→ 对每个 due scope `Resolve` 校验 owner 失效。删除无用且 O(N) 的 `owner.DeadScopes`。
  3. **runner 到期才扫**：`timer.Registry` 记录每个 scope 的 next due（`Arm`/`Due`）；`Runner.Pass` 只处理 `due<=now`，派发后按 `Store.NextDue` 重武装或 `Remove`（无 timer 的 scope 注销）。DO alarm/cron/workflow/queue 的 arm 点保持 `Add`（due=0 首轮检查），KV 用 `Arm(scope,next)`。
  4. **接管/重启后重新注册**：`handleClaim` 成功后 `registerPendingTimers`（读该 cell 的 `NextDue` 并 `Arm`）；cell-agent 启动时按 wake 索引 + `owner==self` 重新注册。
- **验证**：单测 `TestWakeIndexPutDueDelete`、`TestStoreSyncsWakeIndex`、`TestRegistryDueFiltersByTime`、`TestRunnerSkipsFarFutureScope`；live 双节点：A 写 `ttl=1s` → 生成 `wake/capA/__kv__/sessions.json`；`kill -9 A`；B `claim` 接管 → B 的定时器路径完成清理（`expire` 返回 `deleted=0,next=0`）。
- **复杂度**：waker 由 **O(全部 cell)** → **O(有 pending timer 的 scope)**；runner 由 **O(全部注册)** → **O(到期)**。
- **边界（如实）**：waker 派发仍以 owner 记录为准；owner 真死时，执行发生在**接管（claim）之后**（已由 claim 钩子重新注册）。期间读仍正确（KV 过期靠读过滤），只是回收延后。分片未做。

---

## ADR-100 D1 只读语句跳过捕获 ✅

- **背景**：`/v1/d1/query` 无差别地走 `capturedWrite`（Ensure=owner 解析 → 执行 → Wait），SELECT 这类只读语句白付一次捕获路径开销。
- **改动**：`d1.IsReadOnly(sql)`（保守：仅 `SELECT`/`EXPLAIN`/`VALUES` 视为只读；`WITH`/`PRAGMA` 可能写，仍捕获）。server 的 D1 query 对只读直接执行，**不**走 `capturedWrite`；写仍捕获（RPO=0）。请求仍由 `scopeAuth` 的驱逐门 pin 住，安全不变。
- **测试**：`TestIsReadOnly`（含 `WITH ... INSERT`、`PRAGMA journal_mode=WAL` 判为写）；`TestD1ReadSkipsCapture`（mutation 触发 Ensure，read 不触发）。
- **结果（如实）**：修后复测 D1 query（capture ON）c=64/128 = **26045/26524 rps**，与修前（26006/26652）**无差异** → ON/OFF 的差距不是每请求证明开销，而是捕获/上传的**环境争用**。改动保留（少一次 owner 解析、语义更干净）。

---

## ADR-101 resident 上限 + 所有权 rebalance（雏形）+ RPO=0 故障注入测试 ✅

两个方向（hibernation/placement 与故障注入验证）：

- **#4 RPO=0 故障注入**（`scripts/rpo-zero-fault.sh`，`make rpo-test`）：节点 A 顺序写唯一 key/value，每个 200 后记录 key；写到第 K 个 **SIGKILL A**；**删除 A 的本地数据目录**（本地库彻底丢失）；节点 B（同桶）claim 接管，对**每个 acked key** GET 校验值精确一致，并用 `restoreverify` 验 `integrity=ok`。实测：`N=1500, KILL_AT=900` → acked=900、`missing=0 wrong=0`、`{"mode":"l0","integrity":"ok","keys":900}` → **PASS**。证明"acked ⇒ durable"，恢复只能来自桶。
- **#2 hibernation/placement 雏形**：
  1. `CELLHIVE_MAX_RESIDENT_CELLS`（0=不限）→ `cellstore.Store.MaxOpenCells`：resident/句柄上限，超限由现有 LRU/空闲 sweep 驱逐（与 `CELLHIVE_CELL_IDLE_S` 配合）。"hibernation" 即**句柄关闭但保留所有权/epoch**，下次请求重开。
  2. `internal/rebalance.Plan`（**纯函数**）：按 `Weight` 比例算目标份额，只在本节点**超份额**且某 peer **低于其目标（留 2% 余量）**时，选出最多 `maxMove` 个**空闲**（句柄未打开）owned cell（按 scope 排序，确定性）。
  3. cell-agent 循环（`CELLHIVE_REBALANCE_INTERVAL`，默认 **0=关**；`CELLHIVE_REBALANCE_MAX_MOVE`=32）：采样 lease（`lease.Load` 新增 `placement_weight`）→ Plan → 对选中 scope `om.Release`（并 `timers.Remove`）；**对端按需 claim**（无主动 push）。`cellstore.Store.IsOpen` 用于判定空闲。
  4. **测试**：planner 单测（低于份额不动/按权重/尊重 maxMove 与 2% 余量/只动空闲/确定性顺序）；**live 双节点**：A 拥有 6 个空闲 cell、B 为 0 → A 发布 `placement_weight=NumCPU` 后按轮次共释放 **4** 个，B `claim` 已释放的 `capA/__kv__/c1` 返回 **200**。
- **边界（雏形）**：只搬**空闲（hibernated）**cell；不做迁移中的请求重路由、不做实时/主动 push、不做 drain 集成（`performHandoff` 已有）；默认关闭，行为不回归。

---

## ADR-102 service binding 走同实例原生 JSRPC ✅

- **背景**：`env.SVC.method(...)` 过去是 **cell-agent HTTP/JSON 两跳**（`bindings.js` → `/v1/service/run` → cell-agent → user-runtime → `callMethod`），参数被 `JSON.stringify`，丢掉结构化克隆/RpcTarget/流。而 user-runtime 是**一个 workerd 实例 + workerLoader 按需加载**，目标 worker（无状态）完全可以**就地加载**（`extends WorkerEntrypoint` + `LOADER.get().getEntrypoint()`）。
- **改动**：
  1. `bindings.js`：`ServiceBinding` 支持注入式 `serviceLoader`（`setServiceLoader`）。方法调用**优先**走：注入的 loader 在**本实例**加载 pinned 目标 → `stub.getEntrypoint("CellHiveHost").callMethod(entrypoint, method, args)`（**原生 JSRPC**，参数按结构化克隆，RpcTarget/流都保留）；未注入或无 `version` 时**回退**原 HTTP/JSON 路径（安全增量）。
  2. `loader.js`：抽出 `workerStub(...)` 供 `runWorker` 与 service loader 共用；模块初始化时 `setServiceLoader(...)`：从 routing projection 解析目标 → `bundle` + `bindingSpec` → `workerStub`（同一实例）。`bindingSpec` 为 service binding 增加 `version`（目标 active `bundle_sha`）。
  3. `callMethod` 已按 JS 值直调租户命名 entrypoint（`new Entry(ctx, env)[method](...args)`），所以本地调用即原生语义。
- **修了一个真 bug**：service loader 里把 `entry.deleted_classes` 传成 `|| {}` 而后处用 `|| []` → `new Set({})` 抛 "object is not iterable"；改为 `|| []`。
- **验证**：新增 `TestUserRuntimeServiceBindingNativeRPC`（真 workerd）：caller `await env.SVC.greet("bob")` + `await env.SVC.unbox(new Box(41))`（`Box extends RpcTarget`），target 返回 `box.get()+1`；断言响应 `rpc:hi-bob:42` **且 `/v1/service/run` 调用次数为 0**（证明同实例、无 cell-agent、非 JSON）。原有 `TestUserRuntimeServiceBinding` / `...RPC` 也走新路径通过。
- **边界（如实）**：
  - 目标版本**部署时 pin（ADR-104）**：`control.Binding.Version` 记录部署时目标的 active `bundle_sha`，目标重部署不影响已部署调用方；缺失（旧部署/目标无 active）时回退"渲染时解析"。
  - **DO 的 `env.DO.get(id).method()` 仍然是 owner 路由 + JSON-RPC**（状态亲和性，不能就地加载；见 ADR-084/101）。
  - `env.SVC.fetch(...)` 仍走 HTTP（本次未改）。
  - 就地加载会让**调用方实例**多驻留一个目标 worker isolate（内存；无 sibling eviction）。

---

## ADR-103 DO owner-hint（cell-agent 侧）省一跳 ✅

- **背景**：DO 调用路径是 tenant → **cell-agent（按 shard 哈希选 do-runtime）** → do-runtime →（内部 owner hint 转发）→ owner。do-runtime 侧早有 owner hint（`host.js:31`，ADR-080），但**上游**不知道 owner，于是"选到的 runtime 不是 owner"时多一跳。改为把 hint 放在**调用方**（owner-hint-cache）。
- **改动（安全等价版，不动令牌边界）**：
  1. `host.js`：本运行时**作为 owner 就地服务** invoke 时，响应加 `x-cellhive-do-owner: <ADVERTISE>`；`forward()` 把 owner 响应的该头**透传**。
  2. `cell-agent`（`do_proxy.go`）：按 `ns|worker|storage_class|shard` 缓存 owner 地址（TTL 30s）。命中时**直连 owner**；仅"证明未开始执行"的失败（传输错误 / 503 draining / `owner_unavailable`）清 hint 并回退 shard 轮转；**`result_unknown`/`ownership_lost_after_dispatch`（409）不重放**，原样返回（守 ADR-080 的 fence 语义）。owner 头透传给客户端（便于以后客户端缓存）；`postDO` 对 `host:port` 归一化补 `http://`。
- **测试**：`TestDOOwnerHintSkipsShardingRouter`——首调走轮转并从响应学到 owner；次调**直连 owner**（轮转源不再被调用）；`409 result_unknown` **不被重放**；owner 不可达（连接失败）时清 hint 回退轮转成功。
- **未做（需新令牌）**：真正的**调用方 1 跳直连**要求 tenant 侧 facade 能直连 owner，而它按 **ADR-029/I-09 故意不持 internal token**。要做需新增窄令牌 `CELLHIVE_TOKEN_DO`（**只授权 `POST /v1/do/invoke`**，同 `CELLHIVE_TOKEN_LOG` 的模式），把爆炸半径限制在该端点；本次不改变令牌边界。

---

## ADR-104 service 目标版本在部署时 pin ✅

- **背景**：ADR-102 的 service binding 目标版本是在**渲染时**从 routing projection 解析（取目标**当前** active），于是目标一重部署，已部署的调用方就跟着漂移——与 CF"调用方部署时固定目标版本"的语义不同。
- **改动**：
  1. `control.Binding` 增加 `Version`（目标 bundle_sha）。
  2. `control.Store.Deploy` 在事务内 `pinServiceBindings`：对每个 `type=service` 且未带 `Version` 的 binding，把目标 worker 的**当前 active 版本**写入该版本记录（自绑定也 pin 到当时的 active；目标无 active 则留空）。调用方**重新部署**时才按当时 active re-pin。
  3. `loader.bindingSpec` 优先用 `b.version`（pin），缺失才回退"渲染时解析 active"（向后兼容旧部署）。
- **测试**：`TestDeployPinsServiceBindingVersion`——部署 b(v1) 后部署 a 带 service binding → `a.Version.Bindings[0].Version == shaB1`；**重部署 b(v2)** 后 a 的 pin **仍为 shaB1**；**重部署 a** 才 re-pin 到 shaB2。
- **边界**：若 pinned 版本对应的 bundle 被删除，调用失败（内容寻址、当前无 GC，风险低）；未做"目标版本缺失时告警/回退"（可后续）。

---

## ADR-105 service fetch 本地化 + DO 调用方 1 跳（按 shard 签名的 owner ticket）✅

- **(b) service `fetch` 本地化**：`ServiceBinding.fetch` 在注入 loader 且有 pinned version 时，改为**同实例原生**（`serviceLoader(...)` → 剥掉 `x-cellhive-*` 平台头 → `entrypoint ? callMethod(entrypoint,"fetch",[req]) : host.fetch(req)`）；错误直接抛出（**不**在 dispatch 后回退，避免重复副作用）；未注入则回退 HTTP。测试断言 `/v1/service/fetch` 调用次数为 **0**。
- **(a) DO 调用方 1 跳**：新增窄能力 **owner ticket**——
  1. `internal/doticket`：`base64url(JSON{ns,worker,storage_class,shard,exp}).base64url(HMAC-SHA256)`，TTL 30s（复用 `doOwnerHintTTL`）。
  2. 配置 `CELLHIVE_DO_TICKET_SECRET`（cell-agent 与 do-runtime 共用；空则回退 `SCOPE_SECRET`）；do-runtime 渲染 `DO_TICKET_SECRET`。
  3. cell-agent `handleDOProxy` 在返回 owner hint 时**同时**返回 `x-cellhive-do-owner-ticket` + `x-cellhive-do-owner-exp`。
  4. do-runtime `authorize`：**internal token = 全权**；**ticket = 仅授权 `POST /v1/do/invoke`**（其它 `connect/objects/abort/delete/restart/drain` 一律 403）；`invoke` 再校验 ticket 的 ns/worker/storage_class/shard 与该 spec 一致（纵深防御）。
  5. facade（`makeDO`）缓存 `{owner,ticket,exp}`；未过期**直连 owner**（带 `x-cellhive-do-ticket`，经 `private-outbound` 网络），过期/安全失败回退 cell-agent 重新学习；**409 result_unknown 永不重放**（ADR-080）。
- **安全**：ticket 是**按 shard 的窄能力**（只 invoke、30s），且只在 cell-agent 通过 `scopeAuth("do")`+binding ACL 之后签发 → **ACL 不绕过**；internal token 的既有边界不变。
- **测试**：`TestTicketMintVerify`（签名/过期/畸形）；`TestDOOwnerHintSkipsShardingRouter` 断言返回的 ticket 可验签；`TestUserRuntimeDOOwnerHintDirectCall`（真 workerd：router=1、直连带 ticket=1）；`TestUserRuntimeServiceBinding` 断言 `/v1/service/fetch` 为 0。
- **边界**：ticket TTL 30s（到期经 cell-agent 重新签）；owner 变迁时 hint 失效自动回退重学；`/v1/do/connect`（WS）仍只接受 internal token。

---

## ADR-106 快速路径开关 + A/B 延迟量化 ✅

- **开关（运维 kill-switch，默认开）**：`CELLHIVE_SERVICE_NATIVE=0` 关闭 service 同实例原生 RPC/本地 fetch（ADR-102/105）；`CELLHIVE_DO_DIRECT=0` 关闭 DO 调用方直连 owner（ADR-103/105）。都只影响快路径，关闭后回退到原 HTTP 路径（便于灰度/回滚/对照）。
- **量化测试**：`TestUserRuntimeFastPathLatency`（真 workerd，同一构建跑开关开/关）：worker 内用 `performance.now()` 量每次调用；stub 在"router/service 跳"上注入 **3ms** 延迟以模拟一跳。
  - **关闭**：DO 首调/稳态 ≈ 4.00/3.00ms（每次都付跳），service RPC ≈ 4.00ms。
  - **开启**：DO 首调 4.00ms（首次经 router 学习）、**稳态 0.00ms**；service RPC **0.00ms**。
  - → 每次 DO 调用**省 ~3ms**、每次 service RPC **省 ~4ms**（= 注入的一跳 + 基线）。
- **说明（如实）**：这是 **loopback + 注入延迟** 的**机制验证**（证明"跳"确实被移除），绝对值不代表生产；生产的节省 = 真实 cell-agent/跨主机往返（loopback <1ms，跨主机为 RTT）。真实跨主机尺寸需第二台主机（C 类 blocker）。
- 配置入口：`cmd/user-runtime` 读取 `CELLHIVE_SERVICE_NATIVE`/`CELLHIVE_DO_DIRECT` → 渲染进 loader env（`CH_SERVICE_NATIVE`/`CH_DO_DIRECT`）。

---

## ADR-107 DO 会话策略 + 删除锁（对齐 CF）✅

- **背景**：我们的部署默认**惰性重启** DO（`CELLHIVE_DO_EAGER_RESTART=false`），没有显式"部署时保留/重启"语义；`DeleteWorker` 与 `Deploy` 并发时可能"删除中途被部署复活"。CF 有 session policy（preserve/restart）与 version delete lock。
- **改动**：
  1. **会话策略**：`control.DeploySpec.SessionPolicy` + `control.Version.SessionPolicy`（审计）；部署请求 `session_policy`；`server.deployRestartPolicy(eager, policy)`——`"restart"` **强制** eager abort（关 WS 1012 后按新代码重启）、`"preserve"` **禁止** abort（驻留对象保 WS，直到空闲驱逐/休眠后按新代码唤醒）、空则沿用 `CELLHIVE_DO_EAGER_RESTART`。
  2. **删除锁**：`control.AcquireDeleteLock(ns,worker,actor,ttl)` / `ReleaseDeleteLock`（控制 cell 行 `lock/delete/<worker>`，带 `actor/at_ms/exp_ms`）；`Deploy` 在**同一事务**内发现未过期锁即拒绝；`handleControlWorkerDelete` 先取锁（占用则 **409 `delete_in_progress`**）、`defer` 释放（都走 `capturedWrite`，RPO=0）。
- **测试**：`TestDeleteLockBlocksDeploy`（持有→部署失败；释放→成功；**过期锁不阻塞**）；`TestVersionRecordsSessionPolicy`；`TestDeployRestartPolicy`（默认惰性 / restart / preserve / 未知→默认）。
- **默认行为**：不指定 `session_policy` 时行为不变（惰性重启）。
- **边界（如实）**：app 级删除未加锁（按 worker 的锁不适用，删除 app 是 namespace 级）；锁 TTL 10min 防崩溃卡死；跨 worker `transferred_classes` 仍拒绝。

---

## ADR-108 owner epoch 单调 generation（修复 epoch 复用）✅

- **背景**：确定性仿真（2000 seed × 300 步，注入 CAS/传输失败）发现两个缺陷：
  1. `Claim` 对**同一节点**的活租约跳过 expiry 检查并 `cur.Epoch+1`——无谓推进 epoch，污染复制 lineage。
  2. `Claim` 不走持久 generation 计数器：冷 claim 固定 `epoch=1`、接管用 `cur.Epoch+1` → **release 后再 claim 复用 epoch 1**；与 `ClaimAs`（已用 `owner-gen`）两条路径互相错位。epoch 复用会与已释放 epoch 的复制 lineage（`cells/<scope>/ltx/e<epoch>/`）冲突，且 `replica.Restore(scope,1)` 可能拉到旧数据。旧测试 `TestClaimRenewRelease` 把 "reclaim epoch=1" 固化成期望。
- **改动**：`Claim` 两条路径统一走 `Manager.nextEpoch`（持久 `owner-gen`，从不删除，只跳不重用）；**活租约无论谁持有都 `ErrOwnerLive`**（同节点重 claim 不再 bump epoch；同 node id 重启也等旧 session 租约到期）；`nextEpoch` 重试 4→8 并返回哨兵 `owner.ErrGenContention`（可重试）。
- **测试**：`internal/owner/sim_test.go`（`fakeBucket` 注入 etag/CAS 失败；3 节点/3 scope/时钟偏移；断言跨节点 claim 仅在旧租约过期后成功、epoch 不回退、被取代 owner 不能 renew/release、resolve epoch 不回退）；`TestClaimRenewRelease` 改为断言单调（reclaim = old+1）。
- **影响**：epoch 单调递增，复制 lineage/恢复按 epoch 取最新代；`ErrGenContention` 调用方可重试。
- **边界（如实）**：仿真为**模型级**（确定性、单进程、内存 bucket），非真实跨主机时序；真实 RTT/网络分区仍属 C 类环境阻塞。

---

## ADR-109 DO 租约 TTL 可配 + 持久对象登记索引（可选）✅

- **背景**：常见实现的 DO owner TTL 较长且对象登记在持久的外部存储；我们 DO 租约 TTL 固定 30s（`host.js LEASE_TTL_S`），对象登记在 do-runtime **内存** `seen`（重启即丢，列表靠 fan-out 各 runtime 聚合）。
- **改动**：
  1. **租约 TTL 可配**：`host.js` 读 `DO_LEASE_S`（`CELLHIVE_DO_LEASE_S`，默认 30）用于 claim/renew 的 `ttl_seconds`；长 TTL 省续约 churn、容忍 cell-agent 抖动，短 TTL 故障切换更快。
  2. **持久对象索引（默认关）**：`CELLHIVE_DO_OBJECT_INDEX=1` 时，do-runtime 在**首见**对象时 `POST /v1/internal/do/objects`（best-effort、失败不影响调用）、`deleteFacet` 时 `DELETE`；cell-agent 写桶 `dosupervisor/do-objects/<ns>/<worker>/<class>/<shard>/<name>.json`（复用 `objectstore.NewOwned(PrefixSupervisor, OwnerSupervisor)`，无新前缀）；`GET /v1/internal/do/objects` 合并桶索引（`source:"index"`）与各 runtime fan-out，并按 `ns/worker/class/shard/name` 去重。
- **测试**：`TestDOObjectIndex`（默认关→404；开启→put、列出去重、删除；无 do-runtime 时仅索引）。
- **边界**：默认关（热路径零成本）；索引是"冷启动加速器"，运行期仍以各 runtime `seen` 为准（best-effort，非强一致）；`GET` 仍走 List，属诊断/冷路径，符合"热路径禁 List"。

---

## ADR-110 bundle GC（未被引用的 worker bundle 回收）✅

- **背景**：bundle 是内容寻址（`bundles/sha256/<aa>/<sha>`，同内容去重，ADR-062）；`DeleteWorker`/`DeleteApp` 会清 cell 数据，但**从不删 bundle** → 长期只增不减。
- **决策**：冷路径后台 GC，**两阶段标记 + 宽限期**（避免删掉"刚上传、还没被 deploy 引用"的 bundle，也避免 deploy 竞争）：
  1. **引用集** `control.Store.BundleRefs`：枚举所有 app 的 `worker/<name>/version/<n>`，收集每个版本的 `BundleSHA` **以及 service binding 里 pin 的目标 bundle**（ADR-104）。被引用的 SHA 永不删除（旧版本保留，回滚可用）。
  2. **标记**：未引用的 SHA 在全局 cell 记 `bundle-gc/<sha> = 首次发现时间`（`BundleGCMark/SetBundleGCMark/ClearBundleGCMark`；key 校验 hex，防前缀逃逸）。重新被引用则**清除标记**。超过 `BundleGCGrace`（默认 24h）仍未被引用才删除。
  3. **实现**：`internal/objgc.GC.Pass`（窄接口 `Items{List,Delete}`/`Refs{Referenced,Mark,SetMark,ClearMark}`，可测；ADR-111 泛化后 bundle 与 assets 共用）；`artifacts.Store.ListBundles`/`DeleteBundle`（`DeleteBundle` 要求完整 64 位小写 hex sha，删除面不超过单个 bundle）。
  4. **入口**：admin `POST /v1/control/gc/bundles`；CLI `cellhive gc bundles`；cell-agent 后台循环 `CELLHIVE_BUNDLE_GC_INTERVAL`（**默认 0=关**）+ `CELLHIVE_BUNDLE_GC_GRACE`（默认 24h）。
- **测试**：`internal/objgc`（标记→宽限期内不删→过期删除；被引用者清标记且不删）、`internal/artifacts`（List/Delete/幂等/坏 sha 拒绝）、`internal/control`（BundleRefs 含版本与 pin、标记往返、坏 sha 拒绝）、`internal/server`（`TestGCAdminEndpoints` 端到端）。
- **边界（如实）**：引用集来自控制面元数据，若控制面状态丢失则可能误删（bundle 可由源码重建，风险可接受）；`List` 为冷路径（符合"热路径禁 List"）。assets 回收见 **ADR-111**。

---

## ADR-111 assets GC（未被引用的 assets 版本回收）✅

- **背景**：assets 按 `assets/<ns>/<worker>/<token>/<path>` 存放（token = 版本内容哈希，ADR-062/069）。`DeleteWorker`/`DeleteApp` 清 cell 数据但不删 assets → 只增不减；ADR-110 只回收 bundle。
- **决策**：把 ADR-110 的 bundle GC **泛化为 `internal/objgc`**（`Items{List,Delete}` + `Refs{Referenced,Mark,SetMark,ClearMark}`），bundle 与 assets 共用同一"两阶段标记 + 宽限期"逻辑：
  1. **引用集** `control.AssetRefs`：枚举各 app 所有 `worker/<name>/version/<n>` 的 `AssetsSHA`，得到 `<ns>/<worker>/<token>`（**保留所有版本**，回滚仍有 assets）。
  2. **标记**：通用化为 `control.GCMark/SetGCMark/ClearGCMark(kind,id)`，key `asset-gc/<ns>/<worker>/<token>`（kind ∈ `bundle`|`asset`，id 校验防前缀逃逸）；被引用则清除标记。
  3. **实现**：`artifacts.Store.ListAssets`/`DeleteAsset`（`DeleteAsset` 要求 3 段安全组件，删除面 ≤ 单个版本）、适配器 `artifacts.BundleItems`/`AssetItems` 与 `control.GCRefs{Kind}`。
- **入口**：admin `POST /v1/control/gc/assets`；CLI `cellhive gc assets`；cell-agent 后台循环每轮跑 **bundle + asset** 两遍（`CELLHIVE_BUNDLE_GC_INTERVAL`/`CELLHIVE_BUNDLE_GC_GRACE`，默认关/24h）。
- **测试**：`internal/objgc`（标记→宽限期内不删→过期删；被引用清标记；新未引用只标记）、`internal/artifacts`（List/Delete 只删该版本、坏 id 拒绝）、`internal/control`（`AssetRefs` 含全部版本的 token、标记按 kind 隔离）、`internal/server`（`TestGCAdminEndpoints`：bundle 两遍删除且引用保留；assets 两遍删除）。
- **边界（如实）**：`DeleteAsset` 先 List 再逐个 Delete（非原子；部分失败留待下次）；assets token 若控制面状态丢失可能误删（可由源码重建）。

---

## ADR-112 Queue 消费者 `max_concurrency` ✅

- **背景**：wrangler 的 queue consumer 可配 `max_concurrency`；我们既不识别也不支持（`Consumers` 无该字段，deploy JSON 会被静默忽略），且 `queue.Runner` 对每个队列是**串行**的（每 Pass 每队列最多一批）。
- **决策**：
  1. `control.Consumer.MaxConcurrency`（0/1 = 串行，**默认**，行为不变）；`queue.Ref.MaxConcurrency`；cell-agent 从 `QueueTargets` 透传。
  2. `Runner.dispatchRef`：每个 Pass 对每个队列取**一波**最多 N 批，波内**并发**派发（`sync.WaitGroup` + 每批结果归并），波间由轮询间隔驱动 —— goroutine 数量有界（≤ N），不会无界堆积。**claim 仍串行**（保持消息可见性/去重语义），批内顺序不保证（与 at-least-once 契约一致）。
  3. 重试/死信逻辑抽成 `dispatchBatch`，每批独立；语义不变。
  4. CLI：`--consumer <queue>[:maxRetries[:deadLetterQueue[:maxConcurrency]]]`。
- **测试**：`internal/queue` `TestRunnerMaxConcurrency`（屏障 dispatcher 证明 3 批同时在飞；未设时仍是 **1 批/Pass** 不回归）、`cmd/cellhive` `TestConsumeSpecs`（含第 4 段解析）。
- **边界（如实）**：`max_concurrency` 只限"同时在飞的批次数"，不等同 CF 的并发实例数；`contentType=v8` 仍不支持；不保证跨批顺序。

---

## ADR-113 R2 multipart 上传 ✅

- **背景**：R2 之前只有 put/get(+Range)/delete/list/presign，**无 multipart**（此前记为"暂不做"）；大对象只能一次性 `put`。
- **决策**：在 **r2 store 层做服务端分片暂存 + 拼装**（不改 `bucket.Bucket` 契约，不引入流式 Put）：
  1. `CreateMultipart` 生成 32 位 hex upload id；分片存 `r2/.mpu/<ns>/<bucket>/<uploadID>/<part>`——**刻意不放在** `r2/<ns>/<bucket>/` 前缀下，故用户 `list` 永远看不到分片。
  2. `UploadPart` 写分片并返回其 etag；`CompleteMultipart` **按 partNumber 顺序**读分片、校验 etag（不符则 **fail-closed 且不写对象**）、拼装后 `Put` 最终对象，并尽力清理分片；`AbortMultipart` 幂等删除分片。
  3. 上限：`MaxMultipartParts=10000`、`MaxMultipartBytes=512MiB`（超出 → 413）。
  4. **端点**（`scopeAuth("r2")`+`admit`）：`POST /v1/r2/multipart/create`、`PUT /v1/r2/multipart/part`、`POST /v1/r2/multipart/complete`、`DELETE /v1/r2/multipart`（abort）。
  5. **facade**：`env.BUCKET.createMultipartUpload(key)` → `R2MultipartUpload{uploadId, uploadPart(n,value)->{partNumber,etag}, complete(parts), abort()}`；`resumeMultipartUpload(key, uploadId)`。
- **测试**：`internal/r2` `TestR2Multipart`（分片对 List 不可见、etag 不符 fail-closed、顺序拼装、abort、坏 upload id/part 0 拒绝）；`internal/userruntime` `TestUserRuntimePublicLoaderD1R2Queue`（**真实 workerd e2e**：tenant 调 `createMultipartUpload`/`uploadPart`/`complete`/`get`，读回 `part-a|part-b`）。
- **边界（如实）**：`complete` 为**内存拼装**（Bucket 契约无流式 Put）→ 512MiB 上限同时约束峰值内存；不强制 CF 的 5MiB 最小分片规则；`resumeMultipartUpload` 只按 (key, uploadId) 定位，不额外校验 upload 存在。

---

## ADR-114 dev CLI assets 与生产 loader 对齐（`_headers`/`_redirects`/`not_found_handling`/worker 回退）✅

- **背景**：`cellhive dev`（Bun + Miniflare）此前只把 `assets.directory` 交给 Miniflare，worker 与 assets 的交互未接线（打印 `assets_worker_interop ... pending`），`_headers`/`_redirects`/`not_found_handling` 未验证。
- **历史 Miniflare 4 决策**：原纯映射 `assetsOptions(assets, projectDir)` 曾把 wrangler `assets` 配置直接译为 Miniflare 资产路由配置：
  1. `routerConfig.has_user_worker=true` → 资产未命中**回退 worker**（生产："未命中回退 worker"）。
  2. `run_worker_first: true` → `invoke_user_worker_ahead_of_assets=true`（worker 先于资产）；`run_worker_first: [paths]` → `static_routing.user_worker=paths`（**只**这些路径走 worker；注意**不**设全局 invoke-ahead，否则所有路径都走 worker）。
  3. `assetConfig.not_found_handling` 透传（`404-page`/`single-page-application`/`none`）；`_headers`/`_redirects` 由 Miniflare 从资产目录读取。
  4. `cli/src/config.ts` 解析 `run_worker_first`（bool/数组）与 `not_found_handling`。
- **验证**：
  - 单测 `cli/test/assets.test.ts`（5 例：assets-only 保 worker 回退、paths→static_routing（且不设全局）、bool→invoke-ahead、binding/not_found_handling 透传、无配置→undefined）。
  - e2e `cli/test/dev-assets-e2e.test.ts`（真实 Miniflare/workerd）：`/`→index、`/asset.txt` 带 `_headers` 的 `x-custom`、`/old`→**302** `_redirects`（`redirect:"manual"`）、`/nope`→**404** + 404.html、`/api/hi`→worker。
  - CLI 冒烟（真实 `cellhive dev` + curl）：同一组行为逐条确认。
- **入口**：`make cli-test`（`cd cli && bun test`）；`make js-test` 不变。
- **历史边界（已由下述 Miniflare 5 修订解决）**：Miniflare 4 的路由在 worker 对现有路径返回 404 时可能偏离生产；新入口 router 已覆盖“worker 404 → 资产”真实测试。`html_handling` 仍未解析（保持 Miniflare 默认）。
- **Miniflare 5 修订（2026-09-22，已实现）**：`miniflare@5.20260916.0-alpha` 把“资产 miss 是否回退用户 worker”和“是否执行 `404-page`/SPA”同时耦合到 `has_user_worker`：`true` 会在 miss 时绕过 not-found handling，`false` 又拒绝 `run_worker_first` 的用户路由；`5.20260921.0-alpha` 仍是同一核心逻辑，无法只用原生 router config 同时满足 ADR-071。dev CLI 因此使用一个**仅开发态、同一 Miniflare 实例内**的入口 router worker，通过 service binding 分别调用原生 asset service 与用户 worker，并按 ADR-071 明确编排：匹配 `run_worker_first` 时先 worker、worker 404 再资产；否则先资产（含 `_headers`/`_redirects`/`404-page`/SPA），仅 `none` 的 miss 回退 worker。`assetsOptions` 现在只配置 `has_user_worker=false` 的原生 asset service。该适配不新增端口、进程、凭据或持久状态，不进入生产镜像，不改变生产 `user-runtime`，也不构成独立 gateway。module/KV/D1/R2、Text rules、全部 assets 语义、binding 与 hot reload 共 18 项 Bun 测试已通过。

---

## ADR-115 路由读取路径：per-host 指针 + 按版本 worker 详情 + 缓存治理 ✅

- **背景**：loader 原先是「每 5s 拉**全量投影**（含每个 worker 的完整 Version/bindings/vars）」。规模上升后问题叠加：载荷随 worker 数增长（MB 级）、服务端每次 GET 都全量扫描（无缓存）、ETag/304 从未被使用、任一改动令整个投影失效（失效被放大）、没有负缓存/单飞/退避（未知 host 与上游故障会放大请求），且非 owner 节点读本地 control cell 可能**无限期陈旧**。
- **决策**：
  1. **两段式读取**（控制面新增两个内部端点）：
     - `GET /v1/control/host?host=` → **指针视图**（小、易变）：`{host, routes:[{ns,path,worker}], workers:{"<ns>/<worker>":{active,version,bundle_sha,assets_sha}}}`；ETag **含版本号**（binding-only 部署版本号变、bundle_sha 可能不变）；无路由 → `404 + ETag`（负结果可缓存）；同名 host+path 多 ns → ns 升序 + `conflicts:true`；host 规范化（小写、去端口）。
     - `GET /v1/control/worker?ns=&worker=` → **不可变版本详情**（大）：`{version,bundle_sha,assets_sha,assets,bindings,vars,storage_id,class_storage,deleted_classes}`；**点查**实现（不建投影），带 ETag。
  2. **服务端投影缓存**：`control.Projection()` 结果按 revision 缓存；任何 control cell 的 `capturedWrite` 成功即 bump；另加 1s TTL 兜跨节点。`/v1/control/routes` 改为读缓存（保留兼容/诊断）。
  3. **loader 缓存治理**：`hostCache`（LRU 10k，TTL **10s ± 20% 抖动**，惰性过期）；**single-flight**（同 host 并发 miss 共享一次请求，含冷条目）；**stale-while-revalidate**（过期先回旧值，后台刷新经 `ctx.waitUntil` 绑定，附 5s 卡死兜底）；**ETag 重验证**（304 只刷新过期时间、不重解析）；**负缓存**（404+ETag）；**失败退避**（1s→2s→4s 封顶）+ **stale-on-error**；`envCache`（键 `ns/worker`，按指针 version 失效，无 TTL）；`bundleCache`/`assetCache`/`metaCache` 全部有界 LRU。
  4. **读新鲜性**：失去 control cell 所有权时 `cellstore.Forget` 丢弃本地 cell（capture 失所有权钩子），下次读从桶重新 hydrate —— 非 owner 不再无限期返回陈旧投影。
- **测试**：`internal/server` `TestControlHostAndWorkerEndpoints`（200/304/404、host 规范化、指针不含 bindings、revision 失效、binding-only 部署 ETag 变）；`internal/userruntime` `TestUserRuntimeHostCacheGovernance`（真实 workerd：负缓存只查一次、并发单飞只查一次、TTL 重验证带 If-None-Match、失败 stale + 退避、路由删除生效、新 host 生效）；原有 15 处测试 stub 迁移到新协议。
- **边界（如实）**：host 仍**精确匹配**（无通配符/header 路由）；`/v1/control/routes` 保留（全量投影，供诊断/兼容）。撤销窗口与 Host 扫描由 **ADR-116** 收敛；失所有权时的本地文件回收策略见 **ADR-122**。

---

## ADR-116 路由撤销加速 + 未知 host 查询节流 ✅

- **背景**：ADR-115 之后，撤销/换路由的生效窗口 = host 缓存 TTL（8–12s）；且每个**唯一未知 host** 都会打一次 cell-agent（缓存未命中）→ 恶意/异常 Host 扫描可放大成上游 QPS。
- **决策**：
  1. **变更轮询**：loader 直接轮询 `GET /v1/control/routes` 并带 `If-None-Match`（复用其内容 ETag；**不新增端点**）——稳态是 304（无 body），变更时（罕发）收到一次全量投影 body，loader **不解析、不保留**（`r.body.cancel()`），只取 ETag。
  2. loader **惰性轮询**（`REV_POLL_MS`，默认 2s，env 可调；同样的 `ctx.waitUntil` 约束）：ETag 变化 → 把所有 host 项 `expiresAt=0` → 下一次访问立即重验证（撤销窗口 ≈ 轮询间隔 + 一次请求），未变化的 host 不产生额外请求；轮询失败静默回退到 host TTL。
  3. **未知 host 节流**：冷 host 的上游查询限制为每秒 `HOST_LOOKUP_MAX_PER_SEC`（默认 100），超出直接 404 且**不写缓存**；已缓存（含负缓存）的 host 不受影响。
- **测试**：`internal/userruntime` `TestUserRuntimeRoutingScale`（真实 workerd）：40 个唯一未知 host 只产生 ≤8 次上游查询；在 **10s host TTL** 下删除路由后 <5s 内转 404（证明是变更轮询而非 TTL 生效），且 `/v1/control/routes` 确被轮询。
- **边界（如实）**：轮询每节点每 `REV_POLL_MS` 一次 `/routes`（稳态 304；变更时收到一次全量 body 但被丢弃），服务端重算由 `projectionCached` 的 1s TTL 兜住（≤1 次/秒/节点）；节流窗口内突发的大量**合法**冷 host 会短暂 404（可用 env 调大）；跨节点撤销仍受服务端缓存 TTL（~1s）约束。若日后“变更时下一次全量 body”成为负担，可在**单控制库**里放一行 `meta.rev` 计数器（控制写同事务 bump）改回 O(1) 信号。

---

## ADR-117 单控制库 + 关系表（取代 ADR-060 的按 app 分片）✅

- **背景**：ADR-060 把控制面元数据**按 app 分片**存进 `<ns>/__control__/main`（+ 全局 `__platform__/__control__/main`），用 KV 文档（key 前缀）表达层次。查询形态变化后暴露问题：① 按 host 点查需先知道 ns，跨 ns 聚合/GC 引用必须遍历每个 app 的 cell；② 控制面写必须打到该 ns control cell 的 owner；③ schema 演进要在 N 个分片重复；④“前缀扫描 + Go 里过滤”替代了本该是 SQL 的查询（唯一约束/索引都拿不到）。
- **决策**：
  1. **单控制库**：所有控制面元数据进**一个** cell `__platform__/__control__/main`（`control.Scope()`；`ScopeFor(ns)`/`GlobalScope()` 保留为指向它的别名，ns 变成**列**而不是 scope）。
  2. **关系表**：`apps / workers / versions / version_bundle_refs / routes / resources / secrets / audit / do_classes / delete_locks / gc_marks / deploy_idempotency / meta`；版本的开放字段（`bindings/vars/consumers/crons/assets`）保持 **JSON 列**（避免过度规范化）。
  3. **路由**：`routes(host, path)` 复合主键（同 host 不同 path 可来自不同 ns；`host+path` 全局唯一 → 不再有跨 ns 歧义），`NormalizeHost` 后入库；host 点查可走主键索引。
  4. **revision**：每个控制写事务自增 `meta.rev`；服务端投影缓存按 `rev` 失效（**取代** ADR-115 里只在本地写的服务端计数器），保留 1s TTL 兜跨节点；`/v1/control/routes` 的 ETag 变为 `"r<rev>"`。
  5. **GC 引用**：`version_bundle_refs` 在 deploy 时写入（自身 sha + pin 的 service 目标），`BundleRefs` 退化为 `SELECT DISTINCT sha`。
- **收益**：建/删 app 单事务**原子**；跨 ns 聚合与 GC 引用是 SQL；不再需要 key 前缀/`%08d` 补零/前缀扫描；schema 只有一份。
- **代价（如实）**：控制面变**单写者**（该 owner 不可用时 admin 写停摆到租约过期；数据面不受影响）；admin 写**不做** owner 转发（多节点部署需打 owner 或单节点）；所有 app 元数据共用一条复制链（KB/版本级，compaction 折叠）；隔离降级为 `ns` 列（控制面对租户不可达，影响有限）；**破坏性变更，未提供迁移**（pre-production，旧数据需重建）。
- **测试**：`internal/control` 全绿（app/worker/version/route/binding/DO storage/delete lock/audit/projection/GC refs 语义保持）；`internal/server` 端点测试通过；全量 **45/45** 包。

---

## ADR-118 无主（透明认领/转发）的控制面与 D1/KV 写路径 ✅

- **背景**：backend-A（KV/D1/queue/workflow/控制面）的写路径要求“本节点是该 cell 的 owner”（`Capture.Ensure` → `ErrNotOwner`），但仓内**没有生产组件发起认领**（只有 `do-supervisor` 为 DO 认领、以及脚本/压测调 `/v1/internal/claim`）；控制面写还只能打到 owner（无转发），与 ADR-060 文档承诺的“非 owner 副本把写转发给 control cell owner”不符。
- **决策**（对外表现＝“无主”：任何节点都能接、都能完成；owner/epoch 仍是**内部**单写者栅栏）：
  1. **`forwardOrClaim`**（server 写路径统一入口）：写前解析 owner —— ① 别的节点持有**活租约** → 转发给它（复用 `forwardIfNonOwner`：内部 token + loop-guard 头 + owner 地址）；② **无主或租约过期** → 本节点 claim（条件创建/CAS；抢输则重解析并转发）；③ 自己就是 owner → 本地写。认领成功后注册该 scope 的待触发定时器。
  2. **控制面写端点也在内部监听提供**（internal token），使转发有落点；admin 监听仍是操作者入口（admin token/OIDC），复用同一 handler。`s.auth` 只认 internal token → 租户/外部 token 无法触达。
  3. **接线**：KV put/delete、D1 query(非只读)/exec/batch、Queue（send/claim/ack/retry）、Workflow（create/event/pause·resume·terminate·restart/delete + 内部 step/sleep/attempt/wait/event-consume/finish 的写方法）、控制面全部写（app/resource/deploy/promote/rollback/route/secret/app delete/worker delete/gc）。gate 必须在**读 body 之前**（否则转发出去是空 body）。
- **测试**：`TestControlWriteClaimsAndForwards`（A 持有→B 的写被转发且只在 A 落地；A 释放→新节点**隐式认领**并在本地落地；覆盖**控制面 deploy/route、D1 exec、Queue send、Workflow create** 四条路径，均只在 owner 落地）；`TestControlAdminAuthAndDataPlaneIsolation` 更新为“内部监听上的控制写需 internal token，否则 401”。
- **边界（如实）**：读路径见 **ADR-120**（已转发给 owner）；queue 消费者归属与写捕获见 **ADR-119**；控制面写依赖内部监听暴露（内网 + internal token，与 `/commit`、`/claim`、`/do/invoke` 同级别，不新增租户可达面）。

---

## ADR-119 Queue 消费者归属（只有一个 owner 消费）+ 写经捕获 ✅

- **背景**：queue 消费者 runner 在**每个** cell-agent 上运行，`Queues` 列出控制面里**所有** queue 资源 → N 个节点各自在自己的本地副本上 `Claim/Ack`（重复消费 + 副本分叉），而且 runner 直接调 store，写**不经过捕获**（无复制证明）。
- **决策**：
  1. **消费者归属**：新增 `queue.OwnerGate(om)`（作为 `Runner.Filter`）：只消费**本节点拥有**的队列 cell——无主/租约过期则认领（成为唯一消费者），被别的活节点持有则跳过（由它消费）；owner 解析有 1s 缓存，所以释放/死亡后 ≤1s 被对端接管。
  2. **写经捕获（RPO=0）**：`Runner.Commit` 钩子把 `Claim/Ack/Retry/死信 Send` 都包进 `Server.CaptureWrite(scope, fn)`（`Ensure` → 事务 → `Wait(txid)`）；cell-agent 里 runner 的启动移到**捕获接线之后**，保证钩子可用。
- **测试**：`internal/queue` `TestOwnerGate`（A 认领→B 跳过；A 释放→B 接管）、`TestRunnerFilterSkipsQueues`（被过滤的队列连 claim 都不做）、`TestRunnerCommitWrapsMutations`（claim+每消息 ack 都经钩子）。
- **边界（如实）**：每个节点仍会**列出**所有队列（冷路径 1s 一次），只是过滤后只处理自己的；**读路径**（KV get / D1 只读 / workflow get）仍不认领、不转发，非 owner 上可能读到陈旧本地副本（既有问题，未在本次范围）。

---

## ADR-120 读转发给 owner + 转发重试分类 ✅

- **背景**：非 owner 的读会陈旧——复制段只**持久化**不 apply（follower spool），本地 SQLite 只在**首次打开且文件缺失**时从桶 hydrate，而桶上传是**异步批量**；这与 compat 矩阵宣称的 KV“强一致”矛盾。做法是**一个 cell 同时只由一个节点服务，其余节点把读转发给 owner**，不发 follower 读。
- **决策**：
  1. **读转发**：新增 `forwardRead(w, r, sc, preRead)`，接入 KV `get/list`、D1 **只读** query、workflow `get/list/state/step-get/wait(GET)/attempt(GET)`、控制面读（`routes/host/worker/apps/resources/releases/audit/secret-get`）。无主/租约过期的 cell 仍**本地读**（首次打开从桶 hydrate）。
     - 已知取舍：D1 `query` 目前统一走 `forwardOrClaim`（因此**无主**时的只读查询会顺便认领该 cell）——留待后续细分。
  2. **重试分类**：`forwardIfNonOwner` 最多重试**一次**，且只在两类**可证明未执行**的失败上：
     - **NeverConnected**：dial 失败（`net.OpError.Op=="dial"` / `ECONNREFUSED`，请求字节未发出）→ `refreshOwner`（`Owner.Invalidate` + 重解析）后重发；
     - **NotOwner**：对端回 **409**（已不再是 owner，未执行）→ 同样 `refreshOwner` 后重发。
     其它失败（超时/截断/5xx —— **Ambiguous**）**原样返回、绝不重发**，保 at-most-once（与 DO 的 `result_unknown` 不重放一致）。
- **测试**：`TestReadForwardingAndRetry`：A 拥有 KV cell → B 的 `kv get` 被转发并返回正确值，且 **B 本地无该 key**；stale owner 持续回 409 → **恰好 2 次尝试**并把 409 透传给调用方；死地址（dial 失败）→ 502，不挂起不循环。
- **边界/取舍（如实）**：转发让一个 cell 的**读**汇聚到 owner → 读扩展性下降（多节点下 owner 成为该 cell 的读热点）；真正的扩展解法是 **follower 读副本**（把段 apply 到本地、追平（lag < 阈值）才读本地，否则转发）——独立大项，未做。另外：既然读统一转发，`Forget`（ADR-115）已改为**可选**并由磁盘预算的 LRU janitor 兜底（ADR-122）。

---

## ADR-121 写路径所有权审计：timer / kv-expire / DO alarm 补齐 gate 与捕获 ✅

- **背景**：对内部监听的全部端点做了一次“是否改 cell、是否已 gate/捕获”的审计，发现三处漏网：
  1. `POST /v1/internal/timer/upsert`：只走 `capturedWrite`（要求 owner）但**没有 gate** → 非 owner 直接 500；
  2. `POST /v1/internal/kv/expire`：同上（TTL 到期清理定时器派发过来时会失败）；
  3. `POST /v1/internal/do/alarm/upsert`：写 `{ns}/__timer__/do` 这个 timer cell，**既没有 gate 也没有捕获** → DO alarm 有 RPO 洞，且只能由碰巧的 owner 处理。
- **决策**：三者统一走写路径家族：**先在 body 里取 scope → `forwardOrClaim(scope, preRead)` → 再解码/执行**；`do/alarm/upsert` 的 remove+upsert 收进**同一个 `capturedWrite` 事务**（并用 `captureErr` 报错）。
- **审计豁免（明确不需要 gate，已确认）**：R2 全部端点（对象在桶，无 cell）、`/v1/peer/*`（复制通道，自带 epoch 栅栏）、`/v1/internal/blob`、`/v1/internal/logs`（桶/内存）、DO owner 记录读写（`owner.Manager`，不是 cell）、`/v1/service/fetch|run`（代理到 user-runtime，不写 cell）。
- **测试**：`TestTimerUpsertForwardsToOwner`：A 持有 timer scope → B 的 `timer/upsert` 被转发且**只在 A 的 timer store 落地**；无主 scope → B 隐式认领并在本地落地。
- **边界（如实）**：`/v1/internal/do/bindings`（读控制面）与 `/v1/internal/do/objects`（桶 + 各 runtime 聚合）未纳入读转发（冷路径，本轮未改）。

---

## ADR-122 本地磁盘：文件账本 + 字节预算 + LRU 驱逐（owned 文件永不被删）✅

- **背景**：本地磁盘此前只有一条回收路径——**失所有权即删（`Forget`，ADR-115）**。后果是：本节点**一直持有**的 cell 文件**没有界**（碰过的每个 cell 一个文件、只增不减）；而放宽 `Forget`（换取同节点再拥有时的本地重开、省一次 restore）又会让文件堆积。做法是把本地文件当**纯优化**，用“**字节预算 + LRU（按 mtime）**”删。
- **决策**（两层界）：
  1. **账本**：`cellstore.DiskFiles()` 遍历 `<DataDir>/cells/<ns>/<class>/<id>.db`（连同 `-wal`/`-shm` 汇总字节与最新 mtime）；`PlanDiskEviction(files, maxBytes)` 是纯函数：按 LRU 选要删的文件，**绝不选择 `Owned && !Safe` 的文件**。
  2. **janitor**（cell-agent，**可选**：仅当 `CELLHIVE_CELL_DISK_MAX` 或 `CELLHIVE_DISK_HIGH` 非 0 才启动）：`CELLHIVE_CELL_DISK_SWEEP`（默认 60s）。字节类配置**单位为字节**，并支持人类可读后缀（`envBytes`：`512MB`=5×10⁸，`2GiB`=2³¹，`1g`/`1k` 为二进制；`CELLHIVE_COMPACTION_MIN_BYTES` 同样改用该解析，纯数字仍兼容）。**两阶段**——阶段 1 只做一次廉价目录遍历统计总量（不打开 cell、不读桶）；**只有在超预算时**才进入阶段 2，按 LRU 从最旧开始选文件，并且**惰性**判定 Owned/Safe（`SelectDiskEvictions(files, max, eligible)`），所以昂贵的 manifest 检查只为真正被考虑的文件执行、且在未超预算时完全不执行。默认（预算=0）**不扫描**。
  3. **owned 文件永不删**：本节点持有的文件可能含**尚未上传到桶**的已 ack 写，删掉会破坏 RPO=0；`Safe` 字段为将来的“L1 manifest 覆盖本地 txid 即可删”预留（未实现）。
  4. **`Forget` 变为可选**：`CELLHIVE_FORGET_ON_LOSS`（默认 **true** = 现行为）。开启磁盘预算后建议设为 false → 失所有权**保留文件**，同节点再拥有时是本地重开而不是桶 restore（读正确性已由 ADR-120 的读转发保证）。四种组合：

     | `FORGET_ON_LOSS` | `CELL_DISK_MAX`/`DISK_HIGH` | 行为 |
     |---|---|---|
     | true（默认） | 0（默认） | 失所有权即删文件；**不扫描、无 503**（原行为） |
     | true | 已设 | 失所有权即删 + janitor 兜底（非 owned 残留 / owned 已有基线） |
     | false | 已设 | 保留文件；**超预算才**按 LRU 回收；未超预算什么都不删 |
     | false | 0 | 保留文件且无预算 → **磁盘无界**（启动打 warn，不推荐） |
  5. **可观测**：`/metrics` 增 `cellhive_cellstore_disk_files`、`cellhive_cellstore_disk_bytes`；`/v1/diagnose` 的 `cellstore` 块增 `disk_files`/`disk_bytes`。
- **测试**：`internal/cellstore` `TestPlanDiskEviction`（预算算术、LRU 顺序、owned 保护、budget=0 关闭、极小预算不失控）、`TestDiskFilesAndForget`（账本内容/字节/mtime、`Forget` 删文件且幂等）。
- **边界（如实）**：owned 文件的可回收性依赖 compaction 产出 L1 manifest（未折叠的大 cell 仍靠 resident cap/rebalance/WAL 截断）；owned 文件的安全删与高水位反压见 **ADR-123**；默认（预算=0 且高水位=0）行为与之前完全一致（仅新增指标）。

---

## ADR-123 磁盘：owned 文件的安全回收（Safe）+ 高水位反压 ✅

- **背景**：ADR-122 只允许回收**非 owned** 文件 → 本节点长期持有的 cell 文件（可能很大）永不回收；且磁盘逼近上限时没有任何前馈信号。
- **决策**：
  1. **owned 文件的安全回收（`Safe`）**：新增 `replica.Manager.Covers(ctx, sc, epoch, txid)`——当该 epoch 的 **L1 manifest `max_txid ≥ 本地 cell txid`** 时，说明桶里已有覆盖全部本地写的完整基线，文件可安全删除（按需再 hydrate）。janitor 对**空闲**（`!store.IsOpen`）的 owned cell 做该判定并置 `Safe`，使其进入 ADR-122 的 LRU 预算淘汰；`Forget` 仍走 eviction 门（等在途请求排空）。
     - 排除 **control cell**（体积小；其写路径不按 eviction 门），也排除正在被写的 cell。
  2. **磁盘高水位反压**：新增 `CELLHIVE_DISK_HIGH`（字节，0=关）。超阈值时 janitor 置 `diskPressure`，服务端 `Deps.Overloaded` 生效：
     - `/readyz` → 503 `overloaded`（编排/对端不再把新流量给它）；
     - `POST /v1/internal/claim` → 503 `overloaded`；
     - `forwardOrClaim` 的**认领**分支 → 503（**不再就地认领**，让别的节点接）——且**仅当存在有放置余量的活节点**（`lease.HasShedTarget`）时才拒绝；**单节点永不拒绝**（继续认领，靠 janitor 释放空间），避免自伤式停摆；
     - 同时每轮把最多 **8 个空闲 owned cell** 释放（`om.Release`，跳过 control cell），交给 fleet 重新放置。
- **测试**：`internal/replica` `TestCovers`（无 manifest→false；`max_txid` 边界）；`internal/server` `TestOverloadedRefusesClaims`（压力下 `/readyz` 与 claim 均 503、**KV 写不认领**；解除压力后同一写认领成功）。
- **边界（如实）**：压力是**本地**信号，不会主动通知对端——转发路径仍可能把写发给已满载的 owner（owner 能服务就不影响；要彻底解决需要网关/入口感知 capacity，属后续）；`Covers` 依赖 compaction 已产出 L1 manifest，未 compaction 的大 cell 仍不可回收（靠 resident cap + rebalance + WAL 截断约束）。

---

## ADR-124 `cellhive deploy --config`：直接吃 wrangler 配置 ✅

- **背景**：`cellhive dev`（Bun）能读 `wrangler.jsonc/json/toml`，但 **Go `cellhive deploy` 只接受显式 flags**（`--kv NAME=ID`、`--consumer`…）——wrangler 用户必须手工把配置翻译成参数，这是「wrangler 兼容」最实际的缺口。
- **决策**：新增 `internal/wrangler` + `cellhive deploy <namespace> [worker|-] --config <wrangler.jsonc|json> [--env <name>]`：
  1. **解析**：jsonc（自带注释剥离 + 尾逗号处理，不引依赖）+ `env.<name>` 叠加（wrangler 语义：**绑定不继承**，其余可覆盖）；`main` 相对配置文件解析。
  2. **映射**：`kv_namespaces/d1_databases/r2_buckets/queues(producers·consumers)/services/workflows/durable_objects.bindings/ai` → `control.Binding`/`Consumer`；`vars`、`triggers.crons`、`assets{directory,binding,not_found_handling,run_worker_first}`、`migrations`、`rules/minify/keep_names/define/no_bundle/nodejs_compat`（打包输入）；`site.bucket`（legacy）→ assets 目录。
  3. **本地预检**：复用 `internal/wranglercompat.Validate`（同一批码：`unknown_field`/`compat_date_too_new`/`unknown_flag`/`unsupported_binding`/`invalid_binding`）+ `ValidateMigrations`/`ValidateCrons`，并先拉 `GET /v1/control/resources` 传入 `IsRegistered` → 提前报 `binding_unregistered`（**服务端仍会权威复验**）。
  4. **打包/上传**：无 `--bundle[-sha]` 时用内嵌 esbuild 打包 `main`（`no_bundle` 则原样上传），`assets.directory` 自动上传为同一版本 token。
  5. `--config` 与显式 binding/bundle 参数**互斥**（避免歧义）。
- **顺带修复的不一致**：`cli/src/validate.ts` 把 **`ai`（BYO 已支持）和 `workflows`（自研引擎已支持）** 误列为不支持 → 移除；`internal/wranglercompat.RejectedBindingHints["ai"]`（"planned, not enabled"）过时 → 删除；`docs/wrangler-compat.md` 的「CLI 面」列表与实际命令面对齐。
- **测试**：`internal/wrangler`（jsonc 注释/尾逗号、完整映射、env 继承、toml 拒绝、被拒 section）；`cmd/cellhive` e2e 扩展为**同一测试里再用 `--config` 部署第二版**（断言 releases=2、租户仍返回 `kv:v1`、`vars` 进了活跃版本）。
- **边界（如实）**：**不解析 TOML**（无第三方依赖；dev 支持）；不注入 `tsconfig` 路径到 esbuild；不校验 `route(s)`/`custom_domain`/`preview_urls` 语义（路由仍用 `--route` 或控制面）；不自动 provisioning（与平台一致）。

---

## ADR-125 Hyperdrive 绑定（只给连接串；平台不做连接池）✅

- **背景**：wrangler 用户常配 `hyperdrive: [{binding, id}]`。此前平台**显式拒绝** hyperdrive（`wranglercompat`、`internal/wrangler`、dev preflight、compat 矩阵一致）。CF Hyperdrive 含三件事：① 给 worker `connectionString`；② 边缘**连接池**（worker 连 Hyperdrive 本地代理，代理复用源库连接）；③ 全球加速/查询缓存。
- **决策**：实现 **①**，明确**不做 ②③**：
  1. **服务端**：`hyperdrive` 加入 `SupportedBindingKinds` 与 `ResourceKinds`（必须引用已登记资源，无 auto-provisioning），从 `RejectedBindingHints` 移除。
  2. **facade**：`workerd/platform/bindings.js` 导出 `Hyperdrive(props)` → **普通数据对象** `{connectionString, host, port, user, password, database}`（URL 解析；**不能用 WorkerEntrypoint**，否则属性走 RPC 变成 `JsRpcProperty`）；`loader.js` 的 `bindingSpec` 把 binding 的 `id` 作为连接串传入；`loader.js`/`internal.js` 的 re-export 列表补 `Hyperdrive`。
  3. **配置/CLI**：Go `internal/wrangler` 映射 `hyperdrive[{binding,id}]`；`cellhive deploy --hyperdrive NAME=CONNSTR`；dev CLI 解析 `hyperdrive[{binding,id,localConnectionString}]` → Miniflare `hyperdrives`（dev 直连本地库，符合 CF 的 local 模式）。
- **测试**：`internal/wrangler` 映射（含 hyperdrive）；`internal/wranglercompat` 的 vendor-matrix 契约测试更新；**真实 workerd** e2e（`TestUserRuntimePublicLoaderD1R2Queue`）加 hyperdrive 绑定并断言租户读到 `host:port/database/user`。
- **边界（如实）**：
  - **平台不提供连接池**：worker 拿到的就是源库连接串。**isolate 内**的连接复用由驱动提供（`pg` 的模块级 `Pool` / `mysql2` pool，其空闲连接按 `idleTimeoutMillis`/`max` 自行淘汰）——这依赖 isolate 复用，跨 isolate/跨节点不共享。
  - **共享池必须做协议终止**（pgbouncer 式），属独立项，未做（见下）。
  - **网络可达性**：租户 `globalOutbound` 仍是 **public-only**，所以公有云上的源库可直接连；RFC1918/本机地址连不上 → 私有库需要运维侧代理/池化器（或未来按绑定的出网白名单，未做）。
  - 连接串含口令，落在版本绑定元数据里（控制面 cell，随复制进桶）；建议**低权限库用户**，或运维侧用受控代理不落口令。

---

## ADR-126 loader 缓存 id 必须含版本号（否则 binding-only 部署不生效）✅

- **背景**：user-runtime 用 `env.LOADER.get(id, cb)` 缓存已加载的租户 worker（workerd 的 workerLoader 按 id 缓存 → 同一 id 复用同一个 isolate 及其模块级状态/连接池）。原 id 是 `<ns>/<worker>@<bundle_sha>`，**只编码代码、不编码 env**。于是「**只改 binding/vars、不改代码**」的部署（新版本号、同 `bundle_sha`）会命中旧 id → **复用旧 isolate 与旧 env**，新 binding/vars/secret 一直不生效，直到 isolate 被驱逐。这个问题是从“同一 isolate 什么含义”的讨论里发现的。
- **决策**：
  1. `loader.js` 的 `workerStub`：id 改为 `<ns>/<worker>@<version.number>-<bundle_sha>` —— **(代码, env) 都编码进 id**：同版本复用同一 isolate（保留模块缓存/池），新版本（哪怕 sha 相同）必然加载新的 isolate 与 env。
  2. `internal.js` 的 `loadWorker`/workflow 加载：若调用方提供 `version` 则同样把 `v<number>-` 编入 id（已支持，便于随后接线）。
- **测试**：`internal/userruntime` `TestUserRuntimeBindingOnlyRedeployTakesEffect`（真实 workerd）：v1 `MODE=v1` → 改版本号 2、**同 sha**、`MODE=v2` → 必须返回 `mode:v2`。**已用回退 id 的方式验证过该测试会失败**（旧行为返回 `mode:v1`），证明是有效回归测试。
- **边界/后续（如实）**：queue 消费者与 workflow 的**内部派发**（`internal.js`）body 当时不带 `version`（`QueueTargets`/`WorkflowTarget` 只有 `bundle_sha`）→ 已由 **ADR-127** 收口（审计时还发现 service/DO 派发有同类真 bug）。DO facet 的 env 由 session policy 决定是否重建（惰性重启），属既有语义。
- **顺带说明（回答“同一 isolate”的边界）**：同一 isolate ⇔ 同一 `(worker, version)` id 且未被驱逐；跨版本、跨部署、跨节点/实例、跨 DO 对象都不共享模块状态。

---

## ADR-127 版本号贯穿所有内部派发路径（isolate/facet 键 = worker+version+sha）✅

- **背景**：ADR-126 只修了公开入口（`loader.js workerStub`）的**读取侧**。逐条审计内部派发路径后发现两类问题：
  1. **真 bug（bindings/vars 已随版本取，但缓存键只有 sha）**：service invoke（`/v1/services/run`、`/v1/internal/invoke`）、DO invoke（`facades.js` → `/v1/do/invoke`）、DO alarm（`dispatch/doalarm.go`）。这些路径把**活跃版本的 bindings/vars** 注入 loaded env，却按 `<ns>/<worker>@<bundle_sha>` 缓存 isolate/facet → **同 sha、只改 binding/vars 的部署复用旧 env**（与 ADR-126 同类）。此外 `internal.js` 的 `dispatchServiceFetch`/`dispatchServiceRun` 连 body 里的 `version` 都没转发给 `loadWorker`。
  2. **功能缺口（version 修不了）**：queue dispatch、cron/scheduled、workflow run 的 body **完全不传 bindings/vars** → 这些 handler 的 `env` 里没有 bindings（`queue(batch, env)` 的 `env.KV` 为 undefined）。**只加 version 不产生任何可观察变化**，故本 ADR 不做该缺口（如实记录在“边界”）。
- **决策**：
  1. **凡携带 `bundle_sha` 的派发 body 都携带 `version`**：queue（`queue.Ref.Version`）、cron/scheduled（`timer.Timer.Version` + `cronEnricher`）、workflow（`WorkflowTarget.Version`）、service（`tw.Version.Number`）、DO invoke（`do` binding spec + `facades.js` payload）、DO alarm（`DoAlarmDispatcher` 一并解析 sha+version）。
  2. **读取侧统一键 `<ns>/<worker>@<version>-<bundle_sha>`**（与 ADR-126 的 `loader.js` 一致；workflow 用 `~wf` 后缀）：`internal.js` 的 `loadWorker`（含 service 两处）与 `do-runtime/host.js` 的 facet `buildKey`；**代码仍按 `bundle_sha` 缓存**（同 sha 不重复下载 bundle），但 facet/isolate 重建判断比较 **(version, sha)** → 同 sha 改 binding 会 abort 该 facet 并以新 env 重建（SQLite 保留，ADR-081/082 语义不变）。缺 `version` 时保持旧格式（兼容旧调用方）。
  3. 新增 `Projection.ActiveVersion`（返回 number+sha）；DO alarm 改用它。
- **测试（真实 workerd；两条均已用“回退修复”验证会失败）**：
  - `internal/userruntime TestUserRuntimeServiceDispatchVersionRefreshesEnv`：同 `bundle_sha`、version 1→2、vars 变 → 内部 `/v1/services/run` 必须返回新值（未修时返回旧值）。
  - `internal/doruntime TestDoRuntimeSameShaRedeployRefreshesFacet`：同上，DO facet env 必须刷新（未修时 facet 不重建）。
  - Go：`TestHTTPDispatcherPayload` / `TestHTTPDispatcherPostsTimer` 断言 body 带 `version`。
- **边界/后续（如实）**：① queue/cron/workflow 的 env 当时仍无 bindings（派发不传 spec）——已由 **ADR-128** 补齐（按版本解析 spec 注入）；② workflow run 仍按每次派发的活跃版本解析，**不按启动版本 pin**（ADR-084 语义不变）；③ DO facet 的 version 来自调用方 spec，旧调用方不传则退回 sha-only；④ `/v1/do/connect`（DO WS，目前仅测试使用）已接受 `version` 查询参数，待正式接线。

---

## ADR-128 queue/scheduled/workflow 派发注入 bindings（按版本解析 spec）✅

- **背景**：ADR-127 审计发现这三条内部派发路径的 body **不带 bindings/vars**，因此 `queue(batch, env)` / `scheduled(event, env)` / workflow `run()` 的 `env` 里没有 bindings（`env.KV` 为 undefined），与 CF 行为不同（CF 三种 handler 都拿到完整 env）。另外顺带发现：`internal.js` 每次派发都会 `await fetchBundle(...)`（在 `LOADER.get` 之外）→ 队列 1s 轮询会**每条消息重复下载 bundle**。
- **决策（照搬 DO 的成熟形态：运行时按需取 spec）**：
  1. **控制面端点泛化**：`GET /v1/internal/worker/bindings?ns=&worker=[&version=]`（`handleInternalBindings`，internal token）。`version` 缺省/0 = 活跃版本；给定则用新增的 `control.Store.VersionEnv(ns, worker, number)` **点查该版本的不可变 env**（bindings/vars/class_storage/deleted_classes）。`/v1/internal/do/bindings` 保留为同一 handler 的别名（do-runtime 继续用）。spec 构造抽成 `bindingSpecFor`（原 `doBindingSpec` 变为 version=0 的包装）。
  2. **user-runtime 在加载时补 env**：`internal.js` 的 `loadWorker`/`loadWorkflowWorker` 在 body **没有** `bindings` 键时，按 `(ns, worker, version)` 拉一次 spec 并注入 `tenantEnv`（`body.bindings !== undefined` 时以调用方为准 → service 派发继续用 cell-agent 已解析的 spec）。**失败开放**：spec 拉取失败（非 2xx/网络）就以空 bindings 加载，不让 cell-agent 抖动卡死派发。
  3. **冷路径缓存**：`internal.js` 增加有界 FIFO 缓存——bundle 按 `sha`、spec 按 `ns/worker@version`——因为 `LOADER.get` 命中时不会重跑 getter；顺带修掉“每次派发重复下载 bundle”。
  4. **id 格式统一**：内部路径的 loader id 改为与 `loader.js workerStub` 相同的 `<ns>/<worker>@<version>-<bundle_sha>`（去掉此前的 `v` 前缀）。**更正（2026-09-17 核对）**：id 字符串统一了，但 public loader 与 internal dispatch 是两个不同的 worker service，各自持有**不同的 `workerLoader` 绑定 id**（`cellhive-user-runtime-loader` vs `cellhive-user-runtime`）→ **isolate 缓存并不共享**（同一版本仍可能两份 isolate）。真正的共享需要把两条路径合并进同一个 worker service（未做）。
- **语义**：env 与 code 同版本（版本由派发方按活跃版本解析）→ 不再有“code=v2 而 env=v1”的漂移；workflow 仍**不按启动版本 pin**（每次派发解析活跃版本，ADR-084 语义不变）。
- **测试（真实 workerd，已用“回退修复”验证会失败）**：
  - `internal/userruntime TestUserRuntimeDispatchInjectsBindings`：queue/scheduled 派发不带 bindings → handler 里 `env.KV.get` 可用且 `env.MY_VAR` 正确；断言 bindings 请求带 `version=3`；spec 端点 500 时仍能派发（fail open）。
  - `internal/server TestInternalBindingsVersionPin`：`version=1` 取到 v1 的 env（不含 active 的 v2）、缺省取活跃、未知版本 200+空 spec、非法参数 400；`/do/bindings` 别名同 handler。
- **边界/后续（如实）**：① spec 每次冷加载一次（isolate 存活期间复用），版本变更会产生新的 isolate 与新 spec；② spec 缓存按 `ns/worker@version` 无 TTL、有界（64）；③ 队列/定时的 env 仍按**派发时活跃版本**解析（不做按资源 pin）；④ DO 的 facet spec 拉取现在也会带 version（去掉竞态）。

---

## ADR-129 Hyperdrive 作为注册资源（origin URL 信封加密、按名解析）✅

- **背景**：ADR-125 只做了绑定的「形状」（facade 返回 `connectionString` + host/port/user/password/database；CLI `--hyperdrive NAME=URL`；dev 走 Miniflare）。平台侧缺 CF 的 “Hyperdrive config”，导致三个问题：① `wranglercompat.ResourceKinds` 要求 hyperdrive 绑定必须引用**已注册资源**，但 `resources` 表只有 `scope`、**没有地方放 origin URL** → 连接串只能塞进 `Binding.ID`，凭据进入版本元数据/投影；② `--config` 的 `hyperdrive[{binding,id}]` 因此不可用（id 不是 URL）；③ ADR-128 的 spec 构造当时没有 hyperdrive 分支 → queue/scheduled/workflow/DO 的 `env.HYDR` 缺失。
- **决策**：
  1. **资源可携带密封配置**：`resources` 增加 `cfg_dek/cfg_nonce/cfg_ct`；新增 `CreateResourceWithConfig` + `ResourceConfig`，复用既有 envelope（随机 DEK + root key 包裹），**明文永不落库**；无 root key 时返回 `ErrNoEnvelope`，不降级明文。
  2. **API/CLI**：`POST /v1/control/resource` 接受 `connection_string`（仅 `kind=hyperdrive`，且必须 `postgres://`/`postgresql://`/`mysql://`）；`cellhive resource create <ns> hyperdrive <NAME> --connection-string <url>`。
  3. **按名解析**：`GET /v1/internal/hyperdrive?ns=&name=` 返回解密后的 origin URL（internal token，冷路径）。绑定解析规则：`Binding.ID` 含 `://` → 内联（向后兼容 `--hyperdrive NAME=URL`）；否则按资源名解析（先 `id`、再绑定名）。
  4. **两条读取路径都接线**：公开 loader（`loader.js bindingSpec`，冷路径缓存；解析失败**省略绑定**，不注入空凭据）与内部 spec（`bindingSpecFor` 新增 hyperdrive 分支，ADR-128 的 queue/scheduled/workflow/DO 路径同样生效）。
  5. **dev CLI**：`localConnectionString` 优先；否则 id 为 URL 时用它；资源名在离线 dev 无法解析 → 告警并跳过绑定（与生产的「解析失败即省略」一致）。
- **测试**：`TestResourceConfigSealed`（密文不含明文、无 key 拒绝）、`TestHyperdriveResourceAndBinding`（注册→兼容门通过、未注册仍被拒、内联兼容、internal 端点、spec 注入）、`TestUserRuntimeHyperdriveResolvesFromPlatform`（真实 workerd：env 取到解析后的 URL 而非 id；解析失败则无绑定；已用「回退修复」验证会失败）、CLI e2e 增补资源注册 + `--hyperdrive NAME=NAME`。
- **边界/后续（如实）**：① 平台仍**不做连接池**（ADR-125 边界不变：isolate 内由驱动池化，共享池需 pgbouncer 式协议终止代理）；② `resources` 新增列不被 `CREATE TABLE IF NOT EXISTS` 迁移——pre-production 按 ADR-117 直接重建 control cell；③ origin URL 的轮换在**加载时**生效（需重新部署/驱逐 isolate 才会重取）；④ 资源名约定与其它 kind 一致（资源名 == 绑定名）；⑤ dev 离线解析不了资源名，需要 `localConnectionString`。

---

## ADR-130 本地连接复用 = 在 Durable Object 内持有连接（实证 + 接线）✅

- **背景**：用户决定「不做外部组件，本地复用连接即可」（不引入 pgbouncer/池化代理）。ADR-125 曾写「isolate 内由驱动池化」，但这是**未经验证的假设**。对 pinned workerd（1.20260615.1）实测：
  1. **普通 worker 做不到**：模块作用域的 socket 对象跨请求存活，但其 streams 锁在创建它的请求上（第二次请求报 `This WritableStream is currently locked to a writer.`）→ 请求级 I/O 隔离，普通 handler 无法跨请求持有连接，「模块级连接池」不成立。
  2. **Durable Object 可以**：DO 的 fetch 里把 `connect()` 的 socket + writer/reader 存到实例字段，后续请求复用同一连接（实测 3 次请求 → 服务端只 accept 1 次、每次都正确收发）。这正是 CF「用 DO 持有长连接」的语义。
- **决策**：
  1. **本地复用的形态 = 租户自己的 DO 持有连接**（DO 生命周期内复用；重部署/驱逐/owner 变更时按 ADR-081/107 重建）。平台不提供池化服务、不加外部组件。
  2. **接线/可配**：`userruntime.Config`/`doruntime.Config` 增加 `OutboundAllow`（默认 `["public"]`，即 I-09 不变），env `CELLHIVE_TENANT_OUTBOUND=public[,private[,local]]`（`cmd/user-runtime`/`cmd/do-runtime`）。允许 `private`/`local` 才能连私有网段/本机源库——**默认关闭，开启即放松 I-09 的 public-only 底线**，由运维决定。未知类别在 Render 期直接报错。
  3. **README 里给出形态**：DO 内 `connect()` + 实例字段持有（见下方测试用的最小例子）；驱动需基于 `cloudflare:sockets`（如 `postgres`），平台不自带驱动。
- **测试（真实 workerd，已用「改成每请求重连」验证会失败）**：`internal/doruntime TestDoRuntimeHoldsConnectionAcrossRequests`——租户 DO 持有到 loopback echo 服务的 TCP 连接，两次 invoke 复用（服务端 accepts==1，返回 `n=1;echo:ping-1` / `n=2;echo:ping-2`）。
- **边界/后续（如实）**：① 跨节点/跨 isolate 的共享池**不可能**（除非协议终止代理，即之前说的 pgbouncer/自研池化器——本 ADR 明确不做）；② 连接随 DO 生命周期（驱逐/重部署/`/v1/do/abort` 断）；③ `local`/`private` 出网默认关闭；④ 普通 worker（fetch/queue/scheduled）仍无法跨请求持有连接——**不要在普通 handler 里做模块级池**，把它放进 DO。

---

## ADR-131 控制面 schema v2 + 域名/路由模型 ✅实现

> **修订注记**：本条正文里 `hosts.verify_*`/`SetHostCheck`/`PendingHosts`/`verify_state` 的**域名挑战式校验部分已由 ADR-133 删除**（登记即授权）；下方保留当时的决策记录。

- **背景**：控制面自 ADR-117 起是单个 cell 的关系表。之后确定了多租户用法（一键跑业务 + 再加一层管理页面，无 role、按 ns 授权）、域名接入（CDN 式验证）、删除可重跑、以及"cell-agent 保持干净/可单独给 cell-fuse 用"的约束，需要把表结构一次定稿。
- **决策（schema 见 `docs/control-plane.md` 的 “Schema v2” 一节）**：
  1. **软删 + purge 作业**：`apps/workers` 加 `deleted_ms`；新增 `purges`（`worker=''` = 整个 ns，状态机 pending|running|done|failed，幂等可重跑）。删除不再"行删了数据没人清"。
  2. **绑定规范化**：新增 `bindings(ns,worker,number,name,type,id,class_name,entrypoint,pin_version)` 派生表（`versions.bindings` JSON 仍是真值），`HasBinding`、service pin、GC 引用走索引。
  3. **域名与路由（传统模型 + CDN 式验证）**：
     - 内置域**只有 worker 独立域** `<ns>-<worker>.cell.internal`（`workers.host_label` 唯一），deploy/promote 时系统生成 `hosts(kind='builtin',verify_state='internal')` + `routes(host,'',ns,worker)`；**不做 ns 默认域、不做 ns 级前缀表、不做 alias 层**。
     - 自定义域：`hosts(host PK, ns, verify_state, verify_method, verify_token/target, …)` 先验证再路由；route 仍是传统 `(host,path) → ns,worker`。
     - 规则：① 自定义域必须验证（CNAME 或 TXT）才进路由 —— **（已被 ADR-133 取代：改为登记即生效，不做 DNS 校验）**；② host 唯一性（他 ns 已占用 → 409）；③ 写 route 前必须有 hosts 行且 ns 一致（并发用 `ON CONFLICT … WHERE ns=excluded.ns` 兜）；④ 删 worker 按索引清理 hosts/routes。
     - DNS 校验放控制面（Go `net.Resolver`，可配 resolver，冷路径 + 后台低频重试）。
  4. **审计归因**：`audit` 加 `actor_kind`/`on_behalf_of`/`request_id`（actor 取 JWT `sub`，静态 token 记 token 名）。
  5. **缓存/ETag**：保留全局 `meta.rev`（投影 ETag 用它）。~~新增 `ns_rev`~~ —— **已删除（ADR-135）**：无人消费，避免死代码/写放大。
  6. **迁移**：`CREATE TABLE IF NOT EXISTS` 不给既有 cell 加列 → 仍按 ADR-117 立场 **pre-production 重建**；必须在线迁移时用 `ALTER TABLE … ADD COLUMN` + 一次性回填 `bindings`。
  7. **拆分准备（不在本次范围）**：将来拆 control 时另建 `__platform__/__dispatch__/main`（cell-agent 拥有，control 写入）承载派发索引，使 cell-agent 不读控制面表；现在不建。
- **理由**：把多租户、域名归属、删除可重跑、索引化查询一次收口；cell-agent 侧零新增概念（内置域只是系统生成的路由行）。
- **实现（2026-09-17 落地）**：
  - schema 由 `schemaV2` + `migrateColumns`（`internal/control/store.go`）建立：新表 `hosts/bindings/purges`（`ns_rev` 已按 ADR-135 删除），既有表补列（`apps/workers.deleted_ms`、`workers.host_label`、`audit.actor_kind/on_behalf_of/request_id` 等），无需重建 cell。
  - 软删 + purge：`DeleteApp/DeleteWorker` 立即停路由并写 `purges` 作业；`PurgeAppRows/PurgeWorkerRows` 由 `RunPurgeLoop`（cmd/cell-agent，5s）执行，幂等可重跑。deploy 会复活软删的 worker。
  - 域名：`PutCustomHost/SetHostCheck/DeleteHost/EnsureBuiltinHost/RoutableHosts`；admin 端点 `POST/DELETE /v1/control/domain`、`GET /v1/control/domains`（**DNS 校验已移除，见 ADR-133**：注册即 verified）；`/v1/control/host` 只放行 verified/builtin；内置域空间（`*.<base>`）拒绝用户认领。
  - 授权：`internal/auth` 返回 principal（JWT `cellhive_ns` 授权、`cellhive_kind`、`sub`）；admin 面逐端点 `authorizeNS`/`requireAll`、列表按 ns 过滤（`allowedNS`/`filterByNS`）、审计写 `sub`；CLI 支持 `CELLHIVE_ADMIN_JWT`（Bearer）。**域名命令以 ADR-133 为准**：`cellhive domain add|ls|rm`（无 verify）。
  - 配置：`CELLHIVE_BASE_DOMAIN`（内置域 `<ns>-<worker>.<base>`）、`CELLHIVE_AUTO_CREATE_APP`（默认开，deploy 自动建 ns）。（`CELLHIVE_DOMAIN_VERIFY`/`CELLHIVE_DNS_RESOLVER` 已随 ADR-133 移除。）
  - 列表分页：`apps/resources/releases` 支持 `limit`/`offset`（audit 原有 `limit`/`since_ms` 游标）。
  - **路由 = 挂载点（总是剥离前缀，无开关）**：loader 选路后把路由的 path 从请求路径**无条件**去掉（query/Host 不变），资产与 worker 都看到剥离后的路径；`path=''`/`'/'` = 整个 host（不剥离）；前缀匹配按**路径段边界**（`/api` 匹配 `/api`,`/api/x`，不匹配 `/apix`）。
  - 测试：`internal/control`（软删+purge、hosts 归属/验证状态、bindings 派生）、`internal/server TestBuiltinDomainOnDeploy / TestCustomDomainVerificationGate / TestHostOwnershipConflict / TestAdminNamespaceAuthorization / TestSoftDeleteApp`、真实 workerd 的 hyperdrive/dispatch 套件与 CLI e2e 同步更新（`mustHost`）。
- **代价/边界（如实）**：① `bindings` 是派生表，deploy 时同步、可从 `versions.bindings` 重建（老 cell 的绑定查询会回退 JSON）；② 数据 cell/桶的删除仍需 owner/cell-agent 参与 —— `RunPurgeLoop` 的 hook 目前为空（保留位），known-issues 的跨节点 purge 仍未闭环；③ 自定义域各配自己的 `(host,path)`，同一 ns 的多个域名不共享前缀规则；④ 证书签发（ACME 等）与 Traefik 部署由运维配置，平台只保证「未验证不下发路由」。

---

## ADR-132 移除边缘配置下发（平台不提供外部代理功能）✅实现

- **背景**：ADR-062 提供 `GET /v1/internal/traefik`（内部 token）从路由投影生成 Traefik 动态配置（`Host()`+可选 `PathPrefix()` → service `user-runtime`；admin host → cell-agent）。审视后发现：① 路由语义**本来就在 loader**（`HostView` 解析 host/path、未验证域 404），边缘只需按 Host 转发，所以该配置对路由正确性是冗余的；② `PathPrefix` 让边缘与 loader **双重执行**前缀匹配，两者刷新节奏不同会产生短暂 skew（边缘先拦住、loader 本可服务）；③ 它是 **Traefik 专用契约**，对其他代理（nginx/envoy/caddy/云 LB）无用；④ admin host 与内置域 wildcard 都是**静态值**，不随部署变化。决定：**CellHive 不提供外部代理/边缘功能**。
- **决策**：
  1. 删除 `internal/server/traefik.go`（`BuildTraefik`/`handleTraefik`/Traefik 类型/`sanitizeName`）与路由 `GET /v1/internal/traefik`；删除配置 `CELLHIVE_ADMIN_HOST`/`CELLHIVE_ADMIN_BACKEND_URL`（随之无用的字段）。
  2. **边界归运维**：TLS/证书、host 分流、限流、admin 入口都由运维自备的 L7 反代或云 LB **静态**配置；平台只保证 loader 级行为（host 门控：未注册/未验证 404；`PathPrefix` 不再是平台的职责）。
  3. `control.RoutableHosts`（builtin + verified）保留，作为运维/未来适配器取"允许清单"的只读来源；`/v1/control/domains` 仍是域名管理的唯一入口。
  4. 文档口径更新：边缘是参考拓扑（Traefik 仅作为示例），平台不下发任何边缘配置。
- **影响/边界（如实）**：① 运维需要在边缘写 3 条静态规则：`*.<base>`（+wildcard 证书）、admin host → cell-agent `:8082`、其余（catch-all 或 host 允许清单）→ `user-runtime:8081`；② 若用 on-demand TLS，证书门控需自行按 `RoutableHosts` 或静态清单实现（平台不再代劳）；③ 不影响域名归属/验证、路由、mount 剥离等语义。
- **测试**：删除 `TestTraefikConfigFromProjection`；`TestBuiltinDomainOnDeploy` 改为断言内置 `hosts` 行与投影路由；`TestCustomDomainVerificationGate` 去掉 Traefik 相关断言（路由拒绝 + host 404 已覆盖）。全量门禁绿。

---

## ADR-133 域名不做 DNS 校验（登记即授权）✅实现

- **背景**：ADR-131 的域名校验（CNAME 指向平台 / TXT 挑战）默认开启。但实际部署里域名常由**外部网关/CDN**接管（DNS 不指向 CellHive），CNAME 模型不成立；租户也未必能改 DNS 记录。同时平台已知的用法是"单运营方 + 外部控制层"，域名的授权权威本来就在控制面/网关，不在 DNS。
- **决策**：
  1. **去掉 DNS 校验**：`cellhive domain add` 注册后**立即生效**（`verify_state='verified'`），不跑 CNAME/TXT、不后台复检；删除 `handleControlDomainVerify`、`RunDomainVerifyLoop`、`resolveHostState`、配置 `CELLHIVE_DOMAIN_VERIFY` / `CELLHIVE_DNS_RESOLVER`、CLI `domain verify` 与 `--method/--target`。
  2. **信任模型**：**能管理该 ns 的凭据 / 外部网关 = 该 ns 域名的权威**（注册即授权）；host 唯一性（`hosts.host` 主键）+ 跨 ns 409 + 审计 + `domain rm` 全部保留；内置域（`*.<base>`）仍不可用户认领；`hostRoutable` 仍要求 builtin 或 `verified`（默认路径下注册后就是 verified）。
  3. **字段一并移除**：`hosts` 只保留 `host/ns/worker/kind/created_ms`；`verify_state/verify_method/verify_token/verify_target/last_check_ms/verified_ms` 与 `Store.SetHostCheck/PendingHosts` 全部删除（`kind` 区分 builtin/custom）。将来若要做挑战式校验或外部担保，再加列即可（`migrateColumns` 支持原地补列；既有库里遗留的空列被忽略）。`hostRoutable` 改为 `hostRegistered`（查得到就路由），未注册返回 `host_not_registered`。
- **风险（如实接受）**：① **平台内抢注**：谁先声明 host 归谁，真实所有者后到会 409 → 靠运维改归属/删除（有审计）；② **证书**：边缘若做 on-demand 签发，需运维侧做允许清单（边缘配置已不归平台，ADR-132）；③ 流量劫持仅在"DNS/网关确实指向我们"时才有意义，且由网关的转发决定。
- **测试**：`TestCustomDomainRegistration`（未注册 404 → `domain add` 后立即路由 → `domain rm` 后 404）、`TestHostOwnershipConflict`（第二个 ns 声明同域 409；内置域不可认领）；删除原校验门控测试。全量门禁绿。

---

## ADR-134 2026-09-17 代码 review 修复批次（授权/水位/加载/存储/文档）✅实现

- **背景**：对 133 条 ADR 的实现做了一次三方 review（文档一致性、Go 后端、JS 运行时、存储核心），发现并修复了一批真问题。本 ADR 记录修复内容与边界。
- **Wave 1 授权缺口（安全）**：
  1. `handleControlRollback` 之前**完全没有** `authorizeNS` → 已补；`handleAssetPut`、`handleLogQuery` 同样补上（asset 的 `ns` 来自 query）。
  2. `authorizeControlPath`（`internal/server/authz.go`）在 `adminAuth` 内、**handler 之前**执行：按 query/JSON body 解析目标 ns，检查 `Allows(ns)`；无 ns 的列表端点（`/v1/control/apps`）由 handler 用 `allowedNS` 过滤；其余无 ns 的端点要求 `*`。**这保证转发到 control owner 之前已完成授权**（此前 `forwardOrClaim` 先于 `authorizeNS`，owner 侧走 internal token、无 principal，作用域被绕过）。
  3. 列表读补齐「先取回再过滤」：新增 `fetchViaOwner`（`forwardTo` 抽取），`/v1/control/apps` 用 `appsForRequest` 取回 owner 的 JSON 后在本地按 grant 过滤；internal 监听补齐 `apps/resources/releases/audit/secret(GET)/status` 读端点（此前只在 admin 面，非 owner 转发会 404）。
  4. promote/rollback 补 `invalidateBindings`；删除类 handler 资产/工作流/本地 cell 清理失败会返回 500 + `errors[]`（不再吞错）。
  5. `Config.Validate()` 拒绝空值/内置 dev 凭据（`local-admin-token` 等），除非 `CELLHIVE_ALLOW_INSECURE_DEFAULTS=1`；`make run`/bench 脚本显式放行。
- **Wave 2 RPO=0 水位（核心）**：`Cell.Tx`（bump `cell_meta.txid`）是唯一能推进 capture 水位的写路径，但 control/timer/queue/workflow 的写此前直连 `c.DB` → capture 的 txid 序列跑在 `cell_meta` 前面，`capturedWrite` 的 `Wait(txid)` 可能提前返回（ack 早于持久化）。已把 **所有被捕获的写**改为走 `Cell.Tx`（`control.tx()`、timer Upsert/RemoveByOccurrence/MarkFired/Prune、queue Send/Claim/Ack/Retry、workflow 全量写 + 迁移 DDL）。回归测试：`TestCapturedStoresAdvanceCellTxID`（repo 内四个 store 的写必须推进 txid；**回退 control.tx 会失败**）与 `TestWaitBlocksUntilCommitted`（committer 未完成时 Wait 必须阻塞）。
- **Wave 3 运行时/加载**：
  1. `loader.js` 的 DO binding spec 补 `version` → ADR-127 的 DO facet 惰性重建在 fetch 路径生效。
  2. **compatibility_date/flags 真正生效**：持久化进 `versions.compat_date/compat_flags`，`/v1/control/worker`（含 `?version=`/`?sha=`）与 `/v1/internal/worker/bindings` 下发，三个 runtime（loader/internal/do-runtime）传给 `LOADER.get`；`TestUserRuntimeNodejsCompatFlagApplied` 证明 `node:*` 只在带 flag 的版本可用（**回退修复会失败**）。
  3. **hostView 冷条目毒化修复**：冷 miss 失败不再把 `value=undefined` 永久留在缓存（失败即删除条目/按退避重试）；`TestUserRuntimeColdLookupFailureRecovers` 证明一次 500 后恢复上游即可访问（回退会持续 503）。
  4. service binding 的 ADR-104 pin 真正生效：新增 `control.Store.VersionEnvBySHA` 与 `/v1/control/worker?sha=`，loader 按 pin 的 sha 取该版本的 env 与 bundle（pin 丢失时显式报错）。
  5. `makeDO` 只在可证明未执行时回退（409/5xx/transport 一律不重放，ADR-080）。
  6. SWR/`envCache` 加 epoch/version 守卫（rev 变更后在途响应不得写回旧值）；`internal.js` spec 失败不缓存；do-runtime `seen`/`#bundles` 加界；D1 facade `batch(prepared)` 暴露 `sql/params`；dev CLI DO 用 `useSQLite:true` 并对 `ai`/`workflows` 打警告。
- **Wave 4 存储加固**：`Bucket.ConditionalDelete`（FS 严格 etag 校验 / S3 `If-Match`；do-supervisor 的 peer 视图显式不支持）→ owner `Release`/`ReleaseAs`/drain `Release` 改为条件删除，**不会抹掉别人刚 CAS 的记录**；`Store.Forget` 等 `busy` 排空（`Cell()` 尊重 `evicting`），新增 `ForgetWithVerify` 供 janitor 在栅栏内重新校验 `Covers(txid)`；`PageFetcher` 要求 L0 txid 链连续（gap fail-closed）；recovery `Collect` 任一 follower 不可达/追加失败即报错（**不再 seal**，留给重试）；生产接上 WAL 自动截断（`Cell.SafeCheckpoint` + `CELLHIVE_CELL_WAL_CHECKPOINT` 默认 64MiB）；async bucket 上传有界重试（3 次退避）并计 `dropped`；`autoscaler` 毫秒/秒比较修正；`ParseScope` 拒绝 `..`/`.`。
- **Wave 5 文档/功能回归**：`internal/wrangler/wrangler.go` 把 `hyperdrive` 移出 `rejectedKeys`（此前 `--config` 会拒绝 ADR-129 支持的配置）+ 回归断言；compatibility-matrix/control-plane/known-issues/testing 同步。
- **边界（如实）**：① `r2.List` 仍会物化键并逐个 Get 取大小（接口级分页/尺寸化待做，记 known-issues）；② async 上传失败仍是「重试后丢弃并告警」，没有持久化重试队列；③ S3 的 `If-Match` 删除在忽略该头的兼容存储上退化为无条件删除 —— **`cellhive diagnose` 已加入该探测**（stale etag 必须被拒、正确 etag 必须删除且对象消失），失败信息点名 "conditional delete (reject-stale)"；④ 授权模型仍是 ns-only（无 role），列表端点在转发前已授权但过滤发生在取回后。
- **验证**：`gofmt`/`vet` 干净、47/47 包、`make build`/`js-test`/`cli-test` 全绿；新增回归测试 9 个（4 个用「回退修复会失败」验证）。

---

## ADR-135 review 遗留问题修复批次（purge 复活/提交栅栏/WS 身份/存储栅栏/加载与鉴权加固）✅实现

- **背景**：ADR-134 修完 5 波后，review 报告里仍有未纳入的发现（含 1 个数据丢失类真问题），本 ADR 记录修复。
- **高**：
  1. **purge vs 重新部署（数据丢失）**：`Deploy` 之前不清 pending purge，且 `PurgeWorkerRows/PurgeAppRows` 无条件删除 → 删 worker/app 后重新部署，异步 purge 会把新版本/整个 app 删掉；且 `Deploy` 只清 `workers.deleted_ms`，被软删的 **app** 仍被投影过滤（新部署不可见）。修复：`Deploy` 同事务清 `workers.deleted_ms` + `apps.deleted_ms` + 删除 `(ns,worker)` 与 `(ns,'')` 的 purge 作业；`CreateApp` 也复活 app 并清 app 级作业；`Purge*Rows` 仅当实体仍软删（`deleted_ms != 0`）时才删数据（已复活 → 幂等跳过）。测试 `TestPurgeSkipsResurrectedWorker/App`（回退 Deploy 清理会失败）。
  2. **捕获提交无 epoch 栅栏**：`commitSegmentCore`（所有捕获提交的唯一出口）加 `requireOwnerEpoch`（`Owner == nil` 时跳过）→ ownership 迁移中的在途写不会被 ack 到旧 epoch。测试 `TestCaptureCommitEpochFence`（stale epoch 必须被拒、当前 epoch 通过）。
  3. **DO WS `/v1/do/connect` 身份与 invoke 不一致**：**两处** connect 处理器（顶层 `connect()` 与 Host actor 内的 `/v1/do/connect` 分支）都只解析 `namespace/worker/class/id/bundle_sha` → 类别名下 shard/hostId/facet 与 invoke 不同（WS 落到不同 facet）。修复：两处都解析 `storage_id/storage_class/version` 并复用 invoke 的身份计算；lease/hint 键也改为按 **storage_class**（一个 storage 只持一个租约）。测试 `TestDoRuntimeConnectMatchesInvokeIdentity`（别名下 WS 读到 invoke 写入的状态；claimKeys 只有一个 CounterV2 scope）。
  4. **`cellstore.DeleteNamespace` 不停 capture/不等 busy**：改为对命名空间下每个缓存 scope 走 `forget`（drain busy + Drop 停 capture + Close + 删文件）后再 `RemoveAll`。测试 `TestDeleteNamespaceDrainsAndDrops`（持 BeginRequest 时删除必须等待；Drop 被调用两次；其他 ns 不受影响）。
  5. **`cellcapture.Ensure` 持 `m.mu` 跨 `Snapshot`**：捕获创建/注册在锁内、快照与 Start 在锁外（`state.ready` 让并发 Ensure 等待初始化；失败可重试）→ 一次冷 cell 快照不再阻塞所有 cell。测试 `TestEnsureDoesNotBlockOtherScopes`（回退为锁内快照会失败）。
- **中**：
  6. `applyDOMigrations` 与 deploy 的自动 `CreateApp` 改走 `capturedWrite`（与 deploy 同等的持久性证明）。
  7. 删除死代码 `ns_rev`（无人消费；投影 ETag 用全局 `meta.rev`）；`getDOOwner` 过期即删（防长跑节点 map 无界）。
  8. JWKS 刷新移出互斥锁 + 单飞 + **在途刷新时用缓存密钥服务**（慢 IdP 不再串行化所有校验）；JWT **缺 `exp` 直接拒绝**。测试 `TestJWKSServesStaleDuringRefresh`、`TestJWTWithoutExpiryRejected`。
  9. `internal.js` bundle 缓存 32→256 并加 single-flight（同 sha 并发只取一次）。
  10. JS 小项：`_headers` 按**请求路径**匹配（SPA/200-rewrite 场景）、log-tail 的 flush 绑定 `ctx.waitUntil`（workerd 取消未 await 的后台 fetch 会丢日志）、R2 `range` 无 `length` 时省略 end（避免 `bytes=5-4`）、`version` 的 `null`/`undefined` 统一（`!= null`）、hyperdrive 解析失败不缓存、DO lease/hint 键按 storage_class。
- **低**：CLI 子命令参数不足返回 usage 而非 panic（`TestCLISubcommandArity`）；`restartDurableObjects` 失败打日志/计数（不再静默）；删除 `--config` 的死代码分支；known-issues 的 P0 清单标注为历史。
- **残余（如实）**：云端条件写/条件删除与跨主机 RTT 仍属 C 类环境验证缺口；S3 忽略 `If-Match` 时条件删除退化（`cellhive diagnose` 可自检）；async 上传无持久化重试队列。

---

## ADR-136 取消未实现的 gRPC 平面 + 环境变量/接线清理 ✅实现

- **背景**：review + 环境变量普查发现三处"纸面设计 vs 实际实现"的漂移：
  1. **gRPC 平面从未实现**：全仓无 `google.golang.org/grpc` 依赖，也只有两个 `ListenAndServe`（`:7001` REST、`:8082` admin）；`:7000` 无监听者，仅在 `cmd/cellbench` 硬编码。内部 Go↔Go 一直是 HTTP（LTX 为长度前缀二进制帧 + HTTP 101 持久流，ADR-042）。
  2. **`CELLHIVE_ADVERTISE` 默认 `127.0.0.1:7000` 指向死端口**：该值写进 owner 记录并被转发方 dial（`ownerclient.OwnerURL` 补 `http://`），默认配置下多节点转发必失败（ADR-080 已记为注意事项）。
  3. **两处死配置与一处未接线**：`CELLHIVE_GRPC_ADDR`、`CELLHIVE_CELL_AGENTS`（`ownerclient.New(seeds,…)` 零调用）无消费者；日志尾缓冲 `Deps.Logs` 在生产从未构造（`logbuf.New` 只在测试里），导致 `/v1/internal/logs` 与 admin `GET /v1/control/logs` 恒 503、`cellhive tail --worker` 不可用。
- **决策**：**取消 gRPC 平面（不是延后）**；内部统一 `:7001` HTTP。
  - 删除 `CELLHIVE_GRPC_ADDR`/`Config.GRPCAddr`；`CELLHIVE_ADVERTISE` 默认改 `127.0.0.1:7001`；
  - 删除 `CELLHIVE_CELL_AGENTS`/`Config.CellAgents`（保留 `ownerclient` 的 `Hint`/`OwnerURL`/`ForwardedHeader` 助手）；
  - `cmd/cell-agent` 接线 `Logs: newLogBuffer(cfg)`（`logbuf.New(cfg.LogBufferEntries, cfg.LogBufferWorkers)`）；
  - `CELLHIVE_DO_OBJECT_INDEX` 统一为 `true`/`1`（此前 cell-agent 用 `envBool`、do-runtime 只认 `1`，`=true` 时两侧行为不一致）。
- **验证**：`internal/config TestFromEnvAdvertiseDefault`、`cmd/cell-agent TestNewLogBufferIsWired`（缓冲往返 + 源码守卫 `Logs: newLogBuffer(cfg)`，去掉接线即失败）、`cmd/do-runtime TestEnvTrue`。
- **文档同步**：`configuration.md`、`architecture.md`、`deployment.md`、`operations.md`、`control-plane.md`、`known-issues.md`（M-02）、`cell-protocol.md` §12 改为"当前实现"，ADR-026/028 加修订注记。

---

## ADR-137 单一根密钥派生全部凭据 + 环境变量简化 ✅实现

- **背景**：环境变量普查发现两处可读性/安全问题：
  1. **角色令牌过多**：peer/internal/dispatch/log/admin/scope/do-ticket/secrets-root 共 8 个独立 secret 变量，每个都要单独配置、单独轮换；漏配其一即部分平面 401，且 dev 默认值众所周知（ADR-134 靠 `Validate()` 拒绝）。
  2. **配置面不一致**：桶凭据用自造名（`CELLHIVE_S3_ACCESS_KEY/…`）；时长混用 `_MS`/`_S`/Go duration 三种写法（数量级写错的风险）；若干变量可由其它变量推导（spool/DO 盘/runtime 目录）、成对变量可合并（日志缓冲、限流、waker TTL、drain wait）。
- **决策**：
  1. **单根密钥**：新增 `CELLHIVE_ROOT_KEY`（base64/hex ≥16B）；`DeriveCredentials` 用 HKDF-SHA256 按 `cellhive/token/<role>` 派生 peer/internal/dispatch/log/admin/scope/do-ticket/secrets-root。所有组件（cell-agent、user-runtime、do-runtime、CLI）读同一个 root 派生同一组值。`CELLHIVE_ADMIN_TOKEN` 保留为 admin 的**可选覆盖**（独立轮换）。删除 `CELLHIVE_TOKEN_*`/`CELLHIVE_SCOPE_SECRET`/`CELLHIVE_SECRET_KEY`/`CELLHIVE_DO_TICKET_SECRET`。`Validate()` 改为要求 root（dev 根仅在 `ALLOW_INSECURE_DEFAULTS=1` 下可用）。
  2. **标准 AWS 桶凭据名**：`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`/`AWS_ENDPOINT_URL`/`AWS_REGION`；删除 `CELLHIVE_S3_*` 凭据名（保留 `CELLHIVE_S3_PATH_STYLE` 覆盖，默认按是否有自定义端点推断）。
  3. **时长统一 Go 语法**：`CELLHIVE_{PEER_LATENCY,BINDING_CACHE,CELL_IDLE,CAPTURE_GROUPCOMMIT,CAPTURE_PIPELINE,DO_LEASE}` 取代对应 `_MS`/`_S` 变量。
  4. **派生/合并**：`PEER_SPOOL_DIR`=`<DATA_DIR>/peer-spool`、DO 盘=`<DATA_DIR>/do`、`CELLHIVE_RUNTIME_DIR`(`$TMPDIR/cellhive`) 派生各 runtime 目录；`WAKER_TTL`=2×interval、`DRAIN_WAIT`=DRAIN_TTL；`CELLHIVE_LOG_BUFFER=<entries>:<workers>`；`CELLHIVE_NS_RATE=rps[/burst]`；删除 `CELLHIVE_COMMIT_MODE` 兼容别名。
- **结果**：环境变量 **99 → 85**（其中 1 个 root 取代 8 个 secret），语义不变、热路径不动。
  5. **迁移安全**：`config.LegacyEnvWarnings()` 列出被删除/改名但仍被设置的变量（含 ADR-075/132/133/136 的删除项），各组件启动 WARN，避免"设了却没生效"。
- **验证**：`internal/config`（`TestDeriveCredentials` 确定/域分离/异根不同、`TestLoadRootKey`、`TestFromEnvDerivesRoleCredentials`、`TestFromEnvStorageNames`、`TestFromEnvDurationKnobs`、`TestFromEnvMergedKnobs`、`TestValidateRootKey`、`TestLegacyEnvWarnings`）、`cmd/cell-agent TestDerivedSecretKeyDecodes`、`cmd/do-runtime TestDoLeaseSeconds`/`TestRuntimeRoot`、`cmd/cellhive TestCmdCreds`、`cmd/cellhive` e2e 改用共享 root（CLI 与 loader 派生一致）、`make rpo-test` PASS（跨进程 + 工具凭据一致）。
- **代价**：破坏性改名（未发布，无需迁移）；轮换所有凭据 = 换 root（各组件同版本部署）。

---

## ADR-138 `cellhive wrangler` 前缀（wrangler 风格别名 + 配置自动发现） ✅实现

- **背景**：用户希望有 wrangler 风格用法。两条路：① 自有 CLI 加 wrangler 风格别名/翻译（低风险）；② 实现 CF API 子集让**官方 wrangler** 改 `CLOUDFLARE_API_BASE_URL` 直接连我们（零迁移，但要跟随 CF API 漂移、对拍 `content/v2` multipart 上传与 deployments 响应形状）。
- **实测（wrangler 4.133.0 源码）**：`CLOUDFLARE_API_BASE_URL`（旧名 `CF_API_BASE_URL`）存在；SDK 亦认 `CLOUDFLARE_BASE_URL`（默认 `https://api.cloudflare.com/client/v4`）；认证走 `CLOUDFLARE_API_TOKEN`/`CLOUDFLARE_ACCOUNT_ID`；`getTokenType` 调 `/user/tokens/verify`；deploy 用 `/accounts/{id}/workers/scripts/{name}`、`/versions`、`/deployments`、`/workers/assets/upload`、`/settings`、`/subdomain`、`/schedules`、`/secrets`。→ 路 ② **可行**，但 OAuth 登录走 `dash.cloudflare.com` 无法 shim（只能用 API token），且属未文档化契约。
- **决策**：先做**路 ①**：`cellhive wrangler <命令>`：
  - `deploy` 命令翻译 + `wrangler.jsonc`/`wrangler.json` **自动发现**（`-c/--config` 优先）；`--namespace` 必填（无账号概念）；`worker` 缺省取配置 `name`；
  - 命令映射：`delete→worker delete`、`versions list`/`deployments list`→`releases`、`secret`/`tail`/`rollback`/`promote` 同名；
  - 已知 wrangler flag（`--minify`/`--no-bundle`/`--compatibility-date|flags`/`--secrets-file`/`--outdir|outfile`/`--tag`/`--dry-run`/`--var`/`--alias`/`--site`/`--dispatch-namespace`…）**报错并给替代**，不让它落到 flag 包的通用错误；
  - 未支持命令（`pages`/`kv`/`d1`/`r2`/`queues`/`types`/`init`/`login`/`whoami`…）显式报错并指向 `wrangler-compat.md`。
  - 路 ② 记入本文档为**备选**（独立 spike），不阻塞 ①。
- **实现**：`main` 的 switch 抽成 `dispatch(args)` 以便 `wrangler` 命名空间翻译后重入；`translateWrangler`（纯函数，可测）+ `discoverWranglerConfig` + `wranglerDeployUnsupported` 提示表。
- **验证**：`cmd/cellhive TestTranslateWrangler`/`TestTranslateWranglerExplicitConfig`/`TestDiscoverWranglerConfig`；冒烟：临时目录放 `wrangler.jsonc` 后 `cellhive wrangler deploy --namespace acme` 走通"发现→翻译→配置加载→compat 预检"（报 `compat_date_too_new` 而非 usage/未知 flag）；`--minify` 给替代提示；`pages` 显式不支持；`versions list` 映射成 `/v1/control/releases` 请求。
- **未做**：`--dry-run`/`--var`、`secret delete/list`、资源数据面命令（`d1 execute`/`kv:key`/`r2 object`）、`types`/`init`、路 ② 的 CF API 层。

---

## ADR-139 可部署产物：镜像 + compose + kustomize manifests ✅实现

- **背景**：部署产物停在 P0 骨架且已过期：`deploy/Dockerfile` 只 `COPY go.mod`（缺 `go.sum`，有真实依赖 → 构建失败）、只构建 2 个二进制、运行镜像无 workerd/JS；compose 引用**已删除**的 `CELLHIVE_GRPC_ADDR`/`CELLHIVE_INTERNAL_TOKEN` 与死端口 `:7000`，且有 placeholder 镜像；k8s/Helm 只有文档。
- **决策**：提供"能真跑"的最小部署面：
  1. **单镜像**：多阶段；`go.mod+go.sum` + `go mod download`；构建 5 个服务二进制；运行阶段 `debian:bookworm-slim`（workerd 需 glibc）+ `ca-certificates/curl`，**构建期从 npm 拉 pinned workerd `1.20260615.1`**（`--build-arg` 可改），复制 `workerd/` JS；`/data/{state,runtime,bucket}`。
  2. **entrypoint dispatcher**：Docker 的 `command:` 只覆盖 CMD，若 ENTRYPOINT 固定为 `cell-agent` 则 compose 换服务**静默失效**；改为 `deploy/entrypoint.sh` 按短名/路径分发，缺省 cell-agent。
  3. **compose**：cell-agent(FS 桶 + `/readyz` healthcheck) + user-runtime + do-runtime（`depends_on: service_healthy`）；profile `s3`(MinIO)/`edge`(Traefik)；只要求 `CELLHIVE_ROOT_KEY`。
  4. **k8s**：`deploy/k8s/` kustomize——cell-agent StatefulSet(headless svc, downward API 的 node/advertise, PVC, grace 120, `/readyz`)；user-runtime Deployment+svc+HPA；do-runtime Deployment(headless, `emptyDir`)；ConfigMap + Secret 示例。
  5. Makefile：`docker-build`/`compose-config`/`compose-up`/`k8s-render`；`.dockerignore`。
- **验证（实测）**：`docker build` 成功；镜像内 `workerd --version` = `2026-06-15`（= pinned）；容器起 cell-agent → `/readyz` = `{"status":"ready"}`；同网络起 `user-runtime`（workerd 监听 :8081/:8088，`/`→404）与 `do-runtime`（监听 :8788，→401）；`docker compose config` OK；`kubectl kustomize deploy/k8s` 渲染 9 个文档 OK。
- **未做/残余**：Terraform/Helm；`do-supervisor` 需要"渲染→托管"启动路径（当前只有 do-runtime 会渲染 capnp）→ DO 的**无条件** RPO=0 部署形态未接线；容器默认 root（未做非 root/fsGroup 硬化）；云 registry/CI 流水线（当前无 CI）。

---

## ADR-140 do-supervisor 接线：DO 的输出门部署形态 ✅实现

- **背景**：ADR-139 遗留"`do-supervisor` 未接线"——`-config` 需要**已渲染**的 workerd capnp，而只有 `cmd/do-runtime` 会"渲染并直接 run"；因此 DO 的**无条件 RPO=0** 部署形态缺启动路径。
- **决策**：
  1. `cmd/do-runtime` 加 `-render-only`：渲染 capnp 后把路径打到 stdout 并退出（不启 workerd、不持租约）。
  2. `cmd/do-supervisor` 在 `-config` 存在（即它托管 workerd）时**接管生命周期**：新增 `-do-url`（默认 `http://127.0.0.1:8788`），跑 `RenewLoop`（10s 续租）与退出时 `Drain`——与 `cmd/do-runtime` 行为一致。
  3. 生命周期逻辑抽到 `internal/doruntime`（`PostLocal`/`RenewLoop`/`Drain`，常量 `RenewEvery`/`DrainBudget`），两个 main 共用。
  4. 部署：`deploy/do-runtime-gated.sh`（`do-runtime -render-only` → `exec do-supervisor -config … -dir <DATA_DIR>/do -do-url http://127.0.0.1:8788`，并把 `CELLHIVE_DO_GATE_URL` 指向本地门）；`entrypoint.sh` 增加短名 `do-runtime-gated`；compose 加 profile **`rpo0`**；k8s 重构为 `deploy/k8s/base` + `overlays/rpo0`（overlay 加 gated Deployment、把 `CELLHIVE_DO_RUNTIMES` 指向它、把无门 do-runtime 缩到 0）。
- **修一个 bug**：gated 脚本里 `-do-url "http://127.0.0.1${CELLHIVE_DO_PORT:-8788}"` 少个冒号 → `127.0.0.18788`，续租一直失败；容器 smoke 抓到并修。
- **验证（真实容器，同一镜像）**：
  - 生命周期：`supervising workerd … do-runtime.capnp`、`do-supervisor listening :18901`、门 `GET /status` 返回 `dir/facets/files`、续租失败数 **0**、SIGTERM 时 drain 正常；
  - **DO 端到端经输出门**：cell-agent + `do-runtime-gated` 起栈，`bundle put` 一个 `extends DurableObject` 的 Counter，直接 `POST :8788/v1/do/invoke`（internal token）四次 → `count:2/3/4/5`（状态跨调用持久）；
  - **落桶**：`cells/workerd/__do__/<hosthash>.sqlite/ltx/e1/*.ltx` + `0.snapshot` + `owner.json`（14 个对象）与 `cells/acme/__do__/counter~Counter~shard8/owner.json`（owner 记录 `node=ch-gated, address=ch-gated:8788, epoch=1`）。
  - 单测：`internal/doruntime TestRenewLoopPostsWithToken`/`TestRenewLoopReportsErrors`/`TestDrain`。
- **残余**：非 root 硬化（A2）、CI（A4）；真实云/多主机属 C 类。

---

## ADR-141 部署加固：非 root + Helm chart + CI 门禁 + 编排完整性 ✅实现

- **A2 非 root**：镜像固定 `USER 65532:65532`（/data 与 /app 归该 UID），k8s 所有工作负载加 `securityContext{runAsNonRoot,runAsUser/Group,fsGroup:65532}`。验证：容器内 `id` = 65532:65532，cell-agent `/readyz` ready，gated DO invoke 正常且 LTX 落桶，续租失败 0。
- **A3 Helm chart**：`deploy/helm/cellhive`（`helm lint` 0 failed、`helm template` 16 docs）。values 控制镜像/副本/资源/存储/`doRuntime.gate`（true → `do-runtime-gated`）/`existingSecret` 或内联 `rootKey`/PDB/NetworkPolicy/ingress；缺 rootKey 时模板报清晰错误。
- **A4 CI 门禁**：`scripts/ci.sh`（`make ci`）一键跑 gofmt/vet/test/build/js-test/cli-test/perf-test/rpo-test/s3-test(MInIO)/docker-build/compose-config/k8s-render/helm-lint，工具缺失则 skip（`REQUIRE_ALL=1` 转硬失败），末尾汇总 PASS/FAIL；`.github/workflows/ci.yml` 分 go/js/cli/deploy 四个 job（js job 装 pinned workerd，deploy job 跑镜像与编排校验）。
- **A5 编排完整性**：kustomize base 与 Helm 都提供 ServiceAccount（关闭 SA token 自动挂载）、PDB（minAvailable 1）、NetworkPolicy（cell-agent/do-runtime 仅同 release pod 入站；user-runtime 出网允许 DNS + 公网、排除私网段；admin 入站可用 values 追加）、startup/readiness/liveness 三类探针。
- **k8s 结构**：`deploy/k8s/base` + `overlays/rpo0`（gated），`deploy/k8s/kustomization.yaml` 作为兼容入口；`kubectl kustomize` 与 `helm template` 都离线验证。
- **验证**：`bash scripts/ci.sh` → **GATE: PASS（13/13）**（gofmt/vet/test 49 包/build/js-test/cli-test 8 pass/perf/rpo 1200 keys/s3-test/docker-build/compose-config/k8s-render/helm-lint）。
- **残余**：Terraform 未做（orchestration 以 Helm+kustomize 提供）；真实云/多主机/多云供应商仍属 C 类。

---

## ADR-142 purge 闭环：数据侧清理接线（桶 + 本地副本 + DO 段） ✅实现

- **背景**：ADR-131 的 `purges` 作业只删控制面行；`RunPurgeLoop` 的 hook 一直是 `nil`（`cmd/cell-agent` 传 `nil`），删除 worker/app 后**数据侧永不清理**（桶前缀、各 owner 的本地 cell、DO 段），known-issues 记为"跨节点 purg/hook 仍为空"。
- **决策**：
  1. **hook 可续跑**：`control.PurgeHook = func(ctx, ns, worker) (done bool, err error)`；`done=false` 表示本轮只做了有界删除、**保留 pending**（不删控制行、不计失败），下一 tick 继续；`err` 才计 attempts。`RunPurgeLoop` 据此支持大 namespace 分多轮清理。
  2. **`internal/purge.Purger`**（Bucket + LocalCells 接口）：worker=="" 时 `cellstore.DeleteNamespace(ns)` + 删桶 `cells/<ns>/`、`assets/<ns>/`；worker!="" 时 `ForgetPrefix("<ns>/__do__/<worker>~")` + 删桶该 worker 的 DO 段与 `assets/<ns>/<worker>/`。
  3. **语义决定**：**worker 删除清理该 worker 的 DO 存储**（对齐 CF：DO 属于 worker）；KV/D1/R2/Queue 资源是独立的，不随 worker 删除（对齐 CF 的 namespace/bucket 语义）。
  4. **跨节点**：每个 cell-agent 都跑 `RunPurgeLoop`，hook 在**每台**执行 → 各 owner 的本地副本被 drain（`DeleteNamespace`/`ForgetPrefix` 走 `forget`：等在途 → Drop 停 capture → Close → 删文件）+ 桶删除幂等。
  5. `cellstore.Store.ForgetPrefix(ctx, scopePrefix)` 新增（按 scope 字符串前缀 drain+drop）。
- **修一个可移植性 bug**：FS 桶 `List(prefix)` 需要**真实目录**，而 DO scope 是 `<ns>/__do__/<worker>~<class>~shard<N>`（worker 边界在段内）→ 以 `.../api~` 为前缀列不出任何键。改为列 `cells/<ns>/__do__/` 目录再按 `worker~` 过滤（S3 与 FS 都正确）。
- **验证**：
  - 单测：`internal/purge`（app 级、worker 级只删自己、`MaxDeletes` 分轮 `2+2+1`、非法 ns/worker）、`internal/cellstore TestForgetPrefixDropsMatchingScopes`、`internal/control TestRunPurgeLoopResumableHook`（首轮不完成→job 保留；完成后删行）。
  - 容器端到端（真实镜像）：删 `acme/api` 后 `cells/acme/__do__/api~Room~shard1/*` 与 `assets/acme/api/*` **被删**，`api2~`、`__kv__`、`cells/other/*` **保留**，审计出现 `purge.done`；删 app 后 `cells/acme/*` 与 `assets/acme/*` **全删**，`cells/other/*` 与平台控制 cell 保留。
- **残余**：列出目标前缀时仍会物化键列表（冷路径，允许 List；bucket 接口无游标分页）——单轮删除量已受 `MaxDeletes` 限制。

---

## ADR-143 async 上传：持久化重试队列（write-ahead spool） ✅实现

- **背景**：async（`CELLHIVE_BUCKET_WAIT=false`，RPO>0）桶上传只有 3 次退避重试，失败即 `dropped`（已知问题：**无持久化重试队列，进程重启后丢失**），而它承载的是"已被 ack 的写的桶副本"。
- **决策**：
  1. `internal/upload.Spool`：**写前落盘**的持久队列，每项一个文件 `<DATA_DIR>/upload-spool/<id>.spool`（tmp + rename 原子；有界条目数，满则 `Append` 报错；坏文件跳过不阻塞）。
  2. `Batcher.SetSpool(sp)`；async `Enqueue` 先落盘再入队；批次上传成功 → 删除对应 spool 文件；3 次重试仍失败 → **保留文件、计 `deferred`**（不再丢）；只有**落盘失败 + 上传失败**才计 `dropped`（`spool_errors` 单列）。
  3. `Start` 起后台 replay：每 5s 按 id 升序重传 spooled 项，成功即删。**重放是先序无关的**：段 key 由 txid 区间命名（`ltx.BatchName(first,last)`），PUT 幂等。
  4. RPO=0 路径不变：`EnqueueWait`（bucket-wait）与 follower `peerUploader` 由调用方等待/拿错误，不落 spool。
  5. 可观测：`/metrics` 增 `cellhive_upload_{batches,segments,dropped,deferred,replayed}_total` 与 `cellhive_upload_spool` gauge（掉线/积压可见可告警）。
- **验证**：`internal/upload` 新增 `TestSpoolDefersFailedUploadAndReplays`（失败→deferred+spool=1，恢复后 replayed 且 spool=0）、`TestSpoolReplaysAfterRestart`（首个进程留下 2 项，重启后新 batcher 重放清空）、`TestSpoolFullCountsSpoolError`（满则 spool_errors）、`TestSpoolRoundTrip`（编解码/顺序/坏文件跳过/删除）；既有 batcher 测试（合批/背压/有序/分片）保持全绿。容器：`/metrics` 暴露 upload 指标、`/data/state/upload-spool` 已建。
- **残余**：spool 写入未 fsync（**进程崩溃安全**，非断电安全）；bucket 接口无游标，重放一次性 `Load` 全量。

---

## ADR-144 service binding ACL + 跨命名空间 service binding ✅实现

- **背景**：service binding 只有版本冻结（ADR-104），**没有允许列表**；目标是"谁能 bind 到哪个 worker/entrypoint"，且跨 ns 时目标解析直接用**调用方 ns**（`proj.WorkerFor(callerNS, target)`），跨 ns 目标永远 404（隐性不支持）。
- **决策**：
  1. **目标侧允许列表**：新表 `service_acls(ns, worker, caller_ns, created_ms)`；控制面 `POST/DELETE /v1/control/service-acl`、`GET /v1/control/service-acls`；CLI `cellhive service-acl add|ls|rm`；审计 `service_acl.put/delete`。
  2. **同 ns 默认允许（不回归）**；**跨 ns 需目标 ns 授权**。
  3. **目标语法**：binding `id` 为 `worker`（同 ns）或 `ns/worker`（跨 ns）；`splitServiceTarget` 校验形态，非法 → 部署期 `invalid_service_target`。
  4. **部署期拒绝**：`validateDeploy` 增 `validateServiceBindings`——跨 ns 无授权 → `deploy_rejected` + finding `service_binding_denied`（带修复命令）。
  5. **运行期再校验**（纵深防御）：`serviceTarget` 在 `handleServiceRun/HandleServiceFetch` 中按 `target_ns` 与调用方 ns 比对，缺授权 → 403 `service_binding_denied`；撤销授权立即生效。
  6. **协议**：`ns` 查询参数保持=**调用方 ns**（与 scope token 绑定，`scopeAuth` 前置校验），跨 ns 目标放 `target_ns`；绑定 spec/投影多带 `ns`=目标 ns、`caller_ns`=调用方，facade 同时下发两者（`bindings.js` 与 `loader.js` 两处 spec 构造都已更新；native JSRPC 路径用目标 ns 加载）。
- **验证**：
  - 单测：`internal/control TestServiceACL`（CRUD/幂等/隔离/非法 caller）、`internal/server TestServiceBindingACLDeployGate`（同 ns 通过；跨 ns 无授权 400 `service_binding_denied`；非法 400；授权后 200；撤销后再 400）、`TestServiceFetchCrossNamespaceACL`（运行期 403 → 授权 200）。
  - 真实 workerd：`internal/userruntime TestUserRuntimeCrossNamespaceServiceBinding`（acme/web 绑 `team/api`，JSON 路径实发 `ns=acme&target_ns=team&worker=api`，目标返回 `svc:from-B:hi`）。
- **残余**：entrypoint 级 ACL 未细分到方法（当前粒度为 target worker）。

---

## ADR-145 R2 list：cursor 分页 + 尺寸化（不物化整个前缀） ✅实现

- **背景**：`r2.List` 之前要么 `ListSizes` 整个前缀（一次物化所有键），要么回退 `List`+逐键 `Get` 取大小；对租户大桶放大内存/IO，且 facade 不返回 `truncated`/`cursor`（R2 list 的 API 语义缺失）。
- **决策**：
  1. 新增可选能力 `bucket.PagedLister`：`ListPage(ctx, prefix, after, limit) ([]ObjectInfo, nextCursor, error)`——有序、游标（`after` **独占**）、只带尺寸/etag、不读对象体。
  2. **FS**：走文件树但只保留 `limit+1` 个 `>= after` 的键（有界选择），返回 `limit` 个 + 下一个游标；**S3**：单次 `ListObjectsV2{Prefix, StartAfter, MaxKeys=limit}`，`IsTruncated` 时游标=最后键——每页一次有界调用，无 per-object GET。
  3. `r2.Store.ListPage(ctx, ns, bucket, prefix, cursor, limit) ([]Object, next, error)`；游标对外是**用户键**（内部前缀已剥离）；无 `PagedLister` 时回退 `SizeLister`（排序切片）或 `List`+`Get`；`List` = 第一页（向后兼容）。
  4. `/v1/r2/list` 接受 `cursor`（兼容 `start_after`），返回 `{objects, truncated, cursor?}`；facade `R2Bucket.list` 透传 `cursor`/`startAfter` 并返回 R2 语义的 `{objects, truncated, cursor}`（不再硬编码 `truncated:false`）。
- **验证**：`internal/bucket TestFSListPage`（独占游标/有序/末页无游标/越界）；`internal/r2 TestR2ListPaging`（2+2+1 三页、尺寸正确、桶隔离、`List`=首页）；`internal/server TestR2ListCursorPaging`（HTTP 层 limit=2 → `truncated`+`cursor=d/b` → 第二页收尾）；**真实 MinIO**：`make s3-test` 中 `TestS3BucketIntegration` 增 ListPage 断言（`StartAfter` 分页 a..e、尺寸/etag、终止）。
- **残余**：FS 后端为了 etag 仍会读对象体（dev/test 后端，S3 无此问题）；未实现 `include`/`httpMetadata`/`delimitedPrefixes` 等 R2 list 细节。

---

## ADR-146 W3C trace context（traceparent）传播 ✅实现（有边界）

- **背景**：`observability.md` 声称"内部调用透传 trace context（traceparent）"，但代码里 0 处实现（文档与实现漂移）。
- **决策**：实现 W3C `traceparent` 传播（自实现，不加 OTel 依赖）：
  1. **入口**：user-runtime loader 给每个进入租户的请求补 `traceparent`（客户端已带则原样透传，否则生成 `00-<32hex>-<16hex>-01`）。
  2. **租户 handler**：平台 wrapper（`queue-wrapper.js`）在 `fetch`/`handleQueue`/`handleScheduled`/`callMethod` 期间把 traceparent 放到 isolate 全局，供同 isolate 的 facades 读取。
  3. **facades → cell-agent**：`facades.js` 的 pfetch 与 `bindings.js` 的 `call()` 自动带 `traceparent`（同 isolate 路径）；native service RPC 通过 `callMethod(..., traceparent)` 传递。
  4. **cell-agent 出向**：把调用方的 `traceparent` 放进 service fetch/run 的 dispatch body（→ user-runtime 再带进目标 handler）与 DO invoke body（→ do-runtime host actor 合并进租户 DO 请求头）。
  5. **dispatch/异步**：queue/timer 的 body 支持 `traceparent` 字段（dispatch 侧有则透传，handler 通过 batch/event 的隐藏字段读取）。
- **验证**：`internal/userruntime TestUserRuntimeTraceContextPropagation`（真实 workerd：租户看到**生成**的 traceparent；客户端给定值时**原样保留**）；`internal/server TestServiceFetchPropagatesTraceparent`（service dispatch body 带调用方 traceparent）、`TestDOInvokePropagatesTraceparent`（do invoke body 带 traceparent）。
- **残余（如实）**：**props-bound facades**（KV/D1/R2/Queue 走 `ctx.exports.*`，运行在 loader isolate，与租户 isolate 不同）拿不到每请求上下文，因此它们的 cell-agent 调用目前**不带**租户请求的 traceparent —— 需要把 trace 作为每次调用的参数或共享 wrapper 才能补齐；后台 queue/timer 由调度循环发起时无入站 trace（默认新 trace）。**无 OTLP 导出/采样**（仅传播）。

---

## ADR-147 控制面 secrets 管理：delete/list ✅实现

- **背景**：secrets 只有 put/get（ADR-059），运维无法删除或清点；`wrangler secret delete|list` 无对应能力。
- **决策**：
  1. `control.Store.DeleteSecret(ns, worker, key, actor)`（幂等；审计 `secret.delete`）与 `ListSecrets(ns, worker) []SecretMeta`（**只返回 key/updated_ms，绝不返回密文或明文**）。
  2. 控制面 `DELETE /v1/control/secret`（`forwardOrClaim` + `capturedWrite`）与 `GET /v1/control/secrets`（`forwardRead`），均 `authorizeNS`。
  3. CLI `cellhive secret delete <ns> <worker> <KEY>` / `secret list <ns> <worker>`；`cmdSecret` 的 arity 改为**逐子命令**校验（修掉 `list` 被 4 参数守卫误拒的 bug）。
- **验证**：`internal/control TestSecretDeleteAndList`（列表只含 key/updated_ms、删除后 get 404、其他 worker 不受影响、幂等）；`internal/server TestSecretDeleteAndListEndpoints`（HTTP 列表**不泄露** value/`wrapped_dek`、DELETE 后 404、审计含 `secret.delete`）；容器 CLI 冒烟：put×2 → list（两条）→ delete → list（一条）+ 审计 `secret.put/delete`。
- **残余**：无版本/别名（密钥名即唯一键）；无轮换 API（轮换=覆盖 put）。

---

## ADR-148 CLI 兼容补齐：dry-run / --var / --secrets-file + 兼容命令组 ✅实现

- **背景**：wrangler 迁移路径缺 `deploy --dry-run`/`--var`/`--secrets-file`，且 `versions`/`deployments`/`triggers`/`workflows`/`queues`/`types`/`init`/数据面命令在 `cellhive` 上无对应或只有 "unknown command"。
- **决策**：
  1. **服务端 dry-run**：`deployReq.dry_run` → 跑完整 `validateDeploy`（兼容门 + bundle 存在性 + service-binding ACL）后**直接返回**，不写迁移/不建 app/不建版本、不设路由。
  2. **CLI**：`deploy --dry-run`（同时跳过 assets 上传与路由副作用；bundle 仍上传，内容寻址可由 GC 回收）、`deploy --var NAME=VALUE`（可重复，覆盖 config vars）、`deploy --secrets-file <JSON {KEY:value}>`（部署**前**逐个 `secret put`，dry-run 时跳过）；`--config` 与显式 flags 的互斥检查不含这三个。
  3. **兼容命令组** `cmdCompat`（顶层 `versions|deployments|triggers|workflows|queues|types|init|d1|kv|r2`）：
     - 映射：`versions list`/`deployments list|status` → `releases`；`triggers list` → `releases` 取 crons；`workflows list`/`queues list` → `resource list --kind`；
     - **结构化拒绝 + 替代说明**：`versions upload|view`、`workflows describe|instances`、`queues purge`、`types`、`init`、`d1`/`kv`/`r2`（无数据面 CLI，指引 `cellhive dev`/binding）。
  4. **wrangler 前缀**：`--dry-run`/`--var`/`--secrets-file` 从"不支持"改为透传；`versions|deployments|triggers|workflows|queues|types|init|d1|r2`/`kv*` 转发到兼容组；`pages|vectorize|dev|login|…` 保持明确"不支持"。
- **验证**：`internal/server TestDeployDryRun`（合法 dry-run → 200 `dry_run:true` 且 **releases 为空**；非法 → 400 `missing_bundle` 且仍无版本）；`cmd/cellhive TestTranslateWranglerCompat`（三个 flag 透传、compat 子命令转发）、`TestCmdCompatRejections`（d1/kv/r2/types/init/versions upload/view/queues purge 的结构化消息）；容器冒烟：合法/非法 `--dry-run`、`--var`（deploy 响应含 vars）、`--secrets-file`（secret list 可见）、`triggers list`/`versions list`、`d1 execute` 结构化拒绝。
- **残余**：`dry-run` 仍会上传 bundle（内容寻址、无状态副作用）；无 `versions view`/`workflows instances`（需新读端点）；无数据面 CLI（设计决定，指引 dev/binding）。

---

## ADR-149 C 类环境的本地替代验证 + 残余清单 ✅审计完成

- **背景**：目标要求"C 类环境（真实云/多主机/真实 IdP/编排）用本地替代覆盖，不可替代的如实列残余"。
- **审计结论（本地替代已覆盖）**：
  1. **对象存储条件写/条件删除** → 本地 MinIO：`make s3-test`（`TestS3BucketIntegration` 含 create/reject/CAS/reject-stale/ranged/ListPage）+ `TestDiagnoseDetectsConditionalDeleteIgnored` + `cellhive diagnose`。
  2. **多进程崩溃/接管/RPO=0** → `make rpo-test`：`RPO-ZERO: PASS (1200 keys 精确)` + restoreverify `integrity:ok`。
  3. **跨节点 RTT/流水线** → `internal/peer TestLatencyTransportInjectsRoundTripDelay`（`CELLHIVE_PEER_LATENCY` 往返 2×）。
  4. **慢/轮换/无 `exp` 的 OIDC** → `httptest` mock IdP：`TestJWTBearerRS256`、`TestJWTWithoutExpiryRejected`、`TestJWKSServesStaleDuringRefresh`。
  5. **owner 时序/分区** → `internal/owner` 确定性仿真；**供应商契约** → `TestVendorMatrixContract`。
- **残余（无环境，不可替代）**：真实云端对象存储的条件写与 `If-Match` 语义/延迟；真实第二主机 RTT、跨 AZ、多机混沌；真实 IdP；真实编排（autoscaler 仅信号）；HTTPS 证书/通配符（运维侧）。
- **落地**：`docs/testing.md` 新增"本地替代（已覆盖）/残余（未覆盖）"两张表并附命令证据；`docs/known-issues.md` 保持同一残余口径。

---

## ADR-150 根密钥来源：env + 文件（KMS 接缝）+ mTLS 取舍 ✅实现（KMS 云侧不做）

- **背景**：B10 要求"根密钥 KMS 抽象（至少 env + 文件两种 provider，可选云 KMS 占位）；评估并实现或明确记录 mTLS/内部 CA 取舍"。
- **决策**：
  1. **文件 provider**：`CELLHIVE_ROOT_KEY_FILE`（Docker/K8s secret 挂载；读取后 trim）。解析顺序 `CELLHIVE_ROOT_KEY` → `CELLHIVE_ROOT_KEY_FILE` → dev（仅 `ALLOW_INSECURE_DEFAULTS`）；文件不可读/为空 → 空串 → `Validate()` 失败关闭。实现集中在 `config.LoadRootKey()`（全组件共用，ADR-137 的派生入口）。
  2. **云 KMS = 接缝、不实现**：`LoadRootKey` 即"取根密钥字节"的接缝，接入 Vault/AWS KMS/Aliyun KMS 只需在该处实现一次；无云凭据环境，记为残余。
  3. **mTLS/内部 CA：决定暂不做**（理由）：需要 CA/签发/轮换/信任分发整套 PKI（当前固定集群无 PKI），而内部角色隔离已是**独立派生 secret + 常量时间比较 + 私网隔离**；收益边际、运维面大。触发再评估的条件：跨信任域互通或合规要求密码学身份。记录在 `security.md`。
- **验证**：`internal/config TestLoadRootKeyFromFile`（文件 trim、env 优先、缺失文件失败关闭 + `Validate` 拒绝、`ALLOW_INSECURE` 兜底）；容器实测：挂载文件启动 → `/readyz` ready，且容器内 `cellhive creds internal` 与 env 方式**逐字节一致**（同一 root 派生）；缺失文件启动 → `invalid config ... CELLHIVE_ROOT_KEY is required`。
- **残余**：云 KMS provider（Vault/KMS）未接；mTLS 未做（有意）。

---

## ADR-151 scaling-and-ha 定稿：autoscaler 冷却 + 跨 AZ follower 放置 ✅实现

- **背景**：`scaling-and-ha.md` 的"待细化"（autoscaler 阈值/冷却、rebalance 批大小/节流、多 AZ 放置）一直没落地；autoscaler 每个 interval 都会给出（可能相反的）动作，rebalance 只有间隔+批大小，跨 AZ 复制没有概念。
- **决策**：
  1. **autoscaler 冷却**：`Advisor.Cooldown` ← `CELLHIVE_AUTOSCALE_COOLDOWN`（默认 5m，0=关）。动作**变化**在窗口内被抑制为 `hold`/`cooldown`，并返回 `cooldown_remaining_ms`；动作不变不受影响。阈值仍为目标 `CELLHIVE_CELLS_PER_NODE`（默认 100）、`AUTOSCALE_MIN/MAX`，压力/`shed_cells` 触发至少 +1。
  2. **rebalance 节流定稿**：间隔 `CELLHIVE_REBALANCE_INTERVAL`（默认关）+ 单轮批大小 `CELLHIVE_REBALANCE_MAX_MOVE`（默认 32，仅空闲且自有 cell）。
  3. **跨 AZ/故障域**：新增 `CELLHIVE_PLACEMENT_AZ`（本节点 AZ）→ `lease.NodeLease.AZ`（随 lease 发布）→ `selectFollowers(leases, self, selfAZ, now, max)` 偏好**不同 AZ** 的 follower（跨 AZ 在前、同 AZ 兜底、排除自己/过期/无 peer_url）。一致性模型不变（仍是 follower fsync + quorum 1）。
- **验证**：`internal/autoscaler TestAdvisorCooldown`（首次 scale-up；窗口内反向信号 → `hold/cooldown` + 剩余时间；窗口后允许 scale-down；Cooldown=0 保持旧行为）；`internal/server TestSelectFollowersPrefersOtherAZ`（跨 AZ 优先、无 AZ 配置时按序、仅同 AZ 兜底）；`internal/lease` 现有测试通过（新增字段 omitempty）。
- **残余**：真实多 AZ 拓扑/网络延迟未见（C 类环境）；autoscaler 仍只产信号（外部编排）。

---

## ADR-152 timers-and-dispatch 定稿：批/重试/DLQ 默认参数 + waker 退避 + timer cell 策略 ✅实现

- **背景**：`timers-and-dispatch.md` 的"待细化"（批处理/重试/DLQ 默认参数、timer cell resident/pinned、waker 退避）未落地；queue/timer/waker 的批大小与 fired TTL 有硬编码默认（timer/waker 256、24h），waker 出错后固定间隔重试（无退避）。
- **决策**：
  1. **Queue**：`CELLHIVE_QUEUE_BATCH`/`CELLHIVE_QUEUE_LEASE`（0=store 默认）、`CELLHIVE_QUEUE_RETRY_DELAY`（默认 30s）；重试上限与 DLQ 仍由 per-consumer 配置（ADR-072/112）决定；语义 at-least-once、owner 独占消费、每次变更走捕获证明（ADR-119，不变）。
  2. **Timer**：`CELLHIVE_TIMER_BATCH`（默认 256）、`CELLHIVE_TIMER_FIRED_TTL`（默认 24h）。
  3. **Waker**：`CELLHIVE_WAKER_BATCH`（256）、`CELLHIVE_WAKER_FIRED_TTL`（24h）、**`CELLHIVE_WAKER_BACKOFF_MAX`（默认 1m）**：连续错误时延时 `interval·2^fails` 封顶；成功复位；空闲不退避。
  4. **timer cell 策略（决定）**：**不 pinning**，timer cell 与其它 cell 同为可驱逐（`MAX_RESIDENT_CELLS`/`CELL_IDLE`/盘预算统一管理）；正确性不依赖驻留（权威在 cell + 桶 `wake/` 索引，waker 按索引定位、按需重开）。
- **验证**：`internal/waker TestBackoffDelay`（1→2×→4×→封顶、max≤base 关闭、base=0 回退 5s）；`internal/config TestDispatchDefaults`（timer/queue/waker 默认值 + 覆盖生效）；现有 queue/timer/waker/`internal/dispatch` 测试保持全绿。
- **残余**：无 timer 专用配额/驻留（有意）；waker 退避为进程内状态（重启复位）。

---

## ADR-153 兼容矩阵收口：flag 精确列表 + 框架验收 + D1 sessions 拒绝 ✅实现

- **背景**：`compatibility-matrix.md` 的"待细化"（`compatibility_flags` 精确列表、框架适配逐项验收）与 D1 "无 sessions/bookmark" 都只是口头结论；探索框架验收时发现**真 bug**：平台 bundler 从不把 `cloudflare:*`/`node:*` 标为 external，凡是 import `cloudflare:workers` 的框架预构建产物（OpenNext 等）在 `deploy --config` 时**打包失败**。
- **决策/修复**：
  1. **bundler 修复**：`Build` 默认 `--external:cloudflare:*`，`NodeJSCompat` 时再加 `--external:node:*`（workerd 运行时提供；否则 esbuild unresolved import 直接失败）。回归测试 `TestBuildExternalizesPlatformModules`（含 node:* 在无 nodejs_compat 时必须失败关闭）。
  2. **flag 列表与 pin 绑定**：`wranglercompat.PinnedWorkerdVersion`（= `workerdbin.PinnedVersion`）；`TestKnownFlagsMatchDevCLI` 解析 `cli/src/validate.ts` 的 `KNOWN_COMPAT_FLAGS` 与 Go 列表逐项比对（单边新增即失败）；`TestPinPairsWithCompatibilityDate` 把 pin 与 `MaxCompatibilityDate=2026-06-22` 绑成一个决定。
  3. **框架验收**：`internal/wrangler TestFrameworkPrebuiltLayouts`（OpenNext/SvelteKit/Astro 三种预构建布局：配置映射 + 平台打包；esbuild 缺失则 skip）。
  4. **D1 sessions/bookmarks 显式拒绝**：`bindings.js` 的 `D1Database.session()`/`withSession()` 抛 `d1 sessions are not supported by CellHive (no read replication or bookmarks)`；真实 workerd e2e（`TestUserRuntimePublicLoaderD1R2Queue`）断言该错误文案。矩阵 D1 行标注。
- **验证**：新增 4 个测试（bundler 外置、flag 镜像、pin 配对、三框架验收）+ 既有 e2e 的 sessions 断言；`go test ./...` 50 包全绿。
- **残余**：框架验收是"预构建产物路径 + 打包"级别（不跑真实框架构建工具链/SSR 运行时；三框架的产物形状以文档/适配器约定为准）。

---

## ADR-154 全服务 e2e 暴露的 dispatch 缺陷修复（timer namespace / event.cron / DISPATCH_URL / MessageBatch） ✅实现

- **背景**：跑"完整服务部署后的 e2e"（compose `--profile rpo0`，真实 fetch+KV+DO+queue+cron）时暴露 4 个真缺陷：
  1. **timer/cron 派发永远 400**：`internal/dispatch.HTTPDispatcher` 的 body 不含 `namespace`，而 user-runtime `/v1/timers/dispatch` 明确要求 → `kind=cron` 每秒重试并刷 WARN，**scheduled()/cron 从不执行**。
  2. **部署产物没设 `CELLHIVE_DISPATCH_URL`**：compose/k8s/Helm 都没配 → `dispatch.NewHTTP("")`/`queue.NewHTTP("")` 返回 nil → queue/timer/cron/waker 循环**静默不启动**（队列消息永远躺平）。
  3. **`event.cron` 为空**：`timer.Timer` 不携带表达式 → scheduled() 拿不到 `event.cron`（CF 会带）。
  4. **queue 批次不是 CF 形状**：handler 收到的是裸数组（无 `.messages`/`.queue`/`.ackAll()`/`.retryAll()`）；而且 `internal.js` 在 batch 上加的隐藏属性/traceparent 在**跨 isolate 的 RPC structured clone 中被丢弃** → `.messages` 永远 undefined、queue 的 traceparent 丢失。
- **修复**：
  1. `HTTPDispatcher.Dispatch` 从 `t.Scope` 用 `cell.ParseScope` 派生 `namespace` 并放进 body；回归 `internal/dispatch TestHTTPDispatcherSendsNamespace`。
  2. compose 的 cell-agent / k8s base ConfigMap / Helm chart 均设 `CELLHIVE_DISPATCH_URL=http://user-runtime:8088`；`configuration.md` 说明"空则派发循环不启动"。
  3. `timer.Timer` 增（非持久化的）`Cron`；cronEnricher 按 `(ns,worker)` 找到表达式（单条直接用；多条按 slot 重新 `cron.Parse().Matches` 选中的那条）；dispatcher body 带 `cron` → `scheduled(event)` 的 `event.cron` 正确。
  4. **MessageBatch 在租户侧 wrapper 构造**（`queue-wrapper.js`）：批次保持数组形状（`length`/索引，不破坏既有运行时）并补 `.messages`（自引用）、`.queue`、`.ackAll()`（成功即 ack，no-op）、`.retryAll()`（标记后抛 `retry_all`，runner 见 5xx 整批重投）；`internal.js` 只跨 RPC 传可克隆的 `{messages, queue, traceparent}`（修掉 queue traceparent 被 clone 丢弃的问题）。
- **验证（真实全栈 e2e，compose rpo0）**：`fetch /` → `{"kv":"v1","do":"count:1","worker":"web"}`；`/send`×2 → consumer 收到 **一个 2 条消息的批次**（`queue_last={"handled":2,"total":1}`，`queue_count=1` 无重试）；cron → `cron_count=1`、`cron_last="* * * * *"`（分钟边界）；门 facets `Counter/c1`；桶内 `cells/acme/{__kv__/main,__queue__/JOBS,__timer__/do}` + `cells/workerd/...` DO 段；cell-agent **0 条 dispatch failed**（修复前每秒刷 400）。回归：`internal/dispatch TestHTTPDispatcherSendsNamespace`、`internal/userruntime TestUserRuntimeDispatchInjectsBindings`（新增断言 CF MessageBatch 形状 `true:true:1:string:function:function`）。
- **残余**：`delayed()`/队列延迟与 per-message `ack()`/`retry()`（当前只有整批 ack/retry）未做，记录为后续项。

---

## ADR-155 Queue：per-message ack()/retry({delaySeconds}) + 延迟生产 + body 类型化 ✅实现

- **背景**：ADR-154 补齐了 MessageBatch 形状，但仍是**整批** ack/retry（消费端没有 per-message `ack()`/`retry()`，`retryAll` 只能靠 5xx），且 `message.body` 只按 base64 解成**字符串**（JSON 不解析），与 CF 不一致。
- **决策/实现**：
  1. **每消息结果回传**：`internal.js` 的 dispatch 响应带 `ack: [ids]` 与 `retry: [{id, delay_seconds}]`；`queue-wrapper.js` 在租户侧给每条消息装 `ack()`/`retry({delaySeconds})`、给批次装 `ackAll()`/`retryAll({delaySeconds})`——未显式处理的在成功返回时**隐式 ack**（CF 语义）。
  2. **Runner 应用结果**：新增 `queue.DispatchResult{ Ack, Retry[]{ID,DelaySeconds} }` 与可选接口 `DetailedDispatcher`（HTTP dispatcher 实现 `DispatchDetailed`）；runner 优先用详细结果：先按 `retry`（带 delay）重投、超 `max_retries` 进 DLQ，其余 ack；非详细 dispatcher 走原整批语义（向后兼容）。
  3. **延迟生产**：`env.QUEUE.send(body, {delaySeconds})` 之前已通（`Store.Send` 的 `visible_at_ms` + Claim 过滤），本次用真实栈验证并补 store 单测。
  4. **body 类型化**：`decodeBody(b64, contentType)` —— `application/json`/`+json` 解析为对象，`text/*` 为字符串，其它为字节数组（CF 近似；不做 ReadableStream）。
- **验证（真实全栈 e2e，compose rpo0）**：`message.ack()` → acked=1；`message.retry({delaySeconds:5})` → `retried=1`，t+2/4/6 均未 ack、**t+8s** 以 `attempts=2` 重新投递并 ack；`send(body,{delaySeconds:6})` → 响应回显 `delay:6`，t+0/2/4/6 均未消费、**t+8s** 以 `attempts=1` 消费；cell-agent 0 dispatch 失败。单测：`internal/queue TestSendDelayVisibility`（延迟不可见→到期可见）、`TestRunnerPerMessageAckRetry`（显式 ack + 显式 retry + 其余隐式 ack）、`TestRunnerRetryDelay`（60s 延迟内不可见、过后可见）、`TestRunnerRetryExhaustedGoesToDLQ`（超 max_retries 进 DLQ）；JS e2e `TestUserRuntimeDispatchInjectsBindings` 断言 `ack`/`retry` 与 `delay_seconds`，并断言批次里 `typeof batch[0].ack === "function"`。
- **残余**：非 JSON/text 的 `message.body` 是字节数组（非 ReadableStream）；无 per-message `ackAll` 之外的 `message.attempts` 之外的 CF 字段（`timestamp` 已给）。

## ADR-156 vwork 运维接口：资源吊销 + 队列 status/死信重放 + 就绪与排空探针 ✅实现

- **背景**：vwork（外部编排器）需要三个平台能力，均为**运维动作**（不是租户数据面）：①**吊销**某个 app 的 KV/Queue 等资源（关停），此前只有 `resource create`，没有删除；②队列**积压/死信可见 + 死信重放**，此前只能靠 `queue.OwnerGate` 消费日志；③滚动更新时把实例从 LB 摘除并可主动**排空**，此前只有 TCP/`/healthz` 探针（进程活着但拿不到投影或 body 里还有正在处理的请求也会被判 ready）。
- **决策/实现**：
  1. **R1 资源吊销**：`control.Store.DeleteResource(ns,kind,name,actor)` 幂等；**不删数据 cell**（KV/Queue/DO 的 cell 保留，吊销后再访问拿到 `binding_not_registered`，Queue 消费者因 `ResourcesByKind` 不再列出而停止）。为让吊销**立即生效**（版本元数据不可变但其中仍写着该 binding），引入 `revoked_resources` **墓碑表**，`Control.Binding()` **先查墓碑**（命中即 fail closed，不再回退版本声明），并删掉派生的 `bindings` 行；**重新 `resource create` 清墓碑**。`ResourceReferencedBy` 报告仍在声明该 binding 的 worker。端点 `DELETE /v1/control/resource?namespace=&kind=&name=[&force=1]`：被引用且无 `force` → 409 `resource_in_use` + `referenced_by`；不存在 → 404 `resource_not_found`；成功 → `{"deleted":true,"forced":<bool>}` + 审计 `resource.delete` + `invalidateBindings`。CLI `cellhive resource delete <ns> <kind> <name> [--force]`。
  2. **R4 队列 status + 死信重放**：`queue.Store.Status`（`depth`/`visible`/`leased` 计数，**不 List body**）；`GET /v1/control/queue/status?namespace=&queue=` 返回 `{depth,visible,leased,dead_letter_queue,dead_letter_depth,dead_letter_visible}`（`forwardRead` 到 owner）；`POST /v1/control/queue/replay-dlq?namespace=&queue=&limit=` 在 DLQ owner 上 claim、重投进**主队列**（本地写走 `capturedWrite` 保 RPO=0；远端用 `scopedtoken.Mint` 的 queue scope token POST owner `/v1/queue/send`）、成功后再 ack DLQ；无消费者声明 DLQ → 400 `no_dead_letter_queue`。CLI `cellhive queue status|replay-dlq`。
  3. **R3 就绪/排空探针**：`user-runtime`（loader.js）新增 `GET /ready`（免鉴权）：`draining` → 503；否则**先按需刷新一次投影**（探针没有租户流量也会到，不能等请求路径的轮询）——刷新失败且从未成功过 → 503 `cell_unreachable`；成功过但投影超过 `READY_STALE_MS=30000` → 503 `cell_unreachable`；否则 200 `{ready:true,projection_age_ms,cell:"ok"}`（空投影也是 ready）。`POST /drain`（常量时间比较 `x-cellhive-internal-token` 与 loader 的 `CELL_TOKEN`；不匹配 401）置 `draining=true`；`cmd/user-runtime` 在 SIGTERM 时 **POST 本地 `/drain` → 宽限 3s → 停止 workerd**（滚动更新先摘 LB 再退）。`do-runtime`（host.js）新增 `GET /ready`：`draining`（`POST /v1/do/drain` 后）或 cell-agent `/readyz` 不可达 → 503 `cell_unreachable`。compose/k8s/Helm 的 user-runtime、do-runtime 探针从 `tcpSocket` 改为 `httpGet /ready`（liveness 用 `/healthz`）。
- **验证**：`internal/control TestResourceRevoke`（引用报告/幂等/墓碑立即生效/重新登记解禁）、`internal/server TestResourceDeleteEndpoint`（409→`force=1` 200→列表消失→吊销后 deploy 声明该 binding 被 `binding_unregistered` 拒绝→404→审计含 `resource.delete`）、`internal/queue TestStatusCounts`（延迟不可见/租约）与 `internal/server TestQueueStatusAndReplayDLQ`（DLQ 深度→`replayed:2`→主队列 depth 2/DLQ 0→无 DLQ 400）、`internal/userruntime TestUserRuntimePublicLoaderBuildsBindingFacades`（真 workerd：`/ready` 200 + 错 token 401 + drain 后 503 `draining`）、`internal/doruntime TestDoRuntimeReadyAndDrainProbe`（真 workerd：`/ready` 200、错 token 401、drain 后 503 `draining`）。`make compose-config`/`make k8s-render`/`make helm-lint` 通过。
- **残余**：`replay-dlq` 为**至少一次**（主队列入队成功后若 DLQ ack 失败会重复重放）；重放要求该队列已被 producer binding 登记（才能签 scope token）且消费者声明了 `dead_letter_queue`；吊销**不删**数据 cell（DO 存储/队列消息仍在，按 vwork 语义"关停"而非"销毁"）；`do-runtime` 非 gated 模式自身 SIGTERM 不 drain（gated 由 do-supervisor 接管，见 ADR-140），仅靠 readiness 摘除 + 租约过期；`/ready` 对 **props-bound facades 的租户侧**不适用（它只描述入口/DO 宿主）。

## ADR-157 资源管理下放到各功能域 + 每域 stats（metadata-only）+ 旧路径别名 ✅实现

- **背景**：ADR-156 把资源吊销和队列运维挂在 `/v1/control/*` 下，stats 也只有队列有。运维面与数据面分离不清晰：一个资源（KV/D1/Queue/R2/Workflow/Hyperdrive）的登记、列表、吊销、统计本应属于它自己的功能域；同时"能快速拿到的统计"需要一套统一、诚实的取数口径（SQLite 没有行数元数据，见下）。
- **决策/实现**：
  1. **每域资源登记**：`POST|GET|DELETE /v1/<kind>/resources`（kind=`kv|d1|queue|r2|workflow|hyperdrive`）。kind 由**路径**固定，body 里的冲突 kind → 400；`scope` 缺省由服务端补 `<ns>/__<kind>__/<name>`（与 CLI 默认一致）。撤销语义与 ADR-156 完全一致（墓碑 fail-closed、数据 cell 保留、被引用 409 `resource_in_use`+`referenced_by`、`force=1`、审计 `resource.delete`）。队列死信重放随域走：`POST /v1/queue/dead-letters/replay`。旧 `/v1/control/resource`(POST/DELETE)、`/v1/control/resources`、`/v1/control/queue/status`、`/v1/control/queue/replay-dlq` **保留为别名**（同 handler），并在 admin 与 internal 两个监听上注册（CLI 走 admin）。
  2. **每域 stats**：`GET /v1/<kind>/stats`（`kv|d1|queue|r2|workflow|hyperdrive|do`），**全部只读、只读元数据**（PRAGMA / stat / schema / 有界列表），不解码行内容：
     - 通用 `cellstore.Cell.DiskStats`：`page_size`、`page_count`、`size_bytes`(=page_size×page_count)、`freelist_pages/bytes`、`main_bytes`、`wal_bytes`、`total_bytes`、`journal_mode`。WAL **必须单列**：cellstore 关掉了 autocheckpoint（`wal_autocheckpoint(0)`），`page_count` 会低估逻辑大小。
     - **KV**：`expires_indexed`、`expired`、`next_expiry_ms`（走 `kv_expires` 部分索引）、`rows_estimate`（dbstat 叶页 `ncell` 之和，>20000 页跳过并给 `estimate_note`）；`?exact=1` 才给 `keys`=`count(*)`（走最小索引，仍是 O(keys)）。
     - **D1**：`tables`/`table_names`/`indexes`/`auto_indexes`/`schema_version`/`user_version`/`sqlite_version`；`?tables=1` 追加每表 `pages`/`payload_bytes`/`rows_estimate`（dbstat，超阈值 `note=table_stats_skipped_too_large`）。这些只读 PRAGMA 由平台**直连 cell** 读，不经 `handleD1Query` 的"PRAGMA 可能写"判定，因此不需要改 `d1.IsReadOnly` 白名单。
     - **Queue**：`depth`/`visible`/`leased` + `oldest_visible_ms`（`messages_visible` 索引，滞后指标）+ `max_attempts` + `dead_letter_*` + `disk`。
     - **R2**：有界 List（`limit` 默认 1000、上限 10000）+ `truncated`/`cursor`/`listing_ms` + `multipart_uploads`/`multipart_parts`（`r2/.mpu/<ns>/<bucket>/` 暂存区）。R2 对象直接放桶、无本地索引，所以这是**诊断性近似**（文档标注）。
     - **Workflow**：`disk` + `instances_estimate`（dbstat）；`?exact=1` 给 `instances` + `by_status`。
     - **Hyperdrive**：`registered`/`has_config`，**绝不返回**信封加密的 origin URL。
     - **DO**：对象索引聚合（`indexed_objects`/`classes`/`workers`/`available`/`note`）；索引关闭时 `available:false`（不谎报）。
     - 读路径：由控制面资源行解析 scope（`cell.ParseScope`）→ `forwardRead` 到 owner；`requireControl` + `authorizeNS`；被吊销（墓碑）或不存在 → 404 `resource_not_found`。
  3. **KV 过期索引修复**：`kv_expires` 部分索引此前只在"旧表缺 `expires_ms`"的迁移分支创建 → **新库永远没有索引**，`NextExpiry`/`DeleteExpired`（TTL 清道夫，热路径）整表扫且 `DeleteExpired` 扫两遍。现在 `ensureKVExpires` 在 ALTER 之后**无条件 ensure** 索引（索引不能放进 schema 语句：旧表在 ALTER 前没有该列）。
  4. **`/metrics` 补齐**：新增 `cellhive_bucket_ops_total{op}`（put/get/list/conditional_create/cas/delete）、`cellhive_list_calls_total`（热路径禁 List 的回归哨兵）、`cellhive_owned_cells`（`owner.Manager.OwnedScopes()`）、`cellhive_resident_cells`（=cellstore open cells）。`docs/observability.md` 按代码对齐，未实现的指标明确标注"计划中"。
  5. **CLI**：`cellhive kv namespace create|list|delete|stats`、`d1 create|list|delete|info`、`r2 bucket create|list|delete|stats`、`queue create|list|delete|stats`（保留 `queue status|replay-dlq`、`queues list`）、`workflows create|list|delete|stats`（复数=定义登记，`workflow`=实例 API）、`hyperdrive create|list|delete|stats`；`cellhive resource *` 保留为通用入口（兼容脚本/vwork）；数据面子命令（`kv key get`、`d1 execute`、`r2 object put`）仍结构化拒绝。
- **验证**：`internal/cellstore TestDiskStatsAndKVStats`（page/file 字段、**新库必须有 kv_expires 索引**、expired/next、dbstat 估计、`?exact` 语义、prefix 作用域）与 `TestKVExpiryIndexUsed`（`EXPLAIN QUERY PLAN` 必须命中 `kv_expires`）；`internal/d1 TestD1Stats`/`TestD1StatsLargeCellSkipsDetail`（表清单排除 cellstore 内部表、dbstat 明细、超阈值跳过）；`internal/queue TestStatusCounts`（lag 字段 + disk）；`internal/r2 TestStatsBoundedListing`（总量/前缀/`truncated`+cursor/multipart 计数/暂存区对 List 不可见）；`internal/workflow TestStats`；`internal/server TestPerKindResourceEndpoints`（每域 CRUD、scope 缺省、body/path kind 冲突 400、409+force、**与 `/v1/control/resource*` 别名等价**）、`TestPerKindStats`（七种 stats 真数据 + 旧 `/v1/control/queue/status` 保持扁平）、`TestStatsFailClosedAndScoped`（未知/已吊销 404、JWT 跨 ns 403）、`TestMetricsBucketAndCellGauges`；`cmd/cellhive` e2e 追加每域 CLI（create/list/stats/delete）。门禁 `bash scripts/ci.sh` 13/13、`go test ./...` 50 包。
- **残余**：R2 stats 是**有界 List 近似**（`truncated`/`cursor`，大桶不精确）；KV/Workflow 的 `rows_estimate`/`instances_estimate` 是 dbstat 估计（非精确）；D1 每表行数同样是估计；`?exact=1` 的 `count(*)` 在大库上代价是 O(rows)（文档标注，接口显式区分 `exact`）；DO stats 依赖对象索引开启（ADR-109），workflow 的 `by_status` 依赖 `?exact=1`（未加 `instances(status)` 索引，避免写放大）；`do-runtime` 的 per-class 实时统计仍走 `/v1/internal/do/objects`。

## ADR-158 Vectorize 绑定：注册资源 + cell 内 float32 存储 + Go 精确 KNN ✅实现

- **背景**：用户要求支持 Cloudflare Vectorize。此前 ADR-014/ADR-065 把 `vectorize` 列入"平台拒绝"（Miniflare 认识面 > 平台）。**SQLite 本身没有向量类型/ANN 索引**：核心只有 FTS5（全文/BM25，本构建已启用），向量检索靠 C 扩展（`sqlite-vec`/`sqlite-vss`），而 `modernc.org/sqlite` 是纯 Go 转译、**无法加载 C 扩展**；workerd 的 capnp 配置里也**没有 vectorize 绑定类型**（`grep vectorize workerd.capnp` = 0）。因此走本平台既有的 facade 路线，并把检索做成**精确 KNN**（诚实：不假装近似）。
- **决策/实现**：
  1. **索引 = 注册资源 + 不可变配置**：新 kind `vectorize`（`resource create <ns> vectorize <index> --dimensions 768 --metric cosine [--description ...]` 或 `cellhive vectorize create`），配置 `{dimensions, metric, description}` 以既有**信封加密 config** 存控制面（`CreateResourceWithConfig`；ADR-129 同路径）；`Binding()` 的登记即授权/墓碑语义不变。`dimensions` 1..1536、`metric ∈ {cosine, euclidean, dot-product}`，创建时校验、之后不可变。
  2. **存储**：索引 cell `<ns>/__vectorize__/<index>`，表 `vectors(id TEXT PK, ns TEXT, vec BLOB, norm REAL, metadata TEXT, created_ms, updated_ms)` + `meta`（`last_mutation`/`last_mutation_ms`/`created_ms`）+ `metadata_indexes(property, type)`。`vec` 是 **little-endian float32**（cosine 索引存**已归一化**向量 + 原始 L2 `norm`，`returnValues` 时乘回，省掉每行归一化）。写入走 `Cell.Tx`（RPO=0 捕获），批量 ≤1000/请求，每索引默认上限 **100k 向量**（`index_full`）。
  3. **检索**：Go 精确 KNN（`internal/vectorize`）。score 语义与 CF 一致：**cosine = 余弦相似度**（越大越近）、**euclidean = 距离**（越小越近，文档实测 0.46 即"距离"）、**dot-product = 内积**（越大越近）。`topK` 默认 5，无 payload ≤100 / 带 `returnValues` 或 `returnMetadata!="none"` ≤50（CF 同限）。`filter` 支持 `$eq/$ne/$in/$nin/$lt/$lte/$gt/$gte` + 隐式 `$eq` + 点号嵌套路径 + 多键隐式 AND + 字符串范围（前缀搜索）；`namespace` 独立于 metadata（CF 语义）。**性能实测**（含 SQL 扫描）：20k×256 dims ≈ 47ms、10k×768 ≈ 53ms、5k×1536 ≈ 50ms/查询——约"每 20-30MB 向量数据 50ms"，故上限 100k 且 `stats` 暴露 `max_vectors`（见 known-issues 的延迟表）。
  4. **绑定面（CF 兼容）**：`insert/upsert/query/queryById/getByIds/deleteByIds/describe`（+ `listVectors` 扩展）。`workerd/platform/facades.js makeVectorize` 经 `CH_FACADE_SPEC` 注入 `env.<BINDING>`（`bindingStub` 对未知 kind 返回 undefined → 走 facade，ADR-090 增量迁移路径）；**env key = binding 名，资源 = `index_name`（`Binding.ID`）**，scoped token 用 index 名铸造（`loader.js`/`bindingSpecFor` 分流），校验门（`wranglercompat`）对 vectorize 用 `Binding.ID` 做登记检查。
  5. **服务端点**：租户（scoped token，internal 监听）`POST /v1/vectorize/{insert,upsert,query,get,delete}`、`GET /v1/vectorize/{describe,list}`；运维（control auth，两个监听）`GET /v1/vectorize/stats`（ADR-157 通用 stats 面）、`GET/POST/DELETE /v1/vectorize/metadata-index(es)`；资源 CRUD 走 ADR-157 的 `/v1/vectorize/resources`（新增 body `config`，仅 vectorize 合法）。
  6. **CLI**：`cellhive vectorize create|list|delete|info|stats|insert|upsert|query|get-vectors|delete-vectors|list-vectors|create-metadata-index|list-metadata-index|delete-metadata-index`；数据面命令用**根密钥派生 scope secret 现场铸造 scoped token** 调 internal 监听（operator 面走 admin）。`wrangler vectorize <cmd>` 前缀透传；wrangler.jsonc `vectorize[{binding,index_name}]` 映射为 `Binding{Type:"vectorize",Name:binding,ID:index_name}`。
  7. **dev**：Miniflare **只解析** `vectorize` 绑定（workerd 无该服务），所以 `cellhive dev` 遇到 vectorize 时**明确报错退出**（`DEV_UNSUPPORTED_BINDINGS`），不再假装可用；平台校验侧不再拒绝。
- **验证**：容器 smoke（真栈 `cellhive vectorize create/insert/query/info/stats/create-metadata-index/delete-vectors`）暴露并修复了两个只有真运行才出现的问题：①绑定端点误包 `s.auth`（租户 facade 只有 scoped token → 401），现与 KV/D1/R2/Queue 一致**只做 scopeAuth**（写加 `admit`）；②`topK.push` 只在"窗口已满"时位移，导致部分填充时中间插入覆盖已有匹配（third match 的 id 为空），现改为**两种情况都位移**并有 `TestTopKWindowOrdering` 锁定。`internal/vectorize`（store 单测：insert-only 不覆盖/upsert 覆盖、cosine/euclidean/dot 三种 score 与排序、namespace、9 种 filter 操作与嵌套/AND、维度与 metadata 上限、`topK` 夹取、metadata index 目录与上限、`describe`/`list`/`stats`、`queryById`）+ `BenchmarkQueryExactScan`（延迟表）；`TestTopKWindowOrdering`（top-K 窗口在**部分填充时的中间插入必须位移**，且不得留空槽——这条是**容器 smoke 抓到的真 bug**：原实现只在窗口满时位移，导致第 3 条匹配 id 为空/覆盖已有匹配）与 `internal/server TestVectorizeEndpoints`（无 config 拒绝→创建→stats→insert/query/filter/queryById/get/list/describe/维度错误/delete→metadata index CRUD→吊销后 403 `binding_not_registered`→跨 ns scoped token 403；**绑定面只带 scoped token、不带 internal token**，与 KV/D1 一致；写入走 `forwardOrClaim`、读取走 `forwardRead`）；`internal/userruntime TestUserRuntimeVectorizeBinding`（**真 workerd**：facade 全方法往返、`ns=acme&index=docs` 寻址、JS 铸造的 scoped token 可被 Go 验证、响应映射与 CF 形状一致）；`internal/wranglercompat TestVendorMatrixContract`（vectorize 进支持矩阵 + 用 `index_name` 做登记检查）；CLI `cli/test/bindings-parity.test.ts`（dev 必须拒绝 vectorize，且不再是平台拒绝项）。
- **残余**：**精确 KNN = O(vectors×dims)**（无 ANN/HNSW；`docs/known-issues.md` 有实测延迟表与建议规模）；metadata index **不强制**（CF 要求先建，否则过滤报错——我们允许直接过滤完整 metadata，属超集，但 indexed string 的 64B 截断语义未模拟）；cosine 索引的 `returnValues` 是 float32 归一化后乘 `norm` 的回构值（与 CF 一样非精确 float）；`describe` 不返回 CF 的 `processedUpToDatetime` 之外的内部字段；dev 无本地实现；批量上限 1000（CF HTTP 5000，Workers 1000 一致）；**不变更 ADR-001/014 的其余拒绝项**。

## ADR-159 SQLite 换成 CGo + vec1 ANN（向量检索落地）✅实现

- **背景**：ADR-158 用 Go 精确 KNN（O(n·dims)，20k×256 ≈ 48ms/查询）。用户要求"换成 C 的库"以拿到真正的索引。实测确认：①C 版 SQLite（`mattn/go-sqlite3`，内置 SQLite **3.53.4**）可运行时加载 `.so` 扩展，也可把扩展源码静态编入；②SQLite 官方现在自带 **`vec1`** ANN 扩展（IVFADC + OPQ，AVX2/NEON，L2/cosine，单文件 C，public domain，https://sqlite.org/vec1）；③纯 Go 的 `ncruces/go-sqlite3` 也内置 vec1（无 SIMD）——本 ADR 选 **CGo + 静态 vec1**（用户选择 B：极限延迟）。
- **决策/实现**：
  1. **驱动切换**：`internal/cellstore` 从 `modernc.org/sqlite` 换成 **`mattn/go-sqlite3`**，驱动名与逐连接 pragma 集中在 `internal/cellstore/driver.go`（`DriverName="cellhive-sqlite"`、`Open()`、ConnectHook 设 `wal_autocheckpoint=0/synchronous=NORMAL/foreign_keys=1/busy_timeout=5000`）。三者都是 SQLite 3.53.4 → **cell 文件与 WAL 帧格式不变**，`internal/wal` 捕获、快照/LTX、恢复链路无需改动（`TestS3ReplicationRestoreChain`、rpo-test 全过）。
  2. **构建标签**：mattn 的可选特性用 tag 打开，与旧 modernc 能力对齐：`sqlite_fts5 sqlite_dbstat sqlite_math_functions sqlite_column_metadata sqlite_preupdate_hook`。Makefile/`scripts/ci.sh`/Dockerfile 统一传递（`TAGS`/`SQLITE_TAGS`），并有 `internal/cellstore/require_tags.go` 的 `#error` 守卫：漏加 tag 直接编译失败并提示命令。
  3. **vec1 静态注册**：`internal/vectorize/vec1/` vendored `vec1.c`(v0.7)+官方 `sqlite3.h`/`sqlite3ext.h`；`vec1_static.c` 以 `-DSQLITE_CORE` 编入并用 `sqlite3_auto_extension` 注册 → 无需 `.so` 文件，所有连接（含 D1 租户 cell）都能 `CREATE VIRTUAL TABLE ... USING vec1`。x86-64 用 `-march=x86-64-v3`（AVX2+FMA），arm64 用 NEON 默认；`-tags cellhive_vec1_portable` 回退到标量构建。
  4. **存储/检索**：索引 cell 内 `vectors(id, rid, ns, metadata, raw, ...)`（**真源**，raw = float32）+ `vec USING vec1(embedding, ns)` 派生索引 + `meta`(dims/metric/next_rid/ann_*/index_rev) + `metadata_indexes`。写：`vectors` + vtab 同一事务（RPO=0 捕获不变）；**upsert = 删除 vtab 行 + 重插**（vec1 v0.7 对 rowid-targeted UPDATE 会段错误，实测）。查询：`SELECT rowid, distance FROM vec(?, '{"K":..,"nprobe":..}')`，namespace 作为 vec1 metadata 列**下推**，其余 metadata 过滤在后置过滤并按 4× 过采样；score 语义保持 CF（cosine → `1 - distance`；euclidean → `sqrt(distance)`，vec1 的 l2 是平方距离）。
  5. **ANN 生命周期（算子动作）**：`POST /v1/vectorize/rebuild`（`vec1_train`（`nthread=min(NumCPU,16)`）+ `rebuild`，参数 nbucket/quantizer/codesize/nprobe 存入 meta）与 `DELETE /v1/vectorize/ann`（丢弃模型、重建 flat）；CLI `cellhive vectorize rebuild|drop-ann`；`describe/stats` 返回 `ann{...}`。默认（未建模型）是 **flat 精确扫描**，已比 Go KNN 快约 8×；建 ANN 后再快一个量级（见下）。
  6. **兼容性收口**：`dot-product` 指标被显式拒绝（`vectorize_bad_config`，vec1 只支持 l2/cos，记入 compatibility-matrix/known-issues）；`ParseConfig` 校验 1..1536 维；旧的 100k 向量上限移除（vec1 支持百万级，ANN 为次线性）。ADR-158 的 **API/端点/CLI 形状全部保留**（store 内部替换），迁移旧 ADR-158 cell 自动完成（`migrateLegacy`：vec/norm → raw + rid + vtab）。
  7. **性能（本机 EPYC，20k×256，K=10，含我们的 store 开销）**：旧 Go KNN ≈ 48ms；vec1 flat（精确，AVX2）≈ **6.3ms**；vec1 ANN（nbucket=64、OPQ codesize=32、nprobe=0.05）≈ **0.22ms**（≈210×）。官方公开数据集（1M×128d，AVX2，16 线程）recall@10 ≈ 0.92 @ 1954–3180 QPS。
- **验证**：`internal/vectorize`（store 单测全部改为 vec1 后端：insert-only/upsert 替换、cosine/euclidean score 映射、namespace 下推、9 种 metadata 过滤 + 点路径/隐式 AND、维度/ID/metadata 上限、dot-product 拒绝、config 不可变、metadata index 目录、describe/list/stats、`BuildANN`/`DropANN` 后查询仍正确；`TestVec1ExtensionRegistered` 断言无 `.so` 也能 `vec1_info()`/建 vtab；`BenchmarkQueryVec1` 记录 flat 延迟）；`internal/server TestVectorizeEndpoints`（API 兼容未变）+ `TestVectorizeANNEndpoints`（rebuild/stats ann/drop-ann/dot-product 400）；`internal/userruntime TestUserRuntimeVectorizeBinding`（真 workerd facade 全方法）；**LTX 复制**：`internal/vectorize TestVec1CellReplicatesThroughLTX`——把带 **ANN 模型**的 vec1 索引 cell 经 `cellcapture`（快照+delta）捕获为 LTX 段链，再用 `restore.ApplyFile` 还原到全新 cellstore，断言向量数/模型/删除/元数据一致且查询结果逐条相同、还原后可继续写；**驱动回归**：`go test ./...`（51 包）+ `scripts/ci.sh` 13/13（含 rpo-test 精确 RPO=0、S3 复制/恢复链、真实 workerd JS 测试、docker-build）。
- **残余**：dot-product 未支持；metadata 过滤为后置过滤（CF 是先过滤；过滤过狠时可能少于 topK，已 4× 过采样，记 known-issues）；vec1 无 partition key（namespace 用 metadata 列）；训练是显式算子动作（需代表性数据，OPQ 训练较慢）；`-march=x86-64-v3` 构建的二进制需要 AVX2 CPU（2013+），非 AVX2 平台用 `-tags cellhive_vec1_portable` 或 arm64；vec1 只支持 float32（不支持 int8/bit 量化类型）；D1 租户现在也能用 vec1 vtab（视为超集能力）。

## ADR-160 运行期 VFS 懒读：cell-agent 侧按页冷启动（paged restore）✅实现

- **背景**：ADR-092/055/056 的冷恢复是**整库物化**（`replica.Restore` 整读 L1 对象 + L0 尾巴 → `restore.ApplyFile` 写整文件）；ADR-159 换成 CGo（mattn）后，cell-agent 侧可以注册自定义 SQLite VFS；本次把 cell-agent 侧做成**按页加载**。
- **决策/实现**：
  1. **paged VFS（`internal/pagedvfs`）**：一个包装默认 VFS 的 `sqlite3_vfs`（名 `cellhive-paged`，用 mattn 的 `?vfs=` 选择）。主库是**按 pinned cut 尺寸的稀疏文件**；`xRead` 缺页时**同步**调用 Go（`goPagedFetch`）取该页并写入本地文件、标记 hydrated；`xWrite`/`xTruncate` 维护 hydration 位图（checkpoint 写过的页不 re-fault、truncate 的页遗忘）；fault 失败 → `SQLITE_IOERR_READ`（fail closed，不拿零页继续）。只有注册的主库被包装，WAL/SHM/journal/temp 原样转发。
  2. **页源**：`replica.PageFetcher`（ADR-056 的 L1 `index.bin` + 更新的 L0 尾巴）→ 每页一次 `Bucket.RangedGet`。cell-agent 的 hook：`LatestEpoch` → `NewPageFetcher`（无 L1 索引/未 compaction → **回退整克隆**）→ 镜像字节 < `CELLHIVE_PAGED_MIN_BYTES`（默认 256MiB）→ 回退整克隆；否则 paged open。
  3. **cellstore 集成**：`Store.Paged` hook 优先于 `Hydrate`；`Prepare` 建稀疏文件（并清掉陈旧 `-wal/-shm`）；任何路径（`Store.Cell`/`Store.Open`/`OpenAt`）打开已注册路径都会走 paged VFS（避免用默认 VFS 读到零页）；`Close` 停后台 hydrate 并 `Release` 注册（本地文件当缓存保留）。
  4. **捕获/快照协同（我们的额外约束）**：`Cell.SnapshotPages` 在 `ReadDBPages` 前强制 `pagedvfs.HydrateAll`，确保 LTX 快照不会把稀疏零页编码进去（paged cell 的本地文件只当缓存、不据此发快照）。
  5. **后台补齐**：`CELLHIVE_PAGED_HYDRATE_MBPS`（默认 16；0 = 保持稀疏、每个冷页都读桶），每节点同时只跑一个 cell 的补齐。
  6. **窗口预取（run read）**：`replica.PageFetcher.ReadRun` 把**同一 L1 对象内连续存储的相邻页**用**一次 ranged read** 取回（受 `CELLHIVE_PAGED_WINDOW_PAGES`（默认 64）与 256KiB 解码字节预算约束）；pagedvfs 缓存该窗口，后续同窗 fault **零桶读**；`xTruncate` 回调 Go 丢弃超出新末尾的窗口页。稳定扫描因此是"每窗口一次 GET"而不是"每页一次"。
  7. **开关**：`CELLHIVE_PAGED_RESTORE`（默认 true）、`CELLHIVE_PAGED_MIN_BYTES`（默认 256MiB）、`CELLHIVE_PAGED_HYDRATE_MBPS`（默认 16）、`CELLHIVE_PAGED_WINDOW_PAGES`（默认 64）。DO 侧不变（workerd VFS，后续 FUSE）。
  8. **稀疏感知磁盘口径**：`DiskFile.AllocBytes`（`st_blocks*512`）与 `DiskFile.DiskBytes()`；`DiskUsage`/`/metrics` 与磁盘驱逐预算改用**实占**，paged cell 不再因 apparent size 误触发反压。
- **验证**：
  - `internal/pagedvfs`：`TestPagedOpenFaultsPagesOnDemand`（点读只 fault 少量页、fault 数 < 总页数；`HydrateAll` 后本地文件与源镜像**逐字节相同**）、`TestPagedWritesMarkHydrated`（写入/checkpoint 后不被旧 cut 覆盖）。
  - `internal/cellstore`：`TestPagedCellServesKVAndSnapshots`（冷 cell 经 hook 打开 → KV 点读正确且 fault < 总页；`SnapshotPages` 先 hydrate 全量；写入可用；`Close` 释放注册）。
  - `internal/compaction`：`TestPagedCellAgentEndToEnd`（**真链路**：真 cell → 桶快照 → `Compact` 出 L1 索引 → `NewPageFetcher` → 冷 cellstore 经 paged hook 打开）。实测：**点读 8 次页 fault（共 771 页）、8 次 ranged 读、整对象读 3,158 字节（镜像 3,158,016 字节）≈ 1000× 少**；随后**全表扫描也只用了 20 次 ranged 读**（窗口预取）；`SnapshotPages` hydrate 全量后正确。
  - `internal/pagedvfs TestRunPrefetchReducesBucketReads`（336 页全扫 → **6 次窗口读、0 次单页读**）、`internal/cellstore TestDiskUsageCountsAllocatedBytes`（稀疏文件按 `st_blocks` 计）与 `TestDiskEvictionUsesAllocatedBytes`。
  - 门禁 `bash scripts/ci.sh` 13/13、`make test` 51+2 包。

### 逐项能力对照与本次补齐（ADR-160 续）

| 能力 | 目标做法 | CellHive（本次后） |
|---|---|---|
| fault-in VFS、稀疏 cut 文件、hydration 位图、同步 fault、IOERR fail-closed | ✅ | ✅ 同模型（`internal/pagedvfs`） |
| 页图来源 | LTX 段页索引 | ✅ `replica.PageFetcher`（L1 `index.bin` + L0 尾巴） |
| 链小于阈值整克隆 / 大于阈值分页 | ✅ 256MiB 门槛 | ✅ `CELLHIVE_PAGED_MIN_BYTES`=256MiB |
| 后台补齐限速、每节点一个 | ✅ 16 MB/s | ✅ `CELLHIVE_PAGED_HYDRATE_MBPS`=16 |
| **b-tree 内部分支感知**：内部页登记的 child **单独取**（避免把大行表的 overflow 链一次拉爆），顺序遍历时才按 `SCAN_AHEAD` 预取后续 child | ✅（child 集合 + 预取计划 + 并发 worker） | ✅ **本次补齐**：`parseChildren`（interior index 0x02 / table 0x05，含 rightmost）→ child 集合；child fault 单页；顺序 child 时按 `scanAhead`=64 预取到**内存缓存**（`prefetchBudget`=4MiB），连续 child 合并成一次 run |
| **顺序访问才读窗口** | ✅（点读不预读，每层一次请求） | ✅ **本次补齐**：只有 `pg == windowEnd+1` 才用窗口；否则单页探测 |
| **paged 文件只是缓存**（不据此发快照；重启后重新分页） | ✅（不保留为 eviction snapshot；混合版本用 gate 保护） | ✅ **本次修 bug**：新增 `<cell>.db.paged` **标记位**（pageSize+commit）。有标记=稀疏缓存，必须经 paged VFS 打开；打开时按 cut 匹配**复用缓存并重新注册**，不匹配/无页源则**丢弃并从桶重建**；`HydrateAll`/后台补齐完成即删标记（文件变成完整镜像）。**修复前**：重启/驱逐后用 base VFS 打开稀疏文件 → 读到零页（实测 `database disk image is malformed`） |
| 批量 hydrate（按 run 而不是逐页） | ✅ | ✅ **本次补齐**：`HydrateAll`/`HydrateRate` 用 `ReadRun`（每窗口一次 ranged read） |
| 可观测性 | faults 计数 + `paged_gate` 日志 | ✅ **本次补齐**：`/metrics` 新增 `cellhive_paged_cells`/`_faults_total`/`_runs_total`/`_prefetch_hits_total`/`_hydrated_pages`/`_total_pages` |
| L1 页帧压缩（LZ4 block） | ✅（因此需要"解码字节 vs 帧字节"上限） | ✅ **本次补齐**：`WAL3` page-map v2（逐帧 LZ4，压不小则 raw）+ payload 内 12B/页位置索引；`L1/index.bin` 升级为 `CID2`（每页 off/stored/codec），paged 读取一次 ranged read 取帧并解压（实测解码 1.1 GB/s ≈ 3.6µs/页）。`WAL2`/`CIDX` 仍可解码（向后兼容） |
| 并发预取 worker（`PREFETCH_WORKERS`） | ✅（child 预取并行） | ✅ **本次补齐**：`CELLHIVE_PAGED_PREFETCH_WORKERS`（默认 4），连续 child 合成一次 run 后按 run 并行取（实测 6 个散列 child：peak=4、45.7ms，顺序约 90ms） |
| paged epoch 的链接续（marker 对象） | ✅（无整库快照时用 marker 组合） | 不需要：我们每个 epoch 首次捕获仍写**基线快照**（`AutoSnapshot`，先 `HydrateAll`）→ 恢复永远有基线，无跨 epoch 组合问题（无需 marker 组合，更简单） |

**本次实测**：`internal/pagedvfs TestFaultPolicyChildAloneAndWindow`（内部页 child 单页、顺序 child 预取命中、顺序非 child 才开窗）、`TestParseChildren`、`TestHydrateUsesRuns`（3000 行镜像批量 hydrate ≤ commit/4 次取数）、`TestRunPrefetchReducesBucketReads`（336 页全扫 = 6 窗口读 + 6 单页读）、`internal/cellstore TestPagedCellReopenAfterClose`（**回归**：重启后经标记重新注册，数据正确，不再 malformed）；真栈 smoke 两轮冷启动 + 一次"仅重启不删文件"（marker 复用）均正确，`/metrics` 显示 `cells=1 faults=71 runs=9 prefetch_hits=3 hydrated=12/77 pages`，本地文件 apparent 315,392B / 实占 48K。

- **残余**：**delta** payload（`WAL1`）仍不压缩（逐笔小段，压缩收益低）；LZ4 编码器是朴素哈希匹配（单线程 ~47 MB/s；3MB 快照约 70ms，冷路径可接受），如需更快可换更激进的匹配；paged 本地文件是缓存（`HydrateAll` 在快照前强制物化，标记位管住"缓存≠数据库"）；捕获/快照冷路径仍付一次整库 hydrate（预期）；DO 侧仍需 FUSE；无需 `paged_gate`（每 epoch 都有基线快照）。
- **与 LTX GC 的关系（压缩不改变 GC）**：GC 只依据 **manifest 引用的对象 key** 与 **txid 水位**（`l0Max[k] <= maxTx` 才删 L0），与 payload 编码无关；`index.bin`/`manifest.json` 是**同 key 覆盖写**，从不进入 GC 删除集。因此压缩（`WAL3`）与旧格式（`WAL2`）混链都能正常折叠+回收。验证：`internal/compaction TestCompactFoldsLegacyV1AndGCsCompressedL1`（**v1 WAL2 基线** + delta → 折叠出**压缩 L1** + `CID2` 索引 → 再折叠并 GC：旧 L1/已折叠 L0 被删，index/manifest 保留，`PageFetcher.Page/ReadRun` 从压缩 L1 解压读、`ApplyFile` 仍逐字节正确）、扩展现有 `TestCompactGarbageCollectsSupersededObjects`；真栈：1s 间隔连续 3 轮写入+折叠后 `ltx/e1/L1/` 只剩一个压缩快照 + `index.bin` + `manifest.json`，**L0 计数 = 0**，冷启动 paged restore 正常。副作用：`MinBytes` 阈值现在按**落盘（压缩后）字节**计（更贴近真实取数/IO 预算，符合本意）；`EncodeSnapshotParts` 的分片预算仍按 v1 每页字节估算 → 分片偏小（安全、略保守）。**有意不进一步收紧**（已接受）：分片是 CellHive 的 HTTP `maxSegmentBytes` 传输约束，LTX 无分片概念（一文件一对象，数量由 compaction level 控），精确化收益很小；见 `known-issues`。
- **性能实测（本次，EPYC）**：
  - L1 快照压缩：合成重复页 771 页 **3,161,100 → 140,398 B（4.4%）**；真栈 KV 负载 **315,764 → 86,523 B（3.7×）**（`L1/index.bin` 1,308B、manifest 160B）。
  - 冷启动点读（真链路单测）：8 次 fault/8 次 ranged read，**156,007 B（= raw 镜像的 4.9%）** + 整对象 15,494 B（旧实现整库 hydrate = 3,158,016 B，即 **~18× 更少**）；全表扫描 148 次 ranged read（压缩后帧很小）。
  - 页解码：**1,138 MB/s（3.6µs/页）**，相对一次桶 GET（~ms）可忽略。
  - child 并发预取：6 个散列 child peak=4 worker、45.7ms（顺序 ~90ms）。
  - 真栈：删本地文件后冷请求 **123ms** 返回正确数据，稀疏文件 apparent 315,392B / 实占 48K；`/metrics` `cells=1 faults=72 runs=9 prefetch_hits=3 hydrated=12/77`。
  - **RPO=0 回归**：`scripts/rpo-zero-fault.sh` PASS（1200 acked keys 精确、SIGKILL+接管+桶恢复，`integrity=ok`）。


## ADR-161 LTX 压缩运行期开关（`CELLHIVE_LTX_COMPRESSION`）✅实现

- **背景**：ADR-160 把 L1 快照 / page-map 统一升级为逐帧 LZ4 的 `WAL3`（+`CID2` 页索引），默认压缩。压缩收益在**桶字节数**与**冷启动取数**（L1 315KB→86KB、冷点读 4.9% raw），但编码要花 CPU（朴素匹配 ~47 MB/s），且有些部署更在意 CPU/确定性而非桶字节。
- **决策**：新增进程级开关 `CELLHIVE_LTX_COMPRESSION`（默认 `true` → `ltx.CompressionEnabled`）。关闭时 `ltx.EncodeWALPageMap` 走**v1（`WAL2`）固定帧、不压缩**；解码器与 `PageLocs` 两种格式始终都读，`WAL2`/`WAL3` 可混链折叠与 GC（ADR-160 已证）。开关只影响**本进程**的写出（cell-agent 设；cell-supervisor/do-supervisor 是独立进程，保持默认），契约：必须在任何写者启动前设置、不可并发改。
- **理由**：给"CPU 受限/要确定性"的部署一个逃生门；也让本次性能复核能干净地 A/B（压缩对写吞吐的真实代价）。
- **代价**：关闭后对象体积回到未压缩（L1 快照 3.2× 更大、冷启动取数增多）；不是 per-cell 粒度。
- **验证**：`internal/ltx TestPageMapCompressionDisabledWritesV1`（关闭时输出 `WAL2` magic + 固定帧布局，`PageLocs`/`DecodeWALPageMap` 往返正确）；A/B 实测：单 cell capture-ON `KV put` c=16/32/64 与双 cell c=16/32/64/128，压缩 on vs off 在**同一 box 状态**下差异 1–3%（`docs/archive/bench/ltx-compression-ab.txt`、`single-vs-dual-capture-cgo-on-repeats.txt`）→ 压缩在写热路径**无可见代价**（快照相对逐笔写很稀疏，编码不在 ack 关键路径）。
- **文档**：`docs/configuration.md`（env）、`docs/benchmarks.md`（复测 + A/B）。

## ADR-162 DO 调用支持 RPC（tagged JSON + 原生 JSRPC）✅实现

- **背景**：CellHive 的 DO 只支持 `env.NS.get(id).fetch()`，租户无法 `env.NS.get(id).method(...)`（CF 风格 DO RPC）。常见做法是"自建 RPC 信封 + facet.fetch 哨兵 + 注入基类反射调用"；CellHive 的 host 已用**原生 JSRPC**调 facet（`facet.__chAlarmState()` 等），因此 host→facet 一跳可直接 JSRPC，无需哨兵/反射。
- **决策**：
  - 客户端 `workerd/platform/facades.js`：DO 命名空间 `get`/`getByName` 返回 Proxy——未知方法名 → `rpc(id, method, args)`；保留 `fetch`；`then`/`toJSON` 返回 undefined（不做 thenable）。新增 `getByName`。
  - 传输：`POST /v1/do/invoke` 加 `kind:"rpc"` + `rpc:{method,args}`；Go（`internal/server/do_proxy.go`）用 `json.RawMessage` **字节透传**（不重编码，避免 float64 失真），`request`/`rpc` 互斥，路由体限升到 `8 MiB+256 KiB`（原 1 MiB）。
  - 服务端 `workerd/do-runtime/host.js`：`kind:"rpc"` → 方法名校验（标识符 + 禁 `fetch`/`alarm`/JS 原型内部名/`__ch*`）→ `decode(args)` → `facet[method](...args)`（**原生 JSRPC / structured clone**）→ `encode(result)` 封 `{ok:true,result}`；缺失方法 404 `do_rpc_method_not_found`、handler 异常 500 `do_rpc_error`（带 `name`/`stack`）、参数/结果非法 400/500。仍执行 `reportAlarm`+`gateSync`，RPO=0 不变。
  - 编解码 `workerd/platform/rpc-codec.js`（facade 与 host 共用）：tagged JSON 支持 `undefined`、`-0/NaN/±Infinity`、bigint、Date、RegExp、Map、Set、ArrayBuffer、TypedArray/DataView、Error（含 cause）、URL、URLSearchParams，以及**共享引用/环**（首次出现的可引用节点带 `i`，再次出现 `{t:"ref",i}`）；function/Symbol/Promise/WeakMap/WeakSet/流/RPC stub 拒绝；类实例降级普通对象（同 structured clone 可观察行为）。上限 **8 MiB**（编码后）。
  - 模块接线：`rpc-codec.js` 随 `facades.js` 进所有 tenant module map（user-runtime `loader.js`/`internal.js`×2、do-runtime facet map，以及 do-runtime config 的 `modules` 供 host.js import）；两份 capnp 加 text/`esModule` 绑定 + copy 列表。
- **理由**：补齐 CF 形状的 DO 方法调用；tagged 编解码在跨进程 JSON 约束下尽量贴近 structured clone；host→facet 用 JSRPC 因而 Map/Date/Buffer 无损。
- **代价/边界**：跨进程不能传 function/stub/`RpcTarget`/流；args 与结果各 ≤8 MiB（二进制 base64 膨胀 ~4/3）；类实例丢原型；RPC 无 tenant `Request`，故无 traceparent 透传（仅 requestId）。
- **验证**：`internal/doruntime TestDoRuntimeRPCDispatch`（真实 workerd：普通方法 JSRPC、tagged Map/Date 往返、tagged Map 参数、环、handler 错误、缺失/保留方法、不可序列化结果）、`TestDoRuntimeDurableObjectToDurableObjectRPC`（facet 内经注入 facade 调另一个 DO）；`internal/userruntime TestUserRuntimeDurableObjectRPC`（真 user-runtime：`getByName` → tagged 往返 → 结构化错误）；`internal/server TestDOProxyRPCPassthrough`（字节透传/互斥/超限）。
- **文档**：`docs/compatibility-matrix.md`、`docs/durable-objects.md`、`docs/bindings.md`、`docs/known-issues.md`、`docs/testing.md`、`docs/release-notes.md`。

## ADR-163 R2/D1 契约保真（R2Object 全字段 + put metadata + D1 last_row_id/错误形状）✅实现

- **背景**：`docs/dev-mode.md` 的契约对拍表记录了 facade/binding 相对 CF 的字段缺口：R2 `get()` 只有 `{etag,size,text/json/arrayBuffer}`、`put` 忽略 `httpMetadata/customMetadata/checksums`；D1 `run()` meta 缺 `last_row_id`；错误是平台 `{error:...}` 而非 `D1_ERROR`/`KVError`。
- **决策**：
  - **R2 对象字段**：`get()` 返回完整 `R2ObjectBody`。实现关键：workerd RPC 会**按值序列化数据字段**、把**可枚举的函数属性序列化为可调用 stub**（`RpcTarget` 只暴露方法不暴露数据——实测验证），因此正文对象用「普通对象 + 数据字段 + 可枚举正文方法」：`obj.size`/`obj.httpMetadata` 与 `await obj.text()` 同时可用，`JSON.stringify` 自动跳过函数。`bodyUsed` 为快照值（读取后不翻转）。
  - **R2 metadata sidecar**：`put(key,value,{httpMetadata,customMetadata,md5,sha256})`（md5/sha256 为 hex，写入前校验）把 metadata 存到 `r2meta/<ns>/<bucket>/<key>`（新增保留前缀，owner `r2`；同时登记 `r2/`）；`get()/head()` 读 sidecar 并回填；`delete` 一并删 sidecar。无 metadata 的 put 仍是单写。
  - **`head()`** 新增（`GET /v1/r2/object?head=1`，只回 R2Object JSON，不声明 object 的 `content-length`）；range 响应用 `content-range: bytes a-b/total`（total 优先取 sidecar.size，否则 `bucket.Statter`）。
  - **`bucket.Statter`** 可选接口：S3 `HeadObject`（不传体）、FS 回退读体算 `sha256` etag；`r2.Stat` 优先用它。
  - **list**：透传 `truncated`/`cursor`，对象带 `key/size/etag/httpEtag/version`；**不逐对象**读 sidecar（避免 N 次读），其余字段为默认。
  - **D1**：`d1.Result` 增 `LastInsertID`（`sql.Result.LastInsertId()`），`run()/batch()` meta 增 `last_row_id`/`changed_db`；facade/binding 的错误按 CF 形状抛出（`err.name=D1_ERROR|KVError|R2Error|QueueError`、`err.code`=平台码、`err.status`）。
- **理由**：平台工程责任是契约保真，让同一份用户代码在 CF/`wrangler dev`/`cellhive` 之间可移植；字段通过 RPC 值序列化 + 函数 stub 的机制天然达成，无需把 R2 退回租户本地 facade（避免 ADR-090 反向）。
- **代价/边界**：`get()` 多一次 sidecar 读；`list` 逐对象 metadata 不回填；`bodyUsed` 不翻转；`rows_read/rows_written` 为近似；`put` 的 metadata 只存 map[string]string（CF customMetadata 可含数字会被字符串化）；无 R2 versioning（`version` 恒 `""`）。
- **验证**：`internal/r2 TestR2MetadataStatAndChecksums`、`internal/d1 TestLastInsertID`、`internal/userruntime TestUserRuntimeR2ObjectFidelityAndD1Meta`（真 workerd 端到端）。
- **文档**：`docs/dev-mode.md`（契约对拍表）、`docs/bindings.md`、`docs/compatibility-matrix.md`、`docs/known-issues.md`、`docs/testing.md`、`docs/release-notes.md`。

## ADR-164 peer 自适应 hedge + spool 按 sequence 幂等 ✅实现

- **背景**：fleet 复制是 owner→follower ensemble（≤2 follower、quorum-1 首个 ack）。现状 `ShipBatcher` **无条件并行 fan-out 到全部 follower**：尾延迟已被覆盖，但常态每笔发 2 份（2× 网络/fsync/CPU）。改为"先发一份、超时再补发一份"，自适应等待 = 4×最近最慢 append、下限 250ms；其前提是 append **按 sequence 幂等**。
- **决策**：
  - **spool 幂等**：`Spool.AppendBatch` 按 `ltx.Header.ID()`（`epoch:kind:start:end:CRC`，recovery 本就用它做链去重）过滤已存在的段；纯重复批次是成功的 no-op；进程重启后首次 append 从日志重建身份集。这使重试/流重连/hedge 副本都安全（否则恢复会因重复 txid 报 non-contiguous chain）。
  - **自适应 hedge（默认）**：`followersRace` 先启动 follower[0] 并**同步写**（异步 lane 仍需按批序写主帧），超过等待再 `startNext()` 发下一份，首个成功 ack 胜；全部失败才报错。等待：`CELLHIVE_PEER_HEDGE_MS`（默认 `adaptive`=-1；`0`=只发 primary 单份；`>0` 固定 ms），自适应 = `max(250ms, 4×最近最慢 append)`，上限 `CELLHIVE_PEER_HEDGE_MAX_MS`（默认 2000）。`0`/primary 失败时按 follower 顺序 **failover**（不是副本）。
- **理由**：把该做法作为可选能力；幂等 spool 本身对重试/重连的复制质量也有独立价值。恢复按 `rank+StartTxID` 排序，故延迟副本的乱序到达可容忍。
- **代价/边界**：默认自适应下常态是 **owner 本地 + 1 个 follower**（慢时才补第 2 个；`0` 则永远单份）；相比"无条件并行 fan-out 到 2 个 follower"，常态少一份副本（网络/fsync/CPU 减半），代价是同时坏 owner+follower 的暴露面更大——单节点故障仍可恢复（owner 死→新 owner 从 follower spool `/v1/peer/held` 恢复；follower 死→owner 仍有本地段并后台上传）。无 per-follower 延迟直方图与 hedge 命中率指标（自适应基于滚动窗口的"最近最慢 append"）；延迟副本会让 follower spool 的 append 顺序与 txid 顺序不一致（恢复按 `rank+StartTxID` 排序，已容忍）。
- **验证**：`internal/peer TestSpoolIdempotentPerSequence`、`TestShipBatcherHedge*`（跳过/触发/关闭/全失败/异步/自适应边界）。
- **文档**：`docs/cell-protocol.md`、`docs/networking.md`、`docs/configuration.md`、`docs/testing.md`、`docs/release-notes.md`。

## ADR-165 可观测性指标补齐（batch 1：server 侧）✅实现

- **背景**：`docs/observability.md` 有一份"计划中（勿据此配告警）"指标清单，但**告警建议**里有两条（`cellhive_takeover_total{outcome="timeout"}`、`cellhive_durability_proof_seconds`）当时并不存在——文档自相矛盾。ADR-157 起就要求"未实现的指标显式标注"。
- **决策**（只做 server/cell-agent 侧、可本机验证的部分）：
  - `cellhive_binding_calls_total{kind,outcome}`：`scopeAuth` 包装 `statusRecorder`，按 `ok`(2xx)/`denied`(401/403/429)/`error` 计数（覆盖 kv/d1/r2/queue/workflow/vectorize/do/hyperdrive 等所有绑定端点）。
  - `cellhive_durability_proof_seconds`：`capturedWrite` 里计时 `Capture.Wait`，输出 Prometheus histogram（桶 5ms/25ms/100ms/500ms/1s/5s + `_sum`/`_count`）。实现上每次观测对「`le` ≥ 观测值」的桶都 +1，因此渲染时不二次累加。
  - `cellhive_owner_epoch_changes_total{role="owner"}` 与 `cellhive_takeover_total{outcome=success|failed|blocked}`：`owner.Manager` 加原子计数——`nextEpoch` 成功计 epoch bump；`Claim`/`ClaimAs` 接管**过期且属于其他节点**的记录计 success/failed；活跃外部租约阻塞计 blocked。
  - `cellhive_route_projection_version`：投影重建时把 control `Rev` 存原子量并作为 gauge。
  - `cellhive_peer_hedge_fired_total`/`_won_total`（ADR-164 收尾）：`followersRace` 记录 hedge 副本发出次数与"副本赢得 ack"次数。
- **仍待实现（明确留在"计划中"）**：`cellhive_replication_bytes_total{kind}`、`cellhive_waker_fires_total{kind,outcome}`、以及需 do-runtime 侧暴露端点的 `cellhive_do_*` 系列。`docs/observability.md` 的告警建议改为使用已存在指标（takeover failed、proof p99 经 histogram_quantile、binding denied）。
- **理由**：先消除"文档引用不存在指标"的矛盾，并给最关键的写路径证明延迟、鉴权拒绝、接管竞争加上可告警信号。
- **验证**：`internal/server TestMetricsObservability`（binding calls/proof 桶/投影版本/owner/hedge 行）、`internal/owner TestClaimStatsTakeoverAndEpoch`/`TestClaimAsStats`、`internal/peer TestShipBatcherHedgeStats`。
- **文档**：`docs/observability.md`、`docs/cell-protocol.md`、`docs/testing.md`、`docs/release-notes.md`。

## ADR-166 可观测性指标补齐（batch 2：复制/定时器 + do-runtime）✅实现

- **背景**：ADR-165 后 `observability.md` 的"计划中"还剩 `cellhive_replication_bytes_total{kind}`、`cellhive_waker_fires_total{kind,outcome}` 与全部 `cellhive_do_*`（后者需要 do-runtime 侧暴露端点）。
- **决策**：
  - **复制字节**：`peer.ShipBatcher` 统计每次发出的副本字节（`shipBytes`，`ReplicationBytes()`），server `/metrics` 出 `{kind="shipped"}`；follower 侧在 `/v1/peer/append`、`/append_batch`、持久流 `/v1/peer/stream` 成功落盘后累加 `s.peerRecvBytes`，出 `{kind="received"}`。
  - **定时器派发**：`internal/timer.DispatchDue` 是唯一派发点；新增包级 `FireStats()`（同 `pagedvfs.SnapshotStatsAll` 风格，无需接线），每次派发记 `kind|ok|failed`，server 出 `cellhive_waker_fires_total{kind,outcome}`（waker 与单 scope runner 都覆盖）。
  - **do-runtime**：Go 监督进程 `cellhive-do-supervisor` 在 `-listen` 上新增 `GET /metrics` 与 `POST /internal/do/stats`：
    - 监督进程自己可测的：`SyncAll` 累加捕获输入字节（`cellhive_do_wal_captured_bytes_total`）、`RestoreAll`/`RestoreObject` 计时（`cellhive_do_restore_seconds` summary）、`/sync-all` 失败（`cellhive_do_output_gate_timeouts_total`）。
    - host actor 侧的 `alarm` 结果与 WebSocket 会话数由 `host.js` 在每次 invoke 后 best-effort 上报增量（`alarms_ok`/`alarms_error`/`gate_timeouts`/`ws_sessions`），监督进程累加并在会话 gauge 变负时钳到 0（host 重启语义）。`alarm()` 抛错改为返回 JSON 500（原先是不可区分的 workerd 500），以便仍走 `__chAlarmState`+输出门+指标上报。
- **理由**：把仅剩的三类"设计里有、代码里没有"的指标补齐，`observability.md` 不再有"计划中"项；DO 侧运行期信号（捕获字节、恢复耗时、门超时、alarm、连接数）终于可抓。
- **代价/边界**：do 指标是**进程级**（监督进程/host actor 重启会清零；host 会话 gauge 靠增量与钳零近似）；`cellhive_do_wal_captured_bytes_total` 按捕获时的源文件大小计（非 LTX 落盘字节）；`ws_sessions` 只统计 host 侧能看到的升级/中继与 abort 关闭，facet 内部 socket 生命周期是近似。
- **验证**：`internal/timer TestFireStatsAggregates`/`TestDispatchDueObservesFireOutcomes`、`internal/dosupervisor TestSupervisorMetricsAndStats`、`internal/doruntime TestDoRuntimeMetricsReportedToSupervisor`（真 workerd：alarm 后 host 上报 `/internal/do/stats`）、`internal/server` 既有 `TestMetricsObservability` 覆盖复制/定时器行。
- **文档**：`docs/observability.md`、`docs/testing.md`、`docs/release-notes.md`。

## ADR-167 标准 OpenTelemetry OTLP 追踪导出 ✅实现

- **目标**：走**标准 OTel 协议**，后端可随时更换（Collector/Tempo/Jaeger/云），CellHive 不自存 trace。
- **决策**：
  - 采用官方 `go.opentelemetry.io/otel` SDK + `otlptracehttp`（OTLP/HTTP，不引 gRPC）：`internal/telemetry` 负责 exporter（`CELLHIVE_OTLP_ENDPOINT`、`CELLHIVE_OTLP_HEADERS`）、`ParentBased(TraceIDRatioBased(CELLHIVE_TRACES_SAMPLE_RATIO))` 采样、W3C `traceparent` 传播、Resource（service.name/version/node_id）。端点为空 → 全程 no-op（`telemetry.Enabled()` 早退，零开销）。
  - **Go span**：cell-agent 顶层 `trace` 中间件（`http.server`，`statusRecorder` 转发 Hijack/Flush 以免破坏 peer 流）、`capturedWrite` 的 `cell.durability_proof`、`peer.ShipBatcher` 每个 follower 副本的 `peer.append`；`scopeAuth` 给当前 span 打 namespace/kind/name 属性。
  - **JS span**：workerd 跑不了 OTel SDK，新增 `workerd/platform/telemetry.js`（有界缓冲 + best-effort `POST /v1/internal/telemetry/spans`，internal token）。loader 上报入口 `http.server` span 并在生成 traceparent 时按 `CH_TRACE_RATIO` 决定 sampled 位；do-runtime host 上报 `do.invoke`/`do.gate`。cell-agent 的 `/v1/internal/telemetry/spans` 用 `telemetry.Ingest` 把记录重建进同一 trace 再导出。
  - **传播修复**：loader 在自身 isolate 设置 `globalThis.__cellhiveTraceparent`，props-bound binding 调用（bindings.js）因此带上 `traceparent`，补上 ADR-146 的残余（对应 `TestUserRuntimeTraceContextPropagation` 由"期望为空"改为"期望等于客户端值"）。
- **理由**：标准协议 + 官方 SDK 提供批处理/重试/protobuf/资源语义，避免自研协议；OTLP 端点可换。
- **代价/边界**：引入 OTel SDK 及若干传递依赖（protobuf、`golang.org/x/net` 等；`go mod tidy` 后为直接依赖）；JS 只覆盖平台 worker（loader/host、bindings 调用），租户 isolate 的 `facades.js` 回退路径与 DO connect/abort、后台 compaction/upload 无 span；JS span 时间戳毫秒精度；采样为入口头部采样。
- **验证**：`internal/telemetry TestExportsSpanAndRemoteSpan`/`TestSamplerHonorsRemoteFlag`/`TestDisabledIsNoop`（进程内假 OTLP 接收器解码 protobuf）、`internal/userruntime TestUserRuntimeTraceExport`（真 workerd：loader span 到 `/v1/internal/telemetry/spans`）、`internal/doruntime TestDoRuntimeSpanExport`（真 workerd：`do.invoke` span）、`internal/dosupervisor TestGateEmitsSpan`（`do.gate`）、`TestUserRuntimeTraceContextPropagation`（binding 传播）。
- **文档**：`docs/tracing.md`（含 OpenObserve/Collector/Tempo/Jaeger 对接）、`docs/observability.md`、`docs/configuration.md`、`docs/testing.md`、`docs/release-notes.md`。

## ADR-168 R2 list 的 include / delimiter（CF 对齐）✅实现

- **背景**：ADR-163 把对象的 `httpMetadata/customMetadata/uploaded/checksums` 放进逐对象 sidecar（`r2meta/...`），`list()` 默认只回 `key/size/etag`（避免一页 N 次额外读）；而 CF 的 `list()` 支持 `include:["httpMetadata","customMetadata"]`（正是为"这些字段贵、默认不回"设计的显式选择器）与 `delimiter`/`delimitedPrefixes`。
- **决策**：
  - `/v1/r2/list` 解析 `include`（逗号分隔；只接受 `httpMetadata`/`customMetadata`，其他值 400 `invalid_include`）：**仅当请求时**逐对象 `GetMeta`，响应对象复用 `r2ObjectJSON` 形状带上 metadata/uploaded/checksums；不请求时零额外读。
  - `delimiter`：`r2.Store.ListPageDelimited` 按 CF 语义——把 `keyPrefix` 之后含 delimiter 的 key 归并成公共前缀 `keyPrefix + 到含 delimiter`，去重回 `delimitedPrefixes`，其余作为 `objects`；`after` 独占游标；扫描上限 10000 keys，达到上限或 `limit`（objects+prefixes 合计）时返回 `cursor`（=最后扫描的 key）供续扫，否则 `next=""`。
  - binding（`bindings.js`）与 facade（`facades.js`）透传 `delimiter`/`include`，`delimiter` 时返回 `delimitedPrefixes`（默认 `[]`）。
- **理由**：与 CF 对齐，且默认成本不变（metadata 只在显式 `include` 时读）。
- **代价/边界**：`include` 是 N 次 sidecar 读（用户显式请求）；`delimitedPrefixes` 仅在 `delimiter` 时出现；仍未实现 object versioning / SSE-C / conditional put（有意不做，见 compatibility-matrix）。
- **验证**：`internal/r2 TestListPageDelimited`、`internal/server TestR2ListDelimiterAndInclude`、`internal/userruntime TestUserRuntimeR2ObjectFidelityAndD1Meta`（真 workerd）。
- **文档**：`docs/compatibility-matrix.md`、`docs/bindings.md`、`docs/known-issues.md`、`docs/testing.md`、`docs/release-notes.md`。

## ADR-169 purge 改用游标分页删除 ✅实现

- **背景**：purge 的数据侧 hook（ADR-142）此前用 `objectstore.ListPrefix`（内部 `bucket.List`）**物化整段键列表**再删；大命名空间（`cells/<ns>/`、`assets/<ns>/`）的内存与成本不受控（known-issues 记录为残余）。
- **决策**：新增 `objectstore.Objects.ListPage(ctx, prefix, after, limit)`（优先 `bucket.PagedLister`：S3 `StartAfter`+`MaxKeys`、FS 有界选择；无扩展的 bucket 回退 `List`+切片），并保持前缀边界与 owner 校验；`Purger.delete` 改为每页 ≤1000 的游标遍历，边删边推进（删除是单调的，exclusive cursor 依然有效），预算耗尽返回 `done=false` 由 `RunPurgeLoop` 下一轮续跑。
- **理由**：内存有界、S3 上列取高效；不改控制面 schema、不持久化跨轮 cursor（保持简单）。
- **代价/边界**：每轮从目标前缀起点重扫（未持久化跨轮 cursor）；FS dev 后端每页仍走树（但内存有界）。
- **验证**：`internal/objectstore TestObjectsListPage`（分页/游标/前缀边界/保留前缀拒绝）、`internal/purge TestPurgePaginatesAcrossBudget`（25 键 / 预算 10 → 多轮 drain 且只删目标 ns）。
- **文档**：`docs/known-issues.md`、`docs/testing.md`、`docs/release-notes.md`。

## ADR-170 DO 输出门并发捕获 + orderedDispatcher 主动淘汰（补齐 ADR-047/048 的 workerd 接入）✅实现

- **背景**：ADR-047 的"下一步"（`CommitAsync` + owner 按 `start_txid` 有序交接）由 ADR-048 实现，但两处没闭环：
  1. `orderedDispatcher.sweep`（空闲 scope 淘汰）**没有任何生产调用者**（死代码）：永久 txid 缺口只能靠调用方 ctx 超时，空闲 scope 与其缓冲请求不会释放。
  2. `internal/dosupervisor.SyncAll`（DO/workerd 输出门）仍**逐文件同步 `Commit` 等证明**，完全没有跨 scope 的 RTT 重叠（ADR-048 边界里的"workerd 接入待测"）。
- **决策**：
  1. `orderedDispatcher.Do` 入口调用 `sweep(time.Now())`（`sweep` 内部按 `ttl/2` 节流），把空闲超过 TTL 的 scope 的缓冲请求以 `evicted` 错误释放并删除该 scope；生产不再依赖调用方超时清理。
  2. `Supervisor.SyncAll` 改为**有界并发**捕获：`Concurrency`（默认 8）个 worker 并发跑 `syncFile`，每个文件由 `fileState.mu` 串行、保持自身 txid 顺序；`wg.Wait()` 后**所有证明都完成才返回**（输出门语义不变）；任一失败返回错误且不写 manifest（`n` = 已成功文件数）。
- **理由**：不同 facet 文件是不同 scope，可安全并行提交以重叠 RTT；同一文件内保持串行，回避 `orderedDispatcher` 对"多个 snapshot 分片共享同一 start/end txid"的边界（分片不能走有序分派器）。
- **代价/边界**：同文件内提交仍串行；并发只跨文件（DO 一个门通常有多个 facet 文件，收益在此）；`orderedDispatcher.Close` 仍未接进关机路径（缓冲调用方靠 ctx 取消释放）；跨主机真实 RTT 复测仍属 C 类环境。
- **验证**：`internal/server TestOrderedDispatcherDoSweepsIdleScope`（另一 scope 的 `Do` 触发淘汰，缺口请求收到 evicted）；`internal/dosupervisor TestSyncAllPipelinesAcrossFiles`（6 文件 / 4 并发：max in-flight ≥2、总耗时 < 串行 `files×delay`、失败仍报错）；两包 `-race` 通过。
- **文档**：`docs/decisions.md`（ADR-047/048 修订）、`docs/testing.md`、`docs/release-notes.md`。

## ADR-171 upload spool 断电安全（原子写 + 目录 fsync 记忆化）✅实现

- **背景**：`known-issues` 记着异步上传 spool（ADR-143）的残余——`Spool.Append` 只有 `WriteFile(tmp)+Rename`，**没有 fsync**，进程崩溃安全但**断电可能丢**。
- **决策**：`Spool.Append` 改为
  1. `OpenFile(tmp)` → `Write` → **`File.Sync()`（fsync 数据）** → `Close`；
  2. `Rename(tmp, final)`；
  3. **目录 fsync**：叶子目录（让 rename 的名字持久）+ 其父目录（让 spool 目录自身的目录项持久）各一次，且**按进程记忆化**（目录项只在创建时变化，故每进程只 sync 一次；失败则不置记忆位，下次 append 重试）。任一步失败返回错误（fail-closed），调用方计 `spool_errors` 且不把该 entry 当作 durable；若文件仍在，后续 replay 幂等。
  4. **遥测**：`Spool.Stats()` 暴露 `file_syncs/dir_syncs/sync_us/rename_us`，经 `Batcher.Stats()` 出 `/metrics`：`cellhive_upload_spool_file_syncs_total`、`_dir_syncs_total`、`_sync_seconds_total`。
- **理由**："原子 tmp→fsync→rename + 目录链 fsync + `synced_namespaces` 记忆化 + write/fsync/rename/dir 遥测"把每次 persist 的 fsync 从 ~9 次降到稳定 1 次。
- **代价/边界**：异步上传路径每次 append 多一次文件 fsync（不在写 ack 热路径）；目录 fsync 只在首次；`Remove` 不 fsync（删除未持久只会导致 replay，幂等安全）。
- **验证**：`internal/upload TestSpoolDurableAppendSyncs`（每次 append `file_syncs` +1、`dir_syncs` 记忆化不增长、`Load` 仍可读）、`TestBatcherStatsIncludeSpoolDurability`（`/metrics` 经 `Batcher.Stats()` 暴露）。
- **文档**：`docs/known-issues.md`、`docs/observability.md`、`docs/testing.md`、`docs/release-notes.md`。

## ADR-172 租户日志可选 OTLP 导出（默认关，tail 订阅才导）✅实现

- **背景**：租户 `console.*` 只进 cell-agent 的**有界内存 ring**（ADR-136，非持久、单节点），`cellhive tail` 轮询；运维需要"能集中留存/查询"的能力，但全量导出日志量太大。
- **决策**：在 ring 之外加**标准 OTLP/HTTP logs 导出**（官方 `go.opentelemetry.io/otel/sdk/log` + `otlploghttp`，复用 traces 的 `CELLHIVE_OTLP_ENDPOINT`/`_HEADERS`/resource，路径 `/v1/logs`）：
  - `CELLHIVE_OTLP_LOGS=off|tail|all`，**默认 `off`**（`CELLHIVE_OTLP_ENDPOINT` 为空也视为关）；`config.Validate` 拒绝非法值。
  - `all`：`handleLogIngest` 落 ring 后每条都 `telemetry.ExportLog`。
  - `tail`：**只在存在活跃订阅时导出**。`cellhive tail --worker <ns>/<worker>` 每次轮询前 `POST /v1/control/logs/subscribe`（body `{namespace,worker,ttl_ms}`，TTL 60s，每轮自动续）；订阅过期即停止导出。订阅注册表在 `internal/telemetry`（`Subscribe`/`LogSubscribed`，惰性清理过期项）。
  - 导出是**旁路、best-effort、后台批量**（`sdk/log` BatchProcessor 2s），失败不返回错误、不阻塞请求。
- **理由**：标准 OTLP 可换后端（Collector/OpenObserve/云）；`tail` 门控把日志量限制在"有人正在看"的 worker；需要全量集中采集时切 `all`。
- **代价/边界**：订阅是**单节点**的——`tail` 只向它连接的 cell-agent 订阅，多节点 fleet 中其他节点上的同名 worker 不会被导出（要全量用 `all`）；ring 仍非持久（OTLP 是旁路增强，不替代 ring）；JS 目前不送 `request_id`（tail/导出的行无请求关联）；导出不保证不丢（队列满/后端不可用时丢弃）。
- **验证**：`internal/telemetry TestLogExportModes`（off/all/tail 门控 + TTL 过期）、`TestLogSubscribeTTL`；`internal/server TestLogSubscribeEnablesExport`（订阅端点 + `LogSubscribed`）、既有 `TestLogIngestAndQuery`/`TestUserRuntimeLogTail`/`TestDoRuntimeLogTail`。
- **文档**：`docs/observability.md`、`docs/configuration.md`、`docs/tracing.md`、`docs/known-issues.md`、`docs/testing.md`、`docs/release-notes.md`。

## ADR-173 日志订阅的 fleet 广播（tail 门控覆盖全部节点）✅实现

- **背景**：ADR-172 的 `tail` 门控订阅是**进程本地**的，而 `cellhive tail` 只连一台 cell-agent；多节点 fleet 中由其它节点服务的同名 worker 不会被导出（要全量只能 `all`）。
- **决策**（对等广播，不引入控制面状态）：
  - 新增内部端点 `POST /v1/internal/logs/subscribe`（internal token）→ 本地 `telemetry.Subscribe(ns,worker,ttl)`。
  - admin 的 `POST /v1/control/logs/subscribe` 在本地订阅后调用 `fanOutLogSubscribe`：从 `lease.SampleCached` 取活节点（用 `Advertise`=内部 REST 地址，排除自己），best-effort（3s 超时、fire-and-forget）向每个节点投递同样的 `{ns,worker,ttl}`。**每 worker ≤25s 节流一次**；节点上的 TTL（60s）保证续订期间不失效，漏掉的节点在下次续订（`cellhive tail` 每轮）≤25s 内补上。
- **理由**：复用既有的 lease 节点名单与 `:7001` 内部 REST 平面，零新 schema、零常驻轮询循环；`tail` 的语义变成"整个 fleet 为这个 worker 导出"。
- **代价/边界**：广播是 best-effort（节点短暂失联会漏一次，靠续订恢复）；新增节点在下次广播前不知道订阅；一个 tail 会带来每 25s O(N) 次内部 POST；节点地址用 `Advertise`（需其它节点可达其 REST；跨网段/不同内部端口的部署需一致）。
- **验证**：`internal/server TestLogSubscribeFleetFanout`（admin 订阅广播到第二个（httptest）节点，且内部端点本地生效）、既有 `TestLogSubscribeEnablesExport`/`TestLogIngestAndQuery`/`internal/telemetry TestLogExportModes`。
- **文档**：`docs/observability.md`、`docs/tracing.md`、`docs/known-issues.md`、`docs/testing.md`、`docs/release-notes.md`。

## ADR-174 示例兼容性修复（KV put 体类型 / legacy DO 类 / DO 响应透传）✅实现

- **背景**：用一组 Cloudflare 风格示例逐个跑 CellHive 真实栈（cell-agent + user-runtime + do-runtime）复测兼容性，暴露并按需修复三类缺口。
- **KV `put` 体类型**：此前对非 string 一律 `JSON.stringify`，所以 `env.VALUES.put(key, request.body)`（ReadableStream）被存成了字面量 `"{}"`。改为 string / ArrayBuffer / ArrayBufferView / Blob / ReadableStream 原样交给 fetch（普通对象仍 JSON 兜底）。`TestUserRuntimeKVPutStreamAndBytes`。
- **legacy DO 类**：只实现 `fetch/alarm/webSocket*`、未 `extends DurableObject` 的类，workerd 不允许以 facet stub 调用（`does not support RPC`）。do-runtime 现在按 worker 的 DO binding 生成 facet 入口模块（`renderFacetModule`）：已继承平台基类的类原样导出；legacy 类包装成 `extends cellhive-do.js DurableObject` 的子类，把 `fetch/alarm/webSocketMessage/Close/Error` 转给内部实例（`new Inner(ctx, env)`），从而沿用平台 storage/alarm shim。`TestDoRuntimeClassicDOAndStatus`。
- **DO 响应透传**：`env.DO.get(id).fetch(req)` 此前对任何非 2xx 抛错（边缘变 500），且 cell-agent 代理强制 `content-type: application/json`。现在 do-runtime 的 facet 响应与其顶层 sharding 包装都标记 `x-cellhive-do-app: 1` 并保留 tenant 的 content-type；cell-agent 透传该标记、状态与 content-type（facade 仅在**无标记**的平台错误包络上抛错）。`TestDOProxyForwardsTenantResponse` + `TestDoRuntimeClassicDOAndStatus`。
- **R2ObjectBody**：补 `body`（ReadableStream）与 `writeHttpMetadata`。**边界**：`writeHttpMetadata(headers)` 依赖修改调用方 Headers，而 props-bound binding 走 workerd RPC（参数按值序列化），跨 entrypoint 的修改会丢失——列为 known-issues；`httpMetadata` 字段与 `head()` 正常。
- **未通过的示例**（策略/已登记缺口，非回归）：workflow/wsclient 因 `compatibility_date` 超过平台上限 `2026-06-22` 被拒；facets 的 `worker_loaders`、vectordb 的 `sqlite_vec` 为未登记字段/flag 被拒；**wsecho 的完整 WS echo 不可用**——`env.DO.get(id).fetch(升级请求)` 未接 do-runtime 的 `/v1/do/connect`（known-issues）；alarm 需 do-supervisor gate 才会被调度（本矩阵未起 gate）。
- **验证**：三个新测试；运行矩阵含 Hono 4.13.8 从 `node_modules` 打包 + 多路径 + `--path` 挂载 + KV binding 全 PASS。
- **文档**：`docs/compatibility-matrix.md`、`docs/known-issues.md`、`docs/durable-objects.md`、`docs/testing.md`、`docs/release-notes.md`。

## ADR-175 兼容性收口（DO WebSocket 接线 / R2 本地 metadata / DO alarm 身份）✅实现

- **DO WebSocket 接线**（补 ADR-174 缺口）：租户 `env.DO.get(id).fetch(升级请求)` 不能走 JSON invoke（活 socket 无法序列化）。facade 现在检测 `Upgrade: websocket` → `GET /v1/do/connect`（scoped token）向 cell-agent 取 `{owner, ticket}` → 带 ticket 直连 owner 的 `/v1/do/connect`；do-runtime 允许 ticket 用于 `GET /v1/do/connect`（仅本 shard，校验 ns/worker/storage_class/shard）。跨节点仍由 do-runtime `connect`/`proxyConnect` 处理，1012 语义不变。`wsecho` 示例的完整 WS echo 通过。
- **R2 `writeHttpMetadata`**（补 ADR-174 缺口）：props-bound binding 走 workerd RPC（参数按值序列化），entrypoint 侧方法无法改调用方 `Headers`。`loader.js`/`buildFacetEnv` 现在传 `CH_R2_BINDINGS`，`queue-wrapper.js`/`bindings-wrapper.js` 对每个 R2 binding 套一层本地 Proxy（`facades.js wrapR2Metadata`），在租户 isolate 重新挂上 `writeHttpMetadata`。`r2` 示例的 content-type 正常；`body`/`head`/`list` 等其余方法原样透传（DO 内 R2 同样生效）。
- **DO alarm 身份补全**：此前 alarm dispatch 缺 `storage_id`/`storage_class`，host 会按 `storage_id/shard` 选 Host actor，导致 alarm 落在与 fetch **不同的 facet**（写进另一份 sqlite，`fires` 永远 0）；且 do-runtime 地址无 scheme 时 dispatcher 每 tick `parse "host:port/..."` 失败。现在 `reportAlarm` 携带 `storage_class`/`storage_id`，occurrence 改为 JSON 身份，dispatcher 原样回放全部身份字段并补 `http://`。`dispatch TestDoAlarmDispatcherAddsScheme`（scheme）、`server TestDOAlarmOccurrenceCarriesIdentity`（身份/`|` 安全）、`TestDoAlarmDispatcherRoutesAlarms` 断言 body 含 storage 身份。
- **验证**：真实门控栈（cell-agent + user-runtime + do-supervisor 门 + do-runtime）跑示例套件 + Hono：alarm/wsecho/r2/kv/d1/counter/router/rpc/async/hello/webapi/cron 全 PASS；workflow/wsclient/facets/vectordb 仍按策略拒绝；`bash scripts/ci.sh` GATE PASS。
- **文档**：`docs/compatibility-matrix.md`、`docs/known-issues.md`、`docs/durable-objects.md`、`docs/testing.md`、`docs/release-notes.md`。

## ADR-176 KV 写入校验对齐 Cloudflare（key/meta/TTL/expiration）✅实现

- **背景**：Cloudflare KV 有明确限制（key≤512B、metadata≤1KiB、`expirationTtl`≥60s、`expiration` 须在未来、value≤25MiB），而 CellHive 服务端此前只拦 25MiB body：`expiration_ttl`/`expiration` 只要求 >0、key/metadata 不校验，会接受 CF 会拒绝的写入（lenient divergence）。
- **决策**：在 `handleKVPut`（并在 `get`/`delete` 校验 key）对齐：key ≤512B（`key_too_large`）、metadata ≤1KiB（`metadata_too_large`）、`expirationTtl` ≥60s、`expiration` 必须在未来（`bad_expiration`）。value ≤25MiB 维持原有 413。query-only 校验放在转发之前，非 owner 本地即拒。
- **语义**：过期仍由读过滤（`expires_ms>now`）+ 每 cell 的 `KindKVExpire` timer 回收；绝对 `expiration` 过去时间从"可写入但立即可见为无"改为**拒绝**，与 CF 一致。
- **代价**：此前能写入的"过去 expiration / <60s TTL"现在报错（有意的兼容收紧）；`internal/server TestKVTTLAndMetadataE2E` 改为用近未来 expiration + 等待来验证读过滤。
- **验证**：`internal/server TestKVValidationLimits`（513B key、1025B metadata、ttl 59/60、过去/未来 expiration、恰好 1KiB metadata）。
- **未对齐（已知）**：CellHive KV 的 value 仍 **≤25MiB 全量内联** SQLite 并走 LTX 复制（对齐做法在 >1MiB 时转 bucket blob + 同 alarm GC），这是规模/延迟缺口，留在 `docs/known-issues.md`。
- **文档**：`docs/compatibility-matrix.md`、`docs/bindings.md`、`docs/testing.md`、`docs/release-notes.md`、`docs/known-issues.md`。

## ADR-177 wake 索引不落后于 timer（先发布 + 可修复）✅实现

- **背景**（"索引非权威、可修复"）：timer 行在所属 cell 的 SQLite、wake 索引 `wake/<scope>.json` 在桶，`timer.Store.syncIndex` 是**提交后 best-effort**。若 owner 在"timer 行提交"与"索引 PUT"之间崩溃，或桶写失败，索引就**落后于** timer：死 owner 的到期项永远没有索引条目，waker 发现不了 → 丢失唤醒（KV 过期、DO alarm、workflow sleep 等）。
- **决策一（不变量：索引不落后）**：`Store.Upsert` **先**把索引条目 PUT（取"本次 due 与当前最早 due 的较小者"，确保不会把更早的 timer 盖晚）**再**提交 timer 行；索引 PUT 失败则 Upsert 失败（fail-closed，调用方重试，且不留下无索引的 timer）。提交后（含提交失败时）再 `syncIndex` 精确校正。这样崩溃只可能留下**索引领先**（false positive，waker 打开后发现无到期项即自愈），不可能落后。
- **决策二（可修复）**：公开 `Store.SyncIndex`，在 `registerTimerScope`（节点注册/认领/接管 timer scope，含启动恢复）时重发索引；新增**有界轮转**的本地修复循环：扫描本节点 `cells/*/*/*.db`（`cellstore.LocalScopes` + `PendingTimerMin`，**只读**、不迁移、稀疏 paged 未挂载则跳过），把有 pending timer 的 scope 重发/删除索引，并给自有 scope 重新 Arm。默认 `CELLHIVE_WAKE_REPAIR_INTERVAL=5m`、`_BATCH=256`（0 关）。
- **理由**：崩溃窗口靠写序不变量消除；索引条目丢失/陈旧靠修复循环与认领点重建；waker 仍只读索引，不引入全量扫描 bucket。
- **边界**：修复只扫**本节点**的 cell（死节点的盘不可达；其 timer 由新 owner 认领时的 `SyncIndex` 重建，且因不变量不存在"根本没被发布过"的 timer）；`PendingTimerMin` 打开 DB 不跑迁移；已知旧数据（本次改动前写入的、缺索引的 timer）由首次修复扫描补上。
- **验证**：`internal/timer TestUpsertFailsClosedWhenIndexDown`（索引失败则不提交 timer）、`TestSyncIndexRepairsMissingEntry`、更新 `TestStoreSyncsWakeIndex`（先发布）；`internal/cellstore TestLocalScopesAndPendingTimerMin`（扫描/只读读取）。
- **文档**：`docs/configuration.md`、`docs/testing.md`、`docs/release-notes.md`、`docs/known-issues.md`。

## ADR-178 日志带 trace 上下文 + 多租户可观测性参考管线（push）✅实现

- **背景**：ADR-167/172 已有 OTLP traces/logs 导出，但（a）租户日志行没有 `trace_id`/`span_id`，后端无法从 trace 跳日志；（b）对外查询一直靠"每节点裸 `/metrics` + 自建鉴权"，与多租户目标冲突。
- **决策**：
  1. **日志关联**：`log-tail.js` 在每次 `console.*` 时取当前 `globalThis.__cellhiveTraceparent` 作为该行的 `traceparent` 上报；cell-agent 解析（`telemetry.TraceIDs`）为 `trace_id`/`span_id`，写入 `logbuf.Entry` 与 OTLP log record（SDK 从 Emit 的 ctx 取 span context）。请求级 `traceparent` 头作为回退。
  2. **统一资源属性**：resource 增加 OTel 规范的 `service.instance.id`（= 节点 id），保留 `cellhive.node_id`；日志/span 继续带 `cellhive.namespace`/`cellhive.worker`。
  3. **对外查询用 push**：日志/追踪走 OTLP push 到内网 **Collector**（参考配置 `deploy/observability/otel-collector.yaml`：redact/route/tail-sample → OpenObserve），平台不暴露查询面；**唯一面向租户的跳是后端**，按 namespace 映射到后端 org/stream + 只读用户，不靠属性做行级隔离。**指标保持内部 Prometheus pull**，`/metrics` 免鉴权、不公网暴露；要并入 OTLP 用 Collector 的 `prometheus` receiver。
- **理由**：push 把"暴露端点 + 自建鉴权"换成"后端 RBAC"；`trace_id` 关联让 指标→trace→日志 可跳转；资源属性对齐 OTel 语义，换后端不改代码。
- **代价/边界**：OTLP 导出仍是 best-effort（后端不可用丢遥测）；租户查询依赖后端能力（无行级过滤时需 per-org/stream 映射）；平台不提供自带查询 API/存储（长期项）。
- **验证**：`internal/telemetry TestLogExportTraceCorrelation`（traceparent → OTLP record 的 trace/span id；非法/全零拒绝；无 traceparent 不携带上下文）、`internal/server TestLogIngestAndQuery`（逐条 + 请求头回退都写入 `trace_id`）；`bash scripts/ci.sh` GATE PASS。
- **实测修复（ADR-179 期间）**：新增 `scripts/openobserve-e2e.sh` 真实起 OpenObserve + 仓库参考 collector 配置做端到端，发现参考配置的 `attributes/redact` 用了 `key_regex`（当前 collector 对 `delete` 只接受显式 key，配置直接启动失败）→ 改为显式删除 `authorization`/`x-cellhive-internal-token`/`x-cellhive-scope-token`；并确认 log record 的 `trace_id`（cell-agent 缓冲区与 OTLP 记录都有，`4bf9…`）与 span 同 trace。
- **文档**：`docs/observability.md`（多租户与对外查询）、`docs/tracing.md`、`docs/modules/observability.md`、`docs/en/*` 同步、`deploy/observability/{otel-collector.yaml,README.md}`、`deploy/compose/docker-compose.yml`（`observability` profile）。

---

## ADR-179 指标按命名空间归属（ns 维度）+ 绑定 span 带租户属性 ✅实现

- **背景**：ADR-165 的 `cellhive_binding_calls_total{kind,outcome}` 与 `cellhive_durability_proof_seconds` 是全局聚合，运维无法回答"哪个租户在打/哪个租户慢"；直接把 `ns` 当标签又会因任意租户名打爆 Prometheus 基数。
- **决策**：
  1. **有界 ns 标签**：`/metrics` 的租户可归因指标带 `ns` 标签；cell-agent 维护已见 ns 集合，超过 `CELLHIVE_METRICS_NS_MAX`（默认 1000，0=不限）的后续 ns 一律记为 `ns="other"`，空 ns（无 scope 的内部调用）记为 `ns="platform"`，基数因此有上界。
  2. **归属范围**：`cellhive_binding_calls_total{ns,kind,outcome}`、`cellhive_durability_proof_seconds_*{ns}`（直方图改为 per-ns）；`cellhive_requests_total` 保持全局不加 ns（避免与绑定调用双计）；cellstore/磁盘/租约等**节点级** gauge 不加 ns 并在文档注明。
  3. **trace 同步归属**：绑定端点的 `http.server` span 在 `scopeAuth` 解析出 namespace 后追加 `cellhive.namespace`/`cellhive.binding` 属性，使 指标→trace 按同一维度关联（未开 tracing 时为空操作）。
- **理由**：把"per-tenant 观测"做成**有界标签**而非无界标签或独立端点，既不炸基数也不新增鉴权面（`/metrics` 仍内部 pull）；ns 与 trace 属性同名同值，后端可按 ns 对齐 指标/trace/日志。
- **代价/边界**：溢出到 `other` 后无法区分是哪个长尾 ns；热路径多一次加锁查表（命中后只读锁、未命中才写入）；`/metrics` 仍免鉴权（留给网关，见 ADR-178）。
- **验证**：`internal/server TestMetricsNamespaceLabels`（两个 ns 各自成标签、超 `MetricsNSMax` 溢出为 `other`、空 ns → `platform`）、`TestMetricsObservability`（既有直方图/计数在新标签下仍正确）；**真实 collector 端到端** `bash scripts/otlp-collector-smoke.sh`（Docker 起 `otel/opentelemetry-collector-contrib`，起 cell-agent + user-runtime，部署会 `console.log` 且写 KV 的 worker，带 `traceparent` 请求）：collector 收到 `http.server` span（含 `cellhive.namespace=otlp`）、`cell.durability_proof` span、带调用方 trace_id 的 log record，且 `/metrics` 出现 `cellhive_binding_calls_total{ns="otlp",...}`；`bash scripts/ci.sh` GATE PASS。
- **文档**：`docs/observability.md`、`docs/configuration.md`、`docs/modules/cell-agent.md`、`docs/testing.md`、`docs/en/*` 同步。

---

## ADR-180 接管丢弃陈旧本地 cell（celld took_over 规则）✅实现

- **背景**：本地 SQLite 只在该 scope 由本节点持有时才可信。一个在**接管期间宕机**的节点重启后，本地仍留着一份完整但陈旧的库：读路径在 owner 租约过期/无人持有时直接读本地（`forwardTo` 的 expired 分支），写路径 `forwardOrClaim` 又会用更高 epoch 在陈旧 base 上继续写，产生一条覆盖正确链的 fork 链。实测：A 写 1–100 → B 接管写 101–200 并改 20–50 → A 重启读到的仍是旧值、101–200 读不到；A 再写则新 epoch 链只有 101 keys（覆盖 B 的 200 keys），全新节点从桶恢复也丢 101–200。`OnLostOwner → Forget` 只在**运行中**失权时触发，宕机重启不补触发；`store.Paged`/`Hydrate` 又只对「缺失/带 marker」的冷 cell 生效，完整陈旧文件绕过了按需加载。
- **决策**（对齐 celld `crates/logic/restore.rs` 的 `took_over` 谓词）：**本地 cell 只在「本节点干净续用」时复用；任何接管都丢弃本地、从 durable chain 冷恢复。**
  1. `server.invalidateStaleLocal(sc)`：解析 owner，若 owner 是本节点（或本节点仍持有）→ 保留；若 owner 是**别的节点且租约已过期**、或**无人拥有**→ `store.Forget(sc)`，使下一次 open 走冷路径（paged `PageFetcher` 或全量 `Hydrate`），从桶恢复最新 epoch 链。live 外部 owner 不删（请求会转发，本地不被使用）。
  2. **调用点必须在 `BeginRequest` 之前**：`scopeAuth` 先按 kind 解析 cell scope（`gateScope`，kv/d1/queue/workflow/vectorize）失效，再 `BeginRequest`；否则 `Forget` 会等待本请求自己的 eviction gate 而死锁。内部 `/v1/internal/claim`（`handleClaim`，不在 gate 内）在 `Claim` 前同样失效。
  3. 内部 claim 陈旧副本仍可能出现在 `ClaimAs`（do/queue_admin）路径，属后续对齐项。
- **理由**：复用已有的 `Paged`/`Hydrate`（ADR-092/160），只补「完整文件是否仍属当前 owner」这一判定；同节点重启（owner 仍是自己）保留本地热开，不牺牲既有优化。这正是 celld 的 `previous_epoch_reusable(epoch, took_over) = epoch>1 && !took_over`。
- **代价/边界**：每次绑定请求多一次（1s 缓存的）owner 解析；无人拥有的 scope 首次访问会多一次 hydrate；`handleClaim` 路径强制 hydrate 会放大既有的 control cell 冷恢复时序窗口（torture cycle 3 偶发一次空 control，重跑稳定复现不了）。
- **验证**：`internal/server TestInvalidateStaleLocal`（外部过期/无人拥有 → 删除本地；自己持有 → 保留）；真实两节点 e2e 复现脚本（`/tmp/stale*.sh`）：重启后读 `MODIFIED20`/`v150`、在陈旧副本上写后全新节点恢复 `k20/k150/k201` 全部正确；`scripts/rpo-zero-fault.sh` PASS；`/tmp/torture.sh` 11/11 PASS；`go test ./...`、`make build`/`vet`、`gofmt` 全绿；`-race ./internal/server` 干净。

## ADR-186 stock workerd 运行时基线与安全加固（设计批准，实施中）

- **背景**：当前 pin 仍为 `1.20260615.1`，宿主 URL/token 被模板渲染进 capnp，user-runtime 还把 `CELL_URL`/`CELL_TOKEN` 序列化进最终 WorkerCode；生产镜像缺少 ADR-005 要求的外部 esbuild。compatibility date/flags 依靠人工清单，最终 WorkerCode 与 workerLoader env 也没有前置预算。
- **决策**：
  1. 固定升级到 stock workerd `1.20260916.1`；固定 **esbuild 0.28.2**。校验下载完整性、随附许可证，并在构建和真实镜像内读回版本。dev CLI 只接受同期间、真实 smoke 通过的精确 Miniflare/workerd 组合。
  2. 宿主平台配置改用 capnp `fromEnvironment`；workerd 子进程从空环境构造显式白名单。渲染 capnp、进程参数与日志不含秘密；user-runtime、do-runtime 与 do-supervisor 托管路径遵守同一契约。
  3. 最终动态 WorkerCode 不得包含平台 URL/token。tenant env 继续遵守 ADR-185（平台键为 0、所有名称归用户）；测试递归检查交给 `workerLoader.get()` 的 env、模块名、文本/二进制模块和兼容字段。
  4. 从对应 pin 的 workerd 上游 compatibility 定义生成带 revision/source SHA-256 的单一 manifest；Go 控制面与 Bun CLI 消费同一清单，真实二进制探测允许 flag、未知 flag 和最大 compatibility date；实验 flag 默认 fail-closed。
  5. 最终 WorkerCode 上限 **64 MiB**；workerLoader env 上游 1 MiB，预留 8 KiB，CellHive 上限 **1016 KiB**。控制面在 active pointer 切换前拒绝超限，运行时在 `workerLoader.get()` 前复核；不截断、不删字段、不暴露源码或 secret。
- **升级/回滚**：reader-before-writer；新 pin 先证明能读取旧 artifact/DO working copy，再开放新 date/flag。manifest 与二进制成对回滚；若存在旧 pin 无法加载的 active version，则拒绝回滚。schema、cell/owner/epoch/RPO=0 协议不变。
- **阶段边界**：本 ADR 只覆盖秘密隔离、固定工具链、compatibility authority 与 code/env 预算。冷加载治理、invocation-scoped context、isolate 淘汰、DO mid-flight fence/restart generation、原生 Tail/OTLP 和多模块 artifact 后续分阶段实施。Tenant Tail 当前仍关闭，不以 env transport 回退。
- **验收**：单元测试先红后绿；真实 workerd 覆盖 `fromEnvironment`、env/KV/D1/R2/Queue/Workflow/Service/DO；真实 Docker Compose 覆盖镜像内 esbuild 打包、用户同名 env、KV、gated DO、重启持久性和秘密扫描；最终 `REQUIRE_ALL=1 bash scripts/ci.sh` 无相关隐式 SKIP。
- **详细设计/计划**：`docs/superpowers/specs/2026-09-22-workerd-runtime-baseline-security-design.md`、`docs/superpowers/plans/2026-09-22-workerd-runtime-baseline-security.md`。

---

## ADR-185 租户 env 完全由用户拥有：零平台键

- **背景**：ADR-184 把传输与凭据收敛到平台侧 stub，但仍把 `CH_PLATFORM` 写入 loaded Worker 的 `env`，并因此保留 `CH_*`、`CELL_*`、`__cellhive*` 与历史平台名称。平台键即使没有直接暴露凭据，仍占用用户命名空间，也允许租户代码直接触达平台能力。
- **决策**：
  1. **零平台键、全部名称归用户**：tenant Worker 与 DO facet 的 `env` 只包含用户声明的 version `vars`、用户命名的 binding stub，以及未来以同一规则注入的 secrets；不注入任何平台系统键，不保留任何名称或前缀。`CH_*`、`CELL_*`、`__cellhive*`、`PLATFORM`、`LOG_*`、`WF_*` 等任意合法名称均属用户。控制面删除 `ReservedEnvPrefixes`、`ReservedEnvNames`、`ReservedEnvName`，deploy 的 var/binding 与 secret put 不再返回 `reserved_env_name`；保留名称格式、重复用户 binding、配置 schema、资源登记和不支持 binding type 等无关校验。
  2. **Workflow 以 capability 参数传递**：可信 user-runtime internal host 创建 `WorkflowBridgeTarget extends RpcTarget`，由该 target 留存宿主 env，并固定 `namespace`、`worker`、`workflow`、`id`、`run`；再经原生 JSRPC 作为 `CellHiveWorkflow.handleRun()` 参数传给 wrapper。不能用 `ctx.exports.X({props})` 生成的 `ServiceStub`：pinned stock workerd `1.20260615.1` 会在跨动态 `workerLoader` 边界时拒绝序列化（需要 experimental）。wrapper 用闭包构造交给租户 `run(event, step)` 的 Cloudflare 形状 `step`；capability 不进入 tenant `env`，不直接交给租户 workflow 类，固定 op 表继续拒绝原始路径、身份覆盖和跨租户参数。
  3. **日志优先 stock workerd Tail Worker**：先在 pinned workerd `1.20260615.1` 做真实 spike；通过后，user-runtime 与 do-runtime 的动态 loaded Worker 指向可信 tail service，由它持有平台日志凭据和 private transport，并以不可伪造的 loaded Worker 标识映射回 namespace/worker/version。日志仍 best-effort，不影响租户请求。
  4. **无 Tail 能力时宁可无平台日志**：若动态 Worker 不支持 Tail Worker，删除平台日志捕获并在兼容矩阵明确标记暂不支持；不得恢复向 tenant `env` 注入 log sink、token、URL 或其他系统键。Workflow JSRPC capability 方案不受此回退影响。
- **边界**：可信宿主 Worker 自己的 env 可以持有 `CELL_URL`、角色凭据、private outbound 和 loader；它们不得复制、枚举或暴露给 loaded Worker。租户 global outbound 仍为 public-only；不修改/fork workerd，RPO=0 与既有 binding 权限边界不变。
- **取代范围**：仅取代 ADR-184 中「保留命名空间（防用户命名冲突）+ 平台键压到 2 个」这项，以及其中 tenant env 保留 `CH_PLATFORM`、部署/secret 拒绝保留名的结论；ADR-184 的平台侧 stub、DO fetch 形 RPC、workerLoader 与固定-op 安全边界继续有效，除与本 ADR 冲突者外不变。

---

## ADR-184 租户 env 零平台传输：DO/Workflow/Vectorize 平台侧 stub（WDL 对齐）✅实现

- **背景**：`PLATFORM`（指向 private-outbound 的 service binding）此前注入租户 env，供 DO/Workflow facade、vectorize facade 与 log-tail 出网。它虽无凭据，但让租户代码可以把它当通用私网出口（探测/扫描）。ADR-074 已把角色令牌移出租户 env；本 ADR 移出**传输本身**。
- **决策**：
  1. **平台侧 stub**：`bindings.js` 新增 `DurableObjectNamespace`（`get(id)` 返回 `DOStubTarget extends RpcTarget`，显式 `fetch`/`rpc` 方法）、`WorkflowBinding`（create/get/sendEvent/list；get 返回 `WorkflowInstanceTarget`）、`Vectorize`、`WorkflowSteps`（`call(path,method,body)`）、`LogSink`（`send(ns,worker,batch)`）。`bindingStub` 增加 do/workflow/vectorize case，`CH_FACADE_SPEC` 因此恒为空。
  2. **租户 env 只剩 stub 与名字元数据**：`PLATFORM`/`CELL_URL` 不再注入；DO 只给绑定名元数据（模块作用域常量，无 token/spec），WS 的 owner 查询与 **shard ticket** 由平台 worker 内部完成（do-runtime 的 connect 只认 ticket，不需要 scope token）；workflow 身份（ns/workflow/id/run）与日志身份（ns/worker）绑进 `CH_PLATFORM`（`PlatformBridge`）的 **props**，不再以 `WF_*`/`LOG_NS`/`LOG_WORKER` 出现在 env。
  3. **DO WebSocket 走 `fetch` 形 RPC（无租户传输）**：平台侧 `DurableObjectNamespace` 实现为 **`fetch(request)`**——workerd 只对「方法名 `fetch` + 单个 Request 参数」保留 fetch 语义（流式 + 101/WebSocket 回传），其它名字或把 id 放成前置参数都会退化为普通 RPC、WS 无法序列化（pinned workerd 实测：`Could not serialize object of type "WebSocket"`；同一 101 从 `RpcTarget` 方法返回同样失败）。故 DO id 经共享常量 `DO_ID_HEADER`（`x-cellhive-do-id`）随 Request 传递，平台侧读完即从请求上删除；租户侧 `makeDOFromStub` 只在 cf 形状的 `get(id).fetch(...)` 上补该 header。owner 查询、scoped token、shard ticket 全在平台 worker 内完成，**租户 env 无需任何 WS 传输**。
  4. **workerLoader 语义（实测）**：`ctx.exports` 是**宿主 worker mainModule 的导出集** → 新增 stub 类必须同步 `export {...} from "bindings.js"`（user-runtime 的 loader.js/internal.js、do-runtime 的 host.js）；**方法返回值若是 Proxy，不被 RPC 认作 RpcTarget**（"receiver does not implement"）→ 平台侧只暴露显式方法，任意名由租户侧 Proxy 转成 `(method,args)`。
  5. DO 传输经**模块级缓存**（`doTransports`）跨 RPC 实例保持 owner hint（workerLoader 可能按调用实例化 entrypoint）。
  6. **stub 参数受限（安全边界）**：数据型 stub 不暴露原始 path/身份参数——`WorkflowSteps.call(op,params,body)` 的 `op` 走**固定表**（13 个 workflow 内部端点），`ns` 由 props 强制为调用方 worker 自己的命名空间（参数里的 `ns` 被忽略）；`LogSink.send(batch)` 的 ns/worker 同样来自 props；各 binding stub 的资源名/scope 由 props 固定。故租户即使直接调用这些 stub 也无法把 internal 角色令牌变成开放中继或跨租户操作。
- **理由**：租户 env 不再含任何通用网络出口；私网可达面收敛为「DO WS 的 cluster-only 窄绑定」，其余全部在平台 worker 内完成。与 CF 的一致性提升（CF 租户也只有 namespace 语义的绑定）；与 WDL 的实现方式一致。
- **代价/边界**：DO WebSocket 与普通 DO 调用共用同一个平台侧 stub，**无额外网络跳**（平台 worker 直接连 owner）；租户 env 中已无任何凭据（scoped token 只在平台 worker 内使用，shard ticket 由平台侧 mint 并只发给 owner）。任意 DO 方法仍走 `rpcObject(id, method, args)` 数据通道（ADR-162），因此**返回 RpcTarget/子对象的方法仍不支持**（与 CF 语义的已知差距，未变）。
- **残留**：`CH_FACADE_SPEC` 机制保留但**恒为空**（全部 kind 已 stub 化），作为"新 kind 未处理"的哨兵——非空时打 `console.error` 而非静默丢弃。
- **[ADR-185 仅部分取代] 保留命名空间（防用户命名冲突）+ 平台键压到 2 个**：本项关于 `CH_*`/`CELL_*`/`__cellhive*`、历史平台键的保留和 tenant env 注入 `CH_PLATFORM` 的结论，已由 ADR-185 取代；本 ADR 其余平台侧 stub 与传输结论不变。
- **验证**：`internal/userruntime`（DO fetch/RPC、DO owner hint 直连、vectorize、log tail、workflow 全套、`TestTenantEnvHasNoPlatformCredentials`=PLATFORM/CELL_URL 缺席、`TestTenantWsBindingNarrowed`=窄绑定 allow 生效）、`internal/doruntime`（DO/facet 全套）；`make build/vet/test` 54 包 + `make js-test` 全绿。

---

## ADR-181 凭据收敛与能力令牌委派（a+b 已实现，收敛设计）✅部分实现

> 状态：**凭据收敛的 (a)+(b) 已实现（2026-09-20）；8 凭据收敛/非对称（档 1–3）仍为设计，
> 未实现**。本 ADR 记录收敛设计与已落地部分。

- **已实现（2026-09-20）**：
  1. `internal/scopedtoken`：新增 `Iss` claim（空值仍产出 ADR-074 的 canonical 字节，字段序 `ns,kind,name,iss,exp_ms`）；段级 glob `MatchKind`/`MatchName`（`*` 任意、`pre*` 前缀、否则精确，**逐段锚定**，`acme` 不匹配 `acmex`）；`IssuerKey(scope,iss)=HKDF-SHA256(scope,nil,"issuer/"+iss,32)` 与 `VerifyIssuer`（未验签的 `Iss` 仅用于选 key，伪造者无对应 key 无法签名）。
  2. `server.scopeAuth`：改用 `VerifyIssuer`；**delegated（`Iss != ""`）强制 `exp`**、按 ns/scope 授权（跳过 `HasBinding`）；平台令牌（`Iss==""`）仍走注册绑定 ACL。**ns 始终精确**（隔离边界）；`kind`/`name` 支持 glob；token 派生的资源（kv/vectorize/service/do）**拒绝通配 name**（无法解析 cell）；资源在请求里的 kind（d1/r2/queue/workflow）支持通配。
  3. CLI（**统一签发入口**）：`cellhive creds issuer <name>` 打印派生 issuer key（供可信租户平台持有，vwork 不持 root）；`cellhive token --ns --kind --name [--iss <issuer>] [--ttl 5m] [--key <b64>]` 签发 scoped token（`--key` 让委派方不持 root 也能签；委派令牌强制 `--ttl > 0`）。
  4. 验证：`internal/scopedtoken`（glob、issuer key/verify、canonical 字段序）、`internal/server TestScopeAuthDelegated`（glob 命中/落空、跨 ns 拒绝、强制 exp、kv 通配拒绝、错 issuer 拒绝）；`make build`/`vet`/`test` 全绿。
- **未实现（下一步）**：外部数据面入口（`ExternalHandler` 只挂数据面 + 公开 8 凭据收敛全景）；**委派模型**：`issuer`（vwork 等入口）持由其 `iss` 派生的签名密钥（当前不按 ns 收窄，故可签任意 ns——对应"入口管多个 ns、可委派所有 ns"），**每请求现签短 `exp`（30s–5min）** 的 scoped token；cell-agent 只做无状态验签 + 强制 exp + authorize + 请求 ns == token ns，不做动态 epoch/吊销表。撤销靠"委派方停签 + 短 exp"；per-user/跨 ns 隔离由委派方自律（信任声明）。
- **对称不能合，非对称才能合**：终态引入单一 Ed25519 签名密钥对替代全部 HMAC 签名密钥——私钥只在一处（cell-agent/专用 issuer），其余组件只持公钥，验证者不可伪造；loader 不再本地签，scoped token 改由 cell-agent 在加载期（冷路径）下发（do-runtime 已如此）。
- **key 泄漏必须留最小止损**：`issuer` 密钥泄漏时，纯无状态 + 短 exp **不构成止损**（攻击者可无限现签新 token）。必须二选一并显式记录：(a) 一份 per-issuer 注册表（`{key_version, active, allowed_ns}`，控制面 + TTL 缓存，量级为"每个入口一行"）；或 (b) 接受"整平台换 root"。**推荐 (a)**。
- **理由**：把"全局权"表达成"具名 issuer + ns 范围"，而非"所有组件共用 root"；用非对称把"验证"与"签发"解耦，从而在不牺牲隔离的前提下减少密钥数量；用短 `exp` + 委派方自控替代服务端逐 token 状态，保持热路径无状态。
- **未实现 / 待定**：issuer 取 per-ns 还是 `issuer×ns`；allowlist 存放（控制面保留前缀 vs 静态配置）；是否引入 `jti` 吊销表；引导凭据（bootstrap）；外部 API key 管理 API、审计归属、按 key 限流；外部令牌与 role 令牌的 header/入口区分。

---

---

## 待定（🕓）

无。历史待定项均已定稿：bundle/assets 读取路径 → **ADR-030**；路由投影下发 → **ADR-031**；管理后台/身份模型 → **ADR-036**。

## 待 P0 确认（⏳）

- workerd actor SQLite 是否为 WAL 模式、可被外部只读打开、checkpoint 可对齐（决定 WAL 捕获可行性）。
- **跨节点"捕获→cell-agent→证明"的 p50/p99**（DO 写 ack 热路径）。
- do-runtime **冷激活/恢复**时间（临时盘）。
- cell-agent 集群在 DO 写复制下的吞吐上限。
- bucket 供应商条件写实测（S3 兼容对象存储）。
- 兼容日期/flag 表与固定 workerd 的对应关系。

---

## 修订记录

- ADR-003：由"独立无状态 `cell-router` 服务"改为"**Go 库 + 本地端点**"。
- ADR-006：由"WAL sidecar → S3"改为"**WAL → cell-agent**；owner 记录指 do-runtime"。
- ADR-008：明确默认捕获路径为"上报 cell-agent"。
- ADR-011：`do-runtime` 由"StatefulSet + 本地 PV"改为"**弹性、本地盘可临时**"。
- ADR-012：由"独立 `control` 服务"改为"**并入 cell-agent**（分监听/分鉴权）"。
- 新增 ADR-017 ~ ADR-025（去 gateway、固定/弹性分层、数据落位、双端口、桶角色、唯一桶凭据、ownership、无心跳）。
- ADR-057：无（compaction GC 并入 ADR-056）。
- ADR-107：DO 会话策略 + 删除锁（对齐 CF）。
- ADR-108：owner epoch 单调 generation（Claim 统一走持久 gen 计数器；活租约不再自我 bump；修 epoch 复用）。
- ADR-109：DO 租约 TTL 可配（`CELLHIVE_DO_LEASE_S`）+ 持久对象登记索引（`CELLHIVE_DO_OBJECT_INDEX`，默认关）。
- ADR-110：bundle GC（两阶段标记 + 宽限期；引用集=所有版本 bundle + service pin；默认关）。
- ADR-111：assets GC（`internal/objgc` 泛化；引用集=各版本 `AssetsSHA`；admin/CLI `gc assets`）。
- ADR-112：Queue `max_concurrency`（`Consumer.MaxConcurrency`；每 Pass 一波并发派发，默认 0/1=串行）。
- ADR-113：R2 multipart（服务端分片暂存 + 内存拼装；facade `createMultipartUpload`/`resumeMultipartUpload`）。
- ADR-114：dev CLI assets 对齐生产 loader（`_headers`/`_redirects`/`not_found_handling`/worker 回退；`make cli-test`）。
- ADR-115：路由读取两段式（per-host 指针 + 按版本 worker 详情）+ 服务端投影缓存/revision + loader LRU/单飞/SWR/退避 + 失所有权 forget 本地 cell。
- ADR-116：路由撤销加速（轮询 `/v1/control/routes` 的 ETag）与未知 host 查询节流。
- ADR-117：单控制库 + 关系表（取代 ADR-060 的按 app 分片；`meta.rev` 驱动投影缓存；`version_bundle_refs` 表）。
- ADR-118：无主写路径（`forwardOrClaim` 透明认领/转发；控制面写也在内部监听；KV/D1/Queue/Workflow/控制面接线）。
- ADR-119：Queue 消费者归属（`queue.OwnerGate`，只由 owner 消费）+ runner 写经 `CaptureWrite`（RPO=0）。
- ADR-120：读转发给 owner（KV/D1 只读/workflow/控制面读）+ 转发重试分类（dial 失败与 409 各重试一次；Ambiguous 不重放）。
- ADR-121：写路径所有权审计，补齐 `timer/upsert`、`kv/expire`、`do/alarm/upsert` 的 gate 与捕获。
- ADR-122：本地磁盘账本 + 字节预算 + LRU 驱逐（owned 文件永不删；`Forget` 变可选；磁盘指标）。
- ADR-123：owned 文件安全回收（L1 manifest 覆盖 → LRU 可删）+ 磁盘高水位反压（readyz/claim/写认领 503 + 释放空闲 cell）。
- ADR-124：`cellhive deploy --config wrangler.jsonc`（Go 侧解析+映射+预检+打包，`internal/wrangler`）；修正 dev/服务端对 `ai`、`workflows` 的过时判定。
- ADR-125：Hyperdrive 绑定（只给连接串，平台不做连接池；facade 为普通数据对象；dev 走 Miniflare `hyperdrives`）。
- ADR-126：loader 缓存 id 加入版本号（修「同 sha 只改 binding/vars 的部署不生效」）；含可证伪的 e2e。
- ADR-127：版本号贯穿内部派发路径（service/DO invoke+alarm；queue/cron/workflow 的 body 亦带 version）；isolate/facet 键含 version（含两条可证伪 e2e）。
- ADR-128：queue/scheduled/workflow 派发按版本解析 binding spec 注入 env（`/v1/internal/worker/bindings?version=`）；顺带修每次派发重复下载 bundle；内部 loader id 与公开入口统一。
- ADR-129：Hyperdrive = 注册资源（origin URL 信封加密存储，`/v1/internal/hyperdrive` 按名解析；内联 URL 兼容）；两条读取路径 + CLI/dev 接线。
- ADR-130：本地连接复用 = DO 内持有连接（实测普通 worker 不能跨请求复用 socket）；`CELLHIVE_TENANT_OUTBOUND` 可放出网类别（默认 public）。
- ADR-131：控制面 schema v2 + 域名/路由模型（**已实现**：软删+purge 循环、bindings 派生表、hosts 验证与 loader 门控、内置域 `<ns>-<worker>.<base>`、JWT 按 ns 授权 + 审计主体、列表分页、路由挂载剥离）。
- ADR-132：移除边缘配置下发（删 `GET /v1/internal/traefik` 与 `CELLHIVE_ADMIN_HOST/_ADMIN_BACKEND_URL`）；边缘（反代/云 LB）由运维静态配置，平台只保证 loader 级 host 门控。
- ADR-133：域名不做 DNS 校验（登记即授权）：删 `domain verify`/验证循环/`CELLHIVE_DOMAIN_VERIFY`·`CELLHIVE_DNS_RESOLVER`；host 唯一性/409/审计/`domain rm` 保留，校验字段与状态机保留备用。
- ADR-183：KV TTL 放开 60s 下限（平台要求，vwork 迁移开放项 3）：`expirationTtl` 接受任意正整数秒，`expiration` 仍须在未来；技术依据——过期是**双机制**：读路径惰性过滤（`expires_ms > now`，get/put-if/incr/list 全部带该条件，过期即刻不可见）+ timer 清扫（`kv-expire` 按最近到期武装、`CELLHIVE_TIMER_INTERVAL` 秒级）只负责空间回收，故 60s 下限从来不是技术必需（ADR-176 是对齐 CF 的人为收紧）。影响：与 CF Workers KV 的显式差异（CF 拒 <60s），wranglercompat 不含该校验（已核实），JS facade 直传；`internal/server TestKVTTLAndMetadataE2E` 用例更新（59s 接受）。
- ADR-182：KV 条件写与原子自增（超 CF API 的能力）。**条件写是原子存在性判定（`absent`/`present`），不是版本 CAS**——先后否证两版：① cell txid 守卫（cell 级计数器，无关 key 写入也推进，伪冲突）；② per-key version 列（实现对，但评审发现 vwork `onlyIf` 只有 `"absent"|"present"` 两个字面量，无值/版本比对，version 令牌是过度设计——`sqlite` 单写者 + 同事务 `EXISTS` 判定即原子）。终版：`put?if_exists=absent|present`（`cellstore.PutTxIf` + `PutTxCondition`，存在性判定与写入同事务，条件不满足整体回滚不推进 txid 不捕获，HTTP `200 {applied:false}` 对齐 vwork 布尔语义而非错误码）；过期行计为 absent（锁 + TTL 过期后可重新获取）。`POST /v1/kv/incr?by=`（默认 1，可负）单事务读-加-写（`IncrTx`/`IncrTxIn`，溢出/非整数 `400 not_integer`，纯加保留原 metadata），支持可选 `if_exists` 守卫。动机：vwork 平台适配层需要 `put(onlyIf)`/`incr` 语义（锁/幂等/计数器），SQLite 单写者 + `writeMu` 使事务内判定天然原子。schema 零变更（version 列已从终版移除）。
- ADR-185：tenant env 完全由用户拥有：零平台键、零保留名；`CH_*`/`CELL_*`/`__cellhive*`/历史平台名均可作用户 var、secret 或 binding。Workflow 改由可信 host 创建持有宿主 env 与 dispatcher-bound 身份的 `WorkflowBridgeTarget extends RpcTarget`，作为原生 JSRPC capability 参数交给 wrapper；不用 pinned workerd 无法跨动态 loader 序列化的 `ServiceStub`。日志优先 stock workerd Tail Worker，固定版本动态-tail spike 不通过则关闭平台日志，绝不恢复 env 注入。
- ADR-184：租户 env 零平台传输（WDL 对齐）：DO/Workflow/Vectorize 改为**平台侧 entrypoint stub**（`ctx.exports.X({props})`，:7001 传输与 scoped token 全在平台 worker）；**DO WebSocket 也走同一条 stub**——平台侧 `DurableObjectNamespace.fetch(request)`（fetch 形 RPC，id 经 `DO_ID_HEADER` 随 Request 传），101 + socket 作为 fetch 返回值跨 RPC，因此不再需要 `CH_DO_CONNECT`/`CELLHIVE_CAP_WS`；workflow step 回调与租户日志 ring 分别走数据型 `WorkflowSteps`/`LogSink` stub（后合并为 `PlatformBridge`）；`PLATFORM`/`CELL_URL` 不再进入租户 env。关键实现事实：workerLoader 的 `ctx.exports` = **宿主 worker mainModule 的导出集**（新增 stub 类须同步 `loader.js/internal.js/host.js` 的 re-export）；**只有「方法名 fetch + 单个 Request 参数」的 RPC 才保留 fetch 语义**，只有它能携带 WebSocket 101（pinned workerd 实测；WDL 的 `fetchObject` 同形）；**方法返回的 Proxy 不被 RPC 认作 RpcTarget**，故任意 DO 方法名由**租户侧 Proxy**（`facades.js makeDOFromStub`）转发为 `(method,args)` 数据；其保留名/`CH_PLATFORM` 结论由 ADR-185 部分取代。
- ADR-181：凭据收敛与能力令牌委派（**a+b 已实现**：scoped token 段级 glob + `iss`/`IssuerKey`/`VerifyIssuer` 委派签发，`scopeAuth` 对外部令牌强制 exp/跳 HasBinding，CLI `creds issuer <name>`；**8 凭据收敛/非对称仍设计**）：按信任边界分三类凭据；对称不能合角色、非对称才能合；最小分发；per-issuer kill switch vs 换 root 取舍；log 独立、secrets-root 移出 root；分档 1→1.5→2→3。
- ADR-179：指标按命名空间归属（ns 维度，有界标签）+ 绑定 span 带租户属性。
- ADR-178：日志带 trace 上下文 + 多租户可观测性参考管线（OTLP push + Collector + 后端 org/stream 隔离；指标内部 pull）。
- ADR-177：wake 索引不落后于 timer（先发布 + fail-closed + 认领时重建 + 有界轮转修复）。
- ADR-176：KV 写入校验对齐 Cloudflare（key≤512B、metadata≤1KiB、expirationTtl≥60s、expiration 须在未来）。
- ADR-175：兼容性收口（DO WebSocket→`/v1/do/connect` 接线 + ticket、R2 `writeHttpMetadata` 本地包装、DO alarm 身份/storage_id 与 scheme 修复）。
- ADR-174：示例兼容性修复（KV `put` 体类型、legacy DO 类包装为 facet、DO 响应状态/content-type/`x-cellhive-do-app` 透传、R2ObjectBody `body`/`writeHttpMetadata`）。
- ADR-173：日志订阅的 fleet 广播（`POST /v1/internal/logs/subscribe` + lease 节点名单，best-effort、每 worker ≤25s 节流）。
- ADR-172：租户日志可选 OTLP logs 导出（`CELLHIVE_OTLP_LOGS=off|tail|all`，默认 off；`tail` 由 `cellhive tail` 的 TTL 订阅门控）。
- ADR-171：upload spool 断电安全（tmp fsync → rename → 目录 fsync 记忆化；`/metrics` spool fsync 计数/耗时）。
- ADR-170：DO 输出门跨文件并发捕获（`dosupervisor.Concurrency`）+ `orderedDispatcher.sweep` 生产接线。
- ADR-169：purge 数据侧改 `objectstore.ListPage`+`bucket.PagedLister` 游标分页删除（内存有界）。
- ADR-168：R2 list 的 `include`（按需逐对象 metadata）与 `delimiter`/`delimitedPrefixes`（扫描上限 10000 + cursor）。
- ADR-167：标准 OTel OTLP/HTTP 追踪导出（官方 SDK；Go span + JS 经 `/v1/internal/telemetry/spans` 汇总；`CELLHIVE_OTLP_ENDPOINT` 可换后端；修复 ADR-146 binding 传播残余）。
- ADR-166：可观测性 batch 2（replication_bytes shipped/received、waker_fires{kind,outcome}、do-supervisor `/metrics` + host `/internal/do/stats` 上报 do_*）。
- ADR-165：可观测性指标 batch 1（binding_calls/durability_proof histogram/owner epoch+takeover/projection_version/peer hedge；仍待 replication_bytes、waker_fires、do_*）。
- ADR-164：peer 自适应 hedge（默认自适应、`0`=单份、adaptive=4×最近最慢≥250ms 且封顶）+ spool 按 `ltx.Header.ID()` 幂等（重试/重连/hedge 副本安全）。
- ADR-163：R2/D1 契约保真（R2ObjectBody 全字段 + `put` http/custom metadata/checksums + `head()`；`r2meta/` sidecar；`bucket.Statter`；D1 `last_row_id`/`changed_db` + `D1_ERROR` 错误形状）。
- ADR-162：DO 调用支持 RPC（`kind:"rpc"` tagged JSON 信封 + host→facet 原生 JSRPC；8 MiB；`getByName`；Map/Date/ArrayBuffer/环保真；保留名/超限/错误信封）。
- ADR-161：LTX 压缩运行期开关 `CELLHIVE_LTX_COMPRESSION`（默认 true=WAL3/LZ4；false=WAL2 固定帧不压缩，解码/GC 两者兼容）；A/B 证明压缩对 capture-ON 写吞吐无可见代价。
- ADR-160：cell-agent 侧运行期 VFS 懒读（按页冷启动）：`internal/pagedvfs`（包装默认 VFS 的 fault-in `sqlite3_vfs`、稀疏 cut 文件、hydration 位图、同步 fault、`SQLITE_IOERR_READ` fail-closed）+ `replica.PageFetcher` 页源 + `Store.Paged` hook（< `CELLHIVE_PAGED_MIN_BYTES` 整克隆）+ `SnapshotPages` 前 `HydrateAll` + 后台 `CELLHIVE_PAGED_HYDRATE_MBPS`；实测点读 8/771 页、整对象读 3KB/3.1MB。DO 侧仍待 FUSE。
- ADR-159：SQLite 驱动换 CGo（mattn/go-sqlite3）+ 官方 vec1 ANN 静态编入（构建 tags、`sqlite3_auto_extension`、`-march=x86-64-v3`/NEON）；Vectorize 存储/检索改为 vec1 vtab（flat 精确默认 + `vectorize rebuild` 建 ANN），API/端点/CLI 形状不变；dot-product 显式拒绝；旧 ADR-158 cell 自动迁移。
- ADR-158：Vectorize 绑定（注册资源 + 不可变 dims/metric 配置 + cell 内 float32 存储 + Go 精确 KNN + CF 兼容 score/filter/namespace；facade 注入 `env.<BINDING>`，资源=index_name；CLI `cellhive vectorize ...`；dev 明确不支持）。修订 ADR-014/ADR-065 对 `vectorize` 的"拒绝"。
- ADR-157：资源管理下放到各功能域（`POST|GET|DELETE /v1/<kind>/resources`）+ 每域 metadata-only stats（`/v1/<kind>/stats`）+ 旧控制面路径别名 + KV `kv_expires` 索引真 bug 修复 + `/metrics` 补齐 bucket/list/owned/resident。
- ADR-156：vwork 运维接口（`DELETE /v1/control/resource` 吊销 + 墓碑立即生效；`/v1/control/queue/status`、`/v1/control/queue/replay-dlq`；user-runtime `/ready`+`/drain`+SIGTERM 自排空、do-runtime `/ready`；compose/k8s/Helm 探针改用 `/ready`）。
- ADR-155：Queue per-message `ack()`/`retry({delaySeconds})`（`DispatchResult`/`DetailedDispatcher` + 租户侧 outcomes + DLQ 语义）+ 延迟生产验证 + `message.body` 按 content-type 类型化。
- ADR-154：全栈 e2e 缺陷修复（timer body 缺 namespace、DISPATCH_URL 未配、event.cron 为空、queue MessageBatch 非 CF 形状/跨 isolate 丢属性）。
- ADR-153：兼容矩阵收口（bundler 外置 `cloudflare:*`/`node:*` 真 bug 修复 + flag 列表/CLI 镜像测试 + pin↔兼容上限绑定 + OpenNext/SvelteKit/Astro 验收 + D1 sessions 显式拒绝）。
- ADR-152：timers-and-dispatch 定稿（queue batch/lease/retry、timer batch/fired TTL、waker batch/fired TTL/**退避封顶**、timer cell 不 pinning 的决定）。
- ADR-151：scaling-and-ha 定稿（autoscaler 冷却 `CELLHIVE_AUTOSCALE_COOLDOWN`、rebalance 间隔/批大小、`CELLHIVE_PLACEMENT_AZ` 跨 AZ follower 选择）。
- ADR-150：根密钥文件 provider（`CELLHIVE_ROOT_KEY_FILE`，Docker/K8s `_FILE` 惯例）+ KMS 接缝 + mTLS 决定暂不做（记录理由与触发条件）。
- ADR-149：C 类环境本地替代验证（MinIO/RPO 多进程/延迟注入/mock OIDC/模型仿真/契约测试全部 PASS）+ 不可替代残余清单。
- ADR-148：CLI 兼容补齐（服务端 `dry_run` + `deploy --dry-run/--var/--secrets-file` + `cmdCompat` 兼容组/结构化拒绝 + wrangler 前缀透传）。
- ADR-147：控制面 secrets 管理（`DeleteSecret`/`ListSecrets` 只列 key + CLI `secret delete|list` + 审计）；修 `secret list` 被 arity 误拒。
- ADR-146：W3C `traceparent` 传播（入口生成/透传 + wrapper + facades + cell-agent service/DO 出向）；props-bound facades 与后台 dispatch 的边界如实记录。
- ADR-145：R2 list cursor 分页 + 尺寸化（`bucket.PagedLister`：FS 有界选择 / S3 `StartAfter`+`MaxKeys`；HTTP 与 facade 返回 `truncated`/`cursor`）。
- ADR-144：service binding ACL（目标侧 `service_acls` + 部署期拒绝 + 运行期校验）+ 跨命名空间 service binding（`ns/worker` + `target_ns` 协议）。
- ADR-143：async 上传持久化重试队列（写前 spool + 启动重放 + 有界退避；失败从 dropped 改为 deferred）+ `/metrics` 上传指标。
- ADR-142：purge 闭环（可续跑 hook + `internal/purge.Purger` + `cellstore.ForgetPrefix`）：worker 删除清 DO 存储、app 删除清整 ns；桶/本地副本跨节点幂等清理；修 FS 桶按段内前缀无法列键的可移植性 bug。
- ADR-141：部署加固（非 root 65532 + Helm chart + `scripts/ci.sh`/GitHub Actions 门禁 + SA/PDB/NetworkPolicy/三类探针 + kustomize base/overlay 重构；全门禁 PASS 13/13）。
- ADR-140：do-supervisor 接线（do-runtime `-render-only` + supervisor 托管 workerd/续租/drain + compose profile `rpo0` + k8s base/overlay），DO 经输出门的端到端落桶已验证。
- ADR-139：可部署产物（单镜像 + pinned workerd + entrypoint 分发 + compose + kustomize manifests + Makefile 目标）。
- ADR-138：`cellhive wrangler` 前缀（wrangler 风格别名 + `wrangler.jsonc` 自动发现 + flag 翻译/引导）；真 wrangler 的 `CLOUDFLARE_API_BASE_URL` 兼容记为备选。
- ADR-137：单根密钥 HKDF 派生全部凭据 + 标准 AWS 桶凭据名 + 时长统一 Go 语法 + 目录派生/成对合并（99→85 个 env）。
- ADR-136：取消未实现的 gRPC 平面（内部统一 :7001 HTTP）+ 删 `CELLHIVE_GRPC_ADDR`/`CELLHIVE_CELL_AGENTS` + `ADVERTISE` 默认改 `:7001` + 接线日志尾缓冲（`cellhive tail --worker`）+ 统一 `DO_OBJECT_INDEX` 解析。
- ADR-135：review 遗留修复（purge 复活赛、捕获提交 epoch 栅栏、connect/invoke 身份与 storage_class 租约、DeleteNamespace 栅栏、Ensure 锁范围、JWKS/JWT 加固、JS 小项、CLI arity）。
- ADR-134：2026-09-17 review 修复批次：授权先于转发（rollback/asset/logs/apps/内部读镜像）、所有捕获写走 `Cell.Tx`（RPO=0 水位）、compat flags 真正生效、DO spec 带 version、hostView 毒化修复、service pin 按 sha、`makeDO` 重放收紧、owner 条件删除、Forget 排空/`ForgetWithVerify`、PageFetcher 连续性、recovery 不 seal 部分收集、WAL 自动截断、上传重试、`--config` 接受 hyperdrive。
- ADR-106：快速路径开关 + A/B 延迟量化。
- ADR-105：service fetch 本地化 + DO 调用方 1 跳（按 shard 签名的 owner ticket）。
- ADR-104：service 目标版本在部署时 pin。
- ADR-103：DO owner-hint（cell-agent 侧）省一跳。
- ADR-102：service binding 走同实例原生 JSRPC。
- ADR-101：resident 上限 + 所有权 rebalance（雏形）+ RPO=0 故障注入测试。
- ADR-100：D1 只读语句跳过捕获。
- ADR-099：timer/waker 规模化（bucket 唤醒索引 + 到期才扫；修 alarm 失效）。
- ADR-098：KV TTL 与 metadata（清理复用 timer/waker 激活）。
- ADR-097：KV 库必须有 id（不再写死 default）。
- ADR-096：cell 句柄 LRU + 空闲驱逐。
- ADR-095：写路径成本分解 + txid 内存镜像。
- ADR-094：单键写路径优化（per-cell 写串行化 + 窗口生效）。
- ADR-093：cell-agent 写路径优化（binding 缓存 / capture 调参；KV 批写已移除）。
- ADR-092：backend-A 捕获接线（cellstore → LTX → fleet → bucket）。
- ADR-091：CLI 资源命令（app/resource list、tail）+ assets 版本 token（deploy --assets-dir）。
- ADR-090：Bindings 迁移为 RPC entrypoint env（Phase 0 KV 试点）。
- ADR-089：DO 内 bindings（CF parity：shim 注入 env，`this.env.KV` 可用）。
- ADR-088：P5 协议版本化（reader-before-writer）+ 诊断增强 + 矩阵回归守卫。
- ADR-087：P4 发布日志/幂等部署/Admission/Autoscaler 信号。
- ADR-086：Workflows 自研引擎（shim base + 记忆化 step + sleep via timer；Partial）。
- ADR-085：运行期 VFS 懒读边界（不可行 + 替代；冷启动按对象/按页 materialize）。
- ADR-084：跨节点冷激活/恢复（可逆 scope + manifest + RestoreAll + sidecar；双 runtime e2e）。
- ADR-083：do-supervisor（facet/shard 存储捕获 + 输出门；`.facets` 内部格式不解析）。
- ADR-082：DO migrations v2（对象注册表、rename 别名保数据、delete 标记+物理回收、主动重启）。
- ADR-081：DO 存储生命周期（版本惰性重启、migrations 校验、doStorageId）。
- ADR-080：DO 协议端到端（客户端绑定+放置、owner 转发/hint、result_unknown、WebSocket 1012）。
- ADR-079：DO alarm shim（facet 原生 alarm 不可用；shim 到对象存储 + 统一 timer 派发）。
- ADR-078：DO 归属/栅栏/排空/驻留（ClaimAs + owner-gen 单调栅栏 + shard + drain/renew + resident/evictable）。
- ADR-077：do-runtime host-actor 骨架（facets + getDurableObjectClass + localDisk；真实 workerd e2e）。
- ADR-076：cron 调度器（5 字段 UTC，物化 slot 为 KindCron 定时器；幂等/不补跑）。
- ADR-075：平台内部 Tier-1 按角色令牌（peer/internal/dispatch）+ dispatch 入站鉴权；诚实标注未做 mTLS。
- ADR-074：内部绑定鉴权简化（本地 HMAC、无过期、资源绑定；绑定端点免 internal token；删 mint）。
- ADR-073：租户 binding facade 经平台 service binding（PLATFORM）出网（隔离 + 可达性）。
- ADR-072：队列消费者配置 + 死信 DLQ（Consumer 结构升级；runner DLQ/drop/retry）。
- ADR-071：资产路由配置（run_worker_first / not_found_handling）。
- ADR-070：scheduled/timer 派发接入 user-runtime（cron→scheduled()，Version.Crons + cronEnricher + /v1/timers/dispatch）。
- ADR-069：user-runtime 资产管道（loader 侧 index/type/ETag/_headers/_redirects/fallback）。
- ADR-068：user-runtime 公开 loader（路由投影纯拉取 + 版本 bundle 加载 + facade env + 清头；入口完整）。
- ADR-067：Queue 消费者派发（cell-agent 轮询控制面 queue 资源 + `/v1/queues/dispatch` 客户端；worker 由 user-runtime 解析）。
- ADR-066：节点死亡自动 recovery 编排（waker leader-only；lease 判活；幂等；完成 P1 node-log/recovery 缺口）。
- ADR-065：新增（`cellhive dev` = Bun+Miniflare；deploy 服务端功能/兼容拦截；修订 ADR-005 与 ADR-064）。
- ADR-064：新增（`cellhive dev` 本地开发模式设计，见 dev-mode.md）。
- ADR-063：新增（binding facade + cell-agent API：KV/D1/R2/Queue）。
- ADR-062：新增（bundle/assets 内容寻址 + Traefik 下发 + 审计保留 + OIDC/JWT admin）。
- ADR-061：新增（binding 鉴权纵深防御：scope token + HasBinding）。
- ADR-060：新增（控制面骨架：应用/版本/路由/密钥，按 app 分片；独立 admin 监听；路由投影）。
- ADR-059：新增（owner 解析库 + 端点 + 非 owner 转发）。
- ADR-058：新增（统一定时器抽象 + 本地派发 + 单 fleet waker）。
- ADR-057：新增（drain token + 优雅 handoff + node-log/recovery 完整化；段去重修复）。
- ADR-056：新增（冷启动按需拉取：L1 页索引对象 + ranged-get PageFetcher）。
- ADR-055：新增（按需分页原语 + snapshot watermark 一致性修复）。
- ADR-051~054：新增（workerd DO→WAL→LTX→cell-agent 输出门；cellstore per-scope cell 缓存；do-runtime host-actor+facets 与 supervisor 生命周期；快照分页）。
- ADR-038~050：新增（Go 技术栈；持久性姿态；group commit；HTTP 101 peer 流；真实 SQL benchmark；capture 优化：WAL2 page-map、prepared statement、延迟注入、fleet/capture pipelining、snapshot/apply、连续写入安全 checkpoint 接管）。
- 早期草案中"凭据权威 + 字节直连"已被 ADR-023 取代。

_最后更新：2026-09-22_
