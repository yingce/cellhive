# 架构总览

## 定位

CellHive 是一个自托管的 **Cloudflare Workers 兼容**运行时，面向需要数据主权/私有部署、同时希望沿用 Workers 编程模型与 `wrangler` 工作流的团队。

设计信条：

1. **不修改 workerd。** 计算层使用 stock workerd，仅通过其公开能力（`workerLoader`、bindings、capnp 配置、进程开关）使用它。
2. **状态一切皆 cell。** 每个状态单元是一个有名字的 SQLite 数据库；对象存储（S3 兼容）是长期权威；bucket 条件写决定唯一 owner；不依赖共识服务。
3. **计算与状态分层。** stock workerd 负责执行；所有持久化数据经 `cell-agent`。
4. **固定状态层 + 弹性执行层。** `cell-agent` 是固定集群（有身份的节点）；`do-runtime` 是分布式弹性执行层，本地盘可临时。
5. **可移植。** 运行时不依赖特权或 FUSE；默认可在托管/受限 Kubernetes 上运行。

## 非目标

- 全球边缘网络、跨区域复制、Cloudflare 账户 API 兼容；
- 修改 workerd，或引入第二套 JS 引擎；
- 依赖 NATS / kvrocks / Valkey / etcd / APISIX；
- 独立 gateway 组件（入口职责由运维边缘代理 + user-runtime 承担，见 ADR-017；平台不下发边缘配置，ADR-132）；
- 支持 workerd 本身不具备的 CF 能力（Python Workers、Cache API、Vectorize、Hyperdrive、Browser Rendering、Email Workers 等）。

## 组件清单

| 组件 | 实现 | 端口 | 角色 | 有状态 | 需要自研 PID1 supervisor |
|---|---|---|---|---|---|
| 边缘代理（Traefik/nginx/云 LB） | 运维 ingress（非平台内构件） | 80/443 | TLS 终止、host 分流（租户 vs admin）；**静态配置**（ADR-132） | 否 | 否 |
| `user-runtime` | workerd（`cmd/user-runtime`） | :8081 公开 / :8088 内部特权 | Worker 路由/版本解析/清头 + 租户 Worker 加载执行。**已实现**：`:8081` 公开 loader（路由投影拉取 + 版本 bundle 加载 + facade env + 清头，**ADR-068**）与 `:8088` 内部派发（queue/scheduled handler；ADR-067/070），公开侧含静态资产（ADR-069） | 否 | 否 |
| `do-runtime` | workerd（`cmd/do-runtime`）+ Go supervisor（`cmd/do-supervisor`） | :8788 | 原生 Durable Object 执行；**分布式弹性**。**已实现**：host-actor 骨架（facets + localDisk，ADR-077）+ 归属/栅栏/排空/驻留（ADR-078）+ supervisor 捕获/输出门（ADR-083）+ alarm（ADR-079）+ WS 1012/跨节点转发（ADR-080/084）+ 跨节点冷激活/按对象恢复（ADR-084）+ DO 内 bindings（ADR-090）+ session policy（ADR-107）+ owner epoch 栅栏/租约可配/对象索引（ADR-108/109） | 本地盘可临时（工作副本） | **是** |
| `cell-agent` | Go（固定集群） | :7001 内部 REST（Go↔Go 亦走此口，ADR-136）/ :8082 admin | cell 数据 + 复制 + 租约 + 控制面 + owner 解析 + 计时派发 + **唯一桶凭据** | 是（自己拥有 SQLite） | 否（自身处理 drain） |
| 对象存储 | S3 兼容对象存储 | — | 权威状态 + code + assets | 是（外部） | — |
| `cellhive` CLI | Go | — | 打包/部署/运维 | 否 | — |

> 没有 `gateway`、`cell-router`（改为库）、独立 `control`、独立 `scheduler`、`d1-runtime`、`kv-runtime`。

## 拓扑

```
公网用户 ──TLS──▶ Traefik ──host 分流──┬─▶ user-runtime :8081（Worker 路由/版本/执行）
                                        └─▶ cell-agent :8082（admin/控制面，token 鉴权）

内部（私网 + 内部 token，**直连，不经 Traefik**）
  user-runtime ─binding/scheduled/queue─▶ cell-agent :7001（REST/JSON）
  cell-agent ─派发─▶ user-runtime :8088
  do-runtime ◀─owner 路由/WS─ user-runtime（host adapter → cell-agent 解析 owner）
  do-runtime ──WAL 上报/claim/恢复──▶ cell-agent :7001（直连，HTTP）
  cell-agent ↔ cell-agent（ensemble/acquire/release，直连，HTTP :7001）
  cell-agent ──唯一──▶ 对象存储

固定：cell-agent 集群（≥2）   弹性：do-runtime 节点（可增删，临时盘）

> Traefik 只做**南北向**：公网 TLS + host 分流（租户 → user-runtime :8081；admin/CLI → cell-agent :8082）。
> 所有**东西向/内部**调用**直连私网，不走 Traefik**；`cell-agent :7001` 永不对外。
```

## 分层图

```
┌───────────────────────────────────────────────────────────────┐
│ 接入层   Traefik（TLS + host 分流）                            │
├───────────────────────────────────────────────────────────────┤
│ 计算层   user-runtime (workerd)  入口/路由/版本解析/执行        │
│          do-runtime   (workerd 原生 DO + supervisor)           │
│          · host adapter：env.KV/DB/DO/Queue/... → 私有调用      │
├───────────────────────────────────────────────────────────────┤
│ 状态/控制 cell-agent (Go, 固定集群)                            │
│          · cell 存储/复制/租约/epoch · owner 解析库 · 计时派发   │
│          · 控制面（:8082）· 唯一对象存储凭据与数据端点           │
├───────────────────────────────────────────────────────────────┤
│ 权威存储 S3 兼容对象存储            │
└───────────────────────────────────────────────────────────────┘
```

## 主要数据流

### 普通 fetch（无独立 gateway）

```
Client → Traefik(TLS, host) → user-runtime(:8081)
   loader：host/path → ns/worker；route → active version（读 cell-agent 路由投影 + 缓存）
         → 清可信头 + 生成 request-id + 拦截保留命名空间
         → workerLoader 按 <ns>:<worker>:<version> 加载不可变 bundle → fetch()
```

### 绑定调用（KV/D1/Queue/Workflows/Cron）

```
Worker(env.BINDING) → host adapter（binding-scoped，不可变 props）
       → cell-agent(:7001) → owner cell 的 SQLite 读写 + LTX 复制到对象存储
       → 持久化证明后返回
```

### Durable Object（跨节点）

```
Worker → env.NS.get(id).fetch() → host adapter
       → cell-agent 解析 (ns,class,objectId) → owner **do-runtime 节点**地址 + epoch
       → owner do-runtime：workerd 原生 facet 执行（同步 SQL），写本地工作副本
       → supervisor 捕获 WAL → 上报本地/远端 cell-agent
       → cell-agent：写 e<epoch> 前缀 + 持久化证明 → 输出门 → 返回
```

### 计时（cron / queue 延迟 / workflow sleep / DO alarm）

统一为 timer：due 存在各自所属 cell 的 SQLite；每个 `cell-agent` 为其拥有的计时器 cell 维护本地 due；单 fleet waker（bucket 租约）兜底 owner 已死的到期项。**不扫描 bucket。**

## 职责边界（`cell-agent` vs `do-runtime`）

| 维度 | `cell-agent`（固定） | `do-runtime`（弹性） |
|---|---|---|
| 拥有 backend A 的 SQLite | ✅ | ❌ |
| 拥有 DO 的本地工作副本 | ❌ | ✅（临时盘，权威在 cell-agent） |
| 租约/owner 协调、epoch、复制、RPO 证明 | ✅ | ❌ |
| 控制面 / 鉴权 / 身份 / 密钥 | ✅（:8082） | ❌ |
| owner 解析库 + 端点 | ✅ 提供 | 使用 |
| 计时派发 / waker | ✅ | ❌ |
| 对象存储凭据 | ✅ 唯一持有者 | ❌（经 cell-agent） |
| 监听 | :7001 REST（含 Go↔Go）/ :8082 admin | :8788 私网 |

## ownership 规则

> **owner = 持有该 cell "工作 SQLite" 的节点。**

| backend | owner 节点 | 记录者/协调者 |
|---|---|---|
| A（KV/D1/Queue/Workflows/Cron） | `cell-agent` 节点 | `cell-agent` |
| B（Durable Object） | `do-runtime` 节点 | `cell-agent` 协调并写 owner 记录（含执行地址 + epoch） |

## 发现机制（概要）

| 目标 | 机制 |
|---|---|
| `cell-agent` 集群 | 每节点写 `nodes/<node>.json` bucket lease（自注册，持桶凭据） |
| A 类 cell owner | bucket owner 记录 + 本地缓存（点查，不 LIST） |
| DO owner | owner 记录（指向 do-runtime 地址 + epoch），由 cell-agent 写 |
| `user-runtime` 派发 | 逻辑服务名（mesh），任意健康副本 |
| `do-runtime` 存活/候选 | **无专门心跳**：由复制活动隐式判活；接管候选走平台服务发现（见 ADR-025） |

## 信任边界

- `Traefik` 只做 TLS 与 host 分流，不做业务鉴权；admin host → `cell-agent :8082`。
- `user-runtime` 加载的租户 Worker：**仅公网出网**，看不到内部 Fetcher/凭据；平台 loader 先于租户模块执行，负责路由与清头。
- `do-runtime` 仅私网；不持对象存储凭据。
- `cell-agent`：`:7001` 内部数据/解析（Go↔Go 与 workerd bindings 共用）**仅私网**；`:8082` admin 经边缘，凭据 = 静态运维 token 或 OIDC/JWT（`cellhive_ns` 限定 ns，ADR-131）；数据面路径不可达控制面处理器。
- **租户隔离**：租户 loaded worker 的 `globalOutbound` = **公网 only**（不含 RFC1918 / cell-agent）；internal token 只存在于 host adapter，**绝不进租户 env**；每 worker 配 `limits`（cpu/subrequests）与 V8 heap 上限。详见规划中的 `security.md`。
- 租户密钥：控制面 cell 存信封密文，根密钥在 cell 之外；加载期在 `cell-agent` 内解密注入 `env`，明文只进 load envelope + workerd env。
- 所有内部调用携带共享内部 token；对公网不暴露。

## 状态归属

| 状态 | 权威位置 |
|---|---|
| Worker bundle / 版本元数据 | 对象存储（内容寻址）+ 控制面 cell |
| KV / D1 / Queue / Workflow 数据 | 各自 cell 的 SQLite，经 cell-agent 复制到对象存储 |
| DO 数据 | workerd actor SQLite（工作副本）→ 经 cell-agent 复制到对象存储 |
| cell owner / 节点 lease | 对象存储（条件写） |
| 路由 / 版本 / 身份 / 密钥元数据 | 控制面 cell（**单库**，ADR-117；app 是表里的 `ns` 列） |
| R2 / ASSETS 对象 | 对象存储 |

## 与既有项目的关系

| 项目 | CellHive 借用什么 | 不借用什么 |
|---|---|---|
| **LiteFS / superfly/ltx** | Go 侧的 SQLite 复制与 LTX 库实现参照 | — |

## 服务与 supervisor 的对应

| 服务 | 是否包裹 workerd | 是否有状态租约/复制 | 是否需要自研 PID1 |
|---|---|---|---|
| `user-runtime` | 是（workerd 即 PID1） | 否 | 否 |
| `do-runtime` | 是（Go 拉起 workerd 作子进程） | 是（workerd 拥有工作副本） | **是** |
| `cell-agent` | 否（单 Go 进程） | 是（自己拥有 SQLite） | 否 |

`do-runtime` 之所以需要自研 PID1：workerd 拥有工作 SQLite，关停时必须"先 drain/复制/释放租约，再停 workerd"，跨进程顺序只能由父进程保证（详见 [`durable-objects.md`](./durable-objects.md)）。

## SIGTERM 与 drain 顺序（I-06 fix）

| 服务 | SIGTERM 行为 | 超时建议 |
|---|---|---|
| `user-runtime` | workerd 停止接受新 fetch；等待 in-flight handler 完成（workerd 默认有 graceful shutdown 窗口）；未完成 WS 由 1012 关闭；不需要额外 drain 逻辑 | `terminationGracePeriodSeconds: 30` |
| `do-runtime`（supervisor PID1） | ① 停止接受新 DO 请求；② 等 in-flight handler + 输出门完成（max `DO_DRAIN_IN_FLIGHT_MS`，默认 8000ms）；③ flush 所有待上报 WAL 段并等 cell-agent 持久化证明；④ 调 cell-agent `release(scope)` 释放 owner；⑤ kill workerd；⑥ supervisor 退出 | `terminationGracePeriodSeconds: 60`（含 drain margin） |
| `cell-agent` | 按 cell-protocol §7 的优雅 handoff 流程，获 drain token 后逐批迁移 cell | `terminationGracePeriodSeconds: 120+` |

`do-runtime` SIGTERM 的 supervisor 总超时：`DO_SUPERVISOR_SHUTDOWN_MS`（默认 30000）；超出后强制 kill workerd（数据可能丢最后 WAL 段，但 cell-agent 可从 follower 恢复）。

## 控制面请求路由（I-10 fix）

控制元数据存在**单个** control cell（ADR-117；关系表，app 为 `ns` 列）。**Traefik 将 admin 流量负载均衡到任意 cell-agent 副本**；非 owner 副本通过 owner 解析库将**写操作内部转发到 control cell 的 owner 副本**（内部 HTTP，internal token；ADR-118），读操作可本地缓存服务。deploy/rollback 等写操作顺序由 control cell owner 保证（单写者）。控制 cell 的 owner 死亡时，cell-agent 标准接管流程接管，短暂不可用期间 admin 写操作返回 503。

## 部署形态（概要）

- **固定层** `cell-agent`：≥2 节点、稳定身份（StatefulSet/固定节点），持有 state 桶凭据。
- **弹性层** `do-runtime`：可增删、**本地盘可临时**（状态权威在 cell-agent），可用 Deployment 按需扩缩。
- **无状态层** `user-runtime`：Deployment + HPA（入口 + 执行）。
- 对象存储需同区/就近，且满足条件写要求。

### 不绑定单一编排器

核心发现**不依赖 K8s**：cell-agent peer 用 **bucket node lease**，owner 用 bucket 记录，DO 存活用复制活动。K8s 只提供下述能力的一种实现，均可替换：

| 需要 | K8s | 非 K8s 替代 |
|---|---|---|
| 调用方 → cell-agent（逻辑服务名） | Service/ClusterIP | DNS 轮询、内部 LB（Envoy/HAProxy/nginx）、种子列表 + 客户端轮询 |
| **do-runtime → cell-agent 入口**（WAL/claim/restore） | Service/ClusterIP | **种子列表 / DNS / 内部 LB / Consul**；或给 do-runtime **只读限定 `nodes/*` 的 scoped 凭据**；或 cell-agent 反向下发（do-runtime 不持桶凭据，不能自发现） |
| do-runtime 接管候选 | Endpoints | Consul/Nomad catalog；或 **do-runtime 注册/心跳到 cell-agent**（回退）；或配置列表 |
| 入口 / TLS | Ingress | **Traefik 独立部署**（file provider）、HAProxy/nginx |
| 稳定节点身份 / 地址 | StatefulSet / pod DNS | 静态主机名/IP + `advertise` 配置 |
| 本地盘 | emptyDir / local PV | 本地 SSD / 临时盘 |

因此可部署于：**Docker Compose、systemd 单元、Nomad、裸机/VM、任意编排器**。详见 [`decisions.md`](./decisions.md) ADR-011 / ADR-018 / ADR-025 / ADR-027。

_最后更新：2026-09-17_
