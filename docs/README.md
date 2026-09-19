# CellHive 文档

CellHive 是一套**自托管、Cloudflare Workers 兼容**的运行时：**计算层用原版（stock）workerd**（不修改），**状态层是自研 Go 实现的 cell 存储**——每个状态单元是一份 SQLite，**S3 兼容对象存储是权威**，bucket 条件写决定 owner，所有权 epoch 围栏写者，已提交的写捕获为 LTX 并复制，达到 **RPO=0**、持久寻址、节点可替换。

本目录是**设计定稿**。代码与文档冲突时以文档为准并同步两者；改设计先改 [`decisions.md`](./decisions.md)，实现变更随代码一起更新对应文档。

> English readers: an English mirror of these design docs lives in [`en/`](./en/README.md) (English docs home + module docs). The Chinese originals here remain authoritative.

## 从这里开始

按角色选一条路径，前两步足够上手：

| 你是 | 建议顺序 |
|---|---|
| **要跑起来 / 运维** | [`deployment.md`](./deployment.md) → [`configuration.md`](./configuration.md) → [`operations.md`](./operations.md)；本地试跑看 [`dev-mode.md`](./dev-mode.md) |
| **写 Worker 应用**（绑定/兼容） | [`bindings.md`](./bindings.md) → [`compatibility-matrix.md`](./compatibility-matrix.md) → [`wrangler-compat.md`](./wrangler-compat.md) |
| **平台贡献者** | [`architecture.md`](./architecture.md) → [`cell-protocol.md`](./cell-protocol.md) → [`contributing.md`](./contributing.md)（按改动类型的阅读路径） → [`testing.md`](./testing.md) |
| **协议 / 复制 / DO 深挖** | [`cell-protocol.md`](./cell-protocol.md) → [`protocol-formats.md`](./protocol-formats.md) → [`durable-objects.md`](./durable-objects.md) → [`storage-and-s3.md`](./storage-and-s3.md) |
| **想直接跑起来看代码** | [`../examples/`](../examples/README.md)：每个功能一个可运行示例（KV/D1/R2/Queue/DO/Workflow/service/assets/AI/Vectorize…）+ 代码导航 |

## 核心概念（先读这 7 个词）

- **cell**：一个有名字、有独立 SQLite 的状态单元（`<namespace>/<class>/<id>`）。KV namespace、queue、D1 库、workflow、Durable Object 都是 cell。
- **node 与 fleet**：一个节点是一个进程；共享同一个 bucket 的节点组成一个 fleet。任何节点都能服务任何 cell，加容量就是再加一个节点。
- **owner / epoch / fence**：同一时刻只有一个节点服务一个 cell；**bucket 条件写**决定 owner，**epoch** 单调递增做围栏，租约过期即释放，无需共识服务。
- **LTX**：SQLite 事务日志的复制格式（实现参照 LiteFS/`superfly/ltx`）。已提交的写被捕获、复制、折叠成快照。
- **耐久性证明（RPO=0）**：单节点把写上传 bucket 才算数；≥2 节点时由 peer 先持有（quorum-1 fsync）再回 ack，bucket 随后补齐。**被确认的写永不丢**。
- **bucket 即权威**：长期状态、bundle、assets 都在对象存储；本地盘只是工作副本/缓存，所以节点可替换。
- **计算与状态分离**：workerd 只执行；所有持久化经 `cell-agent`，它是唯一持有桶凭据的进程。

更完整的叙述见 [`architecture.md`](./architecture.md)。

## 一句话架构

```
接入   Traefik（运维 ingress）           TLS + host 分流
计算   user-runtime(workerd)             入口 = Worker 路由/版本解析/执行
       do-runtime(workerd + supervisor)  原生 DO，分布式弹性
状态   cell-agent(Go, 固定集群)          数据 + 控制 + owner 解析 + 计时派发 + 桶凭据
       S3 兼容对象存储                   权威状态 + code + assets
控制   并入 cell-agent（:8082 admin）
```

**三条关键边界**：① 计算与状态分离；② `cell-agent` 固定、`do-runtime` 弹性（状态权威在 cell-agent）；③ `cell-agent` 是唯一长期对象存储凭据持有者，也是唯一持久化数据端点。

## 文档地图

### 概念与设计

| 文档 | 内容 |
|---|---|
| [`architecture.md`](./architecture.md) | 总体架构、组件与端口、拓扑、数据流、信任边界、状态归属 |
| [`cell-protocol.md`](./cell-protocol.md) | cell 身份与持久寻址、bucket 条件写 owner、epoch fence、复制与 RPO=0、bucket 硬要求、发现 |
| [`durable-objects.md`](./durable-objects.md) | DO：原生 facet + 每对象 lease + WAL→cell-agent + 输出门；与 KV 的等价性边界 |
| [`protocol-formats.md`](./protocol-formats.md) | 线格式：owner/lease/node-log/LTX 段/bundle manifest/路由投影/REST/错误码 |
| [`glossary.md`](./glossary.md) | 术语表 |
| [`decisions.md`](./decisions.md) | 决策记录（ADR）——每个选择的原因与代价 |
| [`contributing.md`](./contributing.md) | 贡献者阅读路径：按改动类型给出"先读什么、必须跑什么、守住什么不变量" |
| [`acknowledgements.md`](./acknowledgements.md) | 致谢与设计参考：借鉴了 celld、WDL（以及 LiteFS/superfly-ltx）的哪些设计与工程手法，哪些没有借鉴 |
| [`modules/`](./modules/README.md) | **模块文档**：逐模块的作用、关键接口、环境变量表（默认值）、不变量、源码与测试锚点 |

### 绑定、兼容与打包

| 文档 | 内容 |
|---|---|
| [`bindings.md`](./bindings.md) | 每种 CF binding → cell 映射、host adapter、同步/异步边界 |
| [`compatibility-matrix.md`](./compatibility-matrix.md) | CF 运行时面 + Wrangler 配置面的支持矩阵（Supported/Partial/Rejected） |
| [`wrangler-compat.md`](./wrangler-compat.md) | Wrangler 兼容：Go+esbuild 打包、配置解析、DO 生命周期、路由、assets |
| [`workerd-integration.md`](./workerd-integration.md) | stock workerd 集成：workerLoader、wrapper、host adapter、网络/limits、版本 pin |
| [`routing.md`](./routing.md) | 入口、路由投影、版本解析、host 形态、保留命名空间 |

### 存储、复制、调度与伸缩

| 文档 | 内容 |
|---|---|
| [`storage-and-s3.md`](./storage-and-s3.md) | 对象存储硬要求、桶角色、Key 布局、凭据、生命周期 |
| [`scaling-and-ha.md`](./scaling-and-ha.md) | 分层形态、HA、handoff/drain、自动伸缩、发现 |
| [`timers-and-dispatch.md`](./timers-and-dispatch.md) | 统一定时器、去重、cron/queue/workflow/DO alarm 语义 |
| [`benchmarks.md`](./benchmarks.md) | 基准回归（命令 + 结果，含 FS / 本地 S3 兼容两档） |

### 运行与运维

| 文档 | 内容 |
|---|---|
| [`deployment.md`](./deployment.md) | 服务/端口、K8s、Compose、systemd、滚动升级 |
| [`configuration.md`](./configuration.md) | **全部环境变量、默认值与作用**（按子系统分组） |
| [`operations.md`](./operations.md) | 运维 runbook：启动/诊断/drain/接管/升级/故障排查 |
| [`networking.md`](./networking.md) | 全链路与协议推荐、热路径、端口划分 |
| [`observability.md`](./observability.md) | 指标、日志、追踪、告警建议 |
| [`tracing.md`](./tracing.md) | OpenTelemetry OTLP 追踪：配置、span 目录、后端对接 |
| [`testing.md`](./testing.md) | 测试策略、兼容套件、故障注入、性能门、升级回滚 |
| [`security.md`](./security.md) | 信任边界、多租户隔离、binding 鉴权、网络策略、管理后台 |
| [`control-plane.md`](./control-plane.md) | 管理后台、部署流水线、资源生命周期、控制面请求路由 |
| [`dev-mode.md`](./dev-mode.md) | `cellhive dev` 本地开发模式（单节点 + 文件系统 bucket + 热重载） |

### 状态与计划

| 文档 | 内容 |
|---|---|
| [`roadmap.md`](./roadmap.md) | 路线图与退出标准 |
| [`known-issues.md`](./known-issues.md) | 全部评审问题与决议闭环 + 未验证/待实现清单 |
| [`release-notes.md`](./release-notes.md) | 发布说明 |
| [`archive/`](./archive/) | 历史文档（如 `p0-tasks.md`、`p0-report.md`） |

## 按改动类型找文档

改代码前先读对应的契约与测试锚点；完整清单见 [`contributing.md`](./contributing.md)。

| 改动类型 | 先读 | 必须跑 |
|---|---|---|
| 协议 / 复制 / 恢复（owner、epoch、LTX、RPO） | [`cell-protocol.md`](./cell-protocol.md)、[`protocol-formats.md`](./protocol-formats.md) | 单测 + `make rpo-test` |
| Durable Objects（facet、alarm、WS、migration） | [`durable-objects.md`](./durable-objects.md)、[`workerd-integration.md`](./workerd-integration.md) | `make js-test` + DO e2e |
| binding / facade（KV/D1/R2/Queue/Workflow/AI…） | [`bindings.md`](./bindings.md)、[`compatibility-matrix.md`](./compatibility-matrix.md) | `make js-test` + 对应集成 |
| 计时 / 调度（cron、queue、DO alarm、wake 索引） | [`timers-and-dispatch.md`](./timers-and-dispatch.md) | `internal/timer`、`internal/dispatch` |
| 控制面 / 发布 / 路由 | [`control-plane.md`](./control-plane.md)、[`routing.md`](./routing.md) | `cmd/cellhive` e2e |
| 部署 / 配置 / 运维 | [`deployment.md`](./deployment.md)、[`configuration.md`](./configuration.md) | `make compose-config` / `k8s-render` / `helm-lint` |
| 可观测性 / 日志 / 追踪 | [`observability.md`](./observability.md)、[`tracing.md`](./tracing.md) | 对应指标/span 测试 |
| CLI / 打包 / wrangler 兼容 | [`wrangler-compat.md`](./wrangler-compat.md)、[`dev-mode.md`](./dev-mode.md) | `make cli-test` |

## 文档规则

- **语言**：`docs/` 以中文为现行语言；根 `README.md` 为英文默认、`README.zh.md` 为中文。
- **写契约，不写教程**：内部文档记录 ownership、接口、存储 key、失败语义、部署顺序、可观测性与**测试锚点**，而不是面向用户的使用教程。
- **与代码同步**：改代码就改对应文档；改设计先改 [`decisions.md`](./decisions.md) 并追加 ADR。
- **状态口径**：`Supported`（普通应用可用）· `Partial`（有明确边界）· `Rejected`（部署/配置期显式拒绝）· `Internal`（平台面，非租户 API）。
- **入口优先**：新增文档要在本页的分组地图里登记，并给出"何时该读它"。

## 术语约定

- **cell**：一个有名字、有独立 SQLite 数据库的状态单元，等价于一个 Durable Object。
- **stock workerd**：Cloudflare 官方发布、未打补丁/未 fork 的 workerd。
- **cell-agent**：自研 Go 进程（固定集群）：拥有并复制 KV/D1/Queue/Workflows/Cron 的 cell，兼控制面、owner 解析、计时派发与唯一桶凭据。
- **do-runtime**：承载 workerd 原生 Durable Object 的**分布式弹性执行层**。
- **owner / epoch / fence**：cell 的单写者租约、单调代次与围栏。
- **LTX**：SQLite 事务日志的复制格式（实现参照 LiteFS/`superfly/ltx`）。

_最后更新：2026-09-19_
