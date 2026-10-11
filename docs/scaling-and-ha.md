# 伸缩与高可用

## 分层形态

| 层 | 组件 | 形态 | 状态 |
|---|---|---|---|
| 固定 | `cell-agent` 集群 | **≥2 节点、稳定身份、可横向扩** | 拥有 backend A SQLite + 控制面 |
| 弹性 | `do-runtime` 节点 | 可增删、**本地盘可临时** | DO 工作副本；权威在 cell-agent |
| 无状态 | `user-runtime` | Deployment + HPA | 入口 + 执行 |
| 入口 | `Traefik` | 无状态副本 | TLS + host 分流 |

## HA 基础

- **cell-agent**：每节点写 `nodes/*` bucket lease（自注册）；owner 记录用 bucket **条件写**；epoch fence；节点 lease 过期即接管；
- **写确认**：fleet 模式 = owner + **≥1 follower fsync**（2 节点即可）；**S3 不在 ack 路径**；
- **owner 失败**：lease 过期 → 另一节点条件写接管（epoch+1）；
- **do-runtime 失败**：Service/Endpoints 不再把未就绪 Pod 作为首次落点；已激活对象的 owner lease 停止续约并到期后，其他 Pod 可 claim（epoch+1）并从 cell-agent 恢复。若请求先落非 owner，则按 owner 记录中的 Pod advertise 地址转发；

## 优雅 handoff / drain

- `cell-agent`：获 bucket **drain token** 后逐批迁移 cell（见 [`cell-protocol.md`](./cell-protocol.md) §7）；同时关停由 token 串行化；
- `do-runtime`：supervisor 先停新请求 → 等在途 → flush WAL → `release(owner)` → 停 workerd；
- `user-runtime`：workerd 优雅关停，等 in-flight；WS 断开客户端重连。

`terminationGracePeriodSeconds` 建议：user-runtime 30 / do-runtime 60 / cell-agent 120+。

## 自动伸缩

- 节点 lease 的 `load` 暴露信号：`owned_cells`、`resident_cells`、`rss_bytes`、`cpu_percent_x100`、`pressured`、`memory_headroom`、`shed_cells`、`restoring`；
- **cell-agent 缩容下限 = 2**（fleet 证明需要）；扩容按 owned_cells/负载；
- `do-runtime` 可由外部编排器扩缩，但核心不计算其目标副本数；K8s Service Endpoints 自动跟随副本，新增节点按需从 cell-agent 恢复对象（分页懒加载），无需更新 `CELLHIVE_DO_RUNTIMES`；
- **单节点行为**：A 类 cell 写入默认等待权威桶；DO 只有使用 gated runtime 才有输出门 RPO=0，非 gated runtime 不应作为生产形态；

## 热点与均衡

- cell 归属按 bucket 条件写 + 放置提示（低负载节点优先）；
- DO 协调的 scope-hash 仅 **best-effort affinity**，非硬约束（ADR 中已定）；
- 背景 rebalance 迁移空闲 cell（批量、有上限）。

## 发现（与编排器解耦）

- cell-agent peer：bucket `nodes/*`（权威）；
- 调用方 → cell-agent：service name（K8s/Compose 内置 DNS）或种子列表；
- do-runtime → cell-agent：配置入口/DNS/LB；cell-agent → do-runtime 使用 `CELLHIVE_DO_RUNTIMES` 的逻辑 Service/LB 作为任意入口。K8s Service Endpoints 自动随 Pod 扩缩；owner 记录中的 per-Pod advertise 地址负责定向转发，无需 runtime 注册/心跳；
- 详见 [`decisions.md`](./decisions.md) ADR-027。

## 定稿（ADR-151）

**autoscaler（信号，不编排）**
- 目标：`ceil(owned_cells / CELLHIVE_CELLS_PER_NODE)`，下限 `CELLHIVE_AUTOSCALE_MIN`（默认 1）、上限 `CELLHIVE_AUTOSCALE_MAX`（0=不限）；任一同伴 `pressured`/`shed_cells>0` 时至少 `nodes+1`（压力优先于目标）。
- 评估间隔 `CELLHIVE_AUTOSCALE_INTERVAL`（默认 30s）；**冷却** `CELLHIVE_AUTOSCALE_COOLDOWN`（默认 5m）：动作**变化**在窗口内被抑制为 `hold/cooldown` 并返回 `cooldown_remaining_ms`，防止信号抖动导致反复扩缩。
- 实际增删节点属外部编排；核心只提供信号 + 可插拔 `Actuator`（默认 no-op/log）。

**rebalance（批大小与节流）**
- 节流 = 循环间隔 `CELLHIVE_REBALANCE_INTERVAL`（默认 0=关）；批大小 = `CELLHIVE_REBALANCE_MAX_MOVE`（默认 32，单轮最多释放的空闲 cell）。
- 只释放**空闲**（句柄未打开）且本节点拥有的 cell；epoch/owner 栅栏保证不丢数据。

**多 AZ / 故障域**
- `CELLHIVE_PLACEMENT_AZ`（本节点 AZ；空=无偏好）。设置后 **follower 选择优先不同 AZ**（`selectFollowers`：跨 AZ 在前、同 AZ 兜底、排除自己/过期/无 peer_url），使单 AZ 故障不会带走全部副本。
- owner 仍按 scope 哈希/容量放置；跨 AZ 只用影响复制目标，不改变一致性模型（follower fsync 证明仍在同一 quorum 语义）。

_最后更新：2026-09-25_
