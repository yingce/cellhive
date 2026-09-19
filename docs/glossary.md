# 术语表

- **cell**：一个有名字、有独立 SQLite 数据库的状态单元；等价于一个 Durable Object。scope = `<namespace>/<class>/<id>`。
- **scope**：cell 的持久标识：namespace + class + id。
- **cell-agent**：自研 Go 进程（固定集群）：拥有并复制 KV/D1/Queue/Workflows/Cron 的 cell，兼控制面、owner 解析、计时派发、waker，是**唯一长期对象存储凭据持有者**与唯一持久化数据端点。
- **do-runtime**：承载 workerd 原生 Durable Object 的**分布式弹性执行层**；本地盘可临时。
- **user-runtime**：无状态 workerd 池；南北向入口（Worker 路由/版本解析）+ 租户代码执行；`:8081` 公开、`:8088` 内部派发。
- **owner**：持有某 cell "工作 SQLite"的节点。A 类 = cell-agent 节点；B 类（DO）= do-runtime 节点。
- **owner 记录**：bucket 上的 `cells/<scope>/owner.json`，含 node/role/session/epoch/expiry/address。
- **epoch**：cell 每次激活/接管的单调代次；复制数据写入 `e<epoch>` 前缀，作为写入有效性围栏。
- **fence**：见 epoch；旧 owner 的写入落到废弃前缀，无害化。
- **LTX**：SQLite 事务日志的复制格式（实现参照 LiteFS/`superfly/ltx`）。
- **node lease**：`nodes/<node>.json`，节点存活证明（cell-agent 自注册）。
- **fleet**：共享同一 bucket 的节点集合。
- **ensemble / follower**：fleet 模式下，owner 招募最多 2 个 follower；**≥1 个 follower fsync 落盘即确认**（≥2 节点即可）。
- **输出门（output gate）**：写入在向调用方确认前，必须先获得持久化证明。
- **RPO=0**：已确认的写不会因单节点故障丢失。
- **waker**：bucket 租约选出的单 fleet leader，兜底 owner 已死/失联的计时器。
- **timer**：统一定时事件抽象（do-alarm/cron/queue-delay/queue-retry/workflow-sleep/timeout）。
- **host adapter**：workerd 内的平台代码，把 CF 形态 binding 转成对 cell-agent 的调用（binding-scoped、不可变 props）。
- **worker id**：`<ns>:<worker>:<version>`，不可变版本的加载身份。
- **路由投影**：`host/path → (ns,worker) → active version` 的映射；user-runtime 拉取 + 5–10s TTL 缓存。
- **control cell**：承载控制元数据的 cell（按 app 分片）。
- **state bucket / code bucket / assets / r2 / backup**：对象存储的角色划分（默认单桶 + 前缀）。
- **Traefik**：运维 ingress，负责 TLS 与 host 分流（非平台内构件）。
- **admin host**：管理后台入口（Traefik → cell-agent :8082）。
- **stock workerd**：Cloudflare 官方发布、未打补丁/未 fork 的 workerd。
- **bundle**：部署版本被打包出的代码产物（模块 + manifest），内容寻址（SHA-256）。
- **envelope encryption（信封加密）**：secrets 以密文存储，根密钥（KEK）在 cell 之外（env/KMS）。
- **CAS / 条件写**：bucket 的 compare-and-swap / if-match 写，用于单写者判定。

_最后更新：2026-09-14_
