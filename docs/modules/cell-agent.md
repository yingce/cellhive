# 模块：cell-agent（状态 / 控制 / 复制 / 派发）

固定集群的状态与控制节点：拥有并复制 KV/D1/Queue/Workflow/Cron 的 cell（每 cell 一份 SQLite），维护 owner/epoch 租约，派发计时器，托管控制面，并是**唯一长期持有对象存储凭据**的进程。

> 配置权威来源见 [`../configuration.md`](../configuration.md) 与代码；下表是本模块相关子集。
## 关键接口

`:7001` 内部 REST（Go↔Go 与 workerd bindings 共用，无 gRPC）/ `:8082` admin。关键端点：`/readyz`、`/metrics`（绑定计数与持久化证明直方图带**有界 `ns` 标签**，ADR-179）、`/v1/diagnose`、`/v1/internal/{resolve,claim,renew,release,commit(_binary),worker/bindings,do/*,logs,telemetry/spans}`、`/v1/{kv,d1,r2,queue,workflow,vectorize}/*`、`/v1/control/*`。

## 配置（环境变量）

| 变量 | 默认 | 作用 |
|---|---|---|
| `CELLHIVE_ADMIN_ADDR` | `:8082` | admin/控制面监听 |
| `CELLHIVE_ADVERTISE` | `127.0.0.1:7001` | 对内广告地址（owner 转发目标）；内部只有 REST `:7001` |
| `CELLHIVE_AUDIT_RETENTION` | `720h` (0 永久) | 审计保留 |
| `CELLHIVE_AUTOSCALE_COOLDOWN` | `5m` | 动作**变化**的冷却窗口（防抖；0=关，ADR-151）。冷却期内返回 `hold/cooldown` + `cooldown_remaining_ms` |
| `CELLHIVE_AUTOSCALE_INTERVAL` | `30s` | 评估间隔 |
| `CELLHIVE_AUTOSCALE_MAX` | `0` 不限 | 目标上限 |
| `CELLHIVE_AUTOSCALE_MIN` | `1` | 目标下限 |
| `CELLHIVE_AUTO_CREATE_APP` | `true` | deploy 首次自动建 app |
| `CELLHIVE_BASE_DOMAIN` | 空 | 内置域 `<ns>-<worker>.<base>`；空=禁用 |
| `CELLHIVE_BINDING_CACHE` | `1s` (0 关) | binding 声明缓存 |
| `CELLHIVE_BUNDLE_GC_GRACE` | `24h` | 未引用保留期 |
| `CELLHIVE_BUNDLE_GC_INTERVAL` | `0` 关 | bundle/assets GC |
| `CELLHIVE_CELLS_PER_NODE` | `100` | 每节点容量目标 |
| `CELLHIVE_CELL_DISK_MAX` | `0` 无限 | cell 文件字节预算（LRU 删非 owned） |
| `CELLHIVE_CELL_DISK_SWEEP` | `1m` | janitor 间隔 |
| `CELLHIVE_CELL_IDLE` | `0` 不清理 | 空闲句柄关闭 |
| `CELLHIVE_COMPACTION_INTERVAL` | `30s` (0 关) | L0→L1 压实 |
| `CELLHIVE_COMPACTION_MIN_BYTES` | `64MiB` | 触发字节 |
| `CELLHIVE_COMPACTION_MIN_SEGMENTS` | `64` | 触发段数 |
| `CELLHIVE_CRON_INTERVAL` | `30s` (0 关) | cron→timer 物化 |
| `CELLHIVE_DATA_DIR` | `./.cellhive/data` | 本地工作目录根（spool、DO 盘等由此派生） |
| `CELLHIVE_DISK_HIGH` | `0` 关 | 高水位→overloaded 拒 claim |
| `CELLHIVE_DISPATCH_URL` | 空 | 计时/queue/cron/workflow 派发目标 —— 必须是 **user-runtime 内部特权端点**（`http://user-runtime:8088`）。空则派发循环不启动（compose/k8s/Helm 默认已设） |
| `CELLHIVE_DO_EAGER_RESTART` | `false` | deploy 急切重启 DO |
| `CELLHIVE_DO_RUNTIMES` | 空 | do-runtime 列表（DO 放置）；空则禁用 `/v1/do/invoke` |
| `CELLHIVE_DRAIN_TTL` | `30s` | drain token 有效期（关停等待 = 同值） |
| `CELLHIVE_LOG_BUFFER` | `1000:200` | 日志尾缓冲 `<entries>:<workers>`（`cellhive tail --worker`） |
| `CELLHIVE_LTX_COMPRESSION` | bool，默认 `true` | LTX page-map 用 LZ4（`WAL3`）；`false` 退回 `WAL2` 固定帧不压缩（省 CPU、桶字节更多；解码/GC 两者兼容，ADR-161） |
| `CELLHIVE_MAX_OPEN_CELLS` | `0` 无限 | 缓存句柄上限 |
| `CELLHIVE_MAX_RESIDENT_CELLS` | `0` 无限 | 常驻句柄上限 |
| `CELLHIVE_NODE_ID` | `node-1` | 节点稳定身份（owner/handoff key），必填 |
| `CELLHIVE_NS_RATE` | 空（关） | 每 ns 写准入 `rps[/burst]`（缺省 burst=rps） |
| `CELLHIVE_PAGED_HYDRATE_MBPS` | 整数，默认 `16` | paged cell 后台补齐速率（MiB/s，每节点同时一个）；`0` = 保持稀疏、每个冷页读桶 |
| `CELLHIVE_PAGED_MIN_BYTES` | 字节，默认 `256MiB` | 小于该值的链整克隆；`0` = 一律分页 |
| `CELLHIVE_PAGED_PREFETCH_WORKERS` | 整数，默认 `4` | child 并发预取 worker 数 |
| `CELLHIVE_PAGED_RESTORE` | bool，默认 `true` | cell-agent：冷恢复时对"已 compaction 且链 ≥ MIN_BYTES"的 cell 用 fault-in VFS 按页加载（ADR-160） |
| `CELLHIVE_PAGED_WINDOW_PAGES` | 整数，默认 `64` | 窗口预取：一次 ranged read 最多连取多少相邻页（另有 256KiB 预算） |
| `CELLHIVE_PEER_URL` | `http://127.0.0.1:7001` | 本节点 REST 回指地址（peer 复制目标） |
| `CELLHIVE_PLACEMENT_AZ` | 空 | 本节点故障域（rack/zone）；设置后 follower 选择**优先不同 AZ**（ADR-151）。空=无偏好 |
| `CELLHIVE_PLACEMENT_WEIGHT` | `0`→CPU | ownership 份额 |
| `CELLHIVE_QUEUE_BATCH` / `_LEASE` | `0`（store 默认） | 单批消息数 / 可见性租约 |
| `CELLHIVE_QUEUE_INTERVAL` | `1s` (0 关) | queue 消费 |
| `CELLHIVE_QUEUE_RETRY_DELAY` | `30s` | 失败批次的重投延迟（重试上限/DLQ 由 per-consumer 配置） |
| `CELLHIVE_REBALANCE_INTERVAL` | `0` 关 | 再平衡循环 |
| `CELLHIVE_REBALANCE_MAX_MOVE` | `32` | 单轮最多释放 |
| `CELLHIVE_REST_ADDR` | `:7001` | 内部 REST（Go↔Go 与 workerd bindings 共用，无 gRPC） |
| `CELLHIVE_RUNTIME_DIR` | `$TMPDIR/cellhive` | 一次性运行目录根；各 runtime 用其子目录 |
| `CELLHIVE_SESSION_ID` | 启动纳秒 | 本次会话 id，重启即新 |
| `CELLHIVE_TIMER_BATCH` / `_FIRED_TTL` | `256` / `24h` | 单 pass 到期上限 / fired 标记保留 |
| `CELLHIVE_TIMER_INTERVAL` | `1s` (0 关) | 到期计时派发 |
| `CELLHIVE_WAKER_BACKOFF_MAX` | `1m` | 连续错误时的退避封顶（base=interval，`interval·2^fails`） |
| `CELLHIVE_WAKER_BATCH` / `_FIRED_TTL` | `256` / `24h` | 单 pass 上限 / fired 标记保留 |
| `CELLHIVE_WAKER_INTERVAL` | `5s` (0 关) | fleet waker（TTL 自动取 2×） |
| `CELLHIVE_WAKE_REPAIR_BATCH` | `256` | 单 pass 检查的本地 cell 数（轮转窗口，几趟覆盖全部） |
| `CELLHIVE_WAKE_REPAIR_INTERVAL` | `5m` (0 关) | 本地 wake 索引修复扫描（ADR-177）；启动先跑一次 |
| `CELLHIVE_WORKFLOW_RETENTION` | `0` 永久 | 终态实例修剪 |


> 解析规则（字符串/布尔/时长/字节/列表）见 [`../configuration.md`](../configuration.md#解析规则)。

## 关键不变量

- 热路径禁 List；只做点查与条件写。
- **ack 晚于耐久性证明**（capture 包裹每次 store 变更）。
- **授权先于转发**（scoped token）。
- 不执行租户代码（执行在 workerd）。

## 源码位置

`cmd/cell-agent/`；`internal/{config,cell,cellstore,bucket,objectstore,owner,lease,replica,ltx,compaction,pagedvfs,capture,upload,peer,nodelog,recovery,server,control,timer,dispatch,cron,waker,wake,queue,r2,d1,vectorize,workflow,cron,telemetry,...}`。

## 测试锚点

`make test`、`make rpo-test`（kill -9 后逐 key 校验 RPO=0）、`internal/server`、`internal/cellstore`、`cmd/cellhive` e2e。

## 相关文档

[`architecture.md`](../architecture.md)、[`cell-protocol.md`](../cell-protocol.md)、[`control-plane.md`](../control-plane.md)、[`storage-and-s3.md`](../storage-and-s3.md)、[`configuration.md`](../configuration.md)

_最后更新：2026-09-19_
