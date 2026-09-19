# 可观测性

## 指标（Prometheus 文本；各服务独立暴露）

### cell-agent `/metrics`（**代码实况**，ADR-157 对齐）

| 指标 | 类型 | 说明 |
|---|---|---|
| `cellhive_requests_total` | counter | 内部/数据面 HTTP 请求数 |
| `cellhive_claims_total` | counter | owner claim 次数 |
| `cellhive_append_total` | counter | 段追加次数 |
| `cellhive_segments_read_total` | counter | 段读取次数 |
| `cellhive_cellstore_open_cells` | gauge | 当前打开的 cell（SQLite 句柄） |
| `cellhive_resident_cells` | gauge | 常驻 cell（当前=open cells） |
| `cellhive_owned_cells` | gauge | 本节点持有的 owner scope 数 |
| `cellhive_cellstore_evicted_total` / `cellhive_cellstore_sweeps_total` | counter | 驱逐/清扫 |
| `cellhive_cellstore_disk_files` / `cellhive_cellstore_disk_bytes` | gauge | 本地 cell 文件数与字节 |
| `cellhive_bucket_ops_total{op}` | counter | 桶操作（`put`/`get`/`list`/`conditional_create`/`cas`/`delete`） |
| `cellhive_list_calls_total` | counter | **应为 0（热路径禁 List）**；非 0 说明有诊断/运维路径在列桶 |
| `cellhive_paged_cells` | gauge | 当前打开且经 paged VFS 的 cell 数（ADR-160） |
| `cellhive_paged_faults_total` / `_runs_total` | counter | 页 fault 数 / 取数调用数（窗口或单页） |
| `cellhive_paged_prefetch_hits_total` | counter | 由 child 预取缓存直接命中、未再取数的 fault |
| `cellhive_paged_hydrated_pages` / `_total_pages` | gauge | 已物化页数 / cut 总页数（后台补齐进度） |
| `cellhive_upload_batches_total` / `_segments_total` / `_dropped_total` / `_deferred_total` / `_replayed_total` / `_spool` | counter/gauge | 上传批/spool（ADR-143） |
| `cellhive_upload_spool_file_syncs_total` / `_dir_syncs_total` / `_sync_seconds_total` | counter | spool 断电安全 fsync 计数与耗时（目录 sync 记忆化，ADR-171） |
| `cellhive_binding_calls_total{kind,outcome}` | counter | 绑定端点调用（`outcome=ok|denied|error`；`denied`=401/403/429）（ADR-165） |
| `cellhive_durability_proof_seconds` | histogram | 写路径 `Capture.Wait` 持久化证明耗时（桶 5ms/25ms/100ms/500ms/1s/5s）（ADR-165） |
| `cellhive_owner_epoch_changes_total{role}` | counter | epoch（generation）递增次数（ADR-165） |
| `cellhive_takeover_total{outcome}` | counter | 接管：`success`（接管过期的外部 owner 记录）/`failed`（CAS 竞争失败）/`blocked`（活跃外部租约阻塞）（ADR-165） |
| `cellhive_route_projection_version` | gauge | 路由投影已构建的 control revision（ADR-165） |
| `cellhive_peer_hedge_fired_total` / `_won_total` | counter | fleet 复制 hedge 副本发出/赢得 ack 的次数（ADR-165） |
| `cellhive_replication_bytes_total{kind}` | counter | fleet 复制字节：`shipped`（owner→follower 实际发出的副本字节）/`received`（本节点作为 follower 落盘的字节）（ADR-166） |
| `cellhive_waker_fires_total{kind,outcome}` | counter | 定时器派发结果（`outcome=ok|failed`；kind=cron/queue_delay/workflow_sleep/do_alarm/…），waker 与单 scope runner 都计入（ADR-166） |

### do-supervisor `/metrics`（ADR-166）

do-runtime 的 Go 监督进程在 `-listen`（默认 `:18901`）暴露：

| 指标 | 类型 | 说明 |
|---|---|---|
| `cellhive_do_wal_captured_bytes_total` | counter | 输出门捕获的输入字节（SQLite + sidecar 的文件大小，同步累加） |
| `cellhive_do_restore_seconds` | summary | 冷恢复（`RestoreAll`/`RestoreObject`）时长 `_sum`/`_count` |
| `cellhive_do_output_gate_timeouts_total` | counter | `/sync-all` 输出门失败次数（捕获/证明出错） |
| `cellhive_do_alarms_fired_total{outcome}` | counter | 租户 `alarm()` 执行结果（host actor 上报增量） |
| `cellhive_do_ws_sessions` | gauge | DO WebSocket 会话数（host actor 上报 `opens-closes` 增量，监督进程累加并在 host 重启时钳到 0） |

host actor（`workerd/do-runtime/host.js`）在每次 invoke 后用 best-effort `POST <GATE_URL>/internal/do/stats` 上报增量；它是进程级近似（host 重启会清零，靠下一次上报的负增量钳回 0）。

### 每域 stats（运维，非 Prometheus；ADR-157）

`GET /v1/<kind>/stats`（`kv`/`d1`/`queue`/`r2`/`workflow`/`hyperdrive`/`do`）给**按资源**的只读元数据（页/文件字节、过期索引、表清单、队列积压与滞后、有界 R2 近似、实例估计）。它与指标的分工：**指标=节点级时间序列与告警；stats=单资源即时盘点**（不采样、不聚合、按需）。字段与成本见 `docs/bindings.md`（Vectorize 的 `/v1/vectorize/stats` 还带 `ann` 模型信息）。

### 计划中（尚未实现）

无：`docs/observability.md` 曾列出的"计划中"指标已全部落地（ADR-165 server 侧；ADR-166 replication/waker + do-supervisor）。新增指标前请先在此登记名称与数据源。

## 日志

- 结构化 JSON（Go `log/slog`；workerd 侧结构化 stdout）；
- 必备字段：`ts`、`level`、`service`、`node`、`request_id`（可用时）、`trace_id`/`span_id`（请求内触发时）、`ns`/`worker`/`scope`（可用时）、`event`；
- **禁止**记录：secrets 明文、internal token、原始桶响应体；
- 关键事件：`owner_acquired`、`epoch_bumped`、`takeover_started/finished`、`drain_started/finished`、`waker_fire`、`do_restore_started/finished`、`output_gate_timeout`。

### 租户日志：内存 tail + 可选 OTLP 导出（ADR-172）

- 租户 `console.*` 由 `workerd/platform/log-tail.js` 采集 → `POST /v1/internal/logs` → cell-agent 的**有界内存 ring**（`CELLHIVE_LOG_BUFFER=<entries>:<workers>`，默认 `1000:200`）→ `cellhive tail --worker <ns>/<worker>` 轮询 `GET /v1/control/logs`。ring **非持久、单节点**，忙时丢最旧。
- **可选 OTLP/HTTP logs 导出**（标准协议，换后端只改环境变量；与 traces 共用 endpoint/headers/resource，路径 `/v1/logs`）：`CELLHIVE_OTLP_LOGS=off|tail|all`（默认 **off**）。
  - `all`：每条都导出（集中采集；量大）。
  - `tail`：**只有存在活跃订阅的 `(ns,worker)` 才导出**——`cellhive tail --worker` 每次轮询会 POST `/v1/control/logs/subscribe`（TTL 60s，自动续），tail 退出后 ≤60s 停止导出。
  - 导出是旁路、best-effort、后台批量（`sdk/log` BatchProcessor，2s），失败不影响请求也不阻塞。
  - **fleet 广播**：`cellhive tail` 仍只连一台 cell-agent；那台在收到订阅后**向其它活节点广播**（内部端点 `POST /v1/internal/logs/subscribe`，同 TTL；每 worker 最多每 25s 广播一次）。节点名单来自 lease（`Advertise`），广播 best-effort——某节点短暂失联会漏一次，下次续订（tail 每轮 ≤1s）在 ≤25s 内补上。

## 追踪（OTLP，ADR-167）

详见 [`tracing.md`](./tracing.md)（配置、span 目录、**OpenObserve**/Collector/Tempo/Jaeger 对接、排障）。

概览：对齐标准 **OpenTelemetry OTLP/HTTP**：CellHive 不存 trace，只把 span 导出到你配置的 OTLP 后端，**换后端只改 `CELLHIVE_OTLP_ENDPOINT`/`CELLHIVE_OTLP_HEADERS`**。

- **开关/采样**：`CELLHIVE_OTLP_ENDPOINT` 空 = 关闭（无 span、无开销）；否则按 `CELLHIVE_TRACES_SAMPLE_RATIO` 对入口新 trace 头部采样，上游 `traceparent` 的 sampled 位优先（`ParentBased`）。
- **Go span**（cell-agent）：`http.server`（每请求，`http.route`/`http.status_code` + `cellhive.namespace/kind/name/scope`）、`cell.durability_proof`（`Capture.Wait`）、`peer.append`（每次向 follower 发出副本，带 scope 属性）。do-supervisor 的 gate/restore 也走同一导出。
- **JS span**（workerd，平台 worker 持 internal token）：loader 的 `http.server` 入口 span、do-runtime host 的 `do.invoke` 与 `do.gate`。JS 无法跑 OTel SDK，因此把 span 记录 best-effort `POST` 到 cell-agent 的 `/v1/internal/telemetry/spans`，由 Go 统一导出。
- **传播**：W3C `traceparent` 入口生成/透传 → 租户 handler → props-bound binding 调用（loader 在自身 isolate 设置 `traceparent`，ADR-167 修复了 ADR-146 的"props-bound facades 不带 trace"残余）→ DO invoke spec。
- **残余**：租户 isolate 的 `facades.js` 调用（非 props-bound 回退路径）无 internal token，不上报 span；`/v1/do/connect`/abort、compaction/upload 后台循环无独立 span；JS span 时间戳为毫秒精度。

## 多租户与对外查询（推荐：日志/追踪 push，指标内部 pull）

- **日志/追踪 push**：节点只向**内网 OTLP 端点**推送（`CELLHIVE_OTLP_ENDPOINT` / `_HEADERS`；`CELLHIVE_OTLP_LOGS=off|tail|all`），平台不对外暴露查询面——存储、保留、查询与**租户鉴权**都交给后端（OpenObserve/Tempo/Collector）。
- **统一资源属性**：每条记录带 `service.name`、`service.instance.id`（= 节点 id）、`cellhive.namespace`、`cellhive.worker`；租户日志在请求内触发时带 `trace_id`/`span_id`（`logbuf.Entry` + `log-tail.js` 逐行 `traceparent`，cell-agent 解析）→ 后端可 **指标 → trace → 日志** 跳转。
- **租户隔离的唯一租户面在后端**：把每个 namespace 映射到后端的 **org/stream**（如 OpenObserve 的 `logs-<ns>`），给租户一个只读该范围的用户；**不要**只靠 `cellhive.namespace` 属性做行级隔离（多数后端不支持按任意属性过滤）。
- **参考管线**：[`../deploy/observability/otel-collector.yaml`](../deploy/observability/otel-collector.yaml)（OTLP in → redact/route/tail-sample → OpenObserve）+ [`../deploy/observability/README.md`](../deploy/observability/README.md)；compose 用 `--profile observability`（OpenObserve 在 `tracing` profile）。
- **指标保持内部 pull**：`/metrics` **免鉴权**，不要公网暴露；要并入 OTLP 就用 Collector 的 `prometheus` receiver 抓取后转投，而不是开放裸端点。
- **投递语义**：OTLP 导出是 **best-effort、有界内存批**；后端不可用会丢遥测而不阻塞请求。审计级留存需在 Collector/后端前加持久缓冲（file exporter）。

## 告警建议（运维）

- `cellhive_takeover_total{outcome="failed"}` 上升（接管竞争/失败）；
- `cellhive_durability_proof_seconds` p99 超阈值：`histogram_quantile(0.99, sum(rate(cellhive_durability_proof_seconds_bucket[5m])) by (le))`；
- `cellhive_binding_calls_total{outcome="denied"}` 异常放大（鉴权/配额问题）；
- 节点 lease 过期 / `pressured=true` 持续；
- `cellhive_do_output_gate_timeouts_total` 上升（DO 输出门捕获/证明失败）；
- `cellhive_do_restore_seconds` p99（`rate(_sum)/rate(_count)`）持续升高；
- `cellhive_list_calls_total` > 0（回归热路径禁 List）。

_最后更新：2026-09-19_
