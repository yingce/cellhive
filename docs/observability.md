# 可观测性

## 指标（Prometheus 文本；各服务独立暴露）

> 格式：Prometheus text exposition **0.0.4**（`content-type: text/plain; version=0.0.4`），手写渲染。标签值只按规范转义 `\`、`"`、换行（`escapeLabel`）；**不要**用 Go `%q`（会产出 `\t`/`\xNN`/`\uNNNN`，抓取端拒绝）。histogram 用累计桶 + `le="+Inf"` + `_sum`/`_count`。新增指标先在此登记名称与数据源。

**查询指标名与标签键**

- 直接抓取：`curl -s http://127.0.0.1:7001/metrics`（cell-agent；do-supervisor 用 `:18901`，user-runtime ingress 用 `CELLHIVE_INGRESS_ADMIN_PORT`）。每行形如 `cellhive_binding_calls_total{ns="acme",kind="kv",outcome="ok"} 1`。
- 只列指标名：`curl -s http://127.0.0.1:7001/metrics | sed -n 's/^\([a-zA-Z_:][a-zA-Z0-9_:]*\).*/\1/p' | sort -u`
- 列某个 family 的标签组合：Prometheus `GET /api/v1/series?match[]=cellhive_binding_calls_total`，或 PromQL `count by (__name__) ({__name__=~"cellhive_.*"})`。
- **代码内部的聚合键**不是暴露格式：`bindingCalls` 为 `ns|kind|outcome`，`ensureModes` 为 `ns|mode`，waker 为 `kind|outcome`；只在渲染时拆成标签。

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
| `cellhive_upload_spool_file_syncs_total` / `_dir_syncs_total` / `_sync_seconds_total` | counter | spool 断电安全 fsync 计数与耗时（每次 publish/remove 同步叶目录，父目录创建项按进程记忆化；ADR-171） |
| `cellhive_binding_calls_total{ns,kind,outcome}` | counter | 绑定端点调用（`outcome=ok|denied|error`；`denied`=401/403/429）。`ns` 有上界：超 `CELLHIVE_METRICS_NS_MAX`（默认 1000）的记为 `other`，无 ns 的内部调用记为 `platform`（ADR-165/179） |
| `cellhive_durability_proof_seconds{ns}` | histogram | 写路径 `Capture.Wait` 持久化证明耗时，按 ns 分（桶 5ms/25ms/100ms/500ms/1s/5s；ns 同上（ADR-165/179） |
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

`GET /v1/<kind>/stats`（`kv`/`d1`/`queue`/`r2`/`workflow`/`hyperdrive`/`do`）给**按资源**的只读元数据（页/文件字节、过期索引、表清单、队列积压与滞后、R2 启用后的持久估算用量、实例估计）。R2 只有 `?scan=1`/`prefix`/`limit` 才执行有界 List。它与指标的分工：**指标=节点级时间序列与告警；stats=单资源即时盘点**（不采样、不聚合、按需）。字段与成本见 `docs/bindings.md`（Vectorize 的 `/v1/vectorize/stats` 还带 `ann` 模型信息）。

### 计划中（尚未实现）

无：`docs/observability.md` 曾列出的"计划中"指标已全部落地（ADR-165 server 侧；ADR-166 replication/waker + do-supervisor）。新增指标前请先在此登记名称与数据源。

## 日志

### 当前工作树的 Go 缓冲边界（2026-11-04，未部署）

`internal/logbuf` 在条数/worker 数限制之外，为每个 `(namespace, worker)` 的 entries 和 events **分别**设置 1MiB 保留预算。Entry 的 Message 与 MessageArgs 各最多 64KiB；异常最多 16 个，每个 message 8KiB、stack 16KiB。Entry 先限字段，仍超预算则拒绝；TailEvent 按序列化字节计量，过大或无法序列化则整体拒绝，不冒充完整保留。预算是保留载荷计量，不等于 RSS 或瞬时解码内存上限。

成功保留才推进本进程共享 seq；拒绝返回 `Seq=0` 且不消耗 seq，因此无 seq 缺口不能证明没有丢日志。容量淘汰最旧数据，worker 淘汰及重启也会损失历史。`Stats` 的 Added/Dropped/Truncated/Evicted 是内存 API 计数，不是新增 Prometheus family；Dropped 表示拒绝保留，Evicted 表示保留期损失，均不代表业务执行失败或持久数据丢失。派生 console 的 OTLP 参数使用限幅后的值；旁路导出与 Tail 保留不是同一成功保证。

持久 invocation 的 7 天/10000 条为软目标：按 namespace 在首次写及随后 1 分钟或 256 次写触发清理，每条年龄/数量清理查询最多 1000 行，旧时间戳写入可强制触发。积压、无后续写入和失败事务会延迟收敛；不是独立后台定时清理或硬行数上限。

- 结构化 JSON（Go `log/slog`；workerd 侧结构化 stdout）；
- 必备字段：`ts`、`level`、`service`、`node`、`request_id`（可用时）、`trace_id`/`span_id`（请求内触发时）、`ns`/`worker`/`scope`（可用时）、`event`；
- **禁止**记录：secrets 明文、internal token、原始桶响应体；
- 关键事件：`owner_acquired`、`epoch_bumped`、`takeover_started/finished`、`drain_started/finished`、`waker_fire`、`do_restore_started/finished`、`output_gate_timeout`。

### Timer 派发失败诊断

- `timer dispatch failed` 保留去重 ID `token`（不是鉴权 token）及 `kind`，增加可解析的 `ns`/完整 `scope`、已知的 `worker`、持久 timer 的原始 `scheduled_time_ms` 和本次派发 `duration_ms`。不记录 occurrence、事件 payload、JWT、Authorization 或业务返回值。
- HTTP cron、workflow 和 DO alarm 失败增加 `target_host`（无 userinfo）、`target_path`（固定端点名，不含配置路径前缀、query、fragment）、`http_status`（收到响应时）、`error_code` 和 `phase`。只识别受控错误码 `handler_failed`、`workflow_failed`、`bundle_fetch_failed`、`unauthorized`、`bad_json`、`bad_request`、`dedicated_target_mismatch`；其他响应统一 `http_error`。忽略下游 message、invocation ID 和所有未知字段，HTML/纯文本不入日志。错误响应最多读取 4096 字节，小响应读完后可复用连接，大响应直接关闭，不无限 drain。
- 网络错误只输出 `canceled`/`timeout`/`transport_error` 与可判定的 `dns`/`dial`/`read`/`write` 阶段；未判定则保留 `transport`。请求构造和鉴权失败阶段为 `request`/`auth`，不输出原始错误字符串。其他 dispatcher 错误仅输出通用分类。
- Cron 成功写入重试状态后输出 `attempt`（现有累计失败次数）、`retry_scheduled=true`、`retry_at_ms` 和 `retry_delay_ms`；写入失败输出 `retry_scheduled=false` 与 `retry_error_code=retry_schedule_failed`，不伪造 attempt。达到 `CELLHIVE_CRON_MAX_ATTEMPTS` 时输出 `timer dispatch exhausted`，带 `attempt`、`max_attempts` 和 `retry_scheduled=false`，并在同一事务消费 slot。其他 kind 输出 `retry_policy=next_poll`，不臆造未存储的尝试次数。重试、fired 和持久化 gate 语义不变。
- 这些是 cell-agent 的 slog stderr 字段（当前入口使用 TextHandler），不是租户 OTLP invocation schema 的新增字段。仓库内无外部 `vworklog`/`log_context` 固定 schema；部署后须另验采集器是否保留新增键。本次诊断增强不代表 HTTP 500 根因已修复。

### Queue 派发失败诊断

- Queue 批次派发失败同样只输出 `error_code`、`phase`、`target_host`（无 userinfo）、固定 `target_path=/v1/queues/dispatch`、收到响应时的 `http_status` 和已知 `worker`。错误码只接受与 timer 相同的受控集合，其他响应统一 `http_error`；不透传下游 body、message、invocation ID、配置路径、query、token 或消息 payload。
- 非 2xx 响应最多读取 4096 字节后关闭；重试状态判定不变：仅 502/503/504 自动重试一次，其他状态（含处理器 500）保留原有最终失败与队列重试/死信语义。

### 平台日志：内存 tail + 可选 OTLP 导出（ADR-172）

- `workerd/platform/log-tail.js` 是可信宿主导出的 `TenantTail extends WorkerEntrypoint`。user-runtime、内部 dispatch/service target 与 do-runtime facet 在 `workerLoader` 的 `WorkerCode.tails` 中传入宿主 `ctx.exports.TenantTail({props:{namespace,worker,version}})` 生成的真实 `Fetcher`。Tail 捕获每次调用事件（包括没有 `console.*` 的成功调用）、console 与未捕获异常，保存在所在 workerd service 的有界内存 ring；有人 Tail 订阅时，cell-agent 经受保护的 `/v1/internal/logs/events` 按游标拉取。`OTLP_LOGS=all` 时则后台持续拉取。租户身份取不可伪造的 host-bound props，不取动态事件中为 null 的 `scriptName`。log-role token 和查询能力仅在可信宿主，tenant env 仍为零平台键。runtime 重启、ring 溢出或短暂失联可能丢失实时日志。
- **可选 OTLP/HTTP logs 导出**（标准协议，换后端只改环境变量；与 traces 共用 endpoint/headers/resource，路径 `/v1/logs`）：`CELLHIVE_OTLP_LOGS=off|tail|all`（默认 **off**）。
  - `all`：首个配置的 cell-agent 后台按游标拉取并导出所有 runtime 日志（集中采集；量大）。该 agent 长时间失联或 runtime ring 溢出可能漏日志，属于 best-effort。
  - `tail`：**只有存在活跃订阅的 `(ns,worker)` 才导出**——`cellhive tail --worker` 每次轮询会 POST `/v1/control/logs/subscribe`（TTL 60s，自动续）；Wrangler Tail 的 WebSocket 会续订。cell-agent 在订阅有效时后台拉取对应 runtime ring，tail 退出后 ≤60s 停止导出。
  - Queue/Cron/Workflow 持久调用的完成日志（成功或失败）从记录导出整数属性 `cellhive.started_ms` / `cellhive.finished_ms`（Unix 毫秒，正值才输出）；两者有效且结束不早于开始时，另导出 `cellhive.duration_ms = finished_ms - started_ms`（允许 0）。缺失或倒序时间不臆造 duration；普通 console/Tail 无这组时间时不补值。OTLP `timeUnixNano` 仍使用 `FinishedMs`，`cellhive.request_id` 原样保留。未提供可信单一父 span，不导出 `parent_span_id`；Queue 继续用 producer links。
  - 导出是旁路、best-effort、后台批量（`sdk/log` BatchProcessor，2s），失败不影响请求也不阻塞。
  - **fleet 广播**：`cellhive tail` 仍只连一台 cell-agent；那台在收到订阅后**向其它活节点广播**（内部端点 `POST /v1/internal/logs/subscribe`，同 TTL；每 worker 最多每 25s 广播一次）。节点名单来自 lease（`Advertise`），广播 best-effort——某节点短暂失联会漏一次，下次续订（tail 每轮 ≤1s）在 ≤25s 内补上。

## 追踪（OTLP，ADR-167）

详见 [`tracing.md`](./tracing.md)（配置、span 目录、**OpenObserve**/Collector/Tempo/Jaeger 对接、排障）。

概览：对齐标准 **OpenTelemetry OTLP/HTTP**：CellHive 不存 trace，只把 span 导出到你配置的 OTLP 后端，**换后端只改 `CELLHIVE_OTLP_ENDPOINT`/`CELLHIVE_OTLP_HEADERS`**。

- **开关/采样**：`CELLHIVE_OTLP_ENDPOINT` 空 = 关闭（无 span、无开销）；否则按 `CELLHIVE_TRACES_SAMPLE_RATIO` 对入口新 trace 头部采样，上游 `traceparent` 的 sampled 位优先（`ParentBased`）。
- **Go span**（cell-agent）：`http.server`（每请求，`http.route`/`http.status_code`；绑定端点解析出 scope 后追加 `cellhive.namespace`/`cellhive.binding`，与 ADR-179 指标同维度）、`cell.durability_proof`（`Capture.Wait`）、`peer.append`（每次向 follower 发出副本，带 scope 属性）。do-supervisor 的 gate/restore 也走同一导出。
- **JS span**（workerd，平台 worker 持 internal token）：loader 的 `http.server` 入口 span、do-runtime host 的 `do.invoke` 与 `do.gate`。JS 无法跑 OTel SDK，因此把 span 记录 best-effort `POST` 到 cell-agent 的 `/v1/internal/telemetry/spans`，由 Go 统一导出。
- **传播**：W3C `traceparent` 入口生成/透传 → 租户 handler → props-bound binding 调用（loader 在自身 isolate 设置 `traceparent`，ADR-167 修复了 ADR-146 的"props-bound facades 不带 trace"残余）→ DO invoke spec。
- **残余**：租户 isolate 的 `facades.js` 调用（非 props-bound 回退路径）无 internal token，不上报 span；`/v1/do/connect`/abort、compaction/upload 后台循环无独立 span；JS span 时间戳为毫秒精度。

## 多租户与对外查询（推荐：日志/追踪 push，指标内部 pull）

- **日志/追踪 push**：节点只向**内网 OTLP 端点**推送（`CELLHIVE_OTLP_ENDPOINT` / `_HEADERS`；`CELLHIVE_OTLP_LOGS=off|tail|all`），平台不对外暴露查询面——存储、保留、查询与**租户鉴权**都交给后端（OpenObserve/Tempo/Collector）。
- **统一资源属性与关联**：平台记录带 `service.name`、`service.instance.id`（= 节点 id）；租户日志另带 `cellhive.namespace`、`cellhive.worker`。原生 Tail 已捕获租户 `console.*`/未捕获异常；请求事件带 `traceparent` 时，ring 与 OTLP log record 都保留对应 `trace_id`/`span_id`，可与平台 span 关联（无请求上下文的 alarm/定时事件可能为空）。
- **租户隔离的唯一租户面在后端**：把每个 namespace 映射到后端的 **org/stream**（如 OpenObserve 的 `logs-<ns>`），给租户一个只读该范围的用户；**不要**只靠 `cellhive.namespace` 属性做行级隔离（多数后端不支持按任意属性过滤）。
- **参考管线**：[`../deploy/observability/otel-collector.yaml`](../deploy/observability/otel-collector.yaml)（OTLP in → redact/route/tail-sample → OpenObserve）+ [`../deploy/observability/README.md`](../deploy/observability/README.md)；compose 用 `--profile observability`（OpenObserve 在 `tracing` profile）。
- **可复现冒烟**：
  - `bash scripts/otlp-collector-smoke.sh`：真实 `otelcol-contrib`（file/debug 导出）+ cell-agent + user-runtime + 探针 worker，断言 collector 收到 span（带 `cellhive.namespace`）且 `/metrics` 出现带 `ns` 的绑定计数。它不再把租户 console log record 作为验收条件。
  - `bash scripts/openobserve-e2e.sh`：真实 **OpenObserve** + 仓库参考 collector 配置，端到端验证 traces：慢请求（>1s）经参考配置的 `tail_sampling`（keep-slow）保留整条 trace 进 `default traces`（span 带 `cellhive_namespace`/`cellhive_binding`）。它不再声称租户 console 日志进入 `default logs`。
  - **后端字段命名**：OpenObserve 会把属性点号展平（`cellhive.namespace` → 查询列 `cellhive_namespace`），SQL 用展平名；trace 查询用 `POST /api/<org>/_search?type=traces`，span 名在 `operation_name`。
- **ADR-178 私网采集例外保留**：`:7001/metrics` 全量 pull 免鉴权，Vector/Collector 直接抓取，无 JWT 刷新器、代理或额外服务。私网、防火墙/NetworkPolicy 限定可信采集器，不得公网暴露或允许租户出网触达；业务监控仍须鉴权。ADR-304 无状态修订实施与验收中，未部署，此前有状态测试不证明本次通过。
- `/v1/control/metrics` 使用 Bearer HS256 namespace JWT，与发布/CF 数据同属全部业务，无 metrics-reader/actions。按 kid pattern 上限、token `ns`、请求范围过滤；无 ns 或 `platform/other` 聚合不得泄漏给受限租户。admin 全量 `/metrics` 允许 admin 或 `issuer/*` 且显式 `ns=["*"]`；后者拥有全部全局业务及运维读取/GC 权限。精确/前缀授权无全平台权限，issuer CRUD 已移除。
- 日志/Tail 遵守无状态 pattern 上限与有效期；会话用途 key 与 admin 分离，绑定父 pattern/namespace/exp，不扩权或延寿。Tail 每500ms检查到期，不查询 issuer 撤销记录。无即时单 issuer 撤销，只有 exp/root轮换；泄露 key 需轮换 root 止损。Wrangler query token 仅为协议例外，无普通 Authorization 回退。长期鉴权客户端须刷新 JWT，私网 metrics scrape 不需要。
- **投递语义**：OTLP 导出是 **best-effort、有界内存批**；后端不可用会丢遥测而不阻塞请求。审计级留存需在 Collector/后端前加持久缓冲（file exporter）。

## 告警建议（运维）

- `cellhive_takeover_total{outcome="failed"}` 上升（接管竞争/失败）；
- `cellhive_durability_proof_seconds` p99 超阈值：`histogram_quantile(0.99, sum(rate(cellhive_durability_proof_seconds_bucket[5m])) by (le))`；
- `cellhive_binding_calls_total{outcome="denied"}` 异常放大（鉴权/配额问题）；
- 节点 lease 过期 / `pressured=true` 持续；
- `cellhive_do_output_gate_timeouts_total` 上升（DO 输出门捕获/证明失败）；
- `cellhive_do_restore_seconds` p99（`rate(_sum)/rate(_count)`）持续升高；
- `cellhive_list_calls_total` > 0（回归热路径禁 List）。

_最后更新：2026-11-02_
