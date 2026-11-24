# 运维 Runbook

## DLQ、资源清理与 Queue/Cron 执行记录

以下是 CellHive 管理扩展，使用 `:8082` 和 `x-cellhive-admin-token`（或配置的 JWT），namespace 权限先于转发检查。

| 操作 | API |
|---|---|
| DLQ 只读分页预览 | `GET /v1/queue/dead-letters?namespace=acme&queue=jobs&limit=100&cursor=...` |
| 直接预览独立 DLQ | 上述请求使用 `queue=dlq&direct=1` |
| 按 ID 确认 | `POST /v1/queue/dead-letters/ack?namespace=acme&queue=jobs`，JSON `{"message_ids":["id1","id2"]}`（1..100）；或 `message_id=` 单条 |
| 删除 KV / Queue | `DELETE /v1/kv/resources?namespace=acme&name=CACHE` / `/v1/queue/resources?...`；引用中需 `force=1` |
| 清理状态 | `GET /v1/control/resource/cleanup?namespace=acme&kind=kv&name=CACHE` |
| 手动重试 | `POST /v1/control/resource/cleanup/retry?namespace=acme&kind=kv&name=CACHE` |
| 调用结果 | OTLP 日志（collector/OpenObserve），不再有 `GET /v1/control/invocations` |

预览 `messages[].body` 为 base64；`content_type` 指明解码方式，`timestamp_ms/attempts` 为 DLQ 当前记录，`source_created_ms/source_attempts/source_queue/source_id` 保留来源。预览不领取、不加 attempts、不改变投递状态。重复 ack 安全，但不能取消已开始执行的业务代码。分页为活数据 keyset，不是全队列快照。

清理状态为 `pending/running/failed/complete`，含 `job_id/data_scope/attempts/last_error/next_retry_ms`；删除和手动重试唤醒本机，按持久化截止时间调度，空闲每分钟对账，失败指数退避最多自动 10 次，手动 retry 重置失败预算。每 pass 最多删除 500 replica 对象；大资源继续 pending，不消耗失败预算。删除完成前不允许重建；完成后重新创建才激活新 incarnation。删除主 Queue 不删除独立 DLQ，purge 仍只清消息。旧无来源 DLQ、共享 scope 或 bucket 失败会明确报告，不能当作清理完成。

删除任务完成后保留退役代次记录，以回收迟到的旧文件。启动时开始历史巡检，每个维护周期最多处理 20 条退役记录、每条一页。完整遍历后至少间隔一小时才再次从头扫描；期间只从控制 SQLite 的部分索引选取有未完分页或错误的记录访问对象存储。新删除仍直接清理。重启会提前重新开始历史巡检。永久退出需要系统证明覆盖所有写入者、在途 PUT、待重试和可重放 spool。当前没有跨节点的完整证明，所有完成的记录仍保留低频兜底，不提供人工跳过入口。

## cell-agent 后台定时访问

下表为源码默认值，实际周期以区域配置为准。一个 tick 可能只检查内存，也可能触发多次 COS 操作。生产 GET 归因用 `cellhive_bucket_get_attempts_total` 的 source、key_class 和 via 核对。

| 路径 | 默认周期或触发 | 可能的远端工作 |
|---|---|---|
| 节点租约 | TTL 10 秒，每 TTL/3 续租，已知节点至多每 2 秒刷新 | 发布节点权威、点查已知 peer。最近半个刷新周期已有成功的桶读且租约覆盖下一 tick 时，本轮省去重复点查。另有至多每 100ms 一次的本机自我隔离检查。 |
| Recovery | 每 5 秒，发现快照默认 1 分钟 | 参与 waker 选举，读租约，刷新 node-log 候选；租约变化可提前刷新。 |
| 资源清理、app/worker purge | 删除事件、到期重试，空闲每分钟对账 | 检查控制 owner，按持久任务执行；purge 有分批工作时按 5 秒推进。退役历史另走上述小时级补扫。 |
| Queue、Timer、Cron | Queue 收到通知或到期时派发，每分钟对账；驻留 timer 默认 1 秒；Cron 按 UTC 分钟，失败时默认 30 秒重试 | 按候选做 owner、持久状态检查与派发。shared 模式另有每秒 Redis 到期查询。 |
| 孤儿 wake、wake 修复 | waker 默认 1 分钟，过期索引清理间隔 10 分钟，本机修复默认 5 分钟 | 检查持久到期索引、接管孤儿、修复已知 scope。 |
| Fold、epoch GC、复制上传 | fold 每秒检查本机候选，epoch GC 默认首次 1 分钟后、之后每 1 小时（每节点一个 goroutine 遍历 OwnedScopes）；上传刷新默认 30 秒 | 按 scope 和条件做归并、回收或上传；常规 compaction 由事件唤醒。 |
| R2 multipart、Workflow、审计 | multipart 启动 1 分钟后扫第一页，整轮结束后间隔 1 小时，续页和错误重试间隔 1 分钟；Workflow 策略每分钟、启用默认保留策略时每 10 分钟；审计保留每小时 | multipart 检查旧上传；其余按控制面投影、owner 和保留策略执行。 |
| 交权补偿、磁盘维护、代理 | 交权每 5 秒先检查本机待重试集合；启用磁盘维护时默认 1 分钟；ProxySQL 配置后每 30 秒、Hyperdrive 代理健康每 5 秒 | 交权集合为空时没有 COS 请求；代理健康探测访问代理端点。按需启用的 rebalance 和 bundle/asset GC 默认关闭。 |

Queue/Cron/Workflow 与 HTTP fetch 的调用结果都是 OTLP 日志，字段见下方「OTLP 日志格式」。控制面不再有 `GET /v1/control/invocations`，也不再写 `__invocations__` cell。console 不进 Queue/Cron/Workflow 那条完成记录。是否导出由节点级 `CELLHIVE_INVOCATION_LOGS`（默认 on）控制（ADR-239 2026-11-20 修订）。

Queue 部分重试后批次可能重组：用同一 `cellhive.logical_id` 与 `cellhive.attempt` 在 OTLP 里追踪消息，`producer_links` 关联生产者 trace。handler 的返回值不进 OTLP；`ack/retry` 由 handler 返回给 dispatcher 驱动真实投递，只有计数进日志。

清理完成后保留 retirement GC 义务，后续有界轮转清除迟到的旧 epoch 上传；namespace purge 也保留小型 fence/owner-generation 元数据。删除/重建的本地 incarnation 标记独立于 owner，离线节点不能复用旧 SQLite。跨 Queue 转移带源 incarnation，并在目的地事务内验证，防止迟到 DLQ 转移恢复旧数据。

验收：`scripts/runtime-baseline-e2e.sh`、`scripts/cf-tail-multinode-e2e.sh`，以及 `workerd/user-runtime/invocation-otlp.test.mjs`。

## OTLP 日志格式

协议都是 OTLP/HTTP `ExportLogsServiceRequest` protobuf，`Content-Type: application/x-protobuf`，路径 `{CELLHIVE_OTLP_ENDPOINT 去掉末尾 /v1/logs 或 /v1/traces}/v1/logs`。属性名在后端里常被展成下划线（OpenObserve 用 `cellhive_event_type` 查询）。空字符串、`null` 不写入直接导出的那条记录。

导出有三条路，属性集合相同，Resource 不同：

| 路 | 何时走 | `service.name` | instrumentation scope |
|---|---|---|---|
| runtime 日志管道 | 进程能打开 `CELLHIVE_LOG_DIR` 卷轴时优先。workerd 打一行 `@cellhive-log:` JSON，cell-agent/runtime 再编成 OTLP。单行超过 60000 字节变成 `{"kind":"drop"}`，不进 OTLP | `user-runtime` 或 `do-runtime` | `cellhive.runtime` |
| user-runtime 直接 POST | 没有日志卷轴、但配了 `CELLHIVE_OTLP_ENDPOINT` | `cellhive-user-runtime` | `cellhive.user-runtime` |
| cell-agent Tail 拉取 | `CELLHIVE_OTLP_LOGS=tail\|all`，且该事件尚未由卷轴导出。`tail` 只导出有订阅的 worker；默认 `off` 不走这条 | `CELLHIVE_SERVICE_NAME`，默认 `cellhive-cell-agent`；另有 `service.version`、`service.instance.id`、`cellhive.node_id` | logger 名 `cellhive` |

时间：`TimeUnixNano` 取事件毫秒 × 1e6。卷轴和直接 POST 的 `ObservedTimeUnixNano` 与之一同写入；cell-agent 的 observed 是导出当时。有效 W3C `traceparent`（`00-` + 32 hex + `-` + 16 hex）写入 LogRecord 的 `trace_id` / `span_id`。cell-agent 另外复制字符串属性 `cellhive.trace_id`、`cellhive.span_id`；另外两条路不复制这两项。

严重级别：body 为字符串。`info` → 9，`debug` → 5，`warn`/`warning` → 13，`error` → 17。直接 POST 的 fetch 记录把文本写成大写 `INFO`/`ERROR`，数值仍是 9/17。

### 1. Queue / Cron / Workflow 完成记录

`CELLHIVE_INVOCATION_LOGS=on` 时，一次尝试**结束**才发一条。没有 running 记录，没有 console，没有 handler 返回值。失败只影响这条日志，不把业务成功改成 500。

| 字段 | 值 |
|---|---|
| body | `invocation` |
| `cellhive.invocation` | `true`（布尔） |
| `cellhive.invocation_id` | 本次尝试的 UUID |
| `cellhive.request_id` | 调度器传入的 id；没有则等于 `invocation_id` |
| `cellhive.namespace` / `cellhive.worker` | 租户 |
| `cellhive.event_type` | `queue`、`cron` 或 `workflow` |
| `cellhive.logical_id` | `{kind}:` + SHA-256 hex。queue 的输入是 `[namespace, worker, queue, 排序后的 message id]`；workflow 是 `[namespace, worker, workflow, instance id]`；cron 是 `[namespace, worker, token, scheduled_time_ms, cron]` |
| `cellhive.resource` | queue 名、cron 表达式或 workflow 名 |
| `cellhive.attempt` | queue 为本批消息 `attempts` 的最大值（至少 1）；cron/workflow 为 1 |
| `cellhive.outcome` | `success`、`retry` 或 `failed` |
| `cellhive.started_ms` / `cellhive.finished_ms` / `cellhive.duration_ms` | Unix 毫秒；duration 为两者之差 |
| `cellhive.retry_requested` | 布尔，outcome 为 `retry` 时 true |
| `cellhive.error` | 错误文本，截断到 2048 字符；没有则省略 |
| `cellhive.ack_count` / `cellhive.retry_count` | 本批 ack / retry 的消息个数。失败时两者都是 0 |
| `cellhive.producer_links` | 仅 queue，且至少有一条消息带生产者 `request_id` 或 `traceparent`。JSON 数组，元素为 `{message_id, request_id, traceparent}` |

severity：`failed` 为 error/17，其余为 info/9。

### 2. HTTP fetch 请求记录

同样受 `CELLHIVE_INVOCATION_LOGS` 控制，由 user-runtime loader 在请求结束后发出。这是 fetch 的调用摘要；Tail 路径不再为普通 fetch 另发一条 invocation，避免重复。Durable Object 例外，见第 3 节。

| 字段 | 值 |
|---|---|
| body | `invocation` |
| `cellhive.invocation` | `true` |
| `cellhive.event_type` | `fetch` |
| `cellhive.namespace` / `cellhive.worker` | 租户 |
| `cellhive.request_id` | 请求头 `x-request-id`；没有则现场生成 UUID |
| `cellhive.method` | HTTP 方法 |
| `cellhive.path` | `x-vwork-path-prefix`，否则 URL pathname |
| `cellhive.route` | 固定 `functions` |
| `cellhive.status` | HTTP 状态；没有响应时按 500 |
| `cellhive.outcome` | 状态 ≥ 500 为 `error`，否则 `ok` |
| `cellhive.started_ms` / `cellhive.finished_ms` / `cellhive.duration_ms` | Unix 毫秒，duration 不小于 0 |

没有 `invocation_id`、`logical_id`、`attempt`、`error` 文本。直接 POST 失败且没有卷轴时，loader 改把同一字段 POST 到 cell-agent `/v1/internal/request-log`，再由 cell-agent 按第 4 节的 Resource 导出；这一跳还受 `CELLHIVE_OTLP_LOGS` 控制。

### 3. Tail 事件

console 只存在于 Tail。有日志卷轴时，host 在写入内存环的同时把下面几条打进卷轴。cell-agent 拉取时若看到 `file_exported`，不再向 OTLP 发第二份。`CELLHIVE_OTLP_LOGS=off` 且没有卷轴转发时，这些记录不到 OTLP。平台生命周期 RPC（`CellHiveLifecycle`，以及无 entrypoint 的 `status`/`retire`）不导出。

每条 Tail 事件先有一条调用摘要，普通 fetch 跳过（已由第 2 节发出）。带 `durableObjectId` 的 fetch 仍发。

| 字段 | 值 |
|---|---|
| body | `invocation` |
| `cellhive.invocation` | `true` |
| `cellhive.event_type` | `fetch`（有 request）、`scheduled`（有 cron 或 `handleScheduled`）、`queue`（有 queue 或 `handleQueue`）、`workflow`（`handleRun`）；都没有则为空，该属性省略 |
| `cellhive.namespace` / `cellhive.worker` | 租户 |
| `cellhive.request_id` | Tail 事件上的 request id |
| `cellhive.outcome` | workerd 的 outcome，例如 `ok`、`exception` |
| `cellhive.script_version` | JSON 字符串，`{id, name, tags}` |
| `cellhive.resource` | Durable Object id；没有则省略 |
| `cellhive.method` / `cellhive.path` | 仅 fetch 的 request |
| `cellhive.status` | 仅当响应带 status |
| `cellhive.exceptions` | 异常数组的 JSON。元素含 `name`、`message`、`timestamp`，有 stack 时含 `stack` |
| `cellhive.started_ms` / `cellhive.finished_ms` | 事件上有开始时间才带 |

severity：outcome 为 `ok` 时 info，否则 error。这里的 `event_type` 用 `scheduled`，第 1 节的 Cron 完成记录用 `cron`。两边不是同一条日志。

然后每条 console 单独一条：

| 字段 | 值 |
|---|---|
| body | 各参数格式化后用空格拼接。字符串原样，bigint 加 `n`，其余 `JSON.stringify` |
| severity | console 级别：`log` 按 info，以及 `debug` / `warn` / `error` |
| `cellhive.message_args` | 参数数组的 JSON 字符串 |
| 其余 | 与上面这条调用摘要相同，但**没有** `cellhive.invocation` |

可信调度标记 `__cellhive_dispatch_v1__` 会从公开 logs 里拿掉，不作为 console 导出；Queue/Cron 的 `event.queue` / `event.cron` 留在 Tail 协议里，不进这条 OTLP 属性。

卷轴还会为每个异常再发一条：body 为 `{name}: {message}`，severity `error`，属性与调用摘要相同且不含 `cellhive.invocation`。cell-agent 自己的导出不发这第二条，异常只留在调用摘要的 `cellhive.exceptions` 里。

## 启动

```bash
# 本机单节点：必须提供持久根密钥。
CELLHIVE_ROOT_KEY="$(openssl rand -base64 32)" CELLHIVE_NODE_ID=node-1 \
  CELLHIVE_BUCKET_DIR=./.cellhive/bucket ./bin/cell-agent
```

Compose/K8s/Helm 启动前先运行 `python3 scripts/deploy-preflight.py compose|k8s|helm`（root key、profile 与实际部署一致）；执行步骤和 S3 初始化见 [`deployment.md`](deployment.md)。多 cell-agent 节点不可各用自己的 `/data/bucket`；必须共用经过条件写/删探针的外部 S3，并配置唯一 node ID、Pod 可达的 `CELLHIVE_PEER_URL`。do-runtime 可在同一 Service 后扩副本；Service 提供任意入口，owner 记录中的 Pod advertise 地址提供定向转发。无实时 K8s 控制面时只完成渲染，不能声称已集群验收。

启动顺序：权威桶已创建/探针通过 → `cell-agent` → gated `do-runtime` → `user-runtime` → 运维入口。缩容单节点时 PDB `minAvailable:1` 会阻止普通 eviction，先安排维护停机；不要直接删除权威 PVC。

## 诊断

```bash
# 对象存储探针：条件创建/CAS/reject-stale/ranged read + 条件删除（ADR-135）
bin/cellhive diagnose

# 查看节点 lease / owner（运维，允许 List）
curl -s -H 'x-cellhive-internal-token: <token>' 'http://cell-agent:7001/v1/internal/resolve?scope=demo/__kv__/main'

# 每域运维统计（ADR-157；metadata-only，读不放大）：尺寸/页/表/积压
bin/cellhive d1 info demo app --tables
bin/cellhive kv namespace stats demo KV --exact
bin/cellhive queue stats demo jobs
bin/cellhive r2 bucket stats demo media           # 启用后跟踪的估算用量，无 List
bin/cellhive r2 bucket stats demo media --scan --limit 1000  # 有界 List 诊断

# 向量索引 ANN（ADR-159）：默认 flat 精确扫描；重建为 IVFADC/OPQ 后查询快一个量级
bin/cellhive vectorize rebuild demo docs --buckets 1024 --quantizer opq --codesize 32 --nprobe 0.05
bin/cellhive vectorize stats   demo docs          # stats.ann 显示当前模型
bin/cellhive vectorize drop-ann demo docs         # 回到 flat
```

## 优雅关停（drain）

1. 向 `cell-agent` 发 SIGTERM；
2. 节点标 draining，抢 bucket `fleet/drain-token.json`；
3. 逐批迁移 cell：停新路由 → 等在途 → 发布快照 → 释放 owner → 请 peer 领取；
4. 确认复制/快照完成后退出；K8s `terminationGracePeriodSeconds ≥ 120`。

`do-runtime` SIGTERM：停新请求 → 等在途（≤8s）→ flush WAL → release owner → kill workerd。

## 接管（节点意外死亡）

- 节点 lease 过期（≤10s）→ 其他节点条件写抢 owner（epoch+1）；
- 若前任有 open node-log → 先 **recovery**（seal followers、上传保留段、标记 sealed）→ 再 restore；
- 监控接管日志/审计（`cellhive_takeover_total` 为计划指标，见 `docs/observability.md`）。

## 升级（滚动）

1. **reader-before-writer**：先上新版 reader（能读旧 LTX/owner 记录）；
2. 等旧 reader drain 完；
3. 再上新版 writer；
4. 回滚安全：**读写版本差 ≤1**（ADR-034）。

按 `docs/deployment.md` 逐服务滚动；`do-runtime` 弹性节点逐个替换（先等 `restoring=0`）。

## 桶故障/迁移

- 桶不可达：节点在 lease 过期后自 fence 退出（supervisor 重启）；
- 换桶：**不支持在线切换**；需停机迁移（复制对象后切换配置）。独立 backup 桶角色与归档写入路径尚未实现。

## 常见故障

| 症状 | 排查 |
|---|---|
| claim 持续 `owner_live` | 前任未过期；等 lease TTL 或确认前任进程 |
| `epoch_mismatch` 频发 | 节点时钟/续租异常，或 owner 漂移 |
| 输出门超时 | 检查 follower 可达、bucket 延迟、`pressured` |
| 冷激活慢 | 页未命中；检查 paging 与对象布局 |
| 写入延迟高 | 单节点应答在本地 spool，对象最多晚一个上传周期；检查 ensemble 数 |
| `list_calls_total > 0` | 代码回归：热路径不应 List |
| host 返回 404 | 未注册 host（`cellhive domain ls`）；内置域需 `CELLHIVE_BASE_DOMAIN` 且边缘有通配符 |
| `diagnose` 点名 `conditional delete (reject-stale)` | 桶**忽略 `If-Match`**（条件删除退化为无条件）；更换供应商/端点，owner 条件释放与 fence 依赖该语义 |
| 删除 worker 后重新部署又消失 | purge 作业竞态；ADR-135 后 purge 只对仍软删实体生效、deploy 取消作业（若仍复现按 bug 报） |

_最后更新：2026-09-25_

## Hyperdrive Proxy 健康状态

ProxySQL 模式下，cell-agent 每 5s 探测 `CELLHIVE_HYPERDRIVE_PROXY` 中的每个 scheme endpoint；任何 endpoint 不可达都会让 `/readyz` 返回 503，并让该协议绑定 fail closed。

查看当前快照（admin token）：

```bash
curl -H "x-cellhive-admin-token: $CELLHIVE_ADMIN_TOKEN" \
  http://127.0.0.1:8082/v1/control/hyperdrive/proxy
```

返回 `mode/checked/ready/reason/endpoints[]`，其中每个 endpoint 带 `scheme/endpoint/healthy`。日志在状态切换时分别记录 `hyperdrive proxy endpoint unreachable` / `recovered`，不会每个探测周期重复刷日志。

## 内存与 cell 恢复 metrics

cell-agent `/metrics` 现在额外暴露：

| 指标 | 含义 |
|---|---|
| `cellhive_cellstore_max_open_cells` | 当前 resident cell 上限；0 表示不限 |
| `cellhive_cellstore_idle_ttl_seconds` | 空闲 cell 的 TTL；0 表示只按上限驱逐 |
| `cellhive_cellstore_open_cells` | 当前打开的 cell 数 |
| `cellhive_cellstore_evicted_total` | 进程启动以来累计 eviction/forget 次数，不是当前数量 |
| `cellhive_restore_files_total` | 冷恢复文件数 |
| `cellhive_restore_image_bytes_total` | 累计恢复的 SQLite image 字节数 |
| `cellhive_restore_pages_total` | 累计恢复的 SQLite 页数 |
| `cellhive_restore_last_image_bytes` | 最近一次恢复的 image 大小 |
| `cellhive_restore_max_image_bytes` | 进程启动以来最大恢复 image 大小，用于定位 OOM 峰值 |
| `cellhive_go_heap_alloc_bytes` / `cellhive_go_heap_sys_bytes` | Go 堆分配/保留 |
| `cellhive_process_rss_bytes` | `/proc/self/statm` 的 RSS（Linux） |

恢复路径现在用 `Truncate` + `WriteAt` 直接写目标 SQLite 文件，不再额外分配 `commit*pageSize` 的整库缓冲；仍然会持有从对象存储读入的 segment 数据，因此 bucket 下载/compaction 的峰值需要继续单独观察。

恢复写入按最多 1MiB 的连续页 run 合并后一次 `WriteAt`，避免每页 4KiB 一次 syscall；run buffer 固定有界，不会回到整库缓冲。

## 可选 Redis/Kvrocks 协调权威（2026-11-24，已部署 SG / USW / GZ）

初始 GZ 发布方案采用 Chart 可选 `coordinationRedis.enabled` 单实例 StatefulSet，
持久 PVC、AOF `appendfsync always`、`noeviction`，不配置会回退版本的副本自动提升。
镜像须固定 digest，凭据由 `coordinationRedis.existingSecret` 的 `REDIS_PASSWORD`
提供；agent 单独读取同一 Secret 的 `CELLHIVE_COORDINATION_BACKEND` URL。
仅 agent 和离线迁移 Job 可访问 Redis。节点失联时不得强删 Redis Pod 或强挂卷；
须确认旧进程已隔离，不能以可用性为由放宽协调版本单调性。
切换时将 `userRuntime.autoscalingEnabled=false`，停止 runtime 和全部 agent，
备份协调元数据后执行初始化；激活后恢复副本数和 HPA。不要回滚到旧 COS 写入者。
GZ 首次切换及 outbox 修复见 [历史 GZ 部署记录](reports/2026-11-24-gz-redis-fleet.md)。三地区现已切到 Redis 与 recovery index v2，版本见下文。

默认 `CELLHIVE_COORDINATION_BACKEND=object`。Redis 模式配置：

```sh
export CELLHIVE_COORDINATION_BACKEND=redis://127.0.0.1:6379/0
```

所有 agent 使用相同 Redis 数据库与桶配置；协调和共享调度 namespace 从 bucket 配置自动生成。旧独立 URL/region/SCHEDULE_MODE 环境项必须删除；旧实验 region 的已激活绑定不会自动迁移。Redis 配置持久化、`maxmemory-policy noeviction`；不支持可能丢失已确认写入的透明副本提升，也不能从旧备份直接恢复并继续发授权。

首次切换须停掉全部 agent/runtime、旧二进制和后台写者，确保没有在途写入。然后使用相同桶配置执行 `cell-agent coordination-init --offline`。命令先在桶写后端绑定，复制 owner/owner-gen、节点、fleet 选主与 wake 记录，最后激活。中断后保持停写、使用原配置重跑；已激活时重跑会拒绝覆盖。普通启动不会初始化空 Redis，绑定后的新二进制也拒绝 object 模式。旧二进制不知道此绑定，必须由部署层禁止启动。

日常 owner、lease 和 wake 查询不回 COS；Lua CAS 不使用 COS append/root/ETag 模拟。普通 claim 把 owner-gen 高水位更新和 owner 发布并为一次 Lua；独占维护和资源清理路径使用单次 Lua 预留代次，再 CAS 发布，以保留未知结果回收语义；也不再执行 owner-gen 的读/CAS 循环。owner 释放不清除代次高水位和退役历史。节点按 node/session 统计全部 owner（含休眠）；resident 使用真实本机打开句柄，owned resident 是本机维护集合与打开句柄的交集，采样不是跨节点原子快照。CPU 为进程相邻采样间的核使用率（10000 表示一个核满载），RSS 为进程当前驻留内存；cgroup v2 内存余量覆盖整个容器。未支持的平台/无限制 cgroup 不伪造可用值，通过 valid 字段区分。

Redis URL 自动启用共享 ZSET 调度；object 模式使用 cluster 本地 deadline。Redis authority 下 owner/lease/wake 都走 Redis；业务消息、消费进度、Cron 记录和清理证明继续在 cell。保留低频对账，不能因为启用 ZSET 就关闭恢复发现。重复通知通过 revision 防止旧 ACK 吃掉新通知；不承诺外部副作用 exactly-once。

Redis generation 丢失：保持集群停写，不删除桶绑定来绕过保护，不重新执行初始化覆盖已激活状态；必须先隔离旧 session，再进行专门的协调重建与数据链恢复。本批不提供自动灾难重建/在线后端切换。自动均衡策略和恢复扫描完全消除不在本批实现范围；持久 outbox 已实现，见调度验收报告。

### Redis 待恢复 session 索引升级（已部署 SG / USW / GZ）

新版本使用 Redis pending-session 索引发现恢复候选，COS 保留 node-log 正文和条件写。
首次 `coordination-init --offline` 自动创建索引。已有 Redis 集群必须：

1. 停止全部业务入口、runtime、agent 和其他写者，备份现有协调状态及 node-log。
2. 用新版本运行 `cell-agent coordination-recovery-init --offline`，沿用相同 bucket/Redis 配置。
3. 命令一次性枚举 COS 历史 node-log，补齐非 sealed 候选，写入 ready marker，升级桶绑定到 v2。失败保持停写，修复后重试。
4. 仅启动新版本参与者，再验证 fleet 写入、恢复、重启及 KV/Queue/Alarm/Cron。

新程序拒绝 v1 或缺少索引 marker；旧 v1 程序拒绝 v2 绑定。不可回退旧二进制或跳过离线升级直接滚动。
普通恢复发现无 COS List；启动只点查拟使用的 session ID，实际恢复仍点查/CAS 正文。
索引登记后、COS 创建前崩溃可能留下额外候选；缺失正文不能直接删除候选，以免漏掉迟到创建。
当前 SG revision 150、USW revision 142、GZ revision 9 已执行此升级；见 [三地区发布记录](reports/2026-11-24-all-regions-redis-recovery-release.md)。本地常驻 Compose 未在此次生产发布中更新。


### 托管 Redis endpoint 迁移（2026-11-24，三地区已完成）

GZ 复用现有托管实例，SG / USW 新建 1 GiB Redis 7.0 标准主从并转为 1 个月包月。云实例使用 noeviction、主节点读写、同 VPC 子网安全组。当前托管自动切换可能丢失已确认协调写，部署接受这一限制；见 Redis authority spec 和 known-issues，不宣称托管故障切换 RPO=0。

切换必须停止 HPA、入口 runtime、DO runtime 和全部 agent；完整备份 Redis 所有数据库内的键并保留类型、二进制值和绝对过期时间。向空目标库导入，校验所有未过期键及 generation / revisions，再更新 coordination Secret endpoint。已有 bucket binding 保持原值，不运行 coordination-init。恢复服务后验证旧 KV、新 KV、Queue 重试、Cron、Alarm，保留原 Redis PVC；旧 Redis 不能重新成为在线写入端。发生问题只在目标尚未接受业务写时直接回退，目标已写入后须停写并重新进行完整迁移。

GZ / SG / USW 均已完成切换，Helm revision 分别为 10 / 151 / 143。旧 Redis StatefulSet/Service 已移除，PVC 保留；临时测试应用与迁移 Pod 已清理。SG / USW 包月实付合计 ¥77.33，自动续费已开启（2026-11-24 按用户要求设置并回读确认）、2026-12-24 到期。验收与回滚边界见 [托管 Redis 切换报告](reports/2026-11-24-managed-redis-cutover.md)。

### Redis 恢复权威 v3（本地实现，尚未部署）

v3 将 node-log 正文和 DO recovery pointer 一并迁至 Redis，替代上文 v2 的 COS 正文点查/CAS。部署前停止所有写者、备份 Redis 与历史桶元数据，再用新版本执行 `cell-agent coordination-recovery-init --offline`。命令将 binding 标为 v3/loading，复制所有 session（含 sealed 与 CoveredSeq）及 DO 指针，建立恢复索引，最后激活。旧 v1/v2 二进制拒绝 v3，新程序拒绝未升级的 v1/v2。中断后保持停写重试；active v3 重复执行只校验 ready，绝不从历史 COS 覆盖新 Redis 状态。Redis 故障无 COS 回退。

GC 默认 `CELLHIVE_EPOCH_GC_INTERVAL=1h`、`CELLHIVE_EPOCH_GC_INITIAL=1m`；每个 agent 一个 goroutine 顺序遍历本机 OwnedScopes。仍保留恢复链、安全复核、上一代与一小时对象年龄宽限期；一小时检查周期与一小时宽限期是不同参数。此改动不自动更新线上配置/镜像。


### 临时 trace 阶段日志

`CELLHIVE_TRACE_LOG_UNTIL` 为 RFC3339 绝对截止时间，必须在未来 24 小时内；未设置、过期或无效时关闭。启用后原本未采样的 Go span 只做 RecordOnly 并输出到 stdout 的 `trace_span` JSON，不改变传给下游的 sampled 标记，也不将这些额外 span 发给 ARMS。记录 trace/span/parent ID、阶段、起始时间、耗时及白名单 scope/object key 等属性；不记录请求正文、header、完整 URL 或错误消息。独立 4096 条非阻塞队列防止日志背压阻塞请求；队列满会丢弃诊断日志。自动到期后恢复原采样行为。入口 function_request 总耗时包含 Worker 准备，JS http.server span 从准备完成后开始；两者差值可识别尚未细分的 Worker 准备耗时。
