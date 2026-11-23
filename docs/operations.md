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
| Fold、epoch GC、复制上传 | fold 每秒检查本机候选，epoch GC 默认首次 1 分钟后、之后每 5 分钟；上传刷新默认 30 秒 | 按 scope 和条件做归并、回收或上传；常规 compaction 由事件唤醒。 |
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
