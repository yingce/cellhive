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
| 调用结果与 console | `GET /v1/control/invocations?namespace=acme&worker=web&kind=queue&from_ms=...&to_ms=...&invocation_id=...&limit=100&cursor=...` |

预览 `messages[].body` 为 base64；`content_type` 指明解码方式，`timestamp_ms/attempts` 为 DLQ 当前记录，`source_created_ms/source_attempts/source_queue/source_id` 保留来源。预览不领取、不加 attempts、不改变投递状态。重复 ack 安全，但不能取消已开始执行的业务代码。分页为活数据 keyset，不是全队列快照。

清理状态为 `pending/running/failed/complete`，含 `job_id/data_scope/attempts/last_error/next_retry_ms`；后台每 2 秒检查、失败指数退避最多自动 10 次，手动 retry 重置失败预算。每 pass 最多删除 500 replica 对象；大资源继续 pending，不消耗失败预算。删除完成前不允许重建；完成后重新创建才激活新 incarnation。删除主 Queue 不删除独立 DLQ，purge 仍只清消息。旧无来源 DLQ、共享 scope 或 bucket 失败会明确报告，不能当作清理完成。

调用记录 `invocation_id` 标识一次执行；`logical_id` 关联同 batch/slot，`attempt`、`status=success/failed/retry/running` 与 `retry_requested/no_retry` 区分结果。异常包含 error/stack，console 在 `logs` 中，截断看 `logs_truncated`。请求完成前先持久化结果；持久化失败不静默回成功。保留 7 天或每 namespace 最近 10,000 次（写入时有界回收），运行中异常退出保留 running 供定位；此 API 为完成记录查询，实时 tail 继续使用原日志接口。调用记录由节点级 `CELLHIVE_INVOCATION_LOGS`（默认 on）全局控制（ADR-239），不再有 per-worker 策略。

Queue 部分重试后批次可能重组：使用 `messages[].logical_id/attempt` 与 `parent_invocation_ids` 追踪消息，不应把当前成员哈希当作全链路重试身份。查询可附加 `message_id` 或 `message_logical_id`。返回值在 `result`（`result_encoding=cellhive-tagged-json`，最多 8 KiB；`result_truncated` 或 `unsupported` 显式提示），`ack/retry` 保留逐消息结果及 retry delay。

清理完成后保留 retirement GC 义务，后续有界轮转清除迟到的旧 epoch 上传；namespace purge 也保留小型 fence/owner-generation 元数据。删除/重建的本地 incarnation 标记独立于 owner，离线节点不能复用旧 SQLite。跨 Queue 转移带源 incarnation，并在目的地事务内验证，防止迟到 DLQ 转移恢复旧数据。

验收：`scripts/runtime-baseline-e2e.sh`（真实 Docker 部署）与 `TestQueueCronInvocationLogsAreDurableAndIsolated`（真实 pinned workerd 并发/异常/后台日志）。

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
| 写入延迟高 | 是否降级到单节点（等 bucket）；检查 ensemble 数 |
| `list_calls_total > 0` | 代码回归：热路径不应 List |
| host 返回 404 | 未注册 host（`cellhive domain ls`）；内置域需 `CELLHIVE_BASE_DOMAIN` 且边缘有通配符 |
| `diagnose` 点名 `conditional delete (reject-stale)` | 桶**忽略 `If-Match`**（条件删除退化为无条件）；更换供应商/端点，owner 条件释放与 fence 依赖该语义 |
| 删除 worker 后重新部署又消失 | purge 作业竞态；ADR-135 后 purge 只对仍软删实体生效、deploy 取消作业（若仍复现按 bug 报） |

_最后更新：2026-09-25_
