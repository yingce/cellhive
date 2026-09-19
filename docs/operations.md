# 运维 Runbook

## 启动

```bash
# 单节点（本地）
CELLHIVE_NODE_ID=node-1 CELLHIVE_BUCKET_DIR=./.cellhive/bucket ./bin/cell-agent

# 多节点（生产）：每节点独立 node 名 + 同一 bucket
CELLHIVE_NODE_ID=cell-agent-a CELLHIVE_ADVERTISE=10.0.0.11:7001 \
CELLHIVE_BUCKET_DIR=/data/bucket ./bin/cell-agent
```

启动顺序：`cell-agent`（≥2）→ `do-runtime` → `user-runtime` → 边缘代理（运维自备，平台不下发配置，ADR-132）。

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
bin/cellhive r2 bucket stats demo media --limit 1000

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
- 换桶：**不支持在线切换**；需停机迁移（复制对象后切换配置）；计划中的 backup 角色用于归档。

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

_最后更新：2026-09-17_
