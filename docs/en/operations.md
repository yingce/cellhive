# Operations Runbook

## Startup

```bash
# Single local node; keep the generated root key durable and private.
CELLHIVE_ROOT_KEY="$(openssl rand -base64 32)" CELLHIVE_NODE_ID=node-1 \
  CELLHIVE_BUCKET_DIR=./.cellhive/bucket ./bin/cell-agent
```

Run `python3 scripts/deploy-preflight.py compose|k8s|helm` with the same root key/profile before deployment; see [`deployment.md`](deployment.md) for Compose, cluster rollout and S3 bucket initialization. Multiple cell-agent nodes cannot each use a private `/data/bucket`: they require the same S3 bucket with validated conditional writes/deletes, unique node IDs, and a Pod-reachable `CELLHIVE_PEER_URL`. A single DO Service target supports only one Pod until placement has stable per-instance addresses. Without a working Kubernetes API, rendering is not live-cluster validation.

Startup order: bucket provisioned and probed → cell-agent → gated do-runtime → user-runtime → operator edge. With one replica, a PDB `minAvailable:1` prevents voluntary eviction; schedule downtime for node maintenance and never casually delete the authoritative PVC.

## Diagnostics

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

## Graceful Shutdown (drain)

1. Send SIGTERM to `cell-agent`;
2. The node is marked draining and competes for the bucket `fleet/drain-token.json`;
3. Migrate cells in batches: stop new routing → wait for in-flight requests → publish snapshot → release owner → ask peers to claim;
4. Exit after confirming replication/snapshot completion; K8s `terminationGracePeriodSeconds ≥ 120`.

`do-runtime` SIGTERM: stop new requests → wait for in-flight requests (≤8s) → flush WAL → release owner → kill workerd.

## Takeover (Unexpected Node Death)

- Node lease expires (≤10s) → other nodes use conditional writes to claim owner (epoch+1);
- If the predecessor has an open node-log → perform **recovery** first (seal followers, upload retained segments, mark sealed) → then restore;
- Monitor takeover logs/audit (`cellhive_takeover_total` is a planned metric; see `docs/observability.md`).

## Upgrade (Rolling)

1. **reader-before-writer**: deploy the new reader first (able to read old LTX/owner records);
2. Wait for old readers to drain;
3. Then deploy the new writer;
4. Rollback safety: **read/write version skew ≤1** (ADR-034).

Roll each service according to `docs/deployment.md`; replace `do-runtime` elastic nodes one by one (wait for `restoring=0` first).

## Bucket Failure/Migration

- Bucket unreachable: the node self-fences and exits after the lease expires (supervisor restarts it);
- Changing buckets: **online switching is not supported**; downtime migration is required (copy objects, then switch configuration); the planned backup role is used for archiving.

## Common Issues

| Symptom | Troubleshooting |
|---|---|
| claim continuously returns `owner_live` | The predecessor has not expired; wait for the lease TTL or confirm the predecessor process |
| Frequent `epoch_mismatch` | Node clock/lease renewal anomaly, or owner drift |
| Output gate timeout | Check follower reachability, bucket latency, and `pressured` |
| Slow cold activation | Page miss; check paging and object layout |
| High write latency | Check whether it has degraded to single-node mode (waiting for bucket); check ensemble size |
| `list_calls_total > 0` | Code regression: hot paths should not List |
| host returns 404 | host is not registered (`cellhive domain ls`); built-in domains require `CELLHIVE_BASE_DOMAIN` and a wildcard at the edge |
| `diagnose` names `conditional delete (reject-stale)` | The bucket **ignores `If-Match`** (conditional delete degrades to unconditional); switch provider/endpoint, because owner conditional release and fencing depend on this semantic |
| Worker disappears again after being deleted and redeployed | purge job race; after ADR-135, purge only applies to entities that are still soft-deleted, and deploy cancels the job (if it still reproduces, report it as a bug) |

_Last updated: 2026-09-17_