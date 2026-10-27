# E2E scripts

In-repo e2e for the runtime paths that previously lived in `/tmp` ad-hoc scripts.

## Requirements

- `make build` (uses `bin/cell-agent`, `bin/user-runtime`, `bin/cellhive`)
- `curl`, `jq`, `node`, `openssl`
- `cli/node_modules/.bin/workerd` (or set `CELLHIVE_WORKERD`)
- free ports starting at `E2E_PORT_BASE` (default `19100`):
  - 19100 cell-agent
  - 19101 admin
  - 19102 user-runtime public
  - 19103 user-runtime internal

## Run

```bash
make e2e-bindings   # KV + D1 + R2 + R2 metadata
make e2e-workflow   # sleep / waitForEvent timeout / durable retry / workflow R2 metadata
make e2e-queue      # queue R2 metadata / retry / DLQ
make e2e-queue-multinode  # two cell-agents: owner kill + lease failover
make e2e-queue-concurrency # 100 messages, 4 batches in flight, D1 dedupe
make e2e-service-headers  # service binding JSON fallback preserves request/response headers
make e2e-mysql       # Docker MySQL + ProxySQL; auto-create via CellHive admin
make e2e            # workflow/queue suite (not MySQL; needs Docker + npm)
```

Each script uses a fresh temp dir, starts its own cell-agent + user-runtime, and tears them down.
Set `E2E_KEEP=1` to keep the temp dir/logs on exit; set `E2E_PORT_BASE` to avoid port conflicts.

`e2e-mysql` first builds `scripts/e2e/mysql/worker.bundle.js` with npm/esbuild
(pinned mysql2/esbuild), starts MySQL + ProxySQL, creates a Hyperdrive resource,
and verifies CellHive auto-creates the ProxySQL backend/user. It is intentionally
outside `make e2e` because it needs Docker and network access on first build.

`e2e-queue-concurrency` uses `E2E_QUEUE_MESSAGES` (default 100) and `E2E_QUEUE_CONCURRENCY`
(default 4). Messages are sent sequentially; concurrent `Promise.all(QUEUE.send(...))` is not used.

These are runtime tests. Postgres/MySQL/ProxySQL/Hyperdrive matrices remain manual.

ADR-292 跨模块修复验收：`bash scripts/e2e/review-fixes.sh`。启动独立 agent +
user-runtime，验证 Queue 并发上限、R2 不同/相同内容覆盖后的 HEAD/GET/range，
以及 20,000 行 D1 的完整结果与五次真实 HTTP 时延。结果是同机文件桶验收，
不能外推为生产 COS 性能。

### HK COS review acceptance

`python3 scripts/e2e/hk-review.py --credentials /opt/.secrets/s3` runs the review,
Queue and two-agent failover suites against the fixed dedicated
`vwork-e2e-hk-1376795072` COS bucket. It uses locally built binaries and the pinned
fork at `/opt/cellhive/deploy/workerd-fork/workerd`, unique namespaces/node IDs,
`durability=bucket`, and removes each test app before stopping its processes.
Logs are retained under the printed `/tmp/hk-review-*` directory. Credentials
are parsed without shell execution and never logged. Do not interpret these
local-compute/remote-storage measurements as HK same-region or peer-fsync performance.

For cold KV recovery, run `cold-write-cos.sh` with `CELLHIVE_E2E_BUCKET=cos`,
`CELLHIVE_COS_ENV_FILE=/opt/.secrets/s3`, and `BIN` pointing to the same build.
Run sequentially with the above suite because the dedicated bucket has shared
platform control cells. The cold harness waits for readiness/admin startup and
checks the target scope's owner epoch; global epoch/List counters include other
background scopes and cannot alone establish hot-path activity.

### HK COS performance

```bash
python3 scripts/e2e/hk-review.py --credentials /opt/.secrets/s3 --suite performance --port-base 28200
```

Build first (`make build`). This suite uses a unique namespace and hostname and
runs one local agent plus user-runtime against the dedicated HK test bucket,
with `durability=bucket`. It compares direct KV HTTP with Worker binding HTTP:
1 KiB values, 32 keys, concurrency 1/8/32, two batches each, persistent HTTP
connections, complete-response latency and GET body validation. Prints JSONL
with p50/p95/p99/max, successful RPS and failures. Sequential PUT samples are small
(60 per batch), so their p99 is a sample maximum rather than a stable tail estimate.
GET uses 5,000 requests at concurrency 1 and 10,000 at concurrency 8/32 per batch.
It is a closed-loop single-cell test, not an overload or production capacity test.

Optionally set `WRANGLER_CLI_PATH` to a locally installed Wrangler cli.js to run
three real versions-upload/deploy timing pairs and verify active-version
transitions against the same HK-backed control plane. CLI timing includes Node
startup and bundling. Do not run other HK suites concurrently (shared control cell).

双 agent 对比：将 `--suite performance` 替换为 `--suite performance-dual`。
该模式使用 `durability=auto`，启动独立目录的两个 agent，先验证 KV owner，
实际观察到 fleet 确认后才压测。增加 `forwarded` 路径，并在每个阶段前后抓取
两节点提交模式计数，输出 `commit_delta`。初次写入与发现预热单列，不能混入
稳态数据。两个 agent 在同机，不能替代跨主机延迟或故障域验收。PUT 批次较短，
不代表长时间写压下 COS 上传队列不会回压。

### HK COS upload batching recovery

`python3 scripts/e2e/hk-review.py --credentials /opt/.secrets/s3 --suite batch-recovery --port-base 28600`
uses two local agents and real HK COS. It waits for fleet acknowledgement,
writes 100 unique values, kills the owner, checks every value on the survivor,
then drains the survivor and removes its test SQLite cache to verify cold COS
restore. Upload counters before/after the write phase distinguish batches from
logical segments. All HK harness suites share an exclusive local lock because
the bucket's platform control cells are shared even across unique namespaces.

Add `--trace-endpoint http://127.0.0.1:28480` to the batch-recovery suite for
full OTLP sampling. The first post-restart cold GET prints `cold_get_trace`;
background timer-triggered hydration can have a separate trace, correlated by
`cell.scope` and `cell.wait_existing_open`. Restore spans report downloaded
object count and bytes separately from decoded/duplicate logical segments.

### Real COS storage contract gate (no MinIO)

```bash
python3 scripts/e2e/cos-contract.py --credentials /opt/.secrets/s3
```

Uses only the dedicated HK COS test bucket and requires all six selected tests
to pass (no skipped/missing cases): native append, production provider authority
and immutable writes, conditional deletion, ordinary/range/presigned HTTP reads
and cursor paging, snapshot/compaction/full restore, and 64MiB paged restore.
Credentials stay in the subprocess environment. Test prefixes are unique and
cleaned by the tests; the runner shares the HK harness lock.

COS raw S3 PutObject conditionals are not the production authority contract:
`TestS3BucketIntegration` rejects a provider that ignores duplicate-create
conditions. COS deployments use the native-append provider adapter, tested by
`TestProviderCloudDiagnose`; do not claim raw COS is fully S3-conditional compliant.

For repeated native-append contention with safe response metadata:

```bash
python3 scripts/e2e/cos-contract.py --credentials /opt/.secrets/s3 --native-repeat 300 --append-diag
```

This selects only the concurrent-create/CAS/delete contract and requires all N
iterations to pass. Diagnostics record HTTP response status/length/type/request
ID and timing, never signed headers or bodies. The native probe retains unique
`itest/` keys as evidence; it never removes a shared prefix.
