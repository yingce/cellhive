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
