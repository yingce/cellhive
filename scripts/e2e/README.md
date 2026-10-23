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
