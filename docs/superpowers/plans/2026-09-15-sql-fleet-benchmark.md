# SQL Fleet Benchmark Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build an honest Go SQLite UPSERT → WAL transaction capture → LTX → fleet follower fsync → durability-barrier benchmark.

**Architecture:** `Cell.PutTx` makes the data mutation and txid increment one SQLite transaction. `wal.Cursor` exposes transaction boundaries; `ltx` encodes real WAL pages. A `sqlcapture.Capture` batches committed WAL transactions into LTX segments, submits them to the existing fleet endpoint, and advances a monotonic durability barrier used by `cmd/sqlbench`.

**Tech Stack:** Go 1.27, `modernc.org/sqlite`, stdlib HTTP/testing, existing CellHive WAL/LTX/peer code.

## Global Constraints

- Result label must be “Go SQLite SQL→fleet benchmark,” never workerd DO TPS.
- No new dependency.
- Checkpoint/salt rotation during measurement fails explicitly.
- Every successful SQL operation waits for a covering follower fsync acknowledgement.
- `/opt/cellhive` is not a git repository; replace commit steps with verified checkpoints.

---

### Task 1: Atomic SQL Transaction And Txid

**Files:**
- Modify: `internal/cellstore/cellstore.go`
- Modify: `internal/cellstore/cellstore_test.go`

**Interfaces:**
- Produces: `func (c *Cell) PutTx(ctx context.Context, key string, value, meta []byte) (uint64, error)`
- Preserves: `func (c *Cell) Put(ctx context.Context, key string, value, meta []byte) error`, delegating to `PutTx`.

- [ ] **Step 1: Write failing atomicity test**

Create a test that records `TxID`, calls `PutTx`, verifies returned/final txid increment exactly once, and uses `wal.Read` to verify one additional commit marker, not two.

- [ ] **Step 2: Verify RED**

Run: `go test ./internal/cellstore -run TestPutTxAtomic -v`

Expected: compile failure because `PutTx` does not exist.

- [ ] **Step 3: Implement minimal transaction**

Use `BeginTx`, UPSERT, metadata txid UPDATE, SELECT within the same `*sql.Tx`, then `Commit`. Roll back on every error.

- [ ] **Step 4: Make Put delegate**

```go
func (c *Cell) Put(ctx context.Context, key string, value, meta []byte) error {
    _, err := c.PutTx(ctx, key, value, meta)
    return err
}
```

- [ ] **Step 5: Verify GREEN**

Run: `go test ./internal/cellstore -v`

---

### Task 2: Transaction-Aware WAL Polling

**Files:**
- Modify: `internal/wal/wal.go`
- Modify: `internal/wal/wal_test.go`

**Interfaces:**
- Produces:

```go
type Transaction struct {
    Frames []Frame
    DBSize uint32
}

type PollResult struct {
    Header Header
    NewCommitted []Frame
    Transactions []Transaction
    Checkpoint bool
}
```

- [ ] **Step 1: Write failing grouping test**

Perform three real SQLite commits, poll once, and assert three `Transactions`; each ends in a frame with `DBSize != 0`; flattening equals `NewCommitted`.

- [ ] **Step 2: Verify RED**

Run: `go test ./internal/wal -run TestCursorGroupsTransactions -v`

Expected: compile failure because `Transactions` does not exist.

- [ ] **Step 3: Implement grouping**

Split newly committed frames at each commit frame. Do not advance past an incomplete transaction. Preserve existing `NewCommitted` behavior.

- [ ] **Step 4: Verify checkpoint behavior**

Run: `go test ./internal/wal -v`

Expected: all grouping and existing checkpoint tests pass.

---

### Task 3: WAL Transaction LTX Payload Codec

**Files:**
- Create: `internal/ltx/wal_payload.go`
- Create: `internal/ltx/wal_payload_test.go`

**Interfaces:**
- Produces:

```go
type WALTransaction struct {
    Frames []WALFrame
}
type WALFrame struct {
    PageNo uint32
    DBSize uint32
    Data []byte
}
func EncodeWALPayload(pageSize uint32, txs []WALTransaction) ([]byte, error)
func DecodeWALPayload(payload []byte) (uint32, []WALTransaction, error)
```

- [ ] **Step 1: Write failing round-trip tests**

Test one and multiple transactions, page sizes, transaction boundaries, malformed magic, truncated frame, and inconsistent page length.

- [ ] **Step 2: Verify RED**

Run: `go test ./internal/ltx -run WALPayload -v`

Expected: compile failure because codec types/functions do not exist.

- [ ] **Step 3: Implement exact WAL1 format**

Encode/decode the format in the approved design spec using big-endian integers and strict length validation.

- [ ] **Step 4: Verify GREEN**

Run: `go test ./internal/ltx -v`

---

### Task 4: Capture Loop, Fleet Client, And Durability Barrier

**Files:**
- Create: `internal/sqlcapture/capture.go`
- Create: `internal/sqlcapture/capture_test.go`

**Interfaces:**
- Consumes: `cellstore.Cell.WALPath`, `wal.Cursor.Poll`, `ltx.EncodeWALPayload`, existing `/v1/internal/commit`.
- Produces:

```go
type Committer interface {
    Commit(ctx context.Context, scope cell.Scope, epoch uint64, segment []byte) error
}
func New(cell *cellstore.Cell, scope cell.Scope, epoch, baselineTxID uint64, committer Committer) *Capture
func (c *Capture) Start(ctx context.Context)
func (c *Capture) Notify()
func (c *Capture) Wait(ctx context.Context, txid uint64) error
func (c *Capture) Stats() Stats
```

- [ ] **Step 1: Write failing barrier test**

Register waiters out of order; acknowledge a covering range; assert only covered txids release. Commit error releases covered waiters with error and does not advance durable txid.

- [ ] **Step 2: Verify RED**

Run: `go test ./internal/sqlcapture -run Barrier -v`

Expected: package/functions absent.

- [ ] **Step 3: Implement barrier and capture**

Poll after `Notify` and every 1ms. Pair transactions with consecutive txids, encode one LTX per poll, call `Committer`, then resolve waiters through `EndTxID`.

- [ ] **Step 4: Add real HTTP fleet committer**

Claim the scope once, POST base64 LTX to `/v1/internal/commit`, require HTTP 200 and response `mode="fleet"`.

- [ ] **Step 5: Test checkpoint/error paths**

Run: `go test ./internal/sqlcapture -v`

---

### Task 5: SQL Benchmark And Two-Node Verification

**Files:**
- Create: `cmd/sqlbench/main.go`
- Modify: `Makefile`
- Modify: `docs/archive/p0-report.md`
- Modify: `docs/known-issues.md`

**Interfaces:**
- Consumes: `Cell.PutTx`, `sqlcapture.Capture` and HTTP fleet committer.
- Produces: JSON metrics specified by the design document.

- [ ] **Step 1: Add smoke test harness**

Run 100 UPSERTs at concurrency 4 against two real `cell-agent` processes; assert failures=0, final txid=100, follower-held LTX payload decodes, and result mode is `sql-fleet`.

- [ ] **Step 2: Verify smoke run**

Run: `./bin/sqlbench -owner http://127.0.0.1:8101 -follower http://127.0.0.1:8102 -scope sql_smoke/__kv__/x -db /tmp/cellhive-sqlbench/smoke.sqlite -n 100 -c 4`

Expected: failures=0 and all stage metrics nonzero.

- [ ] **Step 3: Run concurrency sweep**

Run fresh scope/database per concurrency at c=1,8,32,64,128. Record SQL commit, gate wait, end-to-end p50/p99, TPS, transactions/batch, frames/transaction.

- [ ] **Step 4: Update documentation honestly**

Label results “Go SQLite SQL→fleet benchmark”; list exclusions: workerd/V8, actor-file discovery, production checkpoint reconciliation.

- [ ] **Step 5: Final verification checkpoint**

Run:

```bash
gofmt -l .
go build ./...
go vet ./...
go test ./...
go test -race ./internal/sqlcapture ./internal/wal ./internal/cellstore ./internal/ltx
```

Expected: no formatting output and all commands pass.
