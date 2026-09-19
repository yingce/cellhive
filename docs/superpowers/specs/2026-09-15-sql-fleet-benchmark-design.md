# SQL → WAL → LTX → Fleet Benchmark Design

## Goal

Add an honest end-to-end benchmark for the CellHive state path:

```text
SQLite UPSERT transaction
→ committed WAL frames
→ transaction-aware LTX payload
→ owner /v1/internal/commit
→ fleet peer replication
→ follower fsync
→ durability barrier
→ request completion
```

The reported TPS must represent durable SQL transactions, not synthetic LTX submissions. The first implementation uses the Go `cellstore` SQLite runtime; workerd DO integration remains a separate follow-up because no production supervisor/output-gate exists yet.

## Non-Goals

- Do not claim workerd DO end-to-end coverage.
- Do not implement SQLite restore/page application in this change.
- Do not implement checkpoint snapshot/link recovery; encountering a checkpoint during the measured window fails the run explicitly.
- Do not replace the current peer protocol or durability modes.

## SQL Workload

Each benchmark operation executes one atomic transaction:

```sql
BEGIN;
INSERT INTO kv(key, value, meta, updated_ms) VALUES (...)
ON CONFLICT(key) DO UPDATE SET ...;
UPDATE cell_meta
SET v = CAST(CAST(v AS INTEGER) + 1 AS TEXT)
WHERE k = 'txid';
SELECT v FROM cell_meta WHERE k = 'txid';
COMMIT;
```

`Cell.PutTx()` returns the txid committed by this transaction. Existing `Cell.Put()` delegates to `PutTx()` and discards the returned id, preserving its public behavior while fixing the current two-autocommit implementation.

Only benchmark `PutTx()` writes occur after the WAL cursor baseline is established. Therefore committed WAL transactions and txids are both contiguous and can be paired by order.

## WAL Transaction Grouping

Add:

```go
type Transaction struct {
    Frames []Frame
    DBSize uint32
}
```

`PollResult` gains `Transactions []Transaction`. A transaction ends at each frame whose `DBSize != 0`. `NewCommitted` remains populated as the flattened equivalent for compatibility.

The cursor advances only through the last complete commit. Incomplete trailing frames remain unread and are reconsidered on the next poll. Salt rotation or WAL truncation continues to set `Checkpoint=true`.

## LTX WAL Payload

Define a versioned payload codec in `internal/ltx`:

```text
magic          4 bytes  "WAL1"
page_size      u32 BE
tx_count       u32 BE
transaction[]:
  frame_count  u32 BE
  frame[]:
    page_no    u32 BE
    db_size    u32 BE
    page_data  page_size bytes
```

One capture poll emits one LTX segment covering all complete transactions returned by that poll:

- `StartTxID`: first covered SQL txid
- `EndTxID`: last covered SQL txid
- payload: transactions in WAL commit order

This preserves transaction boundaries and lets one fleet proof cover several SQL requests.

## Capture And Durability Barrier

Add `internal/sqlcapture` with three responsibilities:

1. A single WAL cursor polls after writer notification and on a 1ms fallback timer.
2. It converts all newly committed transactions in one poll into one LTX segment and posts it to the owner commit endpoint with the configured follower URL(s).
3. After fleet acknowledgement it advances a monotonic `durableTxID` to the segment's `EndTxID` and releases every waiter whose txid is covered.

Writers execute SQL concurrently. SQLite serializes write commits; after each `PutTx()` they register/wait on their txid and signal the capture loop. The capture loop assigns contiguous txids to WAL transactions starting from the baseline `Cell.TxID()`.

The barrier must support cancellation and must never advance on a failed fleet commit. A commit failure fails all waiters covered by that segment; it is reported rather than silently counted as success.

## Commit Client

Add a small internal client for the existing endpoint:

```text
POST /v1/internal/claim
POST /v1/internal/commit
```

The commit carries the real LTX segment and explicit follower URL(s). A successful operation requires HTTP 200 and response `mode="fleet"`. Bucket fallback is a benchmark failure because this test specifically measures fleet durability.

## Benchmark Command

Add `cmd/sqlbench` flags:

- `-owner`
- `-follower` (repeatable or comma-separated; first version requires at least one)
- `-scope`
- `-db`
- `-n`
- `-c`
- `-value-bytes`
- `-token`

Output JSON:

```json
{
  "mode": "sql-fleet",
  "n": 10000,
  "failures": 0,
  "sql_commit_p50_ms": 0,
  "sql_commit_p99_ms": 0,
  "gate_wait_p50_ms": 0,
  "gate_wait_p99_ms": 0,
  "end_to_end_p50_ms": 0,
  "end_to_end_p99_ms": 0,
  "tps": 0,
  "capture_batches": 0,
  "transactions_per_batch": 0,
  "frames_per_transaction": 0
}
```

`n` counts only operations that completed SQL and crossed the durability barrier. TPS is `n / wall-clock elapsed`.

## Error Handling

- SQL commit failure: fail only that operation.
- `wal.ErrNoWAL`: retry until the first committed WAL exists.
- WAL checkpoint/salt rotation during measurement: stop and fail the run explicitly.
- Malformed frame/LTX payload: stop and fail all outstanding waiters.
- Fleet response is non-200 or not `mode=fleet`: fail the covering batch and its waiters.
- Request cancellation: remove/release that waiter without advancing durability.

## Tests

Follow red-green-refactor in this order:

1. `cellstore`: `PutTx()` changes value and txid in one WAL commit; rollback changes neither.
2. `wal`: one poll groups multiple SQLite commits into distinct transactions and leaves an incomplete tail unconsumed.
3. `ltx`: WAL payload codec round-trip and rejects malformed/truncated data.
4. `sqlcapture`: barrier releases covered txids, handles out-of-order waiter registration, and does not advance on commit errors.
5. Integration: real SQLite UPSERTs produce real WAL transactions, real LTX payloads reach a real two-node fleet follower spool, txids are contiguous, and persisted payloads decode successfully.
6. Benchmark smoke: `n=100`, `c=4`, failures=0 before any performance run is reported.

## Success Criteria

- `go build ./...`, `go vet ./...`, and `go test ./...` pass.
- SQL benchmark reports failures=0 and response mode `sql-fleet`.
- Follower spool contains LTX payloads decoded from actual SQLite WAL pages.
- Returned txids are contiguous and match the final `Cell.TxID()`.
- Every successful benchmark operation waited for a covering follower fsync acknowledgement.
- Results are labelled “Go SQLite SQL→fleet benchmark,” never “workerd DO TPS.”

## Expected Interpretation

The resulting TPS includes SQLite writer serialization, WAL parsing/page copying, LTX encoding, owner HTTP handling, peer group commit/101 stream, follower fsync, and output-gate waiting. It excludes workerd/V8, supervisor actor-file discovery, and production checkpoint reconciliation. Those exclusions must appear beside benchmark results.
