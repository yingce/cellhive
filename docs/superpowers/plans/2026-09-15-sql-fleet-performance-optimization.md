# SQL Fleet Performance Optimization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reach 10k durable Go SQLite SQL→fleet TPS with p99 below 100ms by eliminating full-WAL rereads and JSON/base64 commit overhead while bounding capture batches.

**Architecture:** `wal.Cursor` holds a WAL fd and reads only complete new frames with `ReadAt`. `sqlcapture.HTTPCommitter` sends raw LTX to a binary commit endpoint sharing the existing durability decision. Capture splits polls into ordered segments of at most 128 transactions or 1MiB payload.

**Tech Stack:** Go 1.27, stdlib `os`, `net/http`, existing modernc SQLite/WAL/LTX/peer packages.

## Global Constraints

- No new dependency.
- Preserve JSON `/v1/internal/commit` compatibility.
- Preserve RPO=0 and same-scope ordering.
- Checkpoint remains explicit failure until snapshot/link apply exists.
- Use TDD and run race tests.
- `/opt/cellhive` is not a Git repository; use verified checkpoints instead of commits.

---

### Task 1: Incremental WAL Cursor

**Files:**
- Modify: `internal/wal/wal.go`
- Modify: `internal/wal/wal_test.go`

**Interfaces:**
- Add: `func (c *Cursor) BytesRead() uint64`
- Add: `func (c *Cursor) Close() error`

- [ ] Write a failing test that creates 100 baseline transactions, polls, adds one transaction, polls again, and asserts the bytes-read delta is below four frame sizes rather than the full WAL size.
- [ ] Run `go test ./internal/wal -run TestCursorReadsOnlyNewWALTail -v`; expect missing `BytesRead`/full-read failure.
- [ ] Replace `os.ReadFile` inside `Cursor.Poll` with held-fd `Stat` + header `ReadAt` + exact new-tail `ReadAt`; reopen once after replacement/short-read.
- [ ] Preserve transaction grouping, incomplete-tail handling, salt/shrink checkpoint detection.
- [ ] Run `go test ./internal/wal -v` and `go test -race ./internal/wal`.

---

### Task 2: Binary LTX Commit Endpoint

**Files:**
- Modify: `internal/server/server.go`
- Modify: `internal/server/server_test.go`
- Modify: `internal/sqlcapture/client.go`
- Modify: `internal/sqlcapture/capture_test.go`

**Interfaces:**
- Add endpoint: `POST /v1/internal/commit_binary?scope=<scope>&epoch=<epoch>`
- Add request header: `X-Cellhive-Followers: <comma-separated URLs>`

- [ ] Write a failing server test that sends raw LTX and verifies a real follower spool receives it through fleet mode.
- [ ] Run `go test ./internal/server -run TestCommitBinaryFleet -v`; expect 404.
- [ ] Extract common commit processing from JSON handler; binary handler reads at most `maxSegmentBytes+1`, parses scope/epoch/followers, and uses the same proof path.
- [ ] Write a failing HTTPCommitter test asserting `application/octet-stream`, raw body equality, and binary endpoint path/query/header.
- [ ] Switch `HTTPCommitter.Commit` to binary endpoint without fallback.
- [ ] Run server/sqlcapture unit and race tests.

---

### Task 3: Bounded Capture Segments And Metrics

**Files:**
- Modify: `internal/sqlcapture/capture.go`
- Modify: `internal/sqlcapture/capture_test.go`
- Modify: `cmd/sqlbench/main.go`

**Interfaces:**
- Segment limits: 128 transactions; 1MiB WAL1 payload.
- Add stats: `WALBytesRead`, `LTXBinaryBytes`, `CaptureBatchDurations` summarized by sqlbench.

- [ ] Write a failing capture test where 300 transactions are captured in exactly 128/128/44 ordered segments.
- [ ] Run the focused test; expect one unbounded segment.
- [ ] Implement deterministic transaction chunking by count and estimated WAL1 size; allow an oversized single transaction alone.
- [ ] Advance the barrier after each segment's fleet acknowledgement.
- [ ] Expose cursor bytes-read and capture duration metrics; add JSON fields to sqlbench.
- [ ] Run `go test ./internal/sqlcapture ./cmd/sqlbench` and race tests.

---

### Task 4: Benchmark And Documentation

**Files:**
- Modify: `docs/archive/p0-report.md`
- Modify: `docs/known-issues.md`
- Modify: `docs/decisions.md`
- Modify: `docs/protocol-formats.md`

- [ ] Run two real cell-agent processes and a 100 transaction/c=4 smoke; require failures=0 and final_txid=100.
- [ ] Run fresh DB/scope at c=32/64/128/256, recording TPS, SQL/gate/e2e p50/p99, tx/batch, WAL bytes/tx, and binary bytes.
- [ ] Compare against ADR-043 baseline and check ≥10k TPS with p99<100ms.
- [ ] If the target is unmet, report measured remaining bottleneck without weakening durability.
- [ ] Update ADR-044 and reports with exact evidence and the chosen design.
- [ ] Run `test -z "$(gofmt -l .)"`, `go build ./...`, `go vet ./...`, `go test ./...`, and race tests for wal/sqlcapture/server/peer/cellstore/ltx.
