# Public DO WebSocket Passthrough Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make an external WebSocket upgrade traverse `user-runtime :8081`, a tenant Worker, and its Durable Object binding without exposing any platform transport in tenant env.

**Architecture:** Preserve WebSocket fetch semantics at both workerLoader RPC boundaries. The public loader calls `CellHiveHost.fetch(Request)` directly; the existing tenant DO facade continues to call the trusted `DurableObjectNamespace.fetch(Request)`, which performs owner lookup and connects to the owning do-runtime with a short-lived ticket.

**Tech Stack:** Go 1.27 tests, stock workerd `2026-09-16`, JavaScript WorkerEntrypoint/workerLoader RPC, Bun WebSocket client, Docker Compose gated do-runtime.

## Global Constraints

- Do not restore `CH_DO_CONNECT`, `CELLHIVE_CAP_WS`, or any tenant-visible platform key.
- `CH_*`, `CELL_*`, `__cellhive*`, and historical platform names remain user-owned.
- Do not fork workerd, add a JS engine, gateway, or network side channel.
- Owner lookup, shard tickets, internal URLs, and tokens remain in trusted platform workers.
- Any skipped WebSocket or DO test is a failure for this change.

---

### Task 1: Public loader WebSocket regression and minimal fix

**Files:**
- Modify: `internal/userruntime/userruntime_test.go`
- Modify: `workerd/user-runtime/loader.js:179`

**Interfaces:**
- Consumes: `CellHiveHost.fetch(Request) -> Promise<Response>` and the existing `DurableObjectNamespace.fetch(Request)` transport.
- Produces: `TestTenantDoWebSocketPassesThroughPublicLoader`, a real-workerd regression covering the public 101 and duplex frames.

- [ ] **Step 1: Write the failing test**

Add `TestTenantDoWebSocketPassesThroughPublicLoader`. Start the real user-runtime as in `TestTenantDoWebSocketCrossesRpc`, serve a tenant bundle whose default `fetch(req, env)` returns `env.ROOM.get("obj1").fetch(req)`, and use the existing stub `/v1/do/connect` owner. Open a raw TCP connection to the public port, send an RFC 6455 upgrade with `Host: ws-public.test`, require `HTTP/1.1 101`, read `owner-hello`, send one masked text frame `from-public`, and require `owner-echo:from-public`.

- [ ] **Step 2: Run the test against pinned workerd and verify RED**

Run:

```bash
CELLHIVE_WORKERD=/path/to/workerd-2026-09-16 go test ./internal/userruntime \
  -run '^TestTenantDoWebSocketPassesThroughPublicLoader$' -count=1 -v
```

Expected: FAIL because the public response is 500 and contains `DataCloneError` / does not contain `101 Switching Protocols`.

- [ ] **Step 3: Make the minimal production change**

Change the public fetch call only:

```js
const res = await stub.getEntrypoint("CellHiveHost").fetch(traced);
```

Keep `handleFetch` for existing internal non-WebSocket call sites.

- [ ] **Step 4: Verify GREEN and adjacent user-runtime behavior**

Run the new test and `TestTenantDoWebSocketCrossesRpc`, both with pinned workerd. Then run `go test ./internal/userruntime -count=1` and `make js-test`. Require zero skip markers in the focused logs.

- [ ] **Step 5: Commit the regression and fix**

```bash
git add internal/userruntime/userruntime_test.go workerd/user-runtime/loader.js
git commit -m "fix(runtime): preserve public DO websocket upgrades"
```

---

### Task 2: Permanent Docker public-WebSocket acceptance

**Files:**
- Modify: `scripts/runtime-baseline-e2e.sh`

**Interfaces:**
- Consumes: the runtime-baseline Compose project and the existing TypeScript `Counter` DO.
- Produces: a host-accessible ephemeral public port and a Bun client assertion for direct external WebSocket traffic.

- [ ] **Step 1: Extend the isolated acceptance app**

Add WebSocket handling to `Counter.fetch(request)`: accept the server side with `ctx.acceptWebSocket`; in `webSocketMessage`, run the same SQL increment and send `{"echo":<message>,"count":<n>}`. Add `/ws` to the default Worker and return `env.COUNTER.getByName("counter-1").fetch(req)`.

- [ ] **Step 2: Publish an ephemeral loopback port**

In the generated Compose override publish `user-runtime` as `127.0.0.1::8081`, then resolve it with `compose port user-runtime 8081`. Do not use a fixed host port.

- [ ] **Step 3: Add the real client assertion**

Run a host Bun WebSocket client with `Host: e2e-api.cell.test`. Before restart, send `before-restart` and require `{"echo":"before-restart","count":2}`. After restarting and waiting for gated do-runtime readiness, reconnect, send `after-restart`, and require count `3`; then require the existing HTTP increment to return count `4`.

- [ ] **Step 4: Run isolated Docker acceptance**

Run `bash scripts/runtime-baseline-e2e.sh`. Expected final line: `RUNTIME-BASELINE-E2E: PASS`; verify no project containers, networks, or volumes remain.

- [ ] **Step 5: Commit Docker acceptance**

```bash
git add scripts/runtime-baseline-e2e.sh
git commit -m "test(runtime): cover public DO websocket in Docker"
```

---

### Task 3: Correct design/status documentation and run final gates

**Files:**
- Modify: `docs/decisions.md`
- Modify: `docs/en/decisions.md`
- Modify: `docs/known-issues.md`
- Modify: `docs/en/known-issues.md`
- Modify: `docs/testing.md`
- Modify: `docs/en/testing.md`
- Modify: `docs/release-notes.md`
- Modify: `docs/en/release-notes.md`

**Interfaces:**
- Consumes: the named automated regression and Docker acceptance from Tasks 1-2.
- Produces: current bilingual documentation that distinguishes internal DO WebSocket transport from public 101 passthrough.

- [ ] **Step 1: Update ADR-184 and known-issues**

Record that both workerLoader boundaries must be fetch-shaped: public loader → `CellHiveHost.fetch(Request)` and tenant DO facade → `DurableObjectNamespace.fetch(Request)`. State explicitly that no `CH_DO_CONNECT` or tenant transport key is used.

- [ ] **Step 2: Update testing and release notes**

Name `TestTenantDoWebSocketPassesThroughPublicLoader`, its public 101/frame assertions, and the Docker `examples/do-websocket`/Counter acceptance. Mirror the factual changes in `docs/en/`.

- [ ] **Step 3: Run focused compatibility gates**

With pinned workerd, run `TestDOCompatSuite` and require 19/19 PASS with zero skips. Run the two user-runtime WebSocket tests and require both PASS with zero skips.

- [ ] **Step 4: Run the full project gate**

Run:

```bash
REQUIRE_ALL=1 bash scripts/ci.sh
```

Expected: all 14 phases `ok`, `RUNTIME-BASELINE-E2E: PASS`, and final `GATE: PASS`. Restore only the known generated timestamp in `docs/archive/bench/rpo-zero-fault.txt` to its committed value.

- [ ] **Step 5: Verify repository and resource cleanliness**

Run `git diff --check`, `git status --short --branch`, and query Docker for runtime-baseline project containers/volumes. Require no generated or test resource residue.

- [ ] **Step 6: Commit documentation**

```bash
git add docs/decisions.md docs/en/decisions.md docs/known-issues.md docs/en/known-issues.md \
  docs/testing.md docs/en/testing.md docs/release-notes.md docs/en/release-notes.md
git commit -m "docs: record public DO websocket passthrough"
```
