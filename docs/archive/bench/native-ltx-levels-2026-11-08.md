# Native LTX codec and level acceptance, 2026-11-08

This is partial migration evidence for ADR-312, not full production/fleet
acceptance. No deployment was performed.

## Reference and codec

Reference: `/opt/celld`, HEAD
`90b43017241f81189453d326d05948f388b34652`.

Go-generated native snapshots and deltas were read by the actual Rust
`celld_ltx::ltx::decode_file` and `decode_file_pages`. Reverse Rust fixtures
are checked into `internal/ltx/testdata`. A separate comparison fed identical
inputs to Go `CompactNative` and celld `Compactor`: headers and decoded page
contents matched, including repeated-page replacement and database truncation.
This verifies semantic equivalence, not identical compressed byte choices.

The shared streaming decoder checks page framing, page order, embedded index,
CRC64 file checksum, trailer, and EOF. Unexpected EOF cannot finalize a file.
The merge retains one page per input plus the indexes. Bucket providers still
buffer Get/Put bodies, so the whole pipeline is not yet bounded to this memory
usage.

## Real HK COS test

Command: tagged `go test ./internal/replica -run
'^TestCOSNativeLevelsColdRestore$' -count=1 -v -timeout=3m` with the dedicated
COS environment. Credentials were loaded from `/opt/.secrets/s3` without being
printed. Bucket: `cellhive-e2e-hk-1376795072`; service endpoint derived from
`https://cellhive-e2e-hk-1376795072.cos.ap-hongkong.myqcloud.com`.

Topology: one Go test process using local SQLite capture and the real COS S3
API. No fleet, HTTP request server, or owner takeover is involved in this test.
The scope was unique to this run and cleaned up afterwards.

- 40 initial KV rows, 1024 bytes per value; 16 updates.
- Durable TXID 57.
- Native L0 capture followed by L1, L2, L3 promotion and one complete L9 file.
- Cold restore selected one L9 input. All 40 final values matched byte-for-byte;
  SQLite `PRAGMA integrity_check` returned `ok`.
- Six stored objects, all ending in `.ltx`; no replication sidecars.
- Capture-through-verification elapsed: 1.503030620 seconds.
- Cold restore, row/integrity verification and final object listing:
  225.339054 milliseconds. This is not a pure GET latency measurement.
- Go test duration including cleanup: 1.66 seconds; package passed.

Evidence: `/tmp/native-cos-level-cold.log` on the development host.

## Remaining gates

Full repository regression is still being migrated. Epoch continuation
publication/local ticket advancement, native embedded-index paged reads,
physical GC/retention, provider streaming/multipart, large-cell and full fleet
COS E2E remain unaccepted. The small test above does not prove any of these.


Native paged restore progress (2026-11-08): PageFetcher now builds a fixed activation cut from native embedded LTX indexes and range-reads native LZ4 frames, without external indexes/manifests. Regressions cover cross-epoch mapping, truncation, overridden pages stopping read runs, and ETag identity. Missing objects fail closed rather than rebuilding against a newer cut. Partial reads validate framing/index bounds and page identity, not whole-file CRC, matching celld's paged boundary. Per-object Stat still obtains size/ETag. Full ltx/restore/sqlcapture/cellstore/dosupervisor/objectstore packages pass. Dedicated HK COS 64MiB test passed: byte-exact 1MiB blob, 5 query range reads / 2,118,006 stored bytes; entire test 8.53s including setup/upload/compaction/cleanup. This is not activation latency or fleet/epoch acceptance. Continuation producer/local TXID advancement, GC, legacy codec removal and full acceptance remain incomplete.


Native epoch continuation progress (2026-11-08): production cell-agent now publishes zero-page [cut+1,cut+1] using actual SQLite page size/count and WAL salts, WALOffset32. It advances persisted local TXID and reinitializes the pre-start capture cursor/barrier only after durable publication; first application delta is cut+2. Ambiguous publication retries require read-only verification of an existing zero-page native seed, never creation that could hide a missing real delta. Full restore stamps the verified chain watermark before atomic image publication; paged restore adopts the source watermark before exposing writers. Native race and recovery regressions pass.

Paged restore no longer creates remote gc/pins JSON. HasCompleteSnapshot uses an independent current-epoch native chain without pin/manifest/index proof. Online physical GC remains deferred; missing objects fail closed. Dedicated HK COS proves predecessor57 -> seed58 -> delta59, cross-epoch L1 compaction, full/paged materialization, byte-exact41rows and SQLite integrity; all 9–10 objects are .ltx (capture batch count varies). This is one Go process with real COS, not fleet owner-lease takeover. Old MarkEpoch API/tests, obsolete codecs/index APIs, physical GC and full acceptance remain incomplete.


Native follower/recovery update (2026-11-08): spool validates native files and takes epoch from the authenticated request/session envelope. Non-session content deduplication uses raw-file SHA256 and records seen identities only after successful file/directory sync. Conflicting same-range contents reach immutable recovery publication and fail closed instead of being silently deduplicated. Recovery batching validates native CRC/ranges without private page-map conversion. Full peer/recovery/upload packages pass, with corruption, ambiguous sync retry and conflict-no-checkpoint regressions.

Real dedicated HK COS TestCOSNativeFollowerRecovery passed: two actual HTTP follower spools acknowledged SQLite capture while COS had no cell LTX; writer stopped, follower recovery uploaded COS, and a fresh SQLite restore verified20rows, TXID21 and integrity. Two LTX objects, recovery plus cold validation1.512s, entiretest2.43s. One Go process hosting two HTTP services plus real COS, not multi-node process crash or owner lease takeover acceptance. Legacy APIs/GC and remaining full-suite fixtures remain incomplete.


原生epoch GC验收补充（2026-11-09）：补充恢复计划可解析检查，当前epoch虽有快照但TXID中间缺口时不得发布退休标记；每轮最多扫描64个旧epoch，与celld一致。owner验证在扫描后及每个候选epoch删除前执行，不为每个LTX重复读取owner。server整包通过（46.039s），GC/驻留门闩相关race通过。真实指定HK COS：停止两个producer并完成分页物化后，以测试时钟推进保留窗口，epoch3发布原生L9，删除epoch1的6个LTX，保留epoch2，剩余5个对象只含LTX和上游retired.json；随后冷恢复41行逐字节一致且integrity_check通过。总测试6.39s；这是离线静默GC+真实COS验收，不代表在线多节点竞争/一小时真实等待验收。全仓make test仍在运行，其他旧接口/level GC测试待修齐。
