> 历史实施记录，已归档。进度、待办和运行环境描述只代表当时状态；当前契约及验收以 [known-issues](../../known-issues.md)、[decisions](../../decisions.md) 和对应报告为准。

# Native paged cold activation

Status: implementation and verification in progress; no deployment.

## Contract

Follow celld's native header/index/Range/page-fault flow without additional remote formats. Online activation uses a fixed native restore plan, validates header/path ranges, page size, structural index bounds, final page coverage including truncation/reextension, and pins object ETags. It does not imply complete file CRC validation. Keep `ProvenWatermark` as an explicit full-body audit; background/maintenance audit is distinct from first-query activation. Never turn missing/corrupt pages into successful zero-filled reads. Owner fencing and acknowledged durability remain unchanged.

## Tasks

1. Trace and replace DO capture seed's full-body proof with existing PageSource fixed index cut. Retain full audit API and fail-closed handling; preserve external non-paging source behavior. Add structural corruption and I/O regression tests.
2. Compact in-memory page-location representation, sharing per-object key/ETag metadata. Preserve sparse holes, pending lock page, truncation, source identity and immutable activation cut. Measure allocations and run correctness tests.
3. Investigate and implement workerd-fork SQLite fault-in VFS integration; Go supervisor must bridge only authorized immutable page reads, no runtime bucket credentials. Required production protocol/integration evidence before calling DO paging complete.
4. Real HK COS 10GiB indexed cold read: measure activation/index, first row, subsequent rows, bytes and calls separately. Full audit after reads. Exercise changed pages and epoch continuation. Measure actual CellHive entry activation separately from storage component paths.
5. Review changes, run relevant race/unit/integration checks; document remaining boundaries and results.

## Acceptance

Use bucket cellhive-e2e-hk-1376795072, existing secret file, isolated test scope and cleanup. No initial full-body audit charged to online activation. Validate returned rows and changed marker, ETag mutation failures, gaps and truncation/reextension. Do not describe storage integration as fleet endpoint acceptance. No implicit commits or deployment.

Progress: Tasks1–2 implementation and component tests passed; 10GiB indexed cellstore cold first verified row934ms, full post-query audit passed (50.543s), runner exit0. DO native paging protocol investigation at /tmp/workerd-paging-feasibility.txt; VFS implementation is now present in the fork; native binary build and actual DO integration acceptance remain pending.

Validation: full tagged Go suite, vet, replica/dosupervisor/cellstore race and git diff --check passed. Tasks3 (native DO paged activation + capture boundary), actual HTTP/fleet first-response and owner-switch acceptance remain unfinished; do not mark overall plan complete.

## DO paging implementation contract

Native workerd VFS calls an authenticated loopback page service before main-database reads/writes; the supervisor owns immutable PageFetcher, fills missing original pages into the shared sparse file, and tracks hydration. Native connections serialize paging with writes/truncation, bypass mmap, and poison failed views. Unknown existing files with no valid registry fail closed; registry capability is per supervised process. No bucket credentials or new COS metadata.

Restored scopes seed capture through a native zero-page continuation, then capture WAL changes; they must not publish an initial whole-database snapshot. Before any subsequent raw baseline/checkpoint snapshot, fully hydrate missing original pages under the paging registry. This preserves the current snapshot correctness boundary; checkpoint full hydration remains an explicit performance limitation until merged streaming capture is implemented. The bridge cannot silently run against an older unmodified workerd binary.

Main-file writes and truncations now bracket the native operation with a supervisor mutation generation and active-operation counter. Raw snapshots reject overlapping operations; failed native I/O keeps the capture barrier active. The local sparse guard directory is fsynced before truncating a cache image. Regression coverage for active, completed, stale-generation and poisoned-view capture passed under the Go race detector. These are implementation checks, not native DO/fleet acceptance.

2026-11-09 capture 一致性补充：主库 write/truncate 使用开始/结束计数和 generation 校验，失败 native I/O 不释放 capture 屏障。checkpoint 快照提交后，同一次 gate 必须重读并持久化当前 WAL，不能等待下一次请求。新增立即跨目录恢复回归：旧行为返回值 1（预期 42）失败，修复后返回 42 且 dosupervisor race 通过。原生 DO/COS 验收仍待新 workerd 构建完成。


2026-11-09 follow-up: native fork build completed with 4 cores; do-paging/1 verified. FS/COS native 16MiB cold-read/recapture/independent restore PASS, full runtime/supervisor PASS plus opt-in COS and 2000 DO stress PASS. Evidence in latest section of docs/reports/2026-11-09-cos-fleet-acceptance.md. Native 10GiB and actual fleet owner-switch paging remain unverified.
