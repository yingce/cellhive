> 历史实施记录，已归档。进度、待办和运行环境描述只代表当时状态；当前契约及验收以 [known-issues](../../known-issues.md)、[decisions](../../decisions.md) 和对应报告为准。

# Large cell bounded storage and 10 GiB COS acceptance

Approved scope: preserve native celld LTX format and data correctness; bound compaction, open inputs, node background work, reuse immutable local LTX, avoid buffered production restore, reduce repeated I/O only with verified immutable identity. Large cells may decline optional L9 without losing the existing chain. No new remote markers or legacy formats.

1. Bound incremental rounds to 256 files / 64 MiB; an indivisible large input runs alone. Check L9 logical size before reading bodies; never publish a partial snapshot. Bound simultaneous maintenance jobs. Keep continuation chains when optional snapshot exceeds budgets.
2. Bound active input streams and use local version-bound LTX cache where practical. Keep CRC, exact TXID cut, ETag validation and failure cleanup. No cache hit may bypass ownership/fencing or change durable head.
3. Remove full-body fallback from production paged restoration when paging fails; propagate corruption/network errors. Bound concurrent index building/materialization and eliminate retain-all intermediate index duplication.
4. Add regression tests and real 10 GiB SQLite acceptance in HK COS cellhive-e2e-hk-1376795072: allocated data, upload, large-input compaction, skipped optional L9, cross-epoch cold restore, SQLite integrity and byte SHA. Record elapsed/RSS/disk/request counts. A smaller smoke test is not 10 GiB acceptance.
5. Run affected tests, full tests/build/vet, review and document measured results and any outstanding scope.

Environment: initial root disk full, /opt 7.7GiB free. Cleared regeneratable Go build cache only. Test must serialize /tmp/cellhive-hk-review.lock, use /opt/.secrets/s3 without exposing secrets, and manage its own temporary files to fit available storage. Existing dirty tree retained on perf/large-cell-bounded-storage; no implicit commit/deploy.

## Implemented pending final acceptance

- Adjacent-level batches: 256 files/64MiB; one indivisible oversized source allowed. One active compaction per process; low scratch space rejects before output. Optional L9 default logical budget80MiB, pre-release honors existing256MiB config. L9 also refuses over-budget fan-in/input bytes, never publishes a partial chain.
- At most two native read/proof plans per process; >16 inputs use short stable-ETag ranged reads with lazy64KiB buffers instead of retaining all network connections. At most two file-upload validations and two index-build/materialization jobs per process. Index batches32, releases intermediate slices. Final page map is still proportional to page count, not a hard byte-limited memory allocator.
- Local disk LTX cache64MiB/256 entries, new successful file-backed publications only; remote Stat size/ETag gates every cache reuse. Pinned entries count toward budget; cache population contention is skipped. Startup cleanup requires exclusive data-dir instance lock.
- Production DO paging failures propagate instead of full-body fallback. External non-paging SegmentSource adapters retain their explicit buffered interface.
- Native spool reads/writes use256KiB buffers; native decoder compares embedded index in64KiB chunks instead of allocating a second whole index. Full CRC remains mandatory; no unsafe trusted-spool bypass was added.
- LZ4 table pooling with generation stamps preserves exact output bytes; measured4KiB-page microbenchmark changed from529KiB/op,0.93–1.17ms to~5KiB/op,10.7–11.9us on shared host. Not end-to-end throughput evidence.
- Full materialization coalesces up to8MiB decoded pages while retaining1MiB encoded request cap, same source/ETag adjacency and requested-range boundaries.

Acceptance in progress: populated10GiB SQLite source actual10,751,451,136bytes, allocated10,751,512,576bytes; encoded787,262,031bytes. Initial pre-pooling run stopped during slow encoding and own temporary fixture removed. First optimized binary still uses1MiB decoded restore runs; final acceptance must distinguish that from latest8MiB run and decoder-memory changes. General cell SQLite export/import remains a separate outstanding earlier request, not implemented here.

Cache review correction: ordinary byte delta publication never populates the disk cache, avoiding additional small-file I/O on acknowledged writes. Only existing snapshot/compaction spools are copied after new authoritative publication. This does not change the large-COS fixture path (cache disabled in that fixture). Regression TestForegroundDeltaPublicationDoesNotPopulateDiskCache guards the boundary.

## Final acceptance

10GiB COS final run PASS536.62s, fullSQLiteSHA/integrity/all10240rows equal, epoch2 cut42; proof+index+coldmaterialize107.672s versus first502.646s. Final processRSS762564KiB, encoded787262031bytes forSQLite10751451136bytes. OptionalL9 readsone100byteheader/no payload. Final fullgo test /tmp/large-cell-complete-test.log session32010exit0;build/vet /tmp/large-cell-release-{build,vet}.log exit0; relevant races PASS. See docs/reports/2026-11-09-large-cell-storage.md for detailed boundaries. No live tests or COS runners remain. Precise byte-hard index memory admission and durable-proof caching were not added; concurrency/input/cache limits bound work while pageindexes stillscale. Export/import remainsseparate. No implicitcommit/deploy.
