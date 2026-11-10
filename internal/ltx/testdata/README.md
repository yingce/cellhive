# Native LTX interoperability fixtures

`native-snapshot.ltx` and `native-delta.ltx` were generated with
`celld_ltx::ltx::encode_file_v0_5_2` from `/opt/celld` at
`90b43017241f81189453d326d05948f388b34652` on 2026-11-08.
They use a 4096-byte page size, commit=2, NoChecksum, post-apply checksum=0,
and otherwise zero header fields (version=3).

- Snapshot: TXID 1..19, page 1 consists of byte 23, page 2 of byte 42.
- Delta: TXID 20..21, only page 2, consisting of byte 42.

Each page contains 4096 copies of its stated byte. These are codec fixtures,
not SQLite database images. `TestNativeCelldGoldenFiles` verifies the Rust
output in Go. The same images encoded by Go were independently decoded and
checked using Rust's `decode_file` and `decode_file_pages`.

The initial Go compressed output was rejected by celld: its LZ4 matches ran
through the end of input. The encoder now preserves the required five final
literal bytes and forbids match starts in the final twelve bytes.

`native-compacted.ltx` was produced by celld's public `Compactor` with
`header_flags=HEADER_FLAG_NO_CHECKSUM`, from identical Go-generated inputs:
TXID 1..3, commit=3, pages 1/2/3 filled with byte 3; then TXID 4..5,
commit=2, page 1 filled with byte 5. Both Rust and Go compaction outputs were
read by Rust and compared for header and page equality. The result is TXID
1..5, commit=2, page 1 filled with 5 and page 2 filled with 3 (512-byte pages).
`TestNativeCompactorMatchesCelldGolden` preserves this cross-implementation
regression independently of compression-byte choices.
