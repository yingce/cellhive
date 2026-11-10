package ltx

// DefaultSnapshotPartBytes bounds the raw-page read buffer feeding a single
// native snapshot encoder. Callback batches are not separate LTX files.
const DefaultSnapshotPartBytes = 8 << 20
