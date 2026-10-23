// Package sqlcapture converts committed SQLite WAL transactions into LTX and
// gates SQL completion on fleet durability.
package sqlcapture

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
	"cellhive/internal/ltx"
	"cellhive/internal/wal"
)

// Committer proves one LTX segment durable.
type Committer interface {
	Commit(ctx context.Context, scope cell.Scope, epoch uint64, segment []byte) error
}

// BatchCommitter is an optional Committer extension: it proves a group of
// snapshot parts durable with one request, which avoids one round trip per part
// when a full-page snapshot is split into many parts. The capture only uses it
// when SnapshotBatchParts > 1. Every segment must become durable before the
// call returns nil (RPO=0, same proof as Commit).
type BatchCommitter interface {
	CommitBatch(ctx context.Context, scope cell.Scope, epoch uint64, segments [][]byte) error
}

// ErrSnapshotTooLarge reports that a full-page snapshot image exceeds
// Capture.SnapshotMaxBytes. No part is committed when it is returned, so the
// existing durable chain is untouched.
var ErrSnapshotTooLarge = errors.New("sqlcapture: snapshot exceeds SnapshotMaxBytes")

// Stats summarizes capture batching.
type Stats struct {
	CaptureBatches uint64
	Transactions   uint64
	Frames         uint64
	Pages          uint64
	DurableTxID    uint64
	WALBytesRead   uint64
	LTXBinaryBytes uint64
	Snapshots      uint64
	BatchDurations []time.Duration
}

const (
	// maxCaptureTransactions bounds one LTX chunk. Larger chunks amortize a
	// single in-flight RTT (throughput ~= chunk/RTT) and, because the WAL2
	// payload is page-map deduplicated, a hot-page chunk stays tiny. The
	// 1MiB payload cap still bounds wide-write chunks.
	maxCaptureTransactions = 512
	maxCapturePayloadBytes = 1 << 20
	captureGroupCommitWait = 2 * time.Millisecond
	// capturePipeline is how many chunks may be in flight to the owner at once.
	capturePipeline = 4
)

// Capture polls a cell WAL, emits real WAL-page LTX segments, and advances a
// durability barrier only after Committer confirms fleet durability.
type Capture struct {
	cell             *cellstore.Cell
	scope            cell.Scope
	epoch            uint64
	cursor           *wal.Cursor
	baselineWALBytes uint64
	nextTxID         uint64
	committer        Committer
	barrier          *barrier
	notify           chan struct{}
	done             chan struct{}

	// GroupCommitWait and PipelineThreshold are tunable (0 = package defaults).
	// They are set before Start.
	GroupCommitWait   time.Duration
	PipelineThreshold time.Duration

	// inflight pipelines commit chunks in txid order. The barrier advances only
	// through contiguous acks, preserving RPO=0 and ordering.
	inflight []chunkFuture
	// pipeline is the in-flight cap. It starts at 1 (serial) and switches once,
	// stickily, to capturePipeline when a commit is seen to take a round trip.
	// The stickiness keeps the owner's ordering baseline monotonic: mixing
	// serial and pipelined commits after a switch would leave the baseline
	// behind.
	pipeline int
	// pipelineBase is the first txid of the pipelined epoch; the owner uses it
	// to initialize the scope's ordering baseline.
	pipelineBase uint64
	// committed reports the writer's highest committed ticket. After a
	// checkpoint snapshot the barrier advances to the watermark, because the
	// snapshot reflects every commit at that moment even if the WAL frame was
	// truncated before the capture saw it as a delta.
	committed func() uint64
	// autoCheckpointBytes truncates the WAL once it grows past this size. 0
	// disables it. It requires a checkpointer hook so the truncate and the
	// watermark read happen while the writer is paused.
	autoCheckpointBytes int64
	// checkpointer pauses the writer, runs a TRUNCATE checkpoint, returns the
	// writer's committed ticket, and resumes.
	checkpointer func(ctx context.Context) (uint64, error)

	// SnapshotMaxBytes rejects a full-page snapshot whose image exceeds this
	// many bytes with ErrSnapshotTooLarge (0 = unlimited). Captures are streamed
	// part by part, so the budget bounds the image, not the RSS; it lets a
	// deployment fail closed instead of durably committing a snapshot larger
	// than the storage/restore path allows. It applies to every snapshot path:
	// a checkpoint-triggered rebaseline over budget fails the capture loop
	// rather than silently skipping the snapshot.
	SnapshotMaxBytes int64
	// SnapshotPartBytes bounds one streamed snapshot part (0 = 8MiB,
	// ltx.DefaultSnapshotPartBytes).
	SnapshotPartBytes int
	// SnapshotBatchParts groups this many streamed parts into one CommitBatch
	// when the committer implements BatchCommitter (0 or 1 = one commit per
	// part). Parts are additionally capped at 64MiB per batch.
	SnapshotBatchParts int

	statsMu sync.Mutex
	stats   Stats
}

type chunkFuture struct {
	end     uint64
	started time.Time
	ch      <-chan error
}

// New creates a capture loop and consumes the current WAL as its baseline.
func New(db *cellstore.Cell, scope cell.Scope, epoch, baselineTxID uint64, committer Committer) (*Capture, error) {
	if db == nil || committer == nil {
		return nil, fmt.Errorf("sqlcapture: cell and committer are required")
	}
	cursor := wal.NewCursor()
	if _, err := cursor.Poll(db.WALPath()); err != nil && !errors.Is(err, wal.ErrNoWAL) {
		return nil, fmt.Errorf("sqlcapture: baseline WAL poll: %w", err)
	}
	return &Capture{
		cell: db, scope: scope, epoch: epoch, cursor: cursor, baselineWALBytes: cursor.BytesRead(),
		nextTxID: baselineTxID, committer: committer,
		barrier: newBarrier(baselineTxID), notify: make(chan struct{}, 1),
		done: make(chan struct{}),
	}, nil
}

// Start runs capture until ctx is cancelled or a fatal error. Call Snapshot
// once, before any writer starts, to establish a baseline for restores.
func (c *Capture) Start(ctx context.Context) { go c.loop(ctx) }

// Notify asks the loop to poll promptly after a SQL commit.
func (c *Capture) Notify() {
	select {
	case c.notify <- struct{}{}:
	default:
	}
}

// Wait blocks until a covering LTX segment is fleet-durable.
func (c *Capture) Wait(ctx context.Context, txid uint64) error {
	return c.barrier.wait(ctx, txid)
}

// Stats returns a capture snapshot.
func (c *Capture) Stats() Stats {
	c.statsMu.Lock()
	defer c.statsMu.Unlock()
	stats := c.stats
	stats.BatchDurations = append([]time.Duration(nil), c.stats.BatchDurations...)
	stats.WALBytesRead = c.cursor.BytesRead() - c.baselineWALBytes
	stats.DurableTxID = c.barrier.durable()
	return stats
}

func (c *Capture) Done() <-chan struct{} { return c.done }

func (c *Capture) loop(ctx context.Context) {
	defer close(c.done)
	defer c.cursor.Close()
	// The idle poll cadence follows the group-commit window, otherwise a fixed
	// 1ms ticker drains the WAL before the window can coalesce and the window
	// knob has no effect.
	interval := c.GroupCommitWait
	if interval <= 0 {
		interval = captureGroupCommitWait
	}
	if interval < time.Millisecond {
		interval = time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			c.barrier.fail(ctx.Err())
			return
		case <-c.notify:
			wg := c.GroupCommitWait
			if wg <= 0 {
				wg = captureGroupCommitWait
			}
			if err := waitGroupCommit(ctx, c.notify, wg); err != nil {
				c.barrier.fail(err)
				return
			}
		case <-ticker.C:
		}
		if err := c.poll(ctx); err != nil {
			c.barrier.fail(err)
			return
		}
	}
}

func waitGroupCommit(ctx context.Context, notify <-chan struct{}, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-notify:
			// Coalesce notifications while retaining the original deadline.
		case <-timer.C:
			return nil
		}
	}
}

func (c *Capture) poll(ctx context.Context) error {
	// Always advance the barrier through completed chunks first. An ack that
	// arrived after the previous post-dispatch drain must not be stranded just
	// because this poll found no new WAL transactions.
	if err := c.drain(ctx, false); err != nil {
		return err
	}
	result, err := c.cursor.Poll(c.cell.WALPath())
	if errors.Is(err, wal.ErrNoWAL) {
		return nil
	}
	if errors.Is(err, wal.ErrChecksum) {
		// A frame can be observed between SQLite's header and page writes. Do
		// not turn a torn tail into committed data and do not kill the capture
		// loop: the next Notify/poll retries once the write completes.
		return nil
	}
	if err != nil {
		return fmt.Errorf("sqlcapture: WAL poll: %w", err)
	}
	if result.Checkpoint {
		// The WAL was checkpointed/truncated, so the delta chain is broken.
		// Emit a full-page snapshot (after draining in-flight chunks) and keep
		// going; the cursor already re-baselined to the new WAL generation.
		if err := c.rebaseline(ctx); err != nil {
			return err
		}
		return nil
	}
	if len(result.Transactions) == 0 {
		return nil
	}
	txs := make([]ltx.WALTransaction, 0, len(result.Transactions))
	for _, transaction := range result.Transactions {
		out := ltx.WALTransaction{Frames: make([]ltx.WALFrame, 0, len(transaction.Frames))}
		for _, frame := range transaction.Frames {
			out.Frames = append(out.Frames, ltx.WALFrame{PageNo: frame.PageNo, DBSize: frame.DBSize, Data: frame.Data})
		}
		txs = append(txs, out)
	}
	for _, chunk := range chunkTransactions(result.Header.PageSize, txs) {
		commit, pages, err := ltx.PageMapFromTransactions(result.Header.PageSize, chunk)
		if err != nil {
			return fmt.Errorf("sqlcapture: build page map: %w", err)
		}
		payload, err := ltx.EncodeWALPageMap(result.Header.PageSize, commit, len(chunk), pages)
		if err != nil {
			return fmt.Errorf("sqlcapture: encode page map: %w", err)
		}
		startTxID := c.nextTxID + 1
		endTxID := c.nextTxID + uint64(len(chunk))
		segment := ltx.Encode(ltx.Header{
			Kind: ltx.KindDelta, Epoch: c.epoch,
			StartTxID: startTxID, EndTxID: endTxID,
		}, payload)
		started := time.Now()

		// Bound in-flight chunks: block on the head before dispatching another.
		if len(c.inflight) >= c.pipeline {
			if err := c.drain(ctx, true); err != nil {
				return err
			}
		}
		if ac, ok := c.committer.(AsyncCommitter); ok && c.pipeline > 1 {
			if c.pipelineBase == 0 {
				c.pipelineBase = startTxID
			}
			c.inflight = append(c.inflight, chunkFuture{end: endTxID, started: time.Now(), ch: ac.CommitAsync(ctx, c.scope, c.epoch, segment, c.pipelineBase)})
		} else if err := c.committer.Commit(ctx, c.scope, c.epoch, segment); err != nil {
			return fmt.Errorf("sqlcapture: fleet commit %d-%d: %w", startTxID, endTxID, err)
		} else {
			c.adaptPipeline(time.Since(started))
			c.barrier.advance(endTxID)
		}

		chunkFrames := 0
		for _, transaction := range chunk {
			chunkFrames += len(transaction.Frames)
		}
		c.nextTxID = endTxID
		c.statsMu.Lock()
		c.stats.CaptureBatches++
		c.stats.Transactions += uint64(len(chunk))
		c.stats.Frames += uint64(chunkFrames)
		c.stats.Pages += uint64(len(pages))
		c.stats.LTXBinaryBytes += uint64(len(segment))
		c.stats.BatchDurations = append(c.stats.BatchDurations, time.Since(started))
		c.statsMu.Unlock()

		if err := c.drain(ctx, false); err != nil {
			return err
		}
	}
	if err := c.maybeAutoCheckpoint(ctx); err != nil {
		return err
	}
	return nil
}

// maybeAutoCheckpoint safely truncates the WAL once it grows past the threshold.
//
// Correctness: the writer is paused across the TRUNCATE and the watermark read,
// so the checkpoint backfills every commit and W0 covers them; the database
// file is then stable until the next checkpoint, so reading it yields a
// consistent snapshot whose page numbers match later deltas. Commits after the
// resume land in the fresh WAL and become deltas with txid > W0, so no ticket
// is released before a durable proof covers it.
func (c *Capture) maybeAutoCheckpoint(ctx context.Context) error {
	if c.autoCheckpointBytes <= 0 || c.checkpointer == nil || len(c.inflight) > 0 {
		return nil
	}
	info, err := os.Stat(c.cell.WALPath())
	if err != nil || info.Size() < c.autoCheckpointBytes {
		return nil
	}
	w0, err := c.checkpointer(ctx)
	if err != nil {
		return nil // busy: retry on a later poll
	}
	w := c.nextTxID
	if w0 > w {
		w = w0
	}
	return c.emitSnapshot(ctx, w)
}

// adaptPipeline widens the in-flight cap when commits take a round trip and
// narrows it when they are local, so a fast link keeps the serial path while a
// high-RTT link overlaps.
func (c *Capture) adaptPipeline(latency time.Duration) {
	if c.pipeline > 1 {
		return // sticky: never mix modes once pipelined
	}
	thr := c.PipelineThreshold
	if thr <= 0 {
		thr = 8 * time.Millisecond
	}
	if latency >= thr {
		c.pipeline = capturePipeline
	}
}

// SetCommittedWatermark wires the writer's committed-ticket accessor so a
// checkpoint snapshot can release tickets whose WAL frames were truncated
// before the capture consumed them.
func (c *Capture) SetCommittedWatermark(f func() uint64) { c.committed = f }

// CommittedTicket reports the writer's committed ticket currently surfaced to
// the checkpoint watermark (0 when no accessor is wired).
func (c *Capture) CommittedTicket() uint64 {
	if c.committed == nil {
		return 0
	}
	return c.committed()
}

// SetCheckpointer wires the safe TRUNCATE hook: it must pause the writer, run
// the checkpoint, read the committed ticket, and resume.
func (c *Capture) SetCheckpointer(f func(ctx context.Context) (uint64, error)) { c.checkpointer = f }

// SetAutoCheckpoint enables (n>0) or disables (n<=0) automatic WAL truncation
// once the WAL grows past n bytes. It needs a checkpointer hook.
func (c *Capture) SetAutoCheckpoint(n int64) { c.autoCheckpointBytes = n }

// snapshotMode selects how a streamed snapshot obtains its page image.
type snapshotMode int

const (
	// snapshotModeCheckpoint checkpoints under the cell write mutex and labels
	// the snapshot with the cell's committed txid (the capture's startup path).
	snapshotModeCheckpoint snapshotMode = iota
	// snapshotModePages streams an image the caller already checkpointed (with
	// the writer paused) and labels it with the capture's watermark.
	snapshotModePages
)

// maxSnapshotBatchBytes caps the encoded bytes buffered for one CommitBatch.
const maxSnapshotBatchBytes = 64 << 20

// Snapshot checkpoints the database (backfilling WAL frames) and streams a full
// snapshot. The checkpoint, watermark read and page image are taken under the
// cell write mutex, so a commit cannot land between them and release a ticket
// whose pages are absent from the image. Parts are read, encoded and committed
// one at a time, so memory is O(SnapshotPartBytes), not O(database).
func (c *Capture) Snapshot(ctx context.Context) error {
	return c.emitSnapshotStream(ctx, snapshotModeCheckpoint, 0)
}

// Checkpoint forces a WAL truncate checkpoint. The capture loop then emits a
// full-page snapshot on its next poll, so the truncated WAL has a durable
// baseline to restore from. Callers must not run it while writes are in flight.
func (c *Capture) Checkpoint(ctx context.Context) error {
	return c.cell.Checkpoint(ctx)
}

// maxWatermark returns the highest txid a snapshot at this instant may claim:
// the capture's own position or the writer's committed ticket.
func (c *Capture) maxWatermark() uint64 {
	watermark := c.nextTxID
	if c.committed != nil {
		if w := c.committed(); w > watermark {
			watermark = w
		}
	}
	return watermark
}

// rebaseline emits a baseline snapshot after making the database file reflect
// every commit up to the snapshot's watermark. When a checkpointer is wired it
// pauses the writer and truncates the WAL (backfilling all committed frames)
// before reading pages, so the image and the watermark cannot diverge; this is
// the safe path for a checkpoint detected mid-capture. Without a checkpointer it
// falls back to reading the database file at the writer's committed watermark.
func (c *Capture) rebaseline(ctx context.Context) error {
	if c.checkpointer == nil {
		return c.emitSnapshot(ctx, c.maxWatermark())
	}
	if err := c.drain(ctx, true); err != nil {
		return err
	}
	w0, err := c.checkpointer(ctx)
	if err != nil {
		return fmt.Errorf("sqlcapture: rebaseline checkpoint: %w", err)
	}
	w := c.nextTxID
	if w0 > w {
		w = w0
	}
	return c.emitSnapshot(ctx, w)
}

// emitSnapshot drains in-flight chunks, streams a full-page KindSnapshot at the
// given watermark, and waits for it to be durable. The caller must ensure the
// database file reflects every commit up to watermark (checkpoint/backfill);
// the snapshot consumes no txid, so the delta sequence continues unchanged.
func (c *Capture) emitSnapshot(ctx context.Context, watermark uint64) error {
	if err := c.drain(ctx, true); err != nil {
		return err
	}
	return c.emitSnapshotStream(ctx, snapshotModePages, watermark)
}

// emitSnapshotStream streams a full-page snapshot part by part and commits each
// part as it is encoded. Nothing advances — no cursor reset, no nextTxID, no
// barrier — unless every part is durable, so a failed snapshot never becomes the
// capture's new baseline. (Parts already committed before a mid-stream failure
// stay in the durable chain as an incomplete snapshot; see the ADR-272 P2 notes
// on partial-snapshot cleanup.)
func (c *Capture) emitSnapshotStream(ctx context.Context, mode snapshotMode, watermark uint64) error {
	partBytes := c.SnapshotPartBytes
	if partBytes <= 0 {
		partBytes = ltx.DefaultSnapshotPartBytes
	}
	if max := c.SnapshotMaxBytes; max > 0 {
		// The database file is a lower bound for the image (a checkpoint can
		// only grow it), so fail before the checkpoint or any page read when it
		// already exceeds the budget.
		if info, err := os.Stat(c.cell.Path); err == nil && info.Size() > max {
			return fmt.Errorf("%w: database %d bytes, budget %d", ErrSnapshotTooLarge, info.Size(), max)
		}
	}
	var batcher BatchCommitter
	if c.SnapshotBatchParts > 1 {
		batcher, _ = c.committer.(BatchCommitter)
	}
	var (
		batch         [][]byte
		batchBytes    int
		label         = watermark
		snapshotBytes int
	)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		segments := batch
		batch, batchBytes = nil, 0
		if err := batcher.CommitBatch(ctx, c.scope, c.epoch, segments); err != nil {
			return fmt.Errorf("sqlcapture: snapshot batch commit (%d parts): %w", len(segments), err)
		}
		return nil
	}
	stream := func(partIndex, total int, wm uint64, pageSize, commit, firstPage uint32, pages [][]byte) error {
		if max := c.SnapshotMaxBytes; max > 0 {
			// Exact check: the image size is only known once the checkpoint and
			// the file header are read, still before the first part commits.
			if imageBytes := int64(commit) * int64(pageSize); imageBytes > max {
				return fmt.Errorf("%w: image %d bytes, budget %d", ErrSnapshotTooLarge, imageBytes, max)
			}
		}
		if mode == snapshotModeCheckpoint && partIndex == 0 {
			label = wm
		}
		walPages := make([]ltx.WALPage, len(pages))
		for i, data := range pages {
			walPages[i] = ltx.WALPage{PageNo: firstPage + uint32(i), Data: data}
		}
		segment, err := ltx.EncodeSnapshotPart(ltx.Header{
			Kind: ltx.KindSnapshot, Epoch: c.epoch,
			StartTxID: label, EndTxID: label,
		}, pageSize, commit, walPages, partIndex, total)
		if err != nil {
			return fmt.Errorf("sqlcapture: encode snapshot part %d/%d: %w", partIndex+1, total, err)
		}
		snapshotBytes += len(segment)
		if batcher == nil {
			if err := c.committer.Commit(ctx, c.scope, c.epoch, segment); err != nil {
				return fmt.Errorf("sqlcapture: snapshot commit part %d/%d: %w", partIndex+1, total, err)
			}
			return nil
		}
		if batchBytes+len(segment) > maxSnapshotBatchBytes && len(batch) > 0 {
			if err := flush(); err != nil {
				return err
			}
		}
		batch = append(batch, segment)
		batchBytes += len(segment)
		if len(batch) >= c.SnapshotBatchParts {
			return flush()
		}
		return nil
	}

	var err error
	if mode == snapshotModeCheckpoint {
		var wm uint64
		wm, _, _, err = c.cell.SnapshotStream(ctx, partBytes, stream)
		if err == nil {
			label = wm
		}
	} else {
		_, _, _, err = c.cell.SnapshotStreamPages(ctx, partBytes, stream)
	}
	if err != nil {
		return err
	}
	if batcher != nil {
		if err := flush(); err != nil {
			return err
		}
	}
	// The database file is now this snapshot's baseline; re-anchor the cursor so
	// later WAL frames become deltas with txid > watermark.
	c.cursor.Reset()
	c.nextTxID = label
	c.barrier.advance(label)
	c.statsMu.Lock()
	c.stats.Snapshots++
	c.stats.LTXBinaryBytes += uint64(snapshotBytes)
	c.statsMu.Unlock()
	return nil
}

// drain advances the barrier through contiguous completed chunks. With block it
// waits for the head first (backpressure); otherwise it only consumes chunks
// that already finished, so an out-of-order ack never advances the barrier past
// an unproven chunk.
func (c *Capture) drain(ctx context.Context, block bool) error {
	for len(c.inflight) > 0 {
		head := c.inflight[0]
		var err error
		if block {
			select {
			case err = <-head.ch:
			case <-ctx.Done():
				return ctx.Err()
			}
			block = false
		} else {
			select {
			case err = <-head.ch:
			default:
				return nil
			}
		}
		if err != nil {
			return fmt.Errorf("sqlcapture: fleet commit ending %d: %w", head.end, err)
		}
		c.adaptPipeline(time.Since(head.started))
		c.barrier.advance(head.end)
		c.inflight = c.inflight[1:]
	}
	return nil
}

// chunkTransactions splits a poll into ordered chunks bounded by transaction
// count and the deduplicated page-map payload size (last write per page). In a
// hot-page workload the deduplicated size stays tiny, so the count bound
// dominates; the byte bound only fires for wide-write workloads.
func chunkTransactions(pageSize uint32, txs []ltx.WALTransaction) [][]ltx.WALTransaction {
	const headerBytes = 20 // WAL2 header
	var chunks [][]ltx.WALTransaction
	var current []ltx.WALTransaction
	seen := map[uint32]struct{}{}
	pages := 0
	flush := func() {
		if len(current) == 0 {
			return
		}
		chunks = append(chunks, current)
		current = nil
		seen = map[uint32]struct{}{}
		pages = 0
	}
	countNew := func(tx ltx.WALTransaction) int {
		n := 0
		for _, frame := range tx.Frames {
			if _, ok := seen[frame.PageNo]; !ok {
				seen[frame.PageNo] = struct{}{}
				n++
			}
		}
		return n
	}
	for _, transaction := range txs {
		newPages := countNew(transaction)
		if len(current) > 0 && (len(current) >= maxCaptureTransactions ||
			headerBytes+(pages+newPages)*(4+int(pageSize)) > maxCapturePayloadBytes) {
			flush()
			newPages = countNew(transaction)
		}
		current = append(current, transaction)
		pages += newPages
	}
	flush()
	return chunks
}

type barrier struct {
	mu          sync.Mutex
	durableTxID uint64
	fatal       error
	waiters     map[uint64][]chan error
}

func newBarrier(durableTxID uint64) *barrier {
	return &barrier{durableTxID: durableTxID, waiters: make(map[uint64][]chan error)}
}

func (b *barrier) wait(ctx context.Context, txid uint64) error {
	b.mu.Lock()
	if txid <= b.durableTxID {
		b.mu.Unlock()
		return nil
	}
	if b.fatal != nil {
		err := b.fatal
		b.mu.Unlock()
		return err
	}
	ch := make(chan error, 1)
	b.waiters[txid] = append(b.waiters[txid], ch)
	b.mu.Unlock()
	select {
	case err := <-ch:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *barrier) advance(endTxID uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.fatal != nil || endTxID <= b.durableTxID {
		return
	}
	b.durableTxID = endTxID
	for txid, waiters := range b.waiters {
		if txid > endTxID {
			continue
		}
		for _, waiter := range waiters {
			waiter <- nil
		}
		delete(b.waiters, txid)
	}
}

func (b *barrier) fail(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.fatal != nil {
		return
	}
	b.fatal = err
	for txid, waiters := range b.waiters {
		for _, waiter := range waiters {
			waiter <- err
		}
		delete(b.waiters, txid)
	}
}

func (b *barrier) durable() uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.durableTxID
}
