// Package upload batches replication segments into fewer object-store writes
// (group commit). This is the fleet posture's asynchronous bucket uploader.
package upload

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"cellhive/internal/cell"
)

// Sink writes a batch of segments for one scope/epoch.
type Sink interface {
	AppendBatch(ctx context.Context, s cell.Scope, epoch uint64, segments [][]byte) (string, string, error)
}

type item struct {
	scope   cell.Scope
	epoch   uint64
	seg     []byte
	segs    [][]byte      // non-nil for a pre-grouped batch (EnqueueBatchWait)
	done    chan error    // non-nil for EnqueueWait callers
	spoolID uint64        // >0 when the segment is persisted in the spool
	barrier chan struct{} // non-nil: flush prior items, then close
}

// itemSegs returns the LTX segments carried by one item.
func itemSegs(it item) [][]byte {
	if len(it.segs) > 0 {
		return it.segs
	}
	if it.seg != nil {
		return [][]byte{it.seg}
	}
	return nil
}

func itemBytes(it item) int {
	if len(it.segs) > 0 {
		n := 0
		for _, seg := range it.segs {
			n += len(seg)
		}
		return n
	}
	return len(it.seg)
}

// Batcher accumulates segments and flushes them in batches. Uploads are
// sharded by scope so independent cells commit to the object store in parallel;
// a given scope always hashes to one shard, so per-scope segment order (and
// therefore the txid chain) is preserved.
type Batcher struct {
	sink        Sink
	log         *slog.Logger
	shards      []*shard
	maxSegments int
	maxBytes    int
	interval    time.Duration

	batches   atomic.Uint64
	segments  atomic.Uint64
	errors    atomic.Uint64
	dropped   atomic.Uint64
	discarded atomic.Uint64

	// Durable queue for async (RPO>0) uploads: persisted before enqueue,
	// removed on success, replayed on start and retried until it drains.
	spool          *Spool
	spoolErrors    atomic.Uint64
	deferred       atomic.Uint64
	replayed       atomic.Uint64
	replayInterval time.Duration

	closeOnce sync.Once
}

// shard is one serial upload lane with its own queue and flush loop.
type shard struct {
	ch   chan item
	wake chan struct{}
	done chan struct{}
}

// New creates a batcher. Enqueue never blocks; if the queue is full it uploads
// synchronously so no acknowledged write is dropped.
func New(sink Sink, log *slog.Logger, maxSegments, maxBytes int, interval time.Duration) *Batcher {
	return NewSharded(sink, log, maxSegments, maxBytes, interval, 1)
}

// NewSharded creates a batcher with `shards` parallel upload lanes (>= 1). More
// shards let independent cells commit concurrently instead of serialising on one
// upload goroutine.
func NewSharded(sink Sink, log *slog.Logger, maxSegments, maxBytes int, interval time.Duration, shards int) *Batcher {
	if maxSegments <= 0 {
		maxSegments = 64
	}
	if maxBytes <= 0 {
		maxBytes = 1 << 20
	}
	if interval <= 0 {
		interval = 20 * time.Millisecond
	}
	if shards < 1 {
		shards = 1
	}
	b := &Batcher{
		sink:        sink,
		log:         log,
		maxSegments: maxSegments,
		maxBytes:    maxBytes,
		interval:    interval,
	}
	per := 8192 / shards
	if per < 256 {
		per = 256
	}
	for i := 0; i < shards; i++ {
		b.shards = append(b.shards, &shard{
			ch:   make(chan item, per),
			wake: make(chan struct{}, 1),
			done: make(chan struct{}),
		})
	}
	return b
}

// SetSpool attaches a durable write-ahead spool for asynchronous uploads. It
// must be called before Start. Without a spool the batcher keeps its previous
// best-effort behavior (bounded retries, then dropped+alert).
func (b *Batcher) SetSpool(sp *Spool) {
	if b.replayInterval <= 0 {
		b.replayInterval = 5 * time.Second
	}
	b.spool = sp
}

// shardFor picks the upload lane for a scope (stable, so per-scope order holds).
func (b *Batcher) shardFor(s cell.Scope) *shard {
	if len(b.shards) == 1 {
		return b.shards[0]
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(s.String()))
	return b.shards[int(h.Sum32())%len(b.shards)]
}

// Start runs the flush loops until Stop, then replays any spooled uploads left
// by a previous process (or a failure) in the background.
func (b *Batcher) Start(ctx context.Context) {
	for _, sh := range b.shards {
		go b.loop(ctx, sh)
	}
	if b.spool != nil {
		go b.replayLoop(ctx)
	}
}

// replayLoop retries spooled uploads until they succeed or the context ends.
func (b *Batcher) replayLoop(ctx context.Context) {
	interval := b.replayInterval
	if interval <= 0 {
		interval = 5 * time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			b.replayOnce(ctx)
		}
	}
}

// replayOnce uploads pending spooled entries oldest first in bounded windows and
// removes the ones that succeeded. Segment keys are txid-range named, so
// replaying out of band cannot corrupt the replica (idempotent PUTs). A failed
// window stops the pass; the next replay tick retries from the oldest entry.
func (b *Batcher) replayOnce(ctx context.Context) {
	for {
		before := b.spool.Count()
		entries, err := b.spool.LoadLimit(512)
		if err != nil {
			if b.log != nil {
				b.log.Warn("upload spool load failed", "err", err)
			}
			return
		}
		if len(entries) == 0 {
			return
		}
		b.replayEntries(ctx, entries)
		// Stop when a pass makes no progress (the remaining entries are all
		// failing); the next replay tick retries them from the oldest.
		if b.spool.Count() >= before {
			return
		}
	}
}

func (b *Batcher) replayEntries(ctx context.Context, entries []SpoolEntry) {
	for _, e := range entries {
		if ctx.Err() != nil {
			return
		}
		if _, _, err := b.sink.AppendBatch(ctx, e.Scope, e.Epoch, e.Segments); err != nil {
			if errors.Is(err, cell.ErrResourceDeleted) {
				if err := b.spool.Remove(e.ID); err != nil {
					b.spoolErrors.Add(1)
				} else {
					b.discarded.Add(uint64(len(e.Segments)))
				}
				continue
			}
			b.errors.Add(1)
			if b.log != nil {
				b.log.Warn("spooled upload replay failed", "scope", e.Scope.String(), "epoch", e.Epoch, "segments", len(e.Segments), "err", err)
			}
			continue
		}
		b.replayed.Add(1)
		b.batches.Add(1)
		b.segments.Add(uint64(len(e.Segments)))
		_ = b.spool.Remove(e.ID)
	}
}

func (b *Batcher) loop(ctx context.Context, sh *shard) {
	defer close(sh.done)
	ticker := time.NewTicker(b.interval)
	defer ticker.Stop()
	var pending []item
	var pendingBytes int
	flush := func() {
		if len(pending) == 0 {
			return
		}
		b.flushGroup(ctx, pending)
		pending = pending[:0]
		pendingBytes = 0
	}
	for {
		select {
		case <-ctx.Done():
			// Drain remaining queued items, then flush.
			for {
				select {
				case it := <-sh.ch:
					pending = append(pending, it)
				default:
					flush()
					return
				}
			}
		case it := <-sh.ch:
			if it.barrier != nil {
				flush()
				close(it.barrier)
				continue
			}
			pending = append(pending, it)
			pendingBytes += itemBytes(it)
			if len(pending) >= b.maxSegments || pendingBytes >= b.maxBytes {
				flush()
			}
		case <-sh.wake:
			flush()
		case <-ticker.C:
			flush()
		}
	}
}

// flushGroup groups pending items by (scope, epoch) and uploads each group.
func (b *Batcher) flushGroup(ctx context.Context, items []item) {
	type group struct {
		scope    cell.Scope
		epoch    uint64
		segments [][]byte
		items    []item
	}
	var groups []*group
	index := map[string]*group{}
	for _, it := range items {
		key := it.scope.String() + "@" + itoa(it.epoch)
		g := index[key]
		if g == nil {
			g = &group{scope: it.scope, epoch: it.epoch}
			index[key] = g
			groups = append(groups, g)
		}
		g.segments = append(g.segments, itemSegs(it)...)
		g.items = append(g.items, it)
	}
	for _, g := range groups {
		// Bounded retries with backoff: a transient bucket error must not
		// silently drop the async copy of an already-acked write. A permanent
		// failure is counted and logged loudly (see docs/known-issues.md: no
		// durable retry queue yet).
		var err error
		for attempt := 0; attempt < 3; attempt++ {
			if _, _, err = b.sink.AppendBatch(ctx, g.scope, g.epoch, g.segments); err == nil {
				break
			}
			if errors.Is(err, cell.ErrResourceDeleted) {
				break
			}
			if ctx.Err() != nil {
				// Canceled: stop retrying and record the cancellation rather than
				// the sink's transient error (the batch is deferred/dropped either
				// way; only the log/attribution should say "canceled").
				err = ctx.Err()
				break
			}
			select {
			case <-ctx.Done():
			case <-time.After(time.Duration(attempt+1) * 200 * time.Millisecond):
			}
		}
		if errors.Is(err, cell.ErrResourceDeleted) {
			for _, it := range g.items {
				if it.spoolID != 0 && b.spool != nil {
					if err := b.spool.Remove(it.spoolID); err != nil {
						b.spoolErrors.Add(1)
						continue
					}
				}
				b.discarded.Add(1)
			}
		} else if err != nil {
			// A spooled segment is not lost: its file stays for replay. Only a
			// segment we could not persist is really dropped (counted + alerted).
			b.errors.Add(1)
			for _, it := range g.items {
				if it.spoolID != 0 {
					b.deferred.Add(1)
				} else {
					b.dropped.Add(1)
				}
			}
			if b.log != nil {
				b.log.Warn("batch upload failed after retries", "scope", g.scope.String(), "epoch", g.epoch, "segments", len(g.segments), "spooled", g.items[0].spoolID != 0, "err", err)
			}
		} else {
			b.batches.Add(1)
			b.segments.Add(uint64(len(g.segments)))
			for _, it := range g.items {
				if it.spoolID != 0 && b.spool != nil {
					_ = b.spool.Remove(it.spoolID)
				}
			}
		}
		for _, it := range g.items {
			if it.done != nil {
				it.done <- err
			}
		}
	}
}

type pendingGroup struct {
	scope    cell.Scope
	epoch    uint64
	segments [][]byte
	items    []item
}

// EnqueueWait submits a segment and blocks until the covering batch is durable.
// This preserves RPO=0 while batching concurrent writers into one upload.
func (b *Batcher) EnqueueWait(ctx context.Context, s cell.Scope, epoch uint64, seg []byte) error {
	done := make(chan error, 1)
	sh := b.shardFor(s)
	select {
	case sh.ch <- item{scope: s, epoch: epoch, seg: seg, done: done}:
	default:
		// Queue full: upload synchronously (backpressure).
		_, _, err := b.sink.AppendBatch(ctx, s, epoch, [][]byte{seg})
		if err != nil {
			b.errors.Add(1)
			return err
		}
		b.batches.Add(1)
		b.segments.Add(1)
		return nil
	}
	// Ask the loop to flush now; coalesces under concurrency, immediate when idle.
	select {
	case sh.wake <- struct{}{}:
	default:
	}
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// EnqueueBatchWait submits pre-grouped segments as one upload item and blocks
// until that item's covering upload is durable. It lets a snapshot stream many
// parts into a few objects instead of one commit per 8MiB part (ADR-272).
func (b *Batcher) EnqueueBatchWait(ctx context.Context, s cell.Scope, epoch uint64, segs [][]byte) error {
	if len(segs) == 0 {
		return nil
	}
	if len(segs) == 1 {
		return b.EnqueueWait(ctx, s, epoch, segs[0])
	}
	done := make(chan error, 1)
	sh := b.shardFor(s)
	select {
	case sh.ch <- item{scope: s, epoch: epoch, segs: segs, done: done}:
	default:
		// Queue full: upload synchronously (backpressure).
		_, _, err := b.sink.AppendBatch(ctx, s, epoch, segs)
		if err != nil {
			b.errors.Add(1)
			return err
		}
		b.batches.Add(1)
		b.segments.Add(uint64(len(segs)))
		return nil
	}
	select {
	case sh.wake <- struct{}{}:
	default:
	}
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Enqueue submits a segment for batched upload. With a spool attached the
// segment is persisted first, so a failure or a restart defers it instead of
// losing it (ADR-143).
func (b *Batcher) Enqueue(ctx context.Context, s cell.Scope, epoch uint64, seg []byte) {
	var spoolID uint64
	if b.spool != nil {
		id, err := b.spool.Append(s, epoch, [][]byte{seg})
		if err != nil {
			b.spoolErrors.Add(1)
			if b.log != nil {
				b.log.Warn("upload spool append failed", "scope", s.String(), "epoch", epoch, "err", err)
			}
		} else {
			spoolID = id
		}
	}
	sh := b.shardFor(s)
	select {
	case sh.ch <- item{scope: s, epoch: epoch, seg: seg, spoolID: spoolID}:
	default:
		// Queue full: upload synchronously (backpressure, no loss).
		if _, _, err := b.sink.AppendBatch(ctx, s, epoch, [][]byte{seg}); err != nil {
			b.errors.Add(1)
			if spoolID != 0 {
				b.deferred.Add(1)
			} else {
				b.dropped.Add(1)
			}
			if b.log != nil {
				b.log.Warn("sync upload failed", "scope", s.String(), "epoch", epoch, "spooled", spoolID != 0, "err", err)
			}
			return
		}
		b.batches.Add(1)
		b.segments.Add(1)
		if spoolID != 0 {
			_ = b.spool.Remove(spoolID)
		}
	}
}

// Flush pushes a barrier through every shard, so all segments accepted before
// the call are flushed (or spooled on failure), then performs a synchronous
// replay of the durable spool. It is used by graceful shutdown: owners are
// released only after every acknowledged async upload has reached the bucket.
func (b *Batcher) Flush(ctx context.Context) error {
	for _, sh := range b.shards {
		done := make(chan struct{})
		select {
		case sh.ch <- item{barrier: done}:
		case <-ctx.Done():
			return ctx.Err()
		}
		select {
		case <-done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if b.spool != nil {
		b.replayOnce(ctx)
		if n := b.spool.Count(); n != 0 {
			return fmt.Errorf("upload: %d spooled segment(s) still unproven", n)
		}
	}
	return nil
}

// Stop flushes and waits for the loop to finish.
func (b *Batcher) Stop() {
	b.closeOnce.Do(func() {})
	// The loops exit on ctx cancellation; callers pass a cancellable ctx.
	for _, sh := range b.shards {
		<-sh.done
	}
}

// Stats returns batcher counters.
func (b *Batcher) Stats() map[string]uint64 {
	st := map[string]uint64{
		"batches":           b.batches.Load(),
		"segments":          b.segments.Load(),
		"errors":            b.errors.Load(),
		"dropped":           b.dropped.Load(),
		"deferred":          b.deferred.Load(),
		"replayed":          b.replayed.Load(),
		"discarded_deleted": b.discarded.Load(),
	}
	if b.spool != nil {
		st["spool"] = uint64(b.spool.Count())
		st["spool_errors"] = b.spoolErrors.Load()
		st["spool_corrupt"] = b.spool.Corrupt()
		ss := b.spool.Stats()
		st["spool_file_syncs"] = uint64(ss["file_syncs"])
		st["spool_dir_syncs"] = uint64(ss["dir_syncs"])
		st["spool_sync_us"] = uint64(ss["sync_us"] + ss["rename_us"])
	}
	return st
}

func itoa(u uint64) string {
	if u == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for u > 0 {
		i--
		buf[i] = byte('0' + u%10)
		u /= 10
	}
	return string(buf[i:])
}
