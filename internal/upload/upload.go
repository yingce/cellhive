// Package upload batches replication segments into fewer object-store writes
// (group commit). This is the fleet posture's asynchronous bucket uploader.
package upload

import (
	"context"
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
	done    chan error // non-nil for EnqueueWait callers
	spoolID uint64     // >0 when the segment is persisted in the spool
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

	batches  atomic.Uint64
	segments atomic.Uint64
	errors   atomic.Uint64
	dropped  atomic.Uint64

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

// replayOnce uploads every pending spooled entry, oldest first, and removes the
// ones that succeeded. Segment keys are txid-range named, so replaying out of
// band cannot corrupt the replica (idempotent PUTs).
func (b *Batcher) replayOnce(ctx context.Context) {
	entries, err := b.spool.Load()
	if err != nil {
		if b.log != nil {
			b.log.Warn("upload spool load failed", "err", err)
		}
		return
	}
	for _, e := range entries {
		if ctx.Err() != nil {
			return
		}
		if _, _, err := b.sink.AppendBatch(ctx, e.Scope, e.Epoch, e.Segments); err != nil {
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
			pending = append(pending, it)
			pendingBytes += len(it.seg)
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
		g.segments = append(g.segments, it.seg)
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
			if ctx.Err() != nil {
				break // canceled: stop retrying instead of ignoring the signal
			}
			select {
			case <-ctx.Done():
			case <-time.After(time.Duration(attempt+1) * 200 * time.Millisecond):
			}
		}
		if err != nil {
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
		"batches":  b.batches.Load(),
		"segments": b.segments.Load(),
		"errors":   b.errors.Load(),
		"dropped":  b.dropped.Load(),
		"deferred": b.deferred.Load(),
		"replayed": b.replayed.Load(),
	}
	if b.spool != nil {
		st["spool"] = uint64(b.spool.Count())
		st["spool_errors"] = b.spoolErrors.Load()
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
