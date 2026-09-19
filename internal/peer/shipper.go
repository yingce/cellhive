package peer

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/telemetry"

	"go.opentelemetry.io/otel/trace"
)

// ShipBatcher coalesces concurrent fleet commits destined for the same follower
// set into one framed AppendBatch per flush, so N commits pay one owner→follower
// round trip and one follower fsync instead of N.
//
// Each shard is a single ordered lane. The accumulator batches items, and a
// sender dispatches up to `pipeline` batches in flight on that lane at once
// (celld CELLD_LOG_PIPELINE). Frames are written in dispatch order, so the
// follower still applies them in order; only the ack wait is pipelined, which
// hides the network RTT on high-latency links.
type ShipBatcher struct {
	t           Transport
	shards      []shipShard
	maxSegments int
	maxBytes    int
	interval    time.Duration
	pipeline    int
	done        chan struct{}

	// Adaptive hedge (ADR-164, celld CELLD_LOG_HEDGE_MS): the primary follower
	// gets the append immediately; a second copy goes to the next follower if no
	// ack arrives within the wait. hedgeMS: <0 adaptive (default), 0 single copy
	// (no hedge copy), >0 fixed milliseconds. Appends are idempotent per
	// sequence, so copies are safe.
	hedgeMS    int
	hedgeMaxMS int

	latMu   sync.Mutex
	latRing [32]time.Duration
	latN    int
	latI    int

	// Metrics (ADR-165): how often a hedge copy was sent and how often it won
	// the ack race (i.e. the primary missed the wait).
	hedgeFired atomic.Int64
	hedgeWon   atomic.Int64

	// shipBytes counts segment bytes written to followers (per copy).
	shipBytes atomic.Int64
}

// ReplicationBytes reports bytes shipped to followers for /metrics.
func (b *ShipBatcher) ReplicationBytes() int64 { return b.shipBytes.Load() }

func totalBytes(segs [][]byte) int {
	n := 0
	for _, s := range segs {
		n += len(s)
	}
	return n
}

// HedgeStats reports hedge-copy counters for /metrics.
func (b *ShipBatcher) HedgeStats() (fired, won int64) {
	return b.hedgeFired.Load(), b.hedgeWon.Load()
}

type shipShard struct {
	ch     chan shipItem
	wake   chan struct{}
	groups chan []shipItem
}

type shipItem struct {
	scope     cell.Scope
	epoch     uint64
	followers []string
	fkey      string
	seg       []byte
	done      chan shipResult
}

type shipResult struct {
	ackedBy string
	err     error
}

// CommitAck is the resolved outcome of one commit's frame.
type CommitAck struct {
	AckedBy string
	Err     error
}

// NewShipBatcher builds a batch shipper. interval is the group-commit window,
// parallel sets the number of ordered shards, and pipeline is how many batches
// may be in flight on one shard (>=1).
func NewShipBatcher(t Transport, maxSegments, maxBytes int, interval time.Duration, parallel, pipeline, hedgeMS, hedgeMaxMS int) *ShipBatcher {
	if maxSegments <= 0 {
		maxSegments = 128
	}
	if maxBytes <= 0 {
		maxBytes = 4 << 20
	}
	if interval <= 0 {
		interval = time.Millisecond
	}
	if parallel <= 0 {
		parallel = 1
	}
	if pipeline <= 0 {
		pipeline = 1
	}
	b := &ShipBatcher{
		t:           t,
		maxSegments: maxSegments,
		maxBytes:    maxBytes,
		interval:    interval,
		pipeline:    pipeline,
		hedgeMS:     hedgeMS,
		hedgeMaxMS:  hedgeMaxMS,
		done:        make(chan struct{}),
	}
	b.shards = make([]shipShard, parallel)
	for i := range b.shards {
		b.shards[i] = shipShard{
			ch:     make(chan shipItem, 8192/parallel),
			wake:   make(chan struct{}, 1),
			groups: make(chan []shipItem, 64),
		}
	}
	return b
}

// Start runs an accumulator and an ordered sender per shard.
func (b *ShipBatcher) Start(ctx context.Context) {
	var wg sync.WaitGroup
	for i := range b.shards {
		shard := &b.shards[i]
		wg.Add(2)
		go func() {
			defer wg.Done()
			b.loop(ctx, shard)
			close(shard.groups)
		}()
		go func() {
			defer wg.Done()
			b.sender(ctx, shard)
		}()
	}
	go func() {
		wg.Wait()
		close(b.done)
	}()
}

// Stop waits for the loops to finish (cancel the Start ctx first).
func (b *ShipBatcher) Stop() { <-b.done }

// Ship replicates one segment, blocking until its covering batch is acknowledged
// by at least one follower (quorum 1).
func (b *ShipBatcher) Ship(ctx context.Context, followers []string, scope cell.Scope, epoch uint64, seg []byte) (string, error) {
	if len(followers) == 0 {
		return "", fmt.Errorf("peer: no followers")
	}
	fkey := followersKey(followers)
	key := scope.String() + "@" + itoa(epoch) + "#" + fkey
	shard := &b.shards[shardIndex(key, len(b.shards))]
	done := make(chan shipResult, 1)
	it := shipItem{scope: scope, epoch: epoch, followers: followers, fkey: fkey, seg: seg, done: done}
	select {
	case shard.ch <- it:
	default:
		// Queue full: replicate synchronously (backpressure).
		return b.replicate(ctx, followers, scope, epoch, [][]byte{seg})
	}
	select {
	case shard.wake <- struct{}{}:
	default:
	}
	select {
	case r := <-done:
		return r.ackedBy, r.err
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

// ShipNow sends an already-grouped LTX segment without waiting in the owner
// group-commit queue. SQL capture uses this because the segment already covers
// many transactions; queueing it again only adds latency.
func (b *ShipBatcher) ShipNow(ctx context.Context, followers []string, scope cell.Scope, epoch uint64, seg []byte) (string, error) {
	if len(followers) == 0 {
		return "", fmt.Errorf("peer: no followers")
	}
	return b.replicate(ctx, followers, scope, epoch, [][]byte{seg})
}

// ShipNowAsync writes an already-grouped LTX segment's frame(s) synchronously
// (so callers can order writes) and returns a future for the follower ack.
// It requires an AsyncTransport; callers must preserve per-scope order.
func (b *ShipBatcher) ShipNowAsync(ctx context.Context, followers []string, scope cell.Scope, epoch uint64, seg []byte) (<-chan CommitAck, error) {
	if len(followers) == 0 {
		return nil, fmt.Errorf("peer: no followers")
	}
	at, ok := b.t.(AsyncTransport)
	if !ok {
		return nil, fmt.Errorf("peer: transport does not support async ship")
	}
	out := make(chan CommitAck, 1)
	// Write the primary synchronously so callers can order lane writes; the
	// race (and any hedge copy) continues in the background.
	race := b.beginRace(followers, b.asyncLaunch(ctx, at, followers, scope, epoch, [][]byte{seg}))
	go func() {
		ackedBy, err := race.wait(ctx)
		out <- CommitAck{AckedBy: ackedBy, Err: err}
	}()
	return out, nil
}

// asyncLaunch writes one follower's frame synchronously (preserving lane order)
// and returns a channel for its ack, recording the latency for the adaptive
// hedge window.
func (b *ShipBatcher) asyncLaunch(ctx context.Context, at AsyncTransport, followers []string, scope cell.Scope, epoch uint64, segs [][]byte) func(i int) (<-chan error, error) {
	segBytes := totalBytes(segs)
	return func(i int) (<-chan error, error) {
		ch, err := at.AppendBatchAsync(ctx, followers[i], scope, epoch, segs)
		if err != nil {
			return nil, err
		}
		b.shipBytes.Add(int64(segBytes))
		out := make(chan error, 1)
		sctx, span := telemetry.Tracer().Start(ctx, "peer.append",
			trace.WithSpanKind(trace.SpanKindClient),
			trace.WithAttributes(telemetry.ScopeAttrs(scope.Namespace, scope.Class, scope.ID, scope.String())...))
		go func() {
			t0 := time.Now()
			e := <-ch
			b.recordAppend(time.Since(t0))
			telemetry.End(span, e)
			_ = sctx
			out <- e
		}()
		return out, nil
	}
}

func (b *ShipBatcher) loop(ctx context.Context, shard *shipShard) {
	ticker := time.NewTicker(b.interval)
	defer ticker.Stop()
	var pending []shipItem
	var nbytes int
	drain := func() {
		for {
			select {
			case it := <-shard.ch:
				pending = append(pending, it)
				nbytes += len(it.seg)
			default:
				return
			}
		}
	}
	flush := func() {
		if len(pending) == 0 {
			return
		}
		batch := pending
		pending = nil
		nbytes = 0
		select {
		case shard.groups <- batch:
		case <-ctx.Done():
			// Shutdown: resolve the batch inline so waiters are released.
			b.flush(ctx, batch)
		}
	}
	for {
		select {
		case <-ctx.Done():
			drain()
			flush()
			return
		case it := <-shard.ch:
			pending = append(pending, it)
			nbytes += len(it.seg)
			if len(pending) >= b.maxSegments || nbytes >= b.maxBytes {
				flush()
			}
		case <-shard.wake:
			drain()
			flush()
		case <-ticker.C:
			drain()
			flush()
		}
	}
}

// sender drains the shard's batches in order, keeping up to b.pipeline in
// flight. Frame order is preserved because dispatchItems writes each batch's
// frames before the sender moves to the next batch.
func (b *ShipBatcher) sender(ctx context.Context, shard *shipShard) {
	var inflight []<-chan struct{}
	for items := range shard.groups {
		if len(inflight) >= b.pipeline {
			<-inflight[0]
			inflight = inflight[1:]
		}
		inflight = append(inflight, b.dispatchItems(ctx, items))
	}
	for _, d := range inflight {
		<-d
	}
}

func (b *ShipBatcher) dispatchItems(ctx context.Context, items []shipItem) <-chan struct{} {
	type group struct {
		scope     cell.Scope
		epoch     uint64
		followers []string
		segs      [][]byte
		items     []shipItem
	}
	var groups []*group
	index := map[string]*group{}
	for _, it := range items {
		key := it.scope.String() + "@" + itoa(it.epoch) + "#" + it.fkey
		g := index[key]
		if g == nil {
			g = &group{scope: it.scope, epoch: it.epoch, followers: it.followers}
			index[key] = g
			groups = append(groups, g)
		}
		g.segs = append(g.segs, it.seg)
		g.items = append(g.items, it)
	}
	resolved := make(chan struct{}, len(groups))
	for _, g := range groups {
		b.dispatchGroup(ctx, g.followers, g.scope, g.epoch, g.segs, g.items, resolved)
	}
	all := make(chan struct{})
	go func() {
		for range groups {
			<-resolved
		}
		close(all)
	}()
	return all
}

// dispatchGroup writes one batch to the followers and signals `resolved` once
// every covered item has a result. With an AsyncTransport the frames are written
// synchronously (so ordering holds) while the acks are collected in the
// background, letting the sender pipeline the next batch.
func (b *ShipBatcher) dispatchGroup(ctx context.Context, followers []string, scope cell.Scope, epoch uint64, segs [][]byte, items []shipItem, resolved chan<- struct{}) {
	at, async := b.t.(AsyncTransport)
	if !async {
		go func() {
			ackedBy, err := b.replicate(ctx, followers, scope, epoch, segs)
			for _, it := range items {
				it.done <- shipResult{ackedBy: ackedBy, err: err}
			}
			resolved <- struct{}{}
		}()
		return
	}
	// beginRace runs in the sender goroutine: the primary frame is written here,
	// in dispatch order, so the lane applies batches in order even with hedging.
	race := b.beginRace(followers, b.asyncLaunch(ctx, at, followers, scope, epoch, segs))
	go func() {
		defer func() { resolved <- struct{}{} }()
		ackedBy, err := race.wait(ctx)
		for _, it := range items {
			it.done <- shipResult{ackedBy: ackedBy, err: err}
		}
	}()
}

// flush resolves a batch inline; used only on shutdown to release waiters.
func (b *ShipBatcher) flush(ctx context.Context, items []shipItem) {
	<-b.dispatchItems(ctx, items)
}

// hedgeEnabled reports whether a second (hedge) copy may be sent. With
// hedgeMS==0 only the primary follower is used (single copy, celld semantics);
// a failed primary still fails over to the next follower in order.
func (b *ShipBatcher) hedgeEnabled() bool { return b.hedgeMS != 0 }

// recordAppend keeps a rolling window of recent ack latencies for the adaptive
// hedge wait.
func (b *ShipBatcher) recordAppend(d time.Duration) {
	b.latMu.Lock()
	b.latRing[b.latI] = d
	b.latI = (b.latI + 1) % len(b.latRing)
	if b.latN < len(b.latRing) {
		b.latN++
	}
	b.latMu.Unlock()
}

// recentMax is the slowest append in the current window (0 when unsampled).
func (b *ShipBatcher) recentMax() time.Duration {
	b.latMu.Lock()
	defer b.latMu.Unlock()
	var m time.Duration
	for i := 0; i < b.latN; i++ {
		if b.latRing[i] > m {
			m = b.latRing[i]
		}
	}
	return m
}

// hedgeWait derives the duplicate-copy delay. Adaptive (celld semantics): four
// times the slowest recent append, at least 250ms, capped by hedgeMaxMS so a
// loaded fleet does not send copies for honest slow appends.
func (b *ShipBatcher) hedgeWait() time.Duration {
	if b.hedgeMS > 0 {
		return time.Duration(b.hedgeMS) * time.Millisecond
	}
	w := 4 * b.recentMax()
	if w < 250*time.Millisecond {
		w = 250 * time.Millisecond
	}
	if max := time.Duration(b.hedgeMaxMS) * time.Millisecond; max > 0 && w > max {
		w = max
	}
	return w
}

// followersRace coordinates a hedged append across an ordered follower list.
// beginRace writes follower 0 synchronously in the caller's goroutine (so the
// async lane keeps batch write order), and launches the remaining copies on a
// timer or on failure; wait blocks for the first successful ack.
type followersRace struct {
	b         *ShipBatcher
	followers []string
	launch    func(i int) (<-chan error, error)
	results   chan raceResult
	next      int
	pending   int
	lastErr   error
	timer     *time.Timer
	timerC    <-chan time.Time
	hedged    map[int]bool
}

type raceResult struct {
	idx  int
	addr string
	err  error
}

func (b *ShipBatcher) beginRace(followers []string, launch func(i int) (<-chan error, error)) *followersRace {
	r := &followersRace{
		b: b, followers: followers, launch: launch,
		results: make(chan raceResult, len(followers)),
	}
	if r.startNext(false) && b.hedgeEnabled() && len(followers) > 1 {
		r.timer = time.NewTimer(b.hedgeWait())
		r.timerC = r.timer.C
	}
	return r
}

func (r *followersRace) startNext(hedged bool) bool {
	for r.next < len(r.followers) {
		i := r.next
		r.next++
		ch, err := r.launch(i)
		if err != nil {
			r.lastErr = err
			continue
		}
		if hedged {
			if r.hedged == nil {
				r.hedged = map[int]bool{}
			}
			r.hedged[i] = true
			r.b.hedgeFired.Add(1)
		}
		r.pending++
		go func(idx int, addr string) { r.results <- raceResult{idx, addr, <-ch} }(i, r.followers[i])
		return true
	}
	return false
}

func (r *followersRace) wait(ctx context.Context) (string, error) {
	if r.timer != nil {
		defer r.timer.Stop()
	}
	if r.pending == 0 {
		return "", fmt.Errorf("peer: no usable follower: %w", r.lastErr)
	}
	for {
		select {
		case res := <-r.results:
			r.pending--
			if res.err == nil {
				if r.hedged[res.idx] {
					r.b.hedgeWon.Add(1)
				}
				return res.addr, nil
			}
			r.lastErr = res.err
			if r.next < len(r.followers) {
				r.startNext(false)
				if r.timerC != nil {
					r.timer.Reset(r.b.hedgeWait())
				}
			} else if r.pending == 0 {
				return "", fmt.Errorf("peer: all followers failed: %w", r.lastErr)
			}
		case <-r.timerC:
			if r.next < len(r.followers) {
				r.startNext(true)
				r.timer.Reset(r.b.hedgeWait())
			} else {
				r.timer.Stop()
				r.timerC = nil
			}
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
}

// raceFollowers is the blocking form used by the non-async dispatch path.
func (b *ShipBatcher) raceFollowers(ctx context.Context, followers []string, launch func(i int) (<-chan error, error)) (string, error) {
	return b.beginRace(followers, launch).wait(ctx)
}

// replicate sends the batch to the followers and returns the first acker.
func (b *ShipBatcher) replicate(ctx context.Context, followers []string, scope cell.Scope, epoch uint64, segments [][]byte) (string, error) {
	segs := totalBytes(segments)
	return b.raceFollowers(ctx, followers, func(i int) (<-chan error, error) {
		addr := followers[i]
		ch := make(chan error, 1)
		b.shipBytes.Add(int64(segs))
		sctx, span := telemetry.Tracer().Start(ctx, "peer.append",
			trace.WithSpanKind(trace.SpanKindClient),
			trace.WithAttributes(telemetry.ScopeAttrs(scope.Namespace, scope.Class, scope.ID, scope.String())...))
		go func() {
			t0 := time.Now()
			err := b.t.AppendBatch(sctx, addr, scope, epoch, segments)
			b.recordAppend(time.Since(t0))
			telemetry.End(span, err)
			ch <- err
		}()
		return ch, nil
	})
}

func followersKey(followers []string) string {
	if len(followers) == 1 {
		return followers[0]
	}
	cp := append([]string(nil), followers...)
	sort.Strings(cp)
	return strings.Join(cp, ",")
}

func shardIndex(key string, n int) int {
	var h uint32 = 2166136261
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= 16777619
	}
	return int(h % uint32(n))
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
