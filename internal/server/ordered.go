package server

import (
	"context"
	"fmt"
	"sync"
	"time"

	"cellhive/internal/peer"
)

// orderedDispatcher serializes commit frame writes per scope by start_txid so a
// client may pipeline commits (send chunk N+1 before chunk N is acked) while the
// follower still receives frames in order. Each caller keeps its own ack future;
// only the frame write is ordered.
//
// A scope's baseline is the first start_txid it sees. Out-of-order requests are
// buffered until the missing predecessors arrive. Each frame write happens
// under the scope's lock, so writes are strictly ordered; the ack wait is
// outside the lock and therefore overlapped.
type orderedDispatcher struct {
	mu        sync.Mutex
	scopes    map[string]*scopeQueue
	closed    bool
	ttl       time.Duration
	lastSweep time.Time
}

type scopeQueue struct {
	mu       sync.Mutex
	next     uint64
	buf      map[uint64]*orderedRequest
	lastUsed time.Time
}

type orderedRequest struct {
	end      uint64
	dispatch func() (<-chan peer.CommitAck, error)
	ready    chan struct{}
	future   <-chan peer.CommitAck
	err      error
}

func newOrderedDispatcher() *orderedDispatcher {
	return &orderedDispatcher{scopes: map[string]*scopeQueue{}, ttl: time.Minute}
}

// sweep evicts scopes idle past the TTL and fails their buffered requests, so a
// missing predecessor cannot stall a scope (or grow the map) forever.
func (d *orderedDispatcher) sweep(now time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed || d.ttl <= 0 || now.Sub(d.lastSweep) < d.ttl/2 {
		return
	}
	d.lastSweep = now
	for key, q := range d.scopes {
		q.mu.Lock()
		idle := now.Sub(q.lastUsed) > d.ttl
		if idle {
			for start, req := range q.buf {
				req.err = fmt.Errorf("ordered scope evicted after %s idle", d.ttl)
				close(req.ready)
				delete(q.buf, start)
			}
		}
		q.mu.Unlock()
		if idle {
			delete(d.scopes, key)
		}
	}
}

// Close releases all buffered callers.
func (d *orderedDispatcher) Close() {
	d.mu.Lock()
	d.closed = true
	scopes := make([]*scopeQueue, 0, len(d.scopes))
	for _, q := range d.scopes {
		scopes = append(scopes, q)
	}
	d.mu.Unlock()
	for _, q := range scopes {
		q.mu.Lock()
		for start, req := range q.buf {
			req.err = fmt.Errorf("ordered dispatcher closed")
			close(req.ready)
			delete(q.buf, start)
		}
		q.mu.Unlock()
	}
}

// Do writes one commit's frame, ordered by start_txid within its scope, and
// returns the ack future. The dispatch closure must write the frame
// synchronously and return the ack channel; it is called at most once.
func (d *orderedDispatcher) Do(ctx context.Context, key string, base, start, end uint64, dispatch func() (<-chan peer.CommitAck, error)) (<-chan peer.CommitAck, error) {
	// Opportunistically evict idle scopes (throttled by ttl/2 inside sweep) so a
	// permanent txid gap cannot grow the map or leave buffered callers forever.
	d.sweep(time.Now())

	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil, fmt.Errorf("ordered dispatcher closed")
	}
	q := d.scopes[key]
	if q == nil {
		q = &scopeQueue{next: base, buf: map[uint64]*orderedRequest{}, lastUsed: time.Now()}
		d.scopes[key] = q
	}
	d.mu.Unlock()

	q.mu.Lock()
	q.lastUsed = time.Now()
	if start != q.next {
		req := &orderedRequest{end: end, dispatch: dispatch, ready: make(chan struct{})}
		q.buf[start] = req
		q.mu.Unlock()
		select {
		case <-req.ready:
			return req.future, req.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	q.next = end + 1
	future, err := dispatch()
	if err == nil {
		for {
			next, ok := q.buf[q.next]
			if !ok {
				break
			}
			delete(q.buf, q.next)
			q.next = next.end + 1
			next.future, next.err = next.dispatch()
			close(next.ready)
		}
	}
	q.mu.Unlock()
	return future, err
}
