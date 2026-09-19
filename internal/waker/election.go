// Package waker implements the single fleet waker (docs/timers-and-dispatch.md):
// a bucket-lease leader that, for timers whose owner is dead or unreachable,
// falls back to dispatching them. Owner-resident timers are dispatched locally
// by their owning cell-agent, so the waker only covers the dead-owner gap.
package waker

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"cellhive/internal/bucket"
)

// Key is the bucket key of the fleet waker lease.
const Key = "fleet/waker.json"

// DefaultTTL is how long a waker lease stays valid without renewal.
const DefaultTTL = 10 * time.Second

// Lease records the current waker leader and its expiry.
type Lease struct {
	Node      string `json:"node"`
	Session   string `json:"session"`
	ExpiresMs int64  `json:"expires_ms"`
	UpdatedMs int64  `json:"updated_ms"`
}

// Live reports whether the lease is unexpired at nowMs.
func (l Lease) Live(nowMs int64) bool { return nowMs < l.ExpiresMs }

// Election picks a single waker leader via a conditional-write bucket lease.
type Election struct {
	B       bucket.Bucket
	NodeID  string
	Session string
	TTL     time.Duration
	Now     func() time.Time
}

// NewElection creates a waker election.
func NewElection(b bucket.Bucket, nodeID, session string, ttl time.Duration) *Election {
	if ttl <= 0 {
		ttl = DefaultTTL
	}
	return &Election{B: b, NodeID: nodeID, Session: session, TTL: ttl}
}

func (e *Election) now() time.Time {
	if e.Now != nil {
		return e.Now()
	}
	return time.Now()
}

func (e *Election) holder(ctx context.Context) (Lease, string, bool, error) {
	data, etag, err := e.B.Get(ctx, Key)
	if err != nil {
		if errors.Is(err, bucket.ErrNotFound) {
			return Lease{}, "", false, nil
		}
		return Lease{}, "", false, err
	}
	var l Lease
	if err := json.Unmarshal(data, &l); err != nil {
		return Lease{}, "", false, err
	}
	return l, etag, true, nil
}

// Holder returns the current lease, if any.
func (e *Election) Holder(ctx context.Context) (Lease, bool, error) {
	l, _, ok, err := e.holder(ctx)
	return l, ok, err
}

// Acquire attempts to become (or remain) leader. It succeeds when the lease is
// absent or expired, or already held by this node/session (renewal). ok=false
// means a live peer is leader.
func (e *Election) Acquire(ctx context.Context) (bool, error) {
	now := e.now().UnixMilli()
	data, _ := json.Marshal(Lease{
		Node: e.NodeID, Session: e.Session,
		ExpiresMs: now + e.TTL.Milliseconds(), UpdatedMs: now,
	})
	cur, etag, ok, err := e.holder(ctx)
	if err != nil {
		return false, err
	}
	if !ok {
		if _, err := e.B.ConditionalCreate(ctx, Key, data); err != nil {
			if errors.Is(err, bucket.ErrPrecondition) {
				return false, nil
			}
			return false, err
		}
		return true, nil
	}
	if cur.Node == e.NodeID && cur.Session == e.Session {
		if _, err := e.B.CAS(ctx, Key, data, etag); err != nil {
			if errors.Is(err, bucket.ErrPrecondition) {
				return false, nil
			}
			return false, err
		}
		return true, nil
	}
	if cur.Live(now) {
		return false, nil
	}
	if _, err := e.B.CAS(ctx, Key, data, etag); err != nil {
		if errors.Is(err, bucket.ErrPrecondition) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// StepDown releases the lease if this node/session holds it.
func (e *Election) StepDown(ctx context.Context) error {
	cur, _, ok, err := e.holder(ctx)
	if err != nil {
		return err
	}
	if !ok || cur.Node != e.NodeID || cur.Session != e.Session {
		return nil
	}
	return e.B.Delete(ctx, Key)
}
