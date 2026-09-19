// Package drain implements the bucket drain token that serializes concurrent
// node shutdowns (docs/cell-protocol.md §7, M-07).
//
// A node that is shutting down must acquire the single global drain token before
// it starts migrating cells and releasing ownership, so two nodes never drain at
// once (e.g. during a rolling upgrade). The token is a bucket object with a
// holder identity and an expiry; a crashed holder's token expires and another
// node can take over.
package drain

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"cellhive/internal/bucket"
)

// TokenKey is the well-known bucket key of the global drain token.
const TokenKey = "fleet/drain-token.json"

// DefaultTTL is how long a held token stays valid without renewal.
const DefaultTTL = 30 * time.Second

// DefaultRetryDelay is the backoff between acquisition attempts while another
// node holds the token.
const DefaultRetryDelay = 5 * time.Second

// Token is the drain token payload.
type Token struct {
	Node      string `json:"node"`
	Session   string `json:"session"`
	ExpiresMs int64  `json:"expires_ms"`
	UpdatedMs int64  `json:"updated_ms"`
}

// Live reports whether the token is unexpired at nowMs.
func (t Token) Live(nowMs int64) bool { return nowMs < t.ExpiresMs }

// Manager acquires and releases the drain token for one node/session.
type Manager struct {
	B          bucket.Bucket
	NodeID     string
	Session    string
	TTL        time.Duration
	RetryDelay time.Duration
	// Now is injectable for tests; defaults to time.Now.
	Now func() time.Time
}

// New creates a drain-token manager.
func New(b bucket.Bucket, nodeID, session string, ttl, retryDelay time.Duration) *Manager {
	if ttl <= 0 {
		ttl = DefaultTTL
	}
	if retryDelay <= 0 {
		retryDelay = DefaultRetryDelay
	}
	return &Manager{B: b, NodeID: nodeID, Session: session, TTL: ttl, RetryDelay: retryDelay}
}

func (m *Manager) now() time.Time {
	if m.Now != nil {
		return m.Now()
	}
	return time.Now()
}

// holder reads the token with its etag. ok is false when no token exists.
func (m *Manager) holder(ctx context.Context) (Token, string, bool, error) {
	data, etag, err := m.B.Get(ctx, TokenKey)
	if err != nil {
		if errors.Is(err, bucket.ErrNotFound) {
			return Token{}, "", false, nil
		}
		return Token{}, "", false, err
	}
	var t Token
	if err := json.Unmarshal(data, &t); err != nil {
		return Token{}, "", false, err
	}
	return t, etag, true, nil
}

// Holder returns the current token, if any.
func (m *Manager) Holder(ctx context.Context) (Token, bool, error) {
	t, _, ok, err := m.holder(ctx)
	return t, ok, err
}

// Acquire attempts to take the token. It succeeds when the token is absent or
// expired (conditional create / CAS), or when this node/session already holds it
// (renewal). It returns ok=false when a live peer holds it.
func (m *Manager) Acquire(ctx context.Context) (bool, error) {
	now := m.now().UnixMilli()
	data, _ := json.Marshal(Token{
		Node: m.NodeID, Session: m.Session,
		ExpiresMs: now + m.TTL.Milliseconds(), UpdatedMs: now,
	})

	cur, etag, ok, err := m.holder(ctx)
	if err != nil {
		return false, err
	}
	if !ok {
		// Absent: conditional create.
		if _, err := m.B.ConditionalCreate(ctx, TokenKey, data); err != nil {
			if errors.Is(err, bucket.ErrPrecondition) {
				return false, nil // lost the race
			}
			return false, err
		}
		return true, nil
	}
	if cur.Node == m.NodeID && cur.Session == m.Session {
		// Held by us: renew.
		if _, err := m.B.CAS(ctx, TokenKey, data, etag); err != nil {
			if errors.Is(err, bucket.ErrPrecondition) {
				return false, nil
			}
			return false, err
		}
		return true, nil
	}
	if cur.Live(now) {
		return false, nil // held by a live peer
	}
	// Expired: steal it.
	if _, err := m.B.CAS(ctx, TokenKey, data, etag); err != nil {
		if errors.Is(err, bucket.ErrPrecondition) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// Wait acquires the token, retrying with RetryDelay until ctx is done.
func (m *Manager) Wait(ctx context.Context) error {
	for {
		ok, err := m.Acquire(ctx)
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(m.RetryDelay):
		}
	}
}

// Release deletes the token if this node/session holds it. It is a no-op when
// the token is absent or held by someone else.
func (m *Manager) Release(ctx context.Context) error {
	cur, _, ok, err := m.holder(ctx)
	if err != nil {
		return err
	}
	if !ok || cur.Node != m.NodeID || cur.Session != m.Session {
		return nil
	}
	// Conditional: never delete a token a newer session has taken over.
	data, etag, err := m.B.Get(ctx, TokenKey)
	if err != nil {
		if errors.Is(err, bucket.ErrNotFound) {
			return nil
		}
		return err
	}
	_ = data
	if err := m.B.ConditionalDelete(ctx, TokenKey, etag); err != nil && !errors.Is(err, bucket.ErrPrecondition) {
		return err
	}
	return nil
}
