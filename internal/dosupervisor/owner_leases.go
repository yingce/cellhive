package dosupervisor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"cellhive/internal/bearertoken"
)

// OwnerLeases renews already acquired identities independently of workerd's
// synchronous SQLite event loop. It never claims ownership or changes epochs.
type OwnerLeases struct {
	ctx     context.Context
	cancel  context.CancelFunc
	renew   func(context.Context, RecoveryClaim, time.Duration) (LeaseState, error)
	mu      sync.Mutex
	entries map[string]*ownerLease
}
type LeaseState struct {
	Node   string `json:"node"`
	Epoch  uint64 `json:"epoch"`
	Expiry int64  `json:"expiry_ms"`
}
type ownerLease struct {
	claim  RecoveryClaim
	cancel context.CancelFunc
	mu     sync.Mutex
	state  LeaseState
}

func NewOwnerLeases(ctx context.Context, renew func(context.Context, RecoveryClaim, time.Duration) (LeaseState, error)) *OwnerLeases {
	ctx, cancel := context.WithCancel(ctx)
	return &OwnerLeases{ctx: ctx, cancel: cancel, renew: renew, entries: map[string]*ownerLease{}}
}
func (l *OwnerLeases) Close() {
	l.cancel()
	l.mu.Lock()
	defer l.mu.Unlock()
	for k, e := range l.entries {
		e.cancel()
		delete(l.entries, k)
	}
}
func (l *OwnerLeases) Register(ctx context.Context, c RecoveryClaim, ttl time.Duration) (LeaseState, error) {
	if c.Scope.Class != "__do__" || c.Node == "" || c.Epoch == 0 || ttl <= 0 {
		return LeaseState{}, errors.New("invalid managed DO lease")
	}
	if err := l.ctx.Err(); err != nil {
		return LeaseState{}, err
	}
	// Registration is only invoked after a successful owner claim. An existing
	// identical identity shares its renewal; reads cannot register or resurrect it.
	if state, err := l.Status(c); err == nil {
		return state, nil
	}
	state, err := l.renewChecked(ctx, c, ttl)
	if err != nil {
		return LeaseState{}, err
	}
	child, cancel := context.WithCancel(l.ctx)
	e := &ownerLease{claim: c, cancel: cancel, state: state}
	l.mu.Lock()
	if err = l.ctx.Err(); err != nil {
		l.mu.Unlock()
		cancel()
		return LeaseState{}, err
	}
	if old := l.entries[c.Scope.String()]; old != nil {
		if old.claim.Epoch > c.Epoch || (old.claim.Epoch == c.Epoch && old.claim.Node != c.Node) {
			l.mu.Unlock()
			cancel()
			return LeaseState{}, errors.New("newer DO lease already registered")
		}
		old.cancel()
	}
	l.entries[c.Scope.String()] = e
	l.mu.Unlock()
	go func() {
		defer cancel()
		tick := time.NewTicker(min(10*time.Second, max(10*time.Millisecond, ttl/3)))
		defer tick.Stop()
		for {
			select {
			case <-child.Done():
				return
			case <-tick.C:
				e.mu.Lock()
				expiry := e.state.Expiry
				e.mu.Unlock()
				if expiry <= time.Now().UnixMilli() {
					l.forgetEntry(e)
					return
				}
				attempt, stop := context.WithDeadline(child, time.UnixMilli(expiry))
				next, err := l.renewChecked(attempt, c, ttl)
				stop()
				if err != nil {
					if child.Err() == nil && retryableLeaseError(err) && time.Now().UnixMilli() < expiry {
						continue
					}
					l.forgetEntry(e)
					return
				}
				e.mu.Lock()
				e.state = next
				e.mu.Unlock()
			}
		}
	}()
	return state, nil
}
func (l *OwnerLeases) renewChecked(ctx context.Context, c RecoveryClaim, ttl time.Duration) (LeaseState, error) {
	attempt, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	state, err := l.renew(attempt, c, ttl)
	if err == nil && (state.Node != c.Node || state.Epoch != c.Epoch || state.Expiry <= time.Now().UnixMilli()) {
		err = errors.New("managed DO renewal identity/expiry mismatch")
	}
	return state, err
}
func (l *OwnerLeases) forgetEntry(e *ownerLease) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.entries[e.claim.Scope.String()] == e {
		delete(l.entries, e.claim.Scope.String())
	}
	e.cancel()
}
func (l *OwnerLeases) Forget(c RecoveryClaim) {
	l.mu.Lock()
	e := l.entries[c.Scope.String()]
	l.mu.Unlock()
	if e != nil && e.claim == c {
		l.forgetEntry(e)
	}
}
func (l *OwnerLeases) Status(c RecoveryClaim) (LeaseState, error) {
	if err := l.ctx.Err(); err != nil {
		return LeaseState{}, err
	}
	l.mu.Lock()
	e := l.entries[c.Scope.String()]
	l.mu.Unlock()
	if e == nil || e.claim != c {
		return LeaseState{}, errors.New("DO lease is not managed")
	}
	e.mu.Lock()
	state := e.state
	e.mu.Unlock()
	if state.Expiry <= time.Now().UnixMilli() {
		return LeaseState{}, errors.New("managed DO lease expired")
	}
	return state, nil
}

// RenewDOOwner uses only the cell-agent's existing fenced RenewAs API. Bucket
// credentials remain exclusively with cell-agent.
func (h *HTTPStore) RenewDOOwner(ctx context.Context, c RecoveryClaim, ttl time.Duration) (LeaseState, error) {
	parts := strings.Split(c.Scope.ID, "~")
	if c.Scope.Class != "__do__" || len(parts) != 3 || !strings.HasPrefix(parts[2], "shard") {
		return LeaseState{}, errors.New("invalid DO scope")
	}
	shard, err := strconv.Atoi(strings.TrimPrefix(parts[2], "shard"))
	if err != nil || shard < 0 {
		return LeaseState{}, errors.New("invalid DO shard")
	}
	raw, _ := json.Marshal(map[string]any{"namespace": c.Scope.Namespace, "worker": parts[0], "class": parts[1], "shard": shard, "node": c.Node, "epoch": c.Epoch, "ttl_seconds": max(1, int(ttl/time.Second))})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(h.Base, "/")+"/v1/internal/do/renew", bytes.NewReader(raw))
	if err != nil {
		return LeaseState{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	if err = bearertoken.SetInternal(req, h.Token, "internal"); err != nil {
		return LeaseState{}, err
	}
	resp, err := h.client().Do(req)
	if err != nil {
		return LeaseState{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return LeaseState{}, leaseHTTPError(resp.StatusCode)
	}
	var state LeaseState
	err = json.NewDecoder(io.LimitReader(resp.Body, 4096)).Decode(&state)
	return state, err
}

// Only explicit transport and temporary HTTP errors are retryable. Unknown
// errors, including malformed replies and identity mismatches, fail closed.
type leaseHTTPError int

func (e leaseHTTPError) Error() string { return fmt.Sprintf("DO renewal: HTTP %d", int(e)) }
func retryableLeaseError(err error) bool {
	var status leaseHTTPError
	if errors.As(err, &status) {
		return status == 408 || status == 429 || status >= 500 && status <= 599
	}
	var network net.Error
	return errors.Is(err, context.DeadlineExceeded) || errors.As(err, &network) ||
		errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
}
