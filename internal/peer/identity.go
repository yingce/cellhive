package peer

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"sync"

	"cellhive/internal/cell"
)

const TargetNodeHeader = "X-Cellhive-Target-Node"
const TargetInstanceHeader = "X-Cellhive-Target-Instance"

type identityKey struct{}

func WithIdentity(ctx context.Context, id cell.PeerIdentity) context.Context {
	return context.WithValue(ctx, identityKey{}, id)
}

// Targets is populated once, before opening the fleet log. Neither a lease
// refresh nor an address reuse can silently change an acknowledged ensemble.
type Targets struct {
	mu  sync.RWMutex
	ids map[string]cell.PeerIdentity
}

func (t *Targets) Bind(refs map[string]cell.PeerIdentity) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	for addr, id := range refs {
		if old, ok := t.ids[addr]; ok && old != id {
			return fmt.Errorf("peer identity changed for %s", addr)
		}
	}
	if t.ids == nil {
		t.ids = make(map[string]cell.PeerIdentity)
	}
	for addr, id := range refs {
		t.ids[addr] = id
	}
	return nil
}
func (t *Targets) Get(addr string) cell.PeerIdentity {
	if t == nil {
		return cell.PeerIdentity{}
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.ids[addr]
}
func targetIdentity(ctx context.Context, targets *Targets, addr string) cell.PeerIdentity {
	if id, ok := ctx.Value(identityKey{}).(cell.PeerIdentity); ok {
		return id
	}
	return targets.Get(addr)
}
func setIdentity(h http.Header, id cell.PeerIdentity) {
	if id.Node != "" {
		h.Set(TargetNodeHeader, id.Node)
		h.Set(TargetInstanceHeader, id.Instance)
	}
}
func verifyIdentity(h http.Header, id cell.PeerIdentity) error {
	if id.Node != "" && (h.Get(TargetNodeHeader) != id.Node || h.Get(TargetInstanceHeader) != id.Instance) {
		return fmt.Errorf("peer missing matching persistent identity proof")
	}
	return nil
}
func (t *HTTPTransport) do(req *http.Request, addr string) (*http.Response, error) {
	id := targetIdentity(req.Context(), t.Targets, addr)
	setIdentity(req.Header, id)
	resp, err := t.Client.Do(req)
	if err != nil {
		return nil, err
	}
	if err = verifyIdentity(resp.Header, id); err != nil {
		resp.Body.Close()
		return nil, err
	}
	return resp, nil
}

func (t *HTTPTransport) Prepare(ctx context.Context, addr, session string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, trimSlash(addr)+"/v1/peer/prepare?session="+url.QueryEscape(session), nil)
	if err != nil {
		return err
	}
	req.Header.Set("x-cellhive-internal-token", t.Token)
	resp, err := t.do(req, addr)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("peer prepare: status %d", resp.StatusCode)
	}
	return nil
}
