// Package ownerclient is the owner-resolution client library (ADR-003): a Go
// library that turns a scope into the owner's address + epoch, caches hints, and
// forwards a request to the owner when the caller is not the owner. Callers use
// a logical seed list (mesh service names); any replica answers, and a non-owner
// replica forwards.
package ownerclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"cellhive/internal/cell"
)

// ErrUnowned means no owner record exists for the scope (cold cell).
var ErrUnowned = errors.New("ownerclient: unowned")

// ForwardedHeader marks a request that has already been forwarded once, so a
// non-owner never forwards it again (loop guard).
const ForwardedHeader = "x-cellhive-forwarded"

// Hint is a resolved owner: who owns the scope, where to reach them, and the
// epoch fence.
type Hint struct {
	Node         string
	Role         string
	Address      string
	Epoch        uint64
	Expiry       int64
	ProtoVersion string
}

// Live reports whether the hint's owner lease is unexpired at nowMs.
func (h Hint) Live(nowMs int64) bool { return nowMs < h.Expiry }

type cached struct {
	hint Hint
	at   time.Time
}

// Client resolves owners from a seed list and caches hints per scope.
type Client struct {
	Seeds []string
	Token string
	HTTP  *http.Client
	TTL   time.Duration

	mu    sync.Mutex
	cache map[string]cached
}

// New builds a client. ttl <= 0 uses 1s.
func New(seeds []string, token string, ttl time.Duration) *Client {
	if ttl <= 0 {
		ttl = time.Second
	}
	clean := make([]string, 0, len(seeds))
	for _, s := range seeds {
		if s = strings.TrimSpace(s); s != "" {
			clean = append(clean, strings.TrimRight(s, "/"))
		}
	}
	return &Client{
		Seeds: clean, Token: token,
		HTTP: &http.Client{Timeout: 10 * time.Second},
		TTL:  ttl, cache: map[string]cached{},
	}
}

// Invalidate drops a cached hint for a scope.
func (c *Client) Invalidate(scope string) {
	c.mu.Lock()
	delete(c.cache, scope)
	c.mu.Unlock()
}

// Resolve returns the owner hint for a scope, using a short TTL cache.
func (c *Client) Resolve(ctx context.Context, scope string) (Hint, error) {
	c.mu.Lock()
	if e, ok := c.cache[scope]; ok && time.Since(e.at) < c.TTL {
		c.mu.Unlock()
		return e.hint, nil
	}
	c.mu.Unlock()

	if len(c.Seeds) == 0 {
		return Hint{}, fmt.Errorf("ownerclient: no seeds configured")
	}
	var lastErr error
	for _, seed := range c.Seeds {
		h, err := c.resolveFrom(ctx, seed, scope)
		if err != nil {
			lastErr = err
			continue
		}
		c.mu.Lock()
		c.cache[scope] = cached{hint: h, at: time.Now()}
		c.mu.Unlock()
		return h, nil
	}
	return Hint{}, lastErr
}

type resolveResp struct {
	Owned   bool        `json:"owned"`
	Owner   *cell.Owner `json:"owner,omitempty"`
	Expired bool        `json:"expired"`
}

func (c *Client) resolveFrom(ctx context.Context, seed, scope string) (Hint, error) {
	u := seed + "/v1/internal/resolve?scope=" + url.QueryEscape(scope)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return Hint{}, err
	}
	if c.Token != "" {
		req.Header.Set("x-cellhive-internal-token", c.Token)
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return Hint{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return Hint{}, fmt.Errorf("ownerclient: resolve status %d", resp.StatusCode)
	}
	var rr resolveResp
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&rr); err != nil {
		return Hint{}, err
	}
	if !rr.Owned || rr.Owner == nil || rr.Expired {
		return Hint{}, ErrUnowned
	}
	return Hint{
		Node: rr.Owner.Node, Role: string(rr.Owner.Role), Address: rr.Owner.Address,
		Epoch: rr.Owner.Epoch, Expiry: rr.Owner.Expiry, ProtoVersion: rr.Owner.ProtoVersion,
	}, nil
}

// OwnerURL returns the base URL of an owner hint (adding http:// when the
// address has no scheme).
func OwnerURL(h Hint) string {
	if h.Address == "" {
		return ""
	}
	if strings.Contains(h.Address, "://") {
		return strings.TrimRight(h.Address, "/")
	}
	return "http://" + strings.TrimRight(h.Address, "/")
}

// Forward resolves the owner and sends the request to it, returning the owner's
// response. A stale/mismatched owner (409) invalidates the cache and retries
// once with a fresh hint.
func (c *Client) Forward(ctx context.Context, scope, method, path string, body []byte, header http.Header) (*http.Response, error) {
	h, err := c.Resolve(ctx, scope)
	if err != nil {
		return nil, err
	}
	resp, err := c.forwardTo(ctx, h, method, path, body, header)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusConflict {
		_ = resp.Body.Close()
		c.Invalidate(scope)
		h2, rerr := c.Resolve(ctx, scope)
		if rerr != nil {
			return nil, rerr
		}
		return c.forwardTo(ctx, h2, method, path, body, header)
	}
	return resp, nil
}

func (c *Client) forwardTo(ctx context.Context, h Hint, method, path string, body []byte, header http.Header) (*http.Response, error) {
	base := OwnerURL(h)
	if base == "" {
		return nil, fmt.Errorf("ownerclient: owner has no address")
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	req, err := http.NewRequestWithContext(ctx, method, base+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	for k, vs := range header {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	if c.Token != "" {
		req.Header.Set("x-cellhive-internal-token", c.Token)
	}
	req.Header.Set(ForwardedHeader, "1")
	return c.HTTP.Do(req)
}
