package auth

import (
	"context"
	"crypto/rsa"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// JWKS fetches and caches an OIDC JWKS (JSON Web Key Set) and resolves RSA keys
// by kid. The set is refreshed at most once per TTL.
type JWKS struct {
	URL  string
	HTTP *http.Client
	TTL  time.Duration
	Now  func() time.Time

	mu        sync.Mutex
	keys      map[string]*rsa.PublicKey
	fetchedAt time.Time
	// fetching/fetchDone single-flight the refresh so one slow IdP response does
	// not serialize every JWT verification behind the mutex (stale keys are
	// served while a refresh is in flight).
	fetching  bool
	fetchDone chan struct{}
}

func (p *JWKS) now() time.Time {
	if p.Now != nil {
		return p.Now()
	}
	return time.Now()
}

func (p *JWKS) ttl() time.Duration {
	if p.TTL > 0 {
		return p.TTL
	}
	return 10 * time.Minute
}

// Key returns the RSA public key for a key id, refreshing the set if needed.
//
// The network fetch runs OUTSIDE the mutex: while a refresh is in flight other
// lookups are served from the cached set (stale-while-revalidate) instead of
// blocking on one slow IdP response, and concurrent refreshes are collapsed.
func (p *JWKS) Key(kid string) (*rsa.PublicKey, error) {
	p.mu.Lock()
	if k, ok := p.keys[kid]; ok && p.now().Sub(p.fetchedAt) < p.ttl() {
		p.mu.Unlock()
		return k, nil
	}
	if p.fetching {
		done := p.fetchDone
		stale, haveStale := p.keys[kid]
		p.mu.Unlock()
		if haveStale {
			return stale, nil // never block a verification on a refresh
		}
		<-done
		p.mu.Lock()
		defer p.mu.Unlock()
		if k, ok := p.keys[kid]; ok {
			return k, nil
		}
		return nil, errNoKey
	}
	p.fetching = true
	p.fetchDone = make(chan struct{})
	done := p.fetchDone
	p.mu.Unlock()

	keys, err := p.fetch()

	p.mu.Lock()
	p.fetching = false
	if err == nil {
		p.keys = keys
		p.fetchedAt = p.now()
	}
	k, ok := p.keys[kid]
	close(done)
	p.mu.Unlock()
	switch {
	case err != nil && ok:
		return k, nil // serve stale on refresh failure
	case err != nil:
		return nil, err
	case ok:
		return k, nil
	default:
		return nil, errNoKey
	}
}

// fetch downloads and parses the key set (no locking; the caller publishes it).
func (p *JWKS) fetch() (map[string]*rsa.PublicKey, error) {
	client := p.HTTP
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.URL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("auth: jwks status %d", resp.StatusCode)
	}
	var set struct {
		Keys []struct {
			Kty string `json:"kty"`
			Kid string `json:"kid"`
			N   string `json:"n"`
			E   string `json:"e"`
		} `json:"keys"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&set); err != nil {
		return nil, err
	}
	keys := map[string]*rsa.PublicKey{}
	for _, k := range set.Keys {
		if k.Kty != "RSA" {
			continue
		}
		pub, err := ParseRSAPublicJWK(k.N, k.E)
		if err != nil {
			return nil, err
		}
		keys[k.Kid] = pub
	}
	if len(keys) == 0 {
		return nil, errors.New("auth: jwks has no RSA keys")
	}
	return keys, nil
}
