package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"cellhive/internal/cellstore"
	"cellhive/internal/control"
	"cellhive/internal/server"
)

type authorityProjectionSource struct {
	key    string
	builds int
}

func (s *authorityProjectionSource) AuthorityKey() string { return s.key }
func (s *authorityProjectionSource) Projection(context.Context) (control.Projection, error) {
	s.builds++
	return control.Projection{ETag: s.key}, nil
}

func TestProjectionCacheInvalidatesKnownOwnerChange(t *testing.T) {
	s := &authorityProjectionSource{key: "node-a/session-a/1"}
	c := &projectionCache{src: s, ttl: time.Hour}
	if _, err := c.Projection(t.Context()); err != nil {
		t.Fatal(err)
	}
	s.key = "node-a/session-b/2"
	p, err := c.Projection(t.Context())
	if err != nil || p.ETag != s.key || s.builds != 2 {
		t.Fatalf("cached old owner: etag=%q builds=%d err=%v", p.ETag, s.builds, err)
	}
}

func TestProjectionCacheCanceledWaitDoesNotReturnExpiredView(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	c := &projectionCache{src: &countingProjectionSource{}, ttl: time.Second, proj: &control.Projection{ETag: "old"}, at: time.Now().Add(-time.Hour), building: make(chan struct{})}
	if _, err := c.Projection(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("expired fallback on cancellation: %v", err)
	}
}

func TestProjectionCacheFirstSuccessThenAuthorityFailure(t *testing.T) {
	cs, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer cs.Close()
	s := server.New(server.Deps{Store: cs, Control: control.New(cs, nil)})
	r := s.BackgroundControl()
	c := &projectionCache{src: r, ttl: time.Hour}
	if _, err := c.Projection(t.Context()); err != nil {
		t.Fatal(err)
	}
	s.Control = nil
	if _, err := r.ProjectionStrict(t.Context()); err == nil {
		t.Fatal("fixture did not lose authority")
	}
	if _, err := c.Projection(t.Context()); err == nil {
		t.Fatal("unread identity matched failed identity and reused old view")
	}
}

func TestCronEnricherDoesNotBypassProjectionInvalidation(t *testing.T) {
	s := &countingProjectionSource{proj: dispatchProjection()}
	c := &projectionCache{src: s, ttl: time.Hour}
	e := &cronEnricher{proj: c}
	if _, err := e.lookup(t.Context(), "acme", "web"); err != nil {
		t.Fatal(err)
	}
	c.at = time.Now().Add(-2 * time.Hour)
	unavailable := errors.New("authority unavailable")
	s.setErr(unavailable)
	if _, err := e.lookup(t.Context(), "acme", "web"); !errors.Is(err, unavailable) {
		t.Fatalf("second cache hid projection failure: %v", err)
	}
}
