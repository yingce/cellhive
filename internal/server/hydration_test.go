package server

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"cellhive/internal/cell"
)

func TestHTTPHydrationIndependentOfBackground(t *testing.T) {
	s := newScopeServer(t)
	defer s.Store.Close()
	ctx := context.Background()
	if _, e := s.Control.CreateApp(ctx, "acme", "ops"); e != nil {
		t.Fatal(e)
	}
	if _, e := s.Control.CreateResource(ctx, "acme", "kv", "main", "acme/__kv__/main", "ops"); e != nil {
		t.Fatal(e)
	}
	tok := mint(t, s, "acme", "kv", "main")
	s.Store.HydrateParallel = 1
	s.Store.HydrateInteractiveParallel = 1
	s.Store.HydrateTimeout = 30 * time.Second
	started, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	s.Store.Hydrate = func(ctx context.Context, sc cell.Scope, _ string) error {
		if sc.ID == "background" {
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}
	go func() {
		_, e := s.Store.Cell(ctx, cell.Scope{Namespace: "acme", Class: "__kv__", ID: "background"})
		done <- e
	}()
	<-started
	ts := httptest.NewServer(s.Handler())
	req, e := http.NewRequest("POST", ts.URL+"/v1/kv/put?ns=acme&key=pool", strings.NewReader("ok"))
	if e != nil {
		t.Fatal(e)
	}
	req.Header.Set("x-cellhive-scope-token", tok)
	// This deadline belongs to the client; net/http does not transfer it to
	// the server's Request.Context. The server must mark interactive work.
	client := &http.Client{Timeout: 300 * time.Millisecond}
	res, requestErr := client.Do(req)
	if res != nil {
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
	}
	close(release)
	if e := <-done; e != nil {
		t.Fatal(e)
	}
	ts.Close()
	if requestErr != nil {
		t.Fatalf("HTTP hydration starved behind background: %v", requestErr)
	}
	if res.StatusCode != 200 {
		t.Fatalf("PUT status=%d", res.StatusCode)
	}
}
