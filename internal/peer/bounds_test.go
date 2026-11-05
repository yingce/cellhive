package peer

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestNewHTTPTransportBuildsBoundedDefaultClient(t *testing.T) {
	tr := NewHTTPTransport("", nil)
	if tr.Client == nil {
		t.Fatal("default client is nil")
	}
	if tr.Client == http.DefaultClient {
		t.Fatal("default client is http.DefaultClient (no timeout)")
	}
	if tr.Client.Timeout != defaultPeerHTTPTimeout {
		t.Fatalf("default timeout = %s, want %s", tr.Client.Timeout, defaultPeerHTTPTimeout)
	}
	base, ok := tr.Client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport = %T, want *http.Transport", tr.Client.Transport)
	}
	if base.ResponseHeaderTimeout != defaultPeerResponseHeaderTimeout {
		t.Fatalf("response header timeout = %s, want %s", base.ResponseHeaderTimeout, defaultPeerResponseHeaderTimeout)
	}
}

func TestNewHTTPTransportKeepsProvidedClient(t *testing.T) {
	custom := &http.Client{Timeout: 7 * time.Second}
	tr := NewHTTPTransport("", custom)
	if tr.Client != custom {
		t.Fatal("provided client was replaced")
	}
}

func TestPeerMetadataCallHonorsContextWhenBodyStalls(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", "1000")
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := NewHTTPTransport(jwtTestKey, nil).Held(ctx, srv.URL); err == nil {
		t.Fatal("Held returned nil error after the context deadline")
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("Held took %s, want prompt cancellation", elapsed)
	}
}
