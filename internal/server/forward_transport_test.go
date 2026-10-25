package server

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestForwardTransportReusesConcurrentConnections(t *testing.T) {
	const concurrency = 8
	arrivals := make(chan struct{}, concurrency)
	release := make(chan struct{}, concurrency)
	ctx := t.Context()
	var opened atomic.Int64
	target := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		arrivals <- struct{}{}
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		_, _ = io.WriteString(w, "ok")
	}))
	target.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			opened.Add(1)
		}
	}
	target.Start()
	defer target.Close()
	client := newForwardClient()
	defer client.CloseIdleConnections()
	for round := 0; round < 5; round++ {
		errs := make(chan error, concurrency)
		for i := 0; i < concurrency; i++ {
			go func() {
				req, _ := http.NewRequestWithContext(ctx, http.MethodGet, target.URL, nil)
				resp, err := client.Do(req)
				if err == nil {
					b, e := io.ReadAll(resp.Body)
					resp.Body.Close()
					err = e
					if string(b) != "ok" {
						err = fmt.Errorf("response %q", b)
					}
				}
				errs <- err
			}()
		}
		for i := 0; i < concurrency; i++ {
			select {
			case <-arrivals:
			case <-time.After(5 * time.Second):
				t.Fatal("requests did not reach target concurrently")
			}
		}
		for i := 0; i < concurrency; i++ {
			release <- struct{}{}
		}
		for i := 0; i < concurrency; i++ {
			if err := <-errs; err != nil {
				t.Fatal(err)
			}
		}
	}
	if n := opened.Load(); n != concurrency {
		t.Fatalf("opened %d connections for 5 bursts of %d; want reuse of %d", n, concurrency, concurrency)
	}
	s := &Server{}
	if s.forwardHTTPClient() != s.forwardHTTPClient() {
		t.Fatal("default forwarding client is not shared")
	}
	custom := &http.Client{}
	s.ForwardClient = custom
	if s.forwardHTTPClient() != custom {
		t.Fatal("custom forwarding client ignored")
	}
}
