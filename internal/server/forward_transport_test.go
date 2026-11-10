package server

import (
	"bytes"
	"cellhive/internal/control"
	"context"
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

func TestImportForwardOutlivesOrdinaryTimeoutAndHonorsCancel(t *testing.T) {
	for _, cancelRequest := range []bool{false, true} {
		t.Run(fmt.Sprint(cancelRequest), func(t *testing.T) {
			entered := make(chan struct{})
			target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

				if cancelRequest {
					if _, err := io.Copy(io.Discard, r.Body); err != nil {
						t.Error(err)
					}
					close(entered)
					select {
					case <-r.Context().Done():
					case <-time.After(3 * time.Second):
						t.Error("upstream did not observe cancellation")
					}
					return
				}
				close(entered)
				time.Sleep(60 * time.Millisecond)
				body, err := io.ReadAll(r.Body)
				if err != nil || len(body) != 2<<20 {
					t.Errorf("stream body len=%d err=%v", len(body), err)
				}
				_, _ = io.WriteString(w, `{"imported":true}`)
			}))
			defer target.Close()
			s, om, _ := newControlForwardServer(t, t.TempDir(), "entry")
			om.NodeID = "target"
			om.Session = "target-session"
			om.AdminAdvertise = target.URL
			if _, err := om.Claim(t.Context(), control.Scope(), time.Now()); err != nil {
				t.Fatal(err)
			}
			s.Cfg.NodeID = "entry"
			client := newForwardClient()
			client.Timeout = 10 * time.Millisecond
			s.ForwardClient = client
			defer client.CloseIdleConnections()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			rr := httptest.NewRecorder()
			req := httptest.NewRequest("POST", "/v1/control/cell/import?scope=acme/Counter/id", bytes.NewReader(make([]byte, 2<<20))).WithContext(ctx)
			done := make(chan struct{})
			go func() {
				defer close(done)
				if !s.forwardAdmin(rr, req, control.Scope(), nil) {
					t.Error("not forwarded")
				}
			}()
			select {
			case <-entered:
			case <-time.After(3 * time.Second):
				t.Fatal("upstream not reached")
			}
			if cancelRequest {
				cancel()
			}
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Fatal("forward did not terminate")
			}
			if cancelRequest {
				if rr.Code != 502 {
					t.Fatalf("cancel status %d", rr.Code)
				}
			} else if rr.Code != 200 {
				t.Fatalf("slow import %d %s", rr.Code, rr.Body)
			}
			if client.Timeout != 10*time.Millisecond {
				t.Fatal("shared client timeout changed")
			}
		})
	}
}
