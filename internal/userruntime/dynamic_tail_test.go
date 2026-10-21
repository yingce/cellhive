package userruntime

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"
)

// TestUserRuntimeDynamicTailGate covers ADR-238: a runtime captures native Tail
// only while the cell-agent reports an active subscription for the worker, and
// switches the loaded variant when the subscription appears/disappears.
func TestUserRuntimeDynamicTailGate(t *testing.T) {
	if _, err := FindWorkerd(); err != nil {
		t.Skipf("workerd unavailable: %v", err)
	}

	var mu sync.Mutex
	workers := []string{}
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/internal/worker/bindings":
			_, _ = w.Write([]byte(`{"bindings":{},"vars":{}}`))
		case "/v1/internal/bundle":
			_, _ = w.Write([]byte(tenantBundle))
		case "/v1/internal/log-capture":
			mu.Lock()
			body, _ := json.Marshal(map[string]any{"mode": "off", "workers": workers})
			mu.Unlock()
			w.Header().Set("content-type", "application/json")
			_, _ = w.Write(body)
		case "/v1/internal/logs":
			http.Error(w, "unexpected Tail push", http.StatusInternalServerError)
		default:
			http.NotFound(w, r)
		}
	}))
	defer stub.Close()

	internalPort, publicPort := freePort(t), freePort(t)
	capnpPath, err := Render(t.TempDir(), Config{
		CellURL:       stub.URL,
		CellToken:     "test-token",
		DispatchToken: "test-token",
		LogToken:      "test-log",
		InternalPort:  internalPort,
		PublicPort:    publicPort,
		PlatformJS:    "../../workerd/user-runtime",
		FacadesJS:     "../../workerd/platform/facades.js",
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	workerd, _ := FindWorkerd()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = Run(ctx, workerd, capnpPath) }()

	base := fmt.Sprintf("http://127.0.0.1:%d", internalPort)
	client := noProxyClient()
	deadline := time.Now().Add(25 * time.Second)
	for {
		resp, err := client.Get(base + "/healthz")
		if err == nil {
			resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("user-runtime did not become healthy: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}

	dispatch := func() error {
		payload := map[string]any{
			"namespace": "demo", "worker": "consumer", "bundle_sha": "sha1", "queue": "jobs",
			"messages": []map[string]any{{
				"id": "m1", "body": base64.StdEncoding.EncodeToString([]byte("hello")), "content_type": "text/plain", "attempts": 1,
			}},
		}
		body, _ := json.Marshal(payload)
		req, _ := http.NewRequest(http.MethodPost, base+"/v1/queues/dispatch", bytes.NewReader(body))
		req.Header.Set("content-type", "application/json")
		req.Header.Set("x-cellhive-internal-token", "test-token")
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("dispatch status %d", resp.StatusCode)
		}
		return nil
	}

	var since uint64
	var events int
	collect := func() error {
		req, _ := http.NewRequest(http.MethodGet, base+"/v1/internal/logs/events?ns=demo&worker=consumer&since="+strconv.FormatUint(since, 10), nil)
		req.Header.Set("x-cellhive-internal-token", "test-log")
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("Tail query status %d", resp.StatusCode)
		}
		var page struct {
			Events []struct {
				Seq uint64 `json:"seq"`
			} `json:"events"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&page); err != nil {
			return err
		}
		for _, item := range page.Events {
			since = item.Seq
			events++
		}
		return nil
	}
	settle := func(d time.Duration) {
		end := time.Now().Add(d)
		for time.Now().Before(end) {
			_ = collect()
			time.Sleep(50 * time.Millisecond)
		}
	}

	// No subscription: dispatch succeeds, no Tail event is produced.
	if err := dispatch(); err != nil {
		t.Fatalf("dispatch without subscription: %v", err)
	}
	settle(600 * time.Millisecond)
	if events != 0 {
		t.Fatalf("captured %d Tail events without a subscription, want 0", events)
	}

	// Subscription appears: the next dispatch loads the tail-enabled variant.
	mu.Lock()
	workers = []string{"demo/consumer"}
	mu.Unlock()
	time.Sleep(1200 * time.Millisecond) // capture poll interval
	if err := dispatch(); err != nil {
		t.Fatalf("dispatch with subscription: %v", err)
	}
	waitUntil := time.Now().Add(5 * time.Second)
	for events == 0 && time.Now().Before(waitUntil) {
		_ = collect()
		time.Sleep(100 * time.Millisecond)
	}
	if events == 0 {
		t.Fatal("no Tail events captured after the subscription appeared")
	}
	settle(500 * time.Millisecond) // drain trailing events from this variant

	// Subscription disappears: the next dispatch switches back to the no-tail
	// variant and produces no further events.
	mu.Lock()
	workers = []string{}
	mu.Unlock()
	time.Sleep(1200 * time.Millisecond)
	before := events
	if err := dispatch(); err != nil {
		t.Fatalf("dispatch after unsubscribe: %v", err)
	}
	settle(800 * time.Millisecond)
	if events != before {
		t.Fatalf("captured %d Tail events after unsubscribe, want 0", events-before)
	}
}
