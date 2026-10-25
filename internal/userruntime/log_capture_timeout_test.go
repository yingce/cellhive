package userruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// Internal dispatch awaits capture policy. A hung policy GET must not hang a
// queue invocation or retain a shared connection forever.
func TestUserRuntimeLogCaptureTimeoutDoesNotBlockDispatch(t *testing.T) {
	workerd, err := FindWorkerd()
	if err != nil {
		t.Fatal(err)
	}
	var cancelled atomic.Bool
	agent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/internal/log-capture":
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"mode":`))
			w.(http.Flusher).Flush()
			<-r.Context().Done()
			cancelled.Store(true)
		case "/v1/internal/worker/bindings":
			w.Write([]byte(`{"bindings":{},"vars":{}}`))
		case "/v1/internal/bundle":
			w.Write([]byte(tenantBundle))
		default:
			http.NotFound(w, r)
		}
	}))
	defer agent.Close()
	internalPort, publicPort := freePort(t), freePort(t)
	cfg, err := Render(t.TempDir(), Config{CellURL: agent.URL, CellToken: "test-token", DispatchToken: "test-token", InternalPort: internalPort, PublicPort: publicPort, PlatformJS: "../../workerd/user-runtime", FacadesJS: "../../workerd/platform/facades.js"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, workerd, cfg) }()
	defer func() { cancel(); <-done }()
	base := fmt.Sprintf("http://127.0.0.1:%d", internalPort)
	client := noProxyClient()
	deadline := time.Now().Add(10 * time.Second)
	for {
		r, e := client.Get(base + "/healthz")
		if e == nil {
			r.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatal(e)
		}
		time.Sleep(50 * time.Millisecond)
	}
	payload := map[string]any{"namespace": "demo", "worker": "consumer", "bundle_sha": "sha1", "queue": "jobs", "messages": []map[string]any{{"id": "m1", "body": "aGVsbG8=", "content_type": "text/plain", "attempts": 1}}}
	raw, _ := json.Marshal(payload)
	req, _ := http.NewRequest(http.MethodPost, base+"/v1/queues/dispatch", bytes.NewReader(raw))
	req.Header.Set("content-type", "application/json")
	req.Header.Set("x-cellhive-internal-token", "test-token")
	requestCtx, requestCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer requestCancel()
	resp, err := client.Do(req.WithContext(requestCtx))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 || !strings.Contains(string(body), "handled:1:hello") {
		t.Fatalf("dispatch=%d %s", resp.StatusCode, body)
	}
	if !cancelled.Load() {
		t.Fatal("dispatch did not cancel the stuck policy request")
	}
}
