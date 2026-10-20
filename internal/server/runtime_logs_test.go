package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"

	"cellhive/internal/config"
	"cellhive/internal/logbuf"
)

func TestRuntimeLogsPullByCursorAndRestart(t *testing.T) {
	var mu sync.Mutex
	session := "first"
	events := []string{"one", "two"}
	runtime := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("x-cellhive-internal-token") != "log-token" {
			http.Error(w, "unauthorized", 401)
			return
		}
		since, _ := strconv.Atoi(r.URL.Query().Get("since"))
		mu.Lock()
		defer mu.Unlock()
		page := map[string]any{"session": session, "events": []any{}}
		items := make([]any, 0)
		for i, message := range events {
			if i+1 <= since {
				continue
			}
			items = append(items, map[string]any{"seq": i + 1, "namespace": "acme", "worker": "web", "event": map[string]any{
				"scriptName": "web", "outcome": "ok", "eventTimestamp": 42,
				"logs": []any{map[string]any{"level": "log", "message": []string{message}, "timestamp": 42}},
			}})
		}
		page["events"] = items
		_ = json.NewEncoder(w).Encode(page)
	}))
	defer runtime.Close()
	s := New(Deps{Cfg: config.Config{TokenInternal: "internal-token", TokenLog: "log-token", AdminToken: "admin-tok"}, Logs: logbuf.New(100, 10)})
	register := func() {
		body, _ := json.Marshal(map[string]any{"session": "runtime-1", "urls": []string{runtime.URL}})
		if rr := do(t, s.Handler(), http.MethodPost, "/v1/internal/logs/runtime", "internal-token", body); rr.Code != 200 {
			t.Fatalf("register = %d %s", rr.Code, rr.Body.String())
		}
	}
	register()
	query := func(want int) {
		t.Helper()
		rr := adminDo(t, s.AdminHandler(), http.MethodGet, "/v1/control/logs?namespace=acme&worker=web", "admin-tok", "")
		if rr.Code != 200 {
			t.Fatalf("query = %d %s", rr.Code, rr.Body.String())
		}
		var result struct {
			Entries []logbuf.Entry `json:"entries"`
		}
		if err := json.Unmarshal(rr.Body.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		if len(result.Entries) != want {
			t.Fatalf("entries=%d want=%d: %s", len(result.Entries), want, rr.Body.String())
		}
	}
	query(2)
	query(2) // repeated poll does not duplicate records
	mu.Lock()
	session = "second"
	events = []string{"after-restart"}
	mu.Unlock()
	query(3) // session change resets the runtime cursor
}
