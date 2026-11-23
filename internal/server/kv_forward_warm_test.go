package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/owner"
)

func TestForwardedKVAccessDoesNotCreateWarmWindow(t *testing.T) {
	for _, tt := range []struct {
		name, method, path, body string
		status                   int
	}{
		{"put", "POST", "/v1/kv/put?ns=acme&key=k", "v", 200},
		{"get", "GET", "/v1/kv/get?ns=acme&key=k", "", 200},
		{"get-miss", "GET", "/v1/kv/get?ns=acme&key=k", "", 404},
		{"get-batch", "POST", "/v1/kv/get-batch?ns=acme", `{"keys":["k"]}`, 200},
		{"put-denied", "POST", "/v1/kv/put?ns=acme&key=k", "v", 403},
		{"put-failed", "POST", "/v1/kv/put?ns=acme&key=k", "v", 503},
		{"get-failed", "GET", "/v1/kv/get?ns=acme&key=k", "", 503},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s, _ := newTestServer(t)
			s.Cfg.BindingCacheTTL = time.Second
			if err := registerTestBinding(s, "acme", "kv", "KV", "main"); err != nil {
				t.Fatal(err)
			}
			var hits atomic.Int64
			target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				hits.Add(1)
				w.WriteHeader(tt.status)
				_, _ = w.Write([]byte(`{"ok":true,"applied":true,"txid":1}`))
			}))
			defer target.Close()
			other := &owner.Manager{B: s.Bucket, NodeID: "node-2", Session: "s2", Advertise: strings.TrimPrefix(target.URL, "http://"), Role: cell.RoleCellAgent, OwnerTTL: time.Minute}
			sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "main"}
			if _, err := other.Claim(t.Context(), sc, time.Now()); err != nil {
				t.Fatal(err)
			}
			token := mint(t, s, "acme", "kv", "KV")
			for i := 0; i < 3; i++ {
				if code := kvReq(t, s, tt.method, tt.path, token, []byte(tt.body)); code != tt.status {
					t.Fatalf("forwarded status=%d want=%d", code, tt.status)
				}
			}
			if hits.Load() != 3 {
				t.Fatalf("owner HTTP requests=%d want=3", hits.Load())
			}

		})
	}
}
