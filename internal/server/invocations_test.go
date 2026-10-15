package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"cellhive/internal/auth"
	"cellhive/internal/invocation"
)

func TestInvocationQueryFiltersAndNamespaceAuthorization(t *testing.T) {
	s, _ := newTestServer(t)
	s.Owner = nil
	now := time.Now().UnixMilli()
	h := s.Handler()
	for i, item := range []struct{ ns, worker, kind string }{{"acme", "web", "queue"}, {"acme", "web", "cron"}, {"acme", "other", "queue"}, {"foreign", "web", "queue"}} {
		r := invocation.Record{ID: fmt.Sprintf("id-%d", i), LogicalID: fmt.Sprintf("logical-%d", i), Namespace: item.ns, Worker: item.worker, Kind: item.kind, Attempt: 1, StartedMs: now + int64(i), Status: "success", FinishedMs: now + 100, Logs: []invocation.Log{{Level: "log", Message: fmt.Sprintf("line-%d", i), AtMs: now}}}
		raw, _ := json.Marshal(r)
		res := do(t, h, http.MethodPost, "/v1/internal/invocations?ns="+item.ns, "tok", raw)
		if res.Code != 200 {
			t.Fatalf("record=%d %s", res.Code, res.Body.String())
		}
	}
	rr := mustAdmin(t, s.AdminHandler(), http.MethodGet, fmt.Sprintf("/v1/control/invocations?namespace=acme&worker=web&kind=cron&from_ms=%d&to_ms=%d", now, now+10), "")
	var out struct {
		Items []invocation.Record `json:"invocations"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Items) != 1 || out.Items[0].ID != "id-1" || out.Items[0].Logs[0].Message != "line-1" {
		t.Fatalf("filter/query association failed: %s", rr.Body.String())
	}
	secret := []byte("invocation-namespace-test-secret")
	s.AdminAuth = auth.Any{&auth.JWTBearer{HS256Secret: secret}}
	token := signJWT(t, secret, map[string]any{"sub": "alice", "cellhive_ns": []string{"acme"}, "exp": time.Now().Add(time.Hour).Unix()})
	own := adminTokenDo(t, s, http.MethodGet, "/v1/control/invocations?namespace=acme&invocation_id=id-0", token, "")
	if own.Code != 200 || !strings.Contains(own.Body.String(), "line-0") || strings.Contains(own.Body.String(), "line-3") {
		t.Fatalf("own record query=%d %s", own.Code, own.Body.String())
	}
	forbidden := adminTokenDo(t, s, http.MethodGet, "/v1/control/invocations?namespace=foreign&invocation_id=id-3", token, "")
	if forbidden.Code != 403 {
		t.Fatalf("foreign records exposed: %d %s", forbidden.Code, forbidden.Body.String())
	}
}
