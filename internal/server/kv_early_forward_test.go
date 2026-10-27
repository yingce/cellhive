package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/owner"
	"cellhive/internal/ownerclient"
)

type rejectFenceBucket struct {
	bucket.Bucket
	reads atomic.Int64
}

func (b *rejectFenceBucket) Get(ctx context.Context, key string) ([]byte, string, error) {
	if strings.HasSuffix(key, "/resource-fence.v2") {
		b.reads.Add(1)
		return nil, "", context.DeadlineExceeded
	}
	return b.Bucket.Get(ctx, key)
}
func TestKVForwardSkipsOnlyEntryResourceAdmission(t *testing.T) {
	entry := newScopeServer(t)
	target := newScopeServer(t)
	for _, s := range []*Server{entry, target} {
		if err := registerTestBinding(s, "acme", "kv", "KV", "main"); err != nil {
			t.Fatal(err)
		}
	}
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "main"}
	var handled atomic.Int64
	remote := httptest.NewServer(target.scopeAuth("kv")(func(w http.ResponseWriter, r *http.Request) { handled.Add(1); w.WriteHeader(200) }))
	defer remote.Close()
	raw, err := cell.MarshalOwner(cell.Owner{Node: "remote", Session: "s", Epoch: 1, Address: strings.TrimPrefix(remote.URL, "http://"), Expiry: time.Now().Add(time.Minute).UnixMilli()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := entry.Bucket.Put(t.Context(), owner.Key(sc), raw); err != nil {
		t.Fatal(err)
	}
	entry.Cfg.BindingCacheTTL = time.Minute
	if ok, _, err := entry.bindingInfo(t.Context(), "acme", "kv", "KV"); err != nil || !ok {
		t.Fatalf("prime binding: %v %v", ok, err)
	}
	entry.Owner = &owner.Manager{B: entry.Bucket, NodeID: entry.Cfg.NodeID}
	bad := &rejectFenceBucket{Bucket: entry.resourceReplica().B}
	entry.resourceReplica().B = bad
	token := mint(t, entry, "acme", "kv", "KV")
	request := func(tok, path string, forwarded bool) int {
		r := httptest.NewRequest(http.MethodPost, path, strings.NewReader("value"))
		r.Header.Set("x-cellhive-scope-token", tok)
		if forwarded {
			r.Header.Set(ownerclient.ForwardedHeader, "1")
		}
		rr := httptest.NewRecorder()
		entry.scopeAuth("kv")(func(w http.ResponseWriter, r *http.Request) { t.Error("entry executed request") }).ServeHTTP(rr, r)
		if rr.Code >= 500 {
			t.Log(rr.Body.String())
		}
		return rr.Code
	}
	if got := request(token, "/v1/kv/put?ns=acme&key=k", false); got != 200 {
		t.Fatalf("forward=%d", got)
	}
	if bad.reads.Load() != 0 || handled.Load() != 1 {
		t.Fatalf("entry fence=%d target=%d", bad.reads.Load(), handled.Load())
	}
	for _, c := range []struct{ token, path string }{{"bad", "/v1/kv/put?ns=acme&key=k"}, {token, "/v1/kv/put?ns=other&key=k"}} {
		if got := request(c.token, c.path, false); got != 403 {
			t.Fatalf("invalid auth=%d", got)
		}
	}
	if handled.Load() != 1 {
		t.Fatal("unauthorized request reached owner")
	}
	// A blocked resource must still be rejected by the target's full gate.
	if err := target.resourceReplica().BlockResource(t.Context(), sc, "delete"); err != nil {
		t.Fatal(err)
	}
	if got := request(token, "/v1/kv/put?ns=acme&key=k", false); got != 403 {
		t.Fatalf("deleted=%d", got)
	}
	if handled.Load() != 1 || bad.reads.Load() != 0 {
		t.Fatal("deleted request bypassed owner admission")
	}
	// A forwarded request cannot bypass local admission or create a forwarding loop.
	if got := request(token, "/v1/kv/put?ns=acme&key=k", true); got != 503 {
		t.Fatalf("forward loop guard=%d", got)
	}
	if bad.reads.Load() != 1 {
		t.Fatalf("target-side gate was skipped: %d", bad.reads.Load())
	}
}
