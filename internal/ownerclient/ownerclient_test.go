package ownerclient

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"cellhive/internal/cell"
)

func resolveServer(t *testing.T, hits *atomic.Int64, owner *cell.Owner, expired bool) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"owned": owner != nil, "owner": owner, "expired": expired,
		})
	}))
}

func TestResolveCachesAndInvalidates(t *testing.T) {
	ctx := context.Background()
	var hits atomic.Int64
	o := &cell.Owner{Node: "node-1", Role: cell.RoleCellAgent, Address: "127.0.0.1:7001", Epoch: 3, Expiry: time.Now().Add(time.Minute).UnixMilli(), ProtoVersion: "v1"}
	srv := resolveServer(t, &hits, o, false)
	defer srv.Close()
	c := New([]string{srv.URL}, "tok", 50*time.Millisecond)

	h, err := c.Resolve(ctx, "app/__kv__/x")
	if err != nil || h.Node != "node-1" || h.Epoch != 3 || h.Address != "127.0.0.1:7001" {
		t.Fatalf("resolve = %+v, %v", h, err)
	}
	if _, err := c.Resolve(ctx, "app/__kv__/x"); err != nil {
		t.Fatalf("cached resolve: %v", err)
	}
	if hits.Load() != 1 {
		t.Fatalf("expected 1 seed hit (cached), got %d", hits.Load())
	}
	c.Invalidate("app/__kv__/x")
	if _, err := c.Resolve(ctx, "app/__kv__/x"); err != nil {
		t.Fatalf("resolve after invalidate: %v", err)
	}
	if hits.Load() != 2 {
		t.Fatalf("expected a second hit after invalidate, got %d", hits.Load())
	}
}

func TestResolveUnowned(t *testing.T) {
	ctx := context.Background()
	var hits atomic.Int64
	srv := resolveServer(t, &hits, nil, false)
	defer srv.Close()
	c := New([]string{srv.URL}, "tok", time.Second)
	if _, err := c.Resolve(ctx, "app/__kv__/x"); !errors.Is(err, ErrUnowned) {
		t.Fatalf("err = %v, want ErrUnowned", err)
	}
}

func TestForwardPostsToOwner(t *testing.T) {
	ctx := context.Background()
	var hits atomic.Int64
	o := &cell.Owner{Node: "owner", Role: cell.RoleCellAgent, Address: "", Epoch: 1, Expiry: time.Now().Add(time.Minute).UnixMilli()}
	resolve := resolveServer(t, &hits, o, false)
	defer resolve.Close()

	var gotPath, gotToken, gotGuard string
	var gotBody []byte
	ownerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotToken = r.Header.Get("x-cellhive-internal-token")
		gotGuard = r.Header.Get(ForwardedHeader)
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer ownerSrv.Close()
	o.Address = ownerSrv.URL // includes http://

	c := New([]string{resolve.URL}, "tok", time.Second)
	resp, err := c.Forward(ctx, "app/__kv__/x", http.MethodPost, "/v1/internal/commit_binary?scope=app%2F__kv__%2Fx&epoch=1", []byte("payload"), http.Header{"x-cellhive-followers": []string{"f1"}})
	if err != nil {
		t.Fatalf("forward: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if gotPath != "/v1/internal/commit_binary" || gotToken != "tok" || gotGuard != "1" || string(gotBody) != "payload" {
		t.Fatalf("owner got path=%q token=%q guard=%q body=%q", gotPath, gotToken, gotGuard, gotBody)
	}
}

func TestForwardRetriesOnceOnConflict(t *testing.T) {
	ctx := context.Background()
	var hits atomic.Int64
	o := &cell.Owner{Node: "owner", Role: cell.RoleCellAgent, Epoch: 1, Expiry: time.Now().Add(time.Minute).UnixMilli()}
	resolve := resolveServer(t, &hits, o, false)
	defer resolve.Close()
	var calls atomic.Int64
	ownerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusConflict) // stale once
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer ownerSrv.Close()
	o.Address = ownerSrv.URL

	c := New([]string{resolve.URL}, "tok", time.Minute)
	resp, err := c.Forward(ctx, "s", http.MethodPost, "/x", nil, nil)
	if err != nil {
		t.Fatalf("forward: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 || calls.Load() != 2 {
		t.Fatalf("status=%d calls=%d, want 200 and 2", resp.StatusCode, calls.Load())
	}
	if hits.Load() < 2 {
		t.Fatalf("conflict should have re-resolved: hits=%d", hits.Load())
	}
}
