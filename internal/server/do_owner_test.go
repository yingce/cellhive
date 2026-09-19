package server

import (
	"cellhive/internal/lease"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cellstore"
	"cellhive/internal/config"
	"cellhive/internal/control"
	"cellhive/internal/owner"
)

func newDOServer(t *testing.T) *Server {
	t.Helper()
	cs, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatalf("cellstore: %v", err)
	}
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatalf("bucket: %v", err)
	}
	return New(Deps{
		Cfg:     config.Config{NodeID: "cell", TokenInternal: "tok", ScopeSecret: "s"},
		Bucket:  b,
		Control: control.New(cs, nil),
		Owner:   &owner.Manager{B: b, NodeID: "cell", Session: "s", OwnerTTL: time.Minute},
		Log:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
}

// TestDOOwnerEndpoints covers the cell-agent-driven DO claim/renew/release
// contract (ADR-078): generation epoch, live-owner conflict, epoch mismatch.
func TestDOOwnerEndpoints(t *testing.T) {
	s := newDOServer(t)
	h := s.Handler()
	do1 := `{"namespace":"demo","worker":"w","class":"C","shard":0,"node":"do-1","ttl_seconds":30}`
	do2 := `{"namespace":"demo","worker":"w","class":"C","shard":0,"node":"do-2","ttl_seconds":30}`

	rr := do(t, h, http.MethodPost, "/v1/internal/do/claim", "tok", []byte(do1))
	if rr.Code != http.StatusOK {
		t.Fatalf("claim do-1 = %d: %s", rr.Code, rr.Body.String())
	}
	// Another node while live -> 409 owner_live.
	if rr := do(t, h, http.MethodPost, "/v1/internal/do/claim", "tok", []byte(do2)); rr.Code != http.StatusConflict {
		t.Fatalf("claim do-2 = %d, want 409", rr.Code)
	}
	// Wrong role token -> 401.
	if rr := do(t, h, http.MethodPost, "/v1/internal/do/claim", "peer", []byte(do1)); rr.Code != http.StatusUnauthorized {
		t.Fatalf("wrong token = %d, want 401", rr.Code)
	}
	// Renew with the wrong epoch -> 409 owner_epoch; right epoch -> 200.
	bad := `{"namespace":"demo","worker":"w","class":"C","shard":0,"node":"do-1","epoch":99,"ttl_seconds":30}`
	if rr := do(t, h, http.MethodPost, "/v1/internal/do/renew", "tok", []byte(bad)); rr.Code != http.StatusConflict {
		t.Fatalf("renew bad epoch = %d, want 409", rr.Code)
	}
	good := `{"namespace":"demo","worker":"w","class":"C","shard":0,"node":"do-1","epoch":1,"ttl_seconds":30}`
	if rr := do(t, h, http.MethodPost, "/v1/internal/do/renew", "tok", []byte(good)); rr.Code != http.StatusOK {
		t.Fatalf("renew = %d: %s", rr.Code, rr.Body.String())
	}
	// Release with the right epoch -> 200; then do-2 can claim.
	if rr := do(t, h, http.MethodPost, "/v1/internal/do/release", "tok", []byte(good)); rr.Code != http.StatusOK {
		t.Fatalf("release = %d", rr.Code)
	}
	if rr := do(t, h, http.MethodPost, "/v1/internal/do/claim", "tok", []byte(do2)); rr.Code != http.StatusOK {
		t.Fatalf("claim after release = %d", rr.Code)
	}
}

// TestDOObjectIndex covers the durable DO object index (ADR-108): upsert, list
// merge with the runtime registry, delete, and the disabled default.
func TestDOObjectIndex(t *testing.T) {
	s := newDOServer(t)
	h := s.Handler()
	body := []byte(`{"namespace":"demo","worker":"w","class":"C","shard":2,"name":"obj-1"}`)

	// Disabled by default: the write is rejected and the list stays empty.
	if rr := do(t, h, http.MethodPost, "/v1/internal/do/objects", "tok", body); rr.Code != http.StatusNotFound {
		t.Fatalf("disabled index = %d, want 404", rr.Code)
	}

	s.Cfg.DOObjectIndex = true
	if rr := do(t, h, http.MethodPost, "/v1/internal/do/objects", "tok", body); rr.Code != http.StatusOK {
		t.Fatalf("index put = %d: %s", rr.Code, rr.Body.String())
	}
	// No do-runtimes configured: the list comes from the index alone.
	rr := do(t, h, http.MethodGet, "/v1/internal/do/objects", "tok", nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("list = %d", rr.Code)
	}
	got := rr.Body.String()
	if !strings.Contains(got, `"name":"obj-1"`) || !strings.Contains(got, `"source":"index"`) {
		t.Fatalf("list missing indexed object: %s", got)
	}

	// Delete removes it.
	q := "/v1/internal/do/objects?ns=demo&worker=w&class=C&shard=2&name=obj-1"
	if rr := do(t, h, http.MethodDelete, q, "tok", nil); rr.Code != http.StatusOK {
		t.Fatalf("index delete = %d", rr.Code)
	}
	if rr := do(t, h, http.MethodGet, "/v1/internal/do/objects", "tok", nil); strings.Contains(rr.Body.String(), "obj-1") {
		t.Fatalf("object still indexed after delete: %s", rr.Body.String())
	}
}

// TestSelectFollowersPrefersOtherAZ: replicas prefer a different failure domain
// so one zone loss cannot take them all, while still filling from the same AZ
// when needed (ADR-151).
func TestSelectFollowersPrefersOtherAZ(t *testing.T) {
	now := time.Now()
	mk := func(node, az, url string) lease.NodeLease {
		return lease.NodeLease{Node: node, AZ: az, PeerURL: url, Expiry: now.Add(time.Minute).UnixMilli()}
	}
	leases := []lease.NodeLease{
		mk("a1", "az-a", "http://a1"),
		mk("a2", "az-a", "http://a2"),
		mk("b1", "az-b", "http://b1"),
		{Node: "dead", AZ: "az-b", PeerURL: "http://dead"}, // expired
		mk("me", "az-a", "http://me"),                      // self
		{Node: "nourl", AZ: "az-b"},                        // no peer url
	}
	got := selectFollowers(leases, "me", "az-a", now, 2)
	if len(got) != 2 || got[0] != "http://b1" {
		t.Fatalf("followers = %v, want the other-AZ node first", got)
	}
	// With no AZ configured, order is just live peers (no preference).
	got = selectFollowers(leases, "me", "", now, 3)
	if len(got) != 3 {
		t.Fatalf("no-AZ followers = %v, want 3", got)
	}
	// Only same-AZ peers remain when no other AZ is live.
	onlySame := []lease.NodeLease{mk("a1", "az-a", "http://a1"), mk("a2", "az-a", "http://a2")}
	got = selectFollowers(onlySame, "me", "az-a", now, 2)
	if len(got) != 2 || got[0] != "http://a1" || got[1] != "http://a2" {
		t.Fatalf("same-AZ fallback = %v", got)
	}
}
