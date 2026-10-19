package server

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/config"
	"cellhive/internal/lease"
	"cellhive/internal/ltx"
	"cellhive/internal/owner"
	"cellhive/internal/peer"
	"cellhive/internal/recovery"
)

func TestDORecoverScopeWhileCellAgentSessionStaysLive(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestServer(t)
	s.Recovery = recovery.New(s.Replica, peer.NewHTTPTransport("tok", nil))
	if err := s.Lease.Publish(ctx, lease.Load{}); err != nil {
		t.Fatal(err)
	}
	spool, err := peer.NewSpool(filepath.Join(t.TempDir(), "spool"))
	if err != nil {
		t.Fatal(err)
	}
	follower := New(Deps{Cfg: config.Config{TokenPeer: "tok", TokenInternal: "tok", TokenDispatch: "tok", ScopeSecret: "tok"}, Spool: spool})
	fh := httptest.NewServer(follower.Handler())
	defer fh.Close()
	capture := cell.Scope{Namespace: "workerd", Class: "__do__", ID: "facet"}
	if _, err := s.Owner.Claim(ctx, capture, time.Now()); err != nil {
		t.Fatal(err)
	}
	doScope := owner.DOScope("demo", "w", "C", 0)
	doOwner, err := s.Owner.ClaimAs(ctx, doScope, "do-b", "http://do-b", cell.RoleCellAgent, time.Minute, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.NodeLog.Open(ctx, "s1", 1, []string{fh.URL}); err != nil {
		t.Fatal(err)
	}
	if err := s.ensureDORecoveryPointer(ctx, capture, doRecoveryPointer{
		DOOwnerScope: doScope.String(), DOOwnerEpoch: doOwner.Epoch, DOOwnerNode: "do-b",
		CaptureNode: "node-1", CaptureSession: "s1",
	}); err != nil {
		t.Fatal(err)
	}
	seg := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: 1, EndTxID: 1}, []byte("do-write"))
	transport := peer.NewHTTPTransport("tok", nil).WithSession("s1")
	if err := transport.Append(ctx, fh.URL, capture, 1, seg); err != nil {
		t.Fatal(err)
	}
	// Graceful cell-agent handoff can delete the capture owner record while its
	// process session and unrelated cells stay live. The durable pointer must
	// still locate the follower-confirmed DO write.
	if err := s.Owner.Release(ctx, capture, 1); err != nil {
		t.Fatal(err)
	}
	url := "/v1/internal/do/recover-scope"
	body := fmt.Sprintf(`{"scope":%q,"owner_scope":%q,"owner_node":"do-b","owner_epoch":%d}`, capture.String(), doScope.String(), doOwner.Epoch)
	if rr := do(t, s.Handler(), http.MethodPost, url, "tok", []byte(body)); rr.Code != http.StatusOK {
		t.Fatalf("recover = %d %s", rr.Code, rr.Body.String())
	}
	if keys, err := s.Replica.ListSegments(ctx, capture, 1); err != nil || len(keys) != 1 {
		t.Fatalf("recovered segments = %v, %v", keys, err)
	}
	other := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "other"}
	if err := transport.Append(ctx, fh.URL, other, 1, seg); err != nil {
		t.Fatalf("live session sealed: %v", err)
	}
	bad := fmt.Sprintf(`{"scope":%q,"owner_scope":%q,"owner_node":"old-do","owner_epoch":%d}`, capture.String(), doScope.String(), doOwner.Epoch)
	if rr := do(t, s.Handler(), http.MethodPost, url, "tok", []byte(bad)); rr.Code != http.StatusConflict {
		t.Fatalf("stale owner = %d, want 409", rr.Code)
	}
}

func TestDORecoveryPointerABARoundTrip(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestServer(t)
	s.Recovery = recovery.New(s.Replica, peer.NewHTTPTransport("tok", nil))
	if err := s.Lease.Publish(ctx, lease.Load{}); err != nil {
		t.Fatal(err)
	}
	spool, err := peer.NewSpool(filepath.Join(t.TempDir(), "spool"))
	if err != nil {
		t.Fatal(err)
	}
	fh := httptest.NewServer(New(Deps{Cfg: config.Config{TokenPeer: "tok", TokenInternal: "tok", TokenDispatch: "tok", ScopeSecret: "tok"}, Spool: spool}).Handler())
	defer fh.Close()
	if _, err := s.NodeLog.Open(ctx, "s1", 1, []string{fh.URL}); err != nil {
		t.Fatal(err)
	}
	transport := peer.NewHTTPTransport("tok", nil).WithSession("s1")
	sc := cell.Scope{Namespace: "workerd", Class: "__do__", ID: "same-facet"}
	for i, node := range []string{"do-a", "do-b", "do-a"} {
		epoch := uint64(i + 1)
		p := doRecoveryPointer{DOOwnerScope: owner.DOScope("demo", "w", "C", 0).String(), DOOwnerEpoch: epoch, DOOwnerNode: node, CaptureNode: "node-1", CaptureSession: "s1"}
		if err := s.ensureDORecoveryPointer(ctx, sc, p); err != nil {
			t.Fatalf("pointer epoch %d: %v", epoch, err)
		}
		seg := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: epoch, EndTxID: epoch}, []byte(fmt.Sprintf("value-%d", epoch)))
		if err := transport.Append(ctx, fh.URL, sc, 1, seg); err != nil {
			t.Fatal(err)
		}
		if _, err := s.recoverDOPointer(ctx, sc, p); err != nil {
			t.Fatalf("recover epoch %d: %v", epoch, err)
		}
	}
	if keys, err := s.Replica.ListSegments(ctx, sc, 1); err != nil || len(keys) != 3 {
		t.Fatalf("A-B-A segments = %v, %v", keys, err)
	}
	stale := doRecoveryPointer{DOOwnerScope: owner.DOScope("demo", "w", "C", 0).String(), DOOwnerEpoch: 1, DOOwnerNode: "do-a", CaptureNode: "other-agent", CaptureSession: "other-session"}
	if err := s.ensureDORecoveryPointer(ctx, sc, stale); err == nil {
		t.Fatal("old owner replaced newer recovery pointer")
	}
}
