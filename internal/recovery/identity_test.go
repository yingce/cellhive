package recovery_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/config"
	"cellhive/internal/lease"
	"cellhive/internal/ltx"
	"cellhive/internal/nodelog"
	"cellhive/internal/peer"
	"cellhive/internal/recovery"
	"cellhive/internal/replica"
	"cellhive/internal/server"
)

func TestRecoveryFollowsIdentityAfterAddressChange(t *testing.T) {
	ctx := context.Background()
	b, _ := bucket.NewFSBucket(t.TempDir())
	spool, _ := peer.NewSpool(t.TempDir())
	lm := lease.NewManager(b, "follower", "new-process", "", "", time.Minute)
	lm.Instance = "original-disk"
	h := httptest.NewServer(server.New(server.Deps{Cfg: config.Config{NodeID: "follower", TokenPeer: "tok"}, Lease: lm, Spool: spool}).Handler())
	defer h.Close()
	lm.PeerURL = h.URL
	if err := lm.Publish(ctx, lease.Load{}); err != nil {
		t.Fatal(err)
	}
	sc := cell.Scope{Namespace: "test", Class: "kv", ID: "recover"}
	seg := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: 1, EndTxID: 1}, []byte("acked data"))
	if _, _, err := spool.AppendBatchSession(ctx, "old-session", sc, 1, [][]byte{seg}); err != nil {
		t.Fatal(err)
	}
	// Use JSON so the initial regression compiles against the old record schema.
	raw := []byte(`{"node":"owner","session":"old-session","epoch":1,"followers":["http://127.0.0.1:1"],"follower_refs":{"http://127.0.0.1:1":{"node":"follower","instance":"original-disk"}},"status":"open","proto_version":"v3"}`)
	if _, err := b.ConditionalCreate(ctx, nodelog.Key("owner", "old-session"), raw); err != nil {
		t.Fatal(err)
	}
	nl := nodelog.New(b, "owner")
	m := recovery.New(replica.New(b), peer.NewHTTPTransport("tok", nil))
	// Resolver is wired explicitly once the implementation adds it.
	configureIdentityRecovery(m, lm)
	if _, err := m.Recover(ctx, nl, "owner", "old-session"); err != nil {
		t.Fatalf("recover through current address: %v", err)
	}
	rec, _, _ := nl.Get(ctx, "owner", "old-session")
	if rec.Status != nodelog.StatusSealed {
		t.Fatal(rec)
	}
	segments, err := replica.New(b).ListSegments(ctx, sc, 1)
	if err != nil || len(segments) != 1 {
		t.Fatalf("segments=%v err=%v", segments, err)
	}
}

func configureIdentityRecovery(m *recovery.Manager, lm *lease.Manager) { m.Lease = lm }

func TestPeerRejectsWrongPersistentIdentity(t *testing.T) {
	spool, _ := peer.NewSpool(t.TempDir())
	lm := lease.NewManager(nil, "follower", "current", "", "", time.Minute)
	lm.Instance = "replacement-disk"
	h := httptest.NewServer(server.New(server.Deps{Cfg: config.Config{NodeID: "follower", TokenPeer: "tok"}, Lease: lm, Spool: spool}).Handler())
	defer h.Close()
	req, _ := http.NewRequest(http.MethodPost, h.URL+"/v1/peer/seal?session=old", nil)
	req.Header.Set("x-cellhive-internal-token", "tok")
	req.Header.Set("X-Cellhive-Target-Node", "follower")
	req.Header.Set("X-Cellhive-Target-Instance", "original-disk")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		var body any
		_ = json.NewDecoder(resp.Body).Decode(&body)
		t.Fatalf("wrong disk accepted: status=%d body=%v", resp.StatusCode, body)
	}
}

func TestIdentifiedSealRejectsMissingLog(t *testing.T) {
	spool, _ := peer.NewSpool(t.TempDir())
	lm := lease.NewManager(nil, "f", "s", "", "", time.Minute)
	lm.Instance = "disk"
	h := httptest.NewServer(server.New(server.Deps{Cfg: config.Config{TokenPeer: "tok"}, Lease: lm, Spool: spool}).Handler())
	defer h.Close()
	ctx := peer.WithIdentity(context.Background(), cell.PeerIdentity{Node: "f", Instance: "disk"})
	if _, err := peer.NewHTTPTransport("tok", nil).Seal(ctx, h.URL, "missing"); err == nil {
		t.Fatal("missing retained log accepted as a complete empty tail")
	}
}

func TestRecoveryRefusesReplacementDiskAndPreservesRecord(t *testing.T) {
	ctx := context.Background()
	b, _ := bucket.NewFSBucket(t.TempDir())
	spool, _ := peer.NewSpool(t.TempDir())
	lm := lease.NewManager(b, "follower", "new", "", "", time.Minute)
	lm.Instance = "new-disk"
	h := httptest.NewServer(server.New(server.Deps{Cfg: config.Config{TokenPeer: "tok"}, Lease: lm, Spool: spool}).Handler())
	defer h.Close()
	lm.PeerURL = h.URL
	if err := lm.Publish(ctx, lease.Load{}); err != nil {
		t.Fatal(err)
	}
	nl := nodelog.New(b, "owner")
	if _, err := nl.OpenIdentified(ctx, "old", 1, map[string]cell.PeerIdentity{"http://old": {Node: "follower", Instance: "old-disk"}}); err != nil {
		t.Fatal(err)
	}
	m := recovery.New(replica.New(b), peer.NewHTTPTransport("tok", nil))
	m.Lease = lm
	if _, err := m.Recover(ctx, nl, "owner", "old"); err == nil {
		t.Fatal("replacement disk accepted")
	}
	r, _, _ := nl.Get(ctx, "owner", "old")
	if r.Status == nodelog.StatusSealed {
		t.Fatal("missing replica sealed")
	}
}

func TestRecoveryUsesBootAddressBeforeNewLease(t *testing.T) {
	ctx := context.Background()
	b, _ := bucket.NewFSBucket(t.TempDir())
	spool, _ := peer.NewSpool(t.TempDir())
	lm := lease.NewManager(b, "f", "boot", "", "http://127.0.0.1:1", time.Minute)
	lm.Instance = "disk"
	if err := lm.Publish(ctx, lease.Load{}); err != nil {
		t.Fatal(err)
	}
	h := httptest.NewServer(server.New(server.Deps{Cfg: config.Config{TokenPeer: "tok"}, Lease: lm, Spool: spool}).Handler())
	defer h.Close()
	lm.PeerURL = h.URL
	if err := lm.PublishRecoveryAddress(ctx); err != nil {
		t.Fatal(err)
	}
	if err := spool.Prepare("old"); err != nil {
		t.Fatal(err)
	}
	nl := nodelog.New(b, "owner")
	_, err := nl.OpenIdentified(ctx, "old", 1, map[string]cell.PeerIdentity{"http://127.0.0.1:1": {Node: "f", Instance: "disk"}})
	if err != nil {
		t.Fatal(err)
	}
	m := recovery.New(replica.New(b), peer.NewHTTPTransport("tok", nil))
	m.Lease = lm
	if _, err = m.Recover(ctx, nl, "owner", "old"); err != nil {
		t.Fatal(err)
	}
	leases, err := lm.Sample(ctx)
	if err != nil || len(leases) != 1 {
		t.Fatalf("recovery address leaked into membership: %+v %v", leases, err)
	}
}

func TestRecoveryResumesCheckpointAfterTailFailure(t *testing.T) {
	ctx := context.Background()
	b, _ := bucket.NewFSBucket(t.TempDir())
	spool, _ := peer.NewSpool(t.TempDir())
	sc := cell.Scope{Namespace: "test", Class: "kv", ID: "resume"}
	for i := uint64(1); i <= 300; i++ {
		seg := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: i, EndTxID: i}, []byte("data"))
		if _, _, err := spool.AppendBatchSession(ctx, "old", sc, 1, [][]byte{seg}); err != nil {
			t.Fatal(err)
		}
	}
	actual := server.New(server.Deps{Cfg: config.Config{TokenPeer: "tok"}, Spool: spool}).Handler()
	var fail atomic.Bool
	fail.Store(true)
	h := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/peer/tail" && r.URL.Query().Get("after") == "256" && fail.Load() {
			http.Error(w, "interrupted", 503)
			return
		}
		actual.ServeHTTP(w, r)
	}))
	defer h.Close()
	nl := nodelog.New(b, "owner")
	if _, err := nl.Open(ctx, "old", 1, []string{h.URL}); err != nil {
		t.Fatal(err)
	}
	m := recovery.New(replica.New(b), peer.NewHTTPTransport("tok", nil))
	if _, err := m.Recover(ctx, nl, "owner", "old"); err == nil {
		t.Fatal("interruption ignored")
	}
	rec, _, _ := nl.Get(ctx, "owner", "old")
	if rec.Status != nodelog.StatusRecovering || rec.Progress[h.URL] != 256 {
		t.Fatalf("lost durable progress: %+v", rec)
	}
	fail.Store(false)
	// New recovery manager simulates loss of all in-memory recovery state.
	m = recovery.New(replica.New(b), peer.NewHTTPTransport("tok", nil))
	n, err := m.Recover(ctx, nl, "owner", "old")
	if err != nil || n != 44 {
		t.Fatalf("resume uploaded=%d want44 err=%v", n, err)
	}
	rec, _, _ = nl.Get(ctx, "owner", "old")
	if rec.Status != nodelog.StatusSealed || rec.CoveredSeq[h.URL] != 300 {
		t.Fatalf("bad coverage: %+v", rec)
	}
	if _, err := nl.SetStatus(ctx, "owner", "old", nodelog.StatusRecovering); err == nil {
		t.Fatal("sealed session reopened")
	}
}

func TestReplicationRequiresMatchingIdentityHTTPAndStream(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprintf("stream=%v", stream), func(t *testing.T) {
			ctx := context.Background()
			spool, _ := peer.NewSpool(t.TempDir())
			lm := lease.NewManager(nil, "f", "s", "", "", time.Minute)
			lm.Instance = "disk"
			h := httptest.NewServer(server.New(server.Deps{Cfg: config.Config{TokenPeer: "tok"}, Lease: lm, Spool: spool}).Handler())
			defer h.Close()
			targets := &peer.Targets{}
			if err := targets.Bind(map[string]cell.PeerIdentity{h.URL: {Node: "f", Instance: "disk"}}); err != nil {
				t.Fatal(err)
			}
			tr := peer.NewHTTPTransport("tok", nil).WithSession("owner")
			tr.Targets = targets
			if err := tr.Prepare(ctx, h.URL, "owner"); err != nil {
				t.Fatal(err)
			}
			var transport peer.Transport = tr
			if stream {
				st := peer.NewSessionStreamTransport("tok", "owner", 1, 1)
				st.Targets = targets
				transport = st
				defer st.Close()
			}
			sc := cell.Scope{Namespace: "test", Class: "kv", ID: "proof"}
			seg := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: 1, EndTxID: 1}, []byte("confirmed"))
			if err := transport.Append(ctx, h.URL, sc, 1, seg); err != nil {
				t.Fatal(err)
			}
			if n, err := spool.Head("owner"); err != nil || n != 1 {
				t.Fatalf("fsync proof missing: %d %v", n, err)
			}
			bad := &peer.Targets{}
			_ = bad.Bind(map[string]cell.PeerIdentity{h.URL: {Node: "f", Instance: "wrong-disk"}})
			if stream {
				st := peer.NewSessionStreamTransport("tok", "owner", 1, 1)
				st.Targets = bad
				transport = st
				defer st.Close()
			} else {
				tr := peer.NewHTTPTransport("tok", nil).WithSession("owner")
				tr.Targets = bad
				transport = tr
			}
			if err := transport.Append(ctx, h.URL, sc, 1, seg); err == nil {
				t.Fatal("wrong target acknowledged")
			}
			if n, _ := spool.Head("owner"); n != 1 {
				t.Fatalf("wrong target mutated spool: %d", n)
			}
		})
	}
}
