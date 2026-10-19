package recovery_test

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/config"
	"cellhive/internal/ltx"
	"cellhive/internal/nodelog"
	"cellhive/internal/peer"
	"cellhive/internal/recovery"
	"cellhive/internal/replica"
	"cellhive/internal/server"
)

// TestRecoverUnuploadedAck simulates a node that acknowledged a fleet write
// (follower fsynced) but crashed before uploading it to the bucket. A new owner
// must recover the acknowledged write from the follower.
func TestRecoverUnuploadedAck(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	// Shared fleet bucket.
	shared, err := bucket.NewFSBucket(filepath.Join(dir, "bucket"))
	if err != nil {
		t.Fatalf("bucket: %v", err)
	}

	// Follower node holding the spool.
	spool, err := peer.NewSpool(filepath.Join(dir, "follower-spool"))
	if err != nil {
		t.Fatalf("spool: %v", err)
	}
	follower := server.New(server.Deps{Cfg: config.Config{TokenPeer: "tok", TokenInternal: "tok", TokenDispatch: "tok", ScopeSecret: "tok"}, Spool: spool})
	fh := httptest.NewServer(follower.Handler())
	defer fh.Close()

	// Owner acknowledges the write to the follower but does not upload it.
	transport := peer.NewHTTPTransport("tok", nil).WithSession("s1")
	peerMgr := peer.NewManager(transport)
	nl := nodelog.New(shared, "node-1")
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "main"}
	seg := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: 1, EndTxID: 1}, []byte("acked-data"))

	if _, err := nl.Open(ctx, "s1", 1, []string{fh.URL}); err != nil {
		t.Fatalf("node-log open: %v", err)
	}
	if _, err := peerMgr.Replicate(ctx, []string{fh.URL}, sc, 1, seg); err != nil {
		t.Fatalf("replicate: %v", err)
	}

	// The bucket does not have the segment yet (crash before upload).
	rep := replica.New(shared)
	if keys, _ := rep.ListSegments(ctx, sc, 1); len(keys) != 0 {
		t.Fatalf("bucket unexpectedly has segments before recovery: %v", keys)
	}

	// New owner runs recovery from the node-log.
	rec := recovery.New(rep, transport)
	n, err := rec.Recover(ctx, nl, "node-1", "s1")
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if n != 1 {
		t.Fatalf("recovered %d segments, want 1", n)
	}

	// The acknowledged write is now durable in the bucket.
	keys, err := rep.ListSegments(ctx, sc, 1)
	if err != nil || len(keys) != 1 {
		t.Fatalf("bucket after recovery = %v, %v", keys, err)
	}
	got, err := rep.ReadSegment(ctx, keys[0])
	if err != nil {
		t.Fatalf("read recovered: %v", err)
	}
	if _, payload, err := ltx.Decode(got); err != nil || string(payload) != "acked-data" {
		t.Fatalf("recovered payload = %q, %v", payload, err)
	}

	// The node-log is sealed so a second recovery is a no-op.
	rec2, _, err := nl.Get(ctx, "node-1", "s1")
	if err != nil || rec2.Status != nodelog.StatusSealed {
		t.Fatalf("node-log not sealed: %+v %v", rec2, err)
	}
	if n2, err := rec.Recover(ctx, nl, "node-1", "s1"); err != nil || n2 != 0 {
		t.Fatalf("second recover = %d, %v; want 0, nil", n2, err)
	}
}

func TestRecoverLiveScopeLeavesOtherCellWritable(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	b, err := bucket.NewFSBucket(filepath.Join(dir, "bucket"))
	if err != nil {
		t.Fatal(err)
	}
	spool, err := peer.NewSpool(filepath.Join(dir, "spool"))
	if err != nil {
		t.Fatal(err)
	}
	f := server.New(server.Deps{Cfg: config.Config{TokenPeer: "tok", TokenInternal: "tok", TokenDispatch: "tok", ScopeSecret: "tok"}, Spool: spool})
	fh := httptest.NewServer(f.Handler())
	defer fh.Close()
	transport := peer.NewHTTPTransport("tok", nil).WithSession("shared")
	nl := nodelog.New(b, "agent-a")
	target := cell.Scope{Namespace: "demo", Class: "__do__", ID: "facet-a"}
	other := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "other"}
	if _, err := nl.Open(ctx, "shared", 1, []string{fh.URL}); err != nil {
		t.Fatal(err)
	}
	for _, sc := range []cell.Scope{target, other} {
		seg := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: 1, EndTxID: 1}, []byte(sc.ID))
		if err := transport.Append(ctx, fh.URL, sc, 1, seg); err != nil {
			t.Fatal(err)
		}
	}
	rec, _, err := nl.Get(ctx, "agent-a", "shared")
	if err != nil {
		t.Fatal(err)
	}
	r := recovery.New(replica.New(b), transport)
	n, err := r.RecoverLiveScope(ctx, rec, target)
	if err != nil || n != 1 {
		t.Fatalf("recover target = %d, %v", n, err)
	}
	if keys, err := r.Replica.ListSegments(ctx, target, 1); err != nil || len(keys) != 1 {
		t.Fatalf("target segments = %v, %v", keys, err)
	}
	if keys, err := r.Replica.ListSegments(ctx, other, 1); err != nil || len(keys) != 0 {
		t.Fatalf("other unexpectedly uploaded = %v, %v", keys, err)
	}
	// A process-wide seal here would reject this still-live unrelated cell.
	seg := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: 2, EndTxID: 2}, []byte("still-live"))
	if err := transport.Append(ctx, fh.URL, other, 1, seg); err != nil {
		t.Fatalf("other cell blocked: %v", err)
	}
}

func TestRecoverOneSessionAcrossCellEpochs(t *testing.T) {
	ctx := t.Context()
	shared, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	spool, err := peer.NewSpool(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	follower := server.New(server.Deps{Cfg: config.Config{TokenPeer: "tok", TokenInternal: "tok"}, Spool: spool})
	http := httptest.NewServer(follower.Handler())
	defer http.Close()
	nl := nodelog.New(shared, "dead-owner")
	transport := peer.NewHTTPTransport("tok", nil).WithSession("multi-cell")
	for _, tc := range []struct {
		id    string
		epoch uint64
	}{{"first", 15}, {"second", 23}} {
		if _, err := nl.Open(ctx, "multi-cell", tc.epoch, []string{http.URL}); err != nil {
			t.Fatalf("open epoch %d: %v", tc.epoch, err)
		}
		sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: tc.id}
		seg := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: tc.epoch, StartTxID: 1, EndTxID: 1}, []byte(tc.id))
		if err := transport.Append(ctx, http.URL, sc, tc.epoch, seg); err != nil {
			t.Fatalf("append epoch %d: %v", tc.epoch, err)
		}
	}
	rep := replica.New(shared)
	if n, err := recovery.New(rep, transport).Recover(ctx, nl, "dead-owner", "multi-cell"); err != nil || n != 2 {
		t.Fatalf("recover two epochs: segments=%d err=%v", n, err)
	}
	for _, tc := range []struct {
		id    string
		epoch uint64
	}{{"first", 15}, {"second", 23}} {
		sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: tc.id}
		keys, err := rep.ListSegments(ctx, sc, tc.epoch)
		if err != nil || len(keys) != 1 {
			t.Fatalf("%s epoch %d: keys=%v err=%v", tc.id, tc.epoch, keys, err)
		}
	}
}

func TestRecoverySkipsDeletedResourcesButRecoversLiveScopes(t *testing.T) {
	for _, mode := range []string{"legacy", "sealed"} {
		t.Run(mode, func(t *testing.T) {
			ctx := t.Context()
			shared, err := bucket.NewFSBucket(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			spool, err := peer.NewSpool(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			deleted := cell.Scope{Namespace: "acme", Class: "__queue__", ID: "deleted"}
			live := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "live"}
			for _, sc := range []cell.Scope{deleted, live} {
				seg := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: 1, EndTxID: 1}, []byte(sc.ID))
				if mode == "sealed" {
					_, _, err = spool.AppendBatchSession(ctx, "session", sc, 1, [][]byte{seg})
				} else {
					_, _, err = spool.AppendBatch(ctx, sc, 1, [][]byte{seg})
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			follower := server.New(server.Deps{Cfg: config.Config{TokenPeer: "tok", TokenInternal: "tok"}, Spool: spool})
			http := httptest.NewServer(follower.Handler())
			defer http.Close()
			rep := replica.New(shared)
			if err := rep.BlockResource(ctx, deleted, "delete-job"); err != nil {
				t.Fatal(err)
			}
			rec := recovery.New(rep, peer.NewHTTPTransport("tok", nil))
			var n int
			if mode == "sealed" {
				nl := nodelog.New(shared, "dead")
				if _, err := nl.Open(ctx, "session", 1, []string{http.URL}); err != nil {
					t.Fatal(err)
				}
				n, err = rec.Recover(ctx, nl, "dead", "session")
			} else {
				n, err = rec.Collect(ctx, []string{http.URL})
			}
			if err != nil || n != 1 {
				t.Fatalf("deleted scope blocked recovery: segments=%d err=%v", n, err)
			}
			keys, err := rep.ListSegments(ctx, deleted, 1)
			if err != nil || len(keys) != 0 {
				t.Fatalf("deleted scope resurrected: %v %v", keys, err)
			}
			keys, err = rep.ListSegments(ctx, live, 1)
			if err != nil || len(keys) != 1 {
				t.Fatalf("live acked data lost: %v %v", keys, err)
			}
		})
	}
}

func TestRecoverLegacyNodeLogUsesHeldSpool(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	shared, err := bucket.NewFSBucket(filepath.Join(dir, "bucket"))
	if err != nil {
		t.Fatalf("bucket: %v", err)
	}
	spool, err := peer.NewSpool(filepath.Join(dir, "follower-spool"))
	if err != nil {
		t.Fatalf("spool: %v", err)
	}
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "legacy"}
	seg := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: 1, EndTxID: 1}, []byte("legacy-acked"))
	if _, _, err := spool.AppendBatch(ctx, sc, 1, [][]byte{seg}); err != nil {
		t.Fatalf("legacy spool append: %v", err)
	}
	follower := server.New(server.Deps{Cfg: config.Config{TokenPeer: "tok", TokenInternal: "tok", TokenDispatch: "tok", ScopeSecret: "tok"}, Spool: spool})
	fh := httptest.NewServer(follower.Handler())
	defer fh.Close()

	nl := nodelog.New(shared, "node-1")
	legacy := nodelog.Record{Node: "node-1", Session: "legacy-session", Epoch: 1, Followers: []string{fh.URL}, Status: nodelog.StatusOpen, ProtoVersion: nodelog.ProtoVersionLegacy}
	data, _ := json.Marshal(legacy)
	if _, err := shared.ConditionalCreate(ctx, nodelog.Key("node-1", "legacy-session"), data); err != nil {
		t.Fatalf("seed legacy node-log: %v", err)
	}
	transport := peer.NewHTTPTransport("tok", nil)
	rep := replica.New(shared)
	rec := recovery.New(rep, transport)
	if n, err := rec.Recover(ctx, nl, "node-1", "legacy-session"); err != nil || n != 1 {
		t.Fatalf("legacy recover = %d, %v; want 1", n, err)
	}
	if keys, _ := rep.ListSegments(ctx, sc, 1); len(keys) != 1 {
		t.Fatalf("legacy recovered keys = %v", keys)
	}
}

func TestRecoverSessionTailAcrossPages(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	shared, err := bucket.NewFSBucket(filepath.Join(dir, "bucket"))
	if err != nil {
		t.Fatalf("bucket: %v", err)
	}
	spool, err := peer.NewSpool(filepath.Join(dir, "follower-spool"))
	if err != nil {
		t.Fatalf("spool: %v", err)
	}
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "paged-recovery"}
	for i := uint64(1); i <= 300; i++ {
		seg := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: i, EndTxID: i}, []byte{byte(i)})
		if _, _, err := spool.AppendBatchSession(ctx, "paged-session", sc, 1, [][]byte{seg}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	follower := server.New(server.Deps{Cfg: config.Config{TokenPeer: "tok", TokenInternal: "tok", TokenDispatch: "tok", ScopeSecret: "tok"}, Spool: spool})
	fh := httptest.NewServer(follower.Handler())
	defer fh.Close()
	nl := nodelog.New(shared, "node-1")
	if _, err := nl.Open(ctx, "paged-session", 1, []string{fh.URL}); err != nil {
		t.Fatalf("node-log open: %v", err)
	}
	transport := peer.NewHTTPTransport("tok", nil).WithSession("paged-session")
	rep := replica.New(shared)
	rec := recovery.New(rep, transport)
	if n, err := rec.Recover(ctx, nl, "node-1", "paged-session"); err != nil || n != 300 {
		t.Fatalf("paged recover = %d, %v; want 300", n, err)
	}
	if keys, _ := rep.ListSegments(ctx, sc, 1); len(keys) != 300 {
		t.Fatalf("paged recovered keys = %d, want 300", len(keys))
	}
}

// TestRecoverNodeListsAndSkipsSealed verifies that a new owner can recover every
// unsealed session of a dead node and that sealed sessions take the fast path.
func TestRecoverNodeListsAndSkipsSealed(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	shared, err := bucket.NewFSBucket(filepath.Join(dir, "bucket"))
	if err != nil {
		t.Fatalf("bucket: %v", err)
	}
	spool, err := peer.NewSpool(filepath.Join(dir, "follower-spool"))
	if err != nil {
		t.Fatalf("spool: %v", err)
	}
	follower := server.New(server.Deps{Cfg: config.Config{TokenPeer: "tok", TokenInternal: "tok", TokenDispatch: "tok", ScopeSecret: "tok"}, Spool: spool})
	fh := httptest.NewServer(follower.Handler())
	defer fh.Close()
	transport := peer.NewHTTPTransport("tok", nil).WithSession("s1")
	peerMgr := peer.NewManager(transport)
	nl := nodelog.New(shared, "node-1")
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "main"}

	// s1 is open with an acknowledged-but-unuploaded segment on the follower.
	if _, err := nl.Open(ctx, "s1", 1, []string{fh.URL}); err != nil {
		t.Fatalf("open s1: %v", err)
	}
	seg := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: 1, EndTxID: 1}, []byte("s1-data"))
	if _, err := peerMgr.Replicate(ctx, []string{fh.URL}, sc, 1, seg); err != nil {
		t.Fatalf("replicate: %v", err)
	}
	// s2 is already sealed (graceful handoff) and has nothing to recover.
	if _, err := nl.Open(ctx, "s2", 1, nil); err != nil {
		t.Fatalf("open s2: %v", err)
	}
	if _, err := nl.Seal(ctx, "node-1", "s2"); err != nil {
		t.Fatalf("seal s2: %v", err)
	}

	rep := replica.New(shared)
	rec := recovery.New(rep, transport)
	segments, sessions, err := rec.RecoverNode(ctx, nl, "node-1")
	if err != nil {
		t.Fatalf("recover node: %v", err)
	}
	if segments != 1 || sessions != 1 {
		t.Fatalf("recover node = %d segments, %d sessions; want 1, 1", segments, sessions)
	}
	if keys, _ := rep.ListSegments(ctx, sc, 1); len(keys) != 1 {
		t.Fatalf("bucket after recovery = %v", keys)
	}
	if r, _, _ := nl.Get(ctx, "node-1", "s1"); r.Status != nodelog.StatusSealed {
		t.Fatalf("s1 not sealed: %+v", r)
	}
	if r, _, _ := nl.Get(ctx, "node-1", "s2"); r.Status != nodelog.StatusSealed {
		t.Fatalf("s2 changed: %+v", r)
	}
	// A second pass is a no-op (all sealed).
	if segments, sessions, err := rec.RecoverNode(ctx, nl, "node-1"); err != nil || segments != 0 || sessions != 0 {
		t.Fatalf("second recover node = %d, %d, %v", segments, sessions, err)
	}
}
