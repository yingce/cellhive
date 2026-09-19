package server

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"cellhive/internal/artifacts"
	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
	"cellhive/internal/config"
	"cellhive/internal/control"
	"cellhive/internal/d1"
	"cellhive/internal/lease"
	"cellhive/internal/logbuf"
	"cellhive/internal/ltx"
	"cellhive/internal/nodelog"
	"cellhive/internal/owner"
	"cellhive/internal/ownerclient"
	"cellhive/internal/peer"
	"cellhive/internal/queue"
	"cellhive/internal/r2"
	"cellhive/internal/replica"
	"cellhive/internal/scopedtoken"
	"cellhive/internal/telemetry"
	"cellhive/internal/timer"
	"cellhive/internal/upload"
	"cellhive/internal/workflow"
	"sync/atomic"
)

func newTestServer(t *testing.T) (*Server, string) {
	t.Helper()
	dir := t.TempDir()
	b, err := bucket.NewFSBucket(dir)
	if err != nil {
		t.Fatalf("bucket: %v", err)
	}
	cfg := config.Config{NodeID: "node-1", TokenPeer: "tok", TokenInternal: "tok", TokenDispatch: "tok", ScopeSecret: "tok", TokenLog: "tok", AdminToken: "admin-tok", BucketDir: dir, Durability: "auto", BucketWait: true}
	lm := lease.NewManager(b, "node-1", "s1", "127.0.0.1:7000", "", 10*time.Second)
	om := &owner.Manager{B: b, NodeID: "node-1", Session: "s1", Advertise: "127.0.0.1:7000", Role: cell.RoleCellAgent, OwnerTTL: 10 * time.Second}
	rm := replica.New(b)
	spool, err := peer.NewSpool(filepath.Join(dir, "peer-spool"))
	if err != nil {
		t.Fatalf("spool: %v", err)
	}
	pm := peer.NewManager(peer.NewHTTPTransport("tok", nil))
	nl := nodelog.New(b, "node-1")
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	upCtx, upCancel := context.WithCancel(context.Background())
	up := upload.New(rm, log, 64, 1<<20, 5*time.Millisecond)
	up.Start(upCtx)
	peerUp := upload.New(spool, log, 64, 1<<20, 2*time.Millisecond)
	peerUp.Start(upCtx)
	shipper := peer.NewShipBatcher(peer.NewHTTPTransport("tok", nil), 256, 4<<20, time.Millisecond, 4, 4, 0, 0)
	shipper.Start(upCtx)
	t.Cleanup(func() { upCancel(); up.Stop(); peerUp.Stop(); shipper.Stop() })
	cs, err := cellstore.New(filepath.Join(dir, "cells"))
	if err != nil {
		t.Fatalf("cellstore: %v", err)
	}
	reg := timer.NewRegistry()
	open := func(ctx context.Context, sc cell.Scope) (*timer.Store, error) {
		c, err := cs.Cell(ctx, sc)
		if err != nil {
			return nil, err
		}
		return timer.NewStore(ctx, c)
	}
	return New(Deps{Cfg: cfg, Bucket: b, Lease: lm, Owner: om, Replica: rm, Spool: spool, PeerMgr: pm, PeerShipper: shipper, NodeLog: nl, Uploader: up, PeerUploader: peerUp, Store: cs, Timers: reg, OpenTimer: open, Workflows: workflow.New(cs), R2: r2.New(b), Logs: logbuf.New(100, 10), Log: log}), dir
}

func do(t *testing.T, h http.Handler, method, target, token string, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, target, bytes.NewReader(body))
	if token != "" {
		req.Header.Set("x-cellhive-internal-token", token)
	}
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	return rr
}

func TestAppendPipeline(t *testing.T) {
	srv, _ := newTestServer(t)
	h := srv.Handler()
	sc := "demo/__kv__/main"
	ctx := context.Background()

	// Unauthorized without token.
	if rr := do(t, h, http.MethodGet, "/v1/internal/resolve?scope="+sc, "", nil); rr.Code != http.StatusUnauthorized {
		t.Fatalf("no token: code = %d, want 401", rr.Code)
	}

	// Claim owner.
	rr := do(t, h, http.MethodPost, "/v1/internal/claim", "tok", []byte(`{"scope":"`+sc+`"}`))
	if rr.Code != http.StatusOK {
		t.Fatalf("claim code = %d body=%s", rr.Code, rr.Body.String())
	}

	seg := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: 1, EndTxID: 1}, []byte("data"))

	// Append with the correct epoch.
	rr = do(t, h, http.MethodPost, "/v1/internal/append?scope="+sc+"&epoch=1", "tok", seg)
	if rr.Code != http.StatusOK {
		t.Fatalf("append code = %d body=%s", rr.Code, rr.Body.String())
	}
	var app struct {
		Key string `json:"key"`
	}
	_ = json.Unmarshal(rr.Body.Bytes(), &app)
	if app.Key == "" {
		t.Fatalf("empty key")
	}

	// Append with a wrong epoch is fenced.
	if rr := do(t, h, http.MethodPost, "/v1/internal/append?scope="+sc+"&epoch=2", "tok", seg); rr.Code != http.StatusConflict {
		t.Fatalf("append wrong epoch: code = %d, want 409", rr.Code)
	}

	// List segments.
	rr = do(t, h, http.MethodGet, "/v1/internal/segments?scope="+sc+"&epoch=1", "tok", nil)
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), app.Key) {
		t.Fatalf("segments: code=%d body=%s", rr.Code, rr.Body.String())
	}

	// Read segment bytes back.
	rr = do(t, h, http.MethodGet, "/v1/internal/segment?key="+app.Key, "tok", nil)
	if rr.Code != http.StatusOK || !bytes.Equal(rr.Body.Bytes(), seg) {
		t.Fatalf("read segment: code=%d len=%d", rr.Code, rr.Body.Len())
	}
	_ = ctx
}

func TestCommitFleetAndBucketFallback(t *testing.T) {
	ownerSrv, _ := newTestServer(t)
	followerSrv, _ := newTestServer(t)
	fh := httptest.NewServer(followerSrv.Handler())
	defer fh.Close()
	oh := ownerSrv.Handler()
	sc := "demo/__kv__/main"

	// Claim owner on the owner node.
	if rr := do(t, oh, http.MethodPost, "/v1/internal/claim", "tok", []byte(`{"scope":"`+sc+`"}`)); rr.Code != http.StatusOK {
		t.Fatalf("claim: %d %s", rr.Code, rr.Body.String())
	}

	seg := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: 1, EndTxID: 1}, []byte("data"))
	segB64 := base64.StdEncoding.EncodeToString(seg)

	// Fleet posture: a reachable follower acks the write.
	body := fmt.Sprintf(`{"scope":%q,"epoch":1,"segment":%q,"followers":[%q]}`, sc, segB64, fh.URL)
	rr := do(t, oh, http.MethodPost, "/v1/internal/commit", "tok", []byte(body))
	if rr.Code != http.StatusOK {
		t.Fatalf("fleet commit: %d %s", rr.Code, rr.Body.String())
	}
	var res struct {
		Mode    string `json:"mode"`
		AckedBy string `json:"acked_by"`
	}
	_ = json.Unmarshal(rr.Body.Bytes(), &res)
	if res.Mode != "fleet" || res.AckedBy != fh.URL {
		t.Fatalf("fleet commit result = %+v", res)
	}

	// The follower must have fsynced the segment to its spool.
	scp, _ := cell.ParseScope(sc)
	if keys, err := followerSrv.Spool.List(scp, 1); err != nil || len(keys) != 1 {
		t.Fatalf("follower spool = %v, %v", keys, err)
	}

	// Bucket posture: no followers -> synchronous bucket upload.
	body2 := fmt.Sprintf(`{"scope":%q,"epoch":1,"segment":%q,"followers":[]}`, sc, segB64)
	rr2 := do(t, oh, http.MethodPost, "/v1/internal/commit", "tok", []byte(body2))
	if rr2.Code != http.StatusOK {
		t.Fatalf("bucket commit: %d %s", rr2.Code, rr2.Body.String())
	}
	var res2 struct {
		Mode string `json:"mode"`
	}
	_ = json.Unmarshal(rr2.Body.Bytes(), &res2)
	if res2.Mode != "bucket-batch" {
		t.Fatalf("bucket commit mode = %q", res2.Mode)
	}

	// Bucket posture: unreachable follower -> fall back to bucket.
	body3 := fmt.Sprintf(`{"scope":%q,"epoch":1,"segment":%q,"followers":["http://127.0.0.1:1"]}`, sc, segB64)
	rr3 := do(t, oh, http.MethodPost, "/v1/internal/commit", "tok", []byte(body3))
	if rr3.Code != http.StatusOK || !strings.Contains(rr3.Body.String(), `"bucket-batch"`) {
		t.Fatalf("fallback commit: %d %s", rr3.Code, rr3.Body.String())
	}
}

func TestCommitBucketModes(t *testing.T) {
	for _, wait := range []bool{true, false} {
		mode := "async"
		if wait {
			mode = "batch"
		}
		t.Run(mode, func(t *testing.T) {
			srv, _ := newTestServer(t)
			srv.Cfg.BucketWait = wait
			h := srv.Handler()
			sc := "demo/__kv__/" + mode
			if rr := do(t, h, http.MethodPost, "/v1/internal/claim", "tok", []byte(`{"scope":"`+sc+`"}`)); rr.Code != http.StatusOK {
				t.Fatalf("claim: %d %s", rr.Code, rr.Body.String())
			}
			seg := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: 1, EndTxID: 1}, []byte("bucket-"+mode))
			body := fmt.Sprintf(`{"scope":%q,"epoch":1,"segment":%q,"followers":[]}`, sc, base64.StdEncoding.EncodeToString(seg))
			rr := do(t, h, http.MethodPost, "/v1/internal/commit", "tok", []byte(body))
			if rr.Code != http.StatusOK {
				t.Fatalf("commit: %d %s", rr.Code, rr.Body.String())
			}
			var res struct {
				Mode string `json:"mode"`
			}
			_ = json.Unmarshal(rr.Body.Bytes(), &res)
			if res.Mode != "bucket-"+mode {
				t.Fatalf("mode = %q, want bucket-%s", res.Mode, mode)
			}
		})
	}
}

func TestCommitFleetOnSingleNodeWaitsForBucket(t *testing.T) {
	srv, _ := newTestServer(t)
	// fleet requires RPO=0; even with BucketWait=false it must wait (and warn).
	srv.Cfg.Durability = "fleet"
	srv.Cfg.BucketWait = false
	h := srv.Handler()
	sc := "demo/__kv__/main"
	if rr := do(t, h, http.MethodPost, "/v1/internal/claim", "tok", []byte(`{"scope":"`+sc+`"}`)); rr.Code != http.StatusOK {
		t.Fatalf("claim: %d %s", rr.Code, rr.Body.String())
	}
	seg := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: 1, EndTxID: 1}, []byte("fleet-solo"))
	body := fmt.Sprintf(`{"scope":%q,"epoch":1,"segment":%q,"followers":[]}`, sc, base64.StdEncoding.EncodeToString(seg))
	rr := do(t, h, http.MethodPost, "/v1/internal/commit", "tok", []byte(body))
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"bucket-batch"`) {
		t.Fatalf("fleet-without-peer commit = %d %s, want bucket-batch", rr.Code, rr.Body.String())
	}
}

func TestCommitAutoSelectsFollowersFromLeases(t *testing.T) {
	ownerSrv, _ := newTestServer(t)
	followerSrv, _ := newTestServer(t)
	fh := httptest.NewServer(followerSrv.Handler())
	defer fh.Close()
	oh := ownerSrv.Handler()
	sc := "demo/__kv__/main"

	if rr := do(t, oh, http.MethodPost, "/v1/internal/claim", "tok", []byte(`{"scope":"`+sc+`"}`)); rr.Code != http.StatusOK {
		t.Fatalf("claim: %d %s", rr.Code, rr.Body.String())
	}

	// Publish a live peer lease advertising the follower's peer URL.
	lm2 := lease.NewManager(ownerSrv.Bucket, "node-2", "s2", "127.0.0.1:7002", fh.URL, 10*time.Second)
	if err := lm2.Publish(context.Background(), lease.Load{}); err != nil {
		t.Fatalf("publish peer lease: %v", err)
	}

	seg := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: 1, EndTxID: 1}, []byte("auto"))
	segB64 := base64.StdEncoding.EncodeToString(seg)

	// No explicit followers: the owner must discover node-2 from leases.
	body := fmt.Sprintf(`{"scope":%q,"epoch":1,"segment":%q,"followers":[]}`, sc, segB64)
	rr := do(t, oh, http.MethodPost, "/v1/internal/commit", "tok", []byte(body))
	if rr.Code != http.StatusOK {
		t.Fatalf("auto commit: %d %s", rr.Code, rr.Body.String())
	}
	var res struct {
		Mode    string `json:"mode"`
		AckedBy string `json:"acked_by"`
	}
	_ = json.Unmarshal(rr.Body.Bytes(), &res)
	if res.Mode != "fleet" || res.AckedBy != fh.URL {
		t.Fatalf("auto commit result = %+v, want fleet via %s", res, fh.URL)
	}
}

func TestPeerPersistentStreamAppendBatch(t *testing.T) {
	follower, _ := newTestServer(t)
	server := httptest.NewServer(follower.Handler())
	defer server.Close()

	transport := peer.NewStreamTransport("tok", 2, 4)
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "stream"}
	segments := [][]byte{
		ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: 1, EndTxID: 1}, []byte("a")),
		ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: 2, EndTxID: 2}, []byte("b")),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := transport.AppendBatch(ctx, server.URL, sc, 1, segments); err != nil {
		t.Fatalf("stream append batch: %v", err)
	}
	got, err := follower.Spool.Segments(sc, 1)
	if err != nil || len(got) != len(segments) {
		t.Fatalf("spooled segments = %d, %v", len(got), err)
	}
	for i := range segments {
		if !bytes.Equal(got[i], segments[i]) {
			t.Fatalf("segment %d mismatch", i)
		}
	}
}

func TestCommitAcceptsLargeLTXJSON(t *testing.T) {
	srv, _ := newTestServer(t)
	handler := srv.Handler()
	scope := "demo/__kv__/large"
	if rr := do(t, handler, http.MethodPost, "/v1/internal/claim", "tok", []byte(`{"scope":"`+scope+`"}`)); rr.Code != http.StatusOK {
		t.Fatalf("claim: %d %s", rr.Code, rr.Body.String())
	}
	segment := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: 1, EndTxID: 1}, bytes.Repeat([]byte("x"), 800<<10))
	body := fmt.Sprintf(`{"scope":%q,"epoch":1,"segment":%q,"followers":[]}`, scope, base64.StdEncoding.EncodeToString(segment))
	if len(body) <= 1<<20 {
		t.Fatalf("test body is not larger than 1 MiB")
	}
	rr := do(t, handler, http.MethodPost, "/v1/internal/commit", "tok", []byte(body))
	if rr.Code != http.StatusOK {
		t.Fatalf("large commit: %d %s", rr.Code, rr.Body.String())
	}
}

func TestCommitBinaryFleet(t *testing.T) {
	owner, _ := newTestServer(t)
	follower, _ := newTestServer(t)
	followerHTTP := httptest.NewServer(follower.Handler())
	defer followerHTTP.Close()
	scopeText := "demo/__kv__/binary"
	if rr := do(t, owner.Handler(), http.MethodPost, "/v1/internal/claim", "tok", []byte(`{"scope":"`+scopeText+`"}`)); rr.Code != http.StatusOK {
		t.Fatalf("claim: %d %s", rr.Code, rr.Body.String())
	}
	segment := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: 1, EndTxID: 1}, []byte("binary"))
	req := httptest.NewRequest(http.MethodPost, "/v1/internal/commit_binary?scope="+scopeText+"&epoch=1", bytes.NewReader(segment))
	req.Header.Set("x-cellhive-internal-token", "tok")
	req.Header.Set("content-type", "application/octet-stream")
	req.Header.Set("x-cellhive-followers", followerHTTP.URL)
	rr := httptest.NewRecorder()
	owner.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"fleet"`) {
		t.Fatalf("binary commit: %d %s", rr.Code, rr.Body.String())
	}
	scope, _ := cell.ParseScope(scopeText)
	segments, err := follower.Spool.Segments(scope, 1)
	if err != nil || len(segments) != 1 || !bytes.Equal(segments[0], segment) {
		t.Fatalf("follower segments = %d, %v", len(segments), err)
	}
}

func TestPeerStreamAppendBatchAsync(t *testing.T) {
	follower, _ := newTestServer(t)
	server := httptest.NewServer(follower.Handler())
	defer server.Close()

	transport := peer.NewStreamTransport("tok", 2, 4)
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "async"}
	segment := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: 1, EndTxID: 1}, []byte("a"))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ch, err := transport.AppendBatchAsync(ctx, server.URL, sc, 1, [][]byte{segment})
	if err != nil {
		t.Fatalf("append async: %v", err)
	}
	select {
	case err := <-ch:
		if err != nil {
			t.Fatalf("ack: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("async ack timed out")
	}
	got, err := follower.Spool.Segments(sc, 1)
	if err != nil || len(got) != 1 {
		t.Fatalf("spooled = %d, %v", len(got), err)
	}
}

func TestCommitBinaryPipelinedFleet(t *testing.T) {
	owner, _ := newTestServer(t)
	follower, _ := newTestServer(t)
	followerHTTP := httptest.NewServer(follower.Handler())
	defer followerHTTP.Close()

	streamT := peer.NewStreamTransport("tok", 2, 4)
	shipper := peer.NewShipBatcher(streamT, 256, 4<<20, time.Millisecond, 1, 1, 0, 0)
	ctx, cancel := context.WithCancel(context.Background())
	shipper.Start(ctx)
	defer func() { cancel(); shipper.Stop() }()
	owner.PeerShipper = shipper

	scope := "demo/__kv__/pipelined"
	if rr := do(t, owner.Handler(), http.MethodPost, "/v1/internal/claim", "tok", []byte(`{"scope":"`+scope+`"}`)); rr.Code != http.StatusOK {
		t.Fatalf("claim: %d %s", rr.Code, rr.Body.String())
	}
	segment := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: 1, EndTxID: 1}, []byte("x"))
	req := httptest.NewRequest(http.MethodPost, "/v1/internal/commit_binary?scope="+scope+"&epoch=1&pipelined=1&base=1", bytes.NewReader(segment))
	req.Header.Set("x-cellhive-internal-token", "tok")
	req.Header.Set("content-type", "application/octet-stream")
	req.Header.Set("x-cellhive-followers", followerHTTP.URL)

	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		rr := httptest.NewRecorder()
		owner.Handler().ServeHTTP(rr, req)
		done <- rr
	}()
	select {
	case rr := <-done:
		if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"fleet"`) {
			t.Fatalf("pipelined commit: %d %s", rr.Code, rr.Body.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("pipelined commit hung")
	}
}

func TestDrainingRejectsNewClaims(t *testing.T) {
	s, _ := newTestServer(t)
	h := s.Handler()
	sc := "drain/__kv__/main"
	if rr := do(t, h, http.MethodPost, "/v1/internal/claim", "tok", []byte(`{"scope":"`+sc+`"}`)); rr.Code != http.StatusOK {
		t.Fatalf("pre-drain claim = %d: %s", rr.Code, rr.Body.String())
	}
	s.SetDraining(true)
	if !s.Draining() {
		t.Fatalf("Draining() = false after SetDraining(true)")
	}
	sc2 := "drain/__kv__/other"
	rr := do(t, h, http.MethodPost, "/v1/internal/claim", "tok", []byte(`{"scope":"`+sc2+`"}`))
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("draining claim = %d, want 503: %s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "draining") {
		t.Fatalf("draining error body = %s", rr.Body.String())
	}
}

func TestTimerUpsertEndpoint(t *testing.T) {
	s, _ := newTestServer(t)
	h := s.Handler()
	sc := "app/__timers__/main"
	body := []byte(`{"scope":"` + sc + `","due_ms":1000,"kind":"cron","occurrence":"slot1"}`)
	rr := do(t, h, http.MethodPost, "/v1/internal/timer/upsert", "tok", body)
	if rr.Code != http.StatusOK {
		t.Fatalf("upsert = %d: %s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "token") {
		t.Fatalf("response missing token: %s", rr.Body.String())
	}
	found := false
	for _, x := range s.Timers.List() {
		if x == sc {
			found = true
		}
	}
	if !found {
		t.Fatalf("scope not registered: %v", s.Timers.List())
	}
	// An unknown kind is rejected.
	bad := []byte(`{"scope":"` + sc + `","due_ms":1,"kind":"nope","occurrence":"o"}`)
	if rr := do(t, h, http.MethodPost, "/v1/internal/timer/upsert", "tok", bad); rr.Code != http.StatusBadRequest {
		t.Fatalf("invalid kind = %d, want 400", rr.Code)
	}
}

func newForwardServer(t *testing.T, dir, node string) (*Server, *owner.Manager, *httptest.Server) {
	t.Helper()
	b, err := bucket.NewFSBucket(dir)
	if err != nil {
		t.Fatalf("bucket: %v", err)
	}
	cfg := config.Config{NodeID: node, TokenPeer: "tok", TokenInternal: "tok", TokenDispatch: "tok", ScopeSecret: "tok", TokenLog: "tok", AdminToken: "admin-tok", BucketDir: dir, Durability: "bucket", BucketWait: true}
	om := &owner.Manager{B: b, NodeID: node, Session: node + "-s", Role: cell.RoleCellAgent, OwnerTTL: time.Minute}
	srv := New(Deps{Cfg: cfg, Bucket: b, Owner: om, Replica: replica.New(b), Log: slog.New(slog.NewTextHandler(io.Discard, nil))})
	hs := httptest.NewServer(srv.Handler())
	t.Cleanup(hs.Close)
	return srv, om, hs
}

func TestNonOwnerForwardsCommitBinary(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	_, ownerOm, ownerHS := newForwardServer(t, dir, "node-1")
	ownerOm.Advertise = ownerHS.URL
	sc := cell.Scope{Namespace: "app", Class: "__kv__", ID: "fwd"}
	if _, err := ownerOm.Claim(ctx, sc, time.Now()); err != nil {
		t.Fatalf("claim: %v", err)
	}
	_, _, nonOwnerHS := newForwardServer(t, dir, "node-2")

	seg := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: 1, EndTxID: 1}, []byte("p"))
	url := nonOwnerHS.URL + "/v1/internal/commit_binary?scope=" + url.QueryEscape(sc.String()) + "&epoch=1"
	req, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(seg))
	req.Header.Set("x-cellhive-internal-token", "tok")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("forwarded commit: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("forwarded commit status = %d: %s", resp.StatusCode, body)
	}

	// The owner handled it: the segment is in the shared bucket.
	shared, _ := bucket.NewFSBucket(dir)
	rep := replica.New(shared)
	deadline := time.Now().Add(2 * time.Second)
	for {
		keys, kerr := rep.ListSegments(ctx, sc, 1)
		if kerr == nil && len(keys) > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("owner did not commit the forwarded segment: %v %v", keys, kerr)
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Loop guard: an already-forwarded request must not be forwarded again.
	req2, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(seg))
	req2.Header.Set("x-cellhive-internal-token", "tok")
	req2.Header.Set(ownerclient.ForwardedHeader, "1")
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatalf("loop-guard request: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusConflict {
		t.Fatalf("loop-guard status = %d, want 409 not_owner", resp2.StatusCode)
	}
}

func TestInternalBlobRoundTrip(t *testing.T) {
	srv, _ := newTestServer(t)
	h := srv.Handler()
	key := "dosupervisor/workerd___do__/manifest.json"

	if rr := do(t, h, http.MethodPut, "/v1/internal/blob?key="+key, "", []byte(`{"files":[]}`)); rr.Code != http.StatusUnauthorized {
		t.Fatalf("no token: code = %d, want 401", rr.Code)
	}
	body := []byte(`{"files":["host.sqlite"],"raw":[],"facets":{"h":["actor-1"]}}`)
	if rr := do(t, h, http.MethodPut, "/v1/internal/blob?key="+key, "tok", body); rr.Code != http.StatusOK {
		t.Fatalf("put: code = %d body=%s", rr.Code, rr.Body.String())
	}
	rr := do(t, h, http.MethodGet, "/v1/internal/blob?key="+key, "tok", nil)
	if rr.Code != http.StatusOK || !bytes.Equal(rr.Body.Bytes(), body) {
		t.Fatalf("get: code=%d body=%s", rr.Code, rr.Body.String())
	}
	if rr := do(t, h, http.MethodGet, "/v1/internal/blob?key=dosupervisor/missing", "tok", nil); rr.Code != http.StatusNotFound {
		t.Fatalf("missing: code = %d, want 404", rr.Code)
	}
	if rr := do(t, h, http.MethodPut, "/v1/internal/blob?key=cells/evil", "tok", nil); rr.Code != http.StatusBadRequest {
		t.Fatalf("out of prefix: code = %d, want 400", rr.Code)
	}
	if rr := do(t, h, http.MethodPut, "/v1/internal/blob?key=dosupervisor/../cells/x", "tok", nil); rr.Code != http.StatusBadRequest {
		t.Fatalf("traversal: code = %d, want 400", rr.Code)
	}
}

func TestReadSegmentRange(t *testing.T) {
	srv, _ := newTestServer(t)
	h := srv.Handler()
	sc := "demo/__kv__/main"
	seg := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: 1, EndTxID: 1}, []byte("hello-range"))
	if rr := do(t, h, http.MethodPost, "/v1/internal/claim", "tok", []byte(`{"scope":"`+sc+`"}`)); rr.Code != http.StatusOK {
		t.Fatalf("claim: %d", rr.Code)
	}
	rr := do(t, h, http.MethodPost, "/v1/internal/append?scope="+sc+"&epoch=1", "tok", seg)
	if rr.Code != http.StatusOK {
		t.Fatalf("append: %d", rr.Code)
	}
	var app struct {
		Key string `json:"key"`
	}
	_ = json.Unmarshal(rr.Body.Bytes(), &app)

	idx := bytes.Index(seg, []byte("hello-range"))
	if idx < 0 {
		t.Fatal("payload not found in encoded segment")
	}
	req := httptest.NewRequest(http.MethodGet, "/v1/internal/segment?key="+app.Key, nil)
	req.Header.Set("x-cellhive-internal-token", "tok")
	req.Header.Set("range", fmt.Sprintf("bytes=%d-%d", idx, idx+len("hello-range")-1))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusPartialContent {
		t.Fatalf("range code = %d body=%s", rec.Code, rec.Body.String())
	}
	if rec.Body.String() != "hello-range" {
		t.Fatalf("range body = %q, want %q", rec.Body.String(), "hello-range")
	}
	if rr := do(t, h, http.MethodGet, "/v1/internal/segment?key="+app.Key+"&x=1", "tok", nil); rr.Code != http.StatusOK || !bytes.Equal(rr.Body.Bytes(), seg) {
		t.Fatalf("full read regressed: code=%d len=%d", rr.Code, rr.Body.Len())
	}
}

// TestProtocolVersionHandshake covers P5 reader-before-writer: a claim with no
// version (legacy) or a supported version is accepted; an unknown version is
// rejected fail-closed.
func TestProtocolVersionHandshake(t *testing.T) {
	srv, _ := newTestServer(t)
	h := srv.Handler()
	claim := func(body string) *httptest.ResponseRecorder {
		return do(t, h, http.MethodPost, "/v1/internal/claim", "tok", []byte(body))
	}
	if rr := claim(`{"scope":"demo/__kv__/a"}`); rr.Code != http.StatusOK {
		t.Fatalf("legacy claim = %d %s", rr.Code, rr.Body.String())
	}
	if rr := claim(`{"scope":"demo/__kv__/b","proto_version":"v1"}`); rr.Code != http.StatusOK {
		t.Fatalf("v1 claim = %d %s", rr.Code, rr.Body.String())
	}
	if rr := claim(`{"scope":"demo/__kv__/c","proto_version":"v9"}`); rr.Code != http.StatusConflict {
		t.Fatalf("v9 claim = %d, want 409", rr.Code)
	}
	rr := do(t, h, http.MethodGet, "/v1/diagnose", "tok", nil)
	if !bytes.Contains(rr.Body.Bytes(), []byte(`"proto_version":"v1"`)) {
		t.Fatalf("diagnose = %s", rr.Body.String())
	}
}

// TestR2PresignEndpoint covers the tenant R2 presign path (scope-kind r2).
func TestR2PresignEndpoint(t *testing.T) {
	srv, _ := newTestServer(t)
	h := srv.Handler()
	tok, err := scopedtoken.Mint([]byte(srv.Cfg.ScopeSecret), scopedtoken.Claims{Namespace: "acme", Kind: "r2", Name: "BUCKET"})
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	if _, err := srv.R2.Put(context.Background(), "acme", "BUCKET", "k", []byte("x")); err != nil {
		t.Fatalf("put: %v", err)
	}
	req := httptest.NewRequest(http.MethodGet, "/v1/r2/presign?ns=acme&bucket=BUCKET&key=k&expires_in=60", nil)
	req.Header.Set("x-cellhive-scope-token", tok)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK || !bytes.Contains(rr.Body.Bytes(), []byte(`"url"`)) {
		t.Fatalf("presign = %d %s", rr.Code, rr.Body.String())
	}
}

// TestLogIngestAndQuery covers the bounded log buffer endpoints.
func TestLogIngestAndQuery(t *testing.T) {
	srv, _ := newTestServer(t)
	h := srv.Handler()
	admin := srv.AdminHandler()
	// The first entry carries a W3C traceparent, the second inherits the
	// request-level one (header fallback), and must surface as trace_id.
	const tp = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	body := `[{"level":"log","message":"hello","traceparent":"` + tp + `"},{"level":"error","message":"boom"}]`
	{
		req := httptest.NewRequest(http.MethodPost, "/v1/internal/logs?ns=acme&worker=web", strings.NewReader(body))
		req.Header.Set("x-cellhive-internal-token", "tok")
		req.Header.Set("traceparent", tp)
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("ingest = %d %s", rr.Code, rr.Body.String())
		}
	}
	// Wrong token (internal instead of log) is rejected.
	if rr := do(t, h, http.MethodPost, "/v1/internal/logs?ns=acme&worker=web", "wrong", []byte(body)); rr.Code != http.StatusUnauthorized {
		t.Fatalf("bad token = %d, want 401", rr.Code)
	}
	rr := adminDo(t, admin, http.MethodGet, "/v1/control/logs?namespace=acme&worker=web", "admin-tok", "")
	if rr.Code != http.StatusOK || !bytes.Contains(rr.Body.Bytes(), []byte(`"hello"`)) || !bytes.Contains(rr.Body.Bytes(), []byte(`"boom"`)) {
		t.Fatalf("query = %d %s", rr.Code, rr.Body.String())
	}
	if !bytes.Contains(rr.Body.Bytes(), []byte(`"trace_id":"4bf92f3577b34da6a3ce929d0e0e4736"`)) {
		t.Fatalf("query missing trace_id (per-entry and header fallback): %s", rr.Body.String())
	}
	// since cursor advanced past all entries -> empty.
	rr = adminDo(t, admin, http.MethodGet, "/v1/control/logs?namespace=acme&worker=web&since=2", "admin-tok", "")
	if bytes.Contains(rr.Body.Bytes(), []byte(`"hello"`)) {
		t.Fatalf("since should exclude old entries: %s", rr.Body.String())
	}
}

// newControlForwardServer builds a minimal cell-agent that can execute
// control-plane writes: shared bucket (so owner records are visible across
// nodes), separate cellstores (so we can tell which node executed a write).
func newControlForwardServer(t *testing.T, bucketDir, node string) (*Server, *owner.Manager, *httptest.Server) {
	t.Helper()
	b, err := bucket.NewFSBucket(bucketDir)
	if err != nil {
		t.Fatalf("bucket: %v", err)
	}
	cfg := config.Config{
		NodeID: node, TokenPeer: "tok", TokenInternal: "tok", TokenDispatch: "tok",
		ScopeSecret: "tok", TokenLog: "tok", AdminToken: "admin-tok",
		BucketDir: bucketDir, Durability: "bucket", BucketWait: true,
	}
	om := &owner.Manager{B: b, NodeID: node, Session: node + "-s", Role: cell.RoleCellAgent, OwnerTTL: time.Minute}
	cs, err := cellstore.New(filepath.Join(bucketDir, "cells-"+node))
	if err != nil {
		t.Fatalf("cellstore: %v", err)
	}
	srv := New(Deps{
		Cfg: cfg, Bucket: b, Owner: om, Replica: replica.New(b), Store: cs,
		Control: control.New(cs, nil), D1: d1.New(cs),
		Queue: queue.New(cs), Workflows: workflow.New(cs),
		Timers: timer.NewRegistry(),
		OpenTimer: func(ctx context.Context, sc cell.Scope) (*timer.Store, error) {
			c, err := cs.Cell(ctx, sc)
			if err != nil {
				return nil, err
			}
			return timer.NewStore(ctx, c)
		},
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	hs := httptest.NewServer(srv.Handler())
	t.Cleanup(hs.Close)
	return srv, om, hs
}

// TestControlWriteClaimsAndForwards covers ADR-118: a control-plane write works
// on any node — forwarded to the live owner, or claimed transparently when the
// cell is unowned.
func TestControlWriteClaimsAndForwards(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	srvA, omA, hsA := newControlForwardServer(t, dir, "node-1")
	srvB, omB, _ := newControlForwardServer(t, dir, "node-2")
	sc := control.Scope()

	// (a) A owns the control cell: B's write must be forwarded to A.
	omA.Advertise = hsA.URL
	if _, err := omA.Claim(ctx, sc, time.Now()); err != nil {
		t.Fatalf("claim A: %v", err)
	}
	// The deploy handler validates that the bundle exists, so seed one.
	bb, err := bucket.NewFSBucket(dir)
	if err != nil {
		t.Fatalf("bucket: %v", err)
	}
	sha, _, err := artifacts.New(bb).PutBundle(ctx, []byte("export default {}"))
	if err != nil {
		t.Fatalf("put bundle: %v", err)
	}
	rr := do(t, srvB.Handler(), http.MethodPost, "/v1/control/deploy", "tok",
		[]byte(`{"namespace":"app","worker":"web","bundle_sha":"`+sha+`"}`))
	if rr.Code != http.StatusOK {
		t.Fatalf("forwarded deploy = %d: %s", rr.Code, rr.Body.String())
	}
	rels, err := srvA.Control.Releases(ctx, "app", "web")
	if err != nil || len(rels) != 1 {
		t.Fatalf("owner release log = %+v, %v; want the forwarded deploy", rels, err)
	}
	if relsB, _ := srvB.Control.Releases(ctx, "app", "web"); len(relsB) != 0 {
		t.Fatalf("non-owner executed the write locally: %+v", relsB)
	}

	// (b) A releases; a node with no cached owner view claims the cell
	// transparently on write.
	o, _, err := omA.Resolve(ctx, sc)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if err := omA.Release(ctx, sc, o.Epoch); err != nil {
		t.Fatalf("release A: %v", err)
	}
	srvC, omC, hsC := newControlForwardServer(t, dir, "node-3")
	_ = omB
	omC.Advertise = hsC.URL
	if _, err := srvC.Control.CreateApp(ctx, "app", "t"); err != nil {
		t.Fatalf("app: %v", err)
	}
	if _, err := srvC.Control.PutCustomHost(ctx, "app", "app.test", "ops"); err != nil {
		t.Fatalf("register host: %v", err)
	}
	rr = do(t, srvC.Handler(), http.MethodPost, "/v1/control/route", "tok",
		[]byte(`{"namespace":"app","host":"app.test","worker":"web"}`))
	if rr.Code != http.StatusOK {
		t.Fatalf("claim-on-write route = %d: %s", rr.Code, rr.Body.String())
	}
	if o2, _, err := omC.Resolve(ctx, sc); err != nil || o2.Node != "node-3" {
		t.Fatalf("owner after claim = %+v, %v; want node-3", o2, err)
	}
	proj, err := srvC.Control.Projection(ctx)
	if err != nil {
		t.Fatalf("projection: %v", err)
	}
	found := false
	for _, app := range proj.Apps {
		for _, rt := range app.Routes {
			if rt.Host == "app.test" {
				found = true
			}
		}
	}
	if !found {
		t.Fatalf("claimed write did not land locally: %+v", proj)
	}

	// (c) D1 writes go through the same gate: while C owns the cell, a D1 exec on
	// B is forwarded and executed by C.
	for _, srv := range []*Server{srvB, srvC} {
		if _, err := srv.Control.CreateResource(ctx, "app", "d1", "DB", "app/__d1__/db", "t"); err != nil {
			t.Fatalf("resource: %v", err)
		}
	}
	// C also owns the D1 cell, so B's D1 write must be forwarded.
	if _, err := omC.Claim(ctx, d1.Scope("app", "db"), time.Now()); err != nil {
		t.Fatalf("claim d1: %v", err)
	}
	tok, err := scopedtoken.Mint([]byte("tok"), scopedtoken.Claims{Namespace: "app", Kind: "d1", Name: "DB"})
	if err != nil {
		t.Fatalf("scope token: %v", err)
	}
	rec := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodPost, "/v1/d1/exec?ns=app&db=db",
		strings.NewReader(`{"sql":"CREATE TABLE t (n INTEGER); INSERT INTO t VALUES (7)"}`))
	req.Header.Set("x-cellhive-internal-token", "tok")
	req.Header.Set("x-cellhive-scope-token", tok)
	srvB.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("forwarded d1 exec = %d: %s", rec.Code, rec.Body.String())
	}
	res, err := srvC.D1.Query(ctx, "app", "db", "SELECT n FROM t", nil)
	if err != nil || len(res.Rows) != 1 {
		t.Fatalf("owner did not execute the forwarded d1 write: %+v, %v", res, err)
	}
	if got := fmt.Sprint(res.Rows[0][0]); got != "7" {
		t.Fatalf("d1 row = %v, want 7", res.Rows[0])
	}
	if _, err := srvB.D1.Query(ctx, "app", "db", "SELECT n FROM t", nil); err == nil {
		t.Fatal("non-owner executed the d1 write locally")
	}

	// (d) Queue writes go through the same gate.
	for _, srv := range []*Server{srvB, srvC} {
		if _, err := srv.Control.CreateResource(ctx, "app", "queue", "JOBS", "app/__queue__/jobs", "t"); err != nil {
			t.Fatalf("queue resource: %v", err)
		}
	}
	if _, err := omC.Claim(ctx, queue.Scope("app", "jobs"), time.Now()); err != nil {
		t.Fatalf("claim queue: %v", err)
	}
	qtok, err := scopedtoken.Mint([]byte("tok"), scopedtoken.Claims{Namespace: "app", Kind: "queue", Name: "JOBS"})
	if err != nil {
		t.Fatalf("queue scope token: %v", err)
	}
	rec = httptest.NewRecorder()
	req, _ = http.NewRequest(http.MethodPost, "/v1/queue/send?ns=app&queue=jobs", strings.NewReader("hello"))
	req.Header.Set("x-cellhive-internal-token", "tok")
	req.Header.Set("x-cellhive-scope-token", qtok)
	req.Header.Set("content-type", "text/plain")
	srvB.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("forwarded queue send = %d: %s", rec.Code, rec.Body.String())
	}
	if depth, _ := srvC.Queue.Depth(ctx, "app", "jobs"); depth != 1 {
		t.Fatalf("owner queue depth = %d, want 1", depth)
	}
	if depth, _ := srvB.Queue.Depth(ctx, "app", "jobs"); depth != 0 {
		t.Fatalf("non-owner executed the queue send locally (depth %d)", depth)
	}

	// (e) Workflow writes go through the same gate.
	for _, srv := range []*Server{srvB, srvC} {
		if _, err := srv.Control.CreateResource(ctx, "app", "workflow", "WF", "class", "t"); err != nil {
			t.Fatalf("workflow resource: %v", err)
		}
	}
	if _, err := omC.Claim(ctx, workflow.Scope("app", "wf"), time.Now()); err != nil {
		t.Fatalf("claim workflow: %v", err)
	}
	wtok, err := scopedtoken.Mint([]byte("tok"), scopedtoken.Claims{Namespace: "app", Kind: "workflow", Name: "WF"})
	if err != nil {
		t.Fatalf("workflow scope token: %v", err)
	}
	rec = httptest.NewRecorder()
	req, _ = http.NewRequest(http.MethodPost, "/v1/workflow/create?ns=app&workflow=wf", strings.NewReader(`{}`))
	req.Header.Set("x-cellhive-internal-token", "tok")
	req.Header.Set("x-cellhive-scope-token", wtok)
	srvB.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("forwarded workflow create = %d: %s", rec.Code, rec.Body.String())
	}
	var wf struct {
		ID string `json:"id"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &wf)
	if wf.ID == "" {
		t.Fatalf("workflow create response = %s", rec.Body.String())
	}
	if _, err := srvC.Workflows.Get(ctx, "app", "wf", wf.ID); err != nil {
		t.Fatalf("owner did not execute the forwarded workflow create: %v", err)
	}
	if _, err := srvB.Workflows.Get(ctx, "app", "wf", wf.ID); err == nil {
		t.Fatal("non-owner executed the workflow create locally")
	}
}

// TestReadForwardingAndRetry covers ADR-120: reads on a non-owner are forwarded
// to the owner (never served from a stale local copy), and a forward retries at
// most once — only for a dial failure or a 409 from the old owner.
func TestReadForwardingAndRetry(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	srvA, omA, hsA := newControlForwardServer(t, dir, "node-1")
	srvB, _, _ := newControlForwardServer(t, dir, "node-2")
	omA.Advertise = hsA.URL

	// A owns the KV cell; B must forward reads to it.
	kvScope := cell.Scope{Namespace: "app", Class: "__kv__", ID: "kv"}
	for _, srv := range []*Server{srvA, srvB} {
		if _, err := srv.Control.CreateResource(ctx, "app", "kv", "KV", kvScope.String(), "t"); err != nil {
			t.Fatalf("kv resource: %v", err)
		}
	}
	if _, err := omA.Claim(ctx, kvScope, time.Now()); err != nil {
		t.Fatalf("claim kv: %v", err)
	}
	ktok, err := scopedtoken.Mint([]byte("tok"), scopedtoken.Claims{Namespace: "app", Kind: "kv", Name: "KV"})
	if err != nil {
		t.Fatalf("kv scope token: %v", err)
	}
	call := func(srv *Server, method, target, body string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		req, _ := http.NewRequest(method, target, strings.NewReader(body))
		req.Header.Set("x-cellhive-internal-token", "tok")
		req.Header.Set("x-cellhive-scope-token", ktok)
		srv.Handler().ServeHTTP(rec, req)
		return rec
	}
	if rec := call(srvA, http.MethodPost, "/v1/kv/put?ns=app&key=k", "hello"); rec.Code != http.StatusOK {
		t.Fatalf("owner kv put = %d: %s", rec.Code, rec.Body.String())
	}
	rec := call(srvB, http.MethodGet, "/v1/kv/get?ns=app&key=k", "")
	if rec.Code != http.StatusOK || rec.Body.String() != "hello" {
		t.Fatalf("forwarded kv get = %d %q, want 200 hello", rec.Code, rec.Body.String())
	}
	if c, cerr := srvB.Store.Cell(ctx, kvScope); cerr == nil {
		if _, _, gerr := c.Get(ctx, "k"); gerr == nil {
			t.Fatal("non-owner served the read from its own copy")
		}
	}

	// A stale owner that keeps answering 409 is retried exactly once (two calls).
	var mu sync.Mutex
	attempts := 0
	stale := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		attempts++
		mu.Unlock()
		writeJSON(w, http.StatusConflict, map[string]any{"error": "not_owner"})
	}))
	defer stale.Close()
	// Hand the cell to a stale owner that keeps answering 409.
	oA, _, err := omA.Resolve(ctx, kvScope)
	if err != nil {
		t.Fatalf("resolve A: %v", err)
	}
	if err := omA.Release(ctx, kvScope, oA.Epoch); err != nil {
		t.Fatalf("release A: %v", err)
	}
	omX := &owner.Manager{B: srvB.Bucket, NodeID: "node-x", Session: "sx", Advertise: stale.URL,
		Role: cell.RoleCellAgent, OwnerTTL: time.Minute}
	if _, err := omX.Claim(ctx, kvScope, time.Now()); err != nil {
		t.Fatalf("claim by node-x: %v", err)
	}
	srvB.Owner.Invalidate(kvScope)
	mu.Lock()
	attempts = 0
	mu.Unlock()
	rec = call(srvB, http.MethodGet, "/v1/kv/get?ns=app&key=k", "")
	if rec.Code != http.StatusConflict {
		t.Fatalf("stale-owner forward = %d, want 409 relayed", rec.Code)
	}
	mu.Lock()
	got := attempts
	mu.Unlock()
	if got != 2 {
		t.Fatalf("stale owner received %d attempts, want 2 (one retry)", got)
	}

	// A dead address is a dial failure: one retry, then 502 (never a hang/loop).
	oX, _, err := omX.Resolve(ctx, kvScope)
	if err != nil {
		t.Fatalf("resolve X: %v", err)
	}
	if err := omX.Release(ctx, kvScope, oX.Epoch); err != nil {
		t.Fatalf("release X: %v", err)
	}
	omD := &owner.Manager{B: srvB.Bucket, NodeID: "node-d", Session: "sd", Advertise: "http://127.0.0.1:9",
		Role: cell.RoleCellAgent, OwnerTTL: time.Minute}
	if _, err := omD.Claim(ctx, kvScope, time.Now()); err != nil {
		t.Fatalf("claim dead: %v", err)
	}
	srvB.Owner.Invalidate(kvScope)
	rec = call(srvB, http.MethodGet, "/v1/kv/get?ns=app&key=k", "")
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("dead-owner forward = %d, want 502", rec.Code)
	}
}

// TestTimerUpsertForwardsToOwner covers the timer write path (ADR-121): the
// unified timer upsert claims or forwards like every other cell write, and the
// DO alarm upsert now goes through capture too.
func TestTimerUpsertForwardsToOwner(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	srvA, omA, hsA := newControlForwardServer(t, dir, "node-1")
	srvB, _, _ := newControlForwardServer(t, dir, "node-2")
	omA.Advertise = hsA.URL
	sc := cell.Scope{Namespace: "app", Class: "__timer__", ID: "x"}
	if _, err := omA.Claim(ctx, sc, time.Now()); err != nil {
		t.Fatalf("claim timer scope: %v", err)
	}
	body := `{"scope":"app/__timer__/x","due_ms":` + fmt.Sprint(time.Now().Add(time.Minute).UnixMilli()) + `,"kind":"cron","occurrence":"o1"}`
	rr := do(t, srvB.Handler(), http.MethodPost, "/v1/internal/timer/upsert", "tok", []byte(body))
	if rr.Code != http.StatusOK {
		t.Fatalf("forwarded timer upsert = %d: %s", rr.Code, rr.Body.String())
	}
	stA, err := srvA.OpenTimer(ctx, sc)
	if err != nil {
		t.Fatalf("open owner timer: %v", err)
	}
	if next, _ := stA.NextDue(ctx); next == 0 {
		t.Fatal("owner did not store the forwarded timer")
	}
	stB, err := srvB.OpenTimer(ctx, sc)
	if err != nil {
		t.Fatalf("open non-owner timer: %v", err)
	}
	if next, _ := stB.NextDue(ctx); next != 0 {
		t.Fatalf("non-owner stored the timer locally (next=%d)", next)
	}

	// Unowned scope: the non-owner claims it and stores locally.
	sc2 := cell.Scope{Namespace: "app", Class: "__timer__", ID: "y"}
	body2 := `{"scope":"app/__timer__/y","due_ms":` + fmt.Sprint(time.Now().Add(time.Minute).UnixMilli()) + `,"kind":"cron","occurrence":"o2"}`
	if rr := do(t, srvB.Handler(), http.MethodPost, "/v1/internal/timer/upsert", "tok", []byte(body2)); rr.Code != http.StatusOK {
		t.Fatalf("claim-on-write timer upsert = %d: %s", rr.Code, rr.Body.String())
	}
	if o, _, err := srvB.Owner.Resolve(ctx, sc2); err != nil || o.Node != "node-2" {
		t.Fatalf("timer scope owner = %+v, %v; want node-2", o, err)
	}
	st2, err := srvB.OpenTimer(ctx, sc2)
	if err != nil {
		t.Fatalf("open timer: %v", err)
	}
	if next, _ := st2.NextDue(ctx); next == 0 {
		t.Fatal("claimed timer was not stored locally")
	}
}

// TestOverloadedRefusesClaims covers the disk high-watermark backpressure
// (ADR-123): while a node reports pressure it is not ready, refuses new claims,
// and does not claim cells for writes.
func TestOverloadedRefusesClaims(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	_, omA, hsA := newControlForwardServer(t, dir, "node-1")
	srvB, _, _ := newControlForwardServer(t, dir, "node-2")
	omA.Advertise = hsA.URL
	_ = omA

	pressure := true
	srvB.Overloaded = func() bool { return pressure }

	// Readiness reports overload.
	rr := do(t, srvB.Handler(), http.MethodGet, "/readyz", "tok", nil)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("readyz under pressure = %d, want 503", rr.Code)
	}
	// New claims are refused.
	sc := cell.Scope{Namespace: "app", Class: "__kv__", ID: "kv"}
	if rr := do(t, srvB.Handler(), http.MethodPost, "/v1/internal/claim", "tok",
		[]byte(`{"scope":"`+sc.String()+`"}`)); rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("claim under pressure = %d, want 503", rr.Code)
	}
	if _, _, err := srvB.Owner.Resolve(ctx, sc); err == nil {
		t.Fatal("node claimed a cell while overloaded")
	}

	// A write that would claim is refused rather than claiming locally.
	if _, err := srvB.Control.CreateResource(ctx, "app", "kv", "KV", sc.String(), "t"); err != nil {
		t.Fatalf("resource: %v", err)
	}
	ktok, err := scopedtoken.Mint([]byte("tok"), scopedtoken.Claims{Namespace: "app", Kind: "kv", Name: "KV"})
	if err != nil {
		t.Fatalf("scope token: %v", err)
	}
	put := func() *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodPost, "/v1/kv/put?ns=app&key=k", strings.NewReader("v"))
		req.Header.Set("x-cellhive-internal-token", "tok")
		req.Header.Set("x-cellhive-scope-token", ktok)
		srvB.Handler().ServeHTTP(rec, req)
		return rec
	}
	if rr := put(); rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("write under pressure = %d, want 503: %s", rr.Code, rr.Body.String())
	}
	if _, _, err := srvB.Owner.Resolve(ctx, sc); err == nil {
		t.Fatal("node claimed a cell for a write while overloaded")
	}

	// Pressure lifts: the same write claims and succeeds.
	pressure = false
	if rr := put(); rr.Code != http.StatusOK {
		t.Fatalf("write after pressure = %d: %s", rr.Code, rr.Body.String())
	}
	if o, _, err := srvB.Owner.Resolve(ctx, sc); err != nil || o.Node != "node-2" {
		t.Fatalf("owner after pressure = %+v, %v; want node-2", o, err)
	}
}

// TestMetricsObservability covers the ADR-165 metrics: binding calls by kind
// and outcome, the durability-proof histogram, owner lifecycle counters, the
// projection version gauge and the peer hedge counters.
func TestMetricsObservability(t *testing.T) {
	s, _ := newTestServer(t)
	s.recordBindingCall("acme", "kv", 200)
	s.recordBindingCall("acme", "d1", 403)
	s.recordProof("acme", 3*time.Millisecond)
	s.recordProof("acme", 700*time.Millisecond)
	s.projRev.Store(7)

	rr := httptest.NewRecorder()
	s.handleMetrics(rr, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rr.Body.String()
	for _, want := range []string{
		`cellhive_binding_calls_total{ns="acme",kind="kv",outcome="ok"} 1`,
		`cellhive_binding_calls_total{ns="acme",kind="d1",outcome="denied"} 1`,
		`cellhive_durability_proof_seconds_bucket{ns="acme",le="0.005"} 1`,
		`cellhive_durability_proof_seconds_bucket{ns="acme",le="1"} 2`,
		`cellhive_durability_proof_seconds_bucket{ns="acme",le="+Inf"} 2`,
		`cellhive_durability_proof_seconds_count{ns="acme"} 2`,
		`cellhive_durability_proof_seconds_sum{ns="acme"} 0.703`,
		`cellhive_route_projection_version 7`,
		`cellhive_owner_epoch_changes_total{role="owner"}`,
		`cellhive_takeover_total{outcome="success"} 0`,
		`cellhive_peer_hedge_fired_total 0`,
		`cellhive_peer_hedge_won_total 0`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics missing %q\n%s", want, body)
		}
	}
}

// TestLogSubscribeEnablesExport covers ADR-172: the admin subscribe endpoint
// registers a TTL window so "tail" mode exports that worker's logs.
func TestLogSubscribeEnablesExport(t *testing.T) {
	srv, _ := newTestServer(t)
	admin := srv.AdminHandler()
	rr := adminDo(t, admin, http.MethodPost, "/v1/control/logs/subscribe", "admin-tok",
		`{"namespace":"acme","worker":"web","ttl_ms":60000}`)
	if rr.Code != http.StatusOK || !bytes.Contains(rr.Body.Bytes(), []byte(`"subscribed":true`)) {
		t.Fatalf("subscribe = %d %s", rr.Code, rr.Body.String())
	}
	if !telemetry.LogSubscribed("acme", "web") {
		t.Fatal("subscription not visible to the exporter")
	}
	// Missing worker -> 400.
	if rr := adminDo(t, admin, http.MethodPost, "/v1/control/logs/subscribe", "admin-tok",
		`{"namespace":"acme"}`); rr.Code != http.StatusBadRequest {
		t.Fatalf("bad subscribe = %d, want 400", rr.Code)
	}
}

// TestLogSubscribeFleetFanout covers ADR-173: an admin subscription is
// broadcast to the other live cell-agents (so a tail covers workers served on
// any node) and the internal endpoint applies such a broadcast locally.
func TestLogSubscribeFleetFanout(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	b, err := bucket.NewFSBucket(dir)
	if err != nil {
		t.Fatal(err)
	}
	var got atomic.Int64
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/internal/logs/subscribe" {
			if r.Header.Get("x-cellhive-internal-token") != "tok" {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			got.Add(1)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer peer.Close()
	advert := strings.TrimPrefix(peer.URL, "http://")
	self := lease.NewManager(b, "n1", "s1", "127.0.0.1:7001", "n1:7001", 30*time.Second)
	other := lease.NewManager(b, "n2", "s2", advert, advert, 30*time.Second)
	if err := self.Publish(ctx, lease.Load{}); err != nil {
		t.Fatalf("publish self: %v", err)
	}
	if err := other.Publish(ctx, lease.Load{}); err != nil {
		t.Fatalf("publish other: %v", err)
	}
	srv := New(Deps{
		Cfg: config.Config{
			NodeID: "n1", TokenInternal: "tok", AdminToken: "admin-tok", TokenLog: "tok",
			ScopeSecret: "s", BucketDir: dir, Durability: "bucket", BucketWait: true,
		},
		Bucket: b, Lease: self, Logs: logbuf.New(10, 10),
	})

	rr := adminDo(t, srv.AdminHandler(), http.MethodPost, "/v1/control/logs/subscribe", "admin-tok",
		`{"namespace":"acme","worker":"web","ttl_ms":60000}`)
	if rr.Code != http.StatusOK {
		t.Fatalf("admin subscribe = %d %s", rr.Code, rr.Body.String())
	}
	if !telemetry.LogSubscribed("acme", "web") {
		t.Fatal("local subscription missing")
	}
	deadline := time.Now().Add(5 * time.Second)
	for got.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if got.Load() == 0 {
		t.Fatal("peer node was not fanned out the subscription")
	}

	// The internal endpoint applies a peer's broadcast.
	if rr := do(t, srv.Handler(), http.MethodPost, "/v1/internal/logs/subscribe", "tok",
		[]byte(`{"namespace":"team","worker":"api","ttl_ms":1000}`)); rr.Code != http.StatusOK {
		t.Fatalf("internal subscribe = %d %s", rr.Code, rr.Body.String())
	}
	if !telemetry.LogSubscribed("team", "api") {
		t.Fatal("internal subscription missing")
	}
}

// TestMetricsNamespaceLabels covers ADR-179: tenant-attributable metrics carry
// a bounded ns label (binding calls + durability proof), and namespaces beyond
// the configured cap collapse to ns="other" so cardinality cannot explode.
func TestMetricsNamespaceLabels(t *testing.T) {
	srv, _ := newTestServer(t)
	srv.Cfg.MetricsNSMax = 2
	srv.recordBindingCall("acme", "kv", 200)
	srv.recordBindingCall("globex", "d1", 500)
	srv.recordBindingCall("initech", "r2", 200) // beyond the cap
	srv.recordProof("acme", 3*time.Millisecond)
	srv.recordProof("initech", 3*time.Millisecond)
	rr := httptest.NewRecorder()
	srv.handleMetrics(rr, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rr.Body.String()
	for _, want := range []string{
		`cellhive_binding_calls_total{ns="acme",kind="kv",outcome="ok"} 1`,
		`cellhive_binding_calls_total{ns="globex",kind="d1",outcome="error"} 1`,
		`cellhive_binding_calls_total{ns="other",kind="r2",outcome="ok"} 1`,
		`cellhive_durability_proof_seconds_count{ns="acme"} 1`,
		`cellhive_durability_proof_seconds_count{ns="other"} 1`,
	} {
		if !bytes.Contains([]byte(body), []byte(want)) {
			t.Fatalf("metrics missing %q:\n%s", want, body)
		}
	}
	// A request without a namespace is attributed to "platform".
	srv.recordBindingCall("", "kv", 200)
	rr = httptest.NewRecorder()
	srv.handleMetrics(rr, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if !bytes.Contains(rr.Body.Bytes(), []byte(`cellhive_binding_calls_total{ns="platform",kind="kv",outcome="ok"} 1`)) {
		t.Fatalf("missing platform ns attribution:\n%s", rr.Body.String())
	}
}
