package main

import (
	"bytes"
	"cellhive/internal/bearertoken"
	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/cellcapture"
	"cellhive/internal/cellstore"
	"cellhive/internal/compaction"
	"cellhive/internal/config"
	"cellhive/internal/control"
	"cellhive/internal/ltx"
	"cellhive/internal/owner"
	"cellhive/internal/replica"
	"cellhive/internal/restore"
	"cellhive/internal/server"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestEpochFoldCompletionRequiresDurableResult(t *testing.T) {
	for _, tc := range []struct {
		result compaction.Result
		err    error
		done   bool
	}{
		{compaction.Result{Compacted: true}, nil, true},
		{compaction.Result{Skipped: "already compacted"}, nil, true},
		{compaction.Result{Skipped: "waiting for current epoch"}, nil, false},
		{compaction.Result{Skipped: "no snapshot baseline"}, nil, false},
		{compaction.Result{Compacted: true}, errors.New("verify failed"), false},
	} {
		if got := epochFoldComplete(tc.result, tc.err); got != tc.done {
			t.Fatalf("%+v err=%v: got %v", tc.result, tc.err, got)
		}
	}
}

// A real authenticated GET performs claim + hydrate, but no capture Ensure.
// The maintenance loop must create its own continuation without a later write.
func TestEpochFoldLoopMaintainsReadOnlyTakeover(t *testing.T) {
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	rep := replica.New(b)
	sc := cell.Scope{Namespace: "test", Class: "__kv__", ID: "read-only-fold"}
	old := &owner.Manager{B: b, NodeID: "old", Session: "old-session", OwnerTTL: time.Minute}
	first, err := old.Claim(t.Context(), sc, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	source, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	c, err := source.Cell(t.Context(), sc)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.Put(t.Context(), "key", []byte("value"), nil); err != nil {
		t.Fatal(err)
	}
	cut, ps, count, pages, err := c.SnapshotConsistent(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	ordered := make([]ltx.WALPage, 0, len(pages))
	for pg := uint32(1); pg <= count; pg++ {
		ordered = append(ordered, ltx.WALPage{PageNo: pg, Data: pages[pg]})
	}
	raw, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: ps, Commit: count, MinTXID: 1, MaxTXID: cut}, ordered)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = rep.AppendLevel(t.Context(), sc, first.Epoch, 0, raw); err != nil {
		t.Fatal(err)
	}
	if err = old.Release(t.Context(), sc, first.Epoch); err != nil {
		t.Fatal(err)
	}
	store, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	store.Hydrate = func(ctx context.Context, s cell.Scope, path string) error {
		if s != sc {
			return nil
		}
		segments, err := rep.RestoreChain(ctx, s, 0)
		if err != nil {
			return err
		}
		var raws [][]byte
		for _, seg := range segments {
			raws = append(raws, seg.Raw)
		}
		_, err = restore.ApplyFile(path, raws)
		return err
	}
	cfg := config.Config{NodeID: "new", SessionID: "new-session", RootKey: "00112233445566778899aabbccddeeff", CompactionTimeout: time.Second}
	om := &owner.Manager{B: b, NodeID: cfg.NodeID, Session: cfg.SessionID, OwnerTTL: time.Minute}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	envelope, err := control.NewEnvelope(bytes.Repeat([]byte{3}, 32))
	if err != nil {
		t.Fatal(err)
	}
	srv := server.New(server.Deps{Cfg: cfg, Bucket: b, Owner: om, Replica: rep, Store: store, Control: control.New(store, envelope), Log: log})
	if _, err = om.Claim(t.Context(), control.Scope(), time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err = srv.Control.CreateApp(t.Context(), sc.Namespace, "test"); err != nil {
		t.Fatal(err)
	}
	if _, err = srv.Control.CreateResource(t.Context(), sc.Namespace, "kv", sc.ID, sc.String(), "test"); err != nil {
		t.Fatal(err)
	}
	capture := &cellcapture.Manager{Store: store, AutoSnapshot: true, Durable: rep.DurableWatermarkChain, DurableEpoch: rep.DurableWatermark,
		Owner: func(ctx context.Context, s cell.Scope) (bool, uint64, error) {
			o, _, err := om.Resolve(ctx, s)
			return err == nil && o.Node == om.NodeID && o.Session == om.Session, o.Epoch, err
		},
		HasEpoch: func(ctx context.Context, s cell.Scope, e uint64) (bool, error) {
			es, err := rep.Epochs(ctx, s, e)
			for _, v := range es {
				if v == e {
					return true, err
				}
			}
			return false, err
		},
		WriteContinuation: func(ctx context.Context, s cell.Scope, e uint64, h ltx.NativeHeader, pg []ltx.WALPage) error {
			return rep.AppendContinuation(ctx, s, e, h, pg...)
		},
		CheckContinuation: rep.CheckContinuation, Committer: epochTestCommitter{rep}, Log: log,
	}
	defer capture.Stop()
	srv.Capture = capture
	defer srv.WaitBackground()
	httpServer := httptest.NewServer(srv.Handler())
	defer httpServer.Close()
	key, err := cfg.JWTKey("issuer/test")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().Unix()
	token, err := bearertoken.Mint(key, "issuer/test", bearertoken.Claims{Ver: 1, Type: "binding", Subject: "test", Audience: "cellhive", IssuedAt: now, ExpiresAt: now + 60, Namespace: sc.Namespace, Kind: "kv", Name: sc.ID})
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, httpServer.URL+"/v1/kv/get?ns=test&key=key", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	response, err := httpServer.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode != 200 || string(body) != "value" {
		t.Fatalf("GET: %d %s", response.StatusCode, body)
	}
	second, _, err := om.Resolve(t.Context(), sc)
	if err != nil || second.Epoch <= first.Epoch {
		t.Fatalf("GET did not take over: %+v %v", second, err)
	}
	epochs, err := rep.Epochs(t.Context(), sc, 0)
	if err != nil || len(epochs) != 1 || epochs[0] != first.Epoch {
		t.Fatalf("GET unexpectedly published seed: %v %v", epochs, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	stopped := make(chan struct{})
	go func() { defer close(stopped); runEpochFoldLoop(ctx, log, rep, om, cfg, capture.EnsureContinuation) }()
	defer func() { cancel(); <-stopped }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		keys, err := rep.ListLevelFrom(t.Context(), sc, second.Epoch, 9, 0)
		if err != nil {
			t.Fatal(err)
		}
		if len(keys) == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("GET-only takeover never established continuation and folded")
		}
		time.Sleep(10 * time.Millisecond)
	}
	local, err := store.Cell(t.Context(), sc)
	if err != nil {
		t.Fatal(err)
	}
	if tx, err := local.TxID(t.Context()); err != nil || tx != cut+1 {
		t.Fatalf("continuation txid=%d want %d: %v", tx, cut+1, err)
	}
	keys, err := rep.RestoreChainKeys(t.Context(), sc, second.Epoch)
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range keys {
		if !strings.Contains(k, fmt.Sprintf("/e%d/", second.Epoch)) {
			t.Fatalf("still depends on predecessor: %s", k)
		}
	}

	// Restore into a fresh SQLite file from the folded chain, proving that
	// convergence preserved the value without relying on the resident reader.
	restored, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	restored.Hydrate = store.Hydrate
	fresh, err := restored.Cell(t.Context(), sc)
	if err != nil {
		t.Fatal(err)
	}
	value, _, err := fresh.Get(t.Context(), "key")
	if err != nil || string(value) != "value" {
		t.Fatalf("folded restore value=%q: %v", value, err)
	}

}

type epochTestCommitter struct{ rep *replica.Manager }

func (c epochTestCommitter) Commit(ctx context.Context, s cell.Scope, e uint64, raw []byte) error {
	_, _, err := c.rep.Append(ctx, s, e, raw)
	return err
}
