package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"path/filepath"
	"sync"
	"testing"

	"cellhive/internal/cell"
	"cellhive/internal/cellcapture"
	"cellhive/internal/cellstore"
	"cellhive/internal/control"
	"cellhive/internal/ltx"
	"cellhive/internal/replica"
	"cellhive/internal/restore"
)

// TestDeployResumesCaptureWithoutSnapshot pins ADR-272: after a capture restart
// whose local cell equals the bucket's durable watermark, the next deploy only
// emits deltas — no whole-database snapshot — and the chain still restores every
// version.
func TestDeployResumesCaptureWithoutSnapshot(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestServer(t)
	env, err := control.NewEnvelope(bytes.Repeat([]byte{0x6}, 32))
	if err != nil {
		t.Fatal(err)
	}
	s.Control = control.New(s.Store, env)
	s.Cfg.AutoCreateApp = true
	rep := replica.New(s.Bucket)

	var mu sync.Mutex
	var kinds []ltx.Kind
	committer := testCommitterFunc(func(cctx context.Context, sc cell.Scope, epoch uint64, seg []byte) error {
		h, _, err := ltx.Decode(seg)
		if err != nil {
			return err
		}
		if _, _, err := rep.Append(cctx, sc, epoch, seg); err != nil {
			return err
		}
		mu.Lock()
		kinds = append(kinds, h.Kind)
		mu.Unlock()
		return nil
	})
	owner := func(context.Context, cell.Scope) (bool, uint64, error) { return true, 1, nil }
	reset := func() {
		mu.Lock()
		kinds = nil
		mu.Unlock()
	}
	kindsNow := func() []ltx.Kind {
		mu.Lock()
		defer mu.Unlock()
		return append([]ltx.Kind(nil), kinds...)
	}

	s.Capture = &cellcapture.Manager{
		Store: s.Store, AutoSnapshot: true, Owner: owner, Committer: committer,
		SnapshotDone: func(cctx context.Context, sc cell.Scope, epoch, txid uint64) error {
			return rep.MarkSnapshotDone(cctx, sc, epoch, txid)
		},
	}
	admin := s.AdminHandler()
	upload := func(body string) string {
		rr := adminDo(t, admin, http.MethodPost, "/v1/control/bundle", "admin-tok", body)
		if rr.Code != http.StatusOK {
			t.Fatalf("bundle = %d %s", rr.Code, rr.Body.String())
		}
		var out struct {
			SHA string `json:"sha"`
		}
		if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil {
			t.Fatal(err)
		}
		return out.SHA
	}
	deploy := func(sha, key string) {
		body := `{"namespace":"acme","worker":"api","bundle_sha":"` + sha + `","idempotency_key":"` + key + `"}`
		rr := adminDo(t, admin, http.MethodPost, "/v1/control/deploy", "admin-tok", body)
		if rr.Code != http.StatusOK {
			t.Fatalf("deploy = %d %s", rr.Code, rr.Body.String())
		}
	}

	sha1 := upload("export default {v:1}")
	deploy(sha1, "k1")
	if got := kindsNow(); len(got) == 0 || got[0] != ltx.KindSnapshot {
		t.Fatalf("fresh deploy commits = %v, want a snapshot opener", got)
	}

	// Simulate a process restart: capture state is lost, the cell and the bucket
	// chain survive. Resume must continue from the durable watermark.
	s.Capture.Stop()
	reset()
	s.Capture = &cellcapture.Manager{
		Store: s.Store, AutoSnapshot: true, Owner: owner, Committer: committer,
		Durable: func(cctx context.Context, sc cell.Scope, epoch uint64) (uint64, error) {
			return rep.DurableWatermark(cctx, sc, epoch)
		},
		SnapshotDone: func(cctx context.Context, sc cell.Scope, epoch, txid uint64) error {
			return rep.MarkSnapshotDone(cctx, sc, epoch, txid)
		},
	}
	sha2 := upload("export default {v:2}")
	deploy(sha2, "k2")
	gotKinds := kindsNow()
	if len(gotKinds) != 1 || gotKinds[0] != ltx.KindDelta {
		t.Fatalf("resumed deploy commits = %v, want exactly one delta", gotKinds)
	}

	// The bucket chain must still restore both immutable versions.
	segs, err := rep.Restore(ctx, control.Scope(), 1)
	if err != nil || len(segs) == 0 {
		t.Fatalf("restore segments = %d, %v", len(segs), err)
	}
	raw := make([][]byte, 0, len(segs))
	for _, part := range segs {
		raw = append(raw, part.Raw)
	}
	dst := filepath.Join(t.TempDir(), "restored.db")
	if _, err := restore.ApplyFile(dst, raw); err != nil {
		t.Fatalf("apply: %v", err)
	}
	db, err := cellstore.Open(dst)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var versions int
	if err := db.QueryRow(`SELECT count(*) FROM versions WHERE ns='acme' AND worker='api'`).Scan(&versions); err != nil {
		t.Fatal(err)
	}
	if versions != 2 {
		t.Fatalf("restored versions = %d, want 2", versions)
	}
}
