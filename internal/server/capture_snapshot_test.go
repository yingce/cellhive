package server

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/sqlcapture"
)

func TestSnapshotStreamForwardsAndPublishes(t *testing.T) {
	dir := t.TempDir()
	a, owner, ha := newControlForwardServer(t, dir, "snapshot-a")
	b, _, hb := newControlForwardServer(t, dir, "snapshot-b")
	owner.Advertise = ha.URL
	sc := cell.Scope{Namespace: "stream", Class: "c", ID: "snapshot"}
	own, err := owner.Claim(t.Context(), sc, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	raw := nativeTestFile(t, 1, 3, bytes.Repeat([]byte("snapshot"), 2048))
	path := filepath.Join(t.TempDir(), "snapshot.ltx")
	if err = os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	c := sqlcapture.NewHTTPCommitter(hb.URL, b.Cfg.TokenInternal, nil, nil)
	if err = c.CommitSnapshotFile(t.Context(), sc, own.Epoch, 3, path, int64(len(raw))); err != nil {
		t.Fatal(err)
	}
	keys, err := a.Replica.ListSegments(t.Context(), sc, own.Epoch)
	if err != nil || len(keys) != 1 {
		t.Fatalf("keys=%v err=%v", keys, err)
	}
	if err = c.CommitSnapshotFile(t.Context(), sc, own.Epoch, 3, path, int64(len(raw))); err != nil {
		t.Fatalf("retry: %v", err)
	}
	// The same bounded transport accepts an indivisible incremental as L0,
	// retaining its native TXID range rather than disguising it as a snapshot.
	delta := nativeTestFile(t, 4, 4, bytes.Repeat([]byte("changed"), 2048))
	deltaPath := filepath.Join(t.TempDir(), "delta.ltx")
	if err = os.WriteFile(deltaPath, delta, 0600); err != nil {
		t.Fatal(err)
	}
	if err = c.CommitNativeFile(t.Context(), sc, own.Epoch, 4, deltaPath, int64(len(delta))); err != nil {
		t.Fatal(err)
	}
	keys, err = a.Replica.ListSegments(t.Context(), sc, own.Epoch)
	if err != nil || len(keys) != 2 {
		t.Fatalf("native delta keys=%v err=%v", keys, err)
	}
	if err = c.CommitNativeFile(t.Context(), sc, own.Epoch, 4, deltaPath, int64(len(delta))); err != nil {
		t.Fatalf("native delta retry: %v", err)
	}
}

type snapshotBodyHook struct {
	*bytes.Reader
	after func()
}

func (r *snapshotBodyHook) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if r.after != nil {
		fn := r.after
		r.after = nil
		fn()
	}
	return n, err
}
func TestSnapshotStreamRejectsInvalidAndLostOwner(t *testing.T) {
	for _, mode := range []string{"corrupt", "stale-epoch", "owner-lost-during-body", "short-body"} {
		t.Run(mode, func(t *testing.T) {
			s, om, hs := newControlForwardServer(t, t.TempDir(), "snapshot-node")
			om.Advertise = hs.URL
			sc := cell.Scope{Namespace: "stream", Class: "c", ID: mode}
			own, err := om.Claim(t.Context(), sc, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			raw := nativeTestFile(t, 1, 1, []byte("snapshot"))
			size := len(raw)
			epoch := own.Epoch
			if mode == "corrupt" {
				raw[len(raw)-1] ^= 1
			}
			if mode == "stale-epoch" {
				epoch++
			}
			if mode == "short-body" {
				raw = raw[:len(raw)-1]
			}
			var body io.Reader = bytes.NewReader(raw)
			if mode == "owner-lost-during-body" {
				body = &snapshotBodyHook{Reader: bytes.NewReader(raw), after: func() {
					if err := om.Release(context.Background(), sc, own.Epoch); err != nil {
						t.Fatal(err)
					}
				}}
			}
			r := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/v1/internal/commit_binary?scope=%s&epoch=%d&snapshot=1", sc.String(), epoch), body)
			r.ContentLength = int64(size)
			r.Header.Set("Authorization", "Bearer "+testRoleJWT(t, "internal"))
			w := httptest.NewRecorder()
			s.Handler().ServeHTTP(w, r)
			if w.Code < 400 {
				t.Fatalf("invalid snapshot acknowledged: %d %s", w.Code, w.Body.String())
			}
			keys, err := s.Replica.ListSegments(t.Context(), sc, own.Epoch)
			if err != nil || len(keys) != 0 {
				t.Fatalf("invalid snapshot published: %v %v", keys, err)
			}
		})
	}
}

func TestSnapshotStreamDOOwnerLostDuringBody(t *testing.T) {
	s, om, hs := newControlForwardServer(t, t.TempDir(), "snapshot-do-agent")
	om.Advertise = hs.URL
	sc := cell.Scope{Namespace: "stream", Class: "c", ID: "do-snapshot"}
	own, err := om.Claim(t.Context(), sc, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	doScope := cell.Scope{Namespace: "app", Class: DOClass, ID: "w~Probe~shard0"}
	shard, err := om.ClaimAs(t.Context(), doScope, "do-node", "http://do-node", cell.RoleCellAgent, time.Minute, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	raw := nativeTestFile(t, 1, 1, []byte("snapshot"))
	body := &snapshotBodyHook{Reader: bytes.NewReader(raw), after: func() {
		if err := om.ReleaseAs(t.Context(), doScope, shard.Node, shard.Epoch); err != nil {
			t.Fatal(err)
		}
	}}
	req := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/v1/internal/commit_binary?scope=%s&epoch=1&snapshot=1", sc.String()), body)
	req.ContentLength = int64(len(raw))
	req.Header.Set("Authorization", "Bearer "+testRoleJWT(t, "internal"))
	req.Header.Set("x-cellhive-capture-owner-epoch", fmt.Sprint(own.Epoch))
	req.Header.Set("x-cellhive-do-owner-scope", doScope.String())
	req.Header.Set("x-cellhive-do-owner-node", shard.Node)
	req.Header.Set("x-cellhive-do-owner-epoch", fmt.Sprint(shard.Epoch))
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code < 400 {
		t.Fatalf("lost DO owner acknowledged: %d %s", rr.Code, rr.Body.String())
	}
	keys, err := s.Replica.ListSegments(t.Context(), sc, 1)
	if err != nil || len(keys) != 0 {
		t.Fatalf("lost DO owner published: %v %v", keys, err)
	}
}
