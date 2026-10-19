package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/cellcapture"
	"cellhive/internal/cellstore"
	"cellhive/internal/config"
	"cellhive/internal/control"
	"cellhive/internal/replica"
	"cellhive/internal/restore"
)

// A failed durability proof makes the deploy response unknown. A retry with
// the same key must re-establish the capture baseline and return one version.
func TestDeployCaptureFailureReconcilesWithSameKey(t *testing.T) {
	cs, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer cs.Close()
	env, err := control.NewEnvelope(bytes.Repeat([]byte{0x9}, 32))
	if err != nil {
		t.Fatal(err)
	}
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	s := New(Deps{Cfg: config.Config{NodeID: "node-1", TokenPeer: "tok", TokenInternal: "tok", TokenDispatch: "tok", ScopeSecret: "tok", AdminToken: "admin-tok"},
		Bucket: b, Store: cs, Control: control.New(cs, env), Log: slog.New(slog.NewTextHandler(io.Discard, nil))})
	rep := replica.New(b)
	admin := s.AdminHandler()
	app := adminDo(t, admin, http.MethodPost, "/v1/control/app", "admin-tok", `{"namespace":"acme"}`)
	if app.Code != http.StatusOK {
		t.Fatalf("app = %d %s", app.Code, app.Body.String())
	}
	bundle := adminDo(t, admin, http.MethodPost, "/v1/control/bundle", "admin-tok", "export default {}")
	if bundle.Code != http.StatusOK {
		t.Fatalf("bundle = %d %s", bundle.Code, bundle.Body.String())
	}
	var uploaded struct {
		SHA string `json:"sha"`
	}
	if err := json.Unmarshal(bundle.Body.Bytes(), &uploaded); err != nil {
		t.Fatal(err)
	}
	var failOnce atomic.Bool
	failOnce.Store(true)
	s.Capture = &cellcapture.Manager{
		Store: s.Store, AutoSnapshot: true, Checkpoint: time.Hour,
		Owner: func(context.Context, cell.Scope) (bool, uint64, error) { return true, 1, nil },
		Committer: testCommitterFunc(func(ctx context.Context, scope cell.Scope, epoch uint64, segment []byte) error {
			if scope == control.Scope() && failOnce.Load() {
				releases, err := s.Control.Releases(ctx, "acme", "api")
				if err != nil {
					return err
				}
				if len(releases) > 0 && failOnce.CompareAndSwap(true, false) {
					return errors.New("injected durability failure after deploy commit")
				}
			}
			_, _, err := rep.Append(ctx, scope, epoch, segment)
			return err
		}),
	}
	defer s.Capture.Stop()
	request := `{"namespace":"acme","worker":"api","bundle_sha":"` + uploaded.SHA + `","idempotency_key":"capture-failed"}`
	first := adminDo(t, admin, http.MethodPost, "/v1/control/deploy", "admin-tok", request)
	if first.Code == http.StatusOK || !strings.Contains(first.Body.String(), "injected durability failure") {
		t.Fatalf("first deploy = %d %s", first.Code, first.Body.String())
	}
	retry := adminDo(t, admin, http.MethodPost, "/v1/control/deploy", "admin-tok", request)
	if retry.Code != http.StatusOK {
		t.Fatalf("retry deploy = %d %s", retry.Code, retry.Body.String())
	}
	var version control.Version
	if err := json.Unmarshal(retry.Body.Bytes(), &version); err != nil {
		t.Fatal(err)
	}
	if version.Number != 1 {
		t.Fatalf("retry version = %d", version.Number)
	}
	releases, err := s.Control.Releases(context.Background(), "acme", "api")
	if err != nil || len(releases) != 1 {
		t.Fatalf("releases = %+v, %v", releases, err)
	}
	events, err := s.Control.DeploymentEvents(context.Background(), "acme", "api", 10)
	if err != nil || len(events) != 1 {
		t.Fatalf("events = %+v, %v", events, err)
	}
	segments, err := rep.Restore(context.Background(), control.Scope(), 1)
	if err != nil || len(segments) == 0 {
		t.Fatalf("restore segments = %d, %v", len(segments), err)
	}
	raw := make([][]byte, 0, len(segments))
	for _, part := range segments {
		raw = append(raw, part.Raw)
	}
	dst := filepath.Join(t.TempDir(), "recovered.db")
	if _, err := restore.ApplyFile(dst, raw); err != nil {
		t.Fatalf("apply replica: %v", err)
	}
	db, err := cellstore.Open(dst)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var restored int
	if err := db.QueryRow(`SELECT count(*) FROM versions WHERE ns='acme' AND worker='api'`).Scan(&restored); err != nil || restored != 1 {
		t.Fatalf("bucket-restored versions = %d, %v", restored, err)
	}
}
