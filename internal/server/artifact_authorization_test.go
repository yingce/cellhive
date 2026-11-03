package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"cellhive/internal/artifacts"
	"cellhive/internal/cell"
	"cellhive/internal/cellcapture"
	"cellhive/internal/cellstore"
	"cellhive/internal/control"
	"cellhive/internal/replica"
	"cellhive/internal/restore"
)

func TestBundleNamespaceAuthorization(t *testing.T) {
	s := newControlServer(t)
	a := testNamespaceJWT(t, s, "uploader-a", "tenant-a")
	b := testNamespaceJWT(t, s, "uploader-b", "tenant-b")
	source := "export default {fetch(){return new Response('private')}}"
	upload := func(token, ns string) string {
		t.Helper()
		w := adminDo(t, s.AdminHandler(), http.MethodPost, "/v1/control/bundle?namespace="+ns, token, source)
		if w.Code != 200 {
			t.Fatalf("upload %s: %d %s", ns, w.Code, w.Body.String())
		}
		var out struct {
			SHA string `json:"sha"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil || out.SHA == "" {
			t.Fatalf("upload response: %s", w.Body.String())
		}
		return out.SHA
	}
	sha := upload(a, "tenant-a")
	body := `{"namespace":"tenant-b","worker":"web","bundle_sha":"` + sha + `"}`
	w := adminDo(t, s.AdminHandler(), http.MethodPost, "/v1/control/deploy", b, body)
	if w.Code != 403 {
		t.Fatalf("foreign bundle deploy: %d %s", w.Code, w.Body.String())
	}
	if got := upload(b, "tenant-b"); got != sha {
		t.Fatalf("dedup SHA changed: %s != %s", got, sha)
	}
	w = adminDo(t, s.AdminHandler(), http.MethodPost, "/v1/control/deploy", b, body)
	if w.Code != 200 {
		t.Fatalf("own bundle deploy: %d %s", w.Code, w.Body.String())
	}
}

func TestBundleGrantCaptureFailureAndRecovery(t *testing.T) {
	s, _, _ := newControlForwardServer(t, t.TempDir(), "capture-grant")
	s.Owner = nil
	token := testNamespaceJWT(t, s, "uploader", "tenant-a")
	source := "export default {}"
	sha := artifacts.Hash([]byte(source))
	rep := replica.New(s.Bucket)
	var fail atomic.Bool
	fail.Store(true)
	s.Capture = &cellcapture.Manager{
		Store: s.Store, AutoSnapshot: true, Checkpoint: time.Hour,
		Owner: func(context.Context, cell.Scope) (bool, uint64, error) { return true, 1, nil },
		Committer: testCommitterFunc(func(ctx context.Context, sc cell.Scope, epoch uint64, data []byte) error {
			ok, err := s.Control.HasBundleGrant(ctx, "tenant-a", sha)
			if err != nil {
				return err
			}
			if ok && fail.CompareAndSwap(true, false) {
				return errors.New("injected grant durability failure")
			}
			_, _, err = rep.Append(ctx, sc, epoch, data)
			return err
		}),
	}
	defer s.Capture.Stop()
	w := adminDo(t, s.AdminHandler(), http.MethodPost, "/v1/control/bundle?namespace=tenant-a", token, source)
	if w.Code == 200 || !strings.Contains(w.Body.String(), "injected grant durability failure") {
		t.Fatalf("failed proof upload: %d %s", w.Code, w.Body.String())
	}
	w = adminDo(t, s.AdminHandler(), http.MethodPost, "/v1/control/bundle?namespace=tenant-a", token, source)
	if w.Code != 200 {
		t.Fatalf("retry upload: %d %s", w.Code, w.Body.String())
	}
	segments, err := rep.Restore(t.Context(), control.Scope(), 1)
	if err != nil {
		t.Fatal(err)
	}
	raw := make([][]byte, len(segments))
	for i, seg := range segments {
		raw[i] = seg.Raw
	}
	path := filepath.Join(t.TempDir(), "restored.db")
	if _, err := restore.ApplyFile(path, raw); err != nil {
		t.Fatal(err)
	}
	db, err := cellstore.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM bundle_grants WHERE ns=? AND sha=?`, "tenant-a", sha).Scan(&count); err != nil || count != 1 {
		t.Fatalf("restored grant: %d %v", count, err)
	}
}

func TestBundleUploadRequiresExplicitNamespaceForWildcardIssuer(t *testing.T) {
	s := newControlServer(t)
	token := testNamespaceJWT(t, s, "all-uploader", "*")
	w := adminDo(t, s.AdminHandler(), http.MethodPost, "/v1/control/bundle", token, "export default {}")
	if w.Code != 403 {
		t.Fatalf("namespace-less issuer upload: %d %s", w.Code, w.Body.String())
	}
	admin := testRoleJWT(t, "admin")
	w = adminDo(t, s.AdminHandler(), http.MethodPost, "/v1/control/bundle", admin, "export default {}")
	if w.Code != 200 {
		t.Fatalf("bootstrap upload: %d %s", w.Code, w.Body.String())
	}
	sha := artifacts.Hash([]byte("export default {}"))
	body := `{"namespace":"legacy","worker":"web","bundle_sha":"` + sha + `"}`
	w = adminDo(t, s.AdminHandler(), http.MethodPost, "/v1/control/deploy", admin, body)
	if w.Code != 200 {
		t.Fatalf("bootstrap deploy: %d %s", w.Code, w.Body.String())
	}
}

func TestBundleUploadGrantsAtControlOwner(t *testing.T) {
	dir := t.TempDir()
	a, om, hs := newControlForwardServer(t, dir, "grant-owner")
	b, _, _ := newControlForwardServer(t, dir, "grant-follower")
	om.Advertise = hs.URL
	if _, err := om.Claim(t.Context(), control.Scope(), time.Now()); err != nil {
		t.Fatal(err)
	}
	token := testNamespaceJWT(t, a, "uploader", "tenant-a")
	source := "export default {}"
	w := adminDo(t, b.AdminHandler(), http.MethodPost, "/v1/control/bundle?namespace=tenant-a", token, source)
	if w.Code != 200 {
		t.Fatalf("forward upload: %d %s", w.Code, w.Body.String())
	}
	sha := artifacts.Hash([]byte(source))
	if ok, err := a.Control.HasBundleGrant(t.Context(), "tenant-a", sha); err != nil || !ok {
		t.Fatalf("owner grant: %v %v", ok, err)
	}
	if ok, err := b.Control.HasBundleGrant(t.Context(), "tenant-a", sha); err != nil || ok {
		t.Fatalf("follower granted locally: %v %v", ok, err)
	}
}
