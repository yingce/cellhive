package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"cellhive/internal/cell"
	"cellhive/internal/cellcapture"
	"cellhive/internal/control"
)

// TestDeployUsesOneDurabilityProof pins ADR-271: a deploy waits for exactly one
// durability proof, no matter whether it also creates the app row (and even
// when DO migrations are present but write nothing). Before the change a deploy
// recorded three proofs (migrations, auto app create, version commit), each
// paying the control cell's proof latency.
func TestDeployUsesOneDurabilityProof(t *testing.T) {
	s, _ := newTestServer(t)
	env, err := control.NewEnvelope(bytes.Repeat([]byte{0x5}, 32))
	if err != nil {
		t.Fatal(err)
	}
	s.Control = control.New(s.Store, env)
	s.Cfg.AutoCreateApp = true
	s.Capture = &cellcapture.Manager{
		Store: s.Store, AutoSnapshot: true,
		Owner: func(context.Context, cell.Scope) (bool, uint64, error) { return true, 1, nil },
		Committer: testCommitterFunc(func(context.Context, cell.Scope, uint64, []byte) error {
			return nil
		}),
	}
	defer s.Capture.Stop()

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
	proofs := func() int64 { return s.proofFor(control.GlobalNamespace).count.Load() }
	deploy := func(ns, sha, key string) int64 {
		body := `{"namespace":"` + ns + `","worker":"api","bundle_sha":"` + sha + `","idempotency_key":"` + key + `"}`
		before := proofs()
		rr := adminDo(t, admin, http.MethodPost, "/v1/control/deploy", "admin-tok", body)
		if rr.Code != http.StatusOK {
			t.Fatalf("deploy = %d %s", rr.Code, rr.Body.String())
		}
		return proofs() - before
	}

	// Fresh namespace: the deploy creates the app row and the version in one
	// capture section, so there is still a single proof.
	sha1 := upload("export default {v:1}")
	if got := deploy("fresh", sha1, "fresh-k1"); got != 1 {
		t.Fatalf("fresh deploy proofs = %d, want 1", got)
	}
	if _, err := s.Control.GetApp(context.Background(), "fresh"); err != nil {
		t.Fatalf("app auto-create: %v", err)
	}

	// Existing app: CreateApp is a read-only no-op, so the deploy writes only the
	// version and still waits once.
	sha2 := upload("export default {v:2}")
	if got := deploy("fresh", sha2, "fresh-k2"); got != 1 {
		t.Fatalf("second deploy proofs = %d, want 1", got)
	}

	// App auto-create is not re-audited on every deploy.
	c, err := s.Store.Cell(context.Background(), control.Scope())
	if err != nil {
		t.Fatal(err)
	}
	var audits int
	if err := c.DB.QueryRowContext(context.Background(),
		`SELECT count(*) FROM audit WHERE action='app.create' AND ns='fresh'`).Scan(&audits); err != nil {
		t.Fatalf("audit count: %v", err)
	}
	if audits != 1 {
		t.Fatalf("app.create audits = %d, want 1", audits)
	}
}
