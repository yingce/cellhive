package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"cellhive/internal/control"
)

// breakControlRev makes the revision read fail while leaving control writes
// (which only UPDATE meta) usable, so a stale cached projection would let a
// mutator proceed with an old active version.
func breakControlRev(t *testing.T, s *Server) {
	t.Helper()
	c, err := s.Store.Cell(context.Background(), control.Scope())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.DB.ExecContext(context.Background(), `ALTER TABLE meta RENAME TO meta_hidden`); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DB.ExecContext(context.Background(), `CREATE TABLE meta(k TEXT PRIMARY KEY, v TEXT NOT NULL)`); err != nil {
		t.Fatal(err)
	}
}

func strictProjectionFixture(t *testing.T) *Server {
	t.Helper()
	s := newScopeServer(t)
	ctx := context.Background()
	if _, err := s.Control.CreateApp(ctx, "acme", "ops"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Control.Deploy(ctx, "acme", "api", control.DeploySpec{
		BundleSHA: "sha1",
		Bindings:  []control.Binding{{Type: "kv", Name: "OLD", ID: "old"}},
		Crons:     []string{"old-cron"},
	}, "ops"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.projectionCached(ctx); err != nil {
		t.Fatal(err)
	}
	// A newer version becomes active without changing meta.rev (the stale cache
	// still holds v1).
	c, err := s.Store.Cell(ctx, control.Scope())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.DB.ExecContext(ctx, `INSERT INTO versions(ns,worker,number,bundle_sha,bindings,crons,created_ms)
		VALUES('acme','api',2,'sha2','[{"type":"kv","name":"NEW","id":"new"}]','["new-cron"]',1)`); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DB.ExecContext(ctx, `UPDATE workers SET active=2 WHERE ns='acme' AND name='api'`); err != nil {
		t.Fatal(err)
	}
	s.projMu.Lock()
	s.projAt = time.Now()
	s.projMu.Unlock()
	breakControlRev(t, s)
	return s
}

func TestMutationProjectionHelpersFailClosedOnRevError(t *testing.T) {
	s := strictProjectionFixture(t)
	ctx := context.Background()
	if _, err := s.activeCloudflareVersionForWorker(ctx, "acme", "api"); err == nil {
		t.Fatal("worker inheritance helper used a stale projection")
	}
	req := httptest.NewRequest(http.MethodPut, "/client/v4/accounts/acme/workers/scripts/api/schedules", nil)
	req.SetPathValue("account_id", "acme")
	req.SetPathValue("script_name", "api")
	if _, err := s.activeCloudflareVersion(req); err == nil {
		t.Fatal("schedule helper used a stale projection")
	}
}

func TestSchedulesMutationFailsInsteadOfOverwritingWithStaleVersion(t *testing.T) {
	s := strictProjectionFixture(t)
	req := httptest.NewRequest(http.MethodPut, "/client/v4/accounts/acme/workers/scripts/api/schedules", strings.NewReader(`[{"cron":"new-cron"}]`))
	req.SetPathValue("account_id", "acme")
	req.SetPathValue("script_name", "api")
	rr := httptest.NewRecorder()
	s.handleCloudflareWorkerSchedules(rr, req)
	if rr.Code == http.StatusOK {
		t.Fatalf("schedule mutation succeeded from a stale projection: %s", rr.Body.String())
	}
	v, ok, err := s.Control.WorkerEnv(context.Background(), "acme", "api")
	if err != nil || !ok || v.Version != 2 {
		t.Fatalf("active version changed to %+v (ok=%v err=%v), want 2", v, ok, err)
	}
}
