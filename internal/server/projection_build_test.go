package server

import (
	"context"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"cellhive/internal/control"
)

// One projection build must serve an entire queue/workflow list, even with
// thousands of workers and resources.
func TestCloudflareListsBuildProjectionOnce(t *testing.T) {
	s := newScopeServer(t)
	ctx := context.Background()
	if _, err := s.Control.CreateApp(ctx, "acme", "ops"); err != nil {
		t.Fatal(err)
	}
	c, err := s.Store.Cell(ctx, control.Scope())
	if err != nil {
		t.Fatal(err)
	}
	const workers = 1000
	if _, err := c.Tx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `WITH RECURSIVE n(x) AS (SELECT 0 UNION ALL SELECT x+1 FROM n WHERE x<?)
 INSERT INTO workers(ns,name,active,storage_id,created_ms) SELECT 'acme','w'||x,1,'s'||x,1 FROM n`, workers-1); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `WITH RECURSIVE n(x) AS (SELECT 0 UNION ALL SELECT x+1 FROM n WHERE x<?)
 INSERT INTO versions(ns,worker,number,bundle_sha,bindings,consumers,created_ms)
 SELECT 'acme','w'||x,1,'sha'||x,
        json_array(json_object('type','workflow','name','wf'||x,'class_name','WF')),
        json_array(json_object('queue','q'||x,'max_batch_size',10)),1 FROM n`, workers-1); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `WITH RECURSIVE n(x) AS (SELECT 0 UNION ALL SELECT x+1 FROM n WHERE x<?)
 INSERT INTO resources(ns,kind,name,scope,created_ms) SELECT 'acme','workflow','wf'||x,'',1 FROM n`, workers-1); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `WITH RECURSIVE n(x) AS (SELECT 0 UNION ALL SELECT x+1 FROM n WHERE x<?)
 INSERT INTO resources(ns,kind,name,scope,created_ms) SELECT 'acme','queue','q'||x,'',1 FROM n`, workers-1)
		return err
	}); err != nil {
		t.Fatal(err)
	}

	// Prime the cache once; every list request below must reuse that build.
	if _, err := s.projectionCached(ctx); err != nil {
		t.Fatal(err)
	}
	built := s.Control.ProjectionBuilds()
	for i := 0; i < 2; i++ {
		// Each list request has its own cache lifetime. Keep the cache inside
		// its TTL even when the race detector slows the 1000-item walk.
		s.projMu.Lock()
		s.projAt = time.Now()
		s.projMu.Unlock()
		req := httptest.NewRequest(http.MethodGet, "/accounts/acme/workflows", nil)
		req.SetPathValue("account_id", "acme")
		rr := httptest.NewRecorder()
		s.handleCloudflareWorkflowList(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("workflow list %d = %d: %s", i, rr.Code, rr.Body.String())
		}
		s.projMu.Lock()
		s.projAt = time.Now()
		s.projMu.Unlock()
		req = httptest.NewRequest(http.MethodGet, "/accounts/acme/queues", nil)
		req.SetPathValue("account_id", "acme")
		rr = httptest.NewRecorder()
		s.handleCloudflareQueueList(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("queue list %d = %d: %s", i, rr.Code, rr.Body.String())
		}
	}
	if n := s.Control.ProjectionBuilds(); n != built {
		t.Fatalf("projection rebuilt %d times during list requests, want 0", n-built)
	}
}
