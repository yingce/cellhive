package invocation

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	"cellhive/internal/cellstore"
)

// A 100-message start must resolve all parent lineages in one query.
func TestInvocationStartBatchesParentLookup(t *testing.T) {
	cs, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer cs.Close()
	s := New(cs)
	now := time.Unix(10*24*3600, 0)
	s.now = func() time.Time { return now }

	const parents = 100
	ids := make([]string, parents)
	for i := 0; i < parents; i++ {
		logical := fmt.Sprintf("msg-%03d", i)
		ids[i] = fmt.Sprintf("parent-%03d", i)
		r := Record{
			ID: ids[i], LogicalID: ids[i], Namespace: "acme", Worker: "web", Kind: "queue",
			Attempt: 1, StartedMs: now.UnixMilli() + int64(i), Status: "success",
			Messages: []MessageAttempt{{ID: fmt.Sprintf("m-%03d", i), LogicalID: logical, Attempt: 1}},
		}
		if _, err := s.Start(t.Context(), r); err != nil {
			t.Fatalf("parent %d: %v", i, err)
		}
	}

	s.parentQueries.Store(0)
	child := Record{
		ID: "child", LogicalID: "child", Namespace: "acme", Worker: "web", Kind: "queue",
		Attempt: 1, StartedMs: now.UnixMilli() + 1000, Status: "running",
	}
	for i := 0; i < parents; i++ {
		child.Messages = append(child.Messages, MessageAttempt{
			ID: fmt.Sprintf("child-m-%03d", i), LogicalID: fmt.Sprintf("msg-%03d", i), Attempt: 1,
		})
	}
	got, err := s.Start(t.Context(), child)
	if err != nil {
		t.Fatalf("child start: %v", err)
	}
	if n := s.parentQueries.Load(); n != 1 {
		t.Fatalf("parent lookups = %d, want 1 grouped query", n)
	}
	if len(got.Parents) != parents {
		t.Fatalf("parents = %d, want %d", len(got.Parents), parents)
	}
	seen := map[string]bool{}
	for _, id := range got.Parents {
		seen[id] = true
	}
	for _, id := range ids {
		if !seen[id] {
			t.Fatalf("missing parent %s in %v", id, got.Parents)
		}
	}
}

func TestInvocationPruneIsTimeGatedAndBatched(t *testing.T) {
	cs, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer cs.Close()
	s := New(cs)
	now := time.Unix(10*24*3600, 0)
	s.now = func() time.Time { return now }
	c, err := s.open(t.Context(), "acme")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Tx(t.Context(), func(tx *sql.Tx) error {
		_, err := tx.ExecContext(t.Context(), `WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM n WHERE x<10005)
 INSERT INTO invocations(id,worker,kind,started_ms,record) SELECT 'seed-'||x,'web','queue',?,json_object('invocation_id','seed-'||x,'logical_id','seed-'||x,'namespace','acme','worker','web','kind','queue','attempt',1,'started_ms',?,'status','success','logs',json_array()) FROM n`, now.UnixMilli(), now.UnixMilli())
		return err
	}); err != nil {
		t.Fatal(err)
	}

	r := Record{ID: "newest", LogicalID: "newest", Namespace: "acme", Worker: "web", Kind: "queue", Attempt: 1, StartedMs: now.Add(time.Second).UnixMilli(), Status: "success"}
	if err := s.Put(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := c.DB.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM invocations`).Scan(&count); err != nil || count != 10000 {
		t.Fatalf("first prune count = %d, %v", count, err)
	}
	if s.pruneRuns.Load() != 1 {
		t.Fatalf("prune runs = %d, want 1", s.pruneRuns.Load())
	}

	// Add retired rows directly: the next write inside the gate interval must
	// not pay for another retention scan.
	if _, err := c.DB.ExecContext(t.Context(), `WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM n WHERE x<20)
 INSERT INTO invocations(id,worker,kind,started_ms,record) SELECT 'old-'||x,'web','queue',?,json_object('invocation_id','old-'||x,'logical_id','old-'||x,'namespace','acme','worker','web','kind','queue','attempt',1,'started_ms',?,'status','success','logs',json_array()) FROM n`, now.Add(-8*24*time.Hour).UnixMilli(), now.Add(-8*24*time.Hour).UnixMilli()); err != nil {
		t.Fatal(err)
	}
	r.ID, r.LogicalID, r.StartedMs = "second", "second", now.Add(2*time.Second).UnixMilli()
	if err := s.Put(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	if s.pruneSkips.Load() != 1 {
		t.Fatalf("prune skips = %d, want 1", s.pruneSkips.Load())
	}
	if err := c.DB.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM invocations`).Scan(&count); err != nil || count != 10021 {
		t.Fatalf("gated count = %d, %v; cleanup ran inside the gate", count, err)
	}

	now = now.Add(2 * time.Minute)
	r.ID, r.LogicalID, r.StartedMs = "third", "third", now.UnixMilli()
	if err := s.Put(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	if err := c.DB.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM invocations`).Scan(&count); err != nil || count != 10000 {
		t.Fatalf("post-gate count = %d, %v", count, err)
	}
}
