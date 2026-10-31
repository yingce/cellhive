package invocation

import (
	"cellhive/internal/cellstore"
	"database/sql"
	"testing"
	"time"
)

func TestInvocationResultAndLogsSurviveReopen(t *testing.T) {
	cs, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer cs.Close()
	s := New(cs)
	s.now = func() time.Time { return time.UnixMilli(200) }
	r := Record{ID: "attempt-1", LogicalID: "cron-slot", Namespace: "acme", Worker: "web", Kind: "cron", Attempt: 1, StartedMs: 100, Status: "running"}
	if err := s.Put(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	r.Status = "failed"
	r.FinishedMs = 110
	r.Error = "boom"
	r.Logs = []Log{{Level: "log", Message: "before error", AtMs: 101}}
	if err := s.Put(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	s = New(cs)
	items, err := s.List(t.Context(), Filter{Namespace: "acme", Worker: "web", ID: "attempt-1", FromMs: 99, ToMs: 120, Limit: 10})
	if err != nil || len(items) != 1 || items[0].Status != "failed" || items[0].Error != "boom" || len(items[0].Logs) != 1 || items[0].Logs[0].Message != "before error" {
		t.Fatalf("invocation lost: %+v %v", items, err)
	}
	items, err = s.List(t.Context(), Filter{Namespace: "other", ID: r.ID})
	if err != nil || len(items) != 0 {
		t.Fatalf("namespace leak: %+v %v", items, err)
	}
}

func TestInvocationRetryIdentityAndHistoryBounds(t *testing.T) {
	cs, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer cs.Close()
	s := New(cs)
	now := time.Unix(10*24*3600, 0)
	s.now = func() time.Time { return now }
	r := Record{ID: "one", LogicalID: "slot", Namespace: "acme", Worker: "web", Kind: "cron", Attempt: 1, StartedMs: now.UnixMilli(), Status: "running"}
	first, err := s.Start(t.Context(), r)
	if err != nil || first.Attempt != 1 {
		t.Fatalf("start=%+v %v", first, err)
	}
	r.ID = "two"
	r.StartedMs++
	second, err := s.Start(t.Context(), r)
	if err != nil || second.Attempt != 2 {
		t.Fatalf("retry=%+v %v", second, err)
	}
	repeated, err := s.Start(t.Context(), r)
	if err != nil || repeated.Attempt != 2 {
		t.Fatalf("start is not idempotent: %+v %v", repeated, err)
	}
	r.Worker = "wrong"
	if _, err := s.Start(t.Context(), r); err == nil {
		t.Fatal("existing invocation identity was rewritten")
	}
	r.Worker = "web"
	expired := r
	expired.ID = "expired"
	expired.StartedMs = now.Add(-8 * 24 * time.Hour).UnixMilli()
	expired.Status = "success"
	if err := s.Put(t.Context(), expired); err != nil {
		t.Fatal(err)
	}
	items, err := s.List(t.Context(), Filter{Namespace: "acme", ID: "expired"})
	if err != nil || len(items) != 0 {
		t.Fatalf("expired record retained: %+v %v", items, err)
	}
	c, err := s.open(t.Context(), "acme")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Tx(t.Context(), func(tx *sql.Tx) error {
		_, err := tx.ExecContext(t.Context(), `WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM n WHERE x<10005)
 INSERT INTO invocations(id,worker,kind,started_ms,record) SELECT 'seed-'||x,'web','queue',?,json_object('invocation_id','seed-'||x,'logical_id','seed-'||x,'namespace','acme','worker','web','kind','queue','attempt',1,'started_ms',?,'status','success','logs',json_array()) FROM n`, now.UnixMilli(), now.UnixMilli())
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(t.Context(), `WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM n WHERE x<10005)
 INSERT INTO invocation_messages(invocation_id,message_id,logical_id,attempt) SELECT 'seed-'||x,'m-'||x,'seed-'||x,1 FROM n`)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	r.ID = "newest"
	r.StartedMs = now.Add(time.Second).UnixMilli()
	r.Status = "success"
	if err := s.Put(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := c.DB.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM invocations`).Scan(&count); err != nil || count != 10000 {
		t.Fatalf("unbounded history: %d %v", count, err)
	}
	var orphans int
	if err := c.DB.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM invocation_messages WHERE invocation_id NOT IN (SELECT id FROM invocations)`).Scan(&orphans); err != nil || orphans != 0 {
		t.Fatalf("orphan invocation messages = %d, %v", orphans, err)
	}
}
