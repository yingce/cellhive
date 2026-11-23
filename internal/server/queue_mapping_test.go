package server

import (
	"bytes"
	"cellhive/internal/cell"
	"cellhive/internal/cellcapture"
	"cellhive/internal/control"
	"cellhive/internal/queue"
	"cellhive/internal/replica"
	"context"
	"database/sql"
	"net/http"
	"os"
	"sync"
	"testing"
)

func TestQueueSchemaReadIsDurableAndReopenIsReadOnly(t *testing.T) {
	s, _ := newTestServer(t)
	rep := replica.New(s.Bucket)
	s.Capture = &cellcapture.Manager{Store: s.Store, Committer: replicaCommitter{rep}, AutoSnapshot: true, Owner: func(context.Context, cell.Scope) (bool, uint64, error) { return true, 1, nil }}
	defer s.Capture.Stop()
	s.Queue = queue.New(s.Store)
	s.configureQueueStore(s.Queue)
	sc := queue.Scope("acme", "queue_1")
	if n, err := s.Queue.Depth(t.Context(), sc.Namespace, sc.ID); err != nil || n != 0 {
		t.Fatalf("depth=%d err=%v", n, err)
	}
	c, err := s.Store.Cell(t.Context(), sc)
	if err != nil {
		t.Fatal(err)
	}
	local, err := c.TxID(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	durable, err := rep.DurableWatermarkChain(t.Context(), sc, 1)
	if err != nil || durable != local {
		t.Fatalf("schema left local=%d durable=%d err=%v", local, durable, err)
	}
	s.Queue = queue.New(s.Store)
	s.configureQueueStore(s.Queue)
	if _, err := s.Queue.Status(t.Context(), sc.Namespace, sc.ID); err != nil {
		t.Fatal(err)
	}
	after, _ := c.TxID(t.Context())
	if after != local {
		t.Fatalf("reopen changed %d to %d", local, after)
	}
	// Simulate an older schema whose source index has not yet been added.
	if err := s.CaptureWrite(t.Context(), sc, func() error {
		_, err := c.Tx(t.Context(), func(tx *sql.Tx) error {
			_, err := tx.Exec("DROP INDEX messages_source")
			return err
		})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	s.Queue = queue.New(s.Store)
	s.configureQueueStore(s.Queue)
	beforeUpgrade, _ := c.TxID(t.Context())
	var wg sync.WaitGroup
	for range 12 {
		wg.Go(func() {
			if _, err := s.Queue.Status(t.Context(), sc.Namespace, sc.ID); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	afterUpgrade, _ := c.TxID(t.Context())
	if afterUpgrade != beforeUpgrade+1 {
		t.Fatalf("concurrent schema upgrades: %d -> %d", beforeUpgrade, afterUpgrade)
	}
	recovered := restoreDB(t, rep, sc, 1)
	var indexCount int
	if err := recovered.QueryRow("SELECT count(*) FROM sqlite_schema WHERE name='messages_source'").Scan(&indexCount); err != nil || indexCount != 1 {
		t.Fatalf("schema upgrade not recoverable: count=%d err=%v", indexCount, err)
	}
}

func TestQueueAdminUsesRegisteredMainAndDLQScopes(t *testing.T) {
	s := newDomainServer(t)
	ctx := t.Context()
	if _, err := s.Control.CreateApp(ctx, "acme", "test"); err != nil {
		t.Fatal(err)
	}
	for name, id := range map[string]string{"jobs": "queue_1", "failed": "queue_2"} {
		if _, err := s.Control.CreateResource(ctx, "acme", "queue", name, queue.Scope("acme", id).String(), "test"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Control.Deploy(ctx, "acme", "worker", control.DeploySpec{BundleSHA: "sha", Consumers: []control.Consumer{{Queue: "jobs", DeadLetterQueue: "failed"}}}, "test"); err != nil {
		t.Fatal(err)
	}
	producer := scopeDo(t, s, http.MethodPost, "/v1/queue/send?ns=acme&queue=jobs", mint(t, s, "acme", "queue", "jobs"), []byte("producer"))
	if producer.Code != http.StatusOK {
		t.Fatalf("producer: %d %s", producer.Code, producer.Body.String())
	}
	claimSeededScope(t, s, queue.Scope("acme", "queue_2"))
	id, err := s.Queue.Send(ctx, "acme", "queue_2", []byte("dead letter"), "text", 0, "")
	if err != nil {
		t.Fatal(err)
	}
	h := s.AdminHandler()
	preview := mustAdmin(t, h, http.MethodGet, "/v1/queue/dead-letters?namespace=acme&queue=jobs", "")
	if !bytes.Contains(preview.Body.Bytes(), []byte(id)) {
		t.Fatalf("wrong DLQ preview: %s", preview.Body.String())
	}
	mustAdmin(t, h, http.MethodPost, "/v1/queue/dead-letters/replay?namespace=acme&queue=jobs", "")
	if depth, err := s.Queue.Depth(ctx, "acme", "queue_1"); err != nil || depth != 2 {
		t.Fatalf("main depth=%d err=%v", depth, err)
	}
	if depth, err := s.Queue.Depth(ctx, "acme", "queue_2"); err != nil || depth != 0 {
		t.Fatalf("DLQ depth=%d err=%v", depth, err)
	}
	for _, name := range []string{"jobs", "failed"} {
		p, _ := s.Store.Path(queue.Scope("acme", name))
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Fatalf("invented name-based queue %s: %v", name, err)
		}
	}
}
