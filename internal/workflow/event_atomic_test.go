package workflow

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
)

func TestEventConsumptionPersistsStepBeforeReply(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	if _, err := s.Create(ctx, "acme", "wf", "i", nil); err != nil {
		t.Fatal(err)
	}
	tok, _, _, ok, err := s.ClaimRun(ctx, "acme", "wf", "i", 60000)
	if err != nil || !ok {
		t.Fatal(err)
	}
	if err := s.AppendEvent(ctx, "acme", "wf", "i", []byte(`{"type":"ready","value":1}`)); err != nil {
		t.Fatal(err)
	}
	_, found, err := s.ConsumeEventForStep(ctx, "acme", "wf", "i", "wait-ready", "ready", tok)
	if err != nil || !found {
		t.Fatal(err)
	}
	// A lost HTTP response/process exit here means putStep never happens.
	if err := s.ReleaseRun(ctx, "acme", "wf", "i", tok); err != nil {
		t.Fatal(err)
	}
	_, _, _, ok, err = s.ClaimRun(ctx, "acme", "wf", "i", 60000)
	if err != nil || !ok {
		t.Fatal(err)
	}
	_, memo, err := s.GetStep(ctx, "acme", "wf", "i", "wait-ready")
	if err != nil {
		t.Fatal(err)
	}
	_, found, err = s.ConsumeEvent(ctx, "acme", "wf", "i", "ready")
	if err != nil {
		t.Fatal(err)
	}
	if !memo || found {
		t.Fatalf("want durable step and no unconsumed event: memo=%v found=%v", memo, found)
	}
}
func TestConcurrentEventConsume(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	if _, err := s.Create(ctx, "acme", "wf", "i", nil); err != nil {
		t.Fatal(err)
	}
	if err := s.AppendEvent(ctx, "acme", "wf", "i", []byte(`{"type":"ready"}`)); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	start := make(chan struct{})
	results := make(chan bool, 32)
	errs := make(chan error, 32)
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, found, err := s.ConsumeEventForStep(ctx, "acme", "wf", "i", fmt.Sprint("step-", i), "ready", "")
			results <- found
			errs <- err
		}(i)
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	n := 0
	for found := range results {
		if found {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("one event consumed %d times", n)
	}
}

// A stale worker must not change any artifact after a restart/claim, even if
// it passed an earlier HTTP fence. Each store mutation is independently fenced.
func TestRunMutationFence(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	if _, err := s.Create(ctx, "acme", "wf", "i", nil); err != nil {
		t.Fatal(err)
	}
	old, _, _, ok, err := s.ClaimRun(ctx, "acme", "wf", "i", 60000)
	if err != nil || !ok {
		t.Fatal(err)
	}
	if err := s.Restart(ctx, "acme", "wf", "i"); err != nil {
		t.Fatal(err)
	}
	current, _, _, ok, err := s.ClaimRun(ctx, "acme", "wf", "i", 60000)
	if err != nil || !ok {
		t.Fatal(err)
	}
	mutations := map[string]func() error{
		"step":          func() error { return s.PutStep(ctx, "acme", "wf", "i", "a", []byte(`1`), old) },
		"output":        func() error { return s.SetOutput(ctx, "acme", "wf", "i", []byte(`1`), old) },
		"error":         func() error { return s.SetStatus(ctx, "acme", "wf", "i", StatusErrored, "old", old) },
		"attempt":       func() error { return s.SetAttempt(ctx, "acme", "wf", "i", "a", 9, "old", old) },
		"clear_attempt": func() error { return s.ClearAttempt(ctx, "acme", "wf", "i", "a", old) },
		"clear_wait":    func() error { return s.ClearWait(ctx, "acme", "wf", "i", "a", old) },
		"consume":       func() error { _, _, err := s.ConsumeEventForStep(ctx, "acme", "wf", "i", "a", "", old); return err },
	}
	for name, fn := range mutations {
		t.Run(name, func(t *testing.T) {
			if err := fn(); !errors.Is(err, ErrStaleRun) {
				t.Fatalf("want stale run, got %v", err)
			}
		})
	}
	if _, err := s.RunStatus(ctx, "acme", "wf", "i", current); err != nil {
		t.Fatal(err)
	}
}

func TestEventStepReplayAndIsolation(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	if _, err := s.Create(ctx, "acme", "wf", "i", nil); err != nil {
		t.Fatal(err)
	}
	tok, _, _, ok, err := s.ClaimRun(ctx, "acme", "wf", "i", 60000)
	if err != nil || !ok {
		t.Fatal(err)
	}
	want := []byte(`{"type":"ready","payload":{"n":42}}`)
	if err := s.AppendEvent(ctx, "acme", "wf", "i", want); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		got, found, err := s.ConsumeEventForStep(ctx, "acme", "wf", "i", "same", "ready", tok)
		if err != nil || !found || !bytes.Equal(got, want) {
			t.Fatalf("replay %d: %s %v %v", i, got, found, err)
		}
	}
	_, found, err := s.ConsumeEventForStep(ctx, "acme", "wf", "i", "different", "ready", tok)
	if err != nil || found {
		t.Fatalf("event reused for different step: %v %v", found, err)
	}
}

func TestEventAndStepRollbackTogether(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	if _, err := s.Create(ctx, "acme", "wf", "i", nil); err != nil {
		t.Fatal(err)
	}
	if err := s.AppendEvent(ctx, "acme", "wf", "i", []byte(`{"type":"ready"}`)); err != nil {
		t.Fatal(err)
	}
	c, err := s.cell(ctx, "acme", "wf")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.DB.ExecContext(ctx, `CREATE TRIGGER reject_step BEFORE INSERT ON steps BEGIN SELECT RAISE(ABORT, 'injected step failure'); END`); err != nil {
		t.Fatal(err)
	}
	if _, found, err := s.ConsumeEventForStep(ctx, "acme", "wf", "i", "ready", "ready", ""); err == nil || found {
		t.Fatalf("want rollback: found=%v err=%v", found, err)
	}
	if _, err := c.DB.ExecContext(ctx, `DROP TRIGGER reject_step`); err != nil {
		t.Fatal(err)
	}
	if _, found, err := s.ConsumeEventForStep(ctx, "acme", "wf", "i", "ready", "ready", ""); err != nil || !found {
		t.Fatalf("event lost on rollback: found=%v err=%v", found, err)
	}
}

func TestWorkflowRecreatedCellMigrates(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	if _, err := s.Create(ctx, "acme", "wf", "before", nil); err != nil {
		t.Fatal(err)
	}
	if err := s.cs.DeleteNamespace(ctx, "acme"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Create(ctx, "acme", "wf", "after", nil); err != nil {
		t.Fatalf("recreated cell reused old schema cache: %v", err)
	}
}
