package timer

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"
)

type recordingIndex struct {
	mu      sync.Mutex
	dues    []int64
	lastDue int64
	dels    int
}

func (r *recordingIndex) Put(_ context.Context, _ string, due int64, _, _ string) error {
	r.mu.Lock()
	r.dues = append(r.dues, due)
	r.lastDue = due
	r.mu.Unlock()
	return nil
}
func (r *recordingIndex) Delete(context.Context, string) error {
	r.mu.Lock()
	r.dels++
	r.lastDue = 0
	r.mu.Unlock()
	return nil
}

// blockingIndex pauses the first pre-commit publish of a specific due time so a
// concurrent earlier timer can commit first.
type blockingIndex struct {
	recordingIndex
	blockAt int64
	blocked chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *blockingIndex) Put(ctx context.Context, scope string, due int64, kind, token string) error {
	err := b.recordingIndex.Put(ctx, scope, due, kind, token)
	if due == b.blockAt {
		b.once.Do(func() {
			close(b.blocked)
			<-b.release
		})
	}
	return err
}

func TestConcurrentEarlierUpsertIsNotHiddenByPostCommitPublish(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)
	idx := &blockingIndex{blockAt: 2000, blocked: make(chan struct{}), release: make(chan struct{})}
	st.Index = idx

	slow := make(chan error, 1)
	go func() {
		slow <- st.Upsert(ctx, New(2000, KindCron, st.cell.Scope.String(), "slow"))
	}()
	<-idx.blocked // slow has pre-published 2000 but not committed

	// A genuinely earlier timer commits while the first transaction is still
	// pending. Its post-commit sync publishes 1000.
	if err := st.Upsert(ctx, New(1000, KindCron, st.cell.Scope.String(), "fast")); err != nil {
		t.Fatal(err)
	}
	close(idx.release)
	if err := <-slow; err != nil {
		t.Fatal(err)
	}

	idx.mu.Lock()
	last := idx.lastDue
	idx.mu.Unlock()
	if last != 1000 {
		t.Fatalf("wake index ends at %d, want 1000; the earlier committed timer was hidden", last)
	}
}

func TestFailedUpsertLeavesNoPhantomWake(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)
	idx := &recordingIndex{}
	st.Index = idx
	if _, err := st.cell.DB.ExecContext(ctx, `CREATE TRIGGER fail_timer BEFORE INSERT ON timers
		WHEN NEW.token='boom' BEGIN SELECT RAISE(ABORT,'boom'); END`); err != nil {
		t.Fatal(err)
	}
	tm := Timer{Token: "boom", DueAtMs: time.Now().Add(time.Minute).UnixMilli(), Kind: KindCron, Scope: st.cell.Scope.String(), Occurrence: "slot"}
	if err := st.Upsert(ctx, tm); err == nil {
		t.Fatal("trigger did not fail the transaction")
	}
	idx.mu.Lock()
	last, dels := idx.lastDue, idx.dels
	idx.mu.Unlock()
	if last != 0 || dels == 0 {
		t.Fatalf("failed upsert left wake index at %d (deletes=%d), want a fresh empty sync", last, dels)
	}
	if n, err := st.Count(ctx); err != nil || n != 0 {
		t.Fatalf("failed upsert committed %d timers, %v", n, err)
	}
}

func TestTimerSchemaIndexesAndQueryPlans(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)
	for _, name := range []string{"timers_kind_occurrence", "timers_due_token"} {
		var n int
		if err := st.cell.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name=?`, name).Scan(&n); err != nil || n != 1 {
			t.Fatalf("index %s count=%d err=%v", name, n, err)
		}
	}
	plan := func(query string, args ...any) string {
		rows, err := st.cell.DB.QueryContext(ctx, "EXPLAIN QUERY PLAN "+query, args...)
		if err != nil {
			t.Fatalf("explain: %v", err)
		}
		defer rows.Close()
		var out []string
		for rows.Next() {
			var id, parent, notused int
			var detail string
			if err := rows.Scan(&id, &parent, &notused, &detail); err != nil {
				t.Fatal(err)
			}
			out = append(out, detail)
		}
		return strings.Join(out, "\n")
	}
	if got := plan(`SELECT MIN(due_ms), COUNT(*) FROM timers WHERE kind=? AND occurrence=?`, string(KindKVExpire), string(KindKVExpire)); !strings.Contains(got, "timers_kind_occurrence") {
		t.Fatalf("kind/occurrence plan does not use its index:\n%s", got)
	}
	if got := plan(`SELECT t.token FROM timers t LEFT JOIN timer_retry r ON r.token=t.token WHERE t.due_ms <= ? ORDER BY t.due_ms ASC, t.token ASC`, 1); !strings.Contains(got, "timers_due") {
		t.Fatalf("due plan does not use a due index:\n%s", got)
	}
}
