package cellstore

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"cellhive/internal/cell"
)

// TestPutTxIf covers the atomic existence-conditioned put (ADR-182, the
// "onlyIf" semantics): create-if-absent commits exactly once across
// concurrent writers, present-only swaps on a live key, a failed condition
// leaves the key and the cell txid untouched, and — the point of checking
// existence per key — an UNRELATED key's write never affects the outcome.
func TestPutTxIf(t *testing.T) {
	ctx := context.Background()
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "cas"}
	c, err := s.Open(ctx, sc)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer c.Close()

	// absent: key does not exist -> commits.
	if _, err := c.PutTxIf(ctx, "k", []byte("v1"), nil, 0, CondAbsent); err != nil {
		t.Fatalf("create-if-absent: %v", err)
	}
	if v, _, gerr := c.Get(ctx, "k"); gerr != nil || string(v) != "v1" {
		t.Fatalf("get = %q, %v", v, gerr)
	}

	// absent on an existing key: fails WITHOUT advancing the txid.
	before, _ := c.TxID(ctx)
	if _, err := c.PutTxIf(ctx, "k", []byte("v2"), nil, 0, CondAbsent); !errors.Is(err, ErrConditionFailed) {
		t.Fatalf("absent on existing err = %v, want ErrConditionFailed", err)
	}
	if v, _, gerr := c.Get(ctx, "k"); gerr != nil || string(v) != "v1" {
		t.Fatalf("value changed after failed condition: %q, %v", v, gerr)
	}
	if cur, gerr := c.TxID(ctx); gerr != nil || cur != before {
		t.Fatalf("txid advanced on failed condition: %d (%v), want %d", cur, gerr, before)
	}

	// present on an existing key: commits (a value swap on a live key).
	if _, err := c.PutTxIf(ctx, "k", []byte("v2"), nil, 0, CondPresent); err != nil {
		t.Fatalf("present swap: %v", err)
	}
	if v, _, gerr := c.Get(ctx, "k"); gerr != nil || string(v) != "v2" {
		t.Fatalf("get after swap = %q, %v", v, gerr)
	}

	// THE critical property: an unrelated key's write does NOT affect a
	// conditional write on "k" (no spurious conflicts in a shared cell).
	if err := c.Put(ctx, "unrelated", []byte("noise"), nil); err != nil {
		t.Fatalf("unrelated put: %v", err)
	}
	if _, err := c.PutTxIf(ctx, "k", []byte("v3"), nil, 0, CondPresent); err != nil {
		t.Fatalf("condition affected by unrelated write: %v", err)
	}

	// present on a missing key fails.
	if _, err := c.PutTxIf(ctx, "gone", []byte("x"), nil, 0, CondPresent); !errors.Is(err, ErrConditionFailed) {
		t.Fatalf("present on missing err = %v, want ErrConditionFailed", err)
	}
	// absent on a missing key succeeds (create).
	if _, err := c.PutTxIf(ctx, "gone", []byte("x"), nil, 0, CondAbsent); err != nil {
		t.Fatalf("absent create: %v", err)
	}
}

// TestPutTxIfExpiryCountsAsAbsent covers the TTL interaction: an
// expired-but-present row counts as absent (reads already treat expiry as
// absence, so a lock guarded by a TTL is acquirable again after expiry).
func TestPutTxIfExpiryCountsAsAbsent(t *testing.T) {
	ctx := context.Background()
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "casexp"}
	c, err := s.Open(ctx, sc)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer c.Close()

	// Write a key already expired.
	expired := time.Now().UnixMilli() - 1000
	if _, err := c.PutTxOpts(ctx, "lock", []byte("holder"), nil, expired); err != nil {
		t.Fatalf("put expired: %v", err)
	}
	// absent must treat the expired row as absent (lock re-acquirable).
	if _, err := c.PutTxIf(ctx, "lock", []byte("new-holder"), nil, 0, CondAbsent); err != nil {
		t.Fatalf("absent on expired key err = %v, want success", err)
	}
	if v, _, gerr := c.Get(ctx, "lock"); gerr != nil || string(v) != "new-holder" {
		t.Fatalf("get = %q, %v; want new-holder", v, gerr)
	}
}

// TestPutTxIfConcurrent proves N concurrent create-if-absent writers produce
// exactly one winner: the distributed-lock invariant.
func TestPutTxIfConcurrent(t *testing.T) {
	ctx := context.Background()
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "casconc"}
	c, err := s.Open(ctx, sc)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer c.Close()

	const workers = 8
	var wg sync.WaitGroup
	wins := make([]int, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			if _, err := c.PutTxIf(ctx, "lock", []byte(strconv.Itoa(w)), nil, 0, CondAbsent); err == nil {
				wins[w]++
			} else if !errors.Is(err, ErrConditionFailed) {
				t.Errorf("cas: %v", err)
			}
		}(w)
	}
	wg.Wait()

	total := 0
	for _, w := range wins {
		total += w
	}
	if total != 1 {
		t.Fatalf("winners = %d, want exactly 1", total)
	}
	v, _, err := c.Get(ctx, "lock")
	if err != nil {
		t.Fatalf("lock get: %v", err)
	}
	winner, _ := strconv.Atoi(string(v))
	if wins[winner] != 1 {
		t.Fatalf("recorded winner %d does not match stored value", winner)
	}
}

// TestIncrTx covers atomic increments: create-at-delta, add to an existing
// decimal value, rejection of non-integer targets, metadata preservation,
// and concurrent incrementers all landing (no lost updates).
func TestIncrTx(t *testing.T) {
	ctx := context.Background()
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "incr"}
	c, err := s.Open(ctx, sc)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer c.Close()

	// Missing key: created at delta.
	n, txid, err := c.IncrTx(ctx, "hits", 5, 0, []byte("m"))
	if err != nil || n != 5 {
		t.Fatalf("incr create = %d, %v", n, err)
	}
	if v, m, gerr := c.Get(ctx, "hits"); gerr != nil || string(v) != "5" || string(m) != "m" {
		t.Fatalf("get = %q/%q, %v", v, m, gerr)
	}
	t1 := txid

	// Existing key: add, metadata preserved without re-supply.
	if n, txid, err = c.IncrTx(ctx, "hits", 2, 0, nil); err != nil || n != 7 {
		t.Fatalf("incr add = %d, %v", n, err)
	}
	if txid != t1+1 {
		t.Fatalf("incr txid = %d, want %d", txid, t1+1)
	}
	if _, m, gerr := c.Get(ctx, "hits"); gerr != nil || string(m) != "m" {
		t.Fatalf("meta lost: %q, %v", m, gerr)
	}

	// Negative delta.
	if n, _, err = c.IncrTx(ctx, "hits", -10, 0, nil); err != nil || n != -3 {
		t.Fatalf("incr negative = %d, %v", n, err)
	}

	// Non-integer target rejected; txid untouched.
	if err := c.Put(ctx, "text", []byte("abc"), nil); err != nil {
		t.Fatalf("put text: %v", err)
	}
	before, _ := c.TxID(ctx)
	if _, _, err = c.IncrTx(ctx, "text", 1, 0, nil); err == nil {
		t.Fatalf("incr on text: want error")
	}
	if after, _ := c.TxID(ctx); after != before {
		t.Fatalf("txid advanced on failed incr: %d -> %d", before, after)
	}

	// Concurrent incrementers: no lost updates.
	const workers, per = 8, 25
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < per; i++ {
				if _, _, ierr := c.IncrTx(ctx, "conc", 1, 0, nil); ierr != nil {
					t.Errorf("concurrent incr: %v", ierr)
					return
				}
			}
		}()
	}
	wg.Wait()
	v, _, err := c.Get(ctx, "conc")
	if err != nil {
		t.Fatalf("conc get: %v", err)
	}
	if got, _ := strconv.Atoi(string(v)); got != workers*per {
		t.Fatalf("conc = %d, want %d (lost update)", got, workers*per)
	}
}
