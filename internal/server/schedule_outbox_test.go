package server

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/cellcapture"
	"cellhive/internal/cellstore"
	"cellhive/internal/config"
	"cellhive/internal/owner"
	"cellhive/internal/scheduleindex"
	"cellhive/internal/scheduleoutbox"
	"cellhive/internal/timer"
)

func TestScheduleOutboxPublishFailureAndStaleAck(t *testing.T) {
	ctx := t.Context()
	b, _ := bucket.NewFSBucket(t.TempDir())
	store, _ := cellstore.New(t.TempDir())
	defer store.Close()
	om := &owner.Manager{B: b, NodeID: "n", Session: "s", OwnerTTL: time.Hour}
	sc := cell.Scope{Namespace: "test", Class: "clock", ID: "one"}
	if _, err := om.Claim(ctx, sc, time.Now()); err != nil {
		t.Fatal(err)
	}
	c, err := store.Cell(ctx, sc)
	if err != nil {
		t.Fatal(err)
	}
	st, err := timer.NewStore(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	first := timer.New(time.Now().Add(time.Hour).UnixMilli(), timer.KindCron, sc.String(), "one")
	if err = st.Upsert(ctx, first); err != nil {
		t.Fatal(err)
	}
	s := New(Deps{Cfg: config.Config{NodeID: "n"}, Store: store, Owner: om, Bucket: b, Log: slog.Default()})
	if s.ScheduleOutboxEvictionReady(ctx, c) {
		t.Fatal("pending outbox allowed eviction")
	}
	failed := errors.New("redis unavailable")
	if err = s.FlushScheduleOutbox(ctx, sc, false, func(context.Context, scheduleindex.Hint) error { return failed }); !errors.Is(err, failed) {
		t.Fatal(err)
	}
	snap, _ := scheduleoutbox.Read(ctx, c.DB, scheduleoutbox.Timer)
	if !snap.Pending {
		t.Fatal("failed publication acknowledged")
	}
	second := timer.New(first.DueAtMs-1000, timer.KindCron, sc.String(), "two")
	if err = s.FlushScheduleOutbox(ctx, sc, false, func(_ context.Context, h scheduleindex.Hint) error {
		if h.Due != first.DueAtMs {
			t.Fatal(h)
		}
		return st.Upsert(ctx, second)
	}); err != nil {
		t.Fatal(err)
	}
	snap, _ = scheduleoutbox.Read(ctx, c.DB, scheduleoutbox.Timer)
	if !snap.Pending {
		t.Fatal("stale ACK cleared concurrent mutation")
	}
	calls := 0
	publish := func(_ context.Context, h scheduleindex.Hint) error {
		calls++
		if h.Due != second.DueAtMs {
			t.Fatal(h)
		}
		return nil
	}
	if err = s.FlushScheduleOutbox(ctx, sc, false, publish); err != nil {
		t.Fatal(err)
	}
	snap, _ = scheduleoutbox.Read(ctx, c.DB, scheduleoutbox.Timer)
	if snap.Pending {
		t.Fatal("successful revision not acknowledged")
	}
	if !s.ScheduleOutboxEvictionReady(ctx, c) {
		t.Fatal("clean outbox pinned eviction")
	}
	before, _ := c.TxID(ctx)
	if err = s.FlushScheduleOutbox(ctx, sc, false, publish); err != nil || calls != 1 {
		t.Fatal(calls, err)
	}
	after, _ := c.TxID(ctx)
	if after != before {
		t.Fatal("idle relay wrote cell")
	}
	if err = s.FlushScheduleOutbox(ctx, sc, true, publish); err != nil || calls != 2 {
		t.Fatal(calls, err)
	}
	after, _ = c.TxID(ctx)
	if after != before {
		t.Fatal("clean reconciliation wrote ACK")
	}
}

func TestScheduleOutboxRejectsOtherOwner(t *testing.T) {
	ctx := t.Context()
	b, _ := bucket.NewFSBucket(t.TempDir())
	store, _ := cellstore.New(t.TempDir())
	defer store.Close()
	om := &owner.Manager{B: b, NodeID: "n", Session: "s", OwnerTTL: time.Hour}
	other := &owner.Manager{B: b, NodeID: "other", Session: "s2", OwnerTTL: time.Hour}
	sc := cell.Scope{Namespace: "test", Class: "clock", ID: "two"}
	other.Claim(ctx, sc, time.Now())
	s := New(Deps{Store: store, Owner: om, Bucket: b})
	err := s.FlushScheduleOutbox(ctx, sc, true, func(context.Context, scheduleindex.Hint) error { t.Fatal("published as non-owner"); return nil })
	if !errors.Is(err, owner.ErrNotOwner) {
		t.Fatal(err)
	}
	if store.IsOpen(sc) {
		t.Fatal("hydrated non-owned cell")
	}
}

type outboxReadHook struct {
	bucket.Bucket
	before func() error
}

func (b *outboxReadHook) Get(ctx context.Context, key string) ([]byte, string, error) {
	if b.before != nil {
		f := b.before
		b.before = nil
		if err := f(); err != nil {
			return nil, "", err
		}
	}
	return b.Bucket.Get(ctx, key)
}
func TestScheduleOutboxReleasesProbePinBeforeGenerationAdmission(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	fs, _ := bucket.NewFSBucket(t.TempDir())
	store, _ := cellstore.New(t.TempDir())
	defer store.Close()
	b := &outboxReadHook{Bucket: fs}
	om := &owner.Manager{B: b, NodeID: "n", Session: "s", OwnerTTL: time.Hour}
	sc := cell.Scope{Namespace: "test", Class: "clock", ID: "generation"}
	om.Claim(ctx, sc, time.Now())
	c, _ := store.Cell(ctx, sc)
	timer.NewStore(ctx, c)
	// A generation transition may remove the old local activation immediately
	// after the advisory probe. It must not wait for the relay's initial pin.
	b.before = func() error { return store.Forget(ctx, sc) }
	s := New(Deps{Store: store, Owner: om, Bucket: b})
	if err := s.FlushScheduleOutbox(ctx, sc, false, func(context.Context, scheduleindex.Hint) error { return nil }); err != nil {
		t.Fatal(err)
	}
}

func TestScheduleOutboxWaitsForSourceDurability(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 150*time.Millisecond)
	defer cancel()
	b, _ := bucket.NewFSBucket(t.TempDir())
	store, _ := cellstore.New(t.TempDir())
	defer store.Close()
	om := &owner.Manager{B: b, NodeID: "n", Session: "s", OwnerTTL: time.Hour}
	sc := cell.Scope{Namespace: "test", Class: "clock", ID: "durability"}
	om.Claim(ctx, sc, time.Now())
	c, _ := store.Cell(ctx, sc)
	st, _ := timer.NewStore(ctx, c)
	st.Upsert(ctx, timer.New(100, timer.KindCron, sc.String(), "one"))
	capture := &cellcapture.Manager{Store: store, AutoSnapshot: true, Owner: func(context.Context, cell.Scope) (bool, uint64, error) { return true, 1, nil }, Committer: testCommitterFunc(func(context.Context, cell.Scope, uint64, []byte) error { return errors.New("proof unavailable") })}
	defer capture.Stop()
	s := New(Deps{Store: store, Owner: om, Bucket: b, Capture: capture})
	if err := s.FlushScheduleOutbox(ctx, sc, false, func(context.Context, scheduleindex.Hint) error {
		t.Fatal("published without durable proof")
		return nil
	}); err == nil {
		t.Fatal("missing proof accepted")
	}
	snap, err := scheduleoutbox.Read(t.Context(), c.DB, scheduleoutbox.Timer)
	if err != nil || !snap.Pending {
		t.Fatal(snap, err)
	}
}

func TestScheduleOutboxSweepDefersUntilPublished(t *testing.T) {
	ctx := t.Context()
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	store, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	// Make the single cell eligible without wall-clock sleeps or access to the
	// store's private LRU. SQL/schema operations exceed this idle threshold.
	store.IdleTTL = time.Nanosecond
	om := &owner.Manager{B: b, NodeID: "n", Session: "s", OwnerTTL: time.Hour}
	sc := cell.Scope{Namespace: "test", Class: "clock", ID: "eviction"}
	if _, err := om.Claim(ctx, sc, time.Now()); err != nil {
		t.Fatal(err)
	}
	c, err := store.Cell(ctx, sc)
	if err != nil {
		t.Fatal(err)
	}
	st, err := timer.NewStore(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	due := time.Now().Add(time.Hour).UnixMilli()
	if err := st.Upsert(ctx, timer.New(due, timer.KindCron, sc.String(), "one")); err != nil {
		t.Fatal(err)
	}
	s := New(Deps{Cfg: config.Config{NodeID: "n"}, Store: store, Owner: om, Bucket: b, Log: slog.Default()})
	store.EvictionReady = s.ScheduleOutboxEvictionReady
	if n := store.Sweep(ctx); n != 0 || !store.IsOpen(sc) {
		t.Fatalf("pending outbox evicted: count=%d open=%v", n, store.IsOpen(sc))
	}
	unavailable := errors.New("publisher unavailable")
	if err := s.FlushScheduleOutbox(ctx, sc, false, func(context.Context, scheduleindex.Hint) error {
		return unavailable
	}); !errors.Is(err, unavailable) {
		t.Fatal(err)
	}
	if n := store.Sweep(ctx); n != 0 || !store.IsOpen(sc) {
		t.Fatalf("failed publication allowed eviction: count=%d open=%v", n, store.IsOpen(sc))
	}
	published := 0
	if err := s.FlushScheduleOutbox(ctx, sc, false, func(_ context.Context, hint scheduleindex.Hint) error {
		published++
		if hint.Due != due {
			t.Fatalf("published due=%d, want %d", hint.Due, due)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if published != 1 {
		t.Fatalf("published %d hints, want 1", published)
	}
	if n := store.Sweep(ctx); n != 1 || store.IsOpen(sc) {
		t.Fatalf("successful publication did not allow eviction: count=%d open=%v", n, store.IsOpen(sc))
	}
	if err := s.FlushScheduleOutbox(ctx, sc, true, func(_ context.Context, h scheduleindex.Hint) error {
		if h.Due != due {
			t.Fatalf("evicted deadline=%d, want %d", h.Due, due)
		}
		published++
		return nil
	}); err != nil || published != 2 {
		t.Fatalf("evicted local outbox not reconciled: published=%d err=%v", published, err)
	}
}

func TestScheduleOutboxDoesNotHydrateOwnedNativeScope(t *testing.T) {
	ctx := t.Context()
	b, _ := bucket.NewFSBucket(t.TempDir())
	store, _ := cellstore.New(t.TempDir())
	defer store.Close()
	om := &owner.Manager{B: b, NodeID: "n", Session: "s", OwnerTTL: time.Hour}
	sc := cell.Scope{Namespace: "test", Class: "__do__", ID: "native-host"}
	if _, err := om.Claim(ctx, sc, time.Now()); err != nil {
		t.Fatal(err)
	}
	store.Hydrate = func(context.Context, cell.Scope, string) error {
		return errors.New("outbox discovery attempted remote hydration for a native/cold scope")
	}
	s := New(Deps{Cfg: config.Config{NodeID: "n"}, Store: store, Owner: om, Bucket: b, Log: slog.Default()})
	for _, reconcile := range []bool{true, false, true} {
		if err := s.FlushScheduleOutbox(ctx, sc, reconcile, func(context.Context, scheduleindex.Hint) error {
			t.Fatal("published outbox for scope with no local database")
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	if store.IsOpen(sc) {
		t.Fatal("opened a native scope in the agent store")
	}
	scopes, err := store.LocalScopes()
	if err != nil || len(scopes) != 0 {
		t.Fatal(scopes, err)
	}
}

func TestScheduleOutboxSlowCellDoesNotBlockReadyCell(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	store, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	om := &owner.Manager{B: b, NodeID: "n", Session: "s", OwnerTTL: time.Hour}
	for _, id := range []string{"a-slow", "z-ready"} {
		sc := cell.Scope{Namespace: "test", Class: "clock", ID: id}
		if _, err := om.Claim(ctx, sc, time.Now()); err != nil {
			t.Fatal(err)
		}
		c, err := store.Cell(ctx, sc)
		if err != nil {
			t.Fatal(err)
		}
		st, err := timer.NewStore(ctx, c)
		if err != nil {
			t.Fatal(err)
		}
		if err := st.Upsert(ctx, timer.New(time.Now().Add(time.Hour).UnixMilli(), timer.KindCron, sc.String(), "one")); err != nil {
			t.Fatal(err)
		}
	}
	s := New(Deps{Cfg: config.Config{NodeID: "n"}, Store: store, Owner: om, Bucket: b, Log: slog.Default()})
	slow := make(chan struct{}, 1)
	ready := make(chan struct{}, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.RunScheduleOutbox(ctx, func(c context.Context, h scheduleindex.Hint) error {
			if h.Scope == "test/clock/a-slow" {
				select {
				case slow <- struct{}{}:
				default:
				}
				<-c.Done()
				return c.Err()
			}
			select {
			case ready <- struct{}{}:
			default:
			}
			return nil
		})
	}()
	defer func() { cancel(); <-done }()
	select {
	case <-slow:
	case <-time.After(3 * time.Second):
		t.Fatal("slow publication never started")
	}
	select {
	case <-ready:
	case <-time.After(time.Second):
		t.Fatal("ready cell blocked behind slow publication")
	}
}

func TestScheduleOutboxSaturatedWorkersDoNotStarveReadyCell(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	store, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	om := &owner.Manager{B: b, NodeID: "n", Session: "s", OwnerTTL: time.Hour}
	for _, id := range []string{"a-slow", "b-slow", "c-slow", "d-slow", "z-ready"} {
		sc := cell.Scope{Namespace: "test", Class: "clock", ID: id}
		if _, err := om.Claim(ctx, sc, time.Now()); err != nil {
			t.Fatal(err)
		}
		c, err := store.Cell(ctx, sc)
		if err != nil {
			t.Fatal(err)
		}
		st, err := timer.NewStore(ctx, c)
		if err != nil {
			t.Fatal(err)
		}
		if err := st.Upsert(ctx, timer.New(time.Now().Add(time.Hour).UnixMilli(), timer.KindCron, sc.String(), "one")); err != nil {
			t.Fatal(err)
		}
	}
	s := New(Deps{Cfg: config.Config{NodeID: "n"}, Store: store, Owner: om, Bucket: b, Log: slog.Default()})
	slow := make(chan struct{}, 1)
	ready := make(chan struct{}, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.RunScheduleOutbox(ctx, func(c context.Context, h scheduleindex.Hint) error {
			if h.Scope != "test/clock/z-ready" {
				select {
				case slow <- struct{}{}:
				default:
				}
				<-c.Done()
				return c.Err()
			}
			select {
			case ready <- struct{}{}:
			default:
			}
			return nil
		})
	}()
	defer func() { cancel(); <-done }()
	select {
	case <-slow:
	case <-time.After(3 * time.Second):
		t.Fatal("slow publication never started")
	}
	select {
	case <-ready:
	case <-time.After(8 * time.Second):
		t.Fatal("ready cell blocked behind slow publication")
	}
}

func TestScheduleOutboxNewScopeNotificationAndRetry(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	store, err := cellstore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	om := &owner.Manager{B: b, NodeID: "n", Session: "s", OwnerTTL: time.Hour}
	s := New(Deps{Cfg: config.Config{NodeID: "n"}, Store: store, Owner: om, Bucket: b, Log: slog.Default()})
	seed := func(id string) cell.Scope {
		t.Helper()
		sc := cell.Scope{Namespace: "test", Class: "clock", ID: id}
		if _, err := om.Claim(ctx, sc, time.Now()); err != nil {
			t.Fatal(err)
		}
		c, err := store.Cell(ctx, sc)
		if err != nil {
			t.Fatal(err)
		}
		st, err := timer.NewStore(ctx, c)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.CaptureWrite(ctx, sc, func() error {
			return st.Upsert(ctx, timer.New(time.Now().Add(time.Hour).UnixMilli(), timer.KindCron, sc.String(), "one"))
		}); err != nil {
			t.Fatal(err)
		}
		return sc
	}
	seed("initial")
	published := make(chan string, 8)
	done := make(chan struct{})
	attempts := 0
	go func() {
		defer close(done)
		s.RunScheduleOutbox(ctx, func(_ context.Context, h scheduleindex.Hint) error {
			if h.Scope == "test/clock/new" {
				attempts++
				if attempts == 1 {
					return errors.New("temporary publication failure")
				}
			}
			published <- h.Scope
			return nil
		})
	}()
	defer func() { cancel(); <-done }()
	wait := func(want string) {
		t.Helper()
		deadline := time.NewTimer(5 * time.Second)
		defer deadline.Stop()
		for {
			select {
			case got := <-published:
				if got == want {
					return
				}
			case <-deadline.C:
				t.Fatalf("publication missing: %s", want)
			}
		}
	}
	wait("test/clock/initial") // The owned snapshot has already been taken.
	seed("new")
	wait("test/clock/new") // Must use notification and retain it after failure.
}

func TestScheduleOutboxCachedOwnedScopeDoesNotReopen(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	b, _ := bucket.NewFSBucket(t.TempDir())
	store, _ := cellstore.New(t.TempDir())
	defer store.Close()
	om := &owner.Manager{B: b, NodeID: "n", Session: "s", OwnerTTL: time.Hour}
	sc := cell.Scope{Namespace: "test", Class: "clock", ID: "idle"}
	if _, err := om.Claim(ctx, sc, time.Now()); err != nil {
		t.Fatal(err)
	}
	c, err := store.Cell(ctx, sc)
	if err != nil {
		t.Fatal(err)
	}
	st, err := timer.NewStore(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Upsert(ctx, timer.New(time.Now().Add(time.Hour).UnixMilli(), timer.KindCron, sc.String(), "one")); err != nil {
		t.Fatal(err)
	}
	s := New(Deps{Cfg: config.Config{NodeID: "n"}, Store: store, Owner: om, Bucket: b, Log: slog.Default()})
	published := make(chan struct{}, 8)
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.RunScheduleOutbox(ctx, func(context.Context, scheduleindex.Hint) error { published <- struct{}{}; return nil })
	}()
	defer func() { cancel(); <-done }()
	select {
	case <-published:
	case <-time.After(3 * time.Second):
		t.Fatal("missing initial publication")
	}
	// Wait for the captured ACK before simulating the normal eviction callback.
	deadline := time.Now().Add(time.Second)
	for {
		snap, err := scheduleoutbox.Read(ctx, c.DB, scheduleoutbox.Timer)
		if err != nil {
			t.Fatal(err)
		}
		if !snap.Pending {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("ack pending")
		}
		time.Sleep(time.Millisecond)
	}
	store.IdleTTL = time.Nanosecond
	if n := store.Sweep(ctx); n != 1 {
		t.Fatalf("evicted=%d", n)
	}
	om.Forget(sc, 0)
	time.Sleep(1500 * time.Millisecond)
	if store.IsOpen(sc) {
		t.Fatal("cached owned snapshot reopened an evicted cell")
	}
}
