package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/config"
	"cellhive/internal/control"
	"cellhive/internal/drain"
	"cellhive/internal/logbuf"
	"cellhive/internal/nodelog"
	"cellhive/internal/owner"
	"cellhive/internal/upload"
)

// TestNewLogBufferIsWired guards the ADR-136 fix: the bounded log buffer must be
// constructed from the configured bounds and passed to server.Deps.Logs,
// otherwise the ingest/query endpoints return 503 and `cellhive tail --worker`
// is dead in production. Removing the wiring must fail this test.
func TestNewLogBufferIsWired(t *testing.T) {
	b := newLogBuffer(config.Config{LogBufferEntries: 1000, LogBufferWorkers: 200})
	if b == nil {
		t.Fatal("newLogBuffer returned nil")
	}
	b.Add(logbuf.Entry{Namespace: "acme", Worker: "api", Level: "log", Message: "hello", AtMs: 1})
	got := b.Since("acme", "api", 0, 10)
	if len(got) != 1 || got[0].Message != "hello" {
		t.Fatalf("buffer round trip = %+v, want the added entry", got)
	}

	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatalf("read main.go: %v", err)
	}
	if !strings.Contains(string(src), "Logs: newLogBuffer(cfg)") {
		t.Fatal("server.Deps.Logs is not wired via newLogBuffer(cfg): log tail would be dead")
	}
}

// TestDerivedSecretKeyDecodes guards the control-plane envelope key: the
// secrets-root credential must decode to 32 bytes through control.ParseRootKey
// (regression: a RawURL-encoded key silently disabled control-plane secrets).
func TestDerivedSecretKeyDecodes(t *testing.T) {
	creds := config.DeriveCredentials("00112233445566778899aabbccddeeff")
	key, err := control.ParseRootKey(creds.SecretKey)
	if err != nil || len(key) != 32 {
		t.Fatalf("derived secret key does not decode to 32 bytes: len=%d err=%v", len(key), err)
	}
}

func TestOpenBucketRejectsUnsupportedAuthorityProvider(t *testing.T) {
	_, err := openBucket(t.Context(), config.Config{BucketURL: "ftp://bucket", BucketDir: t.TempDir()})
	if err == nil || !strings.Contains(err.Error(), "unsupported bucket scheme") {
		t.Fatalf("openBucket ftp error = %v", err)
	}
	for _, rawURL := range []string{"cos://bucket-appid", "oss://bucket"} {
		if _, err := openBucket(t.Context(), config.Config{BucketURL: rawURL}); err == nil {
			t.Fatalf("openBucket(%q) accepted missing endpoint", rawURL)
		}
	}
}

type handoffSink struct{}

func (handoffSink) AppendBatch(context.Context, cell.Scope, uint64, [][]byte) (string, string, error) {
	return "k", "e", nil
}

func TestPerformHandoffSealsNodeLogAndReleasesOwners(t *testing.T) {
	ctx := context.Background()
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	om := &owner.Manager{B: b, NodeID: "n1", Session: "s1", OwnerTTL: time.Minute}
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "handoff"}
	if _, err := om.Claim(ctx, sc, time.Now()); err != nil {
		t.Fatalf("claim: %v", err)
	}
	nl := nodelog.New(b, "n1")
	if _, err := nl.Open(ctx, "s1", 1, nil); err != nil {
		t.Fatalf("node-log: %v", err)
	}
	dm := drain.New(b, "n1", "s1", time.Minute, 10*time.Millisecond)
	up := upload.New(handoffSink{}, nil, 8, 1<<20, time.Millisecond)
	upCtx, upCancel := context.WithCancel(context.Background())
	up.Start(upCtx)
	defer func() {
		upCancel()
		up.Stop()
	}()

	performHandoff(slog.New(slog.NewTextHandler(io.Discard, nil)), config.Config{
		NodeID: "n1", SessionID: "s1", DrainTTL: time.Minute, DrainWait: 5 * time.Second,
	}, nl, om, dm, up)

	if got := len(om.OwnedScopes()); got != 0 {
		t.Fatalf("owned scopes after handoff = %d, want 0", got)
	}
	rec, _, err := nl.Get(ctx, "n1", "s1")
	if err != nil || rec.Status != nodelog.StatusSealed {
		t.Fatalf("node-log after handoff = %+v, %v; want sealed", rec, err)
	}
	if _, ok, err := dm.Holder(ctx); err != nil || ok {
		t.Fatalf("drain token after handoff = ok=%v err=%v, want absent", ok, err)
	}
}

type failingHandoffSink struct{}

func (failingHandoffSink) AppendBatch(context.Context, cell.Scope, uint64, [][]byte) (string, string, error) {
	return "", "", errors.New("bucket unavailable")
}

func TestPerformHandoffReleasesEvenWhenFoldsExhaustTheDrainBudget(t *testing.T) {
	ctx := context.Background()
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	om := &owner.Manager{B: b, NodeID: "n1", Session: "s1", OwnerTTL: time.Minute}
	for i := 0; i < 4; i++ {
		sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: fmt.Sprintf("slow-fold-%d", i)}
		if _, err := om.Claim(ctx, sc, time.Now()); err != nil {
			t.Fatalf("claim: %v", err)
		}
	}
	// A fold that ignores its budget models the worst case: every release pays
	// the full fold cost. Releases must still complete.
	om.BeforeRelease = func(context.Context, cell.Scope, uint64) error {
		time.Sleep(20 * time.Millisecond)
		return nil
	}
	nl := nodelog.New(b, "n1")
	if _, err := nl.Open(ctx, "s1", 1, nil); err != nil {
		t.Fatalf("node-log: %v", err)
	}
	dm := drain.New(b, "n1", "s1", time.Minute, 10*time.Millisecond)
	up := upload.New(handoffSink{}, nil, 8, 1<<20, time.Millisecond)
	upCtx, upCancel := context.WithCancel(context.Background())
	up.Start(upCtx)
	defer func() {
		upCancel()
		up.Stop()
	}()

	performHandoff(slog.New(slog.NewTextHandler(io.Discard, nil)), config.Config{
		NodeID: "n1", SessionID: "s1", DrainTTL: time.Minute, DrainWait: 3 * time.Second,
	}, nl, om, dm, up)

	if got := len(om.OwnedScopes()); got != 0 {
		t.Fatalf("owned scopes after handoff = %d, want 0", got)
	}
	if _, ok, err := dm.Holder(ctx); err != nil || ok {
		t.Fatalf("drain token after handoff = ok=%v err=%v, want absent", ok, err)
	}
}

func TestFoldBudgetForSkipsWhenTimeIsShort(t *testing.T) {
	tests := []struct {
		name    string
		remain  time.Duration
		wantOK  bool
		maxWant time.Duration
	}{
		{"plenty", time.Hour, true, preReleaseFoldLimit},
		{"just-over-reserve", preReleaseFoldReserve + preReleaseFoldMin + time.Second, true, preReleaseFoldMin + time.Second},
		{"below-min", preReleaseFoldReserve + preReleaseFoldMin - time.Second, false, 0},
		{"already-expired", -time.Second, false, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(tt.remain))
			defer cancel()
			got, ok := foldBudgetFor(ctx, preReleaseFoldLimit)
			if ok != tt.wantOK {
				t.Fatalf("ok=%v want %v (budget=%v)", ok, tt.wantOK, got)
			}
			if ok && got > tt.maxWant {
				t.Fatalf("budget=%v exceeds %v", got, tt.maxWant)
			}
			if ok && got <= 0 {
				t.Fatalf("non-positive budget %v with ok=true", got)
			}
		})
	}
	// No deadline: use the configured limit.
	if got, ok := foldBudgetFor(context.Background(), 7*time.Second); !ok || got != 7*time.Second {
		t.Fatalf("deadline-less budget=%v ok=%v", got, ok)
	}
}

func TestPerformHandoffKeepsOwnersWhenUploadsUnproven(t *testing.T) {
	ctx := context.Background()
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	om := &owner.Manager{B: b, NodeID: "n1", Session: "s1", OwnerTTL: time.Minute}
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "handoff-fail"}
	if _, err := om.Claim(ctx, sc, time.Now()); err != nil {
		t.Fatalf("claim: %v", err)
	}
	nl := nodelog.New(b, "n1")
	if _, err := nl.Open(ctx, "s1", 1, nil); err != nil {
		t.Fatalf("node-log: %v", err)
	}
	sp, err := upload.OpenSpool(t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	up := upload.New(failingHandoffSink{}, nil, 8, 1<<20, time.Millisecond)
	up.SetSpool(sp)
	upCtx, upCancel := context.WithCancel(context.Background())
	up.Start(upCtx)
	defer func() {
		upCancel()
		up.Stop()
	}()
	up.Enqueue(ctx, sc, 1, []byte("acked-but-unuploaded"))

	performHandoff(slog.New(slog.NewTextHandler(io.Discard, nil)), config.Config{
		NodeID: "n1", SessionID: "s1", DrainTTL: time.Minute, DrainWait: 5 * time.Second,
	}, nl, om, drain.New(b, "n1", "s1", time.Minute, 10*time.Millisecond), up)

	if got := len(om.OwnedScopes()); got != 1 {
		t.Fatalf("owned scopes after failed drain = %d, want owner record retained", got)
	}
	rec, _, err := nl.Get(ctx, "n1", "s1")
	if err != nil || rec.Status == nodelog.StatusSealed {
		t.Fatalf("node-log after failed drain = %+v, %v; must remain open for recovery", rec, err)
	}
}

func TestShedIdleCellFoldsWithoutHoldingTheClaimLock(t *testing.T) {
	ctx := t.Context()
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	om := &owner.Manager{B: b, NodeID: "n1", Session: "s1", OwnerTTL: time.Minute}
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "orders"}
	owned, err := om.Claim(ctx, sc, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	foldedUnderLock := false
	om.BeforeRelease = func(rctx context.Context, _ cell.Scope, _ uint64) error {
		if !skipPreReleaseFold(rctx) {
			foldedUnderLock = true
		}
		return nil
	}
	started := make(chan struct{})
	releaseFold := make(chan struct{})
	claimErr := make(chan error, 1)
	go func() {
		<-started
		claimCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		defer cancel()
		_, cerr := om.Claim(claimCtx, sc, time.Now())
		claimErr <- cerr
		close(releaseFold)
	}()
	err = shedIdleCell(ctx, om, sc, owned.Epoch, func() bool { return true }, func(context.Context) error {
		close(started)
		select {
		case <-releaseFold:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	if cerr := <-claimErr; cerr != nil {
		t.Fatalf("claim during fold: %v", cerr)
	}
	if err != nil {
		t.Fatal(err)
	}
	if foldedUnderLock {
		t.Fatal("release folded while holding the claim lock")
	}
	if _, _, err := om.Resolve(ctx, sc); !errors.Is(err, owner.ErrUnowned) {
		t.Fatalf("resolve = %v, want unowned", err)
	}
}

func TestShedIdleCellKeepsTheOwnerWhenTheCellOpensDuringFold(t *testing.T) {
	ctx := t.Context()
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	om := &owner.Manager{B: b, NodeID: "n1", Session: "s1", OwnerTTL: time.Minute}
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "orders"}
	owned, err := om.Claim(ctx, sc, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	released := false
	om.BeforeRelease = func(context.Context, cell.Scope, uint64) error {
		released = true
		return nil
	}
	idle := true
	err = shedIdleCell(ctx, om, sc, owned.Epoch, func() bool { return idle }, func(context.Context) error {
		idle = false
		return nil
	})
	if !errors.Is(err, errShedSkipped) {
		t.Fatalf("shed err=%v, want skipped", err)
	}
	if released {
		t.Fatal("opened cell was released")
	}
	cur, _, err := om.Resolve(ctx, sc)
	if err != nil {
		t.Fatal(err)
	}
	if cur.Epoch != owned.Epoch {
		t.Fatalf("epoch=%d, want %d", cur.Epoch, owned.Epoch)
	}
}

func TestShedIdleCellStillReleasesWhenFoldFails(t *testing.T) {
	ctx := t.Context()
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	om := &owner.Manager{B: b, NodeID: "n1", Session: "s1", OwnerTTL: time.Minute}
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "orders"}
	owned, err := om.Claim(ctx, sc, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	err = shedIdleCell(ctx, om, sc, owned.Epoch, func() bool { return true }, func(context.Context) error {
		return errors.New("temp disk full")
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := om.Resolve(ctx, sc); !errors.Is(err, owner.ErrUnowned) {
		t.Fatalf("resolve = %v, want unowned", err)
	}
}

func TestShedIdleCellDoesNotFoldAnOpenCell(t *testing.T) {
	ctx := t.Context()
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	om := &owner.Manager{B: b, NodeID: "n1", Session: "s1", OwnerTTL: time.Minute}
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "orders"}
	if _, err := om.Claim(ctx, sc, time.Now()); err != nil {
		t.Fatal(err)
	}
	folded := false
	err = shedIdleCell(ctx, om, sc, 1, func() bool { return false }, func(context.Context) error {
		folded = true
		return nil
	})
	if !errors.Is(err, errShedSkipped) {
		t.Fatalf("shed err=%v, want skipped", err)
	}
	if folded {
		t.Fatal("folded an open cell")
	}
}

func TestShedIdleCellStopsWhenTheContextEndsDuringFold(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	om := &owner.Manager{B: b, NodeID: "n1", Session: "s1", OwnerTTL: time.Minute}
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "orders"}
	owned, err := om.Claim(ctx, sc, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	released := false
	om.BeforeRelease = func(context.Context, cell.Scope, uint64) error {
		released = true
		return nil
	}
	started := make(chan struct{})
	go func() {
		<-started
		cancel()
	}()
	err = shedIdleCell(ctx, om, sc, owned.Epoch, func() bool { return true }, func(fctx context.Context) error {
		close(started)
		<-fctx.Done()
		return fctx.Err()
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("shed err=%v, want canceled", err)
	}
	if released {
		t.Fatal("released after cancel")
	}
	cur, _, err := om.Resolve(context.Background(), sc)
	if err != nil {
		t.Fatal(err)
	}
	if cur.Epoch != owned.Epoch {
		t.Fatalf("epoch=%d, want %d", cur.Epoch, owned.Epoch)
	}
}
