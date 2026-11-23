package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/config"
	"cellhive/internal/owner"
	"cellhive/internal/replica"
)

// Count actual restore-discovery requests while retaining the real FS bucket.
type emptyFoldBucket struct {
	bucket.Bucket
	lists     atomic.Int64
	afterList func(int64) error
}

func (b *emptyFoldBucket) List(ctx context.Context, prefix string) ([]string, error) {
	keys, err := b.Bucket.List(ctx, prefix)
	if strings.Contains(prefix, "/ltx/") {
		n := b.lists.Add(1)
		if b.afterList != nil {
			if hookErr := b.afterList(n); hookErr != nil {
				return nil, hookErr
			}
		}
	}
	return keys, err
}

func TestEpochFoldEmptySleepsUntilDurableUpload(t *testing.T) {
	fs, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	b := &emptyFoldBucket{Bucket: fs}
	rep := replica.New(b)
	sc := cell.Scope{Namespace: "test", Class: "__kv__", ID: "empty-fold"}
	om := &owner.Manager{B: b, NodeID: "node", Session: "session", OwnerTTL: time.Minute}
	first, err := om.Claim(t.Context(), sc, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := om.Release(t.Context(), sc, first.Epoch); err != nil {
		t.Fatal(err)
	}
	second, err := om.Claim(t.Context(), sc, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if second.Epoch <= 1 {
		t.Fatal("expected successor")
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		runEpochFoldLoop(ctx, slog.New(slog.NewTextHandler(io.Discard, nil)), rep, om, config.Config{NodeID: "node", CompactionTimeout: time.Second}, nil)
	}()
	defer func() { cancel(); <-done }()
	waitForFoldLists(t, b, 1)
	// Let the first attempt finish, then span multiple scheduling/retry ticks.
	time.Sleep(150 * time.Millisecond)
	initial := b.lists.Load()
	time.Sleep(3500 * time.Millisecond)
	if got := b.lists.Load(); got != initial {
		t.Fatalf("empty chain was rescanned: List calls %d -> %d", initial, got)
	}
	// A real durable L0 publication must wake it even after L1 has consumed
	// the pending tally. No uploader callback is involved in this direct path.
	raw := snapshotSegment(t, second.Epoch, 4096, 1, map[uint32][]byte{1: make([]byte, 4096)})
	if _, _, err := rep.Append(t.Context(), sc, second.Epoch, raw); err != nil {
		t.Fatal(err)
	}
	rep.CoverL0(sc, second.Epoch, 1)
	waitForFoldLists(t, b, initial+1)
	time.Sleep(150 * time.Millisecond)
	afterUpload := b.lists.Load()
	time.Sleep(2500 * time.Millisecond)
	if got := b.lists.Load(); got != afterUpload {
		t.Fatalf("self-contained epoch kept scanning: %d -> %d", afterUpload, got)
	}
	// A new ownership epoch must discard the old completion/sleep state.
	if err := om.Release(t.Context(), sc, second.Epoch); err != nil {
		t.Fatal(err)
	}
	if _, err := om.Claim(t.Context(), sc, time.Now()); err != nil {
		t.Fatal(err)
	}
	waitForFoldLists(t, b, afterUpload+1)
	t.Logf("empty wait: %d List calls, +0 during idle; upload resumed discovery (%d calls total)", initial, afterUpload)
}
func waitForFoldLists(t *testing.T, b *emptyFoldBucket, n int64) {
	t.Helper()
	deadline := time.Now().Add(4 * time.Second)
	for b.lists.Load() < n {
		if time.Now().After(deadline) {
			t.Fatalf("waiting for List calls >= %d, got %d", n, b.lists.Load())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// An upload after the empty List response was read but before it reaches the
// scheduler must not be lost; a discovery error must never put a task to sleep.
func TestEpochFoldEmptyDiscoveryRaces(t *testing.T) {
	for _, mode := range []string{"upload-during-discovery", "discovery-error"} {
		t.Run(mode, func(t *testing.T) {
			fs, err := bucket.NewFSBucket(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			b := &emptyFoldBucket{Bucket: fs}
			rep := replica.New(b)
			sc := cell.Scope{Namespace: "test", Class: "__kv__", ID: mode}
			om := &owner.Manager{B: b, NodeID: "node", Session: "session", OwnerTTL: time.Minute}
			first, err := om.Claim(t.Context(), sc, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			if err := om.Release(t.Context(), sc, first.Epoch); err != nil {
				t.Fatal(err)
			}
			second, err := om.Claim(t.Context(), sc, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			raw := snapshotSegment(t, second.Epoch, 4096, 1, map[uint32][]byte{1: make([]byte, 4096)})
			publication := make(chan error, 1)
			b.afterList = func(n int64) error {
				if mode == "discovery-error" && n == 1 {
					return errors.New("injected discovery failure")
				}
				if mode == "upload-during-discovery" && n == 2 {
					_, _, err := rep.Append(t.Context(), sc, second.Epoch, raw)
					publication <- err
					return err
				}
				return nil
			}
			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan struct{})
			go func() {
				defer close(done)
				runEpochFoldLoop(ctx, slog.New(slog.NewTextHandler(io.Discard, nil)), rep, om, config.Config{NodeID: "node", CompactionTimeout: time.Second}, nil)
			}()
			defer func() { cancel(); <-done }()
			waitForFoldLists(t, b, 3)
			if mode == "upload-during-discovery" {
				if err := <-publication; err != nil {
					t.Fatal(err)
				}
			}
			time.Sleep(150 * time.Millisecond)
			settled := b.lists.Load()
			time.Sleep(2500 * time.Millisecond)
			if got := b.lists.Load(); got != settled {
				t.Fatalf("discovery did not settle: %d -> %d", settled, got)
			}
		})
	}
}
