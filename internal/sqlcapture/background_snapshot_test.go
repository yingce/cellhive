package sqlcapture

import (
	"bytes"
	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
	"cellhive/internal/ltx"
	"cellhive/internal/restore"
	"context"
	"errors"
	"path/filepath"
	"sort"
	"testing"
	"time"
)

type blockedSnapshotCommitter struct {
	snapshotErr error
	recordingCommitter
	entered chan struct{}
	release chan struct{}
}

func (c *blockedSnapshotCommitter) Commit(ctx context.Context, s cell.Scope, e uint64, raw []byte) error {
	h, _, err := ltx.Decode(raw)
	if err != nil {
		return err
	}
	if h.Kind == ltx.KindSnapshot {
		select {
		case c.entered <- struct{}{}:
		default:
		}
		select {
		case <-c.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if h.Kind == ltx.KindSnapshot && c.snapshotErr != nil {
		return c.snapshotErr
	}
	return c.recordingCommitter.Commit(ctx, s, e, raw)
}
func TestBackgroundSnapshotUploadDoesNotBlockDurableWrites(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	db, scope, _ := openStreamCell(t, ctx, "background", 3)
	c := &blockedSnapshotCommitter{entered: make(chan struct{}, 1), release: make(chan struct{})}
	capture, baseline := newCapture(t, db, scope, c)
	capture.SetCheckpointer(db.SafeCheckpoint)
	capture.Start(ctx)
	defer func() { cancel(); <-capture.Done() }()
	done := make(chan error, 1)
	go func() { _, err := capture.RequestBaseline(ctx, nil); done <- err }()
	select {
	case <-c.entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	write := make(chan error, 1)
	go func() {
		tx, err := db.PutTx(ctx, "during", []byte("saved"), nil)
		if err == nil {
			capture.Notify()
			err = capture.Wait(ctx, tx)
		}
		write <- err
	}()
	select {
	case err := <-write:
		if err != nil {
			t.Error(err)
		}
		if capture.barrier.durable() <= baseline {
			t.Error("write not durable")
		}
	case <-time.After(300 * time.Millisecond):
		t.Error("snapshot upload blocks write durability")
	}
	close(c.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	segments := append([][]byte(nil), c.segments...)
	c.mu.Unlock()
	sort.SliceStable(segments, func(i, j int) bool {
		a, _, _ := ltx.Decode(segments[i])
		b, _, _ := ltx.Decode(segments[j])
		return a.EndTxID < b.EndTxID
	})
	dest := filepath.Join(t.TempDir(), "restored.db")
	if _, err := restore.ApplyFile(dest, segments); err != nil {
		t.Fatal(err)
	}
	restored, err := cellstore.OpenAt(ctx, dest)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	got, _, err := restored.Get(ctx, "during")
	if err != nil || !bytes.Equal(got, []byte("saved")) {
		t.Fatalf("restored concurrent write=%q err=%v", got, err)
	}
	var integrity string
	if err := restored.DB.QueryRowContext(ctx, "PRAGMA integrity_check").Scan(&integrity); err != nil || integrity != "ok" {
		t.Fatalf("integrity=%s err=%v", integrity, err)
	}
	leftovers, _ := filepath.Glob(filepath.Join(filepath.Dir(db.Path), ".cellhive-snapshot-*"))
	if len(leftovers) != 0 {
		t.Fatalf("leaked snapshots %v", leftovers)
	}

}

func TestBackgroundSnapshotFailureLeavesCaptureRunning(t *testing.T) {
	for _, failure := range []string{"cancel", "upload"} {
		t.Run(failure, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			db, scope, _ := openStreamCell(t, ctx, "cancel-background", 3)
			seed := &recordingCommitter{}
			initial, _ := newCapture(t, db, scope, seed)
			if err := initial.Snapshot(ctx); err != nil {
				t.Fatal(err)
			}

			c := &blockedSnapshotCommitter{entered: make(chan struct{}, 1), release: make(chan struct{})}
			if failure == "upload" {
				c.snapshotErr = errors.New("upload failed")
			}
			c.segments = append(c.segments, seed.segments...)
			capture, _ := newCapture(t, db, scope, c)
			capture.Start(ctx)
			defer func() { cancel(); <-capture.Done() }()
			snapctx, stop := context.WithCancel(ctx)
			defer stop()
			done := make(chan error, 1)
			go func() { _, err := capture.RequestBaseline(snapctx, nil); done <- err }()
			select {
			case <-c.entered:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if failure == "cancel" {
				stop()
			} else {
				close(c.release)
			}
			if err := <-done; err == nil {
				t.Fatal("cancelled snapshot succeeded")
			}
			tx, err := db.PutTx(ctx, "after-cancel", []byte("safe"), nil)
			if err != nil {
				t.Fatal(err)
			}
			capture.Notify()
			if err := capture.Wait(ctx, tx); err != nil {
				t.Fatal(err)
			}
			c.mu.Lock()
			segments := append([][]byte(nil), c.segments...)
			c.mu.Unlock()
			dest := filepath.Join(t.TempDir(), "restored.db")
			if _, err := restore.ApplyFile(dest, segments); err != nil {
				t.Fatal(err)
			}
			restored, err := cellstore.OpenAt(ctx, dest)
			if err != nil {
				t.Fatal(err)
			}
			defer restored.Close()
			got, _, err := restored.Get(ctx, "after-cancel")
			if err != nil || string(got) != "safe" {
				t.Fatalf("restore after cancelled snapshot=%q err=%v", got, err)
			}
			leftovers, _ := filepath.Glob(filepath.Join(filepath.Dir(db.Path), ".cellhive-snapshot-*"))
			if len(leftovers) != 0 {
				t.Fatalf("leaked snapshots %v", leftovers)
			}
		})
	}
}

func TestCaptureStopCancelsBackgroundSnapshot(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	db, scope, _ := openStreamCell(t, ctx, "stop-background", 3)
	c := &blockedSnapshotCommitter{entered: make(chan struct{}, 1), release: make(chan struct{})}
	capture, _ := newCapture(t, db, scope, c)
	capture.Start(ctx)
	snapctx, stop := context.WithCancel(context.Background())
	defer stop()
	result := make(chan error, 1)
	go func() { _, err := capture.RequestBaseline(snapctx, nil); result <- err }()
	select {
	case <-c.entered:
	case <-time.After(time.Second):
		t.Fatal("snapshot did not start")
	}
	cancel()
	select {
	case err := <-result:
		if err == nil {
			t.Error("snapshot survived capture shutdown")
		}
	case <-time.After(300 * time.Millisecond):
		t.Error("capture shutdown did not cancel snapshot")
		stop()
		<-result
	}
	<-capture.Done()
}

type blockedDeltaCommitter struct {
	recordingCommitter
	release chan struct{}
}

func (c *blockedDeltaCommitter) Commit(ctx context.Context, s cell.Scope, e uint64, raw []byte) error {
	h, _, err := ltx.Decode(raw)
	if err != nil {
		return err
	}
	if h.Kind == ltx.KindDelta {
		select {
		case <-c.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return c.recordingCommitter.Commit(ctx, s, e, raw)
}
func TestBackgroundSnapshotDoesNotAdvanceDeltaBarrier(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	db, scope, _ := openStreamCell(t, ctx, "barrier", 3)
	c := &blockedDeltaCommitter{release: make(chan struct{})}
	capture, base := newCapture(t, db, scope, c)
	capture.Start(ctx)
	defer func() { cancel(); <-capture.Done() }()
	tx, err := db.PutTx(ctx, "pending", []byte("value"), nil)
	if err != nil {
		t.Fatal(err)
	}
	capture.Notify()
	label, err := capture.RequestBaseline(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if label != tx {
		t.Fatalf("snapshot label=%d want %d", label, tx)
	}
	if got := capture.barrier.durable(); got != base {
		t.Fatalf("snapshot advanced delta barrier %d -> %d", base, got)
	}
	close(c.release)
	if err := capture.Wait(ctx, tx); err != nil {
		t.Fatal(err)
	}
}
