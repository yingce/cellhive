package sqlcapture

import (
	"bytes"
	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
	"cellhive/internal/ltx"
	"cellhive/internal/restore"
	"context"
	"errors"
	"fmt"
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

// Force a copy between two unpolled WAL commits, exactly the production
// snapshot-at-3771 / delta-3770..3772 shape. No timing sleeps are needed.
func TestDetachedBaselineSplitsPendingWALBatch(t *testing.T) {
	ctx := context.Background()
	db, scope, _ := openStreamCell(t, ctx, "boundary", 1)
	rec := &recordingCommitter{}
	c, _ := newCapture(t, db, scope, rec)
	if _, err := db.PutTx(ctx, "before", []byte("one"), nil); err != nil {
		t.Fatal(err)
	}
	watermark, err := c.RequestBaseline(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	end, err := db.PutTx(ctx, "after", []byte("two"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.poll(ctx); err != nil {
		t.Fatal(err)
	}
	var tail [][]byte
	for _, raw := range rec.segments {
		h, _, _ := ltx.Decode(raw)
		if h.Kind == ltx.KindSnapshot || h.EndTxID > watermark {
			tail = append(tail, raw)
		}
		if h.Kind == ltx.KindDelta && h.StartTxID <= watermark && h.EndTxID > watermark {
			t.Fatalf("delta %d..%d crosses snapshot %d", h.StartTxID, h.EndTxID, watermark)
		}
	}
	dest := filepath.Join(t.TempDir(), "restored.db")
	if _, err := restore.ApplyFile(dest, tail); err != nil {
		t.Fatal(err)
	}
	got, err := cellstore.OpenAt(ctx, dest)
	if err != nil {
		t.Fatal(err)
	}
	defer got.Close()
	if tx, err := got.TxID(ctx); err != nil || tx != end {
		t.Fatalf("txid=%d want=%d err=%v", tx, end, err)
	}
	for k, want := range map[string]string{"before": "one", "after": "two"} {
		v, _, err := got.Get(ctx, k)
		if err != nil || string(v) != want {
			t.Fatalf("%s: %q %v", k, v, err)
		}
	}
}

func TestDetachedBaselinesDuringConcurrentWritesRemainIndependentlyRestorable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	db, sc, _ := openStreamCell(t, ctx, "many-boundaries", 1)
	rec := &recordingCommitter{}
	c, _ := newCapture(t, db, sc, rec)
	c.Start(ctx)
	defer func() { cancel(); <-c.Done() }()
	writes := make(chan error, 1)
	go func() {
		for i := 0; i < 100; i++ {
			tx, e := db.PutTx(ctx, fmt.Sprintf("saved-%03d", i), []byte(fmt.Sprintf("value-%03d", i)), nil)
			if e == nil {
				c.Notify()
				e = c.Wait(ctx, tx)
			}
			if e != nil {
				writes <- e
				return
			}
		}
		writes <- nil
	}()
	for i := 0; i < 10; i++ {
		if _, e := c.RequestBaseline(ctx, nil); e != nil {
			t.Fatal(e)
		}
	}
	if e := <-writes; e != nil {
		t.Fatal(e)
	}
	rec.mu.Lock()
	segments := append([][]byte(nil), rec.segments...)
	rec.mu.Unlock()
	checked := 0
	for _, baseline := range segments {
		h, _, _ := ltx.Decode(baseline)
		if h.Kind != ltx.KindSnapshot {
			continue
		}
		chain := [][]byte{baseline}
		for _, raw := range segments {
			d, _, _ := ltx.Decode(raw)
			if d.Kind == ltx.KindDelta && d.EndTxID > h.EndTxID {
				chain = append(chain, raw)
			}
		}
		path := filepath.Join(t.TempDir(), "image.db")
		if _, e := restore.ApplyFile(path, chain); e != nil {
			t.Fatalf("baseline %d: %v", h.EndTxID, e)
		}
		got, e := cellstore.OpenAt(ctx, path)
		if e != nil {
			t.Fatal(e)
		}
		for i := 0; i < 100; i++ {
			v, _, e := got.Get(ctx, fmt.Sprintf("saved-%03d", i))
			if e != nil || string(v) != fmt.Sprintf("value-%03d", i) {
				got.Close()
				t.Fatalf("baseline %d lost key %d: %v", h.EndTxID, i, e)
			}
		}
		got.Close()
		checked++
	}
	if checked != 10 {
		t.Fatalf("checked %d snapshots", checked)
	}
}
