package sqlcapture

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/ltx"
	"cellhive/internal/replica"
)

type timestampCommitter struct {
	m    *replica.Manager
	last []byte
}

func (c *timestampCommitter) Commit(ctx context.Context, s cell.Scope, e uint64, raw []byte) error {
	c.last = append([]byte(nil), raw...)
	_, _, err := c.m.Append(ctx, s, e, raw)
	return err
}

type timestampFileCommitter struct{ *timestampCommitter }

func (c timestampFileCommitter) CommitSnapshotFile(ctx context.Context, s cell.Scope, e, tx uint64, path string, size int64) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	c.last = raw
	_, _, err = c.m.AppendSnapshotFile(ctx, s, e, path, size)
	return err
}
func TestSnapshotClockRetryAcrossCaptureInstances(t *testing.T) {
	for _, file := range []bool{false, true} {
		t.Run(map[bool]string{false: "bytes", true: "spool"}[file], func(t *testing.T) {
			ctx := t.Context()
			db, scope, _ := openStreamCell(t, ctx, "timestamp", 2)
			fs, err := bucket.NewFSBucket(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			target := &timestampCommitter{m: replica.New(fs)}
			var committer Committer = target
			if file {
				committer = timestampFileCommitter{target}
			}
			before := time.Now().UnixMilli()
			var original []byte
			for attempt := 0; attempt < 2; attempt++ {
				c, _ := newCapture(t, db, scope, committer)
				err := c.Snapshot(ctx)
				c.cursor.Close()
				c.backgroundCancel()
				if err != nil {
					t.Fatal(err)
				}
				h, err := ltx.ValidateNative(target.last)
				if err != nil || h.Timestamp < before || h.Timestamp > time.Now().UnixMilli() {
					t.Fatalf("header=%+v err=%v", h, err)
				}
				if attempt == 0 {
					original = append([]byte(nil), target.last...)
					time.Sleep(2 * time.Millisecond)
				} else if bytes.Equal(original, target.last) {
					t.Fatal("fixture clock did not change")
				}
			}
			restored, err := target.m.Restore(ctx, scope, 1)
			if err != nil {
				t.Fatal(err)
			}
			if len(restored) != 1 || !bytes.Equal(restored[0].Raw, target.last) {
				t.Fatal("snapshot retry did not publish latest complete image")
			}
		})
	}
}
