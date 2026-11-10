package sqlcapture

import (
	"context"
	"errors"
	"testing"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/cellstore"
)

type shutdownFuture struct {
	ctx    context.Context
	result chan error
}
type shutdownCommitter struct{ started chan shutdownFuture }

func (c *shutdownCommitter) Commit(context.Context, cell.Scope, uint64, []byte) error {
	return errors.New("unexpected synchronous commit")
}
func (c *shutdownCommitter) CommitAsync(ctx context.Context, _ cell.Scope, _ uint64, _ []byte, _ uint64) <-chan error {
	result := make(chan error, 1)
	c.started <- shutdownFuture{ctx: ctx, result: result}
	return result
}
func TestCaptureShutdownJoinsAllAsyncCommits(t *testing.T) {
	for _, mode := range []string{"cancel", "first-future-fails"} {
		t.Run(mode, func(t *testing.T) {
			store, err := cellstore.New(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			scope := cell.Scope{Namespace: "join", Class: "__kv__", ID: mode}
			db, err := store.Cell(t.Context(), scope)
			if err != nil {
				t.Fatal(err)
			}
			baseline, err := db.TxID(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			committer := &shutdownCommitter{started: make(chan shutdownFuture, 2)}
			capture, err := New(db, scope, 1, baseline, committer)
			if err != nil {
				t.Fatal(err)
			}
			capture.pipeline = 4
			capture.GroupCommitWait = time.Millisecond
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			capture.Start(ctx)
			futures := make([]shutdownFuture, 0, 2)
			defer func() {
				cancel()
				for _, f := range futures {
					select {
					case f.result <- context.Canceled:
					default:
					}
				}
				select {
				case <-capture.Done():
				case <-time.After(5 * time.Second):
					t.Error("capture cleanup did not finish")
				}
			}()
			for _, key := range []string{"first", "second"} {
				if err := db.Put(t.Context(), key, []byte("value"), nil); err != nil {
					t.Fatal(err)
				}
				capture.Notify()
				select {
				case f := <-committer.started:
					futures = append(futures, f)
				case <-time.After(5 * time.Second):
					t.Fatal("async publication was not dispatched")
				}
			}
			if mode == "cancel" {
				cancel()
			} else {
				futures[0].result <- errors.New("injected failure")
				capture.Notify()
			}
			// A failure must cancel sibling futures even with the parent still live.
			select {
			case <-futures[1].ctx.Done():
			case <-time.After(5 * time.Second):
				t.Fatal("outstanding publication was not cancelled")
			}
			select {
			case <-capture.Done():
				t.Fatal("Done closed before outstanding publications exited")
			default:
			}
			waitCtx, waitCancel := context.WithTimeout(t.Context(), time.Second)
			err = capture.Wait(waitCtx, baseline+2)
			waitCancel()
			if err == nil || errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("durability wait did not fail promptly: %v", err)
			}
			if mode == "cancel" {
				// Out-of-order completion is not sufficient to prove all work stopped.
				futures[1].result <- nil
				select {
				case <-capture.Done():
					t.Fatal("Done ignored the first outstanding future")
				default:
				}
				futures[0].result <- nil
			} else {
				// The failed head was already consumed. Shutdown must not await it twice.
				futures[1].result <- nil
			}
			select {
			case <-capture.Done():
			case <-time.After(5 * time.Second):
				t.Fatal("shutdown did not join resolved futures")
			}
			if got := capture.Stats().DurableTxID; got != baseline {
				t.Fatalf("shutdown advanced durability: got %d want %d", got, baseline)
			}
		})
	}
}
