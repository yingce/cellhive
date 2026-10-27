package queue

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestHTTPResponseMustPreserveRetry(t *testing.T) {
	for _, mode := range []string{"valid", "truncated", "oversized", "malformed", "empty", "null", "missing_ok", "false_ok", "trailing"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			st := newInternalStore(t)
			id := send(t, st, "must retry", 0)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch mode {
				case "valid":
					fmt.Fprintf(w, `{"ok":true,"retry":[{"id":%q}]}`, id)
				case "truncated":
					w.Header().Set("Content-Length", "4096")
					fmt.Fprint(w, `{"ok":true,"result":null,`)
				case "oversized":
					fmt.Fprintf(w, `{"ok":true,"result":%q,"retry":[{"id":%q}]}`, strings.Repeat("x", (1<<20)+1), id)
				case "null":
					fmt.Fprint(w, `null`)
				case "missing_ok":
					fmt.Fprint(w, `{}`)
				case "false_ok":
					fmt.Fprint(w, `{"ok":false}`)
				case "trailing":
					fmt.Fprint(w, `{"ok":true}garbage`)
				case "malformed":
					fmt.Fprint(w, `{"ok":true,`)
				}
			}))
			defer srv.Close()
			r := &Runner{Store: st, Dispatch: NewHTTP(srv.URL, ""), Batch: 1, Queues: func(context.Context) ([]Ref, error) { return []Ref{{Namespace: "demo", Name: "q"}}, nil }}
			acked, err := r.Pass(ctx)
			if err != nil {
				t.Fatal(err)
			}
			depth, err := st.Depth(ctx, "demo", "q")
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("acked=%d remaining=%d", acked, depth)
			if depth != 1 {
				t.Fatalf("message lost: expected retained retry, remaining=%d", depth)
			}
		})
	}
}

type reviewBlockingDispatch struct {
	entered chan string
	release chan struct{}
}

func (d *reviewBlockingDispatch) Dispatch(ctx context.Context, ref Ref, _ []Message) error {
	d.entered <- ref.Name
	if ref.Name == "slow" {
		<-d.release
	}
	return nil
}
func TestRunnerQueueIsolation(t *testing.T) {
	ctx := context.Background()
	st := newInternalStore(t)
	for _, q := range []string{"slow", "fast"} {
		if _, err := st.Send(ctx, "demo", q, []byte("x"), "text/plain", 0, ""); err != nil {
			t.Fatal(err)
		}
	}
	d := &reviewBlockingDispatch{make(chan string, 2), make(chan struct{})}
	r := &Runner{Store: st, Dispatch: d, Queues: func(context.Context) ([]Ref, error) {
		return []Ref{{Namespace: "demo", Name: "slow"}, {Namespace: "demo", Name: "fast"}}, nil
	}}
	done := make(chan struct{})
	go func() { defer close(done); r.Pass(ctx) }()
	first := <-d.entered
	t.Logf("first dispatch=%s", first)
	select {
	case got := <-d.entered:
		t.Logf("independent dispatch=%s", got)
	case <-time.After(2 * time.Second):
		t.Error("fast queue blocked behind unrelated slow queue for 2s")
	}
	close(d.release)
	<-done
}

func TestRunnerQueueConcurrencyBoundAndCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	st := newInternalStore(t)
	refs := make([]Ref, 20)
	for i := range refs {
		refs[i] = Ref{Namespace: "demo", Name: fmt.Sprint(i)}
	}
	entered := make(chan string, 20)
	release := make(chan struct{})
	r := &Runner{Store: st, Dispatch: &detailedDispatcher{}, Queues: func(context.Context) ([]Ref, error) { return append(refs, refs...), nil }, Filter: func(_ context.Context, ref Ref) (bool, error) { entered <- ref.Name; <-release; return false, nil }}
	done := make(chan struct{})
	go func() { defer close(done); r.Pass(ctx) }()
	for i := 0; i < 8; i++ {
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			cancel()
			close(release)
			<-done
			t.Fatal("workers did not start")
		}
	}
	select {
	case <-entered:
		t.Error("exceeded 8 concurrent queues")
	case <-time.After(50 * time.Millisecond):
	}
	cancel()
	close(release)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("cancellation did not finish")
	}
	if len(entered) != 0 {
		t.Fatal("started more queues after cancellation")
	}
}

func TestHTTPExplicitSuccessAcknowledges(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusNoContent} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			ctx := context.Background()
			st := newInternalStore(t)
			send(t, st, "ack", 0)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(status)
				if status == http.StatusOK {
					fmt.Fprint(w, `{"ok":true,"ack":[],"retry":[]}`)
				}
			}))
			defer srv.Close()
			runner := &Runner{Store: st, Dispatch: NewHTTP(srv.URL, ""), Queues: func(context.Context) ([]Ref, error) { return []Ref{{Namespace: "demo", Name: "q"}}, nil }}
			n, err := runner.Pass(ctx)
			if err != nil || n != 1 {
				t.Fatalf("ack=%d err=%v", n, err)
			}
			depth, err := st.Depth(ctx, "demo", "q")
			if err != nil || depth != 0 {
				t.Fatalf("depth=%d err=%v", depth, err)
			}
		})
	}
}
