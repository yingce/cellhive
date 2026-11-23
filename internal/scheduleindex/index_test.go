package scheduleindex

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"strconv"
	"testing"
	"time"
)

type wakeFixture struct{ err error }

func (f wakeFixture) Put(context.Context, string, int64, string, string) error { return f.err }
func (f wakeFixture) Delete(context.Context, string) error                     { return f.err }

func TestProjectionFollowsDurableIndex(t *testing.T) {
	i := &Index{pending: make(chan Hint, 1)}
	wrapper := TimerIndex{Durable: wakeFixture{errors.New("COS unavailable")}, Shared: i}
	if err := wrapper.Put(t.Context(), "acme/clock/id", 123, "", ""); err == nil || len(i.pending) > 0 {
		t.Fatal("projected failed durable write")
	}
	wrapper.Durable = wakeFixture{}
	for range 2 {
		if err := wrapper.Put(t.Context(), "acme/clock/id", 123, "", ""); err != nil {
			t.Fatal(err)
		}
	}
	if len(i.pending) != 1 || i.dropped.Load() != 1 {
		t.Fatal("buffer did not bound work")
	}
}
func TestCompatibleBackend(t *testing.T) {
	endpoint := os.Getenv("CELLHIVE_TEST_SCHEDULE_URL")
	if endpoint == "" {
		t.Skip("requires isolated Redis or Kvrocks")
	}
	start := time.Now()
	i, err := New(endpoint, "test-"+strconv.FormatInt(start.UnixNano(), 10), slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	defer i.Close()
	defer i.client.Del(t.Context(), i.keys...)
	h := Hint{"timer", "test/clock/id", start.UnixMilli()}
	if err = i.publish(t.Context(), h); err != nil {
		t.Fatal(err)
	}
	claim := func(at time.Time, token string) string {
		t.Helper()
		s, err := i.claim(t.Context(), at, token)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	if claim(start.Add(-time.Second), "early") != "" {
		t.Fatal("early")
	}
	member := claim(start, "a")
	if member == "" {
		t.Fatal("missing due hint")
	}
	if err = i.publish(t.Context(), h); err != nil {
		t.Fatal(err)
	}
	if claim(start.Add(time.Second), "overlap") != "" {
		t.Fatal("visibility lost")
	}
	if claim(start.Add(30*time.Second), "b") != member {
		t.Fatal("failed redelivery")
	}
	if n, err := i.ack(t.Context(), member, "a"); n != 0 || err != nil {
		t.Fatal("stale claim", n, err)
	}
	if n, err := i.ack(t.Context(), member, "b"); n != 1 || err != nil {
		t.Fatal("current claim", n, err)
	}
	// Exercise asynchronous publication and consumption through different clients.
	reader, err := New(endpoint, "test-"+strconv.FormatInt(start.UnixNano(), 10), slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if err = i.publish(t.Context(), h); err != nil {
		t.Fatal(err)
	}
	seen := 0
	if err := reader.consume(t.Context(), func(_ context.Context, got Hint) error {
		if got != h {
			t.Fatalf("wrong hint %+v", got)
		}
		seen++
		return nil
	}); err != nil || seen != 1 {
		t.Fatal("delivery", seen, err)
	}
}
