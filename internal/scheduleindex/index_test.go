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

func TestNotificationDuringClaimSurvivesAck(t *testing.T) {
	endpoint := os.Getenv("CELLHIVE_TEST_SCHEDULE_URL")
	if endpoint == "" {
		t.Skip("requires isolated Redis/Kvrocks")
	}
	i, err := New(endpoint, "repeat-"+strconv.FormatInt(time.Now().UnixNano(), 10), slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	defer i.Close()
	defer i.client.Del(context.Background(), i.keys...)
	h := Hint{"queue", "test/__queue__/q", 0}
	ctx := t.Context()
	now := time.Now()
	if err = i.publish(ctx, h); err != nil {
		t.Fatal(err)
	}
	first, err := i.claim(ctx, now, "a")
	if err != nil || first == "" {
		t.Fatal(first, err)
	}
	if err = i.publish(ctx, h); err != nil {
		t.Fatal(err)
	}
	if _, err = i.ack(ctx, first, "a"); err != nil {
		t.Fatal(err)
	}
	again, err := i.claim(ctx, now, "b")
	if err != nil || again != first {
		t.Fatal("notification lost", again, err)
	}
	if n, err := i.ack(ctx, again, "a"); err != nil || n != 0 {
		t.Fatal(n, err)
	}
	if _, err = i.ack(ctx, again, "b"); err != nil {
		t.Fatal(err)
	}
	if last, err := i.claim(ctx, now, "c"); err != nil || last != "" {
		t.Fatal(last, err)
	}
}

func TestStableHintKeepsNextDeadlineDuringClaim(t *testing.T) {
	endpoint := os.Getenv("CELLHIVE_TEST_SCHEDULE_URL")
	if endpoint == "" {
		t.Skip("isolated backend required")
	}
	i, err := New(endpoint, "next-"+strconv.FormatInt(time.Now().UnixNano(), 10), slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	defer i.Close()
	defer i.client.Del(context.Background(), i.keys...)
	now := time.Now()
	h := Hint{Kind: "timer", Scope: "test/clock/x", Due: now.UnixMilli()}
	ctx := t.Context()
	if err = i.Publish(ctx, h); err != nil {
		t.Fatal(err)
	}
	raw, err := i.claim(ctx, now, "a")
	if err != nil || raw == "" {
		t.Fatal(raw, err)
	}
	h.Due = now.Add(time.Minute).UnixMilli()
	if err = i.Publish(ctx, h); err != nil {
		t.Fatal(err)
	}
	h.Due = now.Add(2 * time.Minute).UnixMilli()
	if err = i.Publish(ctx, h); err != nil {
		t.Fatal(err)
	}
	if n, err := i.client.ZCard(ctx, i.keys[0]).Result(); err != nil || n != 1 {
		t.Fatal(n, err)
	}
	if _, err = i.ack(ctx, raw, "a"); err != nil {
		t.Fatal(err)
	}
	if raw, err = i.claim(ctx, now.Add(time.Second), "early"); err != nil || raw != "" {
		t.Fatal("future work was immediately redelivered", raw, err)
	}
	if raw, err = i.claim(ctx, now.Add(time.Minute), "b"); err != nil || raw == "" {
		t.Fatal("earlier concurrent deadline lost", raw, err)
	}
}

func TestFailedHintRetriesSoonAndOldTokenCannotReleaseNewClaim(t *testing.T) {
	endpoint := os.Getenv("CELLHIVE_TEST_SCHEDULE_URL")
	if endpoint == "" {
		t.Skip("isolated backend required")
	}
	i, err := New(endpoint, "retry-"+strconv.FormatInt(time.Now().UnixNano(), 10), slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	defer i.Close()
	defer i.client.Del(context.Background(), i.keys...)
	ctx := t.Context()
	now := time.Now()
	h := Hint{Kind: "timer", Scope: "test/clock/x", Due: now.UnixMilli()}
	if err := i.Publish(ctx, h); err != nil {
		t.Fatal(err)
	}
	raw, err := i.claim(ctx, now, "old")
	if err != nil || raw == "" {
		t.Fatal(raw, err)
	}
	if n, err := i.retry(ctx, raw, "old", now); err != nil || n != 1 {
		t.Fatal(n, err)
	}
	if r, err := i.claim(ctx, now, "early"); err != nil || r != "" {
		t.Fatal(r, err)
	}
	newer, err := i.claim(ctx, now.Add(time.Second), "new")
	if err != nil || newer == "" {
		t.Fatal(newer, err)
	}
	if n, err := i.retry(ctx, raw, "old", now); err != nil || n != 0 {
		t.Fatal("old token released newer claim", n, err)
	}
	if n, err := i.ack(ctx, newer, "new"); err != nil || n != 1 {
		t.Fatal(n, err)
	}
}
