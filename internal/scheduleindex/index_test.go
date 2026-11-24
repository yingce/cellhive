package scheduleindex

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
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
	claim := func(at time.Time, token string) claimedHint {
		t.Helper()
		s, err := i.claim(t.Context(), at, token)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	if claim(start.Add(-time.Second), "early") != (claimedHint{}) {
		t.Fatal("early")
	}
	member := claim(start, "a")
	if member == (claimedHint{}) {
		t.Fatal("missing due hint")
	}
	if err = i.publish(t.Context(), h); err != nil {
		t.Fatal(err)
	}
	if claim(start.Add(time.Second), "overlap") != (claimedHint{}) {
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
	if err != nil || first == (claimedHint{}) {
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
	if last, err := i.claim(ctx, now, "c"); err != nil || last != (claimedHint{}) {
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
	if err != nil || raw == (claimedHint{}) {
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
	if raw, err = i.claim(ctx, now.Add(time.Second), "early"); err != nil || raw != (claimedHint{}) {
		t.Fatal("future work was immediately redelivered", raw, err)
	}
	if raw, err = i.claim(ctx, now.Add(time.Minute), "b"); err != nil || raw == (claimedHint{}) {
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
	if err != nil || raw == (claimedHint{}) {
		t.Fatal(raw, err)
	}
	if n, err := i.retry(ctx, raw, "old", now); err != nil || n != 1 {
		t.Fatal(n, err)
	}
	if r, err := i.claim(ctx, now, "early"); err != nil || r != (claimedHint{}) {
		t.Fatal(r, err)
	}
	newer, err := i.claim(ctx, now.Add(time.Second), "new")
	if err != nil || newer == (claimedHint{}) {
		t.Fatal(newer, err)
	}
	if n, err := i.retry(ctx, raw, "old", now); err != nil || n != 0 {
		t.Fatal("old token released newer claim", n, err)
	}
	if n, err := i.ack(ctx, newer, "new"); err != nil || n != 1 {
		t.Fatal(n, err)
	}
}

func optimizationIndex(t *testing.T) *Index {
	t.Helper()
	endpoint := os.Getenv("CELLHIVE_TEST_SCHEDULE_URL")
	if endpoint == "" {
		t.Skip("isolated Redis/Kvrocks required")
	}
	i, err := New(endpoint, "optimization-"+strconv.FormatInt(time.Now().UnixNano(), 10), slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { i.client.Del(context.Background(), i.keys...); i.Close() })
	return i
}

func TestRetryBackoffAndConcurrentPublication(t *testing.T) {
	i := optimizationIndex(t)
	ctx := t.Context()
	now := time.Now()
	h := Hint{"timer", "test/clock/x", 0}
	if err := i.Publish(ctx, h); err != nil {
		t.Fatal(err)
	}
	for j, seconds := range []int{1, 2, 4, 8, 16, 32, 60, 60} {
		token := strconv.Itoa(j)
		item, err := i.claim(ctx, now, token)
		if err != nil || item.member == "" {
			t.Fatal(item, err)
		}
		if _, err = i.retry(ctx, item, token, now); err != nil {
			t.Fatal(err)
		}
		due, err := i.client.ZScore(ctx, i.keys[0], hintKey(h)).Result()
		if err != nil || int64(due) != now.Add(time.Duration(seconds)*time.Second).UnixMilli() {
			t.Fatal(due, seconds, err)
		}
		now = now.Add(time.Duration(seconds) * time.Second)
	}
	item, err := i.claim(ctx, now, "old")
	if err != nil {
		t.Fatal(err)
	}
	h.Due = now.Add(200 * time.Millisecond).UnixMilli()
	if err = i.Publish(ctx, h); err != nil {
		t.Fatal(err)
	}
	if _, err = i.retry(ctx, item, "old", now); err != nil {
		t.Fatal(err)
	}
	due, err := i.client.ZScore(ctx, i.keys[0], hintKey(h)).Result()
	if err != nil || int64(due) != h.Due {
		t.Fatal("new notification delayed", due, err)
	}
	item, err = i.claim(ctx, now.Add(time.Second), "new")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = i.retry(ctx, item, "new", now); err != nil {
		t.Fatal(err)
	}
	due, err = i.client.ZScore(ctx, i.keys[0], hintKey(h)).Result()
	if err != nil || int64(due) != now.Add(time.Second).UnixMilli() {
		t.Fatal("backoff not reset", due, err)
	}
}

func TestConsumeDamagedHintsAndContinuesAfterHandlerFailure(t *testing.T) {
	i := optimizationIndex(t)
	ctx := t.Context()
	for _, id := range []string{"a", "b", "c", "d", "e"} {
		if err := i.Publish(ctx, Hint{"timer", "test/clock/" + id, 0}); err != nil {
			t.Fatal(err)
		}
	}
	key := func(id string) string { return hintKey(Hint{"timer", "test/clock/" + id, 0}) }
	if err := i.client.HSet(ctx, i.keys[4], key("a"), "{", key("c"), `{"Kind":"timer","Scope":"test/clock/other","Due":0}`).Err(); err != nil {
		t.Fatal(err)
	}
	if err := i.client.HDel(ctx, i.keys[4], key("b")).Err(); err != nil {
		t.Fatal(err)
	}
	if err := i.client.ZAdd(ctx, i.keys[0], redis.Z{Score: 0, Member: ""}).Err(); err != nil {
		t.Fatal(err)
	}
	seen := 0
	err := i.consume(ctx, func(_ context.Context, h Hint) error {
		seen++
		if h.Scope == "test/clock/d" {
			return errors.New("unavailable")
		}
		return nil
	})
	if err == nil || seen != 2 {
		t.Fatal("batch did not continue", seen, err)
	}
	members, err := i.client.ZRange(ctx, i.keys[0], 0, -1).Result()
	if err != nil || len(members) != 1 || members[0] != key("d") {
		t.Fatal(members, err)
	}
}

func TestDamagedClaimAckPreservesNewPublication(t *testing.T) {
	i := optimizationIndex(t)
	ctx := t.Context()
	h := Hint{"timer", "test/clock/x", 0}
	if err := i.Publish(ctx, h); err != nil {
		t.Fatal(err)
	}
	if err := i.client.HDel(ctx, i.keys[4], hintKey(h)).Err(); err != nil {
		t.Fatal(err)
	}
	item, err := i.claim(ctx, time.Now(), "old")
	if err != nil || item.member == "" || item.payload != "" {
		t.Fatal(item, err)
	}
	if err = i.Publish(ctx, h); err != nil {
		t.Fatal(err)
	}
	if n, err := i.ack(ctx, item, "old"); err != nil || n != 1 {
		t.Fatal(n, err)
	}
	next, err := i.claim(ctx, time.Now(), "new")
	if err != nil || next.member != hintKey(h) || next.payload == "" {
		t.Fatal(next, err)
	}
}

func TestConsumeSharesHandlerBudget(t *testing.T) {
	i := optimizationIndex(t)
	ctx := t.Context()
	for _, id := range []string{"a", "b"} {
		if err := i.Publish(ctx, Hint{"timer", "test/clock/" + id, 0}); err != nil {
			t.Fatal(err)
		}
	}
	seen := 0
	err := i.consume(ctx, func(work context.Context, _ Hint) error { seen++; <-work.Done(); return work.Err() })
	if !errors.Is(err, context.DeadlineExceeded) || seen != 1 {
		t.Fatal("handler budget not shared", seen, err)
	}
	if n, err := i.client.ZCard(ctx, i.keys[0]).Result(); err != nil || n != 2 {
		t.Fatal("failed/unattempted hints lost", n, err)
	}
}
