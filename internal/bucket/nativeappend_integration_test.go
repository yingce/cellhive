package bucket

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

// Run with CELLHIVE_NATIVE_TEST_PROVIDER=oss|cos and provider credentials.
// Keys live only under itest/ and are not deleted if the backend cannot fence.
func TestNativeAppendCloud(t *testing.T) {
	provider := os.Getenv("CELLHIVE_NATIVE_TEST_PROVIDER")
	if provider == "" {
		t.Skip("set CELLHIVE_NATIVE_TEST_PROVIDER")
	}
	var raw PositionAppender
	var err error
	switch provider {
	case "oss":
		raw, err = NewOSSAppender(os.Getenv("OSS_ENDPOINT"), os.Getenv("OSS_BUCKET"), os.Getenv("OSS_ACCESS_KEY_ID"), os.Getenv("OSS_ACCESS_KEY_SECRET"))
	case "cos":
		raw, err = NewCOSAppender(os.Getenv("COS_ENDPOINT"), os.Getenv("COS_SECRET_ID"), os.Getenv("COS_SECRET_KEY"))
	default:
		t.Fatalf("unknown provider %q", provider)
	}
	if err != nil {
		t.Fatal(err)
	}
	b := NewAppendAuthority(raw)
	key := fmt.Sprintf("itest/cellhive-%s-%d/owner.json", provider, time.Now().UnixNano())
	ctx := context.Background()
	var wg sync.WaitGroup
	wins := make(chan string, 2)
	for _, v := range []string{"a", "b"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, e := b.ConditionalCreate(ctx, key, []byte(v)); e == nil {
				wins <- v
			} else if !errors.Is(e, ErrPrecondition) {
				t.Errorf("concurrent create %s: %v", v, e)
			}
		}()
	}
	wg.Wait()
	close(wins)
	var winner string
	for v := range wins {
		if winner != "" {
			t.Fatalf("two owners: %s, %s", winner, v)
		}
		winner = v
	}
	if winner == "" {
		t.Fatal("no winner")
	}
	value, token, err := b.Get(ctx, key)
	if err != nil || string(value) != winner {
		t.Fatalf("read winner %q err=%v", value, err)
	}
	next, err := b.CAS(ctx, key, []byte("takeover"), token)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.ConditionalDelete(ctx, key, token); !errors.Is(err, ErrPrecondition) {
		t.Fatalf("stale delete: %v", err)
	}
	if err := b.ConditionalDelete(ctx, key, next); err != nil {
		t.Fatal(err)
	}
	if _, err := b.ConditionalCreate(ctx, key, []byte("reclaimed")); err != nil {
		t.Fatal(err)
	}
}
