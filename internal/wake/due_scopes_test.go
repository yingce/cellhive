package wake

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"

	"cellhive/internal/bucket"
)

type discoveryBucket struct {
	*bucket.FSBucket
	gets  []string
	lists []string
}

func (b *discoveryBucket) Get(ctx context.Context, key string) ([]byte, string, error) {
	b.gets = append(b.gets, key)
	return b.FSBucket.Get(ctx, key)
}

func (b *discoveryBucket) List(ctx context.Context, prefix string) ([]string, error) {
	b.lists = append(b.lists, prefix)
	return b.FSBucket.List(ctx, prefix)
}

func TestDueScopesMixedLayoutsAndRequestCounts(t *testing.T) {
	fs, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	b := &discoveryBucket{FSBucket: fs}
	index := New(b)
	now := time.Date(2026, 11, 23, 13, 5, 10, 0, time.UTC).UnixMilli()
	past := now - time.Minute.Milliseconds()
	future := now + time.Minute.Milliseconds()
	rev := strings.Repeat("a", 48)
	revKey := func(ms int64, scope string) string { return duePrefix(ms) + scope + "/" + rev + ".json" }
	write := func(key, scope string, ms int64) {
		t.Helper()
		body, err := json.Marshal(Entry{Scope: scope, DueMs: ms})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := b.Put(t.Context(), key, body); err != nil {
			t.Fatal(err)
		}
	}
	write(dueKey(past, "ns/c/old"), "ns/c/old", past)
	write(revKey(past, "ns/c/versioned.json"), "ns/c/versioned.json", past)
	write(revKey(now, "ns/c/old"), "ns/c/old", now) // duplicate across minutes
	ready := revKey(now, "ns/c/boundary")
	later := dueKey(now, "ns/c/later")
	write(ready, "ns/c/boundary", now)
	write(later, "ns/c/later", now+1)
	write(dueKey(future, "ns/c/future"), "ns/c/future", future)
	write(duePrefix(past)+"ns/c/malformed/short.json", "ns/c/malformed", past)

	scopes, err := index.DueScopes(t.Context(), now)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, sc := range scopes {
		got = append(got, sc.String())
	}
	slices.Sort(got)
	if !slices.Equal(got, []string{"ns/c/boundary", "ns/c/old", "ns/c/versioned.json"}) {
		t.Fatalf("scopes = %v", got)
	}
	slices.Sort(b.gets)
	if !slices.Equal(b.gets, []string{ready, later}) {
		t.Fatalf("GETs = %v, want only two exact-time reads in current minute", b.gets)
	}
	if slices.Contains(b.lists, duePrefix(future)) {
		t.Fatalf("opened future minute: %v", b.lists)
	}
}
