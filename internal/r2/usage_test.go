package r2

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"testing"

	"cellhive/internal/bucket"
	"cellhive/internal/objectstore"
)

// List is forbidden on the usage path, including put/delete tracking.
type noListUsageBucket struct{ bucket.Bucket }

func (b noListUsageBucket) List(context.Context, string) ([]string, error) {
	panic("R2 usage hot path called List")
}

type failingUsageBucket struct{ bucket.Bucket }

func (b failingUsageBucket) ConditionalCreate(ctx context.Context, key string, data []byte) (string, error) {
	if strings.HasPrefix(key, objectstore.PrefixR2Usage) {
		return "", errors.New("usage ledger unavailable")
	}
	return b.Bucket.ConditionalCreate(ctx, key, data)
}

func TestUsageStartsEmptyAndPersistsMutations(t *testing.T) {
	ctx := context.Background()
	base, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	// Simulate an object created before usage tracking existed.
	oldKey, _ := Key("acme", "media", "legacy")
	if _, err := base.Put(ctx, oldKey, []byte("old-existing-data")); err != nil {
		t.Fatal(err)
	}
	s := New(noListUsageBucket{base})
	check := func(objects, bytes int64) {
		t.Helper()
		got, err := s.Usage(ctx, "acme", "media")
		if err != nil || got.Objects != objects || got.Bytes != bytes || !got.Estimated {
			t.Fatalf("usage = %+v, %v; want %d objects/%d bytes", got, err, objects, bytes)
		}
	}
	check(0, 0)
	if err := s.Delete(ctx, "acme", "media", "legacy"); err != nil {
		t.Fatal(err)
	}
	check(0, 0) // deleting an untracked object does not subtract old bytes
	if _, err := base.Put(ctx, oldKey, []byte("untracked-again")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Put(ctx, "acme", "media", "legacy", []byte("new")); err != nil {
		t.Fatal(err)
	}
	check(1, 3) // first overwrite counts only the new value
	if _, err := s.Put(ctx, "acme", "media", "legacy", []byte("longer")); err != nil {
		t.Fatal(err)
	}
	check(1, 6)
	if _, err := s.Put(ctx, "acme", "media", "empty", nil); err != nil {
		t.Fatal(err)
	}
	check(2, 6)
	no := false
	if _, matched, err := s.PutConditional(ctx, "acme", "media", "empty", []byte("blocked"), nil, &Condition{Exists: &no}); err != nil || matched {
		t.Fatalf("failed conditional put changed data: matched=%v err=%v", matched, err)
	}
	check(2, 6)
	if err := s.Delete(ctx, "acme", "media", "legacy"); err != nil {
		t.Fatal(err)
	}
	check(1, 0)
	// A new Store instance reads the persisted estimate.
	s = New(noListUsageBucket{base})
	check(1, 0)
}

func TestUsageMultipartCountsOnlyCompletedObject(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	id, err := s.CreateMultipart(ctx, "acme", "media", "large")
	if err != nil {
		t.Fatal(err)
	}
	part, err := s.UploadPart(ctx, "acme", "media", "large", id, 1, []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	if got, err := s.Usage(ctx, "acme", "media"); err != nil || got.Objects != 0 || got.Bytes != 0 {
		t.Fatalf("staged part counted: %+v, %v", got, err)
	}
	if _, _, err := s.CompleteMultipart(ctx, "acme", "media", "large", id, []Part{{PartNumber: 1, ETag: part}}); err != nil {
		t.Fatal(err)
	}
	if got, err := s.Usage(ctx, "acme", "media"); err != nil || got.Objects != 1 || got.Bytes != 5 {
		t.Fatalf("completed object usage = %+v, %v", got, err)
	}
}

func TestUsageFailureDoesNotTurnSuccessfulPutIntoFailure(t *testing.T) {
	ctx := context.Background()
	base, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	s := New(failingUsageBucket{base})
	if _, err := s.Put(ctx, "acme", "media", "k", []byte("data")); err != nil {
		t.Fatalf("object put failed because the usage ledger failed: %v", err)
	}
	if data, _, err := s.Get(ctx, "acme", "media", "k"); err != nil || string(data) != "data" {
		t.Fatalf("object missing after successful put: %q, %v", data, err)
	}
	if got, err := s.Usage(ctx, "acme", "media"); err != nil || got.Objects != 0 || got.Bytes != 0 {
		t.Fatalf("failed ledger update reported a false total: %+v, %v", got, err)
	}
}

func TestUsageConcurrentStoresSharePersistedSummary(t *testing.T) {
	ctx := context.Background()
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const writers = 8
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := strconv.Itoa(i)
			if _, err := New(b).Put(ctx, "acme", "media", key, []byte("x")); err != nil {
				t.Errorf("put %s: %v", key, err)
			}
		}(i)
	}
	wg.Wait()
	if got, err := New(b).Usage(ctx, "acme", "media"); err != nil || got.Objects != writers || got.Bytes != writers {
		t.Fatalf("concurrent usage = %+v, %v; want %d/%d", got, err, writers, writers)
	}
}

func TestUsageConcurrentOverwritesCountOneObject(t *testing.T) {
	ctx := context.Background()
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const writers = 8
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := New(b).Put(ctx, "acme", "media", "same-key", []byte("x")); err != nil {
				t.Errorf("put: %v", err)
			}
		}()
	}
	wg.Wait()
	if got, err := New(b).Usage(ctx, "acme", "media"); err != nil || got.Objects != 1 || got.Bytes != 1 {
		t.Fatalf("concurrent overwrite usage = %+v, %v; want 1/1", got, err)
	}
}
