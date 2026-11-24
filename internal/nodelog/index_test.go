package nodelog

import (
	"context"
	"errors"
	"testing"

	"cellhive/internal/bucket"
)

type testIndex struct {
	entries                  map[string][]byte
	failRegister, failRemove bool
}

func (i *testIndex) RegisterRecoverySession(_ context.Context, k string, v []byte) error {
	if i.failRegister {
		return errors.New("redis unavailable")
	}
	if i.entries == nil {
		i.entries = map[string][]byte{}
	}
	i.entries[k] = append([]byte(nil), v...)
	return nil
}
func (i *testIndex) RemoveRecoverySession(_ context.Context, k string) error {
	if i.failRemove {
		return errors.New("redis unavailable")
	}
	delete(i.entries, k)
	return nil
}
func (i *testIndex) PendingRecoverySessions(context.Context, string) ([]string, map[string][]byte, error) {
	var keys []string
	for k := range i.entries {
		keys = append(keys, k)
	}
	return keys, i.entries, nil
}

type indexCountingBucket struct {
	bucket.Bucket
	gets, lists, creates int
	failCreate           bool
}

func (b *indexCountingBucket) Get(c context.Context, k string) ([]byte, string, error) {
	b.gets++
	return b.Bucket.Get(c, k)
}
func (b *indexCountingBucket) List(c context.Context, k string) ([]string, error) {
	b.lists++
	return b.Bucket.List(c, k)
}
func (b *indexCountingBucket) ConditionalCreate(c context.Context, k string, v []byte) (string, error) {
	b.creates++
	if b.failCreate {
		return "", errors.New("interrupted publication")
	}
	return b.Bucket.ConditionalCreate(c, k, v)
}
func indexFixture(t *testing.T) (*Manager, *indexCountingBucket, *testIndex) {
	t.Helper()
	fs, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	b := &indexCountingBucket{Bucket: fs}
	m := New(b, "node")
	idx := &testIndex{}
	m.Index = idx
	return m, b, idx
}

func TestIndexRegistrationPrecedesPublication(t *testing.T) {
	m, b, i := indexFixture(t)
	i.failRegister = true
	if _, err := m.Open(t.Context(), "session", 1, []string{"peer"}); err == nil {
		t.Fatal("unregistered open succeeded")
	}
	if b.creates != 0 {
		t.Fatal("COS publication before registration")
	}
	i.failRegister = false
	b.failCreate = true
	if _, err := m.Open(t.Context(), "session", 1, []string{"peer"}); err == nil {
		t.Fatal("failed COS publication succeeded")
	}
	if len(i.entries) != 1 {
		t.Fatal("prepublication hint missing")
	}
	// A missing record never removes a hint: publication may merely be delayed.
	if _, _, err := m.Get(t.Context(), "node", "session"); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if len(i.entries) != 1 {
		t.Fatal("removed delayed publisher")
	}
	b.failCreate = false
	if _, err := m.Open(t.Context(), "session", 1, []string{"peer"}); err != nil {
		t.Fatal(err)
	}
	b.gets, b.lists = 0, 0
	restarted := New(b, "node")
	restarted.Index = i
	listing, err := restarted.DiscoverPending(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	recs, err := listing.Records(t.Context(), "node")
	if err != nil || len(recs) != 1 {
		t.Fatal(recs, err)
	}
	if b.gets != 0 || b.lists != 0 {
		t.Fatalf("discovery touched COS: gets=%d lists=%d", b.gets, b.lists)
	}
	if _, _, err = m.Get(t.Context(), "node", "session"); err != nil {
		t.Fatal(err)
	}
	if b.gets != 1 {
		t.Fatal("authoritative Get did not use COS")
	}
}
func TestIndexSealCrashReconcilesOnAuthoritativeRead(t *testing.T) {
	m, _, i := indexFixture(t)
	if _, err := m.Open(t.Context(), "session", 1, []string{"peer"}); err != nil {
		t.Fatal(err)
	}
	i.failRemove = true
	if _, err := m.Seal(t.Context(), "node", "session"); err == nil {
		t.Fatal("index removal failure hidden")
	}
	if len(i.entries) != 1 {
		t.Fatal("lost hint before sealed proof")
	}
	i.failRemove = false
	r, _, err := m.Get(t.Context(), "node", "session")
	if err != nil || r.Status != StatusSealed {
		t.Fatal(r, err)
	}
	if len(i.entries) != 0 {
		t.Fatal("sealed hint not reconciled")
	}
	// A late retry can register but must never reopen or acknowledge sealed COS.
	if _, err = m.Open(t.Context(), "session", 1, []string{"peer"}); !errors.Is(err, ErrSessionClosed) {
		t.Fatal(err)
	}
	if len(i.entries) != 0 {
		t.Fatal("late sealed retry left hint")
	}
}
func TestIndexExistingOpenRetryStillRequiresRegistration(t *testing.T) {
	m, _, i := indexFixture(t)
	if _, err := m.Open(t.Context(), "session", 1, []string{"peer"}); err != nil {
		t.Fatal(err)
	}
	i.failRegister = true
	if _, err := m.Open(t.Context(), "session", 1, []string{"peer"}); err == nil {
		t.Fatal("retry bypassed unavailable index")
	}
	i.failRegister = false
	if _, err := m.Open(t.Context(), "session", 1, []string{"different"}); !errors.Is(err, ErrEnsembleMismatch) {
		t.Fatal(err)
	}
}
