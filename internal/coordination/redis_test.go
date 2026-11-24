package coordination

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/lease"
	"cellhive/internal/owner"
	"cellhive/internal/wake"
)

func fixture(t *testing.T) (*Bucket, *bucket.FSBucket) {
	t.Helper()
	endpoint := os.Getenv("CELLHIVE_TEST_COORDINATION_URL")
	if endpoint == "" {
		t.Skip("requires isolated Redis/Kvrocks")
	}
	base, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	region := "test-" + nonce()
	if err = Initialize(t.Context(), base, endpoint, region); err != nil {
		t.Fatal(err)
	}
	b, err := Open(t.Context(), base, endpoint, region)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Store.client.Del(context.Background(), b.Store.keys...); b.Close() })
	return b, base
}
func TestCASABAAndGeneration(t *testing.T) {
	b, _ := fixture(t)
	c := t.Context()
	k := "nodes/n.json"
	token, err := b.ConditionalCreate(c, k, []byte("a"))
	if err != nil {
		t.Fatal(err)
	}
	var wins atomic.Int32
	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, e := b.CAS(c, k, []byte("b"), token)
			if e == nil {
				wins.Add(1)
			} else if !errors.Is(e, bucket.ErrPrecondition) {
				t.Error(e)
			}
		}()
	}
	wg.Wait()
	if wins.Load() != 1 {
		t.Fatal(wins.Load())
	}
	if err = b.ConditionalDelete(c, k, token); !errors.Is(err, bucket.ErrPrecondition) {
		t.Fatal(err)
	}
	_, current, _ := b.Get(c, k)
	if err = b.ConditionalDelete(c, k, current); err != nil {
		t.Fatal(err)
	}
	again, err := b.ConditionalCreate(c, k, []byte("a"))
	if err != nil || again == token {
		t.Fatal(again, err)
	}
	if _, err = b.CAS(c, k, []byte("stale"), token); !errors.Is(err, bucket.ErrPrecondition) {
		t.Fatal(err)
	}
	b.Store.client.Del(c, b.Store.keys[0])
	if _, _, err = b.Get(c, k); !errors.Is(err, ErrGeneration) {
		t.Fatal(err)
	}
	if _, err = b.Put(c, k, []byte("x")); !errors.Is(err, ErrGeneration) {
		t.Fatal(err)
	}
}
func TestMigrationAndOwnerRestart(t *testing.T) {
	b, base := fixture(t)
	c := t.Context()
	if err := CheckObjectMode(c, base); err == nil {
		t.Fatal("object authority accepted")
	}
	if err := Initialize(c, base, os.Getenv("CELLHIVE_TEST_COORDINATION_URL"), "wrong"); err == nil {
		t.Fatal("wrong region")
	}
	sc := cell.Scope{Namespace: "test", Class: "kv", ID: "one"}
	la := lease.NewManager(b, "a", "a-session", "a", "", time.Second)
	lb := lease.NewManager(b, "b", "b-session", "b", "", time.Second)
	if err := la.Publish(c, lease.Load{}); err != nil {
		t.Fatal(err)
	}
	if err := lb.Publish(c, lease.Load{}); err != nil {
		t.Fatal(err)
	}
	a := &owner.Manager{B: b, NodeID: "a", Session: "a-session", LeaseView: la, OwnerTTL: time.Second}
	bm := &owner.Manager{B: b, NodeID: "b", Session: "b-session", LeaseView: lb, OwnerTTL: time.Second}
	first, err := a.Claim(c, sc, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = bm.Claim(c, sc, time.Now()); !errors.Is(err, owner.ErrOwnerLive) {
		t.Fatal(err)
	}
	a.Forget(sc, first.Epoch)
	if n, err := b.Store.OwnedCount(c, "a", "a-session"); err != nil || n != 1 {
		t.Fatal(n, err)
	}
	// Publish an expired lease to exercise takeover without wall-clock sleeps.
	raw, tag, err := b.Get(c, lease.Key("a"))
	if err != nil {
		t.Fatal(err)
	}
	_ = raw
	if _, err = b.CAS(c, lease.Key("a"), []byte(`{"node":"a","session":"a-session","expiry":1}`), tag); err != nil {
		t.Fatal(err)
	}
	lb = lease.NewManager(b, "b", "b-session", "b", "", time.Second)
	bm.LeaseView = lb
	next, err := bm.Claim(c, sc, time.Now())
	if err != nil || next.Epoch <= first.Epoch {
		t.Fatal(next, err)
	}
	if err = a.Release(c, sc, first.Epoch); !errors.Is(err, owner.ErrEpochMismatch) {
		t.Fatal(err)
	}
	if n, _ := b.Store.OwnedCount(c, "a", "a-session"); n != 0 {
		t.Fatal(n)
	}
	if n, _ := b.Store.OwnedCount(c, "b", "b-session"); n != 1 {
		t.Fatal(n)
	}
	if _, _, err = base.Get(c, owner.Key(sc)); !errors.Is(err, bucket.ErrNotFound) {
		t.Fatal("owner touched bucket", err)
	}
	// New client/session sees the same authority and cannot claim B's cell.
	restart := &owner.Manager{B: b, NodeID: "a", Session: "a-new", LeaseView: la, OwnerTTL: time.Second}
	if _, err = restart.Claim(c, sc, time.Now()); !errors.Is(err, owner.ErrOwnerLive) {
		t.Fatal(err)
	}
}
func TestWakeAndBulkSeparation(t *testing.T) {
	b, base := fixture(t)
	c := t.Context()
	idx := wake.New(b)
	if err := idx.Put(c, "ns/alarm/one", 1, "alarm", "v1"); err != nil {
		t.Fatal(err)
	}
	due, err := idx.DueScopes(c, 2)
	if err != nil || len(due) != 1 {
		t.Fatal(due, err)
	}
	if keys, _ := base.List(c, "wake/"); len(keys) != 0 {
		t.Fatal(keys)
	}
	if err = idx.Delete(c, "ns/alarm/one"); err != nil {
		t.Fatal(err)
	}
	k := "cells/ns/alarm/one/ltx/e1/test.ltx"
	v := []byte("payload")
	if _, err = b.PutReader(c, k, bytes.NewReader(v), int64(len(v))); err != nil {
		t.Fatal(err)
	}
	got, _, err := base.Get(c, k)
	if err != nil || !bytes.Equal(got, v) {
		t.Fatal(string(got), err)
	}
	if _, _, err = b.Store.Get(c, k); !errors.Is(err, bucket.ErrNotFound) {
		t.Fatal(err)
	}
}
func TestOfflineCopyPreservesEpochAndRetirement(t *testing.T) {
	endpoint := os.Getenv("CELLHIVE_TEST_COORDINATION_URL")
	if endpoint == "" {
		t.Skip("isolated Redis required")
	}
	c := t.Context()
	base, _ := bucket.NewFSBucket(t.TempDir())
	region := "test-" + nonce()
	data := []byte(`{"node":"old","session":"s","epoch":18446744073709551614,"retired":true,"retirement_job":"job"}`)
	key := "cells/ns/c/id/own.json"
	base.Put(c, key, data)
	base.Put(c, "cells/ns/c/id/owner-gen", []byte("18446744073709551614"))
	if err := Initialize(c, base, endpoint, region); err != nil {
		t.Fatal(err)
	}
	b, err := Open(c, base, endpoint, region)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	defer b.Store.client.Del(c, b.Store.keys...)
	got, _, err := b.Get(c, key)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatal(string(got), err)
	}
	if n, _ := b.Store.OwnedCount(c, "old", "s"); n != 0 {
		t.Fatal(n)
	}
	if err = Initialize(c, base, endpoint, region); err == nil {
		t.Fatal("active overwritten")
	}
}

func TestAdapterDiagnose(t *testing.T) {
	b, _ := fixture(t)
	if err := bucket.Diagnose(t.Context(), b); err != nil {
		t.Fatal(err)
	}
}

func TestAtomicClaimHighWaterAndFailure(t *testing.T) {
	b, _ := fixture(t)
	ctx := t.Context()
	sc := cell.Scope{Namespace: "ns", Class: "c", ID: "id"}
	k := owner.Key(sc)
	// Above 2^53: Lua must not round either the epoch or unrelated retirement data.
	b.Put(ctx, "cells/ns/c/id/owner-gen", []byte("9007199254740993"))
	body := []byte(`{"node":"a","session":"s","epoch":0,"retired_epoch":9007199254740993}`)
	raw, tag, err := b.ClaimOwner(ctx, k, body, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	o, err := cell.UnmarshalOwner(raw)
	if err != nil || o.Epoch != 9007199254740994 || o.RetiredEpoch != 9007199254740993 {
		t.Fatal(o, err)
	}
	if _, _, err = b.ClaimOwner(ctx, k, body, "", 0); !errors.Is(err, bucket.ErrPrecondition) {
		t.Fatal(err)
	}
	gen, _, _ := b.Get(ctx, "cells/ns/c/id/owner-gen")
	if string(gen) != "9007199254740994" {
		t.Fatal(string(gen))
	}
	if err = b.ConditionalDelete(ctx, k, tag); err != nil {
		t.Fatal(err)
	}
	raw, _, err = b.ClaimOwner(ctx, k, body, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	o, _ = cell.UnmarshalOwner(raw)
	if o.Epoch != 9007199254740995 {
		t.Fatal(o)
	}
	b.Put(ctx, "cells/ns/c/id/owner-gen", []byte("broken"))
	_, tag, _ = b.Get(ctx, k)
	if _, _, err = b.ClaimOwner(ctx, k, body, tag, 0); err == nil {
		t.Fatal("corrupt high-water accepted")
	}
	after, newTag, _ := b.Get(ctx, k)
	if !bytes.Equal(after, raw) || newTag != tag {
		t.Fatal("failed claim mutated owner")
	}
	b.Put(ctx, "cells/ns/c/id/owner-gen", []byte("18446744073709551615"))
	if _, _, err = b.ClaimOwner(ctx, k, body, tag, 0); err == nil {
		t.Fatal("overflow accepted")
	}
}

type unavailableMetadataBucket struct {
	bucket.Bucket
	calls atomic.Int32
}

func (b *unavailableMetadataBucket) Get(c context.Context, k string) ([]byte, string, error) {
	if Selected(k) {
		b.calls.Add(1)
		return nil, "", errors.New("unexpected metadata data-store read")
	}
	return b.Bucket.Get(c, k)
}
func (b *unavailableMetadataBucket) List(c context.Context, p string) ([]string, error) {
	if purePrefix(p) {
		b.calls.Add(1)
		return nil, errors.New("unexpected metadata data-store listing")
	}
	return b.Bucket.List(c, p)
}
func TestMetadataNeverFallsBack(t *testing.T) {
	b, _ := fixture(t)
	meter := &unavailableMetadataBucket{Bucket: b.Base}
	b.Base = meter
	ctx := t.Context()
	if _, _, err := b.Get(ctx, "nodes/missing.json"); !errors.Is(err, bucket.ErrNotFound) {
		t.Fatal(err)
	}
	if _, err := b.List(ctx, "nodes/"); err != nil {
		t.Fatal(err)
	}
	b.Store.client.Del(ctx, b.Store.keys[0])
	if _, _, err := b.Get(ctx, "nodes/missing.json"); !errors.Is(err, ErrGeneration) {
		t.Fatal(err)
	}
	if meter.calls.Load() != 0 {
		t.Fatal(meter.calls.Load())
	}
}

type interruptCopy struct {
	bucket.Bucket
	fail bool
}

func (b *interruptCopy) List(c context.Context, p string) ([]string, error) {
	if b.fail && p == "nodes/" {
		return nil, errors.New("injected migration interruption")
	}
	return b.Bucket.List(c, p)
}
func TestMigrationResumeAndMissingAuthority(t *testing.T) {
	endpoint := os.Getenv("CELLHIVE_TEST_COORDINATION_URL")
	if endpoint == "" {
		t.Skip("isolated Redis required")
	}
	ctx := t.Context()
	fs, _ := bucket.NewFSBucket(t.TempDir())
	base := &interruptCopy{Bucket: fs, fail: true}
	region := "resume-" + nonce()
	if err := Initialize(ctx, base, endpoint, region); err == nil {
		t.Fatal("interruption missing")
	}
	if b, err := Open(ctx, base, endpoint, region); err == nil {
		b.Close()
		t.Fatal("partial migration served")
	}
	if err := CheckObjectMode(ctx, base); err == nil {
		t.Fatal("object mode served during migration")
	}
	base.fail = false
	if err := Initialize(ctx, base, endpoint, region); err != nil {
		t.Fatal(err)
	}
	b, err := Open(ctx, base, endpoint, region)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	defer b.Store.client.Del(context.Background(), b.Store.keys...)
	b.Store.client.Del(ctx, b.Store.keys[0])
	if other, err := Open(ctx, base, endpoint, region); err == nil {
		other.Close()
		t.Fatal("empty authority recreated")
	}
	if err = Initialize(ctx, base, endpoint, region); err == nil {
		t.Fatal("active binding rebuilt empty authority")
	}
}

func TestConcurrentOwnerClaimAllocatesOnlyWinner(t *testing.T) {
	b, _ := fixture(t)
	ctx := t.Context()
	key := "cells/n/c/race/own.json"
	body := []byte(`{"node":"n","session":"s","epoch":0}`)
	var wins atomic.Int32
	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, err := b.ClaimOwner(ctx, key, body, "", 0)
			if err == nil {
				wins.Add(1)
			} else if !errors.Is(err, bucket.ErrPrecondition) {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if wins.Load() != 1 {
		t.Fatal(wins.Load())
	}
	gen, _, err := b.Get(ctx, "cells/n/c/race/owner-gen")
	if err != nil || string(gen) != "1" {
		t.Fatal(string(gen), err)
	}
	if n, err := b.Store.OwnedCount(ctx, "n", "s"); err != nil || n != 1 {
		t.Fatal(n, err)
	}
}

type bulkSpy struct {
	bucket.Bucket
	pages, prefixes, streams int
}

func (s *bulkSpy) ListPage(context.Context, string, string, int) ([]bucket.ObjectInfo, string, error) {
	s.pages++
	return []bucket.ObjectInfo{{Key: "sentinel"}}, "cursor", nil
}
func (s *bulkSpy) CommonPrefixes(context.Context, string, string) ([]string, error) {
	s.prefixes++
	return []string{"sentinel/"}, nil
}
func (s *bulkSpy) PutReader(context.Context, string, io.ReadSeeker, int64) (string, error) {
	s.streams++
	return "stream", nil
}
func TestBulkCapabilitiesForwarded(t *testing.T) {
	fs, _ := bucket.NewFSBucket(t.TempDir())
	spy := &bulkSpy{Bucket: fs}
	b := &Bucket{Base: spy}
	ctx := t.Context()
	if _, _, err := b.ListPage(ctx, "cells/ns/c/id/ltx/", "", 5); err != nil {
		t.Fatal(err)
	}
	if _, err := b.CommonPrefixes(ctx, "cells/ns/c/id/ltx/", "/"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.PutReader(ctx, "cells/ns/c/id/ltx/e1/a.ltx", bytes.NewReader(nil), 0); err != nil {
		t.Fatal(err)
	}
	if spy.pages != 1 || spy.prefixes != 1 || spy.streams != 1 {
		t.Fatal(spy)
	}
}

func TestMaintenanceAllocationSharesHighWater(t *testing.T) {
	b, _ := fixture(t)
	ctx := t.Context()
	key := "cells/ns/c/maintenance/owner-gen"
	n, err := b.ReserveOwnerEpoch(ctx, key, 9)
	if err != nil || n != 10 {
		t.Fatal(n, err)
	}
	k := "cells/ns/c/maintenance/own.json"
	body := []byte(`{"node":"a","session":"s","epoch":0}`)
	raw, _, err := b.ClaimOwner(ctx, k, body, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	o, _ := cell.UnmarshalOwner(raw)
	if o.Epoch != 11 {
		t.Fatal(o)
	}
	n, err = b.ReserveOwnerEpoch(ctx, key, 0)
	if err != nil || n != 12 {
		t.Fatal(n, err)
	}
	if _, err = b.ReserveOwnerEpoch(ctx, key, ^uint64(0)); err == nil {
		t.Fatal("overflow accepted")
	}
}
