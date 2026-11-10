package replica

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
)

type cleanupAuthorityBucket struct {
	bucket.Bucket
	ownerKey string
	casCalls int
	deletes  int
	revoked  bool
}

func (b *cleanupAuthorityBucket) CAS(ctx context.Context, key string, data []byte, etag string) (string, error) {
	if key == b.ownerKey {
		b.casCalls++
		if b.casCalls == 1 {
			b.revoked = true
			return "", bucket.ErrPrecondition
		}
	}
	return b.Bucket.CAS(ctx, key, data, etag)
}
func (b *cleanupAuthorityBucket) Delete(ctx context.Context, key string) error {
	err := b.Bucket.Delete(ctx, key)
	if err == nil {
		b.deletes++
		b.revoked = true
	}
	return err
}

func TestRetirementStopsCASRetryAfterCleanupLeaseLoss(t *testing.T) {
	base, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	sc := cell.Scope{Namespace: "cleanup", Class: "__kv__", ID: "retry"}
	expected := cell.Owner{Node: "n", Session: "s", Epoch: 1}
	raw, _ := cell.MarshalOwner(expected)
	if _, err = base.Put(t.Context(), resourceFenceKey(sc), raw); err != nil {
		t.Fatal(err)
	}
	b := &cleanupAuthorityBucket{Bucket: base, ownerKey: resourceFenceKey(sc)}
	lost := errors.New("cleanup lease revoked")
	err = New(b).RetireOwnedResource(t.Context(), sc, "job", expected, func(context.Context) error {
		if b.revoked {
			return lost
		}
		return nil
	})
	if !errors.Is(err, lost) || b.casCalls != 1 {
		t.Fatalf("err=%v CAS calls=%d", err, b.casCalls)
	}
	got, _, err := base.Get(t.Context(), resourceFenceKey(sc))
	if err != nil {
		t.Fatal(err)
	}
	current, err := cell.UnmarshalOwner(got)
	if err != nil || current.Retired {
		t.Fatalf("owner=%+v err=%v", current, err)
	}
}

func TestPurgeStopsAfterCleanupLeaseLoss(t *testing.T) {
	base, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	sc := cell.Scope{Namespace: "cleanup", Class: "__kv__", ID: "purge"}
	m := New(base)
	if err = m.BlockResource(t.Context(), sc, "job"); err != nil {
		t.Fatal(err)
	}
	if err = m.FenceResource(t.Context(), sc, "job", 1); err != nil {
		t.Fatal(err)
	}
	var old []string
	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("%s/ltx/e0000000000000001/0000/%016x.ltx", sc.Key(), i)
		if _, err = base.Put(t.Context(), key, []byte("old")); err != nil {
			t.Fatal(err)
		}
		old = append(old, key)
	}
	successor := sc.Key() + "/ltx/e0000000000000002/0000/new.ltx"
	if _, err = base.Put(t.Context(), successor, []byte("successor")); err != nil {
		t.Fatal(err)
	}
	b := &cleanupAuthorityBucket{Bucket: base}
	lost := errors.New("cleanup lease revoked")
	done, err := New(b).PurgeResource(t.Context(), sc, "job", 500, func(context.Context) error {
		if b.revoked {
			return lost
		}
		return nil
	})
	if done || !errors.Is(err, lost) || b.deletes != 1 {
		t.Fatalf("done=%v err=%v deletes=%d", done, err, b.deletes)
	}
	remaining := 0
	for _, key := range old {
		if _, _, err := base.Get(t.Context(), key); err == nil {
			remaining++
		} else if !errors.Is(err, bucket.ErrNotFound) {
			t.Fatal(err)
		}
	}
	if remaining != 2 {
		t.Fatalf("remaining=%d", remaining)
	}
	got, _, err := base.Get(t.Context(), successor)
	if err != nil || string(got) != "successor" {
		t.Fatalf("successor=%q err=%v", got, err)
	}
}
