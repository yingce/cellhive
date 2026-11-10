package replica

import (
	"context"
	"errors"
	"testing"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
)

type fenceReadInterleaveBucket struct {
	bucket.Bucket
	key       string
	afterRead func()
}

func (b *fenceReadInterleaveBucket) Get(ctx context.Context, key string) ([]byte, string, error) {
	raw, token, err := b.Bucket.Get(ctx, key)
	if key == b.key && err == nil && b.afterRead != nil {
		hook := b.afterRead
		b.afterRead = nil
		hook()
	}
	return raw, token, err
}

func TestFenceUpdateUsesSameOwnerReadForDecisionAndCAS(t *testing.T) {
	for _, mode := range []string{"recreated", "raised-floor"} {
		t.Run(mode, func(t *testing.T) {
			base, err := bucket.NewFSBucket(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			sc := cell.Scope{Namespace: "atomic", Class: "__kv__", ID: mode}
			owner := cell.Owner{Node: "owner-node", Session: "owner-session", Epoch: 9, Retired: true, RetirementJob: "job", RetiredEpoch: 4}
			raw, err := cell.MarshalOwner(owner)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = base.Put(t.Context(), resourceFenceKey(sc), raw); err != nil {
				t.Fatal(err)
			}
			peer := New(base)
			hooked := &fenceReadInterleaveBucket{Bucket: base, key: resourceFenceKey(sc)}
			hooked.afterRead = func() {
				if mode == "recreated" {
					err = peer.AllowResource(t.Context(), sc, "job")
				} else {
					err = peer.FenceResource(t.Context(), sc, "job", 9)
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			local := New(hooked)
			if mode == "recreated" {
				if err := local.BlockResource(t.Context(), sc, "job"); !errors.Is(err, ErrResourceDeleted) {
					t.Fatalf("stale cleanup reblocked recreated resource: %v", err)
				}
			} else if err := local.AllowResource(t.Context(), sc, "job"); err != nil {
				t.Fatal(err)
			}
			got, _, err := base.Get(t.Context(), resourceFenceKey(sc))
			if err != nil {
				t.Fatal(err)
			}
			final, err := cell.UnmarshalOwner(got)
			if err != nil {
				t.Fatal(err)
			}
			if final.Retired || final.Node != owner.Node || final.Session != owner.Session || final.Epoch != owner.Epoch {
				t.Fatalf("owner lost or retirement resurrected: %+v", final)
			}
			if mode == "raised-floor" && final.RetiredEpoch != 9 {
				t.Fatalf("retired floor regressed to %d", final.RetiredEpoch)
			}
		})
	}
}
