package replica

import (
	"context"
	"errors"
	"testing"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
)

func TestRetireOwnedResourceValidatesEveryCASAttempt(t *testing.T) {
	for _, mode := range []string{"publish", "renewal", "node", "session", "epoch", "recreation", "different-job", "floor"} {
		t.Run(mode, func(t *testing.T) {
			base, err := bucket.NewFSBucket(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			sc := cell.Scope{Namespace: "retire", Class: "__kv__", ID: mode}
			expected := cell.Owner{Node: "node", Session: "session", Epoch: 9, Expiry: 123, RetirementJob: "prior", RetiredEpoch: 4}
			write := func(o cell.Owner) {
				t.Helper()
				raw, err := cell.MarshalOwner(o)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = base.Put(t.Context(), resourceFenceKey(sc), raw); err != nil {
					t.Fatal(err)
				}
			}
			write(expected)
			hooked := &fenceReadInterleaveBucket{Bucket: base, key: resourceFenceKey(sc)}
			next := expected
			if mode != "publish" {
				hooked.afterRead = func() {
					switch mode {
					case "renewal":
						next.Expiry++
					case "node":
						next.Node = "successor"
					case "session":
						next.Session = "successor"
					case "epoch":
						next.Epoch++
					case "recreation":
						next.RetirementJob = "new-generation"
						next.RetiredEpoch = 9
					case "floor":
						next.RetiredEpoch++
					case "different-job":
						next.Retired = true
						next.RetirementJob = "another"
					}
					write(next)
				}
			}
			m := New(hooked)
			err = m.RetireOwnedResource(t.Context(), sc, "delete", expected, func(context.Context) error { return nil })
			success := mode == "publish" || mode == "renewal"
			if success && err != nil {
				t.Fatal(err)
			}
			if !success {
				want := ErrResourceOwnerChanged
				if mode == "recreation" || mode == "different-job" || mode == "floor" {
					want = ErrResourceDeleted
				}
				if !errors.Is(err, want) {
					t.Fatalf("got %v want %v", err, want)
				}
			}
			raw, _, err := base.Get(t.Context(), resourceFenceKey(sc))
			if err != nil {
				t.Fatal(err)
			}
			got, err := cell.UnmarshalOwner(raw)
			if err != nil {
				t.Fatal(err)
			}
			if success {
				if !got.Retired || got.RetirementJob != "delete" || got.RetiredEpoch != 9 || got.Node != expected.Node || got.Session != expected.Session || got.Epoch != 9 || got.Expiry != next.Expiry {
					t.Fatalf("bad publication: %+v", got)
				}
				if err := m.RetireOwnedResource(t.Context(), sc, "delete", expected, func(context.Context) error { return nil }); err != nil {
					t.Fatalf("lost response retry: %v", err)
				}
				if err := m.AllowResource(t.Context(), sc, "delete"); err != nil {
					t.Fatal(err)
				}
				if err := m.RetireOwnedResource(t.Context(), sc, "delete", expected, func(context.Context) error { return nil }); !errors.Is(err, ErrResourceDeleted) {
					t.Fatalf("stale job after recreation: %v", err)
				}
			} else {
				want, _ := cell.MarshalOwner(next)
				if string(raw) != string(want) {
					t.Fatalf("changed ungated owner: %+v", got)
				}
			}
		})
	}
}

func TestRetireOwnedResourceReplayCannotCrossSuccessorOrAllow(t *testing.T) {
	for _, mode := range []string{"node", "session", "epoch", "allow-race"} {
		t.Run(mode, func(t *testing.T) {
			base, err := bucket.NewFSBucket(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			sc := cell.Scope{Namespace: "replay", Class: "__kv__", ID: mode}
			old := cell.Owner{Node: "node", Session: "session", Epoch: 9}
			current := old
			current.Retired = true
			current.RetirementJob = "job"
			current.RetiredEpoch = 9
			switch mode {
			case "node":
				current.Node = "successor"
			case "session":
				current.Session = "successor"
			case "epoch":
				current.Epoch++
			}
			raw, _ := cell.MarshalOwner(current)
			if _, err := base.Put(t.Context(), resourceFenceKey(sc), raw); err != nil {
				t.Fatal(err)
			}
			hooked := &fenceReadInterleaveBucket{Bucket: base, key: resourceFenceKey(sc)}
			if mode == "allow-race" {
				hooked.afterRead = func() {
					if err := New(base).AllowResource(t.Context(), sc, "job"); err != nil {
						t.Fatal(err)
					}
				}
			}
			want := ErrResourceOwnerChanged
			if mode == "allow-race" {
				want = ErrResourceDeleted
				current.Retired = false
			}
			if err := New(hooked).RetireOwnedResource(t.Context(), sc, "job", old, func(context.Context) error { return nil }); !errors.Is(err, want) {
				t.Fatalf("replay: got %v want %v", err, want)
			}
			got, _, err := base.Get(t.Context(), resourceFenceKey(sc))
			if err != nil {
				t.Fatal(err)
			}
			unchanged, _ := cell.MarshalOwner(current)
			if string(got) != string(unchanged) {
				t.Fatal("replay changed successor or recreated owner")
			}
		})
	}
}
