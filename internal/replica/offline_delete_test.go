package replica

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
)

func TestOfflineDeleteRequiresQuiescenceAndNativeProof(t *testing.T) {
	for _, level := range []uint16{0, 1, 2, 3, 9} {
		t.Run(fmt.Sprint(level), func(t *testing.T) {
			m := newManager(t)
			sc := cell.Scope{Namespace: "n", Class: "__kv__", ID: "offline-delete"}
			key, _, err := m.AppendLevel(t.Context(), sc, 1, level, nativeBlob(t, 1, 1, 7))
			if err != nil {
				t.Fatal(err)
			}
			for _, opts := range []RetireOptions{{}, {Safe: true}} {
				if deleted, err := m.DeleteObjectWithOptions(t.Context(), sc, 1, key, opts); deleted || !errors.Is(err, ErrGCDeferred) {
					t.Fatalf("unquiescent deletion: %v %v", deleted, err)
				}
			}
			opts := RetireOptions{OfflineQuiescent: true}
			if deleted, err := m.DeleteObjectWithOptions(t.Context(), sc, 2, key, opts); deleted || err != nil {
				t.Fatalf("cross epoch: %v %v", deleted, err)
			}
			if deleted, err := m.DeleteObjectWithOptions(t.Context(), sc, 1, key, opts); !deleted || err != nil {
				t.Fatalf("delete: %v %v", deleted, err)
			}
			if _, _, err = m.B.Get(t.Context(), key); !errors.Is(err, bucket.ErrNotFound) {
				t.Fatalf("object survived: %v", err)
			}
			if deleted, err := m.DeleteObjectWithOptions(t.Context(), sc, 1, key, opts); deleted || err != nil {
				t.Fatalf("repeat: %v %v", deleted, err)
			}
		})
	}
}

func TestOfflineDeleteRejectsCorruptionAndOwnershipLoss(t *testing.T) {
	for _, mode := range []string{"corrupt", "range-mismatch", "owner-lost"} {
		t.Run(mode, func(t *testing.T) {
			m := newManager(t)
			sc := cell.Scope{Namespace: "n", Class: "__kv__", ID: "proof"}
			key := EpochPrefix(sc, 1) + "0009/0000000000000001-0000000000000001.ltx"
			raw := nativeBlob(t, 1, 1, 7)
			if mode == "corrupt" {
				raw[len(raw)-1] ^= 1
			}
			if mode == "range-mismatch" {
				raw = nativeBlob(t, 1, 2, 7)
			}
			if _, err := m.resourceObjects().Put(t.Context(), key, raw); err != nil {
				t.Fatal(err)
			}
			opts := RetireOptions{OfflineQuiescent: true}
			if mode == "owner-lost" {
				opts.Verify = func(context.Context) error { return errors.New("owner changed") }
			}
			if deleted, err := m.DeleteObjectWithOptions(t.Context(), sc, 1, key, opts); deleted || err == nil {
				t.Fatalf("invalid deletion: %v %v", deleted, err)
			}
			if _, _, err := m.B.Get(t.Context(), key); err != nil {
				t.Fatalf("protected object removed: %v", err)
			}
		})
	}
}
