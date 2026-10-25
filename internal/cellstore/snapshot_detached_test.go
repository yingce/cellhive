package cellstore

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestDetachedSnapshotBoundsWALImageBeforeCopy(t *testing.T) {
	ctx := context.Background()
	c, err := OpenAt(ctx, filepath.Join(t.TempDir(), "source.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.PutTx(ctx, "large", make([]byte, 1<<20), nil); err != nil {
		t.Fatal(err)
	}
	called := false
	_, _, _, err = c.SnapshotDetached(ctx, 8192, 65536, func(int, int, uint64, uint32, uint32, uint32, [][]byte) error { called = true; return nil })
	if !errors.Is(err, ErrSnapshotTooLarge) || called {
		t.Fatalf("err=%v called=%v", err, called)
	}
}
func TestCleanupSnapshotCopiesOnlyOwnedTemporaryDirectories(t *testing.T) {
	root := t.TempDir()
	stale := filepath.Join(root, "cells", "ns", "class", ".cellhive-snapshot-old")
	keep := filepath.Join(root, "cells", "ns", "class", "main.db")
	unrelated := filepath.Join(root, "cells", ".cellhive-snapshot-namespace", "class")
	if err := os.MkdirAll(stale, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(unrelated, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keep, []byte("data"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := CleanupSnapshotCopies(root); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatalf("stale copy remains: %v", err)
	}
	for _, p := range []string{keep, unrelated} {
		if _, err := os.Stat(p); err != nil {
			t.Fatal(err)
		}
	}
}
