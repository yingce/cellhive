package instance

import (
	"path/filepath"
	"testing"
)

func TestAcquirePersistsInstanceAndLocks(t *testing.T) {
	dir := t.TempDir()
	h, err := Acquire(dir)
	if err != nil {
		t.Fatal(err)
	}
	id := h.ID
	h.Close()

	h2, err := Acquire(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer h2.Close()
	if h2.ID != id {
		t.Fatalf("instance id changed: %q != %q", h2.ID, id)
	}
	if _, err := Acquire(dir); err == nil {
		t.Fatal("second lock on the same directory succeeded")
	}
}

func TestAcquireCreatesUnlockedFile(t *testing.T) {
	dir := t.TempDir()
	h, err := Acquire(dir)
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Base(h.ID) != h.ID || len(h.ID) < 16 {
		t.Fatalf("bad instance id %q", h.ID)
	}
	h.Close()
}
