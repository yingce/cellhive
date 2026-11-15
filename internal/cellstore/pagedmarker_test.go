package cellstore

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"cellhive/internal/cell"
	"cellhive/internal/pagedvfs"
)

func TestPagedMarkerReadFailurePreservesFiles(t *testing.T) {
	for _, operation := range []string{"Cell", "Open", "PendingTimerMin"} {
		for _, missing := range []bool{false, true} {
			t.Run(operation+map[bool]string{false: "/existing", true: "/missing"}[missing], func(t *testing.T) {
				ctx := context.Background()
				s, err := New(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				sc := cell.Scope{Namespace: "test", Class: "__kv__", ID: "read-error"}
				p, err := s.Path(sc)
				if err != nil {
					t.Fatal(err)
				}
				var before []byte
				if !missing {
					c, err := s.Open(ctx, sc)
					if err != nil {
						t.Fatal(err)
					}
					if err := c.Put(ctx, "preserve", []byte("value"), nil); err != nil {
						t.Fatal(err)
					}
					if err := c.Close(); err != nil {
						t.Fatal(err)
					}
					before, err = os.ReadFile(p)
					if err != nil {
						t.Fatal(err)
					}
				}
				marker := pagedMarkerPath(p)
				if err := os.Mkdir(marker, 0o755); err != nil {
					t.Fatal(err)
				}
				info, err := os.Stat(marker)
				if err != nil {
					t.Fatal(err)
				}
				calls := 0
				s.Paged = func(context.Context, cell.Scope, string) (pagedvfs.Source, bool, error) {
					calls++
					return nil, false, nil
				}
				s.Hydrate = func(context.Context, cell.Scope, string) error { calls++; return nil }
				switch operation {
				case "Cell":
					var c *Cell
					c, err = s.Cell(ctx, sc)
					if c != nil {
						_ = c.Close()
					}
				case "Open":
					var c *Cell
					c, err = s.Open(ctx, sc)
					if c != nil {
						_ = c.Close()
					}
				case "PendingTimerMin":
					_, _, err = PendingTimerMin(ctx, p)
				}
				var pe *os.PathError
				if !errors.Is(err, syscall.EISDIR) || !errors.As(err, &pe) || pe.Path != marker || pe.Op != "read" {
					t.Errorf("expected original marker read EISDIR, got %v", err)
				}
				if calls != 0 {
					t.Errorf("restore callbacks called %d times", calls)
				}
				afterInfo, err := os.Stat(marker)
				if err != nil || !os.SameFile(info, afterInfo) {
					t.Errorf("marker changed: %v", err)
				}
				after, err := os.ReadFile(p)
				if missing {
					if !errors.Is(err, os.ErrNotExist) {
						t.Errorf("missing DB created: %v", err)
					}
				} else if err != nil || !bytes.Equal(before, after) {
					t.Errorf("original DB changed: %v", err)
				}
			})
		}
	}
}

func TestPagedMarkerReplacementDoesNotTruncateOldFile(t *testing.T) {
	p := filepath.Join(t.TempDir(), "cell.db")
	marker := pagedMarkerPath(p)
	if err := writePagedMarker(p, 4096, 10, 100, true); err != nil {
		t.Fatal(err)
	}
	old := marker + ".old"
	if err := os.Link(marker, old); err != nil {
		t.Fatal(err)
	}
	if err := writePagedMarker(p, 4096, 11, 101, false); err != nil {
		t.Fatal(err)
	}
	for path, want := range map[string]string{old: "4096 10 100 1", marker: "4096 11 101 0"} {
		got, err := os.ReadFile(path)
		if err != nil || string(got) != want {
			t.Errorf("%s = %q, %v; want %q", path, got, err, want)
		}
	}
}

func TestPagedMarkerPersistenceFailurePropagates(t *testing.T) {
	for _, operation := range []string{"Close", "SafeCheckpoint"} {
		t.Run(operation, func(t *testing.T) {
			s, err := New(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			c, err := s.Open(context.Background(), cell.Scope{Namespace: "test", Class: "__kv__", ID: "persist-error"})
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			c.paged = true
			if err := os.Mkdir(pagedMarkerPath(c.Path), 0o755); err != nil {
				t.Fatal(err)
			}
			if operation == "Close" {
				err = c.Close()
			} else {
				_, err = c.SafeCheckpoint(context.Background())
			}
			if !errors.Is(err, syscall.EISDIR) {
				t.Errorf("expected marker read error, got %v", err)
			}
		})
	}
}

func TestPagedMarkerRenameFailurePreservesFiles(t *testing.T) {
	p := filepath.Join(t.TempDir(), "cell.db")
	if err := os.WriteFile(p, []byte("original database"), 0o644); err != nil {
		t.Fatal(err)
	}
	marker := pagedMarkerPath(p)
	if err := os.Mkdir(marker, 0o755); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(marker)
	if err != nil {
		t.Fatal(err)
	}
	if err := writePagedMarker(p, 4096, 10, 100, false); err == nil {
		t.Fatal("marker replacement unexpectedly succeeded")
	}
	afterInfo, err := os.Stat(marker)
	if err != nil || !os.SameFile(info, afterInfo) {
		t.Fatalf("original marker changed: %v", err)
	}
	data, err := os.ReadFile(p)
	if err != nil || string(data) != "original database" {
		t.Fatalf("original DB changed: %q, %v", data, err)
	}
	leftovers, err := filepath.Glob(filepath.Join(filepath.Dir(p), ".paged-marker-*"))
	if err != nil || len(leftovers) != 0 {
		t.Fatalf("temporary markers remain: %v, %v", leftovers, err)
	}
}

func TestPagedMarkerDirtyWriteFailurePreventsPrepare(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	sc := cell.Scope{Namespace: "test", Class: "__kv__", ID: "dirty-error"}
	p, err := s.Path(sc)
	if err != nil {
		t.Fatal(err)
	}
	s.Paged = func(context.Context, cell.Scope, string) (pagedvfs.Source, bool, error) {
		// Force the initial dirty marker replacement to fail after the read.
		if err := os.Mkdir(pagedMarkerPath(p), 0o755); err != nil {
			t.Fatal(err)
		}
		return fakePagedSource{ps: 4096, commit: 10, txid: 100}, true, nil
	}
	s.Hydrate = func(context.Context, cell.Scope, string) error {
		t.Error("hydrate called after marker write failure")
		return nil
	}
	if c, err := s.Cell(context.Background(), sc); err == nil {
		_ = c.Close()
		t.Fatal("open succeeded despite dirty marker write failure")
	}
	if pagedvfs.IsPaged(p) {
		t.Error("paged VFS registered after marker write failure")
	}
	if _, err := os.Stat(p); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("DB created after marker write failure: %v", err)
	}
}

func TestPagedMarkerAbsentAndMalformedRemainDistinct(t *testing.T) {
	p := filepath.Join(t.TempDir(), "cell.db")
	if _, _, _, _, exists, valid, err := readPagedMarker(p); err != nil || exists || valid {
		t.Fatalf("absent marker: exists=%v valid=%v err=%v", exists, valid, err)
	}
	if err := os.WriteFile(pagedMarkerPath(p), []byte("not a marker"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, exists, valid, err := readPagedMarker(p); err != nil || !exists || valid {
		t.Fatalf("malformed marker: exists=%v valid=%v err=%v", exists, valid, err)
	}
}

func TestPagedMarkerInvalidTXIDIsNotClean(t *testing.T) {
	for _, txid := range []string{"invalid", "-1", "18446744073709551616"} {
		t.Run(txid, func(t *testing.T) {
			p := filepath.Join(t.TempDir(), "cell.db")
			if err := os.WriteFile(pagedMarkerPath(p), []byte("4096 10 "+txid+" 1"), 0o644); err != nil {
				t.Fatal(err)
			}
			if _, _, _, clean, exists, valid, err := readPagedMarker(p); err != nil || !exists || valid || clean {
				t.Fatalf("invalid txid accepted: clean=%v exists=%v valid=%v err=%v", clean, exists, valid, err)
			}
		})
	}
}
