package dosupervisor

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/replica"
)

func hostScanFixture(t *testing.T) (*Supervisor, string) {
	t.Helper()
	dir := t.TempDir()
	root := filepath.Join(dir, "cellhive-do-host")
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	mkSQLiteWithRow(t, filepath.Join(root, "target.sqlite"), 1)
	mkSQLiteWithRow(t, filepath.Join(root, "target.1.sqlite"), 2)
	if err := os.WriteFile(filepath.Join(root, "target.facets"), facetsBytes("Tenant/one"), 0600); err != nil {
		t.Fatal(err)
	}
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	s := &Supervisor{Dir: dir, HostStorageDir: root, Epoch: 1, ScopePrefix: "workerd/__do__", Bucket: b, Committer: &bucketCommitter{rep: replica.New(b), epoch: 1}}
	s.Bind("target", "target", "storage", "Tenant")
	return s, root
}

func TestSyncHostDoesNotEnumerateOtherHosts(t *testing.T) {
	for _, others := range []int{0, 256} {
		t.Run(fmt.Sprint(others), func(t *testing.T) {
			s, root := hostScanFixture(t)
			for i := 0; i < others; i++ {
				if err := os.WriteFile(filepath.Join(root, fmt.Sprintf("other-%d.facets", i)), facetsBytes("Tenant/other"), 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(filepath.Join(s.Dir, fmt.Sprintf("other-%d", i)), 0700); err != nil {
					t.Fatal(err)
				}
			}
			fd, err := syscall.InotifyInit1(syscall.IN_NONBLOCK | syscall.IN_CLOEXEC)
			if err != nil {
				t.Fatal(err)
			}
			defer syscall.Close(fd)
			for _, dir := range []string{s.Dir, root} {
				if _, err := syscall.InotifyAddWatch(fd, dir, syscall.IN_OPEN|syscall.IN_CLOSE_NOWRITE); err != nil {
					t.Fatal(err)
				}
			}
			if n, err := s.SyncHost(t.Context(), "target"); err != nil || n != 2 {
				t.Fatalf("sync=%d %v", n, err)
			}
			buf := make([]byte, 65536)
			dirs, foreign, facetReads := 0, 0, 0
			for {
				n, err := syscall.Read(fd, buf)
				if errors.Is(err, syscall.EAGAIN) {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				for off := 0; off < n; {
					mask := binary.NativeEndian.Uint32(buf[off+4:])
					length := int(binary.NativeEndian.Uint32(buf[off+12:]))
					if mask&syscall.IN_Q_OVERFLOW != 0 {
						t.Fatal("inotify overflow")
					}
					if mask&syscall.IN_OPEN != 0 && mask&syscall.IN_ISDIR != 0 {
						dirs++
					}
					if mask&syscall.IN_OPEN != 0 && length >= 6 && string(buf[off+16:off+22]) == "other-" {
						foreign++
					}
					if mask&syscall.IN_OPEN != 0 && strings.TrimRight(string(buf[off+16:off+16+length]), "\x00") == "target.facets" {
						facetReads++
					}
					off += 16 + length
				}
			}
			if dirs != 0 || foreign != 0 {
				t.Fatalf("host gate enumerated %d directories and opened %d foreign files (other hosts=%d)", dirs, foreign, others)
			}
			if facetReads != 1 {
				t.Fatalf("facet index opened %d times, want one snapshot", facetReads)
			}
			t.Logf("other hosts=%d directory opens=%d foreign opens=%d facet reads=%d", others, dirs, foreign, facetReads)
		})
	}
}

func TestSyncHostFreshFacetsAndRetry(t *testing.T) {
	s, root := hostScanFixture(t)
	if _, err := s.SyncHost(t.Context(), "target"); err != nil {
		t.Fatal(err)
	}
	facets := filepath.Join(root, "target.facets")
	if err := os.WriteFile(facets, []byte("unknown layout"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SyncHost(t.Context(), "target"); err == nil {
		t.Fatal("corrupt facet table acknowledged")
	}
	mkSQLiteWithRow(t, filepath.Join(root, "target.2.sqlite"), 42)
	if err := os.WriteFile(facets, facetsBytes("Tenant/one", "Tenant/two"), 0600); err != nil {
		t.Fatal(err)
	}
	if n, err := s.SyncHost(t.Context(), "target"); err != nil || n != 3 {
		t.Fatalf("retry=%d %v", n, err)
	}
	out := t.TempDir()
	if _, err := s.RestoreHost(t.Context(), out, "target"); err != nil {
		t.Fatal(err)
	}
	if got := queryRow(t, filepath.Join(out, "cellhive-do-host", "target.2.sqlite")); got != 42 {
		t.Fatalf("new facet value=%d", got)
	}
}

func TestSyncHostCapturesNewFacetImmediately(t *testing.T) {
	s, root := hostScanFixture(t)
	if _, err := s.SyncHost(t.Context(), "target"); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(root, "target.facets"))
	if err != nil {
		t.Fatal(err)
	}
	mkSQLiteWithRow(t, filepath.Join(root, "target.2.sqlite"), 42)
	if err := os.WriteFile(filepath.Join(root, "target.facets"), facetsBytes("Tenant/one", "Tenant/two"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(filepath.Join(root, "target.facets"), info.ModTime(), info.ModTime()); err != nil {
		t.Fatal(err)
	}
	if n, err := s.SyncHost(t.Context(), "target"); err != nil || n != 3 {
		t.Fatalf("new facet gate=%d %v", n, err)
	}
	out := t.TempDir()
	if _, err := s.RestoreHost(t.Context(), out, "target"); err != nil {
		t.Fatal(err)
	}
	if got := queryRow(t, filepath.Join(out, "cellhive-do-host", "target.2.sqlite")); got != 42 {
		t.Fatalf("new facet value=%d", got)
	}
}

type hostScanCommitter struct {
	Committer
	once  sync.Once
	after func()
}

func (c *hostScanCommitter) Commit(ctx context.Context, scope cell.Scope, epoch uint64, segment []byte) error {
	if err := c.Committer.Commit(ctx, scope, epoch, segment); err != nil {
		return err
	}
	c.once.Do(c.after)
	return nil
}

func TestSyncHostManifestUsesCapturedSnapshot(t *testing.T) {
	s, root := hostScanFixture(t)
	path := filepath.Join(root, "target.facets")
	s.Committer = &hostScanCommitter{Committer: s.Committer, after: func() {
		// A later generation must not leak into this capture's sidecar/manifest.
		if err := os.WriteFile(path, facetsBytes("Tenant/one", "Tenant/two"), 0600); err != nil {
			t.Error(err)
		}
	}}
	if _, err := s.SyncHost(t.Context(), "target"); err != nil {
		t.Fatal(err)
	}
	doc, err := s.readManifestDocKey(t.Context(), s.hostManifestKey("target"))
	if err != nil {
		t.Fatal(err)
	}
	if names := doc.Facets["target"]; len(names) != 1 || names[0] != "Tenant/one" {
		t.Fatalf("manifest names=%v", names)
	}
	out := t.TempDir()
	if _, err := s.RestoreHost(t.Context(), out, "target"); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(out, "cellhive-do-host", "target.facets"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, facetsBytes("Tenant/one")) {
		t.Fatal("restored sidecar differs from captured snapshot")
	}
}

func TestHostScanLegacyAndSyncAllKeepSidecars(t *testing.T) {
	for _, all := range []bool{false, true} {
		t.Run(fmt.Sprint(all), func(t *testing.T) {
			s, root := hostScanFixture(t)
			if err := os.WriteFile(filepath.Join(root, "target.custom"), []byte("sidecar"), 0600); err != nil {
				t.Fatal(err)
			}
			var err error
			if all {
				_, err = s.SyncAll(t.Context())
			} else {
				s.HostStorageDir = ""
				_, err = s.SyncHost(t.Context(), "target")
			}
			if err != nil {
				t.Fatal(err)
			}
			out := t.TempDir()
			if _, err := s.RestoreHost(t.Context(), out, "target"); err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(filepath.Join(out, "cellhive-do-host", "target.custom"))
			if err != nil || string(data) != "sidecar" {
				t.Fatalf("legacy sidecar=%q %v", data, err)
			}
		})
	}
}

func TestHostScanRejectsMissingOrUnreadableMetadata(t *testing.T) {
	for _, kind := range []string{"missing", "directory", "unknown-root", "truncated"} {
		t.Run(kind, func(t *testing.T) {
			s, root := hostScanFixture(t)
			path := filepath.Join(root, "target.facets")
			switch kind {
			case "missing", "directory":
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				if kind == "directory" {
					if err := os.Mkdir(path, 0700); err != nil {
						t.Fatal(err)
					}
				}
			case "unknown-root":
				s.HostStorageDir = filepath.Join(s.Dir, "unknown")
			case "truncated":
				if err := os.WriteFile(path, facetsBytes("Tenant/one")[:10], 0600); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := s.SyncHost(t.Context(), "target"); err == nil {
				t.Fatal("invalid layout acknowledged")
			}
		})
	}
}

func TestSyncHostRetainsFailedCapture(t *testing.T) {
	s, root := hostScanFixture(t)
	if err := os.Remove(filepath.Join(root, "target.sqlite")); err != nil {
		t.Fatal(err)
	}
	fc := &failingCapture{bucketCommitter: s.Committer.(*bucketCommitter), fail: true, persist: true}
	s.Committer = fc
	if _, err := s.SyncHost(t.Context(), "target"); err == nil {
		t.Fatal("failed capture acknowledged")
	}
	if _, err := s.SyncHost(t.Context(), "target"); err != nil {
		t.Fatal(err)
	}
	if !fc.replayed {
		t.Fatal("pending capture not replayed exactly")
	}
	out := t.TempDir()
	if _, err := s.RestoreHost(t.Context(), out, "target"); err != nil {
		t.Fatal(err)
	}
	if got := queryRow(t, filepath.Join(out, "cellhive-do-host", "target.1.sqlite")); got != 2 {
		t.Fatalf("retried facet value=%d", got)
	}
}

func TestSyncHostRejectsAmbiguousRoots(t *testing.T) {
	s, _ := hostScanFixture(t)
	mkSQLiteWithRow(t, filepath.Join(s.Dir, "target.sqlite"), 99)
	if _, err := s.SyncHost(t.Context(), "target"); err == nil {
		t.Fatal("duplicate host in root and namespace directory acknowledged")
	}
}

func TestSyncHostRejectsUnsupportedFacetMetadata(t *testing.T) {
	for _, kind := range []string{"nested-parent", "empty-name", "duplicate-name"} {
		t.Run(kind, func(t *testing.T) {
			s, root := hostScanFixture(t)
			data := facetsBytes("Tenant/one")
			switch kind {
			case "nested-parent":
				data[8] = 1
			case "empty-name":
				data = facetsBytes("")
			case "duplicate-name":
				data = facetsBytes("Tenant/one", "Tenant/one")
			}
			if err := os.WriteFile(filepath.Join(root, "target.facets"), data, 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := s.SyncHost(t.Context(), "target"); err == nil {
				t.Fatal("unsupported facet metadata acknowledged")
			}
		})
	}
}
