package replica

import (
	"bytes"
	"errors"
	"io"
	"os"
	"testing"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
)

func cacheTestManager(t *testing.T) (*Manager, *bucket.FSBucket) {
	t.Helper()
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := New(b)
	close, err := m.EnableLocalLTXCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(close)
	return m, b
}

func TestLocalCacheChecksRemoteVersionAndSize(t *testing.T) {
	for _, change := range []string{"version", "size", "missing", "local-size", "local-missing"} {
		t.Run(change, func(t *testing.T) {
			m, b := cacheTestManager(t)
			key := "cells/n/c/id/ltx/e1/0000/test.ltx"
			raw := []byte("original")
			etag, err := b.Put(t.Context(), key, raw)
			if err != nil {
				t.Fatal(err)
			}
			m.localCache.remember(key, etag, bytes.NewReader(raw), int64(len(raw)))
			r, n, tag, hit, err := m.openCachedNative(t.Context(), key)
			if err != nil || !hit || n != int64(len(raw)) || tag != etag {
				t.Fatalf("initial hit %v %v", hit, err)
			}
			got, _ := io.ReadAll(r)
			r.Close()
			if !bytes.Equal(got, raw) {
				t.Fatal("wrong bytes")
			}
			switch change {
			case "version":
				_, err = b.Put(t.Context(), key, []byte("replaced"))
			case "size":
				_, err = b.Put(t.Context(), key, []byte("short"))
			case "missing":
				err = b.Delete(t.Context(), key)
			case "local-size":
				err = os.WriteFile(m.localCache.entries[key].path, []byte("short"), 0600)
			case "local-missing":
				err = os.Remove(m.localCache.entries[key].path)
			}
			if err != nil {
				t.Fatal(err)
			}
			r, _, _, hit, err = m.openCachedNative(t.Context(), key)
			if hit || r != nil {
				t.Fatal("stale cache hit")
			}
			if change == "missing" && !errors.Is(err, bucket.ErrNotFound) {
				t.Fatalf("missing: %v", err)
			}
			if change != "missing" && err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestLocalCacheEvictionAndPinnedBudget(t *testing.T) {
	m, b := cacheTestManager(t)
	c := m.localCache
	c.maxBytes = 8
	c.maxEntries = 2
	put := func(key string) {
		tag, err := b.Put(t.Context(), key, []byte("1234"))
		if err != nil {
			t.Fatal(err)
		}
		c.remember(key, tag, bytes.NewReader([]byte("1234")), 4)
	}
	a, bkey, d := "cells/n/c/id/ltx/a", "cells/n/c/id/ltx/b", "cells/n/c/id/ltx/d"
	put(a)
	put(bkey)
	r, _, _, hit, err := m.openCachedNative(t.Context(), a)
	if err != nil || !hit {
		t.Fatal(err)
	}
	put(d)
	if c.entries[bkey] != nil || c.entries[a] == nil || c.bytes != 8 {
		t.Fatal("eviction/pin accounting")
	}
	s, _, _, hit, err := m.openCachedNative(t.Context(), d)
	if err != nil || !hit {
		t.Fatal(err)
	}
	put(bkey)
	if c.entries[bkey] != nil || c.bytes != 8 {
		t.Fatal("exceeded pinned budget")
	}
	r.Close()
	s.Close()
	put(bkey)
	if c.entries[bkey] == nil || c.bytes != 8 {
		t.Fatal("failed eviction after close")
	}
	c.remember("oversize", "tag", bytes.NewReader(make([]byte, 9)), 9)
	if c.entries["oversize"] != nil {
		t.Fatal("oversize cached")
	}
	c.maxBytes = 100
	put("cells/n/c/id/ltx/e")
	if len(c.entries) != 2 {
		t.Fatal("entry limit")
	}
}

func TestLocalCacheRetryNeverReturnsStaleBytes(t *testing.T) {
	for _, stream := range []bool{false, true} {
		for _, initiallyCached := range []bool{false, true} {
			m, b := cacheTestManager(t)
			sc := cell.Scope{Namespace: "n", Class: "c", ID: "retry"}
			key := Prefix(sc, 1) + "0009/test.ltx"
			first := retryFixture(t, 1000, true, 7)
			retry := retryFixture(t, 2000, true, 7)
			original, err := m.uploadSegment(t.Context(), sc, 1, key, first)
			if err != nil {
				t.Fatal(err)
			}
			if initiallyCached {
				m.localCache.remember(key, original, bytes.NewReader(first), int64(len(first)))
			}

			var tag string
			if stream {
				f, err := os.CreateTemp(t.TempDir(), "spool")
				if err != nil {
					t.Fatal(err)
				}
				defer f.Close()
				if _, err = f.Write(retry); err != nil {
					t.Fatal(err)
				}
				f.Seek(0, 0)
				tag, err = m.uploadFile(t.Context(), sc, 1, key, f, int64(len(retry)))
			} else {
				tag, err = m.uploadSegment(t.Context(), sc, 1, key, retry)
			}
			if err != nil || tag == original {
				t.Fatalf("retry %s %v", tag, err)
			}
			r, _, _, hit, err := m.openCachedNative(t.Context(), key)
			if err != nil || hit != (stream && !initiallyCached) {
				t.Fatalf("cache %v %v", hit, err)
			}
			if hit {
				got, _ := io.ReadAll(r)
				r.Close()
				if !bytes.Equal(got, retry) {
					t.Fatal("stale cache returned")
				}
			}
			got, _, err := b.Get(t.Context(), key)
			if err != nil || !bytes.Equal(got, retry) {
				t.Fatal("remote overwrite missing")
			}
		}
	}
}

func TestLocalCacheFilePublicationCopiesCallerSpool(t *testing.T) {
	m, _ := cacheTestManager(t)
	sc := cell.Scope{Namespace: "n", Class: "c", ID: "file"}
	key := Prefix(sc, 1) + "0009/test.ltx"
	raw := retryFixture(t, 1000, true, 7)
	f, err := os.CreateTemp(t.TempDir(), "spool")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err = f.Write(raw); err != nil {
		t.Fatal(err)
	}
	f.Seek(0, 0)
	if _, err = m.uploadFile(t.Context(), sc, 1, key, f, int64(len(raw))); err != nil {
		t.Fatal(err)
	}
	if _, err = f.WriteAt(bytes.Repeat([]byte{0}, len(raw)), 0); err != nil {
		t.Fatal(err)
	}
	r, _, _, hit, err := m.openCachedNative(t.Context(), key)
	if err != nil || !hit {
		t.Fatalf("cache %v %v", hit, err)
	}
	got, err := io.ReadAll(r)
	r.Close()
	if err != nil || !bytes.Equal(got, raw) {
		t.Fatal("caller changed cached copy")
	}
}

func TestLocalCacheRejectedPublicationIsNotRemembered(t *testing.T) {
	m, _ := cacheTestManager(t)
	sc := cell.Scope{Namespace: "n", Class: "__kv__", ID: "blocked"}
	if err := m.BlockResource(t.Context(), sc, "delete-job"); err != nil {
		t.Fatal(err)
	}
	key := Prefix(sc, 1) + "0009/test.ltx"
	_, err := m.uploadSegment(t.Context(), sc, 1, key, retryFixture(t, 1000, true, 7))
	if !errors.Is(err, ErrResourceDeleted) {
		t.Fatalf("publication: %v", err)
	}
	if len(m.localCache.entries) != 0 {
		t.Fatal("rejected publication cached")
	}
}

func TestNativeStreamUsesLocalCacheAfterRemoteVersionCheck(t *testing.T) {
	m, b := cacheTestManager(t)
	s := cell.Scope{Namespace: "n", Class: "c", ID: "cached-plan"}
	raw := nativeBlob(t, 1, 5, 7)
	f, err := os.CreateTemp(t.TempDir(), "snapshot")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.Write(raw); err != nil {
		t.Fatal(err)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
	key, _, err := m.AppendSnapshotFile(t.Context(), s, 1, f.Name(), int64(len(raw)))
	if err != nil {
		t.Fatal(err)
	}
	// This backend rejects remote streaming; successful consumption proves that
	// the public plan opener actually uses the version-checked local file.
	m.B = &rangeOnlyProofBucket{FSBucket: b}
	readers, _, close, err := m.OpenNativeFiles(t.Context(), []NativeFileRef{{Key: key, MinTXID: 1, MaxTXID: 5}})
	if err != nil {
		t.Fatal(err)
	}
	defer close()
	got, err := io.ReadAll(readers[0])
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, raw) {
		t.Fatal("cached plan content changed")
	}
}

func TestForegroundDeltaPublicationDoesNotPopulateDiskCache(t *testing.T) {
	m, _ := cacheTestManager(t)
	s := cell.Scope{Namespace: "n", Class: "c", ID: "foreground"}
	if _, _, err := m.Append(t.Context(), s, 1, nativeBlob(t, 1, 5, 7)); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(m.localCache.dir)
	if err != nil || len(entries) != 0 {
		t.Fatalf("foreground publication added cache IO: %v %v", entries, err)
	}
}
