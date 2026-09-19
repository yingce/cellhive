package r2

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"

	"cellhive/internal/bucket"
)

func newStore(t *testing.T) *Store {
	t.Helper()
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatalf("bucket: %v", err)
	}
	return New(b)
}

func TestR2PutGetRangeDeleteList(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	etag, err := s.Put(ctx, "acme", "files", "a/b.txt", []byte("0123456789"))
	if err != nil || etag == "" {
		t.Fatalf("put = %q, %v", etag, err)
	}
	data, e2, err := s.Get(ctx, "acme", "files", "a/b.txt")
	if err != nil || string(data) != "0123456789" || e2 != etag {
		t.Fatalf("get = %q %q %v", data, e2, err)
	}
	part, _, err := s.GetRange(ctx, "acme", "files", "a/b.txt", 2, 3)
	if err != nil || string(part) != "234" {
		t.Fatalf("range = %q, %v", part, err)
	}
	if _, err := s.Put(ctx, "acme", "files", "a/c.txt", []byte("xyz")); err != nil {
		t.Fatalf("put2: %v", err)
	}
	objs, err := s.List(ctx, "acme", "files", "a/", 10)
	if err != nil || len(objs) != 2 {
		t.Fatalf("list = %+v, %v", objs, err)
	}
	if objs[0].Key != "a/b.txt" || objs[0].Size != 10 {
		t.Fatalf("list[0] = %+v", objs[0])
	}
	if err := s.Delete(ctx, "acme", "files", "a/b.txt"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, _, err := s.Get(ctx, "acme", "files", "a/b.txt"); !errors.Is(err, bucket.ErrNotFound) {
		t.Fatalf("get after delete = %v", err)
	}
	// Buckets are isolated by prefix.
	if objs, _ := s.List(ctx, "acme", "other", "", 10); len(objs) != 0 {
		t.Fatalf("other bucket list = %+v", objs)
	}
}

func TestR2KeySafety(t *testing.T) {
	for _, k := range []string{"../x", "/abs", "a/../../b"} {
		if _, err := Key("acme", "files", k); !errors.Is(err, ErrBadKey) {
			t.Fatalf("unsafe key %q accepted: %v", k, err)
		}
	}
	if _, err := Key("", "files", "k"); !errors.Is(err, ErrBadKey) {
		t.Fatalf("empty ns accepted")
	}
}

// TestR2Multipart covers the multipart lifecycle: staged parts are invisible to
// List, complete assembles them in order, and abort cleans up.
func TestR2Multipart(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	up, err := s.CreateMultipart("demo", "files", "big.bin")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if len(up) != 32 {
		t.Fatalf("upload id = %q", up)
	}
	p1, err := s.UploadPart(ctx, "demo", "files", "big.bin", up, 1, []byte("hello "))
	if err != nil {
		t.Fatalf("part 1: %v", err)
	}
	p2, err := s.UploadPart(ctx, "demo", "files", "big.bin", up, 2, []byte("world"))
	if err != nil {
		t.Fatalf("part 2: %v", err)
	}

	// Staged parts must not appear in the user-visible object list.
	objs, err := s.List(ctx, "demo", "files", "", 100)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	for _, o := range objs {
		if o.Key != "" {
			t.Fatalf("staged part leaked into list: %+v", o)
		}
	}

	// ETag mismatch fails closed, and the object is not written.
	if _, _, err := s.CompleteMultipart(ctx, "demo", "files", "big.bin", up, []Part{{PartNumber: 1, ETag: "bogus"}, {PartNumber: 2, ETag: p2}}); err == nil {
		t.Fatal("etag mismatch should fail")
	}
	if _, _, err := s.Get(ctx, "demo", "files", "big.bin"); !errors.Is(err, bucket.ErrNotFound) {
		t.Fatalf("object written despite failure: %v", err)
	}

	// A successful complete assembles in the given order and removes the parts.
	etag, size, err := s.CompleteMultipart(ctx, "demo", "files", "big.bin", up, []Part{{PartNumber: 1, ETag: p1}, {PartNumber: 2, ETag: p2}})
	if err != nil {
		t.Fatalf("complete: %v", err)
	}
	if size != 11 || etag == "" {
		t.Fatalf("complete = %q, %d", etag, size)
	}
	data, _, err := s.Get(ctx, "demo", "files", "big.bin")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(data) != "hello world" {
		t.Fatalf("assembled = %q", data)
	}
	if err := s.AbortMultipart(ctx, "demo", "files", "big.bin", up); err != nil {
		t.Fatalf("abort after complete: %v", err)
	}

	// Abort cancels a pending upload; a bad upload id is rejected.
	up2, _ := s.CreateMultipart("demo", "files", "other.bin")
	if _, err := s.UploadPart(ctx, "demo", "files", "other.bin", up2, 1, []byte("x")); err != nil {
		t.Fatalf("part: %v", err)
	}
	if err := s.AbortMultipart(ctx, "demo", "files", "other.bin", up2); err != nil {
		t.Fatalf("abort: %v", err)
	}
	if _, err := s.UploadPart(ctx, "demo", "files", "other.bin", "not-hex", 1, []byte("x")); err == nil {
		t.Fatal("bad upload id should be rejected")
	}
	if _, _, err := s.CompleteMultipart(ctx, "demo", "files", "other.bin", up2, nil); err == nil {
		t.Fatal("complete with no parts should fail")
	}
	if _, err := s.UploadPart(ctx, "demo", "files", "other.bin", up2, 0, []byte("x")); err == nil {
		t.Fatal("part number 0 should be rejected")
	}
}

// TestR2ListPaging covers cursor paging with sizes (ADR-145): pages are bounded,
// ordered, exclusive at the cursor, and report completion with an empty cursor.
func TestR2ListPaging(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	for _, k := range []string{"dir/a", "dir/b", "dir/c", "dir/d", "dir/e"} {
		if _, err := s.Put(ctx, "acme", "files", k, []byte(k)); err != nil {
			t.Fatalf("put %s: %v", k, err)
		}
	}
	// Another bucket must not leak into the page.
	if _, err := s.Put(ctx, "acme", "other", "dir/zz", []byte("zz")); err != nil {
		t.Fatal(err)
	}

	var all []string
	cursor := ""
	pages := 0
	for {
		objs, next, err := s.ListPage(ctx, "acme", "files", "dir/", cursor, 2)
		if err != nil {
			t.Fatalf("page %d: %v", pages, err)
		}
		pages++
		if len(objs) == 0 {
			t.Fatal("empty page before completion")
		}
		if len(objs) > 2 {
			t.Fatalf("page %d returned %d objects, want <= 2", pages, len(objs))
		}
		for _, o := range objs {
			if o.Size != int64(len(o.Key)) {
				t.Fatalf("size for %s = %d", o.Key, o.Size)
			}
			if o.ETag == "" {
				t.Fatalf("missing etag for %s", o.Key)
			}
			all = append(all, o.Key)
		}
		if next == "" {
			break
		}
		if next != objs[len(objs)-1].Key {
			t.Fatalf("cursor = %q, want last key %q", next, objs[len(objs)-1].Key)
		}
		cursor = next
		if pages > 5 {
			t.Fatal("paging did not terminate")
		}
	}
	want := []string{"dir/a", "dir/b", "dir/c", "dir/d", "dir/e"}
	if strings.Join(all, ",") != strings.Join(want, ",") {
		t.Fatalf("paged keys = %v, want %v", all, want)
	}
	if pages != 3 {
		t.Fatalf("pages = %d, want 3 (2+2+1)", pages)
	}
	// List stays the first page.
	objs, err := s.List(ctx, "acme", "files", "dir/", 2)
	if err != nil || len(objs) != 2 || objs[0].Key != "dir/a" {
		t.Fatalf("List = %+v, %v", objs, err)
	}
}

func TestR2MetadataStatAndChecksums(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	md := &Metadata{
		HTTP:   map[string]string{"contentType": "text/plain"},
		Custom: map[string]string{"who": "bob"},
	}
	etag, err := s.PutWithMeta(ctx, "acme", "files", "m.txt", []byte("hello"), md)
	if err != nil || etag == "" {
		t.Fatalf("put meta = %q, %v", etag, err)
	}
	got, err := s.GetMeta(ctx, "acme", "files", "m.txt")
	if err != nil || got == nil {
		t.Fatalf("get meta = %+v, %v", got, err)
	}
	if got.HTTP["contentType"] != "text/plain" || got.Custom["who"] != "bob" || got.Size != 5 || got.ETag != etag || got.UploadedMs == 0 {
		t.Fatalf("meta = %+v", got)
	}
	size, statETag, err := s.Stat(ctx, "acme", "files", "m.txt")
	if err != nil || size != 5 || statETag != etag {
		t.Fatalf("stat = %d %q %v", size, statETag, err)
	}
	// Checksums are verified before the write.
	good := sha256.Sum256([]byte("hello"))
	if _, err := s.PutWithMeta(ctx, "acme", "files", "m.txt", []byte("hello"), &Metadata{SHA256: hex.EncodeToString(good[:])}); err != nil {
		t.Fatalf("sha256 match: %v", err)
	}
	if _, err := s.PutWithMeta(ctx, "acme", "files", "m.txt", []byte("hello"), &Metadata{MD5: "deadbeef"}); err == nil {
		t.Fatal("md5 mismatch accepted")
	}
	// Delete removes the sidecar too.
	if err := s.Delete(ctx, "acme", "files", "m.txt"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if m2, _ := s.GetMeta(ctx, "acme", "files", "m.txt"); m2 != nil {
		t.Fatalf("sidecar survived delete: %+v", m2)
	}
	// An object without metadata returns (nil, nil).
	if m3, err := s.GetMeta(ctx, "acme", "files", "missing"); err != nil || m3 != nil {
		t.Fatalf("missing meta = %+v, %v", m3, err)
	}
}

func TestListPageDelimited(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	for _, k := range []string{"a/1", "a/2/x", "b/1", "c"} {
		if _, err := s.Put(ctx, "acme", "files", k, []byte("v")); err != nil {
			t.Fatalf("put %s: %v", k, err)
		}
	}
	objs, prefixes, next, err := s.ListPageDelimited(ctx, "acme", "files", "", "/", "", 100)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if next != "" {
		t.Fatalf("next = %q, want exhausted", next)
	}
	if len(objs) != 1 || objs[0].Key != "c" {
		t.Fatalf("objects = %+v, want only c", objs)
	}
	if strings.Join(prefixes, ",") != "a/,b/" {
		t.Fatalf("prefixes = %v, want [a/ b/]", prefixes)
	}
	// A prefix narrows the listing; deeper delimiters still roll up.
	objs, prefixes, _, err = s.ListPageDelimited(ctx, "acme", "files", "a/", "/", "", 100)
	if err != nil || len(objs) != 1 || objs[0].Key != "a/1" || strings.Join(prefixes, ",") != "a/2/" {
		t.Fatalf("prefix a/ = %+v %v %v", objs, prefixes, err)
	}
	// A limit bounds the returned entries while the cursor resumes.
	objs, prefixes, next, err = s.ListPageDelimited(ctx, "acme", "files", "", "/", "", 2)
	if err != nil || len(objs)+len(prefixes) != 2 || next == "" {
		t.Fatalf("bounded = %d objs %d pref next=%q err=%v", len(objs), len(prefixes), next, err)
	}
}
