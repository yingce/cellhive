package r2

import (
	"context"
	"testing"
)

// TestStatsBoundedListing covers the ADR-157 R2 stats: byte/object totals under
// a prefix, the truncation contract, and multipart staging counts.
func TestStatsBoundedListing(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	for _, k := range []string{"a/1", "a/2", "a/3", "b/1"} {
		if _, err := s.Put(ctx, "acme", "media", k, []byte("0123456789")); err != nil {
			t.Fatalf("put %s: %v", k, err)
		}
	}
	// An in-flight multipart upload with two parts (staging area, opaque to List).
	id, err := s.CreateMultipart(ctx, "acme", "media", "big")
	if err != nil {
		t.Fatalf("create mpu: %v", err)
	}
	if _, err := s.UploadPart(ctx, "acme", "media", "big", id, 1, []byte("p1")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.UploadPart(ctx, "acme", "media", "big", id, 2, []byte("p2")); err != nil {
		t.Fatal(err)
	}

	st, err := s.Stats(ctx, "acme", "media", StatsOptions{})
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if st.Objects != 4 || st.Bytes != 40 {
		t.Fatalf("objects/bytes = %d/%d, want 4/40", st.Objects, st.Bytes)
	}
	if st.Truncated {
		t.Fatalf("unexpected truncation: %+v", st)
	}
	if st.MultipartUploads != 1 || st.MultipartParts != 2 {
		t.Fatalf("multipart = %d uploads/%d parts, want 1/2", st.MultipartUploads, st.MultipartParts)
	}
	// The staging area must not leak into the user listing.
	if objs, _ := s.List(ctx, "acme", "media", "", 100); len(objs) != 4 {
		t.Fatalf("user objects = %d, want 4 (multipart hidden)", len(objs))
	}

	// Prefix scoping.
	pre, err := s.Stats(ctx, "acme", "media", StatsOptions{Prefix: "a/"})
	if err != nil {
		t.Fatalf("stats prefix: %v", err)
	}
	if pre.Objects != 3 || pre.Bytes != 30 {
		t.Fatalf("prefix stats = %d/%d, want 3/30", pre.Objects, pre.Bytes)
	}

	// Limit truncates and reports a resumable cursor.
	lim, err := s.Stats(ctx, "acme", "media", StatsOptions{Prefix: "a/", Limit: 2})
	if err != nil {
		t.Fatalf("stats limit: %v", err)
	}
	if lim.Objects != 2 || !lim.Truncated || lim.Cursor == "" {
		t.Fatalf("limited stats = %+v, want 2 objects, truncated with cursor", lim)
	}
	if lim.Cursor != "a/2" {
		t.Fatalf("cursor = %q, want a/2", lim.Cursor)
	}

	// Bad bucket names are rejected like every other R2 call.
	if _, err := s.Stats(ctx, "acme", "../evil", StatsOptions{}); err == nil {
		t.Fatal("unsafe bucket name accepted")
	}
}
