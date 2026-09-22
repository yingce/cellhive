package artifacts

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

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

func TestBundleContentAddressed(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	data := []byte("export default { fetch(){} }")
	sha, size, err := s.PutBundle(ctx, data)
	if err != nil || sha != Hash(data) || size != len(data) {
		t.Fatalf("put bundle = %q %d %v", sha, size, err)
	}
	// Same bytes again is a dedupe no-op.
	sha2, _, err := s.PutBundle(ctx, data)
	if err != nil || sha2 != sha {
		t.Fatalf("dedupe = %q, %v", sha2, err)
	}
	got, err := s.GetBundle(ctx, sha)
	if err != nil || string(got) != string(data) {
		t.Fatalf("get bundle = %q, %v", got, err)
	}
	// The key is content-addressed.
	if k := BundleKey(sha); k != "bundles/sha256/"+sha[:2]+"/"+sha {
		t.Fatalf("bundle key = %q", k)
	}
}

func TestBundleSizeUsesPointLookup(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	data := []byte("bundle-size-without-list")
	sha, _, err := s.PutBundle(ctx, data)
	if err != nil {
		t.Fatal(err)
	}
	size, err := s.BundleSize(ctx, sha)
	if err != nil || size != int64(len(data)) {
		t.Fatalf("BundleSize() = %d, %v", size, err)
	}
	if _, err := s.BundleSize(ctx, "bad"); err == nil {
		t.Fatal("BundleSize(bad sha) error = nil")
	}
	if _, err := s.BundleSize(ctx, strings.Repeat("0", 64)); !errors.Is(err, bucket.ErrNotFound) {
		t.Fatalf("BundleSize(missing) error = %v", err)
	}
}

func TestAssetAddressingAndSafety(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	token, key, err := s.PutAsset(ctx, "acme", "api", "index.html", []byte("<html>"))
	if err != nil || token != Hash([]byte("<html>")) {
		t.Fatalf("put asset = %q %q %v", token, key, err)
	}
	if key != "assets/acme/api/"+token+"/index.html" {
		t.Fatalf("asset key = %q", key)
	}
	got, err := s.GetAsset(ctx, "acme", "api", token, "index.html")
	if err != nil || string(got) != "<html>" {
		t.Fatalf("get asset = %q, %v", got, err)
	}
	// Unsafe paths are rejected.
	for _, p := range []string{"../secret", "/abs", "", "a/../../b"} {
		if _, _, err := s.PutAsset(ctx, "acme", "api", p, []byte("x")); !errors.Is(err, ErrBadPath) {
			t.Fatalf("unsafe path %q accepted: %v", p, err)
		}
	}
	if _, err := AssetKey("acme", "api", token, "ok/fine.txt"); err != nil {
		t.Fatalf("safe nested path rejected: %v", err)
	}
}

func TestPresignBundle(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	sha, _, _ := s.PutBundle(ctx, []byte("b"))
	// FSBucket implements PresignGet as a local file path; just assert no error.
	if _, err := s.PresignBundle(ctx, sha, time.Minute); err != nil {
		t.Fatalf("presign: %v", err)
	}
	if _, err := s.PresignBundle(ctx, "zz", time.Minute); err == nil {
		// A short sha yields a bad key; PresignGet may still error or succeed, but
		// the guard must not panic. Accept either.
		_ = err
	}
}

func TestHashMatchesStdlib(t *testing.T) {
	if Hash([]byte("abc")) != "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad" {
		t.Fatalf("sha mismatch: %s", Hash([]byte("abc")))
	}
}

// TestListAndDeleteBundles covers the GC cold path (ADR-110).
func TestListAndDeleteBundles(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	shaA, _, err := s.PutBundle(ctx, []byte("a"))
	if err != nil {
		t.Fatalf("put a: %v", err)
	}
	shaB, _, err := s.PutBundle(ctx, []byte("b"))
	if err != nil {
		t.Fatalf("put b: %v", err)
	}
	shas, err := s.ListBundles(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(shas) != 2 {
		t.Fatalf("list = %v, want 2", shas)
	}
	found := map[string]bool{}
	for _, s := range shas {
		found[s] = true
	}
	if !found[shaA] || !found[shaB] {
		t.Fatalf("listed %v, want %s and %s", shas, shaA, shaB)
	}
	if err := s.DeleteBundle(ctx, shaA); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := s.GetBundle(ctx, shaA); !errors.Is(err, bucket.ErrNotFound) {
		t.Fatalf("get deleted = %v, want ErrNotFound", err)
	}
	// Deleting a missing bundle is not an error.
	if err := s.DeleteBundle(ctx, shaA); err != nil {
		t.Fatalf("second delete: %v", err)
	}
	if err := s.DeleteBundle(ctx, "not-a-sha"); err == nil {
		t.Fatal("delete with a bad sha should fail")
	}
}

// TestListAndDeleteAssets covers the assets GC cold path (ADR-111).
func TestListAndDeleteAssets(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	// Two files of one version token, plus another version.
	if _, err := s.PutAssetAt(ctx, "acme", "w", "tok1", "index.html", []byte("<h1>1")); err != nil {
		t.Fatalf("put tok1 index: %v", err)
	}
	if _, err := s.PutAssetAt(ctx, "acme", "w", "tok1", "app.js", []byte("x")); err != nil {
		t.Fatalf("put tok1 app: %v", err)
	}
	if _, err := s.PutAssetAt(ctx, "acme", "w", "tok2", "index.html", []byte("<h1>2")); err != nil {
		t.Fatalf("put tok2: %v", err)
	}
	ids, err := s.ListAssets(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(ids) != 2 {
		t.Fatalf("list = %v, want 2 ids (one per token)", ids)
	}
	// Delete tok1 removes both of its files, not tok2's.
	if err := s.DeleteAsset(ctx, "acme/w/tok1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := s.GetAsset(ctx, "acme", "w", "tok1", "index.html"); err == nil {
		t.Fatal("tok1 index still present")
	}
	if _, err := s.GetAsset(ctx, "acme", "w", "tok1", "app.js"); err == nil {
		t.Fatal("tok1 app still present")
	}
	if _, err := s.GetAsset(ctx, "acme", "w", "tok2", "index.html"); err != nil {
		t.Fatalf("tok2 should survive: %v", err)
	}
	// Deleting a missing version is not an error; a bad id is.
	if err := s.DeleteAsset(ctx, "acme/w/tok1"); err != nil {
		t.Fatalf("second delete: %v", err)
	}
	if err := s.DeleteAsset(ctx, "../bad"); err == nil {
		t.Fatal("bad asset id should be rejected")
	}
	if err := s.DeleteAsset(ctx, "acme/w"); err == nil {
		t.Fatal("short asset id should be rejected")
	}
}
