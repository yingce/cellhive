// Package artifacts stores content-addressed worker bundles and versioned assets
// in the object store (ADR-030). Bundles are deduplicated by SHA-256; assets are
// addressed by (ns, worker, content-hash, path). Runtimes obtain short-lived
// presigned URLs from cell-agent and read bytes directly from object storage;
// authenticated point reads remain as the local-filesystem/unsupported-presign fallback.
package artifacts

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/objectstore"
)

// ErrBadPath means a path is unsafe (traversal/absolute/empty).
var ErrBadPath = errors.New("artifacts: unsafe path")

// Store reads and writes artifacts in a bucket.
type Store struct {
	B bucket.Bucket
}

// New creates an artifact store.
func New(b bucket.Bucket) *Store { return &Store{B: b} }

// Hash returns the hex SHA-256 of data.
func Hash(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// BundleKey returns the content-addressed key for a bundle hash.
func BundleKey(sha string) string {
	if len(sha) < 2 {
		return ""
	}
	return "bundles/sha256/" + sha[:2] + "/" + sha
}

// AssetKey returns the key for an asset under a worker's content token.
func AssetKey(ns, worker, token, path string) (string, error) {
	if !safeComponent(ns) || !safeComponent(worker) || !safeComponent(token) {
		return "", ErrBadPath
	}
	if !safePath(path) {
		return "", ErrBadPath
	}
	return fmt.Sprintf("assets/%s/%s/%s/%s", ns, worker, token, strings.TrimPrefix(path, "/")), nil
}

func safeComponent(s string) bool {
	return s != "" && !strings.ContainsAny(s, "/\\")
}

func safePath(p string) bool {
	p = strings.TrimSpace(p)
	if p == "" || strings.HasPrefix(p, "/") || strings.Contains(p, "..") {
		return false
	}
	return true
}

// PutBundle stores a bundle if absent and returns its hash and size. Re-uploading
// identical bytes is a no-op (content-addressed dedup).
func (s *Store) PutBundle(ctx context.Context, data []byte) (sha string, size int, err error) {
	sha = Hash(data)
	key := BundleKey(sha)
	if _, err := s.B.ConditionalCreate(ctx, key, data); err != nil {
		if errors.Is(err, bucket.ErrPrecondition) {
			return sha, len(data), nil // already present
		}
		return "", 0, err
	}
	return sha, len(data), nil
}

// ListBundles returns the SHA of every stored bundle (GC/operator cold path).
func (s *Store) ListBundles(ctx context.Context) ([]string, error) {
	keys, err := objectstore.NewOwned(s.B, objectstore.PrefixBundles, objectstore.OwnerArtifacts).List(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		if i := strings.LastIndexByte(k, '/'); i >= 0 && i+1 < len(k) {
			out = append(out, k[i+1:])
		}
	}
	return out, nil
}

// DeleteBundle removes a bundle by SHA (GC cold path). Missing key is not an
// error. The SHA must be a full lowercase hex SHA-256 so the deletion is never
// broader than one bundle.
func (s *Store) DeleteBundle(ctx context.Context, sha string) error {
	if !validSHA256(sha) {
		return fmt.Errorf("artifacts: bad sha")
	}
	return objectstore.NewOwned(s.B, objectstore.PrefixBundles, objectstore.OwnerArtifacts).Delete(ctx, BundleKey(sha))
}

// validSHA256 reports whether s is a full lowercase hex SHA-256 digest.
func validSHA256(s string) bool {
	if len(s) != 64 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// ListAssets returns one id ("<ns>/<worker>/<token>") per stored asset version
// (GC/operator cold path).
func (s *Store) ListAssets(ctx context.Context) ([]string, error) {
	keys, err := objectstore.NewOwned(s.B, objectstore.PrefixAssets, objectstore.OwnerArtifacts).List(ctx)
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	out := []string{}
	for _, k := range keys {
		parts := strings.SplitN(strings.TrimPrefix(k, objectstore.PrefixAssets), "/", 4)
		if len(parts) < 4 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
			continue
		}
		id := parts[0] + "/" + parts[1] + "/" + parts[2]
		if !seen[id] {
			seen[id] = true
			out = append(out, id)
		}
	}
	return out, nil
}

// DeleteAsset removes every file of one asset version id
// ("<ns>/<worker>/<token>"). Missing keys are not an error; the id is validated
// so the deletion is never broader than one version.
func (s *Store) DeleteAsset(ctx context.Context, id string) error {
	parts := strings.Split(id, "/")
	if len(parts) != 3 || !safeComponent(parts[0]) || !safeComponent(parts[1]) || !safeComponent(parts[2]) {
		return fmt.Errorf("artifacts: bad asset id %q", id)
	}
	st := objectstore.NewOwned(s.B, objectstore.PrefixAssets, objectstore.OwnerArtifacts)
	prefix := fmt.Sprintf("%s%s/%s/%s/", objectstore.PrefixAssets, parts[0], parts[1], parts[2])
	keys, err := st.ListPrefix(ctx, prefix)
	if err != nil {
		return err
	}
	for _, k := range keys {
		if err := st.Delete(ctx, k); err != nil {
			return err
		}
	}
	return nil
}

// BundleItems adapts the store to objgc.Items for worker bundles (ADR-110).
type BundleItems struct{ Store *Store }

// List returns every stored bundle SHA.
func (b BundleItems) List(ctx context.Context) ([]string, error) { return b.Store.ListBundles(ctx) }

// Delete removes one bundle by SHA.
func (b BundleItems) Delete(ctx context.Context, id string) error {
	return b.Store.DeleteBundle(ctx, id)
}

// AssetItems adapts the store to objgc.Items for asset versions (ADR-111).
type AssetItems struct{ Store *Store }

// List returns every stored asset-version id.
func (a AssetItems) List(ctx context.Context) ([]string, error) { return a.Store.ListAssets(ctx) }

// Delete removes every file of one asset version id.
func (a AssetItems) Delete(ctx context.Context, id string) error {
	return a.Store.DeleteAsset(ctx, id)
}

// GetBundle returns a bundle's bytes.
func (s *Store) GetBundle(ctx context.Context, sha string) ([]byte, error) {
	key := BundleKey(sha)
	if key == "" {
		return nil, fmt.Errorf("artifacts: bad sha")
	}
	return objectstore.NewOwned(s.B, objectstore.PrefixBundles, objectstore.OwnerArtifacts).Get(ctx, key)
}

// BundleSize returns the immutable bundle size through a point Stat. It never
// lists the bucket and never allocates the bundle body.
func (s *Store) BundleSize(ctx context.Context, sha string) (int64, error) {
	key := BundleKey(sha)
	if key == "" {
		return 0, fmt.Errorf("artifacts: bad sha")
	}
	size, _, err := objectstore.NewOwned(s.B, objectstore.PrefixBundles, objectstore.OwnerArtifacts).Stat(ctx, key)
	return size, err
}

// PresignBundle returns a short-lived read URL for a bundle.
func (s *Store) PresignBundle(ctx context.Context, sha string, ttl time.Duration) (string, error) {
	key := BundleKey(sha)
	if key == "" {
		return "", fmt.Errorf("artifacts: bad sha")
	}
	return objectstore.NewOwned(s.B, objectstore.PrefixBundles, objectstore.OwnerArtifacts).PresignGet(ctx, key, ttl)
}

// PutAsset stores an asset under its content hash and returns the token and key.
func (s *Store) PutAsset(ctx context.Context, ns, worker, path string, data []byte) (token, key string, err error) {
	token = Hash(data)
	key, err = s.PutAssetAt(ctx, ns, worker, token, path, data)
	return token, key, err
}

// PutAssetAt stores an asset under a caller-provided version token, so every
// file of a version shares one token the loader uses (version.assets_sha).
func (s *Store) PutAssetAt(ctx context.Context, ns, worker, token, path string, data []byte) (key string, err error) {
	if token == "" {
		token = Hash(data)
	}
	key, err = AssetKey(ns, worker, token, path)
	if err != nil {
		return "", err
	}
	if _, err := objectstore.NewOwned(s.B, objectstore.PrefixAssets, objectstore.OwnerArtifacts).Put(ctx, key, data); err != nil {
		return "", err
	}
	return key, nil
}

// GetAsset returns an asset's bytes.
func (s *Store) GetAsset(ctx context.Context, ns, worker, token, path string) ([]byte, error) {
	key, err := AssetKey(ns, worker, token, path)
	if err != nil {
		return nil, err
	}
	return objectstore.NewOwned(s.B, objectstore.PrefixAssets, objectstore.OwnerArtifacts).Get(ctx, key)
}

// PresignAsset returns a short-lived read URL for an asset.
func (s *Store) PresignAsset(ctx context.Context, ns, worker, token, path string, ttl time.Duration) (string, error) {
	key, err := AssetKey(ns, worker, token, path)
	if err != nil {
		return "", err
	}
	objects := objectstore.NewOwned(s.B, objectstore.PrefixAssets, objectstore.OwnerArtifacts)
	if _, _, err := objects.Stat(ctx, key); err != nil {
		return "", err
	}
	return objects.PresignGet(ctx, key, ttl)
}
