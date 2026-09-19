// Package bucket defines the object storage contract and a filesystem
// implementation used for local development and tests.
//
// The contract intentionally exposes only point operations plus an operator
// List; hot paths must never List.
package bucket

import (
	"context"
	"errors"
	"time"
)

var (
	// ErrNotFound is returned when a key does not exist.
	ErrNotFound = errors.New("bucket: not found")
	// ErrPrecondition is returned when a conditional write loses (create
	// conflict or CAS etag mismatch).
	ErrPrecondition = errors.New("bucket: precondition failed")
	// ErrNotSupported is returned for operations a backend cannot provide.
	ErrNotSupported = errors.New("bucket: not supported")
)

// SizeLister is an optional Bucket extension: list keys with their sizes
// without reading object bodies (S3 list responses carry sizes; local files are
// stat'ed). Callers fall back to Get-based sizing when absent.
type SizeLister interface {
	ListSizes(ctx context.Context, prefix string) ([]ObjectInfo, error)
}

// Statser is an optional Bucket extension: operation counters for metrics.
// Keys are op names (put/get/list/conditional_create/cas/delete).
type Statser interface {
	Stats() map[string]uint64
}

// Statter is an optional Bucket extension: object size and etag without a
// body read where the backend supports it (S3 HeadObject). The filesystem
// backend reads the body because its etag is content-addressed.
type Statter interface {
	Stat(ctx context.Context, key string) (size int64, etag string, err error)
}

// PagedLister is an optional Bucket extension: ordered, cursor-based listing
// with sizes and no object bodies. It returns at most limit items with key >
// after (lexicographic) plus the next cursor ("" when exhausted). Backends that
// implement it avoid materialising a whole prefix (ADR-145).
type PagedLister interface {
	ListPage(ctx context.Context, prefix, after string, limit int) ([]ObjectInfo, string, error)
}

// ObjectInfo is one key with its size and etag.
type ObjectInfo struct {
	Key  string
	Size int64
	ETag string
}

// Bucket is the object storage contract required by CellHive.
//
// Implementations MUST provide: conditional create, CAS overwrite,
// read-after-write consistency, and ranged reads (RangedGet).
type Bucket interface {
	// Get returns the value and its etag. Missing key -> ErrNotFound.
	Get(ctx context.Context, key string) ([]byte, string, error)

	// RangedGet returns bytes [off, off+len) and the full etag.
	RangedGet(ctx context.Context, key string, off, length int64) ([]byte, string, error)

	// Put unconditionally writes and returns the new etag.
	Put(ctx context.Context, key string, data []byte) (string, error)

	// ConditionalCreate writes only if the key does not exist.
	ConditionalCreate(ctx context.Context, key string, data []byte) (string, error)

	// CAS overwrites only if the current etag matches expectEtag.
	CAS(ctx context.Context, key string, data []byte, expectEtag string) (string, error)

	// Delete removes a key. Missing key is not an error.
	Delete(ctx context.Context, key string) error

	// ConditionalDelete removes a key only if its current etag matches
	// expectEtag (fencing a stale writer from deleting a newer record).
	// Missing key or a mismatched etag -> ErrPrecondition.
	ConditionalDelete(ctx context.Context, key string, expectEtag string) error

	// List returns keys under prefix. Operator/diagnostic use only.
	List(ctx context.Context, prefix string) ([]string, error)

	// PresignGet returns a short-lived read URL (ADR-030). Optional.
	PresignGet(ctx context.Context, key string, ttl time.Duration) (string, error)
}
