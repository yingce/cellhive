package bucket

import (
	"bytes"
	"context"
	"fmt"
	"time"
)

// Diagnose runs the object storage conformance probe required before a node
// serves: four conditional writes (create, reject-create, update,
// reject-stale) and a ranged read byte check.
//
// It returns an error naming the first failing property.
func Diagnose(ctx context.Context, b Bucket) error {
	if preflight, ok := b.(interface{ diagnosePreflight(context.Context) error }); ok {
		if err := preflight.diagnosePreflight(ctx); err != nil {
			return fmt.Errorf("bucket preflight failed: %w", err)
		}
	}
	base := fmt.Sprintf("probe/%d", time.Now().UnixNano())
	createKey := "nodes/" + base + "/create.json"
	rangeKey := base + "/range"

	// 1. conditional create on an absent object must succeed.
	e1, err := b.ConditionalCreate(ctx, createKey, []byte("one"))
	if err != nil {
		return fmt.Errorf("bucket conditional write (create) failed: %w", err)
	}

	// 2. conditional create on an existing object must fail.
	if _, err := b.ConditionalCreate(ctx, createKey, []byte("two")); err != ErrPrecondition {
		return fmt.Errorf("bucket conditional write (reject-create) expected %v, got %v", ErrPrecondition, err)
	}

	// 3. CAS with the correct etag must succeed.
	e2, err := b.CAS(ctx, createKey, []byte("three"), e1)
	if err != nil {
		return fmt.Errorf("bucket conditional write (update) failed: %w", err)
	}
	_ = e2

	// 4. CAS with a stale etag must fail.
	if _, err := b.CAS(ctx, createKey, []byte("four"), e1); err != ErrPrecondition {
		return fmt.Errorf("bucket conditional write (reject-stale) expected %v, got %v", ErrPrecondition, err)
	}

	// 5. ranged read must return the exact requested bytes.
	payload := []byte("0123456789")
	if _, err := b.Put(ctx, rangeKey, payload); err != nil {
		return fmt.Errorf("bucket put for ranged read failed: %w", err)
	}
	got, _, err := b.RangedGet(ctx, rangeKey, 2, 4)
	if err != nil {
		return fmt.Errorf("bucket ranged read failed: %w", err)
	}
	if !bytes.Equal(got, []byte("2345")) {
		return fmt.Errorf("bucket ranged read returned %q, want %q", got, "2345")
	}

	// 6. conditional delete: a stale etag must be rejected and the current etag
	// must delete. Some S3-compatible stores accept If-Match on PutObject but
	// ignore it on DeleteObject, which would silently degrade the owner/lease
	// fence (ADR-134); the probe names that failure.
	delKey := "nodes/" + base + "/delete.json"
	e3, err := b.ConditionalCreate(ctx, delKey, []byte("del"))
	if err != nil {
		return fmt.Errorf("bucket conditional delete (setup) failed: %w", err)
	}
	if err := b.ConditionalDelete(ctx, delKey, e3+"-stale"); err != ErrPrecondition {
		return fmt.Errorf("bucket conditional delete (reject-stale) expected %v, got %v — the store likely ignores If-Match on DeleteObject", ErrPrecondition, err)
	}
	if err := b.ConditionalDelete(ctx, delKey, e3); err != nil {
		return fmt.Errorf("bucket conditional delete failed: %w", err)
	}
	if _, _, err := b.Get(ctx, delKey); err != ErrNotFound {
		return fmt.Errorf("bucket conditional delete left the object behind (got %v)", err)
	}

	_ = b.Delete(ctx, createKey)
	_ = b.Delete(ctx, rangeKey)
	return nil
}
