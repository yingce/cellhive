package bucket

import (
	"context"
	"errors"
	"testing"
)

func TestFSBucketEnumerationHonorsCancellation(t *testing.T) {
	b, err := NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	for name, call := range map[string]func() error{
		"list":     func() error { _, err := b.List(ctx, ""); return err },
		"sizes":    func() error { _, err := b.ListSizes(ctx, ""); return err },
		"page":     func() error { _, _, err := b.ListPage(ctx, "", "", 1); return err },
		"prefixes": func() error { _, err := b.CommonPrefixes(ctx, "", "/"); return err },
	} {
		t.Run(name, func(t *testing.T) {
			if err := call(); !errors.Is(err, context.Canceled) {
				t.Fatalf("error=%v, want cancellation", err)
			}
		})
	}
}
