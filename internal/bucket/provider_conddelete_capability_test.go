package bucket

import (
	"context"
	"errors"
	"testing"
)

func TestProviderConditionalDeleteCapability(t *testing.T) {
	for _, tc := range []struct {
		name     string
		appender PositionAppender
		raw      bool
	}{
		{"cos", &COSAppender{}, true},
		{"oss", &OSSAppender{}, false},
		{"unknown", etagAppender{}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := NewProviderBucket(nil, tc.appender)
			for _, key := range []string{"r2meta/acme/files/k", "r2/acme/files/k"} {
				if got := b.SupportsConditionalDelete(key); got != tc.raw {
					t.Fatalf("%s support=%v want=%v", key, got, tc.raw)
				}
			}
			for _, key := range []string{"nodes/n.json", "cells/acme/__kv__/main/owner.json", "cells/acme/__kv__/main/resource-fence.v2"} {
				if !b.SupportsConditionalDelete(key) {
					t.Fatalf("append-fenced %s reported unsupported", key)
				}
			}
			if !tc.raw {
				if err := b.ConditionalDelete(context.Background(), "r2meta/acme/files/k", "prior"); !errors.Is(err, ErrNotSupported) {
					t.Fatalf("unsupported raw delete: %v", err)
				}
			}
		})
	}
}
