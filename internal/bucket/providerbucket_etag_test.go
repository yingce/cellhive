package bucket

import (
	"context"
	"errors"
	"testing"
)

type etagAppender struct{ etag string }

func (a etagAppender) Read(context.Context, string) ([]byte, error) { return nil, ErrNotFound }
func (a etagAppender) Append(context.Context, string, int64, []byte) error {
	return nil
}
func (a etagAppender) AppendWithETag(context.Context, string, int64, []byte) (string, error) {
	return a.etag, nil
}

func TestProviderImmutableLTXUsesNativeCreate(t *testing.T) {
	key := "cells/acme/__kv__/main/ltx/e1/0000/0000000000000001-0000000000000001.ltx"
	creator := &assetCreatorProbe{}
	b := NewProviderBucket(nil, creator)
	tag, err := b.ConditionalCreate(t.Context(), key, []byte("segment"))
	if err != nil || tag != "created-etag" || creator.calls != 1 {
		t.Fatalf("create=%s calls=%d err=%v", tag, creator.calls, err)
	}
	creator.err = ErrPrecondition
	if _, err := b.ConditionalCreate(t.Context(), key, []byte("changed")); !errors.Is(err, ErrPrecondition) {
		t.Fatal(err)
	}
	unsupported := NewProviderBucket(nil, etagAppender{})
	if _, err := unsupported.ConditionalCreate(t.Context(), key, nil); !errors.Is(err, ErrNotSupported) {
		t.Fatalf("must not fall back to AppendObject: %v", err)
	}
}
