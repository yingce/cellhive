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
			for _, key := range []string{"r2/acme/files/k"} {
				if got := b.SupportsConditionalDelete(key); got != tc.raw {
					t.Fatalf("%s support=%v want=%v", key, got, tc.raw)
				}
			}
			for _, key := range []string{"r2meta/acme/files/k", "nodes/n.json", "cells/acme/__kv__/main/owner-gen"} {
				if !b.SupportsConditionalDelete(key) {
					t.Fatalf("append-fenced %s reported unsupported", key)
				}
			}
			if !tc.raw {
				if err := b.ConditionalDelete(context.Background(), "r2/acme/files/k", "prior"); !errors.Is(err, ErrNotSupported) {
					t.Fatalf("unsupported raw delete: %v", err)
				}
			}
		})
	}
}

func TestProviderMetadataConditionalDeletePreservesNewerWriter(t *testing.T) {
	raw := newSegmentMem()
	b := NewProviderBucket(nil, raw)
	b.authority = NewSegmentedAuthority(raw, raw, AuthorityOptions{})
	key := "r2meta/acme/files/0123456789abcdef"
	old, err := b.Put(t.Context(), key, []byte("old"))
	if err != nil {
		t.Fatal(err)
	}
	newer, err := b.Put(t.Context(), key, []byte("new"))
	if err != nil {
		t.Fatal(err)
	}
	if err = b.ConditionalDelete(t.Context(), key, old); !errors.Is(err, ErrPrecondition) {
		t.Fatalf("stale delete: %v", err)
	}
	got, _, err := b.Get(t.Context(), key)
	if err != nil || string(got) != "new" {
		t.Fatalf("new writer: %s %v", got, err)
	}
	if err = b.ConditionalDelete(t.Context(), key, newer); err != nil {
		t.Fatal(err)
	}
	if _, _, err = b.Get(t.Context(), key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted: %v", err)
	}
	if _, err = b.Put(t.Context(), key, []byte("recreated")); err != nil {
		t.Fatal(err)
	}
	if err = b.ConditionalDelete(t.Context(), key, newer); !errors.Is(err, ErrPrecondition) {
		t.Fatalf("ABA delete: %v", err)
	}
}

func TestProviderR2CASFailsClosed(t *testing.T) {
	for _, appender := range []PositionAppender{&COSAppender{}, &OSSAppender{}} {
		b := NewProviderBucket(nil, appender)
		if _, err := b.CAS(t.Context(), "r2/acme/files/key", []byte("replacement"), "stale-token"); !errors.Is(err, ErrNotSupported) {
			t.Fatalf("unsafe bulk CAS: %v", err)
		}
	}
}

func TestProviderMultipartStateUsesAuthority(t *testing.T) {
	raw := newSegmentMem()
	b := NewProviderBucket(nil, raw)
	b.authority = NewSegmentedAuthority(raw, raw, AuthorityOptions{})
	key := "r2/.mpu/v2/states/acme/files/v2-0123456789abcdef0123456789abcdef/created"
	first, err := b.ConditionalCreate(t.Context(), key, []byte("active"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.ConditionalCreate(t.Context(), key, []byte("duplicate")); !errors.Is(err, ErrPrecondition) {
		t.Fatalf("duplicate create = %v", err)
	}
	second, err := b.CAS(t.Context(), key, []byte("expired"), first)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.CAS(t.Context(), key, []byte("active-again"), first); !errors.Is(err, ErrPrecondition) {
		t.Fatalf("stale writer = %v", err)
	}
	data, token, err := b.Get(t.Context(), key)
	if err != nil || string(data) != "expired" || token != second {
		t.Fatalf("state=%q token=%q err=%v", data, token, err)
	}
	for _, ordinary := range []string{"r2/.mpu/v2/parts/acme/files/id/1", "r2/.mpu/v2/states-other/id", "r2/acme/files/key"} {
		if authorityKey(ordinary) {
			t.Fatalf("ordinary object routed to authority: %s", ordinary)
		}
		if _, err := b.CAS(t.Context(), ordinary, nil, first); !errors.Is(err, ErrNotSupported) {
			t.Fatalf("ordinary CAS %s = %v", ordinary, err)
		}
	}
}
