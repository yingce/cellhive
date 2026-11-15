package bucket

import (
	"errors"
	"testing"
)

func TestMultipartStateUsesProviderAuthority(t *testing.T) {
	raw := newSegmentMem()
	a := NewProviderBucket(nil, raw)
	b := NewProviderBucket(nil, raw)
	a.authority = NewSegmentedAuthority(raw, raw, AuthorityOptions{})
	b.authority = NewSegmentedAuthority(raw, raw, AuthorityOptions{})
	key := "r2/.mpu/v2/states/acme/files/v2-01234567890123456789012345678901/created"
	old, err := a.ConditionalCreate(t.Context(), key, []byte("active"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.ConditionalCreate(t.Context(), key, []byte("other")); !errors.Is(err, ErrPrecondition) {
		t.Fatalf("duplicate create: %v", err)
	}
	current, err := b.CAS(t.Context(), key, []byte("aborted"), old)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.CAS(t.Context(), key, []byte("active-again"), old); !errors.Is(err, ErrPrecondition) {
		t.Fatalf("stale token: %v", err)
	}
	got, tag, err := a.Get(t.Context(), key)
	if err != nil || string(got) != "aborted" || tag != current {
		t.Fatalf("get=%q/%q, %v", got, tag, err)
	}
	if !s3ControlKey(key) {
		t.Fatal("S3 wrapper does not recognize multipart state")
	}
	for _, ordinary := range []string{
		"r2/.mpu/v2/parts/acme/files/id/00001",
		"r2/.mpu/v2/states-other/key",
		"r2/acme/files/states/key",
	} {
		if authorityKey(ordinary) || s3ControlKey(ordinary) {
			t.Fatalf("ordinary object treated as authority: %s", ordinary)
		}
	}
}
