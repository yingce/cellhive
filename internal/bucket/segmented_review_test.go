package bucket

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestAuthorityRootCapPreservesFittingWritesAcrossClockBoundary(t *testing.T) {
	mem := newSegmentMem()
	now := time.Unix(360000, 0)
	s := NewSegmentedAuthority(mem, mem, AuthorityOptions{MaxRootBytes: 42, Now: func() time.Time { return now }})
	token, err := s.ConditionalCreate(t.Context(), "nodes/cap", []byte("a"))
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Hour)
	token, err = s.CAS(t.Context(), "nodes/cap", []byte("b"), token)
	if err != nil {
		t.Fatalf("fitting CAS after boundary: %v", err)
	}
	if err := s.ConditionalDelete(t.Context(), "nodes/cap", token); err != nil {
		t.Fatalf("fitting delete: %v", err)
	}
}

type retireDuringRange struct {
	*segmentMem
	hook func(string)
}

func (m *retireDuringRange) RangedGet(ctx context.Context, key string, off, n int64) ([]byte, string, error) {
	if m.hook != nil {
		m.hook(key)
	}
	return m.segmentMem.RangedGet(ctx, key, off, n)
}
func TestAuthorityRetirementDuringLargeFrameRangeRetriesRoot(t *testing.T) {
	for _, offset := range []bool{false, true} {
		t.Run(map[bool]string{false: "zero-offset", true: "nonzero-offset"}[offset], func(t *testing.T) {
			mem := newSegmentMem()
			hooked := &retireDuringRange{segmentMem: mem}
			now := time.Unix(360000, 0)
			opts := AuthorityOptions{Now: func() time.Time { return now }}
			writer := NewSegmentedAuthority(mem, mem, opts)
			reader := NewSegmentedAuthority(mem, hooked, opts)
			key := "nodes/large-read"
			data := []byte(strings.Repeat("a", 10000))
			token, err := writer.ConditionalCreate(t.Context(), key, data)
			if err != nil {
				t.Fatal(err)
			}
			if offset {
				token, err = writer.CAS(t.Context(), key, data, token)
				if err != nil {
					t.Fatal(err)
				}
			}
			// Prime the locator before retirement races a large-frame range read.
			if _, _, err := reader.Get(t.Context(), key); err != nil {
				t.Fatal(err)
			}
			fired := false
			hooked.hook = func(k string) {
				if !fired && k == writer.physical(key, 0) {
					fired = true
					now = now.Add(time.Hour)
					if _, err := writer.CAS(t.Context(), key, []byte("current"), token); err != nil {
						t.Fatal(err)
					}
				}
			}
			value, _, err := reader.Get(t.Context(), key)
			if !fired || err != nil || string(value) != "current" {
				t.Fatalf("fired=%v value_len=%d err=%v", fired, len(value), err)
			}
		})
	}
}
