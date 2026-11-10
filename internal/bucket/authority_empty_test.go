package bucket

import (
	"context"
	"errors"
	"strconv"
	"testing"
)

type emptyAuthorityData struct {
	*segmentMem
	empty int
	calls int
}

func (d *emptyAuthorityData) TailGet(ctx context.Context, key string, n int) ([]byte, int64, string, error) {
	d.calls++
	if d.calls <= d.empty {
		return nil, 0, "empty", nil
	}
	return d.segmentMem.TailGet(ctx, key, n)
}
func TestAuthorityEmptyPublicationRead(t *testing.T) {
	for _, empty := range []int{1, 3, 100} {
		t.Run(strconv.Itoa(empty), func(t *testing.T) {
			mem := newSegmentMem()
			writer := NewSegmentedAuthority(mem, mem, AuthorityOptions{})
			if _, err := writer.ConditionalCreate(t.Context(), "own.json", []byte("winner")); err != nil {
				t.Fatal(err)
			}
			data := &emptyAuthorityData{segmentMem: mem, empty: empty}
			reader := NewSegmentedAuthority(mem, data, AuthorityOptions{})
			got, _, err := reader.Get(t.Context(), "own.json")
			if empty < 4 {
				if err != nil || string(got) != "winner" {
					t.Fatalf("got=%q err=%v", got, err)
				}
			} else if err == nil || errors.Is(err, ErrNotFound) || data.calls != 4 {
				t.Fatalf("must fail closed after four reads: calls=%d err=%v", data.calls, err)
			}
		})
	}
}
func TestAuthorityEmptyPublicationCancellation(t *testing.T) {
	data := &emptyAuthorityData{segmentMem: newSegmentMem(), empty: 100}
	reader := NewSegmentedAuthority(data, data, AuthorityOptions{})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, _, _, _, err := reader.tail(ctx, "own.json", 100)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
}
