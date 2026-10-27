package bucket

import (
	"context"
	"errors"
	"strings"
	"testing"
)

type emptyThenAppender struct {
	*memoryAppender
	emptyReads int
	reads      int
	writes     int
	cancel     context.CancelFunc
}

func (a *emptyThenAppender) Read(ctx context.Context, key string) ([]byte, error) {
	a.reads++
	if a.cancel != nil {
		a.cancel()
	}
	if a.reads <= a.emptyReads {
		return nil, nil
	}
	return a.memoryAppender.Read(ctx, key)
}
func (a *emptyThenAppender) Append(ctx context.Context, key string, pos int64, data []byte) error {
	a.writes++
	return a.memoryAppender.Append(ctx, key, pos, data)
}
func emptyFixture(n int) *emptyThenAppender {
	return &emptyThenAppender{memoryAppender: &memoryAppender{objects: map[string][]byte{"owner": encodeFrame([]byte("winner"), false)}}, emptyReads: n}
}
func TestAppendAuthorityEmptyReadBeforeWinner(t *testing.T) {
	a := emptyFixture(2)
	b := NewAppendAuthority(a)
	if _, err := b.ConditionalCreate(t.Context(), "owner", []byte("loser")); !errors.Is(err, ErrPrecondition) {
		t.Fatalf("create=%v, want competing owner rejected", err)
	}
	if a.writes != 0 {
		t.Fatal("retried creation instead of observing winner")
	}
	value, token, err := b.Get(t.Context(), "owner")
	if err != nil || string(value) != "winner" || token != "19" {
		t.Fatalf("value=%q token=%q err=%v", value, token, err)
	}
}
func TestAppendAuthorityGetRetriesEmpty(t *testing.T) {
	a := emptyFixture(1)
	b := NewAppendAuthority(a)
	value, _, err := b.Get(t.Context(), "owner")
	if err != nil || string(value) != "winner" {
		t.Fatalf("get=%q %v", value, err)
	}
}
func TestAppendAuthorityPermanentEmptyFailsClosed(t *testing.T) {
	for _, op := range []string{"get", "create", "cas", "delete"} {
		t.Run(op, func(t *testing.T) {
			a := emptyFixture(100)
			b := NewAppendAuthority(a)
			var err error
			switch op {
			case "get":
				_, _, err = b.Get(t.Context(), "owner")
			case "create":
				_, err = b.ConditionalCreate(t.Context(), "owner", []byte("x"))
			case "cas":
				_, err = b.CAS(t.Context(), "owner", []byte("x"), "19")
			case "delete":
				err = b.ConditionalDelete(t.Context(), "owner", "19")
			}
			if err == nil || !strings.Contains(err.Error(), "empty object") {
				t.Fatalf("err=%v", err)
			}
			if a.writes != 0 || a.reads > 5 {
				t.Fatalf("writes=%d reads=%d", a.writes, a.reads)
			}
		})
	}
}
func TestAppendAuthorityEmptyReadCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	a := emptyFixture(100)
	a.cancel = cancel
	_, _, err := NewAppendAuthority(a).Get(ctx, "owner")
	if !errors.Is(err, context.Canceled) || a.reads != 1 || a.writes != 0 {
		t.Fatalf("err=%v reads=%d writes=%d", err, a.reads, a.writes)
	}
}
func TestAppendAuthorityCorruptNonemptyDoesNotRetry(t *testing.T) {
	a := emptyFixture(0)
	a.objects["owner"] = []byte("broken")
	_, _, err := NewAppendAuthority(a).Get(t.Context(), "owner")
	if err == nil || a.reads != 1 {
		t.Fatalf("err=%v reads=%d", err, a.reads)
	}
}

func TestAppendAuthorityEmptyReadPreservesFences(t *testing.T) {
	for _, op := range []string{"cas", "delete"} {
		for _, token := range []string{"18", "19"} {
			t.Run(op+"/"+token, func(t *testing.T) {
				a := emptyFixture(1)
				b := NewAppendAuthority(a)
				var err error
				if op == "cas" {
					_, err = b.CAS(t.Context(), "owner", []byte("next"), token)
				} else {
					err = b.ConditionalDelete(t.Context(), "owner", token)
				}
				if token == "18" {
					if !errors.Is(err, ErrPrecondition) || a.writes != 0 {
						t.Fatalf("stale token err=%v writes=%d", err, a.writes)
					}
				} else if err != nil || a.writes != 1 {
					t.Fatalf("current token err=%v writes=%d", err, a.writes)
				}
			})
		}
	}
}

func TestAppendAuthorityEmptyThenCorruptFailsClosed(t *testing.T) {
	a := emptyFixture(1)
	a.objects["owner"] = encodeFrame([]byte("winner"), false)
	a.objects["owner"][13] ^= 1 // invalid CRC after the transient empty response
	_, err := NewAppendAuthority(a).ConditionalCreate(t.Context(), "owner", []byte("loser"))
	if err == nil || !strings.Contains(err.Error(), "corrupt frame") || a.reads != 2 || a.writes != 0 {
		t.Fatalf("err=%v reads=%d writes=%d", err, a.reads, a.writes)
	}
}
