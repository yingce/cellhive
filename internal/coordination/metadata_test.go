package coordination

import (
	"context"
	"fmt"
	"net"
	"testing"

	"cellhive/internal/bucket"
	"github.com/redis/go-redis/v9"
)

type metadataCalls struct{ evals int }

func (h *metadataCalls) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) { return next(ctx, network, addr) }
}
func (h *metadataCalls) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if cmd.Name() == "eval" {
			h.evals++
		}
		return next(ctx, cmd)
	}
}
func (h *metadataCalls) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func TestPageMetadataBoundedBatches(t *testing.T) {
	b, _ := fixture(t)
	ctx := t.Context()
	expected := map[string]bucket.ObjectInfo{}
	for j := 0; j < 257; j++ {
		k := fmt.Sprintf("nodes/%04d.json", j)
		value := make([]byte, j*17)
		e, err := b.Put(ctx, k, value)
		if err != nil {
			t.Fatal(err)
		}
		expected[k] = bucket.ObjectInfo{Key: k, Size: int64(len(value)), ETag: e}
	}
	hook := &metadataCalls{}
	b.Store.client.AddHook(hook)
	page, next, err := b.ListPage(ctx, "nodes/", "", 300)
	if err != nil || next != "" || len(page) != 257 {
		t.Fatal(len(page), next, err)
	}
	if hook.evals != 4 {
		t.Fatalf("got %d EVAL calls, want 1 list + 3 metadata batches", hook.evals)
	}
	for _, info := range page {
		if info != expected[info.Key] {
			t.Fatalf("incorrect metadata: %+v", info)
		}
	}
	got, err := b.Store.metadata(ctx, []string{"nodes/missing.json", "nodes/0000.json"})
	if err != nil || len(got) != 1 || got[0] != expected["nodes/0000.json"] {
		t.Fatal(got, err)
	}
	b.Store.generation = "wrong"
	if _, err = b.Store.metadata(ctx, []string{"nodes/0000.json"}); err != ErrGeneration {
		t.Fatal("generation guard", err)
	}
}
