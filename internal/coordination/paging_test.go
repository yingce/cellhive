package coordination

import (
	"context"
	"fmt"
	"testing"

	"cellhive/internal/bucket"

	"github.com/redis/go-redis/v9"
)

type pagingReadMeter struct {
	*bucket.FSBucket
	lists, pages, stats int
}

func (b *pagingReadMeter) List(c context.Context, p string) ([]string, error) {
	b.lists++
	return b.FSBucket.List(c, p)
}
func (b *pagingReadMeter) ListPage(c context.Context, p, a string, n int) ([]bucket.ObjectInfo, string, error) {
	b.pages++
	return b.FSBucket.ListPage(c, p, a, n)
}
func (b *pagingReadMeter) Stat(c context.Context, k string) (int64, string, error) {
	b.stats++
	return b.FSBucket.Stat(c, k)
}
func TestMixedPrefixUsesBoundedPages(t *testing.T) {
	b, base := fixture(t)
	meter := &pagingReadMeter{FSBucket: base}
	b.Base = meter
	for n := 0; n < 5; n++ {
		_, err := base.Put(t.Context(), fmt.Sprintf("cells/ns/__do__/w~%d/ltx/e1/data", n), []byte("data"))
		if err != nil {
			t.Fatal(err)
		}
	}
	after := ""
	total := 0
	for {
		page, next, err := b.ListPage(t.Context(), "cells/ns/__do__/", after, 2)
		if err != nil {
			t.Fatal(err)
		}
		total += len(page)
		if next == "" {
			break
		}
		after = next
	}
	t.Logf("objects=%d full prefix LIST=%d native ListPage=%d per-object Stat=%d", total, meter.lists, meter.pages, meter.stats)
	if total != 5 || meter.lists != 0 || meter.pages != 3 || meter.stats != 0 {
		t.Fatal("paging amplified reads")
	}
}

// Provider cursors can point beyond the logical key returned by a page.
type physicalPageBucket struct{ *bucket.FSBucket }

func (b physicalPageBucket) ListPage(c context.Context, p, a string, n int) ([]bucket.ObjectInfo, string, error) {
	switch a {
	case "":
		return nil, p + "a/own.json.g10", nil
	case p + "a/own.json.g10":
		return []bucket.ObjectInfo{{Key: p + "b/data", Size: 123, ETag: "etag", LastModifiedMs: 456}}, "", nil
	default:
		return b.FSBucket.ListPage(c, p, a, n)
	}
}
func TestMixedPagesPreserveEmptyPhysicalBoundary(t *testing.T) {
	b, base := fixture(t)
	b.Base = physicalPageBucket{base}
	p := "cells/ns/c/"
	_, err := b.Put(t.Context(), p+"z/own.json", []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	page, next, err := b.ListPage(t.Context(), p, "", 2)
	if err != nil || len(page) != 0 || next != p+"a/own.json.g10" {
		t.Fatalf("first page=%v cursor=%q err=%v", page, next, err)
	}
	page, next, err = b.ListPage(t.Context(), p, next, 2)
	if err != nil || len(page) != 2 || next != "" || page[0].LastModifiedMs != 456 || page[0].Size != 123 {
		t.Fatalf("second page=%v cursor=%q err=%v", page, next, err)
	}
}

func TestMixedPagesMergeAndDelete(t *testing.T) {
	b, base := fixture(t)
	p := "cells/ns/c/"
	want := map[string]bool{}
	for n := 0; n < 9; n++ {
		k := fmt.Sprintf("%s%d/data", p, n)
		if n%2 == 0 {
			k = fmt.Sprintf("%s%d/own.json", p, n)
		}
		if _, err := b.Put(t.Context(), k, []byte(`{}`)); err != nil {
			t.Fatal(err)
		}
		want[k] = true
		if Selected(k) {
			if _, err := base.Put(t.Context(), k, []byte(`stale`)); err != nil {
				t.Fatal(err)
			}
		}
	}
	after := ""
	seen := map[string]bool{}
	for rounds := 0; rounds < 30; rounds++ {
		page, next, err := b.ListPage(t.Context(), p, after, 2)
		if err != nil {
			t.Fatal(err)
		}
		if len(page) > 2 {
			t.Fatal("unbounded page")
		}
		for _, x := range page {
			if !want[x.Key] || seen[x.Key] {
				t.Fatalf("unexpected/duplicate %s", x.Key)
			}
			seen[x.Key] = true
			if err := b.Delete(t.Context(), x.Key); err != nil {
				t.Fatal(err)
			}
		}
		if next == "" {
			break
		}
		if next <= after {
			t.Fatal("cursor did not advance")
		}
		after = next
	}
	if len(seen) != len(want) {
		t.Fatalf("lost keys: got=%v want=%v", seen, want)
	}
}

func TestRedisPrefixesSkipLargeSubtrees(t *testing.T) {
	b, _ := fixture(t)
	ctx := t.Context()
	members := make([]redis.Z, 3000)
	for n := range members {
		members[n] = redis.Z{Member: fmt.Sprintf("wake/due/2026-11-24T00:00/ns/c/%04d.json", n)}
	}
	if err := b.Store.client.ZAdd(ctx, b.Store.keys[3], members...).Err(); err != nil {
		t.Fatal(err)
	}
	meter := &prefixReplyMeter{}
	b.Store.client.AddHook(meter)
	dirs, err := b.CommonPrefixes(ctx, "wake/due/", "/")
	if err != nil || len(dirs) != 1 || dirs[0] != "wake/due/2026-11-24T00:00/" {
		t.Fatal(dirs, err)
	}
	if meter.calls > 2 || meter.items > 3 {
		t.Fatalf("downloaded subtree: calls=%d reply items=%d", meter.calls, meter.items)
	}
}

func TestRedisPrefixesAcrossBatchesAndImmediateKeys(t *testing.T) {
	b, _ := fixture(t)
	ctx := t.Context()
	p := "wake/"
	members := make([]redis.Z, 0, 610)
	for n := 0; n < 300; n++ {
		members = append(members, redis.Z{Member: fmt.Sprintf("%sflat%04d", p, n)})
	}
	for n := 0; n < 310; n++ {
		members = append(members, redis.Z{Member: fmt.Sprintf("%sz%04d/x/y", p, n)})
	}
	if err := b.Store.client.ZAdd(ctx, b.Store.keys[3], members...).Err(); err != nil {
		t.Fatal(err)
	}
	dirs, err := b.CommonPrefixes(ctx, p, "/")
	if err != nil {
		t.Fatal(err)
	}
	if len(dirs) != 310 {
		t.Fatalf("prefix count=%d", len(dirs))
	}
	for n, d := range dirs {
		if d != fmt.Sprintf("%sz%04d/", p, n) {
			t.Fatalf("prefix %d=%q", n, d)
		}
	}
	if _, err := b.CommonPrefixes(ctx, p, ""); err == nil {
		t.Fatal("empty delimiter accepted")
	}
}

// Observe this client's replies only, so other tests sharing Redis cannot
// perturb the cost assertion. The real Redis scripts still execute.
type prefixReplyMeter struct{ calls, items int }

func (m *prefixReplyMeter) DialHook(next redis.DialHook) redis.DialHook { return next }
func (m *prefixReplyMeter) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (m *prefixReplyMeter) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		err := next(ctx, cmd)
		if cmd.Name() == "eval" {
			m.calls++
			if c, ok := cmd.(*redis.Cmd); ok {
				if values, ok := c.Val().([]any); ok {
					m.items += len(values)
				}
			}
		}
		return err
	}
}
