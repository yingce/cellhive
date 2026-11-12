package bucket

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"
)

type reclaimPagedMem struct {
	*segmentMem
	pages, scanned int
	beforePage     func()
	failDelete     string
}

func (m *reclaimPagedMem) Delete(ctx context.Context, key string) error {
	if key == m.failDelete {
		return fmt.Errorf("injected delete failure")
	}
	return m.segmentMem.Delete(ctx, key)
}
func (m *reclaimPagedMem) List(context.Context, string) ([]string, error) {
	return nil, fmt.Errorf("unbounded list forbidden")
}
func (m *reclaimPagedMem) ListPage(ctx context.Context, prefix, after string, limit int) ([]ObjectInfo, string, error) {
	if m.beforePage != nil {
		m.beforePage()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pages++
	var keys []string
	for k := range m.objects {
		if strings.HasPrefix(k, prefix) && k > after {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	next := ""
	if len(keys) > limit {
		keys = keys[:limit]
		next = keys[len(keys)-1]
	}
	items := make([]ObjectInfo, len(keys))
	for i, k := range keys {
		items[i] = ObjectInfo{Key: k, Size: int64(len(m.objects[k]))}
	}
	m.scanned += len(items)
	return items, next, nil
}
func TestReclaimPagedCollectorBoundsScanAndResumes(t *testing.T) {
	mem := &reclaimPagedMem{segmentMem: newSegmentMem()}
	clock := &testClock{t: time.Unix(1700000000, 0)}
	auth := NewSegmentedAuthority(mem, mem, AuthorityOptions{ReclaimRetired: true, Segment: time.Minute, Now: clock.Now})
	key := "nodes/paged-gc.json"
	tok, err := auth.ConditionalCreate(t.Context(), key, []byte("v"))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 14; i++ {
		clock.Advance(time.Minute)
		tok, err = auth.CAS(t.Context(), key, []byte("v"), tok)
		if err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 14; i++ {
		_, _ = mem.Put(t.Context(), fmt.Sprintf("%s.g%d", key, i), []byte("garbage"))
	}
	for _, suffix := range []string{".g00", ".g01", ".g15", ".g3-other"} {
		_, _ = mem.Put(t.Context(), key+suffix, []byte("keep"))
	}
	cursor := ""
	total := 0
	for i := 0; i < 20; i++ {
		before := mem.scanned
		n, next, err := auth.CollectRetiredPage(t.Context(), key, cursor, 3)
		if err != nil {
			t.Fatal(err)
		}
		if mem.scanned-before > 3 || n > 3 {
			t.Fatal("unbounded page", n)
		}
		total += n
		if next == "" {
			break
		}
		if next <= cursor {
			t.Fatal("cursor failed to advance")
		}
		cursor = next
	}
	if total != 14 {
		t.Fatalf("deleted %d want14", total)
	}
	for _, suffix := range []string{"", ".g14", ".g15", ".g00", ".g01", ".g3-other"} {
		if _, err := mem.Read(t.Context(), key+suffix); err != nil {
			t.Fatal("deleted protected key", suffix)
		}
	}
	if mem.pages < 5 {
		t.Fatal("not paginated")
	}
	// A delayed genesis behind the completed cursor is collected next sweep.
	_, _ = mem.Put(t.Context(), key+".g0", []byte("late"))
	if n, _, err := auth.CollectRetiredPage(t.Context(), key, "", 3); err != nil || n != 1 {
		t.Fatalf("next sweep %d %v", n, err)
	}
}
func TestReclaimPagedCollectorFailsClosed(t *testing.T) {
	mem := &reclaimPagedMem{segmentMem: newSegmentMem()}
	auth := NewSegmentedAuthority(mem, mem, AuthorityOptions{ReclaimRetired: true})
	key := "nodes/paged-fail.json"
	if _, err := auth.ConditionalCreate(t.Context(), key, []byte("v")); err != nil {
		t.Fatal(err)
	}
	for _, limit := range []int{0, -1, 1001} {
		if _, _, err := auth.CollectRetiredPage(t.Context(), key, "", limit); err == nil {
			t.Fatal("invalid limit accepted")
		}
	}
	if _, _, err := auth.CollectRetiredPage(t.Context(), key, "other.g0", 2); err == nil {
		t.Fatal("foreign cursor accepted")
	}
	legacy := NewSegmentedAuthority(mem, mem, AuthorityOptions{})
	if _, _, err := legacy.CollectRetiredPage(t.Context(), key, "", 2); !errors.Is(err, ErrNotSupported) {
		t.Fatal(err)
	}
	unsupported := NewSegmentedAuthority(mem.segmentMem, struct{ segmentData }{mem.segmentMem}, AuthorityOptions{ReclaimRetired: true})
	if _, _, err := unsupported.CollectRetiredPage(t.Context(), key, "", 2); !errors.Is(err, ErrNotSupported) {
		t.Fatal(err)
	}
	before := mem.pages
	mem.beforePage = func() { _ = mem.Delete(t.Context(), key) }
	if n, _, err := auth.CollectRetiredPage(t.Context(), key, "", 2); err == nil || n != 0 {
		t.Fatalf("unverified root %d %v", n, err)
	}
	if mem.pages != before+1 {
		t.Fatal("missing list probe")
	}
	if _, err := mem.Read(t.Context(), key+".g0"); err != nil {
		t.Fatal("deleted current generation")
	}
}

// Most protocol fixtures can provide paging; the explicit unsupported case
// hides this optional capability behind segmentData.
func (m *segmentMem) ListPage(ctx context.Context, prefix, after string, limit int) ([]ObjectInfo, string, error) {
	return (&reclaimPagedMem{segmentMem: m}).ListPage(ctx, prefix, after, limit)
}

func TestReclaimPagedCollectorRetriesPartialFailure(t *testing.T) {
	mem := &reclaimPagedMem{segmentMem: newSegmentMem()}
	clock := &testClock{t: time.Unix(1700000000, 0)}
	auth := NewSegmentedAuthority(mem, mem, AuthorityOptions{ReclaimRetired: true, Segment: time.Minute, Now: clock.Now})
	key := "nodes/paged-retry.json"
	tok, err := auth.ConditionalCreate(t.Context(), key, []byte("v"))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		clock.Advance(time.Minute)
		tok, err = auth.CAS(t.Context(), key, []byte("v"), tok)
		if err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 3; i++ {
		_, _ = mem.Put(t.Context(), fmt.Sprintf("%s.g%d", key, i), []byte("retired"))
	}
	mem.failDelete = key + ".g1"
	n, next, err := auth.CollectRetiredPage(t.Context(), key, "", 3)
	if n != 1 || next != "" || err == nil {
		t.Fatalf("partial result %d %q %v", n, next, err)
	}
	mem.failDelete = ""
	n, next, err = auth.CollectRetiredPage(t.Context(), key, "", 3)
	if n != 2 || next != "" || err != nil {
		t.Fatalf("retry result %d %q %v", n, next, err)
	}
	if _, err := mem.Read(t.Context(), key+".g3"); err != nil {
		t.Fatal(err)
	}
}
