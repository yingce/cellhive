package dosupervisor

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/ltx"
	"cellhive/internal/replica"
)

func pagingFixture(t *testing.T) (*Paging, *replica.Manager, cell.Scope, string) {
	return pagingFixturePages(t, 3)
}
func pagingFixturePages(t *testing.T, count uint32) (*Paging, *replica.Manager, cell.Scope, string) {
	t.Helper()
	b, e := bucket.NewFSBucket(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	m := replica.New(b)
	sc := cell.Scope{Namespace: "p", Class: "c", ID: scopeID("db.sqlite")}
	var pages []ltx.WALPage
	for pg := uint32(1); pg <= count; pg++ {
		pages = append(pages, ltx.WALPage{PageNo: pg, Data: bytes.Repeat([]byte{byte(pg)}, 512)})
	}
	raw, e := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: 512, Commit: count, MinTXID: 1, MaxTXID: 1}, pages)
	if e != nil {
		t.Fatal(e)
	}
	if _, _, e = m.Append(t.Context(), sc, 1, raw); e != nil {
		t.Fatal(e)
	}
	pf, e := m.NewPageFetcher(t.Context(), sc, 1)
	if e != nil {
		t.Fatal(e)
	}
	root := t.TempDir()
	p, e := NewPaging(root, strings.Repeat("t", 32))
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = p.Close() })
	dest := filepath.Join(root, "db.sqlite")
	if e = p.Register(t.Context(), dest, sc, pf); e != nil {
		t.Fatal(e)
	}
	return p, m, sc, dest
}
func pagingRequest(p *Paging, op, path, params string) *httptest.ResponseRecorder {
	if op != "open" {
		v, _ := p.lookup(path)
		if v != nil {
			v.mu.Lock()
			attached := v.attached
			v.mu.Unlock()
			if !attached {
				pagingRequest(p, "open", path, "")
			}
			v.mu.Lock()
			params += "&activation=" + v.session
			v.mu.Unlock()
		}
	}
	req := httptest.NewRequest(http.MethodPost, "/internal/do-pages/"+op+"?path="+url.QueryEscape(path)+params, nil)
	req.Header.Set("Authorization", "Bearer "+p.token)
	rr := httptest.NewRecorder()
	p.Handler().ServeHTTP(rr, req)
	return rr
}
func TestPagingHydrationPreservesWritesAndTruncation(t *testing.T) {
	p, _, _, path := pagingFixture(t)
	if rr := pagingRequest(p, "open", path, ""); rr.Code != 200 || rr.Body.String() != "512 3\n" {
		t.Fatalf("open %d %s", rr.Code, rr.Body)
	}
	if rr := pagingRequest(p, "ensure", path, "&offset=512&length=10"); rr.Code != 204 {
		t.Fatal(rr.Body)
	}
	f, e := os.OpenFile(path, os.O_RDWR, 0)
	if e != nil {
		t.Fatal(e)
	}
	defer f.Close()
	if _, e = f.WriteAt(bytes.Repeat([]byte{9}, 512), 512); e != nil {
		t.Fatal(e)
	}
	if e = p.Hydrate(t.Context(), path); e != nil {
		t.Fatal(e)
	}
	got, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	if !bytes.Equal(got[512:1024], bytes.Repeat([]byte{9}, 512)) {
		t.Fatal("old page overwrote checkpoint")
	}
	if rr := pagingRequest(p, "truncate", path, "&size=512"); rr.Code != 204 {
		t.Fatal(rr.Body)
	}
	if e = f.Truncate(512); e != nil {
		t.Fatal(e)
	}
	if rr := pagingRequest(p, "write-end", path, ""); rr.Code != 204 {
		t.Fatal(rr.Body)
	}
	if e = f.Truncate(1536); e != nil {
		t.Fatal(e)
	}
	if rr := pagingRequest(p, "ensure", path, "&offset=512&length=1024"); rr.Code != 204 {
		t.Fatal(rr.Body)
	}
	got, e = os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	if !bytes.Equal(got[512:], make([]byte, 1024)) {
		t.Fatal("truncated originals resurrected")
	}
}
func TestPagingAuthenticationAndLostRegistryFailClosed(t *testing.T) {
	p, _, _, path := pagingFixture(t)
	req := httptest.NewRequest("POST", "/internal/do-pages/open?path="+url.QueryEscape(path), nil)
	rr := httptest.NewRecorder()
	p.Handler().ServeHTTP(rr, req)
	if rr.Code != 401 {
		t.Fatal(rr.Code)
	}
	fresh, e := NewPaging(p.root, p.token)
	if e != nil {
		t.Fatal(e)
	}
	if rr = pagingRequest(fresh, "open", path, ""); rr.Code != 409 {
		t.Fatalf("lost registry accepted sparse file: %d", rr.Code)
	}
	if rr = pagingRequest(p, "open", filepath.Join(p.root, "..", "outside.sqlite"), ""); rr.Code != 400 {
		t.Fatal(rr.Code)
	}
	link := filepath.Join(p.root, "link.sqlite")
	if e = os.Symlink(path, link); e != nil {
		t.Fatal(e)
	}
	if rr = pagingRequest(p, "open", link, ""); rr.Code != 400 {
		t.Fatal(rr.Code)
	}
	if rr = pagingRequest(p, "open", filepath.Join(p.root, "new.sqlite"), ""); rr.Code != 204 {
		t.Fatal(rr.Code)
	}
	_ = p.Close()
	if rr = pagingRequest(p, "ensure", path, "&offset=0&length=1"); rr.Code != 503 {
		t.Fatal(rr.Code)
	}
}
func TestPagedCaptureStartsWithContinuation(t *testing.T) {
	p, m, sc, path := pagingFixture(t)
	s := &Supervisor{Dir: p.root, Store: m, Epoch: 2, ScopePrefix: "p/c", Paging: p, Committer: &bucketCommitter{rep: m}}
	if e := s.syncFile(context.Background(), path); e != nil {
		t.Fatal(e)
	}
	pf, e := m.NewPageFetcher(t.Context(), sc, 2)
	if e != nil {
		t.Fatal(e)
	}
	if pf.Watermark() != 2 {
		t.Fatal(pf.Watermark())
	}
	v, _ := p.lookup(path)
	if v.present.count != 0 {
		t.Fatal("baseline hydrated entire database")
	}
	got, e := pf.Page(t.Context(), 3)
	if e != nil || !bytes.Equal(got, bytes.Repeat([]byte{3}, 512)) {
		t.Fatalf("continuation lost untouched page: %v", e)
	}
}

func TestPagingSnapshotRejectsConcurrentMainMutations(t *testing.T) {
	for _, op := range []struct{ name, args string }{
		{"write", "&offset=0&length=512"},
		{"truncate", "&size=512"},
	} {
		t.Run(op.name, func(t *testing.T) {
			p, _, _, path := pagingFixture(t)
			before, _, err := p.MainWrites(path)
			if err != nil {
				t.Fatal(err)
			}
			if rr := pagingRequest(p, op.name, path, op.args); rr.Code != 204 {
				t.Fatal(rr.Body)
			}
			if _, _, err = p.MainWrites(path); err == nil {
				t.Fatal("active mutation accepted")
			}
			if err = p.Captured(path, before); err == nil {
				t.Fatal("snapshot accepted active mutation")
			}
			if rr := pagingRequest(p, "write-end", path, ""); rr.Code != 204 {
				t.Fatal(rr.Body)
			}
			if err = p.Captured(path, before); err == nil {
				t.Fatal("snapshot accepted changed generation")
			}
			after, captured, err := p.MainWrites(path)
			if err != nil || after != before+1 || captured != 0 {
				t.Fatalf("generation: %d %d %v", after, captured, err)
			}
			if err = p.Captured(path, after); err != nil {
				t.Fatal(err)
			}
			if rr := pagingRequest(p, "write-end", path, ""); rr.Code != 500 {
				t.Fatal("unmatched end accepted")
			}
			if err = p.Captured(path, after); err == nil {
				t.Fatal("failed view accepted")
			}
		})
	}
}

func TestPagingSequentialReadAheadBounded(t *testing.T) {
	p, _, _, path := pagingFixturePages(t, 1024)
	v, _ := p.lookup(path)
	read := func(pg uint32) {
		t.Helper()
		if err := v.ensure(t.Context(), int64(pg-1)*512, 512); err != nil {
			t.Fatal(err)
		}
	}
	read(1)
	if v.present.count != 1 {
		t.Fatal("point read expanded", v.present.count)
	}
	read(2)
	if v.present.count != 257 {
		t.Fatal("sequential miss must prefetch 128KiB", v.present.count)
	}
	for pg := uint32(3); pg <= 257; pg++ {
		read(pg)
	}
	if v.present.count != 257 {
		t.Fatal("cached reads extended window", v.present.count)
	}
	// A locally modified page within the next window must never be replaced.
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	dirty := bytes.Repeat([]byte{99}, 512)
	if _, err = f.WriteAt(dirty, 299*512); err != nil {
		t.Fatal(err)
	}
	v.present.mark(300)
	read(258)
	if v.present.count != 513 {
		t.Fatal("second bounded window", v.present.count)
	}
	got := make([]byte, 512)
	if _, err = f.ReadAt(got, 299*512); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, dirty) {
		t.Fatal("overwrote local page")
	}
	read(900)
	if v.present.count != 514 {
		t.Fatal("random read expanded", v.present.count)
	}
}

// A truncated page that was never loaded has no membership bit. It must still
// remain zero on regrowth, rather than being fetched from the old remote cut.
func TestPagingTruncateRegrowDoesNotHydrateDiscardedMissingPages(t *testing.T) {
	p, _, _, path := pagingFixture(t)
	if rr := pagingRequest(p, "truncate", path, "&size=512"); rr.Code != 204 {
		t.Fatal(rr.Body)
	}
	if err := os.Truncate(path, 512); err != nil {
		t.Fatal(err)
	}
	if rr := pagingRequest(p, "write-end", path, ""); rr.Code != 204 {
		t.Fatal(rr.Body)
	}
	if err := os.Truncate(path, 1536); err != nil {
		t.Fatal(err)
	}
	if rr := pagingRequest(p, "ensure", path, "&offset=512&length=1024"); rr.Code != 204 {
		t.Fatal(rr.Body)
	}
	if got := p.HydratedBytes(); got != 0 {
		t.Fatalf("discarded pages hydrated: %d", got)
	}
	if err := p.Hydrate(t.Context(), path); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1536 || !bytes.Equal(got[:512], bytes.Repeat([]byte{1}, 512)) || !bytes.Equal(got[512:], make([]byte, 1024)) {
		t.Fatal("regrowth resurrected missing original pages")
	}
	if n := p.HydratedBytes(); n != 512 {
		t.Fatalf("hydrated bytes=%d want512", n)
	}
}
