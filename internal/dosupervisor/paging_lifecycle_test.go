package dosupervisor

import (
	"bytes"
	"errors"
	"fmt"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"cellhive/internal/ltx"
	"cellhive/internal/wal"
)

func lifecycleRequest(p *Paging, op, path, session, params string) *httptest.ResponseRecorder {
	r := httptest.NewRequest("POST", "/internal/do-pages/"+op+"?path="+url.QueryEscape(path)+"&activation="+session+params, nil)
	r.Header.Set("Authorization", "Bearer "+p.token)
	w := httptest.NewRecorder()
	p.Handler().ServeHTTP(w, r)
	return w
}
func attachLifecycle(t *testing.T, p *Paging, path string) string {
	t.Helper()
	w := lifecycleRequest(p, "open", path, "", "")
	if w.Code != 200 {
		t.Fatalf("attach %d %s", w.Code, w.Body)
	}
	id := w.Header().Get("X-Cellhive-Activation")
	if len(id) != 48 {
		t.Fatal("missing activation")
	}
	return id
}
func requireLifecycle(t *testing.T, p *Paging, op, path, id, params string, code int) {
	t.Helper()
	w := lifecycleRequest(p, op, path, id, params)
	if w.Code != code {
		t.Fatalf("%s: %d %s want %d", op, w.Code, w.Body, code)
	}
}

func TestPagingLifecycleSparseReopenPinnedCutAndFloor(t *testing.T) {
	p, m, scope, path := pagingFixture(t)
	id := attachLifecycle(t, p, path)
	requireLifecycle(t, p, "ensure", path, id, "&offset=0&length=512", 204)
	requireLifecycle(t, p, "write", path, id, "&offset=0&length=512", 204)
	want := bytes.Repeat([]byte{99}, 512)
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.WriteAt(want, 0); err != nil {
		t.Fatal(err)
	}
	requireLifecycle(t, p, "write-end", path, id, "", 204)
	requireLifecycle(t, p, "truncate", path, id, "&size=512", 204)
	if err = f.Truncate(512); err != nil {
		t.Fatal(err)
	}
	requireLifecycle(t, p, "write-end", path, id, "", 204)
	if err = f.Truncate(1536); err != nil {
		t.Fatal(err)
	}
	f.Close()
	v, _ := p.lookup(path)
	writes := v.writes
	// New remote head must never become the source for this local activation.
	raw, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: 512, Commit: 3, MinTXID: 2, MaxTXID: 2}, []ltx.WALPage{{PageNo: 3, Data: bytes.Repeat([]byte{88}, 512)}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = m.Append(t.Context(), scope, 1, raw); err != nil {
		t.Fatal(err)
	}
	requireLifecycle(t, p, "detach", path, id, "", 204)
	if v.resident || v.pf.ResidentBytes() != 0 || len(v.present.blocks) != 0 {
		t.Fatal("payload retained after clean detach")
	}
	if v.floor != 1 || v.writes != writes || v.pf.Watermark() != 1 {
		t.Fatal("proof/cut changed during spill")
	}
	if _, err = os.Stat(p.guard(path)); err != nil {
		t.Fatal("sparse guard removed")
	}
	rr := lifecycleRequest(p, "open", path, "", "")
	if rr.Code != 200 || rr.Header().Get("X-Cellhive-Initial") != "false" || rr.Header().Get("X-Cellhive-Floor") != "1" {
		t.Fatalf("reopen %d %v", rr.Code, rr.Header())
	}
	next := rr.Header().Get("X-Cellhive-Activation")
	if next == id {
		t.Fatal("session reused")
	}
	requireLifecycle(t, p, "ensure", path, id, "&offset=0&length=512", 409)
	requireLifecycle(t, p, "ensure", path, next, "&offset=0&length=1536", 204)
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got[:512], want) || !bytes.Equal(got[512:], make([]byte, 1024)) {
		t.Fatal("overwritten/truncated page resurrected")
	}
}

func TestPagingLifecycleReopenMissingOriginalUsesExactIndex(t *testing.T) {
	p, m, scope, path := pagingFixture(t)
	id := attachLifecycle(t, p, path)
	requireLifecycle(t, p, "detach", path, id, "", 204)
	raw, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: 512, Commit: 3, MinTXID: 2, MaxTXID: 2}, []ltx.WALPage{{PageNo: 3, Data: bytes.Repeat([]byte{88}, 512)}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = m.Append(t.Context(), scope, 1, raw); err != nil {
		t.Fatal(err)
	}
	id = attachLifecycle(t, p, path)
	requireLifecycle(t, p, "ensure", path, id, "&offset=1024&length=512", 204)
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got[1024:], bytes.Repeat([]byte{3}, 512)) {
		t.Fatal("reopen rebased to newer remote head")
	}
}

func TestPagingLifecycleBorrowDelaysReclamation(t *testing.T) {
	p, _, scope, path := pagingFixture(t)
	id := attachLifecycle(t, p, path)
	cut, release, err := p.BorrowCut(path, scope)
	if err != nil {
		t.Fatal(err)
	}
	requireLifecycle(t, p, "detach", path, id, "", 204)
	v, _ := p.lookup(path)
	if !v.resident || cut.ResidentBytes() == 0 {
		t.Fatal("borrowed cut reclaimed")
	}
	if _, err = cut.Page(t.Context(), 3); err != nil {
		t.Fatal(err)
	}
	release()
	release()
	if v.resident || cut.ResidentBytes() != 0 || v.borrowers != 0 {
		t.Fatal("release did not reclaim once")
	}
}

func TestPagingLifecycleAmbiguousMutationAndPoisonRetained(t *testing.T) {
	for _, op := range []string{"mutation", "checkpoint", "poison"} {
		t.Run(op, func(t *testing.T) {
			p, _, _, path := pagingFixture(t)
			id := attachLifecycle(t, p, path)
			v, _ := p.lookup(path)
			switch op {
			case "mutation":
				requireLifecycle(t, p, "write", path, id, "&offset=0&length=512", 204)
			case "checkpoint":
				v.mu.Lock()
				v.checkpoint.copying = true
				v.mu.Unlock()
			case "poison":
				requireLifecycle(t, p, "poison", path, id, "", 204)
			}
			requireLifecycle(t, p, "detach", path, id, "", 503)
			requireLifecycle(t, p, "open", path, "", "", 503)
			if !v.resident || v.failed == nil {
				t.Fatal("lost poison or incomplete proof")
			}
		})
	}
}

func TestPagingLifecycleCapacityDoesNotPoison(t *testing.T) {
	p, m, scope, path := pagingFixture(t)
	p.limits.Active = 1
	p.limits.LocalBytes = 0
	id := attachLifecycle(t, p, path)
	requireLifecycle(t, p, "detach", path, id, "", 204)
	v, _ := p.lookup(path)
	if !v.resident || v.failed != nil {
		t.Fatal("spill capacity poisoned healthy view")
	}
	pf, err := m.NewPageFetcher(t.Context(), scope, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer pf.Close()
	other := filepath.Join(p.root, "other.sqlite")
	if err = p.Register(t.Context(), other, scope, pf); !errors.Is(err, ErrPagingCapacity) {
		t.Fatalf("admission %v", err)
	}
	if _, err = os.Stat(other); !os.IsNotExist(err) {
		t.Fatal("capacity refusal touched native database")
	}
	if _, err = os.Stat(p.guard(other)); !os.IsNotExist(err) {
		t.Fatal("capacity refusal created guard")
	}
	if v.failed != nil {
		t.Fatal("capacity poisoned existing activation")
	}
	id = attachLifecycle(t, p, path)
	requireLifecycle(t, p, "ensure", path, id, "&offset=0&length=512", 204)
}

func TestPagingLifecycleChurnBudgets(t *testing.T) {
	p, m, scope, first := pagingFixture(t)
	p.limits.Active = 2
	p.limits.Descriptors = 24
	for i := 0; i < 24; i++ {
		path := first
		if i != 0 {
			path = filepath.Join(p.root, fmt.Sprintf("db-%d.sqlite", i))
			pf, err := m.NewPageFetcher(t.Context(), scope, 1)
			if err != nil {
				t.Fatal(err)
			}
			if err = p.Register(t.Context(), path, scope, pf); err != nil {
				pf.Close()
				t.Fatal(err)
			}
		}
		id := attachLifecycle(t, p, path)
		requireLifecycle(t, p, "ensure", path, id, "&offset=512&length=512", 204)
		requireLifecycle(t, p, "detach", path, id, "", 204)
		if p.resident.Load() != 0 || p.localBytes.Load() > p.limits.LocalBytes {
			t.Fatal("churn exceeds payload budget")
		}
	}
	for path, v := range p.views {
		if v.resident || len(v.present.blocks) != 0 || v.pf.ResidentBytes() != 0 {
			t.Fatal("idle payload retained")
		}
		id := attachLifecycle(t, p, path)
		requireLifecycle(t, p, "detach", path, id, "", 204)
	}
	pf, err := m.NewPageFetcher(t.Context(), scope, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer pf.Close()
	if err = p.Register(t.Context(), filepath.Join(p.root, "overflow.sqlite"), scope, pf); !errors.Is(err, ErrPagingCapacity) {
		t.Fatalf("descriptor quota %v", err)
	}
	if len(p.views) != 24 {
		t.Fatal("descriptor quota grew")
	}
}

func TestPagingLifecycleCorruptSpillFailsClosed(t *testing.T) {
	p, _, _, path := pagingFixture(t)
	id := attachLifecycle(t, p, path)
	requireLifecycle(t, p, "detach", path, id, "", 204)
	v, _ := p.lookup(path)
	if err := os.WriteFile(p.statePath(v), []byte("corrupt"), 0600); err != nil {
		t.Fatal(err)
	}
	requireLifecycle(t, p, "open", path, "", "", 503)
	if v.failed == nil || v.resident || p.resident.Load() != 0 {
		t.Fatal("corrupt spill accepted or leaked resident reservation")
	}
	fresh, err := NewPaging(p.root, p.token)
	if err != nil {
		t.Fatal(err)
	}
	requireLifecycle(t, fresh, "open", path, "", "", 409)
}

func TestPagingLifecycleEndedCheckpointProofSurvivesReopen(t *testing.T) {
	p, _, _, path := pagingFixture(t)
	id := attachLifecycle(t, p, path)
	v, _ := p.lookup(path)
	v.mu.Lock()
	v.writes = 7
	v.captured = 7
	v.checkpointSerial = 11
	v.checkpoint = checkpointProof{id: 9, position: wal.Position{Valid: true, PageSize: 512, Commit: 3}}
	proof := v.checkpoint
	v.mu.Unlock()
	requireLifecycle(t, p, "detach", path, id, "", 204)
	if v.checkpoint != proof || v.checkpointSerial != 11 || v.writes != 7 || v.captured != 7 {
		t.Fatal("checkpoint proof lost on detach")
	}
	rr := lifecycleRequest(p, "open", path, "", "")
	if rr.Code != 200 || rr.Header().Get("X-Cellhive-Certificate") != "9" || rr.Header().Get("X-Cellhive-Checkpoint-Size") != "1536" {
		t.Fatalf("certificate reopen %d %v", rr.Code, rr.Header())
	}
	if v.checkpoint != proof || v.checkpointSerial != 11 || v.writes != 7 || v.captured != 7 {
		t.Fatal("checkpoint proof lost on reload")
	}
}
