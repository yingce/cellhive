package server

import (
	"bytes"
	"context"
	"encoding/base64"
	"hash/fnv"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/timer"
)

type countingKVWake struct {
	mu      sync.Mutex
	puts    int
	deletes int
}

func (w *countingKVWake) Put(context.Context, string, int64, string, string) error {
	w.mu.Lock()
	w.puts++
	w.mu.Unlock()
	return nil
}

func (w *countingKVWake) Delete(context.Context, string) error {
	w.mu.Lock()
	w.deletes++
	w.mu.Unlock()
	return nil
}

func (w *countingKVWake) counts() (int, int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.puts, w.deletes
}

func TestKVExpiryReusesDurableEarlyTimer(t *testing.T) {
	ctx := t.Context()
	s := newScopeServer(t)
	s.Timers = timer.NewRegistry()
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "main"}
	c, err := s.Store.Cell(ctx, sc)
	if err != nil {
		t.Fatal(err)
	}
	wake := &countingKVWake{}
	opens := 0
	s.OpenTimer = func(ctx context.Context, scope cell.Scope) (*timer.Store, error) {
		opens++
		st, err := timer.NewStore(ctx, c)
		if err == nil {
			st.Index = wake
		}
		return st, err
	}
	base := time.Now().Add(2 * time.Hour).UnixMilli()
	put := func(key string, due int64) {
		t.Helper()
		if _, err := c.PutTxOpts(ctx, key, []byte("v"), nil, due); err != nil {
			t.Fatal(err)
		}
		s.syncKVExpiry(ctx, sc)
	}
	due := func() int64 {
		t.Helper()
		st, err := timer.NewStore(ctx, c)
		if err != nil {
			t.Fatal(err)
		}
		got, exists, err := st.KVExpiryDue(ctx)
		if err != nil || !exists {
			t.Fatalf("KV expiry timer = %d/%t/%v", got, exists, err)
		}
		return got
	}
	put("first", base)
	firstPuts, firstDeletes := wake.counts()
	if opens != 1 || firstPuts == 0 || due() != base {
		t.Fatalf("initial timer opens=%d puts=%d due=%d", opens, firstPuts, due())
	}
	// A later key must reuse the durable first wakeup, including after the
	// process-local registry is replaced as it would be after a restart.
	s.Timers = timer.NewRegistry()
	put("later", base+time.Hour.Milliseconds())
	if puts, deletes := wake.counts(); opens != 1 || puts != firstPuts || deletes != firstDeletes || due() != base {
		t.Fatalf("redundant timer mutation opens=%d puts=%d deletes=%d due=%d", opens, puts, deletes, due())
	}
	put("earlier", base-time.Hour.Milliseconds())
	if puts, _ := wake.counts(); opens != 2 || puts <= firstPuts || due() != base-time.Hour.Milliseconds() {
		t.Fatalf("earlier expiry not published opens=%d puts=%d due=%d", opens, puts, due())
	}
	// The dispatcher deletes an already-due timer after handling it. It cannot
	// be reused for a later key that still needs a wakeup.
	st, err := timer.NewStore(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.RemoveByOccurrence(ctx, string(timer.KindKVExpire)); err != nil {
		t.Fatal(err)
	}
	if err := st.Upsert(ctx, timer.New(time.Now().Add(-time.Second).UnixMilli(), timer.KindKVExpire, sc.String(), string(timer.KindKVExpire))); err != nil {
		t.Fatal(err)
	}
	s.syncKVExpiry(ctx, sc)
	if got := due(); got != base-time.Hour.Milliseconds() {
		t.Fatalf("already-due timer was reused: %d", got)
	}
	// Removing all expiries must disarm; re-querying SQLite prevents a stale
	// cleanup result from deleting a new timer after a concurrent put.
	for _, key := range []string{"first", "later", "earlier"} {
		if _, err := c.PutTxOpts(ctx, key, []byte("v"), nil, 0); err != nil {
			t.Fatal(err)
		}
	}
	s.syncKVExpiry(ctx, sc)
	st, err = timer.NewStore(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	if got, exists, err := st.KVExpiryDue(ctx); err != nil || exists || got != 0 {
		t.Fatalf("timer after clearing TTL = %d/%t/%v", got, exists, err)
	}
}

func TestKVExpiryConcurrentReconcileKeepsEarliest(t *testing.T) {
	ctx := t.Context()
	s := newScopeServer(t)
	s.Timers = timer.NewRegistry()
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "main"}
	c, err := s.Store.Cell(ctx, sc)
	if err != nil {
		t.Fatal(err)
	}
	s.OpenTimer = func(ctx context.Context, scope cell.Scope) (*timer.Store, error) { return timer.NewStore(ctx, c) }
	base := time.Now().Add(2 * time.Hour).UnixMilli()
	var wg sync.WaitGroup
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			due := base + int64(i-12)*time.Minute.Milliseconds()
			if _, err := c.PutTxOpts(ctx, strconv.Itoa(i), []byte("v"), nil, due); err != nil {
				t.Errorf("put %d: %v", i, err)
				return
			}
			s.syncKVExpiry(ctx, sc)
		}(i)
	}
	wg.Wait()
	st, err := timer.NewStore(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	got, exists, err := st.KVExpiryDue(ctx)
	if err != nil || !exists || got != base-12*time.Minute.Milliseconds() {
		t.Fatalf("concurrent timer = %d/%t/%v, want %d", got, exists, err, base-12*time.Minute.Milliseconds())
	}
}

// TestKVTTLAndMetadataE2E covers ADR-098: metadata round-trips, an expired key
// is invisible, and the timer-driven internal cleanup endpoint deletes it.
func TestKVTTLAndMetadataE2E(t *testing.T) {
	ctx := context.Background()
	s := newScopeServer(t)
	if _, err := s.Control.CreateResource(ctx, "acme", "kv", "KV", "default", "test"); err != nil {
		t.Fatalf("resource: %v", err)
	}
	tok := mint(t, s, "acme", "kv", "KV")
	h := s.Handler()
	do := func(method, target, token string, body []byte) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, target, bytes.NewReader(body))
		if token != "" {
			req.Header.Set("x-cellhive-scope-token", token)
		}
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		return rr
	}

	meta := base64.StdEncoding.EncodeToString([]byte(`{"n":1}`))
	if rr := do(http.MethodPost, "/v1/kv/put?ns=acme&key=m&metadata="+url.QueryEscape(meta), tok, []byte("vm")); rr.Code != http.StatusOK {
		t.Fatalf("put m = %d %s", rr.Code, rr.Body.String())
	}
	rr := do(http.MethodGet, "/v1/kv/get?ns=acme&key=m", tok, nil)
	if rr.Code != http.StatusOK || rr.Body.String() != "vm" {
		t.Fatalf("get m = %d %q", rr.Code, rr.Body.String())
	}
	if got := rr.Header().Get("x-cellhive-kv-metadata"); got != meta {
		t.Fatalf("metadata header = %q, want %q", got, meta)
	}

	// A key past its deadline is invisible from the moment it expires. An
	// Arm a native one-second TTL and verify actual read-time expiry.
	if rr := do(http.MethodPost, "/v1/kv/put?ns=acme&key=e&expiration_ttl=1", tok, []byte("ve")); rr.Code != http.StatusOK {
		t.Fatalf("put e = %d %s", rr.Code, rr.Body.String())
	}
	time.Sleep(1200 * time.Millisecond)
	if rr := do(http.MethodGet, "/v1/kv/get?ns=acme&key=e", tok, nil); rr.Code != http.StatusNotFound {
		t.Fatalf("get expired = %d, want 404", rr.Code)
	}

	// The internal cleanup endpoint deletes the expired row (captured write).
	req := httptest.NewRequest(http.MethodPost, "/v1/internal/kv/expire?scope=acme/__kv__/default", nil)
	req.Header.Set("x-cellhive-internal-token", "tok")
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expire = %d %s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), `"deleted":1`) {
		t.Fatalf("expire body = %s, want deleted:1", rr.Body.String())
	}
}

// TestKVValidationLimits covers the documented KV write limits: key <= 512 B,
// metadata <= 1 KiB, any positive expirationTtl, and an absolute expiration
// must be in the future.
func TestKVValidationLimits(t *testing.T) {
	ctx := context.Background()
	s := newScopeServer(t)
	if _, err := s.Control.CreateResource(ctx, "acme", "kv", "KV", "default", "test"); err != nil {
		t.Fatalf("resource: %v", err)
	}
	tok := mint(t, s, "acme", "kv", "KV")
	h := s.Handler()
	do := func(target string, body []byte) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, target, bytes.NewReader(body))
		req.Header.Set("x-cellhive-scope-token", tok)
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		return rr
	}

	longKey := strings.Repeat("k", 513)
	if rr := do("/v1/kv/put?ns=acme&key="+longKey, []byte("v")); rr.Code != http.StatusBadRequest || !strings.Contains(rr.Body.String(), "key_too_large") {
		t.Fatalf("long key = %d %s, want 400 key_too_large", rr.Code, rr.Body.String())
	}
	bigMeta := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte("m"), 1025))
	if rr := do("/v1/kv/put?ns=acme&key=m&metadata="+url.QueryEscape(bigMeta), []byte("v")); rr.Code != http.StatusBadRequest || !strings.Contains(rr.Body.String(), "metadata_too_large") {
		t.Fatalf("big metadata = %d %s, want 400 metadata_too_large", rr.Code, rr.Body.String())
	}
	future := time.Now().Add(time.Hour).Unix()
	for _, ttl := range []string{"1", "10", "59"} {
		if rr := do("/v1/kv/put?ns=acme&key=m&expiration_ttl="+ttl, []byte("v")); rr.Code != http.StatusOK {
			t.Fatalf("ttl %s = %d %s, want 200", ttl, rr.Code, rr.Body.String())
		}
	}
	if rr := do("/v1/kv/put?ns=acme&key=m&expiration_ttl=60", []byte("v")); rr.Code != http.StatusOK {
		t.Fatalf("ttl 60 = %d %s, want 200", rr.Code, rr.Body.String())
	}
	zero := time.Now().Add(-time.Minute).Unix()
	if rr := do("/v1/kv/put?ns=acme&key=m&expiration_ttl=0", []byte("v")); rr.Code != http.StatusBadRequest {
		t.Fatalf("ttl 0 = %d %s, want 400", rr.Code, rr.Body.String())
	}
	_ = zero
	past := time.Now().Add(-time.Minute).Unix()
	if rr := do("/v1/kv/put?ns=acme&key=m&expiration="+strconv.FormatInt(past, 10), []byte("v")); rr.Code != http.StatusBadRequest || !strings.Contains(rr.Body.String(), "future") {
		t.Fatalf("past expiration = %d %s, want 400 expiration in the future", rr.Code, rr.Body.String())
	}
	if rr := do("/v1/kv/put?ns=acme&key=m&expiration="+strconv.FormatInt(future, 10), []byte("v")); rr.Code != http.StatusOK {
		t.Fatalf("future expiration = %d %s, want 200", rr.Code, rr.Body.String())
	}
	// A 1 KiB metadata value (exactly at the limit) is accepted.
	okMeta := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte("m"), 1024))
	if rr := do("/v1/kv/put?ns=acme&key=m&metadata="+url.QueryEscape(okMeta), []byte("v")); rr.Code != http.StatusOK {
		t.Fatalf("1KiB metadata = %d %s, want 200", rr.Code, rr.Body.String())
	}
}

func TestKVExpiryFastPathDoesNotTakeStripeLock(t *testing.T) {
	ctx := t.Context()
	s := newScopeServer(t)
	s.Timers = timer.NewRegistry()
	sc := cell.Scope{Namespace: "acme", Class: "__kv__", ID: "fastpath"}
	c, err := s.Store.Cell(ctx, sc)
	if err != nil {
		t.Fatal(err)
	}
	s.OpenTimer = func(ctx context.Context, scope cell.Scope) (*timer.Store, error) {
		return timer.NewStore(ctx, c)
	}
	due := time.Now().Add(time.Hour).UnixMilli()
	if _, err := c.PutTxOpts(ctx, "k", []byte("v"), nil, due); err != nil {
		t.Fatal(err)
	}
	s.syncKVExpiry(ctx, sc)
	st, err := timer.NewStore(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	if got, exists, err := st.KVExpiryDue(ctx); err != nil || !exists || got != due {
		t.Fatalf("timer = %d/%t/%v, want %d", got, exists, err, due)
	}

	h := fnv.New32a()
	_, _ = h.Write([]byte(sc.String()))
	mu := &s.kvExpiryMu[h.Sum32()%uint32(len(s.kvExpiryMu))]
	mu.Lock()
	defer mu.Unlock()
	done := make(chan struct{})
	go func() {
		s.syncKVExpiry(ctx, sc)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("covered expiry blocked on the striped reconcile lock")
	}
}
