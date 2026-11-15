package server

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/cellcapture"
	"cellhive/internal/control"
)

func TestForwardResponseLargeOwnerReads(t *testing.T) {
	dir := t.TempDir()
	a, oa, ha := newControlForwardServer(t, dir, "a")
	b, ob, hb := newControlForwardServer(t, dir, "b")
	oa.Advertise, ob.Advertise = ha.URL, hb.URL
	for _, s := range []*Server{a, b} {
		for _, kind := range []string{"kv", "d1"} {
			if err := registerTestBinding(s, "forward", kind, "main", "main"); err != nil {
				t.Fatal(err)
			}
		}
		s.Capture = &cellcapture.Manager{
			Store: s.Store, Committer: replicaCommitter{rep: s.Replica}, AutoSnapshot: true,
			Owner: func(ctx context.Context, sc cell.Scope) (bool, uint64, error) {
				o, _, err := s.Owner.ResolveRoute(ctx, sc)
				return o.Node == s.Owner.NodeID && o.Session == s.Owner.Session, o.Epoch, err
			},
		}
		t.Cleanup(func() { s.Capture.Stop(); s.WaitBackground(); _ = s.Store.Close() })
	}
	for _, sc := range []cell.Scope{control.Scope(), {Namespace: "forward", Class: "__kv__", ID: "main"}, {Namespace: "forward", Class: "__d1__", ID: "main"}} {
		if _, err := oa.Claim(t.Context(), sc, time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	call := func(t *testing.T, base, method, path, token string, body []byte) []byte {
		t.Helper()
		req, err := http.NewRequestWithContext(t.Context(), method, base+path, bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := ha.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		data, err := io.ReadAll(resp.Body)
		if err != nil || resp.StatusCode != http.StatusOK {
			t.Fatalf("%s %s: status=%d bytes=%d error=%v", method, path, resp.StatusCode, len(data), err)
		}
		return data
	}
	t.Run("KV", func(t *testing.T) {
		tok := mint(t, a, "forward", "kv", "main")
		payload := bytes.Repeat([]byte("large-kv"), (2<<20)/8)
		call(t, ha.URL, "POST", "/v1/kv/put?ns=forward&key=k", tok, payload)
		for _, base := range []string{ha.URL, hb.URL} {
			got := call(t, base, "GET", "/v1/kv/get?ns=forward&key=k", tok, nil)
			if !bytes.Equal(got, payload) {
				t.Fatalf("%s KV: got %d bytes, want all %d bytes", base, len(got), len(payload))
			}
		}
	})
	t.Run("D1Export", func(t *testing.T) {
		tok := mint(t, a, "forward", "d1", "main")
		call(t, ha.URL, "POST", "/v1/d1/exec?ns=forward&db=main", tok,
			[]byte(`{"sql":"CREATE TABLE witness(v BLOB); INSERT INTO witness VALUES (zeroblob(1048576));"}`))
		path := "/v1/d1/export?ns=forward&db=main"
		want := call(t, ha.URL, "GET", path, tok, nil)
		if len(want) < 2<<20 || !bytes.Contains(want, []byte("CREATE TABLE witness")) {
			t.Fatalf("invalid export fixture: %d bytes", len(want))
		}
		got := call(t, hb.URL, "GET", path, tok, nil)
		if !bytes.Equal(got, want) {
			t.Fatalf("forwarded SQL export: got %d bytes, want all %d bytes", len(got), len(want))
		}
	})
}

func TestForwardResponseAbortsOnUpstreamReadError(t *testing.T) {
	// Flush a substantial prefix, then terminate without the final chunk.
	// The forwarder must not turn the upstream read error into a clean EOF.
	prefix := bytes.Repeat([]byte("x"), 64<<10)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(prefix)
		w.(http.Flusher).Flush()
		panic(http.ErrAbortHandler)
	}))
	defer upstream.Close()
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req, err := http.NewRequestWithContext(r.Context(), "GET", upstream.URL, nil)
		if err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		resp, err := upstream.Client().Do(req)
		if err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()
		copyForwardResponse(w, resp)
	}))
	defer proxy.Close()
	resp, err := proxy.Client().Get(proxy.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	got, err := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK || len(got) == 0 {
		t.Fatalf("expected already-started successful response: status=%d bytes=%d", resp.StatusCode, len(got))
	}
	if err == nil {
		t.Fatalf("truncated upstream returned clean EOF: status=%d bytes=%d", resp.StatusCode, len(got))
	}
}
