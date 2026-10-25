package server

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/ltx"
	"cellhive/internal/peer"
)

// End-to-end HTTP: a peer that accepts TCP but stalls the upgrade must not
// strand the owner commit. Its bucket proof must exist before HTTP success.
func TestPeerStreamStallBucketProofAndRecoveryHTTP(t *testing.T) {
	owner, _ := newTestServer(t)
	follower, _ := newTestServer(t)
	var stalled atomic.Bool
	stalled.Store(true)
	fh := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/peer/stream" && stalled.Load() {
			c, _, e := w.(http.Hijacker).Hijack()
			if e != nil {
				return
			}
			defer c.Close()
			io.Copy(io.Discard, c)
			return
		}
		follower.Handler().ServeHTTP(w, r)
	}))
	defer fh.Close()
	tr := peer.NewSessionStreamTransport("tok", "recovery-http-session", 1, 4)
	defer tr.Close()
	owner.PeerMgr = peer.NewManager(tr)
	shipCtx, stopShip := context.WithCancel(context.Background())
	shipper := peer.NewShipBatcher(tr, 256, 4<<20, time.Millisecond, 1, 1, 0, 0)
	shipper.Start(shipCtx)
	defer func() { stopShip(); shipper.Stop() }()
	owner.PeerShipper = shipper
	oh := httptest.NewServer(owner.Handler())
	defer oh.Close()
	client := &http.Client{Timeout: 10 * time.Second}
	call := func(path string, body []byte) []byte {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, oh.URL+path, bytes.NewReader(body))
		req.Header.Set("x-cellhive-internal-token", "tok")
		resp, e := client.Do(req)
		if e != nil {
			t.Fatal(e)
		}
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != 200 {
			t.Fatalf("%s: %d %s", path, resp.StatusCode, raw)
		}
		return raw
	}
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "stream-stall"}
	call("/v1/internal/claim", []byte(fmt.Sprintf(`{"scope":%q}`, sc.String())))
	commit := func(txid uint64) string {
		seg := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: txid, EndTxID: txid}, []byte(fmt.Sprint(txid)))
		raw := call("/v1/internal/commit", []byte(fmt.Sprintf(`{"scope":%q,"epoch":1,"segment":%q,"followers":[%q]}`, sc.String(), base64.StdEncoding.EncodeToString(seg), fh.URL)))
		var result struct{ Mode string }
		if e := json.Unmarshal(raw, &result); e != nil {
			t.Fatal(e)
		}
		return result.Mode
	}
	start := time.Now()
	if mode := commit(1); mode != "bucket-batch" {
		t.Fatalf("unconfirmed peer mode=%s", mode)
	}
	if elapsed := time.Since(start); elapsed > 8*time.Second {
		t.Fatalf("unbounded fallback %s", elapsed)
	}
	keys, err := owner.Replica.ListSegments(context.Background(), sc, 1)
	if err != nil || len(keys) == 0 {
		t.Fatalf("HTTP succeeded without bucket segment: keys=%v err=%v", keys, err)
	}
	found := false
	expected := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: 1, EndTxID: 1}, []byte("1"))
	for _, key := range keys {
		raw, _, e := owner.Bucket.Get(context.Background(), key)
		if e != nil {
			t.Fatal(e)
		}
		found = found || bytes.Contains(raw, expected)
	}
	if !found {
		t.Fatal("HTTP success preceded the actual bucket segment")
	}
	stalled.Store(false)
	if mode := commit(2); mode != "fleet" {
		t.Fatalf("recovered peer mode=%s", mode)
	}
	tail, err := follower.Spool.TailOpenPage("recovery-http-session", 0, 1, 10, 1<<20)
	if err != nil || len(tail) != 1 {
		t.Fatalf("recovered follower missing durable frame: %d %v", len(tail), err)
	}
	t.Logf("stalled handshake returned bucket proof; next commit restored fleet proof with %d durable frame", len(tail))
}
