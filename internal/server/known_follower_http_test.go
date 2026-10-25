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
	"sync"
	"testing"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/lease"
	"cellhive/internal/ltx"
)

type blockedFollowerDiscovery struct {
	bucket.Bucket
	started chan struct{}
	release chan struct{}
	done    chan struct{}
	once    sync.Once
}

func (b *blockedFollowerDiscovery) List(ctx context.Context, p string) ([]string, error) {
	b.once.Do(func() { close(b.started) })
	select {
	case <-b.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	keys, err := []string{}, error(nil)
	select {
	case b.done <- struct{}{}:
	default:
	}
	return keys, err
}

// Real HTTP owner commit -> follower append/fsync while discovery is stuck.
// A valid refreshed lease must give a fleet proof; an expired authoritative
// record must still force a bucket proof, rather than weaken durability.
func TestKnownFollowerHTTPWithBlockedDiscovery(t *testing.T) {
	owner, _ := newTestServer(t)
	follower, _ := newTestServer(t)
	fh := httptest.NewServer(follower.Handler())
	defer fh.Close()
	oh := httptest.NewServer(owner.Handler())
	defer oh.Close()
	call := func(path string, body []byte) []byte {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, oh.URL+path, bytes.NewReader(body))
		req.Header.Set("x-cellhive-internal-token", "tok")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s: %d %s", path, resp.StatusCode, raw)
		}
		return raw
	}
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "known-http"}
	call("/v1/internal/claim", []byte(`{"scope":"`+sc.String()+`"}`))
	original := lease.NodeLease{Node: "node-2", Session: "peer-session", Instance: "peer-instance", PeerURL: fh.URL, Liveness: lease.NodeLeaseLiveness, Expiry: time.Now().Add(100 * time.Millisecond).UnixMilli()}
	fresh := original
	fresh.Expiry = time.Now().Add(5 * time.Second).UnixMilli()
	publish := func(l lease.NodeLease) {
		t.Helper()
		raw, _ := json.Marshal(l)
		if _, err := owner.Bucket.Put(t.Context(), lease.Key(l.Node), raw); err != nil {
			t.Fatal(err)
		}
	}
	publish(fresh)
	b := &blockedFollowerDiscovery{Bucket: owner.Bucket, started: make(chan struct{}), release: make(chan struct{}), done: make(chan struct{}, 1)}
	owner.Lease.B = b
	owner.Lease.TTL = time.Second
	owner.Lease.SetCachedForTest([]lease.NodeLease{original})
	owner.Lease.SampleCached(t.Context(), time.Nanosecond)
	select {
	case <-b.started:
	case <-time.After(time.Second):
		t.Fatal("discovery did not start")
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); owner.Lease.RunKnownRefresh(ctx) }()
	defer func() { cancel(); <-done; close(b.release); <-b.done }()
	await := func(expiry int64) {
		t.Helper()
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			l, ok, err := owner.Lease.NodeLease(t.Context(), original.Node)
			if err == nil && ok && l.Expiry == expiry {
				return
			}
			time.Sleep(time.Millisecond)
		}
		t.Fatal("known follower did not refresh")
	}
	await(fresh.Expiry)
	// Keep making real requests past the original sample's expiry.
	var txid uint64
	for time.Now().UnixMilli() <= original.Expiry+100 {
		txid++
		seg := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: txid, EndTxID: txid}, []byte(fmt.Sprint(txid)))
		raw := call("/v1/internal/commit", []byte(fmt.Sprintf(`{"scope":%q,"epoch":1,"segment":%q}`, sc.String(), base64.StdEncoding.EncodeToString(seg))))
		var result struct {
			Mode    string `json:"mode"`
			AckedBy string `json:"acked_by"`
		}
		if err := json.Unmarshal(raw, &result); err != nil {
			t.Fatal(err)
		}
		if result.Mode != "fleet" || result.AckedBy != fh.URL {
			t.Fatalf("healthy known peer fell back: %s", raw)
		}
	}
	segs, err := follower.Spool.Segments(sc, 1)
	if err != nil || len(segs) != int(txid) {
		t.Fatalf("durable follower segments=%d want=%d err=%v", len(segs), txid, err)
	}
	// No fake extension: the source of truth now says the node is expired.
	expired := fresh
	expired.Expiry = time.Now().Add(-time.Millisecond).UnixMilli()
	publish(expired)
	await(expired.Expiry)
	txid++
	seg := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: txid, EndTxID: txid}, []byte("expired"))
	raw := call("/v1/internal/commit", []byte(fmt.Sprintf(`{"scope":%q,"epoch":1,"segment":%q}`, sc.String(), base64.StdEncoding.EncodeToString(seg))))
	var result struct {
		Mode    string `json:"mode"`
		AckedBy string `json:"acked_by"`
	}
	_ = json.Unmarshal(raw, &result)
	if result.Mode != "bucket-batch" {
		t.Fatalf("expired peer got fleet proof: %s", raw)
	}
	t.Logf("real HTTP fleet commits=%d; expired peer bucket fallback=PASS", txid-1)
}
