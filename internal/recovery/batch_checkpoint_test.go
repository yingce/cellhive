package recovery_test

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/config"
	"cellhive/internal/ltx"
	"cellhive/internal/nodelog"
	"cellhive/internal/peer"
	"cellhive/internal/recovery"
	"cellhive/internal/replica"
	"cellhive/internal/server"
)

type failSecondBatch struct {
	bucket.Bucket
	fail   bool
	writes int
}

func (b *failSecondBatch) ConditionalCreate(ctx context.Context, key string, data []byte) (string, error) {
	if strings.Contains(key, "/ltx/") {
		b.writes++
		if b.fail && b.writes == 2 {
			return "", errors.New("injected second batch failure")
		}
	}
	return b.Bucket.ConditionalCreate(ctx, key, data)
}

func TestRecoveryBatchFailureDoesNotCheckpointOrReclaim(t *testing.T) {
	ctx := t.Context()
	fs, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fb := &failSecondBatch{Bucket: fs, fail: true}
	spool, err := peer.NewSpool(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	sc := cell.Scope{Namespace: "batch", Class: "__kv__", ID: "checkpoint"}
	for i := uint64(1); i <= 100; i++ {
		seg := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: i, EndTxID: i}, []byte{byte(i)})
		if _, _, err := spool.AppendBatchSession(ctx, "session", sc, 1, [][]byte{seg}); err != nil {
			t.Fatal(err)
		}
	}
	follower := server.New(server.Deps{Cfg: config.Config{TokenPeer: "tok", TokenInternal: "tok"}, Spool: spool})
	http := httptest.NewServer(follower.Handler())
	defer http.Close()
	nl := nodelog.New(fb, "dead")
	if _, err := nl.Open(ctx, "session", 1, []string{http.URL}); err != nil {
		t.Fatal(err)
	}
	rep := replica.New(fb)
	rec := recovery.New(rep, peer.NewHTTPTransport("tok", nil))
	if n, err := rec.Recover(ctx, nl, "dead", "session"); err == nil || n != 64 {
		t.Fatalf("partial recover=%d %v", n, err)
	}
	record, _, err := nl.Get(ctx, "dead", "session")
	if err != nil {
		t.Fatal(err)
	}
	if record.Status == nodelog.StatusSealed || record.Progress[http.URL] != 0 {
		t.Fatalf("premature proof: %+v", record)
	}
	// Retry must recover all data from the follower, including already-uploaded
	// first batch, without immutable-object conflicts or lost acknowledged tail.
	fb.fail = false
	if n, err := rec.Recover(ctx, nl, "dead", "session"); err != nil || n != 100 {
		t.Fatalf("retry=%d %v", n, err)
	}
	segs, err := rep.Restore(ctx, sc, 1)
	if err != nil || len(segs) != 100 {
		t.Fatalf("restored=%d %v", len(segs), err)
	}
	keys, err := rep.ListSegments(ctx, sc, 1)
	if err != nil || len(keys) != 2 {
		t.Fatalf("objects=%v %v", keys, err)
	}
	record, _, err = nl.Get(ctx, "dead", "session")
	if err != nil || record.Status != nodelog.StatusSealed || record.Progress[http.URL] != 100 {
		t.Fatalf("final proof=%+v %v", record, err)
	}
}
