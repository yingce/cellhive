package server

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/ltx"
	"cellhive/internal/upload"
)

type delayedObservationSink struct{ upload.Sink }

func (s delayedObservationSink) AppendBatch(ctx context.Context, sc cell.Scope, epoch uint64, segs [][]byte) (string, string, error) {
	time.Sleep(60 * time.Millisecond)
	return s.Sink.AppendBatch(ctx, sc, epoch, segs)
}

func TestSlowCommitObservationIdentifiesBucketWait(t *testing.T) {
	s, _ := newTestServer(t)
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "observed"}
	rr := do(t, s.Handler(), http.MethodPost, "/v1/internal/claim", "tok", []byte(`{"scope":"`+sc.String()+`"}`))
	if rr.Code != http.StatusOK {
		t.Fatalf("claim: %d %s", rr.Code, rr.Body.String())
	}
	ctx, cancel := context.WithCancel(t.Context())
	up := upload.New(delayedObservationSink{s.Replica}, s.Log, 64, 1<<20, time.Millisecond)
	up.Start(ctx)
	defer func() { cancel(); up.Stop() }()
	s.Uploader = up
	// Remove discovery to exercise the deterministic no-peer fallback.
	s.Lease = nil
	var buf bytes.Buffer
	s.Log = slog.New(slog.NewJSONHandler(&buf, nil))
	seg := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: 1, EndTxID: 1}, []byte("observed"))
	result, err := s.commitSegmentCore(ctx, sc, 1, seg, nil, false, false)
	if err != nil || result.Mode != "bucket-batch" {
		t.Fatalf("result: %+v %v", result, err)
	}
	var record map[string]any
	if err := json.Unmarshal(buf.Bytes(), &record); err != nil {
		t.Fatalf("log %s: %v", buf.String(), err)
	}
	if record["mode"] != "bucket-batch" || record["reason"] != "no_eligible_follower" || record["start_txid"] != float64(1) || record["end_txid"] != float64(1) {
		t.Fatalf("wrong correlation: %v", record)
	}
	if record["bucket_ms"].(float64) < 60 || record["replicate_ms"].(float64) != 0 {
		t.Fatalf("wrong attribution: %v", record)
	}
}
