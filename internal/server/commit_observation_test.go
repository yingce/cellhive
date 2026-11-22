package server

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"
	"testing"
	"time"

	"cellhive/internal/cell"
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
	seg := nativeTestFile(t, 1, 1, []byte("observed"))
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

type countingBatchSink struct {
	mu    sync.Mutex
	calls int
	segs  int
}

func (s *countingBatchSink) AppendBatch(_ context.Context, _ cell.Scope, _ uint64, segs [][]byte) (string, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	s.segs += len(segs)
	return "", "", nil
}

func TestSerialBucketCommitsShareOneUpload(t *testing.T) {
	s, _ := newTestServer(t)
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "batched"}
	rr := do(t, s.Handler(), http.MethodPost, "/v1/internal/claim", "tok", []byte(`{"scope":"`+sc.String()+`"}`))
	if rr.Code != http.StatusOK {
		t.Fatalf("claim: %d %s", rr.Code, rr.Body.String())
	}
	sink := &countingBatchSink{}
	ctx, cancel := context.WithCancel(t.Context())
	up := upload.New(sink, s.Log, 256, 1<<20, time.Hour)
	spool, err := upload.OpenSpool(t.TempDir(), 100)
	if err != nil {
		t.Fatal(err)
	}
	up.SetSpool(spool)
	up.Start(ctx)
	defer func() {
		cancel()
		up.Stop()
	}()
	s.Uploader = up
	s.Lease = nil
	res, err := s.commitSegmentCore(ctx, sc, 1, nativeTestFile(t, 1, 1, []byte("a")), nil, false, false)
	if err != nil {
		t.Fatal(err)
	}
	if res.Mode != "bucket-spool" {
		t.Fatalf("mode = %q, want bucket-spool", res.Mode)
	}
	if _, err := s.commitSegmentCore(ctx, sc, 1, nativeTestFile(t, 2, 2, []byte("b")), nil, false, false); err != nil {
		t.Fatal(err)
	}
	sink.mu.Lock()
	earlyCalls := sink.calls
	sink.mu.Unlock()
	if earlyCalls != 0 {
		t.Fatalf("serial commits uploaded %d times before the flush", earlyCalls)
	}
	if err := up.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	sink.mu.Lock()
	defer sink.mu.Unlock()
	if sink.calls != 1 || sink.segs != 2 {
		t.Fatalf("flush calls=%d segs=%d, want 1 call of 2 segments", sink.calls, sink.segs)
	}
}
