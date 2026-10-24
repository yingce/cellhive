package peer

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/ltx"
)

func TestDecodeBatchFramesDrainsOnlyCompleteBufferedFrames(t *testing.T) {
	a := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "a"}
	b := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "b"}
	seg1 := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: 1, EndTxID: 1}, []byte("a"))
	seg2 := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 2, StartTxID: 1, EndTxID: 1}, []byte("b"))
	first := encodeBatchFrame(a, 1, [][]byte{seg1})
	second := encodeBatchFrame(b, 2, [][]byte{seg2})
	partial := encodeBatchFrame(a, 1, [][]byte{seg1})
	data := append(append(append([]byte{}, first...), second...), partial[:len(partial)-1]...)
	r := bufio.NewReaderSize(bytes.NewReader(data), len(data))

	frames, err := DecodeBatchFrames(r, 64, len(data))
	if err != nil {
		t.Fatalf("decode buffered frames: %v", err)
	}
	if len(frames) != 2 || frames[0].Scope != a || frames[1].Scope != b {
		t.Fatalf("frames = %+v, want complete a,b frames", frames)
	}
	if got := r.Buffered(); got != len(partial)-1 {
		t.Fatalf("buffered bytes = %d, want incomplete frame bytes %d", got, len(partial)-1)
	}
}

func TestDecodeBatchFramesHonorsGroupByteLimit(t *testing.T) {
	a := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "limit"}
	seg := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: 1, EndTxID: 1}, bytes.Repeat([]byte("x"), 256))
	frame := encodeBatchFrame(a, 1, [][]byte{seg})
	r := bufio.NewReaderSize(bytes.NewReader(append(append([]byte{}, frame...), frame...)), len(frame)*2)
	frames, err := DecodeBatchFrames(r, 64, len(frame))
	if err != nil {
		t.Fatalf("decode limited group: %v", err)
	}
	if len(frames) != 1 || r.Buffered() != len(frame) {
		t.Fatalf("frames=%d buffered=%d, want 1 and %d", len(frames), r.Buffered(), len(frame))
	}
}

func TestSessionStreamDoesNotFallbackToLegacyAppend(t *testing.T) {
	legacyAppends := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/peer/stream":
			if r.Header.Get("Upgrade") == StreamProtocol {
				http.Error(w, "session protocol unsupported", http.StatusBadRequest)
				return
			}
			http.Error(w, "unexpected protocol", http.StatusBadRequest)
		case "/v1/peer/append_batch":
			legacyAppends++
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"status":"fsynced"}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	transport := NewSessionStreamTransport("tok", "owner-session", 1, 1)
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "upgrade"}
	seg := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: 1, EndTxID: 1}, []byte("x"))
	if err := transport.AppendBatch(context.Background(), srv.URL, sc, 1, [][]byte{seg}); err == nil {
		t.Fatal("session stream unexpectedly succeeded against legacy follower")
	}
	if legacyAppends != 0 {
		t.Fatalf("legacy append fallback count = %d, want 0", legacyAppends)
	}
}

func TestSessionHTTPAppendRequiresV2Proof(t *testing.T) {
	legacy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"status":"fsynced"}`)
	}))
	defer legacy.Close()
	transport := NewHTTPTransport("tok", nil).WithSession("owner-session")
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "http-v2"}
	seg := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: 1, EndTxID: 1}, []byte("x"))
	if err := transport.Append(context.Background(), legacy.URL, sc, 1, seg); err == nil {
		t.Fatal("session append accepted legacy response without v2 proof")
	}
	if err := transport.AppendBatch(context.Background(), legacy.URL, sc, 1, [][]byte{seg}); err == nil {
		t.Fatal("session batch accepted legacy response without v2 proof")
	}
}

func TestSpoolAppendListRead(t *testing.T) {
	ctx := context.Background()
	sp, err := NewSpool(t.TempDir())
	if err != nil {
		t.Fatalf("spool: %v", err)
	}
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "main"}
	seg := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 2, StartTxID: 1, EndTxID: 1}, []byte("payload"))

	p, err := sp.Append(ctx, sc, 2, seg)
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if _, err := os.Stat(p); err != nil {
		t.Fatalf("spooled log missing: %v", err)
	}
	keys, err := sp.List(sc, 2)
	h, _, _ := ltx.Decode(seg)
	if err != nil || len(keys) != 1 || keys[0] != ltx.SegmentName(h) {
		t.Fatalf("list = %v, %v", keys, err)
	}
	got, err := sp.Segments(sc, 2)
	if err != nil || len(got) != 1 || string(got[0]) != string(seg) {
		t.Fatalf("read mismatch: %v", err)
	}
}

func TestSpoolAppendBatchOneLogPreservesOrder(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	sp, _ := NewSpool(dir)
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "batch"}

	var segs [][]byte
	for i := 0; i < 50; i++ {
		segs = append(segs, ltx.Encode(ltx.Header{
			Kind: ltx.KindDelta, Epoch: 1,
			StartTxID: uint64(i + 1), EndTxID: uint64(i + 1),
		}, []byte{byte(i)}))
	}
	p, _, err := sp.AppendBatch(ctx, sc, 1, segs)
	if err != nil {
		t.Fatalf("append batch: %v", err)
	}
	// One append-log file, not one file per segment.
	entries, _ := os.ReadDir(filepath.Dir(p))
	if len(entries) != 1 || entries[0].Name() != "segments.log" {
		t.Fatalf("expected a single log file, got %v", entries)
	}
	got, err := sp.Segments(sc, 1)
	if err != nil || len(got) != 50 {
		t.Fatalf("segments = %d, %v", len(got), err)
	}
	for i := range got {
		if string(got[i]) != string(segs[i]) {
			t.Fatalf("order broken at %d", i)
		}
	}
}

func TestSpoolHeldReadsFramedLog(t *testing.T) {
	ctx := context.Background()
	sp, _ := NewSpool(t.TempDir())
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "held"}
	var segs [][]byte
	for i := 0; i < 5; i++ {
		segs = append(segs, ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 3, StartTxID: uint64(i + 1), EndTxID: uint64(i + 1)}, []byte("x")))
	}
	if _, _, err := sp.AppendBatch(ctx, sc, 3, segs); err != nil {
		t.Fatalf("append batch: %v", err)
	}
	held, err := sp.Held()
	if err != nil || len(held) != 5 {
		t.Fatalf("held = %d, %v", len(held), err)
	}
	for i, h := range held {
		if h.Scope != sc.String() || h.Epoch != 3 || string(h.Segment) != string(segs[i]) {
			t.Fatalf("held[%d] mismatch: %+v", i, h)
		}
	}
}

func TestSpoolRejectsEpochMismatch(t *testing.T) {
	ctx := context.Background()
	sp, _ := NewSpool(t.TempDir())
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "main"}
	seg := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, EndTxID: 1}, []byte("x"))
	if _, err := sp.Append(ctx, sc, 2, seg); err == nil {
		t.Fatalf("expected epoch mismatch")
	}
}

func TestSessionSealReturnsStableSequencedTail(t *testing.T) {
	ctx := context.Background()
	sp, err := NewSpool(t.TempDir())
	if err != nil {
		t.Fatalf("spool: %v", err)
	}
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "session"}
	s1 := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: 1, EndTxID: 1}, []byte("a"))
	s2 := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: 2, EndTxID: 2}, []byte("b"))
	if _, _, err := sp.AppendBatchSession(ctx, "owner-session", sc, 1, [][]byte{s1}); err != nil {
		t.Fatalf("append first: %v", err)
	}
	if _, _, err := sp.AppendBatchSession(ctx, "owner-session", sc, 1, [][]byte{s2}); err != nil {
		t.Fatalf("append second: %v", err)
	}
	sealedTo, err := sp.Seal("owner-session")
	if err != nil || sealedTo != 2 {
		t.Fatalf("seal = %d, %v; want 2", sealedTo, err)
	}
	tail, err := sp.Tail("owner-session", 0)
	if err != nil || len(tail) != 2 || tail[0].Sequence != 1 || tail[1].Sequence != 2 {
		t.Fatalf("tail = %+v, %v", tail, err)
	}
	if _, _, err := sp.AppendBatchSession(ctx, "owner-session", sc, 1, [][]byte{s2}); !errors.Is(err, ErrSessionSealed) {
		t.Fatalf("append after seal = %v, want ErrSessionSealed", err)
	}
}

func TestSessionTailPageAdvancesWithoutSplittingFragments(t *testing.T) {
	ctx := context.Background()
	sp, err := NewSpool(t.TempDir())
	if err != nil {
		t.Fatalf("spool: %v", err)
	}
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "paged-tail"}
	for i := uint64(1); i <= 3; i++ {
		seg := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: i, EndTxID: i}, bytes.Repeat([]byte{byte(i)}, 128))
		if _, _, err := sp.AppendBatchSession(ctx, "paged-tail-session", sc, 1, [][]byte{seg}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if _, err := sp.Seal("paged-tail-session"); err != nil {
		t.Fatalf("seal: %v", err)
	}
	first, err := sp.TailPage("paged-tail-session", 0, 2, 1)
	if err != nil || len(first) != 1 || first[0].Sequence != 1 {
		t.Fatalf("first page = %+v, %v", first, err)
	}
	second, err := sp.TailPage("paged-tail-session", 1, 2, 1<<20)
	if err != nil || len(second) != 2 || second[0].Sequence != 2 || second[1].Sequence != 3 {
		t.Fatalf("second page = %+v, %v", second, err)
	}
}

func TestTailScopePageSkipsOtherScopesAndRejectsTruncation(t *testing.T) {
	sp, err := NewSpool(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	target := cell.Scope{Namespace: "demo", Class: "__do__", ID: "target"}
	other := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "other"}
	var path string
	for i := uint64(1); i <= 5; i++ {
		sc := other
		if i == 2 || i == 4 {
			sc = target
		}
		seg := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: i, EndTxID: i}, []byte{byte(i)})
		path, _, err = sp.AppendBatchSession(t.Context(), "mixed", sc, 1, [][]byte{seg})
		if err != nil {
			t.Fatal(err)
		}
	}
	final, err := sp.Head("mixed")
	if err != nil || final != 5 {
		t.Fatalf("head = %d, %v", final, err)
	}
	var after uint64
	var offset int64
	var seen []uint64
	for after < final {
		page, next, nextOffset, err := sp.TailScopePage("mixed", target.String(), after, final, offset, 2, 1<<20)
		if err != nil || next <= after || nextOffset <= offset {
			t.Fatalf("page %d = %v, %d, %d, %v", after, page, next, nextOffset, err)
		}
		for _, fragment := range page {
			seen = append(seen, fragment.Sequence)
		}
		after, offset = next, nextOffset
	}
	if len(seen) != 2 || seen[0] != 2 || seen[1] != 4 {
		t.Fatalf("scoped sequences = %v", seen)
	}
	seg := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: 6, EndTxID: 6}, []byte("still-open"))
	if _, _, err := sp.AppendBatchSession(t.Context(), "mixed", other, 1, [][]byte{seg}); err != nil {
		t.Fatalf("other scope append: %v", err)
	}
	if _, err := sp.Head("mixed"); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(path, offset-1); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := sp.TailScopePage("mixed", target.String(), 4, 5, offset-2, 2, 1<<20); err == nil {
		t.Fatal("truncated record accepted")
	}
}

func TestTailScopePageRejectsSequenceGap(t *testing.T) {
	sp, err := NewSpool(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	sc := cell.Scope{Namespace: "demo", Class: "__do__", ID: "gap"}
	seg := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: 1, EndTxID: 1}, []byte("gap"))
	path, _, err := sp.AppendBatchSession(t.Context(), "gap-session", sc, 1, [][]byte{seg})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := sp.AppendBatchSession(t.Context(), "gap-session", sc, 1, [][]byte{seg}); err != nil {
		t.Fatal(err)
	}
	first, next, offset, err := sp.TailScopePage("gap-session", sc.String(), 0, 2, 0, 1, 1<<20)
	if err != nil || len(first) != 1 || next != 1 {
		t.Fatalf("first page: %v %d %v", first, next, err)
	}
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.WriteAt([]byte{0, 0, 0, 0, 0, 0, 0, 4}, offset+4)
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := sp.TailScopePage("gap-session", sc.String(), 1, 2, offset, 1, 1<<20); err == nil {
		t.Fatal("sequence gap accepted")
	}
}

func TestActiveSessionHeadRejectsMissingLog(t *testing.T) {
	sp, err := NewSpool(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sp.Head("missing-session"); err == nil {
		t.Fatal("missing follower session treated as empty")
	}
}

func TestSessionDirectorySyncFailureIsRetried(t *testing.T) {
	ctx := context.Background()
	sp, err := NewSpool(t.TempDir())
	if err != nil {
		t.Fatalf("spool: %v", err)
	}
	original := sp.syncDir
	failed := false
	sp.syncDir = func(path string) error {
		if !failed {
			failed = true
			return errors.New("injected directory sync failure")
		}
		return original(path)
	}
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "dir-sync"}
	seg := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: 1, EndTxID: 1}, []byte("x"))
	if _, _, err := sp.AppendBatchSession(ctx, "dir-sync-session", sc, 1, [][]byte{seg}); err == nil {
		t.Fatal("first append succeeded despite injected directory sync failure")
	}
	if _, _, err := sp.AppendBatchSession(ctx, "dir-sync-session", sc, 1, [][]byte{seg}); err != nil {
		t.Fatalf("retry append: %v", err)
	}
	if !failed {
		t.Fatal("directory sync failure was not injected")
	}
}

func TestSessionSealDirectorySyncFailureIsRetried(t *testing.T) {
	ctx := context.Background()
	sp, err := NewSpool(t.TempDir())
	if err != nil {
		t.Fatalf("spool: %v", err)
	}
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "seal-sync"}
	seg := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: 1, EndTxID: 1}, []byte("x"))
	if _, _, err := sp.AppendBatchSession(ctx, "seal-sync-session", sc, 1, [][]byte{seg}); err != nil {
		t.Fatalf("append: %v", err)
	}
	original := sp.syncDir
	sealDir := sp.sessionDir("seal-sync-session")
	failed := false
	sp.syncDir = func(path string) error {
		if path == sealDir && !failed {
			failed = true
			return errors.New("injected seal directory sync failure")
		}
		return original(path)
	}
	if _, err := sp.Seal("seal-sync-session"); err == nil {
		t.Fatal("first seal succeeded despite injected directory sync failure")
	}
	if final, err := sp.Seal("seal-sync-session"); err != nil || final != 1 {
		t.Fatalf("retry seal = %d, %v; want 1", final, err)
	}
}

func TestSpoolAppendSessionGroupPreservesFramesAndRejectsInvalidGroup(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	sp, err := NewSpool(dir)
	if err != nil {
		t.Fatalf("spool: %v", err)
	}
	a := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "a"}
	b := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "b"}
	seg1 := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: 1, EndTxID: 1}, []byte("a1"))
	seg2 := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 2, StartTxID: 1, EndTxID: 1}, []byte("b1"))

	seqs, err := sp.AppendSessionGroup(ctx, "group-session", []SessionAppend{
		{Scope: a, Epoch: 1, Segments: [][]byte{seg1}},
		{Scope: b, Epoch: 2, Segments: [][]byte{seg2}},
	})
	if err != nil {
		t.Fatalf("append group: %v", err)
	}
	if fmt.Sprint(seqs) != "[1 2]" {
		t.Fatalf("sequences = %v, want [1 2]", seqs)
	}

	bad := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 9, StartTxID: 2, EndTxID: 2}, []byte("bad"))
	if _, err := sp.AppendSessionGroup(ctx, "group-session", []SessionAppend{
		{Scope: a, Epoch: 1, Segments: [][]byte{seg1}},
		{Scope: b, Epoch: 2, Segments: [][]byte{bad}},
	}); err == nil {
		t.Fatal("invalid group was accepted")
	}

	restarted, err := NewSpool(dir)
	if err != nil {
		t.Fatalf("restart spool: %v", err)
	}
	final, err := restarted.Seal("group-session")
	if err != nil || final != 2 {
		t.Fatalf("seal = %d, %v; want 2", final, err)
	}
	tail, err := restarted.Tail("group-session", 0)
	if err != nil {
		t.Fatalf("tail: %v", err)
	}
	if len(tail) != 2 || tail[0].Scope != a.String() || tail[1].Scope != b.String() {
		t.Fatalf("tail = %+v, want ordered a,b frames", tail)
	}
}

func TestSessionBatchesUseOneAppendLog(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	sp, err := NewSpool(dir)
	if err != nil {
		t.Fatalf("spool: %v", err)
	}
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "session-log"}
	for i := uint64(1); i <= 50; i++ {
		seg := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: i, EndTxID: i}, []byte{byte(i)})
		if _, sequence, err := sp.AppendBatchSession(ctx, "append-log-session", sc, 1, [][]byte{seg}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		} else if sequence != fmt.Sprint(i) {
			t.Fatalf("append %d sequence = %q", i, sequence)
		}
	}
	sessionDir := sp.sessionDir("append-log-session")
	entries, err := os.ReadDir(sessionDir)
	if err != nil {
		t.Fatalf("read session dir: %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != "fragments.log" {
		t.Fatalf("session batches must share one append log, got %v", entries)
	}

	// A restarted follower must recover the last sequence from the log once and
	// continue contiguously without overwriting the durable tail.
	sp, err = NewSpool(dir)
	if err != nil {
		t.Fatalf("restart spool: %v", err)
	}
	seg := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: 51, EndTxID: 51}, []byte("restart"))
	if _, sequence, err := sp.AppendBatchSession(ctx, "append-log-session", sc, 1, [][]byte{seg}); err != nil {
		t.Fatalf("append after restart: %v", err)
	} else if sequence != "51" {
		t.Fatalf("sequence after restart = %q, want 51", sequence)
	}
	if final, err := sp.Seal("append-log-session"); err != nil || final != 51 {
		t.Fatalf("seal = %d, %v; want 51", final, err)
	}
	tail, err := sp.Tail("append-log-session", 49)
	if err != nil || len(tail) != 2 || tail[0].Sequence != 50 || tail[1].Sequence != 51 {
		t.Fatalf("tail after restart = %+v, %v", tail, err)
	}
}

func TestSessionAppendTruncatesTornTailAfterRestart(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	sp, err := NewSpool(dir)
	if err != nil {
		t.Fatalf("spool: %v", err)
	}
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "torn-tail"}
	first := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: 1, EndTxID: 1}, []byte("first"))
	path, _, err := sp.AppendBatchSession(ctx, "torn-session", sc, 1, [][]byte{first})
	if err != nil {
		t.Fatalf("append first: %v", err)
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("open log: %v", err)
	}
	if _, err := f.Write([]byte{0, 0, 0, 40, 1, 2, 3}); err != nil {
		t.Fatalf("write torn record: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close torn log: %v", err)
	}

	sp, err = NewSpool(dir)
	if err != nil {
		t.Fatalf("restart spool: %v", err)
	}
	second := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: 2, EndTxID: 2}, []byte("second"))
	if _, sequence, err := sp.AppendBatchSession(ctx, "torn-session", sc, 1, [][]byte{second}); err != nil {
		t.Fatalf("append after torn tail: %v", err)
	} else if sequence != "2" {
		t.Fatalf("sequence = %q, want 2", sequence)
	}
	if final, err := sp.Seal("torn-session"); err != nil || final != 2 {
		t.Fatalf("seal = %d, %v; want 2", final, err)
	}
	tail, err := sp.Tail("torn-session", 0)
	if err != nil || len(tail) != 2 || tail[1].Sequence != 2 {
		t.Fatalf("tail = %+v, %v", tail, err)
	}
}

func TestSessionMigratesLegacyFragments(t *testing.T) {
	dir := t.TempDir()
	sp, err := NewSpool(dir)
	if err != nil {
		t.Fatalf("spool: %v", err)
	}
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "legacy"}
	legacyDir := filepath.Join(sp.sessionDir("legacy-session"), "fragments")
	if err := os.MkdirAll(legacyDir, 0o755); err != nil {
		t.Fatalf("legacy dir: %v", err)
	}
	for i := uint64(1); i <= 2; i++ {
		seg := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: i, EndTxID: i}, []byte{byte(i)})
		path := filepath.Join(legacyDir, fmt.Sprintf("%020d.frame", i))
		if err := os.WriteFile(path, encodeSessionFragment(sc.String(), 1, [][]byte{seg}), 0o644); err != nil {
			t.Fatalf("legacy fragment %d: %v", i, err)
		}
	}

	seg := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: 3, EndTxID: 3}, []byte("new"))
	if _, sequence, err := sp.AppendBatchSession(context.Background(), "legacy-session", sc, 1, [][]byte{seg}); err != nil {
		t.Fatalf("append after upgrade: %v", err)
	} else if sequence != "3" {
		t.Fatalf("sequence after upgrade = %q, want 3", sequence)
	}
	if final, err := sp.Seal("legacy-session"); err != nil || final != 3 {
		t.Fatalf("seal = %d, %v; want 3", final, err)
	}
	tail, err := sp.Tail("legacy-session", 0)
	if err != nil || len(tail) != 3 {
		t.Fatalf("migrated tail = %+v, %v", tail, err)
	}
}

func TestReclaimedSessionStillRejectsAppend(t *testing.T) {
	ctx := context.Background()
	sp, err := NewSpool(t.TempDir())
	if err != nil {
		t.Fatalf("spool: %v", err)
	}
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "reclaimed"}
	seg := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: 1, EndTxID: 1}, []byte("x"))
	if _, _, err := sp.AppendBatchSession(ctx, "reclaimed-session", sc, 1, [][]byte{seg}); err != nil {
		t.Fatalf("append: %v", err)
	}
	if _, err := sp.Seal("reclaimed-session"); err != nil {
		t.Fatalf("seal: %v", err)
	}
	if err := sp.Reclaim("reclaimed-session"); err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if _, _, err := sp.AppendBatchSession(ctx, "reclaimed-session", sc, 1, [][]byte{seg}); !errors.Is(err, ErrSessionSealed) {
		t.Fatalf("append after reclaim = %v, want ErrSessionSealed", err)
	}
}

func TestReplicateQuorumOne(t *testing.T) {
	ctx := context.Background()
	ok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer ok.Close()
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer bad.Close()

	m := NewManager(NewHTTPTransport("tok", nil))
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "main"}
	seg := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, EndTxID: 1}, []byte("x"))

	ackedBy, err := m.Replicate(ctx, []string{bad.URL, ok.URL}, sc, 1, seg)
	if err != nil {
		t.Fatalf("replicate: %v", err)
	}
	if ackedBy != ok.URL {
		t.Fatalf("ackedBy = %q, want %q", ackedBy, ok.URL)
	}
}

func TestReplicateAllFail(t *testing.T) {
	ctx := context.Background()
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer bad.Close()

	m := NewManager(NewHTTPTransport("tok", nil))
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "main"}
	seg := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, EndTxID: 1}, []byte("x"))

	if _, err := m.Replicate(ctx, []string{bad.URL}, sc, 1, seg); err == nil {
		t.Fatalf("expected all-followers-failed error")
	}
	if _, err := m.Replicate(ctx, nil, sc, 1, seg); err == nil {
		t.Fatalf("expected no-followers error")
	}
}

type fakeBatchTransport struct {
	mu       sync.Mutex
	batches  int
	segments int
}

func (f *fakeBatchTransport) Append(_ context.Context, _ string, _ cell.Scope, _ uint64, _ []byte) error {
	return nil
}
func (f *fakeBatchTransport) AppendBatch(_ context.Context, _ string, _ cell.Scope, _ uint64, segs [][]byte) error {
	f.mu.Lock()
	f.batches++
	f.segments += len(segs)
	f.mu.Unlock()
	return nil
}
func (f *fakeBatchTransport) Held(_ context.Context, _ string) ([]HeldSegment, error) {
	return nil, nil
}

func TestShipBatcherCoalesces(t *testing.T) {
	ft := &fakeBatchTransport{}
	b := NewShipBatcher(ft, 256, 4<<20, 5*time.Millisecond, 1, 1, 0, 0)
	ctx, cancel := context.WithCancel(context.Background())
	b.Start(ctx)
	defer func() { cancel(); b.Stop() }()

	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "ship"}
	const N = 200
	var wg sync.WaitGroup
	errs := make(chan error, N)
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			seg := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: uint64(i + 1), EndTxID: uint64(i + 1)}, []byte("x"))
			addr, err := b.Ship(ctx, []string{"http://f1"}, sc, 1, seg)
			if err != nil {
				errs <- err
				return
			}
			if addr != "http://f1" {
				errs <- fmt.Errorf("ackedBy = %q", addr)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("ship: %v", err)
	}

	ft.mu.Lock()
	defer ft.mu.Unlock()
	if ft.segments != N {
		t.Fatalf("segments shipped = %d, want %d", ft.segments, N)
	}
	if ft.batches >= N {
		t.Fatalf("batches = %d, want < %d (group commit)", ft.batches, N)
	}
}

func TestShipBatcherShipNowBypassesQueue(t *testing.T) {
	transport := &fakeBatchTransport{}
	shipper := NewShipBatcher(transport, 256, 4<<20, time.Second, 1, 1, 0, 0)
	scope := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "now"}
	segment := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: 1, EndTxID: 10}, []byte("prebatched"))
	ackedBy, err := shipper.ShipNow(context.Background(), []string{"http://f1"}, scope, 1, segment)
	if err != nil || ackedBy != "http://f1" {
		t.Fatalf("ShipNow = %q, %v", ackedBy, err)
	}
	transport.mu.Lock()
	defer transport.mu.Unlock()
	if transport.batches != 1 || transport.segments != 1 {
		t.Fatalf("transport batches/segments = %d/%d, want 1/1", transport.batches, transport.segments)
	}
}

type recordingAsyncTransport struct {
	mu      sync.Mutex
	order   []byte
	delay   time.Duration
	batches int
}

func (t *recordingAsyncTransport) Append(_ context.Context, _ string, _ cell.Scope, _ uint64, _ []byte) error {
	return nil
}
func (t *recordingAsyncTransport) AppendBatch(_ context.Context, _ string, _ cell.Scope, _ uint64, _ [][]byte) error {
	return nil
}
func (t *recordingAsyncTransport) Held(_ context.Context, _ string) ([]HeldSegment, error) {
	return nil, nil
}
func (t *recordingAsyncTransport) AppendBatchAsync(_ context.Context, _ string, _ cell.Scope, _ uint64, segments [][]byte) (<-chan error, error) {
	t.mu.Lock()
	t.batches++
	for _, seg := range segments {
		t.order = append(t.order, seg[0])
	}
	t.mu.Unlock()
	out := make(chan error, 1)
	go func() {
		time.Sleep(t.delay)
		out <- nil
	}()
	return out, nil
}

func TestShipBatcherPipelinesInOrder(t *testing.T) {
	const delay = 20 * time.Millisecond
	transport := &recordingAsyncTransport{delay: delay}
	shipper := NewShipBatcher(transport, 1, 1<<20, time.Second, 1, 4, 0, 0)
	ctx, cancel := context.WithCancel(context.Background())
	shipper.Start(ctx)
	defer func() { cancel(); shipper.Stop() }()

	scope := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "pipe"}
	const batches = 4
	results := make([]chan shipResult, batches)
	started := time.Now()
	for i := 0; i < batches; i++ {
		done := make(chan shipResult, 1)
		results[i] = done
		item := shipItem{
			scope: scope, epoch: 1, followers: []string{"http://f1"}, fkey: "http://f1",
			seg: []byte{byte(i)}, done: done,
		}
		shipper.shards[0].groups <- []shipItem{item}
	}
	for i, done := range results {
		select {
		case r := <-done:
			if r.err != nil {
				t.Fatalf("batch %d failed: %v", i, r.err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("batch %d timed out", i)
		}
	}
	elapsed := time.Since(started)

	transport.mu.Lock()
	defer transport.mu.Unlock()
	if len(transport.order) != batches {
		t.Fatalf("recorded %d segments, want %d", len(transport.order), batches)
	}
	for i := 1; i < len(transport.order); i++ {
		if transport.order[i] <= transport.order[i-1] {
			t.Fatalf("dispatch order broken: %v", transport.order)
		}
	}
	// Serial would be 4*delay = 80ms; with pipeline 4 it is about one delay.
	if elapsed > 3*delay {
		t.Fatalf("not pipelined: elapsed %s for %d batches of %s", elapsed, batches, delay)
	}
}

// TestSpoolIdempotentPerSequence covers ADR-164: the same segment (same epoch,
// kind, txid range and payload CRC) appended twice is a no-op, so retries,
// stream reconnects and adaptive hedge copies are safe; recovery would
// otherwise reject a duplicated txid as a non-contiguous chain.
func TestSpoolIdempotentPerSequence(t *testing.T) {
	sp, err := NewSpool(t.TempDir())
	if err != nil {
		t.Fatalf("spool: %v", err)
	}
	ctx := context.Background()
	sc := cell.Scope{Namespace: "acme", Class: "kv", ID: "main"}
	s1 := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: 1, EndTxID: 1}, []byte("a"))
	s2 := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: 2, EndTxID: 2}, []byte("b"))

	if _, _, err := sp.AppendBatch(ctx, sc, 1, [][]byte{s1, s2}); err != nil {
		t.Fatalf("first append: %v", err)
	}
	// Re-append, including a mixed batch (s2 duplicate + s3 fresh).
	s3 := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: 3, EndTxID: 3}, []byte("c"))
	if _, _, err := sp.AppendBatch(ctx, sc, 1, [][]byte{s1, s2, s3}); err != nil {
		t.Fatalf("mixed append: %v", err)
	}
	// A pure duplicate batch is still a successful no-op.
	if _, _, err := sp.AppendBatch(ctx, sc, 1, [][]byte{s1, s2}); err != nil {
		t.Fatalf("dup append: %v", err)
	}

	long, _ := sp.Segments(sc, 1)
	if len(long) != 3 {
		t.Fatalf("segments = %d, want 3 (no duplicates spooled)", len(long))
	}

	// A fresh Spool over the same dir reloads the identity set from the log.
	sp2, err := NewSpool(sp.Dir)
	if err != nil {
		t.Fatalf("spool2: %v", err)
	}
	if _, _, err := sp2.AppendBatch(ctx, sc, 1, [][]byte{s1, s3}); err != nil {
		t.Fatalf("reload append: %v", err)
	}
	long, _ = sp2.Segments(sc, 1)
	if len(long) != 3 {
		t.Fatalf("segments after reload = %d, want 3", len(long))
	}
}

type hedgeTransport struct {
	mu    sync.Mutex
	calls []string
	delay map[string]time.Duration
	fail  map[string]bool
}

func (t *hedgeTransport) Append(_ context.Context, _ string, _ cell.Scope, _ uint64, _ []byte) error {
	return nil
}
func (t *hedgeTransport) AppendBatch(_ context.Context, addr string, _ cell.Scope, _ uint64, _ [][]byte) error {
	t.mu.Lock()
	t.calls = append(t.calls, addr)
	d := t.delay[addr]
	fail := t.fail[addr]
	t.mu.Unlock()
	if d > 0 {
		time.Sleep(d)
	}
	if fail {
		return fmt.Errorf("boom %s", addr)
	}
	return nil
}
func (t *hedgeTransport) Held(_ context.Context, _ string) ([]HeldSegment, error) { return nil, nil }

func (t *hedgeTransport) count() map[string]int {
	t.mu.Lock()
	defer t.mu.Unlock()
	m := map[string]int{}
	for _, c := range t.calls {
		m[c]++
	}
	return m
}

func hedgeSeg() []byte {
	return ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: 1, EndTxID: 1}, []byte("h"))
}

// TestShipBatcherHedgeSkipsSlowBackupWhenPrimaryFast: with a fixed hedge wait the
// second follower is never contacted when the primary acks in time (celld's
// cost-saving case).
func TestShipBatcherHedgeSkipsSlowBackupWhenPrimaryFast(t *testing.T) {
	tr := &hedgeTransport{delay: map[string]time.Duration{"a": 0, "b": 300 * time.Millisecond}}
	b := NewShipBatcher(tr, 256, 4<<20, time.Millisecond, 1, 1, 30, 2000)
	addr, err := b.ShipNow(context.Background(), []string{"a", "b"}, cell.Scope{Namespace: "d", Class: "kv", ID: "h"}, 1, hedgeSeg())
	if err != nil || addr != "a" {
		t.Fatalf("ShipNow = %q, %v", addr, err)
	}
	if got := tr.count(); got["a"] != 1 || got["b"] != 0 {
		t.Fatalf("calls = %v, want only a", got)
	}
}

// TestShipBatcherHedgeFiresOnSlowPrimary: the second follower gets the copy once
// the primary misses the wait, and the first ack wins.
func TestShipBatcherHedgeFiresOnSlowPrimary(t *testing.T) {
	tr := &hedgeTransport{delay: map[string]time.Duration{"a": 400 * time.Millisecond, "b": 0}}
	b := NewShipBatcher(tr, 256, 4<<20, time.Millisecond, 1, 1, 20, 2000)
	addr, err := b.ShipNow(context.Background(), []string{"a", "b"}, cell.Scope{Namespace: "d", Class: "kv", ID: "h"}, 1, hedgeSeg())
	if err != nil || addr != "b" {
		t.Fatalf("ShipNow = %q, %v (want b)", addr, err)
	}
	if got := tr.count(); got["a"] != 1 || got["b"] != 1 {
		t.Fatalf("calls = %v, want both", got)
	}
}

// TestShipBatcherHedgeOffSingleCopy pins celld's "0 disables the second copy":
// only the primary follower is used, and a failed primary still fails over to
// the next follower in order (a failover, not a duplicate).
func TestShipBatcherHedgeOffSingleCopy(t *testing.T) {
	tr := &hedgeTransport{delay: map[string]time.Duration{"a": 0, "b": 0}}
	b := NewShipBatcher(tr, 256, 4<<20, time.Millisecond, 1, 1, 0, 2000)
	addr, err := b.ShipNow(context.Background(), []string{"a", "b"}, cell.Scope{Namespace: "d", Class: "kv", ID: "h"}, 1, hedgeSeg())
	if err != nil || addr != "a" {
		t.Fatalf("ShipNow = %q, %v", addr, err)
	}
	if got := tr.count(); got["a"] != 1 || got["b"] != 0 {
		t.Fatalf("calls = %v, want only the primary a", got)
	}

	tr2 := &hedgeTransport{fail: map[string]bool{"a": true}, delay: map[string]time.Duration{"b": 0}}
	b2 := NewShipBatcher(tr2, 256, 4<<20, time.Millisecond, 1, 1, 0, 2000)
	addr, err = b2.ShipNow(context.Background(), []string{"a", "b"}, cell.Scope{Namespace: "d", Class: "kv", ID: "h"}, 1, hedgeSeg())
	if err != nil || addr != "b" {
		t.Fatalf("failover ShipNow = %q, %v (want b)", addr, err)
	}
	if got := tr2.count(); got["a"] != 1 || got["b"] != 1 {
		t.Fatalf("failover calls = %v, want a then b", got)
	}
}

// TestShipBatcherHedgeAllFail: every copy failing still surfaces an error.
func TestShipBatcherHedgeAllFail(t *testing.T) {
	tr := &hedgeTransport{fail: map[string]bool{"a": true, "b": true}}
	b := NewShipBatcher(tr, 256, 4<<20, time.Millisecond, 1, 1, 10, 2000)
	if _, err := b.ShipNow(context.Background(), []string{"a", "b"}, cell.Scope{Namespace: "d", Class: "kv", ID: "h"}, 1, hedgeSeg()); err == nil {
		t.Fatal("expected all-followers error")
	}
}

// TestShipBatcherAdaptiveHedgeWait pins the celld derivation: 4x the slowest
// recent append, floor 250ms, capped by the configured backstop.
func TestShipBatcherAdaptiveHedgeWait(t *testing.T) {
	b := NewShipBatcher(&hedgeTransport{}, 256, 4<<20, time.Millisecond, 1, 1, -1, 1000)
	if got := b.hedgeWait(); got != 250*time.Millisecond {
		t.Fatalf("unsampled adaptive wait = %v, want 250ms floor", got)
	}
	b.recordAppend(10 * time.Millisecond)
	if got := b.hedgeWait(); got != 250*time.Millisecond {
		t.Fatalf("adaptive wait = %v, want 250ms floor", got)
	}
	b.recordAppend(50 * time.Millisecond)
	if got := b.hedgeWait(); got != 250*time.Millisecond {
		t.Fatalf("adaptive wait = %v, want 250ms floor (4x50ms is below it)", got)
	}
	b.recordAppend(100 * time.Millisecond)
	if got := b.hedgeWait(); got != 400*time.Millisecond {
		t.Fatalf("adaptive wait = %v, want 4x slowest = 400ms", got)
	}
	b.recordAppend(500 * time.Millisecond)
	if got := b.hedgeWait(); got != 1000*time.Millisecond {
		t.Fatalf("adaptive wait = %v, want backstop cap 1000ms", got)
	}
	capped := NewShipBatcher(&hedgeTransport{}, 256, 4<<20, time.Millisecond, 1, 1, -1, 300)
	capped.recordAppend(100 * time.Millisecond)
	if got := capped.hedgeWait(); got != 300*time.Millisecond {
		t.Fatalf("capped wait = %v, want 300ms", got)
	}
	fixed := NewShipBatcher(&hedgeTransport{}, 256, 4<<20, time.Millisecond, 1, 1, 42, 300)
	if got := fixed.hedgeWait(); got != 42*time.Millisecond {
		t.Fatalf("fixed wait = %v, want 42ms", got)
	}
}

type asyncHedgeTransport struct {
	hedgeTransport
}

func (t *asyncHedgeTransport) AppendBatchAsync(_ context.Context, addr string, _ cell.Scope, _ uint64, _ [][]byte) (<-chan error, error) {
	t.mu.Lock()
	t.calls = append(t.calls, addr)
	d := t.delay[addr]
	fail := t.fail[addr]
	t.mu.Unlock()
	out := make(chan error, 1)
	go func() {
		if d > 0 {
			time.Sleep(d)
		}
		if fail {
			out <- fmt.Errorf("boom %s", addr)
			return
		}
		out <- nil
	}()
	return out, nil
}

// TestShipBatcherHedgeAsync covers the production (stream) path: the primary
// frame is written before ShipNowAsync returns, the hedged copy follows on the
// timer, and the first ack wins.
func TestShipBatcherHedgeAsync(t *testing.T) {
	tr := &asyncHedgeTransport{hedgeTransport{delay: map[string]time.Duration{"a": 400 * time.Millisecond, "b": 0}}}
	b := NewShipBatcher(tr, 256, 4<<20, time.Millisecond, 1, 1, 20, 2000)
	ch, err := b.ShipNowAsync(context.Background(), []string{"a", "b"}, cell.Scope{Namespace: "d", Class: "kv", ID: "h"}, 1, hedgeSeg())
	if err != nil {
		t.Fatalf("ShipNowAsync: %v", err)
	}
	// Primary written synchronously, before any ack.
	if got := tr.count(); got["a"] != 1 {
		t.Fatalf("primary not written synchronously: %v", got)
	}
	select {
	case ack := <-ch:
		if ack.Err != nil || ack.AckedBy != "b" {
			t.Fatalf("ack = %+v, want b", ack)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ack timed out")
	}
	if got := tr.count(); got["a"] != 1 || got["b"] != 1 {
		t.Fatalf("calls = %v, want both", got)
	}
}

// TestShipBatcherHedgeStats pins the ADR-165 hedge counters: a fired and won
// hedge copy when the primary misses the wait, and no fire when it is fast.
func TestShipBatcherHedgeStats(t *testing.T) {
	slow := &hedgeTransport{delay: map[string]time.Duration{"a": 400 * time.Millisecond, "b": 0}}
	b := NewShipBatcher(slow, 256, 4<<20, time.Millisecond, 1, 1, 20, 2000)
	if _, err := b.ShipNow(context.Background(), []string{"a", "b"}, cell.Scope{Namespace: "d", Class: "kv", ID: "h"}, 1, hedgeSeg()); err != nil {
		t.Fatalf("ship: %v", err)
	}
	if fired, won := b.HedgeStats(); fired != 1 || won != 1 {
		t.Fatalf("hedge stats = %d/%d, want 1/1", fired, won)
	}

	fast := &hedgeTransport{delay: map[string]time.Duration{"a": 0, "b": 300 * time.Millisecond}}
	b2 := NewShipBatcher(fast, 256, 4<<20, time.Millisecond, 1, 1, 30, 2000)
	if _, err := b2.ShipNow(context.Background(), []string{"a", "b"}, cell.Scope{Namespace: "d", Class: "kv", ID: "h"}, 1, hedgeSeg()); err != nil {
		t.Fatalf("ship: %v", err)
	}
	if fired, won := b2.HedgeStats(); fired != 0 || won != 0 {
		t.Fatalf("hedge stats = %d/%d, want 0/0", fired, won)
	}
}

func TestSessionTailPageByteBoundaryIsNotTruncation(t *testing.T) {
	ctx := context.Background()
	sp, err := NewSpool(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	sc := cell.Scope{Namespace: "demo", Class: "__kv__", ID: "byte-boundary"}
	var size int
	for i := uint64(1); i <= 4; i++ {
		seg := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: i, EndTxID: i}, bytes.Repeat([]byte{byte(i)}, 128))
		size = len(seg)
		if _, _, err := sp.AppendBatchSession(ctx, "byte-boundary", sc, 1, [][]byte{seg}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := sp.Seal("byte-boundary"); err != nil {
		t.Fatal(err)
	}
	for after := uint64(0); after < 4; after++ {
		page, err := sp.TailPage("byte-boundary", after, 256, size+size/2)
		if err != nil || len(page) != 1 || page[0].Sequence != after+1 {
			t.Fatalf("after %d: page=%v err=%v", after, page, err)
		}
	}
}
