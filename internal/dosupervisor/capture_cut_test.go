package dosupervisor

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/ltx"
	"cellhive/internal/replica"
)

func TestCaptureSeedUsesIndexRangesNotFullAudit(t *testing.T) {
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	rep := replica.New(b)
	sc := cell.Scope{Namespace: "n", Class: "c", ID: "large-cut"}
	rng := rand.New(rand.NewSource(1))
	pages := make([]ltx.WALPage, 2048)
	for i := range pages {
		data := make([]byte, 512)
		_, _ = rng.Read(data)
		pages[i] = ltx.WALPage{PageNo: uint32(i + 1), Data: data}
	}
	raw, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: 512, Commit: uint32(len(pages)), MinTXID: 1, MaxTXID: 7}, pages)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := rep.Append(t.Context(), sc, 1, raw); err != nil {
		t.Fatal(err)
	}
	agent, _ := agentStub(t, rep, b)
	defer agent.Close()
	var transferred atomic.Int64
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Query().Get("key"), ".ltx") {
			if r.Header.Get("Range") == "" {
				t.Error("full LTX GET during capture seed")
				http.Error(w, "forbidden", 500)
				return
			}
			recorder := httptest.NewRecorder()
			agent.Config.Handler.ServeHTTP(recorder, r)
			transferred.Add(int64(recorder.Body.Len()))
			for k, v := range recorder.Header() {
				w.Header()[k] = v
			}
			w.WriteHeader(recorder.Code)
			_, _ = w.Write(recorder.Body.Bytes())
			return
		}
		agent.Config.Handler.ServeHTTP(w, r)
	}))
	defer proxy.Close()
	store := &HTTPStore{Base: proxy.URL, Token: strings.Repeat("A", 43)}
	s := &Supervisor{Segments: store, Epoch: 1}
	fs := &fileState{scope: sc, seq: 1}
	if err := s.seedCaptureWatermark(t.Context(), fs); err != nil || fs.seq != 8 {
		t.Fatalf("seed=%d: %v", fs.seq, err)
	}
	if n := transferred.Load(); n <= 0 || n >= int64(len(raw))/2 {
		t.Fatalf("activation transferred %d of %d body bytes", n, len(raw))
	}
}

func TestCaptureSeedRejectsInvalidNativeCut(t *testing.T) {
	for _, mode := range []string{"header", "index", "missing-page", "truncate-reextend", "missing-epoch-predecessor"} {
		t.Run(mode, func(t *testing.T) {
			b, err := bucket.NewFSBucket(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			rep := replica.New(b)
			sc := cell.Scope{Namespace: "n", Class: "c", ID: mode}
			appendFile := func(epoch, tx uint64, commit uint32, pgnos ...uint32) (string, []byte) {
				t.Helper()
				pages := make([]ltx.WALPage, len(pgnos))
				for i, pg := range pgnos {
					pages[i] = ltx.WALPage{PageNo: pg, Data: bytes.Repeat([]byte{byte(pg)}, 512)}
				}
				raw, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: 512, Commit: commit, MinTXID: tx, MaxTXID: tx}, pages)
				if err != nil {
					t.Fatal(err)
				}
				key, _, err := rep.Append(t.Context(), sc, epoch, raw)
				if err != nil {
					t.Fatal(err)
				}
				return key, raw
			}
			epoch := uint64(1)
			switch mode {
			case "header", "index":
				key, raw := appendFile(1, 1, 1, 1)
				if mode == "header" {
					raw[0] ^= 1
				} else {
					n := int(binary.BigEndian.Uint64(raw[len(raw)-24:]))
					raw[len(raw)-24-n] = 0
				}
				if _, err := b.Put(t.Context(), key, raw); err != nil {
					t.Fatal(err)
				}
			case "missing-page":
				appendFile(1, 1, 2, 1)
			case "truncate-reextend":
				appendFile(1, 1, 2, 1, 2)
				appendFile(1, 2, 1, 1)
				appendFile(1, 3, 2, 1)
			case "missing-epoch-predecessor":
				epoch = 2
				appendFile(2, 2, 1, 1)
			}
			s := &Supervisor{Store: rep, Epoch: epoch}
			fs := &fileState{scope: sc, seq: 19}
			if err := s.seedCaptureWatermark(t.Context(), fs); err == nil {
				t.Fatal("invalid cut seeded capture")
			}
			if fs.seq != 19 {
				t.Fatalf("failed validation changed sequence: %d", fs.seq)
			}
		})
	}
}

func TestCaptureSeedIndexFailureNeverFallsBack(t *testing.T) {
	sentinel := errors.New("index unavailable")
	source := &failingPageSource{err: sentinel}
	s := &Supervisor{Segments: source, Epoch: 1}
	fs := &fileState{scope: cell.Scope{Namespace: "n", Class: "c", ID: "cut"}, seq: 1}
	if err := s.seedCaptureWatermark(context.Background(), fs); !errors.Is(err, sentinel) {
		t.Fatalf("error=%v", err)
	}
	if source.restores != 0 {
		t.Fatalf("full restore fallback=%d", source.restores)
	}
	source.err = replica.ErrNoPageIndex
	if err := s.seedCaptureWatermark(context.Background(), fs); err != nil || fs.seq != 1 {
		t.Fatalf("empty cut sequence=%d: %v", fs.seq, err)
	}
}

// Activation checks structure and page coverage; native file CRC remains an
// explicit audit. Keep the two contracts separate, even when all bytes happen
// to fit in the tail range of a small object.
func TestCaptureSeedDoesNotRedefineFullIntegrityAudit(t *testing.T) {
	b, err := bucket.NewFSBucket(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	rep := replica.New(b)
	sc := cell.Scope{Namespace: "n", Class: "c", ID: "audit"}
	raw, err := ltx.EncodeNativePages(ltx.NativeHeader{PageSize: 512, Commit: 1, MinTXID: 1, MaxTXID: 1}, []ltx.WALPage{{PageNo: 1, Data: bytes.Repeat([]byte{1}, 512)}})
	if err != nil {
		t.Fatal(err)
	}
	key, _, err := rep.Append(t.Context(), sc, 1, raw)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)-1] ^= 1 // File CRC only; header, index and page frames are intact.
	if _, err := b.Put(t.Context(), key, raw); err != nil {
		t.Fatal(err)
	}
	s := &Supervisor{Store: rep, Epoch: 1}
	fs := &fileState{scope: sc, seq: 1}
	if err := s.seedCaptureWatermark(t.Context(), fs); err != nil || fs.seq != 2 {
		t.Fatalf("index cut=%d: %v", fs.seq, err)
	}
	if _, err := rep.ProvenWatermark(t.Context(), sc, 1); err == nil {
		t.Fatal("full CRC audit accepted corruption")
	}
}
