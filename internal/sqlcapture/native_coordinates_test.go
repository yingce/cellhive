package sqlcapture

import (
	"encoding/binary"
	"os"
	"testing"
	"time"

	"cellhive/internal/ltx"
)

func TestNativeChunkCoordinatesMatchOriginalWAL(t *testing.T) {
	ctx := t.Context()
	db, scope, _ := openStreamCell(t, ctx, "coordinates", 1)
	if _, err := db.DB.Exec("PRAGMA wal_autocheckpoint=0"); err != nil {
		t.Fatal(err)
	}
	recorder := &recordingCommitter{}
	capture, baseline := newCapture(t, db, scope, recorder)
	defer capture.cursor.Close()
	defer capture.backgroundCancel()
	before, err := os.Stat(db.WALPath())
	if err != nil {
		t.Fatal(err)
	}
	// Repeatedly overwrite the same pages: output deduplication must not shrink
	// the source WAL span. Also split at an independent snapshot boundary.
	capture.baselineCuts = []uint64{baseline + 200}
	for i := 0; i < 600; i++ {
		if _, err := db.PutTx(ctx, "same", []byte{byte(i)}, nil); err != nil {
			t.Fatal(err)
		}
	}
	raw, err := os.ReadFile(db.WALPath())
	if err != nil {
		t.Fatal(err)
	}
	beforeCapture := time.Now().UnixMilli()
	if err := capture.poll(ctx); err != nil {
		t.Fatal(err)
	}
	if len(recorder.segments) < 2 {
		t.Fatalf("chunks=%d", len(recorder.segments))
	}
	next := before.Size()
	var transactions uint64
	for _, segment := range recorder.segments {
		h, pages, err := ltx.DecodeNativePages(segment)
		if err != nil {
			t.Fatal(err)
		}
		if h.Timestamp < beforeCapture || h.Timestamp > time.Now().UnixMilli() {
			t.Fatalf("timestamp=%d", h.Timestamp)
		}
		frameSize := int64(24 + h.PageSize)
		if h.WALOffset != next || h.WALSize <= 0 || h.WALSize%frameSize != 0 {
			t.Fatalf("span=%d+%d next=%d", h.WALOffset, h.WALSize, next)
		}
		if h.WALSalt1 != binary.BigEndian.Uint32(raw[16:20]) || h.WALSalt2 != binary.BigEndian.Uint32(raw[20:24]) {
			t.Fatal("wrong WAL generation")
		}
		next += h.WALSize
		if next > int64(len(raw)) {
			t.Fatal("span exceeds WAL")
		}
		if commit := binary.BigEndian.Uint32(raw[next-frameSize+4 : next-frameSize+8]); commit != h.Commit {
			t.Fatalf("commit=%d header=%d", commit, h.Commit)
		}
		if int64(len(pages)) >= h.WALSize/frameSize {
			t.Fatal("fixture did not exercise deduplication")
		}
		transactions += h.MaxTXID - h.MinTXID + 1
	}
	if next != int64(len(raw)) || transactions != 600 {
		t.Fatalf("covered=%d/%d tx=%d", next, len(raw), transactions)
	}
}
