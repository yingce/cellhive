package wal

import (
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type overlayTestFrame struct {
	pg, size uint32
	value    byte
}

func overlayTestWAL(t testing.TB, frames ...overlayTestFrame) (string, []byte) {
	t.Helper()
	raw := make([]byte, walHeaderSize)
	for i, v := range []uint32{magicLE, 3007000, 512, 0, 123, 456} {
		binary.BigEndian.PutUint32(raw[i*4:], v)
	}
	h, _ := parseHeader(raw)
	s1, s2 := headerChecksum(raw, h)
	binary.BigEndian.PutUint32(raw[24:], s1)
	binary.BigEndian.PutUint32(raw[28:], s2)
	for _, f := range frames {
		b := make([]byte, frameHdrSize+512)
		binary.BigEndian.PutUint32(b, f.pg)
		binary.BigEndian.PutUint32(b[4:], f.size)
		binary.BigEndian.PutUint32(b[8:], 123)
		binary.BigEndian.PutUint32(b[12:], 456)
		for i := frameHdrSize; i < len(b); i++ {
			b[i] = f.value
		}
		s1, s2 = frameChecksum(b[:24], b[24:], s1, s2, false)
		binary.BigEndian.PutUint32(b[16:], s1)
		binary.BigEndian.PutUint32(b[20:], s2)
		raw = append(raw, b...)
	}
	path := filepath.Join(t.TempDir(), "db-wal")
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	return path, raw
}
func TestCommittedOverlayLatestCommitAndShrink(t *testing.T) {
	path, _ := overlayTestWAL(t, overlayTestFrame{1, 0, 1}, overlayTestFrame{3, 3, 3}, overlayTestFrame{1, 1, 2}, overlayTestFrame{2, 2, 4}, overlayTestFrame{1, 0, 9})
	o, err := OpenCommittedOverlay(path)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	if o.PageSize != 512 || o.DBSize != 2 {
		t.Fatalf("sizes: %d/%d", o.PageSize, o.DBSize)
	}
	for pg, want := range map[uint32]byte{1: 2, 2: 4, 3: 0} {
		b := make([]byte, 512)
		ok, err := o.ReadPage(pg, b)
		if err != nil || ok != (want != 0) || b[0] != want {
			t.Fatalf("page %d: found %v value %d error %v", pg, ok, b[0], err)
		}
	}
	if err := o.Validate(); err != nil {
		t.Fatal(err)
	}
}
func TestCommittedOverlayDetectsChanges(t *testing.T) {
	for _, kind := range []string{"salt", "payload", "replacement", "unlink", "truncate"} {
		t.Run(kind, func(t *testing.T) {
			path, raw := overlayTestWAL(t, overlayTestFrame{1, 1, 1})
			o, err := OpenCommittedOverlay(path)
			if err != nil {
				t.Fatal(err)
			}
			defer o.Close()
			switch kind {
			case "salt":
				raw[16] ^= 1
				err = os.WriteFile(path, raw, 0600)
			case "payload":
				raw[walHeaderSize+frameHdrSize] ^= 1
				err = os.WriteFile(path, raw, 0600)
			case "replacement":
				err = os.Rename(path, path+".old")
				if err == nil {
					err = os.WriteFile(path, raw, 0600)
				}
			case "unlink":
				err = os.Remove(path)
			case "truncate":
				err = os.Truncate(path, walHeaderSize)
			}
			if err != nil {
				t.Fatal(err)
			}
			if kind == "payload" {
				_, err = o.ReadPage(1, make([]byte, 512))
			} else {
				err = o.Validate()
			}
			if !errors.Is(err, ErrOverlayChanged) {
				t.Fatalf("change undetected: %v", err)
			}
		})
	}
}
func TestCommittedOverlayAppendHorizon(t *testing.T) {
	path, raw := overlayTestWAL(t, overlayTestFrame{1, 1, 1}, overlayTestFrame{1, 0, 2})
	prefix := raw[:walHeaderSize+frameHdrSize+512]
	if err := os.WriteFile(path, prefix, 0600); err != nil {
		t.Fatal(err)
	}
	o, err := OpenCommittedOverlay(path)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.Write(raw[len(prefix):])
	_ = f.Close()
	if err != nil {
		t.Fatal(err)
	}
	b := make([]byte, 512)
	ok, err := o.ReadPage(1, b)
	if err != nil || !ok || b[0] != 1 {
		t.Fatalf("captured prefix changed: %v %v %d", ok, err, b[0])
	}
	if err := o.Validate(); err != nil {
		t.Fatal(err)
	}
}
func TestCommittedOverlayEmpty(t *testing.T) {
	for _, kind := range []string{"missing", "zero", "no-commit"} {
		t.Run(kind, func(t *testing.T) {
			path, _ := overlayTestWAL(t, overlayTestFrame{1, 0, 1})
			if kind == "missing" {
				_ = os.Remove(path)
			}
			if kind == "zero" {
				_ = os.Truncate(path, 0)
			}
			o, err := OpenCommittedOverlay(path)
			if err != nil {
				t.Fatal(err)
			}
			defer o.Close()
			if o.DBSize != 0 || len(o.pages) != 0 {
				t.Fatal("uncommitted pages accepted")
			}
			if err := o.Validate(); err != nil {
				t.Fatal(err)
			}
			if kind == "missing" {
				if err := os.WriteFile(path, []byte{1}, 0600); err != nil {
					t.Fatal(err)
				}
				if !errors.Is(o.Validate(), ErrOverlayChanged) {
					t.Fatal("WAL appearance accepted")
				}
			}
		})
	}
}
func TestCommittedOverlayRejectsTornAndInvalid(t *testing.T) {
	for _, kind := range []string{"payload", "partial", "page-size", "header-checksum"} {
		t.Run(kind, func(t *testing.T) {
			path, raw := overlayTestWAL(t, overlayTestFrame{1, 1, 1})
			switch kind {
			case "payload":
				raw[len(raw)-1] ^= 1
			case "partial":
				raw = raw[:len(raw)-1]
			case "page-size":
				binary.BigEndian.PutUint32(raw[8:], 1<<30)
			case "header-checksum":
				raw[24] ^= 1
			}
			if err := os.WriteFile(path, raw, 0600); err != nil {
				t.Fatal(err)
			}
			o, err := OpenCommittedOverlay(path)
			if err == nil {
				_ = o.Close()
				t.Fatal("invalid WAL accepted")
			}
		})
	}
}

func TestStableOverlayPartialAppend(t *testing.T) {
	path, raw := overlayTestWAL(t, overlayTestFrame{1, 1, 7})
	cut := len(raw) - 10
	if err := os.Truncate(path, int64(cut)); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		time.Sleep(5 * time.Millisecond)
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0600)
		if err == nil {
			_, err = f.Write(raw[cut:])
			_ = f.Close()
		}
		done <- err
	}()
	o, err := OpenStableCommittedOverlay(t.Context(), path)
	if writeErr := <-done; writeErr != nil {
		t.Fatal(writeErr)
	}
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	b := make([]byte, 512)
	ok, err := o.ReadPage(1, b)
	if err != nil || !ok || b[0] != 7 {
		t.Fatalf("partial append recovery: %v %v %d", ok, err, b[0])
	}
}

func TestStableOverlayCancellationAndCorruption(t *testing.T) {
	for _, cancel := range []bool{false, true} {
		path, raw := overlayTestWAL(t, overlayTestFrame{1, 1, 7})
		raw[len(raw)-1] ^= 1
		if err := os.WriteFile(path, raw, 0600); err != nil {
			t.Fatal(err)
		}
		ctx := t.Context()
		if cancel {
			var stop context.CancelFunc
			ctx, stop = context.WithTimeout(ctx, time.Millisecond)
			defer stop()
		}
		o, err := OpenStableCommittedOverlay(ctx, path)
		if o != nil {
			o.Close()
			t.Fatal("corrupt WAL accepted")
		}
		want := ErrChecksum
		if cancel {
			want = context.DeadlineExceeded
		}
		if !errors.Is(err, want) {
			t.Fatalf("got %v want %v", err, want)
		}
	}
}

// A large transaction must not leave an oversized pending map that every
// following tiny commit clears. Report allocations and scan bytes explicitly.
func BenchmarkCommittedOverlayLargeThenTiny(b *testing.B) {
	const pages = 16384
	const commits = 4096
	frames := make([]overlayTestFrame, 0, pages+commits)
	for pg := 1; pg <= pages; pg++ {
		size := uint32(0)
		if pg == pages {
			size = pages
		}
		frames = append(frames, overlayTestFrame{uint32(pg), size, 1})
	}
	for i := 0; i < commits; i++ {
		frames = append(frames, overlayTestFrame{1, pages, byte(i)})
	}
	path, raw := overlayTestWAL(b, frames...)
	b.SetBytes(int64(len(raw)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		o, err := OpenCommittedOverlay(path)
		if err != nil {
			b.Fatal(err)
		}
		if len(o.pages) != pages || o.DBSize != pages {
			b.Fatal("lost committed pages")
		}
		if err := o.Close(); err != nil {
			b.Fatal(err)
		}
	}
}
