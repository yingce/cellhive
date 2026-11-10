package dosupervisor

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"cellhive/internal/cell"
	"cellhive/internal/ltx"
	"cellhive/internal/wal"
)

type spoolCommitter struct {
	paths  []string
	hashes [][32]byte
}

func (*spoolCommitter) Claim(context.Context, cell.Scope) error { return nil }
func (*spoolCommitter) Commit(context.Context, cell.Scope, uint64, []byte) error {
	return errors.New("buffered commit forbidden")
}
func (c *spoolCommitter) CommitSnapshotFile(_ context.Context, _ cell.Scope, _, _ uint64, path string, size int64) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return err
	}
	if n != size {
		return errors.New("wrong spool size")
	}
	c.paths = append(c.paths, path)
	c.hashes = append(c.hashes, [32]byte(h.Sum(nil)))
	if len(c.paths) == 1 {
		return errors.New("lost acknowledgement")
	}
	return nil
}
func sparseImage(t *testing.T, pages int64) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "db.sqlite")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var header [4096]byte
	copy(header[:], "SQLite format 3\x00")
	binary.BigEndian.PutUint16(header[16:18], 4096)
	if _, err = f.Write(header[:]); err != nil {
		t.Fatal(err)
	}
	if err = f.Truncate(pages * 4096); err != nil {
		t.Fatal(err)
	}
	return path
}
func TestSnapshotSpoolRetainsExactRetry(t *testing.T) {
	path := sparseImage(t, 4)
	c := &spoolCommitter{}
	s := &Supervisor{Committer: c, Epoch: 1}
	fs := &fileState{path: path, scope: cell.Scope{Namespace: "test", Class: "c", ID: "x"}, cursor: wal.NewCursor()}
	if err := s.snapshot(t.Context(), fs, "baseline"); err == nil {
		t.Fatal("lost acknowledgement accepted")
	}
	defer os.Remove(fs.pendingFile)
	if fs.seq != 0 || fs.baseline || fs.pendingFile == "" {
		t.Fatal("failed snapshot advanced cursor")
	}
	if err := os.WriteFile(path, []byte("changed source"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := s.flushPending(t.Context(), fs); err != nil {
		t.Fatal(err)
	}
	if c.paths[0] != c.paths[1] || c.hashes[0] != c.hashes[1] {
		t.Fatal("retry regenerated snapshot")
	}
	if fs.seq != 1 || !fs.baseline || fs.pendingFile != "" {
		t.Fatal("snapshot not acknowledged")
	}
	if _, err := os.Stat(c.paths[0]); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("spool not removed")
	}
}
func TestSnapshotSpoolSkipsSQLiteLockPage(t *testing.T) {
	// A sparse logical image crossing 1GiB reproduces the large-DB boundary
	// without allocating a full database image in either test or capture.
	pages := int64(0x40000000/4096 + 2)
	name, _, commit, err := spoolSnapshot(t.Context(), sparseImage(t, pages), 7)
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(name)
	f, err := os.Open(name)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	d, err := ltx.NewNativeDecoder(bufio.NewReader(f))
	if err != nil {
		t.Fatal(err)
	}
	count := int64(0)
	for {
		page, err := d.ReadPage()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if page.PageNo == 0x40000000/4096+1 {
			t.Fatal("lock page encoded")
		}
		count++
	}
	if int64(commit) != pages || count != pages-1 || d.Header.MaxTXID != 7 {
		t.Fatalf("commit=%d count=%d", commit, count)
	}
}

type deltaSpoolCommitter struct {
	spoolCommitter
	native int
}

func (c *deltaSpoolCommitter) CommitNativeFile(ctx context.Context, sc cell.Scope, epoch, tx uint64, path string, size int64) error {
	c.native++
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	d, err := ltx.NewNativeDecoder(f)
	if err != nil {
		return err
	}
	if d.Header.IsSnapshot() || d.Header.MinTXID != 11 || d.Header.MaxTXID != 11 {
		return errors.New("incremental range changed")
	}
	for {
		_, err = d.ReadPage()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}
	return c.spoolCommitter.CommitSnapshotFile(ctx, sc, epoch, tx, path, size)
}
func TestLargeDeltaSpoolRetainsNativeRangeAndRetry(t *testing.T) {
	c := &deltaSpoolCommitter{}
	s := &Supervisor{Committer: c, Epoch: 1}
	fs := &fileState{scope: cell.Scope{Namespace: "test", Class: "c", ID: "delta"}, seq: 10, baseline: true, cursor: wal.NewCursor()}
	page := make([]byte, 4096)
	frames := make([]wal.Frame, 8192)
	for i := range frames {
		frames[i] = wal.Frame{PageNo: uint32(i + 1), Data: page}
	}
	frames[len(frames)-1].DBSize = uint32(len(frames))
	res := wal.PollResult{Header: wal.Header{PageSize: 4096}, Transactions: []wal.Transaction{{Frames: frames}}}
	if err := s.delta(t.Context(), fs, res); err == nil {
		t.Fatal("lost ack accepted")
	}
	defer os.Remove(fs.pendingFile)
	if fs.seq != 10 || fs.pendingFile == "" || fs.pendingSnapshot || c.native != 1 {
		t.Fatal("incorrect pending incremental state")
	}
	page[0] = 99
	if err := s.flushPending(t.Context(), fs); err != nil {
		t.Fatal(err)
	}
	if fs.seq != 11 || c.native != 2 || c.paths[0] != c.paths[1] || c.hashes[0] != c.hashes[1] {
		t.Fatal("retry changed the native transaction")
	}
	if _, err := os.Stat(c.paths[0]); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("delta spool retained after ack")
	}
}
