package peer

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"cellhive/internal/ltx"
)

func TestSealedTailPaginationReadVolume(t *testing.T) {
	for _, n := range []int{2048, 4096, 8192} {
		sp, err := NewSpool(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		dir := sp.sessionDir("review")
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatal(err)
		}
		f, err := os.Create(filepath.Join(dir, sessionLogName))
		if err != nil {
			t.Fatal(err)
		}
		w := bufio.NewWriterSize(f, 1<<20)
		seg := ltx.Encode(ltx.Header{Kind: ltx.KindDelta, Epoch: 1, StartTxID: 1, EndTxID: 1}, bytes.Repeat([]byte{1}, 4096))
		for i := 1; i <= n; i++ {
			if _, err := w.Write(encodeSessionRecord(uint64(i), "app/__kv__/x", 1, [][]byte{seg})); err != nil {
				t.Fatal(err)
			}
		}
		if err := w.Flush(); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, sealName), []byte(fmt.Sprint(n)), 0644); err != nil {
			t.Fatal(err)
		}
		info, err := os.Stat(filepath.Join(dir, sessionLogName))
		if err != nil {
			t.Fatal(err)
		}
		before := tailReadChars()
		start := time.Now()
		for after := uint64(0); after < uint64(n); {
			page, err := sp.TailPage("review", after, 256, 48<<20)
			if err != nil || len(page) == 0 {
				t.Fatalf("tail %v", err)
			}
			after = page[len(page)-1].Sequence
		}
		readBytes := tailReadChars() - before
		t.Logf("frames=%d pages=%d elapsed=%s read_bytes=%d file_bytes=%d", n, n/256, time.Since(start), readBytes, info.Size())
		if before > 0 && readBytes > 3*info.Size() {
			t.Fatalf("pagination reread too much data: read=%d log=%d", readBytes, info.Size())
		}
	}
}

// Linux rchar counts bytes returned by read syscalls, including page-cache
// hits. Bound actual read amplification rather than noisy elapsed time.
func tailReadChars() int64 {
	raw, err := os.ReadFile("/proc/self/io")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(raw), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[0] == "rchar:" {
			n, _ := strconv.ParseInt(fields[1], 10, 64)
			return n
		}
	}
	return 0
}
