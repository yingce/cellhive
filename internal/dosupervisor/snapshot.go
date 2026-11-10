package dosupervisor

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"time"

	"cellhive/internal/ltx"
	"cellhive/internal/wal"
)

// spoolSnapshot retains only one page and the encoder's compact page index.
// The caller holds the capture boundary and validates the paging generation.
func spoolSnapshot(ctx context.Context, path string, watermark uint64) (name string, size int64, pages uint32, err error) {
	src, err := os.Open(path)
	if err != nil {
		return "", 0, 0, err
	}
	defer src.Close()
	before, err := src.Stat()
	if err != nil {
		return "", 0, 0, err
	}
	var header [100]byte
	if _, err = io.ReadFull(src, header[:]); err != nil {
		return "", 0, 0, err
	}
	if string(header[:16]) != "SQLite format 3\x00" {
		return "", 0, 0, fmt.Errorf("dosupervisor: invalid SQLite header")
	}
	ps := uint32(binary.BigEndian.Uint16(header[16:18]))
	if ps == 1 {
		ps = 65536
	}
	if ps < 512 || ps > 65536 || ps&(ps-1) != 0 || before.Size()%int64(ps) != 0 || before.Size()/int64(ps) > int64(^uint32(0)) {
		return "", 0, 0, fmt.Errorf("dosupervisor: invalid SQLite image size")
	}
	pages = uint32(before.Size() / int64(ps))
	// A partial checkpoint may leave committed (even already acknowledged)
	// pages only in WAL. Publishing raw main would supersede those transactions
	// before follow-up capture. Merge its committed prefix into this same native
	// snapshot, retaining frame references rather than whole WAL page payloads.
	overlay, err := wal.OpenStableCommittedOverlay(ctx, path+"-wal")
	if err != nil {
		return "", 0, 0, err
	}
	defer overlay.Close()
	if overlay.PageSize != 0 && overlay.PageSize != ps {
		return "", 0, 0, fmt.Errorf("dosupervisor: snapshot WAL page size differs")
	}
	if overlay.DBSize != 0 {
		pages = overlay.DBSize
	}
	dst, err := os.CreateTemp("", "cellhive-do-snapshot-*.ltx")
	if err != nil {
		return "", 0, 0, err
	}
	name = dst.Name()
	defer func() {
		dst.Close()
		if err != nil {
			os.Remove(name)
		}
	}()
	w := bufio.NewWriterSize(dst, 256<<10)
	enc, err := ltx.NewNativeEncoder(w, ltx.NativeHeader{PageSize: ps, Commit: pages, MinTXID: 1, MaxTXID: watermark, Timestamp: time.Now().UnixMilli()})
	if err != nil {
		return name, 0, pages, err
	}
	page := make([]byte, ps)
	for n := uint64(1); n <= uint64(pages); n++ {
		if err = ctx.Err(); err != nil {
			return name, 0, pages, err
		}
		if n == uint64(0x40000000/ps)+1 {
			continue
		}
		found, e := overlay.ReadPage(uint32(n), page)
		if e != nil {
			return name, 0, pages, e
		}
		if !found {
			if _, err = src.ReadAt(page, int64(n-1)*int64(ps)); err != nil {
				return name, 0, pages, err
			}
		}
		if err = enc.WritePage(ltx.WALPage{PageNo: uint32(n), Data: page}); err != nil {
			return name, 0, pages, err
		}
	}
	after, err := src.Stat()
	if err != nil {
		return name, 0, pages, err
	}
	if after.Size() != before.Size() || !after.ModTime().Equal(before.ModTime()) {
		return name, 0, pages, fmt.Errorf("dosupervisor: SQLite image changed during snapshot")
	}
	current, err := os.Stat(path)
	if err != nil {
		return name, 0, pages, err
	}
	if !os.SameFile(before, current) {
		return name, 0, pages, fmt.Errorf("dosupervisor: SQLite image replaced during snapshot")
	}
	if err = overlay.Validate(); err != nil {
		return name, 0, pages, err
	}
	if err = enc.Close(); err != nil {
		return name, 0, pages, err
	}
	if err = w.Flush(); err != nil {
		return name, 0, pages, err
	}
	if err = dst.Sync(); err != nil {
		return name, 0, pages, err
	}
	if err = dst.Close(); err != nil {
		return name, 0, pages, err
	}
	return name, int64(enc.BytesWritten()), pages, nil
}

// spoolNativePages avoids a second whole-transaction encoded byte buffer.
func spoolNativePages(ctx context.Context, h ltx.NativeHeader, pages []ltx.WALPage) (name string, size int64, err error) {
	f, err := os.CreateTemp("", "cellhive-do-delta-*.ltx")
	if err != nil {
		return "", 0, err
	}
	name = f.Name()
	defer func() {
		f.Close()
		if err != nil {
			os.Remove(name)
		}
	}()
	w := bufio.NewWriterSize(f, 256<<10)
	enc, err := ltx.NewNativeEncoder(w, h)
	if err != nil {
		return name, 0, err
	}
	for _, page := range pages {
		if err = ctx.Err(); err != nil {
			return name, 0, err
		}
		if err = enc.WritePage(page); err != nil {
			return name, 0, err
		}
	}
	if err = enc.Close(); err != nil {
		return name, 0, err
	}
	if err = w.Flush(); err != nil {
		return name, 0, err
	}
	if err = f.Sync(); err != nil {
		return name, 0, err
	}
	if err = f.Close(); err != nil {
		return name, 0, err
	}
	return name, int64(enc.BytesWritten()), nil
}
