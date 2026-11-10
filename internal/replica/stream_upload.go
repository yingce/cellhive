package replica

import (
	"context"
	"fmt"
	"io"
	"os"

	"cellhive/internal/cell"
)

// uploadFile publishes a stable validated spool with bounded upload memory.
func (m *Manager) uploadFile(ctx context.Context, sc cell.Scope, epoch uint64, key string, f *os.File, size int64) (string, error) {
	objects := m.resourceObjects()
	etag, err := objects.PutReader(ctx, key, f, size)
	if err != nil {
		return "", err
	}
	// An opened snapshot spool must remain immutable through publication. A
	// changed length is a caller bug; validation already checked its full CRC.
	if end, e := f.Seek(0, io.SeekEnd); e != nil {
		return "", e
	} else if end != size {
		return "", fmt.Errorf("replica: snapshot spool changed during upload")
	}
	etag, err = m.finishPublication(ctx, sc, epoch, key, etag, true)
	if err == nil {
		m.localCache.remember(key, etag, io.NewSectionReader(f, 0, size), size)
	}
	return etag, err
}
