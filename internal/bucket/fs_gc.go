package bucket

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const fsGCScanIdle = 10 * time.Minute
const fsGCMaxScans = 2

type fsGCScan struct {
	prefix  string
	keys    []string
	expires time.Time
	timer   *time.Timer
}

type fsGCCursor struct {
	ID     string `json:"id"`
	Offset int    `json:"offset"`
}

// One complete cold walk builds a stable discovery snapshot. Subsequent pages
// do no directory I/O. Memory is O(keys) per sweep, with at most two sweeps per
// bucket. Idle leases release abandoned snapshots, including on process use
// without any subsequent listing. Live List/ListPage semantics are unchanged.
func (b *FSBucket) ListGCPage(ctx context.Context, prefix, cursor string, limit int) ([]ObjectInfo, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, cursor, err
	}
	if prefix == "" || !strings.HasSuffix(prefix, "/") || strings.Contains(prefix, "..") || strings.HasPrefix(prefix, "/") || strings.Contains(prefix, "\\") || limit < 1 || limit > 1000 {
		return nil, cursor, fmt.Errorf("bucket: invalid cold GC page")
	}
	b.gcMu.Lock()
	defer b.gcMu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, cursor, err
	}
	var c fsGCCursor
	var scan *fsGCScan
	if cursor == "" {
		for id, old := range b.gcScans {
			if !time.Now().Before(old.expires) {
				old.timer.Stop()
				delete(b.gcScans, id)
			}
		}
		if len(b.gcScans) >= fsGCMaxScans {
			return nil, cursor, ErrGCScanBusy
		}
		scan = &fsGCScan{prefix: prefix}
		b.lists.Add(1)
		err := filepath.WalkDir(b.path(prefix), func(path string, d fs.DirEntry, err error) error {
			if e := ctx.Err(); e != nil {
				return e
			}
			if err != nil {
				if os.IsNotExist(err) {
					return nil
				}
				return err
			}
			if d.IsDir() {
				return nil
			}
			b.gcScanEntries.Add(1)
			key, err := filepath.Rel(b.root, path)
			if err != nil {
				return err
			}
			key = filepath.ToSlash(key)
			if strings.HasPrefix(key, prefix) && !strings.HasSuffix(key, ".tmp") {
				scan.keys = append(scan.keys, key)
			}
			return nil
		})
		if err != nil {
			return nil, cursor, err
		}
		sort.Strings(scan.keys)
		var nonce [16]byte
		if _, err := rand.Read(nonce[:]); err != nil {
			return nil, cursor, err
		}
		c.ID = hex.EncodeToString(nonce[:])
		if b.gcScans == nil {
			b.gcScans = make(map[string]*fsGCScan)
		}
		b.gcScans[c.ID] = scan
	} else {
		if err := json.Unmarshal([]byte(cursor), &c); err != nil {
			return nil, cursor, ErrGCScanExpired
		}
		scan = b.gcScans[c.ID]
		if scan == nil || scan.prefix != prefix || !time.Now().Before(scan.expires) || c.Offset < 0 || c.Offset > len(scan.keys) {
			return nil, cursor, ErrGCScanExpired
		}
	}
	end := c.Offset + limit
	if end > len(scan.keys) {
		end = len(scan.keys)
	}
	items := make([]ObjectInfo, 0, end-c.Offset)
	for _, key := range scan.keys[c.Offset:end] {
		items = append(items, ObjectInfo{Key: key})
	}
	if end == len(scan.keys) {
		if scan.timer != nil {
			scan.timer.Stop()
		}
		delete(b.gcScans, c.ID)
		return items, "", nil
	}
	b.renewGCScan(c.ID, scan)
	c.Offset = end
	next, _ := json.Marshal(c)
	return items, string(next), nil
}

func (b *FSBucket) renewGCScan(id string, scan *fsGCScan) {
	if scan.timer != nil {
		scan.timer.Stop()
	}
	scan.expires = time.Now().Add(fsGCScanIdle)
	expires := scan.expires
	scan.timer = time.AfterFunc(fsGCScanIdle, func() {
		b.gcMu.Lock()
		defer b.gcMu.Unlock()
		if b.gcScans[id] == scan && scan.expires == expires {
			delete(b.gcScans, id)
		}
	})
}

func (b *FSBucket) KeepGCScanAlive(ctx context.Context, prefix, cursor string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var c fsGCCursor
	if err := json.Unmarshal([]byte(cursor), &c); err != nil {
		return ErrGCScanExpired
	}
	b.gcMu.Lock()
	defer b.gcMu.Unlock()
	scan := b.gcScans[c.ID]
	if scan == nil || scan.prefix != prefix || !time.Now().Before(scan.expires) {
		return ErrGCScanExpired
	}
	b.renewGCScan(c.ID, scan)
	return nil
}
