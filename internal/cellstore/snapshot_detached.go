package cellstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	sqlite3 "github.com/mattn/go-sqlite3"
)

var ErrSnapshotTooLarge = errors.New("cellstore: snapshot exceeds size budget")

// SnapshotDetached copies a stable SQLite image without checkpointing the source
// WAL. Only the local backup holds writeMu; the callback runs on the private
// image after releasing it. Unlike VACUUM, backup preserves page numbers.
func (c *Cell) SnapshotDetached(ctx context.Context, partBytes int, maxBytes int64, fn SnapshotPartFunc) (uint64, uint32, uint32, error) {
	dir, err := os.MkdirTemp(filepath.Dir(c.Path), ".cellhive-snapshot-")
	if err != nil {
		return 0, 0, 0, err
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "image.db")
	u := url.URL{Scheme: "file", Path: path}
	dst, err := sql.Open(DriverName, u.String()+"?_journal_mode=DELETE")
	if err != nil {
		return 0, 0, 0, err
	}
	defer dst.Close()
	err = func() error {
		c.writeMu.Lock()
		defer c.writeMu.Unlock()
		srcConn, err := c.DB.Conn(ctx)
		if err != nil {
			return err
		}
		defer srcConn.Close()
		if maxBytes > 0 {
			var pages, size int64
			if err := srcConn.QueryRowContext(ctx, "PRAGMA page_count").Scan(&pages); err != nil {
				return err
			}
			if err := srcConn.QueryRowContext(ctx, "PRAGMA page_size").Scan(&size); err != nil {
				return err
			}
			if size <= 0 || pages > maxBytes/size {
				return fmt.Errorf("%w: image %d pages of %d bytes, budget %d", ErrSnapshotTooLarge, pages, size, maxBytes)
			}
		}

		dstConn, err := dst.Conn(ctx)
		if err != nil {
			return err
		}
		defer dstConn.Close()
		return srcConn.Raw(func(src any) error {
			return dstConn.Raw(func(dest any) error {
				b, err := dest.(*sqlite3.SQLiteConn).Backup("main", src.(*sqlite3.SQLiteConn), "main")
				if err != nil {
					return err
				}
				previous := -1
				for {
					if err := ctx.Err(); err != nil {
						_ = b.Finish()
						return err
					}
					done, err := b.Step(256)
					if err != nil {
						_ = b.Finish()
						return err
					}
					if done {
						return b.Finish()
					}
					// BUSY/LOCKED can be returned as done=false without an error.
					if remaining := b.Remaining(); remaining == previous {
						select {
						case <-ctx.Done():
							_ = b.Finish()
							return ctx.Err()
						case <-time.After(time.Millisecond):
						}
					} else {
						previous = remaining
					}
				}
			})
		})
	}()
	if err != nil {
		return 0, 0, 0, fmt.Errorf("cellstore: snapshot backup: %w", err)
	}
	image := &Cell{Path: path, DB: dst}
	return image.snapshotStream(ctx, partBytes, fn)
}

// CleanupSnapshotCopies removes crash-orphaned optional snapshots. The agent
// calls this only during startup, with the exclusive instance directory lock.
// It never removes cell databases or explores symlinked directories.
func CleanupSnapshotCopies(dataDir string) error {
	root := filepath.Join(dataDir, "cells")
	return filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if os.IsNotExist(err) && path == root {
			return nil
		}
		if err != nil {
			return err
		}
		if !d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if len(strings.Split(rel, string(filepath.Separator))) == 3 {
			if strings.HasPrefix(d.Name(), ".cellhive-snapshot-") {
				if err := os.RemoveAll(path); err != nil {
					return err
				}
			}
			return filepath.SkipDir
		}
		return nil
	})
}
