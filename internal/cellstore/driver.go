package cellstore

import (
	"database/sql"
	"fmt"
	"sync"

	sqlite3 "github.com/mattn/go-sqlite3"

	"cellhive/internal/pagedvfs"
)

// DriverName is the database/sql driver used for every cell file.
//
// cellstore switched from modernc.org/sqlite (pure Go) to mattn/go-sqlite3
// (CGo, the real C SQLite) in ADR-159. The driver is isolated here: everything
// else keeps using database/sql.
const DriverName = "cellhive-sqlite"

var registerOnce sync.Once

// RegisterDriver installs the platform driver (idempotent). It is called from
// init, so importing cellstore is enough.
//
// The CGo driver is the same SQLite version as before (3.53.4), so cell files
// and WAL frames are byte-compatible. Per-connection pragmas that database/sql
// cannot express via the DSN (wal_autocheckpoint) are applied in ConnectHook.
func RegisterDriver() {
	registerOnce.Do(func() {
		sql.Register(DriverName, &sqlite3.SQLiteDriver{
			ConnectHook: func(c *sqlite3.SQLiteConn) error {
				// cellstore does its own checkpoints (capture reads the WAL);
				// SQLite's automatic checkpoint would truncate frames the
				// capture cursor has not consumed yet.
				if _, err := c.Exec("PRAGMA wal_autocheckpoint=0", nil); err != nil {
					return fmt.Errorf("cellstore: wal_autocheckpoint: %w", err)
				}
				if _, err := c.Exec("PRAGMA synchronous=NORMAL", nil); err != nil {
					return fmt.Errorf("cellstore: synchronous: %w", err)
				}
				if _, err := c.Exec("PRAGMA foreign_keys=1", nil); err != nil {
					return fmt.Errorf("cellstore: foreign_keys: %w", err)
				}
				if _, err := c.Exec("PRAGMA busy_timeout=5000", nil); err != nil {
					return fmt.Errorf("cellstore: busy_timeout: %w", err)
				}
				return nil
			},
		})
	})
}

func init() { RegisterDriver() }

// OpenDSN builds the DSN for a cell file: WAL journal (capture reads frames)
// with a busy timeout. The rest of the pragmas live in ConnectHook.
func OpenDSN(path string) string {
	return "file:" + path + "?_journal_mode=WAL&_busy_timeout=5000"
}

// Open opens a cell-store SQLite file with the platform driver and pragmas. It
// is used by OpenAt, tests and the ops tools so they all exercise the same
// driver as production. A path currently registered with the paged VFS opens
// through it, so a sparse file is never read as zeros (ADR-160).
func Open(path string) (*sql.DB, error) {
	dsn := OpenDSN(path)
	if pagedvfs.IsPaged(path) {
		dsn += "&vfs=" + pagedvfs.VFSName
	}
	return sql.Open(DriverName, dsn)
}
