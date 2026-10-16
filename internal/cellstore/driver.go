package cellstore

import (
	"database/sql"
	"fmt"
	"sync"

	sqlite3 "github.com/mattn/go-sqlite3"

	"cellhive/internal/cell"
	"cellhive/internal/pagedvfs"
)

// DriverName is the database/sql driver used for every cell file.
//
// cellstore switched from modernc.org/sqlite (pure Go) to mattn/go-sqlite3
// (CGo, the real C SQLite) in ADR-159. The driver is isolated here: everything
// else keeps using database/sql.
const DriverName = "cellhive-sqlite"
const D1DriverName = "cellhive-d1-sqlite"

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
		sql.Register(D1DriverName, &sqlite3.SQLiteDriver{ConnectHook: d1ConnectHook})
	})
}

func d1ConnectHook(c *sqlite3.SQLiteConn) error {
	for _, pragma := range []string{"PRAGMA wal_autocheckpoint=0", "PRAGMA synchronous=NORMAL", "PRAGMA foreign_keys=1", "PRAGMA busy_timeout=5000"} {
		if _, err := c.Exec(pragma, nil); err != nil {
			return fmt.Errorf("cellstore: d1 %s: %w", pragma, err)
		}
	}
	for id, value := range map[int]int{
		sqlite3.SQLITE_LIMIT_LENGTH: 2_200_000, sqlite3.SQLITE_LIMIT_SQL_LENGTH: 100_000,
		sqlite3.SQLITE_LIMIT_COLUMN: 100, sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER: 100,
		sqlite3.SQLITE_LIMIT_LIKE_PATTERN_LENGTH: 50, sqlite3.SQLITE_LIMIT_ATTACHED: 1,
		sqlite3.SQLITE_LIMIT_COMPOUND_SELECT: 5, sqlite3.SQLITE_LIMIT_EXPR_DEPTH: 100,
		sqlite3.SQLITE_LIMIT_VDBE_OP: 25_000, sqlite3.SQLITE_LIMIT_FUNCTION_ARG: 127,
		sqlite3.SQLITE_LIMIT_TRIGGER_DEPTH: 10, sqlite3.SQLITE_LIMIT_WORKER_THREADS: 0,
	} {
		c.SetLimit(id, value)
	}
	c.RegisterAuthorizer(func(op int, arg1, arg2, _ string) int {
		switch op {
		case sqlite3.SQLITE_CREATE_TEMP_INDEX, sqlite3.SQLITE_CREATE_TEMP_TABLE,
			sqlite3.SQLITE_CREATE_TEMP_TRIGGER, sqlite3.SQLITE_CREATE_TEMP_VIEW,
			sqlite3.SQLITE_DROP_TEMP_INDEX, sqlite3.SQLITE_DROP_TEMP_TABLE,
			sqlite3.SQLITE_DROP_TEMP_TRIGGER, sqlite3.SQLITE_DROP_TEMP_VIEW:
			return sqlite3.SQLITE_DENY
		case sqlite3.SQLITE_FUNCTION:
			if arg1 == "load_extension" || arg2 == "load_extension" {
				return sqlite3.SQLITE_DENY
			}
		}
		return sqlite3.SQLITE_OK
	})
	return nil
}

func driverForScope(sc cell.Scope) string {
	if sc.Class == "__d1__" {
		return D1DriverName
	}
	return DriverName
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
