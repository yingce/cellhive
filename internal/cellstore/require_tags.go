//go:build !sqlite_fts5 || !sqlite_dbstat

package cellstore

/*
#error "CellHive needs SQLite feature tags. Build/test with the Makefile (TAGS), e.g. make build / make test; or: go build -tags 'sqlite_fts5 sqlite_dbstat sqlite_math_functions sqlite_column_metadata sqlite_preupdate_hook' ./..."
*/
import "C"
