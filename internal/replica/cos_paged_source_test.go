package replica_test

import (
	"context"

	"cellhive/internal/pagedvfs"
	"cellhive/internal/replica"
)

type pageSource struct{ pf *replica.PageFetcher }

func (p pageSource) PageSize() int { return int(p.pf.PageSize()) }
func (p pageSource) Commit() int   { return int(p.pf.Commit()) }
func (p pageSource) ReadPage(pgno uint32) ([]byte, error) {
	return p.pf.Page(context.Background(), pgno)
}
func (p pageSource) ReadRun(pgno uint32, maxPages int) ([][]byte, error) {
	return p.pf.ReadRun(context.Background(), pgno, maxPages)
}

var _ pagedvfs.Source = pageSource{}
var _ pagedvfs.RunSource = pageSource{}
