package bucket

import (
	"context"
	"errors"
)

var (
	ErrGCScanExpired = errors.New("bucket: cold GC scan expired; start a new sweep")
	ErrGCScanBusy    = errors.New("bucket: cold GC scan capacity exhausted")
)

// GCPageLister is only for cold reclamation discovery. Cursors are opaque;
// pages can be empty or repeat logical keys. A snapshot may omit newly created
// keys until the next sweep. Every deletion still requires a fresh point read
// and the subsystem's authority fence. Never use this for restore or hot reads.
// KeepGCScanAlive renews an idle lease while a caller drains a pending page.
type GCPageLister interface {
	ListGCPage(context.Context, string, string, int) ([]ObjectInfo, string, error)
	KeepGCScanAlive(context.Context, string, string) error
}
