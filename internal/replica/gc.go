package replica

import (
	"context"
	"errors"
	"time"
)

var ErrGCDeferred = errors.New("replica: physical GC deferred; external writer and reader quiescence required")

// RetireOptions authorizes destructive cleanup. The zero value never deletes.
type RetireOptions struct {
	// Ready proves a resident activation no longer reads its remote paged cut.
	// The caller must retain its local request/eviction gate for the GC pass.
	// Online native epoch GC requires both Ready and fresh Verify callbacks.
	Ready func(context.Context) (bool, error)
	// OfflineQuiescent asserts that all writers AND readers have been stopped
	// and drained externally for the entire operation. Production must not set it.
	OfflineQuiescent bool
	// Safe enables native online epoch GC when Ready and Verify are supplied.
	Safe bool
	// Grace is the epoch object upload-age margin (default one hour).
	// A negative value disables the delay for tests or offline maintenance.
	Grace time.Duration
	// Now is a test seam for object age.
	Now func() time.Time
	// Job identifies the maintenance operation.
	Job string
	// Verify is checked immediately before each Delete. It stops observed owner
	// loss, but cannot close the check/delete TOCTOU or establish quiescence.
	Verify func(context.Context) error
}

func (o RetireOptions) now() time.Time {
	if o.Now != nil {
		return o.Now()
	}
	return time.Now()
}
