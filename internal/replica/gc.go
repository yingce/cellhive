package replica

import (
	"context"
	"errors"
	"time"
)

var ErrGCDeferred = errors.New("replica: physical GC deferred; external writer and reader quiescence required")

// RetireOptions authorizes destructive cleanup. The zero value never deletes.
type RetireOptions struct {
	// OfflineQuiescent asserts that all writers AND readers have been stopped
	// and drained externally for the entire operation. Production must not set it.
	OfflineQuiescent bool
	// Safe authorizes the online pin/seal/tombstone protocol. It is still not a
	// production default: deleting requires a seal (unless AllowUnsealed), no
	// live reader pin, and a due tombstone.
	Safe bool
	// AllowUnsealed is only for same-epoch covered-L0 cleanup. It still requires
	// no live reader pin and a due tombstone.
	AllowUnsealed bool
	// Grace is the tombstone window. 0 uses the default; a negative value means
	// no delay (tests and explicitly offline maintenance only).
	Grace time.Duration
	// DrainTimeout bounds WaitReaderPins. 0 waits until ctx is done.
	DrainTimeout time.Duration
	// Now is a test seam for lease and tombstone deadlines.
	Now func() time.Time
	// Job identifies the maintenance operation that owns the tombstones.
	Job string
	// Verify is checked immediately before each Delete. It stops observed owner
	// loss, but cannot close the check/delete TOCTOU or establish quiescence.
	Verify func(context.Context) error
}

// authorized reports whether the caller provided any deletion authorization.
func (o RetireOptions) authorized() bool { return o.OfflineQuiescent || o.Safe }
