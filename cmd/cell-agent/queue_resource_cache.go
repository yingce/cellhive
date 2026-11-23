package main

import (
	"context"
	"slices"
	"sync"
	"time"

	"cellhive/internal/control"
)

const queueResourceCacheTTL = 5 * time.Second

type queueResourceSource interface {
	ResourcesByKindWithAuthority(context.Context, string) ([]control.ResourceRef, string, error)
	AuthorityKey() string
}

// Discovery is only a scheduling hint: queue admission, owner checks and
// dispatch fencing still validate each access. Never use this cache to
// authorize resource deletion or a binding request.
type queueResourceCache struct {
	src       queueResourceSource
	now       func() time.Time
	mu        sync.Mutex
	rows      []control.ResourceRef
	authority string
	until     time.Time
	flight    *queueResourceFlight
}

type queueResourceFlight struct {
	done     chan struct{}
	rows     []control.ResourceRef
	err      error
	canceled bool
}

func (c *queueResourceCache) Resources(ctx context.Context) ([]control.ResourceRef, error) {
	now := c.now
	if now == nil {
		now = time.Now
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		authority := c.src.AuthorityKey()
		c.mu.Lock()
		if now().Before(c.until) && authority == c.authority {
			rows := slices.Clone(c.rows)
			c.mu.Unlock()
			return rows, nil
		}
		if f := c.flight; f != nil {
			c.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-f.done:
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				if f.canceled {
					continue
				}
				return slices.Clone(f.rows), f.err
			}
		}
		f := &queueResourceFlight{done: make(chan struct{})}
		c.flight = f
		// Invalidate before I/O: a failed refresh must not revive an expired view.
		c.rows, c.until = nil, time.Time{}
		started := now()
		c.mu.Unlock()
		rows, identity, err := c.src.ResourcesByKindWithAuthority(ctx, "queue")
		if ctx.Err() != nil {
			err = ctx.Err()
		}
		c.mu.Lock()
		if err == nil {
			c.rows = slices.Clone(rows)
			c.authority, c.until = identity, started.Add(queueResourceCacheTTL)
			f.rows = c.rows
		}
		f.err, f.canceled = err, ctx.Err() != nil
		c.flight = nil
		close(f.done)
		c.mu.Unlock()
		return slices.Clone(f.rows), err
	}
}
