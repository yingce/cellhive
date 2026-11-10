package cellstore

import (
	"cellhive/internal/cell"
	"context"
)

// AcquireResident pins an existing local activation for maintenance without
// opening or hydrating it. Release must run after all remote maintenance I/O.
func (s *Store) AcquireResident(ctx context.Context, sc cell.Scope) (*Cell, func(), error) {
	p, err := s.Path(sc)
	if err != nil {
		return nil, nil, err
	}
	release, err := s.BeginRequestContext(ctx, sc.String())
	if err != nil {
		return nil, nil, err
	}
	s.mu.Lock()
	c := s.cells[p]
	if c != nil && c.Closed() {
		c = nil
	}
	s.mu.Unlock()
	if c == nil {
		release()
		return nil, func() {}, nil
	}
	return c, release, nil
}
