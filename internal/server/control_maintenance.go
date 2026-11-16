package server

import (
	"context"
	"errors"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/owner"
)

// RunOwnedMaintenance executes a local maintenance write only at its data owner.
func (s *Server) RunOwnedMaintenance(ctx context.Context, sc cell.Scope, work func() (int, error)) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	var before cell.Owner
	var check func() bool
	if s.Owner != nil {
		var err error
		before, _, err = s.Owner.Resolve(ctx, sc)
		if errors.Is(err, owner.ErrUnowned) {
			return 0, nil
		}
		if err != nil {
			return 0, err
		}
		if before.Node != s.Cfg.NodeID || before.Session != s.Owner.Session || !s.ownerLive(ctx, before, time.Now()) || !s.ackAllowed(time.Now()) {
			return 0, nil
		}
		var ok bool
		check, ok = s.Owner.ReadFence(sc)
		if !ok {
			return 0, owner.ErrNotOwner
		}
		defer check()
	}
	end, err := s.BeginResourceRequest(ctx, sc)
	if err != nil {
		return 0, err
	}
	defer end()
	n := 0
	if err := s.CaptureWrite(ctx, sc, func() error { var e error; n, e = work(); return e }); err != nil {
		return 0, err
	}
	if check != nil {
		after, _, err := s.Owner.Resolve(ctx, sc)
		if err != nil {
			return 0, err
		}
		if !sameCleanupControlOwner(before, after) || !s.ackAllowed(time.Now()) || !check() {
			return 0, owner.ErrNotOwner
		}
	}
	return n, nil
}
