package server

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net/http"
	"time"

	"cellhive/internal/artifacts"
	"cellhive/internal/control"
	"cellhive/internal/objgc"
	"cellhive/internal/owner"
	"cellhive/internal/purge"
)

// reclamationCommit has no local-only fallback. A durable exclusion is required
// even when the object upload is a deduplicated same-SHA no-op.
func (s *Server) reclamationCommit(ctx context.Context, write func() error) error {
	if s.Control == nil || s.Capture == nil || s.Owner == nil {
		return fmt.Errorf("reclamation: owner and remote capture required")
	}
	before, err := s.cleanupControlOwner(ctx, "")
	if err != nil {
		return err
	}
	if before.Node != s.Owner.NodeID || before.Session != s.Owner.Session {
		return owner.ErrNotOwner
	}
	if err := s.capturedWrite(ctx, control.Scope(), write); err != nil {
		return err
	}
	after, err := s.cleanupControlOwner(ctx, "")
	if err != nil {
		return err
	}
	if !sameCleanupControlOwner(before, after) {
		return owner.ErrNotOwner
	}
	return nil
}

func (s *Server) reserveArtifact(ctx context.Context, kind, id, path, mode string) (artifacts.Finish, error) {
	var op control.ArtifactOp
	err := s.reclamationCommit(ctx, func() error {
		var err error
		op, err = s.Control.BeginArtifact(ctx, kind, id, path, mode, rand.Text())
		return err
	})
	if err != nil {
		return nil, err
	}
	return func(ctx context.Context, sha string, size int64, success bool) error {
		return s.reclamationCommit(ctx, func() error { return s.Control.FinishArtifact(ctx, op, sha, size, success) })
	}, nil
}

type reclamationRefs struct {
	control.GCRefs
	server *Server
}

func (r reclamationRefs) SetMark(ctx context.Context, id string, ms int64) error {
	return r.server.reclamationCommit(ctx, func() error { return r.GCRefs.SetMark(ctx, id, ms) })
}
func (r reclamationRefs) ClearMark(ctx context.Context, id string) error {
	return r.server.reclamationCommit(ctx, func() error { return r.GCRefs.ClearMark(ctx, id) })
}
func (r reclamationRefs) Claim(ctx context.Context, id string) (func(error) error, bool, error) {
	var op control.ArtifactOp
	var claimed bool
	err := r.server.reclamationCommit(ctx, func() error {
		var err error
		op, claimed, err = r.S.ClaimArtifactGC(ctx, r.Kind, id, rand.Text(), r.server.Cfg.BundleGCGrace)
		return err
	})
	if errors.Is(err, control.ErrArtifactUnavailable) || errors.Is(err, control.ErrPurgeInProgress) {
		return nil, false, nil
	}
	if err != nil || !claimed {
		return nil, false, err
	}
	return func(ioErr error) error {
		return r.server.reclamationCommit(ctx, func() error { return r.S.FinishArtifactGC(ctx, op, ioErr) })
	}, true, nil
}

func (s *Server) ArtifactGCPass(ctx context.Context, kind string) (objgc.Result, error) {
	if err := s.reclamationCommit(ctx, func() error { return nil }); err != nil {
		return objgc.Result{}, err
	}
	st := s.artifactStore()
	if st == nil {
		return objgc.Result{}, fmt.Errorf("reclamation: no bucket")
	}
	var items objgc.Items
	switch kind {
	case "bundle":
		items = artifacts.BundleItems{Store: st}
	case "asset":
		items = artifacts.AssetItems{Store: st}
	default:
		return objgc.Result{}, fmt.Errorf("reclamation: invalid kind")
	}
	return (&objgc.GC{Items: items, Refs: reclamationRefs{control.GCRefs{S: s.Control, Kind: kind}, s}, Grace: s.Cfg.BundleGCGrace, Log: s.Log}).Pass(ctx)
}

func (s *Server) NewPurgeExecutor() *control.PurgeExecutor {
	holder := rand.Text()
	return &control.PurgeExecutor{Store: s.Control, Holder: holder, Commit: s.reclamationCommit, Hook: func(ctx context.Context, ns, worker string) (bool, error) {
		p := &purge.Purger{Bucket: s.Bucket, Cells: s.Store, BeforeDeleteAssets: func(ctx context.Context, keys []string) error {
			return s.reclamationCommit(ctx, func() error { return s.Control.FencePurgeAssetKeys(ctx, ns, worker, holder, keys) })
		}}
		return p.Run(ctx, ns, worker)
	}}
}

func (s *Server) RunPurgeLoop(ctx context.Context, interval time.Duration) {
	s.Control.RunPurgeLoop(ctx, s.Log, interval, s.NewPurgeExecutor())
}

func (s *Server) RunArtifactGCLoop(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		return
	}
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			for _, kind := range []string{"bundle", "asset"} {
				if _, err := s.ArtifactGCPass(ctx, kind); err != nil && s.Log != nil {
					s.Log.Warn("artifact GC retained", "kind", kind, "err", err)
				}
			}
		}
	}
}

func (s *Server) handleReclamationStatus(w http.ResponseWriter, r *http.Request) {
	if !s.requireAll(w, r) || !s.requireControl(w) || s.forwardOrClaim(w, r, control.Scope(), nil) {
		return
	}
	rows, err := s.Control.ReclamationStatus(r.Context())
	if err != nil {
		writeErr(w, 503, "reclamation_status_failed", err.Error())
		return
	}
	writeJSON(w, 200, rows)
}
