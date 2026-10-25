package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"cellhive/internal/bucket"
	"cellhive/internal/cell"
	"cellhive/internal/nodelog"
	"cellhive/internal/objectstore"
)

// doRecoveryPointer survives deletion of a capture scope's owner record during
// graceful cell-agent handoff. It is published before any follower-only ack.
// A newer DO owner epoch supersedes an older pointer, never the reverse.
type doRecoveryPointer struct {
	DOOwnerScope   string `json:"do_owner_scope"`
	DOOwnerEpoch   uint64 `json:"do_owner_epoch"`
	DOOwnerNode    string `json:"do_owner_node"`
	CaptureNode    string `json:"capture_node"`
	CaptureSession string `json:"capture_session"`
}

type doCommitIdentity struct {
	Scope cell.Scope
	Node  string
	Epoch uint64
}

type doCommitIdentityKey struct{}

func doIdentityFromRequest(r *http.Request) (doCommitIdentity, bool, error) {
	scopeText := r.Header.Get("x-cellhive-do-owner-scope")
	if scopeText == "" {
		return doCommitIdentity{}, false, nil
	}
	sc, err := cell.ParseScope(scopeText)
	if err != nil || sc.Class != DOClass {
		return doCommitIdentity{}, true, fmt.Errorf("invalid DO owner scope")
	}
	epoch, err := strconv.ParseUint(r.Header.Get("x-cellhive-do-owner-epoch"), 10, 64)
	if err != nil || epoch == 0 || r.Header.Get("x-cellhive-do-owner-node") == "" {
		return doCommitIdentity{}, true, fmt.Errorf("invalid DO owner identity")
	}
	return doCommitIdentity{Scope: sc, Node: r.Header.Get("x-cellhive-do-owner-node"), Epoch: epoch}, true, nil
}

func (s *Server) validateDOCommit(ctx context.Context, id doCommitIdentity) error {
	if s.Owner == nil {
		return fmt.Errorf("DO owner manager unavailable")
	}
	o, _, err := s.Owner.ResolveCached(ctx, id.Scope, ownerCacheTTL)
	if err != nil {
		return err
	}
	if o.Node != id.Node || o.Epoch != id.Epoch || o.Expired(time.Now()) {
		return fmt.Errorf("DO owner moved or expired")
	}
	return nil
}

func (s *Server) ensureDOCommitPointer(ctx context.Context, sc cell.Scope, id doCommitIdentity) error {
	if err := s.validateDOCommit(ctx, id); err != nil {
		return err
	}
	if s.Cfg.SessionID == "" || s.Cfg.NodeID == "" {
		return fmt.Errorf("cell-agent session identity unavailable")
	}
	return s.ensureDORecoveryPointer(ctx, sc, doRecoveryPointer{
		DOOwnerScope: id.Scope.String(), DOOwnerEpoch: id.Epoch, DOOwnerNode: id.Node,
		CaptureNode: s.Cfg.NodeID, CaptureSession: s.Cfg.SessionID,
	})
}

func doRecoveryPointerKey(sc cell.Scope) string { return sc.Key() + "/do-recovery.json" }

func (s *Server) doRecoveryObjects() *objectstore.Objects {
	return objectstore.NewOwned(s.Bucket, objectstore.PrefixCells, objectstore.OwnerReplica)
}

func (s *Server) getDORecoveryPointer(ctx context.Context, sc cell.Scope) (doRecoveryPointer, string, error) {
	data, etag, err := s.doRecoveryObjects().GetVersion(ctx, doRecoveryPointerKey(sc))
	if err != nil {
		return doRecoveryPointer{}, "", err
	}
	var p doRecoveryPointer
	if err := json.Unmarshal(data, &p); err != nil {
		return p, "", fmt.Errorf("corrupt DO recovery pointer: %w", err)
	}
	if p.CaptureNode == "" || p.CaptureSession == "" || p.DOOwnerEpoch == 0 {
		return p, "", fmt.Errorf("incomplete DO recovery pointer")
	}
	return p, etag, nil
}

func (s *Server) recoverDOPointer(ctx context.Context, sc cell.Scope, p doRecoveryPointer) (int, error) {
	if s.Recovery == nil || s.NodeLog == nil || s.Lease == nil {
		return 0, fmt.Errorf("DO recovery dependencies unavailable")
	}
	rec, _, err := s.NodeLog.Get(ctx, p.CaptureNode, p.CaptureSession)
	if errors.Is(err, nodelog.ErrNotFound) {
		return 0, fmt.Errorf("DO recovery pointer references missing node-log")
	}
	if err != nil {
		return 0, err
	}
	if rec.Status == nodelog.StatusSealed {
		return 0, nil
	}
	l, err := s.Lease.Get(ctx, p.CaptureNode)
	switch {
	case err == nil && l.Live(time.Now()) && l.Session == p.CaptureSession:
		return s.Recovery.RecoverLiveScope(ctx, rec, sc)
	case err == nil && l.Live(time.Now()):
		return s.Recovery.Recover(ctx, s.NodeLog, p.CaptureNode, p.CaptureSession)
	case err == nil || errors.Is(err, bucket.ErrNotFound):
		return s.Recovery.Recover(ctx, s.NodeLog, p.CaptureNode, p.CaptureSession)
	default:
		return 0, fmt.Errorf("capture node lease uncertain: %w", err)
	}
}

func (s *Server) ensureDORecoveryPointer(ctx context.Context, sc cell.Scope, wanted doRecoveryPointer) error {
	if s.Bucket == nil || s.NodeLog == nil {
		return fmt.Errorf("DO recovery pointer unavailable")
	}
	if cached, ok := s.doPointers.Load(sc.String()); ok && cached.(doRecoveryPointer) == wanted {
		return nil
	}
	data, err := json.Marshal(wanted)
	if err != nil {
		return err
	}
	key := doRecoveryPointerKey(sc)
	objects := s.doRecoveryObjects()
	for attempt := 0; attempt < 8; attempt++ {
		current, etag, err := s.getDORecoveryPointer(ctx, sc)
		if errors.Is(err, bucket.ErrNotFound) {
			if _, err := objects.ConditionalCreate(ctx, key, data); err == nil {
				s.doPointers.Store(sc.String(), wanted)
				return nil
			}
			continue
		}
		if err != nil {
			return err
		}
		if current == wanted {
			s.doPointers.Store(sc.String(), wanted)
			return nil
		}
		if current.DOOwnerEpoch > wanted.DOOwnerEpoch {
			return fmt.Errorf("DO recovery pointer has newer owner epoch %d", current.DOOwnerEpoch)
		}
		if current.DOOwnerScope != wanted.DOOwnerScope || current.DOOwnerEpoch == wanted.DOOwnerEpoch && current.DOOwnerNode != wanted.DOOwnerNode {
			return fmt.Errorf("DO recovery pointer owner identity changed without a new generation")
		}
		// A changed cell-agent session can still hold acknowledged tail data.
		// Copy it before replacing the only recovery coordinate for this scope.
		if _, err := s.recoverDOPointer(ctx, sc, current); err != nil {
			return err
		}
		if _, err := objects.CAS(ctx, key, data, etag); err == nil {
			s.doPointers.Store(sc.String(), wanted)
			return nil
		}
	}
	return fmt.Errorf("DO recovery pointer CAS contention")
}

// captureOwnerEpochKey is accepted only together with a verified DO shard claim.
// LTX epoch remains the stable capture-chain identity across agent restarts.
type captureOwnerEpochKey struct{}

func (s *Server) requireCommitOwner(ctx context.Context, sc cell.Scope, epoch uint64) error {
	if captureEpoch, ok := ctx.Value(captureOwnerEpochKey{}).(uint64); ok {
		id, ok := ctx.Value(doCommitIdentityKey{}).(doCommitIdentity)
		if !ok || captureEpoch == 0 {
			return fmt.Errorf("DO capture owner identity required")
		}
		if err := s.validateDOCommit(ctx, id); err != nil {
			return err
		}
		epoch = captureEpoch
	}
	return s.requireOwnerEpoch(ctx, sc, epoch)
}
