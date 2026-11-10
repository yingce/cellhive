package dosupervisor

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"

	"cellhive/internal/sqlcapture"
	"cellhive/internal/wal"
)

var ErrCheckpointOwnerLost = errors.New("paging: checkpoint owner lost")

// PagingHandler adds durable checkpoint preparation to the capability-checked
// paging service. Older native clients ignore its optional response header.
func (s *Supervisor) PagingHandler() http.Handler {
	s.setupPagingLoader()
	handler := s.Paging.handler(s.prepareCheckpoint)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		recorder := &pagingErrorResponse{ResponseWriter: w}
		handler.ServeHTTP(recorder, r)
		if recorder.status >= 400 {
			s.logf("paging request failed operation=%s status=%d reason=%s", r.URL.Path, recorder.status, recorder.body.String())
		}
	})
}

type pagingErrorResponse struct {
	http.ResponseWriter
	status int
	body   bytes.Buffer
}

func (w *pagingErrorResponse) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}
func (w *pagingErrorResponse) Write(p []byte) (int, error) {
	if w.status >= 400 && w.body.Len() < 512 {
		w.body.Write(p[:min(len(p), 512-w.body.Len())])
	}
	return w.ResponseWriter.Write(p)
}

func (s *Supervisor) prepareCheckpoint(ctx context.Context, path string) (uint64, int64, error) {
	fs, err := s.captureFileForCheckpoint(path, true)
	if err != nil {
		return 0, 0, err
	}
	defer fs.mu.Unlock()
	v, err := s.Paging.lookup(path)
	if err != nil {
		return 0, 0, err
	}
	if v == nil {
		return 0, 0, errors.New("paging: unknown checkpoint database")
	}
	ctx, err = s.checkpointContext(ctx, v.claim)
	if err != nil {
		return 0, 0, err
	}
	if err := s.syncFileLocked(ctx, fs); err != nil {
		return 0, 0, err
	}
	position := fs.cursor.Position()
	v.mu.Lock()
	defer v.mu.Unlock()
	id, err := v.beginCheckpoint(position, v.captured)
	return id, int64(position.Commit) * int64(position.PageSize), err
}

// The lease is checked on every preparation; the activation identity cannot be
// replaced by another epoch simply because it has the same database path.
func (s *Supervisor) checkpointContext(ctx context.Context, claim RecoveryClaim) (context.Context, error) {
	if claim.Epoch == 0 {
		return ctx, nil
	}
	if s.OwnerLeases == nil {
		return nil, errors.New("paging: checkpoint owner lease is unmanaged")
	}
	if _, err := s.OwnerLeases.Status(claim); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCheckpointOwnerLost, err)
	}
	return sqlcapture.WithDOOwner(ctx, sqlcapture.DOOwnerIdentity{Scope: claim.Scope, Node: claim.Node, Epoch: claim.Epoch}), nil
}

// checkpointProof exists only within one paging activation. It is neither a
// remote marker nor an alternative snapshot format. The caller must durably
// commit Position before installing it, and serialize access with pagedFile.mu.
type checkpointProof struct {
	id       uint64
	position wal.Position
	copying  bool
}

func (v *pagedFile) beginCheckpoint(position wal.Position, generation uint64) (uint64, error) {
	if v.failed != nil {
		return 0, v.failed
	}
	if !position.Valid || position.Commit == 0 || position.PageSize != v.pf.PageSize() {
		return 0, errors.New("paging: invalid durable checkpoint position")
	}
	if v.activeWrites != 0 || v.checkpoint.copying || v.writes != generation || v.captured != generation {
		return 0, errors.New("paging: checkpoint preparation raced an unproven mutation")
	}
	if v.checkpointSerial == math.MaxUint64 {
		return 0, errors.New("paging: checkpoint certificate exhausted")
	}
	v.checkpointSerial++
	v.checkpoint = checkpointProof{id: v.checkpointSerial, position: position, copying: true}
	return v.checkpoint.id, nil
}

func (v *pagedFile) endCheckpoint(id uint64) error {
	if v.failed != nil {
		return v.failed
	}
	if id == 0 || id != v.checkpoint.id || !v.checkpoint.copying || v.activeWrites != 0 {
		return errors.New("paging: unmatched checkpoint end")
	}
	v.checkpoint.copying = false
	return nil
}

func (v *pagedFile) checkpointMutation(id uint64, truncate bool, size int64) (bool, error) {
	if id == 0 {
		v.checkpoint = checkpointProof{}
		return false, nil
	}
	proof := v.checkpoint
	if v.failed != nil || proof.id != id || v.activeWrites != 0 || v.writes != v.captured {
		return false, errors.New("paging: invalid checkpoint mutation certificate")
	}
	if truncate {
		if size != int64(proof.position.Commit)*int64(proof.position.PageSize) {
			return false, errors.New("paging: checkpoint truncate differs from durable database size")
		}
	} else if !proof.copying {
		return false, errors.New("paging: checkpoint write outside copy phase")
	}
	return true, nil
}

func (p *Paging) consumeCheckpoint(path string, old wal.Position) (bool, error) {
	v, err := p.lookup(path)
	if err != nil || v == nil {
		return false, err
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.consumeCheckpoint(old), v.failed
}

// consumeCheckpoint authorizes only the exact committed prefix made durable
// before checkpoint copying. A later capture, ordinary write, incomplete copy,
// failed callback or replay of the same certificate cannot authorize a reset.
func (v *pagedFile) consumeCheckpoint(old wal.Position) bool {
	proof := v.checkpoint
	v.checkpoint = checkpointProof{}
	return v.failed == nil && proof.id != 0 && !proof.copying &&
		v.activeWrites == 0 && v.writes == v.captured &&
		old.Valid && old == proof.position
}
