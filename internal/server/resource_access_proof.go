package server

import (
	"context"
	"net/http"
	"strconv"

	"cellhive/internal/cell"
	"cellhive/internal/owner"
)

type resourceProofKey struct{}
type resourceAccessProof struct {
	scope cell.Scope
	proof *owner.ResourceProof
}

func resourceProof(ctx context.Context, sc cell.Scope) *owner.ResourceProof {
	p, _ := ctx.Value(resourceProofKey{}).(*resourceAccessProof)
	if p == nil || p.scope != sc {
		return nil
	}
	return p.proof
}
func resourceProofGeneration(p *owner.ResourceProof) (string, error) {
	if !p.Validate() {
		return "", owner.ErrNotOwner
	}
	if p.Owner.RetirementJob == "" {
		return "", nil
	}
	return p.Owner.RetirementJob + "/" + strconv.FormatUint(p.Owner.RetiredEpoch, 10), nil
}
func (s *Server) preparedResourceGeneration(ctx context.Context, sc cell.Scope) (string, error) {
	if p := resourceProof(ctx, sc); p != nil {
		return resourceProofGeneration(p)
	}
	return s.resourceReplica().ResourceGeneration(ctx, sc)
}

// The resource admission gate stays held until the handler returns. Retirement
// must drain that gate before publication; local loss of owner/lease authority
// is checked immediately before sending response headers. No payload buffering.
type resourceProofWriter struct {
	http.ResponseWriter
	proof             *owner.ResourceProof
	started, rejected bool
}

func (w *resourceProofWriter) WriteHeader(code int) {
	if w.started {
		return
	}
	if !w.proof.Validate() {
		w.started = true
		w.rejected = true
		// Drop application content headers before writing the rejection body.
		for k := range w.Header() {
			w.Header().Del(k)
		}
		writeErr(w.ResponseWriter, http.StatusServiceUnavailable, "owner_proof_expired", "local resource authority changed")
		return
	}
	if code >= 200 {
		w.started = true
	}
	w.ResponseWriter.WriteHeader(code)
}
func (w *resourceProofWriter) Write(b []byte) (int, error) {
	if !w.started {
		w.WriteHeader(http.StatusOK)
	}
	if w.rejected {
		return 0, owner.ErrNotOwner
	}
	return w.ResponseWriter.Write(b)
}
