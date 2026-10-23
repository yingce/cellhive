package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/control"
	queuepkg "cellhive/internal/queue"
)

func (s *Server) configureQueueStore(store *queuepkg.Store) {
	if store == nil || s.resourceReplica() == nil {
		return
	}
	store.ResourceGeneration = func(ctx context.Context, ns, name string) (string, error) {
		return s.resourceReplica().ResourceGeneration(ctx, queuepkg.Scope(ns, name))
	}
}
func (s *Server) validateQueueTransfer(ctx context.Context, ns string, m queuepkg.Message) error {
	if m.SourceQueue == "" {
		if m.SourceGeneration != "" {
			return fmt.Errorf("invalid queue source generation")
		}
		return nil
	}
	if s.resourceReplica() == nil {
		return fmt.Errorf("queue source coordination unavailable")
	}
	current, err := s.resourceReplica().ResourceGeneration(ctx, queuepkg.Scope(ns, m.SourceQueue))
	if err != nil {
		return err
	}
	if current != m.SourceGeneration {
		return cell.ErrResourceDeleted
	}
	return nil
}

type queueDestination struct {
	Namespace   string `json:"namespace"`
	Source      string `json:"source_queue"`
	Generation  string `json:"source_generation"`
	Destination string `json:"destination_queue"`
}

func (s *Server) recordQueueDestination(ctx context.Context, ns, name string, m queuepkg.Message) error {
	if m.SourceQueue == "" {
		return nil
	}
	intent := queueDestination{ns, m.SourceQueue, m.SourceGeneration, name}
	if s.Control == nil {
		return fmt.Errorf("queue transfer registry unavailable")
	}
	if s.Owner != nil {
		o, _, err := s.Owner.Resolve(ctx, control.Scope())
		if err != nil || !s.ownerLive(ctx, o, time.Now()) {
			s.invalidateStaleLocal(ctx, control.Scope())
			o, err = s.Owner.Claim(ctx, control.Scope(), time.Now())
			if err != nil {
				return err
			}
		}
		if o.Node != s.Cfg.NodeID {
			return s.cleanupRPC(ctx, o.Address, "/v1/internal/queue/transfer-destination", intent, nil)
		}
	}
	return s.capturedWrite(ctx, control.Scope(), func() error {
		return s.Control.RecordQueueDestination(ctx, ns, m.SourceQueue, m.SourceGeneration, name)
	})
}
func (s *Server) handleQueueTransferDestination(w http.ResponseWriter, r *http.Request) {
	raw, err := io.ReadAll(io.LimitReader(r.Body, 16*1024))
	var intent queueDestination
	if err != nil || json.Unmarshal(raw, &intent) != nil {
		writeErr(w, 400, "invalid_transfer", "invalid transfer intent")
		return
	}
	if s.forwardOrClaim(w, r, control.Scope(), raw) {
		return
	}
	if s.Control == nil {
		writeErr(w, 503, "no_control", "queue transfer registry unavailable")
		return
	}
	if err := s.capturedWrite(r.Context(), control.Scope(), func() error {
		return s.Control.RecordQueueDestination(r.Context(), intent.Namespace, intent.Source, intent.Generation, intent.Destination)
	}); err != nil {
		s.captureErr(w, err, "queue_transfer_failed")
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true})
}
func (s *Server) acceptLocalQueueTransfer(ctx context.Context, ns, name string, m queuepkg.Message) error {
	if err := s.recordQueueDestination(ctx, ns, name, m); err != nil {
		return err
	}
	if s.Store == nil {
		return fmt.Errorf("queue data store unavailable")
	}
	end, err := s.BeginResourceRequest(ctx, queuepkg.Scope(ns, name))
	if err != nil {
		return err
	}
	defer end()
	return s.capturedWrite(ctx, queuepkg.Scope(ns, name), func() error {
		_, err := s.Queue.TransferValidated(ctx, ns, name, m, func(ctx context.Context) error { return s.validateQueueTransfer(ctx, ns, m) })
		return err
	})
}
