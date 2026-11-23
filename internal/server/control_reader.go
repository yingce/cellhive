package server

import (
	"cellhive/internal/bucket"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/control"
	"cellhive/internal/owner"
	"cellhive/internal/queue"
)

// ControlReader supplies the configuration consumed by background dispatchers.
// It never claims ownership, refreshes local SQLite files, or serves a stale
// local fallback. Successful view caching belongs to the scheduling caller.
type ControlReader struct {
	s          *Server
	mu         sync.Mutex
	calls      map[controlReadQuery]*controlReadCall
	authority  string
	generation uint64
}

type controlReadQuery struct {
	Op        string `json:"op"`
	Namespace string `json:"namespace,omitempty"`
	Kind      string `json:"kind,omitempty"`
	Name      string `json:"name,omitempty"`
}
type controlReadReply struct {
	Owner    cell.Owner      `json:"owner"`
	Data     json.RawMessage `json:"data"`
	NotFound bool            `json:"not_found,omitempty"`
}
type controlReadCall struct {
	done     chan struct{}
	loading  bool
	canceled bool
	value    controlReadValue
	err      error
	delay    time.Duration
	retryAt  time.Time
}

type controlReadValue struct {
	data      json.RawMessage
	authority string
}

func (s *Server) BackgroundControl() *ControlReader {
	s.controlReaderOnce.Do(func() { s.controlReader = &ControlReader{s: s} })
	return s.controlReader
}

func (r *ControlReader) Projection(ctx context.Context) (control.Projection, error) {
	out, _, err := r.ProjectionWithAuthority(ctx)
	return out, err
}

// ProjectionWithAuthority binds the view to the exact successful read, not a
// later observation of another query's shared authority state.
func (r *ControlReader) ProjectionWithAuthority(ctx context.Context) (control.Projection, string, error) {
	var out control.Projection
	key, err := r.read(ctx, controlReadQuery{Op: "projection"}, &out, false)
	return out, key, err
}

// RevisionWithAuthority requests only the ETag. Older owners ignore the field
// selector and return a full projection carrying the same ETag.
func (r *ControlReader) RevisionWithAuthority(ctx context.Context) (string, string, error) {
	var result struct {
		ETag string `json:"etag"`
	}
	key, err := r.read(ctx, controlReadQuery{Op: "projection", Kind: "revision"}, &result, false)
	if err == nil && result.ETag == "" {
		err = errors.New("control revision missing")
	}
	return result.ETag, key, err
}

func (r *ControlReader) ProjectionStrict(ctx context.Context) (control.Projection, error) {
	var out control.Projection
	_, err := r.read(ctx, controlReadQuery{Op: "projection"}, &out, true)
	return out, err
}
func (r *ControlReader) ResourcesByKind(ctx context.Context, kind string) ([]control.ResourceRef, error) {
	out, _, err := r.ResourcesByKindWithAuthority(ctx, kind)
	return out, err
}

// ResourcesByKindWithAuthority binds discovery data to its own validated read.
func (r *ControlReader) ResourcesByKindWithAuthority(ctx context.Context, kind string) ([]control.ResourceRef, string, error) {
	var out []control.ResourceRef
	key, err := r.read(ctx, controlReadQuery{Op: "resources", Kind: kind}, &out, false)
	return out, key, err
}
func (r *ControlReader) ResourceConfig(ctx context.Context, ns, kind, name string) ([]byte, error) {
	var out []byte
	_, err := r.read(ctx, controlReadQuery{Op: "config", Namespace: ns, Kind: kind, Name: name}, &out, true)
	return out, err
}

func (r *ControlReader) QueueConsumer(ctx context.Context, ns, id string) (queue.Ref, bool, error) {
	// An older owner ignores the selector and returns the full projection.
	// ETag distinguishes that legacy wire reply from a resolved queue.Ref.
	var reply struct {
		queue.Ref
		ETag string `json:"etag"`
	}
	key, err := r.read(ctx, controlReadQuery{Op: "projection", Kind: "queue-consumer", Namespace: ns, Name: id}, &reply, true)
	if errors.Is(err, control.ErrNotFound) {
		return queue.Ref{}, false, nil
	}
	if err != nil {
		return queue.Ref{}, false, err
	}
	if reply.ETag == "" {
		if reply.Ref.Namespace != ns || reply.Ref.Name != id {
			return queue.Ref{}, false, errors.New("control returned a different queue identity")
		}
		return reply.Ref, true, nil
	}
	var resources []control.ResourceRef
	resourceKey, err := r.read(ctx, controlReadQuery{Op: "resources", Kind: "queue"}, &resources, true)
	if err != nil {
		return queue.Ref{}, false, err
	}
	// The legacy reads span multiple SQLite snapshots. A control revision
	// change or ownership transfer invalidates their combined result.
	var current control.Projection
	revisionKey, err := r.read(ctx, controlReadQuery{Op: "projection"}, &current, true)
	if err != nil {
		return queue.Ref{}, false, err
	}
	if current.ETag != reply.ETag || key != resourceKey || key != revisionKey {
		return queue.Ref{}, false, errors.New("queue configuration changed during legacy lookup")
	}
	for _, target := range current.QueueTargets() {
		if target.Namespace != ns {
			continue
		}
		sc, err := control.QueueResourceScope(resources, ns, target.Queue)
		if err != nil || sc.ID != id {
			continue
		}
		dest := ""
		if target.Consumer.DeadLetterQueue != "" {
			dlq, err := control.QueueResourceScope(resources, ns, target.Consumer.DeadLetterQueue)
			if err != nil {
				return queue.Ref{}, false, err
			}
			dest = dlq.ID
		}
		return target.ResolvedRef(sc, dest), true, nil
	}
	return queue.Ref{}, false, nil
}

func (r *ControlReader) queueExists(ctx context.Context, ns, name string) (bool, error) {
	var exists bool
	_, err := r.read(ctx, controlReadQuery{Op: "binding", Namespace: ns, Kind: "queue", Name: name}, &exists, true)
	return exists, err
}

// AuthorityKey only reports identity learned by a completed read; consulting it
// never adds a bucket request to a scheduling-cache hit.
func (r *ControlReader) AuthorityKey() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return fmt.Sprintf("%d:%s", r.generation, r.authority)
}

func (r *ControlReader) read(ctx context.Context, q controlReadQuery, out any, strict bool) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	var value controlReadValue
	var err error
	if strict {
		value, err = r.load(ctx, q)
	} else {
		value, err = r.shared(ctx, q)
	}
	if err != nil {
		return "", err
	}
	return value.authority, json.Unmarshal(value.data, out)
}

func (r *ControlReader) shared(ctx context.Context, q controlReadQuery) (controlReadValue, error) {
	for {
		r.mu.Lock()
		prior := r.calls[q]
		if prior != nil && prior.loading {
			r.mu.Unlock()
			select {
			case <-ctx.Done():
				return controlReadValue{}, ctx.Err()
			case <-prior.done:
				if prior.canceled {
					continue
				}
				return prior.value, prior.err
			}
		}
		if prior != nil && time.Now().Before(prior.retryAt) {
			r.mu.Unlock()
			return controlReadValue{}, prior.err
		}
		if r.calls == nil {
			r.calls = make(map[controlReadQuery]*controlReadCall)
		}
		if len(r.calls) >= 256 {
			for key, old := range r.calls {
				if !old.loading && time.Now().After(old.retryAt) {
					delete(r.calls, key)
				}
			}
			if len(r.calls) >= 256 {
				r.mu.Unlock()
				return r.load(ctx, q)
			}
		}
		call := &controlReadCall{done: make(chan struct{}), loading: true, delay: 250 * time.Millisecond}
		if prior != nil {
			call.delay = min(prior.delay*2, 5*time.Second)
		}
		r.calls[q] = call
		r.mu.Unlock()
		value, err := r.load(ctx, q)
		r.mu.Lock()
		call.loading, call.value, call.err = false, value, err
		call.canceled = ctx.Err() != nil
		if err == nil || call.canceled || errors.Is(err, control.ErrNotFound) {
			delete(r.calls, q)
		} else {
			call.retryAt = time.Now().Add(call.delay)
		}
		close(call.done)
		r.mu.Unlock()
		return value, err
	}
}

func (r *ControlReader) load(ctx context.Context, q controlReadQuery) (value controlReadValue, err error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var result controlReadReply
	defer func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		before := r.authority
		if err == nil || errors.Is(err, control.ErrNotFound) {
			r.authority = fmt.Sprintf("%s/%s/%d", result.Owner.Node, result.Owner.Session, result.Owner.Epoch)
		} else {
			r.authority = ""
		}
		if before != r.authority || (err != nil && !errors.Is(err, control.ErrNotFound)) {
			r.generation++
		}
		value.authority = fmt.Sprintf("%d:%s", r.generation, r.authority)
	}()
	if r.s.Owner == nil {
		// Explicit single-process embedding, not an authority lookup fallback.
		result, err = r.s.readLocalControl(ctx, q)
	} else {
		before, e := r.s.cleanupControlOwner(ctx, "")
		if e != nil {
			return value, e
		}
		if before.Node == r.s.Cfg.NodeID {
			// Reuse this call's authoritative initial sample. The helper still
			// fences the local read and rechecks authority after reading SQLite.
			result, err = r.s.readLocalControlOwned(ctx, q, &before)
		} else {
			if before.Address == "" {
				return value, owner.ErrNotOwner
			}
			err = r.s.cleanupRPC(ctx, before.Address, "/v1/internal/control/read", q, &result)
			if err != nil {
				return value, err
			}
			after, e := r.s.cleanupControlOwner(ctx, "")
			if e != nil {
				return value, e
			}
			if !sameCleanupControlOwner(before, after) || !sameCleanupControlOwner(before, result.Owner) {
				return value, owner.ErrNotOwner
			}
		}
	}
	if err != nil {
		return value, err
	}
	if result.NotFound {
		return value, control.ErrNotFound
	}
	value.data = result.Data
	return value, nil
}

func (s *Server) readLocalControl(ctx context.Context, q controlReadQuery) (controlReadReply, error) {
	return s.readLocalControlOwned(ctx, q, nil)
}

// initial is only supplied by load after its fresh authority read, never from
// a cache or an RPC argument. The internal endpoint obtains its own sample.
func (s *Server) readLocalControlOwned(ctx context.Context, q controlReadQuery, initial *cell.Owner) (controlReadReply, error) {
	var result controlReadReply
	if s.Control == nil {
		return result, errors.New("control plane unavailable")
	}
	var check func() bool
	if s.Owner != nil {
		var before cell.Owner
		if initial != nil {
			before = *initial
		} else {
			var err error
			before, err = s.cleanupControlOwner(ctx, "")
			if err != nil {
				return result, err
			}
		}
		if before.Node != s.Cfg.NodeID || before.Session != s.Owner.Session || !s.ackAllowed(time.Now()) {
			return result, owner.ErrNotOwner
		}
		var ok bool
		check, ok = s.Owner.ReadFence(control.Scope())
		if !ok {
			return result, owner.ErrNotOwner
		}
		defer check()
		result.Owner = before
	}
	var value any
	var err error
	switch {
	case q.Op == "projection" && q.Kind == "revision":
		var rev int64
		rev, err = s.Control.Rev(ctx)
		value = struct {
			ETag string `json:"etag"`
		}{fmt.Sprintf(`"r%d"`, rev)}
	case q.Op == "projection" && q.Kind == "queue-consumer" && q.Namespace != "" && q.Name != "":
		var ok bool
		value, ok, err = s.Control.ResolveQueueConsumer(ctx, q.Namespace, q.Name)
		if err == nil && !ok {
			err = control.ErrNotFound
		}
	case q.Op == "projection":
		value, err = s.Control.Projection(ctx)
	case q.Op == "resources" && q.Kind == "queue":
		value, err = s.Control.ResourcesByKind(ctx, q.Kind)
	case q.Op == "config" && q.Kind == "workflow" && q.Namespace != "":
		value, err = s.Control.ResourceConfig(ctx, q.Namespace, q.Kind, q.Name)
	case q.Op == "binding" && q.Kind == "queue" && q.Namespace != "" && q.Name != "":
		var exists bool
		_, exists, err = s.Control.Binding(ctx, q.Namespace, q.Kind, q.Name)
		value = exists
	default:
		return result, errors.New("unsupported background control read")
	}
	if errors.Is(err, control.ErrNotFound) {
		result.NotFound = true
	} else if err != nil {
		return result, err
	}
	if check != nil {
		after, e := s.cleanupControlOwner(ctx, "")
		if e != nil {
			return result, e
		}
		if !sameCleanupControlOwner(result.Owner, after) || !s.ackAllowed(time.Now()) || !check() {
			return result, owner.ErrNotOwner
		}
	}
	result.Data, err = json.Marshal(value)
	return result, err
}

func (s *Server) handleControlRead(w http.ResponseWriter, r *http.Request) {
	var q controlReadQuery
	data, err := io.ReadAll(io.LimitReader(r.Body, 4097))
	if err != nil || len(data) > 4096 || json.Unmarshal(data, &q) != nil {
		writeErr(w, 400, "bad_request", "invalid background control read")
		return
	}
	// Never forward here: stale clients must not recurse through wrong owners.
	result, err := s.readLocalControl(bucket.WithReadSource(r.Context(), bucket.ReadSource(r.Header.Get("X-Cellhive-Read-Source"))), q)
	if err != nil {
		writeErr(w, 503, "control_read_unavailable", err.Error())
		return
	}
	writeJSON(w, 200, result)
}
