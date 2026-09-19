package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/owner"
	"cellhive/internal/timer"
)

// DOClass is the cell class holding Durable Object owner records.
const DOClass = "__do__"

// doOwnerScope is the owner scope for a (worker, class, shard) Durable Object
// host: <ns>/__do__/<worker>~<class>~shard<N>.
func doOwnerScope(ns, worker, class string, shard int) (cell.Scope, error) {
	if ns == "" || worker == "" || class == "" {
		return cell.Scope{}, errors.New("namespace, worker and class are required")
	}
	return owner.DOScope(ns, worker, class, shard), nil
}

type doClaimReq struct {
	Namespace  string `json:"namespace"`
	Worker     string `json:"worker"`
	Class      string `json:"class"`
	Shard      int    `json:"shard"`
	Node       string `json:"node"`
	Advertise  string `json:"advertise,omitempty"`
	TTLSeconds int    `json:"ttl_seconds,omitempty"`
}

func (r doClaimReq) ttl() time.Duration {
	if r.TTLSeconds > 0 {
		return time.Duration(r.TTLSeconds) * time.Second
	}
	return 30 * time.Second
}

// handleDOClaim assigns Durable Object ownership to a do-runtime task
// (cell-agent driven, ADR-078). Returns the epoch; a live lease held by another
// node returns 409 owner_live.
func (s *Server) handleDOClaim(w http.ResponseWriter, r *http.Request) {
	if s.Owner == nil {
		writeErr(w, http.StatusServiceUnavailable, "no_owner", "owner manager not configured")
		return
	}
	var req doClaimReq
	if !decode(w, r, &req) {
		return
	}
	if req.Node == "" {
		writeErr(w, http.StatusBadRequest, "no_node", "node is required")
		return
	}
	scope, err := doOwnerScope(req.Namespace, req.Worker, req.Class, req.Shard)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "bad_scope", err.Error())
		return
	}
	o, err := s.Owner.ClaimAs(r.Context(), scope, req.Node, req.Advertise, cell.RoleCellAgent, req.ttl(), time.Now())
	if errors.Is(err, owner.ErrOwnerLive) {
		writeJSON(w, http.StatusConflict, map[string]any{
			"error": "owner_live", "node": o.Node, "address": o.Address,
			"epoch": o.Epoch, "expiry_ms": o.Expiry,
		})
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "claim_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"epoch": o.Epoch, "expiry_ms": o.Expiry, "node": o.Node})
}

type doRenewReq struct {
	doClaimReq
	Epoch uint64 `json:"epoch"`
}

func (s *Server) handleDORenew(w http.ResponseWriter, r *http.Request) {
	if s.Owner == nil {
		writeErr(w, http.StatusServiceUnavailable, "no_owner", "owner manager not configured")
		return
	}
	var req doRenewReq
	if !decode(w, r, &req) {
		return
	}
	scope, err := doOwnerScope(req.Namespace, req.Worker, req.Class, req.Shard)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "bad_scope", err.Error())
		return
	}
	o, err := s.Owner.RenewAs(r.Context(), scope, req.Node, req.Epoch, req.ttl(), time.Now())
	if errors.Is(err, owner.ErrEpochMismatch) {
		writeErr(w, http.StatusConflict, "owner_epoch", "owner epoch no longer matches")
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "renew_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"epoch": o.Epoch, "expiry_ms": o.Expiry, "node": o.Node})
}

func (s *Server) handleDORelease(w http.ResponseWriter, r *http.Request) {
	if s.Owner == nil {
		writeErr(w, http.StatusServiceUnavailable, "no_owner", "owner manager not configured")
		return
	}
	var req doRenewReq
	if !decode(w, r, &req) {
		return
	}
	scope, err := doOwnerScope(req.Namespace, req.Worker, req.Class, req.Shard)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "bad_scope", err.Error())
		return
	}
	if err := s.Owner.ReleaseAs(r.Context(), scope, req.Node, req.Epoch); err != nil {
		if errors.Is(err, owner.ErrEpochMismatch) {
			writeErr(w, http.StatusConflict, "owner_epoch", "owner epoch no longer matches")
			return
		}
		writeErr(w, http.StatusInternalServerError, "release_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"released": true})
}

type doAlarmReq struct {
	Namespace    string `json:"namespace"`
	Worker       string `json:"worker"`
	Class        string `json:"class"`
	Shard        int    `json:"shard"`
	ID           string `json:"id"`
	StorageClass string `json:"storage_class"`
	StorageID    string `json:"storage_id"`
	DueMs        int64  `json:"due_ms"`
}

// doAlarmOccurrence is the timer occurrence carrying the full object identity.
// The dispatcher must replay every identity input the host uses to pick the
// Durable Object's host actor (storage_class/storage_id included), or the alarm
// runs against a different facet than the object's fetch calls (ADR-174).
func doAlarmOccurrence(r doAlarmReq) (string, error) {
	sc := r.StorageClass
	if sc == "" {
		sc = r.Class
	}
	b, err := json.Marshal(map[string]any{
		"w": r.Worker, "c": r.Class, "s": r.Shard, "sc": sc, "sid": r.StorageID, "id": r.ID,
	})
	return string(b), err
}

// handleDOAlarmUpsert records (or clears, when due_ms<=0) a Durable Object's
// alarm as a unified KindDOAlarm timer (ADR-079). The dispatcher resolves the
// owning do-runtime from the owner record and delivers alarm().
func (s *Server) handleDOAlarmUpsert(w http.ResponseWriter, r *http.Request) {
	if s.OpenTimer == nil {
		writeErr(w, http.StatusServiceUnavailable, "no_timers", "timer store not configured")
		return
	}
	raw, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "read_failed", err.Error())
		return
	}
	var req doAlarmReq
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &req); err != nil {
			writeErr(w, http.StatusBadRequest, "bad_json", err.Error())
			return
		}
	}
	if req.Namespace == "" || req.Worker == "" || req.Class == "" || req.ID == "" {
		writeErr(w, http.StatusBadRequest, "bad_request", "namespace, worker, class and id are required")
		return
	}
	scope := cell.Scope{Namespace: req.Namespace, Class: "__timer__", ID: "do"}
	if s.forwardOrClaim(w, r, scope, raw) {
		return
	}
	st, err := s.OpenTimer(r.Context(), scope)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "open_failed", err.Error())
		return
	}
	occ, oerr := doAlarmOccurrence(req)
	if oerr != nil {
		writeErr(w, http.StatusBadRequest, "bad_occurrence", oerr.Error())
		return
	}
	cleared := req.DueMs <= 0
	err = s.capturedWrite(r.Context(), scope, func() error {
		if err := st.RemoveByOccurrence(r.Context(), occ); err != nil {
			return err
		}
		if cleared {
			return nil
		}
		return st.Upsert(r.Context(), timer.New(req.DueMs, timer.KindDOAlarm, scope.String(), occ))
	})
	if err != nil {
		s.captureErr(w, err, "alarm_upsert_failed")
		return
	}
	if !cleared && s.Timers != nil {
		s.Timers.Add(scope.String())
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "cleared": cleared})
}

// doObjectIndexKey is the bucket key for one indexed DO object (ADR-108).
func doObjectIndexKey(ns, worker, class string, shard int, name string) string {
	return fmt.Sprintf("%sdo-objects/%s/%s/%s/%d/%s.json",
		supervisorBlobPrefix, url.PathEscape(ns), url.PathEscape(worker),
		url.PathEscape(class), shard, url.PathEscape(name))
}

type doObjectEntry struct {
	Namespace string `json:"namespace"`
	Worker    string `json:"worker"`
	Class     string `json:"class"`
	Shard     int    `json:"shard"`
	Name      string `json:"name"`
	StorageID string `json:"storage_id,omitempty"`
	HostHash  string `json:"host_hash,omitempty"`
}

// handleDOObjectIndex upserts (POST) or removes (DELETE) one do-runtime object
// registry entry, so the object list survives a runtime restart (ADR-108).
func (s *Server) handleDOObjectIndex(w http.ResponseWriter, r *http.Request) {
	if !s.Cfg.DOObjectIndex {
		writeErr(w, http.StatusNotFound, "no_object_index", "DO object index is disabled")
		return
	}
	st := s.supervisorBlobs()
	if st == nil {
		writeErr(w, http.StatusServiceUnavailable, "no_bucket", "object store not configured")
		return
	}
	var e doObjectEntry
	if r.Method == http.MethodDelete {
		q := r.URL.Query()
		e = doObjectEntry{Namespace: q.Get("ns"), Worker: q.Get("worker"), Class: q.Get("class"), Name: q.Get("name")}
		fmt.Sscanf(q.Get("shard"), "%d", &e.Shard)
	} else if !decode(w, r, &e) {
		return
	}
	if e.Namespace == "" || e.Worker == "" || e.Class == "" || e.Name == "" {
		writeErr(w, http.StatusBadRequest, "bad_request", "namespace, worker, class and name are required")
		return
	}
	key := doObjectIndexKey(e.Namespace, e.Worker, e.Class, e.Shard, e.Name)
	if r.Method == http.MethodDelete {
		_ = st.Delete(r.Context(), key)
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
		return
	}
	data, _ := json.Marshal(e)
	if _, err := st.Put(r.Context(), key, data); err != nil {
		writeErr(w, http.StatusInternalServerError, "put_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// indexedDOObjects lists the durable object index (ADR-108).
func (s *Server) indexedDOObjects(ctx context.Context) []doObjectEntry {
	if !s.Cfg.DOObjectIndex {
		return nil
	}
	st := s.supervisorBlobs()
	if st == nil {
		return nil
	}
	keys, err := st.ListPrefix(ctx, supervisorBlobPrefix+"do-objects/")
	if err != nil {
		return nil
	}
	out := make([]doObjectEntry, 0, len(keys))
	for _, k := range keys {
		data, gerr := st.Get(ctx, k)
		if gerr != nil {
			continue
		}
		var e doObjectEntry
		if json.Unmarshal(data, &e) == nil && e.Name != "" {
			out = append(out, e)
		}
	}
	return out
}

// handleDOObjects aggregates the do-runtime object registries (ADR-082) and, when
// enabled, the durable bucket index (ADR-108). It is a cold path.
func (s *Server) handleDOObjects(w http.ResponseWriter, r *http.Request) {
	all := []map[string]any{}
	seen := map[string]bool{}
	for _, e := range s.indexedDOObjects(r.Context()) {
		k := fmt.Sprintf("%s/%s/%s/%d/%s", e.Namespace, e.Worker, e.Class, e.Shard, e.Name)
		if seen[k] {
			continue
		}
		seen[k] = true
		all = append(all, map[string]any{"namespace": e.Namespace, "worker": e.Worker,
			"class": e.Class, "shard": e.Shard, "name": e.Name, "source": "index"})
	}
	client := s.ForwardClient
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	for _, rt := range s.Cfg.DoRuntimes {
		req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, strings.TrimRight(rt, "/")+"/v1/do/objects", nil)
		if err != nil {
			continue
		}
		req.Header.Set("x-cellhive-internal-token", s.Cfg.TokenInternal)
		resp, err := client.Do(req)
		if err != nil {
			continue
		}
		var out struct {
			Objects []map[string]any `json:"objects"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&out); err == nil {
			for _, o := range out.Objects {
				k := fmt.Sprintf("%v/%v/%v/%v/%v", o["namespace"], o["worker"], o["class"], o["shard"], o["name"])
				if seen[k] {
					continue
				}
				seen[k] = true
				all = append(all, o)
			}
		}
		resp.Body.Close()
	}
	writeJSON(w, http.StatusOK, map[string]any{"objects": all})
}
