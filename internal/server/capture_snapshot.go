package server

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"syscall"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/ownerclient"
)

var snapshotStreamSlots = make(chan struct{}, 2)

// Snapshot spools bypass the small segment/follower framing path. A successful
// response proves the complete native object durable in the bucket.
func (s *Server) handleSnapshotStream(w http.ResponseWriter, r *http.Request, sc cell.Scope, epoch uint64) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Minute)
	defer cancel()
	r = r.WithContext(ctx)
	if r.URL.Query().Get("pipelined") == "1" || r.ContentLength < 100 || r.ContentLength > 5<<40 {
		writeErr(w, 400, "bad_snapshot", "snapshot requires bounded content length and no pipelining")
		return
	}
	if s.Owner != nil && r.Header.Get(ownerclient.ForwardedHeader) == "" {
		o, _, err := s.Owner.ResolveForwardRoute(ctx, sc)
		if err == nil && o.Node != s.Cfg.NodeID && o.Address != "" && s.routeLive(ctx, sc, o, time.Now()) {
			target := ownerclient.OwnerURL(ownerclient.Hint{Address: o.Address}) + r.URL.RequestURI()
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, r.Body)
			if err != nil {
				writeErr(w, 502, "snapshot_forward", err.Error())
				return
			}
			req.Header = r.Header.Clone()
			req.Header.Set(ownerclient.ForwardedHeader, "1")
			req.ContentLength = r.ContentLength
			client := *s.forwardHTTPClient()
			client.Timeout = 0
			resp, err := client.Do(req)
			if err != nil {
				writeErr(w, 502, "snapshot_forward", err.Error())
				return
			}
			defer resp.Body.Close()
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(resp.StatusCode)
			_, _ = io.Copy(w, io.LimitReader(resp.Body, 1<<20))
			return
		}
	}
	if err := s.requireCommitOwner(ctx, sc, epoch); err != nil {
		mapOwnerErr(w, err)
		return
	}
	if s.Replica == nil {
		writeErr(w, 503, "no_replica", "replica unavailable")
		return
	}
	select {
	case snapshotStreamSlots <- struct{}{}:
		defer func() { <-snapshotStreamSlots }()
	case <-ctx.Done():
		return
	}
	var disk syscall.Statfs_t
	if err := syscall.Statfs(os.TempDir(), &disk); err != nil {
		writeErr(w, 500, "snapshot_disk", err.Error())
		return
	}
	if uint64(r.ContentLength)+(512<<20) > disk.Bavail*uint64(disk.Bsize) {
		writeErr(w, 507, "snapshot_disk", "insufficient spool space")
		return
	}
	f, err := os.CreateTemp("", "cellhive-received-snapshot-*.ltx")
	if err != nil {
		writeErr(w, 500, "snapshot_spool", err.Error())
		return
	}
	defer os.Remove(f.Name())
	defer f.Close()
	n, err := io.Copy(f, http.MaxBytesReader(w, r.Body, r.ContentLength))
	if err != nil || n != r.ContentLength {
		writeErr(w, 400, "snapshot_body", fmt.Sprintf("incomplete snapshot: bytes=%d error=%v", n, err))
		return
	}
	if err = f.Sync(); err != nil {
		writeErr(w, 500, "snapshot_spool", err.Error())
		return
	}
	if err = s.requireCommitOwner(ctx, sc, epoch); err != nil {
		mapOwnerErr(w, err)
		return
	}
	if !s.ackAllowed(time.Now()) {
		writeErr(w, 503, "not_ready", "node cannot acknowledge")
		return
	}
	var key, etag string
	if r.URL.Query().Get("snapshot") == "1" {
		key, etag, err = s.Replica.AppendSnapshotFile(ctx, sc, epoch, f.Name(), n)
	} else {
		key, etag, err = s.Replica.AppendLevelFile(ctx, sc, epoch, 0, f.Name(), n)
	}
	if err != nil {
		writeErr(w, 500, "snapshot_publish", err.Error())
		return
	}
	if err = s.requireCommitOwner(ctx, sc, epoch); err != nil {
		mapOwnerErr(w, err)
		return
	}
	if !s.ackAllowed(time.Now()) {
		writeErr(w, 503, "not_ready", "node cannot acknowledge")
		return
	}
	s.commits.Add(1)
	s.recordCommit("bucket")
	w.Header().Set("X-Cellhive-Snapshot-Bytes", strconv.FormatInt(n, 10))
	writeJSON(w, 200, map[string]any{"mode": "bucket", "key": key, "etag": etag})
}
