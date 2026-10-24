package server

import (
	"cellhive/internal/peer"
	"net/http"
	"strings"
)

// Check identity on every HTTP request and on the stream handshake. This also
// prevents a reused DNS/IP address from sealing an empty replacement spool.
func (s *Server) peerIdentity(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/v1/peer/") && (r.Header.Get(peer.TargetNodeHeader) != "" || r.Header.Get(peer.TargetInstanceHeader) != "") {
			node, instance := r.Header.Get(peer.TargetNodeHeader), r.Header.Get(peer.TargetInstanceHeader)
			if s.Lease == nil || node == "" || instance == "" || node != s.Lease.NodeID || instance != s.Lease.Instance {
				writeErr(w, http.StatusConflict, "peer_identity_mismatch", "persistent follower identity does not match")
				return
			}
			if r.URL.Path == "/v1/peer/append" || r.URL.Path == "/v1/peer/append_batch" || r.URL.Path == "/v1/peer/stream" {
				session := r.Header.Get("x-cellhive-owner-session")
				if s.Spool == nil || session == "" || !s.Spool.SessionExists(session) {
					writeErr(w, 409, "session_missing", "identified session was not prepared")
					return
				}
			}
			w.Header().Set(peer.TargetNodeHeader, node)
			w.Header().Set(peer.TargetInstanceHeader, instance)
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) handlePeerPrepare(w http.ResponseWriter, r *http.Request) {
	if s.Spool == nil {
		writeErr(w, 503, "no_spool", "spool missing")
		return
	}
	if err := s.Spool.Prepare(r.URL.Query().Get("session")); err != nil {
		writeErr(w, 409, "prepare_failed", err.Error())
		return
	}
	writeJSON(w, 200, map[string]bool{"ok": true})
}
