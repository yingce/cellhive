package server

// Cloudflare-shaped Worker tail sessions for Wrangler. The log buffer and
// subscription fan-out already exist in the native CellHive tail path; this
// adapter only adds the short-lived session and WebSocket transport expected by
// Wrangler.

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
	neturl "net/url"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

type cloudflareTailSession struct {
	ID      string
	NS      string
	Worker  string
	Expires time.Time
	Search  string
}

var cloudflareTailUpgrader = websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}

func newCloudflareTailID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return hex.EncodeToString([]byte(time.Now().String()))
	}
	return hex.EncodeToString(b)
}

func (s *Server) handleCloudflareTailCreate(w http.ResponseWriter, r *http.Request) {
	ns, worker := r.PathValue("account_id"), r.PathValue("script_name")
	var filters struct {
		Search string `json:"search"`
	}
	_ = json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&filters)
	id := newCloudflareTailID()
	expires := time.Now().Add(10 * time.Minute)
	s.tailMu.Lock()
	s.tails[id] = &cloudflareTailSession{ID: id, NS: ns, Worker: worker, Expires: expires, Search: filters.Search}
	s.tailMu.Unlock()
	// The URL carries the short-lived admin credential because Wrangler's
	// WebSocket client does not forward the HTTP Authorization header used for
	// the POST that creates the session.
	scheme := "ws"
	if r.TLS != nil {
		scheme = "wss"
	}
	token := r.URL.Query().Get("token")
	if token == "" {
		token = s.Cfg.AdminToken
	}
	url := scheme + "://" + r.Host + "/client/v4/accounts/" + ns + "/workers/scripts/" + worker + "/tails/" + id + "/stream?token=" + neturl.QueryEscape(token)
	cloudflareResult(w, map[string]any{"id": id, "url": url, "expires_at": expires.UTC().Format(time.RFC3339)})
}

func (s *Server) handleCloudflareTailDelete(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("tail_id")
	s.tailMu.Lock()
	_, ok := s.tails[id]
	delete(s.tails, id)
	s.tailMu.Unlock()
	if !ok {
		writeErr(w, http.StatusNotFound, "tail_not_found", "tail session does not exist")
		return
	}
	cloudflareResult(w, nil)
}

func (s *Server) handleCloudflareTailStream(w http.ResponseWriter, r *http.Request) {
	if r.URL.Query().Get("token") != s.Cfg.AdminToken {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	id := r.PathValue("tail_id")
	s.tailMu.Lock()
	session := s.tails[id]
	s.tailMu.Unlock()
	if session == nil || time.Now().After(session.Expires) {
		http.Error(w, "tail session expired", http.StatusNotFound)
		return
	}
	conn, err := cloudflareTailUpgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()
	_ = conn.WriteJSON(map[string]any{"type": "tail_connected", "id": id})
	var cursor uint64
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-tick.C:
			if time.Now().After(session.Expires) {
				return
			}
			if s.Logs == nil {
				continue
			}
			for _, entry := range s.Logs.Since(session.NS, session.Worker, cursor, 128) {
				cursor = entry.Seq
				if session.Search != "" && !strings.Contains(entry.Message, session.Search) {
					continue
				}
				if err := conn.WriteJSON(map[string]any{"outcome": "ok", "scriptName": session.Worker, "logs": []string{entry.Message}, "exceptions": []any{}, "eventTimestamp": entry.AtMs, "requestId": entry.RequestID}); err != nil {
					return
				}
			}
		}
	}
}
