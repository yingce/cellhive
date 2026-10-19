package server

// Cloudflare-shaped Worker tail sessions for Wrangler. The log buffer and
// subscription fan-out already exist in the native CellHive tail path; this
// adapter only adds the short-lived session and WebSocket transport expected by
// Wrangler.

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	neturl "net/url"
	"strconv"
	"strings"
	"time"

	"cellhive/internal/logbuf"
	"cellhive/internal/scopedtoken"
	"github.com/gorilla/websocket"
)

const tailTokenIssuer = "tail-stream"

type peerTailEvent struct {
	Seq   uint64           `json:"seq"`
	Event logbuf.TailEvent `json:"event"`
}

type peerTailPage struct {
	Events []peerTailEvent `json:"events"`
}

type peerTailCursor struct {
	Session string
	Seq     uint64
}

// Peer Tail reads are bounded point reads from the local ring. The sequence is
// an internal cursor and is never included in a Wrangler TailItem.
func (s *Server) handlePeerLogEvents(w http.ResponseWriter, r *http.Request) {
	ns, worker := r.URL.Query().Get("ns"), r.URL.Query().Get("worker")
	if ns == "" || worker == "" {
		writeErr(w, http.StatusBadRequest, "bad_request", "ns and worker are required")
		return
	}
	since, err := strconv.ParseUint(r.URL.Query().Get("since"), 10, 64)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "bad_request", "since must be an unsigned integer")
		return
	}
	page := peerTailPage{Events: []peerTailEvent{}}
	if s.Logs != nil {
		for _, event := range s.Logs.EventsSince(ns, worker, since, 32) {
			page.Events = append(page.Events, peerTailEvent{Seq: event.Seq, Event: event})
		}
	}
	w.Header().Set("cache-control", "no-store")
	writeJSON(w, http.StatusOK, page)
}

func (s *Server) fetchPeerLogEvents(ctx context.Context, advert, ns, worker string, since uint64) (peerTailPage, error) {
	if !strings.HasPrefix(advert, "http://") && !strings.HasPrefix(advert, "https://") {
		advert = "http://" + advert
	}
	query := neturl.Values{"ns": {ns}, "worker": {worker}, "since": {strconv.FormatUint(since, 10)}}
	ctx, cancel := context.WithTimeout(ctx, 400*time.Millisecond)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(advert, "/")+"/v1/internal/logs/events?"+query.Encode(), nil)
	if err != nil {
		return peerTailPage{}, err
	}
	req.Header.Set("x-cellhive-internal-token", s.Cfg.TokenPeer)
	client := &http.Client{Timeout: 400 * time.Millisecond}
	if s.ForwardClient != nil {
		copy := *s.ForwardClient
		client = &copy
	}
	// Lease advertisements are internal node addresses. Never carry the peer
	// credential to a redirect target supplied by a misconfigured peer.
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	resp, err := client.Do(req)
	if err != nil {
		return peerTailPage{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return peerTailPage{}, fmt.Errorf("peer Tail: %s", resp.Status)
	}
	var page peerTailPage
	if err := json.NewDecoder(io.LimitReader(resp.Body, 2<<20)).Decode(&page); err != nil {
		return peerTailPage{}, err
	}
	if len(page.Events) > 32 {
		return peerTailPage{}, fmt.Errorf("peer Tail: page exceeds 32 events")
	}
	return page, nil
}

var cloudflareTailUpgrader = websocket.Upgrader{
	CheckOrigin:  func(*http.Request) bool { return true },
	Subprotocols: []string{"trace-v1"}, // Wrangler 4.133.0 requests this protocol.
}

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
	if len(filters.Search) > 256 {
		writeErr(w, http.StatusBadRequest, "invalid_tail_filter", "tail search exceeds 256 bytes")
		return
	}
	id := newCloudflareTailID()
	expires := time.Now().Add(10 * time.Minute)
	key, err := scopedtoken.IssuerKey([]byte(s.Cfg.ScopeSecret), tailTokenIssuer)
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "tail_token_unavailable", err.Error())
		return
	}
	name := worker + "#" + id + "#" + base64.RawURLEncoding.EncodeToString([]byte(filters.Search))
	token, err := scopedtoken.Mint(key, scopedtoken.Claims{Namespace: ns, Kind: tailTokenIssuer, Name: name, Iss: tailTokenIssuer, ExpiresMs: expires.UnixMilli()})
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "tail_token_unavailable", err.Error())
		return
	}
	// Wrangler's WebSocket client does not forward the POST Authorization header.
	// The URL therefore carries only this expiring, worker-scoped credential.
	scheme := "ws"
	if r.TLS != nil || r.Header.Get("X-Forwarded-Proto") == "https" {
		scheme = "wss"
	}
	url := scheme + "://" + r.Host + "/client/v4/accounts/" + ns + "/workers/scripts/" + worker + "/tails/" + id + "/stream?token=" + neturl.QueryEscape(token)
	cloudflareResult(w, map[string]any{"id": id, "url": url, "expires_at": expires.UTC().Format(time.RFC3339)})
}

func (s *Server) handleCloudflareTailDelete(w http.ResponseWriter, r *http.Request) {
	// Session credentials are stateless so streams can land on any cell-agent.
	// Wrangler closes its WebSocket before calling DELETE; expiration bounds an
	// issued credential independently of which node receives the DELETE.
	cloudflareResult(w, nil)
}

func (s *Server) handleCloudflareTailStream(w http.ResponseWriter, r *http.Request) {
	claims, err := scopedtoken.VerifyIssuer([]byte(s.Cfg.ScopeSecret), r.URL.Query().Get("token"), time.Now())
	if err != nil || claims.Iss != tailTokenIssuer || claims.Kind != tailTokenIssuer || claims.Namespace != r.PathValue("account_id") {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	prefix := r.PathValue("script_name") + "#" + r.PathValue("tail_id") + "#"
	if !strings.HasPrefix(claims.Name, prefix) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	search, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(claims.Name, prefix))
	if err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	conn, err := cloudflareTailUpgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()
	// Wrangler sends an initial unmasked debug frame (mask:false in its official
	// client). Gorilla correctly rejects unmasked client frames on ReadMessage.
	// Keep that client write-only and answer its fixed keepalive ping with a
	// legal unsolicited Pong; standard clients use the normal reader/control
	// frame path. This also prevents Wrangler's 10s reconnect/ring replay loop.
	wrangler := strings.HasPrefix(strings.ToLower(r.UserAgent()), "wrangler/") && conn.Subprotocol() == "trace-v1"
	var readDone <-chan struct{}
	if !wrangler {
		done := make(chan struct{})
		readDone = done
		go func() {
			defer close(done)
			for {
				if _, _, err := conn.ReadMessage(); err != nil {
					return
				}
			}
		}()
	}
	lastPong := time.Time{}
	var cursor uint64
	peerCursors := map[string]peerTailCursor{}
	worker := r.PathValue("script_name")
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-readDone:
			return
		case <-tick.C:
			if time.Now().UnixMilli() >= claims.ExpiresMs {
				return
			}
			if wrangler && time.Since(lastPong) >= 2*time.Second {
				if err := conn.WriteControl(websocket.PongMessage, []byte("wrangler tail ping"), time.Now().Add(time.Second)); err != nil {
					return
				}
				lastPong = time.Now()
			}
			var local []logbuf.TailEvent
			if s.Logs != nil {
				local = s.Logs.EventsSince(claims.Namespace, worker, cursor, 128)
			}
			for _, event := range local {
				cursor = event.Seq
				if len(search) != 0 {
					encoded, _ := json.Marshal(event)
					if !strings.Contains(string(encoded), string(search)) {
						continue
					}
				}
				if err := conn.WriteJSON(event); err != nil {
					return
				}
			}
			if s.Lease == nil || s.Cfg.TokenPeer == "" {
				continue
			}
			for _, peer := range s.Lease.SampleCached(r.Context(), 15*time.Second) {
				if peer.Node == s.Cfg.NodeID || peer.Advertise == "" || !peer.Live(time.Now()) {
					continue
				}
				pc := peerCursors[peer.Node]
				if pc.Session != peer.Session {
					pc = peerTailCursor{Session: peer.Session}
				}
				page, err := s.fetchPeerLogEvents(r.Context(), peer.Advertise, claims.Namespace, worker, pc.Seq)
				if err != nil {
					peerCursors[peer.Node] = pc
					continue
				}
				for _, item := range page.Events {
					if item.Seq <= pc.Seq {
						continue
					}
					pc.Seq = item.Seq
					if len(search) != 0 {
						encoded, _ := json.Marshal(item.Event)
						if !strings.Contains(string(encoded), string(search)) {
							continue
						}
					}
					if err := conn.WriteJSON(item.Event); err != nil {
						return
					}
				}
				peerCursors[peer.Node] = pc
			}
		}
	}
}
