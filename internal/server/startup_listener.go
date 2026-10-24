package server

import (
	"context"
	"net"
	"net/http"
	"sync/atomic"
	"time"
)

type handlerBox struct{ h http.Handler }

// StartupListener serves retained follower disks while boot recovery waits.
// Promotion swaps the handler without closing the address or any tail request.
type StartupListener struct {
	server  *http.Server
	handler atomic.Pointer[handlerBox]
	Errors  chan error
}

func StartRecoveryListener(addr string, s *Server) (*StartupListener, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	l := &StartupListener{Errors: make(chan error, 2)}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		writeErr(w, 503, "recovering", "predecessor recovery is incomplete")
	})
	mux.HandleFunc("POST /v1/peer/seal", s.roleAuth("peer")(s.handlePeerSeal))
	mux.HandleFunc("GET /v1/peer/tail", s.roleAuth("peer")(s.handlePeerTail))
	mux.HandleFunc("GET /v1/peer/head", s.roleAuth("peer")(s.handlePeerHead))
	mux.HandleFunc("DELETE /v1/peer/session", s.roleAuth("peer")(s.handlePeerSessionDelete))
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeErr(w, 503, "recovering", "predecessor recovery is incomplete")
	})
	l.Promote(s.peerIdentity(mux))
	l.server = &http.Server{Addr: ln.Addr().String(), Handler: l, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		err := l.server.Serve(ln)
		if err != nil && err != http.ErrServerClosed {
			l.Errors <- err
		}
	}()
	return l, nil
}
func (l *StartupListener) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	l.handler.Load().h.ServeHTTP(w, r)
}
func (l *StartupListener) Promote(h http.Handler)             { l.handler.Store(&handlerBox{h: h}) }
func (l *StartupListener) Close() error                       { return l.server.Close() }
func (l *StartupListener) Shutdown(ctx context.Context) error { return l.server.Shutdown(ctx) }
