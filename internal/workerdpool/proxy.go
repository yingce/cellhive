package workerdpool

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"time"
)

// Route is resolved once at admission. PrepareRequest may transform a public
// request into a trusted fixed-version execution request, never from client-
// supplied platform headers. Internal dispatch stays on the separate socket.
type Route struct {
	Key            FunctionKey
	Internal       bool
	PrepareRequest func(*http.Request) error
}
type Resolver func(*http.Request) (Route, error)

type HTTPError struct {
	Status  int
	Message string
}

func (e *HTTPError) Error() string { return e.Message }

type AcquireFunc func(context.Context, FunctionKey) (Slot, error)
type Proxy struct {
	Supervisor *Supervisor
	Resolve    Resolver
	Acquire    AcquireFunc
	transport  *http.Transport
	buffers    byteBuffers
	mu         sync.Mutex
	backends   map[string]*httputil.ReverseProxy
}
type byteBuffers struct{ sync.Pool }

func (b *byteBuffers) Get() []byte {
	if v := b.Pool.Get(); v != nil {
		return v.([]byte)
	}
	return make([]byte, 32<<10)
}
func (b *byteBuffers) Put(v []byte) {
	if cap(v) == 32<<10 {
		b.Pool.Put(v[:32<<10])
	}
}

func NewProxy(s *Supervisor, resolve Resolver, acquire AcquireFunc) *Proxy {
	return &Proxy{Supervisor: s, Resolve: resolve, Acquire: acquire, transport: &http.Transport{Proxy: nil, MaxIdleConns: 256, MaxIdleConnsPerHost: 64, IdleConnTimeout: 60 * time.Second, ResponseHeaderTimeout: 0}, backends: map[string]*httputil.ReverseProxy{}}
}
func (p *Proxy) Close() { p.transport.CloseIdleConnections() }
func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	route, err := p.Resolve(r)
	if err != nil {
		status := http.StatusBadRequest
		var rejected *HTTPError
		if errors.As(err, &rejected) {
			status = rejected.Status
		}
		if errors.Is(err, ErrCapacity) {
			status = http.StatusServiceUnavailable
		}
		http.Error(w, "request routing unavailable", status)
		return
	}
	var slot Slot
	if p.Acquire != nil {
		slot, err = p.Acquire(r.Context(), route.Key)
	} else {
		var ok bool
		slot, ok = p.Supervisor.Pool.Select(route.Key, time.Now())
		if !ok {
			err = ErrCapacity
		}
	}
	if err != nil {
		http.Error(w, "runtime execution capacity unavailable", http.StatusServiceUnavailable)
		return
	}
	// ReverseProxy blocks until body copying or websocket tunnelling completes.
	// It never returns early on Response headers, and never retries business calls.
	defer func() { p.Supervisor.Pool.Release(slot.ID, slot.Generation, time.Now()) }()
	h, ok := p.Supervisor.Handle(slot.ID, slot.Generation)
	if !ok {
		http.Error(w, "execution generation exited", http.StatusBadGateway)
		return
	}
	base := h.PublicURL
	if route.Internal {
		base = h.InternalURL
	}
	target, err := url.Parse(base)
	if err != nil || target.Host == "" {
		http.Error(w, "invalid execution backend", http.StatusBadGateway)
		return
	}
	request := r.Clone(r.Context())
	if route.PrepareRequest != nil {
		if err = route.PrepareRequest(request); err != nil {
			http.Error(w, "execution request rejected", http.StatusBadRequest)
			return
		}
	}
	proxy := p.backendProxy(target)
	upgrade := strings.EqualFold(request.Header.Get("Upgrade"), "websocket")
	if upgrade {
		p.Supervisor.Pool.AddConnection(slot.ID, slot.Generation, WebSocket)
		defer p.Supervisor.Pool.RemoveConnection(slot.ID, slot.Generation, WebSocket)
	}
	tracked := &trackingResponseWriter{ResponseWriter: w, pool: p.Supervisor.Pool, slot: slot}
	proxy.ServeHTTP(tracked, request)
	if tracked.sse {
		p.Supervisor.Pool.RemoveConnection(slot.ID, slot.Generation, SSE)
	}
}

func (p *Proxy) backendProxy(target *url.URL) *httputil.ReverseProxy {
	key := target.String()
	p.mu.Lock()
	defer p.mu.Unlock()
	if proxy := p.backends[key]; proxy != nil {
		return proxy
	}
	if len(p.backends) >= 256 {
		for old := range p.backends {
			delete(p.backends, old)
			break
		}
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.Transport = p.transport
	proxy.BufferPool = &p.buffers
	proxy.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, _ error) {
		http.Error(w, "execution failed; result may be unknown", http.StatusBadGateway)
	}
	p.backends[key] = proxy
	return proxy
}

type trackingResponseWriter struct {
	http.ResponseWriter
	pool  *Pool
	slot  Slot
	sse   bool
	wrote bool
}

func (w *trackingResponseWriter) WriteHeader(status int) {
	w.wrote = true
	w.detect()
	w.ResponseWriter.WriteHeader(status)
}
func (w *trackingResponseWriter) Write(b []byte) (int, error) {
	if !w.wrote {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(b)
}
func (w *trackingResponseWriter) detect() {
	if w.sse {
		return
	}
	if strings.Contains(strings.ToLower(w.Header().Get("content-type")), "text/event-stream") && w.pool.AddConnection(w.slot.ID, w.slot.Generation, SSE) {
		w.sse = true
	}
}
func (w *trackingResponseWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}
func (w *trackingResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	h, ok := w.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("response writer does not support hijack")
	}
	return h.Hijack()
}
func (w *trackingResponseWriter) Push(target string, opts *http.PushOptions) error {
	if p, ok := w.ResponseWriter.(http.Pusher); ok {
		return p.Push(target, opts)
	}
	return http.ErrNotSupported
}
func (w *trackingResponseWriter) ReadFrom(r io.Reader) (int64, error) {
	if rf, ok := w.ResponseWriter.(io.ReaderFrom); ok {
		return rf.ReadFrom(r)
	}
	return io.Copy(w.ResponseWriter, r)
}
