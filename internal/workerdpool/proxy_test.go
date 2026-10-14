package workerdpool

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestProxyKeepsLeaseUntilStreamEnds(t *testing.T) {
	first := make(chan struct{})
	finish := make(chan struct{})
	var once sync.Once
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "first")
		w.(http.Flusher).Flush()
		once.Do(func() { close(first) })
		<-finish
		io.WriteString(w, "last")
	}))
	defer backend.Close()
	done := make(chan error, 1)
	s := &Supervisor{Pool: &Pool{Config: Config{MaxSlots: 1}}, Launcher: LauncherFunc(func(context.Context, Slot) (*Handle, error) {
		return &Handle{PublicURL: backend.URL, InternalURL: backend.URL, Done: done, Kill: func() error { done <- nil; return nil }}, nil
	})}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	slot, err := s.Start(ctx, FunctionKey{}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(context.Background())
	proxy := NewProxy(s, func(*http.Request) (Route, error) {
		return Route{Key: FunctionKey{Namespace: "n", Worker: "w", Version: 1}}, nil
	}, nil)
	server := httptest.NewServer(proxy)
	defer server.Close()
	r, err := http.Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 5)
	if _, err = io.ReadFull(r.Body, buf); err != nil {
		t.Fatal(err)
	}
	if s.Pool.CanStop(slot.ID, slot.Generation) {
		t.Fatal("proxy released before response EOF")
	}
	close(finish)
	body, err := io.ReadAll(r.Body)
	r.Body.Close()
	if err != nil || string(buf) != "first" || string(body) != "last" {
		t.Fatalf("stream=%q %q %v", buf, body, err)
	}
	deadline := time.Now().Add(time.Second)
	for !s.Pool.CanStop(slot.ID, slot.Generation) {
		if time.Now().After(deadline) {
			t.Fatal("proxy retained finished stream")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestProxyDoesNotReplayBackendFailure(t *testing.T) {
	done := make(chan error, 1)
	s := &Supervisor{Pool: &Pool{Config: Config{MaxSlots: 1}}, Launcher: LauncherFunc(func(context.Context, Slot) (*Handle, error) {
		return &Handle{PublicURL: "http://127.0.0.1:1", Done: done, Kill: func() error { done <- nil; return nil }}, nil
	})}
	s.Start(context.Background(), FunctionKey{}, time.Now())
	defer s.Close(context.Background())
	proxy := NewProxy(s, func(*http.Request) (Route, error) {
		return Route{Key: FunctionKey{Namespace: "n", Worker: "w", Version: 1}}, nil
	}, nil)
	w := httptest.NewRecorder()
	proxy.ServeHTTP(w, httptest.NewRequest("POST", "http://app/", strings.NewReader("write")))
	if w.Code != http.StatusBadGateway {
		t.Fatalf("status=%d", w.Code)
	}
	if len(s.Pool.Slots()) != 1 {
		t.Fatal("failure triggered automatic process retry")
	}
}

func TestProxyTracksSSEUntilStreamCloses(t *testing.T) {
	release := make(chan struct{})
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.(http.Flusher).Flush()
		<-release
	}))
	defer backend.Close()
	done := make(chan error, 1)
	s := &Supervisor{Pool: &Pool{Config: Config{MaxSlots: 1}}, Launcher: LauncherFunc(func(context.Context, Slot) (*Handle, error) {
		return &Handle{PublicURL: backend.URL, Done: done, Kill: func() error { done <- nil; return nil }}, nil
	})}
	slot, err := s.Start(context.Background(), FunctionKey{}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(context.Background())
	proxy := NewProxy(s, func(*http.Request) (Route, error) { return Route{Key: FunctionKey{}}, nil }, nil)
	server := httptest.NewServer(proxy)
	defer server.Close()
	response := make(chan *http.Response, 1)
	go func() { r, _ := http.Get(server.URL); response <- r }()
	var observed Slot
	deadline := time.Now().Add(time.Second)
	for {
		observed = s.Pool.Slots()[0]
		if observed.ActiveSSE == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("SSE was not tracked")
		}
		time.Sleep(time.Millisecond)
	}
	close(release)
	r := <-response
	r.Body.Close()
	deadline = time.Now().Add(time.Second)
	for {
		observed = s.Pool.Slots()[0]
		if observed.ActiveSSE == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("SSE was not released")
		}
		time.Sleep(time.Millisecond)
	}
	_ = slot
}

func TestProxyTracksWebSocketUntilTunnelCloses(t *testing.T) {
	closeBackend := make(chan struct{})
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h, ok := w.(http.Hijacker)
		if !ok {
			t.Error("backend lacks hijack")
			return
		}
		c, rw, err := h.Hijack()
		if err != nil {
			return
		}
		_, _ = rw.WriteString("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n")
		_ = rw.Flush()
		<-closeBackend
		_ = c.Close()
	}))
	defer backend.Close()
	done := make(chan error, 1)
	s := &Supervisor{Pool: &Pool{Config: Config{MaxSlots: 1}}, Launcher: LauncherFunc(func(context.Context, Slot) (*Handle, error) {
		return &Handle{PublicURL: backend.URL, Done: done, Kill: func() error { done <- nil; return nil }}, nil
	})}
	_, err := s.Start(context.Background(), FunctionKey{}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(context.Background())
	proxy := NewProxy(s, func(*http.Request) (Route, error) { return Route{Key: FunctionKey{}}, nil }, nil)
	server := httptest.NewServer(proxy)
	defer server.Close()
	addr := server.Listener.Addr().String()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_, _ = io.WriteString(conn, "GET / HTTP/1.1\r\nHost: ws.test\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n")
	buf := make([]byte, 128)
	_, err = conn.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		slots := s.Pool.Slots()
		if len(slots) > 0 && slots[0].ActiveWebSockets == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("WebSocket was not tracked")
		}
		time.Sleep(time.Millisecond)
	}
	close(closeBackend)
	_ = conn.Close()
	deadline = time.Now().Add(time.Second)
	for {
		slots := s.Pool.Slots()
		if len(slots) > 0 && slots[0].ActiveWebSockets == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("WebSocket was not released")
		}
		time.Sleep(time.Millisecond)
	}
}
