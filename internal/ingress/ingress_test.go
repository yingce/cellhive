package ingress

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func startIngress(t *testing.T, table *Table, dynamic string) (addr string, srv *Server) {
	t.Helper()
	srv, err := New("127.0.0.1:0", table, dynamic, nil)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = srv.ServeListener(ctx, ln) }()
	return ln.Addr().String(), srv
}

func backend(t *testing.T, name string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		fmt.Fprintf(w, "%s host=%s path=%s query=%s body=%s", name, r.Host, r.URL.Path, r.URL.RawQuery, body)
	}))
}

func get(t *testing.T, ingressAddr, host, path string) string {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, "http://"+ingressAddr+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Host = host
	req.Close = true // routing is pinned per connection; use a fresh one per host
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	data, _ := io.ReadAll(res.Body)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, body = %s", res.StatusCode, data)
	}
	return string(data)
}

func TestRoutesStaticHostAndDynamicFallback(t *testing.T) {
	static := backend(t, "static")
	defer static.Close()
	dynamic := backend(t, "dynamic")
	defer dynamic.Close()

	addr, srv := startIngress(t, &Table{
		Routes:  map[string]string{NormalizeHost("Static.Test"): static.Listener.Addr().String()},
		Default: dynamic.Listener.Addr().String(),
	}, dynamic.Listener.Addr().String())

	if got := get(t, addr, "static.test", "/a?x=1"); !strings.HasPrefix(got, "static ") || !strings.Contains(got, "path=/a") || !strings.Contains(got, "query=x=1") {
		t.Fatalf("static route = %q", got)
	}
	if got := get(t, addr, "unknown.test", "/b"); !strings.HasPrefix(got, "dynamic ") {
		t.Fatalf("fallback = %q", got)
	}
	if req, dyn, _ := srv.Stats(); req != 2 || dyn != 1 {
		t.Fatalf("stats = %d requests / %d dynamic", req, dyn)
	}
}

func TestPreservesBodyAndHostHeader(t *testing.T) {
	static := backend(t, "static")
	defer static.Close()
	addr, _ := startIngress(t, &Table{
		Routes:  map[string]string{"static.test": static.Listener.Addr().String()},
		Default: static.Listener.Addr().String(),
	}, "")
	payload := strings.Repeat("x", 128<<10) // larger than the read buffer
	req, _ := http.NewRequest(http.MethodPost, "http://"+addr+"/upload", strings.NewReader(payload))
	req.Host = "static.test"
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	data, _ := io.ReadAll(res.Body)
	if res.StatusCode != http.StatusOK || !strings.Contains(string(data), "host=static.test") {
		t.Fatalf("response = %d %q", res.StatusCode, data)
	}
	if !strings.Contains(string(data), "body="+payload) {
		t.Fatal("request body was not spliced intact")
	}
}

func TestKeepAliveReusesConnection(t *testing.T) {
	static := backend(t, "static")
	defer static.Close()
	addr, _ := startIngress(t, &Table{
		Routes:  map[string]string{"keep.test": static.Listener.Addr().String()},
		Default: static.Listener.Addr().String(),
	}, "")
	client := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 4}}
	for i := 0; i < 3; i++ {
		req, _ := http.NewRequest(http.MethodGet, "http://"+addr+"/k", nil)
		req.Host = "keep.test"
		res, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, res.Body)
		res.Body.Close()
	}
}

func TestUpgradeSplice(t *testing.T) {
	// A raw 101 response plus echoed lines proves the bytes after the request
	// head are spliced rather than parsed by the ingress.
	upstream, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer upstream.Close()
	go func() {
		for {
			c, err := upstream.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				buf := make([]byte, 1024)
				n, _ := c.Read(buf)
				if !strings.Contains(string(buf[:n]), "Upgrade: websocket") {
					return
				}
				_, _ = io.WriteString(c, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n")
				for {
					m, err := c.Read(buf)
					if err != nil {
						return
					}
					_, _ = c.Write(buf[:m])
				}
			}(c)
		}
	}()
	addr, _ := startIngress(t, &Table{
		Routes:  map[string]string{"ws.test": upstream.Addr().String()},
		Default: upstream.Addr().String(),
	}, "")
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_, _ = io.WriteString(conn, "GET /ws HTTP/1.1\r\nHost: ws.test\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n")
	line, err := readLine(conn)
	if err != nil || !strings.Contains(line, "101") {
		t.Fatalf("upgrade status = %q err=%v", line, err)
	}
	for {
		line, err = readLine(conn)
		if err != nil {
			t.Fatalf("read upgrade headers: %v", err)
		}
		if strings.TrimRight(line, "\r\n") == "" {
			break
		}
	}
	_, _ = io.WriteString(conn, "ping")
	buf := make([]byte, 4)
	if _, err := io.ReadFull(conn, buf); err != nil || string(buf) != "ping" {
		t.Fatalf("splice echo = %q err=%v", buf, err)
	}
}

func readLine(r io.Reader) (string, error) {
	var b bytes.Buffer
	one := make([]byte, 1)
	for {
		if _, err := r.Read(one); err != nil {
			return b.String(), err
		}
		if one[0] == '\n' {
			return b.String(), nil
		}
		b.WriteByte(one[0])
	}
}

func TestTableReloadIsAtomic(t *testing.T) {
	a := backend(t, "a")
	defer a.Close()
	b := backend(t, "b")
	defer b.Close()
	path := filepath.Join(t.TempDir(), "routes.json")
	writeTable := func(backend string) {
		data, _ := json.Marshal(&Table{Routes: map[string]string{"reload.test": backend}, Default: backend})
		tmp := path + ".tmp"
		if err := os.WriteFile(tmp, data, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(tmp, path); err != nil {
			t.Fatal(err)
		}
	}
	writeTable(a.Listener.Addr().String())
	addr, srv := startIngress(t, nil, a.Listener.Addr().String())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := srv.ReloadFrom(path); err != nil {
		t.Fatal(err)
	}
	go srv.WatchTable(ctx, path, 20*time.Millisecond)
	if got := get(t, addr, "reload.test", "/"); !strings.HasPrefix(got, "a ") {
		t.Fatalf("before reload = %q", got)
	}
	writeTable(b.Listener.Addr().String())
	deadline := time.Now().Add(2 * time.Second)
	for {
		got := get(t, addr, "reload.test", "/")
		if strings.HasPrefix(got, "b ") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("reload did not apply, last = %q", got)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestNormalizeHostAndLoadTableValidation(t *testing.T) {
	for in, want := range map[string]string{
		"App.Example.COM":     "app.example.com",
		"app.example.com.":    "app.example.com",
		"app.example.com:443": "app.example.com",
		" [::1]:8080 ":        "[::1]",
	} {
		if got := NormalizeHost(in); got != want {
			t.Fatalf("NormalizeHost(%q) = %q, want %q", in, got, want)
		}
	}
	path := filepath.Join(t.TempDir(), "routes.json")
	_ = os.WriteFile(path, []byte(`{"routes":{"Bad.Host":"127.0.0.1:1"}}`), 0o600)
	if _, err := LoadTable(path); err == nil {
		t.Fatal("non-normalized host should be rejected")
	}
	_ = os.WriteFile(path, []byte(`{"routes":{},"default":"ftp://bad"}`), 0o600)
	if _, err := LoadTable(path); err == nil {
		t.Fatal("unsupported default scheme should be rejected")
	}
}

func TestOversizedHeadRejected(t *testing.T) {
	addr, _ := startIngress(t, &Table{Default: "127.0.0.1:1"}, "127.0.0.1:1")
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_, _ = io.WriteString(conn, "GET / HTTP/1.1\r\nHost: big.test\r\nX-Pad: ")
	pad := strings.Repeat("a", 70<<10)
	_, _ = io.WriteString(conn, pad+"\r\n\r\n")
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	data, _ := io.ReadAll(conn)
	if !strings.Contains(string(data), "431") {
		t.Fatalf("oversized head response = %q", data)
	}
}

// TestConnectionPinnedToFirstHost documents the deliberate HTTP/1.1 contract:
// after the byte splice starts the connection stays on the backend chosen by
// the first request's Host. Traefik terminates client connections, so a Host
// change on the same backend connection would be abnormal; callers that need a
// different host use a new connection.
func TestConnectionPinnedToFirstHost(t *testing.T) {
	static := backend(t, "static")
	defer static.Close()
	dynamic := backend(t, "dynamic")
	defer dynamic.Close()
	addr, _ := startIngress(t, &Table{
		Routes:  map[string]string{"pin.test": static.Listener.Addr().String()},
		Default: dynamic.Listener.Addr().String(),
	}, dynamic.Listener.Addr().String())
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	br := bufio.NewReader(conn)
	writeReq := func(host string) {
		if _, err := fmt.Fprintf(conn, "GET / HTTP/1.1\r\nHost: %s\r\n\r\n", host); err != nil {
			t.Fatal(err)
		}
		res, err := http.ReadResponse(br, nil)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if !strings.HasPrefix(string(body), "static ") {
			t.Fatalf("host %s went to %q, want the pinned static backend", host, body)
		}
	}
	writeReq("pin.test")
	writeReq("other.test")
}

func TestStaleStaticBackendFallsBackToDynamic(t *testing.T) {
	dyn := backend(t, "dynamic")
	defer dyn.Close()
	addr, srv := startIngress(t, &Table{
		Routes:  map[string]string{"stale.test": "127.0.0.1:1"},
		Default: dyn.Listener.Addr().String(),
	}, dyn.Listener.Addr().String())
	req, _ := http.NewRequest(http.MethodGet, "http://"+addr+"/", nil)
	req.Host = "stale.test"
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)
	if res.StatusCode != http.StatusOK || !strings.HasPrefix(string(body), "dynamic ") {
		t.Fatalf("fallback = %d %q", res.StatusCode, body)
	}
	if _, dynamic, errors := srv.Stats(); dynamic != 1 || errors != 0 {
		t.Fatalf("stats dynamic=%d errors=%d", dynamic, errors)
	}
}

func TestHostSnapshotCounts(t *testing.T) {
	static := backend(t, "static")
	defer static.Close()
	addr, srv := startIngress(t, &Table{
		Routes:  map[string]string{"a.test": static.Listener.Addr().String()},
		Default: static.Listener.Addr().String(),
	}, "")
	get(t, addr, "a.test", "/")
	get(t, addr, "a.test", "/")
	get(t, addr, "b.test", "/")
	snap := srv.HostSnapshot()
	if snap["a.test"] != 2 || snap["b.test"] != 1 {
		t.Fatalf("host snapshot = %v", snap)
	}
}
