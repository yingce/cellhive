// Package userruntimepool integrates control-plane routing with the process
// pool. The workerdpool package remains independent of tenant metadata/storage.
package userruntimepool

import (
	"bytes"
	"container/list"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"cellhive/internal/bearertoken"
	"cellhive/internal/control"
	"cellhive/internal/workerdpool"
)

type Router struct {
	cellURL, internalToken, dispatchToken string
	client                                *http.Client
	mu                                    sync.Mutex
	hosts                                 map[string]hostEntry
	hostLRU                               map[string]*list.Element
	hostOrder                             *list.List
	inflight                              map[string]chan struct{}
	lookupAt                              time.Time
	lookupCount                           int
}
type hostEntry struct {
	view    control.HostView
	found   bool
	expires time.Time
	err     error
}

// maxCachedHosts bounds the per-request host cache. The Host header is
// attacker-controlled (especially on the public loader), so an unbounded map
// would be a remote memory-exhaustion primitive.
const maxCachedHosts = 10000

func NewRouter(cellURL, internalToken, dispatchToken string) *Router {
	return &Router{cellURL: strings.TrimRight(cellURL, "/"), internalToken: internalToken, dispatchToken: dispatchToken, client: &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{Proxy: nil, MaxIdleConns: 64, MaxIdleConnsPerHost: 64}}, hosts: map[string]hostEntry{}, hostLRU: map[string]*list.Element{}, hostOrder: list.New(), inflight: map[string]chan struct{}{}}
}
func (r *Router) Close() { r.client.CloseIdleConnections() }
func matches(prefix, path string) bool {
	prefix = strings.TrimRight(prefix, "/")
	return prefix == "" || path == prefix || strings.HasPrefix(path, prefix+"/")
}
func reject(code int, text string) error { return &workerdpool.HTTPError{Status: code, Message: text} }

func (r *Router) Public(req *http.Request) (workerdpool.Route, error) {
	host := control.NormalizeHost(req.Host)
	if len(host) > 253 {
		return workerdpool.Route{}, reject(400, "host too long")
	}
	view, found, err := r.hostView(req, host)
	if err != nil {
		return workerdpool.Route{}, err
	}
	if !found {
		return workerdpool.Route{}, reject(404, "host not registered")
	}
	return r.routeView(req, view)
}
func (r *Router) hostView(req *http.Request, host string) (control.HostView, bool, error) {
	var ch chan struct{}
	for {
		if err := req.Context().Err(); err != nil {
			return control.HostView{}, false, err
		}
		now := time.Now()
		r.mu.Lock()
		if e, ok := r.hosts[host]; ok && now.Before(e.expires) {
			r.touchLocked(host)
			r.mu.Unlock()
			return e.view, e.found, e.err
		}
		if wait, ok := r.inflight[host]; ok {
			r.mu.Unlock()
			select {
			case <-wait:
				continue
			case <-req.Context().Done():
				return control.HostView{}, false, req.Context().Err()
			}
		}
		if now.Sub(r.lookupAt) >= time.Second {
			r.lookupAt = now
			r.lookupCount = 0
		}
		if r.lookupCount >= 100 {
			r.mu.Unlock()
			return control.HostView{}, false, reject(429, "host lookup rate limited")
		}
		r.lookupCount++
		ch = make(chan struct{})
		r.inflight[host] = ch
		r.mu.Unlock()
		break
	}
	view, found, err := r.fetchHost(req, host)
	r.mu.Lock()
	delete(r.inflight, host)
	if err != nil {
		// Cache the failure briefly so a control-plane outage does not turn
		// every request into an upstream call; waiters receive the error, not a
		// fabricated 404.
		r.storeLocked(host, hostEntry{err: err, expires: time.Now().Add(time.Second)})
	} else {
		r.storeLocked(host, hostEntry{view: view, found: found, expires: time.Now().Add(10 * time.Second)})
	}
	close(ch)
	r.mu.Unlock()
	return view, found, err
}

func (r *Router) initLRULocked() {
	if r.hostLRU == nil {
		r.hostLRU = map[string]*list.Element{}
	}
	if r.hostOrder == nil {
		r.hostOrder = list.New()
	}
}

func (r *Router) touchLocked(host string) {
	r.initLRULocked()
	if el, ok := r.hostLRU[host]; ok {
		r.hostOrder.MoveToFront(el)
		return
	}
	r.hostLRU[host] = r.hostOrder.PushFront(host)
}

// storeLocked inserts a host entry and evicts the least-recently-used entry
// when the cache is full. A full scan on every insert made a 10k-entry cache an
// O(n) operation under the routing mutex; the LRU list keeps this O(1).
// Caller holds r.mu.
func (r *Router) storeLocked(host string, e hostEntry) {
	r.touchLocked(host)
	r.hosts[host] = e
	for len(r.hosts) > maxCachedHosts {
		if back := r.hostOrder.Back(); back != nil {
			key := back.Value.(string)
			r.hostOrder.Remove(back)
			delete(r.hostLRU, key)
			delete(r.hosts, key)
			continue
		}
		// Entries inserted directly (tests/legacy) have no LRU element. Drop one
		// so the cache still converges to the bound.
		dropped := false
		for k := range r.hosts {
			if k != host {
				delete(r.hosts, k)
				dropped = true
				break
			}
		}
		if !dropped {
			return
		}
	}
}
func (r *Router) fetchHost(req *http.Request, host string) (control.HostView, bool, error) {
	lookup, err := http.NewRequestWithContext(req.Context(), "GET", r.cellURL+"/v1/control/host?host="+url.QueryEscape(host), nil)
	if err != nil {
		return control.HostView{}, false, err
	}
	if err := bearertoken.SetInternal(lookup, r.internalToken, "internal"); err != nil {
		return control.HostView{}, false, err
	}
	resp, err := r.client.Do(lookup)
	if err != nil {
		return control.HostView{}, false, reject(503, "control unavailable")
	}
	defer resp.Body.Close()
	if resp.StatusCode == 404 {
		return control.HostView{}, false, nil
	}
	if resp.StatusCode != 200 {
		return control.HostView{}, false, reject(503, "host projection unavailable")
	}
	var view control.HostView
	if err = json.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(&view); err != nil {
		return control.HostView{}, false, reject(503, "invalid host projection")
	}
	return view, true, nil
}
func (r *Router) routeView(req *http.Request, view control.HostView) (workerdpool.Route, error) {
	if view.Conflicts {
		return workerdpool.Route{}, reject(503, "conflicting routes")
	}
	var chosen *control.HostRoute
	longest := -1
	for i := range view.Routes {
		rt := &view.Routes[i]
		prefix := strings.TrimRight(rt.Path, "/")
		if matches(prefix, req.URL.Path) && len(prefix) > longest {
			chosen = rt
			longest = len(prefix)
		}
	}
	if chosen == nil {
		return workerdpool.Route{}, reject(404, "route not found")
	}
	ptr, ok := view.Workers[chosen.Namespace+"/"+chosen.Worker]
	if !ok || ptr.Version <= 0 || ptr.BundleSHA == "" {
		return workerdpool.Route{}, reject(503, "no active worker version")
	}
	key := workerdpool.FunctionKey{Namespace: chosen.Namespace, Worker: chosen.Worker, Version: ptr.Version, BundleSHA: ptr.BundleSHA, StorageID: ptr.StorageID}
	prefix := strings.TrimRight(chosen.Path, "/")
	return workerdpool.Route{Key: key, PrepareRequest: func(out *http.Request) error {
		// ReverseProxy removes Connection-nominated headers after this callback.
		// Public callers must not be able to remove our freshly installed pins.
		var connection []string
		for _, value := range out.Header.Values("Connection") {
			for _, token := range strings.Split(value, ",") {
				token = strings.TrimSpace(token)
				if token != "" && !strings.HasPrefix(strings.ToLower(token), "x-cellhive-") && !strings.EqualFold(token, "Authorization") {
					connection = append(connection, token)
				}
			}
		}
		out.Header.Del("Connection")
		if len(connection) > 0 {
			out.Header.Set("Connection", strings.Join(connection, ", "))
		}
		for name := range out.Header {
			if strings.HasPrefix(strings.ToLower(name), "x-cellhive-") {
				out.Header.Del(name)
			}
		}
		if auth := out.Header.Get("Authorization"); auth != "" {
			out.Header.Set("x-cellhive-app-authorization", auth)
		}
		if err := bearertoken.SetInternal(out, r.internalToken, "internal"); err != nil {
			return err
		}
		out.Header.Set("x-cellhive-pool-namespace", key.Namespace)
		out.Header.Set("x-cellhive-pool-worker", key.Worker)
		out.Header.Set("x-cellhive-pool-version", strconv.Itoa(key.Version))
		out.Header.Set("x-cellhive-pool-bundle", key.BundleSHA)
		if key.StorageID != "" {
			out.Header.Set("x-cellhive-pool-storage-id", key.StorageID)
		}
		if prefix != "" {
			path := strings.TrimPrefix(out.URL.Path, prefix)
			if path == "" {
				path = "/"
			}
			out.URL.Path = path
			out.URL.RawPath = ""
		}
		return nil
	}}, nil
}

func (r *Router) Internal(req *http.Request) (workerdpool.Route, error) {
	if req.Method != "POST" {
		return workerdpool.Route{}, reject(405, "POST required")
	}
	if bearertoken.VerifyInternal(req.Header, r.dispatchToken, "dispatch", time.Now()) != nil {
		return workerdpool.Route{}, reject(401, "dispatch authentication required")
	}
	switch req.URL.Path {
	case "/v1/queues/dispatch", "/v1/timers/dispatch", "/v1/workflows/run", "/v1/services/fetch", "/v1/services/run":
	default:
		return workerdpool.Route{}, reject(404, "unknown dispatch")
	}
	// Bounded decode preserves exact original bytes for the JS adapter; no JSON
	// reserialization of args, binary payloads or messages.
	// service.go may base64-encode up to 16 MiB of raw body; allow envelope
	// expansion while retaining a hard bounded dispatch payload.
	const maxDispatchEnvelope = 24 << 20
	b, err := io.ReadAll(io.LimitReader(req.Body, maxDispatchEnvelope+1))
	req.Body.Close()
	if err != nil {
		return workerdpool.Route{}, err
	}
	if len(b) > maxDispatchEnvelope {
		return workerdpool.Route{}, reject(413, "dispatch body too large")
	}
	req.Body = io.NopCloser(bytes.NewReader(b))
	var identity struct {
		Namespace string `json:"namespace"`
		Worker    string `json:"worker"`
		Version   int    `json:"version"`
		BundleSHA string `json:"bundle_sha"`
		StorageID string `json:"storage_id,omitempty"`
	}
	if err = json.Unmarshal(b, &identity); err != nil {
		return workerdpool.Route{}, reject(400, "invalid dispatch JSON")
	}
	if identity.Namespace == "" || identity.Worker == "" || identity.BundleSHA == "" || identity.Version < 0 {
		return workerdpool.Route{}, reject(400, "invalid dispatch identity")
	}
	return workerdpool.Route{Internal: true, Key: workerdpool.FunctionKey{Namespace: identity.Namespace, Worker: identity.Worker, Version: identity.Version, BundleSHA: identity.BundleSHA, StorageID: identity.StorageID}}, nil
}
