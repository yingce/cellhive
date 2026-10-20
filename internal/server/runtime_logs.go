package server

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"cellhive/internal/logbuf"
	"cellhive/internal/telemetry"
)

// Each runtime registers the individual workerd service addresses, not a
// load-balanced Service URL. Registration lives only in memory and expires if
// the runtime stops heartbeating.
type runtimeLogSource struct {
	url     string
	session string
	expires time.Time
	mu      sync.Mutex
	cursors map[string]runtimeLogCursor
}

type runtimeLogCursor struct {
	session string
	seq     uint64
}

type runtimeLogPage struct {
	Session string `json:"session"`
	Events  []struct {
		Seq       uint64           `json:"seq"`
		Namespace string           `json:"namespace"`
		Worker    string           `json:"worker"`
		Event     logbuf.TailEvent `json:"event"`
	} `json:"events"`
}

func (s *Server) handleRuntimeLogRegister(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Session string   `json:"session"`
		URLs    []string `json:"urls"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&input); err != nil || input.Session == "" || len(input.URLs) == 0 || len(input.URLs) > 4 {
		writeErr(w, http.StatusBadRequest, "bad_registration", "session and 1..4 URLs required")
		return
	}
	for _, raw := range input.URLs {
		u, err := url.Parse(raw)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" {
			writeErr(w, http.StatusBadRequest, "bad_registration", "URLs must be absolute HTTP service origins")
			return
		}
	}
	s.runtimeLogMu.Lock()
	if s.runtimeLogs == nil {
		s.runtimeLogs = make(map[string]*runtimeLogSource)
	}
	for _, raw := range input.URLs {
		source := s.runtimeLogs[raw]
		if source == nil || source.session != input.Session {
			source = &runtimeLogSource{url: raw, session: input.Session, cursors: make(map[string]runtimeLogCursor)}
			s.runtimeLogs[raw] = source
		}
		source.expires = time.Now().Add(20 * time.Second)
	}
	s.runtimeLogMu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) runtimeLogSources() []*runtimeLogSource {
	now := time.Now()
	s.runtimeLogMu.Lock()
	defer s.runtimeLogMu.Unlock()
	out := make([]*runtimeLogSource, 0, len(s.runtimeLogs))
	for key, source := range s.runtimeLogs {
		if now.After(source.expires) {
			delete(s.runtimeLogs, key)
			continue
		}
		out = append(out, source)
	}
	return out
}

// PullRuntimeLogs imports bounded pages only while a Tail reader is active or
// OTLP all mode requires continuous export. A cursor belongs to a particular
// runtime service and namespace/worker; process restarts reset it.
func (s *Server) PullRuntimeLogs(ctx context.Context, ns, worker string) {
	if s.Logs == nil || s.Cfg.TokenLog == "" {
		return
	}
	// Reuse one global cursor in all mode; a second filtered cursor would import
	// the same invocation twice into the agent ring and OTLP exporter.
	if telemetry.LogsMode() == telemetry.LogsAll {
		ns, worker = "", ""
	}
	var wg sync.WaitGroup
	for _, source := range s.runtimeLogSources() {
		wg.Add(1)
		go func() { defer wg.Done(); s.pullRuntimeSource(ctx, source, ns, worker) }()
	}
	wg.Wait()
}

func (s *Server) runtimeLogExporter() bool {
	if len(s.Cfg.LogRegisterURLs) == 0 || s.Cfg.NodeID == "" {
		return true
	}
	u, err := url.Parse(s.Cfg.LogRegisterURLs[0])
	if err != nil {
		return true
	}
	return strings.SplitN(u.Hostname(), ".", 2)[0] == s.Cfg.NodeID
}

func (s *Server) pullRuntimeSource(ctx context.Context, source *runtimeLogSource, ns, worker string) {
	key := ns + "\x00" + worker
	source.mu.Lock()
	defer source.mu.Unlock()
	cursor := source.cursors[key]
	client := s.ForwardClient
	if client == nil {
		client = &http.Client{Timeout: 500 * time.Millisecond}
	}
	copyClient := *client
	copyClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	for pageNum := 0; pageNum < 8; pageNum++ {
		query := url.Values{"since": {strconv.FormatUint(cursor.seq, 10)}}
		if ns != "" {
			query.Set("ns", ns)
			query.Set("worker", worker)
		}
		callCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		req, err := http.NewRequestWithContext(callCtx, http.MethodGet, strings.TrimRight(source.url, "/")+"/v1/internal/logs/events?"+query.Encode(), nil)
		if err != nil {
			cancel()
			return
		}
		req.Header.Set("x-cellhive-internal-token", s.Cfg.TokenLog)
		resp, err := copyClient.Do(req)
		if err != nil {
			cancel()
			return
		}
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			cancel()
			return
		}
		var page runtimeLogPage
		err = json.NewDecoder(io.LimitReader(resp.Body, (2<<20)+1)).Decode(&page)
		resp.Body.Close()
		cancel()
		if err != nil || page.Session == "" || len(page.Events) > 128 {
			return
		}
		if cursor.session != page.Session {
			cursor = runtimeLogCursor{session: page.Session}
			if query.Get("since") != "0" {
				continue
			}
		}
		for _, item := range page.Events {
			if item.Seq <= cursor.seq {
				continue
			}
			eventNS, eventWorker := ns, worker
			if ns == "" {
				eventNS, eventWorker = item.Namespace, item.Worker
			}
			if eventNS == "" || eventWorker == "" {
				continue
			}
			item.Event.RuntimeOrigin = source.url + "#" + page.Session
			item.Event.RuntimeSeq = item.Seq
			s.recordTailEventWithExport(eventNS, eventWorker, item.Event, "", s.runtimeLogExporter())
			cursor.seq = item.Seq
		}
		source.cursors[key] = cursor
		if len(page.Events) < 128 {
			return
		}
	}
}
