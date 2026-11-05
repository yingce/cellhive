// Package dispatch delivers due timers to a logical endpoint (the mesh service
// name of user-runtime). There is no node table: any healthy replica may serve.
package dispatch

import (
	"bytes"
	"cellhive/internal/bearertoken"
	"cellhive/internal/cell"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	"cellhive/internal/timer"
)

// HTTPDispatcher POSTs a due timer to a logical endpoint.
type HTTPDispatcher struct {
	URL    string
	Token  string
	Client *http.Client
}

// maxDispatchResponseBody bounds the successful no_retry envelope. Timer
// dispatch replies are tiny; an unbounded ReadAll lets a compromised or broken
// runtime force arbitrary allocation.
const maxDispatchResponseBody = 64 << 10

func readSuccessBody(r io.Reader) ([]byte, error) {
	return io.ReadAll(io.LimitReader(r, maxDispatchResponseBody))
}

// NewHTTP builds a dispatcher for the given base URL. An empty URL yields nil so
// callers can fall back to the no-op dispatcher.
func NewHTTP(url, token string) *HTTPDispatcher {
	url = strings.TrimSpace(url)
	if url == "" {
		return nil
	}
	return &HTTPDispatcher{
		URL:    strings.TrimRight(url, "/"),
		Token:  token,
		Client: &http.Client{Timeout: 15 * time.Second},
	}
}

// Dispatch delivers one timer.
func (d *HTTPDispatcher) Dispatch(ctx context.Context, t timer.Timer) error {
	// The scope carries the namespace ("<ns>/__timer__/<id>"); user-runtime's
	// dispatch endpoint requires it explicitly, so derive it here (ADR-154).
	namespace := ""
	if sc, err := cell.ParseScope(t.Scope); err == nil {
		namespace = sc.Namespace
	}
	body, err := json.Marshal(map[string]any{
		"kind": string(t.Kind), "scope": t.Scope, "namespace": namespace, "token": t.Token,
		"due_ms": t.DueAtMs, "occurrence": t.Occurrence,
		"worker": t.Worker, "bundle_sha": t.BundleSHA, "version": t.Version,
		"scheduled_time_ms": t.DueAtMs, "cron": t.Cron,
	})
	if err != nil {
		return dispatchFailure(d.URL, "/v1/timers/dispatch", t.Worker, "encode", err, nil)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.URL+"/v1/timers/dispatch", bytes.NewReader(body))
	if err != nil {
		return dispatchFailure(d.URL, "/v1/timers/dispatch", t.Worker, "request", err, nil)
	}
	req.Header.Set("content-type", "application/json")
	if d.Token != "" {
		if err := bearertoken.SetInternal(req, d.Token, "dispatch"); err != nil {
			return dispatchFailure(d.URL, "/v1/timers/dispatch", t.Worker, "auth", err, nil)
		}
	}
	resp, err := d.Client.Do(req)
	if err != nil {
		return dispatchFailure(d.URL, "/v1/timers/dispatch", t.Worker, "transport", err, nil)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return dispatchFailure(d.URL, "/v1/timers/dispatch", t.Worker, "response", nil, resp)
	}
	// scheduled() may call controller.noRetry() and then fail. The runtime
	// returns a successful transport response with no_retry=true so the timer
	// slot is consumed without a retry.
	var result struct {
		NoRetry bool `json:"no_retry"`
	}
	if b, e := readSuccessBody(resp.Body); e == nil && len(b) != 0 {
		_ = json.Unmarshal(b, &result)
	}
	return nil
}
