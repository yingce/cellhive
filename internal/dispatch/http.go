// Package dispatch delivers due timers to a logical endpoint (the mesh service
// name of user-runtime). There is no node table: any healthy replica may serve.
package dispatch

import (
	"bytes"
	"cellhive/internal/cell"
	"context"
	"encoding/json"
	"fmt"
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
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.URL+"/v1/timers/dispatch", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("content-type", "application/json")
	if d.Token != "" {
		req.Header.Set("x-cellhive-internal-token", d.Token)
	}
	resp, err := d.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("dispatch: status %d", resp.StatusCode)
	}
	return nil
}
