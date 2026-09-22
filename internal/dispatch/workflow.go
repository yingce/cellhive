package dispatch

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"cellhive/internal/control"
	"cellhive/internal/timer"
)

// WorkflowDispatcher runs/resumes a workflow instance on user-runtime, and
// handles KindWorkflowSleep timers by re-dispatching the slept instance.
type WorkflowDispatcher struct {
	// URL is the user-runtime privileged dispatch base (e.g. http://host:8088).
	URL string
	// Token is the dispatch-role token (ADR-075).
	Token string
	// Target resolves a workflow definition to its worker + entrypoint class.
	Target func(ctx context.Context, ns, name string) (control.WorkflowTarget, bool)
	// Params reads the durable instance input for every run and timer resume.
	Params func(ctx context.Context, ns, name, id string) ([]byte, error)
	// Claim acquires a run lease for the instance (nil = dispatch without lease).
	Claim func(ctx context.Context, ns, name, id string) (token string, generation uint64, ok bool, err error)
	// Next receives non-workflow timer kinds.
	Next   timer.Dispatcher
	Client *http.Client
	Log    *slog.Logger
}

func (d *WorkflowDispatcher) client() *http.Client {
	if d.Client != nil {
		return d.Client
	}
	return &http.Client{Timeout: 30 * time.Second}
}

func (d *WorkflowDispatcher) log() *slog.Logger {
	if d.Log != nil {
		return d.Log
	}
	return slog.Default()
}

// Run dispatches one attempt of a workflow instance.
func (d *WorkflowDispatcher) Run(ctx context.Context, ns, name, id, runToken string) error {
	if d.URL == "" || d.Target == nil {
		return fmt.Errorf("workflow: dispatcher not configured")
	}
	t, ok := d.Target(ctx, ns, name)
	if !ok {
		return fmt.Errorf("workflow: no active definition %s/%s", ns, name)
	}
	if d.Params == nil {
		return fmt.Errorf("workflow: instance params reader not configured")
	}
	params, err := d.Params(ctx, ns, name, id)
	if err != nil {
		return fmt.Errorf("workflow: read instance %s/%s/%s: %w", ns, name, id, err)
	}
	payload, _ := json.Marshal(map[string]any{
		"namespace": ns, "worker": t.Worker, "bundle_sha": t.BundleSHA, "version": t.Version,
		"workflow": name, "class_name": t.ClassName, "id": id, "run_token": runToken,
		"params": base64.StdEncoding.EncodeToString(params),
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		strings.TrimRight(d.URL, "/")+"/v1/workflows/run", bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("content-type", "application/json")
	req.Header.Set("x-cellhive-internal-token", d.Token)
	resp, err := d.client().Do(req)
	if err != nil {
		return fmt.Errorf("workflow: dispatch: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("workflow: dispatch status %d", resp.StatusCode)
	}
	return nil
}

// Dispatch is the timer.Dispatcher facade: KindWorkflowSleep timers resume the
// slept instance; everything else passes to Next.
func (d *WorkflowDispatcher) Dispatch(ctx context.Context, t timer.Timer) error {
	if t.Kind != timer.KindWorkflowSleep {
		if d.Next != nil {
			return d.Next.Dispatch(ctx, t)
		}
		return nil
	}
	ns, name, ok := splitWorkflowScope(t.Scope)
	if !ok {
		return fmt.Errorf("workflow: bad sleep scope %q", t.Scope)
	}
	// Timer-driven resume: claim a fresh run attempt (the previous attempt
	// released its lease when it parked).
	token, _, ok, err := d.Claim(ctx, ns, name, t.Occurrence)
	if err != nil || !ok {
		return err
	}
	return d.Run(ctx, ns, name, t.Occurrence, token)
}

// splitWorkflowScope parses "<ns>/__workflow__/<name>".
func splitWorkflowScope(scope string) (ns, name string, ok bool) {
	parts := strings.SplitN(scope, "/", 3)
	if len(parts) != 3 || parts[1] != "__workflow__" {
		return "", "", false
	}
	return parts[0], parts[2], true
}
