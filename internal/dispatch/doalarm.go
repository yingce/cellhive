package dispatch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/owner"
	"cellhive/internal/timer"
)

// DoAlarmDispatcher routes KindDOAlarm timers to the owning do-runtime task,
// which runs the object's alarm() handler (ADR-079). Other kinds pass through
// to the next dispatcher (user-runtime).
type DoAlarmDispatcher struct {
	Next    timer.Dispatcher
	Resolve func(ctx context.Context, s cell.Scope) (cell.Owner, string, error)
	// Bundle resolves the worker's active bundle sha + version number for the
	// alarm invocation (version lets the runtime key the DO facet/isolate by
	// version, ADR-127).
	Bundle func(ctx context.Context, ns, worker string) (string, int, bool)
	Token  string
	Client *http.Client
	Log    *slog.Logger
}

func (d *DoAlarmDispatcher) client() *http.Client {
	if d.Client != nil {
		return d.Client
	}
	return &http.Client{Timeout: 15 * time.Second}
}

func (d *DoAlarmDispatcher) log() *slog.Logger {
	if d.Log != nil {
		return d.Log
	}
	return slog.Default()
}

// Dispatch delivers an alarm to the owning do-runtime.
func (d *DoAlarmDispatcher) Dispatch(ctx context.Context, t timer.Timer) error {
	if t.Kind != timer.KindDOAlarm {
		return d.Next.Dispatch(ctx, t)
	}
	sc, err := cell.ParseScope(t.Scope)
	if err != nil {
		return fmt.Errorf("do alarm: bad scope %q: %w", t.Scope, err)
	}
	var occ struct {
		Worker       string `json:"w"`
		Class        string `json:"c"`
		Shard        int    `json:"s"`
		StorageClass string `json:"sc"`
		StorageID    string `json:"sid"`
		ID           string `json:"id"`
	}
	if err := json.Unmarshal([]byte(t.Occurrence), &occ); err != nil {
		return fmt.Errorf("do alarm: bad occurrence %q: %w", t.Occurrence, err)
	}
	worker, class, id := occ.Worker, occ.Class, occ.ID
	shard := occ.Shard
	if worker == "" || class == "" || id == "" {
		return fmt.Errorf("do alarm: incomplete occurrence %q", t.Occurrence)
	}
	if d.Resolve == nil || d.Bundle == nil {
		return fmt.Errorf("do alarm: dispatcher not configured")
	}
	o, _, err := d.Resolve(ctx, owner.DOScope(sc.Namespace, worker, class, shard))
	if err != nil {
		return fmt.Errorf("do alarm: resolve owner: %w", err)
	}
	if o.Address == "" {
		return fmt.Errorf("do alarm: owner %s has no address", o.Node)
	}
	sha, ver, ok := d.Bundle(ctx, sc.Namespace, worker)
	if !ok {
		return fmt.Errorf("do alarm: no active bundle for %s/%s", sc.Namespace, worker)
	}
	body, _ := json.Marshal(map[string]any{
		"namespace": sc.Namespace, "worker": worker, "class": class, "shard": shard,
		"id": id, "bundle_sha": sha, "version": ver, "kind": "alarm",
		"storage_class": occ.StorageClass, "storage_id": occ.StorageID,
	})
	// do-runtime addresses are advertised as host:port (ADR-080); add the
	// scheme before URL parsing or the dispatcher fails every tick
	// ("first path segment in URL cannot contain colon").
	addr := o.Address
	if !strings.HasPrefix(addr, "http://") && !strings.HasPrefix(addr, "https://") {
		addr = "http://" + addr
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(addr, "/")+"/v1/do/invoke", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("content-type", "application/json")
	if d.Token != "" {
		req.Header.Set("x-cellhive-internal-token", d.Token)
	}
	resp, err := d.client().Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("do alarm: invoke -> %s", resp.Status)
	}
	return nil
}
