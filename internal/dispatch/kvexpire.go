package dispatch

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/timer"
)

// KVExpireDispatcher routes KindKVExpire timers to the cell's owner, which runs
// the captured cleanup (delete expired rows, re-arm/disarm the timer) at
// POST /v1/internal/kv/expire (ADR-098). Other kinds pass through to Next.
//
// It reuses the unified timer activation: the local runner dispatches due timers
// for the scopes it owns, and the fleet waker dispatches them for a cell whose
// owner stopped, so expiry cleanup happens after a takeover too.
type KVExpireDispatcher struct {
	Next    timer.Dispatcher
	Resolve func(ctx context.Context, s cell.Scope) (cell.Owner, string, error)
	Token   string
	Client  *http.Client
	Log     *slog.Logger
}

func (d *KVExpireDispatcher) client() *http.Client {
	if d.Client != nil {
		return d.Client
	}
	return &http.Client{Timeout: 15 * time.Second}
}

func (d *KVExpireDispatcher) log() *slog.Logger {
	if d.Log != nil {
		return d.Log
	}
	return slog.Default()
}

// Dispatch delivers a KV expiry cleanup to the owning cell-agent.
func (d *KVExpireDispatcher) Dispatch(ctx context.Context, t timer.Timer) error {
	if t.Kind != timer.KindKVExpire {
		return d.Next.Dispatch(ctx, t)
	}
	sc, err := cell.ParseScope(t.Scope)
	if err != nil {
		return fmt.Errorf("kv expire: bad scope %q: %w", t.Scope, err)
	}
	if d.Resolve == nil {
		return fmt.Errorf("kv expire: dispatcher not configured")
	}
	o, _, err := d.Resolve(ctx, sc)
	if err != nil {
		return fmt.Errorf("kv expire: resolve owner: %w", err)
	}
	if o.Address == "" {
		return fmt.Errorf("kv expire: owner %s has no address", o.Node)
	}
	base := strings.TrimRight(o.Address, "/")
	if !strings.HasPrefix(base, "http://") && !strings.HasPrefix(base, "https://") {
		base = "http://" + base
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		base+"/v1/internal/kv/expire?scope="+url.QueryEscape(sc.String()), nil)
	if err != nil {
		return err
	}
	if d.Token != "" {
		req.Header.Set("x-cellhive-internal-token", d.Token)
	}
	resp, err := d.client().Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("kv expire: cleanup -> %s", resp.Status)
	}
	return nil
}
