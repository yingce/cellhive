// Package runtimelog advertises individual workerd service origins for
// on-demand Tail reads. Registrations expire when a supervisor stops.
package runtimelog

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"
	"time"
)

func RegisterLoop(ctx context.Context, cellURL, token string, urls []string) {
	RegisterLoopFunc(ctx, cellURL, token, func() []string { return urls })
}

// RegisterLoopFunc is RegisterLoop with a dynamic URL list: the static
// generation's tail socket changes on every rollout, so the advertised sources
// must be recomputed on each registration tick.
func RegisterLoopFunc(ctx context.Context, cellURL, token string, urls func() []string) {
	if cellURL == "" || token == "" || urls == nil {
		return
	}
	var session [16]byte
	if _, err := rand.Read(session[:]); err != nil {
		return
	}
	client := &http.Client{Timeout: 2 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	agents := strings.Split(cellURL, ",")
	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()
	for {
		current := urls()
		if len(current) == 0 {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			}
			continue
		}
		body, _ := json.Marshal(struct {
			Session string   `json:"session"`
			URLs    []string `json:"urls"`
		}{hex.EncodeToString(session[:]), current})
		for _, agent := range agents {
			callCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			req, err := http.NewRequestWithContext(callCtx, http.MethodPost, strings.TrimRight(strings.TrimSpace(agent), "/")+"/v1/internal/logs/runtime", bytes.NewReader(body))
			if err != nil {
				cancel()
				continue
			}
			req.Header.Set("content-type", "application/json")
			req.Header.Set("x-cellhive-internal-token", token)
			resp, err := client.Do(req)
			if err != nil {
				cancel()
				continue
			}
			resp.Body.Close()
			cancel()
			// Every agent must know every runtime so CLI Tail works regardless of
			// which agent its admin request reaches. Each source keeps its own cursor.
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}
