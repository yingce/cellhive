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
	if cellURL == "" || token == "" || len(urls) == 0 {
		return
	}
	var session [16]byte
	if _, err := rand.Read(session[:]); err != nil {
		return
	}
	body, _ := json.Marshal(struct {
		Session string   `json:"session"`
		URLs    []string `json:"urls"`
	}{hex.EncodeToString(session[:]), urls})
	client := &http.Client{Timeout: 2 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	agents := strings.Split(cellURL, ",")
	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()
	for {
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
