package doruntime

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// RenewEvery is how often a do-runtime renews its owner leases with the local
// host actor; DrainBudget bounds the best-effort drain on shutdown (ADR-078).
const (
	RenewEvery  = 10 * time.Second
	DrainBudget = 15 * time.Second
)

// PostLocal sends a small JSON POST to the local host actor with the internal
// token (renew/drain are loopback-only, never a service alias).
func PostLocal(ctx context.Context, base, token, path, body string) error {
	url := strings.TrimRight(base, "/") + path
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("content-type", "application/json")
	req.Header.Set("x-cellhive-internal-token", token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("%s -> %s", url, resp.Status)
	}
	return nil
}

// RenewLoop renews the DO owner leases until ctx is done. onErr (optional)
// receives transient failures so the caller can log them.
func RenewLoop(ctx context.Context, base, token string, every time.Duration, onErr func(error)) {
	if every <= 0 {
		every = RenewEvery
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := PostLocal(ctx, base, token, "/v1/do/renew", "{}"); err != nil && onErr != nil {
				onErr(err)
			}
		}
	}
}

// Drain stops new work, waits for in-flight requests and releases leases. It is
// best effort: a failure just means the leases expire instead.
func Drain(base, token string) error {
	ctx, cancel := context.WithTimeout(context.Background(), DrainBudget)
	defer cancel()
	return PostLocal(ctx, base, token, "/v1/do/drain", "{}")
}
