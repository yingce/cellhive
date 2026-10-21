package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"cellhive/internal/hotset"
	"cellhive/internal/hybrid"
	"cellhive/internal/ingress"
	"cellhive/internal/staticgen"
)

// runHotLoop drives per-pod hot-set selection from ingress counters and renders
// the selected hosts as static generations. A separate refresh ticker rebuilds
// the same set so deploy/version changes roll into a new generation without
// waiting for the hot set to change.
func runHotLoop(ctx context.Context, front *ingress.Server, mgr *staticgen.Manager, cellURL, cellToken, logToken string, log *slog.Logger) {
	interval := time.Duration(envInt("CELLHIVE_HOT_INTERVAL_SEC", 60)) * time.Second
	window := time.Duration(envInt("CELLHIVE_HOT_WINDOW_SEC", 300)) * time.Second
	refresh := time.Duration(envInt("CELLHIVE_PLAN_REFRESH_SEC", 30)) * time.Second
	policy := hotset.Policy{
		Max:             envInt("CELLHIVE_HOT_MAX", 0),
		Window:          window,
		EvalInterval:    interval,
		MinQPS:          envFloat("CELLHIVE_HOT_MIN_QPS", 10),
		SwapRatio:       envFloat("CELLHIVE_HOT_SWAP_RATIO", 0.1),
		ReplaceFactor:   envFloat("CELLHIVE_HOT_REPLACE_FACTOR", 2),
		DeployStability: time.Duration(envInt("CELLHIVE_HOT_DEPLOY_STABILITY_SEC", 300)) * time.Second,
	}
	builder := newPlanBuilder(cellURL, cellToken, logToken)
	coord := hybrid.New(hybrid.Config{Policy: policy, Window: window, RefreshInterval: refresh, Logger: log}, builder, mgr)
	eval := time.NewTicker(interval)
	defer eval.Stop()
	refreshTick := time.NewTicker(refresh)
	defer refreshTick.Stop()
	poll := time.NewTicker(time.Duration(envInt("CELLHIVE_ROUTE_POLL_SEC", 5)) * time.Second)
	defer poll.Stop()
	lastRev := ""
	refreshNow := func(label string) {
		applied, err := coord.Refresh(ctx)
		if err != nil {
			log.Warn("static plan refresh failed", "trigger", label, "err", err)
		} else if applied {
			log.Info("static plan refreshed", "trigger", label)
		}
	}
	revisionChanged := func() bool {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(cellURL, "/")+"/v1/control/routes", nil)
		if err != nil {
			return false
		}
		req.Header.Set("x-cellhive-internal-token", cellToken)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			return false
		}
		defer res.Body.Close()
		if res.StatusCode != http.StatusOK {
			return false
		}
		data, err := io.ReadAll(io.LimitReader(res.Body, 8<<20))
		if err != nil {
			return false
		}
		sum := sha256.Sum256(data)
		rev := hex.EncodeToString(sum[:])
		if rev == lastRev {
			return false
		}
		lastRev = rev
		return true
	}
	log.Info("hot-set loop started", "interval", interval, "window", window, "refresh", refresh, "min_qps", policy.MinQPS, "max", policy.Max)
	for {
		select {
		case <-ctx.Done():
			return
		case <-eval.C:
			decision, applied := coord.Tick(ctx, front.HostSnapshot(), time.Now())
			if applied {
				log.Info("hot-set rendered", "static", len(decision.Static), "add", len(decision.Add), "remove", len(decision.Remove), "reason", decision.Reason)
			}
		case <-refreshTick.C:
			refreshNow("interval")
		case <-poll.C:
			if revisionChanged() {
				refreshNow("routes revision")
			}
		}
	}
}
