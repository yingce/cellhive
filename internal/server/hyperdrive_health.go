package server

import (
	"context"
	"net"
	"net/url"
	"strings"
	"time"
)

// hyperdriveProxyEndpoint is one configured protocol-aware pooler address.
type hyperdriveProxyEndpoint struct {
	scheme string
	host   string
}

// parseHyperdriveProxyEndpoints mirrors hyperdriveProxyFor: bare host:port is
// the PostgreSQL endpoint; scheme://host:port entries select PG or MySQL.
func parseHyperdriveProxyEndpoints(raw string) []hyperdriveProxyEndpoint {
	var out []hyperdriveProxyEndpoint
	for _, part := range strings.Split(strings.TrimSpace(raw), ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if !strings.Contains(part, "://") {
			out = append(out, hyperdriveProxyEndpoint{scheme: "postgres", host: part})
			continue
		}
		u, err := url.Parse(part)
		if err != nil || u.Host == "" {
			continue
		}
		switch strings.ToLower(u.Scheme) {
		case "postgres", "postgresql":
			out = append(out, hyperdriveProxyEndpoint{scheme: "postgres", host: u.Host})
		case "mysql":
			out = append(out, hyperdriveProxyEndpoint{scheme: "mysql", host: u.Host})
		}
	}
	return out
}

func hyperdriveProxyMode(cfg string) bool {
	return strings.EqualFold(strings.TrimSpace(cfg), "proxy")
}

// RunHyperdriveProxyHealthLoop probes every configured pooler endpoint. It
// starts with all endpoints unhealthy: /readyz and Hyperdrive resolution fail
// closed until the first successful probe. This avoids advertising readiness
// while a configured database proxy is unreachable.
func (s *Server) RunHyperdriveProxyHealthLoop(ctx context.Context) {
	if !hyperdriveProxyMode(s.Cfg.HyperdriveMode) {
		return
	}
	endpoints := parseHyperdriveProxyEndpoints(s.Cfg.HyperdriveProxy)
	if len(endpoints) == 0 {
		return
	}
	s.proxyHealthMu.Lock()
	if s.proxyHealth == nil {
		s.proxyHealth = make(map[string]bool, len(endpoints))
	}
	for _, ep := range endpoints {
		s.proxyHealth[proxyHealthKey(ep.scheme, ep.host)] = false
	}
	s.proxyHealthChecked = true
	s.proxyHealthMu.Unlock()

	check := func() {
		for _, ep := range endpoints {
			reachable := false
			conn, err := net.DialTimeout("tcp", ep.host, 750*time.Millisecond)
			if err == nil {
				reachable = true
				_ = conn.Close()
			}
			key := proxyHealthKey(ep.scheme, ep.host)
			s.proxyHealthMu.Lock()
			previous, hadPrevious := s.proxyHealth[key]
			s.proxyHealth[key] = reachable
			s.proxyHealthMu.Unlock()
			if s.Log != nil && hadPrevious && previous != reachable {
				if reachable {
					s.Log.Info("hyperdrive proxy endpoint recovered", "scheme", ep.scheme, "endpoint", ep.host)
				} else {
					s.Log.Warn("hyperdrive proxy endpoint unreachable", "scheme", ep.scheme, "endpoint", ep.host)
				}
			}
		}
	}
	check()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			check()
		}
	}
}

func proxyHealthKey(scheme, host string) string {
	scheme = strings.ToLower(scheme)
	if scheme == "postgresql" {
		scheme = "postgres"
	}
	return scheme + "|" + host
}

// HyperdriveProxyReady reports whether every configured endpoint was reachable
// at the last probe. Before the first probe it returns false in proxy mode.
func (s *Server) HyperdriveProxyReady() (bool, string) {
	if !hyperdriveProxyMode(s.Cfg.HyperdriveMode) {
		return true, ""
	}
	endpoints := parseHyperdriveProxyEndpoints(s.Cfg.HyperdriveProxy)
	if len(endpoints) == 0 {
		return false, "no Hyperdrive proxy endpoint configured"
	}
	s.proxyHealthMu.RLock()
	defer s.proxyHealthMu.RUnlock()
	if !s.proxyHealthChecked || s.proxyHealth == nil {
		return false, "Hyperdrive proxy health not checked yet"
	}
	for _, ep := range endpoints {
		if !s.proxyHealth[proxyHealthKey(ep.scheme, ep.host)] {
			return false, "Hyperdrive proxy " + ep.scheme + " endpoint " + ep.host + " is unreachable"
		}
	}
	return true, ""
}

// HyperdriveProxyStatus returns a small operator-facing snapshot. endpoints is
// ordered by configuration and safe to serialize.
func (s *Server) HyperdriveProxyStatus() map[string]any {
	s.proxyHealthMu.RLock()
	checked := s.proxyHealthChecked
	health := make(map[string]bool, len(s.proxyHealth))
	for k, v := range s.proxyHealth {
		health[k] = v
	}
	s.proxyHealthMu.RUnlock()
	ready, reason := s.HyperdriveProxyReady()
	endpoints := make([]map[string]any, 0)
	for _, ep := range parseHyperdriveProxyEndpoints(s.Cfg.HyperdriveProxy) {
		endpoints = append(endpoints, map[string]any{
			"scheme": ep.scheme, "endpoint": ep.host,
			"healthy": health[proxyHealthKey(ep.scheme, ep.host)],
		})
	}
	return map[string]any{
		"mode": s.Cfg.HyperdriveMode, "configured": len(endpoints) > 0,
		"checked": checked, "ready": ready, "reason": reason, "endpoints": endpoints,
	}
}

// hyperdriveProxyHostHealthy returns true when no health loop has been started
// (unit-test compatibility) or the selected host passed its last probe.
func (s *Server) hyperdriveProxyHostHealthy(scheme, host string) bool {
	s.proxyHealthMu.RLock()
	defer s.proxyHealthMu.RUnlock()
	if s.proxyHealth == nil || !s.proxyHealthChecked {
		return true
	}
	return s.proxyHealth[proxyHealthKey(scheme, host)]
}
