package server

import (
	"testing"

	"cellhive/internal/config"
)

func TestParseHyperdriveProxyEndpoints(t *testing.T) {
	eps := parseHyperdriveProxyEndpoints("proxysql:6133,mysql://proxy-mysql:6033,postgres://proxy-pg:6133")
	if len(eps) != 3 {
		t.Fatalf("endpoints=%+v", eps)
	}
	if eps[0].scheme != "postgres" || eps[0].host != "proxysql:6133" {
		t.Fatalf("bare endpoint=%+v", eps[0])
	}
	if eps[1].scheme != "mysql" || eps[2].scheme != "postgres" {
		t.Fatalf("endpoints=%+v", eps)
	}
}

func TestHyperdriveProxyReadinessFailsClosed(t *testing.T) {
	s := &Server{Cfg: config.Config{HyperdriveMode: "proxy", HyperdriveProxy: "postgres://db:5432"}}
	if ok, _ := s.HyperdriveProxyReady(); ok {
		t.Fatal("readiness must fail before the first health probe")
	}
	s.proxyHealth = map[string]bool{"postgres|db:5432": false}
	s.proxyHealthChecked = true
	if ok, _ := s.HyperdriveProxyReady(); ok {
		t.Fatal("readiness must fail for an unreachable endpoint")
	}
	if s.hyperdriveProxyHostHealthy("postgres", "db:5432") {
		t.Fatal("binding must fail closed for an unhealthy endpoint")
	}
	s.proxyHealth["postgres|db:5432"] = true
	if ok, _ := s.HyperdriveProxyReady(); !ok {
		t.Fatal("readiness should recover after a successful probe")
	}
}

func TestHyperdriveBindingUsesHealthyProxy(t *testing.T) {
	s := &Server{Cfg: config.Config{HyperdriveMode: "proxy", HyperdriveProxy: "postgres://db:5432"}}
	s.proxyHealth = map[string]bool{"postgres|db:5432": true}
	s.proxyHealthChecked = true
	got, ok := s.hyperdriveBindingConnString("postgres://u:p@origin:5432/app", "acme", "HD")
	if !ok || got != "postgres://u:p@db:5432/app" {
		t.Fatalf("binding=%q ok=%v", got, ok)
	}
	s.proxyHealth["postgres|db:5432"] = false
	if _, ok := s.hyperdriveBindingConnString("postgres://u:p@origin:5432/app", "acme", "HD"); ok {
		t.Fatal("binding should fail closed when proxy is unhealthy")
	}
}

func TestHyperdriveProxyStatusSnapshot(t *testing.T) {
	s := &Server{Cfg: config.Config{HyperdriveMode: "proxy", HyperdriveProxy: "postgres://db:5432,mysql://db:3306"}}
	s.proxyHealth = map[string]bool{"postgres|db:5432": true, "mysql|db:3306": false}
	s.proxyHealthChecked = true
	got := s.HyperdriveProxyStatus()
	if got["ready"] != false || got["checked"] != true {
		t.Fatalf("status=%+v", got)
	}
	eps, _ := got["endpoints"].([]map[string]any)
	if len(eps) != 2 || eps[0]["healthy"] != true || eps[1]["healthy"] != false {
		t.Fatalf("endpoints=%+v", eps)
	}
}
