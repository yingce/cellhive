package server

import (
	"strings"
	"testing"

	"cellhive/internal/config"
)

func TestHyperdriveBindingConnStringModes(t *testing.T) {
	origin := "postgres://app:secret@db.internal:5432/prod?sslmode=require"
	direct := &Server{Cfg: config.Config{HyperdriveMode: "direct"}}
	if got, ok := direct.hyperdriveBindingConnString(origin, "acme", "hd1"); !ok || got != origin {
		t.Fatalf("direct = %q, %v", got, ok)
	}
	proxy := &Server{Cfg: config.Config{HyperdriveMode: "proxy", HyperdriveProxy: "pooler:6432", HyperdriveDBMode: "origin"}}
	got, ok := proxy.hyperdriveBindingConnString(origin, "acme", "hd1")
	if !ok || got != "postgres://app:secret@pooler:6432/prod?sslmode=require" {
		t.Fatalf("proxy = %q, %v", got, ok)
	}
	proxy.Cfg.HyperdriveProxy = "postgres://pooler.svc:6432"
	if got, ok = proxy.hyperdriveBindingConnString(origin, "acme", "hd1"); !ok || got != "postgres://app:secret@pooler.svc:6432/prod?sslmode=require" {
		t.Fatalf("proxy with scheme = %q, %v", got, ok)
	}
	proxy.Cfg.HyperdriveProxy = "pooler:6432"
	proxy.Cfg.HyperdriveDBMode = "binding"
	if got, ok = proxy.hyperdriveBindingConnString(origin, "acme", "HD-1"); !ok || got != "postgres://app:secret@pooler:6432/hd_hd-1?sslmode=require" {
		t.Fatalf("logical db = %q, %v", got, ok)
	}
	proxy.Cfg.HyperdriveProxy = ""
	if _, ok = proxy.hyperdriveBindingConnString(origin, "acme", "hd1"); ok {
		t.Fatal("proxy mode without endpoint must fail closed")
	}
	mysql := "mysql://app:secret@mysql.internal:3306/app?ssl-mode=required"
	if got, ok := direct.hyperdriveBindingConnString(mysql, "acme", "hd1"); !ok || got != mysql {
		t.Fatalf("direct mysql = %q, %v", got, ok)
	}
	my := &Server{Cfg: config.Config{HyperdriveMode: "proxy", HyperdriveProxy: "mysql://proxysql:6033", HyperdriveDBMode: "binding"}}
	if got, ok := my.hyperdriveBindingConnString(mysql, "acme", "HD-1"); !ok || got != "mysql://app:secret@proxysql:6033/app?ssl-mode=required" {
		t.Fatalf("mysql proxy = %q, %v", got, ok)
	}
	my.Cfg.HyperdriveProxy = "pooler:6432"
	if _, ok := my.hyperdriveBindingConnString(mysql, "acme", "hd1"); ok {
		t.Fatal("mysql proxy mode without mysql endpoint must fail closed")
	}
	mixed := &Server{Cfg: config.Config{HyperdriveMode: "proxy", HyperdriveProxy: "postgres://pooler:6432,mysql://proxysql:6033"}}
	if got, ok := mixed.hyperdriveBindingConnString(mysql, "acme", "hd1"); !ok || got != "mysql://app:secret@proxysql:6033/app?ssl-mode=required" {
		t.Fatalf("mixed mysql = %q, %v", got, ok)
	}
	if got, ok := mixed.hyperdriveBindingConnString(origin, "acme", "hd1"); !ok || got != "postgres://app:secret@pooler:6432/prod?sslmode=require" {
		t.Fatalf("mixed postgres = %q, %v", got, ok)
	}
	managed := &Server{Cfg: config.Config{HyperdriveMode: "direct", HyperdriveManagedUsers: true, ScopeSecret: "scope-secret"}}
	got, ok = managed.hyperdriveBindingConnString(origin, "acme", "hd1")
	if !ok || !strings.Contains(got, "hd_") || strings.Contains(got, ":secret@") {
		t.Fatalf("managed direct = %q, %v", got, ok)
	}
}
