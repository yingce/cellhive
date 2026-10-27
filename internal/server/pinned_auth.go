package server

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cellhive/internal/cell"
	"cellhive/internal/scopedtoken"
	"cellhive/internal/telemetry"
)

func pinnedAuthKey(p scopedtoken.Claims) string {
	b, _ := json.Marshal(p)
	return p.Namespace + "|pinned|" + string(b)
}

func (s *Server) pinnedBindingAllowed(ctx context.Context, p scopedtoken.Claims, refresh bool) (bool, error) {
	key := pinnedAuthKey(p)
	if s.Cfg.BindingCacheTTL <= 0 {
		_, ok, err := s.bindingFromOwner(ctx, p.Namespace, p.Kind, p.Name, p)
		return ok, err
	}
	if !refresh {
		s.bindMu.Lock()
		e, found := s.bindCache[key]
		s.bindMu.Unlock()
		if found && time.Now().Before(e.exp) {
			return e.ok, nil
		}
	}
	ctx, span := telemetry.Tracer().Start(ctx, "cell.version_authorization_refresh")
	defer span.End()
	ok, _, err := s.refreshBinding(ctx, key, func() (string, bool, error) { return s.bindingFromOwner(ctx, p.Namespace, p.Kind, p.Name, p) })
	if err != nil || !ok {
		return ok, err
	}
	s.bindMu.Lock()
	e, found := s.bindCache[key]
	s.bindMu.Unlock()
	if !found || !time.Now().Before(e.exp) {
		return false, fmt.Errorf("version authorization expired during refresh")
	}
	return e.ok, nil
}

// Use the existing bounded warm scheduler; do not create a goroutine per token.
func (s *Server) notePinnedAuth(p scopedtoken.Claims) {
	if s.Cfg.BindingCacheTTL <= 0 {
		return
	}
	key := pinnedAuthKey(p)
	s.hotKVAccessMu.Lock()
	defer s.hotKVAccessMu.Unlock()
	now := time.Now()
	if e := s.hotKVAccesses[key]; e != nil {
		e.lastAccess = now
		return
	}
	if s.hotKVAccesses == nil {
		s.hotKVAccesses = make(map[string]*hotKVAccess)
	}
	if len(s.hotKVAccesses) >= hotKVAccessLimit {
		return
	} // ordinary fail-closed read-through at capacity
	sc := cell.Scope{Namespace: p.Namespace, Class: "__kv__", ID: p.Resource}
	if parsed, err := cell.ParseScope(p.Resource); err == nil {
		sc = parsed
	}
	s.hotKVAccesses[key] = &hotKVAccess{scope: sc, ns: p.Namespace, name: p.Name, lastAccess: now, next: now.Add(s.pinnedRefreshInterval()), active: true, pinned: &p}
}

func (s *Server) pinnedRefreshInterval() time.Duration {
	d := s.Cfg.BindingCacheTTL / 3
	if d > 500*time.Millisecond {
		d = 500 * time.Millisecond
	}
	if d < time.Millisecond {
		d = time.Millisecond
	}
	return d
}
