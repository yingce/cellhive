package server

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cellhive/internal/scopedtoken"
	"cellhive/internal/telemetry"
)

func pinnedAuthKey(p scopedtoken.Claims) string {
	// Expiry is verified per request, not by the declaration lookup.
	p.ExpiresMs = 0
	b, _ := json.Marshal(p)
	return p.Namespace + "|pinned|" + string(b)
}

func (s *Server) pinnedBindingAllowed(ctx context.Context, p scopedtoken.Claims, refresh bool) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
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
