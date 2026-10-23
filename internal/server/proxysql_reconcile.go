package server

import (
	"context"
	"time"

	"cellhive/internal/control"
	"cellhive/internal/hyperdriveauth"
	"cellhive/internal/proxysql"
)

// RunProxySQLReconcileLoop periodically re-applies every registered Hyperdrive
// origin to the configured ProxySQL admin endpoint(s). It covers restarts and
// missed create/delete hooks; only the control owner runs it.
func (s *Server) RunProxySQLReconcileLoop(ctx context.Context) {
	if s.ProxySQL == nil {
		return
	}
	if err := s.ReconcileProxySQL(ctx); err != nil && s.Log != nil {
		s.Log.Warn("proxysql reconcile failed", "err", err)
	}
	tick := time.NewTicker(30 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			if err := s.ReconcileProxySQL(ctx); err != nil && s.Log != nil {
				s.Log.Warn("proxysql reconcile failed", "err", err)
			}
		}
	}
}

// ReconcileProxySQL upserts all registered Hyperdrive resources. It does not
// delete unknown ProxySQL rows: hostgroups may be owned by another controller.
func (s *Server) ReconcileProxySQL(ctx context.Context) error {
	if s.ProxySQL == nil || s.Control == nil {
		return nil
	}
	if s.Owner != nil {
		o, _, err := s.Owner.Resolve(ctx, control.Scope())
		if err != nil || !s.ownerLive(ctx, o, time.Now()) || o.Node != s.Cfg.NodeID {
			return nil
		}
	}
	refs, err := s.Control.ResourcesByKind(ctx, "hyperdrive")
	if err != nil {
		return err
	}
	for _, ref := range refs {
		cfg, err := s.Control.ResourceConfig(ctx, ref.Namespace, "hyperdrive", ref.Name)
		if err != nil || len(cfg) == 0 {
			continue
		}
		var generated hyperdriveauth.Credentials
		if s.Cfg.HyperdriveManagedUsers && s.Cfg.ScopeSecret != "" {
			generated = hyperdriveauth.Derive(s.Cfg.ScopeSecret, ref.Namespace, ref.Name)
			if err := hyperdriveauth.Provision(ctx, string(cfg), generated); err != nil {
				return err
			}
		}
		if err := s.ProxySQL.SyncOrigin(ctx, proxysql.Origin{
			Namespace: ref.Namespace, Name: ref.Name, URL: string(cfg),
			ClientUser: generated.User, ClientPassword: generated.Password,
		}); err != nil {
			return err
		}
	}
	return nil
}
