package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"cellhive/internal/control"
	"cellhive/internal/hyperdriveauth"
	"cellhive/internal/proxysql"
)

// Cloudflare-shaped Hyperdrive management API used by `wrangler hyperdrive`.
// CellHive stores the origin URL as an encrypted resource config; the returned
// id is the CellHive resource name (there is no separate UUID).

type cloudflareHyperdriveOrigin struct {
	Scheme   string `json:"scheme"`
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Database string `json:"database"`
	User     string `json:"user"`
	Password string `json:"password"`
}

type cloudflareHyperdriveConfig struct {
	Name    string                     `json:"name"`
	ID      string                     `json:"id,omitempty"`
	Origin  cloudflareHyperdriveOrigin `json:"origin"`
	Caching json.RawMessage            `json:"caching,omitempty"`
	MTLS    json.RawMessage            `json:"mtls,omitempty"`
}

func (o cloudflareHyperdriveOrigin) connectionString() (string, error) {
	scheme := strings.ToLower(o.Scheme)
	if scheme == "postgres" || scheme == "postgresql" {
		scheme = "postgres"
	} else if scheme != "mysql" {
		return "", fmt.Errorf("hyperdrive origin.scheme must be postgres, postgresql or mysql")
	}
	if o.Host == "" || o.Database == "" || o.User == "" {
		return "", fmt.Errorf("hyperdrive origin requires host, database and user")
	}
	port := o.Port
	if port == 0 {
		if scheme == "mysql" {
			port = 3306
		} else {
			port = 5432
		}
	}
	u := &url.URL{
		Scheme: scheme,
		Host:   net.JoinHostPort(o.Host, strconv.Itoa(port)),
		Path:   "/" + o.Database,
		User:   url.UserPassword(o.User, o.Password),
	}
	return u.String(), nil
}

func hyperdriveOriginResponse(origin string) (cloudflareHyperdriveOrigin, error) {
	u, err := url.Parse(origin)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return cloudflareHyperdriveOrigin{}, fmt.Errorf("invalid hyperdrive origin")
	}
	host, portStr, err := net.SplitHostPort(u.Host)
	if err != nil {
		host, portStr = u.Host, ""
	}
	port, _ := strconv.Atoi(portStr)
	database := strings.TrimPrefix(u.Path, "/")
	user := ""
	if u.User != nil {
		user = u.User.Username()
	}
	return cloudflareHyperdriveOrigin{
		Scheme: u.Scheme, Host: host, Port: port, Database: database, User: user,
	}, nil
}

func cloudflareHyperdriveResult(id, name string, origin cloudflareHyperdriveOrigin) map[string]any {
	return map[string]any{"id": id, "name": name, "origin": origin}
}

func (s *Server) hyperdriveConfigs(w http.ResponseWriter, r *http.Request) {
	ns := r.PathValue("account_id")
	if ns == "" {
		writeErr(w, http.StatusBadRequest, "invalid_request", "account_id is required")
		return
	}
	if s.forwardRead(w, r, control.Scope(), nil) {
		return
	}
	refs, err := s.Control.Resources(r.Context(), ns, "hyperdrive")
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "resources_failed", err.Error())
		return
	}
	out := make([]any, 0, len(refs))
	for _, ref := range refs {
		cfg, err := s.Control.ResourceConfig(r.Context(), ns, "hyperdrive", ref.Name)
		if err != nil {
			continue
		}
		origin, err := hyperdriveOriginResponse(string(cfg))
		if err != nil {
			continue
		}
		out = append(out, cloudflareHyperdriveResult(ref.Name, ref.Name, origin))
	}
	cloudflareResult(w, out)
}

func (s *Server) hyperdriveConfigGet(w http.ResponseWriter, r *http.Request) {
	ns, id := r.PathValue("account_id"), r.PathValue("config_id")
	if s.forwardRead(w, r, control.Scope(), nil) {
		return
	}
	cfg, err := s.Control.ResourceConfig(r.Context(), ns, "hyperdrive", id)
	if errors.Is(err, control.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "hyperdrive_not_found", "no Hyperdrive config named "+id)
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "resources_failed", err.Error())
		return
	}
	origin, err := hyperdriveOriginResponse(string(cfg))
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "invalid_origin", err.Error())
		return
	}
	cloudflareResult(w, cloudflareHyperdriveResult(id, id, origin))
}

func (s *Server) hyperdriveConfigCreate(w http.ResponseWriter, r *http.Request) {
	ns := r.PathValue("account_id")
	var in cloudflareHyperdriveConfig
	if s.forwardOrClaim(w, r, control.Scope(), nil) {
		return
	}
	if !decodeLimit(w, r, &in, 1<<20) {
		return
	}
	if strings.TrimSpace(in.Name) == "" || len(in.Caching) > 0 || len(in.MTLS) > 0 {
		writeErr(w, http.StatusBadRequest, "unsupported_hyperdrive_config", "name is required; caching/mtls are not supported")
		return
	}
	origin, err := in.Origin.connectionString()
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_origin", err.Error())
		return
	}
	res, err := s.createCloudflareHyperdrive(r, ns, in.Name, origin)
	if err != nil {
		s.writeHyperdriveError(w, err)
		return
	}
	originResp := in.Origin
	originResp.Password = ""
	cloudflareResult(w, cloudflareHyperdriveResult(res.Name, res.Name, originResp))
}

func (s *Server) hyperdriveConfigUpdate(w http.ResponseWriter, r *http.Request) {
	ns, id := r.PathValue("account_id"), r.PathValue("config_id")
	if s.forwardOrClaim(w, r, control.Scope(), nil) {
		return
	}
	var in cloudflareHyperdriveConfig
	if !decodeLimit(w, r, &in, 1<<20) {
		return
	}
	if len(in.Caching) > 0 || len(in.MTLS) > 0 {
		writeErr(w, http.StatusBadRequest, "unsupported_hyperdrive_config", "caching/mtls are not supported")
		return
	}
	if in.Name != "" && in.Name != id {
		writeErr(w, http.StatusBadRequest, "invalid_name", "renaming a Hyperdrive config is not supported")
		return
	}
	origin, err := in.Origin.connectionString()
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_origin", err.Error())
		return
	}
	oldCfg, err := s.Control.ResourceConfig(r.Context(), ns, "hyperdrive", id)
	if errors.Is(err, control.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "hyperdrive_not_found", "no Hyperdrive config named "+id)
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "resources_failed", err.Error())
		return
	}
	res, err := s.createCloudflareHyperdrive(r, ns, id, origin)
	if err != nil {
		s.writeHyperdriveError(w, err)
		return
	}
	if old := string(oldCfg); old != "" && old != origin && s.Cfg.HyperdriveManagedUsers && s.Cfg.ScopeSecret != "" {
		creds := hyperdriveauth.Derive(s.Cfg.ScopeSecret, ns, id)
		if err := hyperdriveauth.Deprovision(r.Context(), old, creds); err != nil && s.Log != nil {
			s.Log.Warn("hyperdrive old origin deprovision failed", "ns", ns, "resource", id, "err", err)
		}
	}
	originResp := in.Origin
	originResp.Password = ""
	cloudflareResult(w, cloudflareHyperdriveResult(res.Name, res.Name, originResp))
}

func (s *Server) createCloudflareHyperdrive(r *http.Request, ns, name, origin string) (control.Resource, error) {
	var creds hyperdriveauth.Credentials
	if s.Cfg.HyperdriveManagedUsers && s.Cfg.ScopeSecret != "" {
		creds = hyperdriveauth.Derive(s.Cfg.ScopeSecret, ns, name)
		if err := hyperdriveauth.Provision(r.Context(), origin, creds); err != nil {
			return control.Resource{}, fmt.Errorf("hyperdrive_credentials_failed: %w", err)
		}
	}
	var res control.Resource
	err := s.capturedWrite(r.Context(), control.ScopeFor(ns), func() error {
		var e error
		res, e = s.Control.CreateResourceWithConfig(r.Context(), ns, "hyperdrive", name,
			defaultResourceScope(ns, "hyperdrive", name), []byte(origin), "wrangler")
		return e
	})
	if err != nil {
		if creds.User != "" {
			_ = hyperdriveauth.Deprovision(r.Context(), origin, creds)
		}
		return control.Resource{}, err
	}
	s.invalidateBindings(ns)
	if s.ProxySQL != nil {
		_ = s.ProxySQL.SyncOrigin(r.Context(), proxysql.Origin{
			Namespace: ns, Name: name, URL: origin, ClientUser: creds.User, ClientPassword: creds.Password,
		})
	}
	return res, nil
}

func (s *Server) hyperdriveConfigDelete(w http.ResponseWriter, r *http.Request) {
	ns, id := r.PathValue("account_id"), r.PathValue("config_id")
	cfg, err := s.Control.ResourceConfig(r.Context(), ns, "hyperdrive", id)
	if errors.Is(err, control.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "hyperdrive_not_found", "no Hyperdrive config named "+id)
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "resources_failed", err.Error())
		return
	}
	scope, ok, err := s.Control.Binding(r.Context(), ns, "hyperdrive", id)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "resource_lookup_failed", err.Error())
		return
	}
	if !ok {
		writeErr(w, http.StatusNotFound, "hyperdrive_not_found", "no Hyperdrive config named "+id)
		return
	}
	refs, err := s.Control.ResourceReferencedBy(r.Context(), ns, "hyperdrive", id, scope)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "resource_lookup_failed", err.Error())
		return
	}
	if len(refs) > 0 {
		writeJSON(w, http.StatusConflict, map[string]any{"error": "resource_in_use", "referenced_by": refs})
		return
	}
	if s.forwardOrClaim(w, r, control.Scope(), nil) {
		return
	}
	if err := s.capturedWrite(r.Context(), control.ScopeFor(ns), func() error {
		return s.Control.DeleteResource(r.Context(), ns, "hyperdrive", id, "wrangler")
	}); err != nil {
		writeErr(w, http.StatusInternalServerError, "resource_delete_failed", err.Error())
		return
	}
	s.invalidateBindings(ns)
	if s.ProxySQL != nil {
		creds := hyperdriveauth.Credentials{}
		if s.Cfg.HyperdriveManagedUsers && s.Cfg.ScopeSecret != "" {
			creds = hyperdriveauth.Derive(s.Cfg.ScopeSecret, ns, id)
		}
		_ = s.ProxySQL.Remove(r.Context(), ns, id, string(cfg), creds.User)
	}
	cloudflareResult(w, map[string]any{"id": id})
}

func (s *Server) writeHyperdriveError(w http.ResponseWriter, err error) {
	status := http.StatusBadRequest
	if strings.HasPrefix(err.Error(), "hyperdrive_credentials_failed:") {
		status = http.StatusBadGateway
	}
	writeErr(w, status, "hyperdrive_failed", err.Error())
}
