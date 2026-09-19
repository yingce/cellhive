package control

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"
)

var (
	// ErrHostUnknown means routes were set for a host that was never registered.
	ErrHostUnknown = errors.New("control: host not registered")
	// ErrHostTaken means the host belongs to another namespace.
	ErrHostTaken = errors.New("control: host already owned by another namespace")
	// ErrHostUnverified means a custom host is still pending verification.
	ErrHostUnverified = errors.New("control: host not verified")
	// ErrHostReserved means the host is in the platform's built-in domain space.
	ErrHostReserved = errors.New("control: host is reserved for built-in domains")
)

// Host is a domain registration (ADR-131/133). kind=builtin rows are generated
// for <ns>-<worker>.cell.internal and are never user-writable; registering a
// custom host is the authorization to route it (no DNS challenge).
type Host struct {
	Host      string `json:"host"`
	Namespace string `json:"ns"`
	Worker    string `json:"worker,omitempty"` // builtin: the worker it addresses
	Kind      string `json:"kind"`             // builtin|custom (registration is authorization, ADR-133)
	CreatedMs int64  `json:"created_ms"`
}

// Purge is a delete job: worker="" means the whole namespace (ADR-131).
type Purge struct {
	Namespace   string `json:"ns"`
	Worker      string `json:"worker,omitempty"`
	State       string `json:"state"`
	RequestedMs int64  `json:"requested_ms"`
	UpdatedMs   int64  `json:"updated_ms"`
	Attempts    int    `json:"attempts"`
	LastError   string `json:"last_error,omitempty"`
}

// --- hosts ------------------------------------------------------------------

// PutCustomHost registers a custom domain for a namespace. Registration is
// authorization (ADR-133): the host routes immediately. A host belongs to one
// namespace; a claim by another namespace is ErrHostTaken, and the built-in
// domain space is never user-writable.
func (s *Store) PutCustomHost(ctx context.Context, ns, host, actor string) (Host, error) {
	if err := validateNS(ns); err != nil {
		return Host{}, err
	}
	host = NormalizeHost(host)
	if host == "" || strings.ContainsAny(host, "/\\ ") {
		return Host{}, fmt.Errorf("control: invalid host %q", host)
	}
	c, err := s.cell(ctx)
	if err != nil {
		return Host{}, err
	}
	now := s.now().UnixMilli()
	h := Host{Host: host, Namespace: ns, Kind: "custom", CreatedMs: now}
	err = tx(ctx, c, func(t *sql.Tx) error {
		var existingNS, kind string
		err := t.QueryRowContext(ctx, `SELECT ns, kind FROM hosts WHERE host=?`, host).Scan(&existingNS, &kind)
		switch {
		case err == nil:
			if kind == "builtin" {
				return ErrHostReserved
			}
			if existingNS != ns {
				return ErrHostTaken
			}
			return nil // idempotent for the owning namespace
		case errors.Is(err, sql.ErrNoRows):
			if _, err := t.ExecContext(ctx,
				`INSERT INTO hosts(host, ns, kind, created_ms) VALUES (?,?,?,?)`,
				host, ns, "custom", now); err != nil {
				return err
			}
		default:
			return err
		}
		return txAudit(ctx, t, ns, actor, "domain.add", ns+"/"+host)
	})
	return h, err
}

// GetHost returns one domain registration.
func (s *Store) GetHost(ctx context.Context, host string) (Host, error) {
	c, err := s.cell(ctx)
	if err != nil {
		return Host{}, err
	}
	return scanHost(c.DB.QueryRowContext(ctx,
		`SELECT host, ns, worker, kind, created_ms FROM hosts WHERE host=?`, NormalizeHost(host)))
}

// Hosts lists a namespace's domains.
func (s *Store) Hosts(ctx context.Context, ns string) ([]Host, error) {
	if err := validateNS(ns); err != nil {
		return nil, err
	}
	c, err := s.cell(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := c.DB.QueryContext(ctx,
		`SELECT host, ns, worker, kind, created_ms FROM hosts WHERE ns=? ORDER BY host`, ns)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Host
	for rows.Next() {
		h, err := scanHost(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, h)
	}
	return out, rows.Err()
}

// DeleteHost removes a custom domain (built-in hosts are system managed).
func (s *Store) DeleteHost(ctx context.Context, ns, host, actor string) error {
	if err := validateNS(ns); err != nil {
		return err
	}
	host = NormalizeHost(host)
	c, err := s.cell(ctx)
	if err != nil {
		return err
	}
	return tx(ctx, c, func(t *sql.Tx) error {
		var hostNS, kind string
		err := t.QueryRowContext(ctx, `SELECT ns, kind FROM hosts WHERE host=?`, host).Scan(&hostNS, &kind)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			return ErrNotFound
		case err != nil:
			return err
		case hostNS != ns:
			return ErrHostTaken
		case kind == "builtin":
			return ErrHostReserved
		}
		if _, err := t.ExecContext(ctx, `DELETE FROM routes WHERE host=?`, host); err != nil {
			return err
		}
		if _, err := t.ExecContext(ctx, `DELETE FROM hosts WHERE host=?`, host); err != nil {
			return err
		}
		return txAudit(ctx, t, ns, actor, "domain.delete", ns+"/"+host)
	})
}

// EnsureBuiltinHost materializes the built-in route for <ns>-<worker>.<base>
// (caller supplies the full host because the base domain is configuration).
func (s *Store) EnsureBuiltinHost(ctx context.Context, ns, worker, host, actor string) error {
	if err := validateNS(ns); err != nil {
		return err
	}
	host = NormalizeHost(host)
	if host == "" || worker == "" {
		return fmt.Errorf("control: builtin host and worker are required")
	}
	c, err := s.cell(ctx)
	if err != nil {
		return err
	}
	now := s.now().UnixMilli()
	return tx(ctx, c, func(t *sql.Tx) error {
		var hostNS string
		err := t.QueryRowContext(ctx, `SELECT ns FROM hosts WHERE host=?`, host).Scan(&hostNS)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			if _, err := t.ExecContext(ctx,
				`INSERT INTO hosts(host, ns, worker, kind, created_ms) VALUES (?,?,?,?,?)`,
				host, ns, worker, "builtin", now); err != nil {
				return err
			}
		case err != nil:
			return err
		case hostNS != ns:
			return ErrHostTaken
		default:
			if _, err := t.ExecContext(ctx, `UPDATE hosts SET worker=? WHERE host=?`, worker, host); err != nil {
				return err
			}
		}
		res, err := t.ExecContext(ctx,
			`INSERT INTO routes(host, path, ns, worker) VALUES (?,?,?,?)
			 ON CONFLICT(host, path) DO UPDATE SET worker=excluded.worker WHERE routes.ns=excluded.ns`,
			host, "", ns, worker)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n == 0 {
			return ErrHostTaken
		}
		if actor != "" {
			return txAudit(ctx, t, ns, actor, "domain.builtin", ns+"/"+worker)
		}
		return nil
	})
}

// SetHostLabel pins a worker's built-in domain label (unique across workers).
func (s *Store) SetHostLabel(ctx context.Context, ns, worker, label, actor string) error {
	if err := validateNS(ns); err != nil {
		return err
	}
	c, err := s.cell(ctx)
	if err != nil {
		return err
	}
	return tx(ctx, c, func(t *sql.Tx) error {
		if _, err := t.ExecContext(ctx, `UPDATE workers SET host_label=? WHERE ns=? AND name=?`,
			label, ns, worker); err != nil {
			if strings.Contains(err.Error(), "UNIQUE") {
				return ErrHostTaken
			}
			return err
		}
		return txAudit(ctx, t, ns, actor, "worker.host_label", ns+"/"+worker+"="+label)
	})
}

func scanHost(row interface{ Scan(...any) error }) (Host, error) {
	var h Host
	err := row.Scan(&h.Host, &h.Namespace, &h.Worker, &h.Kind, &h.CreatedMs)
	if errors.Is(err, sql.ErrNoRows) {
		return Host{}, ErrNotFound
	}
	return h, err
}

// --- purge jobs -------------------------------------------------------------

func enqueuePurgeTx(ctx context.Context, t *sql.Tx, ns, worker string, now int64) error {
	_, err := t.ExecContext(ctx,
		`INSERT INTO purges(ns, worker, state, requested_ms, updated_ms, attempts, last_error)
		 VALUES (?,?, 'pending', ?, ?, 0, '')
		 ON CONFLICT(ns, worker) DO UPDATE SET state='pending', requested_ms=excluded.requested_ms,
		   updated_ms=excluded.updated_ms, last_error=''`,
		ns, worker, now, now)
	return err
}

// PendingPurges lists jobs to run (pending or failed with attempts left).
func (s *Store) PendingPurges(ctx context.Context, limit int) ([]Purge, error) {
	if limit <= 0 {
		limit = 20
	}
	c, err := s.cell(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := c.DB.QueryContext(ctx,
		`SELECT ns, worker, state, requested_ms, updated_ms, attempts, last_error FROM purges
		 WHERE state IN ('pending','failed') AND attempts < 10 ORDER BY requested_ms LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Purge
	for rows.Next() {
		var p Purge
		if err := rows.Scan(&p.Namespace, &p.Worker, &p.State, &p.RequestedMs, &p.UpdatedMs, &p.Attempts, &p.LastError); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// FinishPurge removes a completed job and audits it.
func (s *Store) FinishPurge(ctx context.Context, ns, worker string) error {
	c, err := s.cell(ctx)
	if err != nil {
		return err
	}
	return tx(ctx, c, func(t *sql.Tx) error {
		if _, err := t.ExecContext(ctx, `DELETE FROM purges WHERE ns=? AND worker=?`, ns, worker); err != nil {
			return err
		}
		return txAudit(ctx, t, ns, "purge", "purge.done", ns+"/"+worker)
	})
}

// FailPurge records a failed attempt so the loop retries later.
func (s *Store) FailPurge(ctx context.Context, ns, worker, msg string) error {
	c, err := s.cell(ctx)
	if err != nil {
		return err
	}
	_, err = c.DB.ExecContext(ctx,
		`UPDATE purges SET state='failed', attempts=attempts+1, updated_ms=?, last_error=? WHERE ns=? AND worker=?`,
		s.now().UnixMilli(), msg, ns, worker)
	return err
}

// PurgeHook cleans the data side of a purge job. done=false means work remains
// (the job stays pending and is retried); a non-nil error is recorded against
// the job and retried with backoff semantics (attempts).
type PurgeHook func(ctx context.Context, ns, worker string) (done bool, err error)

// RunPurgeLoop drains purge jobs until ctx is done. hook (optional) lets the
// host drop data cells and blobs before the control rows go away.
func (s *Store) RunPurgeLoop(ctx context.Context, log *slog.Logger, interval time.Duration, hook PurgeHook) {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	if log == nil {
		log = slog.Default()
	}
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		jobs, err := s.PendingPurges(ctx, 20)
		if err != nil {
			log.Warn("purge: list", "err", err)
			continue
		}
		for _, j := range jobs {
			if ctx.Err() != nil {
				return
			}
			if hook != nil {
				done, herr := hook(ctx, j.Namespace, j.Worker)
				if herr != nil {
					_ = s.FailPurge(ctx, j.Namespace, j.Worker, herr.Error())
					log.Warn("purge: hook", "ns", j.Namespace, "worker", j.Worker, "err", herr)
					continue
				}
				if !done {
					// Partial pass (bounded deletions): leave the job pending and
					// pick it up again next tick, before deleting the control rows.
					continue
				}
			}
			if j.Worker == "" {
				err = s.PurgeAppRows(ctx, j.Namespace)
			} else {
				err = s.PurgeWorkerRows(ctx, j.Namespace, j.Worker)
			}
			if err != nil {
				_ = s.FailPurge(ctx, j.Namespace, j.Worker, err.Error())
				log.Warn("purge: rows", "ns", j.Namespace, "worker", j.Worker, "err", err)
				continue
			}
			if err := s.FinishPurge(ctx, j.Namespace, j.Worker); err != nil {
				log.Warn("purge: finish", "ns", j.Namespace, "worker", j.Worker, "err", err)
			}
		}
	}
}

// --- normalized bindings ----------------------------------------------------

// syncBindingsTx rewrites the derived bindings rows for one version.
func syncBindingsTx(ctx context.Context, t *sql.Tx, ns, worker string, number int, bs []Binding) error {
	if _, err := t.ExecContext(ctx, `DELETE FROM bindings WHERE ns=? AND worker=? AND number=?`, ns, worker, number); err != nil {
		return err
	}
	for _, b := range bs {
		if b.Name == "" || b.Type == "" {
			continue
		}
		pin := ""
		if b.Type == "service" {
			pin = b.Version
		}
		if _, err := t.ExecContext(ctx,
			`INSERT OR REPLACE INTO bindings(ns, worker, number, name, type, id, class_name, entrypoint, pin_sha)
			 VALUES (?,?,?,?,?,?,?,?,?)`,
			ns, worker, number, b.Name, b.Type, b.ID, b.ClassName, b.Entrypoint, pin); err != nil {
			return err
		}
	}
	return nil
}

// BindingsFor returns one version's normalized bindings (empty when the derived
// table has not been populated for a pre-ADR-131 version).
func (s *Store) BindingsFor(ctx context.Context, ns, worker string, number int) ([]Binding, error) {
	c, err := s.cell(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := c.DB.QueryContext(ctx,
		`SELECT name, type, id, class_name, entrypoint, pin_sha FROM bindings
		 WHERE ns=? AND worker=? AND number=? ORDER BY name`, ns, worker, number)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Binding
	for rows.Next() {
		var b Binding
		var pin string
		if err := rows.Scan(&b.Name, &b.Type, &b.ID, &b.ClassName, &b.Entrypoint, &pin); err != nil {
			return nil, err
		}
		b.Version = pin
		out = append(out, b)
	}
	return out, rows.Err()
}

// RoutableHosts returns every registered host (ADR-133: registration is
// authorization). Useful for edge allowlists / certificate policy.
func (s *Store) RoutableHosts(ctx context.Context) (map[string]bool, error) {
	c, err := s.cell(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := c.DB.QueryContext(ctx,
		`SELECT host FROM hosts`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var h string
		if err := rows.Scan(&h); err != nil {
			return nil, err
		}
		out[h] = true
	}
	return out, rows.Err()
}

// --- service binding ACL (ADR-144) ------------------------------------------

// ServiceACL grants callers in CallerNS permission to bind to Ns/Worker.
type ServiceACL struct {
	Namespace string `json:"namespace"`
	Worker    string `json:"worker"`
	CallerNS  string `json:"caller_ns"`
	CreatedMs int64  `json:"created_ms"`
}

// PutServiceACL grants (ns, worker) to callers from callerNS (idempotent).
func (s *Store) PutServiceACL(ctx context.Context, ns, worker, callerNS, actor string) error {
	if err := validateNS(ns); err != nil {
		return err
	}
	if err := validateNS(callerNS); err != nil {
		return fmt.Errorf("control: invalid caller namespace: %w", err)
	}
	if worker == "" || strings.ContainsAny(worker, "/\\") {
		return fmt.Errorf("control: invalid worker %q", worker)
	}
	c, err := s.cell(ctx)
	if err != nil {
		return err
	}
	now := s.now().UnixMilli()
	return tx(ctx, c, func(t *sql.Tx) error {
		if _, err := t.ExecContext(ctx,
			`INSERT INTO service_acls(ns, worker, caller_ns, created_ms) VALUES (?,?,?,?)
			 ON CONFLICT(ns, worker, caller_ns) DO NOTHING`,
			ns, worker, callerNS, now); err != nil {
			return err
		}
		return txAudit(ctx, t, ns, actor, "service_acl.put", ns+"/"+worker+"<-"+callerNS)
	})
}

// DeleteServiceACL revokes the grant (missing is not an error).
func (s *Store) DeleteServiceACL(ctx context.Context, ns, worker, callerNS, actor string) error {
	if err := validateNS(ns); err != nil {
		return err
	}
	c, err := s.cell(ctx)
	if err != nil {
		return err
	}
	return tx(ctx, c, func(t *sql.Tx) error {
		if _, err := t.ExecContext(ctx,
			`DELETE FROM service_acls WHERE ns=? AND worker=? AND caller_ns=?`,
			ns, worker, callerNS); err != nil {
			return err
		}
		return txAudit(ctx, t, ns, actor, "service_acl.delete", ns+"/"+worker+"<-"+callerNS)
	})
}

// ListServiceACLs returns the allowlist of a namespace.
func (s *Store) ListServiceACLs(ctx context.Context, ns string) ([]ServiceACL, error) {
	if err := validateNS(ns); err != nil {
		return nil, err
	}
	c, err := s.cell(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := c.DB.QueryContext(ctx,
		`SELECT ns, worker, caller_ns, created_ms FROM service_acls WHERE ns=? ORDER BY worker, caller_ns`, ns)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ServiceACL
	for rows.Next() {
		var a ServiceACL
		if err := rows.Scan(&a.Namespace, &a.Worker, &a.CallerNS, &a.CreatedMs); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// HasServiceACL reports whether callerNS may bind to ns/worker.
func (s *Store) HasServiceACL(ctx context.Context, ns, worker, callerNS string) (bool, error) {
	if ns == "" || worker == "" || callerNS == "" {
		return false, nil
	}
	c, err := s.cell(ctx)
	if err != nil {
		return false, err
	}
	var one int
	err = c.DB.QueryRowContext(ctx,
		`SELECT 1 FROM service_acls WHERE ns=? AND worker=? AND caller_ns=? LIMIT 1`,
		ns, worker, callerNS).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}
