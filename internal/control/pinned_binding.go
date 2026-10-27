package control

import (
	"context"
	"database/sql"
	"errors"
)

// PinnedBindingAllowed checks dynamic revocation separately from the immutable
// version mapping. One SQL statement observes a consistent control snapshot.
func (s *Store) PinnedBindingAllowed(ctx context.Context, ns, worker string, version int, name, resource string) (bool, error) {
	c, err := s.cell(ctx)
	if err != nil {
		return false, err
	}
	var raw string
	err = c.DB.QueryRowContext(ctx, `SELECT v.bindings FROM versions v
 JOIN workers w ON w.ns=v.ns AND w.name=v.worker
 JOIN apps a ON a.ns=v.ns
 WHERE v.ns=? AND v.worker=? AND v.number=? AND w.deleted_ms=0 AND a.deleted_ms=0
 AND NOT EXISTS (SELECT 1 FROM revoked_resources r WHERE r.ns=v.ns AND r.kind='kv' AND r.name=?)`, ns, worker, version, name).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var bindings []Binding
	if err := unmarshal([]byte(raw), &bindings); err != nil {
		return false, err
	}
	for _, b := range bindings {
		if b.Type == "kv" && b.Name == name && b.ID == resource {
			return true, nil
		}
	}
	return false, nil
}
