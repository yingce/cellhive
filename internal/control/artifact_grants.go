package control

import (
	"context"
	"database/sql"
	"fmt"
)

func validateBundleGrant(ns, sha string) error {
	if err := validateNS(ns); err != nil {
		return err
	}
	if len(sha) != 64 {
		return fmt.Errorf("control: invalid bundle SHA")
	}
	for _, c := range sha {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return fmt.Errorf("control: invalid bundle SHA")
		}
	}
	return nil
}

// GrantBundle authorizes a namespace to deploy uploaded content without changing
// global blob deduplication. The caller must own and capture the control cell.
func (s *Store) GrantBundle(ctx context.Context, ns, sha, actor string) error {
	if err := validateBundleGrant(ns, sha); err != nil {
		return err
	}
	c, err := s.cell(ctx)
	if err != nil {
		return err
	}
	return tx(ctx, c, func(t *sql.Tx) error {
		if _, err := t.ExecContext(ctx, `INSERT INTO bundle_grants(ns,sha,created_ms) VALUES (?,?,?) ON CONFLICT(ns,sha) DO NOTHING`, ns, sha, s.now().UnixMilli()); err != nil {
			return err
		}
		return txAudit(ctx, t, ns, actor, "bundle.grant", sha)
	})
}

// HasBundleGrant requires an authoritative control-cell read by the caller.
func (s *Store) HasBundleGrant(ctx context.Context, ns, sha string) (bool, error) {
	if err := validateBundleGrant(ns, sha); err != nil {
		return false, err
	}
	c, err := s.cell(ctx)
	if err != nil {
		return false, err
	}
	var granted bool
	err = c.DB.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM bundle_grants WHERE ns=? AND sha=?)`, ns, sha).Scan(&granted)
	return granted, err
}
