package control

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// snapshotCurrentSecretsTx captures only ciphertext. No plaintext enters a
// version row, a bundle, or the deployment event.
func snapshotCurrentSecretsTx(ctx context.Context, t *sql.Tx, ns, worker string, number int) error {
	_, err := t.ExecContext(ctx, `INSERT INTO version_secrets
		(ns,worker,number,key,wrapped_dek,ciphertext,updated_ms)
		SELECT ns,worker,?,key,wrapped_dek,ciphertext,updated_ms
		FROM secrets WHERE ns=? AND worker=?`, number, ns, worker)
	return err
}

// restoreVersionSecretsTx makes the selected version the baseline for the next
// secret mutation or ordinary deploy. The target version remains immutable.
func restoreVersionSecretsTx(ctx context.Context, t *sql.Tx, ns, worker string, number int) error {
	if _, err := t.ExecContext(ctx, `DELETE FROM secrets WHERE ns=? AND worker=?`, ns, worker); err != nil {
		return err
	}
	_, err := t.ExecContext(ctx, `INSERT INTO secrets
		(ns,worker,key,wrapped_dek,value_nonce,ciphertext,updated_ms)
		SELECT ns,worker,key,wrapped_dek,NULL,ciphertext,updated_ms
		FROM version_secrets WHERE ns=? AND worker=? AND number=?`, ns, worker, number)
	return err
}

// publishSecretChangeTx copies the active code/configuration into a new version
// after the current secret set has changed. All records and the active pointer
// commit together in the caller's control-cell transaction.
func (s *Store) publishSecretChangeTx(ctx context.Context, t *sql.Tx, ns, worker, actor string) error {
	var active int
	err := t.QueryRowContext(ctx, `SELECT active FROM workers WHERE ns=? AND name=? AND deleted_ms=0`, ns, worker).Scan(&active)
	if errors.Is(err, sql.ErrNoRows) || active == 0 {
		return nil // pre-deploy secret: first deploy captures it
	}
	if err != nil {
		return err
	}
	var next int
	if err := t.QueryRowContext(ctx, `SELECT COALESCE(MAX(number),0)+1 FROM versions WHERE ns=? AND worker=?`, ns, worker).Scan(&next); err != nil {
		return err
	}
	now := s.now().UnixMilli()
	res, err := t.ExecContext(ctx, `INSERT INTO versions
		(ns,worker,number,bundle_sha,assets_sha,storage_id,session_policy,
		 bindings,vars,consumers,crons,assets,compat_date,compat_flags,limits,log_policy,
		 created_ms,actor,secret_snapshot)
		SELECT ns,worker,?,bundle_sha,assets_sha,storage_id,session_policy,
		 bindings,vars,consumers,crons,assets,compat_date,compat_flags,limits,log_policy,
		 ?,?,1 FROM versions WHERE ns=? AND worker=? AND number=?`,
		next, now, actor, ns, worker, active)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return ErrNoVersion
	}
	if err := snapshotCurrentSecretsTx(ctx, t, ns, worker, next); err != nil {
		return err
	}
	if _, err := t.ExecContext(ctx, `INSERT INTO version_bundle_refs(ns,worker,number,sha)
		SELECT ns,worker,?,sha FROM version_bundle_refs WHERE ns=? AND worker=? AND number=?`, next, ns, worker, active); err != nil {
		return err
	}
	if _, err := t.ExecContext(ctx, `INSERT INTO bindings
		(ns,worker,number,name,type,id,class_name,entrypoint,pin_sha)
		SELECT ns,worker,?,name,type,id,class_name,entrypoint,pin_sha
		FROM bindings WHERE ns=? AND worker=? AND number=?`, next, ns, worker, active); err != nil {
		return err
	}
	if _, err := t.ExecContext(ctx, `UPDATE workers SET active=?, previous=? WHERE ns=? AND name=?`, next, active, ns, worker); err != nil {
		return err
	}
	if err := appendDeploymentEvent(ctx, t, ns, worker, next, actor, now); err != nil {
		return err
	}
	return txAudit(ctx, t, ns, actor, "secret.deploy", fmt.Sprintf("%s/%s@%d", ns, worker, next))
}

// VersionSecrets decrypts exactly the secrets captured for one version.
func (s *Store) VersionSecrets(ctx context.Context, ns, worker string, number int) (map[string]string, error) {
	c, err := s.cell(ctx)
	if err != nil {
		return nil, err
	}
	if number <= 0 {
		if err := c.DB.QueryRowContext(ctx, `SELECT active FROM workers WHERE ns=? AND name=?`, ns, worker).Scan(&number); err != nil {
			return nil, err
		}
	}
	if s.env == nil {
		var hasSecret int
		if err := c.DB.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM version_secrets WHERE ns=? AND worker=? AND number=?)`, ns, worker, number).Scan(&hasSecret); err != nil {
			return nil, err
		}
		if hasSecret != 0 {
			return nil, ErrNoEnvelope
		}
		return map[string]string{}, nil
	}
	rows, err := c.DB.QueryContext(ctx, `SELECT key,wrapped_dek,ciphertext FROM version_secrets
		WHERE ns=? AND worker=? AND number=? ORDER BY key`, ns, worker, number)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var key string
		var wrapped, ciphertext []byte
		if err := rows.Scan(&key, &wrapped, &ciphertext); err != nil {
			return nil, err
		}
		plain, err := s.env.Open(wrapped, ciphertext)
		if err != nil {
			return nil, err
		}
		out[key] = string(plain)
	}
	return out, rows.Err()
}
