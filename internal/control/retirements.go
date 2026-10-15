package control

import (
	"context"
	"database/sql"
	"errors"
)

func (s *Store) ResourceRetirement(ctx context.Context, scope string) (ResourceRetirement, error) {
	c, err := s.cell(ctx)
	if err != nil {
		return ResourceRetirement{}, err
	}
	var r ResourceRetirement
	err = c.DB.QueryRowContext(ctx, `SELECT ns,data_scope,job_id,epoch_floor,gc_cursor,last_error,updated_ms FROM resource_retirements WHERE data_scope=?`, scope).Scan(&r.Namespace, &r.DataScope, &r.JobID, &r.Epoch, &r.Cursor, &r.LastError, &r.UpdatedMs)
	if errors.Is(err, sql.ErrNoRows) {
		err = ErrNotFound
	}
	return r, err
}

type ResourceRetirement struct {
	Namespace string `json:"namespace"`
	DataScope string `json:"data_scope"`
	JobID     string `json:"job_id"`
	Epoch     uint64 `json:"epoch_floor"`
	Cursor    string `json:"cursor,omitempty"`
	LastError string `json:"last_error,omitempty"`
	UpdatedMs int64  `json:"updated_ms"`
}

func (s *Store) ResourceRetirements(ctx context.Context, after string, limit int) ([]ResourceRetirement, error) {
	c, err := s.cell(ctx)
	if err != nil {
		return nil, err
	}
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	rows, err := c.DB.QueryContext(ctx, `SELECT ns,data_scope,job_id,epoch_floor,gc_cursor,last_error,updated_ms FROM resource_retirements WHERE data_scope>? ORDER BY data_scope LIMIT ?`, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []ResourceRetirement{}
	for rows.Next() {
		var r ResourceRetirement
		if err := rows.Scan(&r.Namespace, &r.DataScope, &r.JobID, &r.Epoch, &r.Cursor, &r.LastError, &r.UpdatedMs); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
func (s *Store) UpdateResourceRetirement(ctx context.Context, r ResourceRetirement, next string, cause error) error {
	c, err := s.cell(ctx)
	if err != nil {
		return err
	}
	message := ""
	if cause != nil {
		message = cause.Error()
		if len(message) > 4096 {
			message = message[:4096]
		}
	}
	_, err = c.Tx(ctx, func(t *sql.Tx) error {
		_, err := t.ExecContext(ctx, `UPDATE resource_retirements SET gc_cursor=?,last_error=?,updated_ms=? WHERE data_scope=? AND job_id=? AND epoch_floor=?`, next, message, s.now().UnixMilli(), r.DataScope, r.JobID, r.Epoch)
		return err
	})
	return err
}
