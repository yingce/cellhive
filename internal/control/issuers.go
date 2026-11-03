package control

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

var (
	ErrInvalidIssuer = errors.New("control: invalid issuer")
	ErrIssuerExists  = errors.New("control: issuer name already exists")
	ErrIssuerDeleted = errors.New("control: issuer permanently deleted")
)

// Issuer metadata stays below the 16 KiB authoritative lookup response cap.
// The JSON budget reserves maximum-width timestamps for future updates/deletion.
const MaxIssuerMetadataBytes = 8 << 10

// Issuer is persisted authorization metadata, never signing key material (ADR-304).
type Issuer struct {
	ID                 string   `json:"id"`
	Name               string   `json:"name"`
	Namespaces         []string `json:"namespaces"`
	Enabled            bool     `json:"enabled"`
	MaxTokenTTLSeconds int64    `json:"max_token_ttl_seconds"`
	CreatedMs          int64    `json:"created_ms"`
	UpdatedMs          int64    `json:"updated_ms"`
	DeletedMs          int64    `json:"deleted_ms"`
}

func validateIssuerNamespace(pattern string) error {
	if pattern == "*" {
		return nil
	}
	ns := strings.TrimSuffix(pattern, "*")
	if strings.HasPrefix(ns, "__") {
		return ErrReserved
	}
	for _, r := range ns {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '-' || r == '.') {
			return fmt.Errorf("control: invalid issuer namespace %q", pattern)
		}
	}
	return validateScopeComponent("issuer namespace", ns)
}

// ValidateIssuer validates creation input before any capture or storage access.
func ValidateIssuer(name string, namespaces []string, maxTTL int64) error {
	if len(name) > 256 || strings.TrimSpace(name) == "" || !utf8.ValidString(name) || strings.ContainsFunc(name, unicode.IsControl) {
		return fmt.Errorf("%w: name must be nonempty UTF-8 without controls and at most 256 bytes", ErrInvalidIssuer)
	}
	if len(namespaces) == 0 || len(namespaces) > 64 {
		return fmt.Errorf("%w: between 1 and 64 namespace patterns required", ErrInvalidIssuer)
	}
	for _, ns := range namespaces {
		if len(ns) > 128 {
			return fmt.Errorf("%w: namespace pattern exceeds 128 bytes", ErrInvalidIssuer)
		}
		if err := validateIssuerNamespace(ns); err != nil {
			return fmt.Errorf("%w: %w", ErrInvalidIssuer, err)
		}
	}
	if maxTTL <= 0 || maxTTL > 86400 {
		return fmt.Errorf("%w: max token TTL must be between 1 and 86400 seconds", ErrInvalidIssuer)
	}
	const maxTimestamp = int64(1<<63 - 1)
	budget, err := json.Marshal(Issuer{ID: strings.Repeat("0", 32), Name: name, Namespaces: namespaces, MaxTokenTTLSeconds: maxTTL, CreatedMs: maxTimestamp, UpdatedMs: maxTimestamp, DeletedMs: maxTimestamp})
	if err != nil || len(budget) > MaxIssuerMetadataBytes {
		return fmt.Errorf("%w: serialized metadata exceeds 8192-byte budget", ErrInvalidIssuer)
	}
	return nil
}

// CreateIssuer allocates a new immutable identity. Disabled issuers still reserve
// their name; only permanent soft deletion permits a new identity with that name.
// The caller must capture and confirm the control cell write before acknowledging.
func (s *Store) CreateIssuer(ctx context.Context, name string, namespaces []string, maxTTL int64, actor string) (Issuer, error) {
	if err := ValidateIssuer(name, namespaces, maxTTL); err != nil {
		return Issuer{}, err
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return Issuer{}, err
	}
	i := Issuer{ID: hex.EncodeToString(id[:]), Name: name, Namespaces: append([]string(nil), namespaces...), Enabled: true, MaxTokenTTLSeconds: maxTTL}
	c, err := s.cell(ctx)
	if err != nil {
		return Issuer{}, err
	}
	err = tx(ctx, c, func(t *sql.Tx) error {
		var exists bool
		if err := t.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM issuers WHERE name=? AND deleted_ms=0)`, name).Scan(&exists); err != nil {
			return err
		}
		if exists {
			return ErrIssuerExists
		}
		i.CreatedMs = s.now().UnixMilli()
		i.UpdatedMs = i.CreatedMs
		if _, err := t.ExecContext(ctx, `INSERT INTO issuers(id,name,namespaces,enabled,max_token_ttl_seconds,created_ms,updated_ms) VALUES (?,?,?,1,?,?,?)`, i.ID, name, jsonOr(i.Namespaces, "[]"), maxTTL, i.CreatedMs, i.UpdatedMs); err != nil {
			return err
		}
		return txAudit(ctx, t, GlobalNamespace, actor, "issuer.create", i.ID)
	})
	if err != nil {
		return Issuer{}, err
	}
	return i, nil
}

const issuerColumns = `id,name,namespaces,enabled,max_token_ttl_seconds,created_ms,updated_ms,deleted_ms`

func scanIssuer(scan func(...any) error) (Issuer, error) {
	var i Issuer
	var namespaces string
	err := scan(&i.ID, &i.Name, &namespaces, &i.Enabled, &i.MaxTokenTTLSeconds, &i.CreatedMs, &i.UpdatedMs, &i.DeletedMs)
	if errors.Is(err, sql.ErrNoRows) {
		return Issuer{}, ErrNotFound
	}
	if err != nil {
		return Issuer{}, err
	}
	if err := json.Unmarshal([]byte(namespaces), &i.Namespaces); err != nil {
		return Issuer{}, err
	}
	return i, nil
}

// GetIssuer includes permanent tombstones. Authorization callers must check both
// Enabled and DeletedMs and obtain owner authority before reading the control cell.
func (s *Store) GetIssuer(ctx context.Context, id string) (Issuer, error) {
	c, err := s.cell(ctx)
	if err != nil {
		return Issuer{}, err
	}
	return scanIssuer(c.DB.QueryRowContext(ctx, `SELECT `+issuerColumns+` FROM issuers WHERE id=?`, id).Scan)
}

// ListIssuers returns nondeleted issuers, including disabled ones, ordered by name.
func (s *Store) ListIssuers(ctx context.Context) ([]Issuer, error) {
	c, err := s.cell(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := c.DB.QueryContext(ctx, `SELECT `+issuerColumns+` FROM issuers WHERE deleted_ms=0 ORDER BY name,id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Issuer{}
	for rows.Next() {
		i, err := scanIssuer(rows.Scan)
		if err != nil {
			return nil, err
		}
		out = append(out, i)
	}
	return out, rows.Err()
}

// SetIssuerEnabled changes only live records; it can never restore a tombstone.
func (s *Store) SetIssuerEnabled(ctx context.Context, id string, enabled bool, actor string) error {
	c, err := s.cell(ctx)
	if err != nil {
		return err
	}
	return tx(ctx, c, func(t *sql.Tx) error {
		i, err := scanIssuer(t.QueryRowContext(ctx, `SELECT `+issuerColumns+` FROM issuers WHERE id=?`, id).Scan)
		if err != nil {
			return err
		}
		if i.DeletedMs != 0 {
			return ErrIssuerDeleted
		}
		if _, err := t.ExecContext(ctx, `UPDATE issuers SET enabled=?,updated_ms=? WHERE id=?`, enabled, s.now().UnixMilli(), id); err != nil {
			return err
		}
		action := "issuer.disable"
		if enabled {
			action = "issuer.enable"
		}
		return txAudit(ctx, t, GlobalNamespace, actor, action, id)
	})
}

// DeleteIssuer permanently disables an identity. Retries preserve its tombstone.
func (s *Store) DeleteIssuer(ctx context.Context, id, actor string) error {
	c, err := s.cell(ctx)
	if err != nil {
		return err
	}
	return tx(ctx, c, func(t *sql.Tx) error {
		i, err := scanIssuer(t.QueryRowContext(ctx, `SELECT `+issuerColumns+` FROM issuers WHERE id=?`, id).Scan)
		if err != nil {
			return err
		}
		if i.DeletedMs == 0 {
			now := s.now().UnixMilli()
			if _, err := t.ExecContext(ctx, `UPDATE issuers SET enabled=0,deleted_ms=?,updated_ms=? WHERE id=?`, now, now, id); err != nil {
				return err
			}
		}
		return txAudit(ctx, t, GlobalNamespace, actor, "issuer.delete", id)
	})
}
