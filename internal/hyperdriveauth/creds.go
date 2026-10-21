// Package hyperdriveauth creates per-binding database users so workers never
// receive the origin's credentials. The generated identity is stable for a
// namespace/resource pair; the origin URL stays sealed in the control plane.
package hyperdriveauth

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net/url"
	"strings"

	"github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

// Credentials is a generated client identity.
type Credentials struct {
	User     string
	Password string
}

// Derive returns the stable per-binding identity. secret is the platform
// scope secret (base64url); it never leaves the platform.
func Derive(secret, namespace, resource string) Credentials {
	if secret == "" {
		return Credentials{}
	}
	mac := hmac.New(sha256.New, []byte(secret))
	fmt.Fprintf(mac, "hyperdrive-client\x00%s/%s", namespace, resource)
	sum := mac.Sum(nil)
	return Credentials{
		User:     "hd_" + hex.EncodeToString(sum[:10]),
		Password: base64.RawURLEncoding.EncodeToString(sum[10:26]),
	}
}

func quote(s string) string   { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }
func ident(s string) string   { return "`" + strings.ReplaceAll(s, "`", "``") + "`" }
func pgIdent(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }

// Provision creates/updates the generated role on the origin and grants access
// to the origin database. originURL must use credentials with CREATE USER /
// GRANT privileges; it is the sealed resource URL, never exposed to workers.
func Provision(ctx context.Context, originURL string, c Credentials) error {
	u, err := url.Parse(strings.TrimSpace(originURL))
	if err != nil || u.Hostname() == "" {
		return fmt.Errorf("hyperdriveauth: invalid origin URL")
	}
	if c.User == "" || c.Password == "" {
		return fmt.Errorf("hyperdriveauth: empty generated credentials")
	}
	db := strings.TrimPrefix(u.Path, "/")
	if db == "" {
		return fmt.Errorf("hyperdriveauth: origin URL has no database")
	}
	switch strings.ToLower(u.Scheme) {
	case "mysql":
		cfg := mysql.NewConfig()
		cfg.User = u.User.Username()
		cfg.Passwd, _ = u.User.Password()
		cfg.Net = "tcp"
		cfg.Addr = u.Host
		cfg.DBName = db
		cfg.Timeout = 5e9
		conn, err := sql.Open("mysql", cfg.FormatDSN())
		if err != nil {
			return err
		}
		defer conn.Close()
		stmts := []string{
			"CREATE USER IF NOT EXISTS " + quote(c.User) + "@'%' IDENTIFIED BY " + quote(c.Password),
			"ALTER USER " + quote(c.User) + "@'%' IDENTIFIED BY " + quote(c.Password),
			"GRANT ALL PRIVILEGES ON " + ident(db) + ".* TO " + quote(c.User) + "@'%'",
			"FLUSH PRIVILEGES",
		}
		for _, q := range stmts {
			if _, err := conn.ExecContext(ctx, q); err != nil {
				return fmt.Errorf("hyperdriveauth mysql: %s: %w", q, err)
			}
		}
		return nil
	case "postgres", "postgresql":
		conn, err := sql.Open("postgres", originURL)
		if err != nil {
			return err
		}
		defer conn.Close()
		role := pgIdent(c.User)
		pass := quote(c.Password)
		stmts := []string{
			"DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname=" + quote(c.User) + ") THEN CREATE ROLE " + role + " LOGIN PASSWORD " + pass + "; ELSE ALTER ROLE " + role + " LOGIN PASSWORD " + pass + "; END IF; END $$",
			"GRANT CONNECT ON DATABASE " + pgIdent(db) + " TO " + role,
			"GRANT USAGE ON SCHEMA public TO " + role,
			"GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO " + role,
			"GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO " + role,
			"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO " + role,
			"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE, SELECT ON SEQUENCES TO " + role,
		}
		for _, q := range stmts {
			if _, err := conn.ExecContext(ctx, q); err != nil {
				return fmt.Errorf("hyperdriveauth postgres: %s: %w", q, err)
			}
		}
		return nil
	default:
		return fmt.Errorf("hyperdriveauth: unsupported origin scheme %q", u.Scheme)
	}
}

// Deprovision removes the generated role. Failures are returned for logging;
// callers should not fail the resource delete because the role may still own
// objects or the origin may be unavailable.
func Deprovision(ctx context.Context, originURL string, c Credentials) error {
	u, err := url.Parse(strings.TrimSpace(originURL))
	if err != nil || u.Hostname() == "" || c.User == "" {
		return fmt.Errorf("hyperdriveauth: invalid origin URL/credentials")
	}
	switch strings.ToLower(u.Scheme) {
	case "mysql":
		db := strings.TrimPrefix(u.Path, "/")
		cfg := mysql.NewConfig()
		cfg.User = u.User.Username()
		cfg.Passwd, _ = u.User.Password()
		cfg.Net = "tcp"
		cfg.Addr = u.Host
		cfg.DBName = db
		conn, err := sql.Open("mysql", cfg.FormatDSN())
		if err != nil {
			return err
		}
		defer conn.Close()
		_, err = conn.ExecContext(ctx, "DROP USER IF EXISTS "+quote(c.User)+"@'%'")
		return err
	case "postgres", "postgresql":
		conn, err := sql.Open("postgres", originURL)
		if err != nil {
			return err
		}
		defer conn.Close()
		role := pgIdent(c.User)
		if _, err := conn.ExecContext(ctx, "DROP OWNED BY "+role); err != nil {
			return err
		}
		_, err = conn.ExecContext(ctx, "DROP ROLE IF EXISTS "+role)
		return err
	default:
		return fmt.Errorf("hyperdriveauth: unsupported origin scheme %q", u.Scheme)
	}
}
