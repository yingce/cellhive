// Package proxysql reconciles Hyperdrive origins into a ProxySQL admin
// instance. ProxySQL is protocol-aware: MySQL origins live in mysql_servers/
// mysql_users, PostgreSQL origins in pgsql_servers/pgsql_users. Changes are
// applied with the admin SQL interface and then loaded into RUNTIME.
package proxysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"

	_ "github.com/go-sql-driver/mysql"
)

// Sync applies Hyperdrive origin changes to ProxySQL.
type Sync struct {
	dbs  []*sql.DB
	save bool
	mu   sync.Mutex // serialize admin writes; ProxySQL has no transactions
}

// New opens a ProxySQL admin connection. dsn is a go-sql-driver DSN, e.g.
// admin:admin@tcp(127.0.0.1:6032)/.
func New(dsn string, save bool) (*Sync, error) {
	var dbs []*sql.DB
	for _, part := range strings.Split(dsn, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		db, err := sql.Open("mysql", part)
		if err != nil {
			return nil, err
		}
		dbs = append(dbs, db)
	}
	if len(dbs) == 0 {
		return nil, fmt.Errorf("proxysql: empty admin DSN")
	}
	return &Sync{dbs: dbs, save: save}, nil
}

// Close releases the admin connection pool.
func (s *Sync) Close() error {
	if s == nil {
		return nil
	}
	var first error
	for _, db := range s.dbs {
		if err := db.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// Hostgroup returns a stable ProxySQL hostgroup for a Hyperdrive resource.
func Hostgroup(namespace, name string) int {
	var h uint32 = 2166136261
	for _, b := range []byte(namespace + "/" + name) {
		h ^= uint32(b)
		h *= 16777619
	}
	return 1000 + int(h%50000)
}

func quote(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

// Origin describes one Hyperdrive origin.
type Origin struct {
	Namespace string
	Name      string
	URL       string
	// ClientUser/ClientPassword are the generated worker credentials. When
	// empty, the origin user/password are used (legacy behavior).
	ClientUser     string
	ClientPassword string
}

// Statements returns the admin SQL for upserting one origin. It is exported
// for unit tests; Sync executes the same statements.
func Statements(o Origin) ([]string, error) {
	u, err := url.Parse(strings.TrimSpace(o.URL))
	if err != nil || u.Hostname() == "" {
		return nil, fmt.Errorf("proxysql: invalid origin URL")
	}
	host := u.Hostname()
	port := u.Port()
	if port == "" {
		if strings.EqualFold(u.Scheme, "mysql") {
			port = "3306"
		} else {
			port = "5432"
		}
	}
	if _, err := strconv.Atoi(port); err != nil {
		return nil, fmt.Errorf("proxysql: invalid origin port")
	}
	user := ""
	if u.User != nil {
		user = u.User.Username()
	}
	pass, _ := u.User.Password()
	if o.ClientUser != "" {
		user, pass = o.ClientUser, o.ClientPassword
	}
	hg := Hostgroup(o.Namespace, o.Name)
	scheme := strings.ToLower(u.Scheme)

	var stmts []string
	switch scheme {
	case "mysql":
		stmts = []string{
			fmt.Sprintf("DELETE FROM mysql_servers WHERE hostgroup_id=%d", hg),
			fmt.Sprintf("INSERT INTO mysql_servers (hostgroup_id,hostname,port,status) VALUES (%d,%s,%s,'ONLINE')", hg, quote(host), quote(port)),
		}
		if user != "" {
			stmts = append(stmts,
				fmt.Sprintf("DELETE FROM mysql_users WHERE username=%s AND default_hostgroup=%d", quote(user), hg),
				fmt.Sprintf("INSERT INTO mysql_users (username,password,default_hostgroup) VALUES (%s,%s,%d)", quote(user), quote(pass), hg),
			)
		}
		stmts = append(stmts, "LOAD MYSQL SERVERS TO RUNTIME", "LOAD MYSQL USERS TO RUNTIME")
	case "postgres", "postgresql":
		stmts = []string{
			fmt.Sprintf("DELETE FROM pgsql_servers WHERE hostgroup_id=%d", hg),
			fmt.Sprintf("INSERT INTO pgsql_servers (hostgroup_id,hostname,port) VALUES (%d,%s,%s)", hg, quote(host), quote(port)),
		}
		if user != "" {
			stmts = append(stmts,
				fmt.Sprintf("DELETE FROM pgsql_users WHERE username=%s AND default_hostgroup=%d", quote(user), hg),
				fmt.Sprintf("INSERT INTO pgsql_users (username,password,default_hostgroup) VALUES (%s,%s,%d)", quote(user), quote(pass), hg),
			)
		}
		stmts = append(stmts, "LOAD PGSQL SERVERS TO RUNTIME", "LOAD PGSQL USERS TO RUNTIME")
	default:
		return nil, fmt.Errorf("proxysql: unsupported origin scheme %q", u.Scheme)
	}
	return stmts, nil
}

// SyncOrigin upserts one Hyperdrive origin and reloads the relevant runtime
// tables. The admin schema is trusted configuration, so statements are literal
// SQL (ProxySQL's admin console does not support all prepared statements).
func (s *Sync) SyncOrigin(ctx context.Context, o Origin) error {
	if s == nil || len(s.dbs) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	stmts, err := Statements(o)
	if err != nil {
		return err
	}
	if s.save {
		if stmts, err = appendSave(stmts, o.URL); err != nil {
			return err
		}
	}
	return s.execAll(ctx, stmts)
}

// Remove deletes a resource's ProxySQL rows. The scheme is required because
// MySQL and PostgreSQL use different tables.
func (s *Sync) Remove(ctx context.Context, namespace, name, originURL, clientUser string) error {
	if s == nil || len(s.dbs) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	u, err := url.Parse(strings.TrimSpace(originURL))
	if err != nil {
		return fmt.Errorf("proxysql: invalid origin URL")
	}
	hg := Hostgroup(namespace, name)
	user := clientUser
	if user == "" && u.User != nil {
		user = u.User.Username()
	}
	var stmts []string
	switch strings.ToLower(u.Scheme) {
	case "mysql":
		stmts = append(stmts, fmt.Sprintf("DELETE FROM mysql_servers WHERE hostgroup_id=%d", hg))
		if user != "" {
			stmts = append(stmts, fmt.Sprintf("DELETE FROM mysql_users WHERE username=%s AND default_hostgroup=%d", quote(user), hg))
		}
		stmts = append(stmts, "LOAD MYSQL SERVERS TO RUNTIME", "LOAD MYSQL USERS TO RUNTIME")
		if s.save {
			stmts = append(stmts, "SAVE MYSQL SERVERS TO DISK", "SAVE MYSQL USERS TO DISK")
		}
	case "postgres", "postgresql":
		stmts = append(stmts, fmt.Sprintf("DELETE FROM pgsql_servers WHERE hostgroup_id=%d", hg))
		if user != "" {
			stmts = append(stmts, fmt.Sprintf("DELETE FROM pgsql_users WHERE username=%s AND default_hostgroup=%d", quote(user), hg))
		}
		stmts = append(stmts, "LOAD PGSQL SERVERS TO RUNTIME", "LOAD PGSQL USERS TO RUNTIME")
		if s.save {
			stmts = append(stmts, "SAVE PGSQL SERVERS TO DISK", "SAVE PGSQL USERS TO DISK")
		}
	default:
		return fmt.Errorf("proxysql: unsupported origin scheme %q", u.Scheme)
	}
	return s.execAll(ctx, stmts)
}

// execAll applies the statements to every admin endpoint. One unavailable
// replica must not prevent the others from converging; all errors are joined.
func (s *Sync) execAll(ctx context.Context, stmts []string) error {
	var errs []error
	for i, db := range s.dbs {
		for _, q := range stmts {
			if _, err := db.ExecContext(ctx, q); err != nil {
				errs = append(errs, fmt.Errorf("proxysql endpoint %d: %s: %w", i, q, err))
				break
			}
		}
	}
	return errors.Join(errs...)
}

func appendSave(stmts []string, originURL string) ([]string, error) {
	u, err := url.Parse(originURL)
	if err != nil {
		return nil, err
	}
	if strings.EqualFold(u.Scheme, "mysql") {
		return append(stmts, "SAVE MYSQL SERVERS TO DISK", "SAVE MYSQL USERS TO DISK"), nil
	}
	return append(stmts, "SAVE PGSQL SERVERS TO DISK", "SAVE PGSQL USERS TO DISK"), nil
}
