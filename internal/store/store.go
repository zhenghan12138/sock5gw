package store

import (
	"context"
	"database/sql"
	"time"

	_ "modernc.org/sqlite"
)

type DB struct {
	*sql.DB
}

type Lease struct {
	ClientIP  string
	ProxyID   string
	Status    string
	ExpiresAt time.Time
	UpdatedAt time.Time
}

type Proxy struct {
	ID        string
	Address   string
	Username  string
	Password  string
	Disabled  bool
	CreatedAt time.Time
	UpdatedAt time.Time
}

type DynamicProxy struct {
	ProxyID           string
	Country           string
	DurationMinutes   int64
	ProviderExpiresAt time.Time
}

func Open(path string) (*DB, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;`); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := migrate(db); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &DB{DB: db}, nil
}

func migrate(db *sql.DB) error {
	_, err := db.Exec(`
CREATE TABLE IF NOT EXISTS leases (
  client_ip TEXT PRIMARY KEY,
  proxy_id TEXT NOT NULL,
  status TEXT NOT NULL,
  expires_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts INTEGER NOT NULL,
  client_ip TEXT,
  proxy_id TEXT,
  event TEXT NOT NULL,
  detail TEXT
);
CREATE INDEX IF NOT EXISTS idx_events_ts ON events(ts);
CREATE TABLE IF NOT EXISTS proxies (
  id TEXT PRIMARY KEY,
  address TEXT NOT NULL,
  username TEXT NOT NULL DEFAULT '',
  password TEXT NOT NULL DEFAULT '',
  disabled INTEGER NOT NULL DEFAULT 0,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS dynamic_proxies (
  proxy_id TEXT PRIMARY KEY,
  country TEXT NOT NULL,
  duration_minutes INTEGER NOT NULL,
  provider_expires_at INTEGER NOT NULL
);
`)
	return err
}

func (db *DB) UpsertProxy(ctx context.Context, p Proxy) error {
	now := time.Now()
	if p.CreatedAt.IsZero() {
		p.CreatedAt = now
	}
	p.UpdatedAt = now
	disabled := 0
	if p.Disabled {
		disabled = 1
	}
	_, err := db.ExecContext(ctx, `
INSERT INTO proxies(id, address, username, password, disabled, created_at, updated_at)
VALUES(?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
  address=excluded.address,
  username=excluded.username,
  password=excluded.password,
  disabled=excluded.disabled,
  updated_at=excluded.updated_at`,
		p.ID, p.Address, p.Username, p.Password, disabled, p.CreatedAt.Unix(), p.UpdatedAt.Unix())
	return err
}

func (db *DB) DeleteProxy(ctx context.Context, id string) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM dynamic_proxies WHERE proxy_id=?`, id); err != nil {
		_ = tx.Rollback()
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM proxies WHERE id=?`, id); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (db *DB) ListProxies(ctx context.Context) ([]Proxy, error) {
	rows, err := db.QueryContext(ctx, `SELECT id, address, username, password, disabled, created_at, updated_at FROM proxies ORDER BY created_at, id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var proxies []Proxy
	for rows.Next() {
		var p Proxy
		var disabled int
		var createdAt, updatedAt int64
		if err := rows.Scan(&p.ID, &p.Address, &p.Username, &p.Password, &disabled, &createdAt, &updatedAt); err != nil {
			return nil, err
		}
		p.Disabled = disabled != 0
		p.CreatedAt = time.Unix(createdAt, 0)
		p.UpdatedAt = time.Unix(updatedAt, 0)
		proxies = append(proxies, p)
	}
	return proxies, rows.Err()
}

func (db *DB) ListDynamicProxies(ctx context.Context) ([]DynamicProxy, error) {
	rows, err := db.QueryContext(ctx, `SELECT proxy_id, country, duration_minutes, provider_expires_at FROM dynamic_proxies`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var proxies []DynamicProxy
	for rows.Next() {
		var proxy DynamicProxy
		var expiresAt int64
		if err := rows.Scan(&proxy.ProxyID, &proxy.Country, &proxy.DurationMinutes, &expiresAt); err != nil {
			return nil, err
		}
		proxy.ProviderExpiresAt = time.Unix(expiresAt, 0)
		proxies = append(proxies, proxy)
	}
	return proxies, rows.Err()
}

func (db *DB) ActivateDynamicLease(ctx context.Context, proxy Proxy, dynamic DynamicProxy, lease Lease, deleteProxyID string) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	rollback := func(err error) error {
		_ = tx.Rollback()
		return err
	}
	now := time.Now()
	if _, err := tx.ExecContext(ctx, `
INSERT INTO proxies(id, address, username, password, disabled, created_at, updated_at)
VALUES(?, ?, ?, ?, 0, ?, ?)
ON CONFLICT(id) DO UPDATE SET
  address=excluded.address,
  username=excluded.username,
  password=excluded.password,
  disabled=0,
  updated_at=excluded.updated_at`,
		proxy.ID, proxy.Address, proxy.Username, proxy.Password, now.Unix(), now.Unix()); err != nil {
		return rollback(err)
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO dynamic_proxies(proxy_id, country, duration_minutes, provider_expires_at)
VALUES(?, ?, ?, ?)
ON CONFLICT(proxy_id) DO UPDATE SET
  country=excluded.country,
  duration_minutes=excluded.duration_minutes,
  provider_expires_at=excluded.provider_expires_at`,
		dynamic.ProxyID, dynamic.Country, dynamic.DurationMinutes, dynamic.ProviderExpiresAt.Unix()); err != nil {
		return rollback(err)
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO leases(client_ip, proxy_id, status, expires_at, updated_at)
VALUES(?, ?, ?, ?, ?)
ON CONFLICT(client_ip) DO UPDATE SET
  proxy_id=excluded.proxy_id,
  status=excluded.status,
  expires_at=excluded.expires_at,
  updated_at=excluded.updated_at`,
		lease.ClientIP, lease.ProxyID, lease.Status, lease.ExpiresAt.Unix(), lease.UpdatedAt.Unix()); err != nil {
		return rollback(err)
	}
	if deleteProxyID != "" && deleteProxyID != proxy.ID {
		if _, err := tx.ExecContext(ctx, `DELETE FROM dynamic_proxies WHERE proxy_id=?`, deleteProxyID); err != nil {
			return rollback(err)
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM proxies WHERE id=?`, deleteProxyID); err != nil {
			return rollback(err)
		}
	}
	return tx.Commit()
}

func (db *DB) DeleteLeaseAndProxy(ctx context.Context, clientIP, proxyID string) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	rollback := func(err error) error {
		_ = tx.Rollback()
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM leases WHERE client_ip=?`, clientIP); err != nil {
		return rollback(err)
	}
	if proxyID != "" {
		if _, err := tx.ExecContext(ctx, `DELETE FROM dynamic_proxies WHERE proxy_id=?`, proxyID); err != nil {
			return rollback(err)
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM proxies WHERE id=?`, proxyID); err != nil {
			return rollback(err)
		}
	}
	return tx.Commit()
}

func (db *DB) UpsertLease(ctx context.Context, l Lease) error {
	_, err := db.ExecContext(ctx, `
INSERT INTO leases(client_ip, proxy_id, status, expires_at, updated_at)
VALUES(?, ?, ?, ?, ?)
ON CONFLICT(client_ip) DO UPDATE SET
  proxy_id=excluded.proxy_id,
  status=excluded.status,
  expires_at=excluded.expires_at,
  updated_at=excluded.updated_at`,
		l.ClientIP, l.ProxyID, l.Status, l.ExpiresAt.Unix(), l.UpdatedAt.Unix())
	return err
}

func (db *DB) DeleteLease(ctx context.Context, clientIP string) error {
	_, err := db.ExecContext(ctx, `DELETE FROM leases WHERE client_ip=?`, clientIP)
	return err
}

func (db *DB) ListLeases(ctx context.Context) ([]Lease, error) {
	rows, err := db.QueryContext(ctx, `SELECT client_ip, proxy_id, status, expires_at, updated_at FROM leases`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var leases []Lease
	for rows.Next() {
		var l Lease
		var expiresAt, updatedAt int64
		if err := rows.Scan(&l.ClientIP, &l.ProxyID, &l.Status, &expiresAt, &updatedAt); err != nil {
			return nil, err
		}
		l.ExpiresAt = time.Unix(expiresAt, 0)
		l.UpdatedAt = time.Unix(updatedAt, 0)
		leases = append(leases, l)
	}
	return leases, rows.Err()
}

func (db *DB) AddEvent(ctx context.Context, clientIP, proxyID, event, detail string) {
	_, _ = db.ExecContext(ctx, `INSERT INTO events(ts, client_ip, proxy_id, event, detail) VALUES(?, ?, ?, ?, ?)`,
		time.Now().Unix(), clientIP, proxyID, event, detail)
}
