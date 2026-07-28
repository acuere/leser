// Package metadata is the embedded SQLite metadata store (order-2 §2.2):
// orgs, projects, DSN keys, and later users/tokens/issues. Single-writer
// discipline is structural — every write goes through one goroutine via a
// channel; readers use the pool concurrently. WAL mode, NORMAL sync.
package metadata

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	_ "modernc.org/sqlite" // pure-Go driver; keeps CGO_ENABLED=0
)

// SchemaVersion is the current migration level. Migrations are forward-only.
const SchemaVersion = 3

// ErrNotFound is returned when a lookup matches nothing.
var ErrNotFound = errors.New("metadata: not found")

// writeOp is one unit of work executed by the single writer goroutine.
type writeOp struct {
	fn   func(tx *sql.Tx) error
	resp chan error
}

// DB wraps the SQLite handle with structural single-writer discipline.
type DB struct {
	sql    *sql.DB
	writes chan writeOp
	done   chan struct{}
}

// Open opens (creating if needed) the metadata database at path and applies
// pending migrations. The returned DB is ready for concurrent use.
func Open(path string) (*DB, error) {
	dsn := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(ON)", path)
	sdb, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	// The write path is serialized by our writer goroutine; cap readers to a
	// small pool (order-2 §2.2: explicit limits, not defaults).
	sdb.SetMaxOpenConns(8)

	db := &DB{
		sql:    sdb,
		writes: make(chan writeOp, 256), // bounded: writers block, never grow
		done:   make(chan struct{}),
	}
	if err := db.migrate(); err != nil {
		sdb.Close()
		return nil, err
	}
	go db.writeLoop()
	return db, nil
}

// writeLoop is the only goroutine that executes write transactions.
func (db *DB) writeLoop() {
	defer close(db.done)
	for op := range db.writes {
		op.resp <- db.execTx(op.fn)
	}
}

// execTx runs fn inside a transaction with rollback on error.
func (db *DB) execTx(fn func(tx *sql.Tx) error) error {
	tx, err := db.sql.Begin()
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

// Write submits a write transaction to the single writer and waits for it.
func (db *DB) Write(ctx context.Context, fn func(tx *sql.Tx) error) error {
	op := writeOp{fn: fn, resp: make(chan error, 1)}
	select {
	case db.writes <- op:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-op.resp:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Read exposes the read connection pool. Callers must not write through it.
func (db *DB) Read() *sql.DB { return db.sql }

// Close stops the writer and closes the database.
func (db *DB) Close() error {
	close(db.writes)
	<-db.done
	// Truncate the WAL so the .db file is self-contained for backup.
	_, _ = db.sql.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
	return db.sql.Close()
}

// migrate applies forward-only migrations tracked in schema_migrations.
func (db *DB) migrate() error {
	if _, err := db.sql.Exec(`CREATE TABLE IF NOT EXISTS schema_migrations (
		version INTEGER PRIMARY KEY,
		applied_at TEXT NOT NULL
	)`); err != nil {
		return err
	}
	var current int
	if err := db.sql.QueryRow(`SELECT COALESCE(MAX(version),0) FROM schema_migrations`).Scan(&current); err != nil {
		return err
	}
	for v := current + 1; v <= SchemaVersion; v++ {
		stmts, ok := migrations[v]
		if !ok {
			return fmt.Errorf("metadata: missing migration %d", v)
		}
		err := db.execTx(func(tx *sql.Tx) error {
			for _, s := range stmts {
				if _, err := tx.Exec(s); err != nil {
					return fmt.Errorf("metadata: migration %d: %w", v, err)
				}
			}
			_, err := tx.Exec(`INSERT INTO schema_migrations(version, applied_at) VALUES(?,?)`,
				v, time.Now().UTC().Format(time.RFC3339))
			return err
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// migrations are append-only. Never edit an applied version; add the next one.
var migrations = map[int][]string{
	1: {
		`CREATE TABLE organizations (
			id INTEGER PRIMARY KEY,
			slug TEXT NOT NULL UNIQUE,
			name TEXT NOT NULL,
			created_at TEXT NOT NULL
		)`,
		`CREATE TABLE projects (
			id INTEGER PRIMARY KEY,
			org_id INTEGER NOT NULL REFERENCES organizations(id),
			slug TEXT NOT NULL,
			name TEXT NOT NULL,
			platform TEXT NOT NULL DEFAULT '',
			created_at TEXT NOT NULL,
			UNIQUE(org_id, slug)
		)`,
		`CREATE TABLE project_keys (
			id INTEGER PRIMARY KEY,
			project_id INTEGER NOT NULL REFERENCES projects(id),
			org_id INTEGER NOT NULL REFERENCES organizations(id),
			public_key TEXT NOT NULL UNIQUE,
			is_active INTEGER NOT NULL DEFAULT 1,
			created_at TEXT NOT NULL
		)`,
		`CREATE INDEX idx_project_keys_lookup ON project_keys(public_key) WHERE is_active = 1`,
	},
	2: {
		`CREATE TABLE issues (
			id INTEGER PRIMARY KEY,
			org_id INTEGER NOT NULL REFERENCES organizations(id),
			project_id INTEGER NOT NULL REFERENCES projects(id),
			fingerprint TEXT NOT NULL,
			basis TEXT NOT NULL DEFAULT '',
			title TEXT NOT NULL DEFAULT '',
			level TEXT NOT NULL DEFAULT 'error',
			status TEXT NOT NULL DEFAULT 'unresolved',
			first_seen INTEGER NOT NULL,
			last_seen INTEGER NOT NULL,
			times_seen INTEGER NOT NULL DEFAULT 0,
			merged_into INTEGER REFERENCES issues(id),
			UNIQUE(project_id, fingerprint)
		)`,
		`CREATE INDEX idx_issues_stream ON issues(project_id, status, last_seen DESC)`,
	},
	3: {
		`CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			org_id INTEGER NOT NULL REFERENCES organizations(id),
			email TEXT NOT NULL UNIQUE,
			password_hash TEXT NOT NULL,
			role TEXT NOT NULL DEFAULT 'member',
			created_at TEXT NOT NULL
		)`,
		`CREATE TABLE sessions (
			id TEXT PRIMARY KEY,
			user_id INTEGER NOT NULL REFERENCES users(id),
			created_at INTEGER NOT NULL,
			expires_at INTEGER NOT NULL,
			revoked INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE TABLE api_tokens (
			id INTEGER PRIMARY KEY,
			org_id INTEGER NOT NULL REFERENCES organizations(id),
			user_id INTEGER NOT NULL REFERENCES users(id),
			name TEXT NOT NULL,
			token_hash TEXT NOT NULL UNIQUE,
			permissions TEXT NOT NULL DEFAULT '',
			expires_at INTEGER NOT NULL DEFAULT 0,
			revoked INTEGER NOT NULL DEFAULT 0,
			created_at TEXT NOT NULL
		)`,
		`CREATE TABLE audit_log (
			id INTEGER PRIMARY KEY,
			org_id INTEGER NOT NULL,
			actor_user_id INTEGER NOT NULL DEFAULT 0,
			action TEXT NOT NULL,
			target TEXT NOT NULL DEFAULT '',
			detail TEXT NOT NULL DEFAULT '',
			ip TEXT NOT NULL DEFAULT '',
			user_agent TEXT NOT NULL DEFAULT '',
			at INTEGER NOT NULL
		)`,
		`CREATE INDEX idx_audit_org_time ON audit_log(org_id, at DESC)`,
	},
}

// Org is an organization row.
type Org struct {
	ID   int64
	Slug string
	Name string
}

// Project is a project row.
type Project struct {
	ID       int64
	OrgID    int64
	Slug     string
	Name     string
	Platform string
}

// ProjectKey is a DSN ingest key.
type ProjectKey struct {
	ID        int64
	ProjectID int64
	OrgID     int64
	PublicKey string
	Active    bool
}

// newPublicKey generates a 32-hex-char DSN public key.
func newPublicKey() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// Bootstrap ensures a default org, project, and active DSN key exist, creating
// them on first boot. Idempotent; returns the first active key of the first
// project.
func (db *DB) Bootstrap(ctx context.Context, orgName, projectName string) (Project, ProjectKey, error) {
	var proj Project
	var key ProjectKey
	err := db.Write(ctx, func(tx *sql.Tx) error {
		now := time.Now().UTC().Format(time.RFC3339)
		var orgID int64
		err := tx.QueryRow(`SELECT id FROM organizations ORDER BY id LIMIT 1`).Scan(&orgID)
		if errors.Is(err, sql.ErrNoRows) {
			res, ierr := tx.Exec(`INSERT INTO organizations(slug,name,created_at) VALUES(?,?,?)`,
				slugify(orgName), orgName, now)
			if ierr != nil {
				return ierr
			}
			orgID, _ = res.LastInsertId()
		} else if err != nil {
			return err
		}

		err = tx.QueryRow(`SELECT id, org_id, slug, name, platform FROM projects WHERE org_id=? ORDER BY id LIMIT 1`, orgID).
			Scan(&proj.ID, &proj.OrgID, &proj.Slug, &proj.Name, &proj.Platform)
		if errors.Is(err, sql.ErrNoRows) {
			res, ierr := tx.Exec(`INSERT INTO projects(org_id,slug,name,created_at) VALUES(?,?,?,?)`,
				orgID, slugify(projectName), projectName, now)
			if ierr != nil {
				return ierr
			}
			proj = Project{OrgID: orgID, Slug: slugify(projectName), Name: projectName}
			proj.ID, _ = res.LastInsertId()
		} else if err != nil {
			return err
		}

		err = tx.QueryRow(`SELECT id, project_id, org_id, public_key, is_active FROM project_keys WHERE project_id=? AND is_active=1 ORDER BY id LIMIT 1`, proj.ID).
			Scan(&key.ID, &key.ProjectID, &key.OrgID, &key.PublicKey, &key.Active)
		if errors.Is(err, sql.ErrNoRows) {
			key = ProjectKey{ProjectID: proj.ID, OrgID: orgID, PublicKey: newPublicKey(), Active: true}
			res, ierr := tx.Exec(`INSERT INTO project_keys(project_id,org_id,public_key,is_active,created_at) VALUES(?,?,?,1,?)`,
				proj.ID, orgID, key.PublicKey, now)
			if ierr != nil {
				return ierr
			}
			key.ID, _ = res.LastInsertId()
			return nil
		}
		return err
	})
	return proj, key, err
}

// LookupKey resolves an active DSN public key to its project. Called on every
// ingest request — kept to one indexed point query.
func (db *DB) LookupKey(ctx context.Context, publicKey string) (ProjectKey, error) {
	var k ProjectKey
	err := db.sql.QueryRowContext(ctx,
		`SELECT id, project_id, org_id, public_key, is_active FROM project_keys WHERE public_key=? AND is_active=1`,
		publicKey).Scan(&k.ID, &k.ProjectID, &k.OrgID, &k.PublicKey, &k.Active)
	if errors.Is(err, sql.ErrNoRows) {
		return k, ErrNotFound
	}
	return k, err
}

// CreateOrg inserts a new organization.
func (db *DB) CreateOrg(ctx context.Context, name string) (int64, error) {
	var id int64
	err := db.Write(ctx, func(tx *sql.Tx) error {
		res, err := tx.Exec(`INSERT INTO organizations(slug,name,created_at) VALUES(?,?,?)`,
			slugify(name), name, time.Now().UTC().Format(time.RFC3339))
		if err != nil {
			return err
		}
		id, _ = res.LastInsertId()
		return nil
	})
	return id, err
}

// CreateProject inserts a project with a fresh active DSN key.
func (db *DB) CreateProject(ctx context.Context, orgID int64, name, platform string) (Project, ProjectKey, error) {
	var p Project
	var k ProjectKey
	err := db.Write(ctx, func(tx *sql.Tx) error {
		now := time.Now().UTC().Format(time.RFC3339)
		res, err := tx.Exec(`INSERT INTO projects(org_id,slug,name,platform,created_at) VALUES(?,?,?,?,?)`,
			orgID, slugify(name), name, platform, now)
		if err != nil {
			return err
		}
		p = Project{OrgID: orgID, Slug: slugify(name), Name: name, Platform: platform}
		p.ID, _ = res.LastInsertId()
		k = ProjectKey{ProjectID: p.ID, OrgID: orgID, PublicKey: newPublicKey(), Active: true}
		res, err = tx.Exec(`INSERT INTO project_keys(project_id,org_id,public_key,is_active,created_at) VALUES(?,?,?,1,?)`,
			p.ID, orgID, k.PublicKey, now)
		if err != nil {
			return err
		}
		k.ID, _ = res.LastInsertId()
		return nil
	})
	return p, k, err
}

// ProjectInfo is a project plus its first active DSN key (control plane).
type ProjectInfo struct {
	Project
	PublicKey string `json:"public_key"`
}

// ListProjects returns all projects with their first active key. Bounded to
// 200 (the control plane is not a pagination exercise yet).
func (db *DB) ListProjects(ctx context.Context) ([]ProjectInfo, error) {
	rows, err := db.sql.QueryContext(ctx, `SELECT p.id, p.org_id, p.slug, p.name, p.platform,
		COALESCE((SELECT public_key FROM project_keys k WHERE k.project_id=p.id AND k.is_active=1 ORDER BY k.id LIMIT 1),'')
		FROM projects p ORDER BY p.id LIMIT 200`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ProjectInfo
	for rows.Next() {
		var p ProjectInfo
		if err := rows.Scan(&p.ID, &p.OrgID, &p.Slug, &p.Name, &p.Platform, &p.PublicKey); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// ProjectOrg resolves a project's owning org (for issue tenancy).
func (db *DB) ProjectOrg(ctx context.Context, projectID int64) (int64, error) {
	var orgID int64
	err := db.sql.QueryRowContext(ctx, `SELECT org_id FROM projects WHERE id=?`, projectID).Scan(&orgID)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, ErrNotFound
	}
	return orgID, err
}

// slugify lowercases and dashes a display name into a slug.
func slugify(s string) string {
	out := make([]rune, 0, len(s))
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z' || r >= '0' && r <= '9':
			out = append(out, r)
		case r >= 'A' && r <= 'Z':
			out = append(out, r+32)
		case r == ' ' || r == '-' || r == '_':
			out = append(out, '-')
		}
	}
	if len(out) == 0 {
		return "default"
	}
	return string(out)
}
