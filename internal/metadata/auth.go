package metadata

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"
)

// User is an account row (password hash never leaves this package's callers).
type User struct {
	ID           int64
	OrgID        int64
	Email        string
	PasswordHash string
	Role         string
}

// CreateUser inserts a user with a pre-hashed password.
func (db *DB) CreateUser(ctx context.Context, orgID int64, email, passwordHash, role string) (int64, error) {
	email = strings.ToLower(strings.TrimSpace(email))
	if email == "" || passwordHash == "" {
		return 0, errors.New("metadata: email and password hash required")
	}
	var id int64
	err := db.Write(ctx, func(tx *sql.Tx) error {
		res, err := tx.Exec(`INSERT INTO users(org_id,email,password_hash,role,created_at) VALUES(?,?,?,?,?)`,
			orgID, email, passwordHash, role, time.Now().UTC().Format(time.RFC3339))
		if err != nil {
			return err
		}
		id, _ = res.LastInsertId()
		return nil
	})
	return id, err
}

// UserByEmail fetches a user for login.
func (db *DB) UserByEmail(ctx context.Context, email string) (User, error) {
	var u User
	err := db.sql.QueryRowContext(ctx, `SELECT id, org_id, email, password_hash, role FROM users WHERE email=?`,
		strings.ToLower(strings.TrimSpace(email))).Scan(&u.ID, &u.OrgID, &u.Email, &u.PasswordHash, &u.Role)
	if errors.Is(err, sql.ErrNoRows) {
		return u, ErrNotFound
	}
	return u, err
}

// UserByID fetches a user by primary key.
func (db *DB) UserByID(ctx context.Context, id int64) (User, error) {
	var u User
	err := db.sql.QueryRowContext(ctx, `SELECT id, org_id, email, password_hash, role FROM users WHERE id=?`,
		id).Scan(&u.ID, &u.OrgID, &u.Email, &u.PasswordHash, &u.Role)
	if errors.Is(err, sql.ErrNoRows) {
		return u, ErrNotFound
	}
	return u, err
}

// CountUsers reports whether any account exists (bootstrap decision).
func (db *DB) CountUsers(ctx context.Context) (int64, error) {
	var n int64
	err := db.sql.QueryRowContext(ctx, `SELECT COUNT(*) FROM users`).Scan(&n)
	return n, err
}

// --- sessions (server-side, revocable) ---

// CreateSession stores a session ID with TTL.
func (db *DB) CreateSession(ctx context.Context, id string, userID int64, ttl time.Duration) error {
	now := time.Now().UnixNano()
	return db.Write(ctx, func(tx *sql.Tx) error {
		_, err := tx.Exec(`INSERT INTO sessions(id,user_id,created_at,expires_at) VALUES(?,?,?,?)`,
			id, userID, now, now+ttl.Nanoseconds())
		return err
	})
}

// SessionUser resolves a live session to its user. Expired/revoked = ErrNotFound.
func (db *DB) SessionUser(ctx context.Context, id string) (User, error) {
	var u User
	err := db.sql.QueryRowContext(ctx, `SELECT u.id, u.org_id, u.email, u.password_hash, u.role
		FROM sessions s JOIN users u ON u.id = s.user_id
		WHERE s.id=? AND s.revoked=0 AND s.expires_at > ?`,
		id, time.Now().UnixNano()).Scan(&u.ID, &u.OrgID, &u.Email, &u.PasswordHash, &u.Role)
	if errors.Is(err, sql.ErrNoRows) {
		return u, ErrNotFound
	}
	return u, err
}

// RevokeSession invalidates one session immediately.
func (db *DB) RevokeSession(ctx context.Context, id string) error {
	return db.Write(ctx, func(tx *sql.Tx) error {
		_, err := tx.Exec(`UPDATE sessions SET revoked=1 WHERE id=?`, id)
		return err
	})
}

// RevokeUserSessions invalidates all of a user's sessions (privilege change).
func (db *DB) RevokeUserSessions(ctx context.Context, userID int64) error {
	return db.Write(ctx, func(tx *sql.Tx) error {
		_, err := tx.Exec(`UPDATE sessions SET revoked=1 WHERE user_id=?`, userID)
		return err
	})
}

// --- API tokens ---

// APIToken is a personal access token row (hash only).
type APIToken struct {
	ID          int64
	OrgID       int64
	UserID      int64
	Name        string
	Permissions []string // subset restriction; empty = all of user's perms
	ExpiresAt   int64    // unix nanos; 0 = no expiry
}

// CreateToken stores a token hash with an optional permission subset + expiry.
func (db *DB) CreateToken(ctx context.Context, t APIToken, tokenHash string) (int64, error) {
	var id int64
	err := db.Write(ctx, func(tx *sql.Tx) error {
		res, err := tx.Exec(`INSERT INTO api_tokens(org_id,user_id,name,token_hash,permissions,expires_at,created_at)
			VALUES(?,?,?,?,?,?,?)`,
			t.OrgID, t.UserID, t.Name, tokenHash, strings.Join(t.Permissions, ","),
			t.ExpiresAt, time.Now().UTC().Format(time.RFC3339))
		if err != nil {
			return err
		}
		id, _ = res.LastInsertId()
		return nil
	})
	return id, err
}

// TokenByHash resolves a presented token hash to its row + owning user.
// Revoked or expired tokens are not found.
func (db *DB) TokenByHash(ctx context.Context, hash string) (APIToken, User, error) {
	var t APIToken
	var u User
	var perms string
	err := db.sql.QueryRowContext(ctx, `SELECT t.id, t.org_id, t.user_id, t.name, t.permissions, t.expires_at,
			u.id, u.org_id, u.email, u.password_hash, u.role
		FROM api_tokens t JOIN users u ON u.id = t.user_id
		WHERE t.token_hash=? AND t.revoked=0 AND (t.expires_at=0 OR t.expires_at > ?)`,
		hash, time.Now().UnixNano()).
		Scan(&t.ID, &t.OrgID, &t.UserID, &t.Name, &perms, &t.ExpiresAt,
			&u.ID, &u.OrgID, &u.Email, &u.PasswordHash, &u.Role)
	if errors.Is(err, sql.ErrNoRows) {
		return t, u, ErrNotFound
	}
	if perms != "" {
		t.Permissions = strings.Split(perms, ",")
	}
	return t, u, err
}

// RevokeToken invalidates a token immediately (scoped to org).
func (db *DB) RevokeToken(ctx context.Context, orgID, tokenID int64) error {
	return db.Write(ctx, func(tx *sql.Tx) error {
		res, err := tx.Exec(`UPDATE api_tokens SET revoked=1 WHERE id=? AND org_id=?`, tokenID, orgID)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n == 0 {
			return ErrNotFound
		}
		return nil
	})
}

// --- audit log (append-only; no update/delete paths exist) ---

// AuditEntry is one immutable audit record.
type AuditEntry struct {
	OrgID     int64  `json:"org_id"`
	ActorID   int64  `json:"actor_user_id"`
	Action    string `json:"action"`
	Target    string `json:"target"`
	Detail    string `json:"detail"`
	IP        string `json:"ip"`
	UserAgent string `json:"user_agent"`
	At        int64  `json:"at"`
}

// Audit appends one entry. Failures are returned but callers must not abort
// the underlying action for audit failure — log loudly instead.
func (db *DB) Audit(ctx context.Context, e AuditEntry) error {
	if e.At == 0 {
		e.At = time.Now().UnixNano()
	}
	return db.Write(ctx, func(tx *sql.Tx) error {
		_, err := tx.Exec(`INSERT INTO audit_log(org_id,actor_user_id,action,target,detail,ip,user_agent,at)
			VALUES(?,?,?,?,?,?,?,?)`,
			e.OrgID, e.ActorID, e.Action, e.Target, e.Detail, e.IP, e.UserAgent, e.At)
		return err
	})
}

// AuditList returns an org's audit entries, newest first, bounded.
func (db *DB) AuditList(ctx context.Context, orgID int64, limit int) ([]AuditEntry, error) {
	if limit <= 0 || limit > 1000 {
		limit = 200
	}
	rows, err := db.sql.QueryContext(ctx, `SELECT org_id,actor_user_id,action,target,detail,ip,user_agent,at
		FROM audit_log WHERE org_id=? ORDER BY at DESC LIMIT ?`, orgID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []AuditEntry
	for rows.Next() {
		var e AuditEntry
		if err := rows.Scan(&e.OrgID, &e.ActorID, &e.Action, &e.Target, &e.Detail, &e.IP, &e.UserAgent, &e.At); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// ProjectInOrg reports whether a project belongs to an org — the object-level
// tenancy oracle used by AuthContext.
func (db *DB) ProjectInOrg(ctx context.Context, orgID, projectID int64) bool {
	var n int
	if err := db.sql.QueryRowContext(ctx, `SELECT COUNT(*) FROM projects WHERE id=? AND org_id=?`,
		projectID, orgID).Scan(&n); err != nil {
		return false
	}
	return n > 0
}
