package metadata

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// Issue lifecycle states (order.md milestone 3).
const (
	IssueUnresolved = "unresolved"
	IssueResolved   = "resolved"
	IssueIgnored    = "ignored"
	IssueRegressed  = "regressed"
)

// Issue is a grouped error with lifecycle state.
type Issue struct {
	ID          int64  `json:"id"`
	OrgID       int64  `json:"org_id"`
	ProjectID   int64  `json:"project_id"`
	Fingerprint string `json:"fingerprint"`
	Basis       string `json:"basis"`
	Title       string `json:"title"`
	Level       string `json:"level"`
	Status      string `json:"status"`
	FirstSeen   int64  `json:"first_seen"` // unix nanos
	LastSeen    int64  `json:"last_seen"`
	TimesSeen   int64  `json:"times_seen"`
	MergedInto  int64  `json:"merged_into,omitempty"` // 0 = not merged
}

// IssueUpsert is one observed event hit for a fingerprint.
type IssueUpsert struct {
	OrgID       int64
	ProjectID   int64
	Fingerprint string
	Basis       string
	Title       string
	Level       string
	SeenAt      int64 // unix nanos
}

// IssueOutcome reports what an upsert did — the alert engine's input.
type IssueOutcome struct {
	ID        int64
	IsNew     bool
	Regressed bool
	TimesSeen int64
	Status    string
}

// UpsertIssue records an event against its issue group, creating the issue on
// first sight. Merge decisions persist: a fingerprint whose issue was merged
// counts into the merge target. A resolved issue seeing a new event becomes
// regressed (never silently resolved).
func (db *DB) UpsertIssue(ctx context.Context, u IssueUpsert) (IssueOutcome, error) {
	if u.ProjectID == 0 || u.Fingerprint == "" {
		return IssueOutcome{}, fmt.Errorf("metadata: upsert requires project and fingerprint")
	}
	var out IssueOutcome
	err := db.Write(ctx, func(tx *sql.Tx) error {
		var id, mergedInto int64
		var status string
		err := tx.QueryRow(
			`SELECT id, status, COALESCE(merged_into,0) FROM issues WHERE project_id=? AND fingerprint=?`,
			u.ProjectID, u.Fingerprint).Scan(&id, &status, &mergedInto)
		if errors.Is(err, sql.ErrNoRows) {
			res, ierr := tx.Exec(`INSERT INTO issues
				(org_id, project_id, fingerprint, basis, title, level, status, first_seen, last_seen, times_seen)
				VALUES (?,?,?,?,?,?,?,?,?,1)`,
				u.OrgID, u.ProjectID, u.Fingerprint, u.Basis, u.Title, u.Level, IssueUnresolved, u.SeenAt, u.SeenAt)
			if ierr != nil {
				return ierr
			}
			eid, _ := res.LastInsertId()
			out = IssueOutcome{ID: eid, IsNew: true, TimesSeen: 1, Status: IssueUnresolved}
			return nil
		}
		if err != nil {
			return err
		}
		// Merge decision persists: count into the target (single hop enforced
		// at merge time, so no chase loop is possible).
		target := id
		if mergedInto != 0 {
			target = mergedInto
			if err := tx.QueryRow(`SELECT status FROM issues WHERE id=?`, target).Scan(&status); err != nil {
				return err
			}
		}
		newStatus := status
		regressed := false
		if status == IssueResolved {
			newStatus = IssueRegressed // regression detection
			regressed = true
		}
		if _, err := tx.Exec(`UPDATE issues SET
				last_seen = MAX(last_seen, ?), times_seen = times_seen + 1,
				level = ?, status = ?
				WHERE id = ?`,
			u.SeenAt, u.Level, newStatus, target); err != nil {
			return err
		}
		var seen int64
		if err := tx.QueryRow(`SELECT times_seen FROM issues WHERE id=?`, target).Scan(&seen); err != nil {
			return err
		}
		out = IssueOutcome{ID: target, Regressed: regressed, TimesSeen: seen, Status: newStatus}
		return nil
	})
	return out, err
}

// GetIssue fetches one issue scoped to a project (object-level tenant check).
func (db *DB) GetIssue(ctx context.Context, projectID, issueID int64) (Issue, error) {
	var i Issue
	err := db.sql.QueryRowContext(ctx, `SELECT id, org_id, project_id, fingerprint, basis, title, level,
			status, first_seen, last_seen, times_seen, COALESCE(merged_into,0)
			FROM issues WHERE id=? AND project_id=?`, issueID, projectID).
		Scan(&i.ID, &i.OrgID, &i.ProjectID, &i.Fingerprint, &i.Basis, &i.Title, &i.Level,
			&i.Status, &i.FirstSeen, &i.LastSeen, &i.TimesSeen, &i.MergedInto)
	if errors.Is(err, sql.ErrNoRows) {
		return i, ErrNotFound
	}
	return i, err
}

// ListIssues returns a project's issues, optionally filtered by status,
// newest-activity first. Limit is bounded to 500.
func (db *DB) ListIssues(ctx context.Context, projectID int64, status string, limit int) ([]Issue, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	q := `SELECT id, org_id, project_id, fingerprint, basis, title, level,
		status, first_seen, last_seen, times_seen, COALESCE(merged_into,0)
		FROM issues WHERE project_id=? AND merged_into IS NULL`
	args := []any{projectID}
	if status != "" {
		q += ` AND status=?`
		args = append(args, status)
	}
	q += ` ORDER BY last_seen DESC LIMIT ?`
	args = append(args, limit)

	rows, err := db.sql.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Issue
	for rows.Next() {
		var i Issue
		if err := rows.Scan(&i.ID, &i.OrgID, &i.ProjectID, &i.Fingerprint, &i.Basis, &i.Title, &i.Level,
			&i.Status, &i.FirstSeen, &i.LastSeen, &i.TimesSeen, &i.MergedInto); err != nil {
			return nil, err
		}
		out = append(out, i)
	}
	return out, rows.Err()
}

// SetIssueStatus transitions an issue's lifecycle state.
func (db *DB) SetIssueStatus(ctx context.Context, projectID, issueID int64, status string) error {
	switch status {
	case IssueUnresolved, IssueResolved, IssueIgnored:
	default:
		return fmt.Errorf("metadata: invalid status %q", status)
	}
	return db.Write(ctx, func(tx *sql.Tx) error {
		res, err := tx.Exec(`UPDATE issues SET status=? WHERE id=? AND project_id=?`, status, issueID, projectID)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n == 0 {
			return ErrNotFound
		}
		return nil
	})
}

// MergeIssues merges source issues into target within one project. Sources'
// future events count into target. Chains are flattened: anything already
// merged into a source is repointed at target, so merged_into is always one
// hop deep.
func (db *DB) MergeIssues(ctx context.Context, projectID, targetID int64, sourceIDs ...int64) error {
	return db.Write(ctx, func(tx *sql.Tx) error {
		var exists int
		if err := tx.QueryRow(`SELECT COUNT(*) FROM issues WHERE id=? AND project_id=? AND merged_into IS NULL`,
			targetID, projectID).Scan(&exists); err != nil {
			return err
		}
		if exists == 0 {
			return fmt.Errorf("metadata: merge target %d: %w", targetID, ErrNotFound)
		}
		for _, src := range sourceIDs {
			if src == targetID {
				return fmt.Errorf("metadata: cannot merge issue %d into itself", src)
			}
			var count int64
			err := tx.QueryRow(`SELECT times_seen FROM issues WHERE id=? AND project_id=?`, src, projectID).Scan(&count)
			if errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("metadata: merge source %d: %w", src, ErrNotFound)
			}
			if err != nil {
				return err
			}
			if _, err := tx.Exec(`UPDATE issues SET merged_into=? WHERE id=? AND project_id=?`, targetID, src, projectID); err != nil {
				return err
			}
			// Flatten chains: repoint anything merged into src.
			if _, err := tx.Exec(`UPDATE issues SET merged_into=? WHERE merged_into=? AND project_id=?`, targetID, src, projectID); err != nil {
				return err
			}
			if _, err := tx.Exec(`UPDATE issues SET times_seen = times_seen + ? WHERE id=?`, count, targetID); err != nil {
				return err
			}
		}
		return nil
	})
}

// SplitIssue undoes a merge for one source issue: its fingerprint groups
// independently again. The events already counted into the target stay there
// (history is not rewritten); new events count into the split issue.
func (db *DB) SplitIssue(ctx context.Context, projectID, sourceID int64) error {
	return db.Write(ctx, func(tx *sql.Tx) error {
		res, err := tx.Exec(`UPDATE issues SET merged_into=NULL, status=? WHERE id=? AND project_id=? AND merged_into IS NOT NULL`,
			IssueUnresolved, sourceID, projectID)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n == 0 {
			return ErrNotFound
		}
		return nil
	})
}

// touchTime is a seam for tests.
var _ = time.Now
