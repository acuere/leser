package metadata

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"leser/internal/alerts"
)

// CreateAlertRule inserts a rule scoped to a project.
func (db *DB) CreateAlertRule(ctx context.Context, r alerts.Rule) (int64, error) {
	switch r.Condition {
	case alerts.CondNewIssue, alerts.CondRegression, alerts.CondFrequency:
	default:
		return 0, fmt.Errorf("metadata: invalid alert condition %q", r.Condition)
	}
	if r.WebhookURL == "" || r.Name == "" {
		return 0, errors.New("metadata: alert rule needs name and webhook_url")
	}
	var id int64
	err := db.Write(ctx, func(tx *sql.Tx) error {
		res, err := tx.Exec(`INSERT INTO alert_rules(org_id,project_id,name,condition,threshold,webhook_url,enabled,created_at)
			VALUES(?,?,?,?,?,?,?,?)`,
			r.OrgID, r.ProjectID, r.Name, r.Condition, r.Threshold, r.WebhookURL,
			boolInt(r.Enabled), time.Now().UTC().Format(time.RFC3339))
		if err != nil {
			return err
		}
		id, _ = res.LastInsertId()
		return nil
	})
	return id, err
}

// EnabledRules implements alerts.RuleSource.
func (db *DB) EnabledRules(ctx context.Context, projectID int64) ([]alerts.Rule, error) {
	rows, err := db.sql.QueryContext(ctx, `SELECT id, org_id, project_id, name, condition, threshold, webhook_url, enabled
		FROM alert_rules WHERE project_id=? AND enabled=1 LIMIT 100`, projectID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanRules(rows)
}

// ListAlertRules returns all rules for a project (enabled or not).
func (db *DB) ListAlertRules(ctx context.Context, projectID int64) ([]alerts.Rule, error) {
	rows, err := db.sql.QueryContext(ctx, `SELECT id, org_id, project_id, name, condition, threshold, webhook_url, enabled
		FROM alert_rules WHERE project_id=? ORDER BY id LIMIT 200`, projectID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanRules(rows)
}

// DeleteAlertRule removes a rule (tenant-scoped).
func (db *DB) DeleteAlertRule(ctx context.Context, projectID, ruleID int64) error {
	return db.Write(ctx, func(tx *sql.Tx) error {
		res, err := tx.Exec(`DELETE FROM alert_rules WHERE id=? AND project_id=?`, ruleID, projectID)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n == 0 {
			return ErrNotFound
		}
		return nil
	})
}

func scanRules(rows *sql.Rows) ([]alerts.Rule, error) {
	var out []alerts.Rule
	for rows.Next() {
		var r alerts.Rule
		var en int
		if err := rows.Scan(&r.ID, &r.OrgID, &r.ProjectID, &r.Name, &r.Condition, &r.Threshold, &r.WebhookURL, &en); err != nil {
			return nil, err
		}
		r.Enabled = en == 1
		out = append(out, r)
	}
	return out, rows.Err()
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
