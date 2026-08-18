package user_management

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Repository struct {
	db *pgxpool.Pool
}

func NewRepository(db *pgxpool.Pool) *Repository { return &Repository{db: db} }

func (r *Repository) EnsureSchema(ctx context.Context) error {
	_, err := r.db.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS user_balance_rules (
			user_id text NOT NULL,
			admin_account_id text NOT NULL,
			upstream_user_id text NOT NULL,
			email text NOT NULL DEFAULT '',
			username text NOT NULL DEFAULT '',
			warning_enabled boolean NOT NULL DEFAULT false,
			warning_threshold double precision,
			auto_recharge_enabled boolean NOT NULL DEFAULT false,
			auto_recharge_threshold double precision,
			auto_recharge_amount double precision,
			warning_active boolean NOT NULL DEFAULT false,
			recharge_latched boolean NOT NULL DEFAULT false,
			recharge_pending boolean NOT NULL DEFAULT false,
			recharge_event_id text NOT NULL DEFAULT '',
			last_balance double precision,
			last_checked_at timestamptz,
			last_warning_at timestamptz,
			last_recharge_at timestamptz,
			last_error_key text NOT NULL DEFAULT '',
			created_at timestamptz NOT NULL DEFAULT now(),
			updated_at timestamptz NOT NULL DEFAULT now(),
			PRIMARY KEY (user_id, admin_account_id, upstream_user_id)
		);
		CREATE INDEX IF NOT EXISTS idx_user_balance_rules_active
			ON user_balance_rules (updated_at)
			WHERE warning_enabled OR auto_recharge_enabled;
	`)
	return err
}

func (r *Repository) ListForWorkspace(ctx context.Context, userID, adminAccountID string) ([]Rule, error) {
	rows, err := r.db.Query(ctx, ruleSelectSQL+` WHERE user_id = $1 AND admin_account_id = $2 ORDER BY updated_at DESC`, userID, adminAccountID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanRules(rows)
}

func (r *Repository) ListActive(ctx context.Context) ([]Rule, error) {
	rows, err := r.db.Query(ctx, ruleSelectSQL+` WHERE warning_enabled OR auto_recharge_enabled ORDER BY updated_at ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanRules(rows)
}

func (r *Repository) Get(ctx context.Context, userID, adminAccountID, upstreamUserID string) (Rule, error) {
	return scanRule(r.db.QueryRow(ctx, ruleSelectSQL+` WHERE user_id = $1 AND admin_account_id = $2 AND upstream_user_id = $3`, userID, adminAccountID, upstreamUserID))
}

func (r *Repository) Save(ctx context.Context, rule Rule) (Rule, error) {
	row := r.db.QueryRow(ctx, `
		INSERT INTO user_balance_rules (
			user_id, admin_account_id, upstream_user_id, email, username,
			warning_enabled, warning_threshold, auto_recharge_enabled,
			auto_recharge_threshold, auto_recharge_amount, last_balance
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
		ON CONFLICT (user_id, admin_account_id, upstream_user_id) DO UPDATE SET
			email = EXCLUDED.email,
			username = EXCLUDED.username,
			warning_active = CASE
				WHEN user_balance_rules.warning_enabled IS DISTINCT FROM EXCLUDED.warning_enabled
				  OR user_balance_rules.warning_threshold IS DISTINCT FROM EXCLUDED.warning_threshold THEN false
				ELSE user_balance_rules.warning_active END,
			recharge_latched = CASE
				WHEN user_balance_rules.auto_recharge_enabled IS DISTINCT FROM EXCLUDED.auto_recharge_enabled
				  OR user_balance_rules.auto_recharge_threshold IS DISTINCT FROM EXCLUDED.auto_recharge_threshold
				  OR user_balance_rules.auto_recharge_amount IS DISTINCT FROM EXCLUDED.auto_recharge_amount THEN false
				ELSE user_balance_rules.recharge_latched END,
			recharge_pending = CASE
				WHEN user_balance_rules.auto_recharge_enabled IS DISTINCT FROM EXCLUDED.auto_recharge_enabled
				  OR user_balance_rules.auto_recharge_threshold IS DISTINCT FROM EXCLUDED.auto_recharge_threshold
				  OR user_balance_rules.auto_recharge_amount IS DISTINCT FROM EXCLUDED.auto_recharge_amount THEN false
				ELSE user_balance_rules.recharge_pending END,
			recharge_event_id = CASE
				WHEN user_balance_rules.auto_recharge_enabled IS DISTINCT FROM EXCLUDED.auto_recharge_enabled
				  OR user_balance_rules.auto_recharge_threshold IS DISTINCT FROM EXCLUDED.auto_recharge_threshold
				  OR user_balance_rules.auto_recharge_amount IS DISTINCT FROM EXCLUDED.auto_recharge_amount THEN ''
				ELSE user_balance_rules.recharge_event_id END,
			warning_enabled = EXCLUDED.warning_enabled,
			warning_threshold = EXCLUDED.warning_threshold,
			auto_recharge_enabled = EXCLUDED.auto_recharge_enabled,
			auto_recharge_threshold = EXCLUDED.auto_recharge_threshold,
			auto_recharge_amount = EXCLUDED.auto_recharge_amount,
			last_balance = EXCLUDED.last_balance,
			last_error_key = '',
			updated_at = now()
		RETURNING `+ruleColumns,
		rule.UserID, rule.AdminAccountID, rule.UpstreamUserID, rule.Email, rule.Username,
		rule.WarningEnabled, rule.WarningThreshold, rule.AutoRechargeEnabled,
		rule.AutoRechargeThreshold, rule.AutoRechargeAmount, rule.LastBalance)
	return scanRule(row)
}

func (r *Repository) Delete(ctx context.Context, userID, adminAccountID, upstreamUserID string) error {
	_, err := r.db.Exec(ctx, `DELETE FROM user_balance_rules WHERE user_id=$1 AND admin_account_id=$2 AND upstream_user_id=$3`, userID, adminAccountID, upstreamUserID)
	return err
}

func (r *Repository) RecordObservation(ctx context.Context, rule Rule, balance float64, warningActive, rechargeLatched bool, errorKey string) error {
	_, err := r.db.Exec(ctx, `
		UPDATE user_balance_rules SET last_balance=$4, last_checked_at=now(), warning_active=$5,
			recharge_latched=$6, last_error_key=$7,
			last_warning_at=CASE WHEN $5 AND NOT warning_active THEN now() ELSE last_warning_at END,
			updated_at=now()
		WHERE user_id=$1 AND admin_account_id=$2 AND upstream_user_id=$3
	`, rule.UserID, rule.AdminAccountID, rule.UpstreamUserID, balance, warningActive, rechargeLatched, errorKey)
	return err
}

func (r *Repository) ClaimRecharge(ctx context.Context, rule Rule, eventID string) (string, bool, error) {
	var id string
	err := r.db.QueryRow(ctx, `
		UPDATE user_balance_rules SET recharge_pending=true,
			recharge_event_id=CASE WHEN recharge_event_id='' THEN $4 ELSE recharge_event_id END,
			updated_at=now()
		WHERE user_id=$1 AND admin_account_id=$2 AND upstream_user_id=$3
			AND auto_recharge_enabled AND NOT recharge_latched
		RETURNING recharge_event_id
	`, rule.UserID, rule.AdminAccountID, rule.UpstreamUserID, eventID).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	return id, err == nil, err
}

func (r *Repository) CompleteRecharge(ctx context.Context, rule Rule, balance float64) error {
	_, err := r.db.Exec(ctx, `
		UPDATE user_balance_rules SET recharge_pending=false, recharge_event_id='', recharge_latched=true,
			last_balance=$4, last_checked_at=now(), last_recharge_at=now(), last_error_key='',
			warning_active=warning_enabled AND warning_threshold IS NOT NULL AND $4 <= warning_threshold,
			updated_at=now()
		WHERE user_id=$1 AND admin_account_id=$2 AND upstream_user_id=$3
	`, rule.UserID, rule.AdminAccountID, rule.UpstreamUserID, balance)
	return err
}

func (r *Repository) RecordRechargeFailure(ctx context.Context, rule Rule, errorKey string) error {
	_, err := r.db.Exec(ctx, `UPDATE user_balance_rules SET last_checked_at=now(), last_error_key=$4, updated_at=now() WHERE user_id=$1 AND admin_account_id=$2 AND upstream_user_id=$3`, rule.UserID, rule.AdminAccountID, rule.UpstreamUserID, errorKey)
	return err
}

const ruleColumns = `user_id, admin_account_id, upstream_user_id, email, username,
	warning_enabled, warning_threshold, auto_recharge_enabled, auto_recharge_threshold,
	auto_recharge_amount, warning_active, recharge_latched, recharge_pending,
	recharge_event_id, last_balance, last_checked_at, last_warning_at, last_recharge_at,
	last_error_key, created_at, updated_at`
const ruleSelectSQL = `SELECT ` + ruleColumns + ` FROM user_balance_rules`

type rowScanner interface{ Scan(dest ...any) error }

func scanRule(row rowScanner) (Rule, error) {
	var rule Rule
	err := row.Scan(&rule.UserID, &rule.AdminAccountID, &rule.UpstreamUserID, &rule.Email, &rule.Username,
		&rule.WarningEnabled, &rule.WarningThreshold, &rule.AutoRechargeEnabled, &rule.AutoRechargeThreshold,
		&rule.AutoRechargeAmount, &rule.WarningActive, &rule.RechargeLatched, &rule.RechargePending,
		&rule.RechargeEventID, &rule.LastBalance, &rule.LastCheckedAt, &rule.LastWarningAt, &rule.LastRechargeAt,
		&rule.LastErrorKey, &rule.CreatedAt, &rule.UpdatedAt)
	return rule, err
}

func scanRules(rows pgx.Rows) ([]Rule, error) {
	rules := make([]Rule, 0)
	for rows.Next() {
		rule, err := scanRule(rows)
		if err != nil {
			return nil, err
		}
		rules = append(rules, rule)
	}
	return rules, rows.Err()
}
