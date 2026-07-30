package balance_control

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	pauseStatePending = "pending"
	pauseStateApplied = "applied"
)

type PauseRecord struct {
	UserID               string
	AdminAccountID       string
	UpstreamSiteID       string
	AdminAccountIDRemote string
	RealConnectionID     string
	OriginalStatus       string
	OriginalSchedulable  *bool
	Applied              bool
	LastError            string
	UpdatedAt            time.Time
}

type Repository struct {
	db *pgxpool.Pool
}

func NewRepository(db *pgxpool.Pool) *Repository {
	return &Repository{db: db}
}

func (r *Repository) EnsureSchema(ctx context.Context) error {
	if _, err := r.db.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS upstream_balance_account_pauses (
			user_id text NOT NULL,
			admin_account_id text NOT NULL,
			upstream_site_id text NOT NULL,
			remote_account_id text NOT NULL,
			real_connection_id text NOT NULL DEFAULT '',
			original_status text NOT NULL,
			original_schedulable boolean NULL,
			state text NOT NULL DEFAULT 'pending',
			last_error text NOT NULL DEFAULT '',
			created_at timestamptz NOT NULL DEFAULT now(),
			updated_at timestamptz NOT NULL DEFAULT now(),
			PRIMARY KEY (user_id, admin_account_id, upstream_site_id, remote_account_id)
		)
	`); err != nil {
		return err
	}
	if _, err := r.db.Exec(ctx, `
		CREATE INDEX IF NOT EXISTS idx_upstream_balance_account_pauses_workspace_account
		ON upstream_balance_account_pauses (user_id, admin_account_id, remote_account_id, state)
	`); err != nil {
		return err
	}
	if _, err := r.db.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS upstream_balance_profit_cycles (
			user_id text NOT NULL,
			admin_account_id text NOT NULL,
			upstream_site_id text NOT NULL,
			site_name text NOT NULL DEFAULT '',
			status text NOT NULL DEFAULT 'active',
			recharge_amount_cny double precision NOT NULL DEFAULT 0,
			downstream_income_cny double precision NULL,
			complete boolean NOT NULL DEFAULT false,
			last_error text NOT NULL DEFAULT '',
			started_at timestamptz NOT NULL,
			ended_at timestamptz NULL,
			updated_at timestamptz NOT NULL DEFAULT now(),
			PRIMARY KEY (user_id, admin_account_id, upstream_site_id)
		)
	`); err != nil {
		return err
	}
	if _, err := r.db.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS upstream_balance_profit_cycle_accounts (
			user_id text NOT NULL,
			admin_account_id text NOT NULL,
			upstream_site_id text NOT NULL,
			remote_account_id text NOT NULL,
			upstream_group_id text NOT NULL DEFAULT '',
			upstream_group_name text NOT NULL DEFAULT '',
			attribution_status text NOT NULL DEFAULT 'assigned',
			usage_start_date text NOT NULL DEFAULT '',
			baseline_actual_cost double precision NULL,
			current_actual_cost double precision NULL,
			baseline_complete boolean NOT NULL DEFAULT false,
			last_error text NOT NULL DEFAULT '',
			baseline_captured_at timestamptz NULL,
			created_at timestamptz NOT NULL DEFAULT now(),
			updated_at timestamptz NOT NULL DEFAULT now(),
			PRIMARY KEY (user_id, admin_account_id, upstream_site_id, remote_account_id)
		)
	`); err != nil {
		return err
	}
	_, err := r.db.Exec(ctx, `
		CREATE INDEX IF NOT EXISTS idx_upstream_balance_profit_accounts_site
		ON upstream_balance_profit_cycle_accounts (user_id, admin_account_id, upstream_site_id)
	`)
	return err
}

func (r *Repository) UpsertPending(ctx context.Context, record PauseRecord) error {
	_, err := r.db.Exec(ctx, `
		INSERT INTO upstream_balance_account_pauses (
			user_id, admin_account_id, upstream_site_id, remote_account_id,
			real_connection_id, original_status, original_schedulable, state, last_error,
			created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, now(), now())
		ON CONFLICT (user_id, admin_account_id, upstream_site_id, remote_account_id)
		DO UPDATE SET
			real_connection_id = COALESCE(NULLIF(EXCLUDED.real_connection_id, ''), upstream_balance_account_pauses.real_connection_id),
			original_status = upstream_balance_account_pauses.original_status,
			original_schedulable = upstream_balance_account_pauses.original_schedulable,
			state = CASE WHEN upstream_balance_account_pauses.state = 'applied' THEN 'applied' ELSE EXCLUDED.state END,
			last_error = EXCLUDED.last_error,
			updated_at = now()
	`, record.UserID, record.AdminAccountID, record.UpstreamSiteID, record.AdminAccountIDRemote,
		record.RealConnectionID, record.OriginalStatus, record.OriginalSchedulable, pauseStatePending, record.LastError)
	return err
}

func (r *Repository) MarkApplied(ctx context.Context, userID, adminAccountID, siteID, remoteAccountID string) error {
	_, err := r.db.Exec(ctx, `
		UPDATE upstream_balance_account_pauses
		SET state = 'applied', last_error = '', updated_at = now()
		WHERE user_id = $1 AND admin_account_id = $2 AND upstream_site_id = $3 AND remote_account_id = $4
	`, userID, adminAccountID, siteID, remoteAccountID)
	return err
}

func (r *Repository) SaveError(ctx context.Context, userID, adminAccountID, siteID, remoteAccountID, message string) error {
	_, err := r.db.Exec(ctx, `
		UPDATE upstream_balance_account_pauses
		SET last_error = $5, updated_at = now()
		WHERE user_id = $1 AND admin_account_id = $2 AND upstream_site_id = $3 AND remote_account_id = $4
	`, userID, adminAccountID, siteID, remoteAccountID, message)
	return err
}

func (r *Repository) ListForSite(ctx context.Context, userID, adminAccountID, siteID string, appliedOnly bool) ([]PauseRecord, error) {
	query := `
		SELECT user_id, admin_account_id, upstream_site_id, remote_account_id,
		       real_connection_id, original_status, original_schedulable, state, last_error, updated_at
		FROM upstream_balance_account_pauses
		WHERE user_id = $1 AND admin_account_id = $2 AND upstream_site_id = $3
	`
	if appliedOnly {
		query += ` AND state = 'applied'`
	}
	query += ` ORDER BY remote_account_id ASC`

	rows, err := r.db.Query(ctx, query, userID, adminAccountID, siteID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []PauseRecord
	for rows.Next() {
		var record PauseRecord
		var state string
		if err := rows.Scan(
			&record.UserID, &record.AdminAccountID, &record.UpstreamSiteID,
			&record.AdminAccountIDRemote, &record.RealConnectionID,
			&record.OriginalStatus, &record.OriginalSchedulable, &state,
			&record.LastError, &record.UpdatedAt,
		); err != nil {
			return nil, err
		}
		record.Applied = state == pauseStateApplied
		records = append(records, record)
	}
	return records, rows.Err()
}

// IsAccountPausedForWorkspace is used by schedulers that only know the target
// account ID. Account IDs are unique within a Sub2API admin workspace, so a
// lookup across sites still remains workspace-scoped and prevents a protected
// account from being re-enabled by a different worker.
func (r *Repository) IsAccountPausedForWorkspace(ctx context.Context, userID, adminAccountID, remoteAccountID string) (bool, error) {
	var paused bool
	err := r.db.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM upstream_balance_account_pauses
			WHERE user_id = $1 AND admin_account_id = $2 AND remote_account_id = $3 AND state IN ($4, $5)
		)
	`, userID, adminAccountID, remoteAccountID, pauseStatePending, pauseStateApplied).Scan(&paused)
	return paused, err
}

func (r *Repository) Delete(ctx context.Context, userID, adminAccountID, siteID, remoteAccountID string) error {
	_, err := r.db.Exec(ctx, `
		DELETE FROM upstream_balance_account_pauses
		WHERE user_id = $1 AND admin_account_id = $2 AND upstream_site_id = $3 AND remote_account_id = $4
	`, userID, adminAccountID, siteID, remoteAccountID)
	return err
}

func (r *Repository) ClearPending(ctx context.Context, userID, adminAccountID, siteID string) error {
	_, err := r.db.Exec(ctx, `
		DELETE FROM upstream_balance_account_pauses
		WHERE user_id = $1 AND admin_account_id = $2 AND upstream_site_id = $3 AND state = $4
	`, userID, adminAccountID, siteID, pauseStatePending)
	return err
}

func (r *Repository) GetProfitCycle(ctx context.Context, userID, adminAccountID, siteID string) (*ProfitCycle, error) {
	var cycle ProfitCycle
	err := r.db.QueryRow(ctx, `
		SELECT user_id, admin_account_id, upstream_site_id, site_name, status,
		       recharge_amount_cny, downstream_income_cny, complete, last_error,
		       started_at, ended_at
		FROM upstream_balance_profit_cycles
		WHERE user_id = $1 AND admin_account_id = $2 AND upstream_site_id = $3
	`, userID, adminAccountID, siteID).Scan(
		&cycle.UserID, &cycle.AdminAccountID, &cycle.UpstreamSiteID, &cycle.SiteName,
		&cycle.Status, &cycle.RechargeAmountCNY, &cycle.DownstreamIncomeCNY,
		&cycle.Complete, &cycle.LastError, &cycle.StartedAt, &cycle.EndedAt,
	)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &cycle, nil
}

func (r *Repository) StartProfitCycle(ctx context.Context, cycle ProfitCycle) error {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `
		DELETE FROM upstream_balance_profit_cycle_accounts
		WHERE user_id = $1 AND admin_account_id = $2 AND upstream_site_id = $3
	`, cycle.UserID, cycle.AdminAccountID, cycle.UpstreamSiteID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO upstream_balance_profit_cycles (
			user_id, admin_account_id, upstream_site_id, site_name, status,
			recharge_amount_cny, downstream_income_cny, complete, last_error,
			started_at, ended_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, NULL, false, '', $7, NULL, now())
		ON CONFLICT (user_id, admin_account_id, upstream_site_id)
		DO UPDATE SET site_name = EXCLUDED.site_name, status = EXCLUDED.status,
			recharge_amount_cny = EXCLUDED.recharge_amount_cny,
			downstream_income_cny = NULL, complete = false, last_error = '',
			started_at = EXCLUDED.started_at, ended_at = NULL, updated_at = now()
	`, cycle.UserID, cycle.AdminAccountID, cycle.UpstreamSiteID, cycle.SiteName,
		profitCycleActive, cycle.RechargeAmountCNY, cycle.StartedAt); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (r *Repository) AddProfitCycleRecharge(ctx context.Context, userID, adminAccountID, siteID string, amount float64) error {
	_, err := r.db.Exec(ctx, `
		UPDATE upstream_balance_profit_cycles
		SET recharge_amount_cny = recharge_amount_cny + $4, updated_at = now()
		WHERE user_id = $1 AND admin_account_id = $2 AND upstream_site_id = $3 AND status = $5
	`, userID, adminAccountID, siteID, amount, profitCycleActive)
	return err
}

func (r *Repository) ListProfitCycleAccounts(ctx context.Context, userID, adminAccountID, siteID string) ([]ProfitCycleAccount, error) {
	rows, err := r.db.Query(ctx, `
		SELECT user_id, admin_account_id, upstream_site_id, remote_account_id,
		       upstream_group_id, upstream_group_name, attribution_status,
		       usage_start_date, baseline_actual_cost, current_actual_cost,
		       baseline_complete, last_error, baseline_captured_at
		FROM upstream_balance_profit_cycle_accounts
		WHERE user_id = $1 AND admin_account_id = $2 AND upstream_site_id = $3
		ORDER BY remote_account_id ASC
	`, userID, adminAccountID, siteID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var accounts []ProfitCycleAccount
	for rows.Next() {
		var account ProfitCycleAccount
		if err := rows.Scan(
			&account.UserID, &account.AdminAccountID, &account.UpstreamSiteID,
			&account.RemoteAccountID, &account.UpstreamGroupID, &account.UpstreamGroupName,
			&account.AttributionStatus, &account.UsageStartDate, &account.BaselineActualCost,
			&account.CurrentActualCost, &account.BaselineComplete, &account.LastError,
			&account.BaselineCapturedAt,
		); err != nil {
			return nil, err
		}
		accounts = append(accounts, account)
	}
	return accounts, rows.Err()
}

func (r *Repository) UpsertProfitCycleAccount(ctx context.Context, account ProfitCycleAccount) error {
	_, err := r.db.Exec(ctx, `
		INSERT INTO upstream_balance_profit_cycle_accounts (
			user_id, admin_account_id, upstream_site_id, remote_account_id,
			upstream_group_id, upstream_group_name, attribution_status,
			usage_start_date, baseline_actual_cost, current_actual_cost,
			baseline_complete, last_error, baseline_captured_at, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, now(), now())
		ON CONFLICT (user_id, admin_account_id, upstream_site_id, remote_account_id)
		DO UPDATE SET upstream_group_id = EXCLUDED.upstream_group_id,
			upstream_group_name = EXCLUDED.upstream_group_name,
			attribution_status = EXCLUDED.attribution_status,
			usage_start_date = EXCLUDED.usage_start_date,
			baseline_actual_cost = EXCLUDED.baseline_actual_cost,
			current_actual_cost = EXCLUDED.current_actual_cost,
			baseline_complete = EXCLUDED.baseline_complete,
			last_error = EXCLUDED.last_error,
			baseline_captured_at = EXCLUDED.baseline_captured_at,
			updated_at = now()
	`, account.UserID, account.AdminAccountID, account.UpstreamSiteID,
		account.RemoteAccountID, account.UpstreamGroupID, account.UpstreamGroupName,
		account.AttributionStatus, account.UsageStartDate, account.BaselineActualCost,
		account.CurrentActualCost, account.BaselineComplete, account.LastError,
		account.BaselineCapturedAt)
	return err
}

func (r *Repository) FinalizeProfitCycle(ctx context.Context, cycle ProfitCycle, accounts []ProfitCycleAccount) error {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	for _, account := range accounts {
		if _, err := tx.Exec(ctx, `
			UPDATE upstream_balance_profit_cycle_accounts
			SET current_actual_cost = $5, last_error = $6, updated_at = now()
			WHERE user_id = $1 AND admin_account_id = $2 AND upstream_site_id = $3 AND remote_account_id = $4
		`, account.UserID, account.AdminAccountID, account.UpstreamSiteID,
			account.RemoteAccountID, account.CurrentActualCost, account.LastError); err != nil {
			return err
		}
	}
	if _, err := tx.Exec(ctx, `
		UPDATE upstream_balance_profit_cycles
		SET status = $4, downstream_income_cny = $5, complete = $6,
			last_error = $7, ended_at = $8, updated_at = now()
		WHERE user_id = $1 AND admin_account_id = $2 AND upstream_site_id = $3
	`, cycle.UserID, cycle.AdminAccountID, cycle.UpstreamSiteID, profitCycleFinalized,
		cycle.DownstreamIncomeCNY, cycle.Complete, cycle.LastError, cycle.EndedAt); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
