package balance_control

import (
	"context"
	"time"

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
	_, err := r.db.Exec(ctx, `
		CREATE INDEX IF NOT EXISTS idx_upstream_balance_account_pauses_workspace_account
		ON upstream_balance_account_pauses (user_id, admin_account_id, remote_account_id, state)
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
