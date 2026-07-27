package connection_health

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

type groupRateMonitorRepository interface {
	GetGroupRateMonitorSettings(ctx context.Context, userID, adminAccountID string) (GroupRateMonitorSettings, error)
	SaveGroupRateMonitorSettings(ctx context.Context, settings GroupRateMonitorSettings, overrides []GroupRateMonitorOverride) error
	ListEnabledGroupRateMonitorSettings(ctx context.Context) ([]GroupRateMonitorSettings, error)
	ListGroupRateMonitorOverrides(ctx context.Context, userID, adminAccountID string) ([]GroupRateMonitorOverride, error)
	GetGroupRateMonitorState(ctx context.Context, userID, adminAccountID, siteID, groupKey, targetID string) (*GroupRateMonitorTargetState, error)
	UpsertGroupRateMonitorState(ctx context.Context, state GroupRateMonitorTargetState) error
	InsertGroupRateMonitorCycle(ctx context.Context, cycle GroupRateProbeCycle) error
	ListGroupRateMonitorCycles(ctx context.Context, userID, adminAccountID string, perGroupLimit int) ([]GroupRateProbeCycle, error)
	ListLatestGroupRateMonitorCycles(ctx context.Context, userID, adminAccountID string) (map[string]time.Time, error)
	GetGroupRateMonitorAction(ctx context.Context, userID, adminAccountID, targetID string) (*GroupRateMonitorActionState, error)
	ListGroupRateMonitorActions(ctx context.Context, userID, adminAccountID string) ([]GroupRateMonitorActionState, error)
	ListPendingGroupRateMonitorActions(ctx context.Context) ([]GroupRateMonitorActionState, error)
	UpsertGroupRateMonitorAction(ctx context.Context, state GroupRateMonitorActionState) error
	DeleteGroupRateMonitorAction(ctx context.Context, userID, adminAccountID, targetID string) error
	MarkGroupRateMonitorActionConflict(ctx context.Context, userID, adminAccountID, targetID string) error
}

func (r *Repository) GetGroupRateMonitorSettings(ctx context.Context, userID, adminAccountID string) (GroupRateMonitorSettings, error) {
	settings := defaultGroupRateMonitorSettings(userID, adminAccountID)
	err := r.db.QueryRow(ctx, `
		SELECT enabled, probe_interval_seconds, failure_threshold, default_model, updated_at
		FROM connection_health_group_rate_monitor_settings
		WHERE user_id = $1 AND admin_account_id = $2
	`, userID, adminAccountID).Scan(&settings.Enabled, &settings.ProbeIntervalSeconds, &settings.FailureThreshold, &settings.DefaultModel, &settings.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return settings, nil
	}
	return settings, err
}

func (r *Repository) SaveGroupRateMonitorSettings(ctx context.Context, settings GroupRateMonitorSettings, overrides []GroupRateMonitorOverride) error {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err = tx.Exec(ctx, `
		INSERT INTO connection_health_group_rate_monitor_settings (
			user_id, admin_account_id, enabled, probe_interval_seconds, failure_threshold, default_model, updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,now())
		ON CONFLICT (user_id, admin_account_id) DO UPDATE SET
			enabled = EXCLUDED.enabled,
			probe_interval_seconds = EXCLUDED.probe_interval_seconds,
			failure_threshold = EXCLUDED.failure_threshold,
			default_model = EXCLUDED.default_model,
			updated_at = now()
	`, settings.UserID, settings.AdminAccountID, settings.Enabled, settings.ProbeIntervalSeconds, settings.FailureThreshold, settings.DefaultModel); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `
		DELETE FROM connection_health_group_rate_monitor_overrides
		WHERE user_id = $1 AND admin_account_id = $2
	`, settings.UserID, settings.AdminAccountID); err != nil {
		return err
	}
	for _, override := range overrides {
		if _, err = tx.Exec(ctx, `
			INSERT INTO connection_health_group_rate_monitor_overrides (
				user_id, admin_account_id, upstream_site_id, upstream_group_key,
				upstream_group_id, upstream_group_name, enabled, model, updated_at
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,now())
		`, settings.UserID, settings.AdminAccountID, override.UpstreamSiteID, override.UpstreamGroupKey,
			override.UpstreamGroupID, override.UpstreamGroupName, override.Enabled, override.Model); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (r *Repository) ListEnabledGroupRateMonitorSettings(ctx context.Context) ([]GroupRateMonitorSettings, error) {
	rows, err := r.db.Query(ctx, `
		SELECT user_id, admin_account_id, enabled, probe_interval_seconds, failure_threshold, default_model, updated_at
		FROM connection_health_group_rate_monitor_settings
		WHERE enabled
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]GroupRateMonitorSettings, 0)
	for rows.Next() {
		var settings GroupRateMonitorSettings
		if err := rows.Scan(&settings.UserID, &settings.AdminAccountID, &settings.Enabled, &settings.ProbeIntervalSeconds,
			&settings.FailureThreshold, &settings.DefaultModel, &settings.UpdatedAt); err != nil {
			return nil, err
		}
		result = append(result, settings)
	}
	return result, rows.Err()
}

func (r *Repository) ListGroupRateMonitorOverrides(ctx context.Context, userID, adminAccountID string) ([]GroupRateMonitorOverride, error) {
	rows, err := r.db.Query(ctx, `
		SELECT upstream_site_id, upstream_group_key, upstream_group_id, upstream_group_name, enabled, model, updated_at
		FROM connection_health_group_rate_monitor_overrides
		WHERE user_id = $1 AND admin_account_id = $2
	`, userID, adminAccountID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]GroupRateMonitorOverride, 0)
	for rows.Next() {
		var override GroupRateMonitorOverride
		override.UserID = userID
		override.AdminAccountID = adminAccountID
		if err := rows.Scan(&override.UpstreamSiteID, &override.UpstreamGroupKey, &override.UpstreamGroupID,
			&override.UpstreamGroupName, &override.Enabled, &override.Model, &override.UpdatedAt); err != nil {
			return nil, err
		}
		result = append(result, override)
	}
	return result, rows.Err()
}

func (r *Repository) GetGroupRateMonitorState(ctx context.Context, userID, adminAccountID, siteID, groupKey, targetID string) (*GroupRateMonitorTargetState, error) {
	var state GroupRateMonitorTargetState
	err := r.db.QueryRow(ctx, `
		SELECT user_id, admin_account_id, upstream_site_id, upstream_group_key, upstream_group_id,
			upstream_group_name, target_id, account_id, account_name, model, consecutive_failures,
			last_result, last_latency_ms, last_error_key, last_error_detail, unavailable_reason,
			last_probe_at, updated_at
		FROM connection_health_group_rate_monitor_states
		WHERE user_id = $1 AND admin_account_id = $2 AND upstream_site_id = $3
			AND upstream_group_key = $4 AND target_id = $5
	`, userID, adminAccountID, siteID, groupKey, targetID).Scan(
		&state.UserID, &state.AdminAccountID, &state.UpstreamSiteID, &state.UpstreamGroupKey, &state.UpstreamGroupID,
		&state.UpstreamGroupName, &state.TargetID, &state.AccountID, &state.AccountName, &state.Model,
		&state.ConsecutiveFailures, &state.LastResult, &state.LastLatencyMs, &state.LastErrorKey,
		&state.LastErrorDetail, &state.UnavailableReason, &state.LastProbeAt, &state.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return &state, err
}

func (r *Repository) UpsertGroupRateMonitorState(ctx context.Context, state GroupRateMonitorTargetState) error {
	_, err := r.db.Exec(ctx, `
		INSERT INTO connection_health_group_rate_monitor_states (
			user_id, admin_account_id, upstream_site_id, upstream_group_key, upstream_group_id,
			upstream_group_name, target_id, account_id, account_name, model, consecutive_failures,
			last_result, last_latency_ms, last_error_key, last_error_detail, unavailable_reason,
			last_probe_at, updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,now())
		ON CONFLICT (user_id, admin_account_id, upstream_site_id, upstream_group_key, target_id) DO UPDATE SET
			upstream_group_id = EXCLUDED.upstream_group_id,
			upstream_group_name = EXCLUDED.upstream_group_name,
			account_id = EXCLUDED.account_id,
			account_name = EXCLUDED.account_name,
			model = EXCLUDED.model,
			consecutive_failures = EXCLUDED.consecutive_failures,
			last_result = EXCLUDED.last_result,
			last_latency_ms = EXCLUDED.last_latency_ms,
			last_error_key = EXCLUDED.last_error_key,
			last_error_detail = EXCLUDED.last_error_detail,
			unavailable_reason = EXCLUDED.unavailable_reason,
			last_probe_at = EXCLUDED.last_probe_at,
			updated_at = now()
	`, state.UserID, state.AdminAccountID, state.UpstreamSiteID, state.UpstreamGroupKey, state.UpstreamGroupID,
		state.UpstreamGroupName, state.TargetID, state.AccountID, state.AccountName, state.Model,
		state.ConsecutiveFailures, state.LastResult, state.LastLatencyMs, state.LastErrorKey,
		state.LastErrorDetail, state.UnavailableReason, state.LastProbeAt)
	return err
}

func (r *Repository) InsertGroupRateMonitorCycle(ctx context.Context, cycle GroupRateProbeCycle) error {
	details, err := json.Marshal(cycle.Details)
	if err != nil {
		return err
	}
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err = tx.Exec(ctx, `
		INSERT INTO connection_health_group_rate_monitor_cycles (
			id, user_id, admin_account_id, upstream_site_id, upstream_group_key, upstream_group_id,
			upstream_group_name, trigger, status, model, target_count, success_count, details, created_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
	`, cycle.ID, cycle.UserID, cycle.AdminAccountID, cycle.UpstreamSiteID, cycle.UpstreamGroupKey,
		cycle.UpstreamGroupID, cycle.UpstreamGroupName, cycle.Trigger, cycle.Status, cycle.Model,
		cycle.TargetCount, cycle.SuccessCount, details, cycle.CreatedAt); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `
		DELETE FROM connection_health_group_rate_monitor_cycles
		WHERE user_id = $1 AND admin_account_id = $2 AND upstream_site_id = $3 AND upstream_group_key = $4
			AND (created_at < now() - interval '30 days' OR id IN (
				SELECT id FROM connection_health_group_rate_monitor_cycles
				WHERE user_id = $1 AND admin_account_id = $2 AND upstream_site_id = $3 AND upstream_group_key = $4
				ORDER BY created_at DESC OFFSET 100
			))
	`, cycle.UserID, cycle.AdminAccountID, cycle.UpstreamSiteID, cycle.UpstreamGroupKey); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (r *Repository) ListGroupRateMonitorCycles(ctx context.Context, userID, adminAccountID string, perGroupLimit int) ([]GroupRateProbeCycle, error) {
	if perGroupLimit <= 0 || perGroupLimit > 100 {
		perGroupLimit = 10
	}
	rows, err := r.db.Query(ctx, `
		SELECT id, user_id, admin_account_id, upstream_site_id, upstream_group_key, upstream_group_id,
			upstream_group_name, trigger, status, model, target_count, success_count, details, created_at
		FROM (
			SELECT *, row_number() OVER (
				PARTITION BY user_id, admin_account_id, upstream_site_id, upstream_group_key
				ORDER BY created_at DESC
			) AS position
			FROM connection_health_group_rate_monitor_cycles
			WHERE user_id = $1 AND admin_account_id = $2
		) ranked
		WHERE position <= $3
		ORDER BY upstream_site_id, upstream_group_key, created_at ASC
	`, userID, adminAccountID, perGroupLimit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]GroupRateProbeCycle, 0)
	for rows.Next() {
		var cycle GroupRateProbeCycle
		var raw []byte
		if err := rows.Scan(&cycle.ID, &cycle.UserID, &cycle.AdminAccountID, &cycle.UpstreamSiteID,
			&cycle.UpstreamGroupKey, &cycle.UpstreamGroupID, &cycle.UpstreamGroupName, &cycle.Trigger,
			&cycle.Status, &cycle.Model, &cycle.TargetCount, &cycle.SuccessCount, &raw, &cycle.CreatedAt); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(raw, &cycle.Details); err != nil {
			return nil, err
		}
		result = append(result, cycle)
	}
	return result, rows.Err()
}

func (r *Repository) ListLatestGroupRateMonitorCycles(ctx context.Context, userID, adminAccountID string) (map[string]time.Time, error) {
	rows, err := r.db.Query(ctx, `
		SELECT upstream_site_id, upstream_group_key, max(created_at)
		FROM connection_health_group_rate_monitor_cycles
		WHERE user_id = $1 AND admin_account_id = $2
		GROUP BY upstream_site_id, upstream_group_key
	`, userID, adminAccountID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make(map[string]time.Time)
	for rows.Next() {
		var siteID, groupKey string
		var createdAt time.Time
		if err := rows.Scan(&siteID, &groupKey, &createdAt); err != nil {
			return nil, err
		}
		result[groupRateMonitorMapKey(siteID, groupKey)] = createdAt
	}
	return result, rows.Err()
}

func (r *Repository) GetGroupRateMonitorAction(ctx context.Context, userID, adminAccountID, targetID string) (*GroupRateMonitorActionState, error) {
	var state GroupRateMonitorActionState
	err := r.db.QueryRow(ctx, `
		SELECT user_id, admin_account_id, target_id, account_id, account_name, upstream_site_id,
			upstream_group_key, original_status, original_schedulable, last_applied_status,
			last_applied_schedulable, pending_status, pending_schedulable, pending_restore, conflict, updated_at
		FROM connection_health_group_rate_monitor_actions
		WHERE user_id = $1 AND admin_account_id = $2 AND target_id = $3
	`, userID, adminAccountID, targetID).Scan(&state.UserID, &state.AdminAccountID, &state.TargetID,
		&state.AccountID, &state.AccountName, &state.UpstreamSiteID, &state.UpstreamGroupKey,
		&state.OriginalStatus, &state.OriginalSchedulable, &state.LastAppliedStatus,
		&state.LastAppliedSchedulable, &state.PendingStatus, &state.PendingSchedulable,
		&state.PendingRestore, &state.Conflict, &state.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return &state, err
}

func (r *Repository) ListGroupRateMonitorActions(ctx context.Context, userID, adminAccountID string) ([]GroupRateMonitorActionState, error) {
	rows, err := r.db.Query(ctx, `
		SELECT user_id, admin_account_id, target_id, account_id, account_name, upstream_site_id,
			upstream_group_key, original_status, original_schedulable, last_applied_status,
			last_applied_schedulable, pending_status, pending_schedulable, pending_restore, conflict, updated_at
		FROM connection_health_group_rate_monitor_actions
		WHERE user_id = $1 AND admin_account_id = $2
	`, userID, adminAccountID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanGroupRateMonitorActions(rows)
}

func (r *Repository) ListPendingGroupRateMonitorActions(ctx context.Context) ([]GroupRateMonitorActionState, error) {
	rows, err := r.db.Query(ctx, `
		SELECT user_id, admin_account_id, target_id, account_id, account_name, upstream_site_id,
			upstream_group_key, original_status, original_schedulable, last_applied_status,
			last_applied_schedulable, pending_status, pending_schedulable, pending_restore, conflict, updated_at
		FROM connection_health_group_rate_monitor_actions
		WHERE pending_restore
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanGroupRateMonitorActions(rows)
}

func scanGroupRateMonitorActions(rows pgx.Rows) ([]GroupRateMonitorActionState, error) {
	result := make([]GroupRateMonitorActionState, 0)
	for rows.Next() {
		var state GroupRateMonitorActionState
		if err := rows.Scan(&state.UserID, &state.AdminAccountID, &state.TargetID, &state.AccountID,
			&state.AccountName, &state.UpstreamSiteID, &state.UpstreamGroupKey, &state.OriginalStatus,
			&state.OriginalSchedulable, &state.LastAppliedStatus, &state.LastAppliedSchedulable,
			&state.PendingStatus, &state.PendingSchedulable, &state.PendingRestore, &state.Conflict,
			&state.UpdatedAt); err != nil {
			return nil, err
		}
		result = append(result, state)
	}
	return result, rows.Err()
}

func (r *Repository) UpsertGroupRateMonitorAction(ctx context.Context, state GroupRateMonitorActionState) error {
	_, err := r.db.Exec(ctx, `
		INSERT INTO connection_health_group_rate_monitor_actions (
			user_id, admin_account_id, target_id, account_id, account_name, upstream_site_id,
			upstream_group_key, original_status, original_schedulable, last_applied_status,
			last_applied_schedulable, pending_status, pending_schedulable, pending_restore, conflict, updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,now())
		ON CONFLICT (user_id, admin_account_id, target_id) DO UPDATE SET
			account_id = EXCLUDED.account_id,
			account_name = EXCLUDED.account_name,
			upstream_site_id = EXCLUDED.upstream_site_id,
			upstream_group_key = EXCLUDED.upstream_group_key,
			original_status = EXCLUDED.original_status,
			original_schedulable = EXCLUDED.original_schedulable,
			last_applied_status = EXCLUDED.last_applied_status,
			last_applied_schedulable = EXCLUDED.last_applied_schedulable,
			pending_status = EXCLUDED.pending_status,
			pending_schedulable = EXCLUDED.pending_schedulable,
			pending_restore = EXCLUDED.pending_restore,
			conflict = EXCLUDED.conflict,
			updated_at = now()
	`, state.UserID, state.AdminAccountID, state.TargetID, state.AccountID, state.AccountName,
		state.UpstreamSiteID, state.UpstreamGroupKey, state.OriginalStatus, state.OriginalSchedulable,
		state.LastAppliedStatus, state.LastAppliedSchedulable, state.PendingStatus,
		state.PendingSchedulable, state.PendingRestore, state.Conflict)
	return err
}

func (r *Repository) DeleteGroupRateMonitorAction(ctx context.Context, userID, adminAccountID, targetID string) error {
	_, err := r.db.Exec(ctx, `
		DELETE FROM connection_health_group_rate_monitor_actions
		WHERE user_id = $1 AND admin_account_id = $2 AND target_id = $3
	`, userID, adminAccountID, targetID)
	return err
}

func (r *Repository) MarkGroupRateMonitorActionConflict(ctx context.Context, userID, adminAccountID, targetID string) error {
	_, err := r.db.Exec(ctx, `
		UPDATE connection_health_group_rate_monitor_actions
		SET conflict = true, pending_status = '', pending_schedulable = NULL,
			pending_restore = false, updated_at = now()
		WHERE user_id = $1 AND admin_account_id = $2 AND target_id = $3
	`, userID, adminAccountID, targetID)
	return err
}
