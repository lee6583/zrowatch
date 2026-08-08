package connection_health

import (
	"context"
	"time"
)

type ProfitPriorityState struct {
	UserID                  string
	AdminAccountID          string
	AccountID               string
	OriginalPriority        int
	LastAppliedPriority     int
	PendingPriority         *int
	StabilityTier           string
	ObservedStabilityTier   string
	ObservedStabilityRounds int
	ObservedRank            int
	ObservedRankRounds      int
	SampleCount             int
	SuccessRate             *float64
	LatencyMs               *int
	EffectiveCost           *float64
	ShortErrorCount         int
	ShortErrorRate          *float64
	LastUpstreamErrorAt     *time.Time
	LastUpstreamErrorClass  string
	DegradationReason       string
	CleanRecoveryRounds     int
	LastPriorityChangeAt    *time.Time
	CooldownUntil           *time.Time
	LastObservedAt          *time.Time
	Conflict                bool
	UpdatedAt               time.Time
}

type profitPriorityRepository interface {
	ListProfitPriorityStates(ctx context.Context, userID, adminAccountID string) ([]ProfitPriorityState, error)
	UpsertProfitPriorityState(ctx context.Context, state ProfitPriorityState) error
	DeleteProfitPriorityState(ctx context.Context, userID, adminAccountID, accountID string) error
}

func (r *Repository) ListProfitPriorityStates(ctx context.Context, userID, adminAccountID string) ([]ProfitPriorityState, error) {
	rows, err := r.db.Query(ctx, `
		SELECT user_id, admin_account_id, account_id, original_priority, last_applied_priority,
			pending_priority, stability_tier, observed_stability_tier, observed_stability_rounds,
			observed_rank, observed_rank_rounds, sample_count, success_rate, latency_ms, effective_cost,
			short_error_count, short_error_rate, last_upstream_error_at, last_upstream_error_class,
			degradation_reason, clean_recovery_rounds, last_priority_change_at,
			cooldown_until, last_observed_at, conflict, updated_at
		FROM connection_health_profit_priority_states
		WHERE user_id = $1 AND admin_account_id = $2
	`, userID, adminAccountID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]ProfitPriorityState, 0)
	for rows.Next() {
		var state ProfitPriorityState
		if err := rows.Scan(&state.UserID, &state.AdminAccountID, &state.AccountID, &state.OriginalPriority,
			&state.LastAppliedPriority, &state.PendingPriority, &state.StabilityTier,
			&state.ObservedStabilityTier, &state.ObservedStabilityRounds, &state.ObservedRank,
			&state.ObservedRankRounds, &state.SampleCount, &state.SuccessRate, &state.LatencyMs,
			&state.EffectiveCost, &state.ShortErrorCount, &state.ShortErrorRate, &state.LastUpstreamErrorAt,
			&state.LastUpstreamErrorClass, &state.DegradationReason, &state.CleanRecoveryRounds,
			&state.LastPriorityChangeAt, &state.CooldownUntil, &state.LastObservedAt, &state.Conflict,
			&state.UpdatedAt); err != nil {
			return nil, err
		}
		result = append(result, state)
	}
	return result, rows.Err()
}

func (r *Repository) UpsertProfitPriorityState(ctx context.Context, state ProfitPriorityState) error {
	_, err := r.db.Exec(ctx, `
		INSERT INTO connection_health_profit_priority_states (
			user_id, admin_account_id, account_id, original_priority, last_applied_priority,
			pending_priority, stability_tier, observed_stability_tier, observed_stability_rounds,
			observed_rank, observed_rank_rounds, sample_count, success_rate, latency_ms, effective_cost,
			short_error_count, short_error_rate, last_upstream_error_at, last_upstream_error_class,
			degradation_reason, clean_recovery_rounds, last_priority_change_at,
			cooldown_until, last_observed_at, conflict, updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,now())
		ON CONFLICT (user_id, admin_account_id, account_id) DO UPDATE SET
			original_priority = EXCLUDED.original_priority,
			last_applied_priority = EXCLUDED.last_applied_priority,
			pending_priority = EXCLUDED.pending_priority,
			stability_tier = EXCLUDED.stability_tier,
			observed_stability_tier = EXCLUDED.observed_stability_tier,
			observed_stability_rounds = EXCLUDED.observed_stability_rounds,
			observed_rank = EXCLUDED.observed_rank,
			observed_rank_rounds = EXCLUDED.observed_rank_rounds,
			sample_count = EXCLUDED.sample_count,
			success_rate = EXCLUDED.success_rate,
			latency_ms = EXCLUDED.latency_ms,
			effective_cost = EXCLUDED.effective_cost,
			short_error_count = EXCLUDED.short_error_count,
			short_error_rate = EXCLUDED.short_error_rate,
			last_upstream_error_at = EXCLUDED.last_upstream_error_at,
			last_upstream_error_class = EXCLUDED.last_upstream_error_class,
			degradation_reason = EXCLUDED.degradation_reason,
			clean_recovery_rounds = EXCLUDED.clean_recovery_rounds,
			last_priority_change_at = EXCLUDED.last_priority_change_at,
			cooldown_until = EXCLUDED.cooldown_until,
			last_observed_at = EXCLUDED.last_observed_at,
			conflict = EXCLUDED.conflict,
			updated_at = now()
	`, state.UserID, state.AdminAccountID, state.AccountID, state.OriginalPriority, state.LastAppliedPriority,
		state.PendingPriority, state.StabilityTier, state.ObservedStabilityTier, state.ObservedStabilityRounds,
		state.ObservedRank, state.ObservedRankRounds, state.SampleCount, state.SuccessRate, state.LatencyMs,
		state.EffectiveCost, state.ShortErrorCount, state.ShortErrorRate, state.LastUpstreamErrorAt,
		state.LastUpstreamErrorClass, state.DegradationReason, state.CleanRecoveryRounds,
		state.LastPriorityChangeAt, state.CooldownUntil, state.LastObservedAt, state.Conflict)
	return err
}

func (r *Repository) DeleteProfitPriorityState(ctx context.Context, userID, adminAccountID, accountID string) error {
	_, err := r.db.Exec(ctx, `
		DELETE FROM connection_health_profit_priority_states
		WHERE user_id = $1 AND admin_account_id = $2 AND account_id = $3
	`, userID, adminAccountID, accountID)
	return err
}
