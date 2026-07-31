package connection_health

import (
	"context"
	"time"
)

type ProfitPriorityState struct {
	UserID              string
	AdminAccountID      string
	AccountID           string
	OriginalPriority    int
	LastAppliedPriority int
	PendingPriority     *int
	StabilityTier       string
	SuccessRate         *float64
	LatencyMs           *int
	EffectiveCost       *float64
	Conflict            bool
	UpdatedAt           time.Time
}

type profitPriorityRepository interface {
	ListProfitPriorityStates(ctx context.Context, userID, adminAccountID string) ([]ProfitPriorityState, error)
	UpsertProfitPriorityState(ctx context.Context, state ProfitPriorityState) error
	DeleteProfitPriorityState(ctx context.Context, userID, adminAccountID, accountID string) error
}

func (r *Repository) ListProfitPriorityStates(ctx context.Context, userID, adminAccountID string) ([]ProfitPriorityState, error) {
	rows, err := r.db.Query(ctx, `
		SELECT user_id, admin_account_id, account_id, original_priority, last_applied_priority,
			pending_priority, stability_tier, success_rate, latency_ms, effective_cost, conflict, updated_at
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
			&state.LastAppliedPriority, &state.PendingPriority, &state.StabilityTier, &state.SuccessRate,
			&state.LatencyMs, &state.EffectiveCost, &state.Conflict, &state.UpdatedAt); err != nil {
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
			pending_priority, stability_tier, success_rate, latency_ms, effective_cost, conflict, updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,now())
		ON CONFLICT (user_id, admin_account_id, account_id) DO UPDATE SET
			original_priority = EXCLUDED.original_priority,
			last_applied_priority = EXCLUDED.last_applied_priority,
			pending_priority = EXCLUDED.pending_priority,
			stability_tier = EXCLUDED.stability_tier,
			success_rate = EXCLUDED.success_rate,
			latency_ms = EXCLUDED.latency_ms,
			effective_cost = EXCLUDED.effective_cost,
			conflict = EXCLUDED.conflict,
			updated_at = now()
	`, state.UserID, state.AdminAccountID, state.AccountID, state.OriginalPriority, state.LastAppliedPriority,
		state.PendingPriority, state.StabilityTier, state.SuccessRate, state.LatencyMs, state.EffectiveCost, state.Conflict)
	return err
}

func (r *Repository) DeleteProfitPriorityState(ctx context.Context, userID, adminAccountID, accountID string) error {
	_, err := r.db.Exec(ctx, `
		DELETE FROM connection_health_profit_priority_states
		WHERE user_id = $1 AND admin_account_id = $2 AND account_id = $3
	`, userID, adminAccountID, accountID)
	return err
}
