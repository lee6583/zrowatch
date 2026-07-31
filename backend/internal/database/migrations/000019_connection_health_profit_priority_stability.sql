ALTER TABLE IF EXISTS connection_health_profit_priority_states
    ADD COLUMN IF NOT EXISTS observed_stability_tier text NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS observed_stability_rounds integer NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS observed_rank integer NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS observed_rank_rounds integer NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS sample_count integer NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS cooldown_until timestamptz NULL,
    ADD COLUMN IF NOT EXISTS last_observed_at timestamptz NULL;
