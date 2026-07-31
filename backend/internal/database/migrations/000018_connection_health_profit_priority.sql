ALTER TABLE IF EXISTS connection_health_group_rate_monitor_settings
    ADD COLUMN IF NOT EXISTS profit_priority_enabled boolean NOT NULL DEFAULT false;

CREATE TABLE IF NOT EXISTS connection_health_profit_priority_states (
    user_id text NOT NULL,
    admin_account_id text NOT NULL DEFAULT '',
    account_id text NOT NULL,
    original_priority integer NOT NULL,
    last_applied_priority integer NOT NULL,
    pending_priority integer NULL,
    stability_tier text NOT NULL DEFAULT 'unknown',
    success_rate double precision NULL,
    latency_ms integer NULL,
    effective_cost double precision NULL,
    conflict boolean NOT NULL DEFAULT false,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, admin_account_id, account_id)
);

CREATE INDEX IF NOT EXISTS idx_connection_health_profit_priority_workspace
    ON connection_health_profit_priority_states (user_id, admin_account_id);
