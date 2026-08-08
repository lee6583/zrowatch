package my_sites

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// StateMutation mutates the locked latest my_site_states row before it is saved in the same transaction.
type StateMutation func(*State) error

type Repository struct {
	db *pgxpool.Pool
}

func NewRepository(db *pgxpool.Pool) *Repository {
	return &Repository{db: db}
}

func (r *Repository) EnsureSchema(ctx context.Context) error {
	_, err := r.db.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS my_site_states (
			user_id text NOT NULL,
			admin_account_id text NOT NULL DEFAULT '',
			base_url text NOT NULL,
			email text NOT NULL,
			session jsonb NOT NULL,
			mappings jsonb NOT NULL DEFAULT '[]'::jsonb,
			own_groups jsonb NOT NULL DEFAULT '[]'::jsonb,
			updated_at timestamptz NOT NULL DEFAULT now()
		)
	`)
	if err != nil {
		return err
	}
	_, err = r.db.Exec(ctx, `ALTER TABLE my_site_states ADD COLUMN IF NOT EXISTS own_groups jsonb NOT NULL DEFAULT '[]'::jsonb`)
	if err != nil {
		return err
	}
	statements := []string{
		`ALTER TABLE my_site_states ADD COLUMN IF NOT EXISTS admin_account_id text NOT NULL DEFAULT ''`,
		`ALTER TABLE my_site_states DROP CONSTRAINT IF EXISTS my_site_states_pkey`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_my_site_states_workspace ON my_site_states (user_id, admin_account_id)`,
	}
	for _, statement := range statements {
		if _, err := r.db.Exec(ctx, statement); err != nil {
			return err
		}
	}

	// real_connections 表存储真实对接的绑定记录：上游 key + admin 账号 + 自有分组关联。
	// 注意两个不同的 admin account 字段：
	//   - workspace_admin_account_id: TransitHub 工作区归属（对应 admin_accounts 表），
	//     用于 workspace 数据隔离，语义同其他业务表的 admin_account_id 列。
	//   - admin_account_id: 上游平台的 admin 转发账号 ID，是真实对接的业务字段。
	_, err = r.db.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS real_connections (
			id text PRIMARY KEY,
			user_id text NOT NULL,
			workspace_admin_account_id text NOT NULL DEFAULT '',
			upstream_site_id text NOT NULL,
			upstream_group_id text NOT NULL,
			upstream_group_name text NOT NULL,
			upstream_key_id text NOT NULL,
			upstream_key text NOT NULL,
			admin_account_id text NOT NULL,
			admin_account_name text NOT NULL,
			own_group_ids jsonb NOT NULL DEFAULT '[]'::jsonb,
			group_type text NOT NULL,
			created_at timestamptz NOT NULL DEFAULT now()
		)
	`)
	if err != nil {
		return err
	}
	_, err = r.db.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS real_connection_cost_guard_pauses (
			user_id text NOT NULL,
			workspace_admin_account_id text NOT NULL DEFAULT '',
			connection_id text NOT NULL,
			upstream_site_id text NOT NULL,
			upstream_group_id text NOT NULL DEFAULT '',
			upstream_group_name text NOT NULL DEFAULT '',
			own_group_id text NOT NULL,
			own_group_name text NOT NULL DEFAULT '',
			last_error text NOT NULL DEFAULT '',
			updated_at timestamptz NOT NULL DEFAULT now(),
			PRIMARY KEY (user_id, workspace_admin_account_id, connection_id, own_group_id)
		)
	`)
	if err != nil {
		return err
	}
	statements = []string{
		`ALTER TABLE real_connections ADD COLUMN IF NOT EXISTS workspace_admin_account_id text NOT NULL DEFAULT ''`,
		`ALTER TABLE real_connections ADD COLUMN IF NOT EXISTS own_group_names jsonb NOT NULL DEFAULT '[]'::jsonb`,
		`ALTER TABLE real_connections ADD COLUMN IF NOT EXISTS provisioning_mode text NOT NULL DEFAULT 'legacy'`,
		`ALTER TABLE real_connections ADD COLUMN IF NOT EXISTS status text NOT NULL DEFAULT 'active'`,
		`ALTER TABLE real_connections ADD COLUMN IF NOT EXISTS upstream_platform text NOT NULL DEFAULT ''`,
		`ALTER TABLE real_connections ADD COLUMN IF NOT EXISTS admin_platform text NOT NULL DEFAULT ''`,
		`ALTER TABLE real_connections ADD COLUMN IF NOT EXISTS pricing_mapping_enabled boolean NOT NULL DEFAULT true`,
		`ALTER TABLE real_connections ADD COLUMN IF NOT EXISTS operation_id text NOT NULL DEFAULT ''`,
		`CREATE INDEX IF NOT EXISTS idx_real_connections_workspace_group_id ON real_connections (user_id, workspace_admin_account_id, upstream_site_id, upstream_group_id)`,
		`CREATE INDEX IF NOT EXISTS idx_real_connections_workspace_group_name ON real_connections (user_id, workspace_admin_account_id, upstream_site_id, upstream_group_name)`,
		`CREATE INDEX IF NOT EXISTS idx_real_connections_own_group_ids ON real_connections USING GIN (own_group_ids)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_real_connections_operation ON real_connections (user_id, workspace_admin_account_id, operation_id) WHERE operation_id <> ''`,
	}
	for _, statement := range statements {
		if _, err := r.db.Exec(ctx, statement); err != nil {
			return err
		}
	}
	_, err = r.db.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS downstream_consumption_ledger (
			user_id text NOT NULL,
			workspace_admin_account_id text NOT NULL,
			upstream_site_id text NOT NULL,
			admin_account_id text NOT NULL,
			accumulated_amount double precision NOT NULL DEFAULT 0,
			observed_total double precision NOT NULL DEFAULT 0,
			observed_at timestamptz NOT NULL DEFAULT now(),
			created_at timestamptz NOT NULL DEFAULT now(),
			updated_at timestamptz NOT NULL DEFAULT now(),
			PRIMARY KEY (user_id, workspace_admin_account_id, upstream_site_id, admin_account_id)
		)
	`)
	if err != nil {
		return err
	}
	_, err = r.db.Exec(ctx, `
		CREATE INDEX IF NOT EXISTS idx_downstream_consumption_ledger_workspace
		ON downstream_consumption_ledger (user_id, workspace_admin_account_id, upstream_site_id)
	`)
	if err != nil {
		return err
	}
	return nil
}

func (r *Repository) ListDownstreamConsumptionLedger(ctx context.Context, userID, adminAccountID string) ([]DownstreamConsumptionLedgerEntry, error) {
	rows, err := r.db.Query(ctx, `
		SELECT user_id, workspace_admin_account_id, upstream_site_id, admin_account_id,
		       accumulated_amount, observed_total, observed_at
		FROM downstream_consumption_ledger
		WHERE user_id = $1 AND workspace_admin_account_id = $2
	`, userID, adminAccountID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	entries := make([]DownstreamConsumptionLedgerEntry, 0)
	for rows.Next() {
		var entry DownstreamConsumptionLedgerEntry
		if err := rows.Scan(
			&entry.UserID,
			&entry.WorkspaceAdminID,
			&entry.SiteID,
			&entry.AccountID,
			&entry.AccumulatedAmount,
			&entry.ObservedTotal,
			&entry.ObservedAt,
		); err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return entries, rows.Err()
}

// ObserveDownstreamConsumption atomically advances one account's local
// cumulative amount. A lower live total is treated as upstream retention
// reset, never as a negative charge. observed_at prevents an older concurrent
// response from moving the baseline backwards.
func (r *Repository) ObserveDownstreamConsumption(ctx context.Context, entry DownstreamConsumptionLedgerEntry) (float64, error) {
	var accumulated float64
	err := r.db.QueryRow(ctx, `
		INSERT INTO downstream_consumption_ledger (
			user_id, workspace_admin_account_id, upstream_site_id, admin_account_id,
			accumulated_amount, observed_total, observed_at
		)
		VALUES ($1, $2, $3, $4, $5, $5, $6)
		ON CONFLICT (user_id, workspace_admin_account_id, upstream_site_id, admin_account_id)
		DO UPDATE SET
			accumulated_amount = CASE
				WHEN EXCLUDED.observed_at < downstream_consumption_ledger.observed_at
					THEN downstream_consumption_ledger.accumulated_amount
				WHEN EXCLUDED.observed_total >= downstream_consumption_ledger.observed_total
					THEN downstream_consumption_ledger.accumulated_amount +
						(EXCLUDED.observed_total - downstream_consumption_ledger.observed_total)
				ELSE downstream_consumption_ledger.accumulated_amount
			END,
			observed_total = CASE
				WHEN EXCLUDED.observed_at >= downstream_consumption_ledger.observed_at
					THEN EXCLUDED.observed_total
				ELSE downstream_consumption_ledger.observed_total
			END,
			observed_at = GREATEST(downstream_consumption_ledger.observed_at, EXCLUDED.observed_at),
			updated_at = now()
		RETURNING accumulated_amount
	`, entry.UserID, entry.WorkspaceAdminID, entry.SiteID, entry.AccountID, entry.ObservedTotal, entry.ObservedAt).Scan(&accumulated)
	return accumulated, err
}

func (r *Repository) ListDownstreamConsumptionScopes(ctx context.Context) ([]DownstreamConsumptionScope, error) {
	rows, err := r.db.Query(ctx, `
		SELECT DISTINCT user_id, workspace_admin_account_id
		FROM real_connections
		WHERE user_id <> '' AND workspace_admin_account_id <> ''
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	scopes := make([]DownstreamConsumptionScope, 0)
	for rows.Next() {
		var scope DownstreamConsumptionScope
		if err := rows.Scan(&scope.UserID, &scope.WorkspaceAdminID); err != nil {
			return nil, err
		}
		scopes = append(scopes, scope)
	}
	return scopes, rows.Err()
}

func (r *Repository) Get(ctx context.Context, userID string, adminAccountID string) (*State, error) {
	return scanState(r.db.QueryRow(ctx, `SELECT user_id, admin_account_id, base_url, email, session, mappings, own_groups FROM my_site_states WHERE user_id = $1 AND admin_account_id = $2`, userID, adminAccountID))
}

type stateScanner interface {
	Scan(dest ...any) error
}

func scanState(row stateScanner) (*State, error) {
	var state State
	var sessionJSON []byte
	var mappingsJSON []byte
	var ownGroupsJSON []byte
	if err := row.Scan(&state.UserID, &state.AdminAccountID, &state.BaseURL, &state.Email, &sessionJSON, &mappingsJSON, &ownGroupsJSON); err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	if err := json.Unmarshal(sessionJSON, &state.Session); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(mappingsJSON, &state.Mappings); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(ownGroupsJSON, &state.OwnGroups); err != nil {
		return nil, err
	}
	return &state, nil
}

func marshalStateJSON(state State) (sessionJSON, mappingsJSON, ownGroupsJSON []byte, err error) {
	sessionJSON, err = json.Marshal(state.Session)
	if err != nil {
		return nil, nil, nil, err
	}
	mappingsJSON, err = json.Marshal(state.Mappings)
	if err != nil {
		return nil, nil, nil, err
	}
	ownGroupsJSON, err = json.Marshal(state.OwnGroups)
	if err != nil {
		return nil, nil, nil, err
	}
	return sessionJSON, mappingsJSON, ownGroupsJSON, nil
}

func (r *Repository) Save(ctx context.Context, state State) error {
	sessionJSON, mappingsJSON, ownGroupsJSON, err := marshalStateJSON(state)
	if err != nil {
		return err
	}
	_, err = r.db.Exec(ctx, `
		INSERT INTO my_site_states (user_id, admin_account_id, base_url, email, session, mappings, own_groups, updated_at)
		VALUES ($1, $2, $3, $4, $5::jsonb, $6::jsonb, $7::jsonb, now())
		ON CONFLICT (user_id, admin_account_id) DO UPDATE SET
			base_url = EXCLUDED.base_url,
			email = EXCLUDED.email,
			session = EXCLUDED.session,
			mappings = EXCLUDED.mappings,
			own_groups = EXCLUDED.own_groups,
			updated_at = EXCLUDED.updated_at
	`, state.UserID, state.AdminAccountID, state.BaseURL, state.Email, string(sessionJSON), string(mappingsJSON), string(ownGroupsJSON))
	return err
}

// MutateState locks one workspace row and saves the caller's mutation in the same transaction.
// Network calls must happen before this method so the lock is held only for the local JSON merge/write.
func (r *Repository) MutateState(ctx context.Context, userID string, adminAccountID string, mutate StateMutation) (*State, error) {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return nil, err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()

	state, err := scanState(tx.QueryRow(ctx, `SELECT user_id, admin_account_id, base_url, email, session, mappings, own_groups FROM my_site_states WHERE user_id = $1 AND admin_account_id = $2 FOR UPDATE`, userID, adminAccountID))
	if err != nil {
		return nil, err
	}
	if state == nil {
		return nil, nil
	}
	if err := mutate(state); err != nil {
		return nil, err
	}
	sessionJSON, mappingsJSON, ownGroupsJSON, err := marshalStateJSON(*state)
	if err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE my_site_states
		SET base_url = $3,
			email = $4,
			session = $5::jsonb,
			mappings = $6::jsonb,
			own_groups = $7::jsonb,
			updated_at = now()
		WHERE user_id = $1 AND admin_account_id = $2
	`, state.UserID, state.AdminAccountID, state.BaseURL, state.Email, string(sessionJSON), string(mappingsJSON), string(ownGroupsJSON)); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	committed = true
	return state, nil
}

// SaveRealConnection 持久化一条真实对接绑定记录。
func (r *Repository) SaveRealConnection(ctx context.Context, conn RealConnection) error {
	ownGroupIDsJSON, err := json.Marshal(conn.OwnGroupIDs)
	if err != nil {
		return err
	}
	ownGroupNamesJSON, err := json.Marshal(conn.OwnGroupNames)
	if err != nil {
		return err
	}
	_, err = r.db.Exec(ctx, `
		INSERT INTO real_connections (
			id, user_id, workspace_admin_account_id, upstream_site_id, upstream_group_id,
			upstream_group_name, upstream_key_id, upstream_key, admin_account_id,
			admin_account_name, own_group_ids, own_group_names, group_type, provisioning_mode,
			status, upstream_platform, admin_platform, pricing_mapping_enabled, operation_id, created_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb, $12::jsonb, $13, $14, $15, $16, $17, $18, $19, $20)
	`, conn.ID, conn.UserID, conn.WorkspaceAdminAccountID, conn.UpstreamSiteID, conn.UpstreamGroupID, conn.UpstreamGroupName,
		conn.UpstreamKeyID, conn.UpstreamKey, conn.AdminAccountID, conn.AdminAccountName,
		string(ownGroupIDsJSON), string(ownGroupNamesJSON), conn.GroupType, conn.ProvisioningMode,
		conn.Status, conn.UpstreamPlatform, conn.AdminPlatform, conn.PricingMappingEnabled, conn.OperationID, conn.CreatedAt)
	return err
}

// SaveRealConnectionWithPricingMapping writes the connection and its optional
// pricing source in one transaction. Remote resources are created before this
// call; an error therefore lets the service compensate both remote creations.
func (r *Repository) SaveRealConnectionWithPricingMapping(ctx context.Context, conn RealConnection) error {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()

	if conn.PricingMappingEnabled {
		state, err := scanState(tx.QueryRow(ctx, `SELECT user_id, admin_account_id, base_url, email, session, mappings, own_groups FROM my_site_states WHERE user_id = $1 AND admin_account_id = $2 FOR UPDATE`, conn.UserID, conn.WorkspaceAdminAccountID))
		if err != nil {
			return err
		}
		if state == nil {
			return fmt.Errorf("save real connection: workspace state not found")
		}
		addMappingTargetForOwnGroups(state, conn.OwnGroupNames, UpstreamGroupRef{SiteID: conn.UpstreamSiteID, GroupName: conn.UpstreamGroupName})
		if err := updateStateInTx(ctx, tx, *state); err != nil {
			return err
		}
	}

	if err := insertRealConnection(ctx, tx, conn); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	committed = true
	return nil
}

func insertRealConnection(ctx context.Context, tx pgx.Tx, conn RealConnection) error {
	ownGroupIDsJSON, err := json.Marshal(conn.OwnGroupIDs)
	if err != nil {
		return err
	}
	ownGroupNamesJSON, err := json.Marshal(conn.OwnGroupNames)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO real_connections (
			id, user_id, workspace_admin_account_id, upstream_site_id, upstream_group_id,
			upstream_group_name, upstream_key_id, upstream_key, admin_account_id,
			admin_account_name, own_group_ids, own_group_names, group_type, provisioning_mode,
			status, upstream_platform, admin_platform, pricing_mapping_enabled, operation_id, created_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb, $12::jsonb, $13, $14, $15, $16, $17, $18, $19, $20)
	`, conn.ID, conn.UserID, conn.WorkspaceAdminAccountID, conn.UpstreamSiteID, conn.UpstreamGroupID, conn.UpstreamGroupName,
		conn.UpstreamKeyID, conn.UpstreamKey, conn.AdminAccountID, conn.AdminAccountName,
		string(ownGroupIDsJSON), string(ownGroupNamesJSON), conn.GroupType, conn.ProvisioningMode,
		conn.Status, conn.UpstreamPlatform, conn.AdminPlatform, conn.PricingMappingEnabled, conn.OperationID, conn.CreatedAt)
	return err
}

func updateStateInTx(ctx context.Context, tx pgx.Tx, state State) error {
	sessionJSON, mappingsJSON, ownGroupsJSON, err := marshalStateJSON(state)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		UPDATE my_site_states
		SET base_url = $3, email = $4, session = $5::jsonb, mappings = $6::jsonb,
			own_groups = $7::jsonb, updated_at = now()
		WHERE user_id = $1 AND admin_account_id = $2
	`, state.UserID, state.AdminAccountID, state.BaseURL, state.Email, string(sessionJSON), string(mappingsJSON), string(ownGroupsJSON))
	return err
}

func addMappingTargetForOwnGroups(state *State, ownGroupNames []string, target UpstreamGroupRef) {
	if state == nil {
		return
	}
	wanted := make(map[string]struct{}, len(ownGroupNames))
	for _, name := range ownGroupNames {
		if name != "" {
			wanted[name] = struct{}{}
		}
	}
	for i := range state.Mappings {
		delete(wanted, state.Mappings[i].OwnGroup)
		if state.Mappings[i].OwnGroup != "" && containsString(ownGroupNames, state.Mappings[i].OwnGroup) && !hasUpstreamTarget(state.Mappings[i].UpstreamTargets, target) {
			state.Mappings[i].UpstreamTargets = append(state.Mappings[i].UpstreamTargets, target)
		}
	}
	for name := range wanted {
		state.Mappings = append(state.Mappings, GroupMapping{OwnGroup: name, UpstreamTargets: []UpstreamGroupRef{target}})
	}
}

func removeMappingTargetForOwnGroups(state *State, ownGroupNames []string, target UpstreamGroupRef) {
	if state == nil {
		return
	}
	wanted := make(map[string]struct{}, len(ownGroupNames))
	for _, name := range ownGroupNames {
		wanted[name] = struct{}{}
	}
	for i := range state.Mappings {
		if _, ok := wanted[state.Mappings[i].OwnGroup]; !ok {
			continue
		}
		filtered := state.Mappings[i].UpstreamTargets[:0]
		for _, existing := range state.Mappings[i].UpstreamTargets {
			if existing.SiteID != target.SiteID || existing.GroupName != target.GroupName {
				filtered = append(filtered, existing)
			}
		}
		state.Mappings[i].UpstreamTargets = filtered
	}
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

type realConnectionScanner interface {
	Scan(dest ...any) error
}

func scanRealConnection(row realConnectionScanner) (*RealConnection, error) {
	var conn RealConnection
	var ownGroupIDsJSON []byte
	var ownGroupNamesJSON []byte
	var createdAt time.Time
	if err := row.Scan(
		&conn.ID, &conn.UserID, &conn.WorkspaceAdminAccountID, &conn.UpstreamSiteID,
		&conn.UpstreamGroupID, &conn.UpstreamGroupName, &conn.UpstreamKeyID,
		&conn.UpstreamKey, &conn.AdminAccountID, &conn.AdminAccountName,
		&ownGroupIDsJSON, &ownGroupNamesJSON, &conn.GroupType, &conn.ProvisioningMode,
		&conn.Status, &conn.UpstreamPlatform, &conn.AdminPlatform,
		&conn.PricingMappingEnabled, &conn.OperationID, &createdAt,
	); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(ownGroupIDsJSON, &conn.OwnGroupIDs); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(ownGroupNamesJSON, &conn.OwnGroupNames); err != nil {
		return nil, err
	}
	if conn.OwnGroupIDs == nil {
		conn.OwnGroupIDs = []string{}
	}
	if conn.OwnGroupNames == nil {
		conn.OwnGroupNames = []string{}
	}
	conn.CanDeleteRemote = conn.ProvisioningMode == ProvisioningModeManaged
	conn.CreatedAt = createdAt.Format(time.RFC3339)
	return &conn, nil
}

// ListRealConnections 查询指定用户的所有真实对接绑定记录，按创建时间倒序。
func (r *Repository) ListRealConnections(ctx context.Context, userID string, adminAccountID string) ([]RealConnection, error) {
	rows, err := r.db.Query(ctx, `
		SELECT id, user_id, workspace_admin_account_id, upstream_site_id, upstream_group_id, upstream_group_name,
		       upstream_key_id, upstream_key, admin_account_id, admin_account_name,
		       own_group_ids, own_group_names, group_type, provisioning_mode, status,
		       upstream_platform, admin_platform, pricing_mapping_enabled, operation_id, created_at
		FROM real_connections WHERE user_id = $1 AND workspace_admin_account_id = $2 ORDER BY created_at DESC
	`, userID, adminAccountID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var connections []RealConnection
	for rows.Next() {
		conn, err := scanRealConnection(rows)
		if err != nil {
			return nil, err
		}
		connections = append(connections, *conn)
	}
	return connections, rows.Err()
}

// GetRealConnection 根据 ID 和用户 ID 查询单条真实对接绑定记录。
func (r *Repository) GetRealConnection(ctx context.Context, id string, userID string, adminAccountID string) (*RealConnection, error) {
	row := r.db.QueryRow(ctx, `
		SELECT id, user_id, workspace_admin_account_id, upstream_site_id, upstream_group_id, upstream_group_name,
		       upstream_key_id, upstream_key, admin_account_id, admin_account_name,
		       own_group_ids, own_group_names, group_type, provisioning_mode, status,
		       upstream_platform, admin_platform, pricing_mapping_enabled, operation_id, created_at
		FROM real_connections WHERE id = $1 AND user_id = $2 AND workspace_admin_account_id = $3
	`, id, userID, adminAccountID)
	conn, err := scanRealConnection(row)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return conn, nil
}

func (r *Repository) UpdateRealConnectionAdminAccountName(ctx context.Context, id string, userID string, adminAccountID string, name string) error {
	_, err := r.db.Exec(ctx, `
		UPDATE real_connections
		SET admin_account_name = $1
		WHERE id = $2 AND user_id = $3 AND workspace_admin_account_id = $4
	`, name, id, userID, adminAccountID)
	return err
}

// GetRealConnectionByOperationID supports retry-safe connect requests. The
// partial unique index guarantees at most one non-empty operation ID per workspace.
func (r *Repository) GetRealConnectionByOperationID(ctx context.Context, userID string, adminAccountID string, operationID string) (*RealConnection, error) {
	if operationID == "" {
		return nil, nil
	}
	row := r.db.QueryRow(ctx, `
		SELECT id, user_id, workspace_admin_account_id, upstream_site_id, upstream_group_id, upstream_group_name,
		       upstream_key_id, upstream_key, admin_account_id, admin_account_name,
		       own_group_ids, own_group_names, group_type, provisioning_mode, status,
		       upstream_platform, admin_platform, pricing_mapping_enabled, operation_id, created_at
		FROM real_connections WHERE user_id = $1 AND workspace_admin_account_id = $2 AND operation_id = $3
	`, userID, adminAccountID, operationID)
	conn, err := scanRealConnection(row)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	return conn, err
}

// DeleteRealConnection 根据 ID 和用户 ID 删除一条真实对接绑定记录。
func (r *Repository) DeleteRealConnection(ctx context.Context, id string, userID string, adminAccountID string) error {
	_, err := r.db.Exec(ctx, `DELETE FROM real_connections WHERE id = $1 AND user_id = $2 AND workspace_admin_account_id = $3`, id, userID, adminAccountID)
	return err
}

// DeleteRealConnectionWithPricingMapping removes only the mappings created for
// this connection. If another pricing-enabled connection still references the
// same target, its shared mapping is preserved.
func (r *Repository) DeleteRealConnectionWithPricingMapping(ctx context.Context, conn RealConnection, removePricingMapping bool) error {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()

	if removePricingMapping && conn.PricingMappingEnabled {
		var shared bool
		if err := tx.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM real_connections
				WHERE user_id = $1 AND workspace_admin_account_id = $2 AND id <> $3
					AND pricing_mapping_enabled
					AND upstream_site_id = $4 AND upstream_group_name = $5
			)
		`, conn.UserID, conn.WorkspaceAdminAccountID, conn.ID, conn.UpstreamSiteID, conn.UpstreamGroupName).Scan(&shared); err != nil {
			return err
		}
		if !shared {
			state, err := scanState(tx.QueryRow(ctx, `SELECT user_id, admin_account_id, base_url, email, session, mappings, own_groups FROM my_site_states WHERE user_id = $1 AND admin_account_id = $2 FOR UPDATE`, conn.UserID, conn.WorkspaceAdminAccountID))
			if err != nil {
				return err
			}
			if state != nil {
				removeMappingTargetForOwnGroups(state, conn.OwnGroupNames, UpstreamGroupRef{SiteID: conn.UpstreamSiteID, GroupName: conn.UpstreamGroupName})
				if err := updateStateInTx(ctx, tx, *state); err != nil {
					return err
				}
			}
		}
	}
	commandTag, err := tx.Exec(ctx, `DELETE FROM real_connections WHERE id = $1 AND user_id = $2 AND workspace_admin_account_id = $3`, conn.ID, conn.UserID, conn.WorkspaceAdminAccountID)
	if err != nil {
		return err
	}
	if commandTag.RowsAffected() == 0 {
		return fmt.Errorf("delete real connection: no rows affected")
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	committed = true
	return nil
}

// PartialDisconnectRealConnection removes selected downstream groups while
// retaining the connection row and its remote account/key.
func (r *Repository) PartialDisconnectRealConnection(ctx context.Context, conn RealConnection, remainingGroupIDs, remainingGroupNames, removedGroupNames []string, removePricingMapping bool) error {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()

	if removePricingMapping && conn.PricingMappingEnabled {
		state, err := scanState(tx.QueryRow(ctx, `SELECT user_id, admin_account_id, base_url, email, session, mappings, own_groups FROM my_site_states WHERE user_id = $1 AND admin_account_id = $2 FOR UPDATE`, conn.UserID, conn.WorkspaceAdminAccountID))
		if err != nil {
			return err
		}
		if state != nil {
			for _, name := range removedGroupNames {
				var shared bool
				if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM real_connections WHERE user_id = $1 AND workspace_admin_account_id = $2 AND id <> $3 AND pricing_mapping_enabled AND upstream_site_id = $4 AND upstream_group_name = $5 AND own_group_names ? $6)`, conn.UserID, conn.WorkspaceAdminAccountID, conn.ID, conn.UpstreamSiteID, conn.UpstreamGroupName, name).Scan(&shared); err != nil {
					return err
				}
				if !shared {
					removeMappingTargetForOwnGroups(state, []string{name}, UpstreamGroupRef{SiteID: conn.UpstreamSiteID, GroupName: conn.UpstreamGroupName})
				}
			}
			if err := updateStateInTx(ctx, tx, *state); err != nil {
				return err
			}
		}
	}
	idsJSON, err := json.Marshal(remainingGroupIDs)
	if err != nil {
		return err
	}
	namesJSON, err := json.Marshal(remainingGroupNames)
	if err != nil {
		return err
	}
	commandTag, err := tx.Exec(ctx, `UPDATE real_connections SET own_group_ids = $1::jsonb, own_group_names = $2::jsonb WHERE id = $3 AND user_id = $4 AND workspace_admin_account_id = $5`, string(idsJSON), string(namesJSON), conn.ID, conn.UserID, conn.WorkspaceAdminAccountID)
	if err != nil {
		return err
	}
	if commandTag.RowsAffected() == 0 {
		return fmt.Errorf("partial disconnect: no rows affected")
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	committed = true
	return nil
}

func (r *Repository) UpdateRealConnectionGroups(ctx context.Context, conn RealConnection, groupIDs, groupNames, addedGroupNames, removedGroupNames []string) error {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()

	if conn.PricingMappingEnabled {
		state, err := scanState(tx.QueryRow(ctx, `SELECT user_id, admin_account_id, base_url, email, session, mappings, own_groups FROM my_site_states WHERE user_id = $1 AND admin_account_id = $2 FOR UPDATE`, conn.UserID, conn.WorkspaceAdminAccountID))
		if err != nil {
			return err
		}
		if state != nil {
			target := UpstreamGroupRef{SiteID: conn.UpstreamSiteID, GroupName: conn.UpstreamGroupName}
			addMappingTargetForOwnGroups(state, addedGroupNames, target)
			for _, name := range removedGroupNames {
				var shared bool
				if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM real_connections WHERE user_id = $1 AND workspace_admin_account_id = $2 AND id <> $3 AND pricing_mapping_enabled AND upstream_site_id = $4 AND upstream_group_name = $5 AND own_group_names ? $6)`, conn.UserID, conn.WorkspaceAdminAccountID, conn.ID, conn.UpstreamSiteID, conn.UpstreamGroupName, name).Scan(&shared); err != nil {
					return err
				}
				if !shared {
					removeMappingTargetForOwnGroups(state, []string{name}, target)
				}
			}
			if err := updateStateInTx(ctx, tx, *state); err != nil {
				return err
			}
		}
	}

	idsJSON, err := json.Marshal(groupIDs)
	if err != nil {
		return err
	}
	namesJSON, err := json.Marshal(groupNames)
	if err != nil {
		return err
	}
	commandTag, err := tx.Exec(ctx, `UPDATE real_connections SET own_group_ids = $1::jsonb, own_group_names = $2::jsonb WHERE id = $3 AND user_id = $4 AND workspace_admin_account_id = $5`, string(idsJSON), string(namesJSON), conn.ID, conn.UserID, conn.WorkspaceAdminAccountID)
	if err != nil {
		return err
	}
	if commandTag.RowsAffected() == 0 {
		return fmt.Errorf("update real connection groups: no rows affected")
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	committed = true
	return nil
}

// ListCostGuardPauses 返回当前 workspace 下由亏本保护临时移除的下游分组记录。
func (r *Repository) ListCostGuardPauses(ctx context.Context, userID, adminAccountID string) ([]CostGuardPause, error) {
	rows, err := r.db.Query(ctx, `
		SELECT user_id, workspace_admin_account_id, connection_id, upstream_site_id, upstream_group_id,
			upstream_group_name, own_group_id, own_group_name, last_error, updated_at
		FROM real_connection_cost_guard_pauses
		WHERE user_id = $1 AND workspace_admin_account_id = $2
		ORDER BY updated_at DESC, connection_id, own_group_id
	`, userID, adminAccountID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]CostGuardPause, 0)
	for rows.Next() {
		var pause CostGuardPause
		if err := rows.Scan(&pause.UserID, &pause.WorkspaceAdminAccountID, &pause.ConnectionID, &pause.UpstreamSiteID,
			&pause.UpstreamGroupID, &pause.UpstreamGroupName, &pause.OwnGroupID, &pause.OwnGroupName, &pause.LastError, &pause.UpdatedAt); err != nil {
			return nil, err
		}
		result = append(result, pause)
	}
	return result, rows.Err()
}

// UpsertCostGuardPause 写入或刷新一条亏本保护暂停记录。
func (r *Repository) UpsertCostGuardPause(ctx context.Context, pause CostGuardPause) error {
	_, err := r.db.Exec(ctx, `
		INSERT INTO real_connection_cost_guard_pauses (
			user_id, workspace_admin_account_id, connection_id, upstream_site_id,
			upstream_group_id, upstream_group_name, own_group_id, own_group_name, last_error, updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,now())
		ON CONFLICT (user_id, workspace_admin_account_id, connection_id, own_group_id) DO UPDATE SET
			upstream_site_id = EXCLUDED.upstream_site_id,
			upstream_group_id = EXCLUDED.upstream_group_id,
			upstream_group_name = EXCLUDED.upstream_group_name,
			own_group_name = EXCLUDED.own_group_name,
			last_error = EXCLUDED.last_error,
			updated_at = now()
	`, pause.UserID, pause.WorkspaceAdminAccountID, pause.ConnectionID, pause.UpstreamSiteID,
		pause.UpstreamGroupID, pause.UpstreamGroupName, pause.OwnGroupID, pause.OwnGroupName, pause.LastError)
	return err
}

// DeleteCostGuardPause 删除某条连接下单个下游分组的暂停记录。
func (r *Repository) DeleteCostGuardPause(ctx context.Context, userID, adminAccountID, connectionID, ownGroupID string) error {
	_, err := r.db.Exec(ctx, `
		DELETE FROM real_connection_cost_guard_pauses
		WHERE user_id = $1 AND workspace_admin_account_id = $2 AND connection_id = $3 AND own_group_id = $4
	`, userID, adminAccountID, connectionID, ownGroupID)
	return err
}

// DeleteCostGuardPausesForConnection 删除某条连接下全部暂停记录。
func (r *Repository) DeleteCostGuardPausesForConnection(ctx context.Context, userID, adminAccountID, connectionID string) error {
	_, err := r.db.Exec(ctx, `
		DELETE FROM real_connection_cost_guard_pauses
		WHERE user_id = $1 AND workspace_admin_account_id = $2 AND connection_id = $3
	`, userID, adminAccountID, connectionID)
	return err
}

// RemoveUpstreamMappingAndDeleteConnection atomically removes the mapping target and local connection row.
func (r *Repository) RemoveUpstreamMappingAndDeleteConnection(ctx context.Context, userID string, adminAccountID string, connectionID string, siteID string, groupName string) error {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()

	state, err := scanState(tx.QueryRow(ctx, `SELECT user_id, admin_account_id, base_url, email, session, mappings, own_groups FROM my_site_states WHERE user_id = $1 AND admin_account_id = $2 FOR UPDATE`, userID, adminAccountID))
	if err != nil {
		return err
	}
	if state != nil {
		removeMappingTargetFromState(state, siteID, groupName)
		sessionJSON, mappingsJSON, ownGroupsJSON, err := marshalStateJSON(*state)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			UPDATE my_site_states
			SET base_url = $3,
				email = $4,
				session = $5::jsonb,
				mappings = $6::jsonb,
				own_groups = $7::jsonb,
				updated_at = now()
			WHERE user_id = $1 AND admin_account_id = $2
		`, state.UserID, state.AdminAccountID, state.BaseURL, state.Email, string(sessionJSON), string(mappingsJSON), string(ownGroupsJSON)); err != nil {
			return err
		}
	}
	commandTag, err := tx.Exec(ctx, `DELETE FROM real_connections WHERE id = $1 AND user_id = $2 AND workspace_admin_account_id = $3`, connectionID, userID, adminAccountID)
	if err != nil {
		return err
	}
	if commandTag.RowsAffected() == 0 {
		return fmt.Errorf("delete real connection: no rows affected")
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	committed = true
	return nil
}
