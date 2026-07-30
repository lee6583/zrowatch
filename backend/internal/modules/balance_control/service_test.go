package balance_control

import (
	"context"
	"errors"
	"testing"

	"transithub/backend/internal/modules/my_sites"
	"transithub/backend/internal/modules/settings"
	"transithub/backend/internal/modules/upstream"
)

type memoryPauseRepository struct {
	records        map[string]PauseRecord
	cycles         map[string]ProfitCycle
	profitAccounts map[string]ProfitCycleAccount
}

func newMemoryPauseRepository() *memoryPauseRepository {
	return &memoryPauseRepository{
		records: make(map[string]PauseRecord), cycles: make(map[string]ProfitCycle),
		profitAccounts: make(map[string]ProfitCycleAccount),
	}
}

func pauseRecordKey(userID, adminAccountID, siteID, accountID string) string {
	return userID + "|" + adminAccountID + "|" + siteID + "|" + accountID
}

func (r *memoryPauseRepository) EnsureSchema(context.Context) error { return nil }

func (r *memoryPauseRepository) UpsertPending(_ context.Context, record PauseRecord) error {
	key := pauseRecordKey(record.UserID, record.AdminAccountID, record.UpstreamSiteID, record.AdminAccountIDRemote)
	if existing, ok := r.records[key]; ok {
		record.OriginalStatus = existing.OriginalStatus
		record.OriginalSchedulable = existing.OriginalSchedulable
		record.Applied = existing.Applied
	}
	r.records[key] = record
	return nil
}

func (r *memoryPauseRepository) MarkApplied(_ context.Context, userID, adminAccountID, siteID, remoteAccountID string) error {
	key := pauseRecordKey(userID, adminAccountID, siteID, remoteAccountID)
	record := r.records[key]
	record.Applied = true
	record.LastError = ""
	r.records[key] = record
	return nil
}

func (r *memoryPauseRepository) SaveError(_ context.Context, userID, adminAccountID, siteID, remoteAccountID, message string) error {
	key := pauseRecordKey(userID, adminAccountID, siteID, remoteAccountID)
	record := r.records[key]
	record.LastError = message
	r.records[key] = record
	return nil
}

func (r *memoryPauseRepository) ListForSite(_ context.Context, userID, adminAccountID, siteID string, appliedOnly bool) ([]PauseRecord, error) {
	result := make([]PauseRecord, 0)
	for _, record := range r.records {
		if record.UserID != userID || record.AdminAccountID != adminAccountID || record.UpstreamSiteID != siteID || appliedOnly && !record.Applied {
			continue
		}
		result = append(result, record)
	}
	return result, nil
}

func (r *memoryPauseRepository) Delete(_ context.Context, userID, adminAccountID, siteID, remoteAccountID string) error {
	delete(r.records, pauseRecordKey(userID, adminAccountID, siteID, remoteAccountID))
	return nil
}

func (r *memoryPauseRepository) IsAccountPausedForWorkspace(_ context.Context, userID, adminAccountID, remoteAccountID string) (bool, error) {
	for _, record := range r.records {
		if record.UserID == userID && record.AdminAccountID == adminAccountID && record.AdminAccountIDRemote == remoteAccountID {
			return true, nil
		}
	}
	return false, nil
}

func profitCycleKey(userID, adminAccountID, siteID string) string {
	return userID + "|" + adminAccountID + "|" + siteID
}

func profitAccountKey(userID, adminAccountID, siteID, accountID string) string {
	return profitCycleKey(userID, adminAccountID, siteID) + "|" + accountID
}

func (r *memoryPauseRepository) GetProfitCycle(_ context.Context, userID, adminAccountID, siteID string) (*ProfitCycle, error) {
	cycle, ok := r.cycles[profitCycleKey(userID, adminAccountID, siteID)]
	if !ok {
		return nil, nil
	}
	copy := cycle
	return &copy, nil
}

func (r *memoryPauseRepository) StartProfitCycle(_ context.Context, cycle ProfitCycle) error {
	key := profitCycleKey(cycle.UserID, cycle.AdminAccountID, cycle.UpstreamSiteID)
	r.cycles[key] = cycle
	for accountKey, account := range r.profitAccounts {
		if profitCycleKey(account.UserID, account.AdminAccountID, account.UpstreamSiteID) == key {
			delete(r.profitAccounts, accountKey)
		}
	}
	return nil
}

func (r *memoryPauseRepository) AddProfitCycleRecharge(_ context.Context, userID, adminAccountID, siteID string, amount float64) error {
	key := profitCycleKey(userID, adminAccountID, siteID)
	cycle := r.cycles[key]
	cycle.RechargeAmountCNY += amount
	r.cycles[key] = cycle
	return nil
}

func (r *memoryPauseRepository) ListProfitCycleAccounts(_ context.Context, userID, adminAccountID, siteID string) ([]ProfitCycleAccount, error) {
	var result []ProfitCycleAccount
	for _, account := range r.profitAccounts {
		if account.UserID == userID && account.AdminAccountID == adminAccountID && account.UpstreamSiteID == siteID {
			result = append(result, account)
		}
	}
	return result, nil
}

func (r *memoryPauseRepository) UpsertProfitCycleAccount(_ context.Context, account ProfitCycleAccount) error {
	r.profitAccounts[profitAccountKey(account.UserID, account.AdminAccountID, account.UpstreamSiteID, account.RemoteAccountID)] = account
	return nil
}

func (r *memoryPauseRepository) FinalizeProfitCycle(_ context.Context, cycle ProfitCycle, accounts []ProfitCycleAccount) error {
	r.cycles[profitCycleKey(cycle.UserID, cycle.AdminAccountID, cycle.UpstreamSiteID)] = cycle
	for _, account := range accounts {
		r.profitAccounts[profitAccountKey(account.UserID, account.AdminAccountID, account.UpstreamSiteID, account.RemoteAccountID)] = account
	}
	return nil
}

type memorySiteController struct {
	sites map[string]*upstream.Site
}

func (s *memorySiteController) GetSite(_ context.Context, siteID string) (*upstream.Site, error) {
	return s.sites[siteID], nil
}

func (s *memorySiteController) SetBalanceSuspended(_ context.Context, siteID string, suspended bool, reason string) (*upstream.Site, error) {
	site := s.sites[siteID]
	site.BalanceSuspended = suspended
	site.BalancePauseReason = reason
	if suspended {
		site.Status = upstream.StatusDisabled
	} else {
		site.Status = upstream.StatusConnected
	}
	return site, nil
}

type memoryConnectionProvider struct {
	connections []my_sites.RealConnection
	session     upstream.Session
}

func (p *memoryConnectionProvider) ListRealConnectionsForWorkspace(_ context.Context, userID, adminAccountID string) ([]my_sites.RealConnection, error) {
	result := make([]my_sites.RealConnection, 0)
	for _, conn := range p.connections {
		if conn.UserID == userID && conn.WorkspaceAdminAccountID == adminAccountID {
			result = append(result, conn)
		}
	}
	return result, nil
}

func (p *memoryConnectionProvider) RequireSession(context.Context, string, string) (upstream.Session, error) {
	return p.session, nil
}

type memoryAccountController struct {
	states     map[string]upstream.Sub2APIAdminAccountState
	updates    map[string]int
	failUpdate map[string]int
	usage      map[string]float64
	usageErr   map[string]error
}

func (p *memoryAccountController) GetSub2APIAdminAccountState(_ upstream.Session, accountID string) (upstream.Sub2APIAdminAccountState, error) {
	state, ok := p.states[accountID]
	if !ok {
		return upstream.Sub2APIAdminAccountState{}, errors.New("not found")
	}
	return state, nil
}

func (p *memoryAccountController) UpdateSub2APIAdminAccountState(_ upstream.Session, accountID string, status *string, schedulable *bool) error {
	p.updates[accountID]++
	if p.failUpdate[accountID] > 0 {
		p.failUpdate[accountID]--
		return errors.New("remote update failed")
	}
	state := p.states[accountID]
	if status != nil {
		state.Status = *status
	}
	if schedulable != nil {
		value := *schedulable
		state.Schedulable = &value
	}
	p.states[accountID] = state
	return nil
}

func (p *memoryAccountController) FetchSub2APIAdminAccountUsageStats(_ upstream.Session, accountID, _, _ string) (float64, error) {
	if err := p.usageErr[accountID]; err != nil {
		return 0, err
	}
	return p.usage[accountID], nil
}

func newBalanceTestService(site *upstream.Site, connections []my_sites.RealConnection, states map[string]upstream.Sub2APIAdminAccountState) (*Service, *memoryPauseRepository, *memorySiteController, *memoryAccountController) {
	repository := newMemoryPauseRepository()
	sites := &memorySiteController{sites: map[string]*upstream.Site{site.ID: site}}
	accounts := &memoryAccountController{
		states: states, updates: make(map[string]int), failUpdate: make(map[string]int),
		usage: make(map[string]float64), usageErr: make(map[string]error),
	}
	service := &Service{
		repository: repository,
		upstream:   sites,
		mySites: &memoryConnectionProvider{
			connections: connections,
			session:     upstream.Session{Platform: upstream.PlatformSub2API},
		},
		platform: accounts,
	}
	return service, repository, sites, accounts
}

func boolPointer(value bool) *bool        { return &value }
func floatPointer(value float64) *float64 { return &value }

func TestReconcilePausesEligibleAccountsOnce(t *testing.T) {
	site := &upstream.Site{ID: "site-1", UserID: "user-1", AdminAccountID: "workspace-1", RechargeRate: 1, Status: upstream.StatusConnected}
	connections := []my_sites.RealConnection{
		{ID: "conn-1", UserID: "user-1", WorkspaceAdminAccountID: "workspace-1", UpstreamSiteID: "site-1", AdminAccountID: "active", AdminPlatform: string(upstream.PlatformSub2API), Status: my_sites.ConnectionStatusActive},
		{ID: "conn-duplicate", UserID: "user-1", WorkspaceAdminAccountID: "workspace-1", UpstreamSiteID: "site-1", AdminAccountID: "active", AdminPlatform: string(upstream.PlatformSub2API), Status: my_sites.ConnectionStatusActive},
		{ID: "conn-manual", UserID: "user-1", WorkspaceAdminAccountID: "workspace-1", UpstreamSiteID: "site-1", AdminAccountID: "manual", AdminPlatform: string(upstream.PlatformSub2API), Status: my_sites.ConnectionStatusActive},
		{ID: "conn-other-site", UserID: "user-1", WorkspaceAdminAccountID: "workspace-1", UpstreamSiteID: "site-2", AdminAccountID: "other", AdminPlatform: string(upstream.PlatformSub2API), Status: my_sites.ConnectionStatusActive},
		{ID: "conn-other-workspace", UserID: "user-1", WorkspaceAdminAccountID: "workspace-2", UpstreamSiteID: "site-1", AdminAccountID: "foreign", AdminPlatform: string(upstream.PlatformSub2API), Status: my_sites.ConnectionStatusActive},
	}
	service, repository, _, accounts := newBalanceTestService(site, connections, map[string]upstream.Sub2APIAdminAccountState{
		"active": {Name: "active", Status: "active", Schedulable: boolPointer(true)},
		"manual": {Name: "manual", Status: "inactive", Schedulable: boolPointer(false)},
		"other":  {Name: "other", Status: "active", Schedulable: boolPointer(true)},
	})
	strategy := settings.StrategySettings{EnableBalanceWarning: true, DefaultBalanceThreshold: 10}
	metrics := upstream.Metrics{Balance: upstream.MetricValue{Value: floatPointer(5)}}

	first := service.Reconcile(context.Background(), "user-1", "workspace-1", "site-1", "site", strategy, upstream.Metrics{}, metrics)
	if first.Transition != "paused" || len(first.Paused) != 1 || len(first.Skipped) != 1 {
		t.Fatalf("unexpected first reconcile result: %#v", first)
	}
	if !site.BalanceSuspended || site.Status != upstream.StatusDisabled {
		t.Fatalf("expected site disabled, got suspended=%v status=%s", site.BalanceSuspended, site.Status)
	}
	if accounts.updates["active"] != 1 || accounts.updates["manual"] != 0 || accounts.updates["other"] != 0 {
		t.Fatalf("unexpected update counts: %#v", accounts.updates)
	}
	record := repository.records[pauseRecordKey("user-1", "workspace-1", "site-1", "active")]
	if !record.Applied || record.OriginalStatus != "active" || record.OriginalSchedulable == nil || !*record.OriginalSchedulable {
		t.Fatalf("unexpected pause record: %#v", record)
	}

	second := service.Reconcile(context.Background(), "user-1", "workspace-1", "site-1", "site", strategy, upstream.Metrics{}, metrics)
	if second.Transition != "" || accounts.updates["active"] != 1 {
		t.Fatalf("expected sustained low balance without duplicate PUT, result=%#v updates=%#v", second, accounts.updates)
	}
}

func TestReconcileRecoveryRetriesFailedAccount(t *testing.T) {
	site := &upstream.Site{ID: "site-1", UserID: "user-1", AdminAccountID: "workspace-1", RechargeRate: 1, Status: upstream.StatusDisabled, BalanceSuspended: true}
	service, repository, _, accounts := newBalanceTestService(site, nil, map[string]upstream.Sub2APIAdminAccountState{
		"account-1": {Name: "account", Status: "inactive", Schedulable: boolPointer(false)},
	})
	repository.records[pauseRecordKey("user-1", "workspace-1", "site-1", "account-1")] = PauseRecord{
		UserID: "user-1", AdminAccountID: "workspace-1", UpstreamSiteID: "site-1", AdminAccountIDRemote: "account-1",
		OriginalStatus: "active", OriginalSchedulable: boolPointer(true), Applied: true,
	}
	accounts.failUpdate["account-1"] = 1
	strategy := settings.StrategySettings{EnableBalanceWarning: true, DefaultBalanceThreshold: 10}
	metrics := upstream.Metrics{Balance: upstream.MetricValue{Value: floatPointer(10)}}

	first := service.Reconcile(context.Background(), "user-1", "workspace-1", "site-1", "site", strategy, upstream.Metrics{}, metrics)
	if first.Transition != "recovered" || first.PendingRestores != 1 || len(first.Failed) != 1 {
		t.Fatalf("unexpected failed recovery result: %#v", first)
	}
	if site.BalanceSuspended || site.Status != upstream.StatusConnected {
		t.Fatalf("expected visible site recovery, got suspended=%v status=%s", site.BalanceSuspended, site.Status)
	}
	if _, ok := repository.records[pauseRecordKey("user-1", "workspace-1", "site-1", "account-1")]; !ok {
		t.Fatal("expected failed restore record to remain for retry")
	}

	second := service.Reconcile(context.Background(), "user-1", "workspace-1", "site-1", "site", strategy, upstream.Metrics{}, metrics)
	if second.Transition != "" || len(second.Restored) != 1 || second.PendingRestores != 0 {
		t.Fatalf("unexpected retry result: %#v", second)
	}
	if _, ok := repository.records[pauseRecordKey("user-1", "workspace-1", "site-1", "account-1")]; ok {
		t.Fatal("expected successful restore record to be deleted")
	}
	state := accounts.states["account-1"]
	if state.Status != "active" || state.Schedulable == nil || !*state.Schedulable {
		t.Fatalf("expected original state restored, got %#v", state)
	}
}

func TestReconcilePendingAppliedRemotelyDoesNotRepeatPut(t *testing.T) {
	site := &upstream.Site{ID: "site-1", RechargeRate: 1, Status: upstream.StatusDisabled, BalanceSuspended: true}
	connections := []my_sites.RealConnection{{ID: "conn-1", UserID: "user-1", WorkspaceAdminAccountID: "workspace-1", UpstreamSiteID: "site-1", AdminAccountID: "account-1", AdminPlatform: string(upstream.PlatformSub2API), Status: my_sites.ConnectionStatusActive}}
	service, repository, _, accounts := newBalanceTestService(site, connections, map[string]upstream.Sub2APIAdminAccountState{
		"account-1": {Name: "account", Status: "inactive", Schedulable: boolPointer(false)},
	})
	repository.records[pauseRecordKey("user-1", "workspace-1", "site-1", "account-1")] = PauseRecord{
		UserID: "user-1", AdminAccountID: "workspace-1", UpstreamSiteID: "site-1", AdminAccountIDRemote: "account-1",
		OriginalStatus: "active", OriginalSchedulable: boolPointer(true), Applied: false,
	}
	result := service.Reconcile(context.Background(), "user-1", "workspace-1", "site-1", "site",
		settings.StrategySettings{EnableBalanceWarning: true, DefaultBalanceThreshold: 10},
		upstream.Metrics{},
		upstream.Metrics{Balance: upstream.MetricValue{Value: floatPointer(1)}})

	if len(result.Skipped) != 1 || result.Skipped[0].Status != "pause_confirmed" || accounts.updates["account-1"] != 0 {
		t.Fatalf("expected pending record confirmation without PUT, result=%#v updates=%#v", result, accounts.updates)
	}
	if !repository.records[pauseRecordKey("user-1", "workspace-1", "site-1", "account-1")].Applied {
		t.Fatal("expected pending record to be marked applied")
	}
}

func TestIsAccountBalancePausedTreatsPendingRecordAsProtected(t *testing.T) {
	site := &upstream.Site{ID: "site-1", RechargeRate: 1}
	service, repository, _, _ := newBalanceTestService(site, nil, nil)
	repository.records[pauseRecordKey("user-1", "workspace-1", "site-1", "account-1")] = PauseRecord{
		UserID: "user-1", AdminAccountID: "workspace-1", UpstreamSiteID: "site-1", AdminAccountIDRemote: "account-1",
		OriginalStatus: "active", OriginalSchedulable: boolPointer(true), Applied: false,
	}

	paused, err := service.IsAccountBalancePaused(context.Background(), "user-1", "workspace-1", "site-1", "account-1")
	if err != nil {
		t.Fatalf("check balance pause: %v", err)
	}
	if !paused {
		t.Fatal("expected pending pause record to protect account")
	}
}

func TestReconcilePendingRecordRestoresRemoteAccountBeforeDeleting(t *testing.T) {
	site := &upstream.Site{ID: "site-1", RechargeRate: 1, Status: upstream.StatusDisabled, BalanceSuspended: true}
	service, repository, _, accounts := newBalanceTestService(site, nil, map[string]upstream.Sub2APIAdminAccountState{
		"account-1": {Name: "account", Status: "inactive", Schedulable: boolPointer(false)},
	})
	repository.records[pauseRecordKey("user-1", "workspace-1", "site-1", "account-1")] = PauseRecord{
		UserID: "user-1", AdminAccountID: "workspace-1", UpstreamSiteID: "site-1", AdminAccountIDRemote: "account-1",
		OriginalStatus: "active", OriginalSchedulable: boolPointer(true), Applied: false,
	}

	result := service.Reconcile(context.Background(), "user-1", "workspace-1", "site-1", "site",
		settings.StrategySettings{EnableBalanceWarning: true, DefaultBalanceThreshold: 10},
		upstream.Metrics{},
		upstream.Metrics{Balance: upstream.MetricValue{Value: floatPointer(10)}})

	if len(result.Restored) != 1 || result.PendingRestores != 0 || accounts.updates["account-1"] != 1 {
		t.Fatalf("expected pending remotely-paused account to restore, result=%#v updates=%#v", result, accounts.updates)
	}
	if _, ok := repository.records[pauseRecordKey("user-1", "workspace-1", "site-1", "account-1")]; ok {
		t.Fatal("expected restored pending record to be deleted")
	}
}

func TestReconcileMissingBalanceLeavesStateUnchanged(t *testing.T) {
	site := &upstream.Site{ID: "site-1", RechargeRate: 1, Status: upstream.StatusConnected}
	service, repository, _, accounts := newBalanceTestService(site, nil, nil)
	result := service.Reconcile(context.Background(), "user-1", "workspace-1", "site-1", "site",
		settings.StrategySettings{EnableBalanceWarning: true, DefaultBalanceThreshold: 10}, upstream.Metrics{}, upstream.Metrics{})
	if result.Known || site.BalanceSuspended || len(repository.records) != 0 || len(accounts.updates) != 0 {
		t.Fatalf("expected missing balance to be ignored, result=%#v site=%#v", result, site)
	}
}

func TestProfitCycleCombinesRechargesAndAggregatesGroupIncome(t *testing.T) {
	site := &upstream.Site{
		ID: "site-1", UserID: "user-1", AdminAccountID: "workspace-1",
		RechargeRate: 1, Status: upstream.StatusConnected,
	}
	connections := []my_sites.RealConnection{
		profitTestConnection("conn-1", "account-1", "group-a", "codex_plus-福利"),
		profitTestConnection("conn-1-duplicate", "account-1", "group-a", "codex_plus-福利"),
		profitTestConnection("conn-2", "account-2", "group-b", "claude-福利"),
	}
	service, repository, _, accounts := newBalanceTestService(site, connections, map[string]upstream.Sub2APIAdminAccountState{
		"account-1": {Name: "account-1", Status: "active", Schedulable: boolPointer(true)},
		"account-2": {Name: "account-2", Status: "active", Schedulable: boolPointer(true)},
	})
	accounts.usage["account-1"] = 10
	accounts.usage["account-2"] = 20
	strategy := settings.StrategySettings{EnableBalanceWarning: true, DefaultBalanceThreshold: 10}

	first := service.Reconcile(context.Background(), "user-1", "workspace-1", "site-1", "site", strategy,
		profitTestMetrics(80, 0), profitTestMetrics(80, 100))
	if first.Transition != "" || first.Profit != nil {
		t.Fatalf("recharge must start a cycle without notifying: %#v", first)
	}
	cycle := repository.cycles[profitCycleKey("user-1", "workspace-1", "site-1")]
	if cycle.RechargeAmountCNY != 100 || cycle.Status != profitCycleActive || len(repository.profitAccounts) != 2 {
		t.Fatalf("unexpected first cycle state: cycle=%#v accounts=%#v", cycle, repository.profitAccounts)
	}

	second := service.Reconcile(context.Background(), "user-1", "workspace-1", "site-1", "site", strategy,
		profitTestMetrics(80, 100), profitTestMetrics(80, 150))
	if second.Transition != "" {
		t.Fatalf("second recharge must not pause: %#v", second)
	}

	accounts.usage["account-1"] = 130
	accounts.usage["account-2"] = 90
	final := service.Reconcile(context.Background(), "user-1", "workspace-1", "site-1", "site", strategy,
		profitTestMetrics(20, 150), profitTestMetrics(0, 150))
	if final.Transition != "paused" || final.Profit == nil || !final.Profit.Complete {
		t.Fatalf("expected complete paused profit report: %#v", final)
	}
	if final.Profit.RechargeAmountCNY != 150 || final.Profit.DownstreamIncomeCNY != 190 || final.Profit.ProfitCNY != 40 {
		t.Fatalf("unexpected totals: %#v", final.Profit)
	}
	if len(final.Profit.Groups) != 2 || final.Profit.Groups[0].GroupName != "codex_plus-福利" || final.Profit.Groups[0].Amount != 120 ||
		final.Profit.Groups[1].GroupName != "claude-福利" || final.Profit.Groups[1].Amount != 70 {
		t.Fatalf("unexpected group income: %#v", final.Profit.Groups)
	}
	if final.Profit.SuccessfulAccounts != 2 || final.Profit.FailedAccounts != 0 {
		t.Fatalf("duplicate binding must be counted once: %#v", final.Profit)
	}
	cycle = repository.cycles[profitCycleKey("user-1", "workspace-1", "site-1")]
	if cycle.Status != profitCycleFinalized || !cycle.Complete {
		t.Fatalf("cycle was not finalized: %#v", cycle)
	}
}

func TestProfitCycleAppliesRechargeRateAndReportsLoss(t *testing.T) {
	site := &upstream.Site{
		ID: "site-1", UserID: "user-1", AdminAccountID: "workspace-1",
		RechargeRate: 2, Status: upstream.StatusConnected,
	}
	connection := profitTestConnection("conn-1", "account-1", "group-a", "openai")
	service, _, _, accounts := newBalanceTestService(site, []my_sites.RealConnection{connection}, map[string]upstream.Sub2APIAdminAccountState{
		"account-1": {Name: "account", Status: "active", Schedulable: boolPointer(true)},
	})
	strategy := settings.StrategySettings{EnableBalanceWarning: true, DefaultBalanceThreshold: 10}
	accounts.usage["account-1"] = 5
	service.Reconcile(context.Background(), "user-1", "workspace-1", "site-1", "site", strategy,
		profitTestMetrics(20, 10), profitTestMetrics(20, 60))
	accounts.usage["account-1"] = 85
	result := service.Reconcile(context.Background(), "user-1", "workspace-1", "site-1", "site", strategy,
		profitTestMetrics(10, 60), profitTestMetrics(0, 60))
	if result.Profit == nil || !result.Profit.Complete || result.Profit.RechargeAmountCNY != 100 ||
		result.Profit.DownstreamIncomeCNY != 80 || result.Profit.ProfitCNY != -20 {
		t.Fatalf("unexpected loss report: %#v", result.Profit)
	}
}

func TestProfitCycleMarksFailedUsageQueryIncompleteWithoutBlockingPause(t *testing.T) {
	site := &upstream.Site{
		ID: "site-1", UserID: "user-1", AdminAccountID: "workspace-1",
		RechargeRate: 1, Status: upstream.StatusConnected,
	}
	connection := profitTestConnection("conn-1", "account-1", "group-a", "openai")
	service, _, _, accounts := newBalanceTestService(site, []my_sites.RealConnection{connection}, map[string]upstream.Sub2APIAdminAccountState{
		"account-1": {Name: "account", Status: "active", Schedulable: boolPointer(true)},
	})
	strategy := settings.StrategySettings{EnableBalanceWarning: true, DefaultBalanceThreshold: 10}
	accounts.usage["account-1"] = 5
	service.Reconcile(context.Background(), "user-1", "workspace-1", "site-1", "site", strategy,
		profitTestMetrics(20, 0), profitTestMetrics(20, 100))
	accounts.usageErr["account-1"] = errors.New("stats unavailable")
	result := service.Reconcile(context.Background(), "user-1", "workspace-1", "site-1", "site", strategy,
		profitTestMetrics(20, 100), profitTestMetrics(0, 100))
	if result.Transition != "paused" || len(result.Paused) != 1 {
		t.Fatalf("usage failure must not block account pause: %#v", result)
	}
	if result.Profit == nil || result.Profit.Complete || result.Profit.Reason != "account_stats_incomplete" || result.Profit.FailedAccounts != 1 {
		t.Fatalf("expected incomplete profit report: %#v", result.Profit)
	}
}

func TestProfitCycleKeepsAmbiguousGroupIncomeUnattributed(t *testing.T) {
	site := &upstream.Site{
		ID: "site-1", UserID: "user-1", AdminAccountID: "workspace-1",
		RechargeRate: 1, Status: upstream.StatusConnected,
	}
	connections := []my_sites.RealConnection{
		profitTestConnection("conn-1", "account-1", "group-a", "openai"),
		profitTestConnection("conn-2", "account-1", "group-b", "claude"),
	}
	service, _, _, accounts := newBalanceTestService(site, connections, map[string]upstream.Sub2APIAdminAccountState{
		"account-1": {Name: "account", Status: "active", Schedulable: boolPointer(true)},
	})
	strategy := settings.StrategySettings{EnableBalanceWarning: true, DefaultBalanceThreshold: 10}
	accounts.usage["account-1"] = 10
	service.Reconcile(context.Background(), "user-1", "workspace-1", "site-1", "site", strategy,
		profitTestMetrics(20, 0), profitTestMetrics(20, 50))
	accounts.usage["account-1"] = 75
	result := service.Reconcile(context.Background(), "user-1", "workspace-1", "site-1", "site", strategy,
		profitTestMetrics(20, 50), profitTestMetrics(0, 50))
	if result.Profit == nil || !result.Profit.Complete || result.Profit.DownstreamIncomeCNY != 65 ||
		result.Profit.UnattributedIncome != 65 || len(result.Profit.Groups) != 0 {
		t.Fatalf("unexpected ambiguous attribution report: %#v", result.Profit)
	}
}

func TestProfitCycleSurvivesServiceRestartAndCapturesNewAccount(t *testing.T) {
	site := &upstream.Site{
		ID: "site-1", UserID: "user-1", AdminAccountID: "workspace-1",
		RechargeRate: 1, Status: upstream.StatusConnected,
	}
	firstConnection := profitTestConnection("conn-1", "account-1", "group-a", "openai")
	service, repository, sites, accounts := newBalanceTestService(site, []my_sites.RealConnection{firstConnection}, map[string]upstream.Sub2APIAdminAccountState{
		"account-1": {Name: "account-1", Status: "active", Schedulable: boolPointer(true)},
		"account-2": {Name: "account-2", Status: "active", Schedulable: boolPointer(true)},
	})
	provider := service.mySites.(*memoryConnectionProvider)
	strategy := settings.StrategySettings{EnableBalanceWarning: true, DefaultBalanceThreshold: 10}
	accounts.usage["account-1"] = 10
	service.Reconcile(context.Background(), "user-1", "workspace-1", "site-1", "site", strategy,
		profitTestMetrics(20, 0), profitTestMetrics(20, 100))

	provider.connections = append(provider.connections, profitTestConnection("conn-2", "account-2", "group-b", "claude"))
	accounts.usage["account-2"] = 30
	restarted := &Service{repository: repository, upstream: sites, mySites: provider, platform: accounts}
	restarted.Reconcile(context.Background(), "user-1", "workspace-1", "site-1", "site", strategy,
		profitTestMetrics(20, 100), profitTestMetrics(20, 100))

	accounts.usage["account-1"] = 60
	accounts.usage["account-2"] = 70
	result := restarted.Reconcile(context.Background(), "user-1", "workspace-1", "site-1", "site", strategy,
		profitTestMetrics(20, 100), profitTestMetrics(0, 100))
	if result.Profit == nil || !result.Profit.Complete || result.Profit.DownstreamIncomeCNY != 90 || result.Profit.SuccessfulAccounts != 2 {
		t.Fatalf("restart or newly bound account lost cycle state: %#v", result.Profit)
	}
}

func profitTestConnection(id, accountID, groupID, groupName string) my_sites.RealConnection {
	return my_sites.RealConnection{
		ID: id, UserID: "user-1", WorkspaceAdminAccountID: "workspace-1",
		UpstreamSiteID: "site-1", UpstreamGroupID: groupID, UpstreamGroupName: groupName,
		AdminAccountID: accountID, AdminPlatform: string(upstream.PlatformSub2API),
		Status: my_sites.ConnectionStatusActive, CreatedAt: "2026-07-01T00:00:00Z",
	}
}

func profitTestMetrics(balance, historyRecharge float64) upstream.Metrics {
	return upstream.Metrics{
		Balance:         upstream.MetricValue{Value: floatPointer(balance)},
		HistoryRecharge: upstream.MetricValue{Value: floatPointer(historyRecharge)},
	}
}
