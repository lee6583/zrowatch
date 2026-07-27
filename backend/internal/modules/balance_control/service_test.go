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
	records map[string]PauseRecord
}

func newMemoryPauseRepository() *memoryPauseRepository {
	return &memoryPauseRepository{records: make(map[string]PauseRecord)}
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

func newBalanceTestService(site *upstream.Site, connections []my_sites.RealConnection, states map[string]upstream.Sub2APIAdminAccountState) (*Service, *memoryPauseRepository, *memorySiteController, *memoryAccountController) {
	repository := newMemoryPauseRepository()
	sites := &memorySiteController{sites: map[string]*upstream.Site{site.ID: site}}
	accounts := &memoryAccountController{states: states, updates: make(map[string]int), failUpdate: make(map[string]int)}
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

	first := service.Reconcile(context.Background(), "user-1", "workspace-1", "site-1", "site", strategy, metrics)
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

	second := service.Reconcile(context.Background(), "user-1", "workspace-1", "site-1", "site", strategy, metrics)
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

	first := service.Reconcile(context.Background(), "user-1", "workspace-1", "site-1", "site", strategy, metrics)
	if first.Transition != "recovered" || first.PendingRestores != 1 || len(first.Failed) != 1 {
		t.Fatalf("unexpected failed recovery result: %#v", first)
	}
	if site.BalanceSuspended || site.Status != upstream.StatusConnected {
		t.Fatalf("expected visible site recovery, got suspended=%v status=%s", site.BalanceSuspended, site.Status)
	}
	if _, ok := repository.records[pauseRecordKey("user-1", "workspace-1", "site-1", "account-1")]; !ok {
		t.Fatal("expected failed restore record to remain for retry")
	}

	second := service.Reconcile(context.Background(), "user-1", "workspace-1", "site-1", "site", strategy, metrics)
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
		settings.StrategySettings{EnableBalanceWarning: true, DefaultBalanceThreshold: 10}, upstream.Metrics{})
	if result.Known || site.BalanceSuspended || len(repository.records) != 0 || len(accounts.updates) != 0 {
		t.Fatalf("expected missing balance to be ignored, result=%#v site=%#v", result, site)
	}
}
