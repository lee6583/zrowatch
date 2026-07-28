package connection_health

import (
	"context"
	"errors"
	"testing"

	"transithub/backend/internal/modules/my_sites"
	"transithub/backend/internal/modules/upstream"
)

type fakeBalancePauseLookup struct {
	paused bool
	err    error
}

func (f fakeBalancePauseLookup) IsAccountBalancePaused(context.Context, string, string, string, string) (bool, error) {
	return f.paused, f.err
}

func (f fakeBalancePauseLookup) IsAccountBalancePausedForWorkspace(context.Context, string, string, string) (bool, error) {
	return f.paused, f.err
}

type fakeSchedulingActioner struct {
	err   error
	calls []struct {
		accountID   string
		status      *string
		schedulable *bool
	}
}

func (f *fakeSchedulingActioner) UpdateSub2APIAdminAccountState(_ upstream.Session, accountID string, status *string, schedulable *bool) error {
	f.calls = append(f.calls, struct {
		accountID   string
		status      *string
		schedulable *bool
	}{accountID: accountID, status: status, schedulable: schedulable})
	return f.err
}

func TestRemoteActionDispatcherSkipsBalancePausedConnection(t *testing.T) {
	platform := &fakePlatformActioner{}
	dispatcher := newRemoteActionDispatcher(fakeSiteLookup{site: &upstream.Site{Platform: upstream.PlatformSub2API}}, fakeSessionProvider{}, platform)
	dispatcher.SetBalancePauseLookup(fakeBalancePauseLookup{paused: true})
	conn := my_sites.RealConnection{
		UserID: "user-1", WorkspaceAdminAccountID: "workspace-1", UpstreamSiteID: "site-1", AdminAccountID: "account-1",
	}

	action, err := dispatcher.Restore(context.Background(), conn, ConnectionHealthState{})
	if err != nil {
		t.Fatalf("unexpected balance protection error: %v", err)
	}
	if action != RemoteActionSkippedBalanceSuspended || len(platform.sub2APICalls) != 0 {
		t.Fatalf("expected balance-protected restore to be skipped, action=%q calls=%#v", action, platform.sub2APICalls)
	}
}

func TestReconcileTargetRemoteActionSkipsBalancePausedAccount(t *testing.T) {
	repo := newFakeRepository()
	platform := &fakePlatformActioner{}
	targetID := "sub2api:workspace-1:account-1"
	repo.states[targetID] = map[string]ConnectionHealthState{
		"model": {ConnectionID: targetID, ModelName: "model", State: StateHealthy, CurrentWeight: 100},
	}
	repo.targetActionStates["user-1|workspace-1|"+targetID] = TargetActionState{
		UserID: "user-1", AdminAccountID: "workspace-1", TargetID: targetID,
		OriginalStatus: "active", LastAppliedStatus: "inactive",
	}
	service := &Service{
		repo: repo, dispatcher: newRemoteActionDispatcher(nil, nil, platform), balancePauses: fakeBalancePauseLookup{paused: true},
	}
	policy := Policy{ID: "policy", Enabled: true, AutoDegradeEnabled: true, AutoRemoteActionEnabled: true}
	target := AdminProbeTarget{TargetID: targetID, Platform: string(upstream.PlatformSub2API), AccountID: "account-1", AccountStatus: "inactive"}

	action, err := service.reconcileTargetRemoteAction(context.Background(), "user-1", "workspace-1", upstream.Session{Platform: upstream.PlatformSub2API}, target, []probeModelSpec{{modelName: "model", policy: policy}})
	if err != nil {
		t.Fatalf("unexpected reconcile error: %v", err)
	}
	if action != RemoteActionSkippedBalanceSuspended || len(platform.sub2APICalls) != 0 {
		t.Fatalf("expected health restore to be blocked, action=%q calls=%#v", action, platform.sub2APICalls)
	}
}

func TestRestoreUnmanagedTargetActionsSkipsBalancePausedAccount(t *testing.T) {
	repo := newFakeRepository()
	platform := &fakePlatformActioner{}
	targetID := "sub2api:workspace-1:account-1"
	stored := TargetActionState{
		UserID: "user-1", AdminAccountID: "workspace-1", TargetID: targetID,
		OriginalStatus: "active", LastAppliedStatus: "inactive",
	}
	repo.targetActionStates["user-1|workspace-1|"+targetID] = stored
	service := &Service{
		repo: repo, dispatcher: newRemoteActionDispatcher(nil, nil, platform), balancePauses: fakeBalancePauseLookup{paused: true},
	}

	service.restoreUnmanagedTargetActions(context.Background(), nil, nil, nil, nil, []TargetActionState{stored}, make(adminInventoryCache))
	if len(platform.sub2APICalls) != 0 {
		t.Fatalf("expected unmanaged restore to remain blocked, calls=%#v", platform.sub2APICalls)
	}
	if _, ok := repo.targetActionStates["user-1|workspace-1|"+targetID]; !ok {
		t.Fatal("expected health action snapshot to remain while balance protection is active")
	}
}

func TestUpdateTargetSchedulingRejectsEnableDuringBalancePause(t *testing.T) {
	actions := &fakeSchedulingActioner{}
	service := &Service{
		mySites:  fakeMySitesReader{session: upstream.Session{Platform: upstream.PlatformSub2API}},
		accounts: fakeAdminAccountResolver{id: "workspace-1"},
		platformGroups: fakePlatformGroupReader{
			groups: []upstream.AdminGroupInfo{{ID: "group-1", Name: "group"}},
			accountsByGrp: map[string][]upstream.AdminGroupAccountInfo{
				"group-1": {{ID: "account-1", Name: "account", Status: "inactive"}},
			},
		},
		schedulingActions: actions,
		balancePauses:     fakeBalancePauseLookup{paused: true},
	}
	targetID := buildTargetID(string(upstream.PlatformSub2API), "workspace-1", "account-1")

	err := service.UpdateTargetScheduling(context.Background(), "user-1", targetID, true)
	var requestErr requestError
	if !errors.As(err, &requestErr) || requestErr != requestError(ErrorBalanceSuspended) {
		t.Fatalf("expected balance suspended request error, got %v", err)
	}
	if len(actions.calls) != 0 {
		t.Fatalf("expected no remote scheduling call, got %#v", actions.calls)
	}

	if err := service.UpdateTargetScheduling(context.Background(), "user-1", targetID, false); err != nil {
		t.Fatalf("disabling scheduling should remain allowed: %v", err)
	}
	if len(actions.calls) != 1 || actions.calls[0].accountID != "account-1" || actions.calls[0].schedulable == nil || *actions.calls[0].schedulable {
		t.Fatalf("unexpected disable scheduling call: %#v", actions.calls)
	}
}

func TestUpdateTargetDispatchRejectsEnableDuringBalancePause(t *testing.T) {
	repo := newFakeRepository()
	actions := &fakeSchedulingActioner{}
	schedulable := false
	service := &Service{
		repo:     repo,
		mySites:  fakeMySitesReader{session: upstream.Session{Platform: upstream.PlatformSub2API}},
		accounts: fakeAdminAccountResolver{id: "workspace-1"},
		platformGroups: fakePlatformGroupReader{
			groups: []upstream.AdminGroupInfo{{ID: "group-1", Name: "group"}},
			accountsByGrp: map[string][]upstream.AdminGroupAccountInfo{
				"group-1": {{ID: "account-1", Name: "account", Status: "inactive", Schedulable: &schedulable}},
			},
		},
		schedulingActions: actions,
		balancePauses:     fakeBalancePauseLookup{paused: true},
	}
	targetID := buildTargetID(string(upstream.PlatformSub2API), "workspace-1", "account-1")

	_, err := service.UpdateTargetDispatch(context.Background(), "user-1", targetID, true)
	var requestErr requestError
	if !errors.As(err, &requestErr) || requestErr != requestError(ErrorBalanceSuspended) {
		t.Fatalf("expected balance suspended request error, got %v", err)
	}
	if len(actions.calls) != 0 {
		t.Fatalf("expected no remote dispatch call, got %#v", actions.calls)
	}

	state, err := service.UpdateTargetDispatch(context.Background(), "user-1", targetID, false)
	if err != nil {
		t.Fatalf("disabling dispatch should remain allowed: %v", err)
	}
	if state.Status != "inactive" || state.Schedulable {
		t.Fatalf("unexpected dispatch response: %#v", state)
	}
	if len(actions.calls) != 1 || actions.calls[0].status == nil || *actions.calls[0].status != "inactive" || actions.calls[0].schedulable == nil || *actions.calls[0].schedulable {
		t.Fatalf("unexpected disable dispatch call: %#v", actions.calls)
	}
}

func TestBoundDispatchAccountsMarksBalancePausedAccountUnavailable(t *testing.T) {
	schedulable := true
	priority := 7
	dispatchReader := &fakeFastDispatchActioner{
		statesByID: map[string]upstream.Sub2APIAdminAccountState{
			"account-1": {ID: "account-1", Name: "account", Status: "active", Schedulable: &schedulable, Priority: &priority},
		},
	}
	service := &Service{
		mySites: fakeMySitesReader{
			session: upstream.Session{Platform: upstream.PlatformSub2API, AccessToken: "token"},
			connections: []my_sites.RealConnection{{
				ID: "conn-1", UserID: "user-1", WorkspaceAdminAccountID: "workspace-1",
				UpstreamSiteID: "site-1", UpstreamGroupID: "group-1", UpstreamGroupName: "vip",
				AdminAccountID: "account-1", AdminAccountName: "account",
				Status: my_sites.ConnectionStatusActive, AdminPlatform: string(upstream.PlatformSub2API),
			}},
		},
		accounts:       fakeAdminAccountResolver{id: "workspace-1"},
		dispatchStates: dispatchReader,
		balancePauses:  fakeBalancePauseLookup{paused: true},
	}

	accounts, err := service.BoundDispatchAccounts(context.Background(), "user-1")
	if err != nil {
		t.Fatalf("BoundDispatchAccounts returned error: %v", err)
	}
	if len(accounts) != 1 {
		t.Fatalf("expected one account, got %#v", accounts)
	}
	if accounts[0].Available || accounts[0].UnavailableReason != "balance_suspended" {
		t.Fatalf("expected balance suspended unavailable account, got %#v", accounts[0])
	}
	if accounts[0].Schedulable == nil || !*accounts[0].Schedulable || accounts[0].Status != "active" || accounts[0].Priority == nil || *accounts[0].Priority != priority {
		t.Fatalf("expected remote state preserved for display, got %#v", accounts[0])
	}
}
