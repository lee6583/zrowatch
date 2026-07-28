package connection_health

import (
	"context"
	"errors"
	"sync"
	"testing"

	"transithub/backend/internal/modules/my_sites"
	"transithub/backend/internal/modules/upstream"
)

type fakeFastDispatchActioner struct {
	mu            sync.Mutex
	accountID     string
	state         upstream.Sub2APIAdminAccountState
	statesByID    map[string]upstream.Sub2APIAdminAccountState
	getErrors     map[string]error
	getAccountIDs []string
	called        int
}

func (f *fakeFastDispatchActioner) GetSub2APIAdminAccountState(_ upstream.Session, accountID string) (upstream.Sub2APIAdminAccountState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.getAccountIDs = append(f.getAccountIDs, accountID)
	if err := f.getErrors[accountID]; err != nil {
		return upstream.Sub2APIAdminAccountState{}, err
	}
	if state, ok := f.statesByID[accountID]; ok {
		return state, nil
	}
	return f.state, nil
}

func (f *fakeFastDispatchActioner) UpdateSub2APIAdminAccountDispatch(_ upstream.Session, accountID string, enabled bool) (upstream.Sub2APIAdminAccountState, error) {
	f.called++
	f.accountID = accountID
	status := "inactive"
	if enabled {
		status = "active"
	}
	schedulable := enabled
	f.state = upstream.Sub2APIAdminAccountState{Status: status, Schedulable: &schedulable}
	return f.state, nil
}

type priorityMirrorActioner struct {
	calls []priorityUpdateCall
	state map[string]upstream.Sub2APIAdminAccountState
}

func (f *priorityMirrorActioner) UpdateAdminTargetPriority(_ upstream.Session, targetID string, priority int) error {
	f.calls = append(f.calls, priorityUpdateCall{targetID: targetID, priority: priority})
	if f.state != nil {
		st := f.state[targetID]
		st.Priority = &priority
		f.state[targetID] = st
	}
	return nil
}

func (f *priorityMirrorActioner) UpdateSub2APIAdminAccountPriority(_ upstream.Session, targetID string, priority int) (upstream.Sub2APIAdminAccountState, error) {
	if err := f.UpdateAdminTargetPriority(upstream.Session{}, targetID, priority); err != nil {
		return upstream.Sub2APIAdminAccountState{}, err
	}
	return f.state[targetID], nil
}

func dispatchTestService(repo *fakeRepository, actions *fakeSchedulingActioner, platform upstream.Platform) (*Service, string) {
	schedulable := true
	workspaceID := "workspace-1"
	accountID := "account-1"
	targetID := buildTargetID(string(platform), workspaceID, accountID)
	return &Service{
		repo:     repo,
		mySites:  fakeMySitesReader{session: upstream.Session{Platform: platform}},
		accounts: fakeAdminAccountResolver{id: workspaceID},
		platformGroups: fakePlatformGroupReader{
			groups: []upstream.AdminGroupInfo{{ID: "group-1", Name: "group"}},
			accountsByGrp: map[string][]upstream.AdminGroupAccountInfo{
				"group-1": {{ID: accountID, Name: "account", Status: "active", Schedulable: &schedulable}},
			},
		},
		schedulingActions: actions,
	}, targetID
}

func TestUpdateTargetDispatchUpdatesBothFieldsAndClearsAutomationTracking(t *testing.T) {
	repo := newFakeRepository()
	actions := &fakeSchedulingActioner{}
	service, targetID := dispatchTestService(repo, actions, upstream.PlatformSub2API)
	repo.states[targetID] = map[string]ConnectionHealthState{
		"gpt-4o": {
			ConnectionID: targetID, ModelName: "gpt-4o", UserID: "user-1", AdminAccountID: "workspace-1",
			State: StateSuspended, CurrentWeight: 0, LastRemoteAction: RemoteActionSub2APIStatusInactive,
		},
	}
	repo.targetActionStates["user-1|workspace-1|"+targetID] = TargetActionState{
		UserID: "user-1", AdminAccountID: "workspace-1", TargetID: targetID,
		OriginalStatus: "active", LastAppliedStatus: "inactive",
	}

	state, err := service.UpdateTargetDispatch(context.Background(), "user-1", targetID, true)
	if err != nil {
		t.Fatalf("enable dispatch: %v", err)
	}
	if state.Status != "active" || !state.Schedulable {
		t.Fatalf("unexpected response: %#v", state)
	}
	if len(actions.calls) != 1 || actions.calls[0].accountID != "account-1" || actions.calls[0].status == nil || *actions.calls[0].status != "active" || actions.calls[0].schedulable == nil || !*actions.calls[0].schedulable {
		t.Fatalf("unexpected remote update: %#v", actions.calls)
	}
	if _, ok := repo.targetActionStates["user-1|workspace-1|"+targetID]; ok {
		t.Fatal("expected health action snapshot to be cleared")
	}
	stored := repo.states[targetID]["gpt-4o"]
	if stored.LastRemoteAction != "" {
		t.Fatalf("expected legacy remote action marker to be cleared, got %q", stored.LastRemoteAction)
	}
}

func TestUpdateTargetDispatchUsesFastAccountPath(t *testing.T) {
	repo := newFakeRepository()
	actioner := &fakeFastDispatchActioner{}
	service := &Service{
		repo:     repo,
		mySites:  fakeMySitesReader{session: upstream.Session{Platform: upstream.PlatformSub2API}},
		accounts: fakeAdminAccountResolver{id: "workspace-1"},
		// Deliberately leave platformGroups nil: the dispatch switch must not
		// enumerate groups or account lists before updating one account.
		dispatchActions: actioner,
	}
	targetID := buildTargetID(string(upstream.PlatformSub2API), "workspace-1", "account-1")

	state, err := service.UpdateTargetDispatch(context.Background(), "user-1", targetID, true)
	if err != nil {
		t.Fatalf("fast dispatch update: %v", err)
	}
	if state.Status != "active" || !state.Schedulable {
		t.Fatalf("unexpected state: %#v", state)
	}
	if actioner.called != 1 || actioner.accountID != "account-1" {
		t.Fatalf("unexpected fast action call: %#v", actioner)
	}
}

func TestUpdateTargetPriorityUpdatesOnlyPriorityAndReturnsLatestState(t *testing.T) {
	repo := newFakeRepository()
	stateReader := &fakeFastDispatchActioner{
		statesByID: map[string]upstream.Sub2APIAdminAccountState{
			"account-1": {Name: "remote account", Status: "active"},
		},
	}
	priorityActions := &priorityMirrorActioner{state: stateReader.statesByID}
	service := &Service{
		repo:            repo,
		mySites:         fakeMySitesReader{session: upstream.Session{Platform: upstream.PlatformSub2API}},
		accounts:        fakeAdminAccountResolver{id: "workspace-1"},
		dispatchStates:  stateReader,
		priorityActions: priorityActions,
	}
	targetID := buildTargetID(string(upstream.PlatformSub2API), "workspace-1", "account-1")
	repo.targetActionStates["user-1|workspace-1|"+targetID] = TargetActionState{
		UserID: "user-1", AdminAccountID: "workspace-1", TargetID: targetID,
		OriginalStatus: "active", LastAppliedStatus: "inactive",
	}

	state, err := service.UpdateTargetPriority(context.Background(), "user-1", targetID, 40999)
	if err != nil {
		t.Fatalf("update priority: %v", err)
	}
	if state.Priority == nil || *state.Priority != 40999 {
		t.Fatalf("expected returned priority=40999, got %#v", state.Priority)
	}
	if len(priorityActions.calls) != 1 || priorityActions.calls[0].priority != 40999 {
		t.Fatalf("unexpected priority calls: %#v", priorityActions.calls)
	}
	if len(stateReader.getAccountIDs) != 0 {
		t.Fatalf("direct priority update must not reread account state, got %#v", stateReader.getAccountIDs)
	}
	if _, ok := repo.targetActionStates["user-1|workspace-1|"+targetID]; !ok {
		t.Fatal("manual priority change must not clear existing automation snapshot")
	}
}

func TestBoundDispatchAccountsReadsOnlyActiveBoundSub2APIAccounts(t *testing.T) {
	schedulable := true
	priority := 17
	reader := &fakeFastDispatchActioner{
		statesByID: map[string]upstream.Sub2APIAdminAccountState{
			"account-1": {Name: "remote account", Status: "active", Schedulable: &schedulable, Priority: &priority},
		},
		getErrors: map[string]error{"missing": &upstream.RequestError{StatusCode: 404}},
	}
	service := &Service{
		mySites: fakeMySitesReader{
			session: upstream.Session{Platform: upstream.PlatformSub2API},
			connections: []my_sites.RealConnection{
				{UserID: "user-1", WorkspaceAdminAccountID: "workspace-1", AdminAccountID: "account-1", AdminAccountName: "cached account", AdminPlatform: "sub2api", Status: my_sites.ConnectionStatusActive},
				{UserID: "user-1", WorkspaceAdminAccountID: "workspace-1", AdminAccountID: "account-1", AdminPlatform: "sub2api", Status: my_sites.ConnectionStatusActive},
				{UserID: "user-1", WorkspaceAdminAccountID: "workspace-1", AdminAccountID: "missing", AdminAccountName: "missing account", AdminPlatform: "sub2api", Status: my_sites.ConnectionStatusActive},
				{UserID: "user-1", WorkspaceAdminAccountID: "workspace-1", AdminAccountID: "inactive-binding", AdminPlatform: "sub2api", Status: "inactive"},
				{UserID: "user-1", WorkspaceAdminAccountID: "workspace-1", AdminAccountID: "newapi-channel", AdminPlatform: "newapi", Status: my_sites.ConnectionStatusActive},
			},
		},
		accounts:       fakeAdminAccountResolver{id: "workspace-1"},
		dispatchStates: reader,
	}

	accounts, err := service.BoundDispatchAccounts(context.Background(), "user-1")
	if err != nil {
		t.Fatalf("bound dispatch accounts: %v", err)
	}
	if len(accounts) != 2 {
		t.Fatalf("expected two unique active Sub2API accounts, got %#v", accounts)
	}
	byID := map[string]BoundDispatchAccountState{}
	for _, account := range accounts {
		byID[account.ID] = account
	}
	available := byID["account-1"]
	if !available.Available || available.Name != "remote account" || available.Status != "active" || available.Schedulable == nil || !*available.Schedulable {
		t.Fatalf("unexpected available account: %#v", available)
	}
	if available.Priority == nil || *available.Priority != 17 {
		t.Fatalf("expected priority=17, got %#v", available.Priority)
	}
	if available.TargetID != buildTargetID(string(upstream.PlatformSub2API), "workspace-1", "account-1") {
		t.Fatalf("unexpected target id: %q", available.TargetID)
	}
	unavailable := byID["missing"]
	if unavailable.Available || unavailable.Schedulable != nil || unavailable.Name != "missing account" || unavailable.UnavailableReason != "not_found" {
		t.Fatalf("failed detail lookup must remain unavailable with cached metadata: %#v", unavailable)
	}
	if len(reader.getAccountIDs) != 2 {
		t.Fatalf("expected one detail request per unique account, got %#v", reader.getAccountIDs)
	}
}

func TestUpdateTargetDispatchDisableCannotBeLegacyRestored(t *testing.T) {
	repo := newFakeRepository()
	actions := &fakeSchedulingActioner{}
	service, targetID := dispatchTestService(repo, actions, upstream.PlatformSub2API)
	repo.states[targetID] = map[string]ConnectionHealthState{
		"gpt-4o": {
			ConnectionID: targetID, ModelName: "gpt-4o", UserID: "user-1", AdminAccountID: "workspace-1",
			State: StateSuspended, CurrentWeight: 0, LastRemoteAction: RemoteActionSub2APIStatusInactive,
		},
	}

	if _, err := service.UpdateTargetDispatch(context.Background(), "user-1", targetID, false); err != nil {
		t.Fatalf("disable dispatch: %v", err)
	}
	states, err := repo.ListStatesByConnection(context.Background(), targetID)
	if err != nil {
		t.Fatalf("list states: %v", err)
	}
	if legacyTargetWasManaged(states) {
		t.Fatal("manual dispatch disable must not remain eligible for legacy automatic restore")
	}
}

func TestUpdateTargetDispatchEnableReturnsControlToHealthPolicy(t *testing.T) {
	repo := newFakeRepository()
	actions := &fakeSchedulingActioner{}
	service, targetID := dispatchTestService(repo, actions, upstream.PlatformSub2API)
	repo.states[targetID] = map[string]ConnectionHealthState{
		"gpt-4o": {
			ConnectionID: targetID, ModelName: "gpt-4o", UserID: "user-1", AdminAccountID: "workspace-1",
			State: StateSuspended, CurrentWeight: 0, LastRemoteAction: RemoteActionSub2APIStatusInactive,
		},
	}
	repo.targetActionStates["user-1|workspace-1|"+targetID] = TargetActionState{
		UserID: "user-1", AdminAccountID: "workspace-1", TargetID: targetID,
		OriginalStatus: "active", LastAppliedStatus: "inactive",
	}

	if _, err := service.UpdateTargetDispatch(context.Background(), "user-1", targetID, true); err != nil {
		t.Fatalf("enable dispatch: %v", err)
	}
	platform := &fakePlatformActioner{}
	service.dispatcher = newRemoteActionDispatcher(nil, nil, platform)
	policy := Policy{ID: "policy-1", Enabled: true, AutoDegradeEnabled: true, AutoRemoteActionEnabled: true}
	target := AdminProbeTarget{
		TargetID: targetID, Platform: string(upstream.PlatformSub2API), AccountID: "account-1", AccountStatus: "active",
	}

	if _, err := service.reconcileTargetRemoteAction(context.Background(), "user-1", "workspace-1", upstream.Session{Platform: upstream.PlatformSub2API}, target, []probeModelSpec{{modelName: "gpt-4o", policy: policy}}); err != nil {
		t.Fatalf("health reconcile after manual enable: %v", err)
	}
	if len(platform.sub2APICalls) != 1 || platform.sub2APICalls[0].status != "inactive" {
		t.Fatalf("expected unhealthy account to be managed again, got %#v", platform.sub2APICalls)
	}
}

func TestUpdateTargetDispatchRejectsInvalidTargets(t *testing.T) {
	tests := []struct {
		name     string
		platform upstream.Platform
		targetID string
		want     requestError
	}{
		{name: "non sub2api", platform: upstream.PlatformNewAPI, want: requestError(ErrorRequest)},
		{name: "cross workspace", platform: upstream.PlatformSub2API, targetID: buildTargetID(string(upstream.PlatformSub2API), "workspace-2", "account-1"), want: requestError(ErrorProbeTargetNotFound)},
		{name: "unknown account", platform: upstream.PlatformSub2API, targetID: buildTargetID(string(upstream.PlatformSub2API), "workspace-1", "missing"), want: requestError(ErrorProbeTargetNotFound)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			repo := newFakeRepository()
			actions := &fakeSchedulingActioner{}
			service, defaultTargetID := dispatchTestService(repo, actions, test.platform)
			targetID := test.targetID
			if targetID == "" {
				targetID = defaultTargetID
			}
			_, err := service.UpdateTargetDispatch(context.Background(), "user-1", targetID, true)
			var requestErr requestError
			if !errors.As(err, &requestErr) || requestErr != test.want {
				t.Fatalf("expected %q, got %v", test.want, err)
			}
			if len(actions.calls) != 0 {
				t.Fatalf("invalid target must not update remote account: %#v", actions.calls)
			}
		})
	}
}
