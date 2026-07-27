package connection_health

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"
	"testing"
	"time"

	"transithub/backend/internal/modules/my_sites"
	"transithub/backend/internal/modules/upstream"
)

func groupRateWorkspaceKey(userID, adminAccountID string) string {
	return userID + "|" + adminAccountID
}

func groupRateStateKey(userID, adminAccountID, siteID, groupKey, targetID string) string {
	return userID + "|" + adminAccountID + "|" + siteID + "|" + groupKey + "|" + targetID
}

func (f *fakeRepository) GetGroupRateMonitorSettings(_ context.Context, userID, adminAccountID string) (GroupRateMonitorSettings, error) {
	if value, ok := f.groupRateSettings[groupRateWorkspaceKey(userID, adminAccountID)]; ok {
		return value, nil
	}
	return defaultGroupRateMonitorSettings(userID, adminAccountID), nil
}

func (f *fakeRepository) SaveGroupRateMonitorSettings(_ context.Context, settings GroupRateMonitorSettings, overrides []GroupRateMonitorOverride) error {
	f.groupRateSettings[groupRateWorkspaceKey(settings.UserID, settings.AdminAccountID)] = settings
	f.groupRateOverrides[groupRateWorkspaceKey(settings.UserID, settings.AdminAccountID)] = append([]GroupRateMonitorOverride(nil), overrides...)
	return nil
}

func (f *fakeRepository) ListEnabledGroupRateMonitorSettings(context.Context) ([]GroupRateMonitorSettings, error) {
	result := make([]GroupRateMonitorSettings, 0)
	for _, settings := range f.groupRateSettings {
		if settings.Enabled {
			result = append(result, settings)
		}
	}
	return result, nil
}

func (f *fakeRepository) ListGroupRateMonitorOverrides(_ context.Context, userID, adminAccountID string) ([]GroupRateMonitorOverride, error) {
	return append([]GroupRateMonitorOverride(nil), f.groupRateOverrides[groupRateWorkspaceKey(userID, adminAccountID)]...), nil
}

func (f *fakeRepository) GetGroupRateMonitorState(_ context.Context, userID, adminAccountID, siteID, groupKey, targetID string) (*GroupRateMonitorTargetState, error) {
	state, ok := f.groupRateStates[groupRateStateKey(userID, adminAccountID, siteID, groupKey, targetID)]
	if !ok {
		return nil, nil
	}
	copy := state
	return &copy, nil
}

func (f *fakeRepository) UpsertGroupRateMonitorState(_ context.Context, state GroupRateMonitorTargetState) error {
	f.groupRateStates[groupRateStateKey(state.UserID, state.AdminAccountID, state.UpstreamSiteID, state.UpstreamGroupKey, state.TargetID)] = state
	return nil
}

func (f *fakeRepository) InsertGroupRateMonitorCycle(_ context.Context, cycle GroupRateProbeCycle) error {
	f.groupRateCycles = append(f.groupRateCycles, cycle)
	return nil
}

func (f *fakeRepository) ListGroupRateMonitorCycles(_ context.Context, userID, adminAccountID string, perGroupLimit int) ([]GroupRateProbeCycle, error) {
	byGroup := map[string][]GroupRateProbeCycle{}
	for _, cycle := range f.groupRateCycles {
		if cycle.UserID == userID && cycle.AdminAccountID == adminAccountID {
			key := groupRateMonitorMapKey(cycle.UpstreamSiteID, cycle.UpstreamGroupKey)
			byGroup[key] = append(byGroup[key], cycle)
		}
	}
	result := make([]GroupRateProbeCycle, 0)
	for _, cycles := range byGroup {
		sort.Slice(cycles, func(i, j int) bool { return cycles[i].CreatedAt.Before(cycles[j].CreatedAt) })
		if len(cycles) > perGroupLimit {
			cycles = cycles[len(cycles)-perGroupLimit:]
		}
		result = append(result, cycles...)
	}
	return result, nil
}

func (f *fakeRepository) ListLatestGroupRateMonitorCycles(_ context.Context, userID, adminAccountID string) (map[string]time.Time, error) {
	result := map[string]time.Time{}
	for _, cycle := range f.groupRateCycles {
		if cycle.UserID != userID || cycle.AdminAccountID != adminAccountID {
			continue
		}
		key := groupRateMonitorMapKey(cycle.UpstreamSiteID, cycle.UpstreamGroupKey)
		if cycle.CreatedAt.After(result[key]) {
			result[key] = cycle.CreatedAt
		}
	}
	return result, nil
}

func (f *fakeRepository) GetGroupRateMonitorAction(_ context.Context, userID, adminAccountID, targetID string) (*GroupRateMonitorActionState, error) {
	state, ok := f.groupRateActions[groupRateWorkspaceKey(userID, adminAccountID)+"|"+targetID]
	if !ok {
		return nil, nil
	}
	copy := state
	return &copy, nil
}

func (f *fakeRepository) ListGroupRateMonitorActions(_ context.Context, userID, adminAccountID string) ([]GroupRateMonitorActionState, error) {
	result := make([]GroupRateMonitorActionState, 0)
	for _, state := range f.groupRateActions {
		if state.UserID == userID && state.AdminAccountID == adminAccountID {
			result = append(result, state)
		}
	}
	return result, nil
}

func (f *fakeRepository) ListPendingGroupRateMonitorActions(context.Context) ([]GroupRateMonitorActionState, error) {
	result := make([]GroupRateMonitorActionState, 0)
	for _, state := range f.groupRateActions {
		if state.PendingRestore {
			result = append(result, state)
		}
	}
	return result, nil
}

func (f *fakeRepository) UpsertGroupRateMonitorAction(_ context.Context, state GroupRateMonitorActionState) error {
	f.groupRateActions[groupRateWorkspaceKey(state.UserID, state.AdminAccountID)+"|"+state.TargetID] = state
	return nil
}

func (f *fakeRepository) DeleteGroupRateMonitorAction(_ context.Context, userID, adminAccountID, targetID string) error {
	delete(f.groupRateActions, groupRateWorkspaceKey(userID, adminAccountID)+"|"+targetID)
	return nil
}

func (f *fakeRepository) MarkGroupRateMonitorActionConflict(_ context.Context, userID, adminAccountID, targetID string) error {
	key := groupRateWorkspaceKey(userID, adminAccountID) + "|" + targetID
	state, ok := f.groupRateActions[key]
	if ok {
		state.Conflict = true
		f.groupRateActions[key] = state
	}
	return nil
}

type groupRateMonitorAccountActioner struct {
	mu     sync.Mutex
	states map[string]upstream.Sub2APIAdminAccountState
	calls  []struct {
		accountID string
		state     TargetDispatchState
	}
}

func (f *groupRateMonitorAccountActioner) GetSub2APIAdminAccountState(_ upstream.Session, accountID string) (upstream.Sub2APIAdminAccountState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	state, ok := f.states[accountID]
	if !ok {
		return upstream.Sub2APIAdminAccountState{}, errors.New("account not found")
	}
	return state, nil
}

func (f *groupRateMonitorAccountActioner) UpdateSub2APIAdminAccountState(_ upstream.Session, accountID string, status *string, schedulable *bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	state := f.states[accountID]
	if status != nil {
		state.Status = *status
	}
	if schedulable != nil {
		value := *schedulable
		state.Schedulable = &value
	}
	f.states[accountID] = state
	f.calls = append(f.calls, struct {
		accountID string
		state     TargetDispatchState
	}{accountID: accountID, state: TargetDispatchState{Status: state.Status, Schedulable: *state.Schedulable}})
	return nil
}

func TestGroupRateMonitorFailureThresholdStopsAndSuccessRestores(t *testing.T) {
	attempt := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer upstream-group-key" {
			t.Errorf("authorization = %q", got)
		}
		attempt++
		if attempt <= 2 {
			http.Error(w, `{"error":"failed"}`, http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"ok"}}]}`))
	}))
	defer server.Close()

	repo := newFakeRepository()
	schedulable := true
	actions := &groupRateMonitorAccountActioner{states: map[string]upstream.Sub2APIAdminAccountState{
		"account-1": {Name: "account 1", Status: "active", Schedulable: &schedulable},
		"account-2": {Name: "account 2", Status: "active", Schedulable: &schedulable},
	}}
	service := &Service{repo: repo, mySites: fakeMySitesReader{}, sites: fakeSiteLookup{site: &upstream.Site{ID: "site-1", BaseURL: server.URL, Platform: upstream.PlatformNewAPI}},
		probeRunner: NewRealProbeRunner(), dispatchStates: actions, schedulingActions: actions}
	settings := GroupRateMonitorSettings{UserID: "user-1", AdminAccountID: "ws-1", Enabled: true,
		ProbeIntervalSeconds: 30, FailureThreshold: 2, DefaultModel: "gpt-test"}
	group := groupRateMonitorGroup{SiteID: "site-1", GroupID: "group-1", GroupName: "福利",
		GroupKey: groupRateMonitorGroupKey("group-1", "福利"), Accounts: []my_sites.RealConnection{
			{AdminAccountID: "account-1", AdminAccountName: "account 1", AdminPlatform: "sub2api", UpstreamKey: "upstream-group-key"},
			{AdminAccountID: "account-2", AdminAccountName: "account 2", AdminPlatform: "sub2api", UpstreamKey: "another-key-for-same-group"},
		}}
	session := upstream.Session{Platform: upstream.PlatformSub2API}

	first, _, err := service.runGroupRateProbeCycle(context.Background(), settings, session, group, "gpt-test", "manual")
	if err != nil {
		t.Fatalf("first probe: %v", err)
	}
	if first.Status != groupRateProbeWarning || len(actions.calls) != 0 {
		t.Fatalf("first failure must warn without disabling: cycle=%+v calls=%+v", first, actions.calls)
	}
	second, _, err := service.runGroupRateProbeCycle(context.Background(), settings, session, group, "gpt-test", "manual")
	if err != nil {
		t.Fatalf("second probe: %v", err)
	}
	if second.Status != groupRateProbeUnhealthy || len(actions.calls) != 2 || actions.calls[0].state.Status != "inactive" || actions.calls[0].state.Schedulable || actions.calls[1].state.Status != "inactive" || actions.calls[1].state.Schedulable {
		t.Fatalf("second failure must disable both switches: cycle=%+v calls=%+v", second, actions.calls)
	}
	third, _, err := service.runGroupRateProbeCycle(context.Background(), settings, session, group, "gpt-test", "manual")
	if err != nil {
		t.Fatalf("recovery probe: %v", err)
	}
	if third.Status != groupRateProbeHealthy || len(actions.calls) != 4 || actions.calls[2].state.Status != "active" || !actions.calls[2].state.Schedulable || actions.calls[3].state.Status != "active" || !actions.calls[3].state.Schedulable {
		t.Fatalf("one success must restore owned account: cycle=%+v calls=%+v", third, actions.calls)
	}
	if attempt != 3 {
		t.Fatalf("expected one upstream request per group cycle, got %d", attempt)
	}
	if len(repo.groupRateActions) != 0 {
		t.Fatalf("restored action snapshot must be removed: %+v", repo.groupRateActions)
	}
}

func TestGroupRateMonitorManualProbeReclaimsConflictedEnabledAccount(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, `{"error":"failed"}`, http.StatusInternalServerError)
	}))
	defer server.Close()

	repo := newFakeRepository()
	schedulable := true
	actions := &groupRateMonitorAccountActioner{states: map[string]upstream.Sub2APIAdminAccountState{
		"account-1": {Name: "account 1", Status: "active", Schedulable: &schedulable},
	}}
	service := &Service{repo: repo, sites: fakeSiteLookup{site: &upstream.Site{ID: "site-1", BaseURL: server.URL}},
		probeRunner: NewRealProbeRunner(), dispatchStates: actions, schedulingActions: actions}
	settings := GroupRateMonitorSettings{UserID: "user-1", AdminAccountID: "ws-1", Enabled: true,
		ProbeIntervalSeconds: 30, FailureThreshold: 2, DefaultModel: "gpt-test"}
	group := groupRateMonitorGroup{SiteID: "site-1", GroupID: "group-1", GroupName: "福利",
		GroupKey: groupRateMonitorGroupKey("group-1", "福利"), Accounts: []my_sites.RealConnection{{
			AdminAccountID: "account-1", AdminAccountName: "account 1", AdminPlatform: "sub2api", UpstreamKey: "upstream-group-key",
		}}}
	stateTargetID := "upstream-group:" + groupRateMonitorMapKey(group.SiteID, group.GroupKey)
	repo.groupRateStates[groupRateStateKey(settings.UserID, settings.AdminAccountID, group.SiteID, group.GroupKey, stateTargetID)] = GroupRateMonitorTargetState{
		UserID: settings.UserID, AdminAccountID: settings.AdminAccountID, UpstreamSiteID: group.SiteID,
		UpstreamGroupKey: group.GroupKey, TargetID: stateTargetID, ConsecutiveFailures: 1,
	}
	accountTargetID := buildTargetID(string(upstream.PlatformSub2API), settings.AdminAccountID, "account-1")
	repo.groupRateActions[groupRateWorkspaceKey(settings.UserID, settings.AdminAccountID)+"|"+accountTargetID] = GroupRateMonitorActionState{
		UserID: settings.UserID, AdminAccountID: settings.AdminAccountID, TargetID: accountTargetID,
		AccountID: "account-1", AccountName: "account 1", UpstreamSiteID: group.SiteID, UpstreamGroupKey: group.GroupKey,
		OriginalStatus: "active", OriginalSchedulable: true, LastAppliedStatus: "inactive",
		LastAppliedSchedulable: false, Conflict: true,
	}

	cycle, dispatch, err := service.runGroupRateProbeCycle(context.Background(), settings, upstream.Session{Platform: upstream.PlatformSub2API}, group, "gpt-test", "manual")
	if err != nil {
		t.Fatalf("manual probe: %v", err)
	}
	if cycle.Status != groupRateProbeUnhealthy {
		t.Fatalf("threshold failure must be unhealthy: %+v", cycle)
	}
	if len(actions.calls) != 1 || actions.calls[0].state.Status != "inactive" || actions.calls[0].state.Schedulable {
		t.Fatalf("manual probe must reclaim and disable the account: %+v", actions.calls)
	}
	if len(dispatch) != 1 || dispatch[0].ActionResult != "disabled" || dispatch[0].Status != "inactive" || dispatch[0].Schedulable == nil || *dispatch[0].Schedulable {
		t.Fatalf("manual response must return the disabled dispatch state: %+v", dispatch)
	}
	stored := repo.groupRateActions[groupRateWorkspaceKey(settings.UserID, settings.AdminAccountID)+"|"+accountTargetID]
	if stored.Conflict || stored.LastAppliedStatus != "inactive" || stored.LastAppliedSchedulable {
		t.Fatalf("reclaimed action state was not persisted: %+v", stored)
	}
}

func TestGroupRateMonitorNormalizesPartiallyDisabledAccount(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, `{"error":"failed"}`, http.StatusInternalServerError)
	}))
	defer server.Close()

	repo := newFakeRepository()
	schedulable := true
	actions := &groupRateMonitorAccountActioner{states: map[string]upstream.Sub2APIAdminAccountState{
		"account-1": {Name: "account 1", Status: "inactive", Schedulable: &schedulable},
	}}
	service := &Service{repo: repo, sites: fakeSiteLookup{site: &upstream.Site{ID: "site-1", BaseURL: server.URL}},
		probeRunner: NewRealProbeRunner(), dispatchStates: actions, schedulingActions: actions}
	settings := GroupRateMonitorSettings{UserID: "user-1", AdminAccountID: "ws-1", Enabled: true,
		ProbeIntervalSeconds: 30, FailureThreshold: 2, DefaultModel: "gpt-test"}
	group := groupRateMonitorGroup{SiteID: "site-1", GroupID: "group-1", GroupName: "福利",
		GroupKey: groupRateMonitorGroupKey("group-1", "福利"), Accounts: []my_sites.RealConnection{{
			AdminAccountID: "account-1", AdminAccountName: "account 1", AdminPlatform: "sub2api", UpstreamKey: "upstream-group-key",
		}}}
	stateTargetID := "upstream-group:" + groupRateMonitorMapKey(group.SiteID, group.GroupKey)
	repo.groupRateStates[groupRateStateKey(settings.UserID, settings.AdminAccountID, group.SiteID, group.GroupKey, stateTargetID)] = GroupRateMonitorTargetState{
		UserID: settings.UserID, AdminAccountID: settings.AdminAccountID, UpstreamSiteID: group.SiteID,
		UpstreamGroupKey: group.GroupKey, TargetID: stateTargetID, ConsecutiveFailures: 1,
	}

	_, dispatch, err := service.runGroupRateProbeCycle(context.Background(), settings, upstream.Session{Platform: upstream.PlatformSub2API}, group, "gpt-test", "manual")
	if err != nil {
		t.Fatalf("manual probe: %v", err)
	}
	if len(actions.calls) != 1 || actions.calls[0].state.Status != "inactive" || actions.calls[0].state.Schedulable {
		t.Fatalf("partially disabled account must be normalized to inactive and unschedulable: %+v", actions.calls)
	}
	if len(dispatch) != 1 || dispatch[0].ActionResult != "disabled" || dispatch[0].Schedulable == nil || *dispatch[0].Schedulable {
		t.Fatalf("normalized dispatch state must be returned to the UI: %+v", dispatch)
	}
	if len(repo.groupRateActions) != 0 {
		t.Fatalf("an initially disabled account must not be claimed for later restoration: %+v", repo.groupRateActions)
	}
}

func TestGroupRateMonitorManualSuccessRestoresOrphanedDisabledAccount(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"ok"}}]}`))
	}))
	defer server.Close()

	repo := newFakeRepository()
	schedulable := false
	actions := &groupRateMonitorAccountActioner{states: map[string]upstream.Sub2APIAdminAccountState{
		"account-1": {Name: "account 1", Status: "inactive", Schedulable: &schedulable},
	}}
	service := &Service{repo: repo, sites: fakeSiteLookup{site: &upstream.Site{ID: "site-1", BaseURL: server.URL}},
		probeRunner: NewRealProbeRunner(), dispatchStates: actions, schedulingActions: actions}
	settings := GroupRateMonitorSettings{UserID: "user-1", AdminAccountID: "ws-1", Enabled: true,
		ProbeIntervalSeconds: 30, FailureThreshold: 2, DefaultModel: "gpt-test"}
	group := groupRateMonitorGroup{SiteID: "site-1", GroupID: "group-1", GroupName: "福利",
		GroupKey: groupRateMonitorGroupKey("group-1", "福利"), Accounts: []my_sites.RealConnection{{
			AdminAccountID: "account-1", AdminAccountName: "account 1", AdminPlatform: "sub2api", UpstreamKey: "upstream-group-key",
		}}}

	cycle, dispatch, err := service.runGroupRateProbeCycle(context.Background(), settings, upstream.Session{Platform: upstream.PlatformSub2API}, group, "gpt-test", "manual")
	if err != nil {
		t.Fatalf("manual recovery probe: %v", err)
	}
	if cycle.Status != groupRateProbeHealthy {
		t.Fatalf("successful probe must be healthy: %+v", cycle)
	}
	if len(actions.calls) != 1 || actions.calls[0].state.Status != "active" || !actions.calls[0].state.Schedulable {
		t.Fatalf("manual successful probe must restore an orphaned disabled account: %+v", actions.calls)
	}
	if len(dispatch) != 1 || dispatch[0].ActionResult != "restored" || dispatch[0].Status != "active" || dispatch[0].Schedulable == nil || !*dispatch[0].Schedulable {
		t.Fatalf("manual response must return restored dispatch state: %+v", dispatch)
	}
}

func TestGroupRateMonitorAutomaticSuccessDoesNotRestoreUnownedDisabledAccount(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"ok"}}]}`))
	}))
	defer server.Close()

	repo := newFakeRepository()
	schedulable := false
	actions := &groupRateMonitorAccountActioner{states: map[string]upstream.Sub2APIAdminAccountState{
		"account-1": {Name: "account 1", Status: "inactive", Schedulable: &schedulable},
	}}
	service := &Service{repo: repo, sites: fakeSiteLookup{site: &upstream.Site{ID: "site-1", BaseURL: server.URL}},
		probeRunner: NewRealProbeRunner(), dispatchStates: actions, schedulingActions: actions}
	settings := GroupRateMonitorSettings{UserID: "user-1", AdminAccountID: "ws-1", Enabled: true,
		ProbeIntervalSeconds: 30, FailureThreshold: 2, DefaultModel: "gpt-test"}
	group := groupRateMonitorGroup{SiteID: "site-1", GroupID: "group-1", GroupName: "福利",
		GroupKey: groupRateMonitorGroupKey("group-1", "福利"), Accounts: []my_sites.RealConnection{{
			AdminAccountID: "account-1", AdminAccountName: "account 1", AdminPlatform: "sub2api", UpstreamKey: "upstream-group-key",
		}}}

	_, dispatch, err := service.runGroupRateProbeCycle(context.Background(), settings, upstream.Session{Platform: upstream.PlatformSub2API}, group, "gpt-test", "scheduled")
	if err != nil {
		t.Fatalf("automatic recovery probe: %v", err)
	}
	if len(actions.calls) != 0 {
		t.Fatalf("automatic probe must not restore an unowned manually disabled account: %+v", actions.calls)
	}
	if len(dispatch) != 1 || dispatch[0].Schedulable == nil || *dispatch[0].Schedulable {
		t.Fatalf("automatic response must preserve the disabled dispatch state: %+v", dispatch)
	}
}

func TestGroupRateMonitorMissingDownstreamAccountDoesNotHideHealthyUpstream(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"ok"}}]}`))
	}))
	defer server.Close()

	repo := newFakeRepository()
	actions := &groupRateMonitorAccountActioner{states: map[string]upstream.Sub2APIAdminAccountState{}}
	service := &Service{repo: repo, sites: fakeSiteLookup{site: &upstream.Site{ID: "site-1", BaseURL: server.URL, Platform: upstream.PlatformSub2API}},
		probeRunner: NewRealProbeRunner(), dispatchStates: actions, schedulingActions: actions}
	settings := GroupRateMonitorSettings{UserID: "user-1", AdminAccountID: "ws-1", Enabled: true,
		ProbeIntervalSeconds: 30, FailureThreshold: 2, DefaultModel: "gpt-test"}
	group := groupRateMonitorGroup{SiteID: "site-1", GroupID: "group-1", GroupName: "福利",
		GroupKey: groupRateMonitorGroupKey("group-1", "福利"), Accounts: []my_sites.RealConnection{{
			AdminAccountID: "missing-account", AdminAccountName: "missing", AdminPlatform: "sub2api", UpstreamKey: "upstream-group-key",
		}}}

	cycle, dispatch, err := service.runGroupRateProbeCycle(context.Background(), settings, upstream.Session{Platform: upstream.PlatformSub2API}, group, "gpt-test", "manual")
	if err != nil {
		t.Fatalf("probe healthy upstream: %v", err)
	}
	if cycle.Status != groupRateProbeHealthy || len(cycle.Details) != 1 || !cycle.Details[0].Healthy {
		t.Fatalf("upstream health must remain healthy: %+v", cycle)
	}
	if len(dispatch) != 1 || dispatch[0].Available || dispatch[0].UnavailableReason != "not_found" {
		t.Fatalf("missing downstream account should only affect dispatch state: %+v", dispatch)
	}
}

func TestGroupRateMonitorManualProbeWorksWhenAutomaticControlDisabled(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error":"failed"}`, http.StatusInternalServerError)
	}))
	defer server.Close()

	repo := newFakeRepository()
	settings := GroupRateMonitorSettings{UserID: "user-1", AdminAccountID: "ws-1", Enabled: false,
		ProbeIntervalSeconds: 30, FailureThreshold: 2, DefaultModel: "gpt-test"}
	repo.groupRateSettings[groupRateWorkspaceKey(settings.UserID, settings.AdminAccountID)] = settings
	schedulable := true
	actions := &groupRateMonitorAccountActioner{states: map[string]upstream.Sub2APIAdminAccountState{
		"account-1": {Name: "account", Status: "active", Schedulable: &schedulable},
	}}
	connection := my_sites.RealConnection{UserID: "user-1", WorkspaceAdminAccountID: "ws-1",
		UpstreamSiteID: "site-1", UpstreamGroupID: "group-1", UpstreamGroupName: "福利",
		UpstreamKey: "upstream-group-key", AdminAccountID: "account-1", AdminAccountName: "account",
		AdminPlatform: "sub2api", Status: my_sites.ConnectionStatusActive}
	service := &Service{repo: repo, accounts: fakeAdminAccountResolver{id: "ws-1"},
		mySites: fakeMySitesReader{connections: []my_sites.RealConnection{connection}, session: upstream.Session{Platform: upstream.PlatformSub2API}},
		sites:   fakeSiteLookup{site: &upstream.Site{ID: "site-1", BaseURL: server.URL}}, probeRunner: NewRealProbeRunner(),
		dispatchStates: actions, schedulingActions: actions}
	input := GroupRateManualProbeInput{UpstreamSiteID: "site-1", UpstreamGroupID: "group-1", UpstreamGroupName: "福利"}

	first, err := service.ProbeGroupRateMonitor(context.Background(), "user-1", input)
	if err != nil {
		t.Fatalf("first manual probe with automation disabled: %v", err)
	}
	if first.Summary.Enabled || first.Summary.Status != groupRateProbeWarning {
		t.Fatalf("manual result should be visible without enabling automation: %+v", first.Summary)
	}
	second, err := service.ProbeGroupRateMonitor(context.Background(), "user-1", input)
	if err != nil {
		t.Fatalf("second manual probe with automation disabled: %v", err)
	}
	if second.Summary.Status != groupRateProbeUnhealthy {
		t.Fatalf("second manual failure should be unhealthy: %+v", second.Summary)
	}
	if len(actions.calls) != 0 {
		t.Fatalf("manual probes must not change dispatch while automation is disabled: %+v", actions.calls)
	}
	if len(second.DispatchAccounts) != 1 || second.DispatchAccounts[0].Status != "active" || second.DispatchAccounts[0].Schedulable == nil || !*second.DispatchAccounts[0].Schedulable {
		t.Fatalf("dispatch state must remain enabled: %+v", second.DispatchAccounts)
	}
}

func TestGroupRateMonitorUnavailableUpstreamDoesNotDisable(t *testing.T) {
	repo := newFakeRepository()
	schedulable := true
	actions := &groupRateMonitorAccountActioner{states: map[string]upstream.Sub2APIAdminAccountState{
		"account-1": {Status: "active", Schedulable: &schedulable},
	}}
	service := &Service{repo: repo, probeRunner: NewRealProbeRunner(), dispatchStates: actions, schedulingActions: actions}
	settings := GroupRateMonitorSettings{UserID: "user-1", AdminAccountID: "ws-1", Enabled: true, FailureThreshold: 2}
	group := groupRateMonitorGroup{SiteID: "site-1", GroupName: "group", GroupKey: "name:group",
		Accounts: []my_sites.RealConnection{{AdminAccountID: "account-1", AdminPlatform: "sub2api"}}}
	cycle, _, err := service.runGroupRateProbeCycle(context.Background(), settings, upstream.Session{Platform: upstream.PlatformSub2API}, group, "gpt-test", "manual")
	if err != nil {
		t.Fatalf("probe unavailable account: %v", err)
	}
	if cycle.Status != groupRateProbeUnavailable || len(actions.calls) != 0 {
		t.Fatalf("unavailable target must remain gray without remote action: cycle=%+v calls=%+v", cycle, actions.calls)
	}
}

func TestGroupRateProbeStatusUsesUpstreamGroupResult(t *testing.T) {
	if got := groupRateProbeStatus(GroupRateProbeTargetResult{Available: true, ConsecutiveFailures: 1}, 2); got != groupRateProbeWarning {
		t.Fatalf("one pending failure = %s", got)
	}
	if got := groupRateProbeStatus(GroupRateProbeTargetResult{Available: true, ConsecutiveFailures: 2}, 2); got != groupRateProbeUnhealthy {
		t.Fatalf("threshold failure = %s", got)
	}
	if got := groupRateProbeStatus(GroupRateProbeTargetResult{UnavailableReason: upstream.ReasonCredentialUnavailable}, 2); got != groupRateProbeUnavailable {
		t.Fatalf("unavailable upstream group = %s", got)
	}
}

func TestGroupRateMonitorSettingsValidation(t *testing.T) {
	if err := validateGroupRateMonitorInput(GroupRateMonitorSettingsInput{Enabled: true, ProbeIntervalSeconds: 30, FailureThreshold: 2}); !errors.Is(err, requestError(ErrorGroupRateMonitorModelRequired)) {
		t.Fatalf("missing model error = %v", err)
	}
	if err := validateGroupRateMonitorInput(GroupRateMonitorSettingsInput{ProbeIntervalSeconds: 9, FailureThreshold: 2}); err == nil {
		t.Fatal("interval below minimum must fail")
	}
	if err := validateGroupRateMonitorInput(GroupRateMonitorSettingsInput{Enabled: true, ProbeIntervalSeconds: 30, FailureThreshold: 2, DefaultModel: "gpt-test"}); err != nil {
		t.Fatalf("valid settings: %v", err)
	}
}
