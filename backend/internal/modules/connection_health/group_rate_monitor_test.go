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

type groupRateSiteLookup map[string]*upstream.Site

func (f groupRateSiteLookup) GetSite(_ context.Context, siteID string) (*upstream.Site, error) {
	return f[siteID], nil
}

func (f *fakeRepository) GetGroupRateMonitorSettings(_ context.Context, userID, adminAccountID string) (GroupRateMonitorSettings, error) {
	if value, ok := f.groupRateSettings[groupRateWorkspaceKey(userID, adminAccountID)]; ok {
		return value, nil
	}
	return defaultGroupRateMonitorSettings(userID, adminAccountID), nil
}

func (f *fakeRepository) SaveGroupRateMonitorSettings(_ context.Context, settings GroupRateMonitorSettings, typeDefaults []GroupRateMonitorTypeDefault, overrides []GroupRateMonitorOverride) error {
	f.groupRateSettings[groupRateWorkspaceKey(settings.UserID, settings.AdminAccountID)] = settings
	f.groupRateTypeDefaults[groupRateWorkspaceKey(settings.UserID, settings.AdminAccountID)] = append([]GroupRateMonitorTypeDefault(nil), typeDefaults...)
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

func (f *fakeRepository) ListGroupRateMonitorTypeDefaults(_ context.Context, userID, adminAccountID string) ([]GroupRateMonitorTypeDefault, error) {
	return append([]GroupRateMonitorTypeDefault(nil), f.groupRateTypeDefaults[groupRateWorkspaceKey(userID, adminAccountID)]...), nil
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

func TestGroupRateMonitorAutomaticFailureThresholdStopsAndSuccessRestores(t *testing.T) {
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

	first, _, err := service.runGroupRateProbeCycle(context.Background(), settings, session, group, "gpt-test", "scheduled")
	if err != nil {
		t.Fatalf("first probe: %v", err)
	}
	if first.Status != groupRateProbeWarning || len(actions.calls) != 0 {
		t.Fatalf("first failure must warn without disabling: cycle=%+v calls=%+v", first, actions.calls)
	}
	second, _, err := service.runGroupRateProbeCycle(context.Background(), settings, session, group, "gpt-test", "scheduled")
	if err != nil {
		t.Fatalf("second probe: %v", err)
	}
	if second.Status != groupRateProbeUnhealthy || len(actions.calls) != 2 || actions.calls[0].state.Status != "inactive" || actions.calls[0].state.Schedulable || actions.calls[1].state.Status != "inactive" || actions.calls[1].state.Schedulable {
		t.Fatalf("second failure must disable both switches: cycle=%+v calls=%+v", second, actions.calls)
	}
	third, _, err := service.runGroupRateProbeCycle(context.Background(), settings, session, group, "gpt-test", "scheduled")
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

func TestGroupRateMonitorAutomaticProbeReclaimsConflictedEnabledAccount(t *testing.T) {
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

	cycle, dispatch, err := service.runGroupRateProbeCycle(context.Background(), settings, upstream.Session{Platform: upstream.PlatformSub2API}, group, "gpt-test", "scheduled")
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

	_, dispatch, err := service.runGroupRateProbeCycle(context.Background(), settings, upstream.Session{Platform: upstream.PlatformSub2API}, group, "gpt-test", "scheduled")
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
	settings := GroupRateMonitorSettings{UserID: "user-1", AdminAccountID: "ws-1", Enabled: false,
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

func TestGroupRateMonitorAutomaticSuccessOverridesManualDisabledAccount(t *testing.T) {
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
	if len(actions.calls) != 1 || actions.calls[0].state.Status != "active" || !actions.calls[0].state.Schedulable {
		t.Fatalf("automatic probe must restore a manually disabled account: %+v", actions.calls)
	}
	if len(dispatch) != 1 || dispatch[0].Schedulable == nil || !*dispatch[0].Schedulable {
		t.Fatalf("automatic response must return the restored dispatch state: %+v", dispatch)
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
	if len(actions.calls) != 1 || actions.calls[0].state.Status != "inactive" || actions.calls[0].state.Schedulable {
		t.Fatalf("manual probe must disable dispatch after reaching the threshold: %+v", actions.calls)
	}
	if len(second.DispatchAccounts) != 1 || second.DispatchAccounts[0].Status != "inactive" || second.DispatchAccounts[0].Schedulable == nil || *second.DispatchAccounts[0].Schedulable {
		t.Fatalf("dispatch state must be disabled: %+v", second.DispatchAccounts)
	}
}

func TestGroupRateMonitorManualProbeDoesNotControlDispatchWhenAutomaticEnabled(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"ok"}}]}`))
	}))
	defer server.Close()

	repo := newFakeRepository()
	schedulable := false
	actions := &groupRateMonitorAccountActioner{states: map[string]upstream.Sub2APIAdminAccountState{
		"account-1": {Name: "account", Status: "inactive", Schedulable: &schedulable},
	}}
	service := &Service{repo: repo, sites: fakeSiteLookup{site: &upstream.Site{ID: "site-1", BaseURL: server.URL}},
		probeRunner: NewRealProbeRunner(), dispatchStates: actions, schedulingActions: actions}
	settings := GroupRateMonitorSettings{UserID: "user-1", AdminAccountID: "ws-1", Enabled: true,
		ProbeIntervalSeconds: 30, FailureThreshold: 2, DefaultModel: "gpt-test"}
	group := groupRateMonitorGroup{SiteID: "site-1", GroupID: "group-1", GroupName: "福利",
		GroupKey: groupRateMonitorGroupKey("group-1", "福利"), Accounts: []my_sites.RealConnection{{
			AdminAccountID: "account-1", AdminAccountName: "account", AdminPlatform: "sub2api", UpstreamKey: "upstream-group-key",
		}}}

	cycle, dispatch, err := service.runGroupRateProbeCycle(context.Background(), settings, upstream.Session{Platform: upstream.PlatformSub2API}, group, "gpt-test", "manual")
	if err != nil {
		t.Fatalf("manual probe: %v", err)
	}
	if cycle.Status != groupRateProbeHealthy || len(actions.calls) != 0 {
		t.Fatalf("manual probe must only record while automatic probing is enabled: cycle=%+v calls=%+v", cycle, actions.calls)
	}
	if len(dispatch) != 1 || dispatch[0].Schedulable == nil || *dispatch[0].Schedulable {
		t.Fatalf("manual probe must preserve the current dispatch state: %+v", dispatch)
	}
}

func TestResolveGroupRateMonitorConfigPrecedence(t *testing.T) {
	settings := GroupRateMonitorSettings{Enabled: true, ProbeIntervalSeconds: 30, FailureThreshold: 2, DefaultModel: "global-model"}
	group := groupRateMonitorGroup{SiteID: "site-1", GroupID: "group-1", GroupName: "福利", GroupType: "openai", GroupKey: "id:group-1"}
	typeDefaults := map[string]GroupRateMonitorTypeDefault{
		"openai": {Enabled: true, Model: "type-model", ProbeIntervalSeconds: 45, FailureThreshold: 3},
	}
	overrideInterval := 75
	overrideFailures := 4
	overrides := map[string]GroupRateMonitorOverride{
		groupRateMonitorMapKey(group.SiteID, group.GroupKey): {Enabled: true, Model: "group-model", ProbeIntervalSeconds: &overrideInterval, FailureThreshold: &overrideFailures},
	}

	resolved := resolveGroupRateMonitorConfig(settings, typeDefaults, overrides, group)
	if !resolved.Enabled || resolved.Model != "group-model" || resolved.ProbeIntervalSeconds != 75 || resolved.FailureThreshold != 4 {
		t.Fatalf("group override must have highest priority: %+v", resolved)
	}
	delete(overrides, groupRateMonitorMapKey(group.SiteID, group.GroupKey))
	resolved = resolveGroupRateMonitorConfig(settings, typeDefaults, overrides, group)
	if resolved.Model != "type-model" || resolved.ProbeIntervalSeconds != 45 || resolved.FailureThreshold != 3 {
		t.Fatalf("type defaults must apply when a group has no override: %+v", resolved)
	}
	delete(typeDefaults, "openai")
	resolved = resolveGroupRateMonitorConfig(settings, typeDefaults, overrides, group)
	if resolved.Model != "global-model" || resolved.ProbeIntervalSeconds != 30 || resolved.FailureThreshold != 2 {
		t.Fatalf("global defaults must apply when a group has no group or type override: %+v", resolved)
	}
	settings.DefaultModel = ""
	resolved = resolveGroupRateMonitorConfig(settings, typeDefaults, overrides, group)
	if resolved.Model != "" {
		t.Fatalf("a group without any configured model must remain unconfigured: %+v", resolved)
	}
}

func TestResolveGroupRateMonitorConfigUsesIndependentGroupSwitch(t *testing.T) {
	settings := GroupRateMonitorSettings{Enabled: true, ProbeIntervalSeconds: 30, FailureThreshold: 2, DefaultModel: "global-model"}
	group := groupRateMonitorGroup{SiteID: "site-1", GroupID: "group-1", GroupName: "福利", GroupKey: "id:group-1"}
	overrides := map[string]GroupRateMonitorOverride{
		groupRateMonitorMapKey(group.SiteID, group.GroupKey): {Enabled: false},
	}
	resolved := resolveGroupRateMonitorConfig(settings, nil, overrides, group)
	if resolved.Enabled {
		t.Fatalf("disabled group override must turn off automatic probing: %+v", resolved)
	}
}

func TestResolveGroupRateMonitorConfigUsesIndependentTypeSwitch(t *testing.T) {
	settings := GroupRateMonitorSettings{Enabled: true, ProbeIntervalSeconds: 30, FailureThreshold: 2, DefaultModel: "global-model"}
	group := groupRateMonitorGroup{SiteID: "site-1", GroupID: "group-1", GroupName: "福利", GroupType: "openai", GroupKey: "id:group-1"}
	typeDefaults := map[string]GroupRateMonitorTypeDefault{
		"openai": {Enabled: false, Model: "type-model", ProbeIntervalSeconds: 45, FailureThreshold: 3},
	}
	overrides := map[string]GroupRateMonitorOverride{
		groupRateMonitorMapKey(group.SiteID, group.GroupKey): {Enabled: true},
	}
	resolved := resolveGroupRateMonitorConfig(settings, typeDefaults, overrides, group)
	if resolved.Enabled {
		t.Fatalf("disabled type must turn off automatic probing for its groups: %+v", resolved)
	}
	if resolved.Model != "type-model" || resolved.ProbeIntervalSeconds != 45 || resolved.FailureThreshold != 3 {
		t.Fatalf("disabled type must retain its defaults for manual probing: %+v", resolved)
	}
}

func TestGroupRateMonitorSettingsExcludeConnectionsForDeletedSites(t *testing.T) {
	service := &Service{sites: groupRateSiteLookup{"site-active": {ID: "site-active", Name: "可用站点"}}}
	connections := []my_sites.RealConnection{
		{UpstreamSiteID: "site-active", UpstreamGroupID: "group-1", UpstreamGroupName: "有效分组", AdminAccountID: "account-1", AdminPlatform: "sub2api", Status: my_sites.ConnectionStatusActive},
		{UpstreamSiteID: "site-deleted", UpstreamGroupID: "group-2", UpstreamGroupName: "历史分组", AdminAccountID: "account-2", AdminPlatform: "sub2api", Status: my_sites.ConnectionStatusActive},
	}

	groups := service.existingGroupRateMonitorGroups(context.Background(), connections)
	if len(groups) != 1 || groups[0].SiteID != "site-active" || groups[0].SiteName != "可用站点" {
		t.Fatalf("only connections belonging to existing sites should be configurable: %+v", groups)
	}
}

func TestGroupRateMonitorSummaryUsesLatestManualOrAutomaticCycle(t *testing.T) {
	older := time.Now().Add(-time.Minute)
	latest := time.Now()
	group := groupRateMonitorGroup{SiteID: "site-1", GroupID: "group-1", GroupName: "福利"}
	config := resolvedGroupRateMonitorConfig{Enabled: true, Model: "gpt-test", ProbeIntervalSeconds: 30}
	events := []GroupRateProbeCycle{
		{ID: "scheduled", Trigger: "scheduled", Status: groupRateProbeHealthy, CreatedAt: older},
		{ID: "manual", Trigger: "manual", Status: groupRateProbeUnhealthy, CreatedAt: latest},
	}
	summary := buildGroupRateMonitorSummary(group, config, events)
	if summary.LatestProbeAt == nil || !summary.LatestProbeAt.Equal(latest) || summary.Status != groupRateProbeUnhealthy {
		t.Fatalf("latest probe must include failed manual cycles: %+v", summary)
	}
}

func TestGroupRateMonitorChangingModelResetsFailureThreshold(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, `{"error":"failed"}`, http.StatusInternalServerError)
	}))
	defer server.Close()
	repo := newFakeRepository()
	schedulable := true
	actions := &groupRateMonitorAccountActioner{states: map[string]upstream.Sub2APIAdminAccountState{
		"account-1": {Status: "active", Schedulable: &schedulable},
	}}
	service := &Service{repo: repo, sites: fakeSiteLookup{site: &upstream.Site{ID: "site-1", BaseURL: server.URL}},
		probeRunner: NewRealProbeRunner(), dispatchStates: actions, schedulingActions: actions}
	settings := GroupRateMonitorSettings{UserID: "user-1", AdminAccountID: "ws-1", Enabled: true,
		ProbeIntervalSeconds: 30, FailureThreshold: 2, DefaultModel: "new-model"}
	group := groupRateMonitorGroup{SiteID: "site-1", GroupID: "group-1", GroupName: "福利",
		GroupKey: groupRateMonitorGroupKey("group-1", "福利"), Accounts: []my_sites.RealConnection{{
			AdminAccountID: "account-1", AdminPlatform: "sub2api", UpstreamKey: "upstream-group-key",
		}}}
	stateTargetID := "upstream-group:" + groupRateMonitorMapKey(group.SiteID, group.GroupKey)
	repo.groupRateStates[groupRateStateKey(settings.UserID, settings.AdminAccountID, group.SiteID, group.GroupKey, stateTargetID)] = GroupRateMonitorTargetState{
		UserID: settings.UserID, AdminAccountID: settings.AdminAccountID, UpstreamSiteID: group.SiteID,
		UpstreamGroupKey: group.GroupKey, TargetID: stateTargetID, Model: "old-model", ConsecutiveFailures: 1,
	}

	cycle, _, err := service.runGroupRateProbeCycle(context.Background(), settings, upstream.Session{Platform: upstream.PlatformSub2API}, group, "new-model", "scheduled")
	if err != nil {
		t.Fatalf("probe after model change: %v", err)
	}
	if cycle.Status != groupRateProbeWarning || len(actions.calls) != 0 {
		t.Fatalf("the new model must start with a fresh failure count: cycle=%+v calls=%+v", cycle, actions.calls)
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
	if err := validateGroupRateMonitorInput(GroupRateMonitorSettingsInput{Enabled: true, ProbeIntervalSeconds: 30, FailureThreshold: 2}); err != nil {
		t.Fatalf("an empty global fallback model is valid: %v", err)
	}
	if err := validateGroupRateMonitorInput(GroupRateMonitorSettingsInput{ProbeIntervalSeconds: 9, FailureThreshold: 2}); err == nil {
		t.Fatal("interval below minimum must fail")
	}
	if err := validateGroupRateMonitorInput(GroupRateMonitorSettingsInput{Enabled: true, ProbeIntervalSeconds: 30, FailureThreshold: 2, DefaultModel: "gpt-test"}); err != nil {
		t.Fatalf("valid settings: %v", err)
	}
	invalidFailures := 11
	if err := validateGroupRateMonitorInput(GroupRateMonitorSettingsInput{ProbeIntervalSeconds: 30, FailureThreshold: 2, Overrides: []GroupRateMonitorOverride{{FailureThreshold: &invalidFailures}}}); err == nil {
		t.Fatal("group failure threshold above maximum must fail")
	}
	if err := validateGroupRateMonitorInput(GroupRateMonitorSettingsInput{ProbeIntervalSeconds: 30, FailureThreshold: 2, TypeDefaults: []GroupRateMonitorTypeDefault{{GroupType: "openai", ProbeIntervalSeconds: 9, FailureThreshold: 2}}}); err == nil {
		t.Fatal("type interval below minimum must fail")
	}
	if err := validateGroupRateMonitorInput(GroupRateMonitorSettingsInput{ProbeIntervalSeconds: 30, FailureThreshold: 2, TypeDefaults: []GroupRateMonitorTypeDefault{{GroupType: "openai", ProbeIntervalSeconds: 30, FailureThreshold: 11}}}); err == nil {
		t.Fatal("type failure threshold above maximum must fail")
	}
}
