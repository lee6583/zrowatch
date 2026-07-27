package connection_health

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"transithub/backend/internal/modules/my_sites"
	"transithub/backend/internal/modules/upstream"
)

const (
	groupRateMonitorDefaultInterval = 30
	groupRateMonitorDefaultFailures = 2
	groupRateMonitorMinInterval     = 10
	groupRateMonitorMaxInterval     = 86400
	groupRateMonitorMaxFailures     = 10
	groupRateMonitorHistorySize     = 5

	groupRateProbeHealthy      = "healthy"
	groupRateProbeWarning      = "warning"
	groupRateProbeUnhealthy    = "unhealthy"
	groupRateProbeUnavailable  = "unavailable"
	groupRateProbeUnconfigured = "unconfigured"
)

type GroupRateMonitorSettings struct {
	UserID               string    `json:"-"`
	AdminAccountID       string    `json:"-"`
	Enabled              bool      `json:"enabled"`
	ProbeIntervalSeconds int       `json:"probeIntervalSeconds"`
	FailureThreshold     int       `json:"failureThreshold"`
	DefaultModel         string    `json:"defaultModel"`
	UpdatedAt            time.Time `json:"updatedAt"`
}

type GroupRateMonitorOverride struct {
	UserID            string    `json:"-"`
	AdminAccountID    string    `json:"-"`
	UpstreamSiteID    string    `json:"upstreamSiteId"`
	UpstreamGroupKey  string    `json:"-"`
	UpstreamGroupID   string    `json:"upstreamGroupId"`
	UpstreamGroupName string    `json:"upstreamGroupName"`
	Enabled           bool      `json:"enabled"`
	Model             string    `json:"model"`
	UpdatedAt         time.Time `json:"updatedAt"`
}

type GroupRateMonitorRestoreSummary struct {
	Restored int `json:"restored"`
	Pending  int `json:"pending"`
	Conflict int `json:"conflict"`
}

type GroupRateMonitorSettingsView struct {
	Enabled              bool                           `json:"enabled"`
	ProbeIntervalSeconds int                            `json:"probeIntervalSeconds"`
	FailureThreshold     int                            `json:"failureThreshold"`
	DefaultModel         string                         `json:"defaultModel"`
	Restore              GroupRateMonitorRestoreSummary `json:"restore"`
}

type GroupRateMonitorSettingsInput struct {
	Enabled              bool   `json:"enabled"`
	ProbeIntervalSeconds int    `json:"probeIntervalSeconds"`
	FailureThreshold     int    `json:"failureThreshold"`
	DefaultModel         string `json:"defaultModel"`
}

type GroupRateMonitorTargetState struct {
	UserID              string
	AdminAccountID      string
	UpstreamSiteID      string
	UpstreamGroupKey    string
	UpstreamGroupID     string
	UpstreamGroupName   string
	TargetID            string
	AccountID           string
	AccountName         string
	Model               string
	ConsecutiveFailures int
	LastResult          string
	LastLatencyMs       *int
	LastErrorKey        string
	LastErrorDetail     string
	UnavailableReason   string
	LastProbeAt         *time.Time
	UpdatedAt           time.Time
}

type GroupRateProbeTargetResult struct {
	TargetID            string `json:"targetId"`
	AccountID           string `json:"accountId"`
	AccountName         string `json:"accountName"`
	Model               string `json:"model"`
	Result              string `json:"result"`
	Healthy             bool   `json:"healthy"`
	Available           bool   `json:"available"`
	ConsecutiveFailures int    `json:"consecutiveFailures"`
	LatencyMs           *int   `json:"latencyMs"`
	ErrorKey            string `json:"errorKey,omitempty"`
	ErrorDetail         string `json:"errorDetail,omitempty"`
	UnavailableReason   string `json:"unavailableReason,omitempty"`
	RemoteAction        string `json:"remoteAction,omitempty"`
	Status              string `json:"status,omitempty"`
	Schedulable         *bool  `json:"schedulable,omitempty"`
}

type GroupRateProbeCycle struct {
	ID                string                       `json:"id"`
	UserID            string                       `json:"-"`
	AdminAccountID    string                       `json:"-"`
	UpstreamSiteID    string                       `json:"upstreamSiteId"`
	UpstreamGroupKey  string                       `json:"-"`
	UpstreamGroupID   string                       `json:"upstreamGroupId"`
	UpstreamGroupName string                       `json:"upstreamGroupName"`
	Trigger           string                       `json:"trigger"`
	Status            string                       `json:"status"`
	Model             string                       `json:"model"`
	TargetCount       int                          `json:"targetCount"`
	SuccessCount      int                          `json:"successCount"`
	Details           []GroupRateProbeTargetResult `json:"details"`
	CreatedAt         time.Time                    `json:"createdAt"`
}

type GroupRateMonitorActionState struct {
	UserID                 string
	AdminAccountID         string
	TargetID               string
	AccountID              string
	AccountName            string
	UpstreamSiteID         string
	UpstreamGroupKey       string
	OriginalStatus         string
	OriginalSchedulable    bool
	LastAppliedStatus      string
	LastAppliedSchedulable bool
	PendingStatus          string
	PendingSchedulable     *bool
	PendingRestore         bool
	Conflict               bool
	UpdatedAt              time.Time
}

type GroupRateMonitorSummary struct {
	UpstreamSiteID    string                `json:"upstreamSiteId"`
	UpstreamGroupID   string                `json:"upstreamGroupId"`
	UpstreamGroupName string                `json:"upstreamGroupName"`
	Enabled           bool                  `json:"enabled"`
	Model             string                `json:"model"`
	Status            string                `json:"status"`
	Stale             bool                  `json:"stale"`
	SuccessRate       float64               `json:"successRate"`
	LatestProbeAt     *time.Time            `json:"latestProbeAt"`
	Events            []GroupRateProbeCycle `json:"events"`
}

type GroupRateManualProbeInput struct {
	UpstreamSiteID    string `json:"upstreamSiteId"`
	UpstreamGroupID   string `json:"upstreamGroupId"`
	UpstreamGroupName string `json:"upstreamGroupName"`
}

type GroupRateManualProbeResponse struct {
	Summary          GroupRateMonitorSummary     `json:"summary"`
	DispatchAccounts []BoundDispatchAccountState `json:"dispatchAccounts"`
}

type groupRateMonitorGroup struct {
	SiteID    string
	GroupID   string
	GroupName string
	GroupKey  string
	Accounts  []my_sites.RealConnection
}

func defaultGroupRateMonitorSettings(userID, adminAccountID string) GroupRateMonitorSettings {
	return GroupRateMonitorSettings{
		UserID: userID, AdminAccountID: adminAccountID,
		ProbeIntervalSeconds: groupRateMonitorDefaultInterval,
		FailureThreshold:     groupRateMonitorDefaultFailures,
	}
}

func groupRateMonitorGroupKey(groupID, groupName string) string {
	if value := strings.TrimSpace(groupID); value != "" {
		return "id:" + value
	}
	return "name:" + strings.TrimSpace(groupName)
}

func groupRateMonitorMapKey(siteID, groupKey string) string {
	return strings.TrimSpace(siteID) + "|" + strings.TrimSpace(groupKey)
}

func groupRateMonitorRepo(repo healthRepository) (groupRateMonitorRepository, error) {
	result, ok := repo.(groupRateMonitorRepository)
	if !ok {
		return nil, errors.New("group rate monitor repository unavailable")
	}
	return result, nil
}

func (s *Service) groupRateMonitorWorkspace(ctx context.Context, userID string) (string, groupRateMonitorRepository, error) {
	adminAccountID, err := s.currentAdminAccountID(ctx, userID)
	if err != nil {
		return "", nil, err
	}
	repo, err := groupRateMonitorRepo(s.repo)
	return adminAccountID, repo, err
}

func activeSub2APIConnections(connections []my_sites.RealConnection) []my_sites.RealConnection {
	result := make([]my_sites.RealConnection, 0, len(connections))
	for _, connection := range connections {
		platform := strings.ToLower(strings.TrimSpace(connection.AdminPlatform))
		if strings.TrimSpace(connection.AdminAccountID) == "" ||
			(connection.Status != "" && connection.Status != my_sites.ConnectionStatusActive) ||
			(platform != "" && platform != string(upstream.PlatformSub2API)) {
			continue
		}
		result = append(result, connection)
	}
	return result
}

func groupRateMonitorGroups(connections []my_sites.RealConnection) []groupRateMonitorGroup {
	byKey := make(map[string]*groupRateMonitorGroup)
	order := make([]string, 0)
	for _, connection := range activeSub2APIConnections(connections) {
		groupKey := groupRateMonitorGroupKey(connection.UpstreamGroupID, connection.UpstreamGroupName)
		key := groupRateMonitorMapKey(connection.UpstreamSiteID, groupKey)
		group := byKey[key]
		if group == nil {
			group = &groupRateMonitorGroup{SiteID: connection.UpstreamSiteID, GroupID: connection.UpstreamGroupID,
				GroupName: connection.UpstreamGroupName, GroupKey: groupKey}
			byKey[key] = group
			order = append(order, key)
		}
		duplicate := false
		for _, existing := range group.Accounts {
			if existing.AdminAccountID == connection.AdminAccountID {
				duplicate = true
				break
			}
		}
		if !duplicate {
			group.Accounts = append(group.Accounts, connection)
		}
	}
	result := make([]groupRateMonitorGroup, 0, len(order))
	for _, key := range order {
		result = append(result, *byKey[key])
	}
	sort.SliceStable(result, func(i, j int) bool {
		if result[i].SiteID == result[j].SiteID {
			return result[i].GroupName < result[j].GroupName
		}
		return result[i].SiteID < result[j].SiteID
	})
	return result
}

func (s *Service) GroupRateMonitorSettings(ctx context.Context, userID string) (GroupRateMonitorSettingsView, error) {
	adminAccountID, repo, err := s.groupRateMonitorWorkspace(ctx, userID)
	if err != nil {
		return GroupRateMonitorSettingsView{}, err
	}
	settings, err := repo.GetGroupRateMonitorSettings(ctx, userID, adminAccountID)
	if err != nil {
		return GroupRateMonitorSettingsView{}, err
	}
	return GroupRateMonitorSettingsView{
		Enabled: settings.Enabled, ProbeIntervalSeconds: settings.ProbeIntervalSeconds,
		FailureThreshold: settings.FailureThreshold, DefaultModel: settings.DefaultModel,
	}, nil
}

func validateGroupRateMonitorInput(input GroupRateMonitorSettingsInput) error {
	if input.ProbeIntervalSeconds < groupRateMonitorMinInterval || input.ProbeIntervalSeconds > groupRateMonitorMaxInterval {
		return requestError(ErrorRequest)
	}
	if input.FailureThreshold < 1 || input.FailureThreshold > groupRateMonitorMaxFailures {
		return requestError(ErrorRequest)
	}
	if input.Enabled && strings.TrimSpace(input.DefaultModel) == "" {
		return requestError(ErrorGroupRateMonitorModelRequired)
	}
	return nil
}

func (s *Service) SaveGroupRateMonitorSettings(ctx context.Context, userID string, input GroupRateMonitorSettingsInput) (GroupRateMonitorSettingsView, error) {
	if err := validateGroupRateMonitorInput(input); err != nil {
		return GroupRateMonitorSettingsView{}, err
	}
	adminAccountID, repo, err := s.groupRateMonitorWorkspace(ctx, userID)
	if err != nil {
		return GroupRateMonitorSettingsView{}, err
	}
	connections, err := s.mySites.ListRealConnectionsForWorkspace(ctx, userID, adminAccountID)
	if err != nil {
		return GroupRateMonitorSettingsView{}, err
	}
	settings := GroupRateMonitorSettings{UserID: userID, AdminAccountID: adminAccountID, Enabled: input.Enabled,
		ProbeIntervalSeconds: input.ProbeIntervalSeconds, FailureThreshold: input.FailureThreshold,
		DefaultModel: strings.TrimSpace(input.DefaultModel)}
	if err := repo.SaveGroupRateMonitorSettings(ctx, settings, nil); err != nil {
		return GroupRateMonitorSettingsView{}, err
	}
	restore := s.restoreUnmanagedGroupRateMonitorActions(ctx, settings, connections)
	view, err := s.GroupRateMonitorSettings(ctx, userID)
	if err != nil {
		return GroupRateMonitorSettingsView{}, err
	}
	view.Restore = restore
	return view, nil
}

func (s *Service) GroupRateMonitorSummaries(ctx context.Context, userID string) ([]GroupRateMonitorSummary, error) {
	adminAccountID, repo, err := s.groupRateMonitorWorkspace(ctx, userID)
	if err != nil {
		return nil, err
	}
	settings, err := repo.GetGroupRateMonitorSettings(ctx, userID, adminAccountID)
	if err != nil {
		return nil, err
	}
	connections, err := s.mySites.ListRealConnectionsForWorkspace(ctx, userID, adminAccountID)
	if err != nil {
		return nil, err
	}
	cycles, err := repo.ListGroupRateMonitorCycles(ctx, userID, adminAccountID, groupRateMonitorHistorySize)
	if err != nil {
		return nil, err
	}
	cyclesByGroup := make(map[string][]GroupRateProbeCycle)
	for _, cycle := range cycles {
		key := groupRateMonitorMapKey(cycle.UpstreamSiteID, cycle.UpstreamGroupKey)
		cyclesByGroup[key] = append(cyclesByGroup[key], cycle)
	}
	result := make([]GroupRateMonitorSummary, 0)
	for _, group := range groupRateMonitorGroups(connections) {
		enabled, model := settings.Enabled, strings.TrimSpace(settings.DefaultModel)
		events := cyclesByGroup[groupRateMonitorMapKey(group.SiteID, group.GroupKey)]
		result = append(result, buildGroupRateMonitorSummary(settings, group, enabled, model, events))
	}
	return result, nil
}

func buildGroupRateMonitorSummary(settings GroupRateMonitorSettings, group groupRateMonitorGroup, enabled bool, model string, events []GroupRateProbeCycle) GroupRateMonitorSummary {
	summary := GroupRateMonitorSummary{UpstreamSiteID: group.SiteID, UpstreamGroupID: group.GroupID,
		UpstreamGroupName: group.GroupName, Enabled: enabled, Model: model, Events: events, Status: groupRateProbeUnconfigured}
	if summary.Events == nil {
		summary.Events = []GroupRateProbeCycle{}
	}
	if strings.TrimSpace(model) == "" {
		return summary
	}
	if len(events) == 0 {
		if enabled {
			summary.Status = groupRateProbeUnavailable
		}
		return summary
	}
	latest := events[len(events)-1]
	summary.Status = latest.Status
	summary.LatestProbeAt = &latest.CreatedAt
	successes := 0
	for _, event := range events {
		if event.Status == groupRateProbeHealthy {
			successes++
		}
	}
	summary.SuccessRate = float64(successes) * 100 / float64(len(events))
	interval := time.Duration(settings.ProbeIntervalSeconds) * time.Second
	if interval <= 0 {
		interval = groupRateMonitorDefaultInterval * time.Second
	}
	summary.Stale = time.Since(latest.CreatedAt) > 2*interval
	return summary
}

func (s *Service) ProbeGroupRateMonitor(ctx context.Context, userID string, input GroupRateManualProbeInput) (GroupRateManualProbeResponse, error) {
	adminAccountID, repo, err := s.groupRateMonitorWorkspace(ctx, userID)
	if err != nil {
		return GroupRateManualProbeResponse{}, err
	}
	settings, err := repo.GetGroupRateMonitorSettings(ctx, userID, adminAccountID)
	if err != nil {
		return GroupRateManualProbeResponse{}, err
	}
	connections, err := s.mySites.ListRealConnectionsForWorkspace(ctx, userID, adminAccountID)
	if err != nil {
		return GroupRateManualProbeResponse{}, err
	}
	requestedKey := groupRateMonitorGroupKey(input.UpstreamGroupID, input.UpstreamGroupName)
	var selected *groupRateMonitorGroup
	for _, group := range groupRateMonitorGroups(connections) {
		if group.SiteID == input.UpstreamSiteID && group.GroupKey == requestedKey {
			copy := group
			selected = &copy
			break
		}
	}
	if selected == nil {
		return GroupRateManualProbeResponse{}, requestError(ErrorNotFound)
	}
	model := strings.TrimSpace(settings.DefaultModel)
	if model == "" {
		return GroupRateManualProbeResponse{}, requestError(ErrorGroupRateMonitorDisabled)
	}
	session, err := s.mySites.RequireSession(ctx, userID, adminAccountID)
	if err != nil {
		return GroupRateManualProbeResponse{}, err
	}
	cycle, dispatch, err := s.runGroupRateProbeCycle(ctx, settings, session, *selected, model, "manual")
	if err != nil {
		return GroupRateManualProbeResponse{}, err
	}
	events, err := repo.ListGroupRateMonitorCycles(ctx, userID, adminAccountID, groupRateMonitorHistorySize)
	if err != nil {
		return GroupRateManualProbeResponse{}, err
	}
	groupEvents := make([]GroupRateProbeCycle, 0)
	for _, event := range events {
		if event.UpstreamSiteID == selected.SiteID && event.UpstreamGroupKey == selected.GroupKey {
			groupEvents = append(groupEvents, event)
		}
	}
	summary := buildGroupRateMonitorSummary(settings, *selected, settings.Enabled, model, groupEvents)
	if len(groupEvents) == 0 {
		summary.Events = []GroupRateProbeCycle{cycle}
	}
	return GroupRateManualProbeResponse{Summary: summary, DispatchAccounts: dispatch}, nil
}

func (s *Service) runGroupRateProbeCycle(ctx context.Context, settings GroupRateMonitorSettings, session upstream.Session, group groupRateMonitorGroup, model, trigger string) (GroupRateProbeCycle, []BoundDispatchAccountState, error) {
	repo, err := groupRateMonitorRepo(s.repo)
	if err != nil {
		return GroupRateProbeCycle{}, nil, err
	}
	leaseID := "group-rate:" + settings.AdminAccountID + ":" + groupRateMonitorMapKey(group.SiteID, group.GroupKey)
	release, err := s.repo.AcquireTargetLease(ctx, leaseID)
	if err != nil {
		return GroupRateProbeCycle{}, nil, err
	}
	defer release()

	cycle := GroupRateProbeCycle{UserID: settings.UserID, AdminAccountID: settings.AdminAccountID,
		UpstreamSiteID: group.SiteID, UpstreamGroupKey: group.GroupKey, UpstreamGroupID: group.GroupID,
		UpstreamGroupName: group.GroupName, Trigger: trigger, Model: model, TargetCount: 1,
		Details: make([]GroupRateProbeTargetResult, 0, 1), CreatedAt: time.Now()}
	cycle.ID, err = newID()
	if err != nil {
		return GroupRateProbeCycle{}, nil, err
	}

	stateTargetID := "upstream-group:" + groupRateMonitorMapKey(group.SiteID, group.GroupKey)
	state, err := repo.GetGroupRateMonitorState(ctx, settings.UserID, settings.AdminAccountID, group.SiteID, group.GroupKey, stateTargetID)
	if err != nil {
		return GroupRateProbeCycle{}, nil, err
	}
	if state == nil {
		state = &GroupRateMonitorTargetState{UserID: settings.UserID, AdminAccountID: settings.AdminAccountID,
			UpstreamSiteID: group.SiteID, UpstreamGroupKey: group.GroupKey, UpstreamGroupID: group.GroupID,
			UpstreamGroupName: group.GroupName, TargetID: stateTargetID, AccountName: group.GroupName}
	}

	detail := GroupRateProbeTargetResult{TargetID: stateTargetID, AccountName: group.GroupName, Model: model}
	request, unavailableReason := s.groupRateUpstreamProbeRequest(ctx, group, model)
	now := time.Now()
	state.Model = model
	state.LastProbeAt = &now
	if unavailableReason != "" {
		state.LastResult = groupRateProbeUnavailable
		state.LastLatencyMs = nil
		state.LastErrorKey = ""
		state.LastErrorDetail = ""
		state.UnavailableReason = unavailableReason
		detail.UnavailableReason = unavailableReason
		cycle.Status = groupRateProbeUnavailable
	} else {
		outcome := s.probeRunner.Probe(ctx, request)
		state.LastResult = string(outcome.Result)
		state.LastLatencyMs = intPtr(outcome.LatencyMs)
		state.UnavailableReason = ""
		detail.Result = string(outcome.Result)
		detail.LatencyMs = intPtr(outcome.LatencyMs)
		detail.Available = true
		if outcome.Result == ResultOK {
			state.ConsecutiveFailures = 0
			state.LastErrorKey = ""
			state.LastErrorDetail = ""
			detail.Healthy = true
			cycle.SuccessCount = 1
		} else {
			state.ConsecutiveFailures++
			state.LastErrorKey = string(outcome.Result)
			state.LastErrorDetail = outcome.Detail
			detail.ErrorKey = state.LastErrorKey
			detail.ErrorDetail = state.LastErrorDetail
		}
		detail.ConsecutiveFailures = state.ConsecutiveFailures
		cycle.Status = groupRateProbeStatus(detail, settings.FailureThreshold)
	}
	if err := repo.UpsertGroupRateMonitorState(ctx, *state); err != nil {
		return GroupRateProbeCycle{}, nil, err
	}
	cycle.Details = append(cycle.Details, detail)

	dispatch := s.reconcileGroupRateMonitorAccounts(ctx, repo, settings, session, group, detail, settings.Enabled, trigger == "manual")
	if err := repo.InsertGroupRateMonitorCycle(ctx, cycle); err != nil {
		return GroupRateProbeCycle{}, nil, err
	}
	return cycle, dispatch, nil
}

func (s *Service) groupRateUpstreamProbeRequest(ctx context.Context, group groupRateMonitorGroup, model string) (ProbeRequest, string) {
	if s.sites == nil {
		return ProbeRequest{}, upstream.ReasonBaseURLUnavailable
	}
	site, err := s.sites.GetSite(ctx, group.SiteID)
	if err != nil || site == nil || strings.TrimSpace(site.BaseURL) == "" {
		return ProbeRequest{}, upstream.ReasonBaseURLUnavailable
	}
	key := ""
	for _, connection := range group.Accounts {
		if strings.TrimSpace(connection.UpstreamKey) != "" {
			key = connection.UpstreamKey
			break
		}
	}
	if strings.TrimSpace(key) == "" {
		return ProbeRequest{}, upstream.ReasonCredentialUnavailable
	}
	return ProbeRequest{BaseURL: site.BaseURL, UpstreamKey: key, ModelName: model, MaxTokens: 1}, ""
}

func groupRateProbeStatus(detail GroupRateProbeTargetResult, failureThreshold int) string {
	if !detail.Available {
		return groupRateProbeUnavailable
	}
	if detail.Healthy {
		return groupRateProbeHealthy
	}
	if detail.ConsecutiveFailures >= failureThreshold {
		return groupRateProbeUnhealthy
	}
	return groupRateProbeWarning
}

func (s *Service) reconcileGroupRateMonitorAccounts(ctx context.Context, repo groupRateMonitorRepository, settings GroupRateMonitorSettings, session upstream.Session, group groupRateMonitorGroup, probe GroupRateProbeTargetResult, applyActions, reclaimManualConflict bool) []BoundDispatchAccountState {
	result := make([]BoundDispatchAccountState, 0, len(group.Accounts))
	seen := make(map[string]struct{}, len(group.Accounts))
	for _, connection := range group.Accounts {
		accountID := strings.TrimSpace(connection.AdminAccountID)
		if accountID == "" {
			continue
		}
		if _, exists := seen[accountID]; exists {
			continue
		}
		seen[accountID] = struct{}{}
		targetID := buildTargetID(string(upstream.PlatformSub2API), settings.AdminAccountID, accountID)
		item := BoundDispatchAccountState{ID: accountID, Name: connection.AdminAccountName, TargetID: targetID}
		if s.dispatchStates == nil || s.schedulingActions == nil {
			item.UnavailableReason = "unavailable"
			result = append(result, item)
			continue
		}
		release, err := s.repo.AcquireTargetLease(ctx, targetID)
		if err != nil {
			item.UnavailableReason = "target_busy"
			result = append(result, item)
			continue
		}
		remote, remoteErr := s.dispatchStates.GetSub2APIAdminAccountState(session, accountID)
		if remoteErr != nil || remote.Schedulable == nil || strings.TrimSpace(remote.Status) == "" {
			item.UnavailableReason = "not_found"
			release()
			result = append(result, item)
			continue
		}
		item.Name = firstNonEmpty(remote.Name, connection.AdminAccountName)
		item.Status = remote.Status
		item.Schedulable = remote.Schedulable
		if probe.Available && applyActions {
			detail := probe
			detail.TargetID = targetID
			detail.AccountID = accountID
			detail.AccountName = item.Name
			detail.Status = remote.Status
			detail.Schedulable = remote.Schedulable
			item.ActionResult, item.Status, item.Schedulable = s.reconcileGroupRateMonitorAccount(ctx, repo, settings, session, group, detail, remote, reclaimManualConflict)
			if item.ActionResult != "" {
				log.Printf("[group-rate-monitor] account reconcile workspace=%s site=%s group=%s account=%s result=%s", settings.AdminAccountID, group.SiteID, group.GroupName, accountID, item.ActionResult)
			}
		}
		item.Available = true
		release()
		result = append(result, item)
	}
	return result
}

func (s *Service) reconcileGroupRateMonitorAccount(ctx context.Context, repo groupRateMonitorRepository, settings GroupRateMonitorSettings, session upstream.Session, group groupRateMonitorGroup, detail GroupRateProbeTargetResult, remote upstream.Sub2APIAdminAccountState, reclaimManualConflict bool) (string, string, *bool) {
	currentStatus := normalizeTargetStatus(string(upstream.PlatformSub2API), remote.Status)
	currentSchedulable := remote.Schedulable
	if currentSchedulable == nil {
		return "unavailable", remote.Status, remote.Schedulable
	}
	action, err := repo.GetGroupRateMonitorAction(ctx, settings.UserID, settings.AdminAccountID, detail.TargetID)
	if err != nil {
		return "action_state_failed", remote.Status, remote.Schedulable
	}
	if detail.Healthy {
		if action == nil {
			if !reclaimManualConflict || (targetStatusEnabled(string(upstream.PlatformSub2API), currentStatus) && *currentSchedulable) {
				return "", remote.Status, remote.Schedulable
			}
			managed, managedErr := s.repo.GetTargetActionState(ctx, settings.UserID, settings.AdminAccountID, detail.TargetID)
			if managedErr != nil {
				return "action_state_failed", remote.Status, remote.Schedulable
			}
			if managed != nil {
				return "managed_elsewhere", remote.Status, remote.Schedulable
			}
			if s.balancePauses != nil {
				paused, pauseErr := s.balancePauses.IsAccountBalancePausedForWorkspace(ctx, settings.UserID, settings.AdminAccountID, detail.AccountID)
				if pauseErr != nil || paused {
					return RemoteActionSkippedBalanceSuspended, remote.Status, remote.Schedulable
				}
			}
			desiredStatus := "active"
			desiredSchedulable := true
			if err := s.schedulingActions.UpdateSub2APIAdminAccountState(session, detail.AccountID, &desiredStatus, &desiredSchedulable); err != nil {
				return "restore_failed", remote.Status, remote.Schedulable
			}
			return "restored", desiredStatus, boolPointer(true)
		}
		if action.Conflict {
			_ = repo.DeleteGroupRateMonitorAction(ctx, settings.UserID, settings.AdminAccountID, detail.TargetID)
			return "manual_conflict_cleared", remote.Status, remote.Schedulable
		}
		if s.balancePauses != nil {
			paused, pauseErr := s.balancePauses.IsAccountBalancePausedForWorkspace(ctx, settings.UserID, settings.AdminAccountID, detail.AccountID)
			if pauseErr != nil || paused {
				action.PendingRestore = true
				_ = repo.UpsertGroupRateMonitorAction(ctx, *action)
				return RemoteActionSkippedBalanceSuspended, remote.Status, remote.Schedulable
			}
		}
		if groupRateMonitorActionConflicted(*action, currentStatus, *currentSchedulable) {
			action.Conflict = true
			action.PendingStatus = ""
			action.PendingSchedulable = nil
			_ = repo.UpsertGroupRateMonitorAction(ctx, *action)
			return "manual_conflict", remote.Status, remote.Schedulable
		}
		desiredStatus := action.OriginalStatus
		desiredSchedulable := action.OriginalSchedulable
		action.PendingStatus = desiredStatus
		action.PendingSchedulable = boolPointer(desiredSchedulable)
		action.PendingRestore = true
		if err := repo.UpsertGroupRateMonitorAction(ctx, *action); err != nil {
			return "restore_state_failed", remote.Status, remote.Schedulable
		}
		if err := s.schedulingActions.UpdateSub2APIAdminAccountState(session, detail.AccountID, &desiredStatus, &desiredSchedulable); err != nil {
			return "restore_failed", remote.Status, remote.Schedulable
		}
		_ = repo.DeleteGroupRateMonitorAction(ctx, settings.UserID, settings.AdminAccountID, detail.TargetID)
		return "restored", desiredStatus, boolPointer(desiredSchedulable)
	}
	if detail.ConsecutiveFailures < settings.FailureThreshold {
		return "", remote.Status, remote.Schedulable
	}
	if action == nil {
		managed, err := s.repo.GetTargetActionState(ctx, settings.UserID, settings.AdminAccountID, detail.TargetID)
		if err != nil {
			return "action_state_failed", remote.Status, remote.Schedulable
		}
		if managed != nil {
			return "managed_elsewhere", remote.Status, remote.Schedulable
		}
		if !targetStatusEnabled(string(upstream.PlatformSub2API), currentStatus) || !*currentSchedulable {
			return s.normalizeDisabledGroupRateMonitorAccount(session, detail.AccountID, currentStatus, *currentSchedulable)
		}
		action = &GroupRateMonitorActionState{UserID: settings.UserID, AdminAccountID: settings.AdminAccountID,
			TargetID: detail.TargetID, AccountID: detail.AccountID, AccountName: detail.AccountName,
			UpstreamSiteID: group.SiteID, UpstreamGroupKey: group.GroupKey, OriginalStatus: currentStatus,
			OriginalSchedulable: *currentSchedulable, LastAppliedStatus: currentStatus,
			LastAppliedSchedulable: *currentSchedulable}
	} else {
		managed, managedErr := s.repo.GetTargetActionState(ctx, settings.UserID, settings.AdminAccountID, detail.TargetID)
		if managedErr != nil {
			return "action_state_failed", remote.Status, remote.Schedulable
		}
		if managed != nil {
			return "managed_elsewhere", remote.Status, remote.Schedulable
		}
		conflicted := action.Conflict || groupRateMonitorActionConflicted(*action, currentStatus, *currentSchedulable)
		if conflicted && !reclaimManualConflict {
			action.Conflict = true
			action.PendingStatus = ""
			action.PendingSchedulable = nil
			_ = repo.UpsertGroupRateMonitorAction(ctx, *action)
			return "manual_conflict", remote.Status, remote.Schedulable
		}
		if conflicted {
			if !targetStatusEnabled(string(upstream.PlatformSub2API), currentStatus) || !*currentSchedulable {
				// A previous account update may have changed status before the
				// dedicated schedulable update completed. Keep ownership of the
				// original active state so a later successful probe can restore it.
				action.LastAppliedStatus = currentStatus
				action.LastAppliedSchedulable = *currentSchedulable
				action.PendingStatus = ""
				action.PendingSchedulable = nil
				action.PendingRestore = false
				action.Conflict = false
			} else {
				action.OriginalStatus = currentStatus
				action.OriginalSchedulable = *currentSchedulable
				action.LastAppliedStatus = currentStatus
				action.LastAppliedSchedulable = *currentSchedulable
				action.PendingStatus = ""
				action.PendingSchedulable = nil
				action.PendingRestore = false
				action.Conflict = false
			}
		}
	}
	desiredStatus := "inactive"
	desiredSchedulable := false
	if currentStatus == desiredStatus && !*currentSchedulable {
		action.LastAppliedStatus = desiredStatus
		action.LastAppliedSchedulable = false
		action.PendingStatus = ""
		action.PendingSchedulable = nil
		_ = repo.UpsertGroupRateMonitorAction(ctx, *action)
		return "disabled", desiredStatus, boolPointer(false)
	}
	action.PendingStatus = desiredStatus
	action.PendingSchedulable = boolPointer(false)
	if err := repo.UpsertGroupRateMonitorAction(ctx, *action); err != nil {
		return "disable_state_failed", remote.Status, remote.Schedulable
	}
	if err := s.schedulingActions.UpdateSub2APIAdminAccountState(session, detail.AccountID, &desiredStatus, &desiredSchedulable); err != nil {
		return "disable_failed", remote.Status, remote.Schedulable
	}
	action.LastAppliedStatus = desiredStatus
	action.LastAppliedSchedulable = false
	action.PendingStatus = ""
	action.PendingSchedulable = nil
	action.PendingRestore = false
	_ = repo.UpsertGroupRateMonitorAction(ctx, *action)
	return "disabled", desiredStatus, boolPointer(false)
}

func (s *Service) normalizeDisabledGroupRateMonitorAccount(session upstream.Session, accountID, currentStatus string, currentSchedulable bool) (string, string, *bool) {
	desiredStatus := "inactive"
	desiredSchedulable := false
	if currentStatus == desiredStatus && !currentSchedulable {
		return RemoteActionSkippedTargetInitiallyDisabled, desiredStatus, boolPointer(false)
	}
	if err := s.schedulingActions.UpdateSub2APIAdminAccountState(session, accountID, &desiredStatus, &desiredSchedulable); err != nil {
		return "disable_failed", currentStatus, boolPointer(currentSchedulable)
	}
	return "disabled", desiredStatus, boolPointer(false)
}

func groupRateMonitorActionConflicted(action GroupRateMonitorActionState, currentStatus string, currentSchedulable bool) bool {
	if action.PendingStatus != "" && action.PendingSchedulable != nil &&
		currentStatus == action.PendingStatus && currentSchedulable == *action.PendingSchedulable {
		return false
	}
	return currentStatus != action.LastAppliedStatus || currentSchedulable != action.LastAppliedSchedulable
}

func boolPointer(value bool) *bool { return &value }

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func (s *Service) restoreUnmanagedGroupRateMonitorActions(ctx context.Context, settings GroupRateMonitorSettings, connections []my_sites.RealConnection) GroupRateMonitorRestoreSummary {
	repo, err := groupRateMonitorRepo(s.repo)
	if err != nil {
		return GroupRateMonitorRestoreSummary{Pending: 1}
	}
	managedGroups := make(map[string]struct{})
	if settings.Enabled && strings.TrimSpace(settings.DefaultModel) != "" {
		for _, group := range groupRateMonitorGroups(connections) {
			managedGroups[groupRateMonitorMapKey(group.SiteID, group.GroupKey)] = struct{}{}
		}
	}
	actions, err := repo.ListGroupRateMonitorActions(ctx, settings.UserID, settings.AdminAccountID)
	if err != nil {
		return GroupRateMonitorRestoreSummary{Pending: 1}
	}
	result := GroupRateMonitorRestoreSummary{}
	for _, action := range actions {
		if _, managed := managedGroups[groupRateMonitorMapKey(action.UpstreamSiteID, action.UpstreamGroupKey)]; managed {
			continue
		}
		status := s.restoreGroupRateMonitorAction(ctx, repo, action)
		switch status {
		case "restored", "conflict_released":
			result.Restored++
		case "manual_conflict":
			result.Conflict++
		default:
			result.Pending++
		}
	}
	return result
}

func (s *Service) restoreGroupRateMonitorAction(ctx context.Context, repo groupRateMonitorRepository, action GroupRateMonitorActionState) string {
	if action.Conflict {
		_ = repo.DeleteGroupRateMonitorAction(ctx, action.UserID, action.AdminAccountID, action.TargetID)
		return "conflict_released"
	}
	if s.balancePauses != nil {
		paused, err := s.balancePauses.IsAccountBalancePausedForWorkspace(ctx, action.UserID, action.AdminAccountID, action.AccountID)
		if err != nil || paused {
			action.PendingRestore = true
			_ = repo.UpsertGroupRateMonitorAction(ctx, action)
			return RemoteActionSkippedBalanceSuspended
		}
	}
	session, err := s.mySites.RequireSession(ctx, action.UserID, action.AdminAccountID)
	if err != nil {
		action.PendingRestore = true
		_ = repo.UpsertGroupRateMonitorAction(ctx, action)
		return "restore_failed"
	}
	release, err := s.repo.AcquireTargetLease(ctx, action.TargetID)
	if err != nil {
		return "restore_failed"
	}
	defer release()
	remote, err := s.dispatchStates.GetSub2APIAdminAccountState(session, action.AccountID)
	if err != nil || remote.Schedulable == nil {
		action.PendingRestore = true
		_ = repo.UpsertGroupRateMonitorAction(ctx, action)
		return "restore_failed"
	}
	currentStatus := normalizeTargetStatus(string(upstream.PlatformSub2API), remote.Status)
	if groupRateMonitorActionConflicted(action, currentStatus, *remote.Schedulable) {
		action.Conflict = true
		action.PendingRestore = false
		_ = repo.UpsertGroupRateMonitorAction(ctx, action)
		return "manual_conflict"
	}
	action.PendingStatus = action.OriginalStatus
	action.PendingSchedulable = boolPointer(action.OriginalSchedulable)
	action.PendingRestore = true
	if err := repo.UpsertGroupRateMonitorAction(ctx, action); err != nil {
		return "restore_failed"
	}
	if err := s.schedulingActions.UpdateSub2APIAdminAccountState(session, action.AccountID, &action.OriginalStatus, &action.OriginalSchedulable); err != nil {
		return "restore_failed"
	}
	_ = repo.DeleteGroupRateMonitorAction(ctx, action.UserID, action.AdminAccountID, action.TargetID)
	return "restored"
}

func (s *Service) startGroupRateMonitorScheduler(ctx context.Context) {
	if _, err := groupRateMonitorRepo(s.repo); err != nil {
		return
	}
	go func() {
		s.runGroupRateMonitorTickSafely(ctx)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.runGroupRateMonitorTickSafely(ctx)
			}
		}
	}()
}

func (s *Service) runGroupRateMonitorTickSafely(ctx context.Context) {
	defer func() {
		if recovered := recover(); recovered != nil {
			log.Printf("[group-rate-monitor] scheduler panic recovered: %v", recovered)
		}
	}()
	repo, err := groupRateMonitorRepo(s.repo)
	if err != nil {
		return
	}
	settingsList, err := repo.ListEnabledGroupRateMonitorSettings(ctx)
	if err != nil {
		log.Printf("[group-rate-monitor] list settings failed: %v", err)
		return
	}
	var wait sync.WaitGroup
	limit := make(chan struct{}, globalProbeConcurrency)
	for _, settings := range settingsList {
		connections, err := s.mySites.ListRealConnectionsForWorkspace(ctx, settings.UserID, settings.AdminAccountID)
		if err != nil {
			continue
		}
		session, err := s.mySites.RequireSession(ctx, settings.UserID, settings.AdminAccountID)
		if err != nil || session.Platform != upstream.PlatformSub2API {
			continue
		}
		latest, err := repo.ListLatestGroupRateMonitorCycles(ctx, settings.UserID, settings.AdminAccountID)
		if err != nil {
			continue
		}
		now := time.Now()
		for _, group := range groupRateMonitorGroups(connections) {
			enabled, model := settings.Enabled, strings.TrimSpace(settings.DefaultModel)
			if !enabled || strings.TrimSpace(model) == "" {
				continue
			}
			last := latest[groupRateMonitorMapKey(group.SiteID, group.GroupKey)]
			if !last.IsZero() && now.Sub(last) < time.Duration(settings.ProbeIntervalSeconds)*time.Second {
				continue
			}
			settingsCopy, sessionCopy, groupCopy, modelCopy := settings, session, group, model
			wait.Add(1)
			go func() {
				defer wait.Done()
				limit <- struct{}{}
				defer func() { <-limit }()
				// Another process may have completed this group while this job waited for capacity.
				latestNow, listErr := repo.ListLatestGroupRateMonitorCycles(ctx, settingsCopy.UserID, settingsCopy.AdminAccountID)
				if listErr == nil {
					lastRun := latestNow[groupRateMonitorMapKey(groupCopy.SiteID, groupCopy.GroupKey)]
					if !lastRun.IsZero() && time.Since(lastRun) < time.Duration(settingsCopy.ProbeIntervalSeconds)*time.Second {
						return
					}
				}
				if _, _, probeErr := s.runGroupRateProbeCycle(ctx, settingsCopy, sessionCopy, groupCopy, modelCopy, "scheduled"); probeErr != nil {
					log.Printf("[group-rate-monitor] probe failed workspace=%s site=%s group=%s err=%v", settingsCopy.AdminAccountID, groupCopy.SiteID, groupCopy.GroupName, probeErr)
				}
			}()
		}
		_ = s.restoreUnmanagedGroupRateMonitorActions(ctx, settings, connections)
	}
	wait.Wait()
	pending, err := repo.ListPendingGroupRateMonitorActions(ctx)
	if err == nil {
		for _, action := range pending {
			_ = s.restoreGroupRateMonitorAction(ctx, repo, action)
		}
	}
}

func (s *Service) markGroupRateMonitorManualConflict(ctx context.Context, userID, adminAccountID, targetID string) {
	repo, err := groupRateMonitorRepo(s.repo)
	if err != nil {
		return
	}
	if err := repo.MarkGroupRateMonitorActionConflict(ctx, userID, adminAccountID, targetID); err != nil {
		log.Printf("[group-rate-monitor] mark manual conflict failed target_id=%s err=%v", targetID, err)
	}
}

func (s *Service) debugGroupRateMonitorIdentity(group groupRateMonitorGroup) string {
	return fmt.Sprintf("%s/%s", group.SiteID, group.GroupName)
}
