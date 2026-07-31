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
	UserID                string    `json:"-"`
	AdminAccountID        string    `json:"-"`
	Enabled               bool      `json:"enabled"`
	CostGuardEnabled      bool      `json:"costGuardEnabled"`
	ProfitPriorityEnabled bool      `json:"profitPriorityEnabled"`
	ProbeIntervalSeconds  int       `json:"probeIntervalSeconds"`
	FailureThreshold      int       `json:"failureThreshold"`
	DefaultModel          string    `json:"defaultModel"`
	UpdatedAt             time.Time `json:"updatedAt"`
}

type GroupRateMonitorOverride struct {
	UserID               string    `json:"-"`
	AdminAccountID       string    `json:"-"`
	UpstreamSiteID       string    `json:"upstreamSiteId"`
	UpstreamGroupKey     string    `json:"-"`
	UpstreamGroupID      string    `json:"upstreamGroupId"`
	UpstreamGroupName    string    `json:"upstreamGroupName"`
	Enabled              bool      `json:"enabled"`
	Model                string    `json:"model"`
	ProbeIntervalSeconds *int      `json:"probeIntervalSeconds"`
	FailureThreshold     *int      `json:"failureThreshold"`
	UpdatedAt            time.Time `json:"updatedAt"`
}

type GroupRateMonitorTypeDefault struct {
	UserID               string    `json:"-"`
	AdminAccountID       string    `json:"-"`
	GroupType            string    `json:"groupType"`
	Enabled              bool      `json:"enabled"`
	ProbeIntervalSeconds int       `json:"probeIntervalSeconds"`
	FailureThreshold     int       `json:"failureThreshold"`
	Model                string    `json:"model"`
	UpdatedAt            time.Time `json:"updatedAt"`
}

type GroupRateMonitorGroupConfig struct {
	UpstreamSiteID               string `json:"upstreamSiteId"`
	UpstreamSiteName             string `json:"upstreamSiteName"`
	UpstreamGroupID              string `json:"upstreamGroupId"`
	UpstreamGroupName            string `json:"upstreamGroupName"`
	GroupType                    string `json:"groupType"`
	Enabled                      bool   `json:"enabled"`
	Model                        string `json:"model"`
	ProbeIntervalSeconds         *int   `json:"probeIntervalSeconds"`
	FailureThreshold             *int   `json:"failureThreshold"`
	ResolvedModel                string `json:"resolvedModel"`
	ResolvedProbeIntervalSeconds int    `json:"resolvedProbeIntervalSeconds"`
	ResolvedFailureThreshold     int    `json:"resolvedFailureThreshold"`
}

type GroupRateMonitorRestoreSummary struct {
	Restored int `json:"restored"`
	Pending  int `json:"pending"`
	Conflict int `json:"conflict"`
}

type GroupRateMonitorSettingsView struct {
	Enabled               bool                           `json:"enabled"`
	CostGuardEnabled      bool                           `json:"costGuardEnabled"`
	ProfitPriorityEnabled bool                           `json:"profitPriorityEnabled"`
	ProbeIntervalSeconds  int                            `json:"probeIntervalSeconds"`
	FailureThreshold      int                            `json:"failureThreshold"`
	DefaultModel          string                         `json:"defaultModel"`
	TypeDefaults          []GroupRateMonitorTypeDefault  `json:"typeDefaults"`
	Groups                []GroupRateMonitorGroupConfig  `json:"groups"`
	Restore               GroupRateMonitorRestoreSummary `json:"restore"`
}

type GroupRateMonitorSettingsInput struct {
	Enabled               bool                          `json:"enabled"`
	CostGuardEnabled      bool                          `json:"costGuardEnabled"`
	ProfitPriorityEnabled bool                          `json:"profitPriorityEnabled"`
	ProbeIntervalSeconds  int                           `json:"probeIntervalSeconds"`
	FailureThreshold      int                           `json:"failureThreshold"`
	DefaultModel          string                        `json:"defaultModel"`
	TypeDefaults          []GroupRateMonitorTypeDefault `json:"typeDefaults"`
	Overrides             []GroupRateMonitorOverride    `json:"overrides"`
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

type GroupRateMonitorCostGuardInput struct {
	Enabled bool `json:"enabled"`
}

type GroupRateMonitorProfitPriorityInput struct {
	Enabled bool `json:"enabled"`
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
	SiteName  string
	GroupID   string
	GroupName string
	GroupType string
	GroupKey  string
	Accounts  []my_sites.RealConnection
}

type resolvedGroupRateMonitorConfig struct {
	Enabled              bool
	Model                string
	ProbeIntervalSeconds int
	FailureThreshold     int
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
				GroupName: connection.UpstreamGroupName, GroupType: strings.TrimSpace(connection.GroupType), GroupKey: groupKey}
			byKey[key] = group
			order = append(order, key)
		} else if group.GroupType == "" {
			group.GroupType = strings.TrimSpace(connection.GroupType)
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

func (s *Service) existingGroupRateMonitorGroups(ctx context.Context, connections []my_sites.RealConnection) []groupRateMonitorGroup {
	groups := groupRateMonitorGroups(connections)
	if s.sites == nil {
		return groups
	}
	result := make([]groupRateMonitorGroup, 0, len(groups))
	for _, group := range groups {
		site, err := s.sites.GetSite(ctx, group.SiteID)
		if err != nil || site == nil {
			continue
		}
		group.SiteName = strings.TrimSpace(site.Name)
		result = append(result, group)
	}
	return result
}

func (s *Service) GroupRateMonitorSettings(ctx context.Context, userID string) (GroupRateMonitorSettingsView, error) {
	adminAccountID, repo, err := s.groupRateMonitorWorkspace(ctx, userID)
	if err != nil {
		return GroupRateMonitorSettingsView{}, err
	}
	settings, typeDefaults, overrides, groups, err := s.loadGroupRateMonitorConfiguration(ctx, repo, userID, adminAccountID)
	if err != nil {
		return GroupRateMonitorSettingsView{}, err
	}
	return s.buildGroupRateMonitorSettingsView(ctx, settings, typeDefaults, overrides, groups), nil
}

func (s *Service) loadGroupRateMonitorConfiguration(ctx context.Context, repo groupRateMonitorRepository, userID, adminAccountID string) (GroupRateMonitorSettings, []GroupRateMonitorTypeDefault, []GroupRateMonitorOverride, []groupRateMonitorGroup, error) {
	settings, err := repo.GetGroupRateMonitorSettings(ctx, userID, adminAccountID)
	if err != nil {
		return GroupRateMonitorSettings{}, nil, nil, nil, err
	}
	typeDefaults, err := repo.ListGroupRateMonitorTypeDefaults(ctx, userID, adminAccountID)
	if err != nil {
		return GroupRateMonitorSettings{}, nil, nil, nil, err
	}
	overrides, err := repo.ListGroupRateMonitorOverrides(ctx, userID, adminAccountID)
	if err != nil {
		return GroupRateMonitorSettings{}, nil, nil, nil, err
	}
	connections, err := s.mySites.ListRealConnectionsForWorkspace(ctx, userID, adminAccountID)
	if err != nil {
		return GroupRateMonitorSettings{}, nil, nil, nil, err
	}
	return settings, typeDefaults, overrides, s.existingGroupRateMonitorGroups(ctx, connections), nil
}

func groupRateMonitorTypeDefaultMap(items []GroupRateMonitorTypeDefault) map[string]GroupRateMonitorTypeDefault {
	result := make(map[string]GroupRateMonitorTypeDefault, len(items))
	for _, item := range items {
		groupType := strings.TrimSpace(item.GroupType)
		if groupType != "" {
			item.GroupType = groupType
			item.Model = strings.TrimSpace(item.Model)
			result[groupType] = item
		}
	}
	return result
}

func groupRateMonitorOverrideMap(items []GroupRateMonitorOverride) map[string]GroupRateMonitorOverride {
	result := make(map[string]GroupRateMonitorOverride, len(items))
	for _, item := range items {
		result[groupRateMonitorMapKey(item.UpstreamSiteID, item.UpstreamGroupKey)] = item
	}
	return result
}

func resolveGroupRateMonitorConfig(settings GroupRateMonitorSettings, typeDefaults map[string]GroupRateMonitorTypeDefault, overrides map[string]GroupRateMonitorOverride, group groupRateMonitorGroup) resolvedGroupRateMonitorConfig {
	result := resolvedGroupRateMonitorConfig{
		Enabled:              settings.Enabled,
		Model:                strings.TrimSpace(settings.DefaultModel),
		ProbeIntervalSeconds: settings.ProbeIntervalSeconds,
		FailureThreshold:     settings.FailureThreshold,
	}
	if typeDefault, ok := typeDefaults[strings.TrimSpace(group.GroupType)]; ok {
		result.Enabled = settings.Enabled && typeDefault.Enabled
		result.Model = strings.TrimSpace(typeDefault.Model)
		result.ProbeIntervalSeconds = typeDefault.ProbeIntervalSeconds
		result.FailureThreshold = typeDefault.FailureThreshold
	}
	if override, ok := overrides[groupRateMonitorMapKey(group.SiteID, group.GroupKey)]; ok {
		result.Enabled = result.Enabled && override.Enabled
		if model := strings.TrimSpace(override.Model); model != "" {
			result.Model = model
		}
		if override.ProbeIntervalSeconds != nil {
			result.ProbeIntervalSeconds = *override.ProbeIntervalSeconds
		}
		if override.FailureThreshold != nil {
			result.FailureThreshold = *override.FailureThreshold
		}
	}
	if result.ProbeIntervalSeconds < groupRateMonitorMinInterval || result.ProbeIntervalSeconds > groupRateMonitorMaxInterval {
		result.ProbeIntervalSeconds = groupRateMonitorDefaultInterval
	}
	if result.FailureThreshold < 1 || result.FailureThreshold > groupRateMonitorMaxFailures {
		result.FailureThreshold = groupRateMonitorDefaultFailures
	}
	if !groupRateProbeModelCompatible(group.GroupType, result.Model) {
		result.Model = ""
	}
	return result
}

func groupRateProviderFamily(groupType string) string {
	switch strings.ToLower(strings.TrimSpace(groupType)) {
	case "anthropic", "claude":
		return ProviderAnthropic
	case "gemini", "google":
		return ProviderGemini
	case "openai":
		return ProviderOpenAI
	default:
		return ProviderCustom
	}
}

func groupRateProbeModelCompatible(groupType, model string) bool {
	model = strings.ToLower(strings.TrimSpace(model))
	if model == "" || groupRateProviderFamily(groupType) != ProviderAnthropic {
		return true
	}
	return !strings.HasPrefix(model, "gpt-") &&
		!strings.HasPrefix(model, "chatgpt-") &&
		!strings.HasPrefix(model, "gemini-") &&
		!strings.HasPrefix(model, "o1") &&
		!strings.HasPrefix(model, "o3") &&
		!strings.HasPrefix(model, "o4")
}

func (s *Service) buildGroupRateMonitorSettingsView(_ context.Context, settings GroupRateMonitorSettings, typeDefaults []GroupRateMonitorTypeDefault, overrides []GroupRateMonitorOverride, groups []groupRateMonitorGroup) GroupRateMonitorSettingsView {
	typeConfigs := groupRateMonitorTypeDefaultMap(typeDefaults)
	typeNames := make(map[string]struct{}, len(groups))
	for _, group := range groups {
		if groupType := strings.TrimSpace(group.GroupType); groupType != "" {
			typeNames[groupType] = struct{}{}
		}
	}
	typeViews := make([]GroupRateMonitorTypeDefault, 0, len(typeNames))
	for groupType := range typeNames {
		item, ok := typeConfigs[groupType]
		if !ok {
			model := strings.TrimSpace(settings.DefaultModel)
			if !groupRateProbeModelCompatible(groupType, model) {
				model = ""
			}
			item = GroupRateMonitorTypeDefault{GroupType: groupType, Enabled: settings.Enabled,
				ProbeIntervalSeconds: settings.ProbeIntervalSeconds, FailureThreshold: settings.FailureThreshold,
				Model: model}
		}
		typeViews = append(typeViews, item)
	}
	sort.Slice(typeViews, func(i, j int) bool { return typeViews[i].GroupType < typeViews[j].GroupType })
	overrideByGroup := groupRateMonitorOverrideMap(overrides)
	groupViews := make([]GroupRateMonitorGroupConfig, 0, len(groups))
	for _, group := range groups {
		override, hasOverride := overrideByGroup[groupRateMonitorMapKey(group.SiteID, group.GroupKey)]
		if !hasOverride {
			override = GroupRateMonitorOverride{Enabled: true}
		}
		resolved := resolveGroupRateMonitorConfig(settings, typeConfigs, overrideByGroup, group)
		groupViews = append(groupViews, GroupRateMonitorGroupConfig{
			UpstreamSiteID: group.SiteID, UpstreamSiteName: group.SiteName, UpstreamGroupID: group.GroupID,
			UpstreamGroupName: group.GroupName, GroupType: group.GroupType, Enabled: override.Enabled,
			Model: strings.TrimSpace(override.Model), ProbeIntervalSeconds: override.ProbeIntervalSeconds, FailureThreshold: override.FailureThreshold,
			ResolvedModel: resolved.Model, ResolvedProbeIntervalSeconds: resolved.ProbeIntervalSeconds, ResolvedFailureThreshold: resolved.FailureThreshold,
		})
	}
	return GroupRateMonitorSettingsView{
		Enabled: settings.Enabled, CostGuardEnabled: settings.CostGuardEnabled, ProfitPriorityEnabled: settings.ProfitPriorityEnabled,
		ProbeIntervalSeconds: settings.ProbeIntervalSeconds,
		FailureThreshold:     settings.FailureThreshold, DefaultModel: settings.DefaultModel,
		TypeDefaults: typeViews, Groups: groupViews,
	}
}

func validateGroupRateMonitorInput(input GroupRateMonitorSettingsInput) error {
	if input.ProbeIntervalSeconds < groupRateMonitorMinInterval || input.ProbeIntervalSeconds > groupRateMonitorMaxInterval {
		return requestError(ErrorRequest)
	}
	if input.FailureThreshold < 1 || input.FailureThreshold > groupRateMonitorMaxFailures {
		return requestError(ErrorRequest)
	}
	for _, item := range input.TypeDefaults {
		if strings.TrimSpace(item.GroupType) == "" || item.ProbeIntervalSeconds < groupRateMonitorMinInterval || item.ProbeIntervalSeconds > groupRateMonitorMaxInterval ||
			item.FailureThreshold < 1 || item.FailureThreshold > groupRateMonitorMaxFailures {
			return requestError(ErrorRequest)
		}
	}
	for _, override := range input.Overrides {
		if override.ProbeIntervalSeconds != nil && (*override.ProbeIntervalSeconds < groupRateMonitorMinInterval || *override.ProbeIntervalSeconds > groupRateMonitorMaxInterval) {
			return requestError(ErrorRequest)
		}
		if override.FailureThreshold != nil && (*override.FailureThreshold < 1 || *override.FailureThreshold > groupRateMonitorMaxFailures) {
			return requestError(ErrorRequest)
		}
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
	groups := s.existingGroupRateMonitorGroups(ctx, connections)
	validGroups := make(map[string]groupRateMonitorGroup, len(groups))
	validTypes := make(map[string]struct{}, len(groups))
	for _, group := range groups {
		validGroups[groupRateMonitorMapKey(group.SiteID, group.GroupKey)] = group
		if groupType := strings.TrimSpace(group.GroupType); groupType != "" {
			validTypes[groupType] = struct{}{}
		}
	}
	typeDefaults := make([]GroupRateMonitorTypeDefault, 0, len(input.TypeDefaults))
	seenTypes := make(map[string]struct{}, len(input.TypeDefaults))
	for _, item := range input.TypeDefaults {
		groupType := strings.TrimSpace(item.GroupType)
		if _, valid := validTypes[groupType]; !valid {
			continue
		}
		if !groupRateProbeModelCompatible(groupType, item.Model) {
			return GroupRateMonitorSettingsView{}, requestError(ErrorGroupRateMonitorModelRequired)
		}
		if _, duplicate := seenTypes[groupType]; duplicate {
			return GroupRateMonitorSettingsView{}, requestError(ErrorRequest)
		}
		seenTypes[groupType] = struct{}{}
		typeDefaults = append(typeDefaults, GroupRateMonitorTypeDefault{UserID: userID, AdminAccountID: adminAccountID,
			GroupType: groupType, Enabled: item.Enabled, ProbeIntervalSeconds: item.ProbeIntervalSeconds,
			FailureThreshold: item.FailureThreshold, Model: strings.TrimSpace(item.Model)})
	}
	overrides := make([]GroupRateMonitorOverride, 0, len(input.Overrides))
	seenOverrides := make(map[string]struct{}, len(input.Overrides))
	for _, item := range input.Overrides {
		groupKey := groupRateMonitorGroupKey(item.UpstreamGroupID, item.UpstreamGroupName)
		key := groupRateMonitorMapKey(item.UpstreamSiteID, groupKey)
		group, valid := validGroups[key]
		if !valid {
			continue
		}
		if !groupRateProbeModelCompatible(group.GroupType, item.Model) {
			return GroupRateMonitorSettingsView{}, requestError(ErrorGroupRateMonitorModelRequired)
		}
		if _, duplicate := seenOverrides[key]; duplicate {
			return GroupRateMonitorSettingsView{}, requestError(ErrorRequest)
		}
		seenOverrides[key] = struct{}{}
		overrides = append(overrides, GroupRateMonitorOverride{UserID: userID, AdminAccountID: adminAccountID,
			UpstreamSiteID: group.SiteID, UpstreamGroupKey: group.GroupKey, UpstreamGroupID: group.GroupID,
			UpstreamGroupName: group.GroupName, Enabled: item.Enabled, Model: strings.TrimSpace(item.Model),
			ProbeIntervalSeconds: item.ProbeIntervalSeconds, FailureThreshold: item.FailureThreshold})
	}
	settings := GroupRateMonitorSettings{UserID: userID, AdminAccountID: adminAccountID, Enabled: input.Enabled, CostGuardEnabled: input.CostGuardEnabled,
		ProfitPriorityEnabled: input.ProfitPriorityEnabled,
		ProbeIntervalSeconds:  input.ProbeIntervalSeconds, FailureThreshold: input.FailureThreshold,
		DefaultModel: strings.TrimSpace(input.DefaultModel)}
	if err := repo.SaveGroupRateMonitorSettings(ctx, settings, typeDefaults, overrides); err != nil {
		return GroupRateMonitorSettingsView{}, err
	}
	restore := s.restoreUnmanagedGroupRateMonitorActions(ctx, settings, typeDefaults, overrides, connections)
	view, err := s.GroupRateMonitorSettings(ctx, userID)
	if err != nil {
		return GroupRateMonitorSettingsView{}, err
	}
	view.Restore = restore
	return view, nil
}

func (s *Service) SetGroupRateMonitorCostGuard(ctx context.Context, userID string, enabled bool) (GroupRateMonitorCostGuardInput, error) {
	adminAccountID, repo, err := s.groupRateMonitorWorkspace(ctx, userID)
	if err != nil {
		return GroupRateMonitorCostGuardInput{}, err
	}
	settings, err := repo.GetGroupRateMonitorSettings(ctx, userID, adminAccountID)
	if err != nil {
		return GroupRateMonitorCostGuardInput{}, err
	}
	typeDefaults, err := repo.ListGroupRateMonitorTypeDefaults(ctx, userID, adminAccountID)
	if err != nil {
		return GroupRateMonitorCostGuardInput{}, err
	}
	overrides, err := repo.ListGroupRateMonitorOverrides(ctx, userID, adminAccountID)
	if err != nil {
		return GroupRateMonitorCostGuardInput{}, err
	}
	settings.CostGuardEnabled = enabled
	if err := repo.SaveGroupRateMonitorSettings(ctx, settings, typeDefaults, overrides); err != nil {
		return GroupRateMonitorCostGuardInput{}, err
	}
	return GroupRateMonitorCostGuardInput{Enabled: enabled}, nil
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
	typeDefaults, err := repo.ListGroupRateMonitorTypeDefaults(ctx, userID, adminAccountID)
	if err != nil {
		return nil, err
	}
	overrides, err := repo.ListGroupRateMonitorOverrides(ctx, userID, adminAccountID)
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
	typeModels := groupRateMonitorTypeDefaultMap(typeDefaults)
	overrideByGroup := groupRateMonitorOverrideMap(overrides)
	for _, group := range s.existingGroupRateMonitorGroups(ctx, connections) {
		config := resolveGroupRateMonitorConfig(settings, typeModels, overrideByGroup, group)
		events := cyclesByGroup[groupRateMonitorMapKey(group.SiteID, group.GroupKey)]
		result = append(result, buildGroupRateMonitorSummary(group, config, events))
	}
	return result, nil
}

func buildGroupRateMonitorSummary(group groupRateMonitorGroup, config resolvedGroupRateMonitorConfig, events []GroupRateProbeCycle) GroupRateMonitorSummary {
	summary := GroupRateMonitorSummary{UpstreamSiteID: group.SiteID, UpstreamGroupID: group.GroupID,
		UpstreamGroupName: group.GroupName, Enabled: config.Enabled, Model: config.Model, Events: events, Status: groupRateProbeUnconfigured}
	if summary.Events == nil {
		summary.Events = []GroupRateProbeCycle{}
	}
	if strings.TrimSpace(config.Model) == "" {
		return summary
	}
	if len(events) == 0 {
		if config.Enabled {
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
	interval := time.Duration(config.ProbeIntervalSeconds) * time.Second
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
	typeDefaults, err := repo.ListGroupRateMonitorTypeDefaults(ctx, userID, adminAccountID)
	if err != nil {
		return GroupRateManualProbeResponse{}, err
	}
	overrides, err := repo.ListGroupRateMonitorOverrides(ctx, userID, adminAccountID)
	if err != nil {
		return GroupRateManualProbeResponse{}, err
	}
	connections, err := s.mySites.ListRealConnectionsForWorkspace(ctx, userID, adminAccountID)
	if err != nil {
		return GroupRateManualProbeResponse{}, err
	}
	requestedKey := groupRateMonitorGroupKey(input.UpstreamGroupID, input.UpstreamGroupName)
	var selected *groupRateMonitorGroup
	for _, group := range s.existingGroupRateMonitorGroups(ctx, connections) {
		if group.SiteID == input.UpstreamSiteID && group.GroupKey == requestedKey {
			copy := group
			selected = &copy
			break
		}
	}
	if selected == nil {
		return GroupRateManualProbeResponse{}, requestError(ErrorNotFound)
	}
	config := resolveGroupRateMonitorConfig(settings, groupRateMonitorTypeDefaultMap(typeDefaults), groupRateMonitorOverrideMap(overrides), *selected)
	if strings.TrimSpace(config.Model) == "" {
		return GroupRateManualProbeResponse{}, requestError(ErrorGroupRateMonitorDisabled)
	}
	session, err := s.mySites.RequireSession(ctx, userID, adminAccountID)
	if err != nil {
		return GroupRateManualProbeResponse{}, err
	}
	runtimeSettings := settings
	runtimeSettings.Enabled = config.Enabled
	runtimeSettings.ProbeIntervalSeconds = config.ProbeIntervalSeconds
	runtimeSettings.FailureThreshold = config.FailureThreshold
	runtimeSettings.DefaultModel = config.Model
	cycle, dispatch, err := s.runGroupRateProbeCycle(ctx, runtimeSettings, session, *selected, config.Model, "manual")
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
	summary := buildGroupRateMonitorSummary(*selected, config, groupEvents)
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
	if previousModel := strings.TrimSpace(state.Model); previousModel != "" && previousModel != strings.TrimSpace(model) {
		state.ConsecutiveFailures = 0
		state.LastResult = ""
		state.LastErrorKey = ""
		state.LastErrorDetail = ""
		state.UnavailableReason = ""
	}
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

	actionMode := ""
	if trigger == "scheduled" && settings.Enabled {
		actionMode = "automatic"
	} else if trigger == "manual" && !settings.Enabled {
		actionMode = "manual"
	}
	dispatch := s.reconcileGroupRateMonitorAccounts(ctx, repo, settings, session, group, detail, actionMode)
	if err := repo.InsertGroupRateMonitorCycle(ctx, cycle); err != nil {
		return GroupRateProbeCycle{}, nil, err
	}
	if settings.ProfitPriorityEnabled {
		_, _ = s.reconcileProfitPriorityWorkspace(ctx, settings.UserID, settings.AdminAccountID, true)
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
	if groupRateProviderFamily(group.GroupType) == ProviderAnthropic && groupRateGroupClaudeCodeOnly(site.Metrics.Groups, group) {
		return ProbeRequest{}, upstream.ReasonClaudeCodeOnly
	}
	return ProbeRequest{
		BaseURL: site.BaseURL, UpstreamKey: key, ProviderFamily: groupRateProviderFamily(group.GroupType),
		ModelName: model, MaxTokens: 1,
	}, ""
}

func groupRateGroupClaudeCodeOnly(groups []upstream.GroupInfo, target groupRateMonitorGroup) bool {
	targetID := strings.TrimSpace(target.GroupID)
	targetName := strings.TrimSpace(target.GroupName)
	for _, group := range groups {
		if targetID != "" && strings.TrimSpace(group.ID) == targetID {
			return group.ClaudeCodeOnly
		}
	}
	for _, group := range groups {
		if targetName != "" && strings.TrimSpace(group.Name) == targetName {
			return group.ClaudeCodeOnly
		}
	}
	return false
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

func (s *Service) reconcileGroupRateMonitorAccounts(ctx context.Context, repo groupRateMonitorRepository, settings GroupRateMonitorSettings, session upstream.Session, group groupRateMonitorGroup, probe GroupRateProbeTargetResult, actionMode string) []BoundDispatchAccountState {
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
		if probe.Available && actionMode != "" {
			detail := probe
			detail.TargetID = targetID
			detail.AccountID = accountID
			detail.AccountName = item.Name
			detail.Status = remote.Status
			detail.Schedulable = remote.Schedulable
			if actionMode == "manual" {
				item.ActionResult, item.Status, item.Schedulable = s.forceGroupRateMonitorAccount(ctx, repo, settings, session, detail, remote)
			} else {
				item.ActionResult, item.Status, item.Schedulable = s.reconcileGroupRateMonitorAccount(ctx, repo, settings, session, group, detail, remote, true)
			}
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

func (s *Service) forceGroupRateMonitorAccount(ctx context.Context, repo groupRateMonitorRepository, settings GroupRateMonitorSettings, session upstream.Session, detail GroupRateProbeTargetResult, remote upstream.Sub2APIAdminAccountState) (string, string, *bool) {
	if remote.Schedulable == nil {
		return "unavailable", remote.Status, remote.Schedulable
	}
	if !detail.Healthy && detail.ConsecutiveFailures < settings.FailureThreshold {
		return "", remote.Status, remote.Schedulable
	}
	if action, err := repo.GetGroupRateMonitorAction(ctx, settings.UserID, settings.AdminAccountID, detail.TargetID); err != nil {
		return "action_state_failed", remote.Status, remote.Schedulable
	} else if action != nil {
		return "managed_elsewhere", remote.Status, remote.Schedulable
	}
	managed, err := s.repo.GetTargetActionState(ctx, settings.UserID, settings.AdminAccountID, detail.TargetID)
	if err != nil {
		return "action_state_failed", remote.Status, remote.Schedulable
	}
	if managed != nil {
		return "managed_elsewhere", remote.Status, remote.Schedulable
	}
	desiredStatus := "inactive"
	desiredSchedulable := false
	result := "disabled"
	if detail.Healthy {
		if s.balancePauses != nil {
			paused, pauseErr := s.balancePauses.IsAccountBalancePausedForWorkspace(ctx, settings.UserID, settings.AdminAccountID, detail.AccountID)
			if pauseErr != nil || paused {
				return RemoteActionSkippedBalanceSuspended, remote.Status, remote.Schedulable
			}
		}
		desiredStatus = "active"
		desiredSchedulable = true
		result = "restored"
	}
	currentStatus := normalizeTargetStatus(string(upstream.PlatformSub2API), remote.Status)
	if currentStatus == desiredStatus && *remote.Schedulable == desiredSchedulable {
		return result, desiredStatus, boolPointer(desiredSchedulable)
	}
	if err := s.schedulingActions.UpdateSub2APIAdminAccountState(session, detail.AccountID, &desiredStatus, &desiredSchedulable); err != nil {
		if detail.Healthy {
			return "restore_failed", remote.Status, remote.Schedulable
		}
		return "disable_failed", remote.Status, remote.Schedulable
	}
	return result, desiredStatus, boolPointer(desiredSchedulable)
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

func (s *Service) restoreUnmanagedGroupRateMonitorActions(ctx context.Context, settings GroupRateMonitorSettings, typeDefaults []GroupRateMonitorTypeDefault, overrides []GroupRateMonitorOverride, connections []my_sites.RealConnection) GroupRateMonitorRestoreSummary {
	repo, err := groupRateMonitorRepo(s.repo)
	if err != nil {
		return GroupRateMonitorRestoreSummary{Pending: 1}
	}
	managedGroups := make(map[string]struct{})
	typeModels := groupRateMonitorTypeDefaultMap(typeDefaults)
	overrideByGroup := groupRateMonitorOverrideMap(overrides)
	if settings.Enabled {
		for _, group := range s.existingGroupRateMonitorGroups(ctx, connections) {
			config := resolveGroupRateMonitorConfig(settings, typeModels, overrideByGroup, group)
			if !config.Enabled || strings.TrimSpace(config.Model) == "" {
				continue
			}
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
		typeDefaults, err := repo.ListGroupRateMonitorTypeDefaults(ctx, settings.UserID, settings.AdminAccountID)
		if err != nil {
			continue
		}
		overrides, err := repo.ListGroupRateMonitorOverrides(ctx, settings.UserID, settings.AdminAccountID)
		if err != nil {
			continue
		}
		typeModels := groupRateMonitorTypeDefaultMap(typeDefaults)
		overrideByGroup := groupRateMonitorOverrideMap(overrides)
		now := time.Now()
		for _, group := range s.existingGroupRateMonitorGroups(ctx, connections) {
			config := resolveGroupRateMonitorConfig(settings, typeModels, overrideByGroup, group)
			if !config.Enabled || strings.TrimSpace(config.Model) == "" {
				continue
			}
			last := latest[groupRateMonitorMapKey(group.SiteID, group.GroupKey)]
			if !last.IsZero() && now.Sub(last) < time.Duration(config.ProbeIntervalSeconds)*time.Second {
				continue
			}
			settingsCopy, sessionCopy, groupCopy, configCopy := settings, session, group, config
			settingsCopy.Enabled = config.Enabled
			settingsCopy.ProbeIntervalSeconds = config.ProbeIntervalSeconds
			settingsCopy.FailureThreshold = config.FailureThreshold
			settingsCopy.DefaultModel = config.Model
			wait.Add(1)
			go func() {
				defer wait.Done()
				limit <- struct{}{}
				defer func() { <-limit }()
				// Another process may have completed this group while this job waited for capacity.
				latestNow, listErr := repo.ListLatestGroupRateMonitorCycles(ctx, settingsCopy.UserID, settingsCopy.AdminAccountID)
				if listErr == nil {
					lastRun := latestNow[groupRateMonitorMapKey(groupCopy.SiteID, groupCopy.GroupKey)]
					if !lastRun.IsZero() && time.Since(lastRun) < time.Duration(configCopy.ProbeIntervalSeconds)*time.Second {
						return
					}
				}
				if _, _, probeErr := s.runGroupRateProbeCycle(ctx, settingsCopy, sessionCopy, groupCopy, configCopy.Model, "scheduled"); probeErr != nil {
					log.Printf("[group-rate-monitor] probe failed workspace=%s site=%s group=%s err=%v", settingsCopy.AdminAccountID, groupCopy.SiteID, groupCopy.GroupName, probeErr)
				}
			}()
		}
		_ = s.restoreUnmanagedGroupRateMonitorActions(ctx, settings, typeDefaults, overrides, connections)
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
