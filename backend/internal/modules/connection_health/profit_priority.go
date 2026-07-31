package connection_health

import (
	"context"
	"log"
	"math"
	"sort"
	"strings"
	"time"

	"transithub/backend/internal/modules/my_sites"
	"transithub/backend/internal/modules/upstream"
)

const (
	profitPriorityRuntimeWindow       = 10 * time.Minute
	profitPriorityRuntimeLimit        = 200
	profitPriorityProbeLimit          = 5
	profitPriorityCacheTTL            = time.Minute
	profitPriorityObservationInterval = time.Minute
	profitPriorityMinSamples          = 30
	profitPriorityEWMAAlpha           = 0.30
	profitPriorityRankConfirmRounds   = 3
	profitPriorityCooldown            = 5 * time.Minute
	profitPrioritySpacing             = 10
	profitPriorityLatencyAbsMs        = 2000
	profitPriorityLatencyRatio        = 0.20
)

type ProfitPriorityActioner interface {
	GetSub2APIAdminAccountState(session upstream.Session, accountID string) (upstream.Sub2APIAdminAccountState, error)
	UpdateSub2APIAdminAccountPriority(session upstream.Session, accountID string, priority int) (upstream.Sub2APIAdminAccountState, error)
	FetchSub2APIAdminAccountRuntimeSamples(session upstream.Session, accountID string, since time.Time, limit int) ([]upstream.Sub2APIAccountRuntimeSample, error)
}

type ProfitPrioritySummary struct {
	Enabled   bool `json:"enabled"`
	Updated   int  `json:"updated"`
	Unchanged int  `json:"unchanged"`
	Skipped   int  `json:"skipped"`
	Failed    int  `json:"failed"`
	Restored  int  `json:"restored"`
	Conflict  int  `json:"conflict"`
}

type profitRuntimeCacheEntry struct {
	samples   []upstream.Sub2APIAccountRuntimeSample
	fetchedAt time.Time
}

type profitPriorityCandidate struct {
	accountID     string
	accountName   string
	connection    my_sites.RealConnection
	sourceKey     string
	groupIDs      map[string]struct{}
	effectiveCost float64
	remote        upstream.Sub2APIAdminAccountState
	stabilityTier string
	stabilityRank int
	successRate   *float64
	latencyMs     *int
	sampleCount   int
	probeFailures int
	hardFailure   bool
	emergency     bool
	costChanged   bool
	observed      bool
	ambiguousCost bool
}

type profitProbeStats struct {
	successes            int
	failures             int
	consecutiveFailures  int
	latestSuccessLatency *int
}

func profitPriorityRepo(repo healthRepository) (profitPriorityRepository, error) {
	result, ok := repo.(profitPriorityRepository)
	if !ok {
		return nil, requestError(ErrorUnknown)
	}
	return result, nil
}

func (s *Service) SetGroupRateMonitorProfitPriority(ctx context.Context, userID string, enabled bool) (ProfitPrioritySummary, error) {
	adminAccountID, repo, err := s.groupRateMonitorWorkspace(ctx, userID)
	if err != nil {
		return ProfitPrioritySummary{}, err
	}
	settings, err := repo.GetGroupRateMonitorSettings(ctx, userID, adminAccountID)
	if err != nil {
		return ProfitPrioritySummary{}, err
	}
	typeDefaults, err := repo.ListGroupRateMonitorTypeDefaults(ctx, userID, adminAccountID)
	if err != nil {
		return ProfitPrioritySummary{}, err
	}
	overrides, err := repo.ListGroupRateMonitorOverrides(ctx, userID, adminAccountID)
	if err != nil {
		return ProfitPrioritySummary{}, err
	}
	settings.ProfitPriorityEnabled = enabled
	if err := repo.SaveGroupRateMonitorSettings(ctx, settings, typeDefaults, overrides); err != nil {
		return ProfitPrioritySummary{}, err
	}
	return s.reconcileProfitPriorityWorkspace(ctx, userID, adminAccountID, enabled)
}

// ReconcileProfitPriorityWorkspace is called after an upstream rate sync. It is
// best-effort: rate monitoring and notifications must continue when ranking fails.
func (s *Service) ReconcileProfitPriorityWorkspace(ctx context.Context, userID, adminAccountID string) ProfitPrioritySummary {
	repo, err := groupRateMonitorRepo(s.repo)
	if err != nil {
		return ProfitPrioritySummary{Failed: 1}
	}
	settings, err := repo.GetGroupRateMonitorSettings(ctx, userID, adminAccountID)
	if err != nil {
		log.Printf("[profit-priority] settings load failed workspace=%s err=%v", adminAccountID, err)
		return ProfitPrioritySummary{Failed: 1}
	}
	result, err := s.reconcileProfitPriorityWorkspace(ctx, userID, adminAccountID, settings.ProfitPriorityEnabled)
	if err != nil {
		log.Printf("[profit-priority] reconcile failed workspace=%s err=%v", adminAccountID, err)
		result.Failed++
	}
	return result
}

func (s *Service) reconcileProfitPriorityWorkspace(ctx context.Context, userID, adminAccountID string, enabled bool) (ProfitPrioritySummary, error) {
	result := ProfitPrioritySummary{Enabled: enabled}
	if s.profitActions == nil || s.mySites == nil || s.sites == nil {
		result.Failed++
		return result, nil
	}
	repo, err := profitPriorityRepo(s.repo)
	if err != nil {
		return result, err
	}

	// Serialize remote priority writes so a sync-triggered pass and the periodic
	// pass cannot mistake each other's in-flight value for a manual override.
	s.profitPriorityMu.Lock()
	defer s.profitPriorityMu.Unlock()

	session, err := s.mySites.RequireSession(ctx, userID, adminAccountID)
	if err != nil {
		return result, err
	}
	if session.Platform != upstream.PlatformSub2API {
		return result, nil
	}
	states, err := repo.ListProfitPriorityStates(ctx, userID, adminAccountID)
	if err != nil {
		return result, err
	}
	if !enabled {
		return s.restoreProfitPriorityStates(ctx, repo, session, states, result), nil
	}

	connections, err := s.mySites.ListRealConnectionsForWorkspace(ctx, userID, adminAccountID)
	if err != nil {
		return result, err
	}
	monitorRepo, err := groupRateMonitorRepo(s.repo)
	if err != nil {
		return result, err
	}
	cycles, err := monitorRepo.ListGroupRateMonitorCycles(ctx, userID, adminAccountID, profitPriorityProbeLimit)
	if err != nil {
		return result, err
	}
	probeByGroup := profitProbeStatsByGroup(cycles)
	candidates, managed, buildSummary := s.buildProfitPriorityCandidates(ctx, session, userID, adminAccountID, connections, probeByGroup)
	result.Skipped += buildSummary.Skipped
	result.Failed += buildSummary.Failed

	stateByAccount := make(map[string]ProfitPriorityState, len(states))
	for _, state := range states {
		stateByAccount[state.AccountID] = state
	}
	now := time.Now()
	for accountID, candidate := range candidates {
		stored, exists := stateByAccount[accountID]
		stored = prepareProfitPriorityState(now, candidate, stored, exists)
		stateByAccount[accountID] = stored
		if err := repo.UpsertProfitPriorityState(ctx, stored); err != nil {
			delete(candidates, accountID)
			result.Failed++
		}
	}
	components := profitPriorityComponents(candidates)
	for _, component := range components {
		if len(component) < 2 {
			continue
		}
		ranked := rankProfitPriorityCandidates(component)
		base := ranked[0].remotePriority()
		for _, candidate := range ranked[1:] {
			if current := candidate.remotePriority(); current < base {
				base = current
			}
		}
		if base < 1 {
			base = 1
		}
		componentReady := true
		for index, candidate := range ranked {
			stored := stateByAccount[candidate.accountID]
			if candidate.observed {
				observedRank := index + 1
				if stored.ObservedRank == observedRank {
					stored.ObservedRankRounds++
				} else {
					stored.ObservedRank = observedRank
					stored.ObservedRankRounds = 1
				}
				stateByAccount[candidate.accountID] = stored
				if err := repo.UpsertProfitPriorityState(ctx, stored); err != nil {
					componentReady = false
					result.Failed++
				}
			}
		}
		if componentReady && !profitPriorityComponentReady(now, ranked, stateByAccount, base) {
			componentReady = false
		}
		for index, candidate := range ranked {
			desired := base + index*profitPrioritySpacing
			stored, exists := stateByAccount[candidate.accountID]
			if !componentReady {
				if candidate.remotePriority() == desired {
					result.Unchanged++
				} else if stored.Conflict {
					result.Conflict++
				} else {
					result.Skipped++
				}
				continue
			}
			status := s.applyProfitPriority(ctx, repo, session, candidate, desired, stored, exists)
			switch status {
			case "updated":
				result.Updated++
			case "unchanged":
				result.Unchanged++
			case "conflict":
				result.Conflict++
			case "failed":
				result.Failed++
			default:
				result.Skipped++
			}
		}
	}

	// Restore accounts that no longer supply any current downstream group. An
	// inactive account remains managed so a temporary health stop does not erase
	// the original priority snapshot before it recovers.
	stale := make([]ProfitPriorityState, 0)
	for _, state := range states {
		if _, exists := managed[state.AccountID]; !exists {
			stale = append(stale, state)
		}
	}
	result = s.restoreProfitPriorityStates(ctx, repo, session, stale, result)
	return result, nil
}

func (c *profitPriorityCandidate) remotePriority() int {
	if c.remote.Priority == nil || *c.remote.Priority < 1 {
		return 1
	}
	return *c.remote.Priority
}

func (s *Service) buildProfitPriorityCandidates(
	ctx context.Context,
	session upstream.Session,
	userID, adminAccountID string,
	connections []my_sites.RealConnection,
	probeByGroup map[string]profitProbeStats,
) (map[string]*profitPriorityCandidate, map[string]struct{}, ProfitPrioritySummary) {
	result := make(map[string]*profitPriorityCandidate)
	managed := make(map[string]struct{})
	summary := ProfitPrioritySummary{}
	sites := make(map[string]*upstream.Site)
	for _, connection := range connections {
		accountID := strings.TrimSpace(connection.AdminAccountID)
		platform := strings.ToLower(strings.TrimSpace(connection.AdminPlatform))
		if accountID == "" || len(connection.OwnGroupIDs) == 0 ||
			(connection.Status != "" && connection.Status != my_sites.ConnectionStatusActive) ||
			(platform != "" && platform != string(upstream.PlatformSub2API)) {
			continue
		}
		site := sites[connection.UpstreamSiteID]
		if site == nil {
			loaded, err := s.sites.GetSite(ctx, connection.UpstreamSiteID)
			if err != nil || loaded == nil || loaded.UserID != userID || loaded.AdminAccountID != adminAccountID {
				summary.Failed++
				continue
			}
			site = loaded
			sites[connection.UpstreamSiteID] = site
		}
		cost, ok := profitPriorityConnectionCost(connection, site)
		if !ok {
			summary.Skipped++
			continue
		}
		sourceKey := groupRateMonitorMapKey(connection.UpstreamSiteID,
			groupRateMonitorGroupKey(connection.UpstreamGroupID, connection.UpstreamGroupName))
		candidate := result[accountID]
		if candidate == nil {
			candidate = &profitPriorityCandidate{accountID: accountID, accountName: connection.AdminAccountName,
				connection: connection, sourceKey: sourceKey, groupIDs: make(map[string]struct{}), effectiveCost: cost}
			result[accountID] = candidate
		} else if candidate.sourceKey != sourceKey || math.Abs(candidate.effectiveCost-cost) > 1e-9 {
			candidate.ambiguousCost = true
		}
		for _, groupID := range connection.OwnGroupIDs {
			if groupID = strings.TrimSpace(groupID); groupID != "" {
				candidate.groupIDs[groupID] = struct{}{}
			}
		}
	}

	for accountID, candidate := range result {
		if candidate.ambiguousCost {
			delete(result, accountID)
			summary.Skipped++
			continue
		}
		managed[accountID] = struct{}{}
		remote, err := s.profitActions.GetSub2APIAdminAccountState(session, accountID)
		if err != nil {
			delete(result, accountID)
			summary.Failed++
			continue
		}
		candidate.remote = remote
		if !strings.EqualFold(strings.TrimSpace(remote.Status), "active") || remote.Schedulable == nil || !*remote.Schedulable || remote.Priority == nil {
			delete(result, accountID)
			summary.Skipped++
			continue
		}
		if s.balancePauses != nil {
			paused, err := s.balancePauses.IsAccountBalancePausedForWorkspace(ctx, userID, adminAccountID, accountID)
			if err != nil || paused {
				delete(result, accountID)
				summary.Skipped++
				continue
			}
		}
		runtime, err := s.profitRuntimeSamples(ctx, session, adminAccountID, accountID)
		if err != nil {
			summary.Failed++
		}
		probe := probeByGroup[candidate.sourceKey]
		candidate.applyRuntimeAndProbe(runtime, probe)
	}
	return result, managed, summary
}

func profitPriorityConnectionCost(connection my_sites.RealConnection, site *upstream.Site) (float64, bool) {
	if site == nil || site.RechargeRate <= 0 || site.BalanceSuspended || site.Status == upstream.StatusDisabled {
		return 0, false
	}
	for _, group := range site.Metrics.Groups {
		if strings.TrimSpace(connection.UpstreamGroupID) != "" && group.ID == connection.UpstreamGroupID && group.Multiplier != nil && !group.EffectiveMultiplierUnverified {
			return *group.Multiplier * site.RechargeRate, true
		}
	}
	for _, group := range site.Metrics.Groups {
		if group.Name == connection.UpstreamGroupName && group.Multiplier != nil && !group.EffectiveMultiplierUnverified {
			return *group.Multiplier * site.RechargeRate, true
		}
	}
	return 0, false
}

func (s *Service) profitRuntimeSamples(ctx context.Context, session upstream.Session, workspaceID, accountID string) ([]upstream.Sub2APIAccountRuntimeSample, error) {
	cacheKey := workspaceID + "|" + accountID
	now := time.Now()
	if cached, ok := s.profitRuntimeCache[cacheKey]; ok && now.Sub(cached.fetchedAt) < profitPriorityCacheTTL {
		return append([]upstream.Sub2APIAccountRuntimeSample(nil), cached.samples...), nil
	}
	samples, err := s.profitActions.FetchSub2APIAdminAccountRuntimeSamples(session, accountID, now.Add(-profitPriorityRuntimeWindow), profitPriorityRuntimeLimit)
	if err != nil {
		return nil, err
	}
	s.profitRuntimeCache[cacheKey] = profitRuntimeCacheEntry{samples: append([]upstream.Sub2APIAccountRuntimeSample(nil), samples...), fetchedAt: now}
	return samples, nil
}

func profitProbeStatsByGroup(cycles []GroupRateProbeCycle) map[string]profitProbeStats {
	byGroup := make(map[string][]GroupRateProbeCycle)
	for _, cycle := range cycles {
		key := groupRateMonitorMapKey(cycle.UpstreamSiteID, cycle.UpstreamGroupKey)
		byGroup[key] = append(byGroup[key], cycle)
	}
	result := make(map[string]profitProbeStats, len(byGroup))
	for key, events := range byGroup {
		stats := profitProbeStats{}
		for _, event := range events {
			if event.SuccessCount >= event.TargetCount && event.TargetCount > 0 {
				stats.successes++
				if len(event.Details) > 0 && event.Details[0].LatencyMs != nil {
					latency := *event.Details[0].LatencyMs
					stats.latestSuccessLatency = &latency
				}
			} else if event.Status != groupRateProbeUnavailable && event.Status != groupRateProbeUnconfigured {
				stats.failures++
			}
		}
		for index := len(events) - 1; index >= 0; index-- {
			if events[index].SuccessCount >= events[index].TargetCount && events[index].TargetCount > 0 {
				break
			}
			if events[index].Status == groupRateProbeUnavailable || events[index].Status == groupRateProbeUnconfigured {
				break
			}
			stats.consecutiveFailures++
		}
		result[key] = stats
	}
	return result
}

func (candidate *profitPriorityCandidate) applyRuntimeAndProbe(runtime []upstream.Sub2APIAccountRuntimeSample, probe profitProbeStats) {
	candidate.sampleCount = len(runtime)
	candidate.probeFailures = probe.consecutiveFailures
	successes := probe.successes
	failures := probe.failures
	latencies := make([]int, 0, len(runtime))
	for _, sample := range runtime {
		if sample.Success {
			successes++
		} else {
			failures++
		}
		if sample.Success && sample.LatencyMs != nil {
			latencies = append(latencies, *sample.LatencyMs)
		}
	}
	if len(latencies) > 0 {
		sort.Ints(latencies)
		median := latencies[len(latencies)/2]
		if len(latencies)%2 == 0 {
			median = (latencies[len(latencies)/2-1] + latencies[len(latencies)/2]) / 2
		}
		candidate.latencyMs = &median
	} else {
		candidate.latencyMs = probe.latestSuccessLatency
	}
	total := successes + failures
	if total > 0 {
		rate := float64(successes) / float64(total)
		candidate.successRate = &rate
	}
	candidate.hardFailure = probe.consecutiveFailures >= 2 ||
		(candidate.sampleCount >= profitPriorityMinSamples && candidate.successRate != nil && *candidate.successRate < 0.60)
	switch {
	case candidate.hardFailure:
		candidate.stabilityTier, candidate.stabilityRank = "unstable", 3
	case probe.consecutiveFailures == 1 || (candidate.sampleCount >= profitPriorityMinSamples && candidate.successRate != nil && *candidate.successRate < 0.90):
		candidate.stabilityTier, candidate.stabilityRank = "warning", 2
	case candidate.sampleCount < profitPriorityMinSamples:
		candidate.stabilityTier, candidate.stabilityRank = "unknown", 1
	default:
		candidate.stabilityTier, candidate.stabilityRank = "stable", 0
	}
}

func prepareProfitPriorityState(now time.Time, candidate *profitPriorityCandidate, stored ProfitPriorityState, exists bool) ProfitPriorityState {
	current := candidate.remotePriority()
	if !exists {
		stored = ProfitPriorityState{
			UserID:              candidate.connection.UserID,
			AdminAccountID:      candidate.connection.WorkspaceAdminAccountID,
			AccountID:           candidate.accountID,
			OriginalPriority:    current,
			LastAppliedPriority: current,
			StabilityTier:       "unknown",
		}
	}
	if stored.StabilityTier == "" {
		stored.StabilityTier = "unknown"
	}
	if !stored.Conflict {
		switch {
		case stored.PendingPriority != nil && current == *stored.PendingPriority:
			stored.LastAppliedPriority = current
			stored.PendingPriority = nil
		case exists && stored.PendingPriority == nil && current != stored.LastAppliedPriority:
			stored.Conflict = true
		case exists && stored.PendingPriority != nil && current != stored.LastAppliedPriority:
			stored.PendingPriority = nil
			stored.Conflict = true
		}
	}

	candidate.costChanged = exists && stored.EffectiveCost != nil &&
		math.Abs(*stored.EffectiveCost-candidate.effectiveCost) > 1e-9
	cost := candidate.effectiveCost
	stored.EffectiveCost = &cost

	accepted := stored.LastObservedAt == nil || now.Sub(*stored.LastObservedAt) >= profitPriorityObservationInterval
	if accepted {
		candidate.observed = true
		observedAt := now
		stored.LastObservedAt = &observedAt
		stored.SampleCount = candidate.sampleCount
		stored.SuccessRate = profitPrioritySmoothFloat(stored.SuccessRate, candidate.successRate)
		stored.LatencyMs = profitPrioritySmoothInt(stored.LatencyMs, candidate.latencyMs)
	}

	if candidate.hardFailure {
		candidate.emergency = stored.StabilityTier != "unstable"
		stored.StabilityTier = "unstable"
		stored.ObservedStabilityTier = "unstable"
		stored.ObservedStabilityRounds = 0
	} else if accepted && candidate.sampleCount >= profitPriorityMinSamples {
		target := profitPriorityTargetTier(stored.StabilityTier, stored.SuccessRate, candidate.probeFailures)
		if target == stored.StabilityTier {
			stored.ObservedStabilityTier = target
			stored.ObservedStabilityRounds = 0
		} else {
			if stored.ObservedStabilityTier == target {
				stored.ObservedStabilityRounds++
			} else {
				stored.ObservedStabilityTier = target
				stored.ObservedStabilityRounds = 1
			}
			required := 2
			if target == "stable" {
				required = 3
			}
			if stored.ObservedStabilityRounds >= required {
				stored.StabilityTier = target
				stored.ObservedStabilityRounds = 0
			}
		}
	}

	candidate.successRate = stored.SuccessRate
	candidate.latencyMs = stored.LatencyMs
	candidate.stabilityTier = stored.StabilityTier
	candidate.stabilityRank = profitPriorityStabilityRank(stored.StabilityTier)
	return stored
}

func profitPrioritySmoothFloat(previous, current *float64) *float64 {
	if current == nil {
		return previous
	}
	value := *current
	if previous != nil {
		value = (1-profitPriorityEWMAAlpha)*(*previous) + profitPriorityEWMAAlpha*value
	}
	return &value
}

func profitPrioritySmoothInt(previous, current *int) *int {
	if current == nil {
		return previous
	}
	value := *current
	if previous != nil {
		value = int(math.Round((1-profitPriorityEWMAAlpha)*float64(*previous) + profitPriorityEWMAAlpha*float64(value)))
	}
	return &value
}

func profitPriorityTargetTier(current string, successRate *float64, probeFailures int) string {
	if probeFailures > 0 || (successRate != nil && *successRate < 0.90) {
		return "warning"
	}
	if successRate != nil && (*successRate >= 0.95 || (current == "unknown" && *successRate >= 0.90)) {
		return "stable"
	}
	return current
}

func profitPriorityStabilityRank(tier string) int {
	switch tier {
	case "stable":
		return 0
	case "unknown":
		return 1
	case "warning":
		return 2
	case "unstable":
		return 3
	default:
		return 1
	}
}

func profitPriorityComponentReady(now time.Time, ranked []*profitPriorityCandidate, states map[string]ProfitPriorityState, base int) bool {
	for _, candidate := range ranked {
		if candidate.emergency || candidate.costChanged {
			return true
		}
	}
	for index, candidate := range ranked {
		desired := base + index*profitPrioritySpacing
		if candidate.remotePriority() == desired {
			continue
		}
		stored := states[candidate.accountID]
		if stored.Conflict {
			continue
		}
		if stored.ObservedRank != index+1 || stored.ObservedRankRounds < profitPriorityRankConfirmRounds ||
			(stored.CooldownUntil != nil && now.Before(*stored.CooldownUntil)) {
			return false
		}
	}
	return true
}

func profitPriorityComponents(candidates map[string]*profitPriorityCandidate) [][]*profitPriorityCandidate {
	parent := make(map[string]string, len(candidates))
	for id := range candidates {
		parent[id] = id
	}
	var find func(string) string
	find = func(id string) string {
		if parent[id] != id {
			parent[id] = find(parent[id])
		}
		return parent[id]
	}
	union := func(a, b string) {
		ra, rb := find(a), find(b)
		if ra != rb {
			parent[rb] = ra
		}
	}
	byGroup := make(map[string][]string)
	for id, candidate := range candidates {
		for groupID := range candidate.groupIDs {
			byGroup[groupID] = append(byGroup[groupID], id)
		}
	}
	for _, ids := range byGroup {
		for index := 1; index < len(ids); index++ {
			union(ids[0], ids[index])
		}
	}
	components := make(map[string][]*profitPriorityCandidate)
	for id, candidate := range candidates {
		root := find(id)
		components[root] = append(components[root], candidate)
	}
	result := make([][]*profitPriorityCandidate, 0, len(components))
	for _, component := range components {
		result = append(result, component)
	}
	return result
}

func rankProfitPriorityCandidates(component []*profitPriorityCandidate) []*profitPriorityCandidate {
	byTier := make(map[int][]*profitPriorityCandidate)
	for _, candidate := range component {
		byTier[candidate.stabilityRank] = append(byTier[candidate.stabilityRank], candidate)
	}
	result := make([]*profitPriorityCandidate, 0, len(component))
	for tier := 0; tier <= 3; tier++ {
		items := byTier[tier]
		known := make([]*profitPriorityCandidate, 0, len(items))
		unknown := make([]*profitPriorityCandidate, 0, len(items))
		for _, item := range items {
			if item.latencyMs == nil {
				unknown = append(unknown, item)
			} else {
				known = append(known, item)
			}
		}
		sort.SliceStable(known, func(i, j int) bool {
			if *known[i].latencyMs != *known[j].latencyMs {
				return *known[i].latencyMs < *known[j].latencyMs
			}
			return known[i].accountID < known[j].accountID
		})
		for start := 0; start < len(known); {
			anchor := *known[start].latencyMs
			tolerance := maxInt(profitPriorityLatencyAbsMs, int(math.Round(float64(anchor)*profitPriorityLatencyRatio)))
			end := start + 1
			for end < len(known) && *known[end].latencyMs-anchor <= tolerance {
				end++
			}
			sortProfitPriorityByCost(known[start:end])
			result = append(result, known[start:end]...)
			start = end
		}
		sortProfitPriorityByCost(unknown)
		result = append(result, unknown...)
	}
	return result
}

func sortProfitPriorityByCost(items []*profitPriorityCandidate) {
	sort.SliceStable(items, func(i, j int) bool {
		if math.Abs(items[i].effectiveCost-items[j].effectiveCost) > 1e-9 {
			return items[i].effectiveCost < items[j].effectiveCost
		}
		return items[i].accountID < items[j].accountID
	})
}

func (s *Service) applyProfitPriority(ctx context.Context, repo profitPriorityRepository, session upstream.Session, candidate *profitPriorityCandidate, desired int, stored ProfitPriorityState, exists bool) string {
	current := candidate.remotePriority()
	if !exists {
		stored = ProfitPriorityState{UserID: candidate.connection.UserID, AdminAccountID: candidate.connection.WorkspaceAdminAccountID,
			AccountID: candidate.accountID, OriginalPriority: current, LastAppliedPriority: current}
	}
	stored.StabilityTier = candidate.stabilityTier
	stored.SuccessRate = candidate.successRate
	stored.LatencyMs = candidate.latencyMs
	cost := candidate.effectiveCost
	stored.EffectiveCost = &cost
	if stored.Conflict {
		return "conflict"
	}
	if stored.PendingPriority != nil && current == *stored.PendingPriority {
		stored.LastAppliedPriority = current
		stored.PendingPriority = nil
	}
	if exists && stored.PendingPriority == nil && current != stored.LastAppliedPriority {
		stored.Conflict = true
		_ = repo.UpsertProfitPriorityState(ctx, stored)
		return "conflict"
	}
	if stored.PendingPriority != nil && current != stored.LastAppliedPriority {
		stored.PendingPriority = nil
		stored.Conflict = true
		_ = repo.UpsertProfitPriorityState(ctx, stored)
		return "conflict"
	}
	if current == desired {
		if exists {
			_ = repo.UpsertProfitPriorityState(ctx, stored)
		}
		return "unchanged"
	}
	pending := desired
	stored.PendingPriority = &pending
	if err := repo.UpsertProfitPriorityState(ctx, stored); err != nil {
		return "failed"
	}
	if _, err := s.profitActions.UpdateSub2APIAdminAccountPriority(session, candidate.accountID, desired); err != nil {
		return "failed"
	}
	stored.LastAppliedPriority = desired
	stored.PendingPriority = nil
	cooldownUntil := time.Now().Add(profitPriorityCooldown)
	stored.CooldownUntil = &cooldownUntil
	if err := repo.UpsertProfitPriorityState(ctx, stored); err != nil {
		return "failed"
	}
	return "updated"
}

func (s *Service) restoreProfitPriorityStates(ctx context.Context, repo profitPriorityRepository, session upstream.Session, states []ProfitPriorityState, result ProfitPrioritySummary) ProfitPrioritySummary {
	for _, state := range states {
		remote, err := s.profitActions.GetSub2APIAdminAccountState(session, state.AccountID)
		if err != nil || remote.Priority == nil {
			result.Failed++
			continue
		}
		current := *remote.Priority
		if state.Conflict || (state.PendingPriority == nil && current != state.LastAppliedPriority) ||
			(state.PendingPriority != nil && current != state.LastAppliedPriority && current != *state.PendingPriority) {
			result.Conflict++
			_ = repo.DeleteProfitPriorityState(ctx, state.UserID, state.AdminAccountID, state.AccountID)
			continue
		}
		if current != state.OriginalPriority {
			pending := state.OriginalPriority
			state.PendingPriority = &pending
			if err := repo.UpsertProfitPriorityState(ctx, state); err != nil {
				result.Failed++
				continue
			}
			if _, err := s.profitActions.UpdateSub2APIAdminAccountPriority(session, state.AccountID, state.OriginalPriority); err != nil {
				result.Failed++
				continue
			}
		}
		if err := repo.DeleteProfitPriorityState(ctx, state.UserID, state.AdminAccountID, state.AccountID); err != nil {
			result.Failed++
			continue
		}
		result.Restored++
	}
	return result
}

func (s *Service) startProfitPriorityScheduler(ctx context.Context) {
	go func() {
		s.runProfitPriorityTick(ctx)
		ticker := time.NewTicker(schedulerTickInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.runProfitPriorityTick(ctx)
			}
		}
	}()
}

func (s *Service) runProfitPriorityTick(ctx context.Context) {
	repo, err := groupRateMonitorRepo(s.repo)
	if err != nil {
		return
	}
	settingsList, err := repo.ListEnabledGroupRateMonitorSettings(ctx)
	if err != nil {
		log.Printf("[profit-priority] settings scan failed err=%v", err)
		return
	}
	for _, settings := range settingsList {
		if !settings.ProfitPriorityEnabled {
			continue
		}
		release, err := s.repo.AcquireTargetLease(ctx, "profit-priority:"+settings.AdminAccountID)
		if err != nil {
			continue
		}
		_, reconcileErr := s.reconcileProfitPriorityWorkspace(ctx, settings.UserID, settings.AdminAccountID, true)
		release()
		if reconcileErr != nil {
			log.Printf("[profit-priority] periodic reconcile failed workspace=%s err=%v", settings.AdminAccountID, reconcileErr)
		}
	}
}
