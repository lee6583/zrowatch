package connection_health

import (
	"context"
	"testing"
	"time"

	"transithub/backend/internal/modules/upstream"
)

type memoryProfitPriorityRepo struct {
	states map[string]ProfitPriorityState
}

func (repo *memoryProfitPriorityRepo) ListProfitPriorityStates(_ context.Context, _, _ string) ([]ProfitPriorityState, error) {
	result := make([]ProfitPriorityState, 0, len(repo.states))
	for _, state := range repo.states {
		result = append(result, state)
	}
	return result, nil
}

func (repo *memoryProfitPriorityRepo) UpsertProfitPriorityState(_ context.Context, state ProfitPriorityState) error {
	if repo.states == nil {
		repo.states = make(map[string]ProfitPriorityState)
	}
	repo.states[state.AccountID] = state
	return nil
}

func (repo *memoryProfitPriorityRepo) DeleteProfitPriorityState(_ context.Context, _, _, accountID string) error {
	delete(repo.states, accountID)
	return nil
}

type memoryProfitPriorityActions struct {
	states map[string]upstream.Sub2APIAdminAccountState
}

func (actions *memoryProfitPriorityActions) GetSub2APIAdminAccountState(_ upstream.Session, accountID string) (upstream.Sub2APIAdminAccountState, error) {
	return actions.states[accountID], nil
}

func (actions *memoryProfitPriorityActions) UpdateSub2APIAdminAccountPriority(_ upstream.Session, accountID string, priority int) (upstream.Sub2APIAdminAccountState, error) {
	state := actions.states[accountID]
	state.Priority = &priority
	actions.states[accountID] = state
	return state, nil
}

func (actions *memoryProfitPriorityActions) FetchSub2APIAdminAccountRuntimeSamples(_ upstream.Session, _ string, _ time.Time, _ int) ([]upstream.Sub2APIAccountRuntimeSample, error) {
	return nil, nil
}

func profitCandidate(id string, tier int, latency *int, cost float64, groups ...string) *profitPriorityCandidate {
	groupSet := make(map[string]struct{}, len(groups))
	for _, group := range groups {
		groupSet[group] = struct{}{}
	}
	priority := 10
	schedulable := true
	return &profitPriorityCandidate{
		accountID: id, stabilityRank: tier, latencyMs: latency, effectiveCost: cost, groupIDs: groupSet,
		remote: upstream.Sub2APIAdminAccountState{Priority: &priority, Status: "active", Schedulable: &schedulable},
	}
}

func TestRankProfitPriorityCandidatesPrefersStabilityBeforePrice(t *testing.T) {
	fast := 1000
	slow := 1200
	stableExpensive := profitCandidate("stable", 0, &slow, 0.08, "18")
	warningCheap := profitCandidate("warning", 2, &fast, 0.04, "18")

	ranked := rankProfitPriorityCandidates([]*profitPriorityCandidate{warningCheap, stableExpensive})
	if ranked[0].accountID != "stable" {
		t.Fatalf("stable account should rank first, got %s", ranked[0].accountID)
	}
}

func TestRankProfitPriorityCandidatesPrefersCostWhenLatencyIsSimilar(t *testing.T) {
	latencyA := 3000
	latencyB := 4500 // within the fixed 2 second tolerance
	cheap := profitCandidate("cheap", 0, &latencyB, 0.04, "18")
	expensive := profitCandidate("expensive", 0, &latencyA, 0.08, "18")

	ranked := rankProfitPriorityCandidates([]*profitPriorityCandidate{expensive, cheap})
	if ranked[0].accountID != "cheap" {
		t.Fatalf("cheap account should rank first inside one latency cohort, got %s", ranked[0].accountID)
	}
}

func TestRankProfitPriorityCandidatesPrefersClearlyFasterAccount(t *testing.T) {
	fastLatency := 2000
	slowLatency := 12000
	cheapSlow := profitCandidate("cheap-slow", 0, &slowLatency, 0.04, "18")
	expensiveFast := profitCandidate("expensive-fast", 0, &fastLatency, 0.08, "18")

	ranked := rankProfitPriorityCandidates([]*profitPriorityCandidate{cheapSlow, expensiveFast})
	if ranked[0].accountID != "expensive-fast" {
		t.Fatalf("clearly faster account should rank first, got %s", ranked[0].accountID)
	}
}

func TestProfitPriorityRuntimeAndProbeConsecutiveFailuresAreUnstable(t *testing.T) {
	candidate := &profitPriorityCandidate{}
	candidate.applyRuntimeAndProbe([]upstream.Sub2APIAccountRuntimeSample{{Success: true}, {Success: true}, {Success: true}}, profitProbeStats{
		failures: 2, consecutiveFailures: 2,
	})
	if candidate.stabilityTier != "unstable" || candidate.stabilityRank != 3 {
		t.Fatalf("expected unstable tier, got %s/%d", candidate.stabilityTier, candidate.stabilityRank)
	}
}

func TestProfitPriorityEWMA(t *testing.T) {
	previousRate, currentRate := 1.0, 0.5
	if got := *profitPrioritySmoothFloat(&previousRate, &currentRate); got != 0.85 {
		t.Fatalf("smoothed success rate = %.2f, want 0.85", got)
	}
	previousLatency, currentLatency := 1000, 3000
	if got := *profitPrioritySmoothInt(&previousLatency, &currentLatency); got != 1600 {
		t.Fatalf("smoothed latency = %d, want 1600", got)
	}
}

func TestProfitPriorityDowngradeRequiresTwoAcceptedObservations(t *testing.T) {
	now := time.Now().UTC()
	rate := 0.80
	stored := ProfitPriorityState{StabilityTier: "stable", SuccessRate: &rate, LastAppliedPriority: 10}

	first := profitCandidate("42", 0, nil, 0.04, "18")
	first.sampleCount, first.successRate = 30, &rate
	stored = prepareProfitPriorityState(now, first, stored, true)
	if stored.StabilityTier != "stable" || stored.ObservedStabilityRounds != 1 {
		t.Fatalf("first observation should retain stable tier: %#v", stored)
	}

	second := profitCandidate("42", 0, nil, 0.04, "18")
	second.sampleCount, second.successRate = 30, &rate
	stored = prepareProfitPriorityState(now.Add(time.Minute), second, stored, true)
	if stored.StabilityTier != "warning" {
		t.Fatalf("second observation should confirm warning tier: %#v", stored)
	}
}

func TestProfitPriorityStableRecoveryRequiresThreeAcceptedObservations(t *testing.T) {
	now := time.Now().UTC()
	rate := 1.0
	stored := ProfitPriorityState{StabilityTier: "unstable", SuccessRate: &rate, LastAppliedPriority: 10}
	for round := 1; round <= 3; round++ {
		candidate := profitCandidate("42", 3, nil, 0.04, "18")
		candidate.sampleCount, candidate.successRate = 30, &rate
		stored = prepareProfitPriorityState(now.Add(time.Duration(round-1)*time.Minute), candidate, stored, true)
		if round < 3 && stored.StabilityTier != "unstable" {
			t.Fatalf("round %d recovered too early: %#v", round, stored)
		}
	}
	if stored.StabilityTier != "stable" {
		t.Fatalf("third observation should confirm stable recovery: %#v", stored)
	}
}

func TestProfitPriorityInsufficientSamplesRetainConfirmedTier(t *testing.T) {
	rate := 0.70
	stored := ProfitPriorityState{StabilityTier: "stable", SuccessRate: &rate, LastAppliedPriority: 10}
	candidate := profitCandidate("42", 0, nil, 0.04, "18")
	candidate.sampleCount, candidate.successRate = 20, &rate
	stored = prepareProfitPriorityState(time.Now().UTC(), candidate, stored, true)
	if stored.StabilityTier != "stable" || stored.ObservedStabilityRounds != 0 {
		t.Fatalf("insufficient samples changed confirmed tier: %#v", stored)
	}
}

func TestProfitPriorityObservationDoesNotAdvanceTwiceWithinMinute(t *testing.T) {
	now := time.Now().UTC()
	rate := 0.80
	stored := ProfitPriorityState{StabilityTier: "stable", SuccessRate: &rate, LastAppliedPriority: 10}
	first := profitCandidate("42", 0, nil, 0.04, "18")
	first.sampleCount, first.successRate = 30, &rate
	stored = prepareProfitPriorityState(now, first, stored, true)
	second := profitCandidate("42", 0, nil, 0.04, "18")
	second.sampleCount, second.successRate = 30, &rate
	stored = prepareProfitPriorityState(now.Add(30*time.Second), second, stored, true)
	if second.observed || stored.ObservedStabilityRounds != 1 || stored.StabilityTier != "stable" {
		t.Fatalf("duplicate observation advanced state: %#v", stored)
	}
}

func TestProfitPriorityComponentRequiresConfirmedRankAndCooldown(t *testing.T) {
	now := time.Now().UTC()
	first := profitCandidate("a", 0, nil, 0.04, "18")
	second := profitCandidate("b", 0, nil, 0.08, "18")
	firstPriority, secondPriority := 20, 10
	first.remote.Priority, second.remote.Priority = &firstPriority, &secondPriority
	ranked := []*profitPriorityCandidate{first, second}
	states := map[string]ProfitPriorityState{
		"a": {ObservedRank: 1, ObservedRankRounds: 2},
		"b": {ObservedRank: 2, ObservedRankRounds: 2},
	}
	if profitPriorityComponentReady(now, ranked, states, 10) {
		t.Fatal("two rank observations should not allow a reorder")
	}
	states["a"] = ProfitPriorityState{ObservedRank: 1, ObservedRankRounds: 3}
	states["b"] = ProfitPriorityState{ObservedRank: 2, ObservedRankRounds: 3}
	if !profitPriorityComponentReady(now, ranked, states, 10) {
		t.Fatal("three rank observations should allow a reorder")
	}
	cooldown := now.Add(time.Minute)
	state := states["a"]
	state.CooldownUntil = &cooldown
	states["a"] = state
	if profitPriorityComponentReady(now, ranked, states, 10) {
		t.Fatal("cooldown should block an ordinary reorder")
	}
	first.emergency = true
	if !profitPriorityComponentReady(now, ranked, states, 10) {
		t.Fatal("emergency degradation should bypass cooldown")
	}
	first.emergency, first.costChanged = false, true
	if !profitPriorityComponentReady(now, ranked, states, 10) {
		t.Fatal("cost change should bypass cooldown")
	}
}

func TestProfitPriorityHardFailureIsImmediateOnlyOnTransition(t *testing.T) {
	now := time.Now().UTC()
	stored := ProfitPriorityState{StabilityTier: "stable", LastAppliedPriority: 10}
	candidate := profitCandidate("42", 0, nil, 0.04, "18")
	candidate.hardFailure = true
	stored = prepareProfitPriorityState(now, candidate, stored, true)
	if stored.StabilityTier != "unstable" || !candidate.emergency {
		t.Fatalf("hard failure should immediately enter unstable tier: %#v", stored)
	}
	next := profitCandidate("42", 3, nil, 0.04, "18")
	next.hardFailure = true
	_ = prepareProfitPriorityState(now.Add(30*time.Second), next, stored, true)
	if next.emergency {
		t.Fatal("an already unstable account should not bypass cooldown every tick")
	}
}

func TestProfitPriorityUsesSparsePrioritySpacing(t *testing.T) {
	base := 3
	got := []int{base, base + profitPrioritySpacing, base + 2*profitPrioritySpacing}
	want := []int{3, 13, 23}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("priority %d = %d, want %d", index, got[index], want[index])
		}
	}
}

func TestProfitPriorityComponentsOnlyMergeSharedGroups(t *testing.T) {
	candidates := map[string]*profitPriorityCandidate{
		"a": profitCandidate("a", 0, nil, 0.04, "18"),
		"b": profitCandidate("b", 0, nil, 0.08, "18", "20"),
		"c": profitCandidate("c", 0, nil, 0.06, "20"),
		"d": profitCandidate("d", 0, nil, 0.02, "99"),
	}
	components := profitPriorityComponents(candidates)
	sizes := make([]int, 0, len(components))
	for _, component := range components {
		sizes = append(sizes, len(component))
	}
	if len(sizes) != 2 || !((sizes[0] == 3 && sizes[1] == 1) || (sizes[0] == 1 && sizes[1] == 3)) {
		t.Fatalf("unexpected component sizes: %#v", sizes)
	}
}

func TestApplyAndRestoreProfitPriorityPreservesOriginalPriority(t *testing.T) {
	original := 20
	schedulable := true
	actions := &memoryProfitPriorityActions{states: map[string]upstream.Sub2APIAdminAccountState{
		"42": {ID: "42", Status: "active", Schedulable: &schedulable, Priority: &original},
	}}
	repo := &memoryProfitPriorityRepo{states: map[string]ProfitPriorityState{}}
	service := &Service{profitActions: actions}
	candidate := profitCandidate("42", 0, nil, 0.04, "18")
	candidate.connection.UserID = "user-1"
	candidate.connection.WorkspaceAdminAccountID = "workspace-1"
	candidate.remote = actions.states["42"]

	if status := service.applyProfitPriority(context.Background(), repo, upstream.Session{}, candidate, 10, ProfitPriorityState{}, false); status != "updated" {
		t.Fatalf("apply status = %s, want updated", status)
	}
	if got := *actions.states["42"].Priority; got != 10 {
		t.Fatalf("remote priority = %d, want 10", got)
	}
	stored := repo.states["42"]
	if stored.OriginalPriority != 20 || stored.LastAppliedPriority != 10 || stored.PendingPriority != nil {
		t.Fatalf("unexpected stored state: %#v", stored)
	}

	summary := service.restoreProfitPriorityStates(context.Background(), repo, upstream.Session{}, []ProfitPriorityState{stored}, ProfitPrioritySummary{})
	if summary.Restored != 1 || *actions.states["42"].Priority != 20 {
		t.Fatalf("restore summary/state = %#v / %#v", summary, actions.states["42"])
	}
	if _, exists := repo.states["42"]; exists {
		t.Fatal("state should be deleted after restore")
	}
}

func TestApplyProfitPriorityStopsAfterManualOverride(t *testing.T) {
	manual := 7
	candidate := profitCandidate("42", 0, nil, 0.04, "18")
	candidate.remote.Priority = &manual
	repo := &memoryProfitPriorityRepo{states: map[string]ProfitPriorityState{}}
	service := &Service{}
	stored := ProfitPriorityState{UserID: "user-1", AdminAccountID: "workspace-1", AccountID: "42", OriginalPriority: 20, LastAppliedPriority: 10}

	if status := service.applyProfitPriority(context.Background(), repo, upstream.Session{}, candidate, 5, stored, true); status != "conflict" {
		t.Fatalf("apply status = %s, want conflict", status)
	}
	if !repo.states["42"].Conflict {
		t.Fatal("manual override should persist a conflict marker")
	}
}
