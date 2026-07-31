package upstream

import "testing"

func TestPreserveConfirmedSub2APIGroupRates(t *testing.T) {
	confirmedRate := 0.08
	confirmedDefault := 0.10
	failedDefault := 0.12
	previous := Metrics{Groups: []GroupInfo{{
		ID: "1", Name: "vip", Multiplier: &confirmedRate,
		MultiplierDisplay: "0.08x", DefaultMultiplier: &confirmedDefault,
		DedicatedMultiplier: &confirmedRate, DedicatedMultiplierDisplay: "0.08x",
		HasDedicatedMultiplier: true,
	}}}
	current := Metrics{Groups: []GroupInfo{{
		ID: "1", Name: "vip", Multiplier: &failedDefault,
		MultiplierDisplay: "0.12x", DefaultMultiplier: &failedDefault,
		EffectiveMultiplierUnverified: true,
	}}}

	got := preserveConfirmedSub2APIGroupRates(previous, current)
	group := got.Groups[0]
	if group.Multiplier == nil || *group.Multiplier != confirmedRate {
		t.Fatalf("effective multiplier = %v, want last confirmed %.2f", group.Multiplier, confirmedRate)
	}
	if !group.HasDedicatedMultiplier || group.DedicatedMultiplier == nil || *group.DedicatedMultiplier != confirmedRate {
		t.Fatalf("dedicated multiplier metadata was not preserved: %+v", group)
	}
	if group.DefaultMultiplier == nil || *group.DefaultMultiplier != failedDefault {
		t.Fatalf("latest public default = %v, want %.2f", group.DefaultMultiplier, failedDefault)
	}
	if !group.EffectiveMultiplierUnverified {
		t.Fatal("unverified marker should remain set")
	}
}

func TestPreserveConfirmedSub2APIGroupRatesClearsUnverifiedFirstObservation(t *testing.T) {
	publicRate := 0.12
	current := Metrics{Groups: []GroupInfo{{
		ID: "1", Name: "vip", Multiplier: &publicRate,
		MultiplierDisplay: "0.12x", DefaultMultiplier: &publicRate,
		EffectiveMultiplierUnverified: true,
	}}}

	got := preserveConfirmedSub2APIGroupRates(Metrics{}, current)
	if got.Groups[0].Multiplier != nil {
		t.Fatalf("unverified first observation should not expose an effective multiplier: %v", *got.Groups[0].Multiplier)
	}
	if got.Groups[0].DefaultMultiplier == nil || *got.Groups[0].DefaultMultiplier != publicRate {
		t.Fatal("public default should remain available as informational metadata")
	}
}

func TestPreserveConfirmedSub2APIGroupRatesAllowsVerifiedChanges(t *testing.T) {
	oldRate := 0.08
	newRate := 0.06
	previous := Metrics{Groups: []GroupInfo{{
		ID: "1", Name: "vip", Multiplier: &oldRate,
		EffectiveMultiplierUnverified: true,
	}}}
	current := Metrics{Groups: []GroupInfo{{ID: "1", Name: "vip", Multiplier: &newRate}}}

	got := preserveConfirmedSub2APIGroupRates(previous, current)
	if got.Groups[0].Multiplier == nil || *got.Groups[0].Multiplier != newRate {
		t.Fatalf("verified change = %v, want %.2f", got.Groups[0].Multiplier, newRate)
	}
}
