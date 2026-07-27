package upstream

import "testing"

func TestSitePayloadPreservesSettingsAndBalancePause(t *testing.T) {
	threshold := 12.5
	pausedAt := int64(123456)
	site := &Site{
		ID: "site-1", UserID: "user-1", AdminAccountID: "workspace-1", Status: StatusDisabled,
		BalanceSuspended: true, BalanceSuspendedAt: &pausedAt, BalancePauseReason: "balance_below_threshold",
		Settings: SiteSettings{BalanceThreshold: &threshold},
	}

	restored := fromPayload(toPayload(site))
	if restored.Settings.BalanceThreshold == nil || *restored.Settings.BalanceThreshold != threshold {
		t.Fatalf("expected balance threshold preserved, got %#v", restored.Settings)
	}
	if !restored.BalanceSuspended || restored.BalanceSuspendedAt == nil || *restored.BalanceSuspendedAt != pausedAt || restored.BalancePauseReason != "balance_below_threshold" || restored.Status != StatusDisabled {
		t.Fatalf("expected balance pause state preserved, got %#v", restored)
	}
}
