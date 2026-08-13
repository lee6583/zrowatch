package upstream

import (
	"context"
	"testing"
)

type importTestRepository struct {
	sites []Site
	saved []Site
}

func (r *importTestRepository) ListSites(context.Context) ([]Site, error) { return r.sites, nil }
func (r *importTestRepository) ListSitesForUser(_ context.Context, userID string) ([]Site, error) {
	result := make([]Site, 0, len(r.sites))
	for _, site := range r.sites {
		if site.UserID == userID {
			result = append(result, site)
		}
	}
	return result, nil
}
func (r *importTestRepository) ListImportCandidates(context.Context, string, string) ([]ImportCandidate, error) {
	return nil, nil
}
func (r *importTestRepository) SaveSite(_ context.Context, site Site) error {
	r.saved = append(r.saved, site)
	return nil
}
func (r *importTestRepository) DeleteSite(context.Context, string, string) error { return nil }

func TestImportSitesCopiesConfigurationAndResetsRuntimeState(t *testing.T) {
	lastSync := int64(123456)
	suspendAt := int64(123000)
	source := Site{
		ID:                 "source-1",
		UserID:             "user-1",
		AdminAccountID:     "workspace-source",
		Name:               "Shared upstream",
		BaseURL:            "https://upstream.example.com/",
		Platform:           PlatformSub2API,
		RequestedPlatform:  PlatformAuto,
		Account:            "admin@example.com",
		Remark:             "shared remark",
		RechargeRate:       7.2,
		Status:             StatusDisabled,
		BalanceSuspended:   true,
		BalanceSuspendedAt: &suspendAt,
		BalancePauseReason: "low_balance",
		Metrics: Metrics{
			Balance: MetricValue{Display: "100"},
			Groups:  []GroupInfo{{ID: "vip", Name: "vip"}},
		},
		Settings:     SiteSettings{BalanceThreshold: float64Pointer(5)},
		LastSyncedAt: &lastSync,
		Session:      &Session{Platform: PlatformSub2API, BaseURL: "https://upstream.example.com", AccessToken: "secret"},
	}
	repository := &importTestRepository{sites: []Site{source}}
	cache := newFakeSiteCache()
	service := NewService(nil, repository, nil, cache)
	service.SetAdminAccountResolver(&fakeAccountResolver{current: map[string]string{"user-1": "workspace-target"}})

	result, err := service.ImportSites(context.Background(), "user-1", ImportRequest{SourceSiteIDs: []string{"source-1"}})
	if err != nil {
		t.Fatalf("ImportSites returned error: %v", err)
	}
	if len(result.Imported) != 1 || len(repository.saved) != 1 {
		t.Fatalf("expected one imported site, got result=%+v saved=%d", result, len(repository.saved))
	}
	imported := repository.saved[0]
	if imported.ID == source.ID || imported.AdminAccountID != "workspace-target" {
		t.Fatalf("imported site must have a new ID and target workspace: %+v", imported)
	}
	if imported.Session == nil || imported.Session.AccessToken != "secret" || imported.Session == source.Session {
		t.Fatalf("session was not copied independently: %+v", imported.Session)
	}
	if imported.Status != StatusConnected || imported.BalanceSuspended || imported.LastSyncedAt != nil || imported.ErrorKey != nil {
		t.Fatalf("runtime state was not reset: %+v", imported)
	}
	if len(imported.Metrics.Groups) != 0 || imported.Metrics.Balance.Value != nil {
		t.Fatalf("runtime metrics/groups must not be imported: %+v", imported.Metrics)
	}
	if imported.Settings.BalanceThreshold == nil || *imported.Settings.BalanceThreshold != 5 {
		t.Fatalf("site configuration was not copied: %+v", imported.Settings)
	}
}

func TestImportSitesSkipsCurrentWorkspaceDuplicateAndForeignSource(t *testing.T) {
	repository := &importTestRepository{sites: []Site{
		{
			ID: "source-duplicate", UserID: "user-1", AdminAccountID: "workspace-source",
			BaseURL: "https://same.example.com", Platform: PlatformSub2API, Session: &Session{Platform: PlatformSub2API, AccessToken: "secret"},
		},
		{ID: "existing", UserID: "user-1", AdminAccountID: "workspace-target", BaseURL: "https://same.example.com/", Platform: PlatformSub2API},
		{ID: "current", UserID: "user-1", AdminAccountID: "workspace-target", BaseURL: "https://current.example.com"},
		{ID: "foreign", UserID: "user-2", AdminAccountID: "workspace-other", BaseURL: "https://foreign.example.com"},
	}}
	service := NewService(nil, repository, nil, newFakeSiteCache())
	service.SetAdminAccountResolver(&fakeAccountResolver{current: map[string]string{"user-1": "workspace-target"}})

	result, err := service.ImportSites(context.Background(), "user-1", ImportRequest{SourceSiteIDs: []string{"source-duplicate", "current", "foreign"}})
	if err != nil {
		t.Fatalf("ImportSites returned error: %v", err)
	}
	if len(result.Imported) != 0 || len(repository.saved) != 0 || len(result.Skipped) != 3 {
		t.Fatalf("unexpected import result: %+v saved=%d", result, len(repository.saved))
	}
	if result.Skipped[0].Reason != "already_imported" || result.Skipped[1].Reason != "not_found" || result.Skipped[2].Reason != "not_found" {
		t.Fatalf("unexpected skip reasons: %+v", result.Skipped)
	}
}

func float64Pointer(value float64) *float64 { return &value }
