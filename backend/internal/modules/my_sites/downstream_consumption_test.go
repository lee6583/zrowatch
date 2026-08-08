package my_sites

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"transithub/backend/internal/modules/upstream"
)

type downstreamConsumptionConnRepo struct {
	connections   []RealConnection
	listUserID    string
	listWorkspace string
}

type downstreamConsumptionLedgerConnRepo struct {
	*downstreamConsumptionConnRepo
	mu      sync.Mutex
	entries map[string]DownstreamConsumptionLedgerEntry
}

func (r *downstreamConsumptionLedgerConnRepo) ListDownstreamConsumptionLedger(_ context.Context, userID, workspaceID string) ([]DownstreamConsumptionLedgerEntry, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	entries := make([]DownstreamConsumptionLedgerEntry, 0)
	for _, entry := range r.entries {
		if entry.UserID == userID && entry.WorkspaceAdminID == workspaceID {
			entries = append(entries, entry)
		}
	}
	return entries, nil
}

func (r *downstreamConsumptionLedgerConnRepo) ObserveDownstreamConsumption(_ context.Context, entry DownstreamConsumptionLedgerEntry) (float64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := downstreamConsumptionAccountKey(entry.SiteID, entry.AccountID)
	existing, exists := r.entries[key]
	if !exists {
		entry.AccumulatedAmount = entry.ObservedTotal
		r.entries[key] = entry
		return entry.AccumulatedAmount, nil
	}
	if !entry.ObservedAt.Before(existing.ObservedAt) {
		if entry.ObservedTotal >= existing.ObservedTotal {
			existing.AccumulatedAmount += entry.ObservedTotal - existing.ObservedTotal
		}
		existing.ObservedTotal = entry.ObservedTotal
		existing.ObservedAt = entry.ObservedAt
		r.entries[key] = existing
	}
	return existing.AccumulatedAmount, nil
}

func (r *downstreamConsumptionLedgerConnRepo) ListDownstreamConsumptionScopes(context.Context) ([]DownstreamConsumptionScope, error) {
	return []DownstreamConsumptionScope{{UserID: "user-1", WorkspaceAdminID: "workspace-1"}}, nil
}

func (r *downstreamConsumptionConnRepo) SaveRealConnection(context.Context, RealConnection) error {
	return errors.New("not implemented")
}

func (r *downstreamConsumptionConnRepo) ListRealConnections(_ context.Context, userID, adminAccountID string) ([]RealConnection, error) {
	r.listUserID = userID
	r.listWorkspace = adminAccountID
	return append([]RealConnection(nil), r.connections...), nil
}

func (r *downstreamConsumptionConnRepo) GetRealConnection(context.Context, string, string, string) (*RealConnection, error) {
	return nil, errors.New("not implemented")
}

func (r *downstreamConsumptionConnRepo) DeleteRealConnection(context.Context, string, string, string) error {
	return errors.New("not implemented")
}

func TestDownstreamConsumptionAggregatesUniqueAccounts(t *testing.T) {
	type usageQuery struct {
		startDate string
		count     int
	}
	var queryMu sync.Mutex
	queries := make(map[string]usageQuery)
	costs := map[string]float64{
		"101": 12.5,
		"103": 0,
		"105": 3,
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/auth/me":
			writeDownstreamConsumptionJSON(w, http.StatusOK, map[string]any{"data": map[string]any{"role": "admin"}})
		case "/api/v1/admin/usage/stats":
			accountID := r.URL.Query().Get("account_id")
			queryMu.Lock()
			query := queries[accountID]
			query.startDate = r.URL.Query().Get("start_date")
			query.count++
			queries[accountID] = query
			queryMu.Unlock()
			if accountID == "102" || accountID == "106" {
				writeDownstreamConsumptionJSON(w, http.StatusBadGateway, map[string]any{"message": "upstream unavailable"})
				return
			}
			writeDownstreamConsumptionJSON(w, http.StatusOK, map[string]any{"data": map[string]any{"total_actual_cost": costs[accountID]}})
		default:
			writeDownstreamConsumptionJSON(w, http.StatusNotFound, map[string]any{"message": "not found"})
		}
	}))
	defer server.Close()

	repo := &downstreamConsumptionConnRepo{connections: []RealConnection{
		{UpstreamSiteID: "site-a", AdminAccountID: "101", CreatedAt: "2026-07-03T12:00:00Z"},
		{UpstreamSiteID: "site-a", AdminAccountID: "101", CreatedAt: "2026-07-01T12:00:00Z"},
		{UpstreamSiteID: "site-a", AdminAccountID: "102", CreatedAt: "2026-07-02T12:00:00Z"},
		{UpstreamSiteID: "site-b", AdminAccountID: "103", CreatedAt: "2026-07-04T12:00:00Z"},
		{UpstreamSiteID: "site-c", AdminAccountID: "105", CreatedAt: "invalid"},
		{UpstreamSiteID: "site-c", AdminAccountID: "105", CreatedAt: "2026-07-05T12:00:00Z"},
		{UpstreamSiteID: "site-fail", AdminAccountID: "106", CreatedAt: "2026-07-06T12:00:00Z"},
		{UpstreamSiteID: "site-x", AdminAccountID: "104", CreatedAt: "2026-07-07T12:00:00Z"},
		{UpstreamSiteID: "site-y", AdminAccountID: "104", CreatedAt: "2026-07-07T12:00:00Z"},
		{UpstreamSiteID: "site-empty", CreatedAt: "2026-07-08T12:00:00Z"},
	}}
	stateRepo := &testStateRepo{state: &State{
		UserID:         "user-1",
		AdminAccountID: "workspace-1",
		Session: upstream.Session{
			Platform:    upstream.PlatformSub2API,
			BaseURL:     server.URL,
			AccessToken: "test-token",
			TokenType:   "Bearer",
		},
	}}
	service := NewService(stateRepo, upstream.NewPlatformService(upstream.NewHTTPClient(server.Client())), nil)
	service.connRepository = repo
	service.SetAdminAccountResolver(testAdminResolver{currentID: "workspace-1"})

	response, err := service.DownstreamConsumption(context.Background(), "user-1")
	if err != nil {
		t.Fatalf("DownstreamConsumption() error = %v", err)
	}
	if repo.listUserID != "user-1" || repo.listWorkspace != "workspace-1" {
		t.Fatalf("connection scope = %q/%q, want user-1/workspace-1", repo.listUserID, repo.listWorkspace)
	}
	if response.Currency != "CNY" || response.Period != "since_connection" {
		t.Fatalf("response metadata = %q/%q", response.Currency, response.Period)
	}

	items := downstreamConsumptionItemsBySite(response.Items)
	assertDownstreamConsumptionItem(t, items["site-a"], DownstreamConsumptionPartial, 12.5, 2, 1, 1, 0)
	assertDownstreamConsumptionItem(t, items["site-b"], DownstreamConsumptionAvailable, 0, 1, 1, 0, 0)
	assertDownstreamConsumptionItem(t, items["site-c"], DownstreamConsumptionAvailable, 3, 1, 1, 0, 0)
	assertDownstreamConsumptionItemWithoutAmount(t, items["site-fail"], DownstreamConsumptionUnavailable, 1, 0, 1, 0)
	assertDownstreamConsumptionItemWithoutAmount(t, items["site-x"], DownstreamConsumptionPartial, 1, 0, 0, 1)
	assertDownstreamConsumptionItemWithoutAmount(t, items["site-y"], DownstreamConsumptionPartial, 1, 0, 0, 1)
	assertDownstreamConsumptionItemWithoutAmount(t, items["site-empty"], DownstreamConsumptionEmpty, 0, 0, 0, 0)
	if items["site-a"].ErrorKey != upstream.ErrorRequest || items["site-fail"].ErrorKey != upstream.ErrorRequest {
		t.Fatalf("request failure keys = %q/%q, want %q", items["site-a"].ErrorKey, items["site-fail"].ErrorKey, upstream.ErrorRequest)
	}
	if items["site-x"].ErrorKey != downstreamConsumptionAccountConflictError || items["site-y"].ErrorKey != downstreamConsumptionAccountConflictError {
		t.Fatalf("conflict failure keys = %q/%q", items["site-x"].ErrorKey, items["site-y"].ErrorKey)
	}

	queryMu.Lock()
	defer queryMu.Unlock()
	if queries["101"].count != 1 || queries["101"].startDate != "2026-07-01" {
		t.Fatalf("account 101 query = %+v, want one query from 2026-07-01", queries["101"])
	}
	if queries["105"].count != 1 || queries["105"].startDate != "2026-07-05" {
		t.Fatalf("account 105 query = %+v, want valid duplicate date without a failure", queries["105"])
	}
	if queries["104"].count != 0 {
		t.Fatalf("conflicting account 104 queried %d times, want 0", queries["104"].count)
	}
}

func TestDownstreamConsumptionMarksNonSub2APIWorkspaceUnsupported(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/user/self" {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		writeDownstreamConsumptionJSON(w, http.StatusOK, map[string]any{"data": map[string]any{"role": 10}})
	}))
	defer server.Close()

	repo := &downstreamConsumptionConnRepo{connections: []RealConnection{
		{UpstreamSiteID: "site-a", AdminAccountID: "101", CreatedAt: "2026-07-01T12:00:00Z"},
	}}
	stateRepo := &testStateRepo{state: &State{Session: upstream.Session{
		Platform: upstream.PlatformNewAPI,
		BaseURL:  server.URL,
		Cookie:   "session=test",
		UserID:   "1",
	}}}
	service := NewService(stateRepo, upstream.NewPlatformService(upstream.NewHTTPClient(server.Client())), nil)
	service.connRepository = repo
	service.SetAdminAccountResolver(testAdminResolver{currentID: "workspace-1"})

	response, err := service.DownstreamConsumption(context.Background(), "user-1")
	if err != nil {
		t.Fatalf("DownstreamConsumption() error = %v", err)
	}
	item := downstreamConsumptionItemsBySite(response.Items)["site-a"]
	assertDownstreamConsumptionItemWithoutAmount(t, item, DownstreamConsumptionUnsupported, 1, 0, 0, 0)
}

func TestDownstreamConsumptionKeepsLocalTotalWhenUpstreamLogsRollOff(t *testing.T) {
	var mu sync.Mutex
	observedCost := 100.0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/auth/me":
			writeDownstreamConsumptionJSON(w, http.StatusOK, map[string]any{"data": map[string]any{"role": "admin"}})
		case "/api/v1/admin/usage/stats":
			mu.Lock()
			cost := observedCost
			mu.Unlock()
			writeDownstreamConsumptionJSON(w, http.StatusOK, map[string]any{"data": map[string]any{"total_actual_cost": cost}})
		default:
			writeDownstreamConsumptionJSON(w, http.StatusNotFound, map[string]any{"message": "not found"})
		}
	}))
	defer server.Close()

	repo := &downstreamConsumptionLedgerConnRepo{
		downstreamConsumptionConnRepo: &downstreamConsumptionConnRepo{connections: []RealConnection{
			{UpstreamSiteID: "site-a", AdminAccountID: "101", CreatedAt: "2026-08-01T12:00:00Z"},
		}},
		entries: make(map[string]DownstreamConsumptionLedgerEntry),
	}
	stateRepo := &testStateRepo{state: &State{
		UserID:         "user-1",
		AdminAccountID: "workspace-1",
		Session: upstream.Session{
			Platform:    upstream.PlatformSub2API,
			BaseURL:     server.URL,
			AccessToken: "test-token",
			TokenType:   "Bearer",
		},
	}}
	service := NewService(stateRepo, upstream.NewPlatformService(upstream.NewHTTPClient(server.Client())), nil)
	service.connRepository = repo
	service.SetAdminAccountResolver(testAdminResolver{currentID: "workspace-1"})

	readAmount := func() float64 {
		response, err := service.DownstreamConsumption(context.Background(), "user-1")
		if err != nil {
			t.Fatalf("DownstreamConsumption() error = %v", err)
		}
		item := downstreamConsumptionItemsBySite(response.Items)["site-a"]
		if item.Amount == nil {
			t.Fatalf("site-a amount is nil, response = %+v", item)
		}
		return *item.Amount
	}

	if got := readAmount(); got != 100 {
		t.Fatalf("initial amount = %.2f, want 100", got)
	}
	mu.Lock()
	observedCost = 20
	mu.Unlock()
	if got := readAmount(); got != 100 {
		t.Fatalf("retention reset amount = %.2f, want 100", got)
	}
	mu.Lock()
	observedCost = 25
	mu.Unlock()
	if got := readAmount(); got != 105 {
		t.Fatalf("post-reset increment amount = %.2f, want 105", got)
	}
}

func downstreamConsumptionItemsBySite(items []DownstreamConsumptionItem) map[string]DownstreamConsumptionItem {
	result := make(map[string]DownstreamConsumptionItem, len(items))
	for _, item := range items {
		result[item.SiteID] = item
	}
	return result
}

func assertDownstreamConsumptionItem(t *testing.T, item DownstreamConsumptionItem, status DownstreamConsumptionStatus, amount float64, total, successful, failed, conflicts int) {
	t.Helper()
	if item.Amount == nil || *item.Amount != amount {
		t.Fatalf("site %s amount = %v, want %.2f", item.SiteID, item.Amount, amount)
	}
	assertDownstreamConsumptionItemCounts(t, item, status, total, successful, failed, conflicts)
}

func assertDownstreamConsumptionItemWithoutAmount(t *testing.T, item DownstreamConsumptionItem, status DownstreamConsumptionStatus, total, successful, failed, conflicts int) {
	t.Helper()
	if item.Amount != nil {
		t.Fatalf("site %s amount = %v, want nil", item.SiteID, *item.Amount)
	}
	assertDownstreamConsumptionItemCounts(t, item, status, total, successful, failed, conflicts)
}

func assertDownstreamConsumptionItemCounts(t *testing.T, item DownstreamConsumptionItem, status DownstreamConsumptionStatus, total, successful, failed, conflicts int) {
	t.Helper()
	if item.Status != status || item.AccountCount != total || item.SuccessfulAccountCount != successful || item.FailedAccountCount != failed || item.ConflictAccountCount != conflicts {
		t.Fatalf("site %s = %+v, want status=%s total=%d successful=%d failed=%d conflicts=%d", item.SiteID, item, status, total, successful, failed, conflicts)
	}
}

func writeDownstreamConsumptionJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}
