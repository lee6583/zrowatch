package upstream

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestFetchSub2APIAdminAccountUsageStats(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/admin/usage/stats" {
			t.Errorf("path = %s, want usage stats endpoint", r.URL.Path)
			http.NotFound(w, r)
			return
		}
		query := r.URL.Query()
		if query.Get("start_date") != "2026-07-01" {
			t.Errorf("start_date = %q, want 2026-07-01", query.Get("start_date"))
		}
		if query.Get("end_date") != "2026-07-28" {
			t.Errorf("end_date = %q, want 2026-07-28", query.Get("end_date"))
		}
		if query.Get("account_id") != "42" {
			t.Errorf("account_id = %q, want 42", query.Get("account_id"))
		}
		if query.Get("timezone") != "Asia/Shanghai" {
			t.Errorf("timezone = %q, want Asia/Shanghai", query.Get("timezone"))
		}
		if query.Get("nocache") != "1" {
			t.Errorf("nocache = %q, want 1", query.Get("nocache"))
		}
		if got := r.Header.Get("Authorization"); got != "Bearer test-token" {
			t.Errorf("Authorization = %q, want Bearer test-token", got)
		}
		writeJSON(w, map[string]any{"data": map[string]any{"total_actual_cost": 128.56}})
	}))
	defer server.Close()

	service := NewPlatformService(NewHTTPClient(server.Client()))
	session := Session{
		Platform:    PlatformSub2API,
		BaseURL:     server.URL,
		AccessToken: "test-token",
		TokenType:   "Bearer",
	}
	amount, err := service.FetchSub2APIAdminAccountUsageStats(session, "42", "2026-07-01", "2026-07-28")
	if err != nil {
		t.Fatalf("FetchSub2APIAdminAccountUsageStats() error = %v", err)
	}
	if amount != 128.56 {
		t.Fatalf("amount = %.2f, want 128.56", amount)
	}
}

func TestFetchSub2APIAdminAccountUsageStatsRequiresActualCostField(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{"data": map[string]any{"request_count": 3}})
	}))
	defer server.Close()

	service := NewPlatformService(NewHTTPClient(server.Client()))
	session := Session{Platform: PlatformSub2API, BaseURL: server.URL, AccessToken: "test-token"}
	if _, err := service.FetchSub2APIAdminAccountUsageStats(session, "42", "2026-07-01", "2026-07-28"); err == nil {
		t.Fatal("expected a missing total_actual_cost field to be reported as unavailable")
	}
}
