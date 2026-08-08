package upstream

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"
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

func TestFetchSub2APIAdminAccountRuntimeSamples(t *testing.T) {
	now := time.Now().UTC()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/admin/usage":
			if r.URL.Query().Get("account_id") != "42" {
				t.Fatalf("account_id = %q", r.URL.Query().Get("account_id"))
			}
			writeJSON(w, map[string]any{"data": map[string]any{"items": []map[string]any{
				{"created_at": now.Format(time.RFC3339), "first_token_ms": 1250, "output_tokens": 1},
				{"created_at": now.Add(-time.Minute).Format(time.RFC3339), "first_token_ms": 800, "output_tokens": 1},
				{"created_at": now.Add(-48 * time.Hour).Format(time.RFC3339), "first_token_ms": 100, "output_tokens": 1},
			}}})
		case "/api/v1/admin/ops/upstream-errors":
			if r.URL.Query().Get("account_id") != "42" || r.URL.Query().Get("start_time") == "" {
				t.Fatalf("unexpected upstream error filters: %s", r.URL.RawQuery)
			}
			writeJSON(w, map[string]any{"data": map[string]any{"items": []map[string]any{
				{"created_at": now.Add(-30 * time.Second).Format(time.RFC3339), "status_code": 429, "phase": "upstream", "error_owner": "provider"},
				{"created_at": now.Add(-2 * time.Minute).Format(time.RFC3339), "status_code": 500, "phase": "upstream", "error_owner": "provider"},
				{"created_at": now.Add(-3 * time.Minute).Format(time.RFC3339), "status_code": 400, "phase": "upstream", "error_owner": "provider"},
			}}})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer server.Close()

	service := NewPlatformService(NewHTTPClient(server.Client()))
	session := Session{Platform: PlatformSub2API, BaseURL: server.URL, AccessToken: "test-token"}
	samples, err := service.FetchSub2APIAdminAccountRuntimeSamples(session, "42", now.Add(-24*time.Hour), 20)
	if err != nil {
		t.Fatalf("FetchSub2APIAdminAccountRuntimeSamples() error = %v", err)
	}
	if len(samples) != 4 || samples[0].LatencyMs == nil || *samples[0].LatencyMs != 1250 || samples[1].Success ||
		samples[1].FailureClass != "rate_limit" || samples[1].StatusCode == nil || *samples[1].StatusCode != 429 ||
		samples[3].Success || samples[3].FailureClass != "server" {
		t.Fatalf("unexpected runtime samples: %#v", samples)
	}
}

func TestFetchSub2APIAdminAccountRuntimeSamplesRequiresOpsMonitoring(t *testing.T) {
	now := time.Now().UTC()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/admin/ops/upstream-errors" {
			http.Error(w, "monitoring disabled", http.StatusServiceUnavailable)
			return
		}
		writeJSON(w, map[string]any{"data": map[string]any{"items": []map[string]any{
			{"created_at": now.Format(time.RFC3339), "first_token_ms": 900, "output_tokens": 1},
		}}})
	}))
	defer server.Close()

	service := NewPlatformService(NewHTTPClient(server.Client()))
	session := Session{Platform: PlatformSub2API, BaseURL: server.URL, AccessToken: "test-token"}
	samples, err := service.FetchSub2APIAdminAccountRuntimeSamples(session, "42", now.Add(-24*time.Hour), 20)
	if err == nil || len(samples) != 0 {
		t.Fatalf("missing upstream errors must keep runtime data incomplete, samples=%#v err=%v", samples, err)
	}
}

func TestSub2APIUpstreamFailureClass(t *testing.T) {
	tests := []struct {
		name   string
		record map[string]any
		want   string
	}{
		{name: "auth", record: map[string]any{"upstream_status_code": float64(401)}, want: "auth"},
		{name: "rate limit", record: map[string]any{"status_code": float64(429)}, want: "rate_limit"},
		{name: "server", record: map[string]any{"status_code": float64(502)}, want: "server"},
		{name: "transport", record: map[string]any{"error_type": "upstream_transport_error"}, want: "transport"},
		{name: "client", record: map[string]any{"status_code": float64(400), "phase": "client"}, want: ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := sub2APIUpstreamFailureClass(test.record); got != test.want {
				t.Fatalf("failure class = %q, want %q", got, test.want)
			}
		})
	}
}

func TestFetchSub2APIAdminAccountRuntimeSamplesPaginatesAndCapsNewest200(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	usagePages := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/admin/usage":
			usagePages++
			page, _ := strconv.Atoi(r.URL.Query().Get("page"))
			items := make([]map[string]any, 0, 100)
			for index := 0; index < 100; index++ {
				offset := (page-1)*100 + index
				items = append(items, map[string]any{
					"created_at":     now.Add(-time.Duration(offset+1) * time.Second).Format(time.RFC3339),
					"first_token_ms": 500 + offset,
					"output_tokens":  1,
				})
			}
			writeJSON(w, map[string]any{"data": map[string]any{"items": items}})
		case "/api/v1/admin/ops/upstream-errors":
			if r.URL.Query().Get("page_size") != "200" {
				t.Fatalf("error page_size = %q, want 200", r.URL.Query().Get("page_size"))
			}
			writeJSON(w, map[string]any{"data": map[string]any{"items": []map[string]any{
				{"created_at": now.Format(time.RFC3339), "status_code": 500, "phase": "upstream"},
			}}})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer server.Close()

	service := NewPlatformService(NewHTTPClient(server.Client()))
	session := Session{Platform: PlatformSub2API, BaseURL: server.URL, AccessToken: "test-token"}
	samples, err := service.FetchSub2APIAdminAccountRuntimeSamples(session, "42", now.Add(-10*time.Minute), 200)
	if err != nil {
		t.Fatalf("FetchSub2APIAdminAccountRuntimeSamples() error = %v", err)
	}
	if usagePages != 2 || len(samples) != 200 {
		t.Fatalf("usage pages/samples = %d/%d, want 2/200", usagePages, len(samples))
	}
	if samples[0].Success || samples[0].CreatedAt != now {
		t.Fatalf("newest merged sample = %#v", samples[0])
	}
	for index := 1; index < len(samples); index++ {
		if samples[index].CreatedAt.After(samples[index-1].CreatedAt) {
			t.Fatalf("samples are not newest-first at index %d", index)
		}
	}
}

func TestUpdateSub2APIAdminAccountPrioritySendsPartialPayload(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut || r.URL.Path != "/api/v1/admin/accounts/42" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if len(body) != 1 || body["priority"] != float64(3) {
			t.Fatalf("priority update must be partial, got %#v", body)
		}
		writeJSON(w, map[string]any{"data": map[string]any{"id": 42, "name": "account", "priority": 3}})
	}))
	defer server.Close()

	service := NewPlatformService(NewHTTPClient(server.Client()))
	session := Session{Platform: PlatformSub2API, BaseURL: server.URL, AccessToken: "test-token"}
	state, err := service.UpdateSub2APIAdminAccountPriority(session, "42", 3)
	if err != nil {
		t.Fatalf("UpdateSub2APIAdminAccountPriority() error = %v", err)
	}
	if state.Priority == nil || *state.Priority != 3 {
		t.Fatalf("updated priority = %#v", state.Priority)
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
