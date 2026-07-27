package my_sites

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"transithub/backend/internal/modules/upstream"
)

type renameConnectionRepo struct {
	connections []RealConnection
}

func (r *renameConnectionRepo) SaveRealConnection(context.Context, RealConnection) error { return nil }
func (r *renameConnectionRepo) ListRealConnections(context.Context, string, string) ([]RealConnection, error) {
	return append([]RealConnection(nil), r.connections...), nil
}
func (r *renameConnectionRepo) GetRealConnection(context.Context, string, string, string) (*RealConnection, error) {
	return nil, nil
}
func (r *renameConnectionRepo) DeleteRealConnection(context.Context, string, string, string) error {
	return nil
}
func (r *renameConnectionRepo) UpdateRealConnectionAdminAccountName(_ context.Context, id string, _ string, _ string, name string) error {
	for index := range r.connections {
		if r.connections[index].ID == id {
			r.connections[index].AdminAccountName = name
		}
	}
	return nil
}

func TestReplaceTrailingMultiplier(t *testing.T) {
	tests := []struct {
		name       string
		input      string
		multiplier float64
		want       string
		ok         bool
	}{
		{name: "replace", input: "A-【王中王】-0.08x", multiplier: 0.06, want: "A-【王中王】-0.06x", ok: true},
		{name: "preserve prefix", input: "A-【王中王】-0.0800x", multiplier: 1.2, want: "A-【王中王】-1.2x", ok: true},
		{name: "malformed", input: "A-【王中王】", multiplier: 0.06, ok: false},
		{name: "invalid number", input: "A-【王中王】-..8x", multiplier: 0.06, ok: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := replaceTrailingMultiplier(tt.input, tt.multiplier)
			if ok != tt.ok || got != tt.want {
				t.Fatalf("replaceTrailingMultiplier(%q, %v) = %q, %v; want %q, %v", tt.input, tt.multiplier, got, ok, tt.want, tt.ok)
			}
		})
	}
}

func TestSyncSub2APIAccountNamesAfterRateChangesUpdatesAllBoundAccounts(t *testing.T) {
	var putNames []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			id := r.URL.Path[len("/api/v1/admin/accounts/"):]
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"data":{"name":"A-【王中王】-0.08x","type":"openai","status":"active","group_ids":[50]}}`))
			if id == "missing" {
				w.WriteHeader(http.StatusNotFound)
			}
			return
		}
		if r.Method == http.MethodPut {
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode PUT body: %v", err)
			}
			putNames = append(putNames, body["name"].(string))
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"data":{"id":"ok"}}`))
			return
		}
		t.Fatalf("unexpected method %s", r.Method)
	}))
	defer server.Close()

	stateRepo := &testStateRepo{state: &State{
		UserID:         "user-1",
		AdminAccountID: "workspace-1",
		Session:        upstream.Session{Platform: upstream.PlatformSub2API, BaseURL: server.URL, AccessToken: "token"},
	}}
	connRepo := &renameConnectionRepo{connections: []RealConnection{
		{UserID: "user-1", WorkspaceAdminAccountID: "workspace-1", UpstreamSiteID: "site-1", UpstreamGroupID: "group-1", UpstreamGroupName: "王中王", AdminAccountID: "acc-1", AdminAccountName: "A-【王中王】-0.08x", AdminPlatform: string(upstream.PlatformSub2API), Status: ConnectionStatusActive},
		{UserID: "user-1", WorkspaceAdminAccountID: "workspace-1", UpstreamSiteID: "site-1", UpstreamGroupID: "group-1", UpstreamGroupName: "王中王", AdminAccountID: "acc-2", AdminAccountName: "A-【王中王】-0.08x", AdminPlatform: string(upstream.PlatformSub2API), Status: ConnectionStatusActive},
		{UserID: "user-1", WorkspaceAdminAccountID: "workspace-1", UpstreamSiteID: "site-other", UpstreamGroupID: "group-1", UpstreamGroupName: "王中王", AdminAccountID: "acc-other", AdminAccountName: "A-【王中王】-0.08x", AdminPlatform: string(upstream.PlatformSub2API), Status: ConnectionStatusActive},
	}}
	service := NewService(stateRepo, upstream.NewPlatformService(upstream.NewHTTPClient(server.Client())), nil)
	service.connRepository = connRepo

	oldMetrics := upstream.Metrics{Groups: []upstream.GroupInfo{{ID: "group-1", Name: "王中王", Multiplier: pointerFloat64(0.08)}}}
	newMetrics := upstream.Metrics{Groups: []upstream.GroupInfo{{ID: "group-1", Name: "王中王", Multiplier: pointerFloat64(0.06)}}}
	results := service.SyncSub2APIAccountNamesAfterRateChanges(context.Background(), "user-1", "workspace-1", "site-1", "王中王", oldMetrics, newMetrics)

	if len(putNames) != 2 || putNames[0] != "A-【王中王】-0.06x" || putNames[1] != "A-【王中王】-0.06x" {
		t.Fatalf("expected both bound accounts renamed, got %#v", putNames)
	}
	if len(results) != 2 || results[0].Status != "updated" || results[1].Status != "updated" {
		t.Fatalf("unexpected rename results: %#v", results)
	}
}

func TestSyncSub2APIAccountNamesAfterRateChangesDoesNotPutWhenUnchangedOrMalformed(t *testing.T) {
	putCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/v1/admin/accounts/") {
			name := "A-【王中王】-0.06x"
			if strings.HasSuffix(r.URL.Path, "/acc-2") {
				name = "A-【malformed】"
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{
				"name": name, "type": "openai", "status": "active", "group_ids": []int{50},
			}})
			return
		}
		if r.Method == http.MethodPut {
			putCalls++
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":{"name":"A-【王中王】-0.06x","type":"openai","status":"active","group_ids":[50]}}`))
	}))
	defer server.Close()

	stateRepo := &testStateRepo{state: &State{Session: upstream.Session{Platform: upstream.PlatformSub2API, BaseURL: server.URL, AccessToken: "token"}}}
	connRepo := &renameConnectionRepo{connections: []RealConnection{
		{UpstreamSiteID: "site-1", UpstreamGroupName: "unchanged", AdminAccountID: "acc-1", AdminAccountName: "A-【unchanged】-0.06x", AdminPlatform: string(upstream.PlatformSub2API), Status: ConnectionStatusActive},
		{UpstreamSiteID: "site-1", UpstreamGroupName: "malformed", AdminAccountID: "acc-2", AdminAccountName: "A-【malformed】", AdminPlatform: string(upstream.PlatformSub2API), Status: ConnectionStatusActive},
	}}
	service := NewService(stateRepo, upstream.NewPlatformService(upstream.NewHTTPClient(server.Client())), nil)
	service.connRepository = connRepo
	results := service.SyncSub2APIAccountNamesAfterRateChanges(context.Background(), "user-1", "workspace-1", "site-1", "王中王",
		upstream.Metrics{Groups: []upstream.GroupInfo{{ID: "1", Name: "unchanged", Multiplier: pointerFloat64(0.08)}, {ID: "2", Name: "malformed", Multiplier: pointerFloat64(0.08)}}},
		upstream.Metrics{Groups: []upstream.GroupInfo{{ID: "1", Name: "unchanged", Multiplier: pointerFloat64(0.06)}, {ID: "2", Name: "malformed", Multiplier: pointerFloat64(0.06)}}})

	if putCalls != 0 {
		t.Fatalf("expected no PUT for unchanged/malformed names, got %d", putCalls)
	}
	if len(results) != 2 || results[0].Status != "unchanged" || results[1].Status != "skipped" {
		t.Fatalf("unexpected results: %#v", results)
	}
}

func TestSyncSub2APIAccountNamesAfterRateChangesUsesRemoteNameAndCalibratesBinding(t *testing.T) {
	putCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPut {
			putCalls++
			_, _ = w.Write([]byte(`{"data":{"id":"acc-1"}}`))
			return
		}
		_, _ = w.Write([]byte(`{"data":{"name":"A-【王中王】-0.06x","type":"openai","status":"active","group_ids":[50]}}`))
	}))
	defer server.Close()

	stateRepo := &testStateRepo{state: &State{Session: upstream.Session{Platform: upstream.PlatformSub2API, BaseURL: server.URL, AccessToken: "token"}}}
	connRepo := &renameConnectionRepo{connections: []RealConnection{{
		ID: "conn-1", UpstreamSiteID: "site-1", UpstreamGroupID: "1", UpstreamGroupName: "王中王",
		AdminAccountID: "acc-1", AdminAccountName: "A-【王中王】-0.08x", AdminPlatform: string(upstream.PlatformSub2API), Status: ConnectionStatusActive,
	}}}
	service := NewService(stateRepo, upstream.NewPlatformService(upstream.NewHTTPClient(server.Client())), nil)
	service.connRepository = connRepo
	results := service.SyncSub2APIAccountNamesAfterRateChanges(context.Background(), "user-1", "workspace-1", "site-1", "王中王",
		upstream.Metrics{Groups: []upstream.GroupInfo{{ID: "1", Name: "王中王", Multiplier: pointerFloat64(0.08)}}},
		upstream.Metrics{Groups: []upstream.GroupInfo{{ID: "1", Name: "王中王", Multiplier: pointerFloat64(0.06)}}})

	if putCalls != 0 || len(results) != 1 || results[0].Status != "unchanged" || results[0].OldName != "A-【王中王】-0.06x" {
		t.Fatalf("expected remote name to be authoritative without PUT, calls=%d results=%#v", putCalls, results)
	}
	if connRepo.connections[0].AdminAccountName != "A-【王中王】-0.06x" {
		t.Fatalf("expected stale local binding name calibrated, got %q", connRepo.connections[0].AdminAccountName)
	}
}

func TestSyncSub2APIAccountNamesAfterRateChangesUsesUniqueLegacyMatch(t *testing.T) {
	putCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			if r.URL.Path == "/api/v1/admin/accounts" {
				_, _ = w.Write([]byte(`{"data":[{"id":"legacy-1","name":"A-【王中王】-0.08x"}]}`))
				return
			}
			_, _ = w.Write([]byte(`{"data":{"name":"A-【王中王】-0.08x","type":"openai","status":"active","group_ids":[50]}}`))
		case http.MethodPut:
			putCalls++
			_, _ = w.Write([]byte(`{"data":{"id":"legacy-1"}}`))
		default:
			t.Fatalf("unexpected method %s", r.Method)
		}
	}))
	defer server.Close()

	stateRepo := &testStateRepo{state: &State{Session: upstream.Session{Platform: upstream.PlatformSub2API, BaseURL: server.URL, AccessToken: "token"}}}
	service := NewService(stateRepo, upstream.NewPlatformService(upstream.NewHTTPClient(server.Client())), nil)
	service.connRepository = &renameConnectionRepo{}
	results := service.SyncSub2APIAccountNamesAfterRateChanges(context.Background(), "user-1", "workspace-1", "site-1", "王中王",
		upstream.Metrics{Groups: []upstream.GroupInfo{{ID: "1", Name: "group", Multiplier: pointerFloat64(0.08)}}},
		upstream.Metrics{Groups: []upstream.GroupInfo{{ID: "1", Name: "group", Multiplier: pointerFloat64(0.06)}}})

	if putCalls != 1 {
		t.Fatalf("expected one legacy account rename, got %d", putCalls)
	}
	if len(results) != 1 || results[0].Status != "updated" || results[0].NewName != "A-【王中王】-0.06x" {
		t.Fatalf("unexpected legacy rename results: %#v", results)
	}
}

func TestSyncSub2APIAccountNamesAfterRateChangesIsolatesRemoteFailure(t *testing.T) {
	putNames := make([]string, 0, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodGet && r.URL.Path == "/api/v1/admin/accounts/acc-fail" {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"error":"failed"}`))
			return
		}
		if r.Method == http.MethodPut {
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode PUT body: %v", err)
			}
			putNames = append(putNames, body["name"].(string))
		}
		_, _ = w.Write([]byte(`{"data":{"name":"A-【王中王】-0.08x","type":"openai","status":"active","group_ids":[50]}}`))
	}))
	defer server.Close()

	stateRepo := &testStateRepo{state: &State{Session: upstream.Session{Platform: upstream.PlatformSub2API, BaseURL: server.URL, AccessToken: "token"}}}
	connRepo := &renameConnectionRepo{connections: []RealConnection{
		{UpstreamSiteID: "site-1", UpstreamGroupID: "1", UpstreamGroupName: "王中王", AdminAccountID: "acc-fail", AdminAccountName: "A-【王中王】-0.08x", AdminPlatform: string(upstream.PlatformSub2API), Status: ConnectionStatusActive},
		{UpstreamSiteID: "site-1", UpstreamGroupID: "1", UpstreamGroupName: "王中王", AdminAccountID: "acc-ok", AdminAccountName: "A-【王中王】-0.08x", AdminPlatform: string(upstream.PlatformSub2API), Status: ConnectionStatusActive},
	}}
	service := NewService(stateRepo, upstream.NewPlatformService(upstream.NewHTTPClient(server.Client())), nil)
	service.connRepository = connRepo
	results := service.SyncSub2APIAccountNamesAfterRateChanges(context.Background(), "user-1", "workspace-1", "site-1", "王中王",
		upstream.Metrics{Groups: []upstream.GroupInfo{{ID: "1", Name: "王中王", Multiplier: pointerFloat64(0.08)}}},
		upstream.Metrics{Groups: []upstream.GroupInfo{{ID: "1", Name: "王中王", Multiplier: pointerFloat64(0.06)}}})

	if len(putNames) != 1 || putNames[0] != "A-【王中王】-0.06x" {
		t.Fatalf("expected healthy account to be renamed, got %#v", putNames)
	}
	if len(results) != 2 || results[0].Status != "failed" || results[1].Status != "updated" {
		t.Fatalf("expected isolated failure and success, got %#v", results)
	}
}
