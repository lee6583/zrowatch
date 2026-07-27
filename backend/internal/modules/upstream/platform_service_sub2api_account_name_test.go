package upstream

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestUpdateSub2APIAdminAccountNamePreservesAccountFields(t *testing.T) {
	var putBody map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/admin/accounts/acc-1" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		if r.Method == http.MethodGet {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"data":{"name":"A-【王中王】-0.08x","notes":"note","type":"openai","credentials":{"key":"secret"},"status":"active","concurrency":5,"priority":10,"rate_multiplier":1.5,"load_factor":2,"group_ids":[50],"expires_at":"2027-01-01T00:00:00Z","auto_pause_on_expired":true}}`))
			return
		}
		if r.Method == http.MethodPut {
			if err := json.NewDecoder(r.Body).Decode(&putBody); err != nil {
				t.Fatalf("decode PUT body: %v", err)
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"data":{"id":"acc-1"}}`))
			return
		}
		t.Fatalf("unexpected method: %s", r.Method)
	}))
	defer server.Close()

	service := NewPlatformService(NewHTTPClient(server.Client()))
	session := Session{Platform: PlatformSub2API, BaseURL: server.URL, AccessToken: "token"}
	if err := service.UpdateSub2APIAdminAccountName(session, "acc-1", "A-【王中王】-0.06x"); err != nil {
		t.Fatalf("update account name: %v", err)
	}

	if got := putBody["name"]; got != "A-【王中王】-0.06x" {
		t.Fatalf("expected updated name, got %#v", got)
	}
	if got := putBody["notes"]; got != "note" {
		t.Fatalf("expected notes preserved, got %#v", got)
	}
	if got := putBody["rate_multiplier"]; got != float64(1.5) {
		t.Fatalf("expected rate multiplier preserved, got %#v", got)
	}
	groupIDs, ok := putBody["group_ids"].([]any)
	if !ok || len(groupIDs) != 1 || groupIDs[0] != float64(50) {
		t.Fatalf("expected numeric group IDs preserved, got %#v", putBody["group_ids"])
	}
	if _, ok := putBody["credentials"].(map[string]any); !ok {
		t.Fatalf("expected credentials preserved, got %#v", putBody["credentials"])
	}
}

func TestUpdateSub2APIAdminAccountNameRejectsNonSub2API(t *testing.T) {
	service := NewPlatformService(NewHTTPClient(http.DefaultClient))
	err := service.UpdateSub2APIAdminAccountName(Session{Platform: PlatformNewAPI, BaseURL: "https://example.com", UserID: "1", AccessToken: "token"}, "acc-1", "new")
	if err == nil {
		t.Fatal("expected non-sub2api session to be rejected")
	}
}

func TestUpdateSub2APIAdminAccountStatePreservesFieldsAndNumericGroupIDs(t *testing.T) {
	var putBody map[string]any
	var schedulableBody map[string]any
	status := "active"
	schedulable := true
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/admin/accounts/acc-1":
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{
				"name": "account", "notes": "note", "type": "openai", "credentials": map[string]any{"key": "secret"},
				"extra": map[string]any{"region": "cn"}, "proxy_id": 3, "status": status, "schedulable": schedulable,
				"concurrency": 5, "priority": 10, "rate_multiplier": 1.5, "load_factor": 2,
				"group_ids": []int{50}, "auto_pause_on_expired": true,
			}})
		case r.Method == http.MethodPut && r.URL.Path == "/api/v1/admin/accounts/acc-1":
			if err := json.NewDecoder(r.Body).Decode(&putBody); err != nil {
				t.Fatalf("decode PUT body: %v", err)
			}
			status = putBody["status"].(string)
			_, _ = w.Write([]byte(`{"data":{"id":"acc-1"}}`))
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/admin/accounts/acc-1/schedulable":
			if err := json.NewDecoder(r.Body).Decode(&schedulableBody); err != nil {
				t.Fatalf("decode schedulable body: %v", err)
			}
			schedulable = schedulableBody["schedulable"].(bool)
			_, _ = w.Write([]byte(`{"data":{"id":"acc-1","status":"inactive","schedulable":false}}`))
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	service := NewPlatformService(NewHTTPClient(server.Client()))
	session := Session{Platform: PlatformSub2API, BaseURL: server.URL, AccessToken: "token"}
	desiredStatus := "inactive"
	desiredSchedulable := false
	if err := service.UpdateSub2APIAdminAccountState(session, "acc-1", &desiredStatus, &desiredSchedulable); err != nil {
		t.Fatalf("update account state: %v", err)
	}

	if putBody["status"] != "inactive" || putBody["schedulable"] != true {
		t.Fatalf("expected PUT to change status while preserving the original schedulable field, got %#v", putBody)
	}
	if schedulableBody["schedulable"] != false {
		t.Fatalf("expected dedicated schedulable endpoint to disable scheduling, got %#v", schedulableBody)
	}
	for _, field := range []string{"name", "notes", "type", "credentials", "extra", "proxy_id", "concurrency", "priority", "rate_multiplier", "load_factor", "auto_pause_on_expired"} {
		if _, ok := putBody[field]; !ok {
			t.Fatalf("expected %s to be preserved in %#v", field, putBody)
		}
	}
	groupIDs, ok := putBody["group_ids"].([]any)
	if !ok || len(groupIDs) != 1 || groupIDs[0] != float64(50) {
		t.Fatalf("expected numeric group IDs preserved, got %#v", putBody["group_ids"])
	}
}

func TestUpdateSub2APIAdminAccountDispatchUsesDedicatedSchedulableEndpoint(t *testing.T) {
	var putBody map[string]any
	var schedulableBody map[string]any
	status := "active"
	schedulable := true
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/admin/accounts/acc-1":
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{
				"name": "account", "type": "openai", "credentials": map[string]any{"key": "secret"},
				"status": status, "schedulable": schedulable, "group_ids": []int{50},
			}})
		case r.Method == http.MethodPut && r.URL.Path == "/api/v1/admin/accounts/acc-1":
			if err := json.NewDecoder(r.Body).Decode(&putBody); err != nil {
				t.Fatalf("decode account PUT body: %v", err)
			}
			status = putBody["status"].(string)
			_, _ = w.Write([]byte(`{"data":{"id":"acc-1","status":"inactive"}}`))
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/admin/accounts/acc-1/schedulable":
			if err := json.NewDecoder(r.Body).Decode(&schedulableBody); err != nil {
				t.Fatalf("decode schedulable POST body: %v", err)
			}
			schedulable = schedulableBody["schedulable"].(bool)
			_, _ = w.Write([]byte(`{"data":{"id":"acc-1","status":"inactive","schedulable":false}}`))
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	service := NewPlatformService(NewHTTPClient(server.Client()))
	session := Session{Platform: PlatformSub2API, BaseURL: server.URL, AccessToken: "token"}
	state, err := service.UpdateSub2APIAdminAccountDispatch(session, "acc-1", false)
	if err != nil {
		t.Fatalf("update dispatch state: %v", err)
	}
	if putBody["status"] != "inactive" {
		t.Fatalf("expected status update in account PUT, got %#v", putBody)
	}
	if schedulableBody["schedulable"] != false {
		t.Fatalf("expected schedulable=false in dedicated POST, got %#v", schedulableBody)
	}
	if state.Status != "inactive" || state.Schedulable == nil || *state.Schedulable {
		t.Fatalf("unexpected returned dispatch state: %#v", state)
	}
}

func TestUpdateSub2APIAdminAccountStateRejectsUnpersistedSchedulableChange(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/admin/accounts/acc-1":
			_, _ = w.Write([]byte(`{"data":{"name":"account","type":"openai","credentials":{"key":"secret"},"status":"inactive","schedulable":true,"group_ids":[50]}}`))
		case r.Method == http.MethodPut && r.URL.Path == "/api/v1/admin/accounts/acc-1":
			_, _ = w.Write([]byte(`{"data":{"id":"acc-1","status":"inactive"}}`))
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/admin/accounts/acc-1/schedulable":
			// Simulate a remote version that acknowledges the request without
			// persisting it. The verification GET above must catch this.
			_, _ = w.Write([]byte(`{"data":{"id":"acc-1"}}`))
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	service := NewPlatformService(NewHTTPClient(server.Client()))
	session := Session{Platform: PlatformSub2API, BaseURL: server.URL, AccessToken: "token"}
	status := "inactive"
	schedulable := false
	if err := service.UpdateSub2APIAdminAccountState(session, "acc-1", &status, &schedulable); err == nil {
		t.Fatal("expected an error when the remote schedulable state was not persisted")
	}
}
