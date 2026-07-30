package my_sites

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"transithub/backend/internal/modules/upstream"
)

func TestListRealConnectionsReconciledTracksRemoteGroupRemovalAndAddition(t *testing.T) {
	remote := &costGuardRemote{accountGroupIDs: []any{float64(12)}}
	server := newCostGuardSub2APIServer(t, remote)
	defer server.Close()

	connRepo := newCostGuardTestConnRepo(costGuardConnection([]string{"8"}, []string{"down-0.08"}))
	service := newCostGuardTestService(server.URL, connRepo)

	connections, err := service.ListRealConnectionsReconciled(context.Background(), "user-1")
	if err != nil {
		t.Fatalf("reconcile removal: %v", err)
	}
	if len(connections) != 1 || !reflect.DeepEqual(connections[0].OwnGroupIDs, []string{"12"}) ||
		!reflect.DeepEqual(connections[0].OwnGroupNames, []string{"down-0.12"}) {
		t.Fatalf("expected remote group 12 after removal, got %#v", connections)
	}
	if !reflect.DeepEqual(connRepo.lastAddedNames, []string{"down-0.12"}) || !reflect.DeepEqual(connRepo.lastRemovedNames, []string{"down-0.08"}) {
		t.Fatalf("unexpected mapping changes added=%v removed=%v", connRepo.lastAddedNames, connRepo.lastRemovedNames)
	}

	remote.accountGroupIDs = []any{float64(8), float64(12)}
	connections, err = service.ListRealConnectionsReconciled(context.Background(), "user-1")
	if err != nil {
		t.Fatalf("reconcile addition: %v", err)
	}
	if !reflect.DeepEqual(connections[0].OwnGroupIDs, []string{"8", "12"}) ||
		!reflect.DeepEqual(connections[0].OwnGroupNames, []string{"down-0.08", "down-0.12"}) {
		t.Fatalf("expected manually added group 8, got %#v", connections[0])
	}
	if !reflect.DeepEqual(connRepo.lastAddedNames, []string{"down-0.08"}) || len(connRepo.lastRemovedNames) != 0 {
		t.Fatalf("unexpected addition mapping changes added=%v removed=%v", connRepo.lastAddedNames, connRepo.lastRemovedNames)
	}
}

func TestListRealConnectionsReconciledAcceptsExplicitEmptyRemoteGroups(t *testing.T) {
	remote := &costGuardRemote{accountGroupIDs: []any{}}
	server := newCostGuardSub2APIServer(t, remote)
	defer server.Close()

	connRepo := newCostGuardTestConnRepo(costGuardConnection([]string{"8"}, []string{"down-0.08"}))
	service := newCostGuardTestService(server.URL, connRepo)

	connections, err := service.ListRealConnectionsReconciled(context.Background(), "user-1")
	if err != nil {
		t.Fatalf("reconcile empty groups: %v", err)
	}
	if len(connections) != 1 || len(connections[0].OwnGroupIDs) != 0 || len(connRepo.connections["conn-1"].OwnGroupIDs) != 0 {
		t.Fatalf("explicit empty remote groups must clear local active groups, response=%#v stored=%#v", connections, connRepo.connections["conn-1"])
	}
}

func TestListRealConnectionsReconciledKeepsCostGuardPausedGroupExcluded(t *testing.T) {
	remote := &costGuardRemote{accountGroupIDs: []any{float64(8), float64(12)}}
	server := newCostGuardSub2APIServer(t, remote)
	defer server.Close()

	connRepo := newCostGuardTestConnRepo(costGuardConnection([]string{"12"}, []string{"down-0.12"}))
	connRepo.pauses[costGuardPauseKey("conn-1", "8")] = CostGuardPause{
		UserID: "user-1", WorkspaceAdminAccountID: "admin-1", ConnectionID: "conn-1",
		OwnGroupID: "8", OwnGroupName: "down-0.08",
	}
	service := newCostGuardTestService(server.URL, connRepo)

	connections, err := service.ListRealConnectionsReconciled(context.Background(), "user-1")
	if err != nil {
		t.Fatalf("reconcile paused group: %v", err)
	}
	if len(connections) != 1 || !reflect.DeepEqual(connections[0].OwnGroupIDs, []string{"12"}) ||
		!reflect.DeepEqual(connections[0].CostGuardPausedOwnGroupIDs, []string{"8"}) {
		t.Fatalf("cost-guard pause must stay excluded from active groups, got %#v", connections)
	}
	if connRepo.updateCalls != 0 {
		t.Fatalf("paused remote group must not trigger a local active-group update")
	}
}

func TestListRealConnectionsReconciledPreservesLocalGroupsOnRemoteFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/auth/me":
			writeConnectionTestJSON(w, map[string]any{"data": map[string]any{"role": "admin"}})
		case "/api/v1/admin/groups":
			writeConnectionTestJSON(w, map[string]any{"data": []map[string]any{{"id": 8, "name": "down-0.08"}}})
		case "/api/v1/admin/accounts/acc-1":
			w.WriteHeader(http.StatusBadGateway)
			writeConnectionTestJSON(w, map[string]any{"message": "temporarily unavailable"})
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	connRepo := newCostGuardTestConnRepo(costGuardConnection([]string{"8"}, []string{"down-0.08"}))
	session := upstream.Session{Platform: upstream.PlatformSub2API, BaseURL: server.URL, AccessToken: "token", TokenType: "Bearer"}
	stateRepo := &testStateRepo{state: &State{UserID: "user-1", AdminAccountID: "admin-1", Session: session}}
	service := NewService(stateRepo, upstream.NewPlatformService(upstream.NewHTTPClient(server.Client())), nil)
	service.connRepository = connRepo
	service.SetAdminAccountResolver(testAdminResolver{currentID: "admin-1"})

	connections, err := service.ListRealConnectionsReconciled(context.Background(), "user-1")
	if err != nil {
		t.Fatalf("remote failure must not fail local list: %v", err)
	}
	if len(connections) != 1 || !reflect.DeepEqual(connections[0].OwnGroupIDs, []string{"8"}) || connRepo.updateCalls != 0 {
		t.Fatalf("remote failure must preserve local groups, got %#v", connections)
	}
}
