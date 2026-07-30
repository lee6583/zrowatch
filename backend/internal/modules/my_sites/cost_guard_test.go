package my_sites

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"transithub/backend/internal/modules/upstream"
)

type costGuardTestConnRepo struct {
	connections      map[string]RealConnection
	pauses           map[string]CostGuardPause
	updateCalls      int
	lastAddedNames   []string
	lastRemovedNames []string
}

func newCostGuardTestConnRepo(connections ...RealConnection) *costGuardTestConnRepo {
	repo := &costGuardTestConnRepo{
		connections: make(map[string]RealConnection, len(connections)),
		pauses:      make(map[string]CostGuardPause),
	}
	for _, conn := range connections {
		repo.connections[conn.ID] = conn
	}
	return repo
}

func costGuardPauseKey(connectionID, ownGroupID string) string {
	return connectionID + "|" + ownGroupID
}

func (r *costGuardTestConnRepo) SaveRealConnection(_ context.Context, conn RealConnection) error {
	r.connections[conn.ID] = conn
	return nil
}

func (r *costGuardTestConnRepo) ListRealConnections(_ context.Context, userID, adminAccountID string) ([]RealConnection, error) {
	result := make([]RealConnection, 0, len(r.connections))
	for _, conn := range r.connections {
		if conn.UserID == userID && conn.WorkspaceAdminAccountID == adminAccountID {
			result = append(result, conn)
		}
	}
	return result, nil
}

func (r *costGuardTestConnRepo) GetRealConnection(_ context.Context, id, userID, adminAccountID string) (*RealConnection, error) {
	conn, ok := r.connections[id]
	if !ok || conn.UserID != userID || conn.WorkspaceAdminAccountID != adminAccountID {
		return nil, nil
	}
	return &conn, nil
}

func (r *costGuardTestConnRepo) DeleteRealConnection(_ context.Context, id, userID, adminAccountID string) error {
	conn, ok := r.connections[id]
	if ok && conn.UserID == userID && conn.WorkspaceAdminAccountID == adminAccountID {
		delete(r.connections, id)
	}
	return nil
}

func (r *costGuardTestConnRepo) UpdateRealConnectionGroups(_ context.Context, conn RealConnection, groupIDs, groupNames, addedNames, removedNames []string) error {
	stored := conn
	stored.OwnGroupIDs = append([]string(nil), groupIDs...)
	stored.OwnGroupNames = append([]string(nil), groupNames...)
	r.connections[conn.ID] = stored
	r.updateCalls++
	r.lastAddedNames = append([]string(nil), addedNames...)
	r.lastRemovedNames = append([]string(nil), removedNames...)
	return nil
}

func (r *costGuardTestConnRepo) ListCostGuardPauses(_ context.Context, userID, adminAccountID string) ([]CostGuardPause, error) {
	result := make([]CostGuardPause, 0, len(r.pauses))
	for _, pause := range r.pauses {
		if pause.UserID == userID && pause.WorkspaceAdminAccountID == adminAccountID {
			result = append(result, pause)
		}
	}
	return result, nil
}

func (r *costGuardTestConnRepo) UpsertCostGuardPause(_ context.Context, pause CostGuardPause) error {
	r.pauses[costGuardPauseKey(pause.ConnectionID, pause.OwnGroupID)] = pause
	return nil
}

func (r *costGuardTestConnRepo) DeleteCostGuardPause(_ context.Context, userID, adminAccountID, connectionID, ownGroupID string) error {
	pause, ok := r.pauses[costGuardPauseKey(connectionID, ownGroupID)]
	if ok && pause.UserID == userID && pause.WorkspaceAdminAccountID == adminAccountID {
		delete(r.pauses, costGuardPauseKey(connectionID, ownGroupID))
	}
	return nil
}

func (r *costGuardTestConnRepo) DeleteCostGuardPausesForConnection(_ context.Context, userID, adminAccountID, connectionID string) error {
	for key, pause := range r.pauses {
		if pause.UserID == userID && pause.WorkspaceAdminAccountID == adminAccountID && pause.ConnectionID == connectionID {
			delete(r.pauses, key)
		}
	}
	return nil
}

type costGuardRemote struct {
	accountGroupIDs []any
	failPut         bool
	putBodies       []map[string]any
}

func newCostGuardSub2APIServer(t *testing.T, remote *costGuardRemote) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/auth/me":
			writeConnectionTestJSON(w, map[string]any{"data": map[string]any{"role": "admin"}})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/admin/groups":
			writeConnectionTestJSON(w, map[string]any{"data": []map[string]any{
				{"id": 8, "name": "down-0.08", "rate_multiplier": 0.08, "status": "active"},
				{"id": 12, "name": "down-0.12", "rate_multiplier": 0.12, "status": "active"},
			}})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/admin/accounts/acc-1":
			writeConnectionTestJSON(w, map[string]any{"data": map[string]any{
				"id": "acc-1", "name": "account", "type": "openai", "status": "active",
				"credentials": map[string]any{"api_key": "sk-secret"}, "group_ids": remote.accountGroupIDs,
				"priority": 10, "concurrency": 5,
			}})
		case r.Method == http.MethodPut && r.URL.Path == "/api/v1/admin/accounts/acc-1":
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode PUT body: %v", err)
			}
			remote.putBodies = append(remote.putBodies, body)
			if remote.failPut {
				w.WriteHeader(http.StatusBadGateway)
				_, _ = w.Write([]byte(`{"message":"failed"}`))
				return
			}
			if groupIDs, ok := body["group_ids"].([]any); ok {
				remote.accountGroupIDs = append([]any(nil), groupIDs...)
			}
			writeConnectionTestJSON(w, map[string]any{"success": true})
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))
}

func newCostGuardTestService(serverURL string, connRepo *costGuardTestConnRepo) *Service {
	session := upstream.Session{Platform: upstream.PlatformSub2API, BaseURL: serverURL, AccessToken: "token", TokenType: "Bearer"}
	stateRepo := &testStateRepo{state: &State{UserID: "user-1", AdminAccountID: "admin-1", Session: session}}
	lookup := testUpstreamLookup{sites: map[string]*upstream.Site{
		"site-1": {ID: "site-1", UserID: "user-1", AdminAccountID: "admin-1", Name: "source", RechargeRate: 1},
	}}
	service := NewService(stateRepo, upstream.NewPlatformService(upstream.NewHTTPClient(http.DefaultClient)), lookup)
	service.platformService = upstream.NewPlatformService(upstream.NewHTTPClient(http.DefaultClient))
	service.connRepository = connRepo
	service.SetAdminAccountResolver(testAdminResolver{currentID: "admin-1"})
	return service
}

func costGuardConnection(groupIDs, groupNames []string) RealConnection {
	return RealConnection{
		ID: "conn-1", UserID: "user-1", WorkspaceAdminAccountID: "admin-1",
		UpstreamSiteID: "site-1", UpstreamGroupID: "up-1", UpstreamGroupName: "vip",
		AdminAccountID: "acc-1", AdminAccountName: "account",
		OwnGroupIDs: groupIDs, OwnGroupNames: groupNames,
		GroupType: "openai", Status: ConnectionStatusActive, AdminPlatform: string(upstream.PlatformSub2API),
	}
}

func costGuardMetrics(oldValue, newValue float64) (upstream.Metrics, upstream.Metrics) {
	oldMetrics := upstream.Metrics{Groups: []upstream.GroupInfo{{ID: "up-1", Name: "vip", Multiplier: floatPtr(oldValue)}}}
	newMetrics := upstream.Metrics{Groups: []upstream.GroupInfo{{ID: "up-1", Name: "vip", Multiplier: floatPtr(newValue)}}}
	return oldMetrics, newMetrics
}

func TestGroupRateCostGuardRemovesOnlyUnprofitableDownstreamGroup(t *testing.T) {
	remote := &costGuardRemote{accountGroupIDs: []any{float64(8), float64(12)}}
	server := newCostGuardSub2APIServer(t, remote)
	defer server.Close()

	connRepo := newCostGuardTestConnRepo(costGuardConnection([]string{"8", "12"}, []string{"down-0.08", "down-0.12"}))
	service := newCostGuardTestService(server.URL, connRepo)
	oldMetrics, newMetrics := costGuardMetrics(0.10, 0.11)

	results := service.ApplyGroupRateCostGuardAfterSync(context.Background(), "user-1", "admin-1", "site-1", "source", oldMetrics, newMetrics, true)
	if !hasCostGuardStatus(results, "removed", "8") {
		t.Fatalf("expected downstream group 8 removed, got %#v", results)
	}
	if hasCostGuardStatus(results, "removed", "12") {
		t.Fatalf("downstream group 12 should stay bound, got %#v", results)
	}
	conn := connRepo.connections["conn-1"]
	if !reflect.DeepEqual(conn.OwnGroupIDs, []string{"12"}) {
		t.Fatalf("expected local connection groups [12], got %#v", conn.OwnGroupIDs)
	}
	if _, ok := connRepo.pauses[costGuardPauseKey("conn-1", "8")]; !ok {
		t.Fatal("expected pause record for removed group 8")
	}
	groupIDs := remote.putBodies[0]["group_ids"].([]any)
	if len(groupIDs) != 1 || groupIDs[0] != float64(12) {
		t.Fatalf("expected PUT group_ids to keep numeric 12 only, got %#v", groupIDs)
	}
}

func TestGroupRateCostGuardDoesNotRemoveWhenCostEqualsDownstream(t *testing.T) {
	remote := &costGuardRemote{accountGroupIDs: []any{float64(8)}}
	server := newCostGuardSub2APIServer(t, remote)
	defer server.Close()

	connRepo := newCostGuardTestConnRepo(costGuardConnection([]string{"8"}, []string{"down-0.08"}))
	service := newCostGuardTestService(server.URL, connRepo)
	oldMetrics, newMetrics := costGuardMetrics(0.07, 0.08)

	results := service.ApplyGroupRateCostGuardAfterSync(context.Background(), "user-1", "admin-1", "site-1", "source", oldMetrics, newMetrics, true)
	if len(results) != 0 || len(remote.putBodies) != 0 || len(connRepo.pauses) != 0 {
		t.Fatalf("equal cost must not remove groups, results=%#v puts=%d pauses=%#v", results, len(remote.putBodies), connRepo.pauses)
	}
}

func TestGroupRateCostGuardRestoresPreviouslyPausedGroupWhenCostFalls(t *testing.T) {
	remote := &costGuardRemote{accountGroupIDs: []any{float64(12)}}
	server := newCostGuardSub2APIServer(t, remote)
	defer server.Close()

	connRepo := newCostGuardTestConnRepo(costGuardConnection([]string{"12"}, []string{"down-0.12"}))
	connRepo.pauses[costGuardPauseKey("conn-1", "8")] = CostGuardPause{
		UserID: "user-1", WorkspaceAdminAccountID: "admin-1", ConnectionID: "conn-1",
		UpstreamSiteID: "site-1", UpstreamGroupID: "up-1", UpstreamGroupName: "vip",
		OwnGroupID: "8", OwnGroupName: "down-0.08",
	}
	service := newCostGuardTestService(server.URL, connRepo)
	oldMetrics, newMetrics := costGuardMetrics(0.11, 0.07)

	results := service.ApplyGroupRateCostGuardAfterSync(context.Background(), "user-1", "admin-1", "site-1", "source", oldMetrics, newMetrics, true)
	if !hasCostGuardStatus(results, "restored", "8") {
		t.Fatalf("expected group 8 restored, got %#v", results)
	}
	conn := connRepo.connections["conn-1"]
	if !reflect.DeepEqual(conn.OwnGroupIDs, []string{"12", "8"}) {
		t.Fatalf("expected local connection groups restored to [12 8], got %#v", conn.OwnGroupIDs)
	}
	if _, ok := connRepo.pauses[costGuardPauseKey("conn-1", "8")]; ok {
		t.Fatal("expected pause record deleted after restore")
	}
	groupIDs := remote.putBodies[0]["group_ids"].([]any)
	if len(groupIDs) != 2 || groupIDs[0] != float64(12) || groupIDs[1] != float64(8) {
		t.Fatalf("expected PUT group_ids [12 8], got %#v", groupIDs)
	}
}

func TestGroupRateCostGuardRestoresAfterRemovingLastRemoteGroup(t *testing.T) {
	remote := &costGuardRemote{accountGroupIDs: []any{float64(8)}}
	server := newCostGuardSub2APIServer(t, remote)
	defer server.Close()

	connRepo := newCostGuardTestConnRepo(costGuardConnection([]string{"8"}, []string{"down-0.08"}))
	service := newCostGuardTestService(server.URL, connRepo)
	oldMetrics, expensiveMetrics := costGuardMetrics(0.07, 0.11)

	removed := service.ApplyGroupRateCostGuardAfterSync(context.Background(), "user-1", "admin-1", "site-1", "source", oldMetrics, expensiveMetrics, true)
	if !hasCostGuardStatus(removed, "removed", "8") {
		t.Fatalf("expected last downstream group removed, got %#v", removed)
	}
	if len(remote.accountGroupIDs) != 0 || len(connRepo.connections["conn-1"].OwnGroupIDs) != 0 {
		t.Fatalf("expected remote and local groups empty, remote=%#v local=%#v", remote.accountGroupIDs, connRepo.connections["conn-1"].OwnGroupIDs)
	}

	_, cheapMetrics := costGuardMetrics(0.11, 0.07)
	restored := service.ApplyGroupRateCostGuardAfterSync(context.Background(), "user-1", "admin-1", "site-1", "source", expensiveMetrics, cheapMetrics, true)
	if !hasCostGuardStatus(restored, "restored", "8") {
		t.Fatalf("expected group restored from an empty remote account, got %#v", restored)
	}
	if !reflect.DeepEqual(connRepo.connections["conn-1"].OwnGroupIDs, []string{"8"}) {
		t.Fatalf("expected local group 8 restored, got %#v", connRepo.connections["conn-1"].OwnGroupIDs)
	}
	if len(remote.accountGroupIDs) != 1 || remote.accountGroupIDs[0] != float64(8) {
		t.Fatalf("expected numeric remote group 8 restored, got %#v", remote.accountGroupIDs)
	}
	if len(connRepo.pauses) != 0 {
		t.Fatalf("expected pause cleared after restoration, got %#v", connRepo.pauses)
	}
}

func TestUpdateRealConnectionGroupsCanRestorePausedOnlyGroup(t *testing.T) {
	remote := &costGuardRemote{accountGroupIDs: []any{}}
	server := newCostGuardSub2APIServer(t, remote)
	defer server.Close()

	connRepo := newCostGuardTestConnRepo(costGuardConnection([]string{}, []string{}))
	connRepo.pauses[costGuardPauseKey("conn-1", "8")] = CostGuardPause{
		UserID: "user-1", WorkspaceAdminAccountID: "admin-1", ConnectionID: "conn-1",
		UpstreamSiteID: "site-1", UpstreamGroupID: "up-1", UpstreamGroupName: "vip",
		OwnGroupID: "8", OwnGroupName: "down-0.08",
	}
	service := newCostGuardTestService(server.URL, connRepo)

	updated, err := service.UpdateRealConnectionGroups(context.Background(), "user-1", "conn-1", UpdateRealConnectionGroupsRequest{OwnGroupIDs: []string{"8"}})
	if err != nil {
		t.Fatalf("restore paused group through editor: %v", err)
	}
	if !reflect.DeepEqual(updated.OwnGroupIDs, []string{"8"}) || len(updated.CostGuardPausedOwnGroupIDs) != 0 {
		t.Fatalf("expected active group with no pause, got %#v", updated)
	}
	if len(connRepo.pauses) != 0 {
		t.Fatalf("expected manual update to clear pause, got %#v", connRepo.pauses)
	}
}

func TestListRealConnectionsIncludesCostGuardPausedGroups(t *testing.T) {
	connRepo := newCostGuardTestConnRepo(costGuardConnection([]string{}, []string{}))
	connRepo.pauses[costGuardPauseKey("conn-1", "8")] = CostGuardPause{
		UserID: "user-1", WorkspaceAdminAccountID: "admin-1", ConnectionID: "conn-1",
		UpstreamSiteID: "site-1", UpstreamGroupID: "up-1", UpstreamGroupName: "vip",
		OwnGroupID: "8", OwnGroupName: "down-0.08",
	}
	service := newCostGuardTestService("http://unused.invalid", connRepo)

	connections, err := service.ListRealConnections(context.Background(), "user-1")
	if err != nil {
		t.Fatalf("list real connections: %v", err)
	}
	if len(connections) != 1 || !reflect.DeepEqual(connections[0].CostGuardPausedOwnGroupIDs, []string{"8"}) ||
		!reflect.DeepEqual(connections[0].CostGuardPausedOwnGroupNames, []string{"down-0.08"}) {
		t.Fatalf("expected paused group metadata in response, got %#v", connections)
	}
}

func TestDisconnectCanDeletePausedOnlyConnection(t *testing.T) {
	connRepo := newCostGuardTestConnRepo(costGuardConnection([]string{}, []string{}))
	connRepo.pauses[costGuardPauseKey("conn-1", "8")] = CostGuardPause{
		UserID: "user-1", WorkspaceAdminAccountID: "admin-1", ConnectionID: "conn-1",
		UpstreamSiteID: "site-1", UpstreamGroupID: "up-1", UpstreamGroupName: "vip",
		OwnGroupID: "8", OwnGroupName: "down-0.08",
	}
	service := newCostGuardTestService("http://unused.invalid", connRepo)

	err := service.realDisconnectConnection(context.Background(), "user-1", RealDisconnectRequest{
		ConnectionID: "conn-1", Mode: "unlink", OwnGroupIDs: []string{"8"},
	})
	if err != nil {
		t.Fatalf("disconnect paused-only connection: %v", err)
	}
	if _, exists := connRepo.connections["conn-1"]; exists {
		t.Fatal("expected paused-only connection deleted")
	}
	if len(connRepo.pauses) != 0 {
		t.Fatalf("expected paused records deleted, got %#v", connRepo.pauses)
	}
}

func TestGroupRateCostGuardRemoteFailureKeepsPauseForRetry(t *testing.T) {
	remote := &costGuardRemote{accountGroupIDs: []any{float64(8), float64(12)}, failPut: true}
	server := newCostGuardSub2APIServer(t, remote)
	defer server.Close()

	connRepo := newCostGuardTestConnRepo(costGuardConnection([]string{"8", "12"}, []string{"down-0.08", "down-0.12"}))
	service := newCostGuardTestService(server.URL, connRepo)
	oldMetrics, newMetrics := costGuardMetrics(0.10, 0.11)

	results := service.ApplyGroupRateCostGuardAfterSync(context.Background(), "user-1", "admin-1", "site-1", "source", oldMetrics, newMetrics, true)
	if !hasCostGuardStatus(results, "failed", "") {
		t.Fatalf("expected failed result, got %#v", results)
	}
	conn := connRepo.connections["conn-1"]
	if !reflect.DeepEqual(conn.OwnGroupIDs, []string{"8", "12"}) {
		t.Fatalf("remote failure should not update local groups, got %#v", conn.OwnGroupIDs)
	}
	if _, ok := connRepo.pauses[costGuardPauseKey("conn-1", "8")]; !ok {
		t.Fatal("expected pause record retained for retry")
	}
}

func hasCostGuardStatus(results []GroupRateCostGuardResult, status, ownGroupID string) bool {
	for _, result := range results {
		if result.Status == status && (ownGroupID == "" || result.OwnGroupID == ownGroupID) {
			return true
		}
	}
	return false
}
