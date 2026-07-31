package my_sites

import (
	"context"
	"log"
	"strings"
	"sync"

	"transithub/backend/internal/modules/upstream"
)

type remoteAccountGroups struct {
	accountID string
	state     upstream.Sub2APIAdminAccountState
	err       error
}

// reconcileSub2APIRealConnectionGroups treats a successful remote account GET
// as authoritative for active group membership. Cost-guard pauses remain local
// policy state and are excluded from the active groups even if someone briefly
// adds one of them back on the remote account.
func (s *Service) reconcileSub2APIRealConnectionGroups(
	ctx context.Context,
	userID string,
	adminAccountID string,
	connections []RealConnection,
	pausesByConnection map[string][]CostGuardPause,
) []RealConnection {
	updater, ok := s.connRepository.(RealConnectionGroupUpdater)
	if !ok || s.repository == nil || s.platformService == nil || len(connections) == 0 {
		return connections
	}

	accountIDs := make([]string, 0)
	seenAccounts := make(map[string]struct{})
	for _, conn := range connections {
		accountID := strings.TrimSpace(conn.AdminAccountID)
		if !isSub2APIReconcilableConnection(conn) {
			continue
		}
		if _, exists := seenAccounts[accountID]; exists {
			continue
		}
		seenAccounts[accountID] = struct{}{}
		accountIDs = append(accountIDs, accountID)
	}
	if len(accountIDs) == 0 {
		return connections
	}

	state, err := s.authenticatedState(ctx, userID, adminAccountID)
	if err != nil {
		log.Printf("[real-connection-reconcile] admin session unavailable user_id=%s admin_account_id=%s err=%v", userID, adminAccountID, err)
		return connections
	}
	if state.Session.Platform != upstream.PlatformSub2API {
		return connections
	}
	groups, err := s.platformService.FetchAdminAllGroups(state.Session)
	if err != nil {
		log.Printf("[real-connection-reconcile] downstream groups unavailable user_id=%s admin_account_id=%s err=%v", userID, adminAccountID, err)
		return connections
	}
	groupNamesByID := make(map[string]string, len(groups))
	for _, group := range groups {
		id := strings.TrimSpace(group.ID)
		name := strings.TrimSpace(group.Name)
		if id != "" {
			groupNamesByID[id] = firstNonEmpty(name, id)
		}
	}

	remoteAccounts := make([]remoteAccountGroups, len(accountIDs))
	var wait sync.WaitGroup
	limit := make(chan struct{}, 6)
	for index, accountID := range accountIDs {
		wait.Add(1)
		go func(index int, accountID string) {
			defer wait.Done()
			limit <- struct{}{}
			defer func() { <-limit }()
			remoteState, remoteErr := s.platformService.GetSub2APIAdminAccountState(state.Session, accountID)
			remoteAccounts[index] = remoteAccountGroups{accountID: accountID, state: remoteState, err: remoteErr}
		}(index, accountID)
	}
	wait.Wait()

	remoteByAccountID := make(map[string]remoteAccountGroups, len(remoteAccounts))
	for _, remote := range remoteAccounts {
		remoteByAccountID[remote.accountID] = remote
	}
	changed := false
	for index := range connections {
		conn := connections[index]
		if !isSub2APIReconcilableConnection(conn) {
			continue
		}
		remote, exists := remoteByAccountID[strings.TrimSpace(conn.AdminAccountID)]
		if !exists {
			continue
		}
		if remote.err != nil {
			log.Printf("[real-connection-reconcile] account groups unavailable connection_id=%s account_id=%s err=%v", conn.ID, conn.AdminAccountID, remote.err)
			continue
		}
		if !remote.state.GroupIDsKnown {
			log.Printf("[real-connection-reconcile] account response omitted groups connection_id=%s account_id=%s", conn.ID, conn.AdminAccountID)
			continue
		}

		desiredIDs, desiredNames := reconciledConnectionGroups(conn, remote.state.GroupIDs, groupNamesByID, pausesByConnection[conn.ID])
		if sameConnectionGroups(conn.OwnGroupIDs, conn.OwnGroupNames, desiredIDs, desiredNames) {
			continue
		}
		addedNames, removedNames := changedConnectionGroupNames(conn.OwnGroupIDs, conn.OwnGroupNames, desiredIDs, desiredNames)
		if err := updater.UpdateRealConnectionGroups(ctx, conn, desiredIDs, desiredNames, addedNames, removedNames); err != nil {
			log.Printf("[real-connection-reconcile] local update failed connection_id=%s account_id=%s err=%v", conn.ID, conn.AdminAccountID, err)
			continue
		}
		connections[index].OwnGroupIDs = desiredIDs
		connections[index].OwnGroupNames = desiredNames
		changed = true
		log.Printf("[real-connection-reconcile] updated connection_id=%s account_id=%s old_groups=%v new_groups=%v", conn.ID, conn.AdminAccountID, conn.OwnGroupIDs, desiredIDs)
	}
	if changed {
		s.notifyRealConnectionsChanged(ctx, userID, adminAccountID)
	}
	return connections
}

func isSub2APIReconcilableConnection(conn RealConnection) bool {
	if strings.TrimSpace(conn.AdminAccountID) == "" || (conn.Status != "" && conn.Status != ConnectionStatusActive) {
		return false
	}
	adminPlatform := strings.ToLower(strings.TrimSpace(conn.AdminPlatform))
	return adminPlatform == "" || adminPlatform == string(upstream.PlatformSub2API)
}

func reconciledConnectionGroups(conn RealConnection, remoteGroupIDs []string, groupNamesByID map[string]string, pauses []CostGuardPause) ([]string, []string) {
	pausedIDs := make(map[string]struct{}, len(pauses))
	for _, pause := range pauses {
		if id := strings.TrimSpace(pause.OwnGroupID); id != "" {
			pausedIDs[id] = struct{}{}
		}
	}
	existingNames := connectionGroupNameMap(conn.OwnGroupIDs, conn.OwnGroupNames)
	ids := make([]string, 0, len(remoteGroupIDs))
	names := make([]string, 0, len(remoteGroupIDs))
	seen := make(map[string]struct{}, len(remoteGroupIDs))
	for _, rawID := range remoteGroupIDs {
		id := strings.TrimSpace(rawID)
		if id == "" {
			continue
		}
		if _, paused := pausedIDs[id]; paused {
			continue
		}
		if _, duplicate := seen[id]; duplicate {
			continue
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
		names = append(names, firstNonEmpty(groupNamesByID[id], existingNames[id], id))
	}
	return ids, names
}

func connectionGroupNameMap(ids, names []string) map[string]string {
	result := make(map[string]string, len(ids))
	for index, rawID := range ids {
		id := strings.TrimSpace(rawID)
		if id != "" {
			result[id] = groupNameAt(names, index, id)
		}
	}
	return result
}

func sameConnectionGroups(oldIDs, oldNames, newIDs, newNames []string) bool {
	if len(oldIDs) != len(newIDs) {
		return false
	}
	oldByID := connectionGroupNameMap(oldIDs, oldNames)
	newByID := connectionGroupNameMap(newIDs, newNames)
	if len(oldByID) != len(newByID) {
		return false
	}
	for id, name := range oldByID {
		if newByID[id] != name {
			return false
		}
	}
	return true
}

func changedConnectionGroupNames(oldIDs, oldNames, newIDs, newNames []string) ([]string, []string) {
	oldByID := connectionGroupNameMap(oldIDs, oldNames)
	newByID := connectionGroupNameMap(newIDs, newNames)
	added := make([]string, 0)
	removed := make([]string, 0)
	for _, id := range newIDs {
		name := newByID[id]
		if oldName, exists := oldByID[id]; !exists || oldName != name {
			added = append(added, name)
		}
	}
	for _, id := range oldIDs {
		name := oldByID[id]
		if newName, exists := newByID[id]; !exists || newName != name {
			removed = append(removed, name)
		}
	}
	return added, removed
}
