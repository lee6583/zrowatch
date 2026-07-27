package my_sites

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"transithub/backend/internal/modules/upstream"
)

// connectionContext contains the two independently authenticated sides of a
// connection. Their platforms may differ and must never be inferred from one another.
type connectionContext struct {
	adminAccountID  string
	state           *State
	upstreamSite    *upstream.Site
	upstreamSession upstream.Session
	groupType       string
	groupName       string
	multiplierLabel string
}

func addToPricingMapping(value *bool) bool {
	return value == nil || *value
}

func normalizeOperationID(value string) (string, error) {
	value = strings.TrimSpace(value)
	if len(value) > 128 {
		return "", requestError(ErrorRequest)
	}
	return value, nil
}

func (s *Service) prepareConnectionContext(ctx context.Context, userID, siteID, groupID, groupName, requestedType string, requireAdminResourceType bool) (connectionContext, error) {
	adminAccountID, err := s.currentAdminAccountID(ctx, userID)
	if err != nil {
		return connectionContext{}, err
	}
	state, err := s.authenticatedState(ctx, userID, adminAccountID)
	if err != nil {
		return connectionContext{}, err
	}
	upstreamSite, err := s.upstreamLookup.GetSite(ctx, strings.TrimSpace(siteID))
	if err != nil || upstreamSite == nil || upstreamSite.Session == nil || upstreamSite.UserID != userID || upstreamSite.AdminAccountID != adminAccountID {
		return connectionContext{}, requestError(ErrorRequest)
	}

	groupType, multiplierLabel := resolveGroupInfo(upstreamSite.Metrics.Groups, strings.TrimSpace(groupID))
	if groupType == "" {
		groupType = strings.ToLower(strings.TrimSpace(requestedType))
	}
	resolvedName := strings.TrimSpace(groupName)
	if resolvedName == "" {
		resolvedName = strings.TrimSpace(groupID)
	}
	if resolvedName == "" {
		return connectionContext{}, requestError(ErrorRequest)
	}
	// A Sub2API admin account requires a concrete provider type even when the
	// upstream side is NewAPI and its group itself has no type metadata.
	if requireAdminResourceType && state.Session.Platform == upstream.PlatformSub2API && groupType == "" {
		return connectionContext{}, requestError(ErrorRequest)
	}

	return connectionContext{
		adminAccountID:  adminAccountID,
		state:           state,
		upstreamSite:    upstreamSite,
		upstreamSession: *upstreamSite.Session,
		groupType:       groupType,
		groupName:       resolvedName,
		multiplierLabel: multiplierLabel,
	}, nil
}

func (s *Service) idempotentConnection(ctx context.Context, userID, adminAccountID, operationID string) (*RealConnection, error) {
	if operationID == "" || s.connRepository == nil {
		return nil, nil
	}
	repo, ok := s.connRepository.(IdempotentRealConnectionRepository)
	if !ok {
		return nil, nil
	}
	return repo.GetRealConnectionByOperationID(ctx, userID, adminAccountID, operationID)
}

func (s *Service) rejectDuplicateTarget(ctx context.Context, userID, adminAccountID, siteID, groupID, groupName string) error {
	if s.connRepository == nil {
		return nil
	}
	connections, err := s.connRepository.ListRealConnections(ctx, userID, adminAccountID)
	if err != nil {
		return err
	}
	for _, conn := range connections {
		if conn.UpstreamSiteID != siteID || conn.Status != "" && conn.Status != ConnectionStatusActive {
			continue
		}
		if conn.UpstreamGroupID == groupID || (conn.UpstreamGroupID == "" && groupID == "" && conn.UpstreamGroupName == groupName) {
			return requestError(ErrorConnectionExists)
		}
	}
	return nil
}

func (s *Service) resolveAdminGroups(ctx context.Context, state *State, requestedIDs []string) ([]string, []string, error) {
	if state == nil || len(requestedIDs) == 0 {
		return nil, nil, requestError(ErrorRequest)
	}
	groups, err := s.platformService.FetchAdminAllGroups(state.Session)
	if err != nil {
		return nil, nil, err
	}
	byID := make(map[string]upstream.AdminGroupInfo, len(groups))
	for _, group := range groups {
		byID[group.ID] = group
	}
	ids := make([]string, 0, len(requestedIDs))
	names := make([]string, 0, len(requestedIDs))
	seen := make(map[string]struct{}, len(requestedIDs))
	for _, requestedID := range requestedIDs {
		requestedID = strings.TrimSpace(requestedID)
		group, ok := byID[requestedID]
		if !ok {
			return nil, nil, requestError(ErrorRequest)
		}
		if _, exists := seen[group.ID]; exists {
			continue
		}
		seen[group.ID] = struct{}{}
		ids = append(ids, group.ID)
		names = append(names, group.Name)
	}
	if len(ids) == 0 {
		return nil, nil, requestError(ErrorRequest)
	}
	return ids, names, nil
}

func (s *Service) realConnectManaged(ctx context.Context, userID string, req RealConnectRequest) (RealConnectResponse, error) {
	if strings.TrimSpace(req.UpstreamSiteID) == "" || strings.TrimSpace(req.UpstreamGroupID) == "" || len(req.OwnGroupIDs) == 0 {
		return RealConnectResponse{}, requestError(ErrorRequest)
	}
	operationID, err := normalizeOperationID(req.OperationID)
	if err != nil {
		return RealConnectResponse{}, err
	}
	connectionCtx, err := s.prepareConnectionContext(ctx, userID, req.UpstreamSiteID, req.UpstreamGroupID, req.UpstreamGroupName, req.GroupType, true)
	if err != nil {
		return RealConnectResponse{}, err
	}
	if existing, err := s.idempotentConnection(ctx, userID, connectionCtx.adminAccountID, operationID); err != nil {
		return RealConnectResponse{}, err
	} else if existing != nil {
		return RealConnectResponse{Connection: publicRealConnection(*existing)}, nil
	}
	if err := s.rejectDuplicateTarget(ctx, userID, connectionCtx.adminAccountID, req.UpstreamSiteID, req.UpstreamGroupID, connectionCtx.groupName); err != nil {
		return RealConnectResponse{}, err
	}
	ownGroupIDs, ownGroupNames, err := s.resolveAdminGroups(ctx, connectionCtx.state, req.OwnGroupIDs)
	if err != nil {
		return RealConnectResponse{}, err
	}

	connID, err := randomConnID()
	if err != nil {
		return RealConnectResponse{}, err
	}
	resourceName := fmt.Sprintf("%s-%s-%s", randomKeyPrefix(), connectionCtx.upstreamSite.Name, connectionCtx.groupName)
	keyID, key, err := s.createUpstreamCredential(connectionCtx.upstreamSession, resourceName, req.UpstreamGroupID)
	if err != nil {
		return RealConnectResponse{}, err
	}
	rollbackKey := func() {
		if rollbackErr := s.deleteUpstreamCredential(connectionCtx.upstreamSession, keyID); rollbackErr != nil {
			log.Printf("[real-connect] compensate upstream credential failed platform=%s id=%s err=%v", connectionCtx.upstreamSession.Platform, keyID, rollbackErr)
		}
	}

	adminResourceID, adminResourceName, err := s.createAdminResource(connectionCtx, req.ChannelType, ownGroupIDs, key)
	if err != nil {
		rollbackKey()
		return RealConnectResponse{}, err
	}
	rollbackAdmin := func() {
		if rollbackErr := s.deleteAdminResource(connectionCtx.state.Session, adminResourceID); rollbackErr != nil {
			log.Printf("[real-connect] compensate admin resource failed platform=%s id=%s err=%v", connectionCtx.state.Session.Platform, adminResourceID, rollbackErr)
		}
	}

	conn := RealConnection{
		ID:                      connID,
		UserID:                  userID,
		WorkspaceAdminAccountID: connectionCtx.adminAccountID,
		UpstreamSiteID:          req.UpstreamSiteID,
		UpstreamGroupID:         req.UpstreamGroupID,
		UpstreamGroupName:       connectionCtx.groupName,
		UpstreamKeyID:           keyID,
		UpstreamKey:             key,
		AdminAccountID:          adminResourceID,
		AdminAccountName:        adminResourceName,
		OwnGroupIDs:             ownGroupIDs,
		OwnGroupNames:           ownGroupNames,
		GroupType:               connectionCtx.groupType,
		ProvisioningMode:        ProvisioningModeManaged,
		Status:                  ConnectionStatusActive,
		UpstreamPlatform:        string(connectionCtx.upstreamSession.Platform),
		AdminPlatform:           string(connectionCtx.state.Session.Platform),
		PricingMappingEnabled:   addToPricingMapping(req.AddToPricingMapping),
		OperationID:             operationID,
		CanDeleteRemote:         true,
		CreatedAt:               time.Now().Format(time.RFC3339),
	}
	if err := s.persistConnection(ctx, conn); err != nil {
		rollbackAdmin()
		rollbackKey()
		return RealConnectResponse{}, err
	}
	return RealConnectResponse{Connection: publicRealConnection(conn)}, nil
}

func (s *Service) createUpstreamCredential(session upstream.Session, name, groupID string) (string, string, error) {
	switch session.Platform {
	case upstream.PlatformNewAPI:
		return s.platformService.CreateNewAPIToken(session, name, groupID)
	case upstream.PlatformSub2API:
		numericGroupID, err := strconv.Atoi(groupID)
		if err != nil {
			return "", "", requestError(ErrorRequest)
		}
		return s.platformService.CreateSub2APIKey(session, name, numericGroupID)
	default:
		return "", "", requestError(ErrorRequest)
	}
}

func (s *Service) deleteUpstreamCredential(session upstream.Session, keyID string) error {
	if strings.TrimSpace(keyID) == "" {
		return nil
	}
	if session.Platform == upstream.PlatformNewAPI {
		return s.platformService.DeleteNewAPIToken(session, keyID)
	}
	return s.platformService.DeleteSub2APIKey(session, keyID)
}

func (s *Service) createAdminResource(connectionCtx connectionContext, requestedChannelType int, ownGroupIDs []string, key string) (string, string, error) {
	if connectionCtx.state.Session.Platform == upstream.PlatformNewAPI {
		channelType := requestedChannelType
		if channelType <= 0 {
			channelType = groupTypeToNewAPIChannelType(connectionCtx.groupType)
		}
		name := fmt.Sprintf("%s-【%s】-%s", newAPIChannelTypeName(channelType), connectionCtx.upstreamSite.Name, connectionCtx.groupName)
		id, err := s.platformService.CreateNewAPIChannel(connectionCtx.state.Session, name, connectionCtx.upstreamSite.BaseURL, key, channelType, ownGroupIDs)
		return id, name, err
	}

	numericGroupIDs, err := stringsToInts(ownGroupIDs)
	if err != nil {
		return "", "", requestError(ErrorRequest)
	}
	rateLabel := connectionCtx.multiplierLabel
	if rateLabel == "" {
		rateLabel = connectionCtx.groupName
	}
	name := fmt.Sprintf("%s-【%s】-%s", groupTypePrefix(connectionCtx.groupType), connectionCtx.upstreamSite.Name, rateLabel)
	payload := buildAccountPayload(connectionCtx.groupType, connectionCtx.upstreamSite.BaseURL, key, numericGroupIDs, name)
	id, err := s.platformService.CreateSub2APIAdminAccount(connectionCtx.state.Session, payload)
	return id, name, err
}

func (s *Service) deleteAdminResource(session upstream.Session, resourceID string) error {
	if strings.TrimSpace(resourceID) == "" {
		return nil
	}
	if session.Platform == upstream.PlatformNewAPI {
		return s.platformService.DeleteNewAPIChannel(session, resourceID)
	}
	return s.platformService.DeleteSub2APIAdminAccount(session, resourceID)
}

func (s *Service) persistConnection(ctx context.Context, conn RealConnection) error {
	if s.connRepository == nil {
		return requestError(ErrorRequest)
	}
	if repository, ok := s.connRepository.(AtomicRealConnectionRepository); ok {
		return repository.SaveRealConnectionWithPricingMapping(ctx, conn)
	}
	if err := s.connRepository.SaveRealConnection(ctx, conn); err != nil {
		return err
	}
	// Older/in-memory repositories do not expose a transaction boundary. Keep
	// their existing behavior for tests and rolling deployments.
	if conn.PricingMappingEnabled {
		s.addUpstreamMapping(ctx, conn.UserID, conn.WorkspaceAdminAccountID, conn.OwnGroupIDs, conn.UpstreamSiteID, conn.UpstreamGroupName)
	}
	return nil
}

func (s *Service) listOwnedUpstreamKeys(ctx context.Context, userID, adminAccountID, siteID string) (*upstream.Site, []upstream.Sub2APIKeyItem, error) {
	upstreamSite, err := s.upstreamLookup.GetSite(ctx, strings.TrimSpace(siteID))
	if err != nil || upstreamSite == nil || upstreamSite.Session == nil || upstreamSite.UserID != userID || upstreamSite.AdminAccountID != adminAccountID {
		return nil, nil, requestError(ErrorRequest)
	}
	var keys []upstream.Sub2APIKeyItem
	if upstreamSite.Session.Platform == upstream.PlatformNewAPI {
		keys, err = s.platformService.ListNewAPITokens(*upstreamSite.Session)
	} else {
		keys, err = s.platformService.ListSub2APIKeys(*upstreamSite.Session)
	}
	return upstreamSite, keys, err
}

func credentialMatchesGroup(item upstream.Sub2APIKeyItem, groupID, groupName string) bool {
	groupID = strings.TrimSpace(groupID)
	groupName = strings.TrimSpace(groupName)
	return (item.GroupID != "" && (item.GroupID == groupID || item.GroupID == groupName)) ||
		(item.GroupName != "" && (item.GroupName == groupName || item.GroupName == groupID))
}

// ListUpstreamCredentials returns only credentials that belong to the selected
// upstream group and strips the full secret before crossing the HTTP boundary.
func (s *Service) ListUpstreamCredentials(ctx context.Context, userID, siteID, groupID, groupName string) ([]UpstreamCredentialOption, error) {
	adminAccountID, err := s.currentAdminAccountID(ctx, userID)
	if err != nil {
		return nil, err
	}
	_, keys, err := s.listOwnedUpstreamKeys(ctx, userID, adminAccountID, siteID)
	if err != nil {
		return nil, err
	}
	result := make([]UpstreamCredentialOption, 0, len(keys))
	for _, key := range keys {
		if !credentialMatchesGroup(key, groupID, groupName) {
			continue
		}
		result = append(result, UpstreamCredentialOption{
			ID: key.ID, Name: key.Name, GroupID: key.GroupID, GroupName: key.GroupName,
			Status: key.Status, KeyPreview: safeCredentialPreview(key.Key),
		})
	}
	return result, nil
}

func safeCredentialPreview(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if len(value) <= 12 {
		return value
	}
	return value[:6] + "..." + value[len(value)-4:]
}

// ListAdminResources lists existing accounts/channels in one current-admin
// group. Selection is revalidated by RealBind; this endpoint is presentation only.
func (s *Service) ListAdminResources(ctx context.Context, userID, adminGroupID string) ([]AdminResourceOption, error) {
	adminAccountID, err := s.currentAdminAccountID(ctx, userID)
	if err != nil {
		return nil, err
	}
	state, err := s.authenticatedState(ctx, userID, adminAccountID)
	if err != nil {
		return nil, err
	}
	group, err := s.findAdminGroup(ctx, state, adminGroupID)
	if err != nil {
		return nil, err
	}
	resources, err := s.platformService.ListAdminGroupAccounts(state.Session, group)
	if err != nil {
		return nil, err
	}
	result := make([]AdminResourceOption, 0, len(resources))
	for _, resource := range resources {
		groupIDs := resource.GroupIDs
		if len(groupIDs) == 0 {
			groupIDs = []string{group.ID}
		}
		result = append(result, AdminResourceOption{
			ID: resource.ID, Name: resource.Name, Type: resource.Type,
			Status: resource.Status, Platform: resource.Platform, GroupIDs: groupIDs,
		})
	}
	return result, nil
}

func (s *Service) findAdminGroup(ctx context.Context, state *State, groupID string) (upstream.AdminGroupInfo, error) {
	groups, err := s.platformService.FetchAdminAllGroups(state.Session)
	if err != nil {
		return upstream.AdminGroupInfo{}, err
	}
	groupID = strings.TrimSpace(groupID)
	for _, group := range groups {
		if group.ID == groupID || group.Name == groupID {
			return group, nil
		}
	}
	return upstream.AdminGroupInfo{}, requestError(ErrorRequest)
}

func (s *Service) resolveExistingAdminResource(ctx context.Context, state *State, groupID, resourceID string) (upstream.AdminGroupAccountInfo, []string, []string, error) {
	group, err := s.findAdminGroup(ctx, state, groupID)
	if err != nil {
		return upstream.AdminGroupAccountInfo{}, nil, nil, err
	}
	resources, err := s.platformService.ListAdminGroupAccounts(state.Session, group)
	if err != nil {
		return upstream.AdminGroupAccountInfo{}, nil, nil, err
	}
	var selected *upstream.AdminGroupAccountInfo
	for i := range resources {
		if resources[i].ID == strings.TrimSpace(resourceID) {
			selected = &resources[i]
			break
		}
	}
	if selected == nil {
		return upstream.AdminGroupAccountInfo{}, nil, nil, requestError(ErrorRequest)
	}
	groupIDs := selected.GroupIDs
	if len(groupIDs) == 0 {
		groupIDs = []string{group.ID}
	}
	returnResource := *selected
	ids, names, err := s.resolveAdminGroups(ctx, state, groupIDs)
	if err != nil {
		return upstream.AdminGroupAccountInfo{}, nil, nil, err
	}
	return returnResource, ids, names, nil
}

func (s *Service) resolveExistingCredential(site *upstream.Site, keys []upstream.Sub2APIKeyItem, keyID, groupID, groupName string, allowLegacy bool, legacyKey string) (string, error) {
	for _, item := range keys {
		if item.ID != strings.TrimSpace(keyID) {
			continue
		}
		if !allowLegacy && !credentialMatchesGroup(item, groupID, groupName) {
			return "", requestError(ErrorRequest)
		}
		if site.Session.Platform == upstream.PlatformNewAPI {
			return s.platformService.FetchNewAPITokenKey(*site.Session, item.ID)
		}
		if strings.TrimSpace(item.Key) == "" {
			return "", requestError(ErrorRequest)
		}
		return item.Key, nil
	}
	// Old clients historically sent the Sub2API key value themselves. Preserve
	// that fallback only for legacy requests without an admin resource selection.
	if allowLegacy && site.Session.Platform == upstream.PlatformSub2API && strings.TrimSpace(legacyKey) != "" {
		return strings.TrimSpace(legacyKey), nil
	}
	return "", requestError(ErrorRequest)
}

func (s *Service) realBindExisting(ctx context.Context, userID string, req RealBindRequest) (RealConnectResponse, error) {
	if strings.TrimSpace(req.UpstreamSiteID) == "" || strings.TrimSpace(req.UpstreamGroupID) == "" || strings.TrimSpace(req.UpstreamKeyID) == "" {
		return RealConnectResponse{}, requestError(ErrorRequest)
	}
	operationID, err := normalizeOperationID(req.OperationID)
	if err != nil {
		return RealConnectResponse{}, err
	}
	connectionCtx, err := s.prepareConnectionContext(ctx, userID, req.UpstreamSiteID, req.UpstreamGroupID, req.UpstreamGroupName, req.GroupType, false)
	if err != nil {
		return RealConnectResponse{}, err
	}
	if existing, err := s.idempotentConnection(ctx, userID, connectionCtx.adminAccountID, operationID); err != nil {
		return RealConnectResponse{}, err
	} else if existing != nil {
		return RealConnectResponse{Connection: publicRealConnection(*existing)}, nil
	}
	if err := s.rejectDuplicateTarget(ctx, userID, connectionCtx.adminAccountID, req.UpstreamSiteID, req.UpstreamGroupID, connectionCtx.groupName); err != nil {
		return RealConnectResponse{}, err
	}

	legacyRequest := strings.TrimSpace(req.AdminGroupID) == "" && strings.TrimSpace(req.AdminResourceID) == ""
	if !legacyRequest && (strings.TrimSpace(req.AdminGroupID) == "" || strings.TrimSpace(req.AdminResourceID) == "") {
		return RealConnectResponse{}, requestError(ErrorRequest)
	}
	_, keys, err := s.listOwnedUpstreamKeys(ctx, userID, connectionCtx.adminAccountID, req.UpstreamSiteID)
	if err != nil {
		return RealConnectResponse{}, err
	}
	key, err := s.resolveExistingCredential(connectionCtx.upstreamSite, keys, req.UpstreamKeyID, req.UpstreamGroupID, connectionCtx.groupName, legacyRequest, req.UpstreamKey)
	if err != nil {
		return RealConnectResponse{}, err
	}

	mode := ProvisioningModeExisting
	adminResourceID := strings.TrimSpace(req.AdminResourceID)
	adminResourceName := ""
	var ownGroupIDs, ownGroupNames []string
	if legacyRequest {
		if len(req.OwnGroupIDs) == 0 {
			return RealConnectResponse{}, requestError(ErrorRequest)
		}
		mode = ProvisioningModeLegacy
		ownGroupIDs, ownGroupNames, err = s.resolveAdminGroups(ctx, connectionCtx.state, req.OwnGroupIDs)
	} else {
		resource, resolvedIDs, resolvedNames, resolveErr := s.resolveExistingAdminResource(ctx, connectionCtx.state, req.AdminGroupID, req.AdminResourceID)
		if resolveErr != nil {
			return RealConnectResponse{}, resolveErr
		}
		adminResourceName = resource.Name
		ownGroupIDs, ownGroupNames = resolvedIDs, resolvedNames
	}
	if err != nil {
		return RealConnectResponse{}, err
	}

	connID, err := randomConnID()
	if err != nil {
		return RealConnectResponse{}, err
	}
	conn := RealConnection{
		ID: connID, UserID: userID, WorkspaceAdminAccountID: connectionCtx.adminAccountID,
		UpstreamSiteID: req.UpstreamSiteID, UpstreamGroupID: req.UpstreamGroupID,
		UpstreamGroupName: connectionCtx.groupName, UpstreamKeyID: strings.TrimSpace(req.UpstreamKeyID),
		UpstreamKey: key, AdminAccountID: adminResourceID, AdminAccountName: adminResourceName,
		OwnGroupIDs: ownGroupIDs, OwnGroupNames: ownGroupNames, GroupType: connectionCtx.groupType,
		ProvisioningMode: mode, Status: ConnectionStatusActive,
		UpstreamPlatform: string(connectionCtx.upstreamSession.Platform), AdminPlatform: string(connectionCtx.state.Session.Platform),
		PricingMappingEnabled: addToPricingMapping(req.AddToPricingMapping), OperationID: operationID,
		CanDeleteRemote: false, CreatedAt: time.Now().Format(time.RFC3339),
	}
	if err := s.persistConnection(ctx, conn); err != nil {
		return RealConnectResponse{}, err
	}
	return RealConnectResponse{Connection: publicRealConnection(conn)}, nil
}

func (s *Service) UpdateRealConnectionGroups(ctx context.Context, userID, connectionID string, req UpdateRealConnectionGroupsRequest) (RealConnection, error) {
	connectionID = strings.TrimSpace(connectionID)
	if connectionID == "" || len(req.OwnGroupIDs) == 0 || s.connRepository == nil {
		return RealConnection{}, requestError(ErrorRequest)
	}
	adminAccountID, err := s.currentAdminAccountID(ctx, userID)
	if err != nil {
		return RealConnection{}, err
	}
	conn, err := s.connRepository.GetRealConnection(ctx, connectionID, userID, adminAccountID)
	if err != nil {
		return RealConnection{}, err
	}
	if conn == nil || strings.TrimSpace(conn.AdminAccountID) == "" {
		return RealConnection{}, requestError(ErrorRequest)
	}
	state, err := s.authenticatedState(ctx, userID, adminAccountID)
	if err != nil {
		return RealConnection{}, err
	}
	groupIDs, groupNames, err := s.resolveAdminGroups(ctx, state, req.OwnGroupIDs)
	if err != nil {
		return RealConnection{}, err
	}
	if string(state.Session.Platform) != conn.AdminPlatform && conn.AdminPlatform != "" {
		return RealConnection{}, requestError(ErrorRequest)
	}
	if sameStringSet(groupIDs, conn.OwnGroupIDs) {
		conn.OwnGroupIDs = groupIDs
		conn.OwnGroupNames = groupNames
		return publicRealConnection(*conn), nil
	}

	switch state.Session.Platform {
	case upstream.PlatformSub2API:
		if err := s.platformService.UpdateSub2APIAdminAccountGroupIDs(state.Session, conn.AdminAccountID, groupIDs); err != nil {
			return RealConnection{}, err
		}
	case upstream.PlatformNewAPI:
		if err := s.platformService.UpdateNewAPIChannelGroupIDs(state.Session, conn.AdminAccountID, groupIDs); err != nil {
			return RealConnection{}, err
		}
	default:
		return RealConnection{}, requestError(ErrorRequest)
	}

	oldNamesByID := make(map[string]string, len(conn.OwnGroupIDs))
	for index, id := range conn.OwnGroupIDs {
		name := id
		if index < len(conn.OwnGroupNames) && strings.TrimSpace(conn.OwnGroupNames[index]) != "" {
			name = conn.OwnGroupNames[index]
		}
		oldNamesByID[id] = name
	}
	newNamesByID := make(map[string]string, len(groupIDs))
	for index, id := range groupIDs {
		newNamesByID[id] = groupNames[index]
	}
	addedNames := make([]string, 0)
	for id, name := range newNamesByID {
		if _, existed := oldNamesByID[id]; !existed {
			addedNames = append(addedNames, name)
		}
	}
	removedNames := make([]string, 0)
	for id, name := range oldNamesByID {
		if _, retained := newNamesByID[id]; !retained {
			removedNames = append(removedNames, name)
		}
	}

	repository, ok := s.connRepository.(RealConnectionGroupUpdater)
	if !ok {
		return RealConnection{}, requestError(ErrorRequest)
	}
	if err := repository.UpdateRealConnectionGroups(ctx, *conn, groupIDs, groupNames, addedNames, removedNames); err != nil {
		return RealConnection{}, err
	}
	conn.OwnGroupIDs = groupIDs
	conn.OwnGroupNames = groupNames
	return publicRealConnection(*conn), nil
}

func sameStringSet(first, second []string) bool {
	if len(first) != len(second) {
		return false
	}
	values := make(map[string]struct{}, len(first))
	for _, value := range first {
		values[value] = struct{}{}
	}
	for _, value := range second {
		if _, ok := values[value]; !ok {
			return false
		}
	}
	return true
}

func (s *Service) realDisconnectConnection(ctx context.Context, userID string, req RealDisconnectRequest) error {
	if strings.TrimSpace(req.ConnectionID) == "" || (req.Mode != "unlink" && req.Mode != "full") || s.connRepository == nil {
		return requestError(ErrorRequest)
	}
	adminAccountID, err := s.currentAdminAccountID(ctx, userID)
	if err != nil {
		return err
	}
	conn, err := s.connRepository.GetRealConnection(ctx, req.ConnectionID, userID, adminAccountID)
	if err != nil {
		return err
	}
	if conn == nil {
		return requestError(ErrorRequest)
	}

	selectedIDs := normalizeDisconnectGroupIDs(req.OwnGroupIDs, conn.OwnGroupIDs)
	if len(req.OwnGroupIDs) > 0 && len(selectedIDs) == 0 {
		return requestError(ErrorRequest)
	}
	if len(selectedIDs) == 0 {
		selectedIDs = append([]string(nil), conn.OwnGroupIDs...)
	}
	selectedSet := make(map[string]struct{}, len(selectedIDs))
	for _, id := range selectedIDs {
		selectedSet[id] = struct{}{}
	}
	remainingIDs := make([]string, 0, len(conn.OwnGroupIDs))
	remainingNames := make([]string, 0, len(conn.OwnGroupNames))
	removedNames := make([]string, 0, len(selectedIDs))
	for index, id := range conn.OwnGroupIDs {
		name := id
		if index < len(conn.OwnGroupNames) && strings.TrimSpace(conn.OwnGroupNames[index]) != "" {
			name = conn.OwnGroupNames[index]
		}
		if _, selected := selectedSet[id]; selected {
			removedNames = append(removedNames, name)
			continue
		}
		remainingIDs = append(remainingIDs, id)
		remainingNames = append(remainingNames, name)
	}
	partial := len(remainingIDs) > 0 && len(removedNames) > 0
	if partial && req.Mode == "full" && !strings.EqualFold(conn.AdminPlatform, string(upstream.PlatformSub2API)) {
		return requestError(ErrorPartialDisconnectUnsupported)
	}

	if req.Mode == "full" {
		legacyManaged := conn.ProvisioningMode == ProvisioningModeLegacy && strings.TrimSpace(conn.AdminAccountID) != ""
		if conn.ProvisioningMode != ProvisioningModeManaged && !legacyManaged {
			return requestError(ErrorManagedDeleteOnly)
		}
		state, err := s.authenticatedState(ctx, userID, adminAccountID)
		if err != nil {
			return err
		}
		upstreamSite, err := s.upstreamLookup.GetSite(ctx, conn.UpstreamSiteID)
		if err != nil || upstreamSite == nil || upstreamSite.Session == nil || upstreamSite.UserID != userID || upstreamSite.AdminAccountID != adminAccountID {
			return requestError(ErrorRequest)
		}
		adminSession := state.Session
		if conn.AdminPlatform != "" && conn.AdminPlatform != string(adminSession.Platform) {
			return requestError(ErrorRequest)
		}
		upstreamSession := *upstreamSite.Session
		if conn.UpstreamPlatform != "" && conn.UpstreamPlatform != string(upstreamSession.Platform) {
			return requestError(ErrorRequest)
		}
		if partial {
			if err := s.platformService.UpdateSub2APIAdminAccountGroupIDs(adminSession, conn.AdminAccountID, remainingIDs); err != nil {
				return err
			}
		} else {
			if err := s.deleteAdminResource(adminSession, conn.AdminAccountID); err != nil {
				return err
			}
			if err := s.deleteUpstreamCredential(upstreamSession, conn.UpstreamKeyID); err != nil {
				return err
			}
		}
	}

	removePricing := conn.PricingMappingEnabled
	if req.RemovePricingMapping != nil {
		removePricing = *req.RemovePricingMapping
	}
	if partial {
		repository, ok := s.connRepository.(PartialRealDisconnectRepository)
		if !ok {
			return requestError(ErrorRequest)
		}
		return repository.PartialDisconnectRealConnection(ctx, *conn, remainingIDs, remainingNames, removedNames, removePricing)
	}
	if repository, ok := s.connRepository.(ScopedRealDisconnectRepository); ok {
		return repository.DeleteRealConnectionWithPricingMapping(ctx, *conn, removePricing)
	}
	if removePricing {
		return s.removeUpstreamMappingAndDeleteConnection(ctx, userID, adminAccountID, req.ConnectionID, conn.UpstreamSiteID, conn.UpstreamGroupName)
	}
	return s.connRepository.DeleteRealConnection(ctx, req.ConnectionID, userID, adminAccountID)
}

func normalizeDisconnectGroupIDs(selected, available []string) []string {
	if len(selected) == 0 {
		return nil
	}
	allowed := make(map[string]struct{}, len(available))
	for _, id := range available {
		allowed[id] = struct{}{}
	}
	result := make([]string, 0, len(selected))
	seen := make(map[string]struct{}, len(selected))
	for _, id := range selected {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		if _, ok := allowed[id]; !ok {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		result = append(result, id)
	}
	return result
}

func publicRealConnection(conn RealConnection) RealConnection {
	conn.UpstreamKey = ""
	conn.OperationID = ""
	conn.CanDeleteRemote = conn.ProvisioningMode == ProvisioningModeManaged ||
		(conn.ProvisioningMode == ProvisioningModeLegacy && conn.AdminAccountID != "")
	return conn
}
