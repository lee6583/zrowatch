package upstream

import (
	"context"
	"crypto/cipher"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log"
	"strings"
	"sync"
	"time"
)

const (
	persistenceTimeout = 5 * time.Second
)

type SiteRepository interface {
	ListSites(ctx context.Context) ([]Site, error)
	ListSitesForUser(ctx context.Context, userID string) ([]Site, error)
	ListImportCandidates(ctx context.Context, userID, currentAdminAccountID string) ([]ImportCandidate, error)
	SaveSite(ctx context.Context, site Site) error
	DeleteSite(ctx context.Context, userID string, id string) error
}

// LoginFailureNotifier reports a site whose saved session and password login
// are both unusable. Implementations must not return notification failures to
// the sync path.
type LoginFailureNotifier interface {
	NotifyUpstreamLoginRequired(ctx context.Context, userID, adminAccountID, siteName, baseURL string)
}

func (s *Service) SetCredentialEncryptionKey(raw string) error {
	gcm, err := parseCredentialEncryptionKey(raw)
	if err != nil {
		return err
	}
	s.credentialGCM = gcm
	return nil
}

func (s *Service) SetLoginFailureNotifier(notifier LoginFailureNotifier) {
	s.loginFailureNotifier = notifier
}

func (s *Service) ListImportCandidates(ctx context.Context, userID string) ([]ImportCandidate, error) {
	adminAccountID, err := s.requireCurrentAdminAccountID(ctx, userID)
	if err != nil {
		return nil, err
	}
	if s.repository == nil {
		return []ImportCandidate{}, nil
	}
	return s.repository.ListImportCandidates(ctx, userID, adminAccountID)
}

// ImportSites copies only upstream site configuration and credentials. Runtime
// metrics and every downstream binding live outside this copy and are reset.
func (s *Service) ImportSites(ctx context.Context, userID string, dto ImportRequest) (ImportResult, error) {
	s.importMu.Lock()
	defer s.importMu.Unlock()

	result := ImportResult{Imported: []Response{}, Skipped: []ImportSkipped{}}
	if len(dto.SourceSiteIDs) == 0 || len(dto.SourceSiteIDs) > 100 {
		return result, newRequestError(ErrorRequest, "")
	}
	adminAccountID, err := s.requireCurrentAdminAccountID(ctx, userID)
	if err != nil {
		return result, err
	}
	if s.repository == nil {
		return result, newRequestError(ErrorRequest, "")
	}

	sites, err := s.repository.ListSitesForUser(ctx, userID)
	if err != nil {
		return result, err
	}
	sources := make(map[string]Site, len(sites))
	destinationURLs := make(map[string]struct{})
	for _, site := range sites {
		if site.AdminAccountID == adminAccountID {
			destinationURLs[normalizeImportSiteKey(site.BaseURL, site.Platform)] = struct{}{}
			continue
		}
		sources[site.ID] = site
	}

	seen := make(map[string]struct{}, len(dto.SourceSiteIDs))
	for _, rawID := range dto.SourceSiteIDs {
		sourceID := strings.TrimSpace(rawID)
		if sourceID == "" {
			continue
		}
		if _, exists := seen[sourceID]; exists {
			continue
		}
		seen[sourceID] = struct{}{}

		source, exists := sources[sourceID]
		if !exists {
			result.Skipped = append(result.Skipped, ImportSkipped{SourceSiteID: sourceID, Reason: "not_found"})
			continue
		}
		urlKey := normalizeImportSiteKey(source.BaseURL, source.Platform)
		if _, exists := destinationURLs[urlKey]; exists {
			result.Skipped = append(result.Skipped, ImportSkipped{SourceSiteID: sourceID, Reason: "already_imported"})
			continue
		}
		if source.Session == nil || !source.Session.IsAuthenticated() {
			if source.PasswordCiphertext == "" {
				result.Skipped = append(result.Skipped, ImportSkipped{SourceSiteID: sourceID, Reason: "credentials_unavailable"})
				continue
			}
		}

		id, err := randomID()
		if err != nil {
			return result, err
		}
		var session *Session
		if source.Session != nil {
			copiedSession := *source.Session
			if source.Session.ExpiresAt != nil {
				expiresAt := *source.Session.ExpiresAt
				copiedSession.ExpiresAt = &expiresAt
			}
			session = &copiedSession
		}
		passwordCiphertext := source.PasswordCiphertext
		password := ""
		if passwordCiphertext != "" {
			password, err = decryptCredentialPassword(s.credentialGCM, userID, source.AdminAccountID, passwordCiphertext)
			if err != nil {
				result.Skipped = append(result.Skipped, ImportSkipped{SourceSiteID: sourceID, Reason: "credentials_unavailable"})
				continue
			}
			passwordCiphertext, err = encryptCredentialPassword(s.credentialGCM, userID, adminAccountID, password)
			if err != nil {
				result.Skipped = append(result.Skipped, ImportSkipped{SourceSiteID: sourceID, Reason: "credentials_unavailable"})
				continue
			}
		}
		imported := &Site{
			ID:                 id,
			UserID:             userID,
			AdminAccountID:     adminAccountID,
			Name:               source.Name,
			BaseURL:            source.BaseURL,
			Platform:           source.Platform,
			RequestedPlatform:  source.RequestedPlatform,
			Account:            source.Account,
			Remark:             source.Remark,
			RechargeRate:       source.RechargeRate,
			Status:             StatusConnected,
			Metrics:            defaultMetrics(),
			Settings:           source.Settings,
			Session:            session,
			PasswordCiphertext: passwordCiphertext,
		}
		if password != "" && s.platformService != nil {
			if loginResult, loginErr := s.platformService.LoginContext(ctx, source.BaseURL, source.Platform, source.Account, password); loginErr == nil {
				imported.Session = &loginResult.Session
				imported.Metrics = loginResult.Metrics
			} else {
				log.Printf("[upstream] imported site password login failed id=%s key=%s; trying saved session", imported.ID, errorKey(loginErr))
				if imported.Session == nil || !imported.Session.IsAuthenticated() {
					imported.Status = StatusError
					key := errorKey(loginErr)
					imported.ErrorKey = &key
				}
			}
		}
		if err := s.setCachedSite(ctx, imported); err != nil {
			return result, err
		}
		if err := s.saveSite(ctx, imported); err != nil {
			_ = s.cache.Delete(ctx, imported.ID, userID)
			return result, err
		}

		// A site import intentionally does not copy the source workspace's
		// runtime metrics or group-rate snapshots. Populate this workspace's own
		// group list immediately through the normal sync path instead, so the
		// imported site is ready for new bindings as soon as the dialog closes.
		response := toResponse(imported)
		if s.platformService != nil {
			if synced, syncErr := s.sync(ctx, imported.ID); syncErr != nil {
				log.Printf("[upstream] imported site initial sync failed id=%s err=%v", imported.ID, syncErr)
				if current, cacheErr := s.cache.Get(ctx, imported.ID); cacheErr == nil && current != nil {
					response = toResponse(current)
				}
			} else {
				response = synced
			}
		} else {
			s.mu.Lock()
			s.scheduleSyncLocked(imported.ID, imported)
			s.mu.Unlock()
		}
		destinationURLs[urlKey] = struct{}{}
		result.Imported = append(result.Imported, response)
	}
	return result, nil
}

func normalizeImportSiteURL(value string) string {
	return strings.ToLower(strings.TrimRight(strings.TrimSpace(value), "/"))
}

func normalizeImportSiteKey(baseURL string, platform Platform) string {
	return normalizeImportSiteURL(baseURL) + "\x00" + strings.ToLower(strings.TrimSpace(string(platform)))
}

type AdminAccountResolver interface {
	RequireCurrentID(ctx context.Context, userID string) (string, error)
}

// SiteConnectionCleaner removes local bindings and remote resources created
// for an upstream site. It is injected by the assembly layer to keep the
// upstream module independent from my_sites.
type SiteConnectionCleaner interface {
	CleanupSiteConnections(ctx context.Context, userID, adminAccountID, siteID string) error
}

// RefreshConfig 控制后台定时同步行为，由系统设置模块驱动。
type RefreshConfig struct {
	Enabled  bool
	Interval time.Duration
}

// Service 管理上游站点的生命周期（创建、编辑、同步、删除）。
// 站点运行时状态缓存在 Redis（通过 SiteCache），PostgreSQL 负责持久化。
// 当系统设置开启了数据刷新频率时，定时器按配置的间隔自动同步各站点。
type Service struct {
	platformService      *PlatformService
	snapshotWriter       SnapshotWriter
	repository           SiteRepository
	cache                SiteCache
	accounts             AdminAccountResolver
	connectionCleaner    SiteConnectionCleaner
	loginFailureNotifier LoginFailureNotifier
	credentialGCM        cipher.AEAD
	refreshConfig        RefreshConfig
	timers               map[string]*time.Timer
	deletedSites         map[string]struct{}
	loginFailureAlerts   map[string]struct{}
	importMu             sync.Mutex
	mu                   sync.Mutex
	// AfterSync 在站点同步成功后被调用，传入同步前后的指标数据。
	// 由系统设置模块注入，用于余额预警和倍率变更检测。
	AfterSync func(ctx context.Context, userID, adminAccountID, siteID, siteName string, oldMetrics, newMetrics Metrics)
}

func (s *Service) SetAdminAccountResolver(accounts AdminAccountResolver) {
	s.accounts = accounts
}

func (s *Service) SetSiteConnectionCleaner(cleaner SiteConnectionCleaner) {
	s.connectionCleaner = cleaner
}

// requireCurrentAdminAccountID 解析当前工作区 ID，解析失败时返回错误（fail-closed）。
// 面向用户的业务接口必须使用此方法，确保无法解析 workspace 时不会泄露全量数据。
func (s *Service) requireCurrentAdminAccountID(ctx context.Context, userID string) (string, error) {
	if s.accounts == nil {
		return "", newRequestError("admin.adminAccounts.errors.noCurrentAccount", "")
	}
	return s.accounts.RequireCurrentID(ctx, userID)
}

func NewService(platformService *PlatformService, repository SiteRepository, snapshotWriter SnapshotWriter, cache SiteCache) *Service {
	return &Service{
		platformService:    platformService,
		snapshotWriter:     snapshotWriter,
		repository:         repository,
		cache:              cache,
		timers:             make(map[string]*time.Timer),
		deletedSites:       make(map[string]struct{}),
		loginFailureAlerts: make(map[string]struct{}),
	}
}

// SetRefreshConfig 更新后台定时同步配置。
// 开启时为所有有会话的站点启动定时器；关闭时清除所有定时器。
// 由系统设置模块在启动和保存策略时调用。
func (s *Service) SetRefreshConfig(config RefreshConfig) {
	s.mu.Lock()
	defer s.mu.Unlock()
	prev := s.refreshConfig
	s.refreshConfig = config

	if !config.Enabled {
		// 关闭：清除所有定时器。
		for id := range s.timers {
			s.clearTimerLocked(id)
		}
		if prev.Enabled {
			log.Printf("[upstream] 后台定时同步已关闭")
		}
		return
	}

	log.Printf("[upstream] 后台定时同步已开启 interval=%s", config.Interval)

	// 开启或间隔变更：重新调度所有有会话的站点。
	if s.repository == nil {
		return
	}
	sites, err := s.repository.ListSites(context.Background())
	if err != nil {
		log.Printf("[upstream] 无法读取站点列表来调度定时器: %v", err)
		return
	}
	for i := range sites {
		s.scheduleSyncLocked(sites[i].ID, &sites[i])
	}
}

// RestoreSavedSites 从 PostgreSQL 恢复所有站点到 Redis 缓存。
// 先清空 Redis 中的旧数据，确保与数据库完全一致。
func (s *Service) RestoreSavedSites(ctx context.Context) error {
	if s.repository == nil {
		return nil
	}

	// 清空 Redis 旧数据，防止残留 key 与数据库不一致。
	if err := s.cache.Flush(ctx); err != nil {
		return err
	}

	sites, err := s.repository.ListSites(ctx)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range sites {
		if strings.TrimSpace(sites[i].UserID) == "" {
			continue
		}
		site := &sites[i]
		if _, deleted := s.deletedSites[site.ID]; deleted {
			continue
		}
		if err := s.cache.Set(ctx, site); err != nil {
			return err
		}
		s.scheduleSyncLocked(site.ID, site)
	}
	return nil
}

// List 返回指定用户当前工作区的站点列表（从 Redis 缓存读取）。
// fail-closed：无法解析工作区时返回空列表，不泄露跨 workspace 数据。
func (s *Service) List(ctx context.Context, userID string) []Response {
	adminAccountID, err := s.requireCurrentAdminAccountID(ctx, userID)
	if err != nil {
		return nil
	}
	sites, err := s.cache.ListByUser(ctx, userID)
	if err != nil {
		log.Printf("upstream list: cache read failed user_id=%s err=%v", userID, err)
		return nil
	}
	responses := make([]Response, 0, len(sites))
	for _, site := range sites {
		if site.AdminAccountID != adminAccountID {
			continue
		}
		responses = append(responses, toResponse(site))
	}
	return responses
}

// ListForAccount 按指定 adminAccountID 列出站点，供后台调度等跨工作区内部流程使用。
// 与 List 不同：不依赖当前工作区上下文，而是显式传入 adminAccountID。
func (s *Service) ListForAccount(ctx context.Context, userID, adminAccountID string) []Response {
	sites, err := s.cache.ListByUser(ctx, userID)
	if err != nil {
		log.Printf("upstream list-for-account: cache read failed user_id=%s err=%v", userID, err)
		return nil
	}
	responses := make([]Response, 0, len(sites))
	for _, site := range sites {
		if site.AdminAccountID != adminAccountID {
			continue
		}
		responses = append(responses, toResponse(site))
	}
	return responses
}

// FetchGroupDailyStats 获取指定站点的分组每日统计数据。
// 从缓存读取站点会话和分组列表，调用平台 API 获取统计数据。
func (s *Service) FetchGroupDailyStats(ctx context.Context, userID string, id string) ([]GroupDailyStat, error) {
	site, err := s.cache.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if site == nil || site.UserID != userID || site.Session == nil {
		return nil, newRequestError(ErrorNotFound, "")
	}
	aid, err := s.requireCurrentAdminAccountID(ctx, userID)
	if err != nil {
		return nil, err
	}
	if site.AdminAccountID != aid {
		return nil, newRequestError(ErrorNotFound, "")
	}
	session := *site.Session
	groups := append([]GroupInfo(nil), site.Metrics.Groups...)

	refreshedSession, err := s.platformService.RefreshSession(session)
	if err != nil {
		return nil, err
	}
	var stats []GroupDailyStat
	if refreshedSession.Platform == PlatformNewAPI {
		stats, err = s.platformService.FetchNewAPIGroupDailyStats(refreshedSession, groups)
	} else if refreshedSession.Platform == PlatformSub2API {
		stats, err = s.platformService.FetchSub2APIGroupDailyStats(refreshedSession, groups)
	} else {
		return nil, newRequestError(ErrorNotFound, "")
	}
	if err != nil {
		return nil, err
	}

	// 将刷新后的会话写回缓存和数据库。
	site, err = s.cache.Get(ctx, id)
	if err == nil && site != nil && site.UserID == userID {
		site.Session = &refreshedSession
		_ = s.setCachedSite(ctx, site)
		_ = s.saveSite(ctx, site)
	}
	return stats, nil
}

// KeyUsageToday 返回当前工作区所有上游站点中，今天有消费的 key 明细（仪表盘「今日成本」下钻数据源）。
// 只处理有 session 且 rechargeRate > 0 的站点：这与 dashboard.MetricsService.LiveMetrics() 中
// todayPurchase 的统计口径完全一致（rechargeRate <= 0 的站点被整体跳过），确保弹窗总额与卡片数值一致。
// 站点级并发限制 4；任一站点请求上游平台失败即让整个方法返回错误，不允许把失败站点当 0 处理。
func (s *Service) KeyUsageToday(ctx context.Context, userID string) ([]KeyUsageTodayItem, error) {
	adminAccountID, err := s.requireCurrentAdminAccountID(ctx, userID)
	if err != nil {
		return nil, err
	}
	sites, err := s.cache.ListByUser(ctx, userID)
	if err != nil {
		return nil, err
	}

	targets := make([]*Site, 0, len(sites))
	for _, site := range sites {
		if site.AdminAccountID != adminAccountID || site.Session == nil || site.RechargeRate <= 0 {
			continue
		}
		targets = append(targets, site)
	}

	const maxSiteConcurrency = 4
	sem := make(chan struct{}, maxSiteConcurrency)
	var wg sync.WaitGroup
	var mu sync.Mutex
	items := make([]KeyUsageTodayItem, 0)
	var firstErr error
	failedSites := 0
	recordFailure := func(failure error) {
		mu.Lock()
		defer mu.Unlock()
		failedSites++
		if firstErr == nil {
			firstErr = failure
		}
	}

	for _, site := range targets {
		wg.Add(1)
		sem <- struct{}{}
		go func(site *Site) {
			defer wg.Done()
			defer func() { <-sem }()

			session := *site.Session
			groups := append([]GroupInfo(nil), site.Metrics.Groups...)

			refreshedSession, refreshErr := s.platformService.RefreshSession(session)
			if refreshErr != nil {
				recordFailure(refreshErr)
				return
			}

			stats, fetchErr := s.platformService.FetchKeyUsageToday(refreshedSession, groups)
			if fetchErr != nil {
				recordFailure(fetchErr)
				return
			}

			// 将刷新后的会话写回缓存和数据库，与 FetchGroupDailyStats 的写回模式一致。
			if cached, cacheErr := s.cache.Get(ctx, site.ID); cacheErr == nil && cached != nil && cached.UserID == site.UserID {
				cached.Session = &refreshedSession
				_ = s.setCachedSite(ctx, cached)
				_ = s.saveSite(ctx, cached)
			}

			mu.Lock()
			for _, stat := range stats {
				if stat.TodayAmount <= 0 {
					continue
				}
				groupName := strings.TrimSpace(stat.GroupName)
				if groupName == "" {
					groupName = "Ungrouped"
				}
				items = append(items, KeyUsageTodayItem{
					SiteID:       site.ID,
					SiteName:     site.Name,
					Platform:     site.Platform,
					KeyID:        stat.KeyID,
					KeyName:      stat.KeyName,
					GroupName:    groupName,
					TodayAmount:  stat.TodayAmount * site.RechargeRate,
					RawAmount:    stat.TodayAmount,
					RechargeRate: site.RechargeRate,
				})
			}
			mu.Unlock()
		}(site)
	}
	wg.Wait()

	if firstErr != nil {
		return items, &KeyUsageCollectionError{
			FailedSites: failedSites,
			TotalSites:  len(targets),
			Cause:       firstErr,
		}
	}
	return items, nil
}

// BalanceBreakdown 返回当前工作区所有上游站点的余额明细（仪表盘「上游总余额」下钻数据源）。
// 纯读缓存，不触发任何外部平台请求。rechargeRate <= 0 或余额尚未同步成功的站点，
// Balance/RawBalance 返回 nil（前端展示为"未知余额"并排在最后），但站点本身仍会展示。
func (s *Service) BalanceBreakdown(ctx context.Context, userID string) ([]BalanceBreakdownItem, error) {
	adminAccountID, err := s.requireCurrentAdminAccountID(ctx, userID)
	if err != nil {
		return nil, err
	}
	sites, err := s.cache.ListByUser(ctx, userID)
	if err != nil {
		return nil, err
	}

	items := make([]BalanceBreakdownItem, 0, len(sites))
	for _, site := range sites {
		if site.AdminAccountID != adminAccountID {
			continue
		}
		item := BalanceBreakdownItem{
			SiteID:       site.ID,
			SiteName:     site.Name,
			Platform:     site.Platform,
			RechargeRate: site.RechargeRate,
			LastSyncedAt: site.LastSyncedAt,
			Status:       site.Status,
		}
		if site.RechargeRate > 0 && site.Metrics.Balance.Value != nil {
			raw := *site.Metrics.Balance.Value
			balance := raw * site.RechargeRate
			item.RawBalance = &raw
			item.Balance = &balance
		}
		items = append(items, item)
	}
	return items, nil
}

// Create 创建一个新的上游站点。
// 先保存到 Redis 缓存和 PostgreSQL，然后执行平台登录。
// 登录成功后更新站点状态并启动定时同步。
func (s *Service) Create(ctx context.Context, userID string, dto CreateRequest) (Response, error) {
	if err := validateCreate(dto); err != nil {
		return Response{}, err
	}
	// 创建站点必须归属于当前工作区。
	adminAccountID, err := s.requireCurrentAdminAccountID(ctx, userID)
	if err != nil {
		return Response{}, err
	}
	id, err := randomID()
	if err != nil {
		return Response{}, err
	}
	requestedPlatform := dto.Platform
	platform := resolvedPlatform(dto.Platform)
	account := strings.TrimSpace(dto.Account)
	if normalizedAuthMode(dto.AuthMode) == AuthModeUserKey {
		account = strings.TrimSpace(dto.UserID)
	}
	site := &Site{
		ID:                 id,
		UserID:             userID,
		AdminAccountID:     adminAccountID,
		Name:               strings.TrimSpace(dto.Name),
		BaseURL:            strings.TrimSpace(dto.SiteURL),
		Platform:           platform,
		RequestedPlatform:  requestedPlatform,
		Account:            account,
		Remark:             strings.TrimSpace(dto.Remark),
		RechargeRate:       dto.RechargeRate,
		Status:             StatusConnecting,
		ErrorKey:           nil,
		Metrics:            defaultMetrics(),
		LastSyncedAt:       nil,
		Session:            nil,
		PasswordCiphertext: "",
	}

	// 先写入缓存和数据库。
	if normalizedAuthMode(dto.AuthMode) == AuthModePassword && strings.TrimSpace(dto.Password) != "" {
		ciphertext, encryptErr := encryptCredentialPassword(s.credentialGCM, userID, adminAccountID, dto.Password)
		if encryptErr != nil {
			return Response{}, encryptErr
		}
		site.PasswordCiphertext = ciphertext
	}
	if err := s.setCachedSite(ctx, site); err != nil {
		return Response{}, err
	}
	if err := s.saveSite(ctx, site); err != nil {
		_ = s.cache.Delete(ctx, id, userID)
		return Response{}, err
	}

	log.Printf("[upstream] 创建站点登录开始 name=%s url=%s platform=%s", dto.Name, dto.SiteURL, dto.Platform)
	result, loginErr := s.createLogin(ctx, dto)
	if loginErr != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), persistenceTimeout)
			defer cancel()
			_ = s.cache.Delete(cleanupCtx, id, userID)
			_ = s.deleteSite(cleanupCtx, userID, id)
			return Response{}, ctxErr
		}
		log.Printf("[upstream] 创建站点登录失败 name=%s err=%v", dto.Name, loginErr)
		site.Status = StatusError
		key := errorKey(loginErr)
		site.ErrorKey = &key
		response := toResponse(site)
		_ = s.setCachedSite(ctx, site)
		if saveErr := s.saveSite(ctx, site); saveErr != nil {
			return response, saveErr
		}
		return response, nil
	}

	// 登录成功：更新站点状态。
	now := time.Now().UnixMilli()
	site.BaseURL = result.Session.BaseURL
	site.Platform = result.Platform
	site.Session = &result.Session
	site.Metrics = result.Metrics
	if site.BalanceSuspended {
		site.Status = StatusDisabled
	} else {
		site.Status = StatusConnected
	}
	site.ErrorKey = nil
	site.LastSyncedAt = &now

	_ = s.setCachedSite(ctx, site)

	s.mu.Lock()
	s.scheduleSyncLocked(id, site)
	s.mu.Unlock()

	response := toResponse(site)
	if err := s.saveSite(ctx, site); err != nil {
		return response, err
	}
	s.saveSnapshot(ctx, site)
	return response, nil
}

// Update 更新指定站点的配置。
// 如果提供了新的凭证，会重新执行平台登录。
func (s *Service) Update(ctx context.Context, userID string, id string, dto UpdateRequest) (Response, error) {
	if err := validateUpdate(dto); err != nil {
		return Response{}, err
	}

	site, err := s.cache.Get(ctx, id)
	if err != nil {
		return Response{}, err
	}
	if site == nil || site.UserID != userID {
		return Response{}, newRequestError(ErrorNotFound, "")
	}
	// 工作区隔离：站点必须属于当前工作区，否则拒绝操作。
	aid, err := s.requireCurrentAdminAccountID(ctx, userID)
	if err != nil {
		return Response{}, err
	}
	if site.AdminAccountID != aid {
		return Response{}, newRequestError(ErrorNotFound, "")
	}

	// 保存更新前的状态，以便数据库写入失败时回滚。
	previousSite := *site
	site.Name = strings.TrimSpace(dto.Name)
	site.BaseURL = strings.TrimSpace(dto.SiteURL)
	site.RequestedPlatform = dto.Platform
	site.Platform = resolvedPlatform(dto.Platform)
	site.Account = strings.TrimSpace(dto.Account)
	if normalizedAuthMode(dto.AuthMode) == AuthModeUserKey {
		site.Account = strings.TrimSpace(dto.UserID)
	}
	site.Remark = strings.TrimSpace(dto.Remark)
	site.RechargeRate = dto.RechargeRate
	newPassword := dto.Password
	passwordProvided := strings.TrimSpace(newPassword) != ""
	var newPasswordCiphertext string
	if normalizedAuthMode(dto.AuthMode) == AuthModePassword && passwordProvided {
		newPasswordCiphertext, err = encryptCredentialPassword(s.credentialGCM, userID, aid, newPassword)
		if err != nil {
			return Response{}, err
		}
	}
	shouldRelogin := passwordProvided || strings.TrimSpace(dto.AccessToken) != "" || strings.TrimSpace(dto.RefreshToken) != ""

	if shouldRelogin {
		// 先保存基本字段更新到缓存。
		_ = s.setCachedSite(ctx, site)

		log.Printf("[upstream] 更新站点登录开始 id=%s name=%s url=%s", id, dto.Name, dto.SiteURL)
		result, loginErr := s.updateLogin(ctx, dto)
		if loginErr != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				restoreCtx, cancel := context.WithTimeout(context.Background(), persistenceTimeout)
				defer cancel()
				s.restoreSite(restoreCtx, id, &previousSite)
				_ = s.saveSite(restoreCtx, &previousSite)
				return Response{}, ctxErr
			}
			log.Printf("[upstream] 更新站点登录失败 id=%s name=%s err=%v", id, dto.Name, loginErr)
		} else {
			log.Printf("[upstream] 更新站点登录成功 id=%s name=%s", id, dto.Name)
		}
		// 重新读取缓存，确认站点未被删除。
		site, err = s.cache.Get(ctx, id)
		if err != nil || site == nil || site.UserID != userID {
			return Response{}, newRequestError(ErrorNotFound, "")
		}

		if loginErr != nil {
			site.Status = StatusError
			key := errorKey(loginErr)
			site.ErrorKey = &key
			response := toResponse(site)
			_ = s.setCachedSite(ctx, site)
			if saveErr := s.saveSite(ctx, site); saveErr != nil {
				s.restoreSite(ctx, id, &previousSite)
				return response, saveErr
			}
			return response, nil
		}

		now := time.Now().UnixMilli()
		site.BaseURL = result.Session.BaseURL
		site.Platform = result.Platform
		site.Session = &result.Session
		site.Metrics = result.Metrics
		if newPasswordCiphertext != "" {
			site.PasswordCiphertext = newPasswordCiphertext
		}
		if site.BalanceSuspended {
			site.Status = StatusDisabled
		} else {
			site.Status = StatusConnected
		}
		site.ErrorKey = nil
		site.LastSyncedAt = &now

		_ = s.setCachedSite(ctx, site)

		s.mu.Lock()
		s.scheduleSyncLocked(id, site)
		s.mu.Unlock()

		response := toResponse(site)
		if err := s.saveSite(ctx, site); err != nil {
			return response, err
		}
		s.saveSnapshot(ctx, site)
		return response, nil
	}

	// 无需重新登录：仅更新基本字段。
	_ = s.setCachedSite(ctx, site)
	if err := s.saveSite(ctx, site); err != nil {
		s.restoreSite(ctx, id, &previousSite)
		return toResponse(site), err
	}

	// 重新读取确保返回最新状态。
	site, err = s.cache.Get(ctx, id)
	if err != nil || site == nil || site.UserID != userID {
		return Response{}, newRequestError(ErrorNotFound, "")
	}
	return toResponse(site), nil
}

func resolvedPlatform(platform Platform) Platform {
	if platform == PlatformSub2API {
		return PlatformSub2API
	}
	return PlatformNewAPI
}

func (s *Service) createLogin(ctx context.Context, dto CreateRequest) (LoginResult, error) {
	switch normalizedAuthMode(dto.AuthMode) {
	case AuthModeToken:
		return s.platformService.LoginWithTokenContext(ctx, dto.SiteURL, dto.Platform, dto.Account, dto.AccessToken, dto.RefreshToken, dto.TokenType)
	case AuthModeUserKey:
		return s.platformService.LoginWithUserKeyContext(ctx, dto.SiteURL, dto.UserID, dto.AccessToken)
	default:
		return s.platformService.LoginContext(ctx, dto.SiteURL, dto.Platform, dto.Account, dto.Password)
	}
}

func (s *Service) updateLogin(ctx context.Context, dto UpdateRequest) (LoginResult, error) {
	switch normalizedAuthMode(dto.AuthMode) {
	case AuthModeToken:
		return s.platformService.LoginWithTokenContext(ctx, dto.SiteURL, dto.Platform, dto.Account, dto.AccessToken, dto.RefreshToken, dto.TokenType)
	case AuthModeUserKey:
		return s.platformService.LoginWithUserKeyContext(ctx, dto.SiteURL, dto.UserID, dto.AccessToken)
	default:
		return s.platformService.LoginContext(ctx, dto.SiteURL, dto.Platform, dto.Account, dto.Password)
	}
}

func normalizedAuthMode(authMode AuthMode) AuthMode {
	switch authMode {
	case AuthModeToken:
		return AuthModeToken
	case AuthModeUserKey:
		return AuthModeUserKey
	default:
		return AuthModePassword
	}
}

// Sync 手动触发单个站点的同步。
func (s *Service) Sync(ctx context.Context, userID string, id string) (Response, error) {
	site, err := s.cache.Get(ctx, id)
	if err != nil {
		return Response{}, err
	}
	if site == nil || site.UserID != userID {
		return Response{}, newRequestError(ErrorNotFound, "")
	}
	// 工作区隔离：站点必须属于当前工作区，否则拒绝操作。
	aid, err := s.requireCurrentAdminAccountID(ctx, userID)
	if err != nil {
		return Response{}, err
	}
	if site.AdminAccountID != aid {
		return Response{}, newRequestError(ErrorNotFound, "")
	}
	return s.sync(ctx, id)
}

// SyncAll 并发同步指定用户当前工作区的所有站点。
// 有会话的站点并行同步，无会话的直接返回当前状态。
func (s *Service) SyncAll(ctx context.Context, userID string) ([]Response, error) {
	sites, err := s.cache.ListByUser(ctx, userID)
	if err != nil {
		return nil, err
	}

	// 工作区隔离：只同步当前工作区的站点。
	adminAccountID, err := s.requireCurrentAdminAccountID(ctx, userID)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0)
	responses := make([]Response, 0)
	for _, site := range sites {
		if site.AdminAccountID != adminAccountID {
			continue
		}
		if site.Session == nil {
			responses = append(responses, toResponse(site))
			continue
		}
		ids = append(ids, site.ID)
	}

	results := make([]Response, len(ids))
	var wg sync.WaitGroup
	for index, id := range ids {
		wg.Add(1)
		go func(index int, id string) {
			defer wg.Done()
			response, err := s.sync(ctx, id)
			if err != nil {
				// 同步失败时返回缓存中的当前状态。
				if cached, cacheErr := s.cache.Get(ctx, id); cacheErr == nil && cached != nil {
					response = toResponse(cached)
				}
				results[index] = response
				return
			}
			results[index] = response
		}(index, id)
	}
	wg.Wait()
	for _, response := range results {
		if response.ID != "" {
			responses = append(responses, response)
		}
	}
	return responses, nil
}

// SyncAllStream 以 SSE 流方式并发同步所有站点，完成一个推送一个。
// 并发上限 5，每个站点独立同步，结果实时推送给前端。
func (s *Service) SyncAllStream(ctx context.Context, userID string, emit SyncEventCallback) error {
	const maxConcurrency = 5

	sites, err := s.cache.ListByUser(ctx, userID)
	if err != nil {
		return err
	}

	// emit 会写 ResponseWriter，多 goroutine 并发调用需要加锁。
	var emitMu sync.Mutex
	safeEmit := func(event SyncEvent) {
		emitMu.Lock()
		defer emitMu.Unlock()
		emit(event)
	}

	// 工作区隔离：只同步当前工作区的站点。
	adminAccountID, err := s.requireCurrentAdminAccountID(ctx, userID)
	if err != nil {
		return err
	}
	ids := make([]string, 0, len(sites))
	for _, site := range sites {
		if site.AdminAccountID != adminAccountID {
			continue
		}
		if site.Session == nil {
			resp := toResponse(site)
			safeEmit(SyncEvent{Event: SyncEventDone, SiteID: site.ID, Site: &resp})
			continue
		}
		ids = append(ids, site.ID)
	}

	// 有会话的站点并发同步，用 channel 信号量限制并发数。
	sem := make(chan struct{}, maxConcurrency)
	var wg sync.WaitGroup

	for _, id := range ids {
		wg.Add(1)
		sem <- struct{}{}
		go func(id string) {
			defer wg.Done()
			defer func() { <-sem }()

			if ctx.Err() != nil {
				return
			}

			log.Printf("[upstream-stream] 开始同步站点 id=%s", id)
			safeEmit(SyncEvent{Event: SyncEventSyncing, SiteID: id})

			response, syncErr := s.sync(ctx, id)
			if syncErr != nil {
				log.Printf("[upstream-stream] 同步失败 id=%s err=%v", id, syncErr)
				if cached, cacheErr := s.cache.Get(ctx, id); cacheErr == nil && cached != nil {
					response = toResponse(cached)
				}
				key := errorKey(syncErr)
				safeEmit(SyncEvent{Event: SyncEventError, SiteID: id, ErrorKey: key, Site: &response})
			} else {
				log.Printf("[upstream-stream] 同步成功 id=%s", id)
				safeEmit(SyncEvent{Event: SyncEventDone, SiteID: id, Site: &response})
			}
		}(id)
	}

	wg.Wait()
	emit(SyncEvent{Event: SyncEventComplete})
	return nil
}

// sync 执行单个站点的同步流程：刷新会话 → 拉取最新指标 → 更新缓存和数据库。
func (s *Service) sync(ctx context.Context, id string) (Response, error) {
	site, err := s.cache.Get(ctx, id)
	if err != nil {
		return Response{}, err
	}
	if site == nil || site.Session == nil {
		return Response{}, newRequestError(ErrorNotFound, "")
	}

	// 余额暂停站点仍需继续同步余额，因此保留 disabled 状态，避免页面在轮询时闪回
	// 「同步中」或「已连接」。
	wasBalanceSuspended := site.BalanceSuspended
	previousStatus := site.Status
	previousErrorKey := site.ErrorKey
	if !wasBalanceSuspended {
		site.Status = StatusSyncing
	}
	site.ErrorKey = nil
	_ = s.setCachedSite(ctx, site)
	session := *site.Session

	// 刷新会话并拉取指标（无锁操作，可能耗时较长）。Sub2API 会话失效时，
	// 使用安全保存的密码重新登录一次，并用新 token 替换旧会话。
	refreshedSession, metrics, reloginRequired, refreshErr := s.refreshSiteSession(ctx, site, session)
	if ctxErr := ctx.Err(); ctxErr != nil {
		restoreCtx, cancel := context.WithTimeout(context.Background(), persistenceTimeout)
		defer cancel()
		if current, currentErr := s.cache.Get(restoreCtx, id); currentErr == nil && current != nil {
			current.Status = previousStatus
			current.ErrorKey = previousErrorKey
			_ = s.setCachedSite(restoreCtx, current)
			_ = s.saveSite(restoreCtx, current)
		}
		return Response{}, ctxErr
	}

	// 重新读取站点确认仍存在（可能在同步期间被删除）。
	site, err = s.cache.Get(ctx, id)
	if err != nil || site == nil {
		return Response{}, newRequestError(ErrorNotFound, "")
	}

	// 在覆盖前保存旧指标，用于同步后的预警检测。
	oldMetrics := site.Metrics
	if refreshErr == nil && refreshedSession.Platform == PlatformSub2API {
		metrics = preserveConfirmedSub2APIGroupRates(oldMetrics, metrics)
	}

	if refreshErr != nil {
		if wasBalanceSuspended || site.BalanceSuspended {
			site.Status = StatusDisabled
		} else {
			site.Status = StatusError
		}
		key := errorKey(refreshErr)
		if reloginRequired {
			// A single authentication failure can still be a transient proxy or
			// deployment event. Require two consecutive confirmed auth failures
			// before telling the operator that manual login is required.
			if stringPointerValue(previousErrorKey) == ErrorAuth || stringPointerValue(previousErrorKey) == ErrorReloginRequired {
				key = ErrorReloginRequired
			} else {
				key = ErrorAuth
			}
		}
		site.ErrorKey = &key
	} else {
		now := time.Now().UnixMilli()
		site.Session = &refreshedSession
		site.Metrics = metrics
		if site.BalanceSuspended {
			site.Status = StatusDisabled
		} else {
			site.Status = StatusConnected
		}
		site.ErrorKey = nil
		site.LastSyncedAt = &now
	}

	_ = s.setCachedSite(ctx, site)

	s.mu.Lock()
	s.scheduleSyncLocked(id, site)
	s.mu.Unlock()

	response := toResponse(site)
	if saveErr := s.saveSite(ctx, site); saveErr != nil {
		return response, saveErr
	}
	if reloginRequired && stringPointerValue(site.ErrorKey) == ErrorReloginRequired && stringPointerValue(previousErrorKey) != ErrorReloginRequired && s.markLoginFailureAlert(site.ID) {
		s.notifyLoginRequired(site)
	}
	if refreshErr == nil || !reloginRequired {
		s.clearLoginFailureAlert(site.ID)
	}
	if refreshErr == nil {
		s.saveSnapshot(ctx, site)
		if s.AfterSync != nil {
			go s.AfterSync(context.Background(), site.UserID, site.AdminAccountID, site.ID, site.Name, oldMetrics, metrics)
		}
	}
	return response, nil
}

func (s *Service) refreshSiteSession(ctx context.Context, site *Site, session Session) (Session, Metrics, bool, error) {
	refreshAttempted := sub2APIRefreshDue(session)
	refreshedSession, refreshErr := s.platformService.RefreshSessionContext(ctx, session)
	if refreshErr == nil {
		metrics, metricsErr := s.platformService.FetchMetricsContext(ctx, refreshedSession)
		if metricsErr == nil {
			return refreshedSession, metrics, false, nil
		}
		if !isSessionAuthFailure(metricsErr) {
			return refreshedSession, Metrics{}, false, metricsErr
		}
		refreshErr = metricsErr

		// Access tokens may be revoked before their recorded expiry. In that case
		// RefreshSessionContext intentionally skipped the refresh, so force one
		// refresh-token exchange before falling back to the saved password.
		if session.Platform == PlatformSub2API && !refreshAttempted && strings.TrimSpace(refreshedSession.RefreshToken) != "" {
			refreshedSession, refreshErr = s.platformService.refreshSub2APISessionContext(ctx, refreshedSession)
			if refreshErr == nil {
				metrics, metricsErr = s.platformService.FetchMetricsContext(ctx, refreshedSession)
				if metricsErr == nil {
					return refreshedSession, metrics, false, nil
				}
				if !isSessionAuthFailure(metricsErr) {
					return refreshedSession, Metrics{}, false, metricsErr
				}
				refreshErr = metricsErr
			}
		}
	}

	if session.Platform != PlatformSub2API {
		return Session{}, Metrics{}, false, refreshErr
	}
	refreshAuthFailure := isSessionAuthFailure(refreshErr)
	if site == nil || strings.TrimSpace(site.Account) == "" || strings.TrimSpace(site.PasswordCiphertext) == "" {
		return Session{}, Metrics{}, refreshAuthFailure, refreshErr
	}

	password, err := decryptCredentialPassword(s.credentialGCM, site.UserID, site.AdminAccountID, site.PasswordCiphertext)
	if err != nil {
		log.Printf("[upstream] saved password unavailable for automatic relogin site_id=%s key=%s", site.ID, errorKey(err))
		return Session{}, Metrics{}, refreshAuthFailure, err
	}
	loginResult, err := s.platformService.LoginContext(ctx, site.BaseURL, PlatformSub2API, site.Account, password)
	if err != nil {
		log.Printf("[upstream] automatic password relogin failed site_id=%s key=%s", site.ID, errorKey(err))
		return Session{}, Metrics{}, refreshAuthFailure && isSessionAuthFailure(err), err
	}
	log.Printf("[upstream] automatic password relogin succeeded site_id=%s", site.ID)
	return loginResult.Session, loginResult.Metrics, false, nil
}

func sub2APIRefreshDue(session Session) bool {
	if session.Platform != PlatformSub2API || strings.TrimSpace(session.RefreshToken) == "" {
		return false
	}
	return session.ExpiresAt == nil || *session.ExpiresAt-time.Now().UnixMilli() <= refreshSkewMS
}

func isSessionAuthFailure(err error) bool {
	var requestErr *RequestError
	if !errors.As(err, &requestErr) {
		return false
	}
	return requestErr.MessageKey == ErrorAuth || requestErr.StatusCode == 401 || requestErr.StatusCode == 403
}

func stringPointerValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func (s *Service) notifyLoginRequired(site *Site) {
	if s.loginFailureNotifier == nil || site == nil {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		s.loginFailureNotifier.NotifyUpstreamLoginRequired(ctx, site.UserID, site.AdminAccountID, site.Name, site.BaseURL)
	}()
}

// markLoginFailureAlert deduplicates alerts from a timer sync and a manual sync
// that happen at the same time. The persisted error key remains the source of
// truth across restarts; this in-memory marker closes the concurrent window.
func (s *Service) markLoginFailureAlert(siteID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.loginFailureAlerts == nil {
		s.loginFailureAlerts = make(map[string]struct{})
	}
	if _, exists := s.loginFailureAlerts[siteID]; exists {
		return false
	}
	s.loginFailureAlerts[siteID] = struct{}{}
	return true
}

func (s *Service) clearLoginFailureAlert(siteID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.loginFailureAlerts, siteID)
}

// preserveConfirmedSub2APIGroupRates prevents a transient /groups/rates
// failure from replacing the user's effective rate with the public default.
// The rest of the metrics remain fresh so balance monitoring can continue.
func preserveConfirmedSub2APIGroupRates(previous, current Metrics) Metrics {
	previousByID := make(map[string]GroupInfo, len(previous.Groups))
	previousByName := make(map[string]GroupInfo, len(previous.Groups))
	for _, group := range previous.Groups {
		if id := strings.TrimSpace(group.ID); id != "" {
			previousByID[id] = group
		}
		if name := strings.TrimSpace(group.Name); name != "" {
			previousByName[name] = group
		}
	}

	for i := range current.Groups {
		group := &current.Groups[i]
		if !group.EffectiveMultiplierUnverified {
			continue
		}
		confirmed, ok := previousByID[strings.TrimSpace(group.ID)]
		if !ok {
			confirmed, ok = previousByName[strings.TrimSpace(group.Name)]
		}
		if !ok || confirmed.EffectiveMultiplierUnverified || confirmed.Multiplier == nil {
			group.Multiplier = nil
			group.MultiplierDisplay = defaultDisplay
			group.DedicatedMultiplier = nil
			group.DedicatedMultiplierDisplay = ""
			group.HasDedicatedMultiplier = false
			continue
		}

		rate := *confirmed.Multiplier
		group.Multiplier = &rate
		group.MultiplierDisplay = confirmed.MultiplierDisplay
		group.HasDedicatedMultiplier = confirmed.HasDedicatedMultiplier
		if confirmed.DedicatedMultiplier != nil {
			dedicatedRate := *confirmed.DedicatedMultiplier
			group.DedicatedMultiplier = &dedicatedRate
			group.DedicatedMultiplierDisplay = confirmed.DedicatedMultiplierDisplay
		}
	}
	if len(current.Groups) > 0 {
		current.Group = current.Groups[0]
	}
	return current
}

func (s *Service) saveSnapshot(ctx context.Context, site *Site) {
	if s.snapshotWriter == nil {
		return
	}
	if site == nil || strings.TrimSpace(site.UserID) == "" {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := s.snapshotWriter.SaveSiteSnapshot(ctx, site.UserID, site.AdminAccountID, site.ID, site.Name, site.Platform, snapshotGroups(site.Metrics.Groups)); err != nil {
		log.Printf("group rate snapshot failed site_id=%s err=%v", site.ID, err)
	}
}

func snapshotGroups(groups []GroupInfo) []SnapshotGroup {
	snapshots := make([]SnapshotGroup, 0, len(groups))
	for _, group := range groups {
		snapshots = append(snapshots, SnapshotGroup{
			ID:         group.ID,
			Name:       group.Name,
			Platform:   group.Platform,
			Multiplier: group.Multiplier,
		})
	}
	return snapshots
}

// Remove 删除指定站点。
// 先从缓存和定时器中移除，再删除 PostgreSQL 记录。
// 数据库删除失败时回滚缓存。
func (s *Service) Remove(ctx context.Context, userID string, id string) error {
	site, err := s.cache.Get(ctx, id)
	if err != nil {
		return err
	}
	if site == nil || site.UserID != userID {
		return newRequestError(ErrorNotFound, "")
	}
	// 工作区隔离：站点必须属于当前工作区，否则拒绝删除。
	aid, err := s.requireCurrentAdminAccountID(ctx, userID)
	if err != nil {
		return err
	}
	if site.AdminAccountID != aid {
		return newRequestError(ErrorNotFound, "")
	}
	// Keep the site and its session available while remote bindings are removed.
	// A failure leaves the site intact so the user can retry the cleanup.
	if s.connectionCleaner != nil {
		if err := s.connectionCleaner.CleanupSiteConnections(ctx, userID, aid, id); err != nil {
			return err
		}
	}

	s.mu.Lock()
	s.clearTimerLocked(id)
	delete(s.loginFailureAlerts, id)
	s.mu.Unlock()

	removedSite := *site
	if err := s.cache.Delete(ctx, id, userID); err != nil {
		return err
	}

	// 删除数据库记录。失败时把站点还原回缓存。
	if err := s.deleteSite(ctx, userID, id); err != nil {
		s.restoreSite(ctx, id, &removedSite)
		return err
	}
	return nil
}

// CleanupDeletedWorkspaceSites 清理工作区删除后遗留的本地运行时状态。
// 它只停止内存定时器并删除 Redis site cache，不调用任何上游远程删除接口。
func (s *Service) CleanupDeletedWorkspaceSites(ctx context.Context, userID string, siteIDs []string) error {
	ids := make([]string, 0, len(siteIDs))
	seen := make(map[string]struct{}, len(siteIDs))
	for _, id := range siteIDs {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}

	s.mu.Lock()
	for _, id := range ids {
		s.deletedSites[id] = struct{}{}
		s.clearTimerLocked(id)
		delete(s.loginFailureAlerts, id)
	}
	s.mu.Unlock()

	var errs []error
	for _, id := range ids {
		if err := s.cache.Delete(ctx, id, userID); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (s *Service) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id := range s.timers {
		s.clearTimerLocked(id)
	}
}

// scheduleSyncLocked 根据刷新配置为站点调度下一次定时同步。
// 仅在系统设置开启了数据刷新频率时才实际调度，否则为 no-op。
// 调用方必须持有 s.mu 锁。
func (s *Service) scheduleSyncLocked(id string, site *Site) {
	s.clearTimerLocked(id)
	if _, deleted := s.deletedSites[id]; deleted {
		return
	}
	if !s.refreshConfig.Enabled || site == nil || site.Session == nil {
		return
	}
	delay := s.refreshConfig.Interval
	log.Printf("[upstream-timer] 定时同步已调度 id=%s delay=%s", id, delay)
	s.timers[id] = time.AfterFunc(delay, func() {
		log.Printf("[upstream-timer] 定时同步触发 id=%s", id)
		s.sync(context.Background(), id)
	})
}

func (s *Service) clearTimerLocked(id string) {
	if timer := s.timers[id]; timer != nil {
		timer.Stop()
	}
	delete(s.timers, id)
}

func validateCreate(dto CreateRequest) error {
	fields := make([]string, 0)
	if strings.TrimSpace(dto.Name) == "" {
		fields = append(fields, "name")
	}
	if strings.TrimSpace(dto.SiteURL) == "" {
		fields = append(fields, "siteUrl")
	}
	if dto.Platform != PlatformAuto && dto.Platform != PlatformNewAPI && dto.Platform != PlatformSub2API {
		fields = append(fields, "platform")
	}
	if dto.AuthMode != "" && dto.AuthMode != AuthModePassword && dto.AuthMode != AuthModeToken && dto.AuthMode != AuthModeUserKey {
		fields = append(fields, "authMode")
	}
	if normalizedAuthMode(dto.AuthMode) == AuthModeToken && dto.Platform == PlatformNewAPI {
		fields = append(fields, "platform")
	}
	if normalizedAuthMode(dto.AuthMode) == AuthModeUserKey && dto.Platform != PlatformNewAPI {
		fields = append(fields, "platform")
	}
	if normalizedAuthMode(dto.AuthMode) == AuthModePassword && strings.TrimSpace(dto.Account) == "" {
		fields = append(fields, "account")
	}
	if normalizedAuthMode(dto.AuthMode) == AuthModePassword && strings.TrimSpace(dto.Password) == "" {
		fields = append(fields, "password")
	}
	if normalizedAuthMode(dto.AuthMode) == AuthModeToken && strings.TrimSpace(dto.AccessToken) == "" && strings.TrimSpace(dto.RefreshToken) == "" {
		fields = append(fields, "accessToken")
	}
	if normalizedAuthMode(dto.AuthMode) == AuthModeUserKey && strings.TrimSpace(dto.AccessToken) == "" {
		fields = append(fields, "accessToken")
	}
	if normalizedAuthMode(dto.AuthMode) == AuthModeUserKey && strings.TrimSpace(dto.UserID) == "" {
		fields = append(fields, "userId")
	}
	if dto.RechargeRate <= 0 {
		fields = append(fields, "rechargeRate")
	}
	if len(fields) > 0 {
		return invalidBodyError(fields...)
	}
	return nil
}

func validateUpdate(dto UpdateRequest) error {
	fields := make([]string, 0)
	if strings.TrimSpace(dto.Name) == "" {
		fields = append(fields, "name")
	}
	if strings.TrimSpace(dto.SiteURL) == "" {
		fields = append(fields, "siteUrl")
	}
	if dto.Platform != PlatformAuto && dto.Platform != PlatformNewAPI && dto.Platform != PlatformSub2API {
		fields = append(fields, "platform")
	}
	if dto.AuthMode != "" && dto.AuthMode != AuthModePassword && dto.AuthMode != AuthModeToken && dto.AuthMode != AuthModeUserKey {
		fields = append(fields, "authMode")
	}
	if normalizedAuthMode(dto.AuthMode) == AuthModeToken && dto.Platform == PlatformNewAPI {
		fields = append(fields, "platform")
	}
	if normalizedAuthMode(dto.AuthMode) == AuthModeUserKey && dto.Platform != PlatformNewAPI {
		fields = append(fields, "platform")
	}
	if normalizedAuthMode(dto.AuthMode) == AuthModePassword && strings.TrimSpace(dto.Account) == "" {
		fields = append(fields, "account")
	}
	if normalizedAuthMode(dto.AuthMode) == AuthModeToken && strings.TrimSpace(dto.AccessToken) == "" && strings.TrimSpace(dto.RefreshToken) == "" {
		fields = append(fields, "accessToken")
	}
	if normalizedAuthMode(dto.AuthMode) == AuthModeUserKey && strings.TrimSpace(dto.AccessToken) == "" {
		fields = append(fields, "accessToken")
	}
	if normalizedAuthMode(dto.AuthMode) == AuthModeUserKey && strings.TrimSpace(dto.UserID) == "" {
		fields = append(fields, "userId")
	}
	if dto.RechargeRate <= 0 {
		fields = append(fields, "rechargeRate")
	}
	if len(fields) > 0 {
		return invalidBodyError(fields...)
	}
	return nil
}

// UpdateSettings 更新站点级预警覆盖配置，不触发重新登录或同步。
func (s *Service) UpdateSettings(ctx context.Context, userID string, siteID string, dto SiteSettings) (Response, error) {
	site, err := s.cache.Get(ctx, siteID)
	if err != nil {
		return Response{}, err
	}
	if site == nil || site.UserID != userID {
		return Response{}, newRequestError(ErrorNotFound, "")
	}
	// 工作区隔离：站点必须属于当前工作区，否则拒绝操作。
	aid, err := s.requireCurrentAdminAccountID(ctx, userID)
	if err != nil {
		return Response{}, err
	}
	if site.AdminAccountID != aid {
		return Response{}, newRequestError(ErrorNotFound, "")
	}
	site.Settings = dto
	_ = s.setCachedSite(ctx, site)
	if saveErr := s.saveSite(ctx, site); saveErr != nil {
		return Response{}, saveErr
	}
	return toResponse(site), nil
}

// GetSite 根据 ID 获取站点（供 alert 逻辑读取站点级配置）。
func (s *Service) GetSite(ctx context.Context, siteID string) (*Site, error) {
	return s.cache.Get(ctx, siteID)
}

// SetBalanceSuspended persists the automatic balance protection state. It is
// intentionally separate from real_connections.status: a balance pause must
// never look like an unlink operation to mapping and pricing code.
func (s *Service) SetBalanceSuspended(ctx context.Context, siteID string, suspended bool, reason string) (*Site, error) {
	site, err := s.cache.Get(ctx, siteID)
	if err != nil {
		return nil, err
	}
	if site == nil {
		return nil, newRequestError(ErrorNotFound, "")
	}
	updated := *site
	site = &updated

	if suspended {
		now := time.Now().UnixMilli()
		site.BalanceSuspended = true
		site.BalanceSuspendedAt = &now
		site.BalancePauseReason = strings.TrimSpace(reason)
		site.Status = StatusDisabled
	} else {
		site.BalanceSuspended = false
		site.BalanceSuspendedAt = nil
		site.BalancePauseReason = ""
		if site.Status == StatusDisabled {
			site.Status = StatusConnected
		}
	}
	if err := s.saveSite(ctx, site); err != nil {
		return nil, err
	}
	if err := s.setCachedSite(ctx, site); err != nil {
		return nil, err
	}
	return site, nil
}

func toResponse(site *Site) Response {
	return Response{
		ID:                 site.ID,
		UserID:             site.UserID,
		Name:               site.Name,
		BaseURL:            site.BaseURL,
		Platform:           site.Platform,
		RequestedPlatform:  site.RequestedPlatform,
		Account:            site.Account,
		Remark:             site.Remark,
		RechargeRate:       site.RechargeRate,
		Status:             site.Status,
		BalanceSuspended:   site.BalanceSuspended,
		BalanceSuspendedAt: site.BalanceSuspendedAt,
		BalancePauseReason: site.BalancePauseReason,
		ErrorKey:           site.ErrorKey,
		Metrics:            site.Metrics,
		Settings:           site.Settings,
		LastSyncedAt:       site.LastSyncedAt,
	}
}

func (s *Service) saveSite(ctx context.Context, site *Site) error {
	if s.repository == nil || site == nil {
		return nil
	}
	if s.isSiteDeleted(site.ID) {
		return newRequestError(ErrorNotFound, "")
	}
	ctx, cancel := context.WithTimeout(ctx, persistenceTimeout)
	defer cancel()
	return s.repository.SaveSite(ctx, *site)
}

func (s *Service) setCachedSite(ctx context.Context, site *Site) error {
	if site == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, deleted := s.deletedSites[site.ID]; deleted {
		return newRequestError(ErrorNotFound, "")
	}
	return s.cache.Set(ctx, site)
}

func (s *Service) isSiteDeleted(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, deleted := s.deletedSites[id]
	return deleted
}

func (s *Service) deleteSite(ctx context.Context, userID string, id string) error {
	if s.repository == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, persistenceTimeout)
	defer cancel()
	return s.repository.DeleteSite(ctx, userID, id)
}

// restoreSite 将站点回滚到之前的状态（缓存和定时器）。
// 用于数据库写入失败后恢复一致性。
func (s *Service) restoreSite(ctx context.Context, id string, site *Site) {
	if site == nil {
		return
	}
	_ = s.setCachedSite(ctx, site)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.scheduleSyncLocked(id, site)
}

func randomID() (string, error) {
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err != nil {
		return "", errors.New("generate id")
	}
	bytes[6] = (bytes[6] & 0x0f) | 0x40
	bytes[8] = (bytes[8] & 0x3f) | 0x80
	encoded := hex.EncodeToString(bytes)
	return encoded[0:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:32], nil
}
