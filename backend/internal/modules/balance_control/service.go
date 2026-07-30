package balance_control

import (
	"context"
	"log"
	"strings"
	"sync"

	"transithub/backend/internal/modules/my_sites"
	"transithub/backend/internal/modules/settings"
	"transithub/backend/internal/modules/upstream"
)

type Service struct {
	repository  pauseRepository
	upstream    siteBalanceController
	mySites     connectionProvider
	platform    accountStateController
	profitLocks sync.Map
}

type pauseRepository interface {
	EnsureSchema(ctx context.Context) error
	UpsertPending(ctx context.Context, record PauseRecord) error
	MarkApplied(ctx context.Context, userID, adminAccountID, siteID, remoteAccountID string) error
	SaveError(ctx context.Context, userID, adminAccountID, siteID, remoteAccountID, message string) error
	ListForSite(ctx context.Context, userID, adminAccountID, siteID string, appliedOnly bool) ([]PauseRecord, error)
	Delete(ctx context.Context, userID, adminAccountID, siteID, remoteAccountID string) error
	IsAccountPausedForWorkspace(ctx context.Context, userID, adminAccountID, remoteAccountID string) (bool, error)
	GetProfitCycle(ctx context.Context, userID, adminAccountID, siteID string) (*ProfitCycle, error)
	StartProfitCycle(ctx context.Context, cycle ProfitCycle) error
	AddProfitCycleRecharge(ctx context.Context, userID, adminAccountID, siteID string, amount float64) error
	ListProfitCycleAccounts(ctx context.Context, userID, adminAccountID, siteID string) ([]ProfitCycleAccount, error)
	UpsertProfitCycleAccount(ctx context.Context, account ProfitCycleAccount) error
	FinalizeProfitCycle(ctx context.Context, cycle ProfitCycle, accounts []ProfitCycleAccount) error
}

type siteBalanceController interface {
	GetSite(ctx context.Context, siteID string) (*upstream.Site, error)
	SetBalanceSuspended(ctx context.Context, siteID string, suspended bool, reason string) (*upstream.Site, error)
}

type connectionProvider interface {
	ListRealConnectionsForWorkspace(ctx context.Context, userID, adminAccountID string) ([]my_sites.RealConnection, error)
	RequireSession(ctx context.Context, userID, adminAccountID string) (upstream.Session, error)
}

type accountStateController interface {
	GetSub2APIAdminAccountState(session upstream.Session, accountID string) (upstream.Sub2APIAdminAccountState, error)
	UpdateSub2APIAdminAccountState(session upstream.Session, accountID string, status *string, schedulable *bool) error
	FetchSub2APIAdminAccountUsageStats(session upstream.Session, accountID, startDate, endDate string) (float64, error)
}

type AccountAction struct {
	AccountID string
	Name      string
	Status    string
	Error     string
}

type Result struct {
	Known           bool
	BelowThreshold  bool
	Transition      string
	BalanceCNY      float64
	Threshold       float64
	Paused          []AccountAction
	Restored        []AccountAction
	Skipped         []AccountAction
	Failed          []AccountAction
	PendingRestores int
	Profit          *ProfitReport
}

func NewService(repository *Repository, upstreamService *upstream.Service, mySitesService *my_sites.Service, platform *upstream.PlatformService) *Service {
	return &Service{repository: repository, upstream: upstreamService, mySites: mySitesService, platform: platform}
}

func (s *Service) EnsureSchema(ctx context.Context) error {
	return s.repository.EnsureSchema(ctx)
}

// Reconcile evaluates a successfully fetched balance and applies the durable
// site/account protection state. Remote account failures are returned as data so
// the caller can notify and continue the normal sync pipeline.
func (s *Service) Reconcile(ctx context.Context, userID, adminAccountID, siteID, siteName string, strategy settings.StrategySettings, oldMetrics, metrics upstream.Metrics) Result {
	result := Result{}
	if !strategy.EnableBalanceWarning || metrics.Balance.Value == nil {
		return result
	}
	lock := s.profitSiteLock(userID, adminAccountID, siteID)
	lock.Lock()
	defer lock.Unlock()

	site, err := s.upstream.GetSite(ctx, siteID)
	if err != nil || site == nil {
		if err != nil {
			log.Printf("[balance-control] load site failed site_id=%s err=%v", siteID, err)
		}
		return result
	}
	rate := site.RechargeRate
	if rate <= 0 {
		rate = 1
	}
	result.Known = true
	result.BalanceCNY = *metrics.Balance.Value * rate
	result.Threshold = strategy.DefaultBalanceThreshold
	if site.Settings.BalanceThreshold != nil {
		result.Threshold = *site.Settings.BalanceThreshold
	}
	result.BelowThreshold = result.BalanceCNY < result.Threshold
	s.reconcileProfitCycle(ctx, userID, adminAccountID, siteID, siteName, rate, oldMetrics, metrics, result.BelowThreshold, site.BalanceSuspended, &result)

	if result.BelowThreshold {
		result.Transition = transitionIf(site.BalanceSuspended, "", "paused")
		if !site.BalanceSuspended {
			if _, err := s.upstream.SetBalanceSuspended(ctx, siteID, true, "balance_below_threshold"); err != nil {
				result.Failed = append(result.Failed, AccountAction{Status: "site_state_failed", Error: err.Error()})
				return result
			}
		}
		s.processPause(ctx, userID, adminAccountID, siteID, &result)
		return result
	}

	// A recovered balance clears the visible site pause first. Any account
	// restore failures remain in the durable table and are retried below.
	if site.BalanceSuspended {
		result.Transition = "recovered"
		if _, err := s.upstream.SetBalanceSuspended(ctx, siteID, false, ""); err != nil {
			result.Failed = append(result.Failed, AccountAction{Status: "site_state_failed", Error: err.Error()})
			return result
		}
	}
	s.processRestore(ctx, userID, adminAccountID, siteID, &result)
	return result
}

func transitionIf(condition bool, current, next string) string {
	if condition {
		return current
	}
	return next
}

func (s *Service) processPause(ctx context.Context, userID, adminAccountID, siteID string, result *Result) {
	connections, err := s.mySites.ListRealConnectionsForWorkspace(ctx, userID, adminAccountID)
	if err != nil {
		result.Failed = append(result.Failed, AccountAction{Status: "bindings_failed", Error: err.Error()})
		return
	}
	records, err := s.repository.ListForSite(ctx, userID, adminAccountID, siteID, false)
	if err != nil {
		result.Failed = append(result.Failed, AccountAction{Status: "pause_records_failed", Error: err.Error()})
		return
	}
	recordByAccount := make(map[string]PauseRecord, len(records))
	for _, record := range records {
		recordByAccount[record.AdminAccountIDRemote] = record
	}
	session, err := s.mySites.RequireSession(ctx, userID, adminAccountID)
	if err != nil || session.Platform != upstream.PlatformSub2API {
		if err != nil {
			result.Failed = append(result.Failed, AccountAction{Status: "session_failed", Error: err.Error()})
		}
		return
	}

	seen := make(map[string]struct{})
	for _, conn := range connections {
		if conn.UpstreamSiteID != siteID || conn.AdminPlatform != string(upstream.PlatformSub2API) || strings.TrimSpace(conn.Status) != "" && conn.Status != my_sites.ConnectionStatusActive {
			continue
		}
		accountID := strings.TrimSpace(conn.AdminAccountID)
		if accountID == "" {
			continue
		}
		if _, ok := seen[accountID]; ok {
			continue
		}
		seen[accountID] = struct{}{}
		record, hasRecord := recordByAccount[accountID]
		s.pauseAccount(ctx, session, userID, adminAccountID, siteID, conn, record, hasRecord, result)
	}
}

func (s *Service) pauseAccount(ctx context.Context, session upstream.Session, userID, adminAccountID, siteID string, conn my_sites.RealConnection, existing PauseRecord, hasRecord bool, result *Result) {
	remote, err := s.platform.GetSub2APIAdminAccountState(session, conn.AdminAccountID)
	if err != nil {
		result.Failed = append(result.Failed, AccountAction{AccountID: conn.AdminAccountID, Name: conn.AdminAccountName, Status: "get_failed", Error: err.Error()})
		return
	}
	currentlySchedulable := remote.Schedulable == nil || *remote.Schedulable
	targetAlreadyApplied := (remote.Status == "inactive" || remote.Status == "disabled") && !currentlySchedulable
	if hasRecord && existing.Applied && targetAlreadyApplied {
		result.Skipped = append(result.Skipped, AccountAction{AccountID: conn.AdminAccountID, Name: remote.Name, Status: "already_paused"})
		return
	}
	if hasRecord && !existing.Applied && targetAlreadyApplied {
		if err := s.repository.MarkApplied(ctx, userID, adminAccountID, siteID, conn.AdminAccountID); err != nil {
			result.Failed = append(result.Failed, AccountAction{AccountID: conn.AdminAccountID, Name: remote.Name, Status: "record_applied_failed", Error: err.Error()})
			return
		}
		result.Skipped = append(result.Skipped, AccountAction{AccountID: conn.AdminAccountID, Name: remote.Name, Status: "pause_confirmed"})
		return
	}
	if hasRecord && existing.Applied && !targetAlreadyApplied {
		log.Printf("[balance-control] re-apply account protection site_id=%s account_id=%s", siteID, conn.AdminAccountID)
	}
	if !hasRecord && targetAlreadyApplied {
		// No pause record means this was already stopped by the user or another
		// feature. Never claim ownership of it and never restore it later.
		result.Skipped = append(result.Skipped, AccountAction{AccountID: conn.AdminAccountID, Name: remote.Name, Status: "already_stopped"})
		return
	}
	record := PauseRecord{
		UserID:               userID,
		AdminAccountID:       adminAccountID,
		UpstreamSiteID:       siteID,
		AdminAccountIDRemote: conn.AdminAccountID,
		RealConnectionID:     conn.ID,
		OriginalStatus:       remote.Status,
		OriginalSchedulable:  remote.Schedulable,
	}
	if hasRecord {
		// A retry must restore the state captured before the first attempt, not
		// the partially changed state observed by a later GET.
		record = existing
	}
	if err := s.repository.UpsertPending(ctx, record); err != nil {
		result.Failed = append(result.Failed, AccountAction{AccountID: conn.AdminAccountID, Name: remote.Name, Status: "record_failed", Error: err.Error()})
		return
	}
	status := "inactive"
	schedulable := false
	if err := s.platform.UpdateSub2APIAdminAccountState(session, conn.AdminAccountID, &status, &schedulable); err != nil {
		_ = s.repository.SaveError(ctx, userID, adminAccountID, siteID, conn.AdminAccountID, err.Error())
		result.Failed = append(result.Failed, AccountAction{AccountID: conn.AdminAccountID, Name: remote.Name, Status: "put_failed", Error: err.Error()})
		return
	}
	if err := s.repository.MarkApplied(ctx, userID, adminAccountID, siteID, conn.AdminAccountID); err != nil {
		result.Failed = append(result.Failed, AccountAction{AccountID: conn.AdminAccountID, Name: remote.Name, Status: "record_applied_failed", Error: err.Error()})
		return
	}
	result.Paused = append(result.Paused, AccountAction{AccountID: conn.AdminAccountID, Name: remote.Name, Status: "paused"})
}

func (s *Service) processRestore(ctx context.Context, userID, adminAccountID, siteID string, result *Result) {
	records, err := s.repository.ListForSite(ctx, userID, adminAccountID, siteID, false)
	if err != nil {
		result.Failed = append(result.Failed, AccountAction{Status: "restore_records_failed", Error: err.Error()})
		return
	}
	if len(records) == 0 {
		return
	}
	session, err := s.mySites.RequireSession(ctx, userID, adminAccountID)
	if err != nil || session.Platform != upstream.PlatformSub2API {
		if err != nil {
			result.Failed = append(result.Failed, AccountAction{Status: "restore_session_failed", Error: err.Error()})
		}
		result.PendingRestores = len(records)
		return
	}
	for _, record := range records {
		if !record.Applied {
			// A pending record means the remote PUT and the local state write may
			// have completed in either order. Inspect the remote account before
			// deleting it so a successful PUT cannot leave an account stranded.
			remote, getErr := s.platform.GetSub2APIAdminAccountState(session, record.AdminAccountIDRemote)
			if getErr != nil {
				_ = s.repository.SaveError(ctx, userID, adminAccountID, siteID, record.AdminAccountIDRemote, getErr.Error())
				result.Failed = append(result.Failed, AccountAction{AccountID: record.AdminAccountIDRemote, Status: "restore_get_failed", Error: getErr.Error()})
				result.PendingRestores++
				continue
			}
			currentlySchedulable := remote.Schedulable == nil || *remote.Schedulable
			if (remote.Status != "inactive" && remote.Status != "disabled") || currentlySchedulable {
				_ = s.repository.Delete(ctx, userID, adminAccountID, siteID, record.AdminAccountIDRemote)
				result.Skipped = append(result.Skipped, AccountAction{AccountID: record.AdminAccountIDRemote, Name: remote.Name, Status: "pending_not_applied"})
				continue
			}
			// The remote account is in the protection state, so continue through
			// the same restore path as an applied record.
		}
		status := record.OriginalStatus
		if strings.TrimSpace(status) == "" {
			status = "active"
		}
		if err := s.platform.UpdateSub2APIAdminAccountState(session, record.AdminAccountIDRemote, &status, record.OriginalSchedulable); err != nil {
			_ = s.repository.SaveError(ctx, userID, adminAccountID, siteID, record.AdminAccountIDRemote, err.Error())
			result.Failed = append(result.Failed, AccountAction{AccountID: record.AdminAccountIDRemote, Status: "restore_failed", Error: err.Error()})
			result.PendingRestores++
			continue
		}
		_ = s.repository.Delete(ctx, userID, adminAccountID, siteID, record.AdminAccountIDRemote)
		result.Restored = append(result.Restored, AccountAction{AccountID: record.AdminAccountIDRemote, Status: "restored"})
	}
}

func (s *Service) IsAccountBalancePaused(ctx context.Context, userID, adminAccountID, siteID, accountID string) (bool, error) {
	// Pending records are also protective: a durable pause intent is written
	// before the remote PUT, so health workers must not re-enable an account
	// during a failed or in-flight pause attempt.
	records, err := s.repository.ListForSite(ctx, userID, adminAccountID, siteID, false)
	if err != nil {
		return false, err
	}
	for _, record := range records {
		if record.AdminAccountIDRemote == accountID {
			return true, nil
		}
	}
	return false, nil
}

func (s *Service) IsAccountBalancePausedForWorkspace(ctx context.Context, userID, adminAccountID, accountID string) (bool, error) {
	return s.repository.IsAccountPausedForWorkspace(ctx, userID, adminAccountID, accountID)
}
