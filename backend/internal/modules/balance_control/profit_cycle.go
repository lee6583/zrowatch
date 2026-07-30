package balance_control

import (
	"context"
	"fmt"
	"log"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"transithub/backend/internal/modules/my_sites"
	"transithub/backend/internal/modules/upstream"
	apptimezone "transithub/backend/internal/timezone"
)

const (
	profitCycleActive             = "active"
	profitCycleFinalized          = "finalized"
	profitAttributionAssigned     = "assigned"
	profitAttributionUnattributed = "unattributed"
	profitRechargeEpsilon         = 1e-9
)

type ProfitCycle struct {
	UserID              string
	AdminAccountID      string
	UpstreamSiteID      string
	SiteName            string
	Status              string
	RechargeAmountCNY   float64
	DownstreamIncomeCNY *float64
	Complete            bool
	LastError           string
	StartedAt           time.Time
	EndedAt             *time.Time
}

type ProfitCycleAccount struct {
	UserID             string
	AdminAccountID     string
	UpstreamSiteID     string
	RemoteAccountID    string
	UpstreamGroupID    string
	UpstreamGroupName  string
	AttributionStatus  string
	UsageStartDate     string
	BaselineActualCost *float64
	CurrentActualCost  *float64
	BaselineComplete   bool
	LastError          string
	BaselineCapturedAt *time.Time
}

type ProfitGroupIncome struct {
	GroupName string
	Amount    float64
}

type ProfitReport struct {
	CycleFound          bool
	Complete            bool
	RechargeAmountCNY   float64
	DownstreamIncomeCNY float64
	ProfitCNY           float64
	Groups              []ProfitGroupIncome
	UnattributedIncome  float64
	SuccessfulAccounts  int
	FailedAccounts      int
	Reason              string
	StartedAt           *time.Time
	EndedAt             *time.Time
}

type profitAccountDescriptor struct {
	accountID         string
	groupID           string
	groupName         string
	attribution       string
	usageStartDate    string
	crossSiteConflict bool
}

func (s *Service) profitSiteLock(userID, adminAccountID, siteID string) *sync.Mutex {
	key := userID + "|" + adminAccountID + "|" + siteID
	lock, _ := s.profitLocks.LoadOrStore(key, &sync.Mutex{})
	return lock.(*sync.Mutex)
}

func (s *Service) reconcileProfitCycle(
	ctx context.Context,
	userID, adminAccountID, siteID, siteName string,
	rechargeRate float64,
	oldMetrics, newMetrics upstream.Metrics,
	belowThreshold, wasSuspended bool,
	result *Result,
) {
	transitioningToPaused := belowThreshold && !wasSuspended
	cycle, err := s.repository.GetProfitCycle(ctx, userID, adminAccountID, siteID)
	if err != nil {
		log.Printf("[balance-profit] load cycle failed site_id=%s err=%v", siteID, err)
		if transitioningToPaused {
			result.Profit = incompleteProfitReport("storage_failed")
		}
		return
	}

	if rechargeAmount, ok := profitRechargeDelta(oldMetrics, newMetrics, rechargeRate); ok {
		now := time.Now()
		if cycle == nil || cycle.Status != profitCycleActive {
			cycle = &ProfitCycle{
				UserID: userID, AdminAccountID: adminAccountID, UpstreamSiteID: siteID,
				SiteName: siteName, Status: profitCycleActive,
				RechargeAmountCNY: rechargeAmount, StartedAt: now,
			}
			if err := s.repository.StartProfitCycle(ctx, *cycle); err != nil {
				log.Printf("[balance-profit] start cycle failed site_id=%s err=%v", siteID, err)
				if transitioningToPaused {
					result.Profit = incompleteProfitReport("storage_failed")
				}
				return
			}
		} else {
			if err := s.repository.AddProfitCycleRecharge(ctx, userID, adminAccountID, siteID, rechargeAmount); err != nil {
				log.Printf("[balance-profit] add recharge failed site_id=%s err=%v", siteID, err)
				if transitioningToPaused {
					result.Profit = incompleteProfitReport("storage_failed")
				}
				return
			}
			cycle.RechargeAmountCNY += rechargeAmount
		}
		log.Printf("[balance-profit] recharge detected site_id=%s amount_cny=%.4f cycle_total_cny=%.4f", siteID, rechargeAmount, cycle.RechargeAmountCNY)
	}

	if cycle == nil || cycle.Status != profitCycleActive {
		if transitioningToPaused {
			result.Profit = incompleteProfitReport("cycle_missing")
		}
		return
	}

	finalizing := belowThreshold
	captureErr := s.captureProfitAccountBaselines(ctx, userID, adminAccountID, siteID, finalizing)
	if captureErr != nil {
		log.Printf("[balance-profit] capture baselines incomplete site_id=%s err=%v", siteID, captureErr)
	}
	if !finalizing {
		return
	}

	report := s.finalizeProfitCycle(ctx, *cycle, captureErr)
	if transitioningToPaused {
		result.Profit = report
	}
}

func profitRechargeDelta(oldMetrics, newMetrics upstream.Metrics, rechargeRate float64) (float64, bool) {
	if oldMetrics.HistoryRecharge.Value == nil || newMetrics.HistoryRecharge.Value == nil {
		return 0, false
	}
	oldValue := *oldMetrics.HistoryRecharge.Value
	newValue := *newMetrics.HistoryRecharge.Value
	if math.IsNaN(oldValue) || math.IsNaN(newValue) || math.IsInf(oldValue, 0) || math.IsInf(newValue, 0) {
		return 0, false
	}
	delta := newValue - oldValue
	if delta <= profitRechargeEpsilon {
		return 0, false
	}
	if rechargeRate <= 0 || math.IsNaN(rechargeRate) || math.IsInf(rechargeRate, 0) {
		rechargeRate = 1
	}
	amount := delta * rechargeRate
	return amount, amount > profitRechargeEpsilon
}

func (s *Service) captureProfitAccountBaselines(ctx context.Context, userID, adminAccountID, siteID string, finalizing bool) error {
	connections, err := s.mySites.ListRealConnectionsForWorkspace(ctx, userID, adminAccountID)
	if err != nil {
		return err
	}
	descriptors := profitAccountDescriptors(connections, siteID)
	existing, err := s.repository.ListProfitCycleAccounts(ctx, userID, adminAccountID, siteID)
	if err != nil {
		return err
	}
	existingByID := make(map[string]ProfitCycleAccount, len(existing))
	for _, account := range existing {
		existingByID[account.RemoteAccountID] = account
	}
	if len(descriptors) == 0 {
		return nil
	}

	var session upstream.Session
	var sessionErr error
	for _, descriptor := range descriptors {
		if account, ok := existingByID[descriptor.accountID]; ok {
			if account.AttributionStatus != descriptor.attribution ||
				(account.UpstreamGroupID != descriptor.groupID && descriptor.attribution == profitAttributionAssigned) {
				account.AttributionStatus = profitAttributionUnattributed
				account.UpstreamGroupID = ""
				account.UpstreamGroupName = ""
				if err := s.repository.UpsertProfitCycleAccount(ctx, account); err != nil {
					return err
				}
			}
			continue
		}

		account := ProfitCycleAccount{
			UserID: userID, AdminAccountID: adminAccountID, UpstreamSiteID: siteID,
			RemoteAccountID: descriptor.accountID, UpstreamGroupID: descriptor.groupID,
			UpstreamGroupName: descriptor.groupName, AttributionStatus: descriptor.attribution,
			UsageStartDate: descriptor.usageStartDate,
		}
		switch {
		case descriptor.crossSiteConflict:
			account.LastError = "account_bound_to_multiple_sites"
		case descriptor.usageStartDate == "":
			account.LastError = "invalid_connection_date"
		case finalizing:
			account.LastError = "baseline_missing_before_finalize"
		default:
			if sessionErr == nil && session.Platform == "" {
				session, sessionErr = s.mySites.RequireSession(ctx, userID, adminAccountID)
				if sessionErr == nil && session.Platform != upstream.PlatformSub2API {
					sessionErr = fmt.Errorf("unsupported downstream platform: %s", session.Platform)
				}
			}
			if sessionErr != nil {
				account.LastError = "session_unavailable"
			} else {
				baseline, queryErr := s.platform.FetchSub2APIAdminAccountUsageStats(
					session, descriptor.accountID, descriptor.usageStartDate, apptimezone.Today(),
				)
				if queryErr != nil {
					account.LastError = "baseline_query_failed"
				} else {
					now := time.Now()
					account.BaselineActualCost = &baseline
					account.BaselineComplete = true
					account.BaselineCapturedAt = &now
				}
			}
		}
		if err := s.repository.UpsertProfitCycleAccount(ctx, account); err != nil {
			return err
		}
	}
	return nil
}

func profitAccountDescriptors(connections []my_sites.RealConnection, targetSiteID string) []profitAccountDescriptor {
	type accountBuilder struct {
		groups    map[string]string
		sites     map[string]struct{}
		startDate string
	}
	builders := make(map[string]*accountBuilder)
	for _, connection := range connections {
		accountID := strings.TrimSpace(connection.AdminAccountID)
		if accountID == "" || !strings.EqualFold(strings.TrimSpace(connection.AdminPlatform), string(upstream.PlatformSub2API)) ||
			(strings.TrimSpace(connection.Status) != "" && connection.Status != my_sites.ConnectionStatusActive) {
			continue
		}
		builder := builders[accountID]
		if builder == nil {
			builder = &accountBuilder{groups: make(map[string]string), sites: make(map[string]struct{})}
			builders[accountID] = builder
		}
		siteID := strings.TrimSpace(connection.UpstreamSiteID)
		builder.sites[siteID] = struct{}{}
		if siteID != targetSiteID {
			continue
		}
		groupID := strings.TrimSpace(connection.UpstreamGroupID)
		groupName := strings.TrimSpace(connection.UpstreamGroupName)
		groupKey := groupID
		if groupKey == "" {
			groupKey = groupName
		}
		if groupKey != "" {
			if groupName == "" {
				groupName = groupKey
			}
			builder.groups[groupKey] = groupName
		}
		if parsed, parseErr := time.Parse(time.RFC3339, strings.TrimSpace(connection.CreatedAt)); parseErr == nil {
			date := apptimezone.DateAt(parsed)
			if builder.startDate == "" || date < builder.startDate {
				builder.startDate = date
			}
		}
	}

	accountIDs := make([]string, 0, len(builders))
	for accountID, builder := range builders {
		if _, belongs := builder.sites[targetSiteID]; belongs {
			accountIDs = append(accountIDs, accountID)
		}
	}
	sort.Strings(accountIDs)
	descriptors := make([]profitAccountDescriptor, 0, len(accountIDs))
	for _, accountID := range accountIDs {
		builder := builders[accountID]
		descriptor := profitAccountDescriptor{
			accountID: accountID, usageStartDate: builder.startDate,
			attribution:       profitAttributionUnattributed,
			crossSiteConflict: len(builder.sites) > 1,
		}
		if len(builder.groups) == 1 {
			for groupID, groupName := range builder.groups {
				descriptor.groupID = groupID
				descriptor.groupName = groupName
				descriptor.attribution = profitAttributionAssigned
			}
		}
		descriptors = append(descriptors, descriptor)
	}
	return descriptors
}

func (s *Service) finalizeProfitCycle(ctx context.Context, cycle ProfitCycle, captureErr error) *ProfitReport {
	now := time.Now()
	report := &ProfitReport{
		CycleFound: true, Complete: captureErr == nil,
		RechargeAmountCNY: cycle.RechargeAmountCNY,
		StartedAt:         &cycle.StartedAt, EndedAt: &now,
	}
	accounts, err := s.repository.ListProfitCycleAccounts(ctx, cycle.UserID, cycle.AdminAccountID, cycle.UpstreamSiteID)
	if err != nil {
		report.Complete = false
		report.Reason = "storage_failed"
		return report
	}

	var session upstream.Session
	if len(accounts) > 0 {
		session, err = s.mySites.RequireSession(ctx, cycle.UserID, cycle.AdminAccountID)
		if err != nil || session.Platform != upstream.PlatformSub2API {
			report.Complete = false
			report.Reason = "session_unavailable"
		}
	}
	groupIncome := make(map[string]float64)
	for index := range accounts {
		account := &accounts[index]
		if !account.BaselineComplete || account.BaselineActualCost == nil {
			report.FailedAccounts++
			report.Complete = false
			continue
		}
		if err != nil || session.Platform != upstream.PlatformSub2API {
			account.LastError = "session_unavailable"
			report.FailedAccounts++
			continue
		}
		current, queryErr := s.platform.FetchSub2APIAdminAccountUsageStats(
			session, account.RemoteAccountID, account.UsageStartDate, apptimezone.Today(),
		)
		if queryErr != nil {
			account.LastError = "current_query_failed"
			report.FailedAccounts++
			report.Complete = false
			continue
		}
		account.CurrentActualCost = &current
		if current+profitRechargeEpsilon < *account.BaselineActualCost {
			account.LastError = "usage_counter_decreased"
			report.FailedAccounts++
			report.Complete = false
			continue
		}
		delta := current - *account.BaselineActualCost
		if delta < 0 {
			delta = 0
		}
		report.SuccessfulAccounts++
		report.DownstreamIncomeCNY += delta
		if account.AttributionStatus == profitAttributionAssigned && strings.TrimSpace(account.UpstreamGroupName) != "" {
			groupIncome[account.UpstreamGroupName] += delta
		} else {
			report.UnattributedIncome += delta
		}
	}
	if !report.Complete && report.Reason == "" {
		report.Reason = "account_stats_incomplete"
	}
	report.ProfitCNY = report.DownstreamIncomeCNY - report.RechargeAmountCNY
	for groupName, amount := range groupIncome {
		report.Groups = append(report.Groups, ProfitGroupIncome{GroupName: groupName, Amount: amount})
	}
	sort.Slice(report.Groups, func(i, j int) bool {
		if report.Groups[i].Amount == report.Groups[j].Amount {
			return report.Groups[i].GroupName < report.Groups[j].GroupName
		}
		return report.Groups[i].Amount > report.Groups[j].Amount
	})

	cycle.Status = profitCycleFinalized
	cycle.DownstreamIncomeCNY = &report.DownstreamIncomeCNY
	cycle.Complete = report.Complete
	cycle.LastError = report.Reason
	cycle.EndedAt = &now
	if err := s.repository.FinalizeProfitCycle(ctx, cycle, accounts); err != nil {
		log.Printf("[balance-profit] finalize persistence failed site_id=%s err=%v", cycle.UpstreamSiteID, err)
		report.Complete = false
		report.Reason = "storage_failed"
	}
	return report
}

func incompleteProfitReport(reason string) *ProfitReport {
	return &ProfitReport{Complete: false, Reason: reason}
}
