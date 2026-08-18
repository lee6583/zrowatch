package user_management

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"math"
	"strings"
	"sync"
	"time"

	"transithub/backend/internal/modules/upstream"
)

const (
	defaultPageSize   = 20
	maxPageSize       = 100
	schedulerInterval = time.Minute
)

type ruleRepository interface {
	EnsureSchema(context.Context) error
	ListForWorkspace(context.Context, string, string) ([]Rule, error)
	ListActive(context.Context) ([]Rule, error)
	Get(context.Context, string, string, string) (Rule, error)
	Save(context.Context, Rule) (Rule, error)
	Delete(context.Context, string, string, string) error
	RecordObservation(context.Context, Rule, float64, bool, bool, string) error
	ClaimRecharge(context.Context, Rule, string) (string, bool, error)
	CompleteRecharge(context.Context, Rule, float64) error
	RecordRechargeFailure(context.Context, Rule, string) error
}

type AdminSessionProvider interface {
	RequireSession(context.Context, string, string) (upstream.Session, error)
}

type Sub2APIClient interface {
	FetchSub2APIAdminUsersPage(upstream.Session, upstream.Sub2APIAdminUsersQuery) (upstream.Sub2APIAdminUsersPage, error)
	FetchSub2APIAdminUser(upstream.Session, string) (upstream.Sub2APIAdminUser, error)
	UpdateSub2APIAdminUserBalance(upstream.Session, string, float64, string, string) (upstream.Sub2APIAdminUser, error)
}

type DingtalkNotifier interface {
	NotifyUserBalanceEvent(context.Context, string, string, string)
}

type Service struct {
	repository  ruleRepository
	sessions    AdminSessionProvider
	client      Sub2APIClient
	notifier    DingtalkNotifier
	reconcileMu sync.Mutex
}

func NewService(repository ruleRepository, sessions AdminSessionProvider, client Sub2APIClient, notifier DingtalkNotifier) *Service {
	return &Service{repository: repository, sessions: sessions, client: client, notifier: notifier}
}

func (s *Service) EnsureSchema(ctx context.Context) error { return s.repository.EnsureSchema(ctx) }

func (s *Service) ListUsers(ctx context.Context, userID, adminAccountID string, query UserQuery) (UsersPage, error) {
	session, err := s.sessions.RequireSession(ctx, userID, adminAccountID)
	if err != nil {
		return UsersPage{}, mapUpstreamError(err)
	}
	page, err := s.client.FetchSub2APIAdminUsersPage(session, upstream.Sub2APIAdminUsersQuery{
		Page: clamp(query.Page, 1, math.MaxInt, 1), PageSize: clamp(query.PageSize, 1, maxPageSize, defaultPageSize),
		Status: strings.TrimSpace(query.Status), Role: strings.TrimSpace(query.Role), Search: strings.TrimSpace(query.Search),
		SortBy: strings.TrimSpace(query.SortBy), SortOrder: strings.TrimSpace(query.SortOrder), Timezone: strings.TrimSpace(query.Timezone),
	})
	if err != nil {
		return UsersPage{}, mapUpstreamError(err)
	}
	rules, err := s.repository.ListForWorkspace(ctx, userID, adminAccountID)
	if err != nil {
		return UsersPage{}, ErrPersistence
	}
	ruleByUser := make(map[string]Rule, len(rules))
	for _, rule := range rules {
		ruleByUser[rule.UpstreamUserID] = rule
	}
	items := make([]UserDTO, 0, len(page.Items))
	for _, item := range page.Items {
		dto := userDTO(item)
		if rule, ok := ruleByUser[item.ID]; ok {
			value := ruleDTO(rule)
			dto.Rule = &value
		}
		items = append(items, dto)
	}
	return UsersPage{Items: items, Total: page.Total, Page: page.Page, PageSize: page.PageSize, Pages: page.Pages}, nil
}

func (s *Service) SaveRule(ctx context.Context, userID, adminAccountID, upstreamUserID string, input RuleInput) (*RuleDTO, error) {
	upstreamUserID = strings.TrimSpace(upstreamUserID)
	if upstreamUserID == "" || validateRuleInput(input) != nil {
		return nil, ErrInvalidRequest
	}
	if !input.WarningEnabled && !input.AutoRechargeEnabled {
		if err := s.repository.Delete(ctx, userID, adminAccountID, upstreamUserID); err != nil {
			return nil, ErrPersistence
		}
		return nil, nil
	}
	session, err := s.sessions.RequireSession(ctx, userID, adminAccountID)
	if err != nil {
		return nil, mapUpstreamError(err)
	}
	remoteUser, err := s.client.FetchSub2APIAdminUser(session, upstreamUserID)
	if err != nil {
		return nil, mapUpstreamError(err)
	}
	rule := Rule{
		UserID: userID, AdminAccountID: adminAccountID, UpstreamUserID: upstreamUserID,
		Email: remoteUser.Email, Username: remoteUser.Username, WarningEnabled: input.WarningEnabled,
		WarningThreshold: input.WarningThreshold, AutoRechargeEnabled: input.AutoRechargeEnabled,
		AutoRechargeThreshold: input.AutoRechargeThreshold, AutoRechargeAmount: input.AutoRechargeAmount,
		LastBalance: remoteUser.Balance,
	}
	saved, err := s.repository.Save(ctx, rule)
	if err != nil {
		return nil, ErrPersistence
	}
	go s.reconcileOne(context.Background(), saved)
	dto := ruleDTO(saved)
	return &dto, nil
}

func (s *Service) DeleteRule(ctx context.Context, userID, adminAccountID, upstreamUserID string) error {
	if strings.TrimSpace(upstreamUserID) == "" {
		return ErrInvalidRequest
	}
	if err := s.repository.Delete(ctx, userID, adminAccountID, strings.TrimSpace(upstreamUserID)); err != nil {
		return ErrPersistence
	}
	return nil
}

func (s *Service) StartScheduler(ctx context.Context) {
	go func() {
		s.reconcileAll(ctx)
		ticker := time.NewTicker(schedulerInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.reconcileAll(ctx)
			}
		}
	}()
}

func (s *Service) reconcileAll(ctx context.Context) {
	rules, err := s.repository.ListActive(ctx)
	if err != nil {
		log.Printf("[user-balance] list rules failed: %v", err)
		return
	}
	for _, rule := range rules {
		if ctx.Err() != nil {
			return
		}
		s.reconcileOne(ctx, rule)
	}
}

func (s *Service) reconcileOne(ctx context.Context, rule Rule) {
	s.reconcileMu.Lock()
	defer s.reconcileMu.Unlock()
	defer func() {
		if recovered := recover(); recovered != nil {
			log.Printf("[user-balance] reconcile panic user=%s: %v", rule.UpstreamUserID, recovered)
		}
	}()
	latest, err := s.repository.Get(ctx, rule.UserID, rule.AdminAccountID, rule.UpstreamUserID)
	if err != nil {
		return
	}
	rule = latest
	if !rule.WarningEnabled && !rule.AutoRechargeEnabled {
		return
	}
	session, err := s.sessions.RequireSession(ctx, rule.UserID, rule.AdminAccountID)
	if err != nil {
		_ = s.repository.RecordRechargeFailure(ctx, rule, errorKey(err))
		return
	}
	remoteUser, err := s.client.FetchSub2APIAdminUser(session, rule.UpstreamUserID)
	if err != nil {
		_ = s.repository.RecordRechargeFailure(ctx, rule, errorKey(err))
		return
	}
	if remoteUser.Balance == nil {
		_ = s.repository.RecordRechargeFailure(ctx, rule, string(ErrUpstreamRequest))
		return
	}
	balance := *remoteUser.Balance
	warningActive := rule.WarningEnabled && rule.WarningThreshold != nil && balance <= *rule.WarningThreshold
	newWarning := warningActive && !rule.WarningActive
	rechargeLatched := rule.RechargeLatched
	if rule.AutoRechargeEnabled && rule.AutoRechargeThreshold != nil && balance > *rule.AutoRechargeThreshold && !rule.RechargePending {
		rechargeLatched = false
	}
	if err := s.repository.RecordObservation(ctx, rule, balance, warningActive, rechargeLatched, ""); err != nil {
		return
	}
	if newWarning {
		s.notify(rule, fmt.Sprintf("【用户余额预警】\n用户：%s\n用户 ID：%s\n当前余额：%.4f\n预警阈值：%.4f\n站点：%s", userLabel(remoteUser), rule.UpstreamUserID, balance, *rule.WarningThreshold, session.BaseURL))
	}
	if !rule.AutoRechargeEnabled || rule.AutoRechargeThreshold == nil || rule.AutoRechargeAmount == nil {
		return
	}
	if balance > *rule.AutoRechargeThreshold {
		if rule.RechargePending {
			_ = s.repository.CompleteRecharge(ctx, rule, balance)
			s.notify(rule, rechargeSuccessMessage(remoteUser, rule, balance, session.BaseURL))
		}
		return
	}
	if rechargeLatched {
		return
	}
	eventID := rule.RechargeEventID
	if eventID == "" {
		eventID = newEventID()
	}
	eventID, claimed, err := s.repository.ClaimRecharge(ctx, rule, eventID)
	if err != nil || !claimed {
		return
	}
	notes := fmt.Sprintf("ZroWatch 自动充值：余额 %.4f 低于阈值 %.4f，充值 %.4f", balance, *rule.AutoRechargeThreshold, *rule.AutoRechargeAmount)
	updated, err := s.client.UpdateSub2APIAdminUserBalance(session, rule.UpstreamUserID, *rule.AutoRechargeAmount, notes, eventID)
	if err != nil {
		key := errorKey(err)
		firstFailure := rule.LastErrorKey == ""
		_ = s.repository.RecordRechargeFailure(ctx, rule, key)
		if firstFailure {
			s.notify(rule, rechargeFailureMessage(remoteUser, rule, balance, session.BaseURL))
		}
		return
	}
	newBalance := balance + *rule.AutoRechargeAmount
	if updated.Balance != nil {
		newBalance = *updated.Balance
	}
	if err := s.repository.CompleteRecharge(ctx, rule, newBalance); err != nil {
		return
	}
	s.notify(rule, rechargeSuccessMessage(remoteUser, rule, newBalance, session.BaseURL))
}

func (s *Service) notify(rule Rule, message string) {
	if s.notifier == nil || strings.TrimSpace(message) == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	s.notifier.NotifyUserBalanceEvent(ctx, rule.UserID, rule.AdminAccountID, message)
}

func validateRuleInput(input RuleInput) error {
	if input.WarningEnabled && (input.WarningThreshold == nil || *input.WarningThreshold < 0 || math.IsNaN(*input.WarningThreshold) || math.IsInf(*input.WarningThreshold, 0)) {
		return ErrInvalidRequest
	}
	if input.AutoRechargeEnabled && (input.AutoRechargeThreshold == nil || *input.AutoRechargeThreshold < 0 || input.AutoRechargeAmount == nil || *input.AutoRechargeAmount <= 0 || math.IsNaN(*input.AutoRechargeThreshold) || math.IsInf(*input.AutoRechargeThreshold, 0) || math.IsNaN(*input.AutoRechargeAmount) || math.IsInf(*input.AutoRechargeAmount, 0)) {
		return ErrInvalidRequest
	}
	return nil
}

func userDTO(user upstream.Sub2APIAdminUser) UserDTO {
	return UserDTO{ID: user.ID, Email: user.Email, Username: user.Username, Role: user.Role, Status: user.Status, Balance: user.Balance, CreatedAt: user.CreatedAt}
}
func ruleDTO(rule Rule) RuleDTO {
	return RuleDTO{WarningEnabled: rule.WarningEnabled, WarningThreshold: rule.WarningThreshold, AutoRechargeEnabled: rule.AutoRechargeEnabled, AutoRechargeThreshold: rule.AutoRechargeThreshold, AutoRechargeAmount: rule.AutoRechargeAmount, WarningActive: rule.WarningActive, RechargePending: rule.RechargePending, LastCheckedAt: rule.LastCheckedAt, LastWarningAt: rule.LastWarningAt, LastRechargeAt: rule.LastRechargeAt, LastErrorKey: rule.LastErrorKey}
}
func userLabel(user upstream.Sub2APIAdminUser) string {
	if strings.TrimSpace(user.Username) != "" {
		return user.Username
	}
	if strings.TrimSpace(user.Email) != "" {
		return user.Email
	}
	return user.ID
}
func rechargeSuccessMessage(user upstream.Sub2APIAdminUser, rule Rule, balance float64, site string) string {
	return fmt.Sprintf("【用户自动充值成功】\n用户：%s\n用户 ID：%s\n充值金额：%.4f\n当前余额：%.4f\n站点：%s", userLabel(user), rule.UpstreamUserID, *rule.AutoRechargeAmount, balance, site)
}
func rechargeFailureMessage(user upstream.Sub2APIAdminUser, rule Rule, balance float64, site string) string {
	return fmt.Sprintf("【用户自动充值失败】\n用户：%s\n用户 ID：%s\n触发余额：%.4f\n计划充值：%.4f\n站点：%s", userLabel(user), rule.UpstreamUserID, balance, *rule.AutoRechargeAmount, site)
}
func clamp(value, min, max, fallback int) int {
	if value < min {
		return fallback
	}
	if value > max {
		return max
	}
	return value
}
func newEventID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("zrowatch-%d", time.Now().UnixNano())
	}
	return "zrowatch-" + hex.EncodeToString(b)
}
func errorKey(err error) string {
	var req *upstream.RequestError
	if errors.As(err, &req) {
		return req.MessageKey
	}
	if err != nil {
		return err.Error()
	}
	return ""
}
func mapUpstreamError(err error) error {
	var req *upstream.RequestError
	if errors.As(err, &req) {
		if req.MessageKey == upstream.ErrorAuth {
			return ErrUpstreamAuth
		}
		return ErrUpstreamRequest
	}
	if err != nil && strings.Contains(err.Error(), "authRequired") {
		return ErrUpstreamAuth
	}
	return ErrUpstreamRequest
}
