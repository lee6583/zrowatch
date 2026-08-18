package user_management

import (
	"context"
	"errors"
	"strings"
	"testing"

	"transithub/backend/internal/modules/upstream"
)

type fakeRuleRepository struct{ rule Rule }

func (r *fakeRuleRepository) EnsureSchema(context.Context) error { return nil }
func (r *fakeRuleRepository) ListForWorkspace(context.Context, string, string) ([]Rule, error) {
	return []Rule{r.rule}, nil
}
func (r *fakeRuleRepository) ListActive(context.Context) ([]Rule, error) { return []Rule{r.rule}, nil }
func (r *fakeRuleRepository) Get(context.Context, string, string, string) (Rule, error) {
	return r.rule, nil
}
func (r *fakeRuleRepository) Save(_ context.Context, rule Rule) (Rule, error) {
	r.rule = rule
	return rule, nil
}
func (r *fakeRuleRepository) Delete(context.Context, string, string, string) error {
	r.rule = Rule{}
	return nil
}
func (r *fakeRuleRepository) RecordObservation(_ context.Context, _ Rule, balance float64, warningActive, rechargeLatched bool, errorKey string) error {
	r.rule.LastBalance = &balance
	r.rule.WarningActive = warningActive
	r.rule.RechargeLatched = rechargeLatched
	r.rule.LastErrorKey = errorKey
	return nil
}
func (r *fakeRuleRepository) ClaimRecharge(_ context.Context, _ Rule, eventID string) (string, bool, error) {
	if r.rule.RechargeLatched {
		return "", false, nil
	}
	if r.rule.RechargeEventID == "" {
		r.rule.RechargeEventID = eventID
	}
	r.rule.RechargePending = true
	return r.rule.RechargeEventID, true, nil
}
func (r *fakeRuleRepository) CompleteRecharge(_ context.Context, _ Rule, balance float64) error {
	r.rule.RechargePending = false
	r.rule.RechargeEventID = ""
	r.rule.RechargeLatched = true
	r.rule.LastBalance = &balance
	r.rule.LastErrorKey = ""
	r.rule.WarningActive = r.rule.WarningEnabled && r.rule.WarningThreshold != nil && balance <= *r.rule.WarningThreshold
	return nil
}
func (r *fakeRuleRepository) RecordRechargeFailure(_ context.Context, _ Rule, errorKey string) error {
	r.rule.LastErrorKey = errorKey
	return nil
}

type fakeSessionProvider struct{}

func (fakeSessionProvider) RequireSession(context.Context, string, string) (upstream.Session, error) {
	return upstream.Session{Platform: upstream.PlatformSub2API, BaseURL: "https://sub2.example.com", AccessToken: "token"}, nil
}

type fakeSub2Client struct {
	user         upstream.Sub2APIAdminUser
	failRecharge bool
	updateCalls  int
	keys         []string
}

func (c *fakeSub2Client) FetchSub2APIAdminUsersPage(upstream.Session, upstream.Sub2APIAdminUsersQuery) (upstream.Sub2APIAdminUsersPage, error) {
	return upstream.Sub2APIAdminUsersPage{Items: []upstream.Sub2APIAdminUser{c.user}}, nil
}
func (c *fakeSub2Client) FetchSub2APIAdminUser(upstream.Session, string) (upstream.Sub2APIAdminUser, error) {
	return c.user, nil
}
func (c *fakeSub2Client) UpdateSub2APIAdminUserBalance(_ upstream.Session, _ string, amount float64, _ string, key string) (upstream.Sub2APIAdminUser, error) {
	c.updateCalls++
	c.keys = append(c.keys, key)
	if c.failRecharge {
		return upstream.Sub2APIAdminUser{}, errors.New("remote failed")
	}
	value := amount
	if c.user.Balance != nil {
		value += *c.user.Balance
	}
	c.user.Balance = &value
	return c.user, nil
}

type fakeNotifier struct{ messages []string }

func (n *fakeNotifier) NotifyUserBalanceEvent(_ context.Context, _, _ string, message string) {
	n.messages = append(n.messages, message)
}

func testRule() Rule {
	return Rule{UserID: "owner", AdminAccountID: "workspace", UpstreamUserID: "42", Email: "user@example.com"}
}

func ptr(value float64) *float64 { return &value }

func TestBalanceWarningNotifiesOnceAndRearmsAfterRecovery(t *testing.T) {
	rule := testRule()
	rule.WarningEnabled = true
	rule.WarningThreshold = ptr(10)
	repo := &fakeRuleRepository{rule: rule}
	client := &fakeSub2Client{user: upstream.Sub2APIAdminUser{ID: "42", Email: rule.Email, Balance: ptr(5)}}
	notifier := &fakeNotifier{}
	service := NewService(repo, fakeSessionProvider{}, client, notifier)

	service.reconcileOne(context.Background(), rule)
	service.reconcileOne(context.Background(), rule)
	if len(notifier.messages) != 1 || !strings.Contains(notifier.messages[0], "用户余额预警") {
		t.Fatalf("expected one warning, got %#v", notifier.messages)
	}
	client.user.Balance = ptr(11)
	service.reconcileOne(context.Background(), rule)
	client.user.Balance = ptr(4)
	service.reconcileOne(context.Background(), rule)
	if len(notifier.messages) != 2 {
		t.Fatalf("expected warning to rearm, got %d messages", len(notifier.messages))
	}
}

func TestAutoRechargeRunsOncePerLowBalanceEvent(t *testing.T) {
	rule := testRule()
	rule.AutoRechargeEnabled = true
	rule.AutoRechargeThreshold = ptr(10)
	rule.AutoRechargeAmount = ptr(20)
	repo := &fakeRuleRepository{rule: rule}
	client := &fakeSub2Client{user: upstream.Sub2APIAdminUser{ID: "42", Balance: ptr(5)}}
	notifier := &fakeNotifier{}
	service := NewService(repo, fakeSessionProvider{}, client, notifier)

	service.reconcileOne(context.Background(), rule)
	service.reconcileOne(context.Background(), rule)
	if client.updateCalls != 1 {
		t.Fatalf("expected one recharge, got %d", client.updateCalls)
	}
	if len(notifier.messages) != 1 || !strings.Contains(notifier.messages[0], "自动充值成功") {
		t.Fatalf("unexpected messages: %#v", notifier.messages)
	}

	client.user.Balance = ptr(5)
	service.reconcileOne(context.Background(), rule)
	if client.updateCalls != 2 {
		t.Fatalf("expected a new recharge after balance recovered and fell again, got %d", client.updateCalls)
	}
	if client.keys[0] == client.keys[1] {
		t.Fatal("separate low-balance events must use separate idempotency keys")
	}
}

func TestAutoRechargeRetryReusesIdempotencyKey(t *testing.T) {
	rule := testRule()
	rule.AutoRechargeEnabled = true
	rule.AutoRechargeThreshold = ptr(10)
	rule.AutoRechargeAmount = ptr(20)
	repo := &fakeRuleRepository{rule: rule}
	client := &fakeSub2Client{user: upstream.Sub2APIAdminUser{ID: "42", Balance: ptr(5)}, failRecharge: true}
	notifier := &fakeNotifier{}
	service := NewService(repo, fakeSessionProvider{}, client, notifier)

	service.reconcileOne(context.Background(), rule)
	client.failRecharge = false
	service.reconcileOne(context.Background(), rule)
	if client.updateCalls != 2 {
		t.Fatalf("expected retry, got %d calls", client.updateCalls)
	}
	if len(client.keys) != 2 || client.keys[0] != client.keys[1] {
		t.Fatalf("retry keys differ: %#v", client.keys)
	}
	if repo.rule.RechargePending || !repo.rule.RechargeLatched {
		t.Fatalf("unexpected final state: %+v", repo.rule)
	}
}
