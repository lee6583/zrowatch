package settings

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type fakeSettingsRepository struct {
	strategies map[string]StrategySettings
	channels   map[string]NotificationChannelSettings
}

func workspaceKey(userID, adminAccountID string) string {
	return userID + "|" + adminAccountID
}

func (f *fakeSettingsRepository) EnsureSchema(context.Context) error { return nil }

func (f *fakeSettingsRepository) GetFirstStrategy(context.Context) (StrategySettings, error) {
	return StrategySettings{}, nil
}

func (f *fakeSettingsRepository) GetStrategy(_ context.Context, userID string, adminAccountID string) (StrategySettings, error) {
	return f.strategies[workspaceKey(userID, adminAccountID)], nil
}

func (f *fakeSettingsRepository) SaveStrategy(_ context.Context, userID string, adminAccountID string, strategy StrategySettings) error {
	f.strategies[workspaceKey(userID, adminAccountID)] = strategy
	return nil
}

func (f *fakeSettingsRepository) GetNotificationChannels(_ context.Context, userID string, adminAccountID string) (NotificationChannelSettings, error) {
	return f.channels[workspaceKey(userID, adminAccountID)], nil
}

func (f *fakeSettingsRepository) SaveNotificationChannels(_ context.Context, userID string, adminAccountID string, channels NotificationChannelSettings) error {
	f.channels[workspaceKey(userID, adminAccountID)] = channels
	return nil
}

func TestGetStrategyForWorkspaceUsesEventWorkspace(t *testing.T) {
	repo := &fakeSettingsRepository{
		strategies: map[string]StrategySettings{
			workspaceKey("user-1", "workspace-disabled"): {EnableMultiplierAlert: false},
			workspaceKey("user-1", "workspace-enabled"):  {EnableMultiplierAlert: true, MultiplierNotifyBotIDs: []string{"ding-1"}},
		},
	}
	service := &Service{repository: repo, accounts: &fakeAdminAccountResolver{id: "workspace-disabled"}}

	strategy, err := service.GetStrategyForWorkspace(context.Background(), "user-1", "workspace-enabled")
	if err != nil {
		t.Fatalf("get workspace strategy: %v", err)
	}
	if !strategy.EnableMultiplierAlert || len(strategy.MultiplierNotifyBotIDs) != 1 {
		t.Fatalf("expected enabled workspace strategy, got %+v", strategy)
	}
}

func TestSendToBotsForWorkspaceUsesEventWorkspaceChannels(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"errcode":0,"errmsg":"ok"}`))
	}))
	defer server.Close()

	repo := &fakeSettingsRepository{
		channels: map[string]NotificationChannelSettings{
			workspaceKey("user-1", "workspace-disabled"): {
				Dingtalk: []DingtalkChannelSettings{{ID: "other", Name: "other", Enabled: true, Webhook: server.URL}},
			},
			workspaceKey("user-1", "workspace-enabled"): {
				Dingtalk: []DingtalkChannelSettings{{ID: "ding-1", Name: "rate-alert", Enabled: true, Webhook: server.URL}},
			},
		},
	}
	service := &Service{
		client:     server.Client(),
		repository: repo,
		accounts:   &fakeAdminAccountResolver{id: "workspace-disabled"},
	}

	service.SendToBotsForWorkspace(context.Background(), "user-1", "workspace-enabled", []string{"ding-1"}, "rate changed")
	if requests != 1 {
		t.Fatalf("expected one request through event workspace channel, got %d", requests)
	}
}

func TestSendDingtalkRejectsJSONErrorWithHTTP200(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"errcode":310000,"errmsg":"keywords not in content"}`))
	}))
	defer server.Close()

	service := &Service{client: server.Client()}
	if err := service.sendDingtalk(context.Background(), server.URL, "", "rate changed"); err == nil {
		t.Fatal("expected DingTalk JSON error to be returned")
	}
}

func TestNotifyUpstreamLoginRequiredUsesEnabledWorkspaceDingtalkBotsOnly(t *testing.T) {
	requests := 0
	var message string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		body, _ := io.ReadAll(r.Body)
		message = string(body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"errcode":0,"errmsg":"ok"}`))
	}))
	defer server.Close()

	repo := &fakeSettingsRepository{channels: map[string]NotificationChannelSettings{
		workspaceKey("user-1", "workspace-1"): {
			Dingtalk: []DingtalkChannelSettings{
				{ID: "enabled", Name: "enabled", Enabled: true, Webhook: server.URL},
				{ID: "disabled", Name: "disabled", Enabled: false, Webhook: server.URL},
			},
			Feishu: []WebhookChannelSettings{{ID: "feishu", Enabled: true, Webhook: server.URL}},
		},
	}}
	service := &Service{client: server.Client(), repository: repo}

	service.NotifyUpstreamLoginRequired(context.Background(), "user-1", "workspace-1", "Test Site", "https://upstream.example.com")
	if requests != 1 {
		t.Fatalf("expected only one enabled DingTalk request, got %d", requests)
	}
	if !strings.Contains(message, "Test Site") || !strings.Contains(message, "https://upstream.example.com") || !strings.Contains(message, "重新手动登录") {
		t.Fatalf("unexpected notification payload: %s", message)
	}
}
