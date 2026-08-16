package upstream

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type sessionRecoveryNotifier struct {
	called chan struct{}
}

func (n *sessionRecoveryNotifier) NotifyUpstreamLoginRequired(context.Context, string, string, string, string) {
	select {
	case n.called <- struct{}{}:
	default:
	}
}

func sessionRecoveryMetrics(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/api/v1/auth/me":
		writeJSON(w, map[string]any{"data": map[string]any{"balance": 10.0, "total_recharged": 20.0}})
	case "/api/v1/usage/dashboard/stats":
		writeJSON(w, map[string]any{"data": map[string]any{"total_actual_cost": 1.0}})
	case "/api/v1/usage/stats":
		writeJSON(w, map[string]any{"data": map[string]any{"total_actual_cost": 1.0}})
	case "/api/v1/groups/available":
		writeJSON(w, map[string]any{"data": []map[string]any{{"id": 1, "name": "default", "platform": "openai", "rate_multiplier": 1.0}}})
	case "/api/v1/groups/rates":
		writeJSON(w, map[string]any{"data": map[string]any{}})
	default:
		http.NotFound(w, r)
	}
}

func TestSyncRefreshFailureFallsBackToSavedPassword(t *testing.T) {
	var refreshCalls, loginCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/auth/refresh":
			refreshCalls++
			w.WriteHeader(http.StatusUnauthorized)
		case "/api/v1/auth/login":
			loginCalls++
			var body map[string]string
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode login body: %v", err)
			}
			if body["email"] != "admin@example.com" || body["password"] != "saved-password" {
				t.Fatalf("unexpected relogin body: %#v", body)
			}
			writeJSON(w, map[string]any{"data": map[string]any{
				"access_token": "new-access", "refresh_token": "new-refresh", "expires_in": 3600,
			}})
		default:
			sessionRecoveryMetrics(w, r)
		}
	}))
	defer server.Close()

	gcm := testCredentialGCM(t)
	ciphertext, err := encryptCredentialPassword(gcm, "user-1", "workspace-1", "saved-password")
	if err != nil {
		t.Fatalf("encrypt password: %v", err)
	}
	expiresAt := time.Now().Add(-time.Minute).UnixMilli()
	site := &Site{
		ID: "site-1", UserID: "user-1", AdminAccountID: "workspace-1", Name: "upstream",
		BaseURL: server.URL, Platform: PlatformSub2API, Account: "admin@example.com",
		Status: StatusConnected, Metrics: defaultMetrics(), PasswordCiphertext: ciphertext,
		Session: &Session{Platform: PlatformSub2API, BaseURL: server.URL, AccessToken: "old-access", RefreshToken: "old-refresh", ExpiresAt: &expiresAt},
	}
	cache := newFakeSiteCache()
	cache.add(site)
	repository := &importTestRepository{}
	service := NewService(NewPlatformService(NewHTTPClient(server.Client())), repository, nil, cache)
	service.credentialGCM = gcm

	if _, err := service.sync(context.Background(), site.ID); err != nil {
		t.Fatalf("sync returned error: %v", err)
	}
	updated, err := cache.Get(context.Background(), site.ID)
	if err != nil || updated == nil {
		t.Fatalf("read updated site: site=%+v err=%v", updated, err)
	}
	if refreshCalls != 1 || loginCalls != 1 {
		t.Fatalf("expected one refresh and one password login, got refresh=%d login=%d", refreshCalls, loginCalls)
	}
	if updated.Status != StatusConnected || updated.ErrorKey != nil {
		t.Fatalf("expected recovered connected site, got status=%s error=%v", updated.Status, updated.ErrorKey)
	}
	if updated.Session == nil || updated.Session.AccessToken != "new-access" || updated.Session.RefreshToken != "new-refresh" {
		t.Fatalf("new session was not persisted: %+v", updated.Session)
	}
}

func TestSyncRevokedAccessTokenForcesRefreshBeforePasswordLogin(t *testing.T) {
	var refreshCalls, loginCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/auth/refresh":
			refreshCalls++
			writeJSON(w, map[string]any{"data": map[string]any{
				"access_token": "new-access", "refresh_token": "new-refresh", "expires_in": 3600,
			}})
		case "/api/v1/auth/login":
			loginCalls++
			w.WriteHeader(http.StatusUnauthorized)
		case "/api/v1/auth/me":
			if r.Header.Get("Authorization") == "Bearer old-access" {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			sessionRecoveryMetrics(w, r)
		default:
			sessionRecoveryMetrics(w, r)
		}
	}))
	defer server.Close()

	expiresAt := time.Now().Add(time.Hour).UnixMilli()
	site := &Site{
		ID: "site-1", UserID: "user-1", AdminAccountID: "workspace-1", Name: "upstream",
		BaseURL: server.URL, Platform: PlatformSub2API, Account: "admin@example.com",
		Status: StatusConnected, Metrics: defaultMetrics(),
		Session: &Session{Platform: PlatformSub2API, BaseURL: server.URL, AccessToken: "old-access", RefreshToken: "old-refresh", ExpiresAt: &expiresAt},
	}
	cache := newFakeSiteCache()
	cache.add(site)
	service := NewService(NewPlatformService(NewHTTPClient(server.Client())), &importTestRepository{}, nil, cache)

	if _, err := service.sync(context.Background(), site.ID); err != nil {
		t.Fatalf("sync returned error: %v", err)
	}
	updated, _ := cache.Get(context.Background(), site.ID)
	if refreshCalls != 1 || loginCalls != 0 {
		t.Fatalf("expected forced refresh without password login, got refresh=%d login=%d", refreshCalls, loginCalls)
	}
	if updated == nil || updated.Session == nil || updated.Session.AccessToken != "new-access" {
		t.Fatalf("forced refresh session was not persisted: %+v", updated)
	}
}

func TestSyncReloginFailureNotifiesOnceUntilRecovery(t *testing.T) {
	var loginCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/auth/refresh", "/api/v1/auth/login":
			if r.URL.Path == "/api/v1/auth/login" {
				loginCalls++
			}
			w.WriteHeader(http.StatusUnauthorized)
		default:
			sessionRecoveryMetrics(w, r)
		}
	}))
	defer server.Close()

	gcm := testCredentialGCM(t)
	ciphertext, err := encryptCredentialPassword(gcm, "user-1", "workspace-1", "saved-password")
	if err != nil {
		t.Fatalf("encrypt password: %v", err)
	}
	expiresAt := time.Now().Add(-time.Minute).UnixMilli()
	site := &Site{
		ID: "site-1", UserID: "user-1", AdminAccountID: "workspace-1", Name: "upstream",
		BaseURL: server.URL, Platform: PlatformSub2API, Account: "admin@example.com",
		Status: StatusConnected, Metrics: defaultMetrics(), PasswordCiphertext: ciphertext,
		Session: &Session{Platform: PlatformSub2API, BaseURL: server.URL, AccessToken: "old-access", RefreshToken: "old-refresh", ExpiresAt: &expiresAt},
	}
	cache := newFakeSiteCache()
	cache.add(site)
	service := NewService(NewPlatformService(NewHTTPClient(server.Client())), &importTestRepository{}, nil, cache)
	service.credentialGCM = gcm
	notifier := &sessionRecoveryNotifier{called: make(chan struct{}, 2)}
	service.SetLoginFailureNotifier(notifier)

	if _, err := service.sync(context.Background(), site.ID); err != nil {
		t.Fatalf("first sync returned error: %v", err)
	}
	select {
	case <-notifier.called:
	case <-time.After(time.Second):
		t.Fatal("expected relogin failure notification")
	}

	updated, _ := cache.Get(context.Background(), site.ID)
	if updated == nil || updated.ErrorKey == nil || *updated.ErrorKey != ErrorReloginRequired {
		t.Fatalf("expected persisted relogin-required error, got %+v", updated)
	}
	if _, err := service.sync(context.Background(), site.ID); err != nil {
		t.Fatalf("second sync returned error: %v", err)
	}
	select {
	case <-notifier.called:
		t.Fatal("repeated failure must not send a duplicate notification")
	case <-time.After(100 * time.Millisecond):
	}
	if loginCalls != 2 {
		t.Fatalf("expected each sync to retry password login, got %d attempts", loginCalls)
	}
}

func TestLoginFailureAlertDedupResetsAfterRecovery(t *testing.T) {
	service := NewService(nil, nil, nil, nil)
	if !service.markLoginFailureAlert("site-1") {
		t.Fatal("expected the first failure to request an alert")
	}
	if service.markLoginFailureAlert("site-1") {
		t.Fatal("expected a concurrent failure to be deduplicated")
	}
	service.clearLoginFailureAlert("site-1")
	if !service.markLoginFailureAlert("site-1") {
		t.Fatal("expected a new failure after recovery to request an alert")
	}
}
