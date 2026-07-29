package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func adminTestConfig(t *testing.T) Config {
	t.Helper()
	root := t.TempDir()
	return Config{
		Bark: BarkConfig{Server: "https://api.day.app"},
		Server: ServerConfig{
			AdminUsername:      "saevio",
			AdminPassword:      "unit-test-admin-password",
			AdminSessionHours:  8,
			DataPath:           filepath.Join(root, "subscriptions.json"),
			AuditPath:          filepath.Join(root, "audit"),
			AuditRetentionDays: 14,
		},
		Wolfx: WolfxConfig{HealthStaleSeconds: 180},
	}
}

func adminTestHandler(t *testing.T, cfg Config) (http.Handler, *Store, *RuntimeHealth) {
	t.Helper()
	store, err := NewStore(cfg.Server.DataPath)
	if err != nil {
		t.Fatal(err)
	}
	health := &RuntimeHealth{}
	return newHTTPHandler(cfg, store, NewAlertCache(time.Hour, 100), NewNotifier(cfg.Bark), health), store, health
}

func adminRequest(method, target string, body []byte, cfg Config) *http.Request {
	req := httptest.NewRequest(method, target, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(cfg.Server.AdminUsername, cfg.Server.AdminPassword)
	return req
}

func TestAdminLoginUsesSignedHTTPOnlySession(t *testing.T) {
	cfg := adminTestConfig(t)
	handler, _, _ := adminTestHandler(t, cfg)

	unauthorized := httptest.NewRecorder()
	handler.ServeHTTP(unauthorized, httptest.NewRequest(http.MethodGet, "/api/admin/overview", nil))
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized status=%d body=%s", unauthorized.Code, unauthorized.Body.String())
	}

	badBody := []byte(`{"username":"saevio","password":"wrong"}`)
	bad := httptest.NewRecorder()
	handler.ServeHTTP(bad, httptest.NewRequest(http.MethodPost, "/api/admin/login", bytes.NewReader(badBody)))
	if bad.Code != http.StatusUnauthorized {
		t.Fatalf("bad login status=%d body=%s", bad.Code, bad.Body.String())
	}

	loginBody, _ := json.Marshal(map[string]string{"username": cfg.Server.AdminUsername, "password": cfg.Server.AdminPassword})
	loginReq := httptest.NewRequest(http.MethodPost, "/api/admin/login", bytes.NewReader(loginBody))
	loginReq.Header.Set("X-Forwarded-Proto", "https")
	login := httptest.NewRecorder()
	handler.ServeHTTP(login, loginReq)
	if login.Code != http.StatusOK {
		t.Fatalf("login status=%d body=%s", login.Code, login.Body.String())
	}
	result := login.Result()
	cookies := result.Cookies()
	if len(cookies) != 1 || cookies[0].Name != adminSessionCookie || !cookies[0].HttpOnly || !cookies[0].Secure || cookies[0].SameSite != http.SameSiteStrictMode {
		t.Fatalf("unexpected admin session cookie: %#v", cookies)
	}

	overviewReq := httptest.NewRequest(http.MethodGet, "/api/admin/overview", nil)
	overviewReq.AddCookie(cookies[0])
	overview := httptest.NewRecorder()
	handler.ServeHTTP(overview, overviewReq)
	if overview.Code != http.StatusOK || strings.Contains(overview.Body.String(), cfg.Server.AdminPassword) {
		t.Fatalf("authorized overview status=%d body=%s", overview.Code, overview.Body.String())
	}
}

func TestAdminPageNeverEmbedsConfiguredCredentials(t *testing.T) {
	cfg := adminTestConfig(t)
	handler, _, _ := adminTestHandler(t, cfg)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/admin", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("admin page status=%d", recorder.Code)
	}
	body := recorder.Body.String()
	if strings.Contains(body, cfg.Server.AdminPassword) || !strings.Contains(body, "EEW 管理后台") || !strings.Contains(body, "/api/admin/login") {
		t.Fatalf("unexpected admin page body")
	}
}

func TestAdminBatchSubscriptionsValidateAllBeforeWrite(t *testing.T) {
	cfg := adminTestConfig(t)
	cfg.Server.SubscriptionPaused = true
	handler, store, _ := adminTestHandler(t, cfg)
	valid := Subscription{
		BarkID: "valid-key", BarkServer: "https://api.day.app",
		Locations:   []SubscriptionLocation{{Name: "成都", Latitude: 30.5728, Longitude: 104.0668}},
		NotifyRules: defaultNotificationRules(), NotifyBands: defaultNotificationBands(),
	}
	invalid := valid
	invalid.BarkID = "invalid key"
	payload, _ := json.Marshal(adminBatchSubscriptionsRequest{Subscriptions: []Subscription{valid, invalid}})
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, adminRequest(http.MethodPost, "/api/admin/subscriptions/batch", payload, cfg))
	if recorder.Code != http.StatusBadRequest || store.Count() != 0 {
		t.Fatalf("batch validation status=%d count=%d body=%s", recorder.Code, store.Count(), recorder.Body.String())
	}

	second := valid
	second.BarkID = "second-key"
	payload, _ = json.Marshal(adminBatchSubscriptionsRequest{Subscriptions: []Subscription{valid, second}})
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, adminRequest(http.MethodPost, "/api/admin/subscriptions/batch", payload, cfg))
	if recorder.Code != http.StatusOK || store.Count() != 2 {
		t.Fatalf("batch create status=%d count=%d body=%s", recorder.Code, store.Count(), recorder.Body.String())
	}
}

func TestAdminBatchDoesNotOverwriteExistingSubscriptionByDefault(t *testing.T) {
	cfg := adminTestConfig(t)
	handler, store, _ := adminTestHandler(t, cfg)
	original := Subscription{
		BarkID: "existing-key", BarkServer: "https://api.day.app", LocationName: "原地点", Latitude: 30, Longitude: 104,
		NotifyRules: defaultNotificationRules(), NotifyBands: defaultNotificationBands(),
	}
	if err := store.Upsert(original); err != nil {
		t.Fatal(err)
	}
	replacement := original
	replacement.LocationName = "新地点"
	replacement.Locations = []SubscriptionLocation{{Name: "新地点", Latitude: 31, Longitude: 105}}
	payload, _ := json.Marshal(adminBatchSubscriptionsRequest{Subscriptions: []Subscription{replacement}})
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, adminRequest(http.MethodPost, "/api/admin/subscriptions/batch", payload, cfg))
	stored, _ := store.Get(original.BarkID)
	if recorder.Code != http.StatusConflict || stored.LocationName != "原地点" {
		t.Fatalf("overwrite guard status=%d stored=%#v body=%s", recorder.Code, stored, recorder.Body.String())
	}
}

func TestAdminCanListAndBatchDeleteSubscriptions(t *testing.T) {
	cfg := adminTestConfig(t)
	handler, store, _ := adminTestHandler(t, cfg)
	for _, key := range []string{"alpha-key", "beta-key"} {
		if err := store.Upsert(Subscription{BarkID: key, BarkServer: "https://api.day.app", LocationName: "成都", Latitude: 30, Longitude: 104, NotifyRules: defaultNotificationRules()}); err != nil {
			t.Fatal(err)
		}
	}
	list := httptest.NewRecorder()
	handler.ServeHTTP(list, adminRequest(http.MethodGet, "/api/admin/subscriptions?q=alpha", nil, cfg))
	if list.Code != http.StatusOK || !strings.Contains(list.Body.String(), "alpha-key") || strings.Contains(list.Body.String(), "beta-key") {
		t.Fatalf("filtered list status=%d body=%s", list.Code, list.Body.String())
	}
	payload, _ := json.Marshal(adminBatchDeleteRequest{BarkIDs: []string{"alpha-key", "missing-key"}})
	deleted := httptest.NewRecorder()
	handler.ServeHTTP(deleted, adminRequest(http.MethodPost, "/api/admin/subscriptions/delete", payload, cfg))
	if deleted.Code != http.StatusOK || store.Count() != 1 || !strings.Contains(deleted.Body.String(), `"deleted":1`) {
		t.Fatalf("batch delete status=%d count=%d body=%s", deleted.Code, store.Count(), deleted.Body.String())
	}
}

func TestAdminAuditAPIReadsSummaryAndMaskedDetails(t *testing.T) {
	cfg := adminTestConfig(t)
	handler, _, _ := adminTestHandler(t, cfg)
	now := time.Now()
	event := Event{EventID: "audit-test", ReportNum: 2, Type: "sc_eew", OriginTime: now, Magnitude: 5.1}
	sub := Subscription{BarkID: "sensitive-bark-key", BarkServer: "https://api.day.app", LocationName: "成都", Latitude: 30, Longitude: 104}
	record := deliveryAuditRecordForTarget(cfg, event, sub, Decision{EstimatedIntensity: 3.2}, now, now, "pushed", "", "critical", 120*time.Millisecond, 0, nil)
	if err := writeDeliveryAudit(cfg, event, now, now, now.Add(time.Second), 1, 1, 0, 1, 0, []deliveryAuditRecord{record}); err != nil {
		t.Fatal(err)
	}
	list := httptest.NewRecorder()
	handler.ServeHTTP(list, adminRequest(http.MethodGet, "/api/admin/audits", nil, cfg))
	if list.Code != http.StatusOK || !strings.Contains(list.Body.String(), "audit-test") || strings.Contains(list.Body.String(), cfg.Server.AuditPath) {
		t.Fatalf("audit list status=%d body=%s", list.Code, list.Body.String())
	}
	detail := httptest.NewRecorder()
	handler.ServeHTTP(detail, adminRequest(http.MethodGet, "/api/admin/audit?id="+auditFileBase(event)+"&q="+sub.BarkID, nil, cfg))
	if detail.Code != http.StatusOK || !strings.Contains(detail.Body.String(), maskKey(sub.BarkID)) || strings.Contains(detail.Body.String(), sub.BarkID) {
		t.Fatalf("audit detail status=%d body=%s", detail.Code, detail.Body.String())
	}
}

func TestAdminOverviewReportsRuntimeChecks(t *testing.T) {
	cfg := adminTestConfig(t)
	handler, _, health := adminTestHandler(t, cfg)
	now := time.Now()
	health.SetWebSocketConnected(now)
	health.SetWebSocketMessage(now)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, adminRequest(http.MethodGet, "/api/admin/overview", nil, cfg))
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), `"data_source_healthy":true`) || !strings.Contains(recorder.Body.String(), `"checks"`) {
		t.Fatalf("overview status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}
