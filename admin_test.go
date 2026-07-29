package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
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
	for _, required := range []string{
		"EEW 管理后台",
		"/api/admin/login",
		`class="brand-logo" src="/eew-favicon.svg"`,
		`id="subscription-server"`,
		`id="subscription-level"`,
		`id="subscription-created-from"`,
		`id="run-liveness"`,
		`/api/admin/subscriptions/liveness`,
		`id="audit-report-list"`,
	} {
		if !strings.Contains(body, required) {
			t.Fatalf("admin page is missing %q", required)
		}
	}
	if strings.Contains(body, cfg.Server.AdminPassword) {
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
	cfg.Bark.SelfHostedServer = "https://bark.example.test"
	handler, store, _ := adminTestHandler(t, cfg)
	for index, key := range []string{"alpha-key", "beta-key"} {
		server := "https://api.day.app"
		if index == 1 {
			server = cfg.Bark.SelfHostedServer
		}
		if err := store.Upsert(Subscription{BarkID: key, BarkServer: server, LocationName: "成都", Latitude: 30, Longitude: 104, NotifyRules: defaultNotificationRules()}); err != nil {
			t.Fatal(err)
		}
	}
	list := httptest.NewRecorder()
	handler.ServeHTTP(list, adminRequest(http.MethodGet, "/api/admin/subscriptions?q=alpha", nil, cfg))
	if list.Code != http.StatusOK || !strings.Contains(list.Body.String(), "alpha-key") || strings.Contains(list.Body.String(), "beta-key") {
		t.Fatalf("filtered list status=%d body=%s", list.Code, list.Body.String())
	}
	selfHosted := httptest.NewRecorder()
	handler.ServeHTTP(selfHosted, adminRequest(http.MethodGet, "/api/admin/subscriptions?server=self_hosted", nil, cfg))
	if selfHosted.Code != http.StatusOK || !strings.Contains(selfHosted.Body.String(), "beta-key") || strings.Contains(selfHosted.Body.String(), "alpha-key") {
		t.Fatalf("server filtered list status=%d body=%s", selfHosted.Code, selfHosted.Body.String())
	}
	today := time.Now().In(beijingTZ).Format("2006-01-02")
	byDate := httptest.NewRecorder()
	handler.ServeHTTP(byDate, adminRequest(http.MethodGet, "/api/admin/subscriptions?created_from="+today+"&created_to="+today, nil, cfg))
	if byDate.Code != http.StatusOK || !strings.Contains(byDate.Body.String(), `"total":2`) {
		t.Fatalf("date filtered list status=%d body=%s", byDate.Code, byDate.Body.String())
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

func TestAdminAuditGroupsReportsForSameEarthquake(t *testing.T) {
	cfg := adminTestConfig(t)
	handler, _, _ := adminTestHandler(t, cfg)
	now := time.Now()
	sub := Subscription{BarkID: "grouped-key", BarkServer: "https://api.day.app", LocationName: "成都", Latitude: 30, Longitude: 104}
	for _, reportNum := range []int{1, 3} {
		event := Event{EventID: "grouped-event", ReportNum: reportNum, Type: "sc_eew", OriginTime: now, Hypocenter: "四川成都市", Latitude: 30.6, Longitude: 104.1, Magnitude: 5.2, DepthKM: 12, MaxIntensity: "6"}
		record := deliveryAuditRecordForTarget(cfg, event, sub, Decision{EstimatedIntensity: 3.2}, now, now, "pushed", "", "critical", 120*time.Millisecond, 0, nil)
		if err := writeDeliveryAudit(cfg, event, now, now, now.Add(time.Second), 1, 1, 0, 1, 0, []deliveryAuditRecord{record}); err != nil {
			t.Fatal(err)
		}
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, adminRequest(http.MethodGet, "/api/admin/audits", nil, cfg))
	if recorder.Code != http.StatusOK {
		t.Fatalf("grouped audits status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		Data struct {
			Items []adminAuditEventGroup `json:"items"`
			Total int                    `json:"total"`
		} `json:"data"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Data.Total != 1 || len(response.Data.Items) != 1 {
		t.Fatalf("unexpected grouped audits: %#v", response.Data)
	}
	group := response.Data.Items[0]
	if group.ReportCount != 2 || len(group.Reports) != 2 || group.Hypocenter != "四川成都市" || group.Magnitude != 5.2 || group.DepthKM != 12 || group.Latitude != 30.6 || group.Longitude != 104.1 {
		t.Fatalf("unexpected grouped event details: %#v", group)
	}
}

func TestAdminSubscriptionLivenessDoesNotSendNotifications(t *testing.T) {
	var pushRequests atomic.Int32
	barkServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		pushRequests.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer barkServer.Close()
	cfg := adminTestConfig(t)
	cfg.Bark.Server = barkServer.URL
	cfg.Bark.SelfHostedServer = barkServer.URL
	cfg.Bark.DeviceDBPath = filepath.Join(t.TempDir(), "bark.db")
	db, err := bolt.Open(cfg.Bark.DeviceDBPath, 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte("device"))
		if err != nil {
			return err
		}
		return bucket.Put([]byte("alive-key"), []byte("token"))
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	handler, store, _ := adminTestHandler(t, cfg)
	for _, key := range []string{"alive-key", "missing-key"} {
		if err := store.Upsert(Subscription{BarkID: key, BarkServer: barkServer.URL, LocationName: "成都", Latitude: 30, Longitude: 104, NotifyRules: defaultNotificationRules()}); err != nil {
			t.Fatal(err)
		}
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, adminRequest(http.MethodPost, "/api/admin/subscriptions/liveness", nil, cfg))
	if recorder.Code != http.StatusOK {
		t.Fatalf("liveness status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if pushRequests.Load() != 0 {
		t.Fatalf("liveness unexpectedly sent %d HTTP push requests", pushRequests.Load())
	}
	body := recorder.Body.String()
	for _, expected := range []string{`"self_hosted_alive":1`, `"self_hosted_missing":1`, `"notification_sent":false`, `"bark_id":"missing-key"`} {
		if !strings.Contains(body, expected) {
			t.Fatalf("liveness response missing %s: %s", expected, body)
		}
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
