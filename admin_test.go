package main

import (
	"bytes"
	"context"
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
	unauthorizedServices := httptest.NewRecorder()
	handler.ServeHTTP(unauthorizedServices, httptest.NewRequest(http.MethodGet, "/api/admin/services", nil))
	if unauthorizedServices.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized services status=%d body=%s", unauthorizedServices.Code, unauthorizedServices.Body.String())
	}
	unauthorizedHistory := httptest.NewRecorder()
	handler.ServeHTTP(unauthorizedHistory, httptest.NewRequest(http.MethodGet, "/api/admin/services/history", nil))
	if unauthorizedHistory.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized service history status=%d body=%s", unauthorizedHistory.Code, unauthorizedHistory.Body.String())
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
		`id="subscription-liveness"`,
		`id="subscription-created-from"`,
		`id="run-liveness"`,
		`id="run-liveness" class="btn btn-secondary">测活</button>`,
		`data-sort="bark_id"`,
		`data-sort="liveness_status"`,
		`data-sort="updated_at"`,
		`/api/admin/subscriptions/liveness`,
		`id="liveness-result-filter"`,
		`id="liveness-prev"`,
		`官方未验证`,
		`id="audit-report-list"`,
		`id="audit-list-search"`,
		`id="audit-list-source"`,
		`id="audit-list-result"`,
		`id="audit-list-prev"`,
		`首轮全量`,
		`class="audit-table"`,
		`data-audit-sort="status"`,
		`id="audit-records-prev"`,
		`id="audit-records-next"`,
		`data-view="services"`,
		`id="service-history-range"`,
		`id="service-history-chart"`,
		`/api/admin/services`,
		`/api/admin/services/history`,
		`id="show-notification-intensity"`,
		`id="show-notification-time"`,
		`/api/admin/notification-display`,
		`最近 30 天`,
		`不发送任何通知`,
	} {
		if !strings.Contains(body, required) {
			t.Fatalf("admin page is missing %q", required)
		}
	}
	if strings.Contains(body, cfg.Server.AdminPassword) {
		t.Fatalf("unexpected admin page body")
	}
}

func TestAdminNotificationDisplaySettingsAPI(t *testing.T) {
	cfg := adminTestConfig(t)
	handler, _, _ := adminTestHandler(t, cfg)

	unauthorized := httptest.NewRecorder()
	handler.ServeHTTP(unauthorized, httptest.NewRequest(http.MethodGet, "/api/admin/notification-display", nil))
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized status=%d body=%s", unauthorized.Code, unauthorized.Body.String())
	}

	get := httptest.NewRecorder()
	handler.ServeHTTP(get, adminRequest(http.MethodGet, "/api/admin/notification-display", nil, cfg))
	if get.Code != http.StatusOK || !strings.Contains(get.Body.String(), `"show_intensity":true`) || !strings.Contains(get.Body.String(), `"show_estimated_time":true`) {
		t.Fatalf("default settings status=%d body=%s", get.Code, get.Body.String())
	}

	update := httptest.NewRecorder()
	handler.ServeHTTP(update, adminRequest(http.MethodPut, "/api/admin/notification-display", []byte(`{"show_intensity":false,"show_estimated_time":true}`), cfg))
	if update.Code != http.StatusOK || !strings.Contains(update.Body.String(), `"show_intensity":false`) || !strings.Contains(update.Body.String(), `"show_estimated_time":true`) {
		t.Fatalf("update settings status=%d body=%s", update.Code, update.Body.String())
	}

	reopened, err := newNotificationDisplaySettingsStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	settings := reopened.Snapshot()
	if settings.ShowIntensity || !settings.ShowEstimatedTime || settings.UpdatedAt == "" {
		t.Fatalf("settings were not persisted: %#v", settings)
	}

	invalid := httptest.NewRecorder()
	handler.ServeHTTP(invalid, adminRequest(http.MethodPut, "/api/admin/notification-display", []byte(`{"show_intensity":true}`), cfg))
	if invalid.Code != http.StatusBadRequest {
		t.Fatalf("incomplete update status=%d body=%s", invalid.Code, invalid.Body.String())
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

func TestAdminSubscriptionsSupportStableColumnSorting(t *testing.T) {
	cfg := adminTestConfig(t)
	handler, store, _ := adminTestHandler(t, cfg)
	for _, sub := range []Subscription{
		{BarkID: "alpha-key", BarkServer: "https://z.example.test", LocationName: "上海", Latitude: 31, Longitude: 121, NotifyRules: defaultNotificationRules()},
		{BarkID: "beta-key", BarkServer: "https://a.example.test", LocationName: "北京", Latitude: 39, Longitude: 116, NotifyRules: defaultNotificationRules()},
	} {
		if err := store.Upsert(sub); err != nil {
			t.Fatal(err)
		}
	}
	for _, test := range []struct {
		query string
		first string
	}{
		{query: "sort_by=bark_id&sort_order=desc", first: "beta-key"},
		{query: "sort_by=server&sort_order=asc", first: "beta-key"},
		{query: "sort_by=location&sort_order=asc", first: "alpha-key"},
	} {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, adminRequest(http.MethodGet, "/api/admin/subscriptions?"+test.query, nil, cfg))
		var response struct {
			Data struct {
				Items []Subscription `json:"items"`
			} `json:"data"`
		}
		if recorder.Code != http.StatusOK || json.Unmarshal(recorder.Body.Bytes(), &response) != nil || len(response.Data.Items) != 2 || response.Data.Items[0].BarkID != test.first {
			t.Fatalf("sort %s status=%d body=%s", test.query, recorder.Code, recorder.Body.String())
		}
	}
}

func TestAdminAuditAPIReadsSummaryAndMaskedDetails(t *testing.T) {
	cfg := adminTestConfig(t)
	handler, _, _ := adminTestHandler(t, cfg)
	now := time.Now()
	event := Event{EventID: "audit-test", ReportNum: 2, Type: "sc_eew", OriginTime: now, Magnitude: 5.1}
	sub := Subscription{BarkID: "sensitive-bark-key", BarkServer: "https://api.day.app", LocationName: "成都", Latitude: 30, Longitude: 104}
	record := deliveryAuditRecordForTarget(cfg, event, sub, Decision{EstimatedIntensity: 3.2}, now, now, "pushed", "", "critical", 120*time.Millisecond, time.Time{}, false, false, 0, nil)
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

func TestAdminAuditDetailSortsAllMatchingRecordsBeforePagination(t *testing.T) {
	cfg := adminTestConfig(t)
	handler, _, _ := adminTestHandler(t, cfg)
	now := time.Now()
	event := Event{EventID: "audit-sort", ReportNum: 1, Type: "cenc_eew", OriginTime: now, Magnitude: 5.3}
	records := []deliveryAuditRecord{
		{Status: "pushed", BarkMasked: "z***", BarkHash: "hash-z", LocationName: "成都", NotifyLevel: "active", EstimatedIntensity: 2.1, ElapsedMS: 10},
		{Status: "failed", BarkMasked: "a***", BarkHash: "hash-a", LocationName: "北京", NotifyLevel: "critical", EstimatedIntensity: 4.2, ElapsedMS: 50, Error: "timeout"},
		{Status: "filtered", BarkMasked: "c***", BarkHash: "hash-c", LocationName: "上海", EstimatedIntensity: 1.2, ElapsedMS: 30, Reason: "notify_band"},
		{Status: "skipped", BarkMasked: "b***", BarkHash: "hash-b", LocationName: "广州", NotifyLevel: "passive", EstimatedIntensity: 0.8, ElapsedMS: 20, Reason: "queue"},
		{Status: "pushed", BarkMasked: "d***", BarkHash: "hash-d", LocationName: "武汉", NotifyLevel: "active", EstimatedIntensity: 3.1, ElapsedMS: 40},
	}
	if err := writeDeliveryAudit(cfg, event, now, now, now.Add(time.Second), len(records), 0, len(records), 0, 1, records); err != nil {
		t.Fatal(err)
	}

	type auditPage struct {
		Records   []deliveryAuditRecord `json:"records"`
		Total     int                   `json:"total"`
		Offset    int                   `json:"offset"`
		Limit     int                   `json:"limit"`
		SortBy    string                `json:"sort_by"`
		SortOrder string                `json:"sort_order"`
	}
	request := func(query string) auditPage {
		t.Helper()
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, adminRequest(http.MethodGet, "/api/admin/audit?id="+auditFileBase(event)+"&"+query, nil, cfg))
		var response struct {
			Data auditPage `json:"data"`
		}
		if recorder.Code != http.StatusOK || json.Unmarshal(recorder.Body.Bytes(), &response) != nil {
			t.Fatalf("audit detail query=%s status=%d body=%s", query, recorder.Code, recorder.Body.String())
		}
		return response.Data
	}

	page := request("sort_by=elapsed_ms&sort_order=desc&offset=1&limit=2")
	if page.Total != 5 || page.Offset != 1 || page.Limit != 2 || page.SortBy != "elapsed_ms" || page.SortOrder != "desc" || len(page.Records) != 2 || page.Records[0].ElapsedMS != 40 || page.Records[1].ElapsedMS != 30 {
		t.Fatalf("global elapsed sort was not applied before pagination: %#v", page)
	}
	statusPage := request("sort_by=status&sort_order=asc&limit=5")
	wantStatuses := []string{"failed", "filtered", "skipped", "pushed", "pushed"}
	for index, want := range wantStatuses {
		if statusPage.Records[index].Status != want {
			t.Fatalf("status sort index=%d got=%q want=%q records=%#v", index, statusPage.Records[index].Status, want, statusPage.Records)
		}
	}
}

func TestAdminAuditGroupsReportsForSameEarthquake(t *testing.T) {
	cfg := adminTestConfig(t)
	handler, _, _ := adminTestHandler(t, cfg)
	now := time.Now()
	sub := Subscription{BarkID: "grouped-key", BarkServer: "https://api.day.app", LocationName: "成都", Latitude: 30, Longitude: 104}
	for _, reportNum := range []int{1, 3} {
		event := Event{EventID: "grouped-event", ReportNum: reportNum, Type: "sc_eew", OriginTime: now, Hypocenter: "四川成都市", Latitude: 30.6, Longitude: 104.1, Magnitude: 5.2, DepthKM: 12, MaxIntensity: "6"}
		record := deliveryAuditRecordForTarget(cfg, event, sub, Decision{EstimatedIntensity: 3.2}, now, now, "pushed", "", "critical", 120*time.Millisecond, time.Time{}, false, false, 0, nil)
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

func TestAdminAuditGroupsSameEarthquakeAcrossSourcesWithoutMergingSameSourceAftershock(t *testing.T) {
	cfg := adminTestConfig(t)
	handler, _, _ := adminTestHandler(t, cfg)
	now := time.Now().Truncate(time.Second)
	sub := Subscription{BarkID: "cross-source-key", BarkServer: "https://api.day.app", LocationName: "成都", Latitude: 30, Longitude: 104}
	events := []Event{
		{EventID: "cenc-event", ReportNum: 1, Type: "cenc_eew", OriginTime: now, Hypocenter: "四川成都市", Latitude: 30.6, Longitude: 104.1, Magnitude: 5.2},
		{EventID: "cq-event", ReportNum: 1, Type: "cq_eew", OriginTime: now.Add(time.Second), Hypocenter: "成都附近", Latitude: 30.61, Longitude: 104.11, Magnitude: 5.1},
		{EventID: "cenc-aftershock", ReportNum: 1, Type: "cenc_eew", OriginTime: now.Add(2 * time.Second), Hypocenter: "华北地区", Latitude: 40, Longitude: 120, Magnitude: 4.8},
	}
	for _, event := range events {
		record := deliveryAuditRecordForTarget(cfg, event, sub, Decision{EstimatedIntensity: 3.2}, now, now, "pushed", "", "critical", 120*time.Millisecond, time.Time{}, false, false, 0, nil)
		if err := writeDeliveryAudit(cfg, event, now, now, now.Add(time.Second), 1, 1, 0, 1, 0, []deliveryAuditRecord{record}); err != nil {
			t.Fatal(err)
		}
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, adminRequest(http.MethodGet, "/api/admin/audits", nil, cfg))
	var response struct {
		Data struct {
			Items []adminAuditEventGroup `json:"items"`
			Total int                    `json:"total"`
		} `json:"data"`
	}
	if recorder.Code != http.StatusOK || json.Unmarshal(recorder.Body.Bytes(), &response) != nil {
		t.Fatalf("cross-source audits status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if response.Data.Total != 2 {
		t.Fatalf("same-source aftershock was incorrectly merged: %#v", response.Data.Items)
	}
	foundCrossSource := false
	for _, group := range response.Data.Items {
		if len(group.Sources) == 2 {
			foundCrossSource = group.ReportCount == 2 && len(group.EventIDs) == 2 && strings.Contains(group.Source, "中国地震台网") && strings.Contains(group.Source, "重庆地震局")
		}
	}
	if !foundCrossSource {
		t.Fatalf("cross-source earthquake was not grouped: %#v", response.Data.Items)
	}
}

func TestAdminAuditGroupsCrossSourceLegacyEventsByOriginTime(t *testing.T) {
	cfg := adminTestConfig(t)
	origin := time.Date(2026, 7, 28, 11, 16, 6, 0, beijingTZ)
	reports := []adminAuditSummary{
		{ID: "cenc-legacy", Modified: origin, deliveryAuditSummary: deliveryAuditSummary{EventID: "202607281116.0001", Type: "cenc_eew", OriginTime: origin.Format(time.RFC3339)}},
		{ID: "cq-legacy", Modified: origin.Add(time.Second), deliveryAuditSummary: deliveryAuditSummary{EventID: "202607281116.0001", Type: "cq_eew", OriginTime: origin.Add(time.Second).Format(time.RFC3339)}},
	}
	groups := groupDeliveryAuditSummaries(cfg, reports)
	if len(groups) != 1 || groups[0].ReportCount != 2 || len(groups[0].Sources) != 2 || groups[0].Title == groups[0].EventID {
		t.Fatalf("legacy cross-source reports were not grouped with a descriptive title: %#v", groups)
	}
}

func TestAdminAuditFiltersGroupsAcrossAllCriteria(t *testing.T) {
	groups := []adminAuditEventGroup{
		{Title: "四川成都市", EventID: "event-cenc", EventIDs: []string{"event-cenc"}, Source: "中国地震台网", OriginTime: "2026-08-01T09:00:00+08:00", TotalFailed: 2, Reports: []adminAuditSummary{{deliveryAuditSummary: deliveryAuditSummary{Type: "cenc_eew"}}}},
		{Title: "熊本県熊本地方", EventID: "event-jma", EventIDs: []string{"event-jma"}, Source: "日本气象厅", OriginTime: "2026-08-02T10:00:00+08:00", Reports: []adminAuditSummary{{deliveryAuditSummary: deliveryAuditSummary{Type: "jma_eew"}}}},
	}
	tests := []struct {
		name   string
		filter adminAuditFilter
		wantID string
	}{
		{name: "keyword", filter: adminAuditFilter{Query: "成都"}, wantID: "event-cenc"},
		{name: "source", filter: adminAuditFilter{Source: "jma_eew"}, wantID: "event-jma"},
		{name: "failed", filter: adminAuditFilter{Result: "failed"}, wantID: "event-cenc"},
		{name: "success", filter: adminAuditFilter{Result: "success"}, wantID: "event-jma"},
		{name: "date", filter: adminAuditFilter{DateFrom: "2026-08-02", DateTo: "2026-08-02"}, wantID: "event-jma"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := filterAdminAuditGroups(groups, test.filter)
			if len(got) != 1 || got[0].EventID != test.wantID {
				t.Fatalf("filter=%#v got=%#v want=%s", test.filter, got, test.wantID)
			}
		})
	}
}

func TestAdminAuditEnrichesLegacySummaryBySourceAndOriginTime(t *testing.T) {
	cfg := adminTestConfig(t)
	cfg.Server.HistoryPath = filepath.Join(t.TempDir(), "history.json")
	origin := time.Date(2026, 7, 29, 11, 16, 6, 0, beijingTZ)
	if err := saveHistoryCache(cfg.Server.HistoryPath, HistoryCacheFile{UpdatedAt: time.Now().Unix(), Records: []HistoryRecord{{
		Source: "cenc", Key: "No1", EventID: "different-final-id", OriginTime: origin.Format("2006-01-02 15:04:05"), Hypocenter: "四川省成都市", Latitude: 30.6, Longitude: 104.1, Magnitude: 5.1, DepthKM: 10, MaxIntensity: "5",
	}}}); err != nil {
		t.Fatal(err)
	}
	handler, _, _ := adminTestHandler(t, cfg)
	event := Event{EventID: "legacy-eew-id", ReportNum: 1, Type: "cq_eew", OriginTime: origin}
	record := deliveryAuditRecord{EventID: event.EventID, ReportNum: event.ReportNum, Type: event.Type, OriginTime: formatBeijing(origin, time.RFC3339), Status: "filtered"}
	if err := writeDeliveryAudit(cfg, event, origin, origin, origin.Add(time.Second), 1, 0, 0, 1, 1, []deliveryAuditRecord{record}); err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, adminRequest(http.MethodGet, "/api/admin/audits", nil, cfg))
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "四川省成都市") || !strings.Contains(recorder.Body.String(), `"magnitude":5.1`) || !strings.Contains(recorder.Body.String(), `"depth_km":10`) {
		t.Fatalf("legacy audit was not enriched by origin time: status=%d body=%s", recorder.Code, recorder.Body.String())
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
		if err := bucket.Put([]byte("alive-key"), []byte("token")); err != nil {
			return err
		}
		return bucket.Put([]byte("tokenless-key"), []byte{})
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	handler, store, _ := adminTestHandler(t, cfg)
	for _, key := range []string{"alive-key", "tokenless-key", "missing-key"} {
		if err := store.Upsert(Subscription{BarkID: key, BarkServer: barkServer.URL, LocationName: "成都", Latitude: 30, Longitude: 104, NotifyRules: defaultNotificationRules()}); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Upsert(Subscription{BarkID: "official-key", BarkServer: "https://api.day.app", LocationName: "成都", Latitude: 30, Longitude: 104, NotifyRules: defaultNotificationRules()}); err != nil {
		t.Fatal(err)
	}
	if err := store.Upsert(Subscription{BarkID: "invalid-key", BarkServer: barkServer.URL, LocationName: "成都", Latitude: 30, Longitude: 104, NotifyRules: defaultNotificationRules(), NotifyBands: []NotificationBand{{Min: 2, Max: 2, Level: "active"}}}); err != nil {
		t.Fatal(err)
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
	for _, expected := range []string{`"total_subscriptions":5`, `"self_hosted_alive":1`, `"self_hosted_token_missing":1`, `"self_hosted_missing":1`, `"official_not_checked":1`, `"invalid_subscriptions":1`, `"result_total":5`, `"labels_saved":true`, `"notification_sent":false`, `"bark_id":"alive-key"`, `"bark_id":"tokenless-key"`, `"bark_id":"missing-key"`, `"bark_id":"official-key"`, `"bark_id":"invalid-key"`, `"status":"device_present"`, `"status":"device_token_missing"`, `"status":"device_missing"`, `"status":"official_unverified"`, `"status":"configuration_invalid"`} {
		if !strings.Contains(body, expected) {
			t.Fatalf("liveness response missing %s: %s", expected, body)
		}
	}
	reopened, err := newSubscriptionLivenessStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for key, status := range map[string]string{
		"alive-key":     subscriptionLivenessDevicePresent,
		"tokenless-key": subscriptionLivenessDeviceTokenMissing,
		"missing-key":   subscriptionLivenessDeviceMissing,
		"official-key":  subscriptionLivenessOfficialUnverified,
		"invalid-key":   subscriptionLivenessConfigurationInvalid,
	} {
		sub, ok := store.Get(key)
		if !ok || reopened.Snapshot(sub).Status != status {
			t.Fatalf("persisted liveness label for %s = %#v", key, reopened.Snapshot(sub))
		}
	}
	for status, expectedKey := range map[string]string{
		subscriptionLivenessDevicePresent:        "alive-key",
		subscriptionLivenessDeviceTokenMissing:   "tokenless-key",
		subscriptionLivenessDeviceMissing:        "missing-key",
		subscriptionLivenessOfficialUnverified:   "official-key",
		subscriptionLivenessConfigurationInvalid: "invalid-key",
	} {
		filteredList := httptest.NewRecorder()
		target := "/api/admin/subscriptions?liveness_status=" + status
		handler.ServeHTTP(filteredList, adminRequest(http.MethodGet, target, nil, cfg))
		filteredBody := filteredList.Body.String()
		if filteredList.Code != http.StatusOK || !strings.Contains(filteredBody, `"total":1`) || !strings.Contains(filteredBody, `"bark_id":"`+expectedKey+`"`) || !strings.Contains(filteredBody, `"liveness_status":"`+status+`"`) {
			t.Fatalf("liveness filter %s status=%d body=%s", status, filteredList.Code, filteredBody)
		}
	}
	selectedPayload, _ := json.Marshal(adminSubscriptionLivenessRequest{BarkIDs: []string{"alive-key"}})
	selected := httptest.NewRecorder()
	handler.ServeHTTP(selected, adminRequest(http.MethodPost, "/api/admin/subscriptions/liveness", selectedPayload, cfg))
	if selected.Code != http.StatusOK || !strings.Contains(selected.Body.String(), `"scope":"selected"`) || !strings.Contains(selected.Body.String(), `"total_subscriptions":1`) || !strings.Contains(selected.Body.String(), `"self_hosted_alive":1`) || strings.Contains(selected.Body.String(), `"bark_id":"missing-key"`) {
		t.Fatalf("selected liveness status=%d body=%s", selected.Code, selected.Body.String())
	}
	filteredPayload, _ := json.Marshal(adminSubscriptionLivenessRequest{adminSubscriptionFilter: adminSubscriptionFilter{Query: "missing"}})
	filtered := httptest.NewRecorder()
	handler.ServeHTTP(filtered, adminRequest(http.MethodPost, "/api/admin/subscriptions/liveness", filteredPayload, cfg))
	if filtered.Code != http.StatusOK || !strings.Contains(filtered.Body.String(), `"scope":"filtered"`) || !strings.Contains(filtered.Body.String(), `"total_subscriptions":1`) || !strings.Contains(filtered.Body.String(), `"self_hosted_missing":1`) || strings.Contains(filtered.Body.String(), `"bark_id":"alive-key"`) {
		t.Fatalf("filtered liveness status=%d body=%s", filtered.Code, filtered.Body.String())
	}
	if pushRequests.Load() != 0 {
		t.Fatalf("scoped liveness unexpectedly sent %d HTTP push requests", pushRequests.Load())
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

func TestAdminServiceMonitorUsesReadOnlyHealthProbes(t *testing.T) {
	var requests atomic.Int32
	bark := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.Method != http.MethodGet || r.URL.Path != "/ping" {
			t.Fatalf("unexpected Bark health probe: %s %s", r.Method, r.URL.Path)
		}
		_, _ = w.Write([]byte("pong"))
	}))
	defer bark.Close()

	cfg := adminTestConfig(t)
	cfg.Bark.Server = bark.URL
	handler, _, health := adminTestHandler(t, cfg)
	now := time.Now()
	health.SetWebSocketConnected(now)
	health.SetWebSocketMessage(now)

	request := func() adminServiceHealthSnapshot {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, adminRequest(http.MethodGet, "/api/admin/services", nil, cfg))
		if recorder.Code != http.StatusOK {
			t.Fatalf("services status=%d body=%s", recorder.Code, recorder.Body.String())
		}
		var response struct {
			Success bool                       `json:"success"`
			Data    adminServiceHealthSnapshot `json:"data"`
		}
		if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
			t.Fatal(err)
		}
		if !response.Success {
			t.Fatalf("service response failed: %s", recorder.Body.String())
		}
		return response.Data
	}

	first := request()
	if !first.Healthy || first.Status != "healthy" || first.EnabledServices == 0 || first.HealthyServices != first.EnabledServices {
		t.Fatalf("unexpected healthy service snapshot: %#v", first)
	}
	official, ok := findAdminService(first.Services, "official_bark")
	if !ok || !official.Healthy || official.Metrics["notification_sent"] != false || official.LastSuccessAt == "" {
		t.Fatalf("unexpected official Bark service: %#v", official)
	}
	second := request()
	secondOfficial, _ := findAdminService(second.Services, "official_bark")
	if secondOfficial.LastSuccessAt == "" || requests.Load() != 1 {
		t.Fatalf("service monitor did not retain success/probe count: service=%#v requests=%d", secondOfficial, requests.Load())
	}
	if strings.Contains(recorderJSON(second), "notification_sent\":true") {
		t.Fatal("read-only service monitor reported a notification send")
	}
	historyRecorder := httptest.NewRecorder()
	handler.ServeHTTP(historyRecorder, adminRequest(http.MethodGet, "/api/admin/services/history?hours=168", nil, cfg))
	if historyRecorder.Code != http.StatusOK || historyRecorder.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("service history status=%d cache=%q body=%s", historyRecorder.Code, historyRecorder.Header().Get("Cache-Control"), historyRecorder.Body.String())
	}
	var historyResponse struct {
		Success bool                        `json:"success"`
		Data    adminServiceHistoryResponse `json:"data"`
	}
	if err := json.Unmarshal(historyRecorder.Body.Bytes(), &historyResponse); err != nil {
		t.Fatal(err)
	}
	if !historyResponse.Success || historyResponse.Data.RangeHours != 168 || historyResponse.Data.BucketMinutes != 10 || len(historyResponse.Data.Points) != 1 {
		t.Fatalf("unexpected service history: %s", historyRecorder.Body.String())
	}
}

func TestAdminServiceMonitorReportsStaleEarthquakeSource(t *testing.T) {
	bark := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("pong")) }))
	defer bark.Close()
	cfg := adminTestConfig(t)
	cfg.Bark.Server = bark.URL
	store, err := NewStore(cfg.Server.DataPath)
	if err != nil {
		t.Fatal(err)
	}
	health := &RuntimeHealth{}
	health.SetWebSocketConnected(time.Now().Add(-10 * time.Minute))
	health.SetWebSocketMessage(time.Now().Add(-10 * time.Minute))
	monitor := newAdminServiceMonitor(cfg, store, NewNotifier(cfg.Bark), health)
	snapshot := monitor.Snapshot(context.Background())
	dataSource, ok := findAdminService(snapshot.Services, "earthquake_source")
	if snapshot.Healthy || snapshot.Status != "unhealthy" || !ok || dataSource.Status != "unhealthy" {
		t.Fatalf("stale data source was not surfaced: snapshot=%#v service=%#v", snapshot, dataSource)
	}
}

func findAdminService(services []adminServiceStatus, id string) (adminServiceStatus, bool) {
	for _, service := range services {
		if service.ID == id {
			return service, true
		}
	}
	return adminServiceStatus{}, false
}

func recorderJSON(value any) string {
	data, _ := json.Marshal(value)
	return string(data)
}
