package main

import (
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestNotificationDisplaySettingsPersist(t *testing.T) {
	cfg := Config{Server: ServerConfig{DataPath: filepath.Join(t.TempDir(), "subscriptions.json")}}
	store, err := newNotificationDisplaySettingsStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defaults := store.Snapshot()
	if !defaults.ShowIntensity || !defaults.ShowEstimatedTime {
		t.Fatalf("unexpected defaults: %#v", defaults)
	}
	now := time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC)
	saved, err := store.Update(false, false, now)
	if err != nil {
		t.Fatal(err)
	}
	if saved.ShowIntensity || saved.ShowEstimatedTime || saved.UpdatedAt == "" {
		t.Fatalf("unexpected saved settings: %#v", saved)
	}
	info, err := os.Stat(filepath.Join(filepath.Dir(cfg.Server.DataPath), "notification-display.json"))
	if err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" && info.Mode().Perm() != 0o600 {
		t.Fatalf("settings mode=%o", info.Mode().Perm())
	}
	reopened, err := newNotificationDisplaySettingsStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if got := reopened.Snapshot(); got != saved {
		t.Fatalf("reloaded settings=%#v want=%#v", got, saved)
	}
	replaced, err := reopened.Update(true, false, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	reopenedAgain, err := newNotificationDisplaySettingsStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if got := reopenedAgain.Snapshot(); got != replaced {
		t.Fatalf("replaced settings=%#v want=%#v", got, replaced)
	}
}

func TestFormatAlertRespectsIndependentDisplaySettings(t *testing.T) {
	now := time.Date(2026, 7, 29, 12, 0, 0, 0, beijingTZ)
	event := Event{Type: "cenc_eew", EventID: "display-test", ReportNum: 2, Revision: true, RevisionFields: []string{"magnitude", "max_intensity"}, OriginTime: now, Hypocenter: "四川成都", Latitude: 30.6, Longitude: 104.1, Magnitude: 5.2, DepthKM: 10, MaxIntensity: "6"}
	decision := Decision{DistanceKM: 20, HypocentralKM: 22, EstimatedIntensity: 4.1, SecondsToP: 2, SecondsToS: 5, PArrival: now.Add(2 * time.Second), SArrival: now.Add(5 * time.Second)}
	sub := Subscription{NotifyBands: []NotificationBand{{Min: 3, Max: notificationOpenEndedMax, Level: "critical"}}}

	title, subtitle, body := formatAlert(event, decision, sub, NotificationDisplaySettings{})
	combined := title + "\n" + subtitle + "\n" + body
	for _, hidden := range []string{"预计烈度", "最大烈度", "烈度", "预计到达", "P波", "S波", "提醒:"} {
		if strings.Contains(combined, hidden) {
			t.Fatalf("both settings disabled but output contains %q: %s", hidden, combined)
		}
	}
	if !strings.Contains(title, "地震警报修订") || strings.Contains(body, "[修订]") || !strings.Contains(subtitle, "M5.2") {
		t.Fatalf("essential earthquake information missing: title=%q subtitle=%q body=%q", title, subtitle, body)
	}

	title, subtitle, body = formatAlert(event, decision, sub, NotificationDisplaySettings{ShowIntensity: true})
	if strings.Contains(title+body, "到达") || strings.Contains(body, "P波") || strings.Contains(body, "S波") || !strings.Contains(subtitle, "预计烈度 4.1") || !strings.Contains(body, "最大烈度6") {
		t.Fatalf("intensity-only display is incorrect: title=%q subtitle=%q body=%q", title, subtitle, body)
	}

	title, subtitle, body = formatAlert(event, decision, sub, NotificationDisplaySettings{ShowEstimatedTime: true})
	if !strings.Contains(title, "5秒后到达") || !strings.Contains(body, "预计到达时间") || !strings.Contains(body, "P波+2秒 S波+5秒") || strings.Contains(subtitle+body, "烈度") {
		t.Fatalf("time-only display is incorrect: title=%q subtitle=%q body=%q", title, subtitle, body)
	}
}

func TestAlertPageHidesDisabledDisplaySections(t *testing.T) {
	now := time.Now()
	page := AlertPage{
		Event:      Event{Type: "cenc_eew", EventID: "page-test", ReportNum: 1, OriginTime: now, Hypocenter: "测试震中", Latitude: 30, Longitude: 104, Magnitude: 5, DepthKM: 10, MaxIntensity: "6"},
		Decision:   Decision{DistanceKM: 100, HypocentralKM: 101, EstimatedIntensity: 3.5, PArrival: now.Add(10 * time.Second), SArrival: now.Add(20 * time.Second), SecondsToP: 10, SecondsToS: 20},
		Subscriber: Subscription{BarkID: "display-key", Latitude: 30.5, Longitude: 104.1},
		CreatedAt:  now,
	}
	hidden := httptest.NewRecorder()
	renderAlertPage(hidden, page)
	hiddenBody := hidden.Body.String()
	for _, markup := range []string{"S 波到达订阅地", `<div class="label">预计烈度</div>`, "<dt>最大烈度</dt>", "<dt>P 波到达</dt>", "<dt>S 波到达</dt>"} {
		if strings.Contains(hiddenBody, markup) {
			t.Fatalf("disabled alert page contains %q", markup)
		}
	}
	page.Display = defaultNotificationDisplaySettings()
	shown := httptest.NewRecorder()
	renderAlertPage(shown, page)
	shownBody := shown.Body.String()
	for _, markup := range []string{"S 波到达订阅地", `<div class="label">预计烈度</div>`, "<dt>最大烈度</dt>", "<dt>P 波到达</dt>"} {
		if !strings.Contains(shownBody, markup) {
			t.Fatalf("enabled alert page is missing %q", markup)
		}
	}
}

func TestBuildFanoutTargetSnapshotsNotificationDisplaySettings(t *testing.T) {
	cfg := Config{
		Server: ServerConfig{
			DataPath:  filepath.Join(t.TempDir(), "subscriptions.json"),
			PublicURL: "https://eew.example.com",
		},
	}
	store, err := newNotificationDisplaySettingsStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Update(false, false, time.Now()); err != nil {
		t.Fatal(err)
	}
	cfg.notificationDisplay = store

	now := time.Now()
	event := Event{Type: "cenc_eew", EventID: "fanout-display-test", ReportNum: 1, OriginTime: now, Hypocenter: "测试震中", Latitude: 30, Longitude: 104, Magnitude: 5, DepthKM: 10, MaxIntensity: "6"}
	decision := Decision{DistanceKM: 100, HypocentralKM: 101, EstimatedIntensity: 3.5, PArrival: now.Add(10 * time.Second), SArrival: now.Add(20 * time.Second), SecondsToP: 10, SecondsToS: 20}
	sub := Subscription{BarkID: "display-key", Latitude: 30.5, Longitude: 104.1}
	cache := NewAlertCache(time.Hour, 10)
	target := buildFanoutTarget(cfg, event, cache, sub, decision, PushOptions{}, "critical", 10)
	combined := target.Title + "\n" + target.Subtitle + "\n" + target.Body
	for _, hidden := range []string{"预计烈度", "最大烈度", "预计到达", "P波", "S波", "提醒:"} {
		if strings.Contains(combined, hidden) {
			t.Fatalf("fanout target contains disabled display value %q: %s", hidden, combined)
		}
	}

	alertURL := target.Params["url"]
	token := strings.TrimPrefix(alertURL, cfg.Server.PublicURL+"/alert/")
	if token == alertURL || token == "" {
		t.Fatalf("unexpected alert URL %q", alertURL)
	}
	page, ok := cache.Get(token)
	if !ok {
		t.Fatalf("alert page %q was not cached", token)
	}
	if page.Display.ShowIntensity || page.Display.ShowEstimatedTime {
		t.Fatalf("alert page did not snapshot disabled settings: %#v", page.Display)
	}
}
