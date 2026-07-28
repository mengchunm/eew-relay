package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}

func TestParseCencEvent(t *testing.T) {
	payload := []byte(`{
		"type": "cenc_eew",
		"EventID": "202607070001",
		"ReportNum": 2,
		"OriginTime": "2026-07-07 09:30:00",
		"HypoCenter": "四川阿坝",
		"Latitude": 31.9,
		"Longitude": 102.2,
		"Magnitude": 5.8,
		"Depth": 10,
		"MaxIntensity": 6
	}`)

	event, ok, err := parseEvent(payload)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected EEW event")
	}
	if event.Type != "cenc_eew" || event.EventID != "202607070001" || event.ReportNum != 2 {
		t.Fatalf("unexpected identity: %#v", event)
	}
	if event.Hypocenter != "四川阿坝" || event.Magnitude != 5.8 || event.DepthKM != 10 {
		t.Fatalf("unexpected event fields: %#v", event)
	}
	if event.MaxIntensity != "6" {
		t.Fatalf("unexpected max intensity: %q", event.MaxIntensity)
	}
}

func TestEvaluateETA(t *testing.T) {
	cfg := Config{
		Alert: AlertConfig{
			SWaveKMS: 3.5,
			PWaveKMS: 6.0,
		},
	}
	sub := Subscription{Latitude: 31.2304, Longitude: 121.4737}
	event := Event{
		Type:      "cenc_eew",
		EventID:   "x",
		Latitude:  31.2304,
		Longitude: 121.5737,
		Magnitude: 5.0,
		DepthKM:   10,
	}
	decision := evaluate(cfg, sub, event)
	if decision.DistanceKM <= 0 || decision.HypocentralKM <= decision.DistanceKM {
		t.Fatalf("bad distances: %#v", decision)
	}
	if decision.SArrival.Before(decision.PArrival) {
		t.Fatalf("S wave should not arrive before P wave: %#v", decision)
	}
}

func TestRegionalTravelTimeInterpolation(t *testing.T) {
	cfg := Config{Alert: AlertConfig{PWaveKMS: 6.0, SWaveKMS: 3.5}}
	p, s := seismicTravelSeconds(cfg, 1000, 15)
	fixedP, fixedS := fixedWaveTravelSeconds(cfg, math.Sqrt(1000*1000+15*15))
	if p >= fixedP || s >= fixedS {
		t.Fatalf("regional table should be faster than fixed crustal speed at 1000km, got p=%.1f s=%.1f fixedP=%.1f fixedS=%.1f", p, s, fixedP, fixedS)
	}
	if s <= p {
		t.Fatalf("S arrival must be after P arrival, p=%.1f s=%.1f", p, s)
	}
}

func TestEstimateIntensitySmoothsMagnitudeBoundary(t *testing.T) {
	if got := estimateIntensity(5.1, 280); got != 2.5 {
		t.Fatalf("expected M5.1 at 280km to display intensity 2.5 after coefficient smoothing, got %.1f", got)
	}
	if got := estimateIntensityRank(5.1, 280); got != 2 {
		t.Fatalf("expected M5.1 at 280km to keep notification rank 2, got %d", got)
	}
	if got := estimateIntensity(4.8, 280); got != 1.4 {
		t.Fatalf("expected M4.8 at 280km to display intensity 1.4, got %.1f", got)
	}
	if got := estimateIntensityRank(4.8, 280); got != 1 {
		t.Fatalf("expected M4.8 at 280km to keep notification rank 1, got %d", got)
	}

	prev := estimateIntensity(4.9, 240)
	next := estimateIntensity(5.0, 240)
	if next-prev > 1 {
		t.Fatalf("unexpected M5.0 boundary jump at 240km: M4.9=%.1f M5.0=%.1f", prev, next)
	}
}

func TestEstimatedIntensityJSONKeepsOneDecimal(t *testing.T) {
	data, err := json.Marshal(SimulationPreview{EstimatedIntensity: Decimal1(2)})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"estimated_intensity":2.0`) {
		t.Fatalf("expected estimated_intensity to keep one decimal, got %s", data)
	}
}

func TestWriteDeliveryAudit(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{Server: ServerConfig{AuditPath: dir}, Bark: BarkConfig{Server: "https://bark.example.test"}}
	event := Event{EventID: "test/event", ReportNum: 1, Type: "cenc_eew", OriginTime: time.Now(), Magnitude: 5.1}
	sub := Subscription{BarkID: "secretKey", BarkServer: "https://bark.example.test", Latitude: 30.5, Longitude: 104.1}
	decision := Decision{EstimatedIntensity: 2, EstimatedIntensityRank: 2, DistanceKM: 120.4, HypocentralKM: 121, SecondsToS: 20}
	started := time.Now()
	record := deliveryAuditRecordForTarget(cfg, event, sub, decision, started, started, "pushed", "", "active", 150*time.Millisecond, 0, nil)
	if strings.Contains(record.BarkMasked, "secretKey") || record.BarkHash == "" {
		t.Fatalf("audit record should mask and hash bark key: %#v", record)
	}
	if err := writeDeliveryAudit(cfg, event, started, started, started.Add(time.Second), 1, 0, 1, 0, 1, []deliveryAuditRecord{record}); err != nil {
		t.Fatal(err)
	}
	detailPath := filepath.Join(dir, "test_event-r1-cenc_eew.jsonl.gz")
	detail, err := os.Open(detailPath)
	if err != nil {
		t.Fatalf("expected compressed detail audit file: %v", err)
	}
	defer detail.Close()
	compressed, err := gzip.NewReader(detail)
	if err != nil {
		t.Fatal(err)
	}
	var decoded deliveryAuditRecord
	if err := json.NewDecoder(compressed).Decode(&decoded); err != nil {
		t.Fatal(err)
	}
	if err := compressed.Close(); err != nil {
		t.Fatal(err)
	}
	if decoded.BarkHash != record.BarkHash || decoded.BarkMasked != record.BarkMasked || decoded.Status != "pushed" {
		t.Fatalf("unexpected compressed audit record: %#v", decoded)
	}
	if strings.Contains(decoded.BarkMasked, sub.BarkID) {
		t.Fatalf("compressed audit leaked Bark Key: %#v", decoded)
	}

	summaryPath := filepath.Join(dir, "test_event-r1-cenc_eew.summary.json")
	if _, err := os.Stat(summaryPath); err != nil {
		t.Fatalf("expected summary audit file: %v", err)
	}
	if runtime.GOOS != "windows" {
		for _, path := range []string{detailPath, summaryPath} {
			info, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			if got := info.Mode().Perm(); got != 0o600 {
				t.Fatalf("audit file %s permissions=%#o want 0600", filepath.Base(path), got)
			}
		}
	}
}

func TestCleanupAuditFilesHonorsRetentionAndFileTypes(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 7, 10, 9, 0, 0, 0, time.UTC)
	old := now.Add(-8 * 24 * time.Hour)
	recent := now.Add(-6 * 24 * time.Hour)
	files := map[string]time.Time{
		"old.jsonl":        old,
		"old.jsonl.gz":     old,
		"old.summary.json": old,
		"recent.jsonl.gz":  recent,
		"old.txt":          old,
	}
	for name, modTime := range files {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte("test"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(path, modTime, modTime); err != nil {
			t.Fatal(err)
		}
	}

	if err := cleanupAuditFiles(dir, 7, now); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"old.jsonl", "old.jsonl.gz", "old.summary.json"} {
		if _, err := os.Stat(filepath.Join(dir, name)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("expected expired audit %s to be removed, err=%v", name, err)
		}
	}
	for _, name := range []string{"recent.jsonl.gz", "old.txt"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Fatalf("expected retained file %s: %v", name, err)
		}
	}
	if runtime.GOOS != "windows" {
		info, err := os.Stat(filepath.Join(dir, "recent.jsonl.gz"))
		if err != nil {
			t.Fatal(err)
		}
		if got := info.Mode().Perm(); got != 0o600 {
			t.Fatalf("retained audit permissions=%#o want 0600", got)
		}
	}
}

func TestTravelTimeFallbackForTableOutOfRange(t *testing.T) {
	cfg := Config{Alert: AlertConfig{PWaveKMS: 6.0, SWaveKMS: 3.5}}
	p, s := seismicTravelSeconds(cfg, 5000, 10)
	fixedP, fixedS := fixedWaveTravelSeconds(cfg, math.Sqrt(5000*5000+10*10))
	if p != fixedP || s != fixedS {
		t.Fatalf("expected fixed-speed fallback outside table, got p=%.3f s=%.3f fixedP=%.3f fixedS=%.3f", p, s, fixedP, fixedS)
	}
}

func TestParseJMAEventUsesJST(t *testing.T) {
	payload := []byte(`{
		"type": "jma_eew",
		"EventID": "202607070001",
		"Serial": 1,
		"OriginTime": "2026-07-07 09:30:00",
		"Hypocenter": "岩手県沖",
		"Latitude": 40.1,
		"Longitude": 142.5,
		"Magunitude": 4.6,
		"Depth": 40,
		"MaxIntensity": "2"
	}`)

	event, ok, err := parseEvent(payload)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected EEW event")
	}
	want := time.Date(2026, 7, 7, 0, 30, 0, 0, time.UTC)
	if !event.OriginTime.Equal(want) {
		t.Fatalf("expected JST origin to equal %s, got %s", want, event.OriginTime)
	}
}

func TestHistoryRecordFromRaw(t *testing.T) {
	record, ok := historyRecordFromRaw("jma", "No1", RawEvent{
		"EventID":   "20260707061650",
		"time_full": "2026/07/07 06:16:50",
		"location":  "島根県西部",
		"magnitude": "3.2",
		"shindo":    "1",
		"depth":     "10km",
		"latitude":  "34.7",
		"longitude": "132.0",
	})
	if !ok {
		t.Fatal("expected valid history record")
	}
	if record.Source != "jma" || record.Key != "No1" || record.Hypocenter != "島根県西部" {
		t.Fatalf("unexpected identity: %#v", record)
	}
	if record.Magnitude != 3.2 || record.DepthKM != 10 || record.MaxIntensity != "1" {
		t.Fatalf("unexpected values: %#v", record)
	}
}

func TestSimulationAndHistoryPreviewsUseSubscriberLocation(t *testing.T) {
	cfg := Config{Alert: AlertConfig{SWaveKMS: 3.5, PWaveKMS: 6.0}}
	sub := Subscription{Latitude: 31.2304, Longitude: 121.4737}
	normalizeSubscription(&sub)

	previews := simulationPreviews(cfg, sub)
	if len(previews) != 3 {
		t.Fatalf("expected 3 simulation previews, got %d", len(previews))
	}
	for _, preview := range previews {
		if preview.Kind == "tiny" || preview.DistanceKM <= 0 || preview.EstimatedIntensity < 0 || preview.NotifyLevel == "" {
			t.Fatalf("bad preview: %#v", preview)
		}
	}

	records := annotateHistoryRecords(cfg, sub, []HistoryRecord{{
		Source:     "cenc",
		Key:        "No1",
		EventID:    "x",
		Hypocenter: "nearby",
		Latitude:   31.3,
		Longitude:  121.5,
		Magnitude:  4.5,
		DepthKM:    10,
	}})
	if len(records) != 1 || records[0].DistanceKM <= 0 || records[0].HypocentralKM <= records[0].DistanceKM {
		t.Fatalf("bad annotated history record: %#v", records)
	}
}

func TestNearestSubscriptionLocationForEvent(t *testing.T) {
	cfg := Config{Alert: AlertConfig{SWaveKMS: 3.5, PWaveKMS: 6.0}}
	sub := Subscription{
		BarkID:    "test",
		Latitude:  30.0,
		Longitude: 104.0,
		Locations: []SubscriptionLocation{
			{Name: "成都", Latitude: 30.0, Longitude: 104.0},
			{Name: "唐山", Latitude: 39.6, Longitude: 118.0},
		},
	}
	event := Event{
		Type:       "cenc_eew",
		EventID:    "near-tangshan",
		OriginTime: time.Now(),
		Hypocenter: "河北唐山",
		Latitude:   39.57,
		Longitude:  117.98,
		Magnitude:  5.0,
		DepthKM:    10,
	}
	selected, decision := nearestSubscriptionForEvent(cfg, sub, event)
	if selected.LocationName != "唐山" {
		t.Fatalf("expected nearest location Tangshan, got %#v", selected)
	}
	if decision.DistanceKM > 10 {
		t.Fatalf("expected near distance for selected location, got %.2fkm", decision.DistanceKM)
	}
}

func TestNotificationRules(t *testing.T) {
	sub := Subscription{}
	normalizeSubscription(&sub)
	if sub.NotifyRules != (NotificationRules{PassiveMax: 1, ActiveMax: 2, CriticalMin: 3}) {
		t.Fatalf("unexpected default rules: %#v", sub.NotifyRules)
	}
	if notifyLevelForIntensity(sub, 0) != "" || notifyLevelForIntensity(sub, 1) != "passive" || notifyLevelForIntensity(sub, 2) != "active" || notifyLevelForIntensity(sub, 3) != "critical" {
		t.Fatalf("unexpected level mapping")
	}
	if err := validateNotificationRules(NotificationRules{PassiveMax: 1, ActiveMax: 3, CriticalMin: 4}); err != nil {
		t.Fatalf("expected active range to be valid: %v", err)
	}
	sub.NotifyRules = NotificationRules{PassiveMax: 1, ActiveMax: 3, CriticalMin: 4}
	sub.NotifyBands = nil
	if notifyLevelForIntensity(sub, 3) != "active" || notifyLevelForIntensity(sub, 4) != "critical" {
		t.Fatalf("unexpected ranged level mapping")
	}
	if err := validateNotificationRules(NotificationRules{PassiveMax: 1, ActiveMax: 3, CriticalMin: 5}); err == nil {
		t.Fatal("expected non-contiguous critical range to fail")
	}
}

func TestNotificationBandsAllowDeletedRanges(t *testing.T) {
	sub := Subscription{NotifyBands: []NotificationBand{{Min: 3, Max: notificationOpenEndedMax, Level: "critical", Label: "高烈度"}}}
	normalizeSubscription(&sub)
	if notifyLevelForIntensity(sub, 2) != "" {
		t.Fatalf("expected uncovered intensity to be filtered")
	}
	if notifyLevelForIntensity(sub, 3) != "critical" {
		t.Fatalf("expected critical band for intensity 3")
	}
	if notifyLevelForIntensity(sub, 8) != "critical" {
		t.Fatalf("expected open-ended critical band for intensity above 7")
	}
	if err := validateNotificationBands([]NotificationBand{{Min: 0, Max: 2, Level: "passive"}, {Min: 2, Max: 4, Level: "active"}}); err != nil {
		t.Fatalf("expected adjacent decimal ranges to be valid: %v", err)
	}
	if err := validateNotificationBands([]NotificationBand{{Min: 0, Max: 3, Level: "passive"}, {Min: 2, Max: 4, Level: "active"}}); err == nil {
		t.Fatal("expected overlapping bands to fail")
	}
	if err := validateNotificationBands([]NotificationBand{{Min: 0, Max: 1, Level: "passive"}, {Min: 2, Max: 2, Level: "passive"}}); err == nil {
		t.Fatal("expected duplicate notification level to fail")
	}
	if err := validateNotificationBands([]NotificationBand{{Min: 0, Max: 1, Level: "passive"}, {Min: 2, Max: 2, Level: "active"}, {Min: 3, Max: notificationOpenEndedMax, Level: "critical"}, {Min: 6, Max: 6, Level: "active"}}); err == nil {
		t.Fatal("expected more than three notification bands to fail")
	}
	if err := validateNotificationBands([]NotificationBand{{Min: 2, Max: notificationOpenEndedMax, Level: "active"}}); err != nil {
		t.Fatalf("expected any notification level to support an open-ended threshold: %v", err)
	}
	activeOnly := Subscription{NotifyBands: []NotificationBand{{Min: 2, Max: notificationOpenEndedMax, Level: "active"}}}
	if got := notifyLevelForEstimatedIntensity(activeOnly, 6.5); got != "active" {
		t.Fatalf("expected open-ended active threshold to match higher intensity, got %q", got)
	}
	if err := validateNotificationBands([]NotificationBand{{Min: 2, Max: 2, Level: "active"}}); err == nil {
		t.Fatal("expected empty non-critical range to fail")
	}
	if err := validateNotificationBands([]NotificationBand{{Min: 2, Max: 3, Level: "critcal"}}); err == nil {
		t.Fatal("expected misspelled notification level to fail")
	}
}

func TestNotificationBandsMayStartAtZero(t *testing.T) {
	sub := Subscription{
		NotifyBands: []NotificationBand{
			{Min: 0, Max: 1, Level: "passive", Label: "低烈度"},
			{Min: 2, Max: 2, Level: "active", Label: "中等烈度"},
			{Min: 3, Max: notificationOpenEndedMax, Level: "critical", Label: "高烈度"},
		},
	}
	normalizeSubscription(&sub)
	if notifyLevelForIntensity(sub, 0) != "passive" {
		t.Fatalf("expected intensity 0 to match passive notification band")
	}
}

func TestNotificationBandsUseDecimalBoundaries(t *testing.T) {
	sub := Subscription{}
	normalizeSubscription(&sub)
	if got := notifyLevelForEstimatedIntensity(sub, 1.9); got != "passive" {
		t.Fatalf("expected 1.9 to be passive, got %q", got)
	}
	if got := notifyLevelForEstimatedIntensity(sub, 2.0); got != "active" {
		t.Fatalf("expected 2.0 to be active, got %q", got)
	}
	if got := notifyLevelForEstimatedIntensity(sub, 2.9); got != "active" {
		t.Fatalf("expected 2.9 to be active, got %q", got)
	}
	if got := notifyLevelForEstimatedIntensity(sub, 3.0); got != "critical" {
		t.Fatalf("expected 3.0 to be critical, got %q", got)
	}
}

func TestSimulationTargetUsesNotificationBands(t *testing.T) {
	sub := Subscription{
		NotifyRules: NotificationRules{PassiveMax: 1, ActiveMax: 2, CriticalMin: 3},
		NotifyBands: []NotificationBand{
			{Min: 1, Max: 2, Level: "passive", Label: "低烈度"},
			{Min: 3, Max: 4, Level: "active", Label: "中等烈度"},
			{Min: 5, Max: notificationOpenEndedMax, Level: "critical", Label: "高烈度"},
		},
	}
	normalizeSubscription(&sub)

	medium := simulationTargetIntensity(sub, "medium")
	if notifyLevelForIntensityRank(sub, medium) != "active" {
		t.Fatalf("medium simulation target should match active band, got intensity %d level %q", medium, notifyLevelForIntensityRank(sub, medium))
	}
	large := simulationTargetIntensity(sub, "large")
	if notifyLevelForIntensityRank(sub, large) != "critical" {
		t.Fatalf("large simulation target should match critical band, got intensity %d level %q", large, notifyLevelForIntensityRank(sub, large))
	}
}

func TestSimulationPushOptionsUseButtonLevel(t *testing.T) {
	cfg := Config{Bark: BarkConfig{Sound: "alarm", Volume: 10, Call: true}}
	sub := Subscription{
		NotifyRules: NotificationRules{PassiveMax: 1, ActiveMax: 2, CriticalMin: 3},
		NotifyBands: []NotificationBand{
			{Min: 1, Max: 1, Level: "passive", Label: "低烈度"},
			{Min: 2, Max: 3, Level: "active", Label: "中等烈度"},
			{Min: 4, Max: notificationOpenEndedMax, Level: "critical", Label: "高烈度"},
		},
	}
	normalizeSubscription(&sub)
	decision := Decision{EstimatedIntensity: 3, EstimatedIntensityRank: 3}
	event := Event{Type: "simulate_eew", Raw: RawEvent{"simulation_kind": "large"}}

	options := pushOptions(cfg, event, sub, decision)
	if options.Level != "critical" || !options.Call || options.Sound != "alarm" {
		t.Fatalf("large simulation should use critical options regardless of overlapping rank, got %#v", options)
	}
	_, _, body := formatAlert(event, decision, sub)
	if !strings.Contains(body, "提醒: 强行响铃（铃声不可换）") {
		t.Fatalf("large simulation body should show critical level, got %q", body)
	}
}

func TestFormatAlertUsesWarningCopy(t *testing.T) {
	now := time.Date(2026, 6, 29, 0, 11, 19, 0, beijingTZ)
	event := Event{
		Type:         "cenc_eew",
		Hypocenter:   "四川宜宾市高县",
		Latitude:     28.50,
		Longitude:    104.69,
		Magnitude:    5.5,
		DepthKM:      6,
		MaxIntensity: "7",
		ReportNum:    1,
		OriginTime:   time.Date(2026, 6, 29, 0, 12, 7, 0, beijingTZ),
	}
	decision := Decision{
		DistanceKM:             223,
		HypocentralKM:          223,
		EstimatedIntensity:     3.9,
		EstimatedIntensityRank: 4,
		SecondsToP:             27,
		SecondsToS:             48,
		PArrival:               now.Add(27 * time.Second),
		SArrival:               now.Add(48 * time.Second),
	}
	sub := Subscription{NotifyBands: []NotificationBand{{Min: 3, Max: notificationOpenEndedMax, Level: "critical", Label: "高烈度"}}}
	title, subtitle, body := formatAlert(event, decision, sub)
	if title != "地震警报 48秒后到达" {
		t.Fatalf("unexpected title: %q", title)
	}
	if subtitle != "M5.5 预计烈度 3.9（较强震感）" {
		t.Fatalf("unexpected subtitle: %q", subtitle)
	}
	for _, want := range []string{
		"四川宜宾市高县 距223km",
		"预计到达时间 00:12:07",
		"请准备好应对较强震感。保持冷静，寻找安全场所。",
		"来源: CENC 第1报",
		"震源: 28.50, 104.69 深度6km",
		"距离: 震中223km 震源223km",
		"预计: P波+27秒 S波+48秒 烈度3.9",
		"提醒: 强行响铃（铃声不可换）",
		"震级: M5.5 最大烈度7",
		"发震: 2026-06-29 00:12:07",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("alert body missing %q in %q", want, body)
		}
	}
}

func TestAddBarkIconParam(t *testing.T) {
	params := map[string]string{"url": "https://example.com/alert/x"}
	addBarkIconParam(Config{Server: ServerConfig{PublicURL: "https://eew.example.test/"}}, params, "critical")
	if params["icon"] != "https://eew.example.test/bark-icon.png?level=high&v=10" {
		t.Fatalf("unexpected bark icon url: %#v", params)
	}

	params = map[string]string{}
	addBarkIconParam(Config{}, params, "active")
	if _, ok := params["icon"]; ok {
		t.Fatalf("icon should not be set without public url: %#v", params)
	}

	if barkIconLevelForNotifyLevel("passive") != "low" || barkIconLevelForNotifyLevel("active") != "medium" || barkIconLevelForNotifyLevel("critical") != "high" {
		t.Fatal("unexpected level icon mapping")
	}
}

func TestValidateSubscriptionRejectsMissingOrZeroLocation(t *testing.T) {
	base := Subscription{
		BarkID:      "validKey",
		BarkServer:  "https://bark.example.test",
		NotifyRules: defaultNotificationRules(),
	}
	if err := validateSubscription(base); err == nil {
		t.Fatal("expected missing location to be rejected")
	}
	base.Latitude = 0
	base.Longitude = 0
	base.Locations = []SubscriptionLocation{{Name: "zero", Latitude: 0, Longitude: 0}}
	if err := validateSubscription(base); err == nil {
		t.Fatal("expected 0,0 location to be rejected")
	}
	base.Latitude = 30.5
	base.Longitude = 104.1
	base.Locations = []SubscriptionLocation{{Name: "成都", Latitude: 30.5, Longitude: 104.1}}
	if err := validateSubscription(base); err != nil {
		t.Fatalf("expected real location to be accepted: %v", err)
	}
}

func TestValidateSubscriptionRejectsDataThatNormalizationWouldDrop(t *testing.T) {
	base := Subscription{
		BarkID:     "validKey",
		BarkServer: "https://bark.example.test",
		Locations: []SubscriptionLocation{
			{Name: "成都", Latitude: 30.5, Longitude: 104.1},
		},
		NotifyBands: defaultNotificationBands(),
	}
	tooMany := base
	tooMany.Locations = append(append([]SubscriptionLocation{}, base.Locations...),
		SubscriptionLocation{Name: "北京", Latitude: 39.9, Longitude: 116.4},
		SubscriptionLocation{Name: "上海", Latitude: 31.2, Longitude: 121.5},
		SubscriptionLocation{Name: "广州", Latitude: 23.1, Longitude: 113.3},
	)
	if err := validateSubscription(tooMany); err == nil {
		t.Fatal("expected more than three locations to fail instead of being truncated")
	}
	duplicateCoordinates := base
	duplicateCoordinates.Locations = append(append([]SubscriptionLocation{}, base.Locations...), SubscriptionLocation{Name: "同坐标", Latitude: 30.5, Longitude: 104.1})
	if err := validateSubscription(duplicateCoordinates); err == nil || !strings.Contains(err.Error(), "不能重复") {
		t.Fatalf("expected duplicate coordinates to be rejected, got %v", err)
	}
	badLevel := base
	badLevel.NotifyBands = []NotificationBand{{Min: 3, Max: notificationOpenEndedMax, Level: "critcal"}}
	if err := validateSubscription(badLevel); err == nil {
		t.Fatal("expected unknown notification level to fail instead of becoming passive")
	}
	emptyRange := base
	emptyRange.NotifyBands = []NotificationBand{{Min: 2, Max: 2, Level: "active"}}
	if err := validateSubscription(emptyRange); err == nil {
		t.Fatal("expected empty notification range to fail")
	}
}

func TestMinimalCancellationParsesAndUsesDistinctDedupKey(t *testing.T) {
	payload := []byte(`{"type":"cenc_eew","EventID":"cancel-123","isCancel":true}`)
	event, ok, err := parseEvent(payload)
	if err != nil || !ok {
		t.Fatalf("minimal cancellation parse ok=%v err=%v", ok, err)
	}
	if !event.Cancel || event.EventID != "cancel-123" || event.ReportNum != 1 {
		t.Fatalf("unexpected cancellation event: %#v", event)
	}
	regular := event
	regular.Cancel = false
	if event.Key() == regular.Key() {
		t.Fatal("cancellation must have a distinct dedup key")
	}
}

func TestParseEventRejectsUnsafeSourceValues(t *testing.T) {
	base := `{"type":"cenc_eew","EventID":"bad","OriginTime":"2026-07-10 10:00:00","Latitude":%s,"Longitude":%s,"Magnitude":%s,"Depth":%s}`
	tests := []struct {
		name      string
		latitude  string
		longitude string
		magnitude string
		depth     string
	}{
		{name: "latitude", latitude: "91", longitude: "104", magnitude: "5", depth: "10"},
		{name: "longitude", latitude: "30", longitude: "181", magnitude: "5", depth: "10"},
		{name: "magnitude", latitude: "30", longitude: "104", magnitude: "12", depth: "10"},
		{name: "negative depth", latitude: "30", longitude: "104", magnitude: "5", depth: "-1"},
		{name: "extreme depth", latitude: "30", longitude: "104", magnitude: "5", depth: "1001"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			payload := []byte(fmt.Sprintf(base, tc.latitude, tc.longitude, tc.magnitude, tc.depth))
			if _, ok, err := parseEvent(payload); err == nil || ok {
				t.Fatalf("unsafe event accepted ok=%v err=%v payload=%s", ok, err, payload)
			}
		})
	}
	future := time.Now().Add(30 * time.Minute).In(beijingTZ).Format("2006-01-02 15:04:05")
	payload := []byte(fmt.Sprintf(`{"type":"cenc_eew","EventID":"future","OriginTime":%q,"Latitude":30,"Longitude":104,"Magnitude":5,"Depth":10}`, future))
	if _, ok, err := parseEvent(payload); err == nil || ok {
		t.Fatalf("future event accepted ok=%v err=%v", ok, err)
	}
}

func TestCancellationDispatchBypassesEarthquakeFilters(t *testing.T) {
	var title string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Error(err)
		}
		title = r.Form.Get("title")
		_, _ = w.Write([]byte(`{"code":200}`))
	}))
	defer server.Close()
	cfg := Config{
		Bark:  BarkConfig{Server: server.URL, Group: "test"},
		Alert: AlertConfig{MaxDistanceKM: 1},
	}
	sub := Subscription{BarkID: "cancelKey", BarkServer: server.URL, Latitude: 30.5, Longitude: 104.1}
	pushed, skipped := dispatchOne(context.Background(), cfg, NewNotifier(cfg.Bark), NewAlertCache(time.Hour, 10), Event{Type: "cenc_eew", EventID: "cancel-123", Cancel: true}, sub)
	if pushed != 1 || skipped != 0 {
		t.Fatalf("cancellation dispatch pushed=%d skipped=%d", pushed, skipped)
	}
	if title != "地震预警已取消" {
		t.Fatalf("unexpected cancellation title %q", title)
	}
}

func TestNotifierDrainsSuccessfulResponsesForConnectionReuse(t *testing.T) {
	var mu sync.Mutex
	remotes := map[string]bool{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		remotes[r.RemoteAddr] = true
		mu.Unlock()
		_, _ = w.Write([]byte(strings.Repeat("x", 32<<10)))
	}))
	defer server.Close()
	notifier := NewNotifier(BarkConfig{Server: server.URL, Group: "test"})
	for i := 0; i < 2; i++ {
		if err := notifier.Send(context.Background(), server.URL, "reuseKey", "title", "subtitle", "body", nil, PushOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	mu.Lock()
	connections := len(remotes)
	mu.Unlock()
	if connections != 1 {
		t.Fatalf("expected one reused connection, got %d", connections)
	}
}

func TestNotifierUsesHTTP2AndReusesTLSConnection(t *testing.T) {
	var mu sync.Mutex
	remotes := map[string]bool{}
	protocols := []int{}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		remotes[r.RemoteAddr] = true
		protocols = append(protocols, r.ProtoMajor)
		mu.Unlock()
		_, _ = w.Write([]byte(`{"code":200}`))
	}))
	server.EnableHTTP2 = true
	server.StartTLS()
	defer server.Close()

	leaf, err := x509.ParseCertificate(server.TLS.Certificates[0].Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(leaf)
	notifier := NewNotifier(BarkConfig{Server: server.URL, Group: "test"})
	transport := notifier.client.Transport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
	notifier.client.Transport = transport
	defer transport.CloseIdleConnections()

	for i := 0; i < 2; i++ {
		if err := notifier.Send(context.Background(), server.URL, "reuseKey", "title", "subtitle", "body", nil, PushOptions{}); err != nil {
			t.Fatal(err)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	if len(remotes) != 1 {
		t.Fatalf("expected one reused HTTP/2 connection, got %d", len(remotes))
	}
	if len(protocols) != 2 || protocols[0] != 2 || protocols[1] != 2 {
		t.Fatalf("expected two HTTP/2 requests, got protocol majors %#v", protocols)
	}
}

func TestStoreSkipsZeroLocationSubscriptionsOnLoad(t *testing.T) {
	path := filepath.Join(t.TempDir(), "subscriptions.json")
	data := []byte(`[
		{"bark_id":"badKey","bark_server":"https://bark.example.test","latitude":0,"longitude":0},
		{"bark_id":"goodKey","bark_server":"https://bark.example.test","latitude":30.5,"longitude":104.1}
	]`)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	store, err := NewStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := store.Get("badKey"); ok {
		t.Fatal("expected zero-location subscription to be skipped")
	}
	if _, ok := store.Get("goodKey"); !ok {
		t.Fatal("expected valid subscription to load")
	}
}

func TestStoreRollsBackMemoryWhenPersistenceFails(t *testing.T) {
	path := filepath.Join(t.TempDir(), "subscriptions.json")
	store, err := NewStore(path)
	if err != nil {
		t.Fatal(err)
	}
	original := Subscription{
		BarkID:       "existingKey",
		BarkServer:   "https://bark.example.test",
		LocationName: "before",
		Latitude:     30.5,
		Longitude:    104.1,
	}
	if err := store.Upsert(original); err != nil {
		t.Fatal(err)
	}
	before, ok := store.Get(original.BarkID)
	if !ok {
		t.Fatal("expected initial subscription")
	}

	blocker := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	store.path = filepath.Join(blocker, "subscriptions.json")

	updated := before
	updated.LocationName = "after"
	if err := store.Upsert(updated); err == nil {
		t.Fatal("expected update persistence failure")
	}
	after, ok := store.Get(original.BarkID)
	if !ok || after.LocationName != before.LocationName || after.UpdatedAt != before.UpdatedAt {
		t.Fatalf("failed update changed in-memory subscription: before=%#v after=%#v", before, after)
	}

	newSub := Subscription{BarkID: "newKey", Latitude: 31.2, Longitude: 121.4}
	if err := store.Upsert(newSub); err == nil {
		t.Fatal("expected create persistence failure")
	}
	if _, ok := store.Get(newSub.BarkID); ok {
		t.Fatal("failed create remained in memory")
	}

	if err := store.Delete(original.BarkID); err == nil {
		t.Fatal("expected delete persistence failure")
	}
	if _, ok := store.Get(original.BarkID); !ok {
		t.Fatal("failed delete removed subscription from memory")
	}
}

func TestStoreUpsertWithLimitAllowsUpdatesAndRejectsOverflow(t *testing.T) {
	store, err := NewStore(filepath.Join(t.TempDir(), "subscriptions.json"))
	if err != nil {
		t.Fatal(err)
	}
	first := Subscription{BarkID: "firstKey", LocationName: "before", Latitude: 30.5, Longitude: 104.1}
	created, err := store.UpsertWithLimit(first, 1)
	if err != nil || !created {
		t.Fatalf("expected first subscription to be created, created=%v err=%v", created, err)
	}

	first.LocationName = "after"
	created, err = store.UpsertWithLimit(first, 1)
	if err != nil || created {
		t.Fatalf("expected existing subscription update at capacity, created=%v err=%v", created, err)
	}
	if got, ok := store.Get(first.BarkID); !ok || got.LocationName != "after" {
		t.Fatalf("expected update to persist at capacity, got %#v ok=%v", got, ok)
	}

	created, err = store.UpsertWithLimit(Subscription{BarkID: "secondKey", Latitude: 31.2, Longitude: 121.4}, 1)
	if created || !errors.Is(err, ErrSubscriptionLimit) {
		t.Fatalf("expected capacity error, created=%v err=%v", created, err)
	}
	if store.Count() != 1 {
		t.Fatalf("capacity rejection changed store count: %d", store.Count())
	}
}

func TestStoreUpsertWithLimitIsAtomicAcrossConcurrentCreates(t *testing.T) {
	store, err := NewStore(filepath.Join(t.TempDir(), "subscriptions.json"))
	if err != nil {
		t.Fatal(err)
	}
	const (
		limit      = 3
		candidates = 12
	)
	var wg sync.WaitGroup
	results := make(chan error, candidates)
	for i := 0; i < candidates; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := store.UpsertWithLimit(Subscription{
				BarkID:    fmt.Sprintf("key-%02d", i),
				Latitude:  30.5 + float64(i)/100,
				Longitude: 104.1,
			}, limit)
			results <- err
		}(i)
	}
	wg.Wait()
	close(results)

	succeeded := 0
	limited := 0
	for err := range results {
		switch {
		case err == nil:
			succeeded++
		case errors.Is(err, ErrSubscriptionLimit):
			limited++
		default:
			t.Fatalf("unexpected concurrent upsert error: %v", err)
		}
	}
	if succeeded != limit || limited != candidates-limit || store.Count() != limit {
		t.Fatalf("unexpected capacity result: succeeded=%d limited=%d count=%d", succeeded, limited, store.Count())
	}
}

func TestFanoutConcurrencyAndPriority(t *testing.T) {
	cfg := Config{Alert: AlertConfig{FanoutConcurrency: 1000}}
	if got := officialFanoutConcurrency(cfg, 10); got != 10 {
		t.Fatalf("expected concurrency capped by target count, got %d", got)
	}
	if got := officialFanoutConcurrency(cfg, 700); got != 500 {
		t.Fatalf("expected official hard cap at 500, got %d", got)
	}
	cfg.Alert.FanoutConcurrency = 0
	if got := officialFanoutConcurrency(cfg, 700); got != 100 {
		t.Fatalf("expected official default concurrency 100, got %d", got)
	}
	if got := selfHostedFanoutConcurrency(Config{}, 700); got != 700 {
		t.Fatalf("expected self-hosted concurrency capped only by target count, got %d", got)
	}
	cfg.Alert.SelfHostedConcurrency = 1500
	if got := selfHostedFanoutConcurrency(cfg, 1200); got != maxSelfHostedFanoutConcurrency {
		t.Fatalf("expected self-hosted concurrency hard cap %d, got %d", maxSelfHostedFanoutConcurrency, got)
	}
	if notifyPriority("critical") >= notifyPriority("active") || notifyPriority("active") >= notifyPriority("passive") {
		t.Fatalf("unexpected notify priorities")
	}
}

func TestBarkErrorGuardQuarantinesBadKey(t *testing.T) {
	guard := NewBarkErrorGuard(10, 2, time.Hour)
	now := time.Date(2026, 7, 8, 8, 0, 0, 0, time.UTC)
	if ok, _, _ := guard.Allow("bad-key", now); !ok {
		t.Fatal("new key should be allowed")
	}
	guard.Record("bad-key", &HTTPStatusError{StatusCode: http.StatusBadRequest, Body: "bad key"}, now)
	if ok, _, _ := guard.Allow("bad-key", now.Add(time.Second)); !ok {
		t.Fatal("key should not be quarantined before threshold")
	}
	guard.Record("bad-key", &HTTPStatusError{StatusCode: http.StatusNotFound, Body: "missing"}, now.Add(2*time.Second))
	if ok, reason, until := guard.Allow("bad-key", now.Add(3*time.Second)); ok || reason != "key_quarantined" || until.IsZero() {
		t.Fatalf("expected key quarantine, ok=%v reason=%q until=%s", ok, reason, until)
	}
	if ok, _, _ := guard.Allow("bad-key", now.Add(2*time.Hour)); !ok {
		t.Fatal("key should be allowed after quarantine expires")
	}
}

func TestBarkErrorGuardGlobalBudget(t *testing.T) {
	guard := NewBarkErrorGuard(2, 10, time.Hour)
	now := time.Date(2026, 7, 8, 8, 0, 0, 0, time.UTC)
	guard.Record("a", &HTTPStatusError{StatusCode: http.StatusInternalServerError, Body: "x"}, now)
	guard.Record("b", &HTTPStatusError{StatusCode: http.StatusBadRequest, Body: "x"}, now.Add(time.Second))
	if ok, reason, _ := guard.Allow("c", now.Add(2*time.Second)); ok || reason != "global_error_budget" {
		t.Fatalf("expected global budget stop, ok=%v reason=%q", ok, reason)
	}
	if ok, _, _ := guard.Allow("c", now.Add(6*time.Minute)); !ok {
		t.Fatal("global budget should reset after window")
	}
}

func TestRetryableBarkError(t *testing.T) {
	if !retryableBarkError(errors.New("connection reset")) {
		t.Fatal("network errors should be retryable")
	}
	if !retryableBarkError(&HTTPStatusError{StatusCode: http.StatusTooManyRequests, Body: "rate limited"}) {
		t.Fatal("429 should be retryable")
	}
	if !retryableBarkError(&HTTPStatusError{StatusCode: http.StatusBadGateway, Body: "bad gateway"}) {
		t.Fatal("5xx should be retryable")
	}
	if !retryableBarkError(&HTTPStatusError{StatusCode: http.StatusBadRequest, Body: `{"message":"Error 1040: Too many connections"}`}) {
		t.Fatal("legacy Bark database saturation response should be retryable")
	}
	if !retryableBarkError(&HTTPStatusError{StatusCode: http.StatusBadRequest, Body: "device database temporarily unavailable"}) {
		t.Fatal("legacy Bark temporary database response should be retryable")
	}
	if retryableBarkError(&HTTPStatusError{StatusCode: http.StatusBadRequest, Body: "BadDeviceToken"}) {
		t.Fatal("400 should not be retryable")
	}
	if retryableBarkError(&HTTPStatusError{StatusCode: http.StatusNotFound, Body: "missing key"}) {
		t.Fatal("404 should not be retryable")
	}
}

func TestNormalizeBarkIDInput(t *testing.T) {
	key, err := normalizeBarkIDInput("vRvm9tubpnHJYsX7fE2EYQ")
	if err != nil || key != "vRvm9tubpnHJYsX7fE2EYQ" {
		t.Fatalf("unexpected key normalization: key=%q err=%v", key, err)
	}
	key, err = normalizeBarkIDInput("https://api.day.app/vRvm9tubpnHJYsX7fE2EYQ/")
	if err != nil || key != "vRvm9tubpnHJYsX7fE2EYQ" {
		t.Fatalf("unexpected url normalization: key=%q err=%v", key, err)
	}
	if _, err := normalizeBarkIDInput("https://example.com/vRvm9tubpnHJYsX7fE2EYQ/"); err == nil {
		t.Fatal("expected non-Bark URL to fail")
	}
	if _, err := normalizeBarkIDInput("https://api.day.app/vRvm9tubpnHJYsX7fE2EYQ/bad path"); err != nil {
		t.Fatalf("extra URL path segments should be ignored after key extraction: %v", err)
	}
	key, err = normalizeBarkIDInput("https://api.day.app/vRvm9tubpnHJYsX7fE2EYQ/%E8%BF%99%E9%87%8C%E6%94%B9%E6%88%90%E4%BD%A0%E8%87%AA%E5%B7%B1%E7%9A%84%E6%8E%A8%E9%80%81%E5%86%85%E5%AE%B9")
	if err != nil || key != "vRvm9tubpnHJYsX7fE2EYQ" {
		t.Fatalf("unexpected url with push content normalization: key=%q err=%v", key, err)
	}
}

func TestNormalizeBarkServerPrefersConfiguredDefaultAndRecognizesOptionalSelfHosted(t *testing.T) {
	cfg := Config{Bark: BarkConfig{Server: "https://api.day.app", SelfHostedServer: "https://bark.example.test"}}
	if got := normalizeBarkServer("", cfg); got != "https://api.day.app" {
		t.Fatalf("unexpected default Bark server: %q", got)
	}
	if !isOfficialBarkServer(normalizeBarkServer("https://api.day.app/", cfg)) {
		t.Fatal("official Bark server was not recognized")
	}
	if !isSelfHostedBarkServer("https://bark.example.test", cfg) {
		t.Fatal("configured self-hosted Bark server was not recognized")
	}
}

func TestBarkIDFromRequestPrefersBearerAndSupportsLegacyPath(t *testing.T) {
	t.Run("bearer", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/subscription/legacyKey", nil)
		req.Header.Set("Authorization", "Bearer headerKey")
		got, err := barkIDFromRequest(req, "/api/subscription/")
		if err != nil {
			t.Fatal(err)
		}
		if got != "headerKey" {
			t.Fatalf("expected bearer key to take precedence, got %q", got)
		}
	})

	t.Run("legacy path", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/subscription/legacyKey", nil)
		got, err := barkIDFromRequest(req, "/api/subscription/")
		if err != nil {
			t.Fatal(err)
		}
		if got != "legacyKey" {
			t.Fatalf("unexpected legacy key: %q", got)
		}
	})

	t.Run("missing", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/subscription", nil)
		if _, err := barkIDFromRequest(req, "/api/subscription/"); err == nil {
			t.Fatal("expected request without credentials to fail")
		}
	})

	t.Run("malformed bearer", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/subscription", nil)
		req.Header.Set("Authorization", "Basic headerKey")
		if _, err := barkIDFromRequest(req, "/api/subscription/"); err == nil {
			t.Fatal("expected non-Bearer authorization to fail")
		}
	})
}

func TestRedactSensitivePathRemovesBarkKeys(t *testing.T) {
	const key = "secretBarkKey123"
	tests := []string{
		"/api/subscription/" + key,
		"/api/bark-key/" + key,
		"/api/history/" + key,
		"/api/simulations/" + key,
		"/api/simulate-history/" + key,
		"/api/simulate/" + key,
		"/api/unsubscribe/" + key,
		"/manage/" + key,
		"/alert/" + key,
	}
	for _, path := range tests {
		t.Run(path, func(t *testing.T) {
			got := redactSensitivePath(path)
			if strings.Contains(got, key) {
				t.Fatalf("redacted path leaked Bark Key: %q", got)
			}
			prefix := strings.TrimSuffix(path, key)
			if !strings.HasPrefix(got, prefix) {
				t.Fatalf("redacted path lost route prefix: got %q want prefix %q", got, prefix)
			}
		})
	}
	if got := redactSensitivePath("/api/stats"); got != "/api/stats" {
		t.Fatalf("non-sensitive path changed: %q", got)
	}
}

func TestLogRequestRedactsSensitivePath(t *testing.T) {
	var output bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&output)
	t.Cleanup(func() { log.SetOutput(previous) })

	handler := logRequest(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	const key = "secretBarkKey123"
	req := httptest.NewRequest(http.MethodPost, "/api/simulate/"+key, nil)
	handler.ServeHTTP(httptest.NewRecorder(), req)

	logged := output.String()
	if strings.Contains(logged, key) {
		t.Fatalf("request log leaked Bark Key: %q", logged)
	}
	if !strings.Contains(logged, "/api/simulate/") {
		t.Fatalf("request log lost route context: %q", logged)
	}
}

func TestSimulationPreviewsFollowNotificationBands(t *testing.T) {
	cfg := Config{Alert: AlertConfig{SWaveKMS: 3.5, PWaveKMS: 6.0}}
	sub := Subscription{
		Latitude:    31.2304,
		Longitude:   121.4737,
		NotifyRules: NotificationRules{PassiveMax: 1, ActiveMax: 2, CriticalMin: 3},
	}
	normalizeSubscription(&sub)

	got := map[string]SimulationPreview{}
	for _, preview := range simulationPreviews(cfg, sub) {
		got[preview.Kind] = preview
	}
	if got["small"].NotifyLevel != "passive" {
		t.Fatalf("small test should be passive, got %#v", got["small"])
	}
	if got["medium"].NotifyLevel != "active" {
		t.Fatalf("medium test should be active, got %#v", got["medium"])
	}
	if got["large"].NotifyLevel != "critical" {
		t.Fatalf("large test should be critical, got %#v", got["large"])
	}
}

func TestSimulationKindAliasesDoNotFallBackToSmall(t *testing.T) {
	cfg := Config{Alert: AlertConfig{SWaveKMS: 3.5, PWaveKMS: 6.0}}
	sub := Subscription{
		Latitude:    31.2304,
		Longitude:   121.4737,
		NotifyRules: NotificationRules{PassiveMax: 1, ActiveMax: 2, CriticalMin: 3},
	}
	normalizeSubscription(&sub)

	for _, kind := range []string{"large", "critical", "high", "strong", "severe"} {
		event := simulatedEvent([]Subscription{sub}, kind)
		selectedSub, decision := nearestSubscriptionForEvent(cfg, sub, event)
		if got := notifyLevelForIntensityRank(selectedSub, decision.EstimatedIntensityRank); got != "critical" {
			t.Fatalf("%s test should be critical, got level=%q event=%#v decision=%#v", kind, got, event, decision)
		}
	}
}

func TestLargeSimulationUsesConfiguredCriticalBand(t *testing.T) {
	cfg := Config{Alert: AlertConfig{SWaveKMS: 3.5, PWaveKMS: 6.0}}
	sub := Subscription{
		Latitude:    31.2304,
		Longitude:   121.4737,
		NotifyRules: NotificationRules{PassiveMax: 2, ActiveMax: 5, CriticalMin: 6},
	}
	normalizeSubscription(&sub)

	event := simulatedEvent([]Subscription{sub}, "large")
	selectedSub, decision := nearestSubscriptionForEvent(cfg, sub, event)
	unadjustedLevel := notifyLevelForIntensityRank(selectedSub, decision.EstimatedIntensityRank)
	decision = adjustSimulationDecision(event, selectedSub, decision)
	if got := notifyLevelForIntensityRank(selectedSub, decision.EstimatedIntensityRank); got != "critical" {
		t.Fatalf("large test should follow configured critical band, before=%q after=%q decision=%#v", unadjustedLevel, got, decision)
	}

	options := pushOptions(Config{Bark: BarkConfig{Sound: "alarm", Volume: 10}}, event, selectedSub, decision)
	if options.Level != "critical" || !options.Call || options.Sound != "alarm" || options.Volume != 10 {
		t.Fatalf("large test should send critical Bark options, got %#v", options)
	}
}

func TestHistoricalAlertIsClearlyMarkedAsTest(t *testing.T) {
	record := HistoryRecord{
		Source:       "cenc",
		Key:          "No1",
		EventID:      "hist-1",
		OriginTime:   "2024-01-02 03:04:05",
		Hypocenter:   "测试震中",
		Latitude:     31.2,
		Longitude:    121.4,
		Magnitude:    5.1,
		DepthKM:      10,
		MaxIntensity: "5",
	}
	event := historicalEvent(record)
	if time.Since(event.OriginTime) > time.Second {
		t.Fatalf("historical replay origin should be current for countdown, got %s", event.OriginTime)
	}
	sub := Subscription{Latitude: 31.0, Longitude: 121.0}
	normalizeSubscription(&sub)
	decision := evaluate(Config{Alert: AlertConfig{SWaveKMS: 3.5, PWaveKMS: 6.0}}, sub, event)
	title, _, body := formatAlert(event, decision, sub)
	if !strings.Contains(title, "历史地震复现测试") || !strings.Contains(body, "[历史复现测试]") || !strings.Contains(body, "不是当前发生的地震") {
		t.Fatalf("historical alert must be clearly marked as a test, title=%q body=%q", title, body)
	}
	if !strings.Contains(body, "来源: CENC 第1报") || !strings.Contains(body, "发震: 2024-01-02 03:04:05") || !strings.Contains(body, "预计: P波+") || !strings.Contains(body, "S波+") {
		t.Fatalf("historical alert missing real source/time, body=%q", body)
	}
}

func TestHistoricalZeroIntensityDoesNotEscalate(t *testing.T) {
	sub := Subscription{NotifyBands: defaultNotificationBands()}
	event := Event{Type: "history_simulate_cenc"}
	decision := Decision{EstimatedIntensity: 0}
	if got := notifyLevelForEvent(event, sub, decision); got != "" {
		t.Fatalf("zero intensity should not match the default bands, got %q", got)
	}
	options := pushOptions(Config{Bark: BarkConfig{Level: "critical", Sound: "alarm", Volume: 10, Call: true}}, event, sub, decision)
	if options.Level != "passive" || options.Call || options.Sound != "" || options.Volume != 0 {
		t.Fatalf("unmatched test intensity must fail safe to passive, got %#v", options)
	}
	if bypassDeliveryFilters(event) {
		t.Fatal("historical replay must follow real delivery filters")
	}
}

func TestHistoricalIntensityGapDoesNotEscalate(t *testing.T) {
	sub := Subscription{NotifyBands: []NotificationBand{
		{Min: 0, Max: 2, Level: "passive"},
		{Min: 3, Max: notificationOpenEndedMax, Level: "critical"},
	}}
	event := Event{Type: "history_simulate_cenc"}
	if got := notifyLevelForEvent(event, sub, Decision{EstimatedIntensity: 2.5}); got != "" {
		t.Fatalf("intensity in a configured gap should not trigger, got %q", got)
	}
}

func TestSimulationWithoutRequestedBandDoesNotEscalate(t *testing.T) {
	sub := Subscription{NotifyBands: []NotificationBand{{Min: 0, Max: 2, Level: "passive"}}}
	event := Event{Type: "simulate_eew", Raw: RawEvent{"simulation_kind": "large"}}
	if got := notifyLevelForEvent(event, sub, Decision{EstimatedIntensity: 6}); got != "" {
		t.Fatalf("missing critical band should reject a large simulation, got %q", got)
	}
}

func TestDispatchOneSkipsUnmatchedHistoricalReplay(t *testing.T) {
	var requests atomic.Int32
	bark := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer bark.Close()
	cfg := Config{
		Bark:  BarkConfig{Server: bark.URL, SelfHostedServer: bark.URL, SelfHostedInternalServer: bark.URL, Level: "critical"},
		Alert: AlertConfig{SWaveKMS: 3.5, PWaveKMS: 6.0},
	}
	sub := Subscription{BarkID: "key", BarkServer: bark.URL, Latitude: 31, Longitude: 121, NotifyBands: defaultNotificationBands()}
	event := Event{Type: "history_simulate_cenc", EventID: "hist-zero", ReportNum: 1, OriginTime: time.Now(), Latitude: 40, Longitude: 130, Magnitude: 3, DepthKM: 10}
	pushed, skipped := dispatchOne(context.Background(), cfg, NewNotifier(cfg.Bark), NewAlertCache(time.Hour, 10), event, sub)
	if pushed != 0 || skipped != 1 || requests.Load() != 0 {
		t.Fatalf("unmatched historical replay should not reach Bark: pushed=%d skipped=%d requests=%d", pushed, skipped, requests.Load())
	}
}

func TestAlertBodyIncludesConfiguredIntensityBand(t *testing.T) {
	event := Event{Type: "cenc_eew", EventID: "x", Hypocenter: "测试震中", Latitude: 30, Longitude: 104, Magnitude: 5.0, DepthKM: 10, ReportNum: 1, OriginTime: time.Now()}
	decision := Decision{DistanceKM: 120, HypocentralKM: 121, EstimatedIntensity: 3, EstimatedIntensityRank: 3, SecondsToP: 10, SecondsToS: 20}
	sub := Subscription{NotifyRules: NotificationRules{PassiveMax: 1, ActiveMax: 3, CriticalMin: 4}}
	_, _, body := formatAlert(event, decision, sub)
	if !strings.Contains(body, "提醒: 勿扰静音不响铃") {
		t.Fatalf("expected active intensity band in alert body, got %q", body)
	}
}

func TestAlertPageIncludesSubscriptionActions(t *testing.T) {
	rec := httptest.NewRecorder()
	now := time.Now()
	renderAlertPage(rec, AlertPage{
		Event: Event{
			Type:       "cenc_eew",
			EventID:    "x",
			ReportNum:  1,
			OriginTime: now,
			Hypocenter: "测试震中",
			Latitude:   30,
			Longitude:  104,
			Magnitude:  5,
			DepthKM:    10,
		},
		Decision: Decision{
			DistanceKM:             100,
			HypocentralKM:          101,
			EstimatedIntensity:     2,
			EstimatedIntensityRank: 2,
			PArrival:               now.Add(10 * time.Second),
			SArrival:               now.Add(20 * time.Second),
			SecondsToP:             10,
			SecondsToS:             20,
		},
		Subscriber: Subscription{BarkID: "abc-123", Latitude: 30.5, Longitude: 104.1},
		CreatedAt:  now,
		WeChatURL:  "https://example.com/wechat",
		MapURL:     "https://example.com/map",
	})
	body := rec.Body.String()
	if !strings.Contains(body, `href="/#key=abc-123"`) || !strings.Contains(body, "订阅管理") || !strings.Contains(body, "取消订阅") || !strings.Contains(body, `api+"/api/unsubscribe"`) || !strings.Contains(body, "Authorization") {
		t.Fatalf("alert page missing subscription actions: %q", body)
	}
	if strings.Contains(body, `api+"/api/unsubscribe/"`) {
		t.Fatalf("alert page should send Bark Key in authorization header, not URL path: %q", body)
	}
	if strings.Contains(body, `href="/manage/abc-123"`) || strings.Contains(body, ">测试页</a>") {
		t.Fatalf("alert page should link to subscription management instead of test page: %q", body)
	}
	if !strings.Contains(body, `src="/leaflet/leaflet.js"`) || !strings.Contains(body, `href="/leaflet/leaflet.css"`) || strings.Contains(body, "unpkg.com") {
		t.Fatalf("alert page must load Leaflet locally because it contains a Bark Key: %q", body)
	}
	if !strings.Contains(body, `const tileURL="/map-tiles/{z}/{x}/{y}{r}.png"`) || strings.Contains(body, `https://{s}.basemaps.cartocdn.com`) {
		t.Fatalf("alert page must load map tiles through the same-origin cache: %q", body)
	}
}

func TestIndexAutoFillsBarkKeyFromFragmentAndUsesFixedAPIPaths(t *testing.T) {
	data, err := publicFS.ReadFile("public/index.html")
	if err != nil {
		t.Fatal(err)
	}
	body := string(data)
	for _, required := range []string{
		`new URLSearchParams(location.hash.replace(/^#/, ""))`,
		`"/manage#key=" + encodeURIComponent(key)`,
		`result.set("Authorization", "Bearer " + key)`,
		`barkFetch("/api/subscription", key`,
		`barkFetch("/api/unsubscribe", key`,
		`administrative_id: administrativeID`,
		`同一最低行政区只能添加一个监测地点。`,
		`await resolveLocation(item.latitude, item.longitude, true)`,
		`<select class="band-min" aria-label="烈度阈值">`,
		`${value.toFixed(1)}`,
		`及以上</option>`,
		`if (rule.min >= strongerThreshold) rule.min = strongerThreshold - 1;`,
		`烈度通知方式`,
		`>测试页</a>`,
		`不弹窗通知`,
		`勿扰静音不响铃`,
		`强行响铃`,
		`填写 Bark Key、监测位置和提醒规则即可订阅正式地震预警`,
		`class="actions" id="subscription-actions">`,
		`id="page-nav" aria-label="页面导航">`,
		`id="submit" type="submit">订阅 / 更新</button>`,
		`typeof json.data.persisted !== "boolean"`,
		`订阅已生效，测试通知已发送。`,
		`submit.textContent = "订阅 / 更新"`,
		`window.addEventListener("pageshow", () => { submit.disabled = false; });`,
		`当前页面使用 HTTP，浏览器无法获取设备定位。请使用上方搜索框搜索地址并添加定位，或改用 HTTPS 地址。`,
		`定位失败或未获得定位权限。请使用上方搜索框搜索地址并添加定位。`,
		`window.isSecureContext`,
		`function temporaryTestPayload(key)`,
		`body: JSON.stringify(payload)`,
		`bark_id: key`,
		`barkInput.value = urlBarkID || ""`,
		`[hidden] { display: none !important; }`,
		`manageNav.addEventListener("click"`,
		`id="unsubscribe" type="button" hidden`,
		`const defaultBarkServer = __EEW_DEFAULT_BARK_SERVER_JSON__;`,
		`const selfHostedBarkServer = __EEW_SELF_HOSTED_BARK_SERVER_JSON__;`,
		`let selectedBarkServer = defaultBarkServer;`,
		`/api/bark-key?server=`,
		`bark_server: selectedBarkServer || defaultBarkServer`,
		`OpenStreetMap`,
		`CARTO`,
	} {
		if !strings.Contains(body, required) {
			t.Fatalf("index is missing secure Bark Key flow %q", required)
		}
	}
	for _, forbidden := range []string{
		`本次配置未写入正式订阅服务`,
		`不提供正式地震预警订阅服务`,
		`无限期关停`,
		`"/api/subscription/" + encodeURIComponent(key)`,
		`"/api/unsubscribe/" + encodeURIComponent(key)`,
		`bindTooltip(item.name`,
		`subscriptionActions.hidden = true`,
		`subscriptionActions.hidden = !existingSubscription`,
		`pageNav.hidden = true`,
		`pageNav.hidden = !existingSubscription`,
		`unpkg.com`,
		`至（不含）`,
		`id="stats"`,
		`refreshStats()`,
		`已有正式订阅不受影响`,
		`更新原有订阅`,
		`原有正式订阅`,
		`点击“测试订阅”`,
		`barkInput.value = urlBarkID || localStorage.getItem("eew_bark_id")`,
	} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("index puts Bark Key back into API path: %q", forbidden)
		}
	}
	if !strings.Contains(body, `src="/leaflet/leaflet.js"`) || !strings.Contains(body, `href="/leaflet/leaflet.css"`) {
		t.Fatal("index must load the vendored Leaflet assets")
	}
	if !strings.Contains(body, `tooltip.textContent = item.name`) || !strings.Contains(body, `savedMarker.bindTooltip(tooltip)`) {
		t.Fatal("map tooltip must use a text node instead of HTML content")
	}
}

func TestRootInjectsConfiguredBarkServer(t *testing.T) {
	store, err := NewStore(filepath.Join(t.TempDir(), "subscriptions.json"))
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{Bark: BarkConfig{Server: "https://api.day.app", SelfHostedServer: "https://bark.example.test"}}
	handler := newHTTPHandler(cfg, store, NewAlertCache(time.Hour, 10), NewNotifier(cfg.Bark), &RuntimeHealth{})
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("root status=%d body=%s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	if strings.Contains(body, "__EEW_DEFAULT_BARK_SERVER_JSON__") || strings.Contains(body, "__EEW_SELF_HOSTED_BARK_SERVER_JSON__") ||
		!strings.Contains(body, `const defaultBarkServer = "https://api.day.app";`) ||
		!strings.Contains(body, `const selfHostedBarkServer = "https://bark.example.test";`) {
		t.Fatalf("configured Bark server was not injected safely: %q", body)
	}
}

func TestIndexUsesSameOriginMapTiles(t *testing.T) {
	data, err := publicFS.ReadFile("public/index.html")
	if err != nil {
		t.Fatal(err)
	}
	body := string(data)
	if !strings.Contains(body, `L.tileLayer("/map-tiles/{z}/{x}/{y}{r}.png"`) {
		t.Fatal("index map must use the same-origin map tile cache")
	}
	if strings.Contains(body, "basemaps.cartocdn.com") {
		t.Fatal("index must not load map tiles directly from CARTO")
	}
}

func TestManageHistoryAllowsRuleResponseForUnmatchedEvents(t *testing.T) {
	rec := httptest.NewRecorder()
	renderManagePage(rec)
	body := rec.Body.String()
	for _, required := range []string{
		`未达到通知烈度`,
		`正在按当前订阅规则检查该历史地震`,
		`role="status" aria-live="polite"`,
		`bottom:16px`,
		`background:#fff`,
		`id="status-close"`,
		`window.setTimeout(hideStatus`,
		`id="access-panel" hidden`,
		`测试页需要已有正式订阅`,
		`location.href="/manage#key="+encodeURIComponent(key)`,
		`protectedContent.forEach(function(node){node.hidden=false})`,
		`<link rel="icon" href="/eew-favicon.svg" type="image/svg+xml">`,
	} {
		if !strings.Contains(body, required) {
			t.Fatalf("manage page is missing unmatched-history response UI %q", required)
		}
	}
	if strings.Contains(body, `const disabled=notifyLevel`) {
		t.Fatal("manage page must allow unmatched historical events to request a rule response")
	}
	if strings.Contains(body, `queryKey||localStorage.getItem("eew_bark_id")`) || strings.Contains(body, `body.authorized{visibility:visible}`) || strings.Contains(body, `location.replace(`) {
		t.Fatal("manage page must open with an explicit Bark Key prompt instead of hiding or redirecting")
	}
}

func TestHTTPServesVendoredLeafletWithCompatibleCSP(t *testing.T) {
	store, err := NewStore(filepath.Join(t.TempDir(), "subscriptions.json"))
	if err != nil {
		t.Fatal(err)
	}
	handler := newHTTPHandler(Config{}, store, NewAlertCache(time.Hour, 10), NewNotifier(BarkConfig{}), &RuntimeHealth{})

	for _, target := range []string{"/eew-favicon.svg", "/leaflet/leaflet.css", "/leaflet/leaflet.js", "/leaflet/images/marker-icon.png"} {
		req := httptest.NewRequest(http.MethodGet, target, nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK || rec.Body.Len() == 0 {
			t.Fatalf("asset %s status=%d bytes=%d", target, rec.Code, rec.Body.Len())
		}
	}

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	csp := rec.Header().Get("Content-Security-Policy")
	if strings.Contains(csp, "unpkg.com") || strings.Contains(csp, "autonavi.com") || strings.Contains(csp, "cartocdn.com") || !strings.Contains(csp, "img-src 'self' data:") {
		t.Fatalf("unexpected map CSP: %q", csp)
	}
	if strings.Contains(csp, "upgrade-insecure-requests") || rec.Header().Get("Strict-Transport-Security") != "" {
		t.Fatalf("plain HTTP response must not force relative assets onto HTTPS: csp=%q hsts=%q", csp, rec.Header().Get("Strict-Transport-Security"))
	}

	httpsReq := httptest.NewRequest(http.MethodGet, "/", nil)
	httpsReq.Header.Set("X-Forwarded-Proto", "https")
	httpsRec := httptest.NewRecorder()
	handler.ServeHTTP(httpsRec, httpsReq)
	if !strings.Contains(httpsRec.Header().Get("Content-Security-Policy"), "upgrade-insecure-requests") || httpsRec.Header().Get("Strict-Transport-Security") == "" {
		t.Fatalf("HTTPS response is missing transport security headers: csp=%q hsts=%q", httpsRec.Header().Get("Content-Security-Policy"), httpsRec.Header().Get("Strict-Transport-Security"))
	}
}

func TestMapTileProxyCachesUpstreamResponse(t *testing.T) {
	var requests atomic.Int32
	pngData, err := base64.StdEncoding.DecodeString("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=")
	if err != nil {
		t.Fatal(err)
	}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Type", "image/png")
		_, _ = w.Write(pngData)
	}))
	defer upstream.Close()
	originalUpstreams := mapTileUpstreams
	mapTileUpstreams = []string{upstream.URL + "/%d/%d/%d%s.png"}
	defer func() { mapTileUpstreams = originalUpstreams }()

	handler := newMapTileHandler(t.TempDir())
	for i := 0; i < 2; i++ {
		req := httptest.NewRequest(http.MethodGet, "/map-tiles/5/26/13.png", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK || !bytes.Equal(rec.Body.Bytes(), pngData) {
			t.Fatalf("tile request %d status=%d bytes=%d", i+1, rec.Code, rec.Body.Len())
		}
		if got := rec.Header().Get("Cache-Control"); !strings.Contains(got, "s-maxage=2592000") {
			t.Fatalf("tile request missing shared cache policy: %q", got)
		}
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("expected one upstream request after disk cache, got %d", got)
	}
}

func TestMapTileProxyPreservesRetinaTiles(t *testing.T) {
	var requestedPath string
	pngData, err := base64.StdEncoding.DecodeString("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=")
	if err != nil {
		t.Fatal(err)
	}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestedPath = r.URL.Path
		w.Header().Set("Content-Type", "image/png")
		_, _ = w.Write(pngData)
	}))
	defer upstream.Close()
	originalUpstreams := mapTileUpstreams
	mapTileUpstreams = []string{upstream.URL + "/%d/%d/%d%s.png"}
	defer func() { mapTileUpstreams = originalUpstreams }()

	handler := newMapTileHandler(t.TempDir())
	req := httptest.NewRequest(http.MethodGet, "/map-tiles/5/26/13@2x.png", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || requestedPath != "/5/26/13@2x.png" {
		t.Fatalf("retina tile status=%d upstream=%q", rec.Code, requestedPath)
	}
}

func TestMapTileProxyRejectsInvalidCoordinates(t *testing.T) {
	handler := newMapTileHandler(t.TempDir())
	for _, target := range []string{
		"/map-tiles/19/0/0.png",
		"/map-tiles/5/32/0.png",
		"/map-tiles/5/0/32.png",
		"/map-tiles/5/-1/0.png",
		"/map-tiles/5/0/0.jpg",
	} {
		req := httptest.NewRequest(http.MethodGet, target, nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("invalid tile %s status=%d", target, rec.Code)
		}
	}
}

func TestHTTPSubscribeRejectsUnapprovedBarkServer(t *testing.T) {
	store, err := NewStore(filepath.Join(t.TempDir(), "subscriptions.json"))
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{Bark: BarkConfig{Server: "https://api.day.app", SelfHostedServer: "https://bark.example.test"}}
	handler := newHTTPHandler(cfg, store, NewAlertCache(time.Hour, 10), NewNotifier(cfg.Bark), &RuntimeHealth{})
	body := `{"bark_id":"validKey123","bark_server":"http://127.0.0.1:8080","latitude":30.5,"longitude":104.1}`
	req := httptest.NewRequest(http.MethodPost, "/api/subscribe", strings.NewReader(body))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("unapproved Bark server status=%d body=%s", rec.Code, rec.Body.String())
	}
	if store.Count() != 0 {
		t.Fatal("unapproved Bark server was persisted")
	}
}

func TestHTTPSubscribeCreatesOfficialSubscriptionAfterTestPush(t *testing.T) {
	var pushes atomic.Int32
	notifier := NewNotifier(BarkConfig{Server: "https://api.day.app", SelfHostedServer: "https://bark.example.test", Group: "test"}, AlertConfig{
		SWaveKMS:          3.5,
		PWaveKMS:          6,
		SendRetryAttempts: 1,
		SendRetryDelayMS:  1,
	})
	notifier.client = &http.Client{
		Timeout: time.Second,
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if req.URL.Scheme != "https" || req.URL.Host != "api.day.app" || req.URL.Path != "/officialTestKey" {
				t.Fatalf("unexpected official Bark request: %s", req.URL.String())
			}
			pushes.Add(1)
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(`{"code":200,"message":"success"}`)),
				Request:    req,
			}, nil
		}),
	}

	store, err := NewStore(filepath.Join(t.TempDir(), "subscriptions.json"))
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{
		Bark:  BarkConfig{Server: "https://api.day.app", SelfHostedServer: "https://bark.example.test", Group: "test"},
		Alert: AlertConfig{SWaveKMS: 3.5, PWaveKMS: 6, SendRetryAttempts: 1, SendRetryDelayMS: 1},
	}
	handler := newHTTPHandler(cfg, store, NewAlertCache(time.Hour, 10), notifier, &RuntimeHealth{})

	verifyReq := httptest.NewRequest(http.MethodGet, "/api/bark-key?server=https%3A%2F%2Fapi.day.app", nil)
	verifyReq.Header.Set("Authorization", "Bearer officialTestKey")
	verifyRec := httptest.NewRecorder()
	handler.ServeHTTP(verifyRec, verifyReq)
	if verifyRec.Code != http.StatusOK || !strings.Contains(verifyRec.Body.String(), `"verification":"test_push"`) {
		t.Fatalf("official Bark preflight status=%d body=%s", verifyRec.Code, verifyRec.Body.String())
	}

	body := `{"bark_id":"officialTestKey","bark_server":"https://api.day.app","latitude":30.5,"longitude":104.1,"locations":[{"name":"测试地点","latitude":30.5,"longitude":104.1}],"notify_bands":[{"min":2,"max":99,"level":"active","label":"勿扰静音不响铃"}]}`
	req := httptest.NewRequest(http.MethodPost, "/api/subscribe", strings.NewReader(body))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("official subscription status=%d body=%s", rec.Code, rec.Body.String())
	}
	if pushes.Load() != 1 {
		t.Fatalf("expected one official Bark test push, got %d", pushes.Load())
	}
	created, ok := store.Get("officialTestKey")
	if !ok || created.BarkServer != "https://api.day.app" {
		t.Fatalf("official subscription was not stored correctly: %#v", created)
	}
}

func TestHTTPSubscribeCreatesFormalSubscriptionAfterTestPush(t *testing.T) {
	var pushes atomic.Int32
	bark := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		pushes.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"code":200,"message":"success"}`)
	}))
	defer bark.Close()

	store, err := NewStore(filepath.Join(t.TempDir(), "subscriptions.json"))
	if err != nil {
		t.Fatal(err)
	}
	deviceDBPath := filepath.Join(t.TempDir(), "bark.db")
	deviceDB, err := bolt.Open(deviceDBPath, 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := deviceDB.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte("device"))
		if err != nil {
			return err
		}
		return bucket.Put([]byte("testOnlyKey"), []byte("test-device-token"))
	}); err != nil {
		_ = deviceDB.Close()
		t.Fatal(err)
	}
	if err := deviceDB.Close(); err != nil {
		t.Fatal(err)
	}
	cfg := Config{
		Bark: BarkConfig{Server: bark.URL, SelfHostedServer: bark.URL, DeviceDBPath: deviceDBPath, Group: "test"},
		Alert: AlertConfig{
			SWaveKMS:          3.5,
			PWaveKMS:          6,
			SendRetryAttempts: 1,
			SendRetryDelayMS:  1,
		},
	}
	handler := newHTTPHandler(cfg, store, NewAlertCache(time.Hour, 10), NewNotifier(cfg.Bark, cfg.Alert), &RuntimeHealth{})
	body := fmt.Sprintf(`{"bark_id":"testOnlyKey","bark_server":%q,"latitude":30.5,"longitude":104.1,"locations":[{"name":"测试地点","latitude":30.5,"longitude":104.1}],"notify_bands":[{"min":2,"max":99,"level":"active","label":"勿扰静音不响铃"}]}`, bark.URL)
	req := httptest.NewRequest(http.MethodPost, "/api/subscribe", strings.NewReader(body))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("test subscription status=%d body=%s", rec.Code, rec.Body.String())
	}
	if pushes.Load() != 1 {
		t.Fatalf("expected one Bark test push, got %d", pushes.Load())
	}
	if store.Count() != 1 {
		t.Fatalf("formal subscription was not persisted: count=%d", store.Count())
	}
	var response struct {
		Success bool `json:"success"`
		Data    struct {
			Persisted bool `json:"persisted"`
			Pushed    int  `json:"pushed"`
		} `json:"data"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if !response.Success || !response.Data.Persisted || response.Data.Pushed != 1 {
		t.Fatalf("unexpected formal subscription response: %#v", response)
	}
	created, ok := store.Get("testOnlyKey")
	if !ok || created.Latitude != 30.5 || created.Longitude != 104.1 {
		t.Fatalf("formal subscription was not stored correctly: %#v", created)
	}
}

func TestHTTPSubscribeRejectsNewUserAtCapacityBeforeTestPush(t *testing.T) {
	var pushes atomic.Int32
	bark := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		pushes.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"code":200,"message":"success"}`)
	}))
	defer bark.Close()

	store, err := NewStore(filepath.Join(t.TempDir(), "subscriptions.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Upsert(Subscription{BarkID: "existingKey", BarkServer: bark.URL, Latitude: 30.5, Longitude: 104.1}); err != nil {
		t.Fatal(err)
	}
	cfg := Config{
		Bark: BarkConfig{Server: bark.URL, SelfHostedServer: "https://bark.example.test", Group: "test"},
		Server: ServerConfig{
			SubscriptionLimit:        1,
			SubscriptionLimitMessage: "capacity reached",
		},
	}
	handler := newHTTPHandler(cfg, store, NewAlertCache(time.Hour, 10), NewNotifier(cfg.Bark, cfg.Alert), &RuntimeHealth{})
	body := fmt.Sprintf(`{"bark_id":"newKey","bark_server":%q,"latitude":31.2,"longitude":105.3,"locations":[{"name":"新地点","latitude":31.2,"longitude":105.3}],"notify_bands":[{"min":2,"max":99,"level":"active","label":"勿扰静音不响铃"}]}`, bark.URL)
	req := httptest.NewRequest(http.MethodPost, "/api/subscribe", strings.NewReader(body))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), "capacity reached") {
		t.Fatalf("capacity response status=%d body=%s", rec.Code, rec.Body.String())
	}
	if pushes.Load() != 0 {
		t.Fatalf("capacity rejection unexpectedly sent %d test pushes", pushes.Load())
	}
	if store.Count() != 1 {
		t.Fatalf("capacity rejection changed subscription count: %d", store.Count())
	}
}

func TestHTTPSubscribeUpdatesExistingSubscription(t *testing.T) {
	var pushes atomic.Int32
	bark := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		pushes.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"code":200,"message":"success"}`)
	}))
	defer bark.Close()

	store, err := NewStore(filepath.Join(t.TempDir(), "subscriptions.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Upsert(Subscription{BarkID: "existingKey", BarkServer: bark.URL, Latitude: 30.5, Longitude: 104.1}); err != nil {
		t.Fatal(err)
	}
	cfg := Config{Bark: BarkConfig{Server: bark.URL, SelfHostedServer: "https://bark.example.test", Group: "test"}}
	handler := newHTTPHandler(cfg, store, NewAlertCache(time.Hour, 10), NewNotifier(cfg.Bark, cfg.Alert), &RuntimeHealth{})
	body := fmt.Sprintf(`{"bark_id":"existingKey","bark_server":%q,"latitude":31.2,"longitude":105.3,"locations":[{"name":"更新地点","latitude":31.2,"longitude":105.3}],"notify_bands":[{"min":2,"max":99,"level":"active","label":"勿扰静音不响铃"}]}`, bark.URL)
	req := httptest.NewRequest(http.MethodPost, "/api/subscribe", strings.NewReader(body))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("existing subscription update status=%d body=%s", rec.Code, rec.Body.String())
	}
	if pushes.Load() != 0 {
		t.Fatalf("existing subscription update unexpectedly sent %d test pushes", pushes.Load())
	}
	if store.Count() != 1 {
		t.Fatalf("existing subscription count changed: %d", store.Count())
	}
	updated, ok := store.Get("existingKey")
	if !ok || updated.Latitude != 31.2 || updated.Longitude != 105.3 {
		t.Fatalf("existing subscription was not updated: %#v", updated)
	}
	var response struct {
		Success bool `json:"success"`
		Data    struct {
			Persisted bool `json:"persisted"`
		} `json:"data"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if !response.Success || !response.Data.Persisted {
		t.Fatalf("unexpected existing subscription response: %#v", response)
	}
}

func TestHTTPSimulationAcceptsTemporarySubscriptionWithoutPersisting(t *testing.T) {
	var pushes atomic.Int32
	bark := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		pushes.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"code":200,"message":"success"}`)
	}))
	defer bark.Close()

	store, err := NewStore(filepath.Join(t.TempDir(), "subscriptions.json"))
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{
		Bark: BarkConfig{Server: bark.URL, SelfHostedServer: "https://bark.example.test", Group: "test"},
		Alert: AlertConfig{
			SWaveKMS:          3.5,
			PWaveKMS:          6,
			SendRetryAttempts: 1,
			SendRetryDelayMS:  1,
		},
	}
	handler := newHTTPHandler(cfg, store, NewAlertCache(time.Hour, 10), NewNotifier(cfg.Bark, cfg.Alert), &RuntimeHealth{})
	body := fmt.Sprintf(`{"bark_id":"temporaryKey","bark_server":%q,"latitude":30.5,"longitude":104.1,"locations":[{"name":"临时地点","latitude":30.5,"longitude":104.1}],"notify_bands":[{"min":2,"max":99,"level":"active","label":"勿扰静音不响铃"}]}`, bark.URL)
	req := httptest.NewRequest(http.MethodPost, "/api/simulate?kind=medium", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer temporaryKey")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("temporary simulation status=%d body=%s", rec.Code, rec.Body.String())
	}
	if pushes.Load() != 1 {
		t.Fatalf("expected one temporary simulation push, got %d", pushes.Load())
	}
	if store.Count() != 0 {
		t.Fatalf("temporary simulation persisted a subscription: %d", store.Count())
	}
	if !strings.Contains(rec.Body.String(), `"temporary":true`) {
		t.Fatalf("temporary simulation response missing marker: %s", rec.Body.String())
	}
}

func TestNotifierRejectsPersistedUnapprovedBarkServer(t *testing.T) {
	hit := false
	evil := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hit = true
		w.WriteHeader(http.StatusOK)
	}))
	defer evil.Close()

	notifier := NewNotifier(BarkConfig{Server: "https://api.day.app", SelfHostedServer: "https://bark.example.test"})
	err := notifier.Send(context.Background(), evil.URL, "validKey123", "title", "subtitle", "body", nil, PushOptions{})
	if err == nil || !strings.Contains(err.Error(), "unapproved Bark server") {
		t.Fatalf("expected fail-closed server validation, got %v", err)
	}
	if hit {
		t.Fatal("notifier contacted an unapproved Bark server")
	}
}

func TestHTTPHistoryRefreshRequiresExistingSubscription(t *testing.T) {
	store, err := NewStore(filepath.Join(t.TempDir(), "subscriptions.json"))
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{Server: ServerConfig{HistoryPath: filepath.Join(t.TempDir(), "history.json")}}
	handler := newHTTPHandler(cfg, store, NewAlertCache(time.Hour, 10), NewNotifier(BarkConfig{}), &RuntimeHealth{})

	req := httptest.NewRequest(http.MethodGet, "/api/history?refresh=1", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("anonymous history refresh status=%d body=%s", rec.Code, rec.Body.String())
	}

	req = httptest.NewRequest(http.MethodGet, "/api/history/notSubscribed?refresh=1", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("unknown subscription history refresh status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestHTTPSubscriptionRouteAcceptsBearerWithoutRedirect(t *testing.T) {
	store, err := NewStore(filepath.Join(t.TempDir(), "subscriptions.json"))
	if err != nil {
		t.Fatal(err)
	}
	sub := Subscription{BarkID: "headerKey", BarkServer: "https://bark.example.test", Latitude: 30.5, Longitude: 104.1}
	if err := store.Upsert(sub); err != nil {
		t.Fatal(err)
	}
	handler := newHTTPHandler(Config{}, store, NewAlertCache(time.Hour, 10), NewNotifier(BarkConfig{}), &RuntimeHealth{})

	for _, tc := range []struct {
		name          string
		target        string
		authorization string
	}{
		{name: "bearer", target: "/api/subscription", authorization: "Bearer headerKey"},
		{name: "legacy", target: "/api/subscription/headerKey"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tc.target, nil)
			if tc.authorization != "" {
				req.Header.Set("Authorization", tc.authorization)
			}
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			if rec.Code != http.StatusOK {
				t.Fatalf("unexpected status %d body=%s", rec.Code, rec.Body.String())
			}
			if location := rec.Header().Get("Location"); location != "" {
				t.Fatalf("subscription route unexpectedly redirected to %q", location)
			}
		})
	}
}

func TestHTTPUnsubscribeRetainsBearerAndLegacyFlows(t *testing.T) {
	store, err := NewStore(filepath.Join(t.TempDir(), "subscriptions.json"))
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"headerKey", "legacyKey"} {
		if err := store.Upsert(Subscription{BarkID: key, Latitude: 30.5, Longitude: 104.1}); err != nil {
			t.Fatal(err)
		}
	}
	handler := newHTTPHandler(Config{}, store, NewAlertCache(time.Hour, 10), NewNotifier(BarkConfig{}), &RuntimeHealth{})

	req := httptest.NewRequest(http.MethodDelete, "/api/unsubscribe", nil)
	req.Header.Set("Authorization", "Bearer headerKey")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("bearer unsubscribe status=%d body=%s", rec.Code, rec.Body.String())
	}
	if _, ok := store.Get("headerKey"); ok {
		t.Fatal("bearer unsubscribe did not remove subscription")
	}

	req = httptest.NewRequest(http.MethodDelete, "/api/unsubscribe/legacyKey", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("legacy unsubscribe status=%d body=%s", rec.Code, rec.Body.String())
	}
	if _, ok := store.Get("legacyKey"); ok {
		t.Fatal("legacy unsubscribe did not remove subscription")
	}
}

func TestHTTPSimulationReportsDeliveryFailure(t *testing.T) {
	bark := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "upstream failed", http.StatusBadGateway)
	}))
	defer bark.Close()

	cfg := Config{
		Bark: BarkConfig{Server: bark.URL, Group: "test"},
		Alert: AlertConfig{
			SWaveKMS:          3.5,
			PWaveKMS:          6,
			SendRetryAttempts: 1,
			SendRetryDelayMS:  1,
		},
	}
	store, err := NewStore(filepath.Join(t.TempDir(), "subscriptions.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Upsert(Subscription{
		BarkID:     "failureKey",
		BarkServer: bark.URL,
		Latitude:   30.5,
		Longitude:  104.1,
	}); err != nil {
		t.Fatal(err)
	}
	handler := newHTTPHandler(cfg, store, NewAlertCache(time.Hour, 10), NewNotifier(cfg.Bark, cfg.Alert), &RuntimeHealth{})
	req := httptest.NewRequest(http.MethodPost, "/api/simulate?kind=small", nil)
	req.Header.Set("Authorization", "Bearer failureKey")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code < 400 {
		t.Fatalf("simulation delivery failure returned success status %d body=%s", rec.Code, rec.Body.String())
	}
	var response APIResponse
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if response.Success {
		t.Fatalf("simulation delivery failure reported success: %#v", response)
	}
}

func TestHTTPAdminSimulationRequiresBearerToken(t *testing.T) {
	cfg := Config{Server: ServerConfig{SimulateToken: "admin-secret"}}
	store, err := NewStore(filepath.Join(t.TempDir(), "subscriptions.json"))
	if err != nil {
		t.Fatal(err)
	}
	handler := newHTTPHandler(cfg, store, NewAlertCache(time.Hour, 10), NewNotifier(BarkConfig{}), &RuntimeHealth{})

	req := httptest.NewRequest(http.MethodPost, "/api/admin/simulate?token=admin-secret", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code < 400 {
		t.Fatalf("query token unexpectedly authorized admin simulation: %d %s", rec.Code, rec.Body.String())
	}

	req = httptest.NewRequest(http.MethodPost, "/api/admin/simulate", nil)
	req.Header.Set("Authorization", "Bearer admin-secret")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code == http.StatusForbidden || rec.Code == http.StatusUnauthorized || rec.Code == http.StatusNotFound {
		t.Fatalf("valid bearer token did not reach admin simulation: %d %s", rec.Code, rec.Body.String())
	}
}

func TestHistoricalReplayFollowsRealtimeDistanceFilter(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := Config{
		Bark: BarkConfig{Server: server.URL, Group: "test"},
		Alert: AlertConfig{
			SWaveKMS:      3.5,
			PWaveKMS:      6.0,
			MaxDistanceKM: 1000,
		},
	}
	sub := Subscription{
		BarkID:    "testKey",
		Latitude:  30.5774,
		Longitude: 103.9625,
	}
	normalizeSubscription(&sub)

	event := historicalEvent(HistoryRecord{
		Source:       "major",
		Key:          "tangshan-1976",
		EventID:      "USGS-Tangshan-1976",
		OriginTime:   "1976-07-28 03:42:00",
		Hypocenter:   "河北唐山地震",
		Latitude:     39.57,
		Longitude:    117.98,
		Magnitude:    7.8,
		DepthKM:      15,
		MaxIntensity: "XI",
	})
	decision := evaluate(cfg, sub, event)
	if decision.DistanceKM <= cfg.Alert.MaxDistanceKM {
		t.Fatalf("test setup should exceed distance filter: %#v", decision)
	}

	pushed, skipped := dispatchOne(context.Background(), cfg, NewNotifier(cfg.Bark), NewAlertCache(time.Hour, 1000), event, sub)
	if pushed != 0 || skipped != 1 {
		t.Fatalf("historical replay should follow realtime filters, pushed=%d skipped=%d", pushed, skipped)
	}
}

func TestAlertCacheKeepsDetailsWithinTTL(t *testing.T) {
	cache := NewAlertCache(24*time.Hour, 10)
	token, err := cache.Put(AlertPage{Event: Event{EventID: "x"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := cache.Get(token); !ok {
		t.Fatal("expected alert detail to be available within ttl")
	}
}

func TestAlertCacheTrimsOldestWhenFull(t *testing.T) {
	cache := NewAlertCache(24*time.Hour, 2)
	first, err := cache.Put(AlertPage{Event: Event{EventID: "first"}})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Millisecond)
	second, err := cache.Put(AlertPage{Event: Event{EventID: "second"}})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Millisecond)
	third, err := cache.Put(AlertPage{Event: Event{EventID: "third"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := cache.Get(first); ok {
		t.Fatal("expected oldest alert detail to be trimmed")
	}
	if _, ok := cache.Get(second); !ok {
		t.Fatal("expected second alert detail to remain")
	}
	if _, ok := cache.Get(third); !ok {
		t.Fatal("expected newest alert detail to remain")
	}
}

func TestAlertCacheRepeatedEvictionKeepsNewestEntries(t *testing.T) {
	cache := NewAlertCache(24*time.Hour, 3)
	tokens := make([]string, 0, 20)
	for i := 0; i < 20; i++ {
		token, err := cache.Put(AlertPage{Event: Event{EventID: fmt.Sprintf("event-%02d", i)}})
		if err != nil {
			t.Fatal(err)
		}
		tokens = append(tokens, token)
	}
	for i, token := range tokens {
		page, ok := cache.Get(token)
		want := i >= len(tokens)-3
		if ok != want {
			t.Fatalf("token %d availability=%v want %v", i, ok, want)
		}
		if ok && page.Event.EventID != fmt.Sprintf("event-%02d", i) {
			t.Fatalf("token %d returned wrong page: %#v", i, page)
		}
	}
}

func TestEventPayloadQueueCoalescesUpdatesAndPrioritizesCancellation(t *testing.T) {
	health := &RuntimeHealth{}
	queue := newEventPayloadQueue(4, health)
	regular := func(report int) []byte {
		return []byte(fmt.Sprintf(`{"type":"cenc_eew","EventID":"event-1","ReportNum":%d,"Latitude":30,"Longitude":104,"Magnitude":5,"Depth":10}`, report))
	}
	if !queue.Enqueue(regular(1), time.Now()) || !queue.Enqueue(regular(2), time.Now()) {
		t.Fatal("expected regular reports to enqueue")
	}
	snapshot := health.Snapshot()
	if snapshot.QueueDepth != 1 || snapshot.QueueCoalesced != 1 {
		t.Fatalf("unexpected coalesced queue health: %#v", snapshot)
	}
	item, ok := queue.Dequeue(context.Background())
	if !ok {
		t.Fatal("expected queued update")
	}
	event, ok, err := parseEvent(item.data)
	if err != nil || !ok || event.ReportNum != 2 {
		t.Fatalf("expected latest report, event=%#v ok=%v err=%v", event, ok, err)
	}

	if !queue.Enqueue(regular(3), time.Now()) {
		t.Fatal("expected report 3 to enqueue")
	}
	cancel := []byte(`{"type":"cenc_eew","EventID":"event-1","isCancel":true}`)
	if !queue.Enqueue(cancel, time.Now()) {
		t.Fatal("expected cancellation to enqueue")
	}
	item, ok = queue.Dequeue(context.Background())
	if !ok {
		t.Fatal("expected queued cancellation")
	}
	event, ok, err = parseEvent(item.data)
	if err != nil || !ok || !event.Cancel {
		t.Fatalf("expected cancellation to replace pending report, event=%#v ok=%v err=%v", event, ok, err)
	}
	queue.Close()
	if _, ok := queue.Dequeue(context.Background()); ok {
		t.Fatal("closed empty queue should stop")
	}
}

func TestEventPayloadQueueDropsOldestAtCapacity(t *testing.T) {
	health := &RuntimeHealth{}
	queue := newEventPayloadQueue(2, health)
	for _, id := range []string{"oldest", "middle", "newest"} {
		payload := []byte(fmt.Sprintf(`{"type":"cenc_eew","EventID":%q,"Latitude":30,"Longitude":104,"Magnitude":5,"Depth":10}`, id))
		if !queue.Enqueue(payload, time.Now()) {
			t.Fatalf("failed to enqueue %s", id)
		}
	}
	snapshot := health.Snapshot()
	if snapshot.QueueDepth != 2 || snapshot.QueueDropped != 1 {
		t.Fatalf("unexpected bounded queue health: %#v", snapshot)
	}
	for _, want := range []string{"middle", "newest"} {
		item, ok := queue.Dequeue(context.Background())
		if !ok {
			t.Fatalf("missing %s", want)
		}
		event, _, err := parseEvent(item.data)
		if err != nil || event.EventID != want {
			t.Fatalf("got event=%#v err=%v want=%s", event, err, want)
		}
	}
}

func TestHTTPHealthRejectsStaleWebSocketData(t *testing.T) {
	store, err := NewStore(filepath.Join(t.TempDir(), "subscriptions.json"))
	if err != nil {
		t.Fatal(err)
	}
	health := &RuntimeHealth{}
	now := time.Now()
	health.SetWebSocketConnected(now.Add(-5 * time.Minute))
	health.SetWebSocketMessage(now.Add(-4 * time.Minute))
	cfg := Config{Wolfx: WolfxConfig{HealthStaleSeconds: 60}}
	handler := newHTTPHandler(cfg, store, NewAlertCache(time.Hour, 10), NewNotifier(BarkConfig{}), health)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), "data source stale") {
		t.Fatalf("stale health status=%d body=%s", rec.Code, rec.Body.String())
	}

	health.SetWebSocketMessage(time.Now())
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("fresh health status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestFormatBeijingTime(t *testing.T) {
	utc := time.Date(2026, 7, 7, 2, 3, 4, 0, time.UTC)
	if got := formatBeijing(utc, "2006-01-02 15:04:05"); got != "2026-07-07 10:03:04" {
		t.Fatalf("unexpected Beijing time: %s", got)
	}
	if got := alertOriginTimeLabel(Event{OriginTime: utc}); got != "2026-07-07 10:03:04" {
		t.Fatalf("unexpected alert origin time: %s", got)
	}
}

func TestGeocodeAddressUsesNominatim(t *testing.T) {
	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		if r.URL.Query().Get("q") != "成都市天府广场" || r.URL.Query().Get("countrycodes") != "cn" {
			t.Fatalf("unexpected Nominatim request: %s", r.URL.String())
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{
			"name":"天府广场",
			"display_name":"天府广场, 青羊区, 成都市, 四川省, 中国",
			"lat":"30.6570",
			"lon":"104.0658"
		}]`))
	}))
	defer server.Close()

	store := &Store{backend: &testAdministrativeBoundaryBackend{
		result: GeocodeResult{
			Name:                "四川省 成都市 青羊区",
			Address:             "四川省 成都市 青羊区",
			AdministrativeID:    "510105",
			AdministrativeLevel: 3,
		},
		found: true,
	}}
	results, err := geocodeAddress(context.Background(), Config{Server: ServerConfig{
		GeocodeURL: server.URL,
	}}, store, "成都市天府广场")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Fatalf("expected one result, got %#v", results)
	}
	if results[0].Name != "四川省 成都市 青羊区" || results[0].AdministrativeID != "510105" || results[0].Latitude != 30.6570 || results[0].Longitude != 104.0658 {
		t.Fatalf("unexpected result labels: %#v", results[0])
	}
	if _, err := geocodeAddress(context.Background(), Config{Server: ServerConfig{GeocodeURL: server.URL}}, store, "成都市天府广场"); err != nil {
		t.Fatal(err)
	}
	if requestCount != 1 {
		t.Fatalf("expected cached Nominatim result, requests=%d", requestCount)
	}
}

type testAdministrativeBoundaryBackend struct {
	result GeocodeResult
	found  bool
	err    error
}

func (b *testAdministrativeBoundaryBackend) Upsert(Subscription) error { return nil }
func (b *testAdministrativeBoundaryBackend) Delete(string) error       { return nil }
func (b *testAdministrativeBoundaryBackend) CandidateIDs(float64, float64, float64) ([]string, error) {
	return nil, nil
}
func (b *testAdministrativeBoundaryBackend) Close() error { return nil }
func (b *testAdministrativeBoundaryBackend) LookupAdministrativeLocation(_ context.Context, latitude, longitude float64) (GeocodeResult, bool, error) {
	result := b.result
	result.Latitude = latitude
	result.Longitude = longitude
	return result, b.found, b.err
}

func TestReverseGeocodeUsesLocalAdministrativeBoundaryFirst(t *testing.T) {
	store := &Store{backend: &testAdministrativeBoundaryBackend{
		result: GeocodeResult{
			Name:                "四川省 成都市 武侯区",
			Address:             "四川省 成都市 武侯区",
			Latitude:            30.5728,
			Longitude:           104.0668,
			AdministrativeID:    "510107",
			AdministrativeLevel: 3,
		},
		found: true,
	}}
	result, err := reverseGeocodeAddress(context.Background(), store, 30.5728, 104.0668)
	if err != nil {
		t.Fatal(err)
	}
	if result.Name != "四川省 成都市 武侯区" {
		t.Fatalf("unexpected local reverse geocode result: %#v", result)
	}
}

func TestReverseGeocodeFallsBackToCoordinatesWithoutBoundary(t *testing.T) {
	result, err := reverseGeocodeAddress(context.Background(), nil, 30.5728, 104.0668)
	if err != nil {
		t.Fatal(err)
	}
	if result.Name != "30.5728, 104.0668" || result.Address != result.Name {
		t.Fatalf("unexpected coordinate fallback: %#v", result)
	}
}

func TestJoinAdministrativeNamesRemovesAdjacentDuplicates(t *testing.T) {
	if got := joinAdministrativeNames("北京市", "北京市", "朝阳区"); got != "北京市 朝阳区" {
		t.Fatalf("unexpected administrative name: %s", got)
	}
	if got := joinAdministrativeNames("台湾省", "", ""); got != "台湾省" {
		t.Fatalf("unexpected province-only administrative name: %s", got)
	}
}

func TestCanonicalizeSubscriptionRejectsSameLowestAdministrativeLevel(t *testing.T) {
	store := &Store{backend: &testAdministrativeBoundaryBackend{
		result: GeocodeResult{
			Name:                "四川省 成都市 双流区",
			Address:             "四川省 成都市 双流区",
			AdministrativeID:    "510116",
			AdministrativeLevel: 3,
		},
		found: true,
	}}
	sub := Subscription{Locations: []SubscriptionLocation{
		{Latitude: 30.50, Longitude: 103.90},
		{Latitude: 30.55, Longitude: 103.95},
	}}
	if err := canonicalizeSubscriptionLocations(context.Background(), store, &sub, true); !errors.Is(err, ErrDuplicateAdministrativeLocation) {
		t.Fatalf("expected same district to be rejected, got %v", err)
	}
}

func TestCanonicalizeSubscriptionUsesProvinceAsTaiwanLowestLevel(t *testing.T) {
	store := &Store{backend: &testAdministrativeBoundaryBackend{
		result: GeocodeResult{
			Name:                "台湾省",
			Address:             "台湾省",
			AdministrativeID:    "71",
			AdministrativeLevel: 1,
		},
		found: true,
	}}
	sub := Subscription{Locations: []SubscriptionLocation{
		{Latitude: 25.03, Longitude: 121.56},
		{Latitude: 22.63, Longitude: 120.30},
	}}
	if err := canonicalizeSubscriptionLocations(context.Background(), store, &sub, true); !errors.Is(err, ErrDuplicateAdministrativeLocation) {
		t.Fatalf("expected duplicate Taiwan province to be rejected, got %v", err)
	}
}

func TestBuiltinHistoryAndFilters(t *testing.T) {
	records := builtinHistoryRecords()
	if len(records) < 3 {
		t.Fatalf("expected builtin major history records, got %d", len(records))
	}
	if records[0].Key != "yibin-gaoxian-2026" || records[0].Magnitude != 5.5 || records[0].DepthKM != 6 {
		t.Fatalf("unexpected Yibin history record: %#v", records[0])
	}

	filtered := filterHistoryRecords(records, url.Values{
		"source":        []string{"major"},
		"min_magnitude": []string{"7"},
	})
	if len(filtered) != 2 || filtered[0].Key != "wenchuan-2008" {
		t.Fatalf("unexpected filtered records: %#v", filtered)
	}

	defaultFiltered := filterHistoryRecords(records, url.Values{})
	if len(defaultFiltered) != 0 {
		t.Fatalf("expected default history filter to hide major records, got %#v", defaultFiltered)
	}

	page := filterHistoryRecords(records, url.Values{
		"source": []string{"major"},
		"limit":  []string{"1"},
		"offset": []string{"1"},
	})
	if len(page) != 1 || page[0].Key != "wenchuan-2008" {
		t.Fatalf("unexpected paged records: %#v", page)
	}
}

func TestParseOfficialReports(t *testing.T) {
	html := `<table id="earthquake_subao_guid_catalog_data"><tr id="earthquake_subao_guid_catalog_tr_0"><td><div class='cls-data-content-list'>1</div></td><td style='display:none'></td><td><div class='cls-data-content-list'>2026-7-09 12:25:56</div></td><td><div class='cls-data-content-list'>104.69</div></td><td><div class='cls-data-content-list'>28.52</div></td><td><div class='cls-data-content-list'>5</div></td><td><div class='cls-data-content-list'>3.2</div></td><td><div class='cls-data-content-list'>四川宜宾市高县</div></td><td><div class='cls-data-content-list'>天然地震</div></td></tr></table>`
	reports := parseOfficialReports(html, 5)
	if len(reports) != 1 {
		t.Fatalf("expected one official report, got %#v", reports)
	}
	report := reports[0]
	if report.OriginTime != "2026-7-09 12:25:56" || report.Longitude != 104.69 || report.Latitude != 28.52 || report.DepthKM != 5 || report.Magnitude != 3.2 || report.Location != "四川宜宾市高县" || report.EventType != "天然地震" {
		t.Fatalf("unexpected official report: %#v", report)
	}
}

func TestMergeHistoryRecordsDedupes(t *testing.T) {
	a := HistoryRecord{Source: "cenc", Key: "No1", EventID: "a", Magnitude: 4}
	b := HistoryRecord{Source: "cenc", Key: "No1", EventID: "b", Magnitude: 5}
	c := HistoryRecord{Source: "major", Key: "wenchuan-2008", EventID: "c", Magnitude: 7.9}
	merged := mergeHistoryRecords([]HistoryRecord{a, c}, []HistoryRecord{b})
	if len(merged) != 2 {
		t.Fatalf("expected deduped records, got %#v", merged)
	}
	if merged[0].EventID != "a" {
		t.Fatalf("expected first record to win, got %#v", merged[0])
	}
}

func TestSubscriptionPauseState(t *testing.T) {
	cfg := Config{Server: ServerConfig{
		SubscriptionPaused:        true,
		SubscriptionPausedMessage: "manual pause",
		SubscriptionLimit:         100,
		SubscriptionLimitMessage:  "limit pause",
	}}
	paused, message, reason := subscriptionPauseState(cfg, 10)
	if !paused || message != "manual pause" || reason != "manual" {
		t.Fatalf("expected manual pause, got paused=%v message=%q reason=%q", paused, message, reason)
	}

	cfg.Server.SubscriptionPaused = false
	paused, message, reason = subscriptionPauseState(cfg, 100)
	if !paused || message != "limit pause" || reason != "limit" {
		t.Fatalf("expected limit pause, got paused=%v message=%q reason=%q", paused, message, reason)
	}

	paused, message, reason = subscriptionPauseState(cfg, 99)
	if paused || message != "" || reason != "" {
		t.Fatalf("expected active subscription state, got paused=%v message=%q reason=%q", paused, message, reason)
	}

	cfg.Server.SubscriptionLimit = 0
	paused, message, reason = subscriptionPauseState(cfg, 100000)
	if paused || message != "" || reason != "" {
		t.Fatalf("expected disabled limit, got paused=%v message=%q reason=%q", paused, message, reason)
	}
}

func TestLoadConfigRejectsUnknownFields(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	data := []byte(`server:
  port: 30010
  subscription_limt: 100
`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadConfig(path); err == nil {
		t.Fatal("expected unknown YAML field to be rejected")
	} else if !strings.Contains(err.Error(), "subscription_limt") {
		t.Fatalf("expected error to identify unknown field, got %v", err)
	}
}

func TestLoadConfigRejectsMultipleDocuments(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	data := []byte("server:\n  port: 30010\n---\nserver:\n  port: 30011\n")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadConfig(path); err == nil {
		t.Fatal("expected multiple YAML documents to be rejected")
	}
}

func TestLoadConfigRequiresConfiguredBarkServer(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("server:\n  port: 30010\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadConfig(path); err == nil || !strings.Contains(err.Error(), "bark.server") {
		t.Fatalf("expected missing Bark server error, got %v", err)
	}
}

func TestLoadConfigUsesConfiguredBarkServerWithoutProductionFallback(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	data := []byte("bark:\n  self_hosted_server: https://bark.example.test/\nserver:\n  port: 30010\n")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Bark.Server != "https://bark.example.test" || cfg.Bark.SelfHostedServer != "https://bark.example.test" {
		t.Fatalf("unexpected Bark defaults: %#v", cfg.Bark)
	}
}

func TestLoadConfigKeepsOfficialOnlyDeploymentWithoutSelfHostedFallback(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	data := []byte("bark:\n  server: https://api.day.app/\nserver:\n  port: 30010\n")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Bark.Server != "https://api.day.app" || cfg.Bark.SelfHostedServer != "" {
		t.Fatalf("official deployment unexpectedly enabled self-hosted validation: %#v", cfg.Bark)
	}
	if isSelfHostedBarkServer(cfg.Bark.Server, cfg) {
		t.Fatal("official api.day.app must not be classified as self-hosted")
	}
}
