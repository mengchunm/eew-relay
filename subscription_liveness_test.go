package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestSubscriptionLivenessStorePreservesConfigurationChangesAndInvalidatesServerChanges(t *testing.T) {
	cfg := Config{
		Server: ServerConfig{DataPath: filepath.Join(t.TempDir(), "subscriptions.json")},
		Bark: BarkConfig{
			Server:           "https://api.day.app",
			SelfHostedServer: "https://bark.example.test",
		},
	}
	store, err := newSubscriptionLivenessStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	first := Subscription{BarkID: "first-key", BarkServer: "https://bark.example.test/", UpdatedAt: 100}
	second := Subscription{BarkID: "second-key", BarkServer: "https://api.day.app", UpdatedAt: 200}
	if got := store.Snapshot(first).Status; got != subscriptionLivenessUntested {
		t.Fatalf("new subscription status=%q", got)
	}
	checkedAt := time.Date(2026, 7, 29, 13, 0, 0, 0, time.UTC)
	if err := store.Update(map[string]SubscriptionLivenessRecord{
		first.BarkID: {Status: subscriptionLivenessDevicePresent, Message: "present"},
	}, []Subscription{first, second}, checkedAt); err != nil {
		t.Fatal(err)
	}

	reopened, err := newSubscriptionLivenessStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	record := reopened.Snapshot(first)
	if record.Status != subscriptionLivenessDevicePresent || record.CheckedAt != checkedAt.Format(time.RFC3339) || record.Message != "present" || record.BarkServer != "https://bark.example.test" {
		t.Fatalf("reopened record=%#v", record)
	}
	if runtime.GOOS != "windows" {
		info, err := os.Stat(filepath.Join(filepath.Dir(cfg.Server.DataPath), "subscription-liveness.json"))
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0o600 {
			t.Fatalf("liveness label permissions=%o", info.Mode().Perm())
		}
	}

	changed := first
	changed.UpdatedAt++
	changed.LocationName = "updated location"
	changed.NotifyRules = NotificationRules{PassiveMax: 1, ActiveMax: 3, CriticalMin: 4}
	if got := reopened.Snapshot(changed).Status; got != subscriptionLivenessDevicePresent {
		t.Fatalf("ordinary subscription update cleared liveness status=%q", got)
	}
	if err := reopened.Update(map[string]SubscriptionLivenessRecord{
		second.BarkID: {Status: subscriptionLivenessOfficialUnverified, Message: "unverified"},
	}, []Subscription{changed, second}, checkedAt.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if got := reopened.Snapshot(changed).Status; got != subscriptionLivenessDevicePresent {
		t.Fatalf("partial update did not preserve unchanged endpoint status=%q", got)
	}
	if got := reopened.Snapshot(second).Status; got != subscriptionLivenessOfficialUnverified {
		t.Fatalf("partial update status=%q", got)
	}
	newKey := changed
	newKey.BarkID = "new-key"
	if got := reopened.Snapshot(newKey).Status; got != subscriptionLivenessUntested {
		t.Fatalf("new Bark key inherited another key's status=%q", got)
	}

	serverChanged := changed
	serverChanged.BarkServer = "https://api.day.app"
	serverChanged.UpdatedAt++
	if got := reopened.Snapshot(serverChanged).Status; got != subscriptionLivenessUntested {
		t.Fatalf("changed Bark server retained stale status=%q", got)
	}
	if err := reopened.Update(map[string]SubscriptionLivenessRecord{
		second.BarkID: {Status: subscriptionLivenessOfficialUnverified, Message: "unverified"},
	}, []Subscription{serverChanged, second}, checkedAt.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, ok := reopened.records[first.BarkID]; ok {
		t.Fatal("label for a changed Bark server was not pruned")
	}
}

func TestSubscriptionLivenessStoreMigratesOnlyTrustworthyLegacyLabels(t *testing.T) {
	cfg := Config{
		Server: ServerConfig{DataPath: filepath.Join(t.TempDir(), "subscriptions.json")},
		Bark: BarkConfig{
			Server:           "https://api.day.app",
			SelfHostedServer: "https://bark.example.test",
		},
	}
	checkedAt := time.Date(2026, 7, 29, 13, 0, 0, 0, time.UTC)
	legacy := subscriptionLivenessFile{
		Version:   subscriptionLivenessLegacyFileVersion,
		UpdatedAt: checkedAt.Format(time.RFC3339),
		Records: map[string]SubscriptionLivenessRecord{
			"matched-key": {
				Status:                subscriptionLivenessDevicePresent,
				CheckedAt:             checkedAt.Format(time.RFC3339),
				Message:               "present",
				SubscriptionUpdatedAt: 100,
			},
			"stale-key": {
				Status:                subscriptionLivenessDeviceMissing,
				CheckedAt:             checkedAt.Format(time.RFC3339),
				Message:               "missing",
				SubscriptionUpdatedAt: 200,
			},
		},
	}
	data, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(filepath.Dir(cfg.Server.DataPath), "subscription-liveness.json")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}

	store, err := newSubscriptionLivenessStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	matched := Subscription{BarkID: "matched-key", BarkServer: "https://bark.example.test/", UpdatedAt: 100}
	stale := Subscription{BarkID: "stale-key", BarkServer: "https://bark.example.test", UpdatedAt: 201}
	migrated, err := store.MigrateLegacy([]Subscription{matched, stale})
	if err != nil {
		t.Fatal(err)
	}
	if migrated != 1 {
		t.Fatalf("migrated=%d want 1", migrated)
	}
	if store.version != subscriptionLivenessFileVersion {
		t.Fatalf("version=%d", store.version)
	}

	matched.UpdatedAt++
	matched.LocationName = "updated location"
	if got := store.Snapshot(matched).Status; got != subscriptionLivenessDevicePresent {
		t.Fatalf("migrated label did not survive ordinary update: %q", got)
	}
	if got := store.Snapshot(stale).Status; got != subscriptionLivenessUntested {
		t.Fatalf("stale legacy label was trusted: %q", got)
	}

	reopened, err := newSubscriptionLivenessStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if got := reopened.Snapshot(matched).Status; got != subscriptionLivenessDevicePresent {
		t.Fatalf("persisted migrated label status=%q", got)
	}
}
