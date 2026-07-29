package main

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestSubscriptionLivenessStorePersistsAndInvalidatesChangedSubscriptions(t *testing.T) {
	cfg := Config{Server: ServerConfig{DataPath: filepath.Join(t.TempDir(), "subscriptions.json")}}
	store, err := newSubscriptionLivenessStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	first := Subscription{BarkID: "first-key", UpdatedAt: 100}
	second := Subscription{BarkID: "second-key", UpdatedAt: 200}
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
	if record.Status != subscriptionLivenessDevicePresent || record.CheckedAt != checkedAt.Format(time.RFC3339) || record.Message != "present" {
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
	if got := reopened.Snapshot(changed).Status; got != subscriptionLivenessUntested {
		t.Fatalf("changed subscription retained stale status=%q", got)
	}
	if err := reopened.Update(map[string]SubscriptionLivenessRecord{
		second.BarkID: {Status: subscriptionLivenessOfficialUnverified, Message: "unverified"},
	}, []Subscription{changed, second}, checkedAt.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if got := reopened.Snapshot(first).Status; got != subscriptionLivenessUntested {
		t.Fatalf("stale persisted label was not pruned: %q", got)
	}
	if got := reopened.Snapshot(second).Status; got != subscriptionLivenessOfficialUnverified {
		t.Fatalf("partial update status=%q", got)
	}
}
