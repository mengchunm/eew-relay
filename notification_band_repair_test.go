package main

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"
)

func TestRepairInvalidNotificationBandsAvoidsConflicts(t *testing.T) {
	tests := []struct {
		name        string
		input       []NotificationBand
		want        []NotificationBand
		wantReset   bool
		wantChanged bool
	}{
		{
			name: "passive uses default gap",
			input: []NotificationBand{
				{Min: 1, Max: 1, Level: "passive"},
				{Min: 2, Max: 3, Level: "active"},
				{Min: 4, Max: notificationOpenEndedMax, Level: "critical"},
			},
			want: []NotificationBand{
				{Min: 1, Max: 2, Level: "passive", Label: "不弹窗通知"},
				{Min: 2, Max: 3, Level: "active", Label: "勿扰静音不响铃"},
				{Min: 4, Max: notificationOpenEndedMax, Level: "critical", Label: "强行响铃（铃声不可换）"},
			},
			wantChanged: true,
		},
		{
			name: "active moves beside preserved neighbors",
			input: []NotificationBand{
				{Min: 1, Max: 3, Level: "passive"},
				{Min: 4, Max: 4, Level: "active"},
				{Min: 5, Max: notificationOpenEndedMax, Level: "critical"},
			},
			want: []NotificationBand{
				{Min: 1, Max: 3, Level: "passive", Label: "不弹窗通知"},
				{Min: 3, Max: 4, Level: "active", Label: "勿扰静音不响铃"},
				{Min: 5, Max: notificationOpenEndedMax, Level: "critical", Label: "强行响铃（铃声不可换）"},
			},
			wantChanged: true,
		},
		{
			name: "consecutive invalid bands share available gap",
			input: []NotificationBand{
				{Min: 1, Max: 1, Level: "passive"},
				{Min: 1, Max: 1, Level: "active"},
				{Min: 2, Max: notificationOpenEndedMax, Level: "critical"},
			},
			want: []NotificationBand{
				{Min: 0, Max: 1, Level: "passive", Label: "不弹窗通知"},
				{Min: 1, Max: 2, Level: "active", Label: "勿扰静音不响铃"},
				{Min: 2, Max: notificationOpenEndedMax, Level: "critical", Label: "强行响铃（铃声不可换）"},
			},
			wantChanged: true,
		},
		{
			name: "invalid band does not overlap earlier custom band",
			input: []NotificationBand{
				{Min: 1, Max: 5, Level: "passive"},
				{Min: 5, Max: 5, Level: "active"},
				{Min: 6, Max: notificationOpenEndedMax, Level: "critical"},
			},
			want: []NotificationBand{
				{Min: 1, Max: 5, Level: "passive", Label: "不弹窗通知"},
				{Min: 5, Max: 6, Level: "active", Label: "勿扰静音不响铃"},
				{Min: 6, Max: notificationOpenEndedMax, Level: "critical", Label: "强行响铃（铃声不可换）"},
			},
			wantChanged: true,
		},
		{
			name: "missing levels remain missing",
			input: []NotificationBand{
				{Min: 1, Max: 1, Level: "passive"},
				{Min: 3, Max: notificationOpenEndedMax, Level: "critical"},
			},
			want: []NotificationBand{
				{Min: 1, Max: 2, Level: "passive", Label: "不弹窗通知"},
				{Min: 3, Max: notificationOpenEndedMax, Level: "critical", Label: "强行响铃（铃声不可换）"},
			},
			wantChanged: true,
		},
		{
			name: "unsafe repair resets the whole group",
			input: []NotificationBand{
				{Min: 0, Max: 7, Level: "passive"},
				{Min: 7, Max: 7, Level: "active"},
			},
			want:        defaultNotificationBandsForRepair(),
			wantReset:   true,
			wantChanged: true,
		},
		{
			name:        "valid custom bands are untouched",
			input:       []NotificationBand{{Min: 0, Max: 2, Level: "active"}, {Min: 4, Max: notificationOpenEndedMax, Level: "critical"}},
			want:        []NotificationBand{{Min: 0, Max: 2, Level: "active", Label: "勿扰静音不响铃"}, {Min: 4, Max: notificationOpenEndedMax, Level: "critical", Label: "强行响铃（铃声不可换）"}},
			wantChanged: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, changed, reset := repairInvalidNotificationBands(test.input)
			if changed != test.wantChanged || reset != test.wantReset {
				t.Fatalf("changed=%t reset=%t", changed, reset)
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("bands=%#v want=%#v", got, test.want)
			}
			if err := validateNotificationBands(got); err != nil {
				t.Fatalf("repaired bands conflict: %v", err)
			}
		})
	}
}

func TestRepairStoredNotificationBandsDryRunAndApply(t *testing.T) {
	path := filepath.Join(t.TempDir(), "subscriptions.json")
	valid := Subscription{
		BarkID:      "valid-key",
		BarkServer:  "https://api.day.app",
		Latitude:    30,
		Longitude:   104,
		Locations:   []SubscriptionLocation{{Name: "成都", Latitude: 30, Longitude: 104}},
		NotifyBands: defaultNotificationBands(),
		UpdatedAt:   10,
	}
	broken := valid
	broken.BarkID = "broken-key"
	broken.NotifyBands = []NotificationBand{
		{Min: 1, Max: 1, Level: "passive", Label: "低烈度"},
		{Min: 2, Max: 3, Level: "active", Label: "中等烈度"},
		{Min: 4, Max: notificationOpenEndedMax, Level: "critical", Label: "高烈度"},
	}
	store := &Store{path: path, subscriptions: map[string]Subscription{valid.BarkID: valid, broken.BarkID: broken}}

	dryRun, err := repairStoredNotificationBands(context.Background(), store, false)
	if err != nil {
		t.Fatal(err)
	}
	if dryRun.Scanned != 2 || dryRun.Repaired != 1 || dryRun.IndividuallyRepaired != 1 || dryRun.ResetToDefaults != 0 {
		t.Fatalf("dry-run summary=%#v", dryRun)
	}
	if got, _ := store.Get(broken.BarkID); got.NotifyBands[0].Min != got.NotifyBands[0].Max {
		t.Fatalf("dry run mutated store: %#v", got.NotifyBands)
	}

	applied, err := repairStoredNotificationBands(context.Background(), store, true)
	if err != nil {
		t.Fatal(err)
	}
	if applied != dryRun {
		t.Fatalf("apply summary=%#v want=%#v", applied, dryRun)
	}
	reopened, err := NewStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if reopened.Count() != 2 {
		t.Fatalf("subscription count=%d", reopened.Count())
	}
	for _, sub := range reopened.List() {
		if err := validateSubscription(sub); err != nil {
			t.Fatalf("subscription %s remains invalid: %v", sub.BarkID, err)
		}
	}
}
