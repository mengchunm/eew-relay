package main

import (
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestIntensityModelSettingsPersistAndTakeEffectImmediately(t *testing.T) {
	cfg := Config{
		Server: ServerConfig{DataPath: filepath.Join(t.TempDir(), "subscriptions.json")},
		Alert: AlertConfig{
			IntensityModelMode:          intensityModelModeShadow,
			IntensityModelMaxCorrection: defaultModelCorrection,
		},
	}
	store, err := newIntensityModelSettingsStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	cfg.intensityModelSettings = store
	event := Event{Type: "cenc_eew", Magnitude: 5.8, DepthKM: 12, Latitude: 30.7, Longitude: 103.9}
	shadow := predictIntensity(cfg, event, 80, math.Sqrt(80*80+12*12), SiteCondition{})
	if shadow.Mode != intensityModelModeShadow || shadow.Selected != shadow.Legacy {
		t.Fatalf("unexpected initial prediction: %#v", shadow)
	}
	updatedAt := time.Date(2026, 8, 5, 2, 3, 4, 0, time.UTC)
	updated, err := store.Update(intensityModelModeActive, updatedAt)
	if err != nil {
		t.Fatal(err)
	}
	if updated.Mode != intensityModelModeActive || updated.UpdatedAt == "" {
		t.Fatalf("unexpected updated settings: %#v", updated)
	}
	active := predictIntensity(cfg, event, 80, math.Sqrt(80*80+12*12), SiteCondition{})
	if active.Mode != intensityModelModeActive || active.Selected != active.Candidate {
		t.Fatalf("runtime update did not take effect: %#v", active)
	}
	reopened, err := newIntensityModelSettingsStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if got := reopened.Snapshot(); got.Mode != intensityModelModeActive || got.MaxCorrection != defaultModelCorrection || got.UpdatedAt == "" {
		t.Fatalf("settings were not persisted: %#v", got)
	}
}

func TestIntensityModelSettingsRejectInvalidPersistedData(t *testing.T) {
	root := t.TempDir()
	cfg := Config{Server: ServerConfig{DataPath: filepath.Join(root, "subscriptions.json")}}
	if err := os.WriteFile(filepath.Join(root, "intensity-model.json"), []byte(`{"mode":"unsafe","max_correction":0.8}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := newIntensityModelSettingsStore(cfg); err == nil {
		t.Fatal("expected invalid persisted mode to be rejected")
	}
}

func TestIntensityModelEnvironmentOverrideWinsOverPersistedMode(t *testing.T) {
	cfg := Config{
		Alert:                      AlertConfig{IntensityModelMode: intensityModelModeActive},
		intensityModelModeOverride: intensityModelModeLegacy,
	}
	settings := intensityModelSettings(cfg)
	if settings.Mode != intensityModelModeLegacy || !settings.Forced {
		t.Fatalf("environment override was not applied: %#v", settings)
	}
}

func TestIntensityModelSettingsAcceptAllPublishedModes(t *testing.T) {
	for _, mode := range []string{
		intensityModelModeLegacy,
		intensityModelModeShadow,
		intensityModelModeActive,
		intensityModelModeGBT2020,
		intensityModelModeHybrid,
	} {
		if got, err := validateIntensityModelMode(mode); err != nil || got != mode {
			t.Fatalf("mode %q was rejected: got=%q err=%v", mode, got, err)
		}
	}
}

func TestIntensityModelSettingsReportModeSpecificVersion(t *testing.T) {
	cfg := Config{Alert: AlertConfig{IntensityModelMode: intensityModelModeGBT2020}}
	settings := intensityModelSettings(cfg)
	if settings.ModelVersion != gbtIntensityModelVersion || settings.StandardName != gbtIntensityStandardName {
		t.Fatalf("unexpected GBT metadata: %#v", settings)
	}
	if settings.ModelVersions[intensityModelModeActive] != officialIntensityModelVersion ||
		settings.ModelVersions[intensityModelModeHybrid] != gbtIntensityModelVersion {
		t.Fatalf("missing available model versions: %#v", settings.ModelVersions)
	}
}
