package main

import (
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestIntensityModelShadowDoesNotChangeDeliveryDecision(t *testing.T) {
	event := Event{Type: "cenc_eew", Magnitude: 5.8, DepthKM: 12, Latitude: 30.7, Longitude: 103.9}
	prediction := predictIntensity(Config{Alert: AlertConfig{IntensityModelMode: "shadow"}}, event, 80, math.Sqrt(80*80+12*12), SiteCondition{})
	if !prediction.UsedModel || prediction.FallbackReason != "" {
		t.Fatalf("expected in-domain model prediction, got %#v", prediction)
	}
	if prediction.Selected != prediction.Legacy {
		t.Fatalf("shadow mode changed selected intensity: %#v", prediction)
	}
	if prediction.Candidate == 0 || prediction.Version == "" {
		t.Fatalf("shadow result did not retain candidate/version: %#v", prediction)
	}
}

func TestIntensityModelActiveUsesBoundedCandidate(t *testing.T) {
	event := Event{Type: "sc_eew", Magnitude: 6.2, DepthKM: 10, Latitude: 31.0, Longitude: 103.4}
	prediction := predictIntensity(Config{Alert: AlertConfig{IntensityModelMode: "active", IntensityModelMaxCorrection: 0.8}}, event, 120, math.Sqrt(120*120+100), SiteCondition{})
	if !prediction.UsedModel || prediction.Selected != prediction.Candidate {
		t.Fatalf("active mode did not select model candidate: %#v", prediction)
	}
	if math.Abs(prediction.Candidate-prediction.Baseline) > 0.800001 {
		t.Fatalf("candidate escaped physical bound: %#v", prediction)
	}
}

func TestIntensityModelFallsBackOutsideOfficialDatasetDomain(t *testing.T) {
	event := Event{Type: "fj_eew", Magnitude: 3.6, DepthKM: 10, Latitude: 26.0, Longitude: 119.0}
	prediction := predictIntensity(Config{Alert: AlertConfig{IntensityModelMode: "active"}}, event, 30, math.Sqrt(30*30+100), SiteCondition{})
	if prediction.UsedModel || prediction.FallbackReason != "magnitude_out_of_domain" {
		t.Fatalf("expected magnitude fallback, got %#v", prediction)
	}
	if prediction.Selected != prediction.Baseline {
		t.Fatalf("active fallback must use stable baseline: %#v", prediction)
	}
}

func TestIntensityModelFallsBackWhenDepthIsMissing(t *testing.T) {
	event := Event{Type: "fj_eew", Magnitude: 5.1, Latitude: 26.0, Longitude: 119.0}
	prediction := predictIntensity(Config{Alert: AlertConfig{IntensityModelMode: "active"}}, event, 30, 30, SiteCondition{})
	if prediction.UsedModel || prediction.FallbackReason != "depth_missing" || prediction.Selected != prediction.Baseline {
		t.Fatalf("expected stable fallback for missing depth, got %#v", prediction)
	}
}

func TestIntensityModelAppliesBoundedSiteCorrectionOnlyToCandidate(t *testing.T) {
	event := Event{Type: "cenc_eew", Magnitude: 6.0, DepthKM: 12, Latitude: 30.7, Longitude: 103.9}
	cfg := Config{Alert: AlertConfig{IntensityModelMode: intensityModelModeActive, IntensityModelMaxCorrection: defaultModelCorrection}}
	soft := SiteCondition{VS30: 180, Uncertainty: 0.1, Version: geoSCKSiteDataVersion}
	prediction := predictIntensity(cfg, event, 80, math.Sqrt(80*80+12*12), soft)
	if prediction.SiteCorrection <= 0 || prediction.SiteCorrection > maxSiteIntensityIncrease {
		t.Fatalf("unexpected soft-site correction: %#v", prediction)
	}
	if prediction.Selected != prediction.Candidate || math.Abs(prediction.Candidate-prediction.Baseline) > defaultModelCorrection+1e-9 {
		t.Fatalf("site correction escaped overall safety bound: %#v", prediction)
	}

	cfg.Alert.IntensityModelMode = intensityModelModeShadow
	shadow := predictIntensity(cfg, event, 80, math.Sqrt(80*80+12*12), soft)
	if shadow.SiteCorrection <= 0 || shadow.Selected != shadow.Legacy {
		t.Fatalf("shadow mode must audit site correction without changing delivery: %#v", shadow)
	}
}

func TestSiteCorrectionRejectsLowConfidenceAndHardRockIsNegative(t *testing.T) {
	if correction, reason := siteIntensityCorrection(SiteCondition{VS30: 180, Uncertainty: 0.4, Version: geoSCKSiteDataVersion}, 4); correction != 0 || reason != "site_data_low_confidence" {
		t.Fatalf("low-confidence site data must not change intensity: correction=%v reason=%q", correction, reason)
	}
	correction, reason := siteIntensityCorrection(SiteCondition{VS30: 1200, Uncertainty: 0.1, Version: geoSCKSiteDataVersion}, 4)
	if correction >= 0 || correction < -maxSiteIntensityDecrease || reason != "" {
		t.Fatalf("unexpected hard-rock correction: correction=%v reason=%q", correction, reason)
	}
}

func TestIntensityModelMonotonicSafetyConstraints(t *testing.T) {
	cfg := Config{Alert: AlertConfig{IntensityModelMode: "active"}}
	event := Event{Type: "cenc_eew", Magnitude: 4, DepthKM: 10, Latitude: 30.7, Longitude: 103.9}
	previous := -1.0
	for magnitude := 4.0; magnitude <= 8.0001; magnitude += 0.1 {
		event.Magnitude = magnitude
		current := predictIntensity(cfg, event, 80, math.Sqrt(80*80+100), SiteCondition{}).Selected
		if current+1e-9 < previous {
			t.Fatalf("intensity decreased as magnitude increased: M%.1f=%.1f previous=%.1f", magnitude, current, previous)
		}
		previous = current
	}

	event.Magnitude = 6.0
	previous = math.MaxFloat64
	for distance := 0.0; distance <= 700; distance += 5 {
		current := predictIntensity(cfg, event, distance, math.Sqrt(distance*distance+100), SiteCondition{}).Selected
		if current > previous+1e-9 {
			t.Fatalf("intensity increased as distance increased: D%.0f=%.1f previous=%.1f", distance, current, previous)
		}
		previous = current
	}
}

func TestGeneratedIntensityModelMetadata(t *testing.T) {
	if officialIntensityModelRecordCount < 30000 || officialIntensityModelEventCount < 400 {
		t.Fatalf("unexpected generated model coverage records=%d events=%d", officialIntensityModelRecordCount, officialIntensityModelEventCount)
	}
	if len(officialIntensityModelRoots) != 63 || len(officialIntensityModelNodes) < 1000 {
		t.Fatalf("unexpected generated model size roots=%d nodes=%d", len(officialIntensityModelRoots), len(officialIntensityModelNodes))
	}
}

func TestLoadConfigDefaultsIntensityModelToShadow(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("bark:\n  server: https://api.day.app\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Alert.IntensityModelMode != intensityModelModeShadow || cfg.Alert.IntensityModelMaxCorrection != defaultModelCorrection {
		t.Fatalf("unexpected safe defaults: %#v", cfg.Alert)
	}
}

func TestLoadConfigRejectsUnsafeIntensityModelSettings(t *testing.T) {
	for name, alert := range map[string]string{
		"mode":       "intensity_model_mode: unknown",
		"correction": "intensity_model_mode: active\n  intensity_model_max_correction: 2.1",
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yaml")
			data := "bark:\n  server: https://api.day.app\nalert:\n  " + alert + "\n"
			if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := loadConfig(path); err == nil || !strings.Contains(err.Error(), "intensity_model") {
				t.Fatalf("expected intensity model validation error, got %v", err)
			}
		})
	}
}

func BenchmarkPredictIntensity700Locations(b *testing.B) {
	cfg := Config{Alert: AlertConfig{IntensityModelMode: "active"}}
	cfg.intensityModelSettings = defaultIntensityModelSettingsStore(cfg)
	event := Event{Type: "cenc_eew", Magnitude: 6.2, DepthKM: 10, Latitude: 30.7, Longitude: 103.9}
	site := SiteCondition{VS30: 260, Uncertainty: 0.12, Version: geoSCKSiteDataVersion}
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		for index := 0; index < 700; index++ {
			distance := float64(index%350) + 1
			_ = predictIntensity(cfg, event, distance, math.Sqrt(distance*distance+100), site)
		}
	}
}
