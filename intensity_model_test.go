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
	prediction := predictIntensity(Config{Alert: AlertConfig{IntensityModelMode: "shadow"}}, event, 80, math.Sqrt(80*80+12*12), 0, SiteCondition{})
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
	prediction := predictIntensity(Config{Alert: AlertConfig{IntensityModelMode: "active", IntensityModelMaxCorrection: 0.8}}, event, 120, math.Sqrt(120*120+100), 0, SiteCondition{})
	if !prediction.UsedModel || prediction.Selected != prediction.Candidate {
		t.Fatalf("active mode did not select model candidate: %#v", prediction)
	}
	if math.Abs(prediction.Candidate-prediction.Baseline) > 0.800001 {
		t.Fatalf("candidate escaped physical bound: %#v", prediction)
	}
}

func TestIntensityModelFallsBackOutsideOfficialDatasetDomain(t *testing.T) {
	event := Event{Type: "fj_eew", Magnitude: 3.6, DepthKM: 10, Latitude: 26.0, Longitude: 119.0}
	prediction := predictIntensity(Config{Alert: AlertConfig{IntensityModelMode: "active"}}, event, 30, math.Sqrt(30*30+100), 0, SiteCondition{})
	if prediction.UsedModel || prediction.FallbackReason != "magnitude_out_of_domain" {
		t.Fatalf("expected magnitude fallback, got %#v", prediction)
	}
	if prediction.Selected != prediction.Baseline {
		t.Fatalf("active fallback must use stable baseline: %#v", prediction)
	}
}

func TestIntensityModelFallsBackWhenDepthIsMissing(t *testing.T) {
	event := Event{Type: "fj_eew", Magnitude: 5.1, Latitude: 26.0, Longitude: 119.0}
	prediction := predictIntensity(Config{Alert: AlertConfig{IntensityModelMode: "active"}}, event, 30, 30, 0, SiteCondition{})
	if prediction.UsedModel || prediction.FallbackReason != "depth_missing" || prediction.Selected != prediction.Baseline {
		t.Fatalf("expected stable fallback for missing depth, got %#v", prediction)
	}
}

func TestIntensityModelAppliesBoundedSiteCorrectionOnlyToCandidate(t *testing.T) {
	event := Event{Type: "cenc_eew", Magnitude: 6.0, DepthKM: 12, Latitude: 30.7, Longitude: 103.9}
	cfg := Config{Alert: AlertConfig{IntensityModelMode: intensityModelModeActive, IntensityModelMaxCorrection: defaultModelCorrection}}
	soft := SiteCondition{VS30: 180, Uncertainty: 0.1, Version: geoSCKSiteDataVersion}
	prediction := predictIntensity(cfg, event, 80, math.Sqrt(80*80+12*12), 0, soft)
	if prediction.SiteCorrection <= 0 || prediction.SiteCorrection > maxSiteIntensityIncrease {
		t.Fatalf("unexpected soft-site correction: %#v", prediction)
	}
	if prediction.Selected != prediction.Candidate || math.Abs(prediction.Candidate-prediction.Baseline) > defaultModelCorrection+1e-9 {
		t.Fatalf("site correction escaped overall safety bound: %#v", prediction)
	}

	cfg.Alert.IntensityModelMode = intensityModelModeShadow
	shadow := predictIntensity(cfg, event, 80, math.Sqrt(80*80+12*12), 0, soft)
	if shadow.SiteCorrection != 0 || shadow.Selected != shadow.Legacy {
		t.Fatalf("shadow mode must audit the correction-free new algorithm without changing delivery: %#v", shadow)
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
	for _, mode := range []string{intensityModelModeActive} {
		t.Run(mode, func(t *testing.T) {
			cfg := Config{Alert: AlertConfig{IntensityModelMode: mode}}
			event := Event{Type: "cenc_eew", Magnitude: 4, DepthKM: 10, Latitude: 30.7, Longitude: 103.9}
			previous := -1.0
			for magnitude := 4.0; magnitude <= 8.0001; magnitude += 0.1 {
				event.Magnitude = magnitude
				current := predictIntensity(cfg, event, 80, math.Sqrt(80*80+100), 0, SiteCondition{}).Selected
				if current+1e-9 < previous {
					t.Fatalf("intensity decreased as magnitude increased: M%.1f=%.1f previous=%.1f", magnitude, current, previous)
				}
				previous = current
			}

			event.Magnitude = 6.0
			previous = math.MaxFloat64
			for distance := 0.0; distance <= 700; distance += 5 {
				current := predictIntensity(cfg, event, distance, math.Sqrt(distance*distance+100), 0, SiteCondition{}).Selected
				if current > previous+1e-9 {
					t.Fatalf("intensity increased as distance increased: D%.0f=%.1f previous=%.1f", distance, current, previous)
				}
				previous = current
			}
		})
	}
}

func TestYu2013GroundMotionIsMonotonicWithDistance(t *testing.T) {
	event := Event{Type: "cenc_eew", Magnitude: 6, DepthKM: 10, Latitude: 30.7, Longitude: 103.9}
	previousPGA, previousPGV := math.MaxFloat64, math.MaxFloat64
	for distance := 0.0; distance <= 300; distance += 5 {
		_, pga, pgv, reason := yu2013Intensity(event, distance, 45)
		if reason != "" {
			t.Fatalf("unexpected fallback at D%.0f: %s", distance, reason)
		}
		if pga > previousPGA+1e-12 || pgv > previousPGV+1e-12 {
			t.Fatalf("ground motion increased at D%.0f: PGA %.6f->%.6f PGV %.6f->%.6f", distance, previousPGA, pga, previousPGV, pgv)
		}
		previousPGA, previousPGV = pga, pgv
	}
}

func TestYu2013GroundMotionIncreasesWithMagnitudeAcrossDirections(t *testing.T) {
	for _, azimuth := range []float64{0, 45, 90, 135} {
		for _, distance := range []float64{1, 10, 50, 100, 200, 300} {
			event := Event{Type: "cenc_eew", DepthKM: 10, Latitude: 30.7, Longitude: 103.9}
			previousPGA, previousPGV := -1.0, -1.0
			for magnitude := 4.0; magnitude <= 8.0001; magnitude += 0.1 {
				event.Magnitude = magnitude
				_, pga, pgv, reason := yu2013Intensity(event, distance, azimuth)
				if reason != "" {
					t.Fatalf("unexpected fallback M%.1f D%.0f A%.0f: %s", magnitude, distance, azimuth, reason)
				}
				if pga+1e-12 < previousPGA || pgv+1e-12 < previousPGV {
					t.Fatalf("ground motion decreased M%.1f D%.0f A%.0f: PGA %.6f->%.6f PGV %.6f->%.6f", magnitude, distance, azimuth, previousPGA, pga, previousPGV, pgv)
				}
				previousPGA, previousPGV = pga, pgv
			}
		}
	}
}

func TestNewAlgorithmCoversEntireOperationalDomainWithoutLegacyFallback(t *testing.T) {
	cases := []struct {
		name     string
		event    Event
		distance float64
	}{
		{"small_mainland", Event{Type: "cenc_eew", Magnitude: 1.5, DepthKM: 5, Latitude: 30.7, Longitude: 103.9}, 2},
		{"small_japan", Event{Type: "jma_eew", Magnitude: 2.8, DepthKM: 10, Latitude: 35.7, Longitude: 139.7}, 15},
		{"far_field", Event{Type: "cenc_eew", Magnitude: 6.2, DepthKM: 10, Latitude: 30.7, Longitude: 103.9}, 5000},
		{"deep_source", Event{Type: "cwa_eew", Magnitude: 7.1, DepthKM: 650, Latitude: 23.5, Longitude: 121.0}, 300},
		{"large_event", Event{Type: "jma_eew", Magnitude: 8.8, DepthKM: 30, Latitude: 35.7, Longitude: 139.7}, 1000},
	}
	for _, item := range cases {
		t.Run(item.name, func(t *testing.T) {
			prediction := predictIntensity(Config{Alert: AlertConfig{IntensityModelMode: intensityModelModeGBT2020}}, item.event, item.distance, math.Hypot(item.distance, item.event.DepthKM), 45, SiteCondition{})
			if prediction.FallbackReason != "" || !prediction.UsedModel || prediction.Version != yu2013IntensityAlgorithmVersion {
				t.Fatalf("new algorithm did not provide full coverage: %#v", prediction)
			}
			if math.IsNaN(prediction.Selected) || math.IsInf(prediction.Selected, 0) || prediction.Selected < 0 || prediction.Selected > 12 {
				t.Fatalf("invalid full-coverage intensity: %#v", prediction)
			}
		})
	}
	invalid := Event{Type: "cenc_eew", Magnitude: 5, DepthKM: math.NaN(), Latitude: 30.7, Longitude: 103.9}
	if _, _, _, reason := yu2013Intensity(invalid, 20, 45); reason != "non_finite_input" {
		t.Fatalf("invalid depth must be rejected explicitly, got %q", reason)
	}
}

func TestNewAlgorithmIsMonotonicAcrossCoverageTransitions(t *testing.T) {
	cfg := Config{Alert: AlertConfig{IntensityModelMode: intensityModelModeGBT2020}}
	for _, eventType := range []string{"cenc_eew", "jma_eew"} {
		for _, distance := range []float64{0, 20, 100, 300, 1000, 5000} {
			event := Event{Type: eventType, DepthKM: 10, Latitude: 30.7, Longitude: 103.9}
			previous := -1.0
			for magnitude := 1.0; magnitude <= 9.0001; magnitude += 0.1 {
				event.Magnitude = magnitude
				current := predictIntensity(cfg, event, distance, math.Hypot(distance, event.DepthKM), 45, SiteCondition{}).Selected
				if current+1e-9 < previous {
					t.Fatalf("%s intensity decreased across magnitude transition M%.1f D%.0f: %.1f -> %.1f", eventType, magnitude, distance, previous, current)
				}
				previous = current
			}
		}
	}
	for _, magnitude := range []float64{1.5, 3.2, 4.2, 6.2, 8.5} {
		event := Event{Type: "cenc_eew", Magnitude: magnitude, DepthKM: 10, Latitude: 30.7, Longitude: 103.9}
		previous := math.MaxFloat64
		for distance := 0.0; distance <= 20000; distance += 10 {
			current := predictIntensity(cfg, event, distance, math.Hypot(distance, event.DepthKM), 45, SiteCondition{}).Selected
			if current > previous+1e-9 {
				t.Fatalf("intensity increased across distance transition M%.1f D%.0f: %.1f -> %.1f", magnitude, distance, previous, current)
			}
			previous = current
		}
	}
	for _, magnitude := range []float64{1.5, 4.2, 6.2, 8.5} {
		event := Event{Type: "cenc_eew", Magnitude: magnitude, Latitude: 30.7, Longitude: 103.9}
		previous := math.MaxFloat64
		for depth := 0.0; depth <= 700; depth += 5 {
			event.DepthKM = depth
			current := predictIntensity(cfg, event, 20, math.Hypot(20, depth), 45, SiteCondition{}).Selected
			if current > previous+1e-9 {
				t.Fatalf("intensity increased with depth M%.1f Z%.0f: %.1f -> %.1f", magnitude, depth, previous, current)
			}
			previous = current
		}
	}
}

func TestNewAlgorithmRecognizesCENCHistoryReplay(t *testing.T) {
	event := historicalEvent(HistoryRecord{Source: "cenc", EventID: "history-cenc", Magnitude: 6.2, DepthKM: 10, Latitude: 30.7, Longitude: 103.9})
	prediction := predictIntensity(Config{Alert: AlertConfig{IntensityModelMode: intensityModelModeGBT2020}}, event, 80, math.Sqrt(80*80+100), 45, SiteCondition{})
	if !prediction.UsedModel || prediction.FallbackReason != "" || prediction.Selected == prediction.Legacy {
		t.Fatalf("CENC history replay did not exercise the new algorithm: %#v", prediction)
	}
}

func TestInitialBearingDegrees(t *testing.T) {
	if got := initialBearingDegrees(0, 0, 0, 1); math.Abs(got-90) > 1e-9 {
		t.Fatalf("east bearing=%.6f want 90", got)
	}
	if got := initialBearingDegrees(0, 0, 1, 0); math.Abs(got) > 1e-9 {
		t.Fatalf("north bearing=%.6f want 0", got)
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

func TestGBTInstrumentalIntensityMatchesStandardBoundaries(t *testing.T) {
	// GB/T 17742-2020 table A.1: the lower VI boundary is approximately
	// PGA=0.457 m/s² and PGV=0.0381 m/s.
	if got := gbtInstrumentalIntensity(0.457, 0.0381); math.Abs(got-5.5) > 0.02 {
		t.Fatalf("unexpected lower VI boundary: %.4f", got)
	}
	// Once both component intensities reach VI, the standard selects IV.
	if got := gbtInstrumentalIntensity(0.936, 0.0817); math.Abs(got-6.5) > 0.02 {
		t.Fatalf("unexpected upper VI boundary: %.4f", got)
	}
	if got := gbtInstrumentalIntensity(0, 0.1); got != 0 {
		t.Fatalf("invalid ground motion must be rejected, got %.1f", got)
	}
	if got := gbtInstrumentalIntensity(1e-9, 1e-9); got != 1 {
		t.Fatalf("the standard requires a lower clamp of 1.0, got %.1f", got)
	}
}

func TestGBTPredictiveIntensityRemovesAppendixA7ThresholdDrop(t *testing.T) {
	before := gbtPredictiveIntensity(0.965, 0.055)
	after := gbtPredictiveIntensity(1.0, 0.056)
	if after < before {
		t.Fatalf("predictive conversion dropped across the A.7 threshold: %.4f -> %.4f", before, after)
	}
	if exact := gbtInstrumentalIntensity(1.0, 0.056); exact == after {
		t.Fatal("test inputs no longer exercise the separate predictive conversion")
	}
}

func TestNewAlgorithmUsesYu2013WithoutModelOrSiteCorrection(t *testing.T) {
	event := Event{Type: "cenc_eew", Magnitude: 6.2, DepthKM: 10, Latitude: 30.7, Longitude: 103.9}
	soft := SiteCondition{VS30: 180, Uncertainty: 0.1, Version: geoSCKSiteDataVersion}
	prediction := predictIntensity(Config{Alert: AlertConfig{IntensityModelMode: intensityModelModeGBT2020}}, event, 80, math.Sqrt(80*80+100), 45, soft)
	if !prediction.UsedModel || prediction.FallbackReason != "" || prediction.Selected != prediction.Candidate {
		t.Fatalf("expected active in-domain new algorithm: %#v", prediction)
	}
	if prediction.Standard < 1 || prediction.PredictedPGA <= 0 || prediction.PredictedPGV <= 0 {
		t.Fatalf("missing national-standard audit values: %#v", prediction)
	}
	if prediction.Version != yu2013IntensityAlgorithmVersion || prediction.Calibration != 0 || prediction.SiteCorrection != 0 || prediction.SiteVS30 != 0 {
		t.Fatalf("new algorithm unexpectedly used a model or site correction: %#v", prediction)
	}
}

func TestIntensityModelShadowAuditsNewAlgorithmCandidate(t *testing.T) {
	event := Event{Type: "cenc_eew", Magnitude: 6.2, DepthKM: 10, Latitude: 30.7, Longitude: 103.9}
	prediction := predictIntensity(Config{Alert: AlertConfig{IntensityModelMode: intensityModelModeShadow}}, event, 80, math.Sqrt(80*80+100), 0, SiteCondition{})
	if prediction.Selected != prediction.Legacy || !prediction.UsedModel || prediction.Standard <= 0 || prediction.Calibration != 0 {
		t.Fatalf("shadow must retain legacy delivery and new-algorithm audit: %#v", prediction)
	}
	expected := round1(prediction.Standard)
	if prediction.Model != expected {
		t.Fatalf("shadow candidate is not new-algorithm output: got %.1f want %.1f", prediction.Model, expected)
	}
}

func TestLoadConfigDefaultsIntensityModelToLegacy(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("bark:\n  server: https://api.day.app\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Alert.IntensityModelMode != intensityModelModeLegacy || cfg.Alert.IntensityModelMaxCorrection != defaultModelCorrection {
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
	event := Event{Type: "cenc_eew", Magnitude: 6.2, DepthKM: 10, Latitude: 30.7, Longitude: 103.9}
	site := SiteCondition{VS30: 260, Uncertainty: 0.12, Version: geoSCKSiteDataVersion}
	for _, mode := range []string{intensityModelModeActive, intensityModelModeGBT2020} {
		b.Run(mode, func(b *testing.B) {
			cfg := Config{Alert: AlertConfig{IntensityModelMode: mode}}
			cfg.intensityModelSettings = defaultIntensityModelSettingsStore(cfg)
			b.ReportAllocs()
			for n := 0; n < b.N; n++ {
				for index := 0; index < 700; index++ {
					distance := float64(index%350) + 1
					_ = predictIntensity(cfg, event, distance, math.Sqrt(distance*distance+100), 0, site)
				}
			}
		})
	}
}
