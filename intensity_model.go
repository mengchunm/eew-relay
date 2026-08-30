package main

import (
	"math"
	"strings"
)

const (
	intensityModelModeLegacy  = "legacy"
	intensityModelModeShadow  = "shadow"
	intensityModelModeActive  = "active"
	intensityModelModeGBT2020 = "gbt2020"
	// hybrid is retained only as a persisted-configuration migration alias.
	intensityModelModeHybrid = "hybrid"
	defaultModelCorrection   = 0.8
)

type intensityPrediction struct {
	Selected             float64
	Legacy               float64
	Baseline             float64
	Model                float64
	Standard             float64
	Calibration          float64
	Candidate            float64
	PredictedPGA         float64
	PredictedPGV         float64
	Mode                 string
	Version              string
	UsedModel            bool
	FallbackReason       string
	SiteVS30             float64
	SiteUncertainty      float64
	SiteCorrection       float64
	SiteCorrectionReason string
}

func normalizeIntensityModelMode(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case intensityModelModeLegacy:
		return intensityModelModeLegacy
	case intensityModelModeActive:
		return intensityModelModeActive
	case intensityModelModeGBT2020:
		return intensityModelModeGBT2020
	case intensityModelModeHybrid:
		return intensityModelModeGBT2020
	case intensityModelModeShadow:
		return intensityModelModeShadow
	default:
		return intensityModelModeLegacy
	}
}

func predictIntensity(cfg Config, event Event, epicentralKM, hypocentralKM, azimuthDegrees float64, site SiteCondition) intensityPrediction {
	settings := intensityModelSettings(cfg)
	mode := settings.Mode
	legacy := estimateIntensity(event.Magnitude, hypocentralKM)
	baseline := stableIntensityBaseline(event.Magnitude, epicentralKM)
	result := intensityPrediction{
		Selected:        legacy,
		Legacy:          legacy,
		Baseline:        baseline,
		Candidate:       baseline,
		Mode:            mode,
		Version:         intensityModelVersionForMode(mode),
		SiteVS30:        site.VS30,
		SiteUncertainty: site.Uncertainty,
	}
	if mode != intensityModelModeActive {
		result.SiteVS30 = 0
		result.SiteUncertainty = 0
	}
	if mode == intensityModelModeLegacy {
		result.FallbackReason = "legacy_mode"
		return result
	}

	var modelValue, standardValue, predictedPGA, predictedPGV float64
	var reason string
	switch mode {
	case intensityModelModeActive:
		modelValue, reason = officialIntensityModel(event, epicentralKM)
	case intensityModelModeGBT2020:
		standardValue, predictedPGA, predictedPGV, reason = yu2013Intensity(event, epicentralKM, azimuthDegrees)
		modelValue = standardValue
	case intensityModelModeShadow:
		standardValue, predictedPGA, predictedPGV, reason = yu2013Intensity(event, epicentralKM, azimuthDegrees)
		modelValue = standardValue
	}
	if reason != "" {
		result.FallbackReason = reason
		if mode == intensityModelModeActive {
			result.Selected = baseline
		}
		return roundedIntensityPrediction(result)
	}
	result.Model = modelValue
	result.Standard = standardValue
	result.PredictedPGA = predictedPGA
	result.PredictedPGV = predictedPGV
	result.UsedModel = true
	if mode == intensityModelModeGBT2020 || mode == intensityModelModeShadow {
		result.Candidate = clampFloat(modelValue, 1, 12)
		if mode == intensityModelModeGBT2020 {
			result.Selected = result.Candidate
		}
		return roundedIntensityPrediction(result)
	}
	maxCorrection := settings.MaxCorrection
	if maxCorrection <= 0 || maxCorrection > 2 {
		maxCorrection = defaultModelCorrection
	}
	result.Candidate = clampFloat(modelValue, baseline-maxCorrection, baseline+maxCorrection)
	result.SiteCorrection, result.SiteCorrectionReason = siteIntensityCorrection(site, baseline)
	result.Candidate += result.SiteCorrection
	result.Candidate = clampFloat(result.Candidate, baseline-maxCorrection, baseline+maxCorrection)
	result.Candidate = clampFloat(result.Candidate, 0, 12)
	if mode != intensityModelModeShadow {
		result.Selected = result.Candidate
	}
	return roundedIntensityPrediction(result)
}

func roundedIntensityPrediction(value intensityPrediction) intensityPrediction {
	value.Selected = round1(clampFloat(value.Selected, 0, 12))
	value.Legacy = round1(clampFloat(value.Legacy, 0, 12))
	value.Baseline = round1(clampFloat(value.Baseline, 0, 12))
	value.Model = round1(clampFloat(value.Model, 0, 12))
	value.Standard = round1(clampFloat(value.Standard, 0, 12))
	value.Calibration = round1(clampFloat(value.Calibration, 0, 12))
	value.Candidate = round1(clampFloat(value.Candidate, 0, 12))
	value.PredictedPGA = math.Round(value.PredictedPGA*1e6) / 1e6
	value.PredictedPGV = math.Round(value.PredictedPGV*1e6) / 1e6
	value.SiteVS30 = round1(value.SiteVS30)
	value.SiteUncertainty = math.Round(value.SiteUncertainty*1000) / 1000
	value.SiteCorrection = round1(value.SiteCorrection)
	return value
}

func intensityModelVersionForMode(mode string) string {
	if mode == intensityModelModeActive {
		return officialIntensityModelVersion
	}
	if mode == intensityModelModeGBT2020 || mode == intensityModelModeShadow {
		return yu2013IntensityAlgorithmVersion
	}
	return "legacy"
}

func gbtInstrumentalIntensity(pgaMPS2, pgvMPS float64) float64 {
	if pgaMPS2 <= 0 || pgvMPS <= 0 || math.IsNaN(pgaMPS2) || math.IsNaN(pgvMPS) ||
		math.IsInf(pgaMPS2, 0) || math.IsInf(pgvMPS, 0) {
		return 0
	}
	acceleration := 3.17*math.Log10(pgaMPS2) + 6.59
	velocity := 3.00*math.Log10(pgvMPS) + 9.77
	value := (acceleration + velocity) / 2
	if acceleration >= 6 && velocity >= 6 {
		value = velocity
	}
	return clampFloat(value, 1, 12)
}

// gbtPredictiveIntensity uses the two component equations from Appendix A of
// GB/T 17742-2020, but keeps their average continuous across the 6.0 switch.
// Formula A.7 is defined for measured station waveforms and can step downward
// when fed independently predicted PGA and PGV. A continuous conversion is
// required here so a larger magnitude cannot produce a smaller alert value.
func gbtPredictiveIntensity(pgaMPS2, pgvMPS float64) float64 {
	if pgaMPS2 <= 0 || pgvMPS <= 0 || math.IsNaN(pgaMPS2) || math.IsNaN(pgvMPS) ||
		math.IsInf(pgaMPS2, 0) || math.IsInf(pgvMPS, 0) {
		return 0
	}
	acceleration := 3.17*math.Log10(pgaMPS2) + 6.59
	velocity := 3.00*math.Log10(pgvMPS) + 9.77
	return clampFloat((acceleration+velocity)/2, 0, 12)
}

func stableIntensityBaseline(magnitude, epicentralKM float64) float64 {
	if magnitude <= 0 || epicentralKM < 0 || math.IsNaN(magnitude) || math.IsNaN(epicentralKM) {
		return 0
	}
	return clampFloat(1.363*magnitude+2.941-1.494*math.Log(epicentralKM+7), 0, 12)
}

func officialIntensityModel(event Event, epicentralKM float64) (float64, string) {
	if math.IsNaN(event.Magnitude) || math.IsInf(event.Magnitude, 0) ||
		math.IsNaN(event.DepthKM) || math.IsInf(event.DepthKM, 0) ||
		math.IsNaN(event.Latitude) || math.IsInf(event.Latitude, 0) ||
		math.IsNaN(event.Longitude) || math.IsInf(event.Longitude, 0) ||
		math.IsNaN(epicentralKM) || math.IsInf(epicentralKM, 0) {
		return 0, "non_finite_input"
	}
	if event.Magnitude < 4 || event.Magnitude > 8 {
		return 0, "magnitude_out_of_domain"
	}
	if event.DepthKM <= 0 {
		return 0, "depth_missing"
	}
	if event.DepthKM > 100 {
		return 0, "depth_out_of_domain"
	}
	if epicentralKM < 0 || epicentralKM > 700 {
		return 0, "distance_out_of_domain"
	}
	if event.Latitude < officialIntensityModelMinLatitude || event.Latitude > officialIntensityModelMaxLatitude ||
		event.Longitude < officialIntensityModelMinLongitude || event.Longitude > officialIntensityModelMaxLongitude {
		return 0, "region_out_of_domain"
	}
	features := [...]float64{event.Magnitude, math.Log(epicentralKM + 7), event.DepthKM, event.Latitude, event.Longitude}
	value := officialIntensityModelBase
	for _, root := range officialIntensityModelRoots {
		index := root
		for {
			node := officialIntensityModelNodes[index]
			if node.Feature < 0 {
				value += float64(node.Value)
				break
			}
			feature := features[node.Feature]
			if (math.IsNaN(feature) && node.MissingLeft) || feature <= float64(node.Threshold) {
				index = node.Left
			} else {
				index = node.Right
			}
		}
	}
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return 0, "non_finite_model_output"
	}
	return value, ""
}
